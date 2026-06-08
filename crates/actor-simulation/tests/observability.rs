//! Observability event stream (spec §16): supervision decisions and
//! `Terminated` deliveries are emitted as structured events, and
//! `Actor::register` runs at most once per actor type (spec §4.4).

use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::Backoff;
use actor_core::BoxError;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Event;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::Supervision;
use actor_core::SupervisionDecision;
use actor_core::Terminated;
use actor_core::TerminationReason;
use actor_simulation::SimSystem;
use serde::Deserialize;
use serde::Serialize;

mod support;

#[derive(Serialize, Deserialize)]
struct Boom;
impl Message for Boom {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("obs.Boom");
}

#[derive(Serialize, Deserialize)]
struct Stop;
impl Message for Stop {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("obs.Stop");
}

// --- §4.4: register runs at most once per actor type -------------------------

static REGISTER_CALLS: AtomicU32 = AtomicU32::new(0);

struct Counted;
impl Actor for Counted {
    type System = SimSystem;
    fn register(_r: &mut HandlerRegistry<Self>) {
        REGISTER_CALLS.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn register_runs_at_most_once_per_type() {
    let (sim, system) = support::local(40);
    sim.block_on(async move {
        // Three spawns of the same type share one cached dispatch table.
        let _a = system.spawn(Counted);
        let _b = system.spawn(Counted);
        let _c = system.spawn(Counted);
    });
    assert_eq!(REGISTER_CALLS.load(Ordering::SeqCst), 1);
}

// --- §11.2 / §16: supervision decisions are emitted --------------------------

struct Restartable;
impl Actor for Restartable {
    type System = SimSystem;
    fn supervision() -> Supervision {
        Supervision::restart(5, Duration::from_secs(10), Backoff::None)
    }
}
impl Handler<Boom> for Restartable {
    async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
        panic!("boom");
    }
}

struct Stoppable;
impl Actor for Stoppable {
    type System = SimSystem;
    // Default supervision: Stop.
}
impl Handler<Boom> for Stoppable {
    async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
        panic!("boom");
    }
}

fn supervised_decisions(events: &[Event]) -> Vec<SupervisionDecision> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::Supervised { decision, .. } => Some(*decision),
            _ => None,
        })
        .collect()
}

#[test]
fn restart_emits_a_supervised_restart_decision() {
    let (sim, system, recorder) = support::local_recorded(41);
    sim.block_on(async move {
        let actor = system.spawn_with(|| Restartable);
        let _ = actor.tell(Boom).await;
    });
    assert_eq!(
        supervised_decisions(&recorder.events()),
        vec![SupervisionDecision::Restart]
    );
}

#[test]
fn default_stop_emits_a_supervised_stop_decision() {
    let (sim, system, recorder) = support::local_recorded(42);
    sim.block_on(async move {
        let actor = system.spawn(Stoppable);
        let _ = actor.tell(Boom).await;
    });
    assert_eq!(
        supervised_decisions(&recorder.events()),
        vec![SupervisionDecision::Stop]
    );
}

// --- §12 / §16: Terminated deliveries are emitted ----------------------------

struct Target;
impl Actor for Target {
    type System = SimSystem;
}
impl Handler<Stop> for Target {
    async fn handle(&mut self, _msg: Stop, ctx: &Ctx<Self>) {
        ctx.stop();
    }
}

struct Watcher {
    target: ActorRef<Target>,
}
impl Actor for Watcher {
    type System = SimSystem;
    async fn started(&mut self, ctx: &Ctx<Self>) -> Result<(), BoxError> {
        ctx.watch(&self.target);
        Ok(())
    }
}
impl Handler<Terminated> for Watcher {
    async fn handle(&mut self, _signal: Terminated, _ctx: &Ctx<Self>) {}
}

#[test]
fn terminated_delivery_is_emitted_to_each_watcher() {
    let (sim, system, recorder) = support::local_recorded(43);
    let (target_id, watcher_id) = sim.block_on(async move {
        let clock = system.clock().clone();
        let target = system.spawn(Target);
        let watcher = system.spawn(Watcher {
            target: target.clone(),
        });
        // Let the watcher's `started` register the watch before the target stops.
        clock.sleep(Duration::from_millis(1)).await;
        let _ = target.tell(Stop).await;
        (target.id().clone(), watcher.id().clone())
    });

    let deliveries: Vec<(_, _, _)> = recorder
        .events()
        .into_iter()
        .filter_map(|e| match e {
            Event::TerminatedDelivered {
                target,
                watcher,
                reason,
            } => Some((target, watcher, reason)),
            _ => None,
        })
        .collect();

    assert_eq!(
        deliveries,
        vec![(target_id, watcher_id, TerminationReason::Stopped)]
    );
}
