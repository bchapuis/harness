//! Conformance: lifecycle hooks, `Ctx`, and resolution (spec §3.4, §4.2, §4.3).
//! Also folds in the local lifecycle ordering cases from actor.rs (spec §4.2).

mod support;

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorId;
use actor_core::ActorSystem;
use actor_core::BoxError;
use actor_core::CallError;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::StopReason;
use actor_simulation::SimCluster;
use actor_simulation::SimSystem;
use serde::Deserialize;
use serde::Serialize;

use support::Boom;
use support::Greet;
use support::Greeter;
use support::Stop;

// --- §3.1.1: stopped() runs with the right StopReason ------------------------

type Recorded = Arc<Mutex<Option<StopReason>>>;

struct Recording<S> {
    reason: Recorded,
    _system: PhantomData<fn() -> S>,
}
impl<S> Recording<S> {
    fn new(reason: Recorded) -> Self {
        Recording {
            reason,
            _system: PhantomData,
        }
    }
}
impl<S: ActorSystem> Actor for Recording<S> {
    type System = S;
    async fn stopped(self, reason: StopReason) {
        *self.reason.lock().unwrap() = Some(reason);
    }
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Stop>();
        r.accept::<Boom>();
    }
}
impl<S: ActorSystem> Handler<Stop> for Recording<S> {
    async fn handle(&mut self, _msg: Stop, ctx: &Ctx<Self>) {
        ctx.stop();
    }
}
impl<S: ActorSystem> Handler<Boom> for Recording<S> {
    async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
        panic!("boom");
    }
}

#[test]
fn stopped_runs_with_stopped_reason_on_graceful_stop() {
    let (sim, system) = support::local(1);
    let reason: Recorded = Arc::new(Mutex::new(None));
    let observed = Arc::clone(&reason);
    sim.block_on(async move {
        let clock = system.clock().clone();
        let actor = system.spawn(Recording::<SimSystem>::new(observed));
        actor.tell(Stop).await.unwrap();
        clock.sleep(Duration::from_millis(1)).await;
    });
    assert_eq!(*reason.lock().unwrap(), Some(StopReason::Stopped));
}

#[test]
fn stopped_runs_with_failed_reason_on_panic() {
    let (sim, system) = support::local(2);
    let reason: Recorded = Arc::new(Mutex::new(None));
    let observed = Arc::clone(&reason);
    sim.block_on(async move {
        let clock = system.clock().clone();
        let actor = system.spawn(Recording::<SimSystem>::new(observed));
        let _ = actor.tell(Boom).await;
        clock.sleep(Duration::from_millis(1)).await;
    });
    assert_eq!(*reason.lock().unwrap(), Some(StopReason::Failed));
}

// --- §4.2 #6: failed startup resigns without becoming ready ------------------

struct FailsToStart<S> {
    _system: PhantomData<fn() -> S>,
}
impl<S: ActorSystem> Actor for FailsToStart<S> {
    type System = S;
    async fn started(&mut self, _ctx: &Ctx<Self>) -> Result<(), BoxError> {
        Err("startup refused".into())
    }
}

#[test]
fn failed_startup_emits_assign_then_resign_no_ready() {
    let (sim, system, recorder) = support::local_recorded(3);
    sim.block_on(async move {
        let _ = system.spawn(FailsToStart::<SimSystem> {
            _system: PhantomData,
        });
    });
    let lifecycle: Vec<&str> = recorder
        .events()
        .iter()
        .filter_map(|e| match e {
            actor_core::Event::AssignId { .. } => Some("assign"),
            actor_core::Event::ActorReady { .. } => Some("ready"),
            actor_core::Event::ResignId { .. } => Some("resign"),
            _ => None,
        })
        .collect();
    assert_eq!(lifecycle, vec!["assign", "resign"]);
}

// --- §3.4: Ctx::id / this / system -------------------------------------------

#[derive(Serialize, Deserialize)]
struct WhoAmI;
impl Message for WhoAmI {
    type Reply = ActorId;
    const MANIFEST: Manifest = Manifest::new("conf.WhoAmI");
}

#[derive(Serialize, Deserialize)]
struct Kick;
impl Message for Kick {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("conf.Kick");
}
#[derive(Serialize, Deserialize)]
struct Touched;
impl Message for Touched {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("conf.Touched");
}
#[derive(Serialize, Deserialize)]
struct Touch;
impl Message for Touch {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("conf.Touch");
}

struct Introspect<S> {
    touches: u64,
    _system: PhantomData<fn() -> S>,
}
impl<S: ActorSystem> Actor for Introspect<S> {
    type System = S;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<WhoAmI>();
        r.accept::<Kick>();
        r.accept::<Touch>();
        r.accept::<Touched>();
    }
}
impl<S: ActorSystem> Handler<WhoAmI> for Introspect<S> {
    async fn handle(&mut self, _msg: WhoAmI, ctx: &Ctx<Self>) -> ActorId {
        // `this()` must agree with `id()` (spec §3.4).
        assert_eq!(ctx.this().id(), ctx.id());
        ctx.id().clone()
    }
}
impl<S: ActorSystem> Handler<Kick> for Introspect<S> {
    async fn handle(&mut self, _msg: Kick, ctx: &Ctx<Self>) {
        // `this()` is a usable self-reference: send ourselves a message.
        ctx.this().tell(Touch).await.unwrap();
    }
}
impl<S: ActorSystem> Handler<Touch> for Introspect<S> {
    async fn handle(&mut self, _msg: Touch, _ctx: &Ctx<Self>) {
        self.touches += 1;
    }
}
impl<S: ActorSystem> Handler<Touched> for Introspect<S> {
    async fn handle(&mut self, _msg: Touched, _ctx: &Ctx<Self>) -> u64 {
        self.touches
    }
}

#[test]
fn ctx_id_matches_the_spawned_ref() {
    let (sim, system) = support::local(4);
    let (reported, expected) = sim.block_on(async move {
        let actor = system.spawn(Introspect::<SimSystem> {
            touches: 0,
            _system: PhantomData,
        });
        let reported = actor.ask(WhoAmI).await.unwrap();
        (reported, actor.id().clone())
    });
    assert_eq!(reported, expected);
}

#[test]
fn ctx_this_is_a_usable_self_reference() {
    let (sim, system) = support::local(5);
    let touches = sim.block_on(async move {
        let actor = system.spawn(Introspect::<SimSystem> {
            touches: 0,
            _system: PhantomData,
        });
        actor.ask(Kick).await.unwrap(); // handler tells itself `Touch`
        actor.ask(Touched).await
    });
    assert_eq!(touches, Ok(1));
}

// --- §4.3: resolution ---------------------------------------------------------

#[test]
fn resolve_a_live_local_id_yields_a_working_ref() {
    let (sim, system) = support::local(6);
    let reply = sim.block_on(async move {
        let greeter = system.spawn(Greeter::<SimSystem>::new("Hello"));
        system
            .resolve::<Greeter<SimSystem>>(greeter.id().clone())
            .ask(Greet {
                name: "world".into(),
            })
            .await
    });
    assert_eq!(reply, Ok("Hello, world!".to_string()));
}

#[test]
fn resolve_a_resigned_id_dead_letters() {
    let (sim, system) = support::local(7);
    let reply = sim.block_on(async move {
        let clock = system.clock().clone();
        let greeter = system.spawn(Greeter::<SimSystem>::new("Hello"));
        let id = greeter.id().clone();
        greeter.tell(Stop).await.unwrap();
        clock.sleep(Duration::from_millis(1)).await; // let it resign
        system
            .resolve::<Greeter<SimSystem>>(id)
            .ask(Greet {
                name: "world".into(),
            })
            .await
    });
    assert_eq!(reply, Err(CallError::DeadLetter));
}

#[test]
fn locality_is_classified_from_the_id_without_a_network_call() {
    let (_sim, net) = support::cluster(8, None);
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));
    let greeter = node_a.spawn(Greeter::<SimCluster>::new("Hi"));
    let id = greeter.id().clone();
    assert!(node_a.is_local(&id), "owning node sees it local");
    assert!(!node_b.is_local(&id), "other node sees it remote");
}

// =============================================================================
// Merged from actor.rs (spec §4.2, invariant #6): local lifecycle ordering —
// assign → ready → resign exactly once, and a failed startup that resigns
// without ever becoming ready.
// =============================================================================
mod local_lifecycle {
    use std::sync::Arc;

    use actor_core::Actor;
    use actor_core::ActorSystem;
    use actor_core::BoxError;
    use actor_core::Ctx;
    use actor_core::Event;
    use actor_core::Handler;
    use actor_core::LocalSystem;
    use actor_core::LocalSystemBuilder;
    use actor_core::Manifest;
    use actor_core::Message;
    use actor_simulation::Recorder;
    use actor_simulation::SimClock;
    use actor_simulation::SimEntropy;
    use actor_simulation::SimSpawner;
    use actor_simulation::Simulation;
    use serde::Deserialize;
    use serde::Serialize;

    /// The concrete system every test actor runs on.
    type Sys = LocalSystem<SimClock, SimEntropy, SimSpawner>;

    // --- Lifecycle order and exactly-once (spec §4.2, invariant #6) ---------------

    struct Stoppable;

    impl Actor for Stoppable {
        type System = Sys;
    }

    #[derive(Serialize, Deserialize)]
    struct Stop;

    impl Message for Stop {
        type Reply = ();
        const MANIFEST: Manifest = Manifest::new("test.Stop");
    }

    impl Handler<Stop> for Stoppable {
        async fn handle(&mut self, _msg: Stop, ctx: &Ctx<Self>) {
            ctx.stop();
        }
    }

    #[test]
    fn lifecycle_runs_in_order_exactly_once() {
        let recorder = Recorder::new();
        let sim = Simulation::new(0);
        let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
            .events(Arc::new(recorder.clone()))
            .build();

        sim.block_on(async move {
            let actor = system.spawn(Stoppable);
            actor.tell(Stop).await.unwrap();
        });

        let lifecycle = lifecycle_only(&recorder.events());
        assert_eq!(
            lifecycle,
            vec!["AssignId", "ActorReady", "ResignId"],
            "lifecycle must be assign → ready → resign, each exactly once",
        );
    }

    struct FailStart;

    impl Actor for FailStart {
        type System = Sys;

        async fn started(&mut self, _ctx: &Ctx<Self>) -> Result<(), BoxError> {
            Err("startup refused".into())
        }
    }

    #[test]
    fn failed_startup_resigns_without_becoming_ready() {
        let recorder = Recorder::new();
        let sim = Simulation::new(0);
        let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
            .events(Arc::new(recorder.clone()))
            .build();

        sim.block_on(async move {
            let _ = system.spawn(FailStart);
        });

        // assign → resign, no ready: the id was reserved but never became live.
        assert_eq!(
            lifecycle_only(&recorder.events()),
            vec!["AssignId", "ResignId"]
        );
    }

    /// Project an event stream down to its lifecycle markers, in order.
    fn lifecycle_only(events: &[Event]) -> Vec<&'static str> {
        events
            .iter()
            .filter_map(|e| match e {
                Event::AssignId { .. } => Some("AssignId"),
                Event::ActorReady { .. } => Some("ActorReady"),
                Event::ResignId { .. } => Some("ResignId"),
                _ => None,
            })
            .collect()
    }
}
