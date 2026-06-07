//! Conformance: API-misuse semantics. Spec-defined or sensible behavior when an
//! actor author uses the API wrongly — double watch, redundant unwatch, repeated
//! stop, and a zero-capacity mailbox.

mod support;

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::BoxError;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Event;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::Terminated;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

use support::Greeter;
use support::Stop;

type Log = Arc<Mutex<Vec<i64>>>;

// --- §12 #11: watching the same target twice still yields exactly one signal --

struct DoubleWatcher<S: ActorSystem> {
    target: ActorRef<Greeter<S>>,
    log: Log,
    _system: PhantomData<fn() -> S>,
}
impl<S: ActorSystem> Actor for DoubleWatcher<S> {
    type System = S;
    async fn started(&mut self, ctx: &Ctx<Self>) -> Result<(), BoxError> {
        ctx.watch(&self.target);
        ctx.watch(&self.target); // redundant — must not double-deliver
        Ok(())
    }
}
impl<S: ActorSystem> Handler<Terminated> for DoubleWatcher<S> {
    async fn handle(&mut self, _signal: Terminated, _ctx: &Ctx<Self>) {
        self.log.lock().unwrap().push(-1);
    }
}

#[test]
fn watching_twice_still_yields_one_terminated() {
    let (sim, system) = support::local(1);
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&log);
    sim.block_on(async move {
        let clock = system.clock().clone();
        let target = system.spawn(Greeter::<SimSystem>::new("Hi"));
        system.spawn(DoubleWatcher {
            target: target.clone(),
            log: observed,
            _system: PhantomData,
        });
        clock.sleep(Duration::from_millis(1)).await;
        target.tell(Stop).await.unwrap();
        clock.sleep(Duration::from_millis(1)).await;
    });
    assert_eq!(*log.lock().unwrap(), vec![-1]);
}

// --- §12: unwatch of a target never watched is a harmless no-op --------------

#[derive(Serialize, Deserialize)]
struct Ping;
impl Message for Ping {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("conf.MisusePing");
}

struct IdleUnwatcher<S: ActorSystem> {
    target: ActorRef<Greeter<S>>,
    _system: PhantomData<fn() -> S>,
}
impl<S: ActorSystem> Actor for IdleUnwatcher<S> {
    type System = S;
    async fn started(&mut self, ctx: &Ctx<Self>) -> Result<(), BoxError> {
        ctx.unwatch(&self.target); // never watched — must not panic
        Ok(())
    }
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Ping>();
    }
}
impl<S: ActorSystem> Handler<Ping> for IdleUnwatcher<S> {
    async fn handle(&mut self, _msg: Ping, _ctx: &Ctx<Self>) -> u64 {
        1
    }
}

#[test]
fn unwatch_of_a_never_watched_target_is_a_noop() {
    let (sim, system) = support::local(2);
    let alive = sim.block_on(async move {
        let target = system.spawn(Greeter::<SimSystem>::new("Hi"));
        let actor = system.spawn(IdleUnwatcher {
            target,
            _system: PhantomData,
        });
        actor.ask(Ping).await
    });
    assert_eq!(alive, Ok(1), "the actor started cleanly and serves");
}

// --- §3.4: calling stop() repeatedly stops the actor exactly once ------------

#[derive(Serialize, Deserialize)]
struct StopNow;
impl Message for StopNow {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("conf.StopNow");
}

struct StopTwice<S> {
    _system: PhantomData<fn() -> S>,
}
impl<S: ActorSystem> Actor for StopTwice<S> {
    type System = S;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<StopNow>();
    }
}
impl<S: ActorSystem> Handler<StopNow> for StopTwice<S> {
    async fn handle(&mut self, _msg: StopNow, ctx: &Ctx<Self>) {
        ctx.stop();
        ctx.stop(); // idempotent
    }
}

#[test]
fn repeated_stop_resigns_exactly_once() {
    let (sim, system, recorder) = support::local_recorded(3);
    let id = sim.block_on(async move {
        let clock = system.clock().clone();
        let actor = system.spawn(StopTwice::<SimSystem> {
            _system: PhantomData,
        });
        let id = actor.id().clone();
        actor.tell(StopNow).await.unwrap();
        clock.sleep(Duration::from_millis(1)).await;
        id
    });
    let resigns = recorder
        .events()
        .iter()
        .filter(|e| matches!(e, Event::ResignId { id: rid } if *rid == id))
        .count();
    assert_eq!(resigns, 1);
}

// --- §6: a zero-capacity mailbox is rejected ---------------------------------

#[test]
#[should_panic(expected = "positive")]
fn a_zero_capacity_mailbox_is_rejected() {
    let sim = Simulation::new(4);
    let _ = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .mailbox_capacity(0)
        .build();
}
