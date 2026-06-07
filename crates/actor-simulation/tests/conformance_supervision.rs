//! Conformance: supervision (spec §11) — gaps beyond the existing supervision
//! and escalation tests: backoff timing, custom decider integration, repeated
//! startup failure, and sibling isolation.

mod support;

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorId;
use actor_core::ActorSystem;
use actor_core::Backoff;
use actor_core::BoxError;
use actor_core::CallError;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Fault;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Instant;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::Supervision;
use actor_core::SupervisionDirective;
use actor_simulation::SimClock;
use actor_simulation::SimSystem;
use serde::Deserialize;
use serde::Serialize;

use support::Boom;

#[derive(Serialize, Deserialize)]
struct Ping;
impl Message for Ping {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("conf.SupPing");
}

// --- Backoff delays the restart (spec §11.2) ---------------------------------

struct Backed {
    clock: SimClock,
    starts: Arc<Mutex<Vec<Instant>>>,
}
impl Actor for Backed {
    type System = SimSystem;
    async fn started(&mut self, _ctx: &Ctx<Self>) -> Result<(), BoxError> {
        self.starts.lock().unwrap().push(self.clock.now());
        Ok(())
    }
    fn supervision() -> Supervision {
        Supervision::restart(
            5,
            Duration::from_secs(3600),
            Backoff::Exponential {
                base: Duration::from_millis(100),
                max: Duration::from_secs(1),
            },
        )
    }
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Boom>();
    }
}
impl Handler<Boom> for Backed {
    async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
        panic!("boom");
    }
}

#[test]
fn restart_waits_for_the_backoff_before_re_creating() {
    let (sim, system) = support::local(1);
    let starts = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&starts);
    sim.block_on(async move {
        let clock = system.clock().clone();
        let actor = system.spawn_with(move || Backed {
            clock: clock.clone(),
            starts: Arc::clone(&observed),
        });
        let _ = actor.tell(Boom).await;
        // Let the backoff elapse and the restart's `started` record its time.
        system
            .clock()
            .clone()
            .sleep(Duration::from_millis(500))
            .await;
    });
    let times = starts.lock().unwrap();
    assert_eq!(times.len(), 2, "started once, then once more after restart");
    assert!(
        times[1].duration_since(times[0]) >= Duration::from_millis(100),
        "the restart waited at least the base backoff",
    );
}

// --- A custom decider drives the directive (spec §11.2) ----------------------

struct DeciderActor {
    _p: PhantomData<()>,
}
impl Actor for DeciderActor {
    type System = SimSystem;
    fn supervision() -> Supervision {
        Supervision::with(|fault| match fault {
            Fault::Message => SupervisionDirective::Resume,
            _ => SupervisionDirective::Stop,
        })
    }
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Boom>();
        r.accept::<Ping>();
    }
}
impl Handler<Boom> for DeciderActor {
    async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
        panic!("boom");
    }
}
impl Handler<Ping> for DeciderActor {
    async fn handle(&mut self, _msg: Ping, _ctx: &Ctx<Self>) -> u64 {
        42
    }
}

#[test]
fn a_custom_decider_resumes_on_a_message_fault() {
    let (sim, system) = support::local(2);
    let after = sim.block_on(async move {
        let actor = system.spawn(DeciderActor { _p: PhantomData });
        let _ = actor.tell(Boom).await; // decider returns Resume → actor survives
        actor.ask(Ping).await
    });
    assert_eq!(after, Ok(42));
}

// --- Repeated startup failure escalates to Stop (spec §11.2) -----------------

struct NeverStarts {
    attempts: Arc<AtomicU32>,
}
impl Actor for NeverStarts {
    type System = SimSystem;
    async fn started(&mut self, _ctx: &Ctx<Self>) -> Result<(), BoxError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err("nope".into())
    }
    fn supervision() -> Supervision {
        Supervision::restart(2, Duration::from_secs(3600), Backoff::None)
    }
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Ping>();
    }
}
impl Handler<Ping> for NeverStarts {
    async fn handle(&mut self, _msg: Ping, _ctx: &Ctx<Self>) -> u64 {
        0
    }
}

#[test]
fn repeated_startup_failure_escalates_to_stop() {
    let (sim, system) = support::local(3);
    let attempts = Arc::new(AtomicU32::new(0));
    let observed = Arc::clone(&attempts);
    let outcome = sim.block_on(async move {
        let actor = system.spawn_with(move || NeverStarts {
            attempts: Arc::clone(&observed),
        });
        actor.ask(Ping).await
    });
    // Initial start + 2 restarts = 3 attempts, then Stop.
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(outcome, Err(CallError::DeadLetter));
}

// --- A sibling is unaffected by a child's escalation (spec §11.1, §11.3) ------

struct Child;
impl Actor for Child {
    type System = SimSystem;
    fn supervision() -> Supervision {
        Supervision::with(|_| SupervisionDirective::Escalate)
    }
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Boom>();
        r.accept::<Ping>();
    }
}
impl Handler<Boom> for Child {
    async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
        panic!("boom");
    }
}
impl Handler<Ping> for Child {
    async fn handle(&mut self, _msg: Ping, _ctx: &Ctx<Self>) -> u64 {
        7
    }
}

#[derive(Serialize, Deserialize)]
struct SpawnKid;
impl Message for SpawnKid {
    type Reply = ActorId;
    const MANIFEST: Manifest = Manifest::new("conf.SpawnKid");
}

struct Parent;
impl Actor for Parent {
    type System = SimSystem;
    fn supervision() -> Supervision {
        // Resume on a child's escalation: the parent absorbs it and lives on.
        Supervision::resume()
    }
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<SpawnKid>();
    }
}
impl Handler<SpawnKid> for Parent {
    async fn handle(&mut self, _msg: SpawnKid, ctx: &Ctx<Self>) -> ActorId {
        ctx.spawn(Child).id().clone()
    }
}

#[test]
fn one_childs_escalation_leaves_a_sibling_alive() {
    let (sim, system) = support::local(4);
    let (faulted, sibling) = sim.block_on(async move {
        let clock = system.clock().clone();
        let parent = system.spawn(Parent);
        let kid_a = parent.ask(SpawnKid).await.unwrap();
        let kid_b = parent.ask(SpawnKid).await.unwrap();

        system
            .resolve::<Child>(kid_a.clone())
            .tell(Boom)
            .await
            .unwrap();
        clock.sleep(Duration::from_millis(1)).await;

        let faulted = system.resolve::<Child>(kid_a).ask(Ping).await;
        let sibling = system.resolve::<Child>(kid_b).ask(Ping).await;
        (faulted, sibling)
    });
    assert_eq!(
        faulted,
        Err(CallError::DeadLetter),
        "the escalating child is gone"
    );
    assert_eq!(sibling, Ok(7), "its sibling is unaffected");
}
