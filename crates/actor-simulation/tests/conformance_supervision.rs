//! Conformance: supervision (spec §11) — backoff timing, custom decider
//! integration, repeated startup failure, and sibling isolation, together with
//! the restart/backoff cases from supervision.rs and the
//! hierarchy/escalate cases from escalation.rs, plus the local panic-containment
//! case from actor.rs (spec §11, invariant #18).

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
use actor_core::Event;
use actor_core::Fault;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Instant;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::Supervision;
use actor_core::SupervisionDecision;
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
    let waited = times[1].duration_since(times[0]);
    // Backoff is jittered (equal-jitter, spec §11.2): the restart waits between
    // half and all of the 100ms base, so it is delayed but desynchronized.
    assert!(
        waited >= Duration::from_millis(50) && waited <= Duration::from_millis(100),
        "the restart waited a jittered fraction of the base backoff, got {waited:?}",
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

// --- A by-value Restart degrades to Stop, reported as Stop (spec §11.2, §16) --
//
// `Restart` rebuilds an actor from its factory; an actor spawned **by value**
// (`spawn`, not `spawn_with`) has no factory, so the directive degrades to
// `Stop` (spec §11.2). The safety property is unaffected — the fault is still
// contained — but the event stream MUST report the *effective* decision, `Stop`,
// never the candidate `Restart` (§16): an observer or continuous checker that
// saw `Restart` would expect a `Restarted` that can never come.

struct RestartByValue;
impl Actor for RestartByValue {
    type System = SimSystem;
    fn supervision() -> Supervision {
        Supervision::restart(5, Duration::from_secs(3600), Backoff::None)
    }
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Boom>();
    }
}
impl Handler<Boom> for RestartByValue {
    async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
        panic!("boom");
    }
}

#[test]
fn a_by_value_restart_degrades_to_stop_and_reports_stop() {
    let (sim, system, recorder) = support::local_recorded(7);
    sim.block_on(async move {
        let actor = system.spawn(RestartByValue);
        let _ = actor.tell(Boom).await;
        // Let the fault propagate through supervision to quiescence.
        system.clock().clone().sleep(Duration::from_millis(1)).await;
    });
    let events = recorder.events();
    let decisions: Vec<SupervisionDecision> = events
        .iter()
        .filter_map(|e| match e {
            Event::Supervised {
                decision,
                fault: Fault::Message,
                ..
            } => Some(*decision),
            _ => None,
        })
        .collect();
    assert_eq!(
        decisions,
        vec![SupervisionDecision::Stop],
        "a by-value Restart degrades to Stop, and the event reports the effective decision (spec §11.2, §16)"
    );
    assert!(
        !events.iter().any(|e| matches!(e, Event::Restarted { .. })),
        "the actor never actually restarted, so no Restarted event may be emitted"
    );
    assert!(
        events.iter().any(|e| matches!(e, Event::ResignId { .. })),
        "the degraded actor stops and resigns its id (spec §4.2 step 5)"
    );
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

// =============================================================================
// Merged from supervision.rs (spec §11): restart, resume, and backoff.
// =============================================================================
mod merged_supervision {
    //! Supervision (spec §11): a faulting actor is restarted (re-running `started`
    //! on a fresh instance, keeping its mailbox), resumed, or stopped per its
    //! strategy; exceeding the restart window escalates to stop.

    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use actor_core::Actor;
    use actor_core::ActorRef;
    use actor_core::ActorSystem;
    use actor_core::Backoff;
    use actor_core::BoxError;
    use actor_core::CallError;
    use actor_core::Ctx;
    use actor_core::Handler;
    use actor_core::LocalSystem;
    use actor_core::Manifest;
    use actor_core::Message;
    use actor_core::Supervision;
    use actor_simulation::SimClock;
    use actor_simulation::SimEntropy;
    use actor_simulation::SimSpawner;
    use actor_simulation::Simulation;
    use serde::Deserialize;
    use serde::Serialize;

    type Sys = LocalSystem<SimClock, SimEntropy, SimSpawner>;

    fn system(seed: u64) -> (Simulation, Sys) {
        let sim = Simulation::new(seed);
        let system = LocalSystem::new(sim.clock(), sim.entropy(), sim.spawner());
        (sim, system)
    }

    #[derive(Serialize, Deserialize)]
    struct Boom;
    impl Message for Boom {
        type Reply = ();
        const MANIFEST: Manifest = Manifest::new("sup.Boom");
    }

    // --- Restart: fresh instance, re-runs `started`, keeps the mailbox -----------

    struct Flaky {
        starts: Arc<AtomicU32>,
    }

    impl Actor for Flaky {
        type System = Sys;

        async fn started(&mut self, _ctx: &Ctx<Self>) -> Result<(), BoxError> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn supervision() -> Supervision {
            Supervision::restart(5, Duration::from_secs(10), Backoff::None)
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Starts;
    impl Message for Starts {
        type Reply = u32;
        const MANIFEST: Manifest = Manifest::new("sup.Starts");
    }

    impl Handler<Boom> for Flaky {
        async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
            panic!("flaky handler");
        }
    }
    impl Handler<Starts> for Flaky {
        async fn handle(&mut self, _msg: Starts, _ctx: &Ctx<Self>) -> u32 {
            self.starts.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn restart_recreates_the_actor_and_reruns_started() {
        let (sim, system) = system(1);
        let starts = Arc::new(AtomicU32::new(0));
        let factory_starts = Arc::clone(&starts);

        let after = sim.block_on(async move {
            let actor = system.spawn_with(move || Flaky {
                starts: Arc::clone(&factory_starts),
            });
            let _ = actor.tell(Boom).await; // panic → restart, started runs again
            // `Starts` was enqueued before the restart and survives it (same mailbox).
            actor.ask(Starts).await
        });

        // `started` ran once at spawn and once after the restart.
        assert_eq!(after, Ok(2));
        assert_eq!(starts.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn exceeding_the_restart_window_escalates_to_stop() {
        let (sim, system) = system(2);
        let starts = Arc::new(AtomicU32::new(0));
        let factory_starts = Arc::clone(&starts);

        let result = sim.block_on(async move {
            // Allow at most 2 restarts; a 3rd fault stops the actor.
            let actor = system.spawn_with(move || Bounded {
                starts: Arc::clone(&factory_starts),
            });
            for _ in 0..3 {
                let _ = actor.tell(Boom).await;
            }
            actor.ask(Starts).await
        });

        assert_eq!(result, Err(CallError::DeadLetter));
    }

    // A variant with a tight restart limit.
    struct Bounded {
        starts: Arc<AtomicU32>,
    }
    impl Actor for Bounded {
        type System = Sys;
        async fn started(&mut self, _ctx: &Ctx<Self>) -> Result<(), BoxError> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn supervision() -> Supervision {
            Supervision::restart(2, Duration::from_secs(10), Backoff::None)
        }
    }
    impl Handler<Boom> for Bounded {
        async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
            panic!("bounded handler");
        }
    }
    impl Handler<Starts> for Bounded {
        async fn handle(&mut self, _msg: Starts, _ctx: &Ctx<Self>) -> u32 {
            self.starts.load(Ordering::SeqCst)
        }
    }

    // --- A child spawned via Ctx::spawn_with is restartable (spec §11.2) ----------
    //
    // The canonical parent→child path is `Ctx::spawn` (§11.1), but a value-spawned
    // child cannot be reconstructed, so its `Restart` directive degrades to `Stop`.
    // `Ctx::spawn_with` gives the child a factory, so `Restart` actually re-creates
    // it — keeping its id and mailbox — exactly like a factory-spawned root.

    struct Parent {
        starts: Arc<AtomicU32>,
        restartable: bool,
    }
    impl Actor for Parent {
        type System = Sys;
    }

    #[derive(Serialize, Deserialize)]
    struct GetChild;
    impl Message for GetChild {
        type Reply = ActorRef<Flaky>;
        const MANIFEST: Manifest = Manifest::new("sup.GetChild");
    }

    impl Handler<GetChild> for Parent {
        async fn handle(&mut self, _msg: GetChild, ctx: &Ctx<Self>) -> ActorRef<Flaky> {
            let starts = Arc::clone(&self.starts);
            if self.restartable {
                ctx.spawn_with(move || Flaky {
                    starts: Arc::clone(&starts),
                })
            } else {
                ctx.spawn(Flaky {
                    starts: Arc::clone(&starts),
                })
            }
        }
    }

    #[test]
    fn ctx_spawn_with_child_restarts_on_fault() {
        let (sim, system) = system(5);
        let starts = Arc::new(AtomicU32::new(0));
        let parent_starts = Arc::clone(&starts);

        let after = sim.block_on(async move {
            let parent = system.spawn(Parent {
                starts: parent_starts,
                restartable: true,
            });
            let child = parent.ask(GetChild).await.unwrap();
            let _ = child.tell(Boom).await; // panic → Restart re-creates the child
            // `Starts` was enqueued before the restart and survives it (same mailbox).
            child.ask(Starts).await
        });

        // `started` ran once at spawn and once after the restart.
        assert_eq!(after, Ok(2));
    }

    #[test]
    fn ctx_spawn_value_child_degrades_restart_to_stop() {
        let (sim, system) = system(6);
        let starts = Arc::new(AtomicU32::new(0));
        let parent_starts = Arc::clone(&starts);

        let after = sim.block_on(async move {
            let parent = system.spawn(Parent {
                starts: parent_starts,
                restartable: false,
            });
            let child = parent.ask(GetChild).await.unwrap();
            let _ = child.tell(Boom).await; // panic → Restart cannot re-create → Stop
            child.ask(Starts).await
        });

        // The value-spawned child could not be reconstructed, so it stopped.
        assert_eq!(after, Err(CallError::DeadLetter));
    }

    // --- Resume: keep state, drop the failed message -----------------------------

    struct Accumulator {
        sum: u32,
    }
    impl Actor for Accumulator {
        type System = Sys;
        fn supervision() -> Supervision {
            Supervision::resume()
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Add(u32);
    impl Message for Add {
        type Reply = ();
        const MANIFEST: Manifest = Manifest::new("sup.Add");
    }
    #[derive(Serialize, Deserialize)]
    struct Total;
    impl Message for Total {
        type Reply = u32;
        const MANIFEST: Manifest = Manifest::new("sup.Total");
    }

    impl Handler<Add> for Accumulator {
        async fn handle(&mut self, msg: Add, _ctx: &Ctx<Self>) {
            self.sum += msg.0;
        }
    }
    impl Handler<Boom> for Accumulator {
        async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
            panic!("accumulator handler");
        }
    }
    impl Handler<Total> for Accumulator {
        async fn handle(&mut self, _msg: Total, _ctx: &Ctx<Self>) -> u32 {
            self.sum
        }
    }

    #[test]
    fn resume_keeps_state_and_drops_the_failed_message() {
        let (sim, system) = system(3);
        let total = sim.block_on(async move {
            let actor = system.spawn(Accumulator { sum: 0 });
            actor.tell(Add(1)).await.unwrap();
            let _ = actor.tell(Boom).await; // panics → Resume keeps the actor + state
            actor.tell(Add(2)).await.unwrap();
            actor.ask(Total).await
        });
        assert_eq!(total, Ok(3));
    }

    // --- Backoff is honored (restart still recovers after a delay) ---------------

    #[test]
    fn restart_with_backoff_still_recovers() {
        let (sim, system) = system(4);
        let starts = Arc::new(AtomicU32::new(0));
        let factory_starts = Arc::clone(&starts);
        let observed = Arc::new(Mutex::new(0u32));
        let sink = Arc::clone(&observed);

        sim.block_on(async move {
            let actor = system.spawn_with(move || Backed {
                starts: Arc::clone(&factory_starts),
            });
            let _ = actor.tell(Boom).await;
            *sink.lock().unwrap() = actor.ask(Starts).await.unwrap();
        });

        // Recovered after the backoff delay; started ran twice.
        assert_eq!(*observed.lock().unwrap(), 2);
    }

    struct Backed {
        starts: Arc<AtomicU32>,
    }
    impl Actor for Backed {
        type System = Sys;
        async fn started(&mut self, _ctx: &Ctx<Self>) -> Result<(), BoxError> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn supervision() -> Supervision {
            Supervision::restart(
                5,
                Duration::from_secs(10),
                Backoff::Exponential {
                    base: Duration::from_millis(50),
                    max: Duration::from_secs(1),
                },
            )
        }
    }
    impl Handler<Boom> for Backed {
        async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
            panic!("backed handler");
        }
    }
    impl Handler<Starts> for Backed {
        async fn handle(&mut self, _msg: Starts, _ctx: &Ctx<Self>) -> u32 {
            self.starts.load(Ordering::SeqCst)
        }
    }
}

// =============================================================================
// Merged from escalation.rs (spec §11.1): the supervision hierarchy and the
// `Escalate` directive propagating up the tree.
// =============================================================================
mod merged_escalation {
    //! Supervision hierarchy (spec §11.1): a child that faults with `Escalate` fails
    //! its parent, which then applies its own strategy — stop, restart, or escalate
    //! further up the tree.

    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use actor_core::Actor;
    use actor_core::ActorId;
    use actor_core::ActorSystem;
    use actor_core::Backoff;
    use actor_core::Clock;
    use actor_core::Ctx;
    use actor_core::Handler;
    use actor_core::LocalSystem;
    use actor_core::Manifest;
    use actor_core::Message;
    use actor_core::Supervision;
    use actor_simulation::SimClock;
    use actor_simulation::SimEntropy;
    use actor_simulation::SimSpawner;
    use actor_simulation::Simulation;
    use serde::Deserialize;
    use serde::Serialize;

    type Sys = LocalSystem<SimClock, SimEntropy, SimSpawner>;

    fn sys(seed: u64) -> (Simulation, Sys) {
        let sim = Simulation::new(seed);
        let system = LocalSystem::new(sim.clock(), sim.entropy(), sim.spawner());
        (sim, system)
    }

    // --- Messages ----------------------------------------------------------------

    #[derive(Serialize, Deserialize)]
    struct Boom;
    impl Message for Boom {
        type Reply = ();
        const MANIFEST: Manifest = Manifest::new("esc.Boom");
    }

    #[derive(Serialize, Deserialize)]
    struct Ping;
    impl Message for Ping {
        type Reply = u32;
        const MANIFEST: Manifest = Manifest::new("esc.Ping");
    }

    /// Spawn a child and return its id (so a test can reach it).
    #[derive(Serialize, Deserialize)]
    struct SpawnChild;
    impl Message for SpawnChild {
        type Reply = ActorId;
        const MANIFEST: Manifest = Manifest::new("esc.SpawnChild");
    }

    // --- A child that escalates on fault -----------------------------------------

    struct Child;
    impl Actor for Child {
        type System = Sys;
        fn supervision() -> Supervision {
            Supervision::with(|_| actor_core::SupervisionDirective::Escalate)
        }
    }
    impl Handler<Boom> for Child {
        async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
            panic!("child boom");
        }
    }

    // --- A parent that stops when a child escalates ------------------------------

    struct StopParent;
    impl Actor for StopParent {
        type System = Sys;
        // Default supervision is Stop.
    }
    impl Handler<SpawnChild> for StopParent {
        async fn handle(&mut self, _msg: SpawnChild, ctx: &Ctx<Self>) -> ActorId {
            ctx.spawn(Child).id().clone()
        }
    }
    impl Handler<Ping> for StopParent {
        async fn handle(&mut self, _msg: Ping, _ctx: &Ctx<Self>) -> u32 {
            0
        }
    }

    #[test]
    fn child_escalation_stops_a_stop_parent() {
        let (sim, system) = sys(1);
        let result = sim.block_on(async move {
            let clock = system.clock().clone();
            let parent = system.spawn(StopParent);
            let child_id = parent.ask(SpawnChild).await.unwrap();
            system.resolve::<Child>(child_id).tell(Boom).await.unwrap();
            clock.sleep(Duration::from_millis(10)).await; // let the escalation land
            parent.ask(Ping).await
        });
        // The parent stopped because the child escalated, so it dead-letters.
        assert_eq!(result, Err(actor_core::CallError::DeadLetter));
    }

    // --- A parent that restarts when a child escalates ---------------------------

    struct RestartParent {
        starts: Arc<AtomicU32>,
    }
    impl Actor for RestartParent {
        type System = Sys;
        async fn started(&mut self, _ctx: &Ctx<Self>) -> Result<(), actor_core::BoxError> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn supervision() -> Supervision {
            Supervision::restart(5, Duration::from_secs(10), Backoff::None)
        }
    }
    impl Handler<SpawnChild> for RestartParent {
        async fn handle(&mut self, _msg: SpawnChild, ctx: &Ctx<Self>) -> ActorId {
            ctx.spawn(Child).id().clone()
        }
    }
    impl Handler<Ping> for RestartParent {
        async fn handle(&mut self, _msg: Ping, _ctx: &Ctx<Self>) -> u32 {
            self.starts.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn child_escalation_restarts_a_restart_parent() {
        let (sim, system) = sys(2);
        let starts = Arc::new(AtomicU32::new(0));
        let factory_starts = Arc::clone(&starts);

        let started_count = sim.block_on(async move {
            let clock = system.clock().clone();
            let parent = system.spawn_with(move || RestartParent {
                starts: Arc::clone(&factory_starts),
            });
            let child_id = parent.ask(SpawnChild).await.unwrap();
            system.resolve::<Child>(child_id).tell(Boom).await.unwrap();
            clock.sleep(Duration::from_millis(10)).await; // escalation → parent restart
            parent.ask(Ping).await
        });

        // `started` ran once initially and once after the restart.
        assert_eq!(started_count, Ok(2));
    }

    // --- Chained escalation: child → parent (escalate) → grandparent (stop) ------

    struct EscalatingParent;
    impl Actor for EscalatingParent {
        type System = Sys;
        fn supervision() -> Supervision {
            Supervision::with(|_| actor_core::SupervisionDirective::Escalate)
        }
    }
    impl Handler<SpawnChild> for EscalatingParent {
        async fn handle(&mut self, _msg: SpawnChild, ctx: &Ctx<Self>) -> ActorId {
            ctx.spawn(Child).id().clone()
        }
    }

    #[derive(Serialize, Deserialize)]
    struct SpawnParent;
    impl Message for SpawnParent {
        type Reply = ActorId;
        const MANIFEST: Manifest = Manifest::new("esc.SpawnParent");
    }

    struct Grandparent;
    impl Actor for Grandparent {
        type System = Sys;
        // Default Stop.
    }
    impl Handler<SpawnParent> for Grandparent {
        async fn handle(&mut self, _msg: SpawnParent, ctx: &Ctx<Self>) -> ActorId {
            ctx.spawn(EscalatingParent).id().clone()
        }
    }
    impl Handler<Ping> for Grandparent {
        async fn handle(&mut self, _msg: Ping, _ctx: &Ctx<Self>) -> u32 {
            0
        }
    }

    #[test]
    fn escalation_chains_up_to_the_grandparent() {
        let (sim, system) = sys(3);
        let result = sim.block_on(async move {
            let clock = system.clock().clone();
            let grandparent = system.spawn(Grandparent);
            let parent_id = grandparent.ask(SpawnParent).await.unwrap();
            let child_id = system
                .resolve::<EscalatingParent>(parent_id)
                .ask(SpawnChild)
                .await
                .unwrap();
            system.resolve::<Child>(child_id).tell(Boom).await.unwrap();
            clock.sleep(Duration::from_millis(10)).await;
            // child → parent escalates → grandparent stops.
            grandparent.ask(Ping).await
        });
        assert_eq!(result, Err(actor_core::CallError::DeadLetter));
    }
}

// =============================================================================
// Merged from actor.rs (spec §11, invariant #18): a handler panic is contained
// to its actor — the caller observes DeadLetter, the faulted actor resigns, and
// the node keeps serving other actors.
// =============================================================================
mod local_containment {
    use std::sync::Arc;

    use actor_core::Actor;
    use actor_core::ActorSystem;
    use actor_core::CallError;
    use actor_core::Ctx;
    use actor_core::Event;
    use actor_core::Handler;
    use actor_core::HandlerRegistry;
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

    // --- The Appendix A actor, used to prove the node survives a sibling's panic --

    struct Greeter {
        greeting: String,
        served: u64,
    }

    impl Actor for Greeter {
        type System = Sys;

        // Macro-free remote registration, folded into the actor (spec §4.4).
        fn register(r: &mut HandlerRegistry<Self>) {
            r.accept::<Greet>();
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Greet {
        name: String,
    }

    impl Message for Greet {
        type Reply = String;
        const MANIFEST: Manifest = Manifest::new("test.Greet");
    }

    impl Handler<Greet> for Greeter {
        async fn handle(&mut self, msg: Greet, _ctx: &Ctx<Self>) -> String {
            self.served += 1;
            format!("{}, {}!", self.greeting, msg.name)
        }
    }

    // --- Supervision containment (spec §11, invariant #18) ------------------------

    struct Panicky;

    impl Actor for Panicky {
        type System = Sys;
    }

    #[derive(Serialize, Deserialize)]
    struct Boom;

    impl Message for Boom {
        type Reply = ();
        const MANIFEST: Manifest = Manifest::new("test.Boom");
    }

    impl Handler<Boom> for Panicky {
        async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
            panic!("handler blew up");
        }
    }

    #[test]
    fn handler_panic_is_contained_node_survives() {
        let recorder = Recorder::new();
        let sim = Simulation::new(0);
        let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
            .events(Arc::new(recorder.clone()))
            .build();

        let (boom, hello) = sim.block_on(async move {
            let bomb = system.spawn(Panicky);
            // The panic unwinds the reply path, so the caller observes DeadLetter —
            // never a hang, and the node keeps running.
            let boom = bomb.ask(Boom).await;

            let greeter = system.spawn(Greeter {
                greeting: "Hello".into(),
                served: 0,
            });
            let hello = greeter
                .ask(Greet {
                    name: "world".into(),
                })
                .await;
            (boom, hello)
        });

        assert_eq!(boom, Err(CallError::DeadLetter));
        assert_eq!(hello, Ok("Hello, world!".to_string()));
        // The faulted actor resigned (default supervision directive: Stop).
        assert!(
            recorder
                .events()
                .iter()
                .any(|e| matches!(e, Event::ResignId { .. })),
        );
    }
}
