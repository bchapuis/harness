//! Phase B invariants (spec §3, §4, §6, §11): the local actor model running on
//! `LocalSystem` under the deterministic simulator.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::BoxError;
use actor_core::CallError;
use actor_core::Clock;
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

// --- Greeter: the Appendix A actor, local-only --------------------------------

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

#[test]
fn local_ask_round_trip() {
    let sim = Simulation::new(1);
    let system = LocalSystem::new(sim.clock(), sim.entropy(), sim.spawner());
    let reply = sim.block_on(async move {
        let greeter = system.spawn(Greeter {
            greeting: "Hello".into(),
            served: 0,
        });
        greeter
            .ask(Greet {
                name: "world".into(),
            })
            .await
    });
    assert_eq!(reply, Ok("Hello, world!".to_string()));
}

#[test]
fn registration_lists_accepted_manifests() {
    let mut registry = HandlerRegistry::<Greeter>::default();
    Greeter::register(&mut registry);
    assert_eq!(registry.accepted(), &["test.Greet"]);
}

// --- Per-sender FIFO (spec §6, invariant #3) ----------------------------------

struct Log {
    seen: Arc<Mutex<Vec<u64>>>,
}

impl Actor for Log {
    type System = Sys;
}

#[derive(Serialize, Deserialize)]
struct Note(u64);

impl Message for Note {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.Note");
}

impl Handler<Note> for Log {
    async fn handle(&mut self, msg: Note, _ctx: &Ctx<Self>) {
        self.seen.lock().unwrap().push(msg.0);
    }
}

#[test]
fn messages_from_one_sender_keep_fifo_order() {
    let sim = Simulation::new(5);
    let system = LocalSystem::new(sim.clock(), sim.entropy(), sim.spawner());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);

    sim.block_on(async move {
        let log = system.spawn(Log { seen: recorded });
        for i in 0..10u64 {
            log.tell(Note(i)).await.unwrap();
        }
    });

    assert_eq!(*seen.lock().unwrap(), (0..10).collect::<Vec<_>>());
}

// --- Serial, non-reentrant execution (spec §6, invariant #4) ------------------

struct Guard {
    active: Arc<AtomicBool>,
    clock: SimClock,
}

impl Actor for Guard {
    type System = Sys;
}

#[derive(Serialize, Deserialize)]
struct Work;

impl Message for Work {
    // `true` if the handler observed exclusive access; `false` on overlap.
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("test.Work");
}

impl Handler<Work> for Guard {
    async fn handle(&mut self, _msg: Work, _ctx: &Ctx<Self>) -> bool {
        if self.active.swap(true, Ordering::SeqCst) {
            return false; // someone else is already inside — reentrancy!
        }
        // Suspend mid-handler; a non-serial executor could start another message.
        self.clock.sleep(Duration::from_millis(10)).await;
        self.active.store(false, Ordering::SeqCst);
        true
    }
}

#[test]
fn executor_is_serial_and_non_reentrant() {
    let sim = Simulation::new(0);
    let system = LocalSystem::new(sim.clock(), sim.entropy(), sim.spawner());
    let clock = system.clock().clone();

    let (a, b) = sim.block_on(async move {
        let guard = system.spawn(Guard {
            active: Arc::new(AtomicBool::new(false)),
            clock,
        });
        let other = guard.clone();
        futures::future::join(guard.ask(Work), other.ask(Work)).await
    });

    assert_eq!((a, b), (Ok(true), Ok(true)));
}

// --- Bounded, non-dropping mailbox (spec §6, invariant #5) --------------------

struct Sink;

impl Actor for Sink {
    type System = Sys;
}

impl Handler<Note> for Sink {
    async fn handle(&mut self, _msg: Note, _ctx: &Ctx<Self>) {}
}

#[test]
fn full_mailbox_reports_mailboxfull_not_drop() {
    let sim = Simulation::new(0);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .mailbox_capacity(2)
        .build();

    // No `await` between sends, so the executor never runs and the bounded
    // mailbox fills: the third send is rejected rather than dropped.
    let outcome = sim.block_on(async move {
        let sink = system.spawn(Sink);
        let r1 = sink.try_tell(Note(1));
        let r2 = sink.try_tell(Note(2));
        let r3 = sink.try_tell(Note(3));
        (r1, r2, r3)
    });

    assert_eq!(outcome, (Ok(()), Ok(()), Err(CallError::MailboxFull)));
}

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
