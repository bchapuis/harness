//! Conformance: messaging interface — `ask` / `tell` / `ask_timeout` and the
//! error model (spec §3.3, §6, §14). Covers the gaps the audit found; cases that
//! already have coverage elsewhere are not duplicated here. Also folds in the
//! local actor-model messaging cases from actor.rs (spec §3, §6) and the
//! multi-node routing cases from cluster.rs (spec §4, §7, invariant #21).

mod support;

use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorId;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Path;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;

use support::Counter;
use support::Get;
use support::Inc;
use support::Slow;
use support::Work;

// --- §6 #4: tell applies backpressure (awaits), never drops ------------------

#[test]
fn tell_awaits_on_a_full_mailbox_then_delivers_everything() {
    let sim = Simulation::new(1);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .mailbox_capacity(2)
        .build();

    let served = sim.block_on(async move {
        let slow = system.spawn(Slow::new(system.clock().clone()));
        // Each handler sleeps, so the bounded mailbox fills; `tell` must await
        // space rather than drop or error (spec §6 #4, #5).
        for _ in 0..5 {
            slow.tell(Work { ms: 1 }).await.unwrap();
        }
        slow.ask(Get).await
    });

    assert_eq!(
        served,
        Ok(5),
        "all five tells must be delivered, none dropped"
    );
}

// --- §6 #5: the inbound remote path rejects with MailboxFull, never drops -----

#[test]
fn a_full_remote_mailbox_reports_mailboxfull_never_drops() {
    // Backpressure on the *inbound remote* path (spec §6 #5): when a remote
    // actor's bounded mailbox is full, the enqueue is rejected with `MailboxFull`
    // (the receive loop must not block on one full mailbox), and the message is
    // never silently dropped — every caller still gets an outcome.
    let sim = Simulation::new(2);
    let net = SimNetwork::new(&sim).with_mailbox_capacity(1);
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let outcomes = sim.block_on(async move {
        let slow = node_a.spawn(Slow::<SimCluster>::new(node_a.clock().clone()));
        let remote = node_b.resolve::<Slow<SimCluster>>(slow.id().clone());
        // A burst of concurrent asks; the handler sleeps, so a capacity-1 mailbox
        // cannot drain fast enough and overflows.
        futures::future::join_all(
            (0..8).map(|_| remote.ask_timeout(Work { ms: 100 }, Duration::from_secs(30))),
        )
        .await
    });

    assert_eq!(outcomes.len(), 8, "every ask must complete, none may hang");
    assert!(
        outcomes
            .iter()
            .any(|o| matches!(o, Err(CallError::MailboxFull))),
        "a full mailbox must surface MailboxFull, got {outcomes:?}",
    );
    assert!(
        outcomes
            .iter()
            .all(|o| matches!(o, Ok(_) | Err(CallError::MailboxFull))),
        "each ask resolves to Ok or MailboxFull — never a silent drop: {outcomes:?}",
    );
}

// --- §3.2 / §14.1: application errors live inside the reply (two-level) -------

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum MathError {
    DivByZero,
    Transport,
}

impl From<CallError> for MathError {
    fn from(_: CallError) -> Self {
        MathError::Transport
    }
}

#[derive(Serialize, Deserialize)]
struct Divide {
    a: i64,
    b: i64,
}
impl Message for Divide {
    type Reply = Result<i64, MathError>;
    const MANIFEST: Manifest = Manifest::new("conf.Divide");
}

struct Fallible<S> {
    _system: PhantomData<fn() -> S>,
}
impl<S> Fallible<S> {
    fn new() -> Self {
        Fallible {
            _system: PhantomData,
        }
    }
}
impl<S: ActorSystem> Actor for Fallible<S> {
    type System = S;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Divide>();
    }
}
impl<S: ActorSystem> Handler<Divide> for Fallible<S> {
    async fn handle(&mut self, msg: Divide, _ctx: &Ctx<Self>) -> Result<i64, MathError> {
        if msg.b == 0 {
            Err(MathError::DivByZero)
        } else {
            Ok(msg.a / msg.b)
        }
    }
}

#[test]
fn application_error_rides_inside_the_reply() {
    let (sim, system) = support::local(2);
    let (ok, app_err) = sim.block_on(async move {
        let svc = system.spawn(Fallible::<SimSystem>::new());
        let ok = svc.ask(Divide { a: 6, b: 2 }).await;
        let app_err = svc.ask(Divide { a: 1, b: 0 }).await;
        (ok, app_err)
    });
    // The call completed (outer Ok); the handler's outcome is the inner Result.
    assert_eq!(ok, Ok(Ok(3)));
    assert_eq!(app_err, Ok(Err(MathError::DivByZero)));
}

#[test]
fn a_transport_failure_is_the_outer_error_not_the_inner() {
    let (sim, system) = support::local(3);
    let outcome = sim.block_on(async move {
        // No such actor: the call cannot complete — outer `CallError`, not an
        // application error (spec §14.1, §14.3 — the two levels stay distinct).
        let ghost = ActorId::new(NodeId::new(0), Path::new("/user/404"), 0);
        system
            .resolve::<Fallible<SimSystem>>(ghost)
            .ask(Divide { a: 6, b: 2 })
            .await
    });
    assert_eq!(outcome, Err(CallError::DeadLetter));
}

// --- §14.2: deadlines on a slow-but-alive remote target -> Timeout -----------

fn two_nodes(seed: u64) -> (Simulation, SimCluster, SimCluster) {
    let (sim, net) = support::cluster(seed, None); // SWIM off: target stays alive
    let a = net.join(NodeId::new(1));
    let b = net.join(NodeId::new(2));
    (sim, a, b)
}

#[test]
fn ask_timeout_to_a_slow_alive_target_is_timeout_not_unreachable() {
    let (sim, node_a, node_b) = two_nodes(4);
    let outcome = sim.block_on(async move {
        let slow = node_a.spawn(Slow::new(node_a.clock().clone()));
        let remote = node_b.resolve::<Slow<SimCluster>>(slow.id().clone());
        // Handler sleeps far longer than the deadline; the node is alive.
        remote
            .ask_timeout(Work { ms: 10_000 }, Duration::from_secs(1))
            .await
    });
    assert_eq!(outcome, Err(CallError::Timeout));
}

#[test]
fn ask_default_deadline_elapses_to_timeout() {
    let (sim, node_a, node_b) = two_nodes(5);
    let outcome = sim.block_on(async move {
        let slow = node_a.spawn(Slow::new(node_a.clock().clone()));
        let remote = node_b.resolve::<Slow<SimCluster>>(slow.id().clone());
        // No explicit deadline: the system default applies and elapses.
        remote.ask(Work { ms: 60_000 }).await
    });
    assert_eq!(outcome, Err(CallError::Timeout));
}

// --- Spec interface not yet implemented (conformance checklist) --------------

#[test]
fn ask_flat_collapses_the_two_level_result() {
    let (sim, system) = support::local(6);
    let (ok, app_err, transport_err) = sim.block_on(async move {
        let svc = system.spawn(Fallible::<SimSystem>::new());
        let ok = svc.ask_flat(Divide { a: 6, b: 2 }).await;
        let app_err = svc.ask_flat(Divide { a: 1, b: 0 }).await;
        // A dead actor's outer CallError folds into the app error via From.
        let ghost = ActorId::new(NodeId::new(0), Path::new("/user/404"), 0);
        let transport_err = system
            .resolve::<Fallible<SimSystem>>(ghost)
            .ask_flat(Divide { a: 6, b: 2 })
            .await;
        (ok, app_err, transport_err)
    });
    assert_eq!(ok, Ok(3), "success collapses to the inner Ok");
    assert_eq!(
        app_err,
        Err(MathError::DivByZero),
        "an application error passes through unchanged",
    );
    assert_eq!(
        transport_err,
        Err(MathError::Transport),
        "a transport CallError is folded in via From<CallError>",
    );
}

// An actor that records the values it is told (lives on node A).
#[derive(Serialize, Deserialize)]
struct Record(i64);
impl Message for Record {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("conf.Record");
}

struct Collector {
    seen: Arc<Mutex<Vec<i64>>>,
}
impl Actor for Collector {
    type System = SimCluster;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Record>();
    }
}
impl Handler<Record> for Collector {
    async fn handle(&mut self, msg: Record, _ctx: &Ctx<Self>) {
        self.seen.lock().unwrap().push(msg.0);
    }
}

// A message carrying a *reference back to the sender* (lives on node B).
#[derive(Serialize, Deserialize)]
struct Callback {
    value: i64,
    reply_to: ActorRef<Collector>,
}
impl Message for Callback {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("conf.Callback");
}

struct Echoer;
impl Actor for Echoer {
    type System = SimCluster;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Callback>();
    }
}
impl Handler<Callback> for Echoer {
    async fn handle(&mut self, msg: Callback, _ctx: &Ctx<Self>) {
        // `reply_to` was rebound on decode to a handle that routes back to A.
        let _ = msg.reply_to.tell(Record(msg.value)).await;
    }
}

#[test]
fn actorref_in_a_message_rebinds_on_decode() {
    let (sim, node_a, node_b) = two_nodes(7);
    let seen: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&seen);

    sim.block_on(async move {
        let collector = node_a.spawn(Collector { seen: observed });
        let echoer = node_b.spawn(Echoer);
        // A sends B a message embedding a ref to A's collector. B must decode the
        // ref into a working handle and call back across the wire (spec §4.4).
        node_a
            .resolve::<Echoer>(echoer.id().clone())
            .tell(Callback {
                value: 42,
                reply_to: collector,
            })
            .await
            .unwrap();
        node_a.clock().sleep(Duration::from_millis(10)).await;
    });

    assert_eq!(
        *seen.lock().unwrap(),
        vec![42],
        "the embedded ActorRef rebound on B and called back to A",
    );
}

// A request whose *reply* carries an `ActorRef` (spec §4.4, #10).
#[derive(Serialize, Deserialize)]
struct Locate;
impl Message for Locate {
    type Reply = ActorRef<Collector>;
    const MANIFEST: Manifest = Manifest::new("conf.Locate");
}

// Lives on node B; hands out a ref to a collector that also lives on B.
struct Provider {
    collector: ActorRef<Collector>,
}
impl Actor for Provider {
    type System = SimCluster;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Locate>();
    }
}
impl Handler<Locate> for Provider {
    async fn handle(&mut self, _msg: Locate, _ctx: &Ctx<Self>) -> ActorRef<Collector> {
        self.collector.clone()
    }
}

#[test]
fn actorref_in_a_reply_rebinds_on_decode() {
    // The message path (above) covers a ref carried *into* an actor; #10 also
    // requires rebinding a ref carried *out* in a reply. A asks B's provider for
    // a collector ref; the reply crosses the wire, so on decode at A the ref must
    // rebind to a working handle that routes back to B (spec §4.4, #10).
    let (sim, node_a, node_b) = two_nodes(11);
    let seen: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&seen);

    sim.block_on(async move {
        let collector = node_b.spawn(Collector { seen: observed });
        let provider = node_b.spawn(Provider { collector });
        let located = node_a
            .resolve::<Provider>(provider.id().clone())
            .ask(Locate)
            .await
            .unwrap();
        // The reply-carried ref, used from A, must route across the wire to B.
        located.tell(Record(99)).await.unwrap();
        node_a.clock().sleep(Duration::from_millis(10)).await;
    });

    assert_eq!(
        *seen.lock().unwrap(),
        vec![99],
        "the ActorRef carried in the reply rebound on A and routed back to B",
    );
}

#[test]
fn when_local_runs_on_the_actors_executor() {
    // A local target: `when_local` reads, and mutates, the actor's state on its
    // own serial executor (spec §3.5.1).
    let (sim, system) = support::local(9);
    let observed = sim.block_on(async move {
        let counter = system.spawn(Counter::<SimSystem>::new());
        counter.tell(Inc).await.unwrap();
        counter.tell(Inc).await.unwrap();
        let read = counter.when_local(|c| c.count).await;
        let bumped = counter
            .when_local(|c| {
                c.count += 10;
                c.count
            })
            .await;
        (read, bumped)
    });
    assert_eq!(observed, (Some(2), Some(12)));
}

#[test]
fn when_local_is_none_for_a_remote_actor() {
    // A remote target: `when_local` must decline (spec §3.5.1) rather than reach
    // across the network.
    let (sim, node_a, node_b) = two_nodes(10);
    let result = sim.block_on(async move {
        let counter = node_a.spawn(Counter::<SimCluster>::new());
        node_b
            .resolve::<Counter<SimCluster>>(counter.id().clone())
            .when_local(|c| c.count)
            .await
    });
    assert_eq!(result, None);
}

// =============================================================================
// Merged from actor.rs (spec §3, §6): the local actor model running on
// `LocalSystem` — ask round-trip, registration, per-sender FIFO, serial
// non-reentrant execution, and the bounded non-dropping mailbox.
// =============================================================================
mod local_model {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use actor_core::Actor;
    use actor_core::ActorSystem;
    use actor_core::CallError;
    use actor_core::Clock;
    use actor_core::Ctx;
    use actor_core::Handler;
    use actor_core::HandlerRegistry;
    use actor_core::LocalSystem;
    use actor_core::LocalSystemBuilder;
    use actor_core::Manifest;
    use actor_core::Message;
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
}

// =============================================================================
// Merged from cluster.rs (spec §4, §7): multi-node routing over the simulated
// network — the same `ask`/`tell` call site works whether the target is local
// or remote (invariant #21), real JSON crosses every hop, and node/actor
// failures surface as the right `CallError`.
// =============================================================================
mod remote_routing {
    use actor_core::Actor;
    use actor_core::ActorSystem;
    use actor_core::CallError;
    use actor_core::Ctx;
    use actor_core::Handler;
    use actor_core::HandlerRegistry;
    use actor_core::Manifest;
    use actor_core::Message;
    use actor_core::NodeId;
    use actor_core::Path;
    use actor_simulation::SimCluster;
    use actor_simulation::SimNetwork;
    use actor_simulation::Simulation;
    use serde::Deserialize;
    use serde::Serialize;

    type Sys = SimCluster;

    struct Greeter {
        greeting: String,
    }

    impl Actor for Greeter {
        type System = Sys;

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
            format!("{}, {}!", self.greeting, msg.name)
        }
    }

    /// A two-node network on one simulation.
    fn two_nodes(seed: u64) -> (Simulation, SimCluster, SimCluster) {
        let sim = Simulation::new(seed);
        let net = SimNetwork::new(&sim);
        let a = net.join(NodeId::new(1));
        let b = net.join(NodeId::new(2));
        (sim, a, b)
    }

    #[test]
    fn remote_ask_crosses_nodes() {
        let (sim, node_a, node_b) = two_nodes(1);
        let reply = sim.block_on(async move {
            let greeter = node_a.spawn(Greeter {
                greeting: "Hello".into(),
            });
            // node_b holds no local actor for this id, so the call routes to node_a.
            let remote = node_b.resolve::<Greeter>(greeter.id().clone());
            remote
                .ask(Greet {
                    name: "world".into(),
                })
                .await
        });
        assert_eq!(reply, Ok("Hello, world!".to_string()));
    }

    #[test]
    fn location_transparency_local_vs_remote() {
        // Differential check (invariant #21): identical replies from the same call
        // site, target local versus remote.
        let (sim, node_a, node_b) = two_nodes(2);
        let (local, remote) = sim.block_on(async move {
            let greeter = node_a.spawn(Greeter {
                greeting: "Hi".into(),
            });
            let id = greeter.id().clone();
            let local = greeter.ask(Greet { name: "x".into() }).await;
            let remote = node_b
                .resolve::<Greeter>(id)
                .ask(Greet { name: "x".into() })
                .await;
            (local, remote)
        });
        assert_eq!(local, remote);
        assert_eq!(local, Ok("Hi, x!".to_string()));
    }

    // --- A counter to exercise remote tell + per-pair FIFO ------------------------

    struct Counter {
        n: u64,
    }

    impl Actor for Counter {
        type System = Sys;

        fn register(r: &mut HandlerRegistry<Self>) {
            r.accept::<Inc>();
            r.accept::<Get>();
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Inc;

    impl Message for Inc {
        type Reply = ();
        const MANIFEST: Manifest = Manifest::new("test.Inc");
    }

    #[derive(Serialize, Deserialize)]
    struct Get;

    impl Message for Get {
        type Reply = u64;
        const MANIFEST: Manifest = Manifest::new("test.Get");
    }

    impl Handler<Inc> for Counter {
        async fn handle(&mut self, _msg: Inc, _ctx: &Ctx<Self>) {
            self.n += 1;
        }
    }

    impl Handler<Get> for Counter {
        async fn handle(&mut self, _msg: Get, _ctx: &Ctx<Self>) -> u64 {
            self.n
        }
    }

    #[test]
    fn remote_tells_then_ask_observe_fifo() {
        let (sim, node_a, node_b) = two_nodes(3);
        let count = sim.block_on(async move {
            let counter = node_a.spawn(Counter { n: 0 });
            let remote = node_b.resolve::<Counter>(counter.id().clone());
            // tell/ask from one sender to one recipient are FIFO (spec §6, §7.2).
            for _ in 0..5 {
                remote.tell(Inc).await.unwrap();
            }
            remote.ask(Get).await
        });
        assert_eq!(count, Ok(5));
    }

    // --- Failure modes over the wire ----------------------------------------------

    #[test]
    fn ask_to_missing_remote_actor_is_dead_letter() {
        let (sim, _node_a, node_b) = two_nodes(4);
        let result = sim.block_on(async move {
            // A well-formed id on node 1, but no such actor was ever spawned.
            let ghost = actor_core::ActorId::new(NodeId::new(1), Path::new("/user/404"), 0);
            node_b
                .resolve::<Greeter>(ghost)
                .ask(Greet { name: "x".into() })
                .await
        });
        assert_eq!(result, Err(CallError::DeadLetter));
    }

    #[test]
    fn ask_to_unknown_node_is_unreachable() {
        let (sim, _node_a, node_b) = two_nodes(5);
        let result = sim.block_on(async move {
            // Node 99 never joined the network.
            let nowhere = actor_core::ActorId::new(NodeId::new(99), Path::new("/user/0"), 0);
            node_b
                .resolve::<Greeter>(nowhere)
                .ask(Greet { name: "x".into() })
                .await
        });
        assert_eq!(result, Err(CallError::Unreachable));
    }

    // An actor that handles a message but never registered it for remote dispatch.
    struct Closed;

    impl Actor for Closed {
        type System = Sys;
        // register() left as the default: accepts nothing over the wire.
    }

    impl Handler<Greet> for Closed {
        async fn handle(&mut self, _msg: Greet, _ctx: &Ctx<Self>) -> String {
            "unreachable by wire".into()
        }
    }

    #[test]
    fn unregistered_message_over_wire_is_unhandled() {
        let (sim, node_a, node_b) = two_nodes(6);
        let result = sim.block_on(async move {
            let closed = node_a.spawn(Closed);
            // Allowlist (spec §5, §15): the manifest is not registered on the target.
            node_b
                .resolve::<Closed>(closed.id().clone())
                .ask(Greet { name: "x".into() })
                .await
        });
        assert_eq!(result, Err(CallError::Unhandled));
    }
}
