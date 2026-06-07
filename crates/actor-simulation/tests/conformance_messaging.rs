//! Conformance: messaging interface — `ask` / `tell` / `ask_timeout` and the
//! error model (spec §3.3, §6, §14). Covers the gaps the audit found; cases that
//! already have coverage elsewhere are not duplicated here.

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
