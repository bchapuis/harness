//! Conformance: transport fault injection (spec §7.2, §18.3). Per-pair FIFO
//! survives latency jitter (#3); total loss surfaces as `Timeout` rather than a
//! hang (#1); and duplication is tolerated — the framework gives at-most-once
//! *at the caller*, not exactly-once delivery (§7.2).

mod support;

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_simulation::FaultPolicy;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

use support::Counter;
use support::Get;
use support::Greet;
use support::Greeter;
use support::Inc;

fn latency_only(max_ms: u64) -> FaultPolicy {
    FaultPolicy {
        max_latency: Duration::from_millis(max_ms),
        ..FaultPolicy::default()
    }
}

// --- #3: per-pair FIFO survives latency jitter -------------------------------

#[derive(Serialize, Deserialize)]
struct Seq(u64);
impl Message for Seq {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("faults.Seq");
}

struct Order<S> {
    log: Arc<Mutex<Vec<u64>>>,
    _system: PhantomData<fn() -> S>,
}
impl<S: ActorSystem> Actor for Order<S> {
    type System = S;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Seq>();
    }
}
impl<S: ActorSystem> Handler<Seq> for Order<S> {
    async fn handle(&mut self, msg: Seq, _ctx: &Ctx<Self>) {
        self.log.lock().unwrap().push(msg.0);
    }
}

#[test]
fn per_pair_fifo_survives_latency_jitter() {
    let sim = Simulation::new(1);
    let net = SimNetwork::new(&sim).with_faults(latency_only(50));
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&log);
    sim.block_on(async move {
        let order = node_a.spawn(Order {
            log: observed,
            _system: PhantomData,
        });
        let remote = node_b.resolve::<Order<SimCluster>>(order.id().clone());
        for i in 0..10 {
            remote.tell(Seq(i)).await.unwrap();
        }
    });

    // Despite per-frame jitter, frames on one directed pair arrive in send order.
    assert_eq!(*log.lock().unwrap(), (0..10).collect::<Vec<_>>());
}

// --- #1 / §7.2: total loss completes with Timeout, never hangs ---------------

#[test]
fn total_loss_yields_timeout_not_a_hang() {
    let sim = Simulation::new(2);
    let net = SimNetwork::new(&sim).with_faults(FaultPolicy {
        drop_num: 1,
        drop_den: 1, // every frame is lost
        ..FaultPolicy::default()
    });
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let outcome = sim.block_on(async move {
        let greeter = node_a.spawn(Greeter::<SimCluster>::new("Hello"));
        node_b
            .resolve::<Greeter<SimCluster>>(greeter.id().clone())
            .ask_timeout(
                Greet {
                    name: "world".into(),
                },
                Duration::from_secs(1),
            )
            .await
    });
    assert_eq!(outcome, Err(CallError::Timeout));
}

// --- §7.2: duplication double-handles the server but the caller resolves once -

#[test]
fn duplication_is_tolerated_with_one_outcome_at_the_caller() {
    let sim = Simulation::new(3);
    let net = SimNetwork::new(&sim).with_faults(FaultPolicy {
        duplicate_num: 1,
        duplicate_den: 1, // every frame is duplicated
        ..FaultPolicy::default()
    });
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let count = sim.block_on(async move {
        let counter = node_a.spawn(Counter::<SimCluster>::new());
        let remote = node_b.resolve::<Counter<SimCluster>>(counter.id().clone());
        remote.tell(Inc).await.unwrap(); // duplicated → handled twice on the server
        remote.ask(Get).await // reply duplicated → caller resolves once
    });
    // Server double-handled the Inc (count == 2); the caller still got a single
    // well-formed reply.
    assert_eq!(count, Ok(2));
}
