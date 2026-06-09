//! SWIM failure detection and the node-down cascade under fault injection
//! (spec §8.1, §9, §10). Crashes and partitions are injected under seed control;
//! the detector observes them and drives reachability and downing.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::Reachability;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

struct Greeter;

impl Actor for Greeter {
    type System = actor_simulation::SimCluster;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Greet>();
    }
}

#[derive(Serialize, Deserialize)]
struct Greet;

impl Message for Greet {
    type Reply = String;
    const MANIFEST: Manifest = Manifest::new("test.Greet");
}

impl Handler<Greet> for Greeter {
    async fn handle(&mut self, _msg: Greet, _ctx: &Ctx<Self>) -> String {
        "hi".into()
    }
}

/// Fast SWIM timings so a run covers detection in a fraction of a virtual second.
fn fast_swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(200),
        indirect_count: 2,
    }
}

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);

#[test]
fn crash_completes_in_flight_ask_with_unreachable() {
    // Invariant #2 / cascade step 3: an ask to a node that gets declared `down`
    // completes with `Unreachable` rather than hanging.
    let sim = Simulation::new(7);
    let net = SimNetwork::new(&sim).with_gossip(
        fast_swim(),
        DowningPolicy::Timeout(Duration::from_millis(200)),
    );
    let node_a = net.join(A);
    let node_b = net.join(B);

    let greeter = node_b.spawn(Greeter);
    let gid = greeter.id().clone();

    let result = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&result);
    let caller = node_a.clone();
    sim.spawner().launch(Box::pin(async move {
        let outcome = caller.resolve::<Greeter>(gid).ask(Greet).await;
        *sink.lock().unwrap() = Some(outcome);
    }));

    // B crashes while the ask is in flight.
    net.crash(B);
    sim.run_for(Duration::from_secs(10));

    assert_eq!(
        result.lock().unwrap().clone(),
        Some(Err(CallError::Unreachable)),
    );
    assert!(node_a.membership().is_down(B), "B must be declared down");
}

#[test]
fn partition_alone_does_not_down_a_node() {
    // Invariant #16: under the conservative policy a partition yields
    // `unreachable`, never `down`.
    let sim = Simulation::new(8);
    let net = SimNetwork::new(&sim).with_gossip(fast_swim(), DowningPolicy::Conservative);
    let node_a = net.join(A);
    let _node_b = net.join(B);

    net.partition(&[A], &[B]);
    sim.run_for(Duration::from_secs(2));

    assert_eq!(
        node_a.membership().reachability(B),
        Some(Reachability::Unreachable),
    );
    assert!(!node_a.membership().is_down(B), "partition must not down B");
}

#[test]
fn healed_partition_restores_reachability() {
    let sim = Simulation::new(9);
    let net = SimNetwork::new(&sim).with_gossip(fast_swim(), DowningPolicy::Conservative);
    let node_a = net.join(A);
    let _node_b = net.join(B);

    net.partition(&[A], &[B]);
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        node_a.membership().reachability(B),
        Some(Reachability::Unreachable),
    );

    net.heal();
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        node_a.membership().reachability(B),
        Some(Reachability::Reachable),
        "a successful probe after healing clears the suspicion",
    );
}

#[test]
fn down_is_terminal_even_after_healing() {
    // Invariant #15: a node observed `down` never returns, even if it becomes
    // reachable again.
    let sim = Simulation::new(10);
    let net = SimNetwork::new(&sim).with_gossip(
        fast_swim(),
        DowningPolicy::Timeout(Duration::from_millis(200)),
    );
    let node_a = net.join(A);
    let _node_b = net.join(B);

    net.crash(B);
    sim.run_for(Duration::from_secs(2));
    assert!(node_a.membership().is_down(B), "B must be down");

    net.heal();
    sim.run_for(Duration::from_secs(2));
    assert!(
        node_a.membership().is_down(B),
        "down is terminal: B stays down after healing",
    );
}
