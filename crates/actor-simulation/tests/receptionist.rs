//! The receptionist (spec §13): typed service discovery, cluster-replicated,
//! with pruning driven by death watch. Demonstrates the end-to-end goal — a
//! client node discovers a remote service and calls it — and the cascade's final
//! step: a downed node's registrations are pruned (spec §8.1 step 5).

use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Key;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;

type Sys = SimCluster;

struct Greeter;

impl Actor for Greeter {
    type System = Sys;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Greet>();
    }
}

#[derive(Serialize, Deserialize)]
struct Greet;

impl Message for Greet {
    type Reply = String;
    const MANIFEST: Manifest = Manifest::new("recept.Greet");
}

impl Handler<Greet> for Greeter {
    async fn handle(&mut self, _msg: Greet, _ctx: &Ctx<Self>) -> String {
        "hello".into()
    }
}

const GREETERS: Key<Greeter> = Key::new("greeters");

fn fast_swim(downing: DowningPolicy) -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(200),
        indirect_count: 2,
        downing,
    }
}

#[test]
fn lookup_discovers_a_remote_service_and_calls_it() {
    let sim = Simulation::new(1);
    let net = SimNetwork::new(&sim);
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let (count, reply) = sim.block_on(async move {
        let clock = node_a.clock().clone();
        // B hosts and publishes a greeter.
        let greeter = node_b.spawn(Greeter);
        node_b.receptionist().register(GREETERS, &greeter);
        // Let the registration replicate to A.
        clock.sleep(Duration::from_millis(50)).await;

        // A discovers it with no hardcoded id, then calls it (location transparent).
        let listing = node_a.receptionist().lookup(GREETERS);
        let reply = match listing.first() {
            Some(service) => service.ask(Greet).await,
            None => panic!("A should have discovered the greeter"),
        };
        (listing.len(), reply)
    });

    assert_eq!(count, 1);
    assert_eq!(reply, Ok("hello".to_string()));
}

#[test]
fn registration_is_pruned_when_its_node_goes_down() {
    // Cascade step 5 (spec §8.1, §13 rule 3): when B is declared down, A's
    // listing for the key it published loses that registration.
    let sim = Simulation::new(2);
    let net = SimNetwork::new(&sim).with_swim(fast_swim(DowningPolicy::Timeout(
        Duration::from_millis(200),
    )));
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let greeter = node_b.spawn(Greeter);
    node_b.receptionist().register(GREETERS, &greeter);

    // Replication reaches A.
    sim.run_for(Duration::from_millis(500));
    assert_eq!(
        node_a.receptionist().lookup(GREETERS).len(),
        1,
        "A should have learned of B's greeter",
    );

    // B crashes; the detector downs it, and the receptionist's death watch prunes.
    net.crash(NodeId::new(2));
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        node_a.receptionist().lookup(GREETERS).len(),
        0,
        "B's registration must be pruned once B is down",
    );
}

#[test]
fn subscribe_delivers_current_listing_then_updates() {
    // Spec §13 rule 4 / invariant #19: the current snapshot first, then a fresh
    // listing on every change.
    let sim = Simulation::new(3);
    let node = SimNetwork::new(&sim).join(NodeId::new(1));

    let (first, second) = sim.block_on(async move {
        let stream = node.receptionist().subscribe(GREETERS);
        futures::pin_mut!(stream);

        let first = stream.next().await.expect("current snapshot");

        let greeter = node.spawn(Greeter);
        node.receptionist().register(GREETERS, &greeter);

        let second = stream.next().await.expect("update after registration");
        (first.len(), second.len())
    });

    assert_eq!((first, second), (0, 1));
}
