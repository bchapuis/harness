//! Conformance: receptionist (spec §13) — empty lookups, concurrent registration
//! merge, graceful-stop prune, and anti-entropy convergence (late joiners and
//! partition heals), plus end-to-end remote service discovery, death-watch
//! pruning of a downed node's registrations, and subscription snapshot/updates.

mod support;

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
use support::Greet;
use support::Greeter;
use support::Stop;

// --- Remote-service discovery actor (spec §13) -------------------------------

type Sys = SimCluster;

struct ServiceGreeter;

impl Actor for ServiceGreeter {
    type System = Sys;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<ServiceGreet>();
    }
}

#[derive(Serialize, Deserialize)]
struct ServiceGreet;

impl Message for ServiceGreet {
    type Reply = String;
    const MANIFEST: Manifest = Manifest::new("recept.Greet");
}

impl Handler<ServiceGreet> for ServiceGreeter {
    async fn handle(&mut self, _msg: ServiceGreet, _ctx: &Ctx<Self>) -> String {
        "hello".into()
    }
}

const SERVICE_GREETERS: Key<ServiceGreeter> = Key::new("greeters");

fn fast_swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(200),
        indirect_count: 2,
    }
}

/// A brisk SWIM config: anti-entropy rides its probe cadence, so a short
/// interval converges discovery quickly in virtual time. A long suspect timeout
/// keeps the (conservative) detector from interfering with these tests.
fn brisk_swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_secs(30),
        ..SwimConfig::default()
    }
}

const GREETERS: Key<Greeter<SimCluster>> = Key::new("greeters");
const ABSENT: Key<Greeter<SimCluster>> = Key::new("absent");

#[test]
fn lookup_of_an_unregistered_key_is_empty() {
    let (sim, net) = support::cluster(1, None);
    let node = net.join(NodeId::new(1));
    let empty = sim.block_on(async move { node.receptionist().lookup(ABSENT).is_empty() });
    assert!(empty);
}

#[test]
fn lookup_returns_a_correctly_typed_ref_that_is_callable() {
    let (sim, net) = support::cluster(2, None);
    let node = net.join(NodeId::new(1));
    let reply = sim.block_on(async move {
        let greeter = node.spawn(Greeter::<SimCluster>::new("Hello"));
        node.receptionist().register(GREETERS, &greeter);
        let listing = node.receptionist().lookup(GREETERS);
        listing
            .first()
            .unwrap()
            .ask(Greet {
                name: "world".into(),
            })
            .await
    });
    assert_eq!(reply, Ok("Hello, world!".to_string()));
}

#[test]
fn concurrent_registrations_under_one_key_merge() {
    let (sim, net) = support::cluster(3, None);
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));
    let count = sim.block_on(async move {
        let clock = node_a.clock().clone();
        let a = node_a.spawn(Greeter::<SimCluster>::new("A"));
        let b = node_b.spawn(Greeter::<SimCluster>::new("B"));
        node_a.receptionist().register(GREETERS, &a);
        node_b.receptionist().register(GREETERS, &b);
        clock.sleep(Duration::from_millis(50)).await; // let registrations replicate
        node_a.receptionist().lookup(GREETERS).len()
    });
    assert_eq!(
        count, 2,
        "both registrations merge into the listing (OR-set)"
    );
}

#[test]
fn a_registered_actor_is_pruned_when_it_stops() {
    let (sim, net) = support::cluster(4, None);
    let node = net.join(NodeId::new(1));
    let count = sim.block_on(async move {
        let clock = node.clock().clone();
        let greeter = node.spawn(Greeter::<SimCluster>::new("Hello"));
        node.receptionist().register(GREETERS, &greeter);
        assert_eq!(node.receptionist().lookup(GREETERS).len(), 1);
        greeter.tell(Stop).await.unwrap(); // receptionist watches it
        clock.sleep(Duration::from_millis(5)).await;
        node.receptionist().lookup(GREETERS).len()
    });
    assert_eq!(count, 0, "a stopped actor is pruned from the listing");
}

#[test]
fn a_late_joiner_converges_via_anti_entropy() {
    let (sim, net) = support::cluster(7, Some(brisk_swim()));

    // A registers a greeter before B exists, so broadcast-on-change never
    // reaches B.
    let node_a = net.join(NodeId::new(1));
    let greeter = node_a.spawn(Greeter::<SimCluster>::new("Hello"));
    node_a.receptionist().register(GREETERS, &greeter);
    sim.run_for(Duration::from_millis(10));

    let node_b = net.join(NodeId::new(2));
    assert!(
        node_b.receptionist().lookup(GREETERS).is_empty(),
        "B has not yet heard about the registration"
    );

    // Anti-entropy pushes A's registry to B within a few probe intervals.
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        node_b.receptionist().lookup(GREETERS).len(),
        1,
        "the late joiner converges on the existing registration"
    );
}

#[test]
fn anti_entropy_reconciles_a_registration_after_a_partition_heals() {
    let (sim, net) = support::cluster(11, Some(brisk_swim()));
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    // Cut A↔B, then register on A: the broadcast-on-change is dropped in flight.
    net.partition(&[NodeId::new(1)], &[NodeId::new(2)]);
    let greeter = node_a.spawn(Greeter::<SimCluster>::new("Hello"));
    node_a.receptionist().register(GREETERS, &greeter);
    sim.run_for(Duration::from_secs(1));
    assert!(
        node_b.receptionist().lookup(GREETERS).is_empty(),
        "the registration cannot cross the partition"
    );

    // Heal; anti-entropy reconciles the registration B missed.
    net.heal();
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        node_b.receptionist().lookup(GREETERS).len(),
        1,
        "after the heal, B converges on the registration"
    );
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
        let greeter = node_b.spawn(ServiceGreeter);
        node_b.receptionist().register(SERVICE_GREETERS, &greeter);
        // Let the registration replicate to A.
        clock.sleep(Duration::from_millis(50)).await;

        // A discovers it with no hardcoded id, then calls it (location transparent).
        let listing = node_a.receptionist().lookup(SERVICE_GREETERS);
        let reply = match listing.first() {
            Some(service) => service.ask(ServiceGreet).await,
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
    let net = SimNetwork::new(&sim).with_gossip(
        fast_swim(),
        DowningPolicy::Timeout(Duration::from_millis(200)),
    );
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let greeter = node_b.spawn(ServiceGreeter);
    node_b.receptionist().register(SERVICE_GREETERS, &greeter);

    // Replication reaches A.
    sim.run_for(Duration::from_millis(500));
    assert_eq!(
        node_a.receptionist().lookup(SERVICE_GREETERS).len(),
        1,
        "A should have learned of B's greeter",
    );

    // B crashes; the detector downs it, and the receptionist's death watch prunes.
    net.crash(NodeId::new(2));
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        node_a.receptionist().lookup(SERVICE_GREETERS).len(),
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
        let stream = node.receptionist().subscribe(SERVICE_GREETERS);
        futures::pin_mut!(stream);

        let first = stream.next().await.expect("current snapshot");

        let greeter = node.spawn(ServiceGreeter);
        node.receptionist().register(SERVICE_GREETERS, &greeter);

        let second = stream.next().await.expect("update after registration");
        (first.len(), second.len())
    });

    assert_eq!((first, second), (0, 1));
}
