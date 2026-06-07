//! Conformance: receptionist (spec §13) — gaps beyond the existing discovery
//! tests: empty lookups, concurrent registration merge, graceful-stop prune, and
//! anti-entropy convergence (late joiners and partition heals).

mod support;

use std::time::Duration;

use actor_cluster::SwimConfig;
use actor_core::ActorSystem;
use actor_core::Clock;
use actor_core::Key;
use actor_core::NodeId;
use actor_simulation::SimCluster;
use support::Greet;
use support::Greeter;
use support::Stop;

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
