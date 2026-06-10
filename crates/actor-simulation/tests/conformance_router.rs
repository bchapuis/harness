//! Conformance: group routers (utilities spec §3).
//!
//! Routers are node-local functions over the serving listing, so there is no
//! numbered invariant — these tests pin the §3 requirements directly:
//! deterministic round-robin in listing order, seeded (reproducible) random,
//! cross-node agreement of rendezvous-hashed routing with minimal movement,
//! fail-fast `DeadLetter` on an empty group, and the serving filter routing
//! around a drained node until it resumes.

mod support;

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::RouteStrategy;
use actor_cluster::Router;
use actor_cluster::SwimConfig;
use actor_core::ActorId;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::Clock;
use actor_core::Key;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::SimRegistry;
use actor_simulation::Simulation;
use support::Greet;
use support::Greeter;
use support::Stop;

const GREETERS: Key<Greeter<SimCluster>> = Key::new("greeters");
const ABSENT: Key<Greeter<SimCluster>> = Key::new("absent");

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

/// A brisk SWIM config so registrations replicate within a short virtual run.
fn brisk_swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_secs(30),
        ..SwimConfig::default()
    }
}

fn greet() -> Greet {
    Greet {
        name: "world".into(),
    }
}

/// Run `future` to completion under modes whose background loops never quiesce
/// (gossip detector/anti-entropy), where `block_on` would run forever: spawn
/// it, advance virtual time, take the result.
fn drive<T: Send + 'static>(
    sim: &Simulation,
    settle: Duration,
    future: impl Future<Output = T> + Send + 'static,
) -> T {
    let cell: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let out = Arc::clone(&cell);
    sim.spawner().launch(Box::pin(async move {
        *out.lock().unwrap() = Some(future.await);
    }));
    sim.run_for(settle);
    cell.lock()
        .unwrap()
        .take()
        .expect("future did not complete")
}

#[test]
fn round_robin_cycles_the_listing_in_order() {
    // §3 item 3: the cycle follows the listing's deterministic order, visiting
    // every routee once per revolution.
    let (sim, net) = support::cluster(21, None);
    let node = net.join(A);
    let replies = sim.block_on(async move {
        for name in ["one", "two", "three"] {
            let greeter = node.spawn(Greeter::<SimCluster>::new(name));
            node.receptionist().register(GREETERS, &greeter);
        }
        let router = Router::new(&node, GREETERS, RouteStrategy::RoundRobin);
        let mut replies = Vec::new();
        for _ in 0..6 {
            replies.push(router.ask(greet()).await.unwrap());
        }
        replies
    });
    assert_eq!(
        replies[..3],
        replies[3..],
        "the second revolution repeats the first in the same order"
    );
    let mut first: Vec<&String> = replies[..3].iter().collect();
    first.sort();
    first.dedup();
    assert_eq!(first.len(), 3, "one revolution visits every routee once");
}

#[test]
fn random_routing_is_seed_reproducible() {
    // §3 item 3: random draws come from the seeded entropy (core §18.1), so an
    // identical seed yields an identical pick sequence.
    let picks = |seed: u64| {
        let (sim, net) = support::cluster(seed, None);
        let node = net.join(A);
        sim.block_on(async move {
            for name in ["one", "two", "three"] {
                let greeter = node.spawn(Greeter::<SimCluster>::new(name));
                node.receptionist().register(GREETERS, &greeter);
            }
            let router = Router::new(&node, GREETERS, RouteStrategy::Random);
            let mut replies = Vec::new();
            for _ in 0..12 {
                replies.push(router.ask(greet()).await.unwrap());
            }
            replies
        })
    };
    assert_eq!(picks(42), picks(42), "same seed, same pick sequence");
}

#[test]
fn hashed_routing_agrees_across_nodes() {
    // §3 item 3: the same key over the same listing selects the same routee on
    // every node — this is what makes `route_by` usable for affinity.
    let (sim, net) = support::cluster(23, Some(brisk_swim()));
    let node_a = net.join(A);
    let node_b = net.join(B);
    let node_c = net.join(C);
    for (node, name) in [(&node_a, "a"), (&node_b, "b"), (&node_c, "c")] {
        let greeter = node.spawn(Greeter::<SimCluster>::new(name));
        node.receptionist().register(GREETERS, &greeter);
    }
    sim.run_for(Duration::from_secs(2)); // registrations replicate everywhere

    let router_a = Router::new(&node_a, GREETERS, RouteStrategy::RoundRobin);
    let router_b = Router::new(&node_b, GREETERS, RouteStrategy::RoundRobin);
    let router_c = Router::new(&node_c, GREETERS, RouteStrategy::RoundRobin);
    assert_eq!(router_a.routees().len(), 3);
    for i in 0..50 {
        let key = format!("session-{i}").into_bytes();
        let chosen = router_a.route_by(&key).unwrap().id().clone();
        assert_eq!(chosen, router_b.route_by(&key).unwrap().id().clone());
        assert_eq!(chosen, router_c.route_by(&key).unwrap().id().clone());
    }
}

#[test]
fn removing_an_unrelated_routee_leaves_other_keys_mapping_unchanged() {
    // Minimal movement at routee granularity (utilities spec §2 item 5 through
    // §3): stopping one routee only remaps the keys it owned.
    let (sim, net) = support::cluster(29, None);
    let node = net.join(A);
    sim.block_on(async move {
        let mut refs = Vec::new();
        for name in ["one", "two", "three", "four"] {
            let greeter = node.spawn(Greeter::<SimCluster>::new(name));
            node.receptionist().register(GREETERS, &greeter);
            refs.push(greeter);
        }
        let router = Router::new(&node, GREETERS, RouteStrategy::RoundRobin);

        let before: BTreeMap<usize, ActorId> = (0..200)
            .map(|i| {
                let key = format!("k-{i}").into_bytes();
                (i, router.route_by(&key).unwrap().id().clone())
            })
            .collect();

        let removed = refs.remove(1);
        let removed_id = removed.id().clone();
        removed.tell(Stop).await.unwrap();
        node.clock().sleep(Duration::from_millis(5)).await; // prune via watch

        assert_eq!(router.routees().len(), 3);
        for i in 0..200 {
            let key = format!("k-{i}").into_bytes();
            let after = router.route_by(&key).unwrap().id().clone();
            if before[&i] == removed_id {
                assert_ne!(after, removed_id);
            } else {
                assert_eq!(before[&i], after, "an unaffected key moved");
            }
        }
    });
}

#[test]
fn an_empty_group_fails_fast_with_dead_letter() {
    // §3 item 4: no routee, no buffering — the call returns immediately.
    let (sim, net) = support::cluster(31, None);
    let node = net.join(A);
    let (ask, tell, keyed) = sim.block_on(async move {
        let router = Router::new(&node, ABSENT, RouteStrategy::RoundRobin);
        assert!(router.is_empty());
        (
            router.ask(greet()).await,
            router.tell(greet()).await,
            router.ask_by(b"some-key", greet()).await,
        )
    });
    assert_eq!(ask, Err(CallError::DeadLetter));
    assert_eq!(tell, Err(CallError::DeadLetter));
    assert_eq!(keyed, Err(CallError::DeadLetter));
}

#[test]
fn a_drained_routee_is_routed_around_until_resumed() {
    // §3 item 2: decisions draw from the serving listing, so an operator drain
    // removes a routee from rotation and a resume restores it — without any
    // re-registration.
    let sim = Simulation::new(37);
    let registry = SimRegistry::new(&sim);
    for node in [A, B] {
        registry.register(node);
    }
    let net = SimNetwork::new(&sim).with_registry(
        brisk_swim(),
        registry.client(),
        Duration::from_millis(200),
    );
    let node_a = net.join(A);
    let node_b = net.join(B);
    for (node, name) in [(&node_a, "a"), (&node_b, "b")] {
        let greeter = node.spawn(Greeter::<SimCluster>::new(name));
        node.receptionist().register(GREETERS, &greeter);
    }
    sim.run_for(Duration::from_secs(1));

    let router = Router::new(&node_a, GREETERS, RouteStrategy::RoundRobin);
    assert_eq!(router.routees().len(), 2);

    registry.drain(B);
    sim.run_for(Duration::from_secs(1));
    assert_eq!(router.routees().len(), 1, "the drained node's routee left");
    assert!(
        router
            .routees()
            .iter()
            .all(|routee| routee.id().node() == A),
        "only A's routee remains in rotation"
    );

    registry.resume(B);
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        router.routees().len(),
        2,
        "resume restores the routee without re-registration"
    );
}

#[test]
fn remote_routees_are_callable_through_the_router() {
    // §3 item 5: the router adds nothing to (and subtracts nothing from) the
    // underlying remote ask path.
    let (sim, net) = support::cluster(41, Some(brisk_swim()));
    let node_a = net.join(A);
    let node_b = net.join(B);
    let greeter = node_b.spawn(Greeter::<SimCluster>::new("Remote"));
    node_b.receptionist().register(GREETERS, &greeter);
    sim.run_for(Duration::from_secs(1)); // registration reaches A

    let reply = drive(&sim, Duration::from_secs(1), async move {
        let router = Router::new(&node_a, GREETERS, RouteStrategy::RoundRobin);
        router.ask(greet()).await
    });
    assert_eq!(reply, Ok("Remote, world!".to_string()));
}
