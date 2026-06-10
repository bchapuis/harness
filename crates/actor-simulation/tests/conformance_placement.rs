//! Conformance: deterministic placement (utilities spec §2, invariant U1).
//!
//! The placement function is pure, so most of U1 is pinned by direct property
//! tests — determinism, permutation invariance, minimal movement, distribution
//! sanity, and the `top`/`owner` relation. The cluster tests then pin the
//! *serving set* (utilities spec §2.1): converged nodes derive equal sets and
//! therefore equal owners, and a drained or unreachable member leaves the set
//! (and is restored on resume/heal) without disturbing other keys.

use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_cluster::placement;
use actor_core::NodeId;
use actor_simulation::SimNetwork;
use actor_simulation::SimRegistry;
use actor_simulation::Simulation;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

/// Fast SWIM so a few simulated seconds cover several probe/gossip rounds.
fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

fn five_nodes() -> Vec<NodeId> {
    (1..=5).map(NodeId::new).collect()
}

/// A spread of keys for the statistical properties: distinct strings exercise
/// short and longer inputs.
fn keys() -> Vec<Vec<u8>> {
    (0..1000).map(|i| format!("key-{i}").into_bytes()).collect()
}

#[test]
fn owner_is_deterministic_and_permutation_invariant() {
    // U1: a pure function of (set, key) — repeated calls agree, and the order
    // the candidate set is presented in is irrelevant.
    let nodes = five_nodes();
    let mut reversed = nodes.clone();
    reversed.reverse();
    for key in keys() {
        let owner = placement::owner(&nodes, &key);
        assert!(owner.is_some());
        assert_eq!(owner, placement::owner(&nodes, &key), "repeat call differs");
        assert_eq!(
            owner,
            placement::owner(&reversed, &key),
            "candidate order changed the owner"
        );
    }
    assert_eq!(placement::owner(&[], b"anything"), None);
}

#[test]
fn removing_a_member_moves_only_its_keys() {
    // U1 minimal movement: drop node 3; every key it did not own keeps its
    // owner, and every key it owned lands somewhere else.
    let nodes = five_nodes();
    let removed = NodeId::new(3);
    let remaining: Vec<NodeId> = nodes.iter().copied().filter(|n| *n != removed).collect();
    for key in keys() {
        let before = placement::owner(&nodes, &key).unwrap();
        let after = placement::owner(&remaining, &key).unwrap();
        if before == removed {
            assert_ne!(after, removed);
        } else {
            assert_eq!(before, after, "a surviving member's key moved");
        }
    }
}

#[test]
fn adding_a_member_moves_only_keys_it_now_owns() {
    // U1 minimal movement, the join direction: a new member only ever *takes*
    // keys; no key moves between two pre-existing members.
    let four: Vec<NodeId> = five_nodes().into_iter().take(4).collect();
    let five = five_nodes();
    let added = NodeId::new(5);
    for key in keys() {
        let before = placement::owner(&four, &key).unwrap();
        let after = placement::owner(&five, &key).unwrap();
        assert!(
            after == before || after == added,
            "key moved between pre-existing members on a join"
        );
    }
}

#[test]
fn every_member_owns_a_share() {
    // Distribution sanity: with 1000 keys over 5 nodes a member owning nothing
    // (or nearly everything) means the hash is broken, not unlucky.
    let nodes = five_nodes();
    let mut counts = std::collections::BTreeMap::new();
    let keys = keys();
    for key in &keys {
        *counts
            .entry(placement::owner(&nodes, key).unwrap())
            .or_insert(0usize) += 1;
    }
    for &node in &nodes {
        let share = counts.get(&node).copied().unwrap_or(0);
        assert!(
            share > 100,
            "{node} owns {share}/1000 keys — far below a fair share"
        );
    }
}

#[test]
fn top_ranks_distinct_members_and_agrees_with_owner() {
    let nodes = five_nodes();
    for key in keys().into_iter().take(100) {
        let top = placement::top(&nodes, &key, 3);
        assert_eq!(top.len(), 3);
        let distinct: std::collections::BTreeSet<_> = top.iter().collect();
        assert_eq!(distinct.len(), 3, "top returned a duplicate member");
        assert_eq!(placement::top(&nodes, &key, 1), vec![top[0]]);
        assert_eq!(placement::owner(&nodes, &key), Some(top[0]));
        // Asking for more than the set holds returns the whole ranking.
        assert_eq!(placement::top(&nodes, &key, 9).len(), nodes.len());
    }
}

#[test]
fn converged_nodes_compute_identical_owners() {
    // The cluster half of U1: three converged views derive equal serving sets,
    // so `place` agrees everywhere (utilities spec §2.2 item 2).
    let sim = Simulation::new(7);
    let net = SimNetwork::new(&sim).with_gossip(swim(), DowningPolicy::Conservative);
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_secs(1));

    let serving = a.membership().serving_members();
    assert_eq!(serving, vec![A, B, C], "A's serving set is the full roster");
    assert_eq!(serving, b.membership().serving_members());
    assert_eq!(serving, c.membership().serving_members());

    for key in keys().into_iter().take(100) {
        let owner = a.place(&key);
        assert!(owner.is_some());
        assert_eq!(owner, b.place(&key));
        assert_eq!(owner, c.place(&key));
    }
}

#[test]
fn a_drained_member_leaves_the_serving_set_until_resumed() {
    // Utilities spec §2.1: `draining` is excluded from placement (the operator
    // cordon routes new work away), and `resume` restores it.
    let sim = Simulation::new(11);
    let registry = SimRegistry::new(&sim);
    for node in [A, B, C] {
        registry.register(node);
    }
    let sync = Duration::from_millis(200);
    let net = SimNetwork::new(&sim).with_registry(swim(), registry.client(), sync);
    let a = net.join(A);
    let _b = net.join(B);
    let _c = net.join(C);
    sim.run_for(Duration::from_millis(500));
    assert_eq!(a.membership().serving_members(), vec![A, B, C]);

    registry.drain(B);
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        a.membership().serving_members(),
        vec![A, C],
        "a draining member must not be assigned keys"
    );
    for key in keys().into_iter().take(200) {
        assert_ne!(a.place(&key), Some(B));
    }

    registry.resume(B);
    sim.run_for(Duration::from_secs(1));
    assert_eq!(a.membership().serving_members(), vec![A, B, C]);
}

#[test]
fn an_unreachable_member_leaves_the_serving_set_until_healed() {
    // Utilities spec §2.1: placement is stricter than the receptionist filter —
    // a confirmed-unreachable member owns nothing until reachability recovers.
    let sim = Simulation::new(13);
    let net = SimNetwork::new(&sim).with_gossip(swim(), DowningPolicy::Conservative);
    let a = net.join(A);
    let _b = net.join(B);
    let _c = net.join(C);
    sim.run_for(Duration::from_secs(1));

    net.partition(&[A, C], &[B]);
    sim.run_for(Duration::from_secs(1)); // suspect → confirmed unreachable
    assert_eq!(
        a.membership().serving_members(),
        vec![A, C],
        "an unreachable member must not be assigned keys"
    );

    net.heal();
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        a.membership().serving_members(),
        vec![A, B, C],
        "recovered reachability restores the member without re-registration"
    );
}
