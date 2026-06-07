//! Gossip-driven membership (spec §9.2, §10): incarnation-based merge propagates
//! reachability and `down` across the cluster, a suspected node refutes a false
//! suspicion by bumping its incarnation, and views converge once a partition
//! heals.

use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::Reachability;
use actor_cluster::SwimConfig;
use actor_core::NodeId;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

fn swim(downing: DowningPolicy) -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        // These tests exercise gossip dissemination, refutation, and downing
        // (invariants #14–#17), which need a *direct*-probe suspicion to actually
        // form — so indirect probing is disabled here; it has its own test.
        suspect_timeout: Duration::from_millis(500),
        indirect_count: 0,
        downing,
    }
}

/// Build a 3-node cluster, returning the network handle (for faults) and nodes.
fn three_nodes(
    seed: u64,
    downing: DowningPolicy,
) -> (Simulation, SimNetwork, SimCluster, SimCluster, SimCluster) {
    let sim = Simulation::new(seed);
    let net = SimNetwork::new(&sim).with_swim(swim(downing));
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    (sim, net, a, b, c)
}

/// Assert every node sees every other peer reachable.
fn assert_all_reachable(nodes: &[&SimCluster]) {
    for node in nodes {
        for peer in nodes {
            if node.node() != peer.node() {
                assert_eq!(
                    node.membership().reachability(peer.node()),
                    Some(Reachability::Reachable),
                    "{} should see {} reachable",
                    node.node(),
                    peer.node(),
                );
            }
        }
    }
}

#[test]
fn membership_converges_after_a_partition_heals() {
    // Invariant #14: once the partition heals, all nodes converge on a single
    // reachable view.
    let (sim, net, a, b, c) = three_nodes(1, DowningPolicy::Conservative);

    net.partition(&[A], &[B, C]);
    sim.run_for(Duration::from_secs(2));
    // A and {B,C} can't see each other across the partition.
    assert_eq!(
        a.membership().reachability(B),
        Some(Reachability::Unreachable),
    );

    net.heal();
    sim.run_for(Duration::from_secs(3));
    assert_all_reachable(&[&a, &b, &c]);
}

#[test]
fn a_suspected_node_refutes_a_false_suspicion() {
    // Invariant #17: A cannot reach C directly (a one-way partition), so it
    // suspects C; the suspicion travels via B to C, which refutes it by bumping
    // its incarnation. C never gets confirmed unreachable on A.
    let (sim, net, a, _b, c) = three_nodes(2, DowningPolicy::Conservative);

    net.partition(&[A], &[C]); // only A <-> C is severed; B bridges.
    sim.run_for(Duration::from_secs(3));

    assert!(
        c.membership().self_incarnation() > 0,
        "C should have refuted a suspicion about itself",
    );
    assert!(
        !a.membership().is_down(C),
        "refutation keeps C from being downed on A",
    );
}

#[test]
fn down_propagates_to_every_node() {
    // A crashed node is declared down across the cluster (detection + gossip).
    let (sim, net, a, b, c) = three_nodes(3, DowningPolicy::Timeout(Duration::from_millis(300)));
    let _ = &c;

    net.crash(C);
    sim.run_for(Duration::from_secs(3));

    assert!(a.membership().is_down(C), "A must see C down");
    assert!(b.membership().is_down(C), "B must see C down");
}
