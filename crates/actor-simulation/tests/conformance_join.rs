//! Conformance: cluster join and leave (spec §9.1, §9.2, §9.3). A joiner enters
//! `Joining` and is admitted to `Up` by the leader once the cluster learns of it
//! via gossip; a graceful `leave` announces `Leaving` and the leader finalizes it
//! to the terminal `Down`.

mod support;

use std::time::Duration;

use actor_cluster::MemberStatus;
use actor_cluster::SwimConfig;
use actor_core::NodeId;

/// A brisk SWIM config so gossip and leader transitions converge quickly in
/// virtual time. A long suspect timeout and the default (conservative) downing
/// keep the failure detector from interfering with these lifecycle tests.
fn brisk_swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_secs(30),
        ..SwimConfig::default()
    }
}

#[test]
fn a_joiner_is_admitted_to_up_by_the_leader() {
    let (sim, net) = support::cluster(20, Some(brisk_swim()));
    // Two founding members; node 1 is the lowest id, hence the leader.
    let a = net.join(NodeId::new(1));
    let b = net.join(NodeId::new(2));
    // A joiner that knows only the seed (node 1), not node 2.
    let c = net.join_seeded(NodeId::new(3), &[NodeId::new(1)]);

    assert_eq!(
        c.membership().self_status(),
        MemberStatus::Joining,
        "a joiner starts Joining, not yet a full member"
    );

    // Gossip carries node 3 = Joining to the leader, which admits it; the
    // admission then propagates to every member.
    sim.run_for(Duration::from_secs(3));

    assert_eq!(a.leader(), Some(NodeId::new(1)), "lowest-id member leads");
    assert_eq!(
        c.membership().self_status(),
        MemberStatus::Up,
        "the joiner adopts Up once the leader admits it"
    );
    assert_eq!(
        a.membership().status(NodeId::new(3)),
        Some(MemberStatus::Up),
        "the leader admitted the joiner"
    );
    assert_eq!(
        b.membership().status(NodeId::new(3)),
        Some(MemberStatus::Up),
        "admission propagated to the non-leader peer"
    );
}

#[test]
fn a_graceful_leave_reaches_the_terminal_down() {
    let (sim, net) = support::cluster(21, Some(brisk_swim()));
    let a = net.join(NodeId::new(1)); // leader
    let b = net.join(NodeId::new(2));
    sim.run_for(Duration::from_millis(500)); // let the cluster settle

    // Node 2 announces a graceful leave; it is not the leader, so the leader
    // (node 1) finalizes it to Down (spec §9.3).
    b.leave();
    sim.run_for(Duration::from_secs(2));

    assert!(
        a.membership().is_down(NodeId::new(2)),
        "the leader finalized the leaving node to the terminal Down"
    );
    assert_eq!(
        b.membership().self_status(),
        MemberStatus::Down,
        "the leaving node observes its own Down and can shut down"
    );
}

#[test]
fn down_from_a_leave_is_terminal_and_does_not_resurrect() {
    let (sim, net) = support::cluster(22, Some(brisk_swim()));
    let a = net.join(NodeId::new(1));
    let b = net.join(NodeId::new(2));
    sim.run_for(Duration::from_millis(500));

    b.leave();
    sim.run_for(Duration::from_secs(2));
    assert!(a.membership().is_down(NodeId::new(2)));

    // Even though node 2's transport keeps answering pings, the leader must not
    // resurrect a downed member (spec §9.1, invariant #15).
    sim.run_for(Duration::from_secs(3));
    assert!(
        a.membership().is_down(NodeId::new(2)),
        "a downed node never returns to Up"
    );
}

#[test]
fn the_leader_defers_admission_until_the_cluster_converges() {
    // The leader admits a joiner only once membership has converged (spec §9.2):
    // while a partition leaves a member unreachable, admission is deferred; after
    // the heal, the joiner is admitted.
    let (sim, net) = support::cluster(40, Some(brisk_swim()));
    let a = net.join(NodeId::new(1)); // leader
    let _b = net.join(NodeId::new(2));
    sim.run_for(Duration::from_millis(500)); // a, b converge

    // Isolate B from everyone (so even indirect probing can't reach it): A sees B
    // suspect/unreachable — the cluster is not converged.
    net.partition(&[NodeId::new(2)], &[NodeId::new(1), NodeId::new(3)]);
    sim.run_for(Duration::from_secs(1));

    // A joiner reaches the leader (its seed, A) but must not be admitted yet.
    let c = net.join_seeded(NodeId::new(3), &[NodeId::new(1)]);
    sim.run_for(Duration::from_secs(1));
    assert_ne!(
        a.membership().status(NodeId::new(3)),
        Some(MemberStatus::Up),
        "the leader must not admit a joiner while the cluster is unconverged",
    );
    assert_eq!(
        c.membership().self_status(),
        MemberStatus::Joining,
        "the joiner is still awaiting admission",
    );

    // Heal: once every live member is reachable again, the joiner is admitted.
    net.heal();
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        c.membership().self_status(),
        MemberStatus::Up,
        "the joiner is admitted once the cluster converges",
    );
}

#[test]
fn a_long_dead_member_is_tombstoned_then_pruned() {
    // A member that stays down is eventually tombstoned `Removed` and then pruned
    // from the roster entirely, so the membership map does not grow without bound
    // under churn (spec §9.1).
    let (sim, net) = support::cluster(23, Some(brisk_swim()));
    let a = net.join(NodeId::new(1)); // leader
    let b = net.join(NodeId::new(2));
    sim.run_for(Duration::from_millis(500));

    // Graceful leave, then the node actually departs (stops participating); its
    // id will not be reused (spec §9.1).
    b.leave();
    sim.run_for(Duration::from_secs(2));
    b.shutdown();
    assert_eq!(
        a.membership().status(NodeId::new(2)),
        Some(MemberStatus::Down),
        "the leaving node is down, not yet tombstoned",
    );

    // After the tombstone window, Down → Removed.
    sim.run_for(Duration::from_secs(31));
    assert_eq!(
        a.membership().status(NodeId::new(2)),
        Some(MemberStatus::Removed),
        "a long-dead member is tombstoned",
    );

    // After the prune window, the entry is gone from the roster entirely.
    sim.run_for(Duration::from_secs(31));
    assert_eq!(
        a.membership().status(NodeId::new(2)),
        None,
        "the tombstone is pruned, bounding the roster",
    );
    // A live cluster member is unaffected.
    assert_eq!(a.membership().self_status(), MemberStatus::Up);
}
