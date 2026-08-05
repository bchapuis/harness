//! Conformance: the `actor.wire` compatibility boundary under simulation
//! (actor spec §7.1, compatibility spec §3.1, §4).
//!
//! The negotiated wire revision is what a send-side gate reads before composing a
//! frame: a build whose window spans two revisions must write the higher one only
//! to a peer that settled on it. These tests pin the two halves that makes
//! possible — that a simulated node can be told what its *own* build accepts, and
//! that `Transport::peer_version` answers with what the two ends would settle on.
//!
//! With every node on the shipped window there is one revision in the cluster and
//! nothing to settle, so the mixed cases here run against widened windows standing
//! in for the releases that will widen the real one (**V4**: read-new first,
//! write-new later). That is the point of routing every boundary through one
//! `Window` type — the policy can be exercised before a bump is taken.

use actor_cluster::Transport;
use actor_cluster::WIRE;
use actor_core::NodeId;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use compat::Accepted;
use compat::Version;
use compat::Window;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);

/// A window that reads up to `hi` and writes `writes` — the shape of a release
/// mid-bump. `Window::new` refuses `writes` outside the range at compile time
/// (**V3**), which is the invariant this helper must not be able to dodge.
fn window(lo: u16, hi: u16, writes: u16) -> Window {
    Window::new("actor.wire", lo, hi, writes)
}

#[test]
fn a_uniform_cluster_settles_on_the_revision_it_ships() {
    let sim = Simulation::new(1);
    let net = SimNetwork::new(&sim);
    let _a = net.join(A);
    let _b = net.join(B);

    // No node has been given a window, so every one runs the build's own.
    assert_eq!(net.wire_window(A).accepted(), WIRE.accepted());
    assert_eq!(
        net.transport(A).peer_version(B),
        Some(WIRE.accepted().hi),
        "two nodes on the shipped window settle on the highest revision it accepts",
    );
}

#[test]
fn a_peer_that_reads_further_does_not_pull_the_association_up() {
    // The rolling-upgrade middle: A has been upgraded to read v1..=v2, B has not
    // been touched. The association must stay at v1, because that is all B can
    // read — this is the case the gate exists for, and the one an equality check
    // would have turned into a refusal instead.
    let sim = Simulation::new(1);
    let net = SimNetwork::new(&sim);
    let _a = net.join(A);
    let _b = net.join(B);
    net.set_wire_window(A, window(1, 2, 1));

    assert_eq!(
        net.transport(A).peer_version(B),
        Some(Version(1)),
        "an upgraded node must settle down to what its un-upgraded peer reads",
    );
    assert_eq!(
        net.transport(B).peer_version(A),
        Some(Version(1)),
        "and both ends must reach that same answer without a confirmation round",
    );
}

#[test]
fn the_association_rises_only_once_both_ends_have_moved() {
    // The other half of **V4**: once every node reads v2, a release may write it,
    // and only then does the association carry it.
    let sim = Simulation::new(1);
    let net = SimNetwork::new(&sim);
    let _a = net.join(A);
    let _b = net.join(B);

    net.set_wire_window(A, window(1, 2, 2));
    net.set_wire_window(B, window(1, 2, 1)); // reads v2, still writes v1
    assert_eq!(
        net.transport(A).peer_version(B),
        Some(Version(2)),
        "a revision both ends accept is settled on, whichever they write",
    );

    net.set_wire_window(B, window(1, 1, 1)); // rolled back
    assert_eq!(
        net.transport(A).peer_version(B),
        Some(Version(1)),
        "and a rollback takes the association back down with it",
    );
}

#[test]
fn a_peer_with_no_shared_revision_reports_no_association() {
    // Disjoint ranges are a refused association (**V2**). To a caller that is not
    // a lower revision to fall back to — it is no association at all, the same
    // answer an unreachable peer gives.
    let sim = Simulation::new(1);
    let net = SimNetwork::new(&sim);
    let _a = net.join(A);
    let _b = net.join(B);
    net.set_wire_window(B, window(7, 9, 7));

    assert_eq!(net.transport(A).peer_version(B), None);
    assert!(
        WIRE.negotiate(Accepted::new(7, 9)).is_err(),
        "the refusal is the window's, not the simulator's",
    );
}

#[test]
fn an_unknown_or_unreachable_peer_reports_no_association() {
    let sim = Simulation::new(1);
    let net = SimNetwork::new(&sim);
    let _a = net.join(A);
    let _b = net.join(B);

    assert_eq!(
        net.transport(A).peer_version(NodeId::new(9)),
        None,
        "a node that is not on this network has no association to report",
    );

    // A partition is not a version problem, but it is still no association: a gate
    // must not keep writing a revision agreed with a peer it can no longer reach,
    // because what comes back may not be the process that agreed to it.
    net.partition(&[A], &[B]);
    assert_eq!(net.transport(A).peer_version(B), None);
    net.heal();
    assert_eq!(net.transport(A).peer_version(B), Some(WIRE.accepted().hi));
}
