//! Conformance: membership / SWIM (spec §9, §10) — gaps beyond the existing
//! failure and gossip tests: the observable reachability transitions, and that a
//! conservative policy never downs across a long partition.

mod support;

use std::sync::Arc;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::MemberStatus;
use actor_cluster::Reachability;
use actor_cluster::SwimConfig;
use actor_core::Event;
use actor_core::NodeId;
use actor_simulation::Recorder;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);

fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

#[test]
fn reachability_transitions_are_observable() {
    let sim = Simulation::new(1);
    let recorder = Recorder::new();
    let net = SimNetwork::new(&sim)
        .with_gossip(swim(), DowningPolicy::Conservative)
        .with_events(Arc::new(recorder.clone()));
    let _a = net.join(A);
    let _b = net.join(B);

    net.partition(&[A], &[B]);
    sim.run_for(Duration::from_secs(1)); // A suspects then confirms B unreachable
    net.heal();
    sim.run_for(Duration::from_secs(1)); // A probes B again → reachable

    // A's view of B walks Suspect → Unreachable → Reachable (spec §10).
    let transitions: Vec<&str> = recorder
        .events()
        .iter()
        .filter_map(|e| match e {
            Event::Suspected { observer, node } if *observer == A && *node == B => Some("suspect"),
            Event::Unreachable { observer, node } if *observer == A && *node == B => {
                Some("unreachable")
            }
            Event::Reachable { observer, node } if *observer == A && *node == B => {
                Some("reachable")
            }
            _ => None,
        })
        .collect();
    assert_eq!(transitions, vec!["suspect", "unreachable", "reachable"]);
}

#[test]
fn conservative_policy_never_downs_across_a_long_partition() {
    let sim = Simulation::new(2);
    let net = SimNetwork::new(&sim).with_gossip(swim(), DowningPolicy::Conservative);
    let node_a = net.join(A);
    let _b = net.join(B);

    net.partition(&[A], &[B]);
    sim.run_for(Duration::from_secs(30)); // a long partition

    assert_eq!(
        node_a.membership().reachability(B),
        Some(Reachability::Unreachable),
    );
    assert!(
        !node_a.membership().is_down(B),
        "a partition alone must never down a node under the conservative policy (#16)",
    );
}

const C: NodeId = NodeId::new(3);

#[test]
fn indirect_probing_keeps_a_lossy_but_reachable_peer_up() {
    // A's direct link to C is severed, but B can reach both. A probes C
    // indirectly via B, so a single bad link never makes A falsely suspect C
    // (spec §10 #2). Contrast `gossip::a_suspected_node_refutes_a_false_suspicion`,
    // which disables indirect probing so the suspicion forms.
    let sim = Simulation::new(5);
    let net = SimNetwork::new(&sim).with_gossip(swim(), DowningPolicy::Conservative);
    let a = net.join(A);
    let _b = net.join(B);
    let _c = net.join(C);
    sim.run_for(Duration::from_millis(500)); // initial convergence

    net.partition(&[A], &[C]); // sever only A<->C; B still reaches both
    sim.run_for(Duration::from_secs(3));

    assert_eq!(
        a.membership().reachability(C),
        Some(Reachability::Reachable),
        "indirect probing through B keeps C reachable despite the dead direct link",
    );
}

#[test]
fn the_coordinator_role_falls_over_when_the_coordinator_leaves() {
    // Join/leave admission is covered in conformance_join.rs; here we pin the
    // coordinator rule (spec §9.4.4 item 3): a deterministic role, not an
    // election — the lowest-address up, reachable member — that falls over when
    // that node gracefully leaves (spec §9.3).
    let sim = Simulation::new(7);
    let net = SimNetwork::new(&sim).with_gossip(swim(), DowningPolicy::Conservative);
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_millis(500));

    assert_eq!(a.leader(), Some(A), "the lowest-id member coordinates");
    assert_eq!(c.leader(), Some(A), "every node agrees on the coordinator");

    // The coordinator leaves gracefully; the role passes to the next-lowest id.
    a.leave();
    sim.run_for(Duration::from_secs(2));

    assert_eq!(
        b.leader(),
        Some(B),
        "the role falls over to the next-lowest reachable member"
    );
    assert_eq!(c.leader(), Some(B), "the failover is agreed cluster-wide");
}

const D: NodeId = NodeId::new(4);

#[test]
fn the_coordinator_only_acts_on_a_stable_fully_reachable_view() {
    // Spec §9.4.4 item 3: the coordinator transitions members only on a locally
    // stable, fully-reachable view. While one member is unreachable, a fresh
    // joiner stays `Joining`; once the view heals and holds stable for the
    // window, the admission goes through.
    let sim = Simulation::new(8);
    let net = SimNetwork::new(&sim).with_gossip(swim(), DowningPolicy::Conservative);
    let a = net.join(A);
    let _b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_millis(500));

    net.partition(&[C], &[A, B, D]); // C is cut off, the joiner's side included
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        a.membership().reachability(C),
        Some(Reachability::Unreachable)
    );

    let d = net.join_seeded(D, &[A]);
    sim.run_for(Duration::from_secs(5)); // plenty of time — but the view is not stable

    assert_eq!(
        d.membership().self_status(),
        MemberStatus::Joining,
        "no admission while a member is unreachable — the view is in flux",
    );

    net.heal();
    sim.run_for(Duration::from_secs(3)); // recover, hold stable past the window

    assert_eq!(
        d.membership().self_status(),
        MemberStatus::Up,
        "the coordinator admits the joiner once the view is stable and fully reachable",
    );
    let _ = (&c, &a);
}

#[test]
fn downing_is_exempt_from_the_stability_gate() {
    // The carved-out exception (spec §9.4.4 items 3–4): applying the downing
    // policy to an unreachable member cannot be gated on full reachability —
    // the node being downed is unreachable by definition. The coordinator downs
    // it even though its view is not fully reachable.
    let sim = Simulation::new(9);
    let net = SimNetwork::new(&sim)
        .with_gossip(swim(), DowningPolicy::Timeout(Duration::from_millis(400)));
    let a = net.join(A);
    let _b = net.join(B);
    let _c = net.join(C);
    sim.run_for(Duration::from_millis(500));

    net.crash(C); // the view is now permanently not fully reachable
    sim.run_for(Duration::from_secs(5));

    assert!(
        a.membership().is_down(C),
        "the coordinator applies the downing policy despite the unstable view",
    );
}
