//! Conformance: membership / SWIM (spec §9, §10) — the observable reachability
//! transitions, and that a conservative policy never downs across a long
//! partition. Also covers SWIM failure detection and the node-down cascade under
//! fault injection (spec §8.1), plus incarnation-based gossip dissemination and
//! a suspected node refuting a false suspicion by bumping its incarnation.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::MemberStatus;
use actor_cluster::Reachability;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Event;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::Recorder;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

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

/// SWIM timings for the gossip tests: these exercise gossip dissemination,
/// refutation, and downing (invariants #14–#17), which need a *direct*-probe
/// suspicion to actually form — so indirect probing is disabled here.
fn gossip_swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        // These tests exercise gossip dissemination, refutation, and downing
        // (invariants #14–#17), which need a *direct*-probe suspicion to actually
        // form — so indirect probing is disabled here; it has its own test.
        suspect_timeout: Duration::from_millis(500),
        indirect_count: 0,
    }
}

/// Build a 3-node cluster, returning the network handle (for faults) and nodes.
fn three_nodes(
    seed: u64,
    downing: DowningPolicy,
) -> (Simulation, SimNetwork, SimCluster, SimCluster, SimCluster) {
    let sim = Simulation::new(seed);
    let net = SimNetwork::new(&sim).with_gossip(gossip_swim(), downing);
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
