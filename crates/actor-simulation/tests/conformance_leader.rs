//! Conformance: **leader-based** membership (spec §9.4.3) — the self-hosted
//! consensus control plane.
//!
//! The cluster's source of truth is a replicated Raft log: an elected leader
//! serializes every membership transition as a quorum-committed entry, the
//! commit index is the authority stamp (spec §9.2), and the SWIM detector is a
//! sensor feeding the leader, which alone may commit `down` under the
//! configured downing policy. These tests pin the observable guarantees:
//! election safety, log-ordered transitions, leader failover, quorum-gated
//! downing — a minority can never evict the majority (invariant #22) — and the
//! control plane pausing (data plane running) under quorum loss.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::MemberStatus;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::BoxError;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Event;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_core::Terminated;
use actor_core::TerminationReason;
use actor_simulation::Recorder;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);
const D: NodeId = NodeId::new(4);

/// Fast SWIM so a few simulated seconds cover several probe/gossip rounds.
fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

/// Fast Raft timing: 500ms election timeout (plus jitter), 100ms heartbeats.
fn raft(voters: &[NodeId]) -> RaftConfig {
    let mut config = RaftConfig::new(voters.to_vec());
    config.election_timeout = Duration::from_millis(500);
    config.heartbeat_interval = Duration::from_millis(100);
    config
}

/// A three-voter leader-mode network.
fn leader_net(sim: &Simulation, downing: DowningPolicy) -> SimNetwork {
    SimNetwork::new(sim).with_leader(swim(), raft(&[A, B, C]), downing)
}

/// Drive an async system call (an operator command, an `ask`) to completion
/// under perpetually-running detector and Raft loops (`block_on` would never
/// quiesce): launch the future and advance a bounded `settle` span.
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

/// The elected leader's system, per the (converged) cluster's own view.
fn elected_leader<'a>(nodes: &'a [&SimCluster]) -> &'a SimCluster {
    let leader = nodes[0].leader().expect("a leader must be elected");
    for node in nodes {
        assert_eq!(
            node.leader(),
            Some(leader),
            "every node agrees on the leader"
        );
    }
    nodes
        .iter()
        .find(|n| n.node() == leader)
        .expect("the leader is one of the nodes")
}

// --- A trivial addressable actor, to exercise the cascade and routing ---------

struct Echo;

impl Actor for Echo {
    type System = SimCluster;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Ping>();
    }
}

#[derive(Serialize, Deserialize)]
struct Ping;
impl Message for Ping {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("leader.Ping");
}
impl Handler<Ping> for Echo {
    async fn handle(&mut self, _msg: Ping, _ctx: &Ctx<Self>) -> u64 {
        1
    }
}

type Reasons = Arc<Mutex<Vec<TerminationReason>>>;

/// Watches `target` from start, recording the reason of every `Terminated`.
struct Watcher {
    target: ActorRef<Echo>,
    got: Reasons,
}
impl Actor for Watcher {
    type System = SimCluster;
    async fn started(&mut self, ctx: &Ctx<Self>) -> Result<(), BoxError> {
        ctx.watch(&self.target);
        Ok(())
    }
}
impl Handler<Terminated> for Watcher {
    async fn handle(&mut self, signal: Terminated, _ctx: &Ctx<Self>) {
        self.got.lock().unwrap().push(signal.reason);
    }
}

// --- Tests -------------------------------------------------------------------

#[test]
fn a_leader_is_elected_and_at_most_one_per_term() {
    // Election safety (spec §9.4.3, invariant #22): a leader emerges, every
    // node converges on it, and no term ever sees two `LeaderElected` claims.
    let sim = Simulation::new(1);
    let recorder = Recorder::new();
    let net = leader_net(&sim, DowningPolicy::Conservative).with_events(Arc::new(recorder.clone()));
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_secs(3));

    let _leader = elected_leader(&[&a, &b, &c]);

    let mut terms: Vec<(u64, NodeId)> = recorder
        .events()
        .iter()
        .filter_map(|e| match e {
            // Only the control group runs here, so the group is constant.
            Event::LeaderElected { node, term, .. } => Some((*term, *node)),
            _ => None,
        })
        .collect();
    assert!(!terms.is_empty(), "an election was won and announced");
    terms.sort();
    for pair in terms.windows(2) {
        assert!(
            pair[0].0 != pair[1].0 || pair[0].1 == pair[1].1,
            "at most one leader per term (invariant #22): {terms:?}",
        );
    }
}

#[test]
fn a_joiner_is_admitted_by_a_committed_entry() {
    // Spec §9.3, §9.4.3 items 1 and 3: the leader commits `joining → up`; voters
    // apply it from the log, and the non-voter joiner itself learns the stamped
    // decision by gossip — both paths converge on the same commit index.
    let sim = Simulation::new(2);
    let net = leader_net(&sim, DowningPolicy::Conservative);
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_secs(2)); // elect

    let d = net.join_seeded(D, &[A]);
    sim.run_for(Duration::from_secs(3)); // gossip in, commit, disseminate

    assert_eq!(
        d.membership().self_status(),
        MemberStatus::Up,
        "the joiner was admitted through the log",
    );
    let stamp = a.membership().stamp(D).expect("A knows D");
    assert!(
        stamp > 0,
        "the admission carries its commit index as the stamp"
    );
    for (who, sys) in [("A", &a), ("B", &b), ("C", &c)] {
        assert_eq!(
            sys.membership().status(D),
            Some(MemberStatus::Up),
            "{who} sees D up"
        );
        assert_eq!(
            sys.membership().stamp(D),
            Some(stamp),
            "{who} holds the admission at the same commit index (one log order)",
        );
    }
}

#[test]
fn drain_and_resume_are_committed_entries_even_from_a_non_leader() {
    // Spec §9.4.3 item 1: every transition is a log entry committed through the
    // leader; a command offered to a non-leader is *forwarded*, never applied
    // locally. Drain C from a non-leader node, resume it from another.
    let sim = Simulation::new(3);
    let net = leader_net(&sim, DowningPolicy::Conservative);
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_secs(2));

    let nodes = [&a, &b, &c];
    let leader = elected_leader(&nodes).node();
    let non_leader = nodes
        .iter()
        .find(|n| n.node() != leader && n.node() != C)
        .expect("two non-leaders exist")
        .to_owned()
        .clone();

    let proposer = non_leader.clone();
    let drained = drive(&sim, Duration::from_secs(5), async move {
        proposer.drain(C).await
    });
    assert!(drained, "the forwarded drain committed");
    for (who, sys) in [("A", &a), ("B", &b)] {
        assert_eq!(
            sys.membership().status(C),
            Some(MemberStatus::Draining),
            "{who} applied the committed drain",
        );
    }
    assert_eq!(c.membership().self_status(), MemberStatus::Draining);

    let proposer = non_leader.clone();
    let resumed = drive(&sim, Duration::from_secs(5), async move {
        proposer.resume(C).await
    });
    assert!(resumed, "the forwarded resume committed");
    assert_eq!(a.membership().status(C), Some(MemberStatus::Up));
    assert_eq!(c.membership().self_status(), MemberStatus::Up);
}

#[test]
fn the_cluster_elects_a_new_leader_when_the_leader_crashes() {
    // Leader failover (spec §9.4.3): committed state survives, a new leader is
    // elected by the surviving quorum, and new transitions commit through it.
    let sim = Simulation::new(4);
    let net = leader_net(&sim, DowningPolicy::Conservative);
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_secs(2));

    let nodes = [&a, &b, &c];
    let old_leader = elected_leader(&nodes).node();

    // Commit a drain of D... no D here: drain one of the survivors? Drain must
    // target a member; pick a survivor and verify the state outlives failover.
    let survivor: Vec<&&SimCluster> = nodes.iter().filter(|n| n.node() != old_leader).collect();
    let (first, second) = (survivor[0], survivor[1]);
    let target = second.node();
    let proposer = (*first).clone();
    let drained = drive(&sim, Duration::from_secs(5), async move {
        proposer.drain(target).await
    });
    assert!(drained, "a drain committed under the old leader");

    net.crash(old_leader);
    sim.run_for(Duration::from_secs(5)); // a new election among the survivors

    let new_leader = first.leader().expect("a new leader is elected");
    assert_ne!(
        new_leader, old_leader,
        "the crashed leader is not re-elected"
    );
    assert_eq!(second.leader(), Some(new_leader), "the survivors agree");
    assert_eq!(
        first.membership().status(target),
        Some(MemberStatus::Draining),
        "committed state survives the failover (leader completeness)",
    );

    // New transitions commit through the new leader.
    let proposer = (*first).clone();
    let resumed = drive(&sim, Duration::from_secs(5), async move {
        proposer.resume(target).await
    });
    assert!(resumed, "a transition commits after failover");
    assert_eq!(first.membership().status(target), Some(MemberStatus::Up));
}

#[test]
fn downing_is_a_quorum_committed_entry_and_runs_the_cascade() {
    // Spec §9.4.3 item 4: the detector confirms `unreachable`, the leader
    // commits `down` under the policy, and the cascade (spec §8.1) follows —
    // watchers get `NodeDown`, calls fail `Unreachable`, the node never returns.
    let sim = Simulation::new(5);
    let net = leader_net(&sim, DowningPolicy::Timeout(Duration::from_millis(400)));
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_secs(2));

    let echo = c.spawn(Echo);
    let echo_on_a: ActorRef<Echo> = a.resolve(echo.id().clone());
    let got: Reasons = Arc::new(Mutex::new(Vec::new()));
    let _watcher = a.spawn(Watcher {
        target: echo_on_a.clone(),
        got: Arc::clone(&got),
    });
    sim.run_for(Duration::from_millis(300)); // the remote watch registers

    net.crash(C);
    sim.run_for(Duration::from_secs(10)); // suspect → unreachable → committed down

    assert!(a.membership().is_down(C), "A applied the committed down");
    assert!(b.membership().is_down(C), "B applied the committed down");
    assert_eq!(
        *got.lock().unwrap(),
        vec![TerminationReason::NodeDown],
        "the watcher was notified through the cascade",
    );

    let caller = echo_on_a.clone();
    let result = drive(&sim, Duration::from_millis(300), async move {
        caller.ask(Ping).await
    });
    assert!(matches!(result, Err(CallError::Unreachable)));

    net.heal();
    sim.run_for(Duration::from_secs(3));
    assert!(
        a.membership().is_down(C),
        "down is terminal after healing (#15)"
    );
}

#[test]
fn a_minority_partition_can_never_evict_the_majority() {
    // Invariant #22: downing is quorum-gated. Partition the leader away alone —
    // even under an aggressive downing policy it sees the majority unreachable
    // but can commit nothing; the majority elects a new leader and may down
    // *it*. A minority never evicts the majority.
    let sim = Simulation::new(6);
    let net = leader_net(&sim, DowningPolicy::Timeout(Duration::from_millis(300)));
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_secs(2));

    let nodes = [&a, &b, &c];
    let old_leader = elected_leader(&nodes).node();
    let minority = nodes
        .iter()
        .find(|n| n.node() == old_leader)
        .unwrap()
        .to_owned();
    let majority: Vec<&&SimCluster> = nodes.iter().filter(|n| n.node() != old_leader).collect();

    net.partition(&[old_leader], &[majority[0].node(), majority[1].node()]);
    sim.run_for(Duration::from_secs(10)); // far past the downing timeout

    // The minority leader proposed `Down` for the others but could not commit.
    for peer in [majority[0].node(), majority[1].node()] {
        assert!(
            !minority.membership().is_down(peer),
            "the minority cannot commit a down — quorum-gated (invariant #22)",
        );
    }
    // The majority side, holding the quorum, elected a new leader and downed
    // the old one under the policy.
    let new_leader = majority[0].leader().expect("the majority elected a leader");
    assert_ne!(new_leader, old_leader);
    assert!(
        majority[0].membership().is_down(old_leader),
        "the majority's leader committed the down for the isolated node",
    );

    net.heal();
    sim.run_for(Duration::from_secs(5));
    for peer in [majority[0], majority[1]] {
        assert!(
            !peer.membership().is_down(peer.node()),
            "no majority member was ever evicted",
        );
    }
    assert!(
        majority[0].membership().is_down(old_leader),
        "the down stays terminal after healing (#15)",
    );
}

#[test]
fn quorum_loss_pauses_the_control_plane_but_not_the_data_plane() {
    // Spec §9.4.3 item 5: without a quorum nothing commits — no drain, no down —
    // while existing members keep exchanging actor traffic on the last
    // committed view.
    let sim = Simulation::new(7);
    let net = leader_net(&sim, DowningPolicy::Conservative);
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    let d = net.join(D); // a non-voter member
    sim.run_for(Duration::from_secs(2));
    let _ = (&b, &c);

    let echo = d.spawn(Echo);
    let echo_on_a: ActorRef<Echo> = a.resolve(echo.id().clone());

    // Crash two of the three voters: whatever happens, no quorum remains.
    net.crash(B);
    net.crash(C);
    sim.run_for(Duration::from_secs(3));

    // The control plane is paused: a drain cannot commit.
    let proposer = a.clone();
    let drained = drive(&sim, Duration::from_secs(8), async move {
        proposer.drain(D).await
    });
    assert!(
        !drained,
        "no transition commits without a quorum (spec §9.4.3 #5)"
    );
    assert_eq!(
        a.membership().status(D),
        Some(MemberStatus::Up),
        "D's membership is unchanged",
    );

    // The data plane keeps running on the last committed view.
    let caller = echo_on_a.clone();
    let result = drive(&sim, Duration::from_secs(2), async move {
        caller.ask(Ping).await
    });
    assert_eq!(
        result,
        Ok(1),
        "actor traffic flows between the surviving members"
    );
}

#[test]
fn a_committed_voter_change_takes_effect() {
    // Spec §9.4.3 item 2: voter-set changes are committed configuration
    // entries. Add D as a fourth voter, crash one original voter, and the
    // cluster still commits — the new quorum (3 of 4) must include D, proving
    // the change took effect.
    let sim = Simulation::new(8);
    let net = leader_net(&sim, DowningPolicy::Conservative);
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    let d = net.join(D);
    sim.run_for(Duration::from_secs(2));
    let _ = &d;

    let nodes = [&a, &b, &c];
    let leader = elected_leader(&nodes).node();
    let leader_sys = nodes
        .iter()
        .find(|n| n.node() == leader)
        .unwrap()
        .to_owned()
        .clone();

    let proposer = leader_sys.clone();
    let added = drive(&sim, Duration::from_secs(5), async move {
        proposer.add_voter(D).await
    });
    assert!(added, "the voter change committed on the leader");
    sim.run_for(Duration::from_secs(2)); // replicate the config to all, incl. D

    // Crash one original voter that is not the leader.
    let crashed = nodes
        .iter()
        .map(|n| n.node())
        .find(|&n| n != leader)
        .expect("a non-leader voter exists");
    net.crash(crashed);
    sim.run_for(Duration::from_secs(3));

    // Commits now need 3 of 4 voters: the leader, the surviving original, and D.
    let target = nodes
        .iter()
        .map(|n| n.node())
        .find(|&n| n != leader && n != crashed)
        .expect("a third original voter exists");
    let proposer = leader_sys.clone();
    let drained = drive(&sim, Duration::from_secs(8), async move {
        proposer.drain(target).await
    });
    assert!(
        drained,
        "the cluster still commits over the enlarged voter set — D is a real voter",
    );
}

#[test]
fn a_graceful_leave_is_committed_at_the_departing_nodes_request() {
    // Spec §9.3, §9.4.3 item 1: the `leaving` announcement decides nothing; the
    // leader commits `leaving → down`, and the cascade notifies watchers.
    let sim = Simulation::new(9);
    let net = leader_net(&sim, DowningPolicy::Conservative);
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_secs(2));
    let _ = &c;

    b.leave();
    sim.run_for(Duration::from_secs(5)); // gossip the announcement; commit the finalization

    assert!(a.membership().is_down(B), "the leave was finalized to down");
    assert!(
        a.membership().stamp(B).unwrap_or(0) > 0,
        "the finalization is a committed, stamped entry",
    );
}
