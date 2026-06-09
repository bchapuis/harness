//! Conformance: node **restart** under deterministic simulation (spec §9.1,
//! §9.4, §18.3) — the crash-and-recover scenario the isolation-style `crash`
//! cannot model.
//!
//! [`SimNetwork::restart`] stops a node abruptly (no graceful leave) and brings
//! up a successor under the same identity. What must survive is exactly the
//! durable state each mode defines: a voter's Raft term, vote, and log (spec
//! §9.4.3 item 2 — the double-vote hazard), and the node's *identity* in the
//! modes that require it stable across restarts (static §9.4.1, registry-based
//! §9.4.2 item 5). Everything else — actors, the membership view, Raft's
//! volatile role — is correctly lost and rebuilt.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::MemberStatus;
use actor_cluster::RaftConfig;
use actor_cluster::Reachability;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::SimRegistry;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

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

/// Fast Raft timing: 500ms election timeout (plus jitter), 100ms heartbeats.
fn raft(voters: &[NodeId]) -> RaftConfig {
    let mut config = RaftConfig::new(voters.to_vec());
    config.election_timeout = Duration::from_millis(500);
    config.heartbeat_interval = Duration::from_millis(100);
    config
}

/// Drive an async system call to completion under perpetually-running detector
/// and Raft loops (`block_on` would never quiesce): launch the future and
/// advance a bounded `settle` span.
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

// --- A trivial addressable actor ----------------------------------------------

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
    const MANIFEST: Manifest = Manifest::new("restart.Ping");
}
impl Handler<Ping> for Echo {
    async fn handle(&mut self, _msg: Ping, _ctx: &Ctx<Self>) -> u64 {
        1
    }
}

// --- Tests ---------------------------------------------------------------------

#[test]
fn a_restarted_voter_recovers_its_persisted_raft_state() {
    // Spec §9.4.3 item 2: a voter's term, vote, and log survive a restart.
    // Commit a drain, restart a voter, and the restarted instance — whose
    // volatile membership view started empty — reconverges on the committed
    // state from its persisted log and the leader's replication, then takes
    // part in committing the next transition.
    let sim = Simulation::new(1);
    let net =
        SimNetwork::new(&sim).with_leader(swim(), raft(&[A, B, C]), DowningPolicy::Conservative);
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_secs(2)); // elect

    let proposer = a.clone();
    let drained = drive(&sim, Duration::from_secs(5), async move {
        proposer.drain(C).await
    });
    assert!(drained, "the drain commits");
    let stamp = a.membership().stamp(C).expect("A knows C");
    drop(b); // the pre-restart handle is stale after this point

    let b2 = net.restart(B);
    sim.run_for(Duration::from_secs(3));

    assert_eq!(
        b2.membership().status(C),
        Some(MemberStatus::Draining),
        "the restarted voter reconverges on the committed state",
    );
    assert_eq!(
        b2.membership().stamp(C),
        Some(stamp),
        "at the same commit index — one log order, before and after the restart",
    );

    // The restarted voter participates in new commits, proposing one itself.
    let proposer = b2.clone();
    let resumed = drive(&sim, Duration::from_secs(5), async move {
        proposer.resume(C).await
    });
    assert!(resumed, "the restarted voter's proposal commits");
    assert_eq!(a.membership().status(C), Some(MemberStatus::Up));
    assert_eq!(c.membership().self_status(), MemberStatus::Up);
}

#[test]
fn restarting_a_majority_does_not_lose_committed_state() {
    // Restart two of three voters back-to-back: every quorum now contains a
    // restarted voter, so the committed drain can only survive if their logs
    // really did (leader completeness over persisted logs).
    let sim = Simulation::new(2);
    let net =
        SimNetwork::new(&sim).with_leader(swim(), raft(&[A, B, C]), DowningPolicy::Conservative);
    let a = net.join(A);
    let _b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_secs(2));

    let proposer = a.clone();
    let drained = drive(&sim, Duration::from_secs(5), async move {
        proposer.drain(C).await
    });
    assert!(drained, "the drain commits");

    let b2 = net.restart(B);
    sim.run_for(Duration::from_secs(2));
    let a2 = net.restart(A);
    sim.run_for(Duration::from_secs(3));

    for (who, sys) in [("restarted A", &a2), ("restarted B", &b2)] {
        assert_eq!(
            sys.membership().status(C),
            Some(MemberStatus::Draining),
            "{who} recovered the committed drain",
        );
    }
    assert_eq!(
        c.membership().self_status(),
        MemberStatus::Draining,
        "the survivor never saw the decision waver",
    );
}

#[test]
fn a_restarted_registry_member_keeps_its_membership() {
    // Spec §9.4.2 item 5: node identity is stable across restarts in
    // registry-based mode — a draining member that restarts is still the same
    // draining member, resynced from the registry, never evicted.
    let sim = Simulation::new(3);
    let registry = SimRegistry::new(&sim);
    registry.register(A);
    registry.register(B);
    let net =
        SimNetwork::new(&sim).with_registry(swim(), registry.client(), Duration::from_millis(100));
    let a = net.join(A);
    let _b = net.join(B);
    sim.run_for(Duration::from_millis(500));

    registry.drain(B);
    sim.run_for(Duration::from_secs(1));
    assert_eq!(a.membership().status(B), Some(MemberStatus::Draining));

    let b2 = net.restart(B);
    sim.run_for(Duration::from_secs(2));

    assert_eq!(
        b2.membership().self_status(),
        MemberStatus::Draining,
        "the restarted member resynced its own cordon from the registry",
    );
    assert_eq!(
        a.membership().status(B),
        Some(MemberStatus::Draining),
        "B is the same member throughout — a restart never evicts (spec §9.4.2 item 5)",
    );
    assert_eq!(
        a.membership().reachability(B),
        Some(Reachability::Reachable),
        "and it is back in service on the detector axis",
    );
}

#[test]
fn a_restarted_static_member_resumes_its_place_in_the_roster() {
    // Spec §9.4.1: static identity is stable; a restarted node resumes its
    // place, ActorIds minted by its previous run dead-letter like any resigned
    // actor's, and fresh actors on it are addressable again.
    let sim = Simulation::new(4);
    let net = SimNetwork::new(&sim); // static, no detector
    let a = net.join(A);
    let b = net.join(B);
    sim.run_for(Duration::from_millis(200));

    let old_echo = b.spawn(Echo);
    let old_id = old_echo.id().clone();
    let caller: ActorRef<Echo> = a.resolve(old_id.clone());
    let reply = sim.block_on(async move { caller.ask(Ping).await });
    assert_eq!(reply, Ok(1), "the pre-restart actor answers");

    let b2 = net.restart(B);
    sim.run_for(Duration::from_millis(200));

    assert_eq!(
        a.membership().status(B),
        Some(MemberStatus::Up),
        "the roster is exactly as configured — a restart changes nothing (spec §9.4.1)",
    );
    // The previous run's ActorId still resolves to B and dead-letters there.
    let stale: ActorRef<Echo> = a.resolve(old_id);
    let reply = sim.block_on(async move { stale.ask(Ping).await });
    assert_eq!(
        reply,
        Err(CallError::DeadLetter),
        "an id minted by the previous run dead-letters (spec §9.4.1)",
    );
    // A fresh actor on the restarted node is addressable as ever.
    let new_echo = b2.spawn(Echo);
    let fresh: ActorRef<Echo> = a.resolve(new_echo.id().clone());
    let reply = sim.block_on(async move { fresh.ask(Ping).await });
    assert_eq!(reply, Ok(1), "the restarted node hosts and answers again");
}

#[test]
fn a_restarted_gossip_member_recovers_reachability() {
    // Gossip mode has no durable state; what a restart must not break is the
    // detector axis: the node comes back at incarnation 0 while peers may hold
    // a higher one for it, and direct probes — not the gossip merge — restore
    // it to reachable. Conservative downing keeps it a member throughout.
    let sim = Simulation::new(5);
    let net = SimNetwork::new(&sim).with_gossip(swim(), DowningPolicy::Conservative);
    let a = net.join(A);
    let b = net.join(B);
    let _c = net.join(C);
    sim.run_for(Duration::from_millis(500));

    // Let suspicion form first, so B's incarnation on A is past zero.
    net.partition(&[B], &[A, C]);
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        a.membership().reachability(B),
        Some(Reachability::Unreachable)
    );
    drop(b);

    let b2 = net.restart(B); // also clears the blocks involving B
    sim.run_for(Duration::from_secs(2));

    assert_eq!(
        a.membership().reachability(B),
        Some(Reachability::Reachable),
        "direct liveness evidence restores the restarted member",
    );
    assert!(!a.membership().is_down(B), "it was never downed (#16)");
    assert_eq!(
        b2.membership().self_status(),
        MemberStatus::Up,
        "the restarted member is a full member again",
    );
}
