//! Cross-node grains on the clustered `Quorum` journal (per-grain quorum append),
//! under deterministic simulation (granary §14).
//!
//! These exercise the cluster-only invariants the single-node tier cannot reach:
//! a grain hosted on a 3-node cluster activates on its shard's Raft leader and is
//! callable from *any* node with the same reply it would give locally (**G13**),
//! committed state survives a shard-leader crash (**G14**, **G12**), a call that
//! races a leadership change is absorbed by the bounded redirect (§5.4), and a
//! shard that loses its quorum pauses writes as `Unavailable` rather than forking
//! (**G11**, CP). The harness mirrors `tests/raft_journal.rs`; the grain is the
//! Appendix A `Account`, hosted on the clustered `SimCluster`.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::Counter;
use actor_simulation::CounterOp;
use actor_simulation::CounterRet;
use actor_simulation::History;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use actor_simulation::check_linearizable;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainError;
use granary::GrainHandler;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use serde::Deserialize;
use serde::Serialize;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);
const D: NodeId = NodeId::new(4);
const E: NodeId = NodeId::new(5);

// --- The Appendix A account grain, hosted on the clustered system -------------

#[derive(Default)]
struct Account;

#[derive(Default, Serialize, Deserialize)]
struct Balance {
    cents: i64,
}

#[derive(Serialize, Deserialize)]
enum Ledger {
    Deposited(u64),
    Withdrew(u64),
}

/// An application error — lives inside `M::Reply`, never in `GrainError` (§4.2).
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Overdraft;

impl Grain for Account {
    type System = SimCluster;
    type State = Balance;
    type Event = Ledger;
    const GRAIN_TYPE: &'static str = "bank.Account";

    fn apply(state: &mut Balance, event: &Ledger) {
        match event {
            Ledger::Deposited(n) => state.cents += *n as i64,
            Ledger::Withdrew(n) => state.cents -= *n as i64,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Deposit>();
        r.accept::<Withdraw>();
        r.accept::<ReadBalance>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Deposit {
    cents: u64,
}
impl Message for Deposit {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.Deposit");
}

impl GrainHandler<Deposit> for Account {
    async fn handle(&self, state: &Balance, msg: Deposit, _ctx: &GrainCtx<Self>) -> (Vec<Ledger>, i64) {
        (vec![Ledger::Deposited(msg.cents)], state.cents + msg.cents as i64)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Withdraw {
    cents: u64,
}
impl Message for Withdraw {
    type Reply = Result<i64, Overdraft>;
    const MANIFEST: Manifest = Manifest::new("bank.Withdraw");
}

impl GrainHandler<Withdraw> for Account {
    async fn handle(
        &self,
        state: &Balance,
        msg: Withdraw,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<Ledger>, Result<i64, Overdraft>) {
        if (state.cents as u64) < msg.cents {
            return (vec![], Err(Overdraft));
        }
        (vec![Ledger::Withdrew(msg.cents)], Ok(state.cents - msg.cents as i64))
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadBalance;
impl Message for ReadBalance {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.ReadBalance");
}

impl GrainHandler<ReadBalance> for Account {
    async fn handle(&self, state: &Balance, _msg: ReadBalance, _ctx: &GrainCtx<Self>) -> (Vec<Ledger>, i64) {
        (vec![], state.cents)
    }
}

// --- Harness (mirrors tests/raft_journal.rs) ----------------------------------

fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

fn raft() -> RaftConfig {
    let mut config = RaftConfig::new(vec![A, B, C]);
    config.election_timeout = Duration::from_millis(500);
    config.heartbeat_interval = Duration::from_millis(100);
    config
}

fn leader_net(sim: &Simulation) -> SimNetwork {
    // The simulator now applies a fixed minimum delivery latency by default, so
    // virtual time always advances and a failover's simultaneous multi-group
    // re-election under concurrent load no longer starves convergence — no per-test
    // latency workaround needed.
    SimNetwork::new(sim).with_leader(swim(), raft(), DowningPolicy::Conservative)
}

/// A grain config for the cluster: a couple of shards (so names distribute across
/// Raft groups), no hibernation during a test, frequent-ish snapshots.
fn config() -> GranaryConfig {
    GranaryConfig {
        shards: 2,
        idle_after: Duration::from_secs(60),
        snapshot_every: 8,
        ..GranaryConfig::default()
    }
}

/// Drive an async call to completion under the perpetually-running cluster loops
/// (copied from the actor-cluster conformance harness).
fn drive<T: Send + 'static>(
    sim: &Simulation,
    settle: Duration,
    future: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    let cell: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let out = Arc::clone(&cell);
    sim.spawner().launch(Box::pin(async move {
        *out.lock().unwrap() = Some(future.await);
    }));
    sim.run_for(settle);
    cell.lock().unwrap().take().expect("future did not complete")
}

/// Bring up a 3-node leader cluster and host `Account` on every node, so each
/// creates the shards' Raft groups and registers its gateway (§5.3). Returns the
/// network (for fault injection), the systems, and a `Granary` handle per node.
fn cluster(sim: &Simulation) -> (SimNetwork, Vec<SimCluster>, Vec<Granary<Account>>) {
    let net = leader_net(sim);
    let systems = vec![net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let granaries: Vec<Granary<Account>> =
        systems.iter().map(|system| system.granary::<Account>(config())).collect();
    sim.run_for(Duration::from_secs(3)); // elect each shard group's leader
    (net, systems, granaries)
}

/// The index of a node that does **not** lead `key`'s shard — a node that must
/// route the call to the leader.
fn non_leader_of(systems: &[SimCluster], granaries: &[Granary<Account>], key: &str) -> usize {
    let leader = granaries[0].leader(key).expect("the shard elected a leader");
    systems
        .iter()
        .position(|s| s.node() != leader)
        .expect("some node is not the leader")
}

#[test]
fn grain_is_callable_from_a_non_leader_node() {
    // G13: a call from a node that does not lead the grain's shard routes to the
    // leader and returns the same reply a local call would — durable on commit.
    let sim = Simulation::new(1);
    let (_net, systems, granaries) = cluster(&sim);
    let key = "account/42";
    let caller = non_leader_of(&systems, &granaries, key);
    let granary = granaries[caller].clone();

    let (deposit, withdraw, overdraft, balance) = drive(&sim, Duration::from_secs(8), async move {
        let acct = granary.grain(key);
        let deposit = acct.ask(Deposit { cents: 1000 }).await;
        let withdraw = acct.ask(Withdraw { cents: 500 }).await;
        // An overdraw commits nothing (§7.5): the reply is an application error.
        let overdraft = acct.ask(Withdraw { cents: 9999 }).await;
        let balance = acct.ask(ReadBalance).await;
        (deposit, withdraw, overdraft, balance)
    });

    assert_eq!(deposit, Ok(1000), "deposit committed and the reply is post-state");
    assert_eq!(withdraw, Ok(Ok(500)), "withdraw committed durably");
    assert_eq!(overdraft, Ok(Err(Overdraft)), "overdraw is an application error, not a GrainError");
    assert_eq!(balance, Ok(500), "the overdraw left the balance unchanged");
}

#[test]
fn committed_state_survives_shard_leader_failover() {
    // G14/G12: commit, crash the grain's shard leader, let a survivor take over;
    // the next call re-activates the grain on the new leader from the Raft log,
    // with no acknowledged write lost.
    let sim = Simulation::new(2);
    let (net, systems, granaries) = cluster(&sim);
    let key = "account/7";
    let leader = granaries[0].leader(key).expect("the shard elected a leader");
    let caller = systems
        .iter()
        .position(|s| s.node() != leader)
        .expect("a non-leader caller survives the crash");

    // Commit a deposit through the original leader.
    let committed = {
        let granary = granaries[caller].clone();
        drive(&sim, Duration::from_secs(8), async move {
            granary.grain(key).ask(Deposit { cents: 250 }).await
        })
    };
    assert_eq!(committed, Ok(250));

    // Crash the shard leader; the surviving quorum re-elects.
    net.crash(leader);
    sim.run_for(Duration::from_secs(6));

    // A read re-activates the grain on the new leader and sees the durable balance.
    let balance = {
        let granary = granaries[caller].clone();
        drive(&sim, Duration::from_secs(10), async move {
            granary.grain(key).ask_timeout(ReadBalance, Duration::from_secs(9)).await
        })
    };
    assert_eq!(balance, Ok(250), "committed state survived the leader crash (G14)");
}

#[test]
fn a_call_during_failover_is_absorbed_by_the_redirect() {
    // §5.4: a call issued right after the leader crashes — with no settling time —
    // is held by the gateway's bounded redirect until the new leader is elected
    // and discovered, then succeeds. A remote call stays observably identical to a
    // local one across failover (G13), never surfacing NotLeader.
    let sim = Simulation::new(3);
    let (net, systems, granaries) = cluster(&sim);
    let key = "account/13";
    let leader = granaries[0].leader(key).expect("the shard elected a leader");
    // The two survivors of the crash. Write through one, then read through the
    // *other*: its cache is cold, so the read resolves through the gateway and
    // exercises the bounded redirect (a cached host on the just-crashed leader
    // would instead time out, which a write must not auto-retry — §6, §2.2).
    let survivors: Vec<usize> = systems.iter().enumerate().filter(|(_, s)| s.node() != leader).map(|(i, _)| i).collect();
    let (writer, reader) = (survivors[0], survivors[1]);

    let committed = {
        let granary = granaries[writer].clone();
        drive(&sim, Duration::from_secs(8), async move {
            granary.grain(key).ask(Deposit { cents: 400 }).await
        })
    };
    assert_eq!(committed, Ok(400));

    // Crash the leader and immediately issue the read on a cold-cache node; the
    // gateway redirect waits out the election rather than failing fast.
    net.crash(leader);
    let balance = {
        let granary = granaries[reader].clone();
        drive(&sim, Duration::from_secs(12), async move {
            granary.grain(key).ask_timeout(ReadBalance, Duration::from_secs(11)).await
        })
    };
    assert_eq!(balance, Ok(400), "the call during failover was absorbed and succeeded");
}

#[test]
fn the_shard_map_is_consensus_agreed_across_nodes() {
    // §7.6: the allocation is decided once by the per-type map group and committed,
    // so every node reports the *identical* replica set for each grain — the
    // agreement a per-node derived snapshot cannot guarantee under staggered
    // observation. Checks several keys (which hash to different shards).
    let sim = Simulation::new(8);
    let (_net, _systems, granaries) = cluster(&sim);
    for key in ["account/1", "account/42", "account/13", "account/99", "account/replicated"] {
        let sets: Vec<Vec<NodeId>> = granaries.iter().map(|g| g.replicas(key)).collect();
        let first = &sets[0];
        assert!(!first.is_empty(), "the shard for {key} has a committed allocation");
        assert!(
            sets.iter().all(|s| s == first),
            "every node agrees on {key}'s replica set (got {sets:?})",
        );
    }
}

/// Promote `node` (already joined to `net`) into the control quorum via the
/// current control leader, so it enters `cluster_voters()` and the allocator
/// rebalances onto it. Returns once the change is committed.
fn add_control_voter(sim: &Simulation, systems: &[SimCluster], node: NodeId) {
    let leader = systems
        .iter()
        .find(|s| s.leader() == Some(s.node()))
        .cloned()
        .expect("a control leader");
    let ok = drive(sim, Duration::from_secs(15), async move {
        leader.admit(node).await && leader.add_voter(node).await
    });
    assert!(ok, "node {node:?} is admitted and added to the control quorum");
}

#[test]
fn a_compacted_shard_brings_a_new_replica_up_via_snapshot() {
    // §9 log compaction: sustained writes to a grain push its shard's Raft log past
    // the compaction threshold, so each replica discards the committed prefix
    // against a projection snapshot. When the cluster then grows and the shard
    // rebalances onto a *new* node, that node cannot replay the (compacted) history
    // — the shard leader catches it up with an InstallSnapshot. The new replica must
    // still serve the grain's exact committed balance (no lost or doubled writes).
    let sim = Simulation::new(13);
    let net = leader_net(&sim);
    let founders = vec![net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2));
    let granaries: Vec<Granary<Account>> =
        founders.iter().map(|s| s.granary::<Account>(config())).collect();
    sim.run_for(Duration::from_secs(3));

    // Many deposits on one grain — well past the shard's compaction threshold
    // (each deposit is one committed record), so the replicas compact their logs.
    let key = "account/42";
    const DEPOSITS: usize = 90;
    let total = {
        let g = granaries[0].clone();
        drive(&sim, Duration::from_secs(40), async move {
            let acct = g.grain(key);
            for _ in 0..DEPOSITS {
                acct.ask(Deposit { cents: 1 }).await?;
            }
            acct.ask(ReadBalance).await
        })
    };
    assert_eq!(total, Ok(DEPOSITS as i64), "all deposits committed before growth");

    // Grow the cluster so the shard rebalances onto fresh nodes.
    let d = net.join(D);
    let e = net.join(E);
    add_control_voter(&sim, &founders, D);
    add_control_voter(&sim, &founders, E);
    let gd = d.granary::<Account>(config());
    let _ge = e.granary::<Account>(config());
    sim.run_for(Duration::from_secs(12)); // rebalance + InstallSnapshot catch-up

    // The grain's shard rebalanced onto at least one new node, which had to be
    // brought up to date from the snapshot (the prefix was already compacted away).
    let replicas = granaries[0].replicas(key);
    assert!(
        replicas.contains(&D) || replicas.contains(&E),
        "the grain's shard rebalanced onto a new node (got {replicas:?})",
    );

    // A read through the new node returns the exact committed balance — compaction
    // preserved state and the snapshot install reconstructed the projection.
    let balance = {
        let gd = gd.clone();
        drive(&sim, Duration::from_secs(12), async move {
            gd.grain(key).ask_timeout(ReadBalance, Duration::from_secs(11)).await
        })
    };
    assert_eq!(balance, Ok(DEPOSITS as i64), "the new replica serves the committed balance via snapshot");
}

#[test]
fn growing_the_cluster_rebalances_shards_onto_new_nodes() {
    // §7.6/§7.7 rebalancing (grow): a node that joins *after* the map formed is no
    // longer route-only — the allocator recomputes each shard's replica set by
    // rendezvous over the now-larger `cluster_voters()` and re-proposes `Assign`,
    // and the reconcile loop drives each shard group's Raft membership in place onto
    // the new replicas. A grain committed before the growth keeps its value (the
    // shard reconfigures, it does not restart), and the new nodes end up hosting.
    let sim = Simulation::new(9);
    let net = leader_net(&sim);
    let founders = vec![net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let granaries: Vec<Granary<Account>> =
        founders.iter().map(|s| s.granary::<Account>(config())).collect();
    sim.run_for(Duration::from_secs(3)); // map group + shards

    // Commit a deposit; with three founders each shard's replica set is all three.
    let key = "account/42";
    let committed = {
        let g = granaries[0].clone();
        drive(&sim, Duration::from_secs(8), async move {
            g.grain(key).ask(Deposit { cents: 300 }).await
        })
    };
    assert_eq!(committed, Ok(300));
    let founding = granaries[0].replicas(key);
    assert_eq!(founding.len(), 3, "the founding shard has the configured three replicas");

    // Two nodes join and are promoted into the control quorum, and host the type.
    let d = net.join(D);
    let e = net.join(E);
    add_control_voter(&sim, &founders, D);
    add_control_voter(&sim, &founders, E);
    let gd = d.granary::<Account>(config());
    let _ge = e.granary::<Account>(config());
    sim.run_for(Duration::from_secs(10)); // allocator re-proposes; shard groups reconfigure

    // The allocation now spans five nodes, so some shard rebalanced onto D or E.
    let all_replicas: Vec<NodeId> = ["account/42", "account/7", "account/13", "account/99"]
        .iter()
        .flat_map(|k| granaries[0].replicas(*k))
        .collect();
    assert!(
        all_replicas.contains(&D) || all_replicas.contains(&E),
        "growing the cluster rebalanced at least one shard onto a new node (got {all_replicas:?})",
    );
    // Every node still agrees on the (new) allocation.
    let from_new = gd.replicas(key);
    assert_eq!(from_new, granaries[0].replicas(key), "the new node agrees on the rebalanced allocation");

    // The grain committed before the growth survived the in-place reconfiguration:
    // a read on the new node sees the durable balance, and a further write commits.
    let balance = {
        let gd = gd.clone();
        drive(&sim, Duration::from_secs(12), async move {
            let acct = gd.grain(key);
            acct.ask_timeout(Deposit { cents: 200 }, Duration::from_secs(11)).await?;
            acct.ask_timeout(ReadBalance, Duration::from_secs(11)).await
        })
    };
    assert_eq!(balance, Ok(500), "no committed write was lost across the rebalance (G14)");
}

#[test]
fn shrinking_the_cluster_moves_shards_off_a_removed_node() {
    // §7.6/§7.7 rebalancing (shrink): when a node leaves the control quorum, the
    // allocator recomputes each shard over the smaller `cluster_voters()` and
    // re-proposes, and the reconcile loop reconfigures the affected shard groups
    // onto survivors. A grain on a shard that the removed node replicated keeps its
    // committed value — the shard's surviving quorum carries it (G14).
    let sim = Simulation::new(11);
    let net = leader_net(&sim);
    let founders = vec![net.join(A), net.join(B), net.join(C), net.join(D), net.join(E)];
    sim.run_for(Duration::from_secs(2));
    let granaries: Vec<Granary<Account>> =
        founders.iter().map(|s| s.granary::<Account>(config())).collect();
    // Five founders, but their RaftConfig only seeds {A,B,C} as control voters, so
    // promote D and E so the allocator can place shards across all five.
    add_control_voter(&sim, &founders, D);
    add_control_voter(&sim, &founders, E);
    sim.run_for(Duration::from_secs(8)); // allocator spreads shards across five nodes

    // Pick a key whose shard has more than the bare 1-survivor, and a replica of it
    // to remove (prefer a non-leader so the leader stays put for a clean assertion).
    let key = "account/42";
    let committed = {
        let g = granaries[0].clone();
        drive(&sim, Duration::from_secs(8), async move {
            g.grain(key).ask(Deposit { cents: 700 }).await
        })
    };
    assert_eq!(committed, Ok(700));
    let before = granaries[0].replicas(key);
    let shard_leader = granaries[0].leader(key).expect("the shard elected a leader");
    let victim = *before
        .iter()
        .find(|n| **n != shard_leader)
        .expect("the shard has a non-leader replica to remove");

    // Remove the victim from the control quorum, on the current control leader.
    let leader = founders
        .iter()
        .find(|s| s.leader() == Some(s.node()))
        .cloned()
        .expect("a control leader");
    let removed = drive(&sim, Duration::from_secs(15), async move {
        leader.remove_voter(victim).await
    });
    assert!(removed, "the victim left the control quorum");
    sim.run_for(Duration::from_secs(10)); // allocator re-proposes; shard reconfigures

    let after = granaries[0].replicas(key);
    assert!(!after.contains(&victim), "the removed node no longer replicates the shard (got {after:?})");
    assert_eq!(after.len(), before.len(), "the shard kept its replication factor on survivors");

    // The committed value survived the reconfiguration onto survivors.
    let balance = {
        let g = granaries[0].clone();
        drive(&sim, Duration::from_secs(12), async move {
            g.grain(key).ask_timeout(ReadBalance, Duration::from_secs(11)).await
        })
    };
    assert_eq!(balance, Ok(700), "no committed write was lost across the shrink (G14)");
}

#[test]
fn quorum_loss_pauses_grain_writes_with_unavailable() {
    // G11 (CP): an active grain whose shard loses quorum still believes it leads and
    // accepts the write, but the per-grain append can never reach a quorum — the
    // output gate releases `Unavailable`, not a forked success. State is untouched.
    let sim = Simulation::new(4);
    let (net, systems, granaries) = cluster(&sim);
    let key = "account/99";
    let leader = granaries[0].leader(key).expect("the shard elected a leader");
    let leader_idx = systems.iter().position(|s| s.node() == leader).unwrap();

    // Activate the grain and commit one write *with* a quorum, so the leader holds a
    // live, quorum-recovered activation (§8): the §11 scenario is an active grain
    // that then loses quorum, not a cold activation under quorum loss (which can never
    // recover its head and so fails to activate at all — also CP-correct).
    let committed = {
        let granary = granaries[leader_idx].clone();
        drive(&sim, Duration::from_secs(5), async move {
            granary.grain(key).ask(Deposit { cents: 100 }).await
        })
    };
    assert_eq!(committed, Ok(100), "the first write commits with a quorum");

    // Crash the two non-leaders, leaving the leader without a quorum.
    for (idx, system) in systems.iter().enumerate() {
        if idx != leader_idx {
            net.crash(system.node());
        }
    }
    sim.run_for(Duration::from_secs(1));

    // The next write appends to the local replica but can never reach a write quorum
    // (§7.2); the ask deadline outlasts the per-grain append timeout, so the
    // durability outcome surfaces.
    let outcome = {
        let granary = granaries[leader_idx].clone();
        drive(&sim, Duration::from_secs(13), async move {
            granary
                .grain(key)
                .ask_timeout(Deposit { cents: 1 }, Duration::from_secs(12))
                .await
        })
    };
    assert!(
        matches!(outcome, Err(GrainError::Unavailable(_))),
        "quorum loss must pause writes as Unavailable, got {outcome:?}",
    );
}

#[test]
fn storage_distributes_and_non_replicas_still_route() {
    // §7.6 storage distribution: with 5 nodes and replication_factor 3, each shard
    // lives on exactly 3 replica nodes; the other 2 hold no data for it. A
    // non-replica routes to a replica via the shard map, so the grain is callable
    // with identical durable replies from every node — replicas and non-replicas
    // alike (invariant G13) — while storage is bounded to R, not the cluster size.
    let sim = Simulation::new(7);
    let net = SimNetwork::new(&sim).with_leader(
        swim(),
        {
            let mut raft = RaftConfig::new(vec![A, B, C, D, E]);
            raft.election_timeout = Duration::from_millis(500);
            raft.heartbeat_interval = Duration::from_millis(100);
            raft
        },
        DowningPolicy::Conservative,
    );
    let systems: Vec<SimCluster> = [A, B, C, D, E].iter().map(|&n| net.join(n)).collect();
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader

    let cfg = GranaryConfig {
        shards: 1,
        replication_factor: 3,
        idle_after: Duration::from_secs(60),
        snapshot_every: 8,
        ..GranaryConfig::default()
    };
    let granaries: Vec<Granary<Account>> =
        systems.iter().map(|s| s.granary::<Account>(cfg.clone())).collect();
    sim.run_for(Duration::from_secs(3)); // elect the shard group's leader

    let key = "account/replicated";
    // The shard is replicated on exactly 3 of the 5 nodes — storage is bounded to
    // the replication factor, not the cluster size.
    let replicas = granaries[0].replicas(key);
    assert_eq!(replicas.len(), 3, "the shard lives on exactly R replicas, not all 5 nodes");
    // Pick a node that does NOT replicate the shard — it holds no data for it and
    // must route to a replica.
    let non_replica = systems
        .iter()
        .position(|s| !replicas.contains(&s.node()))
        .expect("with R=3 and 5 nodes, 2 nodes do not replicate the shard");

    // Commit through the non-replica: it routes the write to the replica leader.
    let committed = {
        let granary = granaries[non_replica].clone();
        drive(&sim, Duration::from_secs(8), async move {
            granary.grain(key).ask(Deposit { cents: 700 }).await
        })
    };
    assert_eq!(committed, Ok(700), "a non-replica routes the write to a replica leader");

    // Every node — replicas and non-replicas — reads the durable balance.
    for (index, granary) in granaries.iter().enumerate() {
        let balance = {
            let granary = granary.clone();
            drive(&sim, Duration::from_secs(8), async move {
                granary.grain(key).ask_timeout(ReadBalance, Duration::from_secs(7)).await
            })
        };
        assert_eq!(
            balance,
            Ok(700),
            "node index {index} (replica or not) must route and read the durable balance",
        );
    }
}

#[test]
fn quorum_loss_is_contained_to_its_shard_others_keep_serving() {
    // G11 / §11 BLAST RADIUS: "only that shard's grains are affected; the rest of
    // the cluster serves normally." This is the whole point of sharded consensus —
    // a quorum loss is contained to one shard. Every other test either runs one
    // shard or puts both shards' replicas on all three nodes, so a quorum loss
    // takes out everything; none proves containment. Here: 5 nodes, R=3, several
    // shards on *different* replica sets. We kill the two followers of shard X
    // (X's leader survives but loses quorum) while leaving shard Y a full quorum,
    // and prove a grain in X pauses with `Unavailable` while a grain in Y commits
    // — in the same run, same instant.
    let sim = Simulation::new(7);
    // Seed only {A,B,C} as control voters (as `leader_net`/the rebalance tests do),
    // then promote D and E — listing all five in RaftConfig would make them voters
    // already, so `add_voter` would be a no-op.
    let net = leader_net(&sim);
    let systems: Vec<SimCluster> = [A, B, C, D, E].iter().map(|&n| net.join(n)).collect();
    sim.run_for(Duration::from_secs(2));
    const SHARDS: usize = 8;
    let cfg = GranaryConfig {
        shards: SHARDS,
        replication_factor: 3,
        idle_after: Duration::from_secs(60),
        snapshot_every: 8,
        ..GranaryConfig::default()
    };
    let granaries: Vec<Granary<Account>> =
        systems.iter().map(|s| s.granary::<Account>(cfg.clone())).collect();
    // The allocator spreads shards over `cluster_voters()`; the RaftConfig listing
    // D and E is not enough — they must be admitted to the control quorum, exactly
    // as the rebalance tests do, or every shard collapses onto the same 3 voters.
    add_control_voter(&sim, &systems, D);
    add_control_voter(&sim, &systems, E);
    sim.run_for(Duration::from_secs(6)); // allocator spreads shards; elect leaders

    // Probe keys to learn each shard's (representative key, replicas, leader).
    let mut shard_of: std::collections::BTreeMap<u32, (String, Vec<NodeId>, NodeId)> =
        std::collections::BTreeMap::new();
    for i in 0..80u64 {
        let key = format!("account/{i}");
        let idx = granary::shard_for("bank.Account", &key, SHARDS).index;
        if let std::collections::btree_map::Entry::Vacant(slot) = shard_of.entry(idx) {
            let replicas = granaries[0].replicas(&key);
            if let Some(leader) = granaries[0].leader(&key) {
                if replicas.len() == 3 && replicas.contains(&leader) {
                    slot.insert((key, replicas, leader));
                }
            }
        }
    }

    // Find a target shard X (kill its two followers) and a bystander shard Y that
    // keeps a quorum once those two are gone, with Y's leader still alive.
    let mut chosen: Option<((String, NodeId), (String, NodeId), [NodeId; 2])> = None;
    'outer: for (_xi, (xkey, xreplicas, xleader)) in &shard_of {
        let followers: Vec<NodeId> = xreplicas.iter().copied().filter(|&n| n != *xleader).collect();
        let victims = [followers[0], followers[1]];
        for (_yi, (ykey, yreplicas, yleader)) in &shard_of {
            if ykey == xkey {
                continue;
            }
            let y_survivors = yreplicas.iter().filter(|n| !victims.contains(n)).count();
            if !victims.contains(yleader) && y_survivors >= 2 {
                chosen = Some(((xkey.clone(), *xleader), (ykey.clone(), *yleader), victims));
                break 'outer;
            }
        }
    }
    let ((xkey, xleader), (ykey, yleader), victims) = chosen.expect(
        "found a shard X to isolate and a shard Y that keeps quorum — requires shards to \
         spread across replica sets (see the select_replicas rendezvous fix)",
    );
    let xleader_idx = systems.iter().position(|s| s.node() == xleader).unwrap();
    let yleader_idx = systems.iter().position(|s| s.node() == yleader).unwrap();

    // Baseline: both grains commit while healthy.
    for (idx, key, amount) in [(xleader_idx, xkey.clone(), 100u64), (yleader_idx, ykey.clone(), 200)] {
        let g = granaries[idx].clone();
        let label = key.clone();
        let committed =
            drive(&sim, Duration::from_secs(8), async move { g.grain(key).ask(Deposit { cents: amount }).await });
        assert!(matches!(committed, Ok(_)), "baseline commit for {label} failed: {committed:?}");
    }

    // Kill shard X's two followers: X's leader survives but can no longer reach a
    // quorum; shard Y is untouched (its quorum and leader are intact).
    for v in victims {
        net.crash(v);
    }
    sim.run_for(Duration::from_secs(2));

    // Shard X: the write proposes on the surviving leader but never commits.
    let x_outcome = {
        let g = granaries[xleader_idx].clone();
        let xkey = xkey.clone();
        drive(&sim, Duration::from_secs(13), async move {
            g.grain(xkey).ask_timeout(Deposit { cents: 9 }, Duration::from_secs(12)).await
        })
    };
    assert!(
        matches!(x_outcome, Err(GrainError::Unavailable(_))),
        "the isolated shard must pause with Unavailable, got {x_outcome:?}",
    );

    // Shard Y, in the SAME run: still has a quorum, so its grain commits normally.
    let y_outcome = {
        let g = granaries[yleader_idx].clone();
        let ykey = ykey.clone();
        drive(&sim, Duration::from_secs(9), async move {
            g.grain(ykey).ask_timeout(Deposit { cents: 5 }, Duration::from_secs(8)).await
        })
    };
    assert_eq!(
        y_outcome,
        Ok(205),
        "a bystander shard keeps serving while another shard has lost quorum (G11 blast radius)",
    );
}

// --- Concurrent linearizability across a shard-leader failover ----------------
//
// The strongest cross-node correctness check: concurrent clients on *different*
// nodes hammer one counter grain (the single linearizable object) while its shard
// leader is crashed mid-run, and the recorded history is decided against the
// `Counter` reference model. This jointly exercises single-writer ordering (G1),
// the deterministic fold (G2), location transparency (G13), and lossless failover
// (G14): every accepted `Add` must take effect exactly once and every `Read` must
// be explainable by some serial order — a double-applied or lost write would make
// the history non-linearizable. A timed-out call records a *pending* op (`info`),
// which the checker may place or drop, so unknown outcomes under fault are sound.

#[derive(Default)]
struct CounterGrain;

#[derive(Default, Serialize, Deserialize)]
struct CounterState {
    value: i64,
}

#[derive(Serialize, Deserialize)]
enum CounterEvent {
    Added(i64),
}

impl Grain for CounterGrain {
    type System = SimCluster;
    type State = CounterState;
    type Event = CounterEvent;
    const GRAIN_TYPE: &'static str = "test.Counter";

    fn apply(state: &mut CounterState, event: &CounterEvent) {
        match event {
            CounterEvent::Added(d) => state.value += *d,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Add>();
        r.accept::<ReadCount>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Add(i64);
impl Message for Add {
    type Reply = i64; // post-command value
    const MANIFEST: Manifest = Manifest::new("test.Add");
}

impl GrainHandler<Add> for CounterGrain {
    async fn handle(&self, state: &CounterState, msg: Add, _ctx: &GrainCtx<Self>) -> (Vec<CounterEvent>, i64) {
        // Non-idempotent: a double-fold shows up as a wrong Read the checker flags.
        (vec![CounterEvent::Added(msg.0)], state.value + msg.0)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadCount;
impl Message for ReadCount {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("test.ReadCount");
}

impl GrainHandler<ReadCount> for CounterGrain {
    async fn handle(&self, state: &CounterState, _msg: ReadCount, _ctx: &GrainCtx<Self>) -> (Vec<CounterEvent>, i64) {
        (vec![], state.value)
    }
}

/// Bring up a 3-node leader cluster hosting `CounterGrain` on every node.
fn counter_cluster(sim: &Simulation) -> (SimNetwork, Vec<SimCluster>, Vec<Granary<CounterGrain>>) {
    let net = leader_net(sim);
    let systems = vec![net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // control-plane leader
    let granaries: Vec<Granary<CounterGrain>> =
        systems.iter().map(|s| s.granary::<CounterGrain>(config())).collect();
    sim.run_for(Duration::from_secs(3)); // shard-group leaders
    (net, systems, granaries)
}

#[test]
fn concurrent_counter_grain_is_linearizable_across_failover() {
    // G1/G2/G13/G14: one grain, concurrent clients on the two *survivor* nodes,
    // the shard leader crashed while traffic is in flight. The observed history
    // must be linearizable on every seed — a write applied twice (e.g. a
    // non-idempotent retry after an ambiguous failure, the at-most-once contract
    // of §2.2/G5) or a lost committed write (G14) would be caught here. A call
    // whose outcome is unknown (`Unreachable`/`Timeout` during the failover) is
    // recorded as a *pending* op the checker may place or drop, which is sound.
    //
    // Clients run on the survivors, not on the crashed leader, **deliberately**:
    // granary serves reads from the leader's in-memory state without a Raft
    // read-index or leader lease, so a deposed-but-isolated leader can still serve
    // a *stale* read from a client co-located with it (§7.5 offers only this
    // relaxed read; linearizable reads are a deferred extension). Reaching the
    // current leader — the survivors' path — is the guarantee this asserts.
    for seed in 0..16 {
        let sim = Simulation::new(seed);
        let (net, systems, granaries) = counter_cluster(&sim);
        let key = "counter/0";
        let leader = granaries[0].leader(key).expect("the shard elected a leader");
        let survivors: Vec<usize> = systems
            .iter()
            .enumerate()
            .filter(|(_, s)| s.node() != leader)
            .map(|(i, _)| i)
            .collect();

        let history: History<Counter> = History::new();

        // One client per survivor node; their calls route to the leader and, after
        // the crash, fail over to the new leader.
        for &idx in &survivors {
            let granary = granaries[idx].clone();
            let history = history.clone();
            let entropy = systems[0].entropy().clone();
            sim.spawner().launch(Box::pin(async move {
                let counter = granary.grain(key);
                for _ in 0..8 {
                    if entropy.next_u64() % 2 == 0 {
                        let delta = 1 + (entropy.next_u64() % 3) as i64;
                        let id = history.invoke(CounterOp::Add(delta));
                        match counter.ask_timeout(Add(delta), Duration::from_secs(8)).await {
                            Ok(_) => history.ok(id, CounterRet::AddOk),
                            Err(_) => history.info(id), // unknown outcome: pending
                        }
                    } else {
                        let id = history.invoke(CounterOp::Read);
                        match counter.ask_timeout(ReadCount, Duration::from_secs(8)).await {
                            Ok(value) => history.ok(id, CounterRet::Read(value)),
                            Err(_) => history.info(id),
                        }
                    }
                }
            }));
        }

        // Crash the shard leader partway through the traffic, forcing in-flight
        // calls to fail over to a new leader.
        let crasher = net.clone();
        sim.spawner().launch(Box::pin(async move {
            systems[0].clock().sleep(Duration::from_millis(400)).await;
            crasher.crash(leader);
        }));

        sim.run_for(Duration::from_secs(30));

        let verdict = check_linearizable(&history);
        assert!(
            verdict.is_ok(),
            "seed {seed}: counter grain history not linearizable across failover: {verdict:?}",
        );
    }
}

// --- Scaling claims: G7 (bounded groups), G8/G9 (control plane off data path) -

#[test]
fn many_grains_collapse_onto_a_bounded_set_of_shards() {
    // G7: the cluster runs O(shards) consensus groups, never O(grains). The unit
    // of consensus is the shard, which holds many grains (§7.1), so a large grain
    // population maps onto the fixed shard set — the number of Raft groups is the
    // shard count, not the grain count. Observable through the public name→shard
    // map: hundreds of distinct grain names resolve to at most `shards` shards,
    // and every node agrees on the (bounded) replica set of each.
    let sim = Simulation::new(1);
    let (_net, _systems, granaries) = cluster(&sim); // config(): shards = 2
    let shards = 2usize;

    let mut shard_indices = std::collections::BTreeSet::new();
    let mut replica_sets = std::collections::BTreeSet::new();
    for i in 0..300u64 {
        let key = format!("account/{i}");
        shard_indices.insert(granary::shard_for("bank.Account", &key, shards).index);
        let replicas = granaries[0].replicas(&key);
        assert!(!replicas.is_empty(), "every grain's shard has a committed allocation");
        replica_sets.insert(replicas);
    }
    assert!(
        shard_indices.len() <= shards,
        "300 grains collapse onto at most {shards} shards (got {shard_indices:?}) — O(shards), not O(grains)",
    );
    assert!(
        replica_sets.len() <= shards,
        "the distinct replica sets are bounded by the shard count, not the grain count",
    );
}

#[test]
fn activations_and_writes_leave_the_shard_map_unchanged() {
    // G8 (activation without consensus) and G9 (control plane off the data path):
    // the shard map changes only on cluster events (splits/merges/membership),
    // never on a grain activation or write (§7.6, §7.8). Snapshot the full
    // allocation and per-shard leadership, then drive a burst of fresh
    // activations *and* committed writes across many grains; the allocation and
    // leadership must be byte-for-byte unchanged — data-plane traffic never
    // touched the control plane.
    let sim = Simulation::new(2);
    let (_net, _systems, granaries) = cluster(&sim);
    let probe_keys: Vec<String> = (0..40u64).map(|i| format!("account/{i}")).collect();

    let allocation_before: Vec<Vec<NodeId>> =
        probe_keys.iter().map(|k| granaries[0].replicas(k)).collect();
    let leaders_before: Vec<Option<NodeId>> =
        probe_keys.iter().map(|k| granaries[0].leader(k)).collect();

    // A burst of activations + committed writes across many distinct grains.
    let total: i64 = {
        let g = granaries[0].clone();
        drive(&sim, Duration::from_secs(20), async move {
            let mut sum = 0;
            for i in 0..40u64 {
                let acct = g.grain(format!("account/{i}"));
                if let Ok(v) = acct.ask(Deposit { cents: 1 }).await {
                    sum += v;
                }
            }
            sum
        })
    };
    assert_eq!(total, 40, "every fresh grain activated and committed its deposit");

    let allocation_after: Vec<Vec<NodeId>> =
        probe_keys.iter().map(|k| granaries[0].replicas(k)).collect();
    let leaders_after: Vec<Option<NodeId>> =
        probe_keys.iter().map(|k| granaries[0].leader(k)).collect();

    assert_eq!(
        allocation_before, allocation_after,
        "no activation or write changed the shard-map allocation (G9)",
    );
    assert_eq!(
        leaders_before, leaders_after,
        "no activation or write moved a shard's leadership (G8/G9)",
    );
}

// --- Atomic multi-event batch: no observer ever sees a partial command --------
//
// §7.3 / §6: "all of a command's events commit in one Raft entry, so no observer
// ever sees a partial command." Every other grain here emits exactly ONE event
// per command, so the all-or-nothing property of a multi-event batch has never
// been exercised. `Pair` emits TWO events per write — `IncA` then `IncB` — that
// jointly preserve the invariant `a == b`. A reader that ever observed `a != b`
// would prove a torn batch: the entry committed (or folded) one event without the
// other. We hammer one `Pair` grain with concurrent writers and readers while its
// shard leader is crashed mid-run, so a torn commit at the failover boundary (an
// entry partially applied on the old leader, or a half-batch surviving) would be
// caught. The invariant must hold on every read, on every seed.

#[derive(Default)]
struct Pair;

#[derive(Default, Serialize, Deserialize)]
struct PairState {
    a: i64,
    b: i64,
}

#[derive(Serialize, Deserialize)]
enum PairEvent {
    IncA,
    IncB,
}

impl Grain for Pair {
    type System = SimCluster;
    type State = PairState;
    type Event = PairEvent;
    const GRAIN_TYPE: &'static str = "test.Pair";

    fn apply(state: &mut PairState, event: &PairEvent) {
        match event {
            PairEvent::IncA => state.a += 1,
            PairEvent::IncB => state.b += 1,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Bump>();
        r.accept::<ReadPair>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Bump;
impl Message for Bump {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.Bump");
}

impl GrainHandler<Bump> for Pair {
    async fn handle(&self, _state: &PairState, _msg: Bump, _ctx: &GrainCtx<Self>) -> (Vec<PairEvent>, ()) {
        // Two events in one command — they must commit and fold atomically.
        (vec![PairEvent::IncA, PairEvent::IncB], ())
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadPair;
impl Message for ReadPair {
    type Reply = (i64, i64);
    const MANIFEST: Manifest = Manifest::new("test.ReadPair");
}

impl GrainHandler<ReadPair> for Pair {
    async fn handle(&self, state: &PairState, _msg: ReadPair, _ctx: &GrainCtx<Self>) -> (Vec<PairEvent>, (i64, i64)) {
        (vec![], (state.a, state.b))
    }
}

#[test]
fn a_multi_event_command_commits_atomically_across_failover() {
    for seed in 0..12 {
        let sim = Simulation::new(seed);
        let net = leader_net(&sim);
        let systems = vec![net.join(A), net.join(B), net.join(C)];
        sim.run_for(Duration::from_secs(2));
        let granaries: Vec<Granary<Pair>> =
            systems.iter().map(|s| s.granary::<Pair>(config())).collect();
        sim.run_for(Duration::from_secs(3));

        let key = "pair/0";
        let leader = granaries[0].leader(key).expect("the shard elected a leader");
        let survivors: Vec<usize> =
            systems.iter().enumerate().filter(|(_, s)| s.node() != leader).map(|(i, _)| i).collect();

        // Concurrent writers and readers on the survivor nodes; every read asserts
        // the invariant a == b (a partial batch would break it).
        let torn = Arc::new(Mutex::new(Vec::<(i64, i64)>::new()));
        for &idx in &survivors {
            let granary = granaries[idx].clone();
            let torn = Arc::clone(&torn);
            let entropy = systems[0].entropy().clone();
            sim.spawner().launch(Box::pin(async move {
                let pair = granary.grain(key);
                for _ in 0..8 {
                    if entropy.next_u64() % 2 == 0 {
                        let _ = pair.ask_timeout(Bump, Duration::from_secs(8)).await;
                    } else if let Ok((a, b)) = pair.ask_timeout(ReadPair, Duration::from_secs(8)).await {
                        if a != b {
                            torn.lock().unwrap().push((a, b));
                        }
                    }
                }
            }));
        }

        // Crash the shard leader partway through, forcing a failover mid-traffic.
        let crasher = net.clone();
        sim.spawner().launch(Box::pin(async move {
            systems[0].clock().sleep(Duration::from_millis(400)).await;
            crasher.crash(leader);
        }));

        sim.run_for(Duration::from_secs(30));

        let torn = torn.lock().unwrap();
        assert!(
            torn.is_empty(),
            "seed {seed}: a read observed a partial multi-event command (a != b): {torn:?} — \
             the batch did not commit/fold atomically (§7.3)",
        );
    }
}
