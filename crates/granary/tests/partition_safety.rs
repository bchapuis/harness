//! Split-brain safety: grains under a *heal-able* network partition (granary §11,
//! §8, §7.5), under deterministic simulation (granary §14).
//!
//! Every other cluster test fences the single writer with `crash()` — which kills
//! the followers outright, so they fall silent and can never form a competing
//! quorum. That proves the *easy* half of CP. The dangerous half is a partition
//! that **heals**: the isolated old leader stays *alive*, still believes it leads
//! (there is no check-quorum lease today — granary-vv-findings), and the majority
//! side elects a *second* leader that commits in parallel. A naive system forks
//! here. These tests partition the shard leader into a minority, drive committed
//! writes on the majority side and rejected writes on the minority side, then
//! `heal()` and prove the log neither forked nor lost an acknowledged write.
//!
//! - **G11/G1 (no fork).** A minority leader's writes never commit; after heal the
//!   reconciled balance is *exactly* the sum of acknowledged writes — the minority
//!   attempts left no trace (`split_brain_partition_neither_forks_nor_loses`).
//! - **G14 (no loss).** Every write the majority acknowledged survives the heal.
//! - **§7.5 (relaxed reads).** The minority leader MAY serve a stale read but MUST
//!   NOT commit — the documented read/write asymmetry, pinned as an executable
//!   contract (`minority_leader_serves_stale_reads_but_never_commits`).
//! - **Linearizability across partition+heal.** Concurrent clients on both sides,
//!   decided against the `Counter` model
//!   (`counter_grain_is_linearizable_across_a_partition_and_heal`).
//! - **Liveness.** The cluster reconverges and serves the durable value from every
//!   node after the heal (`cluster_reconverges_after_a_partition_heals`).

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

// --- The Appendix A account grain (deposit-only is enough here) ----------------

#[derive(Default)]
struct Account;

#[derive(Default, Serialize, Deserialize)]
struct Balance {
    cents: i64,
}

#[derive(Serialize, Deserialize)]
enum Ledger {
    Deposited(u64),
}

impl Grain for Account {
    type System = SimCluster;
    type State = Balance;
    type Event = Ledger;
    const GRAIN_TYPE: &'static str = "bank.Account";

    fn apply(state: &mut Balance, event: &Ledger) {
        match event {
            Ledger::Deposited(n) => state.cents += *n as i64,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Deposit>();
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

// --- Harness (mirrors tests/clustered_grains.rs) ------------------------------

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
    SimNetwork::new(sim).with_leader(swim(), raft(), DowningPolicy::Conservative)
}

/// One shard, replicated on all three nodes — so partitioning one node away always
/// leaves a 2-of-3 quorum that can elect, and isolates exactly one (deposed)
/// leader. A single shard makes "the leader of *this* key" unambiguous.
fn config() -> GranaryConfig {
    GranaryConfig {
        shards: 1,
        replication_factor: 3,
        idle_after: Duration::from_secs(60),
        snapshot_every: 8,
        ..GranaryConfig::default()
    }
}

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

fn cluster(sim: &Simulation) -> (SimNetwork, Vec<SimCluster>, Vec<Granary<Account>>) {
    let net = leader_net(sim);
    let systems = vec![net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // control-plane leader
    let granaries: Vec<Granary<Account>> =
        systems.iter().map(|system| system.granary::<Account>(config())).collect();
    sim.run_for(Duration::from_secs(3)); // shard-group leader
    (net, systems, granaries)
}

/// The index of the node currently leading `key`'s shard, and the indices of the
/// two survivors (the majority quorum once the leader is partitioned away).
fn leader_and_majority(
    systems: &[SimCluster],
    granaries: &[Granary<Account>],
    key: &str,
) -> (usize, Vec<usize>) {
    let leader = granaries[0].leader(key).expect("the shard elected a leader");
    let leader_idx = systems.iter().position(|s| s.node() == leader).expect("leader is a node");
    let majority: Vec<usize> = (0..systems.len()).filter(|&i| i != leader_idx).collect();
    (leader_idx, majority)
}

// --- The crown jewel: a heal-able partition must not fork or lose writes -------

#[test]
fn split_brain_partition_neither_forks_nor_loses() {
    // G1/G11/G14 under a partition that HEALS (not a crash). The shard leader L is
    // isolated into a minority while still believing it leads; the two survivors
    // elect a competing leader and commit. We drive each phase to completion (no
    // in-flight ambiguity at a transition), so the arithmetic is exact:
    //
    //   * a write the MAJORITY acknowledged MUST appear in the final balance (G14);
    //   * a write the MINORITY leader attempted MUST NOT (G11/G1 — it can never
    //     reach a quorum, so it commits nothing and leaves no trace after heal).
    //
    // A fork would surface the minority deposit in the reconciled total; a lost
    // write would drop the majority deposit. Either falsifies CP. Swept across
    // seeds so every node takes a turn as the partitioned leader.
    for seed in 0..12 {
        let sim = Simulation::new(seed);
        let (net, systems, granaries) = cluster(&sim);
        let key = "account/spine";
        let (leader_idx, majority) = leader_and_majority(&systems, &granaries, key);

        // Phase A (healthy): commit a baseline deposit through the leader.
        let baseline = {
            let g = granaries[majority[0]].clone();
            drive(&sim, Duration::from_secs(8), async move {
                g.grain(key).ask(Deposit { cents: 100 }).await
            })
        };
        assert_eq!(baseline, Ok(100), "seed {seed}: baseline deposit committed");

        // Partition L (the leader) into a minority of one. The survivors keep a
        // 2-of-3 quorum for both the control plane and the shard group.
        let l_node = systems[leader_idx].node();
        let majority_nodes: Vec<NodeId> = majority.iter().map(|&i| systems[i].node()).collect();
        net.partition(&[l_node], &majority_nodes);
        sim.run_for(Duration::from_secs(4)); // survivors elect a new shard leader

        // Phase B (partitioned), majority side: a deposit MUST commit on the new
        // leader. Issued from a survivor; the bounded redirect finds the new leader.
        let majority_commit = {
            let g = granaries[majority[0]].clone();
            drive(&sim, Duration::from_secs(12), async move {
                g.grain(key).ask_timeout(Deposit { cents: 30 }, Duration::from_secs(11)).await
            })
        };
        assert_eq!(
            majority_commit,
            Ok(130),
            "seed {seed}: the majority side committed on the new leader (G14)",
        );

        // Phase B, minority side: a deposit on the deposed leader L MUST be fenced.
        // L still believes it leads (no check-quorum), accepts the command, proposes
        // it, and the wait times out with no quorum — `Unavailable`, never a commit.
        let minority_write = {
            let g = granaries[leader_idx].clone();
            drive(&sim, Duration::from_secs(13), async move {
                g.grain(key).ask_timeout(Deposit { cents: 9000 }, Duration::from_secs(12)).await
            })
        };
        assert!(
            matches!(minority_write, Err(GrainError::Unavailable(_))),
            "seed {seed}: the minority leader must be fenced (Unavailable), got {minority_write:?}",
        );

        // Phase C (healed): the partition clears; L learns the higher term, steps
        // down, and discards its uncommitted tail. Every node must now read the
        // reconciled balance = baseline + majority deposit, and NOT the minority's.
        net.heal();
        sim.run_for(Duration::from_secs(6));

        for &idx in &[leader_idx, majority[0], majority[1]] {
            let balance = {
                let g = granaries[idx].clone();
                drive(&sim, Duration::from_secs(10), async move {
                    g.grain(key).ask_timeout(ReadBalance, Duration::from_secs(9)).await
                })
            };
            assert_eq!(
                balance,
                Ok(130),
                "seed {seed}: node index {idx} sees exactly the acknowledged writes — \
                 no fork (the 9000 minority write left no trace) and no loss (the 30 survived)",
            );
        }
    }
}

// --- §7.5: the minority leader's read/write asymmetry, pinned ------------------

#[test]
fn minority_leader_serves_stale_reads_but_never_commits() {
    // §7.5 is explicit that reads are read-your-leader (relaxed), not linearizable:
    // an isolated minority leader, lacking a check-quorum lease today, MAY serve a
    // stale read until its activation stops — but it can commit NOTHING. This test
    // pins both halves as an executable contract, so the day a leader lease lands
    // (§16) the read assertion is the regression that must flip.
    //
    //   * minority WRITE  -> Unavailable (the firm safety guarantee, always);
    //   * minority READ   -> the *stale* pre-partition value, never the new
    //     committed value (proving reads are NOT linearized on the minority side).
    let sim = Simulation::new(5);
    let (net, systems, granaries) = cluster(&sim);
    let key = "account/asym";
    let (leader_idx, majority) = leader_and_majority(&systems, &granaries, key);

    // Commit 100 while healthy — the value the minority leader will keep serving.
    let baseline = {
        let g = granaries[majority[0]].clone();
        drive(&sim, Duration::from_secs(8), async move {
            g.grain(key).ask(Deposit { cents: 100 }).await
        })
    };
    assert_eq!(baseline, Ok(100));

    // Isolate L; the survivors elect a new leader and advance the committed value.
    let l_node = systems[leader_idx].node();
    let majority_nodes: Vec<NodeId> = majority.iter().map(|&i| systems[i].node()).collect();
    net.partition(&[l_node], &majority_nodes);
    sim.run_for(Duration::from_secs(4));

    let advanced = {
        let g = granaries[majority[0]].clone();
        drive(&sim, Duration::from_secs(12), async move {
            g.grain(key).ask_timeout(Deposit { cents: 50 }, Duration::from_secs(11)).await
        })
    };
    assert_eq!(advanced, Ok(150), "the majority advanced the committed value to 150");

    // A WRITE on the deposed leader is fenced — it can never reach a quorum.
    let minority_write = {
        let g = granaries[leader_idx].clone();
        drive(&sim, Duration::from_secs(13), async move {
            g.grain(key).ask_timeout(Deposit { cents: 1 }, Duration::from_secs(12)).await
        })
    };
    assert!(
        matches!(minority_write, Err(GrainError::Unavailable(_))),
        "the minority leader must not commit, got {minority_write:?}",
    );

    // A READ on the deposed leader, served from its in-memory activation, returns
    // the STALE pre-partition value (100), not the new committed 150. This is the
    // documented §7.5 gap: reads can be stale on the minority side. It is NEVER the
    // linearized-current value while partitioned — that is the property to assert.
    let minority_read = {
        let g = granaries[leader_idx].clone();
        drive(&sim, Duration::from_secs(4), async move {
            g.grain(key).ask_timeout(ReadBalance, Duration::from_secs(3)).await
        })
    };
    assert_ne!(
        minority_read,
        Ok(150),
        "a minority-side read must NOT observe the new committed value (reads are not \
         linearizable on the minority side — §7.5); a leader lease (§16) would make this error",
    );
    assert!(
        matches!(minority_read, Ok(100) | Err(_)),
        "the minority read is the stale value 100 (no lease today) or an error, got {minority_read:?}",
    );

    // After heal, the deposed leader rehydrates to the reconciled state: the stale
    // window closes and every read returns 150.
    net.heal();
    sim.run_for(Duration::from_secs(6));
    let reconciled = {
        let g = granaries[leader_idx].clone();
        drive(&sim, Duration::from_secs(10), async move {
            g.grain(key).ask_timeout(ReadBalance, Duration::from_secs(9)).await
        })
    };
    assert_eq!(reconciled, Ok(150), "after heal the deposed leader serves the reconciled value");
}

// --- Liveness: the cluster reconverges and keeps serving after a heal ----------

#[test]
fn cluster_reconverges_after_a_partition_heals() {
    // CP buys safety at the cost of minority availability *during* the partition;
    // the contract is that availability returns once it heals. Partition, let the
    // majority serve, heal, then prove every node — the formerly isolated one
    // included — serves the durable value and accepts a fresh write that all see.
    let sim = Simulation::new(2);
    let (net, systems, granaries) = cluster(&sim);
    let key = "account/live";
    let (leader_idx, majority) = leader_and_majority(&systems, &granaries, key);

    let l_node = systems[leader_idx].node();
    let majority_nodes: Vec<NodeId> = majority.iter().map(|&i| systems[i].node()).collect();
    net.partition(&[l_node], &majority_nodes);
    sim.run_for(Duration::from_secs(4));

    let committed = {
        let g = granaries[majority[0]].clone();
        drive(&sim, Duration::from_secs(12), async move {
            g.grain(key).ask_timeout(Deposit { cents: 70 }, Duration::from_secs(11)).await
        })
    };
    assert_eq!(committed, Ok(70), "the majority served writes during the partition");

    net.heal();
    sim.run_for(Duration::from_secs(6));

    // A fresh write after heal commits and is visible from every node, including the
    // one that was isolated — no permanent wedge.
    let after_heal = {
        let g = granaries[leader_idx].clone();
        drive(&sim, Duration::from_secs(12), async move {
            g.grain(key).ask_timeout(Deposit { cents: 5 }, Duration::from_secs(11)).await
        })
    };
    assert_eq!(after_heal, Ok(75), "the formerly-isolated node serves writes again after heal");

    for &idx in &[leader_idx, majority[0], majority[1]] {
        let balance = {
            let g = granaries[idx].clone();
            drive(&sim, Duration::from_secs(10), async move {
                g.grain(key).ask_timeout(ReadBalance, Duration::from_secs(9)).await
            })
        };
        assert_eq!(balance, Ok(75), "node index {idx} reconverged to the durable value");
    }
}

// --- `tell` is fire-and-forget even when the commit cannot happen (§6) --------

#[test]
fn tell_under_quorum_loss_never_surfaces_unavailable() {
    // §6: `tell` returns once the host *accepts* the command, not after the commit,
    // so it reports only enqueue-time failures (`Call`/`NotLeader`/`Unhandled`),
    // NEVER `Unavailable`. Under quorum loss the host still accepts and proposes,
    // but the commit can never land; the caller is not notified (at-most-once,
    // §2.2). This is asserted single-node elsewhere; here it must hold on a cluster
    // whose shard has lost its quorum — the exact case where the equivalent `ask`
    // returns `Unavailable`. A `tell` that surfaced `Unavailable` (or blocked on the
    // commit) would violate the fire-and-forget contract.
    let sim = Simulation::new(6);
    let (net, systems, granaries) = cluster(&sim);
    let key = "account/tell";
    let (leader_idx, majority) = leader_and_majority(&systems, &granaries, key);

    // Activate the grain while healthy so its host is live and cached on the leader.
    // The §6 property is about an *accepted enqueue*; a fresh grain would instead
    // spin in activation (the map group has no quorum either, so leadership cannot
    // be confirmed) — that is the resolve path, not the tell contract.
    let warmup = {
        let g = granaries[leader_idx].clone();
        drive(&sim, Duration::from_secs(8), async move { g.grain(key).ask(Deposit { cents: 10 }).await })
    };
    assert_eq!(warmup, Ok(10), "the grain activated and committed while healthy");

    // Crash the leader's two followers: the leader still believes it leads (no
    // check-quorum) and will accept a command on its cached host, but it can reach
    // no quorum to commit it.
    for &m in &majority {
        net.crash(systems[m].node());
    }
    sim.run_for(Duration::from_secs(1));

    // The `tell` hits the cached host and is accepted at enqueue — Ok(()), never
    // Unavailable — even though the deposit can never commit.
    // The `tell` is accepted and returns Ok(()) — never Unavailable. (It is not
    // instant under quorum loss: with the map group also lacking a quorum, host
    // resolution is slow — but the *result* is the contract, and it never reports
    // the durability outcome.)
    let told = {
        let g = granaries[leader_idx].clone();
        drive(&sim, Duration::from_secs(15), async move {
            g.grain(key).tell(Deposit { cents: 1 }).await
        })
    };
    assert_eq!(
        told,
        Ok(()),
        "tell must report only enqueue acceptance, never the durability outcome (§6); got {told:?}",
    );

    // Contrast: the equivalent `ask` in the same state DOES surface the durability
    // failure, so the difference is the `tell`/`ask` contract, not a dead cluster.
    let asked = {
        let g = granaries[leader_idx].clone();
        drive(&sim, Duration::from_secs(13), async move {
            g.grain(key).ask_timeout(Deposit { cents: 1 }, Duration::from_secs(12)).await
        })
    };
    assert!(
        matches!(asked, Err(GrainError::Unavailable(_))),
        "the equivalent ask surfaces Unavailable (proving the shard really lost quorum); got {asked:?}",
    );
}

// --- Linearizability across a partition+heal (the Counter model) --------------

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
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("test.Add");
}

impl GrainHandler<Add> for CounterGrain {
    async fn handle(&self, state: &CounterState, msg: Add, _ctx: &GrainCtx<Self>) -> (Vec<CounterEvent>, i64) {
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

fn counter_cluster(sim: &Simulation) -> (SimNetwork, Vec<SimCluster>, Vec<Granary<CounterGrain>>) {
    let net = leader_net(sim);
    let systems = vec![net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2));
    let granaries: Vec<Granary<CounterGrain>> =
        systems.iter().map(|s| s.granary::<CounterGrain>(config())).collect();
    sim.run_for(Duration::from_secs(3));
    (net, systems, granaries)
}

#[test]
fn counter_grain_is_linearizable_across_a_partition_and_heal() {
    // The strongest cross-node safety net for the split-brain path. Concurrent
    // clients run on BOTH sides of a heal-able partition against one counter grain;
    // the recorded history is decided against the `Counter` model on every seed.
    //
    // The minority-side client only ADDS (writes), deliberately: a minority WRITE
    // that committed would fork the log and surface as a value with no valid serial
    // placement — caught here. (A minority *read* would return a stale value the
    // checker would flag as a non-linearizable `ok`, but that is the documented
    // §7.5 read gap, not a fork; the dedicated test above pins it, so the minority
    // client here writes only.) A fenced write returns an error -> recorded `info`
    // (pending), which the checker may place or drop, so it is sound. The majority
    // client reads and writes against the live leader.
    for seed in 0..16 {
        let sim = Simulation::new(seed);
        let (net, systems, granaries) = counter_cluster(&sim);
        let key = "counter/0";
        let leader = granaries[0].leader(key).expect("the shard elected a leader");
        let leader_idx = systems.iter().position(|s| s.node() == leader).unwrap();
        let majority: Vec<usize> = (0..systems.len()).filter(|&i| i != leader_idx).collect();

        let history: History<Counter> = History::new();

        // Majority client: mixed reads and writes against the live (new) leader.
        {
            let granary = granaries[majority[0]].clone();
            let history = history.clone();
            let entropy = systems[0].entropy().clone();
            sim.spawner().launch(Box::pin(async move {
                let counter = granary.grain(key);
                for _ in 0..8 {
                    if entropy.next_u64() % 2 == 0 {
                        let delta = 1 + (entropy.next_u64() % 3) as i64;
                        let id = history.invoke(CounterOp::Add(delta));
                        match counter.ask_timeout(Add(delta), Duration::from_secs(11)).await {
                            Ok(_) => history.ok(id, CounterRet::AddOk),
                            Err(_) => history.info(id),
                        }
                    } else {
                        let id = history.invoke(CounterOp::Read);
                        match counter.ask_timeout(ReadCount, Duration::from_secs(11)).await {
                            Ok(value) => history.ok(id, CounterRet::Read(value)),
                            Err(_) => history.info(id),
                        }
                    }
                }
            }));
        }

        // Minority client: writes only, against the soon-to-be-deposed leader. Every
        // such write must fail to commit; none may take effect.
        {
            let granary = granaries[leader_idx].clone();
            let history = history.clone();
            let entropy = systems[0].entropy().clone();
            sim.spawner().launch(Box::pin(async move {
                let counter = granary.grain(key);
                for _ in 0..8 {
                    let delta = 1 + (entropy.next_u64() % 3) as i64;
                    let id = history.invoke(CounterOp::Add(delta));
                    match counter.ask_timeout(Add(delta), Duration::from_secs(11)).await {
                        Ok(_) => history.ok(id, CounterRet::AddOk),
                        Err(_) => history.info(id),
                    }
                }
            }));
        }

        // Partition the leader away partway through, then heal before quiescence.
        let net_p = net.clone();
        let l_node = leader;
        let majority_nodes: Vec<NodeId> = majority.iter().map(|&i| systems[i].node()).collect();
        let clock = systems[0].clock().clone();
        sim.spawner().launch(Box::pin(async move {
            clock.sleep(Duration::from_millis(400)).await;
            net_p.partition(&[l_node], &majority_nodes);
            clock.sleep(Duration::from_secs(8)).await;
            net_p.heal();
        }));

        sim.run_for(Duration::from_secs(40));

        let verdict = check_linearizable(&history);
        assert!(
            verdict.is_ok(),
            "seed {seed}: counter history not linearizable across partition+heal: {verdict:?}",
        );
    }
}
