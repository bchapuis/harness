//! The SQL facet under the cluster fault swarm (spec §7.14, §14; V&V checklist
//! #4, #5, #7).
//!
//! `tests/sql.rs` proves the facet's contract on the `Local` tier; this file
//! hosts a SQL-only grain on the leader-based clustered system and sweeps it
//! across seeds while the nemesis injects partitions, crashes, heals, loss,
//! duplication, and delay (spec §18.3). What that uniquely exercises:
//!
//! - **Failover rematerialization.** A leader crash moves the activation to
//!   another node, whose materialization is rebuilt from the composite-snapshot
//!   manifest (blob chunks) plus the committed WAL-frame records —
//!   [`Facet::fold`]/`apply_delta`, the replay path the `Local` tier's
//!   hibernation only partially covers (node-crash cascade, checklist #5).
//! - **Checkpoints under faults.** `snapshot_every` forces checkpoint → blob
//!   puts while the transport drops and duplicates frames.
//! - **Seed-reproducibility (#7).** The same seed replays to a byte-identical
//!   event stream even though every run materializes real SQLite files.
//!
//! Fault *coverage* (#8) for this cluster configuration is already asserted by
//! `tests/grain_swarm.rs` over the same transport; it is not repeated here.
#![cfg(feature = "sql")]

use std::path::PathBuf;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::SimNode;
use actor_simulation::SimSystem;
use actor_simulation::Workload;
use actor_simulation::default_invariants;
use actor_simulation::replay_cluster_swarm;
use actor_simulation::replay_swarm;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_swarm;
use actor_simulation::sweep_seeds;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::NoEvent;
use granary::Sql;
use granary::SqlValue;
use granary::testing::ActivationSingletonPerNode;
use granary::testing::CommitMonotonic;
use serde::Deserialize;
use serde::Serialize;

mod support;
use support::ledger::Add;
use support::ledger::AddRandom;
use support::ledger::Ledger;
use support::ledger::Total;

// --- A grain whose durable state is entirely its SQLite database ---------------

#[derive(Default)]
struct SqlAccount;

impl Grain for SqlAccount {
    type System = SimNode;
    type State = ();
    type Event = NoEvent;
    type Facets = (Sql,);
    const GRAIN_TYPE: &'static str = "bank.SqlAccount";

    fn apply(_state: &mut (), event: &NoEvent) {
        event.unreachable()
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Deposit>();
        r.accept::<ReadTotal>();
    }
}

fn ensure_schema(ctx: &GrainCtx<SqlAccount>) {
    ctx.sql()
        .execute(
            "CREATE TABLE IF NOT EXISTS deposits (cents INTEGER NOT NULL)",
            &[],
        )
        .expect("ddl");
}

/// Insert one deposit row; reply with the running total — a WAL-frame record
/// committing through the quorum path (§7.14, G19).
#[derive(Clone, Serialize, Deserialize)]
struct Deposit {
    cents: u64,
}
impl Message for Deposit {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.SqlDeposit");
}
impl GrainHandler<Deposit> for SqlAccount {
    async fn handle(&self, _state: &(), msg: Deposit, ctx: &GrainCtx<Self>) -> (Vec<NoEvent>, i64) {
        ensure_schema(ctx);
        let sql = ctx.sql();
        sql.execute(
            "INSERT INTO deposits (cents) VALUES (?1)",
            &[SqlValue::Integer(msg.cents as i64)],
        )
        .expect("insert");
        let row = sql
            .query_one("SELECT COALESCE(SUM(cents), 0) FROM deposits", &[])
            .expect("sum");
        let SqlValue::Integer(total) = row[0] else {
            panic!("sum is an integer");
        };
        (vec![], total)
    }
}

/// The running total — a pure read: no frames, no record (§7.5).
#[derive(Clone, Serialize, Deserialize)]
struct ReadTotal;
impl Message for ReadTotal {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.SqlReadTotal");
}
impl GrainHandler<ReadTotal> for SqlAccount {
    async fn handle(
        &self,
        _state: &(),
        _msg: ReadTotal,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, i64) {
        ensure_schema(ctx);
        let row = ctx
            .sql()
            .query_one("SELECT COALESCE(SUM(cents), 0) FROM deposits", &[])
            .expect("sum");
        let SqlValue::Integer(total) = row[0] else {
            panic!("sum is an integer");
        };
        (vec![], total)
    }
}

// --- Grain-specific continuous safety checkers (as in grain_swarm.rs) ----------

// --- The workload ---------------------------------------------------------------

/// Deposit-and-read SQL traffic against a handful of grains under the nemesis,
/// driven through the public `GrainRef` API only (spec §18.4). A faulted call is
/// recorded as nothing and the client moves on, so the drive future always
/// completes and the invariants are checked over whatever the run produced.
///
/// One scratch directory serves every run and every simulated node (the facet
/// keys materializations by node and grain, and restore discards stale files —
/// they are a cache, never truth, §1).
struct SqlAccountSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
    dir: PathBuf,
}

impl SqlAccountSwarm {
    fn config(&self) -> GranaryConfig {
        GranaryConfig {
            shards: 2,
            replication_factor: 3,
            idle_after: Duration::from_secs(60),
            // Checkpoint often: the manifest + blob-chunk path runs under faults,
            // and failover rematerializes from it plus the later frame records.
            snapshot_every: 4,
            data_dir: Some(self.dir.clone()),
            ..GranaryConfig::default()
        }
    }
}

impl ClusterWorkload for SqlAccountSwarm {
    fn name(&self) -> &'static str {
        "granary-sql-account-swarm"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(300),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        // Granary requires the leader-based control plane to host the shard map
        // (§7.6); every node is a control voter so the map group can form.
        ClusterModeSpec::Leader {
            swim: self.swim(),
            voters: self.nodes,
            election_timeout: Duration::from_millis(500),
            heartbeat_interval: Duration::from_millis(100),
            downing: DowningPolicy::Conservative,
        }
    }

    fn setup(&self, _ctx: &ClusterCtx) {}

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        let config = self.config();
        Box::pin(async move {
            let granaries: Vec<_> = nodes
                .iter()
                .map(|s| s.granary::<SqlAccount>(config.clone()))
                .collect();
            let clock = nodes[0].clock().clone();
            let entropy = nodes[0].entropy().clone();
            // Let the control-plane and shard groups elect before traffic.
            clock.sleep(Duration::from_secs(3)).await;

            let mut tasks = Vec::new();
            for c in 0..clients {
                let granary = granaries[c % granaries.len()].clone();
                let entropy = entropy.clone();
                tasks.push(async move {
                    for _ in 0..ops {
                        // A small key space so several grains share each shard.
                        let key = format!("account/{}", entropy.next_u64() % 4);
                        let acct = granary.grain(key);
                        if entropy.next_u64().is_multiple_of(2) {
                            // A short deadline so a faulted call fails fast and
                            // the client keeps issuing traffic.
                            let _ = acct
                                .ask_timeout(Deposit { cents: 1 }, Duration::from_secs(2))
                                .await;
                        } else {
                            let _ = acct.ask_timeout(ReadTotal, Duration::from_secs(2)).await;
                        }
                    }
                });
            }
            futures::future::join_all(tasks).await;
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        let mut invariants = default_invariants();
        invariants.push(Box::new(CommitMonotonic::new(
            "sql-grain-commit-monotonic",
            "grain",
        )));
        invariants.push(Box::new(ActivationSingletonPerNode::new(
            "sql-grain-activation-singleton-per-node",
            "grain",
        )));
        invariants
    }
}

#[test]
fn sql_grain_invariants_hold_under_the_cluster_swarm() {
    // #4: the safety core plus G3/G5 and G6 hold on every seeded run while SQL
    // grains commit WAL-frame records, checkpoint into blobs, and rematerialize
    // across failover, under partitions, crashes, loss, duplication, and delay.
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = SqlAccountSwarm {
        nodes: 3,
        clients: 3,
        ops: 6,
        dir: dir.path().to_path_buf(),
    };
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..16)) {
        panic!("{failure}");
    }
}

#[test]
fn sql_cluster_swarm_is_reproducible() {
    // #7: the same seed replays to a byte-identical event stream — grain events
    // included — even under cluster nemesis and transport faults, with real
    // SQLite files materialized on every node.
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = SqlAccountSwarm {
        nodes: 3,
        clients: 2,
        ops: 5,
        dir: dir.path().to_path_buf(),
    };
    if let Err(divergence) = replay_cluster_swarm(&workload, sweep_seeds(0..8)) {
        panic!("{divergence}");
    }
}

/// SQL traffic under the seeded swarm (spec §18.4): randomized writes and reads
/// across a small key space, with sleeps past `idle_after` so activations
/// hibernate, checkpoint into blobs, and rematerialize mid-run. One scratch
/// directory serves every run: the facet's restore discards stale local files
/// (they are a cache, never truth — §1), which this sharing exercises for free.
///
/// `random` gates the `AddRandom` traffic (SQLite's own `random()`, OS-seeded).
/// It is off for reproducibility sweeps: physical replication makes the value
/// safe for durability (asserted elsewhere), but the §18.1 repro contract is
/// kept strict — no unseeded randomness anywhere in a replayed run.
struct SqlSwarm {
    clients: usize,
    ops: u64,
    random: bool,
    dir: PathBuf,
}
impl Workload for SqlSwarm {
    fn name(&self) -> &'static str {
        "granary-sql-swarm"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let clients = self.clients;
        let ops = self.ops;
        let random = self.random;
        let dir = self.dir.clone();
        Box::pin(async move {
            let ledgers = system.granary::<Ledger>(GranaryConfig {
                idle_after: Duration::from_millis(50),
                snapshot_every: 3, // checkpoint often: the manifest+frames path runs per seed
                data_dir: Some(dir),
                ..GranaryConfig::default()
            });
            let clock = system.clock().clone();
            let entropy = system.entropy().clone();
            let mut tasks = Vec::new();
            for _ in 0..clients {
                let ledgers = ledgers.clone();
                let clock = clock.clone();
                let entropy = entropy.clone();
                tasks.push(async move {
                    for _ in 0..ops {
                        let key = format!("ledger/{}", entropy.next_u64() % 3);
                        let grain = ledgers.grain(key);
                        match entropy.next_u64() % 3 {
                            0 => {
                                let _ = grain
                                    .ask(Add {
                                        name: "swarm".into(),
                                        cents: 1,
                                    })
                                    .await;
                            }
                            1 if random => {
                                let _ = grain.ask(AddRandom).await;
                            }
                            _ => {
                                let _ = grain.ask(Total).await;
                            }
                        }
                        // Sleep past `idle_after` sometimes, so grains hibernate
                        // (checkpoint → blobs) and rehydrate under this seed.
                        if entropy.next_u64().is_multiple_of(4) {
                            clock.sleep(Duration::from_millis(120)).await;
                        }
                    }
                });
            }
            futures::future::join_all(tasks).await;
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        let mut invariants = default_invariants();
        invariants.push(Box::new(CommitMonotonic::new(
            "sql-grain-commit-monotonic",
            "grain",
        )));
        invariants
    }
}
#[test]
fn sql_swarm_invariants_hold_across_seeds() {
    // #4: the safety core plus G3/G5 commit-monotonicity hold across seeds while
    // SQL grains write, hibernate, checkpoint, and rematerialize, with the
    // mailbox capacity fault-sampled per seed.
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = SqlSwarm {
        clients: 3,
        ops: 8,
        random: true,
        dir: dir.path().to_path_buf(),
    };
    if let Err(failure) = run_swarm(&workload, sweep_seeds(0..16)) {
        panic!("{failure}");
    }
}
#[test]
fn sql_swarm_is_reproducible() {
    // #7: the same seed yields a byte-identical event stream — grain events
    // included — even though the workload materializes real SQLite files,
    // checkpoints them into blobs, and rematerializes mid-run. A wall-clock
    // read, an OS thread, or an unseeded RNG anywhere in the facet breaks this.
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = SqlSwarm {
        clients: 2,
        ops: 6,
        random: false,
        dir: dir.path().to_path_buf(),
    };
    if let Err(divergence) = replay_swarm(&workload, sweep_seeds(0..8)) {
        panic!("{divergence}");
    }
}
