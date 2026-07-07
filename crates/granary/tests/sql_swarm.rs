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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::SimCluster;
use actor_simulation::check_cluster_reproducible;
use actor_simulation::default_invariants;
use actor_simulation::run_cluster_swarm;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainEvent;
use granary::GrainHandler;
use granary::GrainName;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::NoEvent;
use granary::Sql;
use granary::SqlValue;
use serde::Deserialize;
use serde::Serialize;

// --- A grain whose durable state is entirely its SQLite database ---------------

#[derive(Default)]
struct SqlAccount;

impl Grain for SqlAccount {
    type System = SimCluster;
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

/// **Commit head is monotonic** (invariants **G3**, **G5**): sound across
/// failover — a new leader inherits every committed entry (G14) and continues
/// at a higher seq; a minority "leader" never commits.
#[derive(Default)]
struct CommitMonotonic {
    last: BTreeMap<GrainName, u64>,
}

impl Invariant for CommitMonotonic {
    fn name(&self) -> &'static str {
        "sql-grain-commit-monotonic"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        if let Some(GrainEvent::Committed { name, seq, .. }) = event.as_app::<GrainEvent>() {
            let prev = self.last.get(name).copied().unwrap_or(0);
            if *seq <= prev {
                return Err(format!(
                    "grain {name} committed seq {seq} not after previous head {prev} (G3/G5)"
                ));
            }
            self.last.insert(name.clone(), *seq);
        }
        Ok(())
    }
}

/// **Exactly-once activation per node** (invariant **G6**), crash-sound: a node
/// reported down loses its live set, so post-recovery re-activation is legal.
#[derive(Default)]
struct ActivationSingletonPerNode {
    live: BTreeSet<(NodeId, GrainName)>,
}

impl Invariant for ActivationSingletonPerNode {
    fn name(&self) -> &'static str {
        "sql-grain-activation-singleton-per-node"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        if let Event::NodeDown { node, .. } = event {
            self.live.retain(|(n, _)| n != node);
            return Ok(());
        }
        match event.as_app::<GrainEvent>() {
            Some(GrainEvent::Activated { node, name }) => {
                let fresh = self.live.insert((*node, name.clone()));
                if !fresh {
                    return Err(format!(
                        "grain {name} activated while already live on {node} (G6)"
                    ));
                }
            }
            Some(GrainEvent::Passivated { node, name }) => {
                self.live.remove(&(*node, name.clone()));
            }
            _ => {}
        }
        Ok(())
    }
}

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
        let nodes: Vec<SimCluster> = ctx.nodes().to_vec();
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
        invariants.push(Box::new(CommitMonotonic::default()));
        invariants.push(Box::new(ActivationSingletonPerNode::default()));
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
    if let Err(failure) = run_cluster_swarm(&workload, 0..16) {
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
    for seed in 0..8 {
        if let Err(divergence) = check_cluster_reproducible(&workload, seed) {
            panic!("{divergence}");
        }
    }
}
