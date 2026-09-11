//! The SQL facet under the cluster fault swarm (spec §7.14, §14; V&V checklist
//! #4, #5, #7).
//!
//! `tests/sql.rs` proves the facet's contract on the `Local` tier. This file
//! hosts a SQL-only entity on the leader-based clustered system and sweeps it
//! across seeds while the nemesis injects partitions, crashes, heals, loss,
//! duplication, and delay (spec §18.3). What that adds:
//!
//! - **Failover rematerialization.** A leader crash moves the activation to
//!   another node, whose materialization is rebuilt from the composite-snapshot
//!   manifest (blob chunks) plus the committed WAL-frame records:
//!   [`Facet::fold`]/`apply_delta`, the replay path the `Local` tier's
//!   hibernation only partially covers (node-crash cascade, checklist #5).
//! - **Checkpoints under faults.** `snapshot_every` forces checkpoint → blob
//!   puts while the transport drops and duplicates frames.
//! - **Seed-reproducibility (#7).** The same seed replays to a byte-identical
//!   event stream even though every run materializes real SQLite files.
//!
//! Fault *coverage* (#8) for this cluster configuration is asserted by
//! `tests/entity_swarm.rs` over the same transport, not repeated here.
#![cfg(feature = "sql")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::Rehost;
use actor_simulation::SimNode;
use actor_simulation::SimSystem;
use actor_simulation::Workload;
use actor_simulation::coverage_seeds;
use actor_simulation::default_invariants;
use actor_simulation::replay_cluster_swarm;
use actor_simulation::replay_swarm;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_cluster_swarm_coverage;
use actor_simulation::run_swarm;
use actor_simulation::sweep_seeds;
use entity::Entity;
use entity::EntityConfig;
use entity::EntityCtx;
use entity::EntityExt;
use entity::EntityHandler;
use entity::NoEvent;
use entity::facet::Sql;
use entity::facet::SqlValue;
use entity::testing::ActivationSingletonPerNode;
use entity::testing::CommitMonotonic;

use serde::Deserialize;
use serde::Serialize;
use support::Exercised;

mod support;
use support::ledger::Add;
use support::ledger::AddRandom;
use support::ledger::Ledger;
use support::ledger::Total;

// --- An entity whose durable state is entirely its SQLite database ---------------

#[derive(Default)]
struct SqlAccount;

impl Entity for SqlAccount {
    type System = SimNode;
    type State = ();
    type Event = NoEvent;
    type Facets = (Sql,);
    const ENTITY_TYPE: &'static str = "bank.SqlAccount";

    fn apply(_state: &mut (), event: &NoEvent) {
        event.unreachable()
    }

    fn register(r: &mut entity::EntityRegistry<Self>) {
        r.accept::<Deposit>();
        r.accept::<ReadTotal>();
    }
}

fn ensure_schema(ctx: &EntityCtx<SqlAccount>) {
    ctx.sql()
        .execute(
            "CREATE TABLE IF NOT EXISTS deposits (cents INTEGER NOT NULL)",
            &[],
        )
        .expect("ddl");
}

/// Insert one deposit row; reply with the running total: a WAL-frame record
/// committing through the quorum path (§7.14, E19).
#[derive(Clone, Serialize, Deserialize)]
struct Deposit {
    cents: u64,
}
impl Message for Deposit {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.SqlDeposit");
}
impl EntityHandler<Deposit> for SqlAccount {
    async fn handle(
        &self,
        _state: &(),
        msg: Deposit,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<NoEvent>, i64) {
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

/// The running total, a pure read: no frames, no record (§7.5).
#[derive(Clone, Serialize, Deserialize)]
struct ReadTotal;
impl Message for ReadTotal {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.SqlReadTotal");
}
impl EntityHandler<ReadTotal> for SqlAccount {
    async fn handle(
        &self,
        _state: &(),
        _msg: ReadTotal,
        ctx: &EntityCtx<Self>,
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

// --- The workload ---------------------------------------------------------------

/// Deposit-and-read SQL traffic against a handful of entities under the nemesis,
/// driven through the public `EntityRef` API only (spec §18.4). A faulted call
/// records nothing and the client moves on, so the drive future always completes
/// and the invariants run over whatever the run produced.
///
/// One scratch directory serves every run and every simulated node: the facet
/// keys materializations by node and entity, and restore discards stale files
/// (a cache, never truth, §1).
struct SqlAccountSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
    dir: PathBuf,
    /// Idle past `idle_after` between operations, so activations hibernate and
    /// the database is rematerialized mid-run.
    hibernating: bool,
    /// Let the nemesis kill and re-launch node processes, not just isolate them.
    restarting: bool,
    /// What the sweep actually exercised, accumulated across its seeds.
    exercised: Exercised,
}

impl SqlAccountSwarm {
    /// Activations resident for the whole run; the nemesis only isolates nodes.
    fn new(nodes: usize, clients: usize, ops: u64, dir: PathBuf) -> SqlAccountSwarm {
        SqlAccountSwarm {
            nodes,
            clients,
            ops,
            dir,
            hibernating: false,
            restarting: false,
            exercised: Exercised::default(),
        }
    }

    /// Short activation lifetime, clients that idle past it, and a nemesis that
    /// kills processes, so a read has to come from a database the next
    /// activation rebuilt: composite snapshot, pages fetched and verified by
    /// content (E17), frame records after the snapshot seq replayed on top
    /// (byte-deterministic, F1).
    ///
    /// `sql_swarm.rs`'s single-node `SqlSwarm` hibernates too, but with no
    /// transport and no nemesis; this is the same restore path under faults.
    fn hibernating(nodes: usize, clients: usize, ops: u64, dir: PathBuf) -> SqlAccountSwarm {
        SqlAccountSwarm {
            hibernating: true,
            restarting: true,
            ..SqlAccountSwarm::new(nodes, clients, ops, dir)
        }
    }

    fn config(&self) -> EntityConfig {
        EntityConfig {
            shards: 2,
            replication_factor: 3,
            idle_after: if self.hibernating {
                IDLE_AFTER
            } else {
                RESIDENT
            },
            // Checkpoint often: the manifest + blob-chunk path runs under faults,
            // and failover rematerializes from it plus the later frame records.
            // Oftener when hibernating, so an entity has a checkpoint before it
            // first hibernates.
            snapshot_every: if self.hibernating { 2 } else { 4 },
            data_dir: Some(self.dir.clone()),
            ..EntityConfig::default()
        }
    }
}

/// Activation lifetime for the resident sweeps: longer than any run.
const RESIDENT: Duration = Duration::from_secs(60);
/// Activation lifetime for the hibernating sweep.
const IDLE_AFTER: Duration = Duration::from_millis(200);
/// How long a hibernating client idles: past [`IDLE_AFTER`], so the host really
/// hibernates.
const IDLE_FOR: Duration = Duration::from_millis(500);

impl ClusterWorkload for SqlAccountSwarm {
    fn name(&self) -> &'static str {
        if self.hibernating {
            "entity-sql-account-hibernating-swarm"
        } else {
            "entity-sql-account-swarm"
        }
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn mode(&self) -> ClusterModeSpec {
        // The entity layer requires the leader-based control plane to host the shard map
        // (§7.6); every node is a control voter so the map group can form.
        ClusterModeSpec::leader(self.swim(), self.nodes, DowningPolicy::Conservative)
    }

    fn rehost(&self) -> Option<Rehost> {
        if !self.restarting {
            return None;
        }
        // A restarted node comes up empty and would otherwise stop hosting
        // `SqlAccount`. The config carries the same scratch root, so a fresh
        // process rebuilds over its predecessor's leftover database file:
        // restore discards stale local files (a cache, never truth; §1).
        let config = self.config();
        Some(Arc::new(move |node: &SimNode| {
            node.entities::<SqlAccount>(config.clone());
        }))
    }

    fn setup(&self, _ctx: &ClusterCtx) {}

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        let hibernating = self.hibernating;
        let restarting = self.restarting;
        let config = self.config();
        Box::pin(async move {
            let hosts: Vec<_> = nodes
                .iter()
                .map(|s| s.entities::<SqlAccount>(config.clone()))
                .collect();
            let clock = nodes[0].clock().clone();
            let entropy = nodes[0].entropy().clone();
            // Let the control-plane and shard groups elect before traffic.
            clock.sleep(Duration::from_secs(3)).await;

            let mut tasks = Vec::new();
            for c in 0..clients {
                // Under restarts every client goes through node 1's gateway, the
                // one node the nemesis leaves alone; location transparency (E13)
                // still lands the call on the shard's leader.
                let entities = if restarting {
                    hosts[0].clone()
                } else {
                    hosts[c % hosts.len()].clone()
                };
                let clock = clock.clone();
                let entropy = entropy.clone();
                tasks.push(async move {
                    for op in 0..ops {
                        // A small key space so several entities share each shard.
                        let key = format!("account/{}", entropy.next_u64() % 4);
                        let acct = entities.entity(key);
                        if entropy.next_u64().is_multiple_of(2) {
                            // A short deadline so a faulted call fails fast and
                            // the client keeps issuing traffic.
                            let _ = acct
                                .ask_timeout(Deposit { cents: 1 }, Duration::from_secs(2))
                                .await;
                        } else {
                            let _ = acct.ask_timeout(ReadTotal, Duration::from_secs(2)).await;
                        }
                        // Idle past the activation lifetime on a fixed cadence, so
                        // the next call rematerializes the database from its
                        // checkpoint plus replayed frames whatever the seed.
                        if hibernating && op % 2 == 1 {
                            clock.sleep(IDLE_FOR).await;
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
            "sql-entity-commit-monotonic",
            "entity",
        )));
        invariants.push(Box::new(ActivationSingletonPerNode::new(
            "sql-entity-activation-singleton-per-node",
            "entity",
        )));
        invariants.push(Box::new(self.exercised.clone()));
        invariants
    }
}

#[test]
fn sql_entity_invariants_hold_under_the_cluster_swarm() {
    // #4: the safety core plus E3/E5 and E6 hold on every seeded run while SQL
    // entities commit WAL-frame records, checkpoint into blobs, and rematerialize
    // across failover, under partitions, crashes, loss, duplication, and delay.
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = SqlAccountSwarm::new(3, 3, 6, dir.path().to_path_buf());
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..16)) {
        panic!("{failure}");
    }
}

#[test]
fn sql_entity_invariants_hold_across_hibernation_and_restarts() {
    // The clustered restore path: the entity hibernates, its database file goes,
    // and the next call rebuilds it from the composite snapshot's pages plus the
    // frame records above it, in a process that may not have existed when the
    // write committed.
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = SqlAccountSwarm::hibernating(3, 3, 6, dir.path().to_path_buf());
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..16)) {
        panic!("{failure}");
    }
    workload.exercised.assert_hibernated();
}

#[test]
fn sql_hibernating_cluster_swarm_is_reproducible() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = SqlAccountSwarm::hibernating(3, 2, 5, dir.path().to_path_buf());
    if let Err(divergence) = replay_cluster_swarm(&workload, sweep_seeds(0..8)) {
        panic!("{divergence}");
    }
}

#[test]
fn sql_cluster_swarm_actually_fires_each_fault_type() {
    // #8: the clustered sweeps configure faults, so something must prove they
    // fire. Eight seeds carry the claim "each fault type fired somewhere across
    // these seeds" as well as 32 would, and a seed here materializes real SQLite
    // files on three nodes.
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = SqlAccountSwarm::new(3, 3, 6, dir.path().to_path_buf());
    let stats = match run_cluster_swarm_coverage(&workload, coverage_seeds(0..8)) {
        Ok(stats) => stats,
        Err(failure) => panic!("{failure}"),
    };
    assert!(
        stats.dropped > 0,
        "the sweep never dropped a frame (loss uncovered): {stats:?}"
    );
    assert!(
        stats.duplicated > 0,
        "the sweep never duplicated a frame: {stats:?}"
    );
    assert!(
        stats.delayed > 0,
        "the sweep never delayed a frame (reordering uncovered): {stats:?}"
    );
    assert!(
        stats.blocked > 0,
        "the sweep never blocked a frame (partition/crash uncovered): {stats:?}"
    );
}

#[test]
fn sql_cluster_swarm_is_reproducible() {
    // #7: the same seed replays to a byte-identical event stream (entity events
    // included) even under cluster nemesis and transport faults, with real
    // SQLite files materialized on every node.
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = SqlAccountSwarm::new(3, 2, 5, dir.path().to_path_buf());
    if let Err(divergence) = replay_cluster_swarm(&workload, sweep_seeds(0..8)) {
        panic!("{divergence}");
    }
}

/// SQL traffic under the seeded swarm (spec §18.4): randomized writes and reads
/// across a small key space, with sleeps past `idle_after` so activations
/// hibernate, checkpoint into blobs, and rematerialize mid-run. One scratch
/// directory serves every run, so the facet's restore has to discard stale
/// local files (a cache, never truth; §1).
///
/// `random` gates the `AddRandom` traffic (SQLite's own `random()`, OS-seeded).
/// It is off for reproducibility sweeps: physical replication makes the value
/// safe for durability (asserted elsewhere), but the §18.1 repro contract stays
/// strict, no unseeded randomness in a replayed run.
struct SqlSwarm {
    clients: usize,
    ops: u64,
    random: bool,
    dir: PathBuf,
}
impl Workload for SqlSwarm {
    fn name(&self) -> &'static str {
        "entity-sql-swarm"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let clients = self.clients;
        let ops = self.ops;
        let random = self.random;
        let dir = self.dir.clone();
        Box::pin(async move {
            let ledgers = system.entities::<Ledger>(EntityConfig {
                idle_after: Duration::from_millis(50),
                snapshot_every: 3, // checkpoint often: the manifest+frames path runs per seed
                data_dir: Some(dir),
                ..EntityConfig::default()
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
                        let entity = ledgers.entity(key);
                        match entropy.next_u64() % 3 {
                            0 => {
                                let _ = entity
                                    .ask(Add {
                                        name: "swarm".into(),
                                        cents: 1,
                                    })
                                    .await;
                            }
                            1 if random => {
                                let _ = entity.ask(AddRandom).await;
                            }
                            _ => {
                                let _ = entity.ask(Total).await;
                            }
                        }
                        // Sleep past `idle_after` sometimes, so entities hibernate
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
            "sql-entity-commit-monotonic",
            "entity",
        )));
        invariants
    }
}
#[test]
fn sql_swarm_invariants_hold_across_seeds() {
    // #4: the safety core plus E3/E5 commit-monotonicity hold across seeds while
    // SQL entities write, hibernate, checkpoint, and rematerialize, with the
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
    // #7: the same seed yields a byte-identical event stream (entity events
    // included) even though the workload materializes real SQLite files,
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
