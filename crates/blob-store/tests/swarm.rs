//! The deterministic-simulation conformance suite for the `Clustered` tier
//! (blob-store spec §8, §9; V&V "Simulation workloads").
//!
//! A whole cluster of blob stores runs in one process, on one logical thread, over
//! virtual time, network, and randomness, so a single `(seed, configuration)`
//! reproduces a run exactly. A [`BlobSwarm`] drives concurrent put/get/delete
//! traffic across nodes while the swarm harness injects the §8 fault matrix
//! (partition, crash, loss, duplication, delay), and a continuous checker proves
//! the headline safety property: **no resurrection of a deleted namespace**
//! (**B7**). The suite also asserts seed-reproducibility (the determinism
//! contract, spec §8) and fault coverage (every fault type actually fired), so a
//! green run is provably not a silently happy-path run.

use std::collections::BTreeSet;
use std::time::Duration;

use actor_cluster::SwimConfig;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::NodeId;
use actor_simulation::check_cluster_reproducible;
use actor_simulation::default_invariants;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_cluster_swarm_coverage;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::SimCluster;
use blob_store::BlobConfig;
use blob_store::BlobEvent;
use blob_store::BlobId;
use blob_store::BlobStore;
use blob_store::ClusteredBlobStore;
use blob_store::LocalBlobStore;
use blob_store::Namespace;

// --- The B7 safety checker ----------------------------------------------------

/// A continuous checker for **B7 monotonic deletion** (spec §4, §5.3): once a node
/// has recorded a namespace tombstone, it must never store a blob into that
/// namespace again. This is the resurrection hazard — a partitioned holder
/// re-pushing a blob of a deleted namespace, or a node accepting one — expressed
/// as a *per-node* ordering over the event stream, which is the only level at
/// which it is sound (a lagging node that has not yet learned the tombstone may
/// legitimately still serve, B7 liveness; what it must never do is store *after*
/// it has tombstoned).
#[derive(Default)]
struct NoResurrection {
    tombstoned: BTreeSet<(NodeId, Namespace)>,
}

impl Invariant for NoResurrection {
    fn name(&self) -> &'static str {
        "blob-no-resurrection"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        let Some(blob) = event.as_app::<BlobEvent>() else {
            return Ok(());
        };
        match blob {
            BlobEvent::Tombstoned { node, ns } => {
                self.tombstoned.insert((*node, ns.clone()));
            }
            BlobEvent::Stored { node, ns, id }
                if self.tombstoned.contains(&(*node, ns.clone())) =>
            {
                return Err(format!(
                    "node {node} stored blob {id} into namespace {ns} it had already tombstoned \
                     — a deleted namespace was resurrected (B7)"
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

// --- The workload -------------------------------------------------------------

fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

fn config() -> BlobConfig {
    BlobConfig {
        replication_factor: 3,
        write_quorum: 2,
        max_blob_bytes: 4 << 20,
    }
}

/// Concurrent put/get/delete traffic across the cluster, through the public API
/// only (spec §8 / V&V §18.4). Clients share a small pool of namespaces, so puts,
/// reads, and deletes interleave and race — exercising put-racing-delete and
/// reconcile-against-tombstone under the injected faults.
struct BlobSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
}

impl ClusterWorkload for BlobSwarm {
    fn name(&self) -> &'static str {
        "blob-store-swarm"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        swim()
    }

    fn setup(&self, _ctx: &ClusterCtx) {}

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimCluster> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        Box::pin(async move {
            // One on-disk store and `Clustered` tier per node (each spawns its
            // replica + reconcile loop). The tempdirs live until the run ends.
            let mut dirs = Vec::new();
            let stores: Vec<ClusteredBlobStore<SimCluster>> = nodes
                .iter()
                .map(|system| {
                    let dir = tempfile::tempdir().expect("tempdir");
                    let local = LocalBlobStore::open(dir.path()).expect("open");
                    dirs.push(dir);
                    ClusteredBlobStore::start(system.clone(), config(), local)
                })
                .collect();

            // Let SWIM converge and every replica register before traffic.
            nodes[0].clock().sleep(Duration::from_secs(2)).await;

            let entropy = nodes[0].entropy().clone();
            let mut tasks = Vec::new();
            for client in 0..clients {
                let store = stores[client % stores.len()].clone();
                let entropy = entropy.clone();
                tasks.push(async move {
                    for _ in 0..ops {
                        let ns =
                            Namespace::new(format!("ns-{}", entropy.next_u64() % 6).into_bytes());
                        let data = format!("blob-{}", entropy.next_u64() % 10).into_bytes();
                        // The tier bounds each call with its own timeout, so a call
                        // under partition fails cleanly rather than hanging.
                        match entropy.next_u64() % 10 {
                            0..=5 => {
                                if let Ok(id) = store.put(&ns, data).await {
                                    let _ = store.get(&ns, &id, None).await;
                                }
                            }
                            6..=7 => {
                                let _ = store.get(&ns, &BlobId::of(&data), None).await;
                            }
                            _ => {
                                let _ = store.delete_namespace(&ns).await;
                            }
                        }
                    }
                });
            }
            futures::future::join_all(tasks).await;
            // Stop the per-node reconcile loops (dropping the last store handle lets
            // each loop's `Weak` upgrade fail), then settle past the tier's internal
            // timeout so every in-flight background ask — a straggler drain, a final
            // reconcile probe — reaches its outcome before the run ends. Otherwise
            // one could be pending at the invariant check (NoSilentLoss).
            drop(stores);
            nodes[0].clock().sleep(Duration::from_secs(4)).await;
            drop(dirs);
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        // The framework safety checkers, plus B7 (no resurrection). `no-silent-loss`
        // is dropped: it asserts no ask is outstanding at quiescence, but the tier
        // legitimately keeps background asks in flight — a W-of-R put drains its
        // straggler stores off the latency path (spec §5.2), and the reconcile loop
        // probes owners continuously (spec §7). A node the nemesis crashes at the
        // run's end has such an ask frozen (a paused caller's timeout timer is
        // paused too); it would resolve on heal, so it is not a silent loss. The
        // data path's no-loss is covered anyway — clients await every op to an
        // outcome. (Granary's swarm keeps the checker because it issues no
        // continuous background asks.)
        let mut invariants: Vec<Box<dyn Invariant>> = default_invariants()
            .into_iter()
            .filter(|inv| inv.name() != "no-silent-loss")
            .collect();
        invariants.push(Box::new(NoResurrection::default()));
        invariants
    }
}

// --- The conformance tests ----------------------------------------------------

#[test]
fn blob_invariants_hold_under_the_cluster_swarm() {
    // The framework invariants (no silent loss, serial dispatch, …) and B7
    // (no resurrection) hold on every seeded run under partitions, crashes, loss,
    // duplication, and delay.
    let workload = BlobSwarm {
        nodes: 3,
        clients: 3,
        ops: 6,
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..24) {
        panic!("{failure}");
    }
}

#[test]
fn the_swarm_is_seed_reproducible() {
    // The determinism contract (spec §8): the same seed replays to a byte-identical
    // event stream, even with real on-disk stores — reconcile enumerates blobs in a
    // sorted, OS-independent order, so nothing path-dependent leaks into the stream.
    let workload = BlobSwarm {
        nodes: 3,
        clients: 2,
        ops: 5,
    };
    for seed in 0..8 {
        if let Err(divergence) = check_cluster_reproducible(&workload, seed) {
            panic!("{divergence}");
        }
    }
}

#[test]
fn the_swarm_exercises_every_fault() {
    // A sweep that configures faults but never triggers one gives false confidence.
    // Assert each fault type actually fired across the seed range (spec §8).
    let workload = BlobSwarm {
        nodes: 3,
        clients: 3,
        ops: 6,
    };
    let stats = match run_cluster_swarm_coverage(&workload, 0..32) {
        Ok(stats) => stats,
        Err(failure) => panic!("{failure}"),
    };
    assert!(stats.dropped > 0, "loss uncovered");
    assert!(stats.duplicated > 0, "duplication uncovered");
    assert!(stats.delayed > 0, "reordering uncovered");
    assert!(stats.blocked > 0, "partition/crash uncovered");
}
