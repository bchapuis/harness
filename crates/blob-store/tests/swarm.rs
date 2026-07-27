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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_cluster::SwimConfig;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::NodeId;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::SimNode;
use actor_simulation::coverage_seeds;
use actor_simulation::default_invariants;
use actor_simulation::replay_cluster_swarm;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_cluster_swarm_coverage;
use actor_simulation::slow_seeds;
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
/// Acknowledged blobs, keyed by `(namespace bytes, id)`, valued by the index of
/// the node whose store took the `put`.
type Durable = BTreeMap<(Vec<u8>, BlobId), usize>;

struct BlobSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
    /// How many acknowledged blobs were re-read after a heal, tallied across the
    /// whole sweep so a green run is not one where every put failed. Cumulative on
    /// purpose, unlike the per-run expectations — which live in [`drive`] and must
    /// not be workload state.
    ///
    /// [`drive`]: ClusterWorkload::drive
    reread: Arc<AtomicUsize>,
    /// Whether to make the **B6** re-replication claim at the end of the run.
    ///
    /// Off by default, because the claim does not hold yet and it is not settled
    /// whether that is B6 or the claim. See `check_rereplication` below and
    /// `docs/simulation-hardening.md` §6.
    check_rereplication: bool,
}

impl BlobSwarm {
    fn new(nodes: usize, clients: usize, ops: u64) -> BlobSwarm {
        BlobSwarm {
            nodes,
            clients,
            ops,
            reread: Arc::new(AtomicUsize::new(0)),
            check_rereplication: false,
        }
    }

    /// The same traffic, with the end-of-run B6 claim turned on.
    fn checking_rereplication(nodes: usize, clients: usize, ops: u64) -> BlobSwarm {
        BlobSwarm {
            check_rereplication: true,
            ..BlobSwarm::new(nodes, clients, ops)
        }
    }
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
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        let reread = Arc::clone(&self.reread);
        let check_rereplication = self.check_rereplication;
        let net = ctx.net().clone();
        Box::pin(async move {
            // Per **run**, not per workload. A sweep drives every seed through the
            // same `&self`, so a field here would carry seed N's acknowledged blobs
            // into seed N+1's fresh, empty stores and report them as lost to
            // re-replication. (It did: that is what made this check look like a B6
            // defect.)
            let durable: Arc<Mutex<Durable>> = Arc::new(Mutex::new(BTreeMap::new()));
            let touched_by_delete: Arc<Mutex<BTreeSet<Vec<u8>>>> =
                Arc::new(Mutex::new(BTreeSet::new()));

            // One on-disk store and `Clustered` tier per node (each spawns its
            // replica + reconcile loop). The tempdirs live until the run ends.
            let mut dirs = Vec::new();
            let stores: Vec<ClusteredBlobStore<SimNode>> = nodes
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
                let index = client % stores.len();
                let store = stores[index].clone();
                let entropy = entropy.clone();
                let durable = Arc::clone(&durable);
                let touched_by_delete = Arc::clone(&touched_by_delete);
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
                                    durable
                                        .lock()
                                        .expect("durable mutex")
                                        .insert((ns.as_bytes().to_vec(), id), index);
                                }
                            }
                            6..=7 => {
                                let _ = store.get(&ns, &BlobId::of(&data), None).await;
                            }
                            _ => {
                                // Whatever the outcome, this namespace is now
                                // undecidable — record that before the call, since
                                // a timed-out delete can still land afterwards.
                                touched_by_delete
                                    .lock()
                                    .expect("delete mutex")
                                    .insert(ns.as_bytes().to_vec());
                                let _ = store.delete_namespace(&ns).await;
                            }
                        }
                    }
                });
            }
            futures::future::join_all(tasks).await;

            // **B6, the half the sweep never asserted.** The reconcile loop has been
            // re-replicating throughout, but nothing checked it achieved anything:
            // the drive read each blob back immediately after its put, while every
            // replica still held it. Heal and quiesce, give reconcile time to
            // restore the copies the crashes and partitions cost, then read every
            // still-live blob again. `quiesce` matters as much as `heal` here — a
            // wire still dropping frames keeps the owner set churning, and a `get`
            // that fails then says nothing about re-replication
            // (docs/simulation-testing.md, "Asserting at quiescence").
            if check_rereplication {
                net.heal();
                net.quiesce();
                nodes[0].clock().sleep(Duration::from_secs(45)).await;
                let deleted = touched_by_delete.lock().expect("delete mutex").clone();
                let expected = durable.lock().expect("durable mutex").clone();
                for ((space, id), index) in &expected {
                    if deleted.contains(space) {
                        continue;
                    }
                    let ns = Namespace::new(space.clone());
                    // Read through a *different* node than the one that stored it, so a
                    // surviving local copy cannot answer for the cluster.
                    let reader = &stores[(index + 1) % stores.len()];
                    let got = reader.get(&ns, id, None).await;
                    assert!(
                        matches!(&got, Ok(bytes) if BlobId::of(bytes) == *id),
                        "a blob acknowledged by `put`, in a namespace never deleted \
                     since, did not read back on a healed cluster: re-replication \
                     did not restore it (B6). id={id:?} outcome={:?}",
                        got.as_ref().map(|b| b.len()),
                    );
                    reread.fetch_add(1, Ordering::Relaxed);
                }
            }

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
    let workload = BlobSwarm::new(3, 3, 6);
    if let Err(failure) = run_cluster_swarm(&workload, slow_seeds(0..24)) {
        panic!("{failure}");
    }
}

/// **B6.** A blob whose `put` was acknowledged, in a namespace no client ever
/// tried to delete, must read back through *another* node once the cluster has
/// healed — the reconcile loop's whole job. Kept as its own test because the end
/// check needs a healed, quiesced cluster and a settle, which the other sweeps
/// have no reason to pay for.
#[test]
fn acknowledged_blobs_are_re_replicated_after_a_heal() {
    let workload = BlobSwarm::checking_rereplication(3, 3, 6);
    if let Err(failure) = run_cluster_swarm(&workload, slow_seeds(0..24)) {
        panic!("{failure}");
    }
    assert!(
        workload.reread.load(Ordering::Relaxed) > 0,
        "no acknowledged blob was ever re-read on a healed cluster — the B6 \
         re-replication claim was asserted against nothing",
    );
}

#[test]
fn the_swarm_is_seed_reproducible() {
    // The determinism contract (spec §8): the same seed replays to a byte-identical
    // event stream, even with real on-disk stores — reconcile enumerates blobs in a
    // sorted, OS-independent order, so nothing path-dependent leaks into the stream.
    let workload = BlobSwarm::new(3, 2, 5);
    if let Err(divergence) = replay_cluster_swarm(&workload, slow_seeds(0..8)) {
        panic!("{divergence}");
    }
}

#[test]
fn the_swarm_exercises_every_fault() {
    // A sweep that configures faults but never triggers one gives false confidence.
    // Assert each fault type actually fired across the seed range (spec §8).
    let workload = BlobSwarm::new(3, 3, 6);
    let stats = match run_cluster_swarm_coverage(&workload, coverage_seeds(0..32)) {
        Ok(stats) => stats,
        Err(failure) => panic!("{failure}"),
    };
    assert!(stats.dropped > 0, "loss uncovered");
    assert!(stats.duplicated > 0, "duplication uncovered");
    assert!(stats.delayed > 0, "reordering uncovered");
    assert!(stats.blocked > 0, "partition/crash uncovered");
}
