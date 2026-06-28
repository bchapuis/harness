//! Adversarial conformance tests for the `Clustered` tier (blob-store spec §8, §9).
//!
//! These probe the failure paths the happy-path cluster tests do not reach, and
//! they assert the **spec-mandated** outcome rather than the current behaviour, so
//! a gap surfaces as a failing test:
//!
//! - **B7 / §8 resurrection-by-rejoin.** The spec's §8 fault matrix explicitly
//!   names "a merely `unreachable` node returning with its disk, whose tombstone
//!   MUST NOT have been forgotten." A node partitioned across a `delete_namespace`
//!   keeps its blobs; on heal it must not resolve the deleted namespace anywhere.
//! - **§2 / §5.2 size bound.** `BlobConfig::max_blob_bytes` is the one lever the
//!   spec gives the tier to bound a blob ("an implementation SHOULD bound a blob's
//!   size"). A `put` past the bound must be rejected, not silently accepted.
//!
//! The harness mirrors `tests/clustered.rs` (SWIM gossip membership).

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use blob_store::BlobConfig;
use blob_store::BlobError;
use blob_store::BlobStore;
use blob_store::ClusteredBlobStore;
use blob_store::LocalBlobStore;
use blob_store::Namespace;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

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
        max_blob_bytes: 1 << 10, // 1 KiB — small, so a test can exceed it cheaply
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
    cell.lock()
        .unwrap()
        .take()
        .expect("future did not complete")
}

type Node = (NodeId, ClusteredBlobStore<SimCluster>);

fn cluster(sim: &Simulation) -> (SimNetwork, Vec<Node>, Vec<tempfile::TempDir>) {
    let net = SimNetwork::new(sim).with_gossip(swim(), DowningPolicy::Conservative);
    let systems: Vec<SimCluster> = [A, B, C].iter().map(|&n| net.join(n)).collect();
    sim.run_for(Duration::from_secs(2));

    let mut dirs = Vec::new();
    let stores = systems
        .into_iter()
        .map(|system| {
            let node = system.node();
            let dir = tempfile::tempdir().expect("tempdir");
            let local = LocalBlobStore::open(dir.path()).expect("open");
            dirs.push(dir);
            (node, ClusteredBlobStore::start(system, config(), local))
        })
        .collect();
    sim.run_for(Duration::from_secs(1));
    (net, stores, dirs)
}

#[test]
fn a_rejoining_unaware_holder_does_not_resurrect_a_deleted_namespace() {
    // Spec §8 / B7: delete a namespace while one owner is partitioned, then heal the
    // partition (the node returns with its disk — only `unreachable`, never downed,
    // under the conservative policy). The healed node MUST NOT resolve the deleted
    // namespace: a `get`/`has` issued *on* it must not serve the swept blob, and it
    // must not push a stale copy back. The tombstone must reach it (re-sync/gossip)
    // before it resumes serving (spec §5.3 "re-syncs the set from the anchor owners
    // on rejoin, before it resumes accepting StoreBlobs or reconciling ns").
    let sim = Simulation::new(7);
    let (net, stores, _dirs) = cluster(&sim);
    let store_of = |n: NodeId| stores.iter().find(|(id, _)| *id == n).map(|(_, s)| s.clone());

    let ns = Namespace::new(b"doomed-workspace".to_vec());
    let bytes = b"a block that outlives its namespace".to_vec();
    let id = drive(&sim, Duration::from_secs(5), {
        let (s, ns, bytes) = (store_of(A).unwrap(), ns.clone(), bytes.clone());
        async move { s.put(&ns, bytes).await }
    })
    .expect("put");

    // Let the straggler drain + reconcile place a copy on all R=3 owners, C included.
    sim.run_for(Duration::from_secs(3));
    assert!(
        store_of(C).unwrap().local().present(&ns, &id),
        "precondition: C holds a copy before the partition",
    );

    // Partition C away from {A, B}; delete the namespace from A. The fan-out reaches
    // A and B (and anchors there) but never C, which keeps its copy and stays
    // unaware of the tombstone.
    net.partition(&[A, B], &[C]);
    sim.run_for(Duration::from_secs(1));
    drive(&sim, Duration::from_secs(5), {
        let (s, ns) = (store_of(A).unwrap(), ns.clone());
        async move { s.delete_namespace(&ns).await }
    })
    .expect("delete acks at W anchors on the majority side");

    // Heal: C returns with its disk intact (conservative downing never downed it).
    net.heal();
    sim.run_for(Duration::from_secs(5)); // SWIM reconverges; reconcile runs on C

    // C must now resolve the deleted namespace nowhere. Today it does not know the
    // tombstone (no gossip/re-sync is wired), so it still serves its own copy — a
    // B7 resurrection.
    let got = drive(&sim, Duration::from_secs(5), {
        let (s, ns) = (store_of(C).unwrap(), ns.clone());
        async move { s.get(&ns, &id, None).await }
    });
    assert_eq!(
        got,
        Err(BlobError::Deleted(ns.clone())),
        "a healed, formerly-partitioned holder resurrected a deleted namespace (B7)",
    );

    let present = drive(&sim, Duration::from_secs(5), {
        let (s, ns) = (store_of(C).unwrap(), ns.clone());
        async move { s.has(&ns, &id).await }
    });
    assert_eq!(present, Ok(false), "has() on the healed node still reports the swept blob");
}

#[test]
fn a_put_past_the_size_bound_is_refused() {
    // Spec §2/§5.2: max_blob_bytes is the tier's one size lever. A blob past the
    // bound must be refused (today it is silently accepted, so the knob is inert).
    let sim = Simulation::new(8);
    let (_net, stores, _dirs) = cluster(&sim);
    let store = stores[0].1.clone();
    let ns = Namespace::new(b"oversized".to_vec());

    let oversized = vec![0u8; (1 << 10) + 1]; // one byte past the 1 KiB bound
    let result = drive(&sim, Duration::from_secs(5), {
        let (s, ns) = (store.clone(), ns.clone());
        async move { s.put(&ns, oversized).await }
    });
    assert!(
        matches!(result, Err(BlobError::Unavailable(_))),
        "a put past max_blob_bytes must be refused, got {result:?}",
    );
}
