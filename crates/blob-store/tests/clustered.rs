//! The `Clustered` tier across a real multi-node cluster, under deterministic
//! simulation (blob-store spec §5.2, §5.3, §8).
//!
//! These exercise the cluster data and delete paths the single-node `Local` tier
//! cannot reach: a `put` replicates to its `R` owners and acks at `W` (**B3**); a
//! blob stored on one node is `get`-able with the same verified bytes from *any*
//! node (**B1**, **B5**); equal content converges to one id (**B2**); and a
//! `delete_namespace` fans a tombstone cluster-wide so the namespace then resolves
//! nowhere and a `put` back into it is refused (**B7**). The reconcile loop (spec
//! §7) and the full fault swarm (spec §8) land in their own phases; this is the
//! happy-path-plus-delete proof that the tier composes over the actor framework.
//!
//! The harness mirrors granary's `tests/clustered_grains.rs`, swapping the
//! leader/raft network for SWIM gossip membership — a blob store needs the serving
//! set, not consensus (spec §1, §4).

use std::fs;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::NodeId;
use actor_core::Spawner;
use blob_store::placement;
use blob_store::BlobConfig;
use blob_store::BlobError;
use blob_store::BlobId;
use blob_store::BlobStore;
use blob_store::ClusteredBlobStore;
use blob_store::LocalBlobStore;
use blob_store::Namespace;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;

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
        max_blob_bytes: 4 << 20,
    }
}

/// Drive an async call to completion under the perpetually-running cluster loops
/// (the pattern from granary's cluster harness — SWIM never quiesces, so the run
/// is time-bounded rather than quiescence-driven).
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

/// Bring up a 3-node gossip cluster, each node hosting the `Clustered` tier over
/// its own on-disk store. Returns the network (for fault injection), the per-node
/// stores, and the tempdirs (kept alive for the test's duration).
fn cluster(
    sim: &Simulation,
) -> (
    SimNetwork,
    Vec<ClusteredBlobStore<SimCluster>>,
    Vec<tempfile::TempDir>,
) {
    let net = SimNetwork::new(sim).with_gossip(swim(), DowningPolicy::Conservative);
    let systems: Vec<SimCluster> = [A, B, C].iter().map(|&n| net.join(n)).collect();
    sim.run_for(Duration::from_secs(2)); // let SWIM converge on the serving set

    let mut dirs = Vec::new();
    let stores: Vec<ClusteredBlobStore<SimCluster>> = systems
        .into_iter()
        .map(|system| {
            let dir = tempfile::tempdir().expect("tempdir");
            let local = LocalBlobStore::open(dir.path()).expect("open");
            dirs.push(dir);
            ClusteredBlobStore::start(system, config(), local)
        })
        .collect();
    sim.run_for(Duration::from_secs(1)); // let every node register its replica
    (net, stores, dirs)
}

#[test]
fn a_blob_put_on_one_node_is_gettable_from_every_node() {
    // B1/B3/B5: a put replicates to its R owners and acks at W; the blob is then
    // readable, verified, from any node — owner or not — because every node
    // computes the same owners and a get widens to find it.
    let sim = Simulation::new(1);
    let (_net, stores, _dirs) = cluster(&sim);
    let ns = Namespace::new(b"workspace-1".to_vec());
    let bytes = b"a replicated block".to_vec();

    let id = drive(&sim, Duration::from_secs(5), {
        let store = stores[0].clone();
        let ns = ns.clone();
        let bytes = bytes.clone();
        async move { store.put(&ns, bytes).await }
    })
    .expect("put acked at W");
    assert_eq!(id, BlobId::of(&bytes));

    for (index, store) in stores.iter().enumerate() {
        let got = drive(&sim, Duration::from_secs(5), {
            let store = store.clone();
            let ns = ns.clone();
            async move { store.get(&ns, &id, None).await }
        });
        assert_eq!(got, Ok(bytes.clone()), "node {index} reads the verified blob");

        let present = drive(&sim, Duration::from_secs(5), {
            let store = store.clone();
            let ns = ns.clone();
            async move { store.has(&ns, &id).await }
        });
        assert_eq!(present, Ok(true), "node {index} sees W durable copies");
    }
}

#[test]
fn equal_content_converges_to_one_id_across_nodes() {
    // B2: storing the same bytes from two different nodes under one namespace
    // yields one id (content addressing), and both reads return it.
    let sim = Simulation::new(2);
    let (_net, stores, _dirs) = cluster(&sim);
    let ns = Namespace::new(b"workspace-2".to_vec());
    let bytes = b"dedup across nodes".to_vec();

    let from_a = drive(&sim, Duration::from_secs(5), {
        let (store, ns, bytes) = (stores[0].clone(), ns.clone(), bytes.clone());
        async move { store.put(&ns, bytes).await }
    })
    .expect("put on A");
    let from_b = drive(&sim, Duration::from_secs(5), {
        let (store, ns, bytes) = (stores[1].clone(), ns.clone(), bytes.clone());
        async move { store.put(&ns, bytes).await }
    })
    .expect("put on B");
    assert_eq!(from_a, from_b, "equal content is one id regardless of writer");
}

#[test]
fn a_tampered_copy_falls_through_to_a_good_owner() {
    // B1: a get verifies after transfer. If the highest-ranked owner serves
    // corrupt bytes (here, on-disk bit-rot), the get falls through to the next
    // owner and returns the verified blob — corruption is never returned as valid.
    let sim = Simulation::new(4);
    let nodes = [A, B, C];
    let (_net, stores, dirs) = cluster(&sim);

    let ns = Namespace::new(b"workspace-bitrot".to_vec());
    let bytes = b"survive a corrupt replica".to_vec();
    let id = drive(&sim, Duration::from_secs(5), {
        let (s, ns, bytes) = (stores[0].clone(), ns.clone(), bytes.clone());
        async move { s.put(&ns, bytes).await }
    })
    .expect("put");

    // Let the put's straggler drain and reconcile place a copy on every owner.
    sim.run_for(Duration::from_secs(2));

    // Corrupt the highest-ranked owner's on-disk copy behind the store's back, so
    // a get asks it first and must fall through.
    let owners = placement::owners(&nodes, &ns, &id, 3);
    let victim = owners[0];
    let victim_dir = &dirs[nodes.iter().position(|n| *n == victim).unwrap()];
    let path = victim_dir
        .path()
        .join("blobs")
        .join(ns.to_string())
        .join(format!("{:02x}", id.as_bytes()[0]))
        .join(id.to_string());
    fs::write(&path, b"corrupted on disk, a different length").expect("tamper");

    // Every node still reads the verified blob — the get fell through the bad owner.
    for store in &stores {
        let got = drive(&sim, Duration::from_secs(5), {
            let (s, ns) = (store.clone(), ns.clone());
            async move { s.get(&ns, &id, None).await }
        });
        assert_eq!(got, Ok(bytes.clone()), "the get fell through to a good owner (B1)");
    }
}

#[test]
fn a_deleted_namespace_resolves_nowhere_and_refuses_puts() {
    // B7: delete_namespace fans a tombstone cluster-wide; afterwards every node
    // returns Deleted for a get and refuses a put back into the namespace.
    let sim = Simulation::new(3);
    let (_net, stores, _dirs) = cluster(&sim);
    let ns = Namespace::new(b"workspace-3".to_vec());
    let bytes = b"to be reclaimed".to_vec();

    let id = drive(&sim, Duration::from_secs(5), {
        let (store, ns, bytes) = (stores[0].clone(), ns.clone(), bytes.clone());
        async move { store.put(&ns, bytes).await }
    })
    .expect("put");

    drive(&sim, Duration::from_secs(5), {
        let (store, ns) = (stores[0].clone(), ns.clone());
        async move { store.delete_namespace(&ns).await }
    })
    .expect("delete acked at W anchors");

    // Give the fan-out a moment to reach every node's replica.
    sim.run_for(Duration::from_secs(1));

    for (index, store) in stores.iter().enumerate() {
        let got = drive(&sim, Duration::from_secs(5), {
            let (store, ns) = (store.clone(), ns.clone());
            async move { store.get(&ns, &id, None).await }
        });
        assert_eq!(
            got,
            Err(BlobError::Deleted(ns.clone())),
            "node {index} resolves a deleted namespace nowhere",
        );

        let refused = drive(&sim, Duration::from_secs(5), {
            let (store, ns) = (store.clone(), ns.clone());
            async move { store.put(&ns, b"resurrect?".to_vec()).await }
        });
        assert_eq!(
            refused,
            Err(BlobError::Deleted(ns.clone())),
            "node {index} refuses a put into a deleted namespace",
        );
    }
}
