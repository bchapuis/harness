//! The reconcile loop across a real multi-node cluster (blob-store spec §7, §5.3,
//! §8): re-replication after a departure (**B6**) and tombstone reclamation
//! (**B7** liveness).
//!
//! The harness mirrors `tests/clustered.rs` (SWIM gossip membership), sized to
//! whatever node set a test needs.

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
use blob_store::BlobId;
use blob_store::BlobStore;
use blob_store::ClusteredBlobStore;
use blob_store::LocalBlobStore;
use blob_store::Namespace;
use blob_store::placement;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);
const D: NodeId = NodeId::new(4);

fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

fn config() -> BlobConfig {
    // R = 3, W = 3: a put stores on all three owners synchronously, so the test
    // controls exactly which nodes hold a blob before a fault.
    BlobConfig {
        replication_factor: 3,
        write_quorum: 3,
        max_blob_bytes: 4 << 20,
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

/// A node's id paired with its `Clustered` tier handle.
type Node = (NodeId, ClusteredBlobStore<SimCluster>);

/// Bring up a gossip cluster on `nodes`, each hosting the `Clustered` tier (which
/// spawns its reconcile loop). Returns the network, the per-node stores keyed by
/// node, and the tempdirs.
fn cluster(sim: &Simulation, nodes: &[NodeId]) -> (SimNetwork, Vec<Node>, Vec<tempfile::TempDir>) {
    let net = SimNetwork::new(sim).with_gossip(swim(), DowningPolicy::Conservative);
    let systems: Vec<SimCluster> = nodes.iter().map(|&n| net.join(n)).collect();
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
fn a_departed_owner_is_re_replicated() {
    // B6: with 4 nodes and R = 3, a blob lives on 3 of them; the 4th is excluded.
    // Crash one owner: under the new 3-node serving set the previously-excluded
    // node becomes an owner but lacks the blob. The reconcile loop on a surviving
    // holder re-pushes it, restoring R copies across the serving set.
    let sim = Simulation::new(1);
    let nodes = [A, B, C, D];
    let (net, stores, _dirs) = cluster(&sim, &nodes);
    let store_of = |n: NodeId| {
        stores
            .iter()
            .find(|(id, _)| *id == n)
            .map(|(_, s)| s.clone())
    };

    let ns = Namespace::new(b"workspace".to_vec());
    let bytes = b"a replicated block".to_vec();
    let id = drive(&sim, Duration::from_secs(5), {
        let (s, ns, bytes) = (store_of(A).unwrap(), ns.clone(), bytes.clone());
        async move { s.put(&ns, bytes).await }
    })
    .expect("put acks on all R owners");

    // Compute the owners the same way every node does (B5), then crash one and
    // identify the node that was excluded (and so lacks the blob).
    let owners = placement::owners(&nodes, &ns, &id, 3);
    let excluded = *nodes
        .iter()
        .find(|n| !owners.contains(n))
        .expect("one node is excluded");
    assert!(
        !store_of(excluded).unwrap().local().present(&ns, &id),
        "the non-owner starts without the blob",
    );

    // Crash an owner; the excluded node is now an owner of an under-replicated blob.
    net.crash(owners[0]);
    sim.run_for(Duration::from_secs(4)); // SWIM converges, reconcile re-replicates

    assert!(
        store_of(excluded).unwrap().local().present(&ns, &id),
        "reconcile restored the blob onto the new owner (B6)",
    );

    // Every surviving serving node now holds a verifying copy.
    for node in nodes.iter().filter(|&&n| n != owners[0]) {
        let bytes = bytes.clone();
        let got = drive(&sim, Duration::from_secs(5), {
            let (s, ns) = (store_of(*node).unwrap(), ns.clone());
            async move { s.get(&ns, &id, None).await }
        });
        assert_eq!(
            got,
            Ok(bytes),
            "node {node} serves the verified blob after repair"
        );
    }
}

#[test]
fn rebalancing_never_deletes() {
    // B6 (additive): reconcile only restores copies, it never drops them. A blob
    // put on its owners stays present across many reconcile passes with no
    // membership change — rebalancing is additive and tolerates over-replication.
    let sim = Simulation::new(2);
    let nodes = [A, B, C, D];
    let (_net, stores, _dirs) = cluster(&sim, &nodes);
    let store_of = |n: NodeId| {
        stores
            .iter()
            .find(|(id, _)| *id == n)
            .map(|(_, s)| s.clone())
    };

    let ns = Namespace::new(b"stable".to_vec());
    let bytes = b"untouched by rebalancing".to_vec();
    let id = drive(&sim, Duration::from_secs(5), {
        let (s, ns, bytes) = (store_of(A).unwrap(), ns.clone(), bytes.clone());
        async move { s.put(&ns, bytes).await }
    })
    .expect("put");

    let owners = placement::owners(&nodes, &ns, &id, 3);
    sim.run_for(Duration::from_secs(3)); // many reconcile passes, no membership change

    for owner in &owners {
        assert!(
            store_of(*owner).unwrap().local().present(&ns, &id),
            "owner {owner} still holds the blob after repeated reconcile passes",
        );
    }
}

#[test]
fn a_delete_with_all_nodes_swept_releases_the_anchor() {
    // B7 liveness: when delete_namespace completes with every serving node swept,
    // the anchor's per-namespace sweep bookkeeping becomes reclaimable, and the
    // reconcile loop releases it — bounding tombstone retention without a timer.
    // The tiny awareness flag is retained, so the namespace still resolves nowhere.
    let sim = Simulation::new(3);
    let nodes = [A, B, C];
    let (_net, stores, _dirs) = cluster(&sim, &nodes);
    let anchor = stores[0].1.clone(); // node A; with R=3 it is a tombstone owner

    let ns = Namespace::new(b"reclaim-me".to_vec());
    drive(&sim, Duration::from_secs(5), {
        let (s, ns, bytes) = (anchor.clone(), ns.clone(), b"x".to_vec());
        async move {
            s.put(&ns, bytes).await.expect("put");
            s.delete_namespace(&ns).await
        }
    })
    .expect("delete acked");
    sim.run_for(Duration::from_secs(1)); // a reconcile pass runs reclamation

    // The delete anchored cluster-wide (the awareness flag is set), and because all
    // three serving nodes swept synchronously during the fan-out, the anchor's
    // per-namespace sweep bookkeeping was reclaimable and the reconcile loop
    // released it — bounding tombstone retention without a timer (B7 liveness). The
    // tiny awareness flag is retained, so the namespace still resolves nowhere.
    assert!(
        anchor.tombstones().contains(&ns),
        "the namespace is tombstoned (the delete anchored and propagated)",
    );
    assert!(
        anchor.anchors().tracked().is_empty(),
        "reconcile released the anchor's sweep bookkeeping once every member swept",
    );

    // The namespace still resolves nowhere from the anchor node.
    let got = drive(&sim, Duration::from_secs(5), {
        let (s, ns) = (anchor.clone(), ns.clone());
        async move { s.get(&ns, &BlobId::of(b"x"), None).await }
    });
    assert!(
        got.is_err(),
        "a reclaimed-bookkeeping namespace still resolves nowhere"
    );
}
