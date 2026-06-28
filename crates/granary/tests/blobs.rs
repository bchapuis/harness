//! The grain-native content-addressed facet, end-to-end under deterministic
//! simulation (durable-workspace design; granary §14).
//!
//! A grain reaches its colocated, immutable blob area through
//! [`GrainCtx::blobs`](granary::GrainCtx::blobs): it stores bulk bytes by content
//! and references them by [`BlobId`] from its small foldable state. These tests
//! drive a `BlobGrain` through the public command API on both tiers:
//!
//! - the single-node `Local` tier covers the round-trip and verification (B1),
//!   idempotent/dedup'd put (B2), survival across hibernation, the mark-from-roots
//!   GC, and grain-scoped destroy;
//! - the clustered `Quorum` tier covers the wire path — a W-of-R put and a verified
//!   read fanned over the shard's replicas, callable from any node, surviving the
//!   loss of a minority of replicas.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::Recorder;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use granary::BlobId;
use granary::FileGrainStore;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainEvent;
use granary::GrainHandler;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::GranarySystem;
use serde::Deserialize;
use serde::Serialize;

// --- A grain that stores bytes in its colocated blob area ---------------------
//
// Generic over the system (like `tenancy::Directory`), so the same grain hosts on
// the `Local` and `Quorum` tiers. Its state is just the ids it has stored — the
// small foldable metadata that references the bulk bytes living in `ctx.blobs()`.

struct BlobGrain<S>(std::marker::PhantomData<fn() -> S>);

impl<S> Default for BlobGrain<S> {
    fn default() -> Self {
        BlobGrain(std::marker::PhantomData)
    }
}

#[derive(Default, Serialize, Deserialize)]
struct Stored {
    ids: Vec<BlobId>,
}

#[derive(Serialize, Deserialize)]
enum Recorded {
    Put(BlobId),
}

impl<S: GranarySystem> Grain for BlobGrain<S> {
    type System = S;
    type State = Stored;
    type Event = Recorded;
    const GRAIN_TYPE: &'static str = "test.BlobGrain";

    fn apply(state: &mut Stored, event: &Recorded) {
        match event {
            Recorded::Put(id) => state.ids.push(*id),
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Put>();
        r.accept::<Fetch>();
        r.accept::<Exists>();
        r.accept::<KeepOnly>();
        r.accept::<Destroy>();
    }
}

/// Store bytes; reply with their content id (or a durability failure as a string).
#[derive(Clone, Serialize, Deserialize)]
struct Put(Vec<u8>);
impl Message for Put {
    type Reply = Result<BlobId, String>;
    const MANIFEST: Manifest = Manifest::new("test.blob.Put");
}
impl<S: GranarySystem> GrainHandler<Put> for BlobGrain<S> {
    async fn handle(
        &self,
        _state: &Stored,
        msg: Put,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Recorded>, Result<BlobId, String>) {
        // The bulk bytes go to the blob area (durable on a quorum) BEFORE the id is
        // journaled, so the reference is never durable ahead of the bytes.
        match ctx.blobs().put(msg.0).await {
            Ok(id) => (vec![Recorded::Put(id)], Ok(id)),
            Err(e) => (vec![], Err(e.to_string())),
        }
    }
}

/// Fetch a blob (optionally a `[start, end)` byte range); reply with the bytes.
#[derive(Clone, Serialize, Deserialize)]
struct Fetch {
    id: BlobId,
    range: Option<(u64, u64)>,
}
impl Message for Fetch {
    type Reply = Result<Vec<u8>, String>;
    const MANIFEST: Manifest = Manifest::new("test.blob.Fetch");
}
impl<S: GranarySystem> GrainHandler<Fetch> for BlobGrain<S> {
    async fn handle(
        &self,
        _state: &Stored,
        msg: Fetch,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Recorded>, Result<Vec<u8>, String>) {
        let range = msg.range.map(|(s, e)| s..e);
        (
            vec![],
            ctx.blobs()
                .get(msg.id, range)
                .await
                .map_err(|e| e.to_string()),
        )
    }
}

/// Whether a blob is present.
#[derive(Clone, Serialize, Deserialize)]
struct Exists(BlobId);
impl Message for Exists {
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("test.blob.Exists");
}
impl<S: GranarySystem> GrainHandler<Exists> for BlobGrain<S> {
    async fn handle(
        &self,
        _state: &Stored,
        msg: Exists,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Recorded>, bool) {
        (vec![], ctx.blobs().has(msg.0).await.unwrap_or(false))
    }
}

/// Keep only the listed blobs, sweeping the rest (the mark-from-roots GC).
#[derive(Clone, Serialize, Deserialize)]
struct KeepOnly(Vec<BlobId>);
impl Message for KeepOnly {
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("test.blob.KeepOnly");
}
impl<S: GranarySystem> GrainHandler<KeepOnly> for BlobGrain<S> {
    async fn handle(
        &self,
        _state: &Stored,
        msg: KeepOnly,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Recorded>, bool) {
        let live: BTreeSet<BlobId> = msg.0.into_iter().collect();
        ctx.blobs().gc(&live).await;
        (vec![], true)
    }
}

/// Drop the grain's whole blob area (destroy).
#[derive(Clone, Serialize, Deserialize)]
struct Destroy;
impl Message for Destroy {
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("test.blob.Destroy");
}
impl<S: GranarySystem> GrainHandler<Destroy> for BlobGrain<S> {
    async fn handle(
        &self,
        _state: &Stored,
        _msg: Destroy,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Recorded>, bool) {
        ctx.blobs().destroy().await;
        (vec![], true)
    }
}

// --- Local tier ---------------------------------------------------------------

fn local() -> (Simulation, SimSystem) {
    let sim = Simulation::new(7);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    (sim, system)
}

#[test]
fn put_get_round_trips_and_verifies() {
    let (sim, system) = local();
    let blobs = system.granary::<BlobGrain<SimSystem>>(GranaryConfig::default());
    let g = blobs.grain("ws/0");
    sim.block_on(async move {
        let bytes = b"a bounded block of workspace bytes".to_vec();
        let id = g.ask(Put(bytes.clone())).await.unwrap().unwrap();
        // The id is the content hash (B1 holds by construction).
        assert_eq!(id, BlobId::of(&bytes));
        // The whole blob round-trips, verified against its id.
        assert_eq!(
            g.ask(Fetch { id, range: None }).await.unwrap(),
            Ok(bytes.clone())
        );
        // A ranged read slices the verified whole blob.
        assert_eq!(
            g.ask(Fetch {
                id,
                range: Some((2, 8))
            })
            .await
            .unwrap(),
            Ok(bytes[2..8].to_vec())
        );
        assert!(g.ask(Exists(id)).await.unwrap());
    });
}

#[test]
fn putting_the_same_bytes_twice_yields_one_id() {
    let (sim, system) = local();
    let blobs = system.granary::<BlobGrain<SimSystem>>(GranaryConfig::default());
    let g = blobs.grain("ws/0");
    sim.block_on(async move {
        let id1 = g.ask(Put(b"dup".to_vec())).await.unwrap().unwrap();
        let id2 = g.ask(Put(b"dup".to_vec())).await.unwrap().unwrap();
        assert_eq!(id1, id2, "equal content addresses one blob (B2)");
    });
}

#[test]
fn a_missing_blob_is_an_error_not_wrong_bytes() {
    let (sim, system) = local();
    let blobs = system.granary::<BlobGrain<SimSystem>>(GranaryConfig::default());
    let g = blobs.grain("ws/0");
    sim.block_on(async move {
        let absent = BlobId::of(b"never stored");
        assert!(
            g.ask(Fetch {
                id: absent,
                range: None
            })
            .await
            .unwrap()
            .is_err()
        );
        assert!(!g.ask(Exists(absent)).await.unwrap());
    });
}

#[test]
fn gc_drops_unreferenced_and_keeps_referenced() {
    let (sim, system) = local();
    let blobs = system.granary::<BlobGrain<SimSystem>>(GranaryConfig::default());
    let g = blobs.grain("ws/0");
    sim.block_on(async move {
        let a = g.ask(Put(b"keep".to_vec())).await.unwrap().unwrap();
        let b = g.ask(Put(b"drop".to_vec())).await.unwrap().unwrap();
        assert!(g.ask(KeepOnly(vec![a])).await.unwrap());
        assert!(g.ask(Exists(a)).await.unwrap(), "the live blob survives");
        assert!(!g.ask(Exists(b)).await.unwrap(), "the orphan is swept");
    });
}

#[test]
fn destroy_drops_the_whole_area() {
    let (sim, system) = local();
    let blobs = system.granary::<BlobGrain<SimSystem>>(GranaryConfig::default());
    let g = blobs.grain("ws/0");
    sim.block_on(async move {
        let a = g.ask(Put(b"a".to_vec())).await.unwrap().unwrap();
        let b = g.ask(Put(b"b".to_vec())).await.unwrap().unwrap();
        assert!(g.ask(Destroy).await.unwrap());
        assert!(!g.ask(Exists(a)).await.unwrap());
        assert!(!g.ask(Exists(b)).await.unwrap());
    });
}

#[test]
fn blobs_survive_hibernation() {
    let sim = Simulation::new(7);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let blobs = system.granary::<BlobGrain<SimSystem>>(GranaryConfig {
        idle_after: Duration::from_millis(10),
        snapshot_every: 1,
        ..GranaryConfig::default()
    });

    let bytes = b"survives the eviction".to_vec();
    let id = sim.block_on({
        let g = blobs.grain("ws/0");
        let bytes = bytes.clone();
        async move { g.ask(Put(bytes)).await.unwrap().unwrap() }
    });

    // Drive past the idle window: the grain snapshots its metadata and hibernates.
    sim.run();
    assert!(
        recorder.events().iter().any(|e| matches!(
            e.as_app::<GrainEvent>(),
            Some(GrainEvent::Passivated { .. })
        )),
        "the idle grain must hibernate",
    );

    // A fresh activation rehydrates the metadata from the snapshot and the bulk
    // bytes are still in the colocated blob area — the acknowledged write survives.
    let reread = blobs.grain("ws/0");
    let got = sim.block_on(async move { reread.ask(Fetch { id, range: None }).await.unwrap() });
    assert_eq!(got, Ok(bytes), "hibernation must not lose a stored blob");
}

// --- Quorum tier --------------------------------------------------------------

fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

fn raft() -> RaftConfig {
    let mut config = RaftConfig::new(vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)]);
    config.election_timeout = Duration::from_millis(500);
    config.heartbeat_interval = Duration::from_millis(100);
    config
}

fn config() -> GranaryConfig {
    // R = 3 over the 3-node cluster; the write quorum is the majority (2), computed
    // by the replicator — there is no separate knob.
    GranaryConfig {
        shards: 2,
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
    cell.lock()
        .unwrap()
        .take()
        .expect("future did not complete")
}

fn cluster(
    sim: &Simulation,
) -> (
    SimNetwork,
    Vec<SimCluster>,
    Vec<Granary<BlobGrain<SimCluster>>>,
) {
    cluster_cfg(sim, config())
}

fn cluster_cfg(
    sim: &Simulation,
    cfg: GranaryConfig,
) -> (
    SimNetwork,
    Vec<SimCluster>,
    Vec<Granary<BlobGrain<SimCluster>>>,
) {
    let net = SimNetwork::new(sim).with_leader(swim(), raft(), DowningPolicy::Conservative);
    let systems = vec![
        net.join(NodeId::new(1)),
        net.join(NodeId::new(2)),
        net.join(NodeId::new(3)),
    ];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let granaries: Vec<Granary<BlobGrain<SimCluster>>> = systems
        .iter()
        .map(|system| system.granary::<BlobGrain<SimCluster>>(cfg.clone()))
        .collect();
    sim.run_for(Duration::from_secs(3)); // elect each shard group's leader
    (net, systems, granaries)
}

/// Overwrite, with non-matching bytes, every on-disk blob file named `id` anywhere
/// under `root` — the file-store names a blob file by its content hash (hex), so
/// this tampers exactly the targeted blob's stored copies. Returns how many it hit.
fn corrupt_blob_files(root: &std::path::Path, id: BlobId) -> usize {
    fn walk(dir: &std::path::Path, name: &str, hits: &mut usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, name, hits);
            } else if path.file_name().and_then(|n| n.to_str()) == Some(name) {
                std::fs::write(&path, b"tampered, no longer matches its content id").unwrap();
                *hits += 1;
            }
        }
    }
    let mut hits = 0;
    walk(root, &id.to_string(), &mut hits);
    hits
}

/// The on-disk bytes of blob `id` under `root` (the first file named by its content
/// hash), or `None` if no such file exists. Used to check a corrupt copy was
/// repaired in place.
fn read_blob_file(root: &std::path::Path, id: BlobId) -> Option<Vec<u8>> {
    fn walk(dir: &std::path::Path, name: &str) -> Option<Vec<u8>> {
        for entry in std::fs::read_dir(dir).ok()?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Some(bytes) = walk(&path, name) {
                    return Some(bytes);
                }
            } else if path.file_name().and_then(|n| n.to_str()) == Some(name) {
                return std::fs::read(&path).ok();
            }
        }
        None
    }
    walk(root, &id.to_string())
}

#[test]
fn a_blob_round_trips_through_the_quorum_from_any_node() {
    // The bytes are stored on a write quorum of the shard's replicas and read back
    // verified, with the call issued from a node that does not lead the shard — so
    // the put fan-out and the verified read both cross the wire (B1, B3, G13).
    let sim = Simulation::new(1);
    let (_net, _systems, granaries) = cluster(&sim);
    let granary = granaries[2].clone();
    let bytes = b"a clustered block".to_vec();

    let (id, fetched, present) = drive(&sim, Duration::from_secs(8), async move {
        let g = granary.grain("ws/42");
        let id = g.ask(Put(bytes.clone())).await.unwrap().unwrap();
        let fetched = g.ask(Fetch { id, range: None }).await.unwrap();
        let present = g.ask(Exists(id)).await.unwrap();
        (id, fetched, present)
    });

    assert_eq!(id, BlobId::of(b"a clustered block"));
    assert_eq!(fetched, Ok(b"a clustered block".to_vec()));
    assert!(present);
}

#[test]
fn a_blob_survives_losing_a_minority_of_replicas() {
    // A put acks at W=2 of R=3; crashing one replica leaves a quorum that still
    // holds the bytes, so the read returns them verified (B3).
    let sim = Simulation::new(2);
    let (net, systems, granaries) = cluster(&sim);
    let granary = granaries[0].clone();
    let bytes = b"durable across a replica loss".to_vec();

    let id = drive(&sim, Duration::from_secs(8), {
        let bytes = bytes.clone();
        async move {
            let g = granary.grain("ws/7");
            g.ask(Put(bytes)).await.unwrap().unwrap()
        }
    });

    // Crash one replica (a non-leader of the control plane), then read from another.
    net.crash(systems[2].node());
    sim.run_for(Duration::from_secs(2));

    let granary = granaries[1].clone();
    let got = drive(&sim, Duration::from_secs(8), async move {
        granary
            .grain("ws/7")
            .ask(Fetch { id, range: None })
            .await
            .unwrap()
    });
    assert_eq!(got, Ok(bytes), "a minority loss must not lose the blob");
}

// --- Verification under corruption (G17) --------------------------------------

#[test]
fn a_corrupt_blob_is_detected_and_never_returned() {
    // The single store's only copy is tampered on disk: `get` re-hashes, sees the
    // mismatch, and errors — it never hands back the wrong bytes (G17).
    let sim = Simulation::new(7);
    let dir = tempfile::tempdir().unwrap();
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let blobs = system.granary::<BlobGrain<SimSystem>>(GranaryConfig {
        grain_store: Some(FileGrainStore::factory(dir.path())),
        ..GranaryConfig::default()
    });

    let id = sim.block_on({
        let g = blobs.grain("ws/0");
        async move {
            g.ask(Put(b"the real bytes".to_vec()))
                .await
                .unwrap()
                .unwrap()
        }
    });
    assert!(
        corrupt_blob_files(dir.path(), id) >= 1,
        "the blob file must exist to corrupt"
    );

    let g = blobs.grain("ws/0");
    let got = sim.block_on(async move { g.ask(Fetch { id, range: None }).await.unwrap() });
    assert!(
        got.is_err(),
        "a corrupt blob is an error, never wrong bytes (G17)"
    );
}

#[test]
fn a_tampered_replica_copy_falls_through_to_a_good_one() {
    // The leader's local copy is tampered; the verified read must fall through to a
    // peer that holds a good copy and return the right bytes (G17, clustered).
    let sim = Simulation::new(1);
    let dir = tempfile::tempdir().unwrap();
    let cfg = GranaryConfig {
        grain_store: Some(FileGrainStore::factory(dir.path())),
        ..config()
    };
    let (_net, _systems, granaries) = cluster_cfg(&sim, cfg);
    let granary = granaries[0].clone();
    let key = "ws/9";
    let bytes = b"verified despite a tampered replica".to_vec();

    let id = drive(&sim, Duration::from_secs(8), {
        let granary = granary.clone();
        let bytes = bytes.clone();
        async move { granary.grain(key).ask(Put(bytes)).await.unwrap().unwrap() }
    });

    // Corrupt only the shard leader's on-disk copy, forcing the read to fall through.
    let leader = granary.leader(key).expect("the shard elected a leader");
    let hits = corrupt_blob_files(&dir.path().join(leader.to_string()), id);
    assert!(hits >= 1, "the leader must hold a local copy to corrupt");

    let got = drive(&sim, Duration::from_secs(8), async move {
        granary
            .grain(key)
            .ask(Fetch { id, range: None })
            .await
            .unwrap()
    });
    assert_eq!(
        got,
        Ok(bytes),
        "a tampered copy falls through to a verifying replica (G17)"
    );
}

#[test]
fn a_tampered_local_copy_is_healed_in_place_from_a_peer() {
    // Beyond falling through (above): the corrupt local copy is *repaired* in place
    // from the verifying peer, restoring this replica's durability margin (§7.10
    // self-heal). Without it, a content-addressed re-put of an id already on disk is
    // a no-op, so the bad copy would persist and every read would re-fetch over the
    // wire — and a later loss of the good peers would lose a blob that looked present.
    let sim = Simulation::new(1);
    let dir = tempfile::tempdir().unwrap();
    let cfg = GranaryConfig {
        grain_store: Some(FileGrainStore::factory(dir.path())),
        ..config()
    };
    let (_net, _systems, granaries) = cluster_cfg(&sim, cfg);
    let granary = granaries[0].clone();
    let key = "ws/heal";
    let bytes = b"repaired in place from a verifying replica".to_vec();

    let id = drive(&sim, Duration::from_secs(8), {
        let granary = granary.clone();
        let bytes = bytes.clone();
        async move { granary.grain(key).ask(Put(bytes)).await.unwrap().unwrap() }
    });

    // Corrupt only the shard leader's on-disk copy; it no longer verifies.
    let leader = granary.leader(key).expect("the shard elected a leader");
    let leader_dir = dir.path().join(leader.to_string());
    assert!(
        corrupt_blob_files(&leader_dir, id) >= 1,
        "the leader must hold a local copy to corrupt"
    );
    assert_ne!(
        read_blob_file(&leader_dir, id).map(|b| BlobId::of(&b)),
        Some(id),
        "the leader's copy must be corrupt before the healing read",
    );

    // A read from the leader falls through to a good peer and returns the right bytes.
    let got = drive(&sim, Duration::from_secs(8), {
        let granary = granary.clone();
        async move {
            granary
                .grain(key)
                .ask(Fetch { id, range: None })
                .await
                .unwrap()
        }
    });
    assert_eq!(got, Ok(bytes.clone()));

    // The leader's on-disk copy is now repaired in place: it verifies against the id.
    assert_eq!(
        read_blob_file(&leader_dir, id).map(|b| BlobId::of(&b)),
        Some(id),
        "the corrupt local copy must be repaired from the verifying replica (§7.10)",
    );

    // And a subsequent read is served locally from the healed copy — still correct.
    let again = drive(&sim, Duration::from_secs(8), async move {
        granary
            .grain(key)
            .ask(Fetch { id, range: None })
            .await
            .unwrap()
    });
    assert_eq!(
        again,
        Ok(bytes),
        "the healed local copy serves the next read"
    );
}
