//! The workspace facet on the clustered `Quorum` tier (spec §7.11, §14).
//!
//! Exercises the cluster-only behavior: a file captured on one node's grain
//! handle is readable from any node (G13), the workspace survives losing a
//! minority of replicas (delta records ride the record quorum, G14), and
//! **root-driven blob repair** (§7.10 B6) re-replicates a checkpoint chunk to a
//! replica that missed its write — the consumer-level restoration of the
//! durability margin.

use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::BoxError;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use granary::BlobId;
use granary::FileGrainStore;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GrainRegistry;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::GranarySystem;
use granary::NoEvent;
use granary::Ws;
use granary::WsError;
use serde::Deserialize;
use serde::Serialize;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

// --- A grain whose durable state is entirely its workspace directory ----------

struct Studio<S>(PhantomData<fn() -> S>);

impl<S> Default for Studio<S> {
    fn default() -> Self {
        Studio(PhantomData)
    }
}

impl<S: GranarySystem> Grain for Studio<S> {
    type System = S;
    type State = ();
    type Event = NoEvent;
    type Facets = (Ws,);
    const GRAIN_TYPE: &'static str = "test.WsStudio";

    fn apply(_state: &mut (), event: &NoEvent) {
        event.unreachable()
    }

    fn register(r: &mut GrainRegistry<Self>) {
        r.accept::<Put>();
        r.accept::<Get>();
    }

    fn on_activate(
        &mut self,
        ctx: &GrainCtx<Self>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        // Off the activation latency path: sweep chunks a prior activation
        // orphaned and re-fan the live checkpoint chunks to the current
        // replicas (§7.10 B6) — the same conduct as the harness agent.
        let blobs = ctx.blobs();
        let system = ctx.system().clone();
        async move {
            system.launch(Box::pin(async move {
                blobs.gc(&std::collections::BTreeSet::new()).await;
                blobs.repair_facets().await;
            }));
            Ok(())
        }
    }
}

/// Write a file into the workspace directory and capture it (§7.11).
#[derive(Clone, Serialize, Deserialize)]
struct Put {
    path: String,
    content: Vec<u8>,
}
impl Message for Put {
    type Reply = Result<u64, WsError>;
    const MANIFEST: Manifest = Manifest::new("test.WsPut");
}
impl<S: GranarySystem> GrainHandler<Put> for Studio<S> {
    async fn handle(
        &self,
        _state: &(),
        msg: Put,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Result<u64, WsError>) {
        let dir = match ctx.ws().dir_path() {
            Ok(dir) => dir,
            Err(e) => return (vec![], Err(e)),
        };
        let disk = dir.join(&msg.path);
        if let Some(parent) = disk.parent()
            && let Err(e) = std::fs::create_dir_all(parent)
        {
            return (vec![], Err(WsError::Io(e.to_string())));
        }
        if let Err(e) = std::fs::write(&disk, &msg.content) {
            return (vec![], Err(WsError::Io(e.to_string())));
        }
        match ctx.ws().capture() {
            Ok(_) => (vec![], Ok(msg.content.len() as u64)),
            Err(e) => (vec![], Err(e)),
        }
    }
}

/// Read a file straight off the materialized directory — a pure read (§7.5).
#[derive(Clone, Serialize, Deserialize)]
struct Get {
    path: String,
}
impl Message for Get {
    type Reply = Result<Vec<u8>, WsError>;
    const MANIFEST: Manifest = Manifest::new("test.WsGet");
}
impl<S: GranarySystem> GrainHandler<Get> for Studio<S> {
    async fn handle(
        &self,
        _state: &(),
        msg: Get,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Result<Vec<u8>, WsError>) {
        let dir = match ctx.ws().dir_path() {
            Ok(dir) => dir,
            Err(e) => return (vec![], Err(e)),
        };
        let read = std::fs::read(dir.join(&msg.path)).map_err(|e| WsError::Io(e.to_string()));
        (vec![], read)
    }
}

// --- Cluster scaffolding (mirrors sql/kv clustered suites) --------------------

type Studios = Granary<Studio<SimCluster>>;

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

fn config(scratch: &Path) -> GranaryConfig {
    GranaryConfig {
        shards: 2,
        replication_factor: 3,
        idle_after: Duration::from_secs(60),
        snapshot_every: 8,
        data_dir: Some(scratch.to_path_buf()),
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

fn cluster(sim: &Simulation, cfg: GranaryConfig) -> (SimNetwork, Vec<SimCluster>, Vec<Studios>) {
    let net = SimNetwork::new(sim).with_leader(swim(), raft(), DowningPolicy::Conservative);
    let systems = vec![net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let granaries: Vec<Studios> = systems
        .iter()
        .map(|system| system.granary::<Studio<SimCluster>>(cfg.clone()))
        .collect();
    sim.run_for(Duration::from_secs(3)); // elect each shard group's leader
    (net, systems, granaries)
}

/// Whether node `node`'s on-disk store holds a blob named `id` (the file-store
/// names a blob file by its content-hash hex).
fn blob_on_node(root: &Path, node: NodeId, id: BlobId) -> bool {
    fn walk(dir: &Path, name: &str) -> bool {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return false;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if walk(&path, name) {
                    return true;
                }
            } else if path.file_name().and_then(|n| n.to_str()) == Some(name) {
                return true;
            }
        }
        false
    }
    walk(&root.join(node.to_string()), &id.to_string())
}

#[test]
fn a_file_round_trips_through_the_cluster_from_any_node() {
    let scratch = tempfile::tempdir().unwrap();
    let sim = Simulation::new(1);
    let (_net, _systems, granaries) = cluster(&sim, config(scratch.path()));
    let writer = granaries[0].clone();
    let reader = granaries[2].clone();
    let content = b"durable across the cluster".to_vec();

    let (wrote, read) = drive(&sim, Duration::from_secs(8), {
        let content = content.clone();
        async move {
            let wrote = writer
                .grain("ws/42")
                .ask(Put {
                    path: "a/b.txt".into(),
                    content: content.clone(),
                })
                .await
                .unwrap();
            // Read the same workspace from a different node — routes to the
            // leader's materialization (G13).
            let read = reader
                .grain("ws/42")
                .ask(Get {
                    path: "a/b.txt".into(),
                })
                .await
                .unwrap();
            (wrote, read)
        }
    });
    assert_eq!(wrote, Ok(content.len() as u64));
    assert_eq!(read, Ok(content));
}

#[test]
fn the_workspace_survives_losing_a_minority_replica() {
    let scratch = tempfile::tempdir().unwrap();
    let sim = Simulation::new(2);
    let (net, systems, granaries) = cluster(&sim, config(scratch.path()));
    let content = b"survives a replica loss".to_vec();

    drive(&sim, Duration::from_secs(8), {
        let g = granaries[0].clone();
        let content = content.clone();
        async move {
            g.grain("ws/7")
                .ask(Put {
                    path: "f".into(),
                    content,
                })
                .await
                .unwrap()
                .unwrap();
        }
    });

    // Crash one node; a quorum of the shard's replicas survives, holding the
    // captured delta records.
    net.crash(systems[2].node());
    sim.run_for(Duration::from_secs(2));

    let read = drive(&sim, Duration::from_secs(8), {
        let g = granaries[1].clone();
        async move { g.grain("ws/7").ask(Get { path: "f".into() }).await.unwrap() }
    });
    assert_eq!(read, Ok(content));
}

#[test]
fn repair_re_replicates_a_chunk_a_partitioned_replica_missed() {
    // A replica partitioned during a checkpoint misses the chunk blobs; after
    // the partition heals, the grain's re-activation runs root-driven repair
    // (§7.10 B6), which re-fans the live checkpoint chunks to the current
    // replicas — restoring the copy on the node that missed them. Verified by
    // the chunk's on-disk presence per node.
    let sim = Simulation::new(3);
    let dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let cfg = GranaryConfig {
        idle_after: Duration::from_secs(1), // hibernate when idle, to force a re-activation
        snapshot_every: 1,                  // checkpoint (and put chunks) on every commit
        grain_store: Some(FileGrainStore::factory(dir.path())),
        ..config(scratch.path())
    };
    let (net, systems, granaries) = cluster(&sim, cfg);

    let key = "ws/9";
    let content = b"a chunk one replica must catch up on".to_vec();
    // A whole small file is one checkpoint chunk, so its id is its content id.
    let id = BlobId::of(&content);

    // The grain activates on its shard leader; partition a NON-leader replica
    // that is also NOT the caller node A (isolating the caller would hang its
    // own asks), so the write still reaches a quorum (leader + the third node)
    // while the victim misses it.
    let leader = granaries[0]
        .leader(key)
        .expect("the shard elected a leader");
    let victim = [B, C]
        .into_iter()
        .find(|&n| n != leader)
        .expect("a non-leader, non-caller");
    net.partition(
        &[victim],
        &[A, B, C]
            .iter()
            .copied()
            .filter(|&n| n != victim)
            .collect::<Vec<_>>(),
    );
    sim.run_for(Duration::from_secs(3)); // SWIM marks the victim unreachable; groups settle

    // The write goes to the majority side and acks at W=2 there; retries absorb
    // any routing churn the partition caused. The snapshot after the commit
    // checkpoints the file into chunk blobs, also at W=2 — the victim has none.
    drive(&sim, Duration::from_secs(30), {
        let g = granaries[0].clone();
        let sys = systems[0].clone();
        let content = content.clone();
        async move {
            for _ in 0..30 {
                let r = g
                    .grain(key)
                    .ask_timeout(
                        Put {
                            path: "data".into(),
                            content: content.clone(),
                        },
                        Duration::from_secs(3),
                    )
                    .await;
                if matches!(r, Ok(Ok(_))) {
                    return;
                }
                sys.sleep(Duration::from_millis(500)).await;
            }
            panic!("the write never reached the majority side");
        }
    });
    sim.run_for(Duration::from_secs(3)); // let the checkpoint land on the majority
    assert!(
        !blob_on_node(dir.path(), victim, id),
        "the partitioned victim must have missed the checkpoint chunk",
    );

    // Heal; the idle grain hibernates, and the next access re-activates it —
    // whose on_activate repair re-fans the live chunk set (§7.10 B6).
    net.heal();
    sim.run_for(Duration::from_secs(5));
    let read = drive(&sim, Duration::from_secs(20), {
        let g = granaries[0].clone();
        async move {
            g.grain(key)
                .ask_timeout(Get { path: "data".into() }, Duration::from_secs(5))
                .await
                .unwrap()
        }
    });
    assert_eq!(read, Ok(content));
    sim.run_for(Duration::from_secs(10)); // let the background repair drain

    assert!(
        blob_on_node(dir.path(), victim, id),
        "repair must re-replicate the live chunk to the healed replica (§7.10 B6)",
    );
}
