//! The durable workspace filesystem grain on the clustered `Quorum` tier
//! (durable-workspace design; granary §7.10, §14).
//!
//! Exercises the cluster-only behavior: a file written on one node's grain handle is
//! readable from any node (G13), the workspace survives losing a minority of replicas
//! (B3/G14 for the metadata, §7.10 for the blocks), and **grain-driven repair**
//! (§7.10 B6) re-replicates a block to a replica that missed its write — the
//! consumer-level restoration of the durability margin.

use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use granary::BlobId;
use granary::FileGrainStore;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::GranarySystem;
use granary::fs::Fs;
use granary::fs::ReadFile;
use granary::fs::Stat;
use granary::fs::WriteFile;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

type Ws = Fs<SimCluster>;

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

fn config() -> GranaryConfig {
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
    cfg: GranaryConfig,
) -> (SimNetwork, Vec<SimCluster>, Vec<Granary<Ws>>) {
    let net = SimNetwork::new(sim).with_leader(swim(), raft(), DowningPolicy::Conservative);
    let systems = vec![net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let granaries: Vec<Granary<Ws>> = systems
        .iter()
        .map(|system| system.granary::<Ws>(cfg.clone()))
        .collect();
    sim.run_for(Duration::from_secs(3)); // elect each shard group's leader
    (net, systems, granaries)
}

/// Whether node `node`'s on-disk store holds a blob named `id` (the file-store names
/// a blob file by its content-hash hex).
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
    let sim = Simulation::new(1);
    let (_net, _systems, granaries) = cluster(&sim, config());
    let writer = granaries[0].clone();
    let reader = granaries[2].clone();
    let content = b"durable across the cluster".to_vec();

    let (wrote, read) = drive(&sim, Duration::from_secs(8), {
        let content = content.clone();
        async move {
            let wrote = writer
                .grain("ws/42")
                .ask(WriteFile {
                    path: "a/b.txt".into(),
                    content: content.clone(),
                })
                .await
                .unwrap();
            // Read the same workspace from a different node — routes to the leader (G13).
            let read = reader
                .grain("ws/42")
                .ask(ReadFile {
                    path: "a/b.txt".into(),
                    range: None,
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
    let sim = Simulation::new(2);
    let (net, systems, granaries) = cluster(&sim, config());
    let content = b"survives a replica loss".to_vec();

    drive(&sim, Duration::from_secs(8), {
        let g = granaries[0].clone();
        let content = content.clone();
        async move {
            g.grain("ws/7")
                .ask(WriteFile {
                    path: "f".into(),
                    content,
                })
                .await
                .unwrap()
                .unwrap();
        }
    });

    // Crash one node; a quorum of the shard's replicas survives, holding both the
    // metadata and the block.
    net.crash(systems[2].node());
    sim.run_for(Duration::from_secs(2));

    let read = drive(&sim, Duration::from_secs(8), {
        let g = granaries[1].clone();
        async move {
            g.grain("ws/7")
                .ask(ReadFile {
                    path: "f".into(),
                    range: None,
                })
                .await
                .unwrap()
        }
    });
    assert_eq!(read, Ok(content));
}

#[test]
fn repair_re_replicates_a_block_a_partitioned_replica_missed() {
    // A replica partitioned during a write misses the block; after the partition
    // heals, the grain's re-activation runs grain-driven repair (§7.10 B6), which
    // re-fans the live block set to the current replicas — restoring the copy on the
    // node that missed it. Verified by the block's on-disk presence per node.
    let sim = Simulation::new(3);
    let dir = tempfile::tempdir().unwrap();
    let cfg = GranaryConfig {
        idle_after: Duration::from_secs(1), // hibernate when idle, to force a re-activation
        snapshot_every: 1,
        grain_store: Some(FileGrainStore::factory(dir.path())),
        ..config()
    };
    let (net, systems, granaries) = cluster(&sim, cfg);

    let key = "ws/9";
    let content = b"a block one replica must catch up on".to_vec();
    let id = BlobId::of(&content);

    // The grain activates on its shard leader; partition a NON-leader replica that is
    // also NOT the caller node A (isolating the caller would hang its own asks), so the
    // write still reaches a quorum (leader + the third node) while the victim misses it.
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

    // The write goes to the majority side and acks at W=2 there; retries absorb any
    // routing churn the partition caused.
    drive(&sim, Duration::from_secs(30), {
        let g = granaries[0].clone();
        let sys = systems[0].clone();
        let content = content.clone();
        async move {
            for _ in 0..30 {
                let r = g
                    .grain(key)
                    .ask_timeout(
                        WriteFile {
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
    assert!(
        !blob_on_node(dir.path(), victim, id),
        "the partitioned replica must miss the write"
    );

    // Heal and let leadership re-converge and the grain hibernate (idle), then
    // re-access it: the re-activation's `on_activate` triggers repair, which re-fans
    // the live block to all replicas. The access retries across residual leadership
    // churn from the membership change (sleeping in sim-time between attempts).
    net.heal();
    sim.run_for(Duration::from_secs(6)); // re-converge after the heal, then idle → hibernate
    drive(&sim, Duration::from_secs(30), {
        let g = granaries[0].clone();
        let sys = systems[0].clone();
        async move {
            for _ in 0..30 {
                let r = g
                    .grain(key)
                    .ask_timeout(
                        Stat {
                            path: "data".into(),
                        },
                        Duration::from_secs(3),
                    )
                    .await;
                if matches!(r, Ok(Ok(_))) {
                    return;
                }
                sys.sleep(Duration::from_millis(500)).await;
            }
            panic!("workspace never became reachable after the heal");
        }
    });
    sim.run_for(Duration::from_secs(3)); // let the background repair complete

    assert!(
        blob_on_node(dir.path(), victim, id),
        "grain-driven repair must re-replicate the missed block to the healed replica",
    );
}
