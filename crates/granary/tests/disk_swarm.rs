//! The disk facet under the cluster fault swarm (spec §7.15, §14; V&V
//! checklist #4, #5, #7).
//!
//! `tests/disk_local.rs` proves the facet's contract on the `Local` tier; this
//! file hosts a disk-only grain on the leader-based clustered system and sweeps
//! it across seeds while the nemesis injects partitions, crashes, heals, loss,
//! duplication, and delay (spec §18.3). What that uniquely exercises:
//!
//! - **Failover rematerialization.** A leader crash moves the activation to
//!   another node, whose image is rebuilt from the composite-snapshot manifest
//!   (blob blocks, G17) plus the committed capture records — [`Facet::fold`]'s
//!   pending queue drained by [`Facet::rehydrate`]'s blob fetches, the one path
//!   the `Local` tier's always-snapshotting hibernation cannot reach.
//! - **Checkpoints under faults.** `snapshot_every` forces the index-manifest
//!   contribution while the transport drops and duplicates records.
//! - **Seed-reproducibility (#7).** The same seed replays to a byte-identical
//!   event stream even though every run materializes real image files.
//!
//! Fault *coverage* (#8) for this cluster configuration is already asserted by
//! `tests/grain_swarm.rs` over the same transport; it is not repeated here.

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
use granary::Disk;
use granary::DiskCaptureStats;
use granary::DiskError;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainEvent;
use granary::GrainHandler;
use granary::GrainName;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::NoEvent;
use serde::Deserialize;
use serde::Serialize;

/// 1 MiB — the facet's fixed block size (spec §7.15).
const BLOCK: u64 = 1 << 20;
/// The base image: two blocks, the second partial.
const IMAGE_BYTES: u64 = BLOCK + BLOCK / 2;

// --- A grain whose durable state is entirely its raw image ---------------------

#[derive(Default)]
struct DiskBox;

impl Grain for DiskBox {
    type System = SimCluster;
    type State = ();
    type Event = NoEvent;
    type Facets = (Disk,);
    const GRAIN_TYPE: &'static str = "machine.DiskBox";

    fn apply(_state: &mut (), event: &NoEvent) {
        event.unreachable()
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Stamp>();
        r.accept::<ReadStamp>();
    }
}

/// Provision on first touch, then write a deterministic stamp into the live
/// image and run the capture command (§7.15) — one command, one manifest
/// record, committing through the quorum path (G19).
#[derive(Clone, Serialize, Deserialize)]
struct Stamp {
    /// Where the stamp lands, `0..IMAGE_BYTES - 8`.
    offset: u64,
    value: u64,
    /// The shared base image every node can read (the workload writes it once).
    base: String,
}
impl Message for Stamp {
    type Reply = Result<DiskCaptureStats, DiskError>;
    const MANIFEST: Manifest = Manifest::new("machine.DiskStamp");
}
impl GrainHandler<Stamp> for DiskBox {
    async fn handle(
        &self,
        _state: &(),
        msg: Stamp,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Result<DiskCaptureStats, DiskError>) {
        use std::io::Seek;
        use std::io::Write;
        let disk = ctx.disk();
        // Provision lazily (the machine's first-activation import, §7.15): the
        // import stages this command's one manifest, so the stamp itself waits
        // for the next command on a fresh grain.
        if disk.image_bytes().expect("size") == 0 {
            return (vec![], disk.import(std::path::Path::new(&msg.base)).await);
        }
        let path = disk.path().expect("image path");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open image");
        file.seek(std::io::SeekFrom::Start(msg.offset))
            .expect("seek");
        file.write_all(&msg.value.to_le_bytes()).expect("write");
        drop(file);
        (vec![], disk.capture().await)
    }
}

/// Read eight bytes at `offset` from the live image — a pure read (§7.5).
#[derive(Clone, Serialize, Deserialize)]
struct ReadStamp {
    offset: u64,
}
impl Message for ReadStamp {
    type Reply = Option<u64>;
    const MANIFEST: Manifest = Manifest::new("machine.DiskReadStamp");
}
impl GrainHandler<ReadStamp> for DiskBox {
    async fn handle(
        &self,
        _state: &(),
        msg: ReadStamp,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Option<u64>) {
        use std::io::Read;
        use std::io::Seek;
        if ctx.disk().image_bytes().expect("size") == 0 {
            return (vec![], None);
        }
        let path = ctx.disk().path().expect("image path");
        let mut file = std::fs::File::open(path).expect("open image");
        file.seek(std::io::SeekFrom::Start(msg.offset))
            .expect("seek");
        let mut bytes = [0u8; 8];
        file.read_exact(&mut bytes).expect("read");
        (vec![], Some(u64::from_le_bytes(bytes)))
    }
}

// --- Grain-specific continuous safety checkers (as in sql_swarm.rs) -------------

/// **Commit head is monotonic** (invariants **G3**, **G5**).
#[derive(Default)]
struct CommitMonotonic {
    last: BTreeMap<GrainName, u64>,
}

impl Invariant for CommitMonotonic {
    fn name(&self) -> &'static str {
        "disk-grain-commit-monotonic"
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

/// **Exactly-once activation per node** (invariant **G6**), crash-sound.
#[derive(Default)]
struct ActivationSingletonPerNode {
    live: BTreeSet<(NodeId, GrainName)>,
}

impl Invariant for ActivationSingletonPerNode {
    fn name(&self) -> &'static str {
        "disk-grain-activation-singleton-per-node"
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

/// Stamp-and-read disk traffic against a handful of grains under the nemesis,
/// driven through the public `GrainRef` API only (spec §18.4). A faulted call
/// is recorded as nothing and the client moves on.
///
/// One scratch directory serves every run and every simulated node (the facet
/// keys materializations by node and grain, and restore discards stale files —
/// they are a cache, never truth, §1). The shared base image lives beside them.
struct DiskBoxSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
    dir: PathBuf,
}

impl DiskBoxSwarm {
    fn config(&self) -> GranaryConfig {
        GranaryConfig {
            shards: 2,
            replication_factor: 3,
            idle_after: Duration::from_secs(60),
            // Checkpoint often: the index-manifest contribution runs under
            // faults, and failover rematerializes from it plus the later
            // capture records (fold + rehydrate).
            snapshot_every: 4,
            data_dir: Some(self.dir.clone()),
            ..GranaryConfig::default()
        }
    }

    fn base_image(&self) -> PathBuf {
        self.dir.join("base.img")
    }
}

impl ClusterWorkload for DiskBoxSwarm {
    fn name(&self) -> &'static str {
        "granary-disk-box-swarm"
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
        ClusterModeSpec::Leader {
            swim: self.swim(),
            voters: self.nodes,
            election_timeout: Duration::from_millis(500),
            heartbeat_interval: Duration::from_millis(100),
            downing: DowningPolicy::Conservative,
        }
    }

    fn setup(&self, _ctx: &ClusterCtx) {
        // The deterministic base image, written once per run (idempotent
        // content, so reruns and concurrent seeds agree).
        let bytes: Vec<u8> = (0..IMAGE_BYTES).map(|i| (i / 11 % 249) as u8).collect();
        std::fs::write(self.base_image(), bytes).expect("write base image");
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimCluster> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        let config = self.config();
        let base = self.base_image().to_string_lossy().into_owned();
        Box::pin(async move {
            let granaries: Vec<_> = nodes
                .iter()
                .map(|s| s.granary::<DiskBox>(config.clone()))
                .collect();
            let clock = nodes[0].clock().clone();
            let entropy = nodes[0].entropy().clone();
            // Let the control-plane and shard groups elect before traffic.
            clock.sleep(Duration::from_secs(3)).await;

            let mut tasks = Vec::new();
            for c in 0..clients {
                let granary = granaries[c % granaries.len()].clone();
                let entropy = entropy.clone();
                let base = base.clone();
                tasks.push(async move {
                    for _ in 0..ops {
                        // A small key space so several grains share each shard.
                        let key = format!("box/{}", entropy.next_u64() % 3);
                        let grain = granary.grain(key);
                        // Stamps land across both blocks, partial tail included.
                        let offset = entropy.next_u64() % (IMAGE_BYTES - 8);
                        if entropy.next_u64().is_multiple_of(2) {
                            let _ = grain
                                .ask_timeout(
                                    Stamp {
                                        offset,
                                        value: entropy.next_u64(),
                                        base: base.clone(),
                                    },
                                    Duration::from_secs(2),
                                )
                                .await;
                        } else {
                            let _ = grain
                                .ask_timeout(ReadStamp { offset }, Duration::from_secs(2))
                                .await;
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
fn disk_grain_invariants_hold_under_the_cluster_swarm() {
    // #4: the safety core plus G3/G5 and G6 hold on every seeded run while disk
    // grains commit capture manifests, checkpoint the block index, and
    // rematerialize across failover (restore + fold + rehydrate, G17), under
    // partitions, crashes, loss, duplication, and delay.
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = DiskBoxSwarm {
        nodes: 3,
        clients: 3,
        ops: 5,
        dir: dir.path().to_path_buf(),
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..16) {
        panic!("{failure}");
    }
}

#[test]
fn disk_cluster_swarm_is_reproducible() {
    // #7: the same seed replays to a byte-identical event stream — grain events
    // included — even under cluster nemesis and transport faults, with real
    // image files materialized on every node.
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = DiskBoxSwarm {
        nodes: 3,
        clients: 2,
        ops: 4,
        dir: dir.path().to_path_buf(),
    };
    for seed in 0..8 {
        if let Err(divergence) = check_cluster_reproducible(&workload, seed) {
            panic!("{divergence}");
        }
    }
}
