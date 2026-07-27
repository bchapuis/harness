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

mod support;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
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
use actor_simulation::coverage_seeds;
use actor_simulation::default_invariants;
use actor_simulation::replay_cluster_swarm;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_cluster_swarm_coverage;
use actor_simulation::slow_seeds;
use granary::Disk;
use granary::DiskCaptureStats;
use granary::DiskError;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::NoEvent;
use granary::testing::ActivationSingletonPerNode;
use granary::testing::CommitMonotonic;

use serde::Deserialize;
use serde::Serialize;
use support::Exercised;

/// 1 MiB — the facet's fixed block size (spec §7.15).
const BLOCK: u64 = 1 << 20;
/// The base image: two blocks, the second partial.
const IMAGE_BYTES: u64 = BLOCK + BLOCK / 2;

// --- A grain whose durable state is entirely its raw image ---------------------

#[derive(Default)]
struct DiskBox;

impl Grain for DiskBox {
    type System = SimNode;
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
    /// Idle past `idle_after` between operations, so activations passivate and
    /// the image is rematerialized mid-run.
    hibernating: bool,
    /// Let the nemesis kill and re-launch node processes, not just isolate them.
    restarting: bool,
    /// What the sweep actually exercised, accumulated across its seeds.
    exercised: Exercised,
}

impl DiskBoxSwarm {
    /// Activations resident for the whole run; the nemesis only isolates nodes.
    fn new(nodes: usize, clients: usize, ops: u64, dir: PathBuf) -> DiskBoxSwarm {
        DiskBoxSwarm {
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
    /// kills processes — so a read has to come from an image the next activation
    /// rebuilt: checkpoint manifest, blocks re-fetched and verified by content
    /// (G17), capture manifests folded and applied on top.
    fn hibernating(nodes: usize, clients: usize, ops: u64, dir: PathBuf) -> DiskBoxSwarm {
        DiskBoxSwarm {
            hibernating: true,
            restarting: true,
            ..DiskBoxSwarm::new(nodes, clients, ops, dir)
        }
    }

    fn config(&self) -> GranaryConfig {
        GranaryConfig {
            shards: 2,
            replication_factor: 3,
            idle_after: if self.hibernating {
                IDLE_AFTER
            } else {
                RESIDENT
            },
            // Checkpoint often: the index-manifest contribution runs under
            // faults, and failover rematerializes from it plus the later
            // capture records (fold + rehydrate). Oftener still when
            // hibernating, so a grain has a checkpoint to come back from before
            // it first passivates — otherwise every rehydration replays from an
            // empty base and the manifest restore path goes untested, which is
            // what `Exercised::from_snapshot` refuses to let pass silently.
            snapshot_every: if self.hibernating { 2 } else { 4 },
            data_dir: Some(self.dir.clone()),
            ..GranaryConfig::default()
        }
    }

    fn base_image(&self) -> PathBuf {
        self.dir.join("base.img")
    }
}

/// Activation lifetime for the resident sweeps: longer than any run.
const RESIDENT: Duration = Duration::from_secs(60);
/// Activation lifetime for the hibernating sweep.
const IDLE_AFTER: Duration = Duration::from_millis(200);
/// How long a hibernating client idles when it idles — comfortably past
/// [`IDLE_AFTER`], so the host really does passivate rather than nearly.
const IDLE_FOR: Duration = Duration::from_millis(500);

impl ClusterWorkload for DiskBoxSwarm {
    fn name(&self) -> &'static str {
        if self.hibernating {
            "granary-disk-box-hibernating-swarm"
        } else {
            "granary-disk-box-swarm"
        }
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

    fn rehost(&self) -> Option<Rehost> {
        if !self.restarting {
            return None;
        }
        // A restarted node comes up empty and would otherwise stop hosting
        // `DiskBox`. The config carries the same scratch root: the facet's
        // restore discards stale local files (a cache, never truth — §1), so a
        // fresh process rebuilding over its predecessor's leftover image is
        // exactly the case worth covering.
        let config = self.config();
        Some(Arc::new(move |node: &SimNode| {
            node.granary::<DiskBox>(config.clone());
        }))
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        let hibernating = self.hibernating;
        let restarting = self.restarting;
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
                // Under restarts every client goes through node 1's gateway, the
                // one node the nemesis leaves alone; location transparency (G13)
                // keeps the coverage, since the call still lands on the shard's
                // leader.
                let granary = if restarting {
                    granaries[0].clone()
                } else {
                    granaries[c % granaries.len()].clone()
                };
                let clock = clock.clone();
                let entropy = entropy.clone();
                let base = base.clone();
                tasks.push(async move {
                    for op in 0..ops {
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
                        // Idle past the activation lifetime often enough that the
                        // next call rehydrates: fetch the checkpoint's blocks,
                        // verify them by content, apply the pending manifests.
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
            "disk-grain-commit-monotonic",
            "grain",
        )));
        invariants.push(Box::new(ActivationSingletonPerNode::new(
            "disk-grain-activation-singleton-per-node",
            "grain",
        )));
        invariants.push(Box::new(self.exercised.clone()));
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
    let workload = DiskBoxSwarm::new(3, 3, 5, dir.path().to_path_buf());
    if let Err(failure) = run_cluster_swarm(&workload, slow_seeds(0..16)) {
        panic!("{failure}");
    }
}

// --- The same claims across hibernation and process death --------------------
//
// The sweep above holds every activation resident, so a read is served from the
// image the write landed in — the block image is never rebuilt. This one tears it
// down: the grain passivates, its image goes, and the next read is served from
// one the new activation rehydrated block by block out of content-addressed
// blobs, possibly in a process that did not exist when the write committed. That
// is the disk facet's whole durability story, and until now only a scripted
// failover reached it.

#[test]
fn disk_grain_invariants_hold_across_hibernation_and_restarts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = DiskBoxSwarm::hibernating(3, 3, 5, dir.path().to_path_buf());
    if let Err(failure) = run_cluster_swarm(&workload, slow_seeds(0..16)) {
        panic!("{failure}");
    }
    workload.exercised.assert_hibernated();
}

#[test]
fn disk_hibernating_swarm_is_reproducible() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = DiskBoxSwarm::hibernating(3, 2, 4, dir.path().to_path_buf());
    if let Err(divergence) = replay_cluster_swarm(&workload, slow_seeds(0..8)) {
        panic!("{divergence}");
    }
}

#[test]
fn disk_cluster_swarm_actually_fires_each_fault_type() {
    // #8, sized for a workload whose seeds cost seconds apiece: `coverage_seeds`
    // never narrows, so the declared range *is* the cost. Four seeds is enough
    // to carry "each fault type fired at least once" — the transport draws its
    // fault rates per seed and the nemesis runs six rounds within each — and
    // small enough not to dominate the suite.
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = DiskBoxSwarm::new(3, 3, 5, dir.path().to_path_buf());
    let stats = match run_cluster_swarm_coverage(&workload, coverage_seeds(0..4)) {
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
fn disk_cluster_swarm_is_reproducible() {
    // #7: the same seed replays to a byte-identical event stream — grain events
    // included — even under cluster nemesis and transport faults, with real
    // image files materialized on every node.
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = DiskBoxSwarm::new(3, 2, 4, dir.path().to_path_buf());
    if let Err(divergence) = replay_cluster_swarm(&workload, slow_seeds(0..8)) {
        panic!("{divergence}");
    }
}
