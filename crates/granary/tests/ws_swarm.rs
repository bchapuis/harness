//! The workspace facet under the cluster fault swarm (spec §7.11, §14; V&V
//! checklist #4, #7, #8).
//!
//! `tests/ws_clustered.rs` drives the workspace through *scripted* faults (one
//! crash, one partition, then repair). This file sweeps a [`ClusterWorkload`] of
//! write/read/overwrite traffic across many seeds while a seeded nemesis injects
//! partitions, crashes, heals, loss, duplication, and delay (core spec §18.3),
//! so the end-to-end product path — captured delta records on the record quorum,
//! checkpoint chunks on the blob quorum (§7.10) — is exercised together under
//! the full fault matrix. A [`Checker`] watches the §13 event stream live.
//!
//! - **Read integrity under faults (#4, the safety property).** Each client owns
//!   a private path subtree, so there are no cross-client overwrite races; a
//!   read of a path this client wrote returns the *exact* bytes of its last
//!   committed write to that path, or an error — never stale, partial, or
//!   another path's bytes. This asserts capture/replay byte-determinism (F1) and
//!   lossless failover (G14): an acknowledged capture is never lost or shadowed
//!   across a leadership change. Asserted in-workload over a shared flag.
//! - **Safety core under faults (#4).** [`default_invariants`] hold on every run.
//! - **Seed-reproducibility (#7).** The same seed replays byte-identically.
//! - **Fault coverage (#8).** Each transport fault type actually fired.

use std::marker::PhantomData;
mod support;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::BoxError;
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
use actor_simulation::sweep_seeds;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainError;
use granary::GrainHandler;
use granary::GrainRegistry;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::GranarySystem;
use granary::NoEvent;
use granary::Ws;
use granary::WsError;

use serde::Deserialize;
use serde::Serialize;
use support::Exercised;

// --- The workspace test grain (the ws_clustered twin) -------------------------

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
        // Root-driven blob repair off the activation path (§7.10 B6), the
        // harness agent's conduct — so the swarm exercises the repair path
        // under faults too.
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

// --- The swarm workload --------------------------------------------------------

/// Content that varies in length and bytes with `n`. Kept small so a run stays
/// cheap; larger trees are covered by the targeted `ws_local` tests.
fn content(n: u64) -> Vec<u8> {
    let len = 1 + (n % 200) as usize;
    (0..len)
        .map(|i| (n.wrapping_add(i as u64) % 251) as u8)
        .collect()
}

/// What one client is entitled to read back at one path.
///
/// A committed `Put` pins the value. A `Put` whose outcome was **ambiguous** does
/// not: `Unavailable` means the quorum was not observed, not that the record was
/// refused (granary §7.2, §11) — a replica minority may hold it, and the next
/// activation's quorum recovery can find it on a majority and adopt it. So the
/// bytes of an ambiguous write are admissible from then on, and the check is
/// that a read returns *some value this client actually wrote and that nothing
/// has superseded* — which is still what F1/G14 claim, stated over the outcomes
/// the spec actually gives.
///
/// Reads never narrow the set: the read path is read-your-leader, not
/// linearizable (§7.5), so a deposed-but-unfenced activation may serve an older
/// admissible value after a newer one was seen.
#[derive(Default)]
struct PathState {
    /// The last value known to have committed, if any.
    committed: Option<Vec<u8>>,
    /// Values written since then whose fate is unknown, oldest first.
    ambiguous: Vec<Vec<u8>>,
}

impl PathState {
    /// A `Put` that returned success: it committed, and it is ordered after every
    /// ambiguous attempt before it, so those can no longer be the visible value.
    fn committed(&mut self, bytes: Vec<u8>) {
        self.committed = Some(bytes);
        self.ambiguous.clear();
    }

    /// A `Put` whose outcome was ambiguous: it may surface later.
    fn may_have_landed(&mut self, bytes: Vec<u8>) {
        self.ambiguous.push(bytes);
    }

    /// Whether `got` is a value this client is entitled to read here.
    fn admits(&self, got: &[u8]) -> bool {
        self.committed.as_deref() == Some(got) || self.ambiguous.iter().any(|v| v == got)
    }
}

/// Write/read/overwrite traffic against a handful of workspace grains, driven
/// through the public `GrainRef` API only (§18.4). Each client writes only paths
/// in its own subtree (`c{client}/...`), so a read of a path it wrote has a
/// well-defined set of admissible values with no cross-client race. A faulted
/// call is recorded per [`PathState`] and the client moves on, so the drive
/// future always completes.
///
/// `stale` is shared across every seeded run: a client sets it if a read of one
/// of its own paths ever returns bytes it cannot account for (an F1/G14
/// violation — a lost or stale-shadowed acknowledged capture).
struct WsSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
    /// One scratch root for the whole sweep; `Host::facet_env` keys
    /// materializations by node and grain beneath it, and every activation
    /// restores through a wipe, so seeded runs cannot contaminate each other.
    scratch: tempfile::TempDir,
    /// Idle past `idle_after` between operations, so activations passivate and
    /// rehydrate mid-run.
    hibernating: bool,
    /// Let the nemesis kill and re-launch node processes, not just isolate them.
    restarting: bool,
    stale: Arc<AtomicBool>,
    reads_verified: Arc<AtomicU64>,
    exercised: Exercised,
}

impl WsSwarm {
    /// Activations resident for the whole run; the nemesis only isolates nodes.
    fn new(nodes: usize, clients: usize, ops: u64) -> WsSwarm {
        WsSwarm {
            nodes,
            clients,
            ops,
            scratch: tempfile::tempdir().expect("scratch tempdir"),
            hibernating: false,
            restarting: false,
            stale: Arc::new(AtomicBool::new(false)),
            reads_verified: Arc::new(AtomicU64::new(0)),
            exercised: Exercised::default(),
        }
    }

    /// Short activation lifetime, clients that idle past it, and a nemesis that
    /// kills processes — so a read has to come from a directory the next
    /// activation rebuilt, byte-deterministically, from the committed records.
    fn hibernating(nodes: usize, clients: usize, ops: u64) -> WsSwarm {
        WsSwarm {
            hibernating: true,
            restarting: true,
            ..WsSwarm::new(nodes, clients, ops)
        }
    }

    /// `hibernating` picks the activation lifetime: resident for the whole run,
    /// or short enough that clients can idle past it so the workspace directory
    /// is wiped and rebuilt mid-run. The snapshot cadence follows, so a returning
    /// activation restores from a composite snapshot plus a replayed tail rather
    /// than from an empty base.
    fn config(&self) -> GranaryConfig {
        GranaryConfig {
            shards: 2,
            replication_factor: 3,
            idle_after: if self.hibernating {
                IDLE_AFTER
            } else {
                RESIDENT
            },
            snapshot_every: if self.hibernating { 3 } else { 8 },
            data_dir: Some(self.scratch.path().to_path_buf()),
            ..GranaryConfig::default()
        }
    }
}

/// Activation lifetime for the resident sweeps: longer than any run.
const RESIDENT: Duration = Duration::from_secs(60);
/// Activation lifetime for the hibernating sweep.
const IDLE_AFTER: Duration = Duration::from_millis(200);
/// How long a hibernating client idles when it idles — comfortably past
/// [`IDLE_AFTER`], so the host really does passivate rather than nearly.
const IDLE_FOR: Duration = Duration::from_millis(500);

impl ClusterWorkload for WsSwarm {
    fn name(&self) -> &'static str {
        if self.hibernating {
            "granary-ws-hibernating-swarm"
        } else {
            "granary-ws-swarm"
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

    fn rehost(&self) -> Option<Rehost> {
        if !self.restarting {
            return None;
        }
        // A restarted node comes up empty and would otherwise stop hosting
        // `Studio` — no gateway, no shard leadership, no replica for the write
        // quorum. Re-host it, or the run shrinks the cluster rather than faulting
        // it. The config carries the same scratch root, which is right: the
        // facet's restore discards stale local files, they are a cache and never
        // truth (§1), and a fresh process rebuilding over its predecessor's
        // leftovers is exactly the case worth covering.
        let config = self.config();
        Some(Arc::new(move |node: &SimNode| {
            node.granary::<Studio<SimNode>>(config.clone());
        }))
    }

    fn setup(&self, _ctx: &ClusterCtx) {}

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        let hibernating = self.hibernating;
        let restarting = self.restarting;
        let config = self.config();
        let stale = Arc::clone(&self.stale);
        let reads_verified = Arc::clone(&self.reads_verified);
        Box::pin(async move {
            let granaries: Vec<_> = nodes
                .iter()
                .map(|s| s.granary::<Studio<SimNode>>(config.clone()))
                .collect();
            let clock = nodes[0].clock().clone();
            let entropy = nodes[0].entropy().clone();
            clock.sleep(Duration::from_secs(3)).await;

            let mut tasks = Vec::new();
            for c in 0..clients {
                // Under restarts every client goes through node 1's gateway, the
                // one node the nemesis leaves alone: a handle for a restarted node
                // points at a system that has been shut down. Location
                // transparency (G13) keeps the coverage — the call still lands on
                // whichever node leads the grain's shard.
                let granary = if restarting {
                    granaries[0].clone()
                } else {
                    granaries[c % granaries.len()].clone()
                };
                let clock = clock.clone();
                let entropy = entropy.clone();
                let stale = Arc::clone(&stale);
                let reads_verified = Arc::clone(&reads_verified);
                tasks.push(async move {
                    // A small per-client key/path space; `paths[path]` tracks what
                    // this client is entitled to read back there.
                    let mut paths: std::collections::BTreeMap<(String, String), PathState> =
                        std::collections::BTreeMap::new();
                    for op in 0..ops {
                        // Workspace key shared across clients (several grains per
                        // shard); the PATH is private to this client.
                        let key = format!("ws/{}", entropy.next_u64() % 3);
                        let path = format!("c{c}/f{}.bin", entropy.next_u64() % 3);
                        let grain = granary.grain(&key);
                        if entropy.next_u64().is_multiple_of(2) {
                            // WRITE (and overwrite).
                            let bytes = content(entropy.next_u64());
                            let outcome = grain
                                .ask_timeout(
                                    Put {
                                        path: path.clone(),
                                        content: bytes.clone(),
                                    },
                                    Duration::from_secs(2),
                                )
                                .await;
                            let state = paths.entry((key, path)).or_default();
                            match outcome {
                                // Committed: this is the value, and it supersedes
                                // every earlier attempt — a late-landing ambiguous
                                // record occupies an *earlier* `Seq` slot, so it can
                                // never shadow a commit that came after it.
                                Ok(Ok(_)) => state.committed(bytes),
                                // `NotLeader` survived the bounded redirect: the
                                // append was refused before any store, and a fenced
                                // one reached no quorum either (§6, §8), so it
                                // provably did not commit and this path is unchanged.
                                Err(GrainError::NotLeader(_)) => {}
                                // Ambiguous (§7.2, §11): the record may already sit
                                // on a replica minority and be adopted by a later
                                // activation's quorum recovery. Reading it back then
                                // is correct, not stale.
                                Err(GrainError::Unavailable(_) | GrainError::Call(_)) => {
                                    state.may_have_landed(bytes)
                                }
                                // The handler ran and its capture failed. Nothing
                                // committed, but this grain writes the file before
                                // capturing it, so the bytes sit in the live
                                // materialization until the next activation wipes it
                                // and rebuilds from the committed records.
                                Ok(Err(_)) => state.may_have_landed(bytes),
                            }
                        } else if let Some(state) = paths.get(&(key.clone(), path.clone())) {
                            // READ a path this client has written: the bytes must be
                            // one this client is entitled to see there, or the call
                            // must fail (under a fault) — never another path's bytes,
                            // never a value it never wrote, never one a later commit
                            // superseded.
                            if let Ok(Ok(got)) = grain
                                .ask_timeout(Get { path: path.clone() }, Duration::from_secs(2))
                                .await
                            {
                                if !state.admits(&got) {
                                    stale.store(true, Ordering::SeqCst);
                                }
                                reads_verified.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        // Idle past the activation lifetime often enough that the
                        // next call wipes the materialization and rebuilds the
                        // directory from the committed records (F1 on the bytes).
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
        invariants.push(Box::new(self.exercised.clone()));
        invariants
    }
}

#[test]
fn workspace_reads_never_go_stale_under_the_cluster_swarm() {
    // #4: a read of an acknowledged capture returns exactly those bytes (or
    // errors), and the safety core holds, on every seeded run under partitions,
    // crashes, loss, duplication, and delay — F1 and G14 together.
    let workload = WsSwarm::new(3, 3, 8);
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..24)) {
        panic!("{failure}");
    }
    assert!(
        !workload.stale.load(Ordering::SeqCst),
        "a read returned bytes other than the last committed write (F1/G14)",
    );
    assert!(
        workload.reads_verified.load(Ordering::SeqCst) > 0,
        "no read ever returned bytes — the integrity check never ran",
    );
}

// --- The same claim across hibernation and process death ---------------------
//
// The sweep above holds every activation resident, so a read is served from the
// live directory the write landed in — the materialization is never rebuilt. This
// one tears it down: the grain passivates, its directory goes, and the next read
// is served from a tree the new activation reconstructed by applying the captured
// bytes over a composite snapshot (F1 holds on the bytes, never on re-execution),
// possibly in a process that did not exist when the write committed.

#[test]
fn workspace_reads_never_go_stale_across_hibernation_and_restarts() {
    let workload = WsSwarm::hibernating(3, 3, 8);
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..24)) {
        panic!("{failure}");
    }
    assert!(
        !workload.stale.load(Ordering::SeqCst),
        "a read returned bytes other than the last committed write (F1/G14)",
    );
    assert!(
        workload.reads_verified.load(Ordering::SeqCst) > 0,
        "no read ever returned bytes — the integrity check never ran",
    );
    workload.exercised.assert_hibernated();
}

#[test]
fn ws_hibernating_swarm_is_reproducible() {
    let workload = WsSwarm::hibernating(3, 2, 6);
    if let Err(divergence) = replay_cluster_swarm(&workload, sweep_seeds(0..12)) {
        panic!("{divergence}");
    }
}

#[test]
fn ws_swarm_is_reproducible() {
    // #7: the same seed replays to a byte-identical event stream.
    let workload = WsSwarm::new(3, 2, 6);
    if let Err(divergence) = replay_cluster_swarm(&workload, sweep_seeds(0..12)) {
        panic!("{divergence}");
    }
}

#[test]
fn ws_swarm_actually_fires_each_fault_type() {
    // #8: a green sweep of the workspace path must not be a silent happy-path run.
    let workload = WsSwarm::new(3, 3, 8);
    let stats = match run_cluster_swarm_coverage(&workload, coverage_seeds(0..32)) {
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
