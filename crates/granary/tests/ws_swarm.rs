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
use actor_simulation::SimCluster;
use actor_simulation::check_cluster_reproducible;
use actor_simulation::default_invariants;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_cluster_swarm_coverage;
use granary::Grain;
use granary::GrainCtx;
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

/// Write/read/overwrite traffic against a handful of workspace grains, driven
/// through the public `GrainRef` API only (§18.4). Each client writes only paths
/// in its own subtree (`c{client}/...`), so a read of a path it wrote has a
/// single well-defined expected value — its last committed write — with no
/// cross-client race. A faulted call is recorded as nothing and the client moves
/// on, so the drive future always completes.
///
/// `stale` is shared across every seeded run: a client sets it if a read of one
/// of its own paths ever returns bytes other than its last committed write to
/// that path (an F1/G14 violation — a lost or stale-shadowed acknowledged
/// capture).
struct WsSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
    /// One scratch root for the whole sweep; `Host::facet_env` keys
    /// materializations by node and grain beneath it, and every activation
    /// restores through a wipe, so seeded runs cannot contaminate each other.
    scratch: tempfile::TempDir,
    stale: Arc<AtomicBool>,
    reads_verified: Arc<AtomicU64>,
}

impl WsSwarm {
    fn new(nodes: usize, clients: usize, ops: u64) -> WsSwarm {
        WsSwarm {
            nodes,
            clients,
            ops,
            scratch: tempfile::tempdir().expect("scratch tempdir"),
            stale: Arc::new(AtomicBool::new(false)),
            reads_verified: Arc::new(AtomicU64::new(0)),
        }
    }

    fn config(&self) -> GranaryConfig {
        GranaryConfig {
            shards: 2,
            replication_factor: 3,
            idle_after: Duration::from_secs(60),
            snapshot_every: 8,
            data_dir: Some(self.scratch.path().to_path_buf()),
            ..GranaryConfig::default()
        }
    }
}

impl ClusterWorkload for WsSwarm {
    fn name(&self) -> &'static str {
        "granary-ws-swarm"
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

    fn setup(&self, _ctx: &ClusterCtx) {}

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimCluster> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        let config = self.config();
        let stale = Arc::clone(&self.stale);
        let reads_verified = Arc::clone(&self.reads_verified);
        Box::pin(async move {
            let granaries: Vec<_> = nodes
                .iter()
                .map(|s| s.granary::<Studio<SimCluster>>(config.clone()))
                .collect();
            let clock = nodes[0].clock().clone();
            let entropy = nodes[0].entropy().clone();
            clock.sleep(Duration::from_secs(3)).await;

            let mut tasks = Vec::new();
            for c in 0..clients {
                let granary = granaries[c % granaries.len()].clone();
                let entropy = entropy.clone();
                let stale = Arc::clone(&stale);
                let reads_verified = Arc::clone(&reads_verified);
                tasks.push(async move {
                    // A small per-client key/path space; `last[path]` is this
                    // client's last committed write to that path.
                    let mut last: std::collections::BTreeMap<(String, String), Vec<u8>> =
                        std::collections::BTreeMap::new();
                    for _ in 0..ops {
                        // Workspace key shared across clients (several grains per
                        // shard); the PATH is private to this client.
                        let key = format!("ws/{}", entropy.next_u64() % 3);
                        let path = format!("c{c}/f{}.bin", entropy.next_u64() % 3);
                        let grain = granary.grain(&key);
                        if entropy.next_u64().is_multiple_of(2) {
                            // WRITE (and overwrite): record only on a committed ack.
                            let bytes = content(entropy.next_u64());
                            if let Ok(Ok(_)) = grain
                                .ask_timeout(
                                    Put {
                                        path: path.clone(),
                                        content: bytes.clone(),
                                    },
                                    Duration::from_secs(2),
                                )
                                .await
                            {
                                last.insert((key, path), bytes);
                            }
                        } else if let Some(want) = last.get(&(key.clone(), path.clone())) {
                            // READ a path this client has committed: the bytes must
                            // equal its last committed write, or the call must fail
                            // (under a fault) — never stale or wrong bytes.
                            if let Ok(Ok(got)) = grain
                                .ask_timeout(Get { path: path.clone() }, Duration::from_secs(2))
                                .await
                            {
                                if &got != want {
                                    stale.store(true, Ordering::SeqCst);
                                }
                                reads_verified.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    }
                });
            }
            futures::future::join_all(tasks).await;
            // Settle before the workload reports done: `on_activate` launches
            // root-driven blob repair (§7.10 B6) — a background task that
            // issues blob asks (and drains their stragglers). A repair pass
            // kicked late by a re-activation can still be in flight when
            // traffic ends. No-silent-loss (#1) requires every issued ask to
            // *reach an outcome*, so give those background asks a window
            // (> the 2s timeout) to resolve before quiescence is measured.
            clock.sleep(Duration::from_secs(5)).await;
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        default_invariants()
    }
}

#[test]
fn workspace_reads_never_go_stale_under_the_cluster_swarm() {
    // #4: a read of an acknowledged capture returns exactly those bytes (or
    // errors), and the safety core holds, on every seeded run under partitions,
    // crashes, loss, duplication, and delay — F1 and G14 together.
    let workload = WsSwarm::new(3, 3, 8);
    if let Err(failure) = run_cluster_swarm(&workload, 0..24) {
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

#[test]
fn ws_swarm_is_reproducible() {
    // #7: the same seed replays to a byte-identical event stream.
    let workload = WsSwarm::new(3, 2, 6);
    for seed in 0..12 {
        if let Err(divergence) = check_cluster_reproducible(&workload, seed) {
            panic!("{divergence}");
        }
    }
}

#[test]
fn ws_swarm_actually_fires_each_fault_type() {
    // #8: a green sweep of the workspace path must not be a silent happy-path run.
    let workload = WsSwarm::new(3, 3, 8);
    let stats = match run_cluster_swarm_coverage(&workload, 0..32) {
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
