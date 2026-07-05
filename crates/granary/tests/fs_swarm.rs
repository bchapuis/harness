//! The durable workspace filesystem grain under the cluster fault swarm
//! (durable-workspace design; granary §7.10, V&V checklist #4, #7, #8).
//!
//! `tests/fs_clustered.rs` drives the workspace through *scripted* faults (one
//! crash, one partition, then repair). This file sweeps a [`ClusterWorkload`] of
//! write/read/overwrite traffic across many seeds while a seeded nemesis injects
//! partitions, crashes, heals, loss, duplication, and delay (spec §18.3), so the
//! end-to-end product path — metadata journaled on the record quorum, file blocks
//! on the unfenced blob quorum (§7.10) — is exercised together under the full
//! fault matrix. A [`Checker`] watches the §13 event stream live.
//!
//! - **Read integrity under faults (#4, the safety property).** Each client owns a
//!   private path subtree, so there are no cross-client overwrite races; a read of
//!   a path this client wrote returns the *exact* bytes of its last committed
//!   write to that path, or an error — never stale, partial, or another path's
//!   bytes. This jointly asserts blob address integrity (G17) and metadata
//!   lossless failover (G14): an acknowledged write's new slice is never lost or
//!   shadowed by a stale one across a leadership change. Asserted in-workload over
//!   a shared flag (the blob path is off the event stream).
//! - **Safety core under faults (#4).** [`default_invariants`] hold on every run.
//! - **Seed-reproducibility (#7).** The same seed replays byte-identically.
//! - **Fault coverage (#8).** Each transport fault type actually fired.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::SimCluster;
use actor_simulation::check_cluster_reproducible;
use actor_simulation::default_invariants;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_cluster_swarm_coverage;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::fs::Workspace;
use granary::fs::ReadFile;
use granary::fs::WriteFile;

type Ws = Workspace<SimCluster>;

fn config() -> GranaryConfig {
    GranaryConfig {
        shards: 2,
        replication_factor: 3,
        idle_after: Duration::from_secs(60),
        snapshot_every: 8,
        ..GranaryConfig::default()
    }
}

/// Content that varies in length and bytes with `n`. Kept small so a run stays
/// cheap; the multi-block boundary is covered by the targeted `fs_local` tests.
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
/// that path (a G14/G17 violation — a lost or stale-shadowed acknowledged write).
struct FsSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
    stale: Arc<AtomicBool>,
    reads_verified: Arc<AtomicU64>,
}

impl FsSwarm {
    fn new(nodes: usize, clients: usize, ops: u64) -> FsSwarm {
        FsSwarm {
            nodes,
            clients,
            ops,
            stale: Arc::new(AtomicBool::new(false)),
            reads_verified: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl ClusterWorkload for FsSwarm {
    fn name(&self) -> &'static str {
        "granary-fs-swarm"
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
        let stale = Arc::clone(&self.stale);
        let reads_verified = Arc::clone(&self.reads_verified);
        Box::pin(async move {
            let granaries: Vec<_> = nodes.iter().map(|s| s.granary::<Ws>(config())).collect();
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
                                    WriteFile {
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
                                .ask_timeout(
                                    ReadFile {
                                        path: path.clone(),
                                        range: None,
                                    },
                                    Duration::from_secs(2),
                                )
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
            // Settle before the workload reports done: unlike the record path,
            // the Fs grain's `on_activate` launches grain-driven blob repair
            // (§7.10 B6) — a background task that issues `FetchBlob`/`StoreBlob`
            // asks (and drains their stragglers, each a `QUORUM_TIMEOUT`). A
            // repair pass kicked late by a re-activation can still be in flight
            // when traffic ends. No-silent-loss (#1) requires every issued ask to
            // *reach an outcome*, not to finish instantly, so we give those
            // background asks a window (> the 2s timeout) to resolve — to a value,
            // `Unreachable`, or timeout — before quiescence is measured. A still-
            // pending ask after this settle would be a genuine silent loss.
            clock.sleep(Duration::from_secs(5)).await;
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        default_invariants()
    }
}

#[test]
fn workspace_reads_never_go_stale_under_the_cluster_swarm() {
    // #4: a read of an acknowledged write returns exactly those bytes (or errors),
    // and the safety core holds, on every seeded run under partitions, crashes,
    // loss, duplication, and delay — G14 (metadata) and G17 (blocks) together.
    let workload = FsSwarm::new(3, 3, 8);
    if let Err(failure) = run_cluster_swarm(&workload, 0..24) {
        panic!("{failure}");
    }
    assert!(
        !workload.stale.load(Ordering::SeqCst),
        "a read returned bytes other than the last committed write (G14/G17)",
    );
    assert!(
        workload.reads_verified.load(Ordering::SeqCst) > 0,
        "no read ever returned bytes — the integrity check never ran",
    );
}

#[test]
fn fs_swarm_is_reproducible() {
    // #7: the same seed replays to a byte-identical event stream.
    let workload = FsSwarm::new(3, 2, 6);
    for seed in 0..12 {
        if let Err(divergence) = check_cluster_reproducible(&workload, seed) {
            panic!("{divergence}");
        }
    }
}

#[test]
fn fs_swarm_actually_fires_each_fault_type() {
    // #8: a green sweep of the workspace path must not be a silent happy-path run.
    let workload = FsSwarm::new(3, 3, 8);
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
