//! The grain-native content-addressed blob store under the cluster fault swarm
//! (durable-workspace design; granary §7.10, V&V checklist #2, #4, #7, #8).
//!
//! `tests/blobs.rs` drives the blob store through *scripted* faults (one crash,
//! one tampered copy). This file applies the V&V doctrine the other way round: a
//! [`ClusterWorkload`] of blob put/get/has/gc traffic is swept across many seeds
//! while a seeded nemesis injects partitions, crashes, heals, loss, duplication,
//! and delay (spec §18.3) on the *unfenced* blob RPCs (`StoreBlob`, `FetchBlob`,
//! `SweepBlobs`), and a [`Checker`] watches the §13 event stream live. Four
//! properties are asserted, the way the record path asserts its own in
//! `grain_swarm.rs`:
//!
//! - **Address integrity, G17 (#2, the safety property).** Every `get` that
//!   returns bytes returns bytes whose BLAKE3 hash equals the requested id —
//!   never wrong or stale-other bytes, under any fault. Asserted in-workload over
//!   a shared flag, because the blob path is off the event stream (it carries no
//!   `Seq`/term, so it emits no `GrainEvent`), so an event-stream invariant
//!   cannot observe it. Also covers idempotent/duplicate-tolerant put: a
//!   duplication fault re-delivering a `StoreBlob` must not change the id (B2).
//! - **Safety core under faults (#4).** [`default_invariants`] — no-silent-loss,
//!   serial execution, lifecycle exactly-once — hold on every run, so the new
//!   blob RPCs integrate with §14 no-silent-loss: each fanned `StoreBlob`/
//!   `FetchBlob` ask closes its `AskIssued`/`AskOutcome` bracket even when it is
//!   the drained straggler of a committed quorum.
//! - **Seed-reproducibility (#7).** The same seed yields a byte-identical event
//!   stream ([`check_cluster_reproducible`]).
//! - **Fault coverage (#8).** Across the sweep each transport fault type actually
//!   fired ([`run_cluster_swarm_coverage`]), so a green sweep is provably not a
//!   silent happy-path sweep of the blob path.

mod support;

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
use granary::BlobId;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GranaryConfig;
use granary::GranaryExt;

use serde::Deserialize;
use serde::Serialize;
use support::Exercised;

// --- A grain that stores bytes in its colocated blob area ----------------------
//
// Its folded state is just the ids it has stored — the small metadata that
// references the bulk bytes living in `ctx.blobs()`. Fixed to `SimNode` so it
// hosts on the leader-based clustered system the shard map requires (§7.6).

#[derive(Default)]
struct BlobGrain;

#[derive(Default, Serialize, Deserialize)]
struct Stored {
    ids: Vec<BlobId>,
}

#[derive(Serialize, Deserialize)]
enum Recorded {
    Put(BlobId),
}

impl Grain for BlobGrain {
    type System = SimNode;
    type State = Stored;
    type Event = Recorded;
    type Facets = ();
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
    }
}

/// Store bytes; reply with their content id (or a durability failure as a string).
/// The bulk bytes reach a write quorum BEFORE the id is journaled (§7.10).
#[derive(Clone, Serialize, Deserialize)]
struct Put(Vec<u8>);
impl Message for Put {
    type Reply = Result<BlobId, String>;
    const MANIFEST: Manifest = Manifest::new("test.blob.Put");
}
impl GrainHandler<Put> for BlobGrain {
    async fn handle(
        &self,
        _state: &Stored,
        msg: Put,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Recorded>, Result<BlobId, String>) {
        match ctx.blobs().put(msg.0).await {
            Ok(id) => (vec![Recorded::Put(id)], Ok(id)),
            Err(e) => (vec![], Err(e.to_string())),
        }
    }
}

/// Fetch a blob; reply with the verified bytes (or a failure as a string).
#[derive(Clone, Serialize, Deserialize)]
struct Fetch(BlobId);
impl Message for Fetch {
    type Reply = Result<Vec<u8>, String>;
    const MANIFEST: Manifest = Manifest::new("test.blob.Fetch");
}
impl GrainHandler<Fetch> for BlobGrain {
    async fn handle(
        &self,
        _state: &Stored,
        msg: Fetch,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Recorded>, Result<Vec<u8>, String>) {
        (
            vec![],
            ctx.blobs()
                .get(msg.0, None)
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
impl GrainHandler<Exists> for BlobGrain {
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
impl GrainHandler<KeepOnly> for BlobGrain {
    async fn handle(
        &self,
        _state: &Stored,
        msg: KeepOnly,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Recorded>, bool) {
        let live = msg.0.into_iter().collect();
        ctx.blobs().gc(&live).await;
        (vec![], true)
    }
}

// --- The workload -------------------------------------------------------------

/// `hibernating` picks the activation lifetime: `false` keeps every activation
/// resident for the whole run, `true` makes it short enough that clients can idle
/// past it, so a grain's blob area is torn down and rebuilt mid-run. The snapshot
/// cadence follows, so a returning activation restores from a composite snapshot
/// (whose blob root set it must re-fan, §7.10) rather than from an empty base.
fn config(hibernating: bool) -> GranaryConfig {
    GranaryConfig {
        shards: 2,
        replication_factor: 3,
        idle_after: if hibernating { IDLE_AFTER } else { RESIDENT },
        snapshot_every: if hibernating { 3 } else { 8 },
        ..GranaryConfig::default()
    }
}

/// Activation lifetime for the resident sweeps: longer than any run.
const RESIDENT: Duration = Duration::from_secs(60);
/// Activation lifetime for the hibernating sweep.
const IDLE_AFTER: Duration = Duration::from_millis(200);
/// How long a hibernating client idles when it idles — comfortably past
/// [`IDLE_AFTER`], so the host really does passivate rather than nearly.
const IDLE_FOR: Duration = Duration::from_millis(500);

/// Content that varies in length and bytes with `n`, so distinct draws address
/// distinct blobs (and equal draws dedup). Kept small so a run stays cheap; the
/// multi-block path is covered by the targeted `fs_local`/`blobs` tests.
fn content(n: u64) -> Vec<u8> {
    let len = 1 + (n % 200) as usize;
    (0..len)
        .map(|i| (n.wrapping_add(i as u64) % 251) as u8)
        .collect()
}

/// Put/get/has/gc traffic against a handful of blob grains, hosted on a
/// leader-based cluster, driven through the public `GrainRef` API only (§18.4).
/// Every call is faulted by the nemesis and the transport; a failed call is
/// recorded as nothing and the client moves on, so the drive future always
/// completes and the invariants are checked over whatever the run produced.
///
/// `corrupt` and `wrong_id` are shared across every seeded run (the swarm holds
/// `&self`): a client sets `corrupt` if a `get` ever returns bytes whose hash is
/// not the requested id (a G17 violation), or `wrong_id` if a `put` ever mints an
/// id that is not the pure content hash (a B2/G18 violation). Asserted false after
/// the sweep.
struct BlobSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
    /// Idle past `idle_after` between operations, so activations passivate and
    /// rehydrate mid-run.
    hibernating: bool,
    /// Let the nemesis kill and re-launch node processes, not just isolate them.
    restarting: bool,
    corrupt: Arc<AtomicBool>,
    wrong_id: Arc<AtomicBool>,
    gets_verified: Arc<AtomicU64>,
    exercised: Exercised,
}

impl BlobSwarm {
    /// Activations resident for the whole run; the nemesis only isolates nodes.
    fn new(nodes: usize, clients: usize, ops: u64) -> BlobSwarm {
        BlobSwarm {
            nodes,
            clients,
            ops,
            hibernating: false,
            restarting: false,
            corrupt: Arc::new(AtomicBool::new(false)),
            wrong_id: Arc::new(AtomicBool::new(false)),
            gets_verified: Arc::new(AtomicU64::new(0)),
            exercised: Exercised::default(),
        }
    }

    /// Short activation lifetime, clients that idle past it, and a nemesis that
    /// kills processes — so a `get` has to serve bytes whose activation was torn
    /// down and rebuilt from a quorum since the `put`.
    fn hibernating(nodes: usize, clients: usize, ops: u64) -> BlobSwarm {
        BlobSwarm {
            hibernating: true,
            restarting: true,
            ..BlobSwarm::new(nodes, clients, ops)
        }
    }
}

impl ClusterWorkload for BlobSwarm {
    fn name(&self) -> &'static str {
        if self.hibernating {
            "granary-blob-hibernating-swarm"
        } else {
            "granary-blob-swarm"
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
        // Granary requires the leader-based control plane to host the shard map
        // (§7.6); every node is a control voter so the map group can form.
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
        // `BlobGrain` — no gateway, no shard leadership, no replica for the blob
        // write quorum. Re-host it, or the run shrinks the cluster rather than
        // faulting it.
        let hibernating = self.hibernating;
        Some(Arc::new(move |node: &SimNode| {
            node.granary::<BlobGrain>(config(hibernating));
        }))
    }

    fn setup(&self, _ctx: &ClusterCtx) {}

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        let hibernating = self.hibernating;
        let restarting = self.restarting;
        let corrupt = Arc::clone(&self.corrupt);
        let wrong_id = Arc::clone(&self.wrong_id);
        let gets_verified = Arc::clone(&self.gets_verified);
        Box::pin(async move {
            let granaries: Vec<_> = nodes
                .iter()
                .map(|s| s.granary::<BlobGrain>(config(hibernating)))
                .collect();
            let clock = nodes[0].clock().clone();
            let entropy = nodes[0].entropy().clone();
            // Let the control-plane and shard groups elect before traffic.
            clock.sleep(Duration::from_secs(3)).await;

            let mut tasks = Vec::new();
            for c in 0..clients {
                // Under restarts every client goes through node 1's gateway, the
                // one node the nemesis leaves alone: a handle for a restarted node
                // points at a system that has been shut down. Location
                // transparency (G13) means this costs no coverage — the call still
                // lands on whichever node leads the grain's shard.
                let granary = if restarting {
                    granaries[0].clone()
                } else {
                    granaries[c % granaries.len()].clone()
                };
                let clock = clock.clone();
                let entropy = entropy.clone();
                let corrupt = Arc::clone(&corrupt);
                let wrong_id = Arc::clone(&wrong_id);
                let gets_verified = Arc::clone(&gets_verified);
                tasks.push(async move {
                    // What this client has successfully put: `(key, id, bytes)`,
                    // so a later get can verify the bytes it gets back are the
                    // exact bytes it stored under that id (catches misrouting too).
                    let mut mine: Vec<(String, BlobId, Vec<u8>)> = Vec::new();
                    for op in 0..ops {
                        // A small key space so several grains share each shard.
                        let key = format!("ws/{}", entropy.next_u64() % 4);
                        let grain = granary.grain(&key);
                        if entropy.next_u64().is_multiple_of(2) || mine.is_empty() {
                            // PUT: a short deadline so a faulted call fails fast.
                            let bytes = content(entropy.next_u64());
                            let want = BlobId::of(&bytes);
                            if let Ok(Ok(id)) = grain
                                .ask_timeout(Put(bytes.clone()), Duration::from_secs(2))
                                .await
                            {
                                // B2/G18: the id is the pure content hash, always.
                                if id != want {
                                    wrong_id.store(true, Ordering::SeqCst);
                                }
                                mine.push((key, id, bytes));
                            }
                        } else {
                            // GET a blob this client put earlier (possibly via a
                            // different node's handle than the put used).
                            let (k, id, bytes) = &mine[(entropy.next_u64() as usize) % mine.len()];
                            let grain = granary.grain(k);
                            if let Ok(Ok(got)) =
                                grain.ask_timeout(Fetch(*id), Duration::from_secs(2)).await
                            {
                                // G17: returned bytes hash to the requested id, and
                                // are the exact bytes stored under it — never wrong,
                                // stale-other, or another grain's blob.
                                if BlobId::of(&got) != *id || &got != bytes {
                                    corrupt.store(true, Ordering::SeqCst);
                                }
                                gets_verified.fetch_add(1, Ordering::SeqCst);
                                // Occasionally also assert presence and prune.
                                let _ =
                                    grain.ask_timeout(Exists(*id), Duration::from_secs(2)).await;
                            }
                        }
                        // Idle past the activation lifetime often enough that the
                        // next call rehydrates the grain and re-fans its blob root
                        // set, on whichever node leads the shard by then.
                        if hibernating && op % 2 == 1 {
                            clock.sleep(IDLE_FOR).await;
                        }
                    }
                    // A best-effort GC pass keeping everything live: exercises the
                    // SweepBlobs fan-out under faults without dropping a live blob.
                    if !mine.is_empty() {
                        let live: Vec<BlobId> = mine.iter().map(|(_, id, _)| *id).collect();
                        let grain = granary.grain(&mine[0].0);
                        let _ = grain
                            .ask_timeout(KeepOnly(live), Duration::from_secs(2))
                            .await;
                    }
                });
            }
            futures::future::join_all(tasks).await;
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        // The safety core observes the blob RPCs as ordinary asks to the
        // per-node ReplicaStore actors: no-silent-loss covers the drained
        // straggler asks, serial-execution the replica store's mailbox.
        let mut invariants = default_invariants();
        invariants.push(Box::new(self.exercised.clone()));
        invariants
    }
}

#[test]
fn blob_integrity_holds_under_the_cluster_swarm() {
    // #2/#4: a `get` never returns wrong bytes and a `put` never mints a wrong id,
    // and the safety core holds, on every seeded run under partitions, crashes,
    // loss, duplication, and delay.
    let workload = BlobSwarm::new(3, 3, 8);
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..24)) {
        panic!("{failure}");
    }
    assert!(
        !workload.corrupt.load(Ordering::SeqCst),
        "a get returned bytes that did not verify against the requested id (G17)",
    );
    assert!(
        !workload.wrong_id.load(Ordering::SeqCst),
        "a put minted an id that was not the content hash (B2/G18)",
    );
    // The integrity assertions are vacuous unless gets actually returned bytes —
    // prove the sweep exercised the verified read path at least once.
    assert!(
        workload.gets_verified.load(Ordering::SeqCst) > 0,
        "no get ever returned bytes — the integrity check never ran",
    );
}

// --- The same traffic across hibernation and process death -------------------
//
// The sweep above holds every activation resident, so a `get` is always served by
// the activation that took the `put`. This one tears that down: the grain
// passivates, its blob area goes with it, and the next `get` must rebuild from a
// composite snapshot and re-fan the root set from a write quorum — on a node that
// may have been a different process entirely a moment ago. G17 (returned bytes
// hash to the requested id) and B2/G18 (the id is the content hash) are the same
// claims, now against a rebuilt blob area.

#[test]
fn blob_integrity_holds_across_hibernation_and_restarts() {
    let workload = BlobSwarm::hibernating(3, 3, 8);
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..24)) {
        panic!("{failure}");
    }
    assert!(
        !workload.corrupt.load(Ordering::SeqCst),
        "a get returned bytes that did not verify against the requested id (G17)",
    );
    assert!(
        !workload.wrong_id.load(Ordering::SeqCst),
        "a put minted an id that was not the content hash (B2/G18)",
    );
    assert!(
        workload.gets_verified.load(Ordering::SeqCst) > 0,
        "no get ever returned bytes — the integrity check never ran",
    );
    workload.exercised.assert_hibernated();
}

#[test]
fn blob_hibernating_swarm_is_reproducible() {
    let workload = BlobSwarm::hibernating(3, 2, 6);
    if let Err(divergence) = replay_cluster_swarm(&workload, sweep_seeds(0..12)) {
        panic!("{divergence}");
    }
}

#[test]
fn blob_swarm_is_reproducible() {
    // #7: the same seed replays to a byte-identical event stream, even under
    // cluster nemesis and transport faults on the blob RPCs.
    let workload = BlobSwarm::new(3, 2, 6);
    if let Err(divergence) = replay_cluster_swarm(&workload, sweep_seeds(0..12)) {
        panic!("{divergence}");
    }
}

#[test]
fn blob_swarm_actually_fires_each_fault_type() {
    // #8: a green sweep of the blob path must not be a silent happy-path sweep.
    // Across the seed range the transport injected loss, duplication, reordering
    // (delay), and partition/crash blocking on the blob RPCs at least once each.
    let workload = BlobSwarm::new(3, 3, 8);
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
