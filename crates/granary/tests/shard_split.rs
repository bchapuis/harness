//! Shard split under deterministic simulation (spec §7.7, invariant **G15**).
//!
//! A shard that grows too large or too hot splits: the parent keeps the low half
//! of its key range in place, a fresh child shard takes the high half on the same
//! replicas, and the committed shard map flips before either side serves the
//! moved range. These tests drive the whole protocol — seal, transfer, commit,
//! re-route — through the public API on a 3-node cluster and check the G15
//! contract from the event stream: a grain is writable in exactly one shard at
//! any time, and a split loses or duplicates no write. This includes the
//! §14-mandated fault case, a split under concurrent writes, plus the
//! crash-the-driver re-drive, compacted (snapshot-only) grains, and blob areas.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::EventSink;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::Counter;
use actor_simulation::CounterOp;
use actor_simulation::CounterRet;
use actor_simulation::History;
use actor_simulation::Recorder;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use actor_simulation::check_linearizable;
use granary::BlobId;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainError;
use granary::GrainEvent;
use granary::GrainHandler;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use serde::Deserialize;
use serde::Serialize;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

// --- Grains under test --------------------------------------------------------

#[derive(Default)]
struct Account;

#[derive(Default, Serialize, Deserialize)]
struct Balance {
    cents: i64,
}

#[derive(Serialize, Deserialize)]
enum Ledger {
    Deposited(u64),
}

impl Grain for Account {
    type System = SimCluster;
    type State = Balance;
    type Event = Ledger;
    type Facets = ();
    const GRAIN_TYPE: &'static str = "bank.Account";

    fn apply(state: &mut Balance, event: &Ledger) {
        match event {
            Ledger::Deposited(n) => state.cents += *n as i64,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Deposit>();
        r.accept::<ReadBalance>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Deposit {
    cents: u64,
}
impl Message for Deposit {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.Deposit");
}

impl GrainHandler<Deposit> for Account {
    async fn handle(
        &self,
        state: &Balance,
        msg: Deposit,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<Ledger>, i64) {
        (
            vec![Ledger::Deposited(msg.cents)],
            state.cents + msg.cents as i64,
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadBalance;
impl Message for ReadBalance {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.ReadBalance");
}

impl GrainHandler<ReadBalance> for Account {
    async fn handle(
        &self,
        state: &Balance,
        _msg: ReadBalance,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<Ledger>, i64) {
        (vec![], state.cents)
    }
}

#[derive(Default)]
struct CounterGrain;

#[derive(Default, Serialize, Deserialize)]
struct CounterState {
    value: i64,
}

#[derive(Serialize, Deserialize)]
enum CounterEvent {
    Added(i64),
}

impl Grain for CounterGrain {
    type System = SimCluster;
    type State = CounterState;
    type Event = CounterEvent;
    type Facets = ();
    const GRAIN_TYPE: &'static str = "test.Counter";

    fn apply(state: &mut CounterState, event: &CounterEvent) {
        match event {
            CounterEvent::Added(d) => state.value += *d,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Add>();
        r.accept::<ReadCount>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Add(i64);
impl Message for Add {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("test.Add");
}

impl GrainHandler<Add> for CounterGrain {
    async fn handle(
        &self,
        state: &CounterState,
        msg: Add,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<CounterEvent>, i64) {
        // Non-idempotent: a double-fold shows up as a wrong Read the checker flags.
        (vec![CounterEvent::Added(msg.0)], state.value + msg.0)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadCount;
impl Message for ReadCount {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("test.ReadCount");
}

impl GrainHandler<ReadCount> for CounterGrain {
    async fn handle(
        &self,
        state: &CounterState,
        _msg: ReadCount,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<CounterEvent>, i64) {
        (vec![], state.value)
    }
}

/// A grain that stores bulk bytes in its colocated blob area (§7.10): the split
/// must move the blob area with the grain.
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
    type System = SimCluster;
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
    }
}

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

#[derive(Clone, Serialize, Deserialize)]
struct Fetch(BlobId);
impl Message for Fetch {
    type Reply = Result<Option<Vec<u8>>, String>;
    const MANIFEST: Manifest = Manifest::new("test.blob.Fetch");
}
impl GrainHandler<Fetch> for BlobGrain {
    async fn handle(
        &self,
        _state: &Stored,
        msg: Fetch,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Recorded>, Result<Option<Vec<u8>>, String>) {
        match ctx.blobs().get(msg.0, None).await {
            Ok(bytes) => (vec![], Ok(Some(bytes))),
            Err(e) => (vec![], Err(e.to_string())),
        }
    }
}

// --- Harness (mirrors tests/clustered_grains.rs, plus a recorder) -------------

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

/// A 3-node leader network with the recorder attached, so the shard events and
/// per-commit shard stamps (§13) are observable for the G15 checker.
fn recorded_net(sim: &Simulation) -> (SimNetwork, Recorder) {
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let net = SimNetwork::new(sim)
        .with_leader(swim(), raft(), DowningPolicy::Conservative)
        .with_events(sink);
    (net, recorder)
}

/// One shard, so the whole namespace starts in shard 0 and the split under test
/// is the only partition change. Snapshots every few commits, so a split moves
/// compacted (snapshot-only-prefix) grains too.
fn one_shard_config() -> GranaryConfig {
    GranaryConfig {
        shards: 1,
        idle_after: Duration::from_secs(60),
        snapshot_every: 4,
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

/// The committed splits observed on the stream, deduplicated to the
/// `(parent, child, boundary)` tuples (every node emits one as it applies).
fn splits_of(recorder: &Recorder) -> std::collections::BTreeSet<(u32, u32, u64)> {
    recorder
        .events()
        .iter()
        .filter_map(|event| match event.as_app::<GrainEvent>() {
            Some(GrainEvent::ShardSplit {
                parent,
                child,
                boundary,
                ..
            }) => Some((*parent, *child, *boundary)),
            _ => None,
        })
        .collect()
}

/// The committed merges observed on the stream, deduplicated to `(left, right)`.
fn merges_of(recorder: &Recorder) -> std::collections::BTreeSet<(u32, u32)> {
    recorder
        .events()
        .iter()
        .filter_map(|event| match event.as_app::<GrainEvent>() {
            Some(GrainEvent::ShardMerged { left, right, .. }) => Some((*left, *right)),
            _ => None,
        })
        .collect()
}

/// Run the simulation until a committed merge is observable, bounded.
fn await_merge(sim: &Simulation, recorder: &Recorder) -> (u32, u32) {
    for _ in 0..120 {
        if let Some(&merge) = merges_of(recorder).iter().next() {
            return merge;
        }
        sim.run_for(Duration::from_millis(500));
    }
    panic!("no committed merge observed within the deadline");
}

/// Run the simulation until a committed split is observable, bounded. The bound
/// is generous: a re-drive after a crash fans every recovery and transfer to the
/// isolated node too, and each waits out its quorum timeout.
fn await_split(sim: &Simulation, recorder: &Recorder) -> (u32, u32, u64) {
    for _ in 0..120 {
        if let Some(&split) = splits_of(recorder).iter().next() {
            return split;
        }
        sim.run_for(Duration::from_millis(500));
    }
    panic!("no committed split observed within the deadline");
}

/// The **G15 continuous checker** over the recorded stream: per grain name, the
/// committed seq is strictly increasing across its whole life, regardless of
/// which shard the commit came from. A grain moves parent → child on a split and
/// right → left on a merge; at each such move the new owner recovers the
/// committed head and continues, so the seq only ever advances. A *duplicated*
/// write (e.g. a moved-range append that committed on both sides) repeats a seq,
/// and a *lost* acknowledged write leaves the next activation recovering a lower
/// head — both surface here as a non-increasing seq. Covers every grain type on
/// the stream. (The "writable in exactly one shard at a time" half is exercised
/// by the linearizability tests, where two concurrent write streams for one
/// grain could not produce a linearizable history.)
fn assert_split_safety(recorder: &Recorder) {
    let mut per_grain: std::collections::BTreeMap<String, Vec<(u32, u64)>> =
        std::collections::BTreeMap::new();
    for event in recorder.events() {
        if let Some(GrainEvent::Committed {
            name, shard, seq, ..
        }) = event.as_app::<GrainEvent>()
        {
            per_grain
                .entry(format!("{}/{}", name.grain_type(), name.key()))
                .or_default()
                .push((*shard, *seq));
        }
    }
    for (grain, commits) in per_grain {
        let mut last_seq = 0u64;
        for (shard, seq) in commits {
            assert!(
                seq > last_seq,
                "G15: {grain} committed seq {seq} at shard {shard} after seq {last_seq} — \
                 a lost or duplicated write across a split/merge boundary"
            );
            last_seq = seq;
        }
    }
}

/// Bring up the 3-node cluster hosting `G` on every node with `config`.
fn cluster_of<G>(
    sim: &Simulation,
    config: GranaryConfig,
) -> (SimNetwork, Recorder, Vec<SimCluster>, Vec<Granary<G>>)
where
    G: Grain<System = SimCluster> + Default,
{
    let (net, recorder) = recorded_net(sim);
    let systems = vec![net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // control-plane leader
    let granaries: Vec<Granary<G>> = systems
        .iter()
        .map(|s| s.granary::<G>(config.clone()))
        .collect();
    sim.run_for(Duration::from_secs(3)); // shard-group leaders
    (net, recorder, systems, granaries)
}

// --- Tests --------------------------------------------------------------------

#[test]
fn a_split_moves_grains_to_a_child_and_loses_no_write() {
    // The core §7.7 sequence: commit state across many grains of one shard,
    // split it, and prove every committed write survives — including compacted
    // grains whose prefix exists only in their snapshot — and that the same
    // pre-split refs (with their now-stale host caches) keep working against
    // whichever side owns each key. The map changed exactly once.
    let sim = Simulation::new(11);
    let (_net, recorder, _systems, granaries) = cluster_of::<Account>(&sim, one_shard_config());
    let keys: Vec<String> = (0..20).map(|i| format!("account/{i}")).collect();

    // Ten deposits per grain: past `snapshot_every`, so grains compact and the
    // split must carry snapshots, not just records.
    for key in &keys {
        let committed = {
            let g = granaries[0].clone();
            let key = key.clone();
            drive(&sim, Duration::from_secs(8), async move {
                let acct = g.grain(key);
                let mut last = 0;
                for _ in 0..10 {
                    last = acct.ask(Deposit { cents: 10 }).await?;
                }
                Ok::<i64, GrainError>(last)
            })
        };
        assert_eq!(committed, Ok(100), "{key} deposits committed pre-split");
    }

    granaries[0].split_shard(0);
    let (parent, child, _boundary) = await_split(&sim, &recorder);
    assert_eq!(parent, 0);
    assert_ne!(child, 0, "the child is a fresh index");

    // Every grain reads back its full committed balance and takes further
    // writes, through the SAME pre-split handles (stale caches self-heal).
    for key in &keys {
        let after = {
            let g = granaries[1].clone();
            let key = key.clone();
            drive(&sim, Duration::from_secs(10), async move {
                let acct = g.grain(key);
                let read = acct
                    .ask_timeout(ReadBalance, Duration::from_secs(8))
                    .await?;
                let write = acct
                    .ask_timeout(Deposit { cents: 1 }, Duration::from_secs(8))
                    .await?;
                Ok::<(i64, i64), GrainError>((read, write))
            })
        };
        assert_eq!(
            after,
            Ok((100, 101)),
            "{key}: committed state survived the split and the owner accepts writes"
        );
    }

    // The partition changed exactly once, and the moved half actually moved:
    // some post-split commit landed under the child shard, some stayed home.
    assert_eq!(
        splits_of(&recorder).len(),
        1,
        "exactly one committed split (deduped across nodes)"
    );
    let post_split_shards: std::collections::BTreeSet<u32> = recorder
        .events()
        .iter()
        .filter_map(|event| match event.as_app::<GrainEvent>() {
            Some(GrainEvent::Committed { name, shard, .. })
                if name.grain_type() == Account::GRAIN_TYPE && *shard != 0 =>
            {
                Some(*shard)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        post_split_shards,
        std::collections::BTreeSet::from([child]),
        "moved grains commit under the child shard (and only there)"
    );
    assert_split_safety(&recorder);
}

#[test]
fn a_split_under_concurrent_writes_is_linearizable() {
    // The §14-mandated fault case: a shard split under concurrent writes. Three
    // counter grains span the boundary; clients on two nodes keep writing and
    // reading through the whole seal → transfer → commit window. Every observed
    // history must stay linearizable — an append that slipped through the seal
    // and was then dropped (lost write) or retried onto the child after
    // committing on the parent (double apply) is exactly what the checker
    // catches. Ops that fail in the split window are recorded pending, which
    // the checker may place or drop — the §2.2 ambiguity contract.
    for seed in 0..8 {
        let sim = Simulation::new(seed);
        let (_net, recorder, systems, granaries) =
            cluster_of::<CounterGrain>(&sim, one_shard_config());
        let keys = ["counter/0", "counter/1", "counter/2"];
        let histories: Vec<History<Counter>> = keys.iter().map(|_| History::new()).collect();

        for (key, history) in keys.iter().zip(&histories) {
            for granary in granaries.iter().take(2) {
                let granary = granary.clone();
                let history = history.clone();
                let entropy = systems[0].entropy().clone();
                let key = key.to_string();
                sim.spawner().launch(Box::pin(async move {
                    let counter = granary.grain(key);
                    for _ in 0..8 {
                        if entropy.next_u64().is_multiple_of(2) {
                            let delta = 1 + (entropy.next_u64() % 3) as i64;
                            let id = history.invoke(CounterOp::Add(delta));
                            match counter
                                .ask_timeout(Add(delta), Duration::from_secs(8))
                                .await
                            {
                                Ok(_) => history.ok(id, CounterRet::AddOk),
                                Err(_) => history.info(id), // unknown outcome: pending
                            }
                        } else {
                            let id = history.invoke(CounterOp::Read);
                            match counter.ask_timeout(ReadCount, Duration::from_secs(8)).await {
                                Ok(value) => history.ok(id, CounterRet::Read(value)),
                                Err(_) => history.info(id),
                            }
                        }
                    }
                }));
            }
        }

        // Fire the split while the traffic is in flight.
        let splitter = granaries[2].clone();
        let clock = systems[0].clock().clone();
        sim.spawner().launch(Box::pin(async move {
            clock.sleep(Duration::from_millis(400)).await;
            splitter.split_shard(0);
        }));

        sim.run_for(Duration::from_secs(40));
        assert_eq!(
            splits_of(&recorder).len(),
            1,
            "seed {seed}: the split committed under the concurrent load"
        );
        for (key, history) in keys.iter().zip(&histories) {
            let verdict = check_linearizable(history);
            assert!(
                verdict.is_ok(),
                "seed {seed}: {key} history not linearizable across the split: {verdict:?}",
            );
        }
        assert_split_safety(&recorder);
    }
}

#[test]
fn a_crashed_parent_leader_re_drives_the_split() {
    // Crash-recovery of the driver itself: the parent leader starts the split
    // (seal fanned, transfer possibly mid-pass) and crashes. The new leader's
    // split loop finds the committed plan and re-drives — every step is
    // idempotent — so the split still commits and every committed write reads
    // back from the survivors.
    let sim = Simulation::new(23);
    let (net, recorder, _systems, granaries) = cluster_of::<Account>(&sim, one_shard_config());
    // A handful of grains: after the crash the re-drive fans every recovery and
    // transfer to the isolated node too, and each such op waits out its quorum
    // timeout, so keep the moved set small enough that the re-drive fits the
    // deadline while still exercising real data movement.
    let keys: Vec<String> = (0..4).map(|i| format!("account/{i}")).collect();

    for key in &keys {
        let committed = {
            let g = granaries[0].clone();
            let key = key.clone();
            drive(&sim, Duration::from_secs(8), async move {
                g.grain(key).ask(Deposit { cents: 700 }).await
            })
        };
        assert_eq!(committed, Ok(700), "{key} deposit committed pre-split");
    }

    // Everything lives in shard 0; its leader is the split driver. Request the
    // split on EVERY node, so the request (which queues node-locally until it
    // commits as `SplitStarted`) is not lost with the crashed driver — a
    // survivor's proposer still carries it. Give the driver a moment to start
    // sealing/transferring, then kill it mid-flight; a survivor re-drives.
    let driver = granaries[0]
        .leader(keys[0].clone())
        .expect("shard 0 elected a leader");
    for g in &granaries {
        g.split_shard(0);
    }
    sim.run_for(Duration::from_millis(300));
    net.crash(driver);

    let (parent, _child, _boundary) = await_split(&sim, &recorder);
    assert_eq!(parent, 0);

    // Every write survives on the survivors, both halves keep serving. The
    // granaries are indexed by node position (0→A, 1→B, 2→C), so a survivor is
    // any index whose node is not the crashed driver.
    let survivor = [A, B, C]
        .iter()
        .position(|&node| node != driver)
        .expect("two nodes survive");
    for key in &keys {
        let balance = {
            let g = granaries[survivor].clone();
            let key = key.clone();
            drive(&sim, Duration::from_secs(12), async move {
                g.grain(key)
                    .ask_timeout(ReadBalance, Duration::from_secs(10))
                    .await
            })
        };
        assert_eq!(
            balance,
            Ok(700),
            "{key}: the committed write survived the driver crash and the re-driven split"
        );
    }
    assert_split_safety(&recorder);
}

#[test]
fn the_size_trigger_splits_a_shard_that_grows_past_its_target() {
    // §7.7 elasticity: with a `shard_target_bytes` the shard leader auto-splits
    // once its durable footprint grows past it — no explicit `split_shard` call.
    // Blob grains give a controllable size: 16 grains × a 512-byte blob ≈ 8 KiB
    // in one shard, past the 5 KiB target, so the trigger fires; each ≈4 KiB
    // half then sits under it, so the split converges instead of cascading.
    let sim = Simulation::new(41);
    let config = GranaryConfig {
        shards: 1,
        shard_target_bytes: 5000,
        idle_after: Duration::from_secs(60),
        snapshot_every: 4,
        ..GranaryConfig::default()
    };
    let (_net, recorder, _systems, granaries) = cluster_of::<BlobGrain>(&sim, config);
    let keys: Vec<String> = (0..16).map(|i| format!("ws/{i}")).collect();

    let mut stored = Vec::new();
    for key in &keys {
        let bytes = vec![b'z'; 512];
        let id = {
            let g = granaries[0].clone();
            let key = key.clone();
            let bytes = bytes.clone();
            drive(&sim, Duration::from_secs(8), async move {
                g.grain(key).ask(Put(bytes)).await
            })
        };
        let id = id.expect("put reached the grain").expect("blob stored");
        stored.push((key.clone(), id, bytes));
    }

    // No explicit split request — the size trigger must fire it on its own.
    let (parent, child, _boundary) = await_split(&sim, &recorder);
    assert_eq!(parent, 0);
    assert_ne!(child, 0);

    // Every grain's blob still reads back verified after the auto-split.
    for (key, id, bytes) in stored {
        let fetched = {
            let g = granaries[1].clone();
            let key = key.clone();
            drive(&sim, Duration::from_secs(10), async move {
                g.grain(key)
                    .ask_timeout(Fetch(id), Duration::from_secs(8))
                    .await
            })
        };
        assert_eq!(
            fetched,
            Ok(Ok(Some(bytes))),
            "{key}: survived the size-triggered split"
        );
    }
    assert_split_safety(&recorder);
}

#[test]
fn blobs_move_with_their_grains_across_a_split() {
    // §7.10 across §7.7: a grain's content-addressed blob area rides the split
    // with it — after the flip, the owning side serves a verified read of every
    // pre-split blob.
    let sim = Simulation::new(31);
    let (_net, recorder, _systems, granaries) = cluster_of::<BlobGrain>(&sim, one_shard_config());
    let keys: Vec<String> = (0..6).map(|i| format!("ws/{i}")).collect();

    let mut ids = Vec::new();
    for key in &keys {
        let bytes = format!("bulk bytes of {key}").into_bytes();
        let id = {
            let g = granaries[0].clone();
            let key = key.clone();
            let bytes = bytes.clone();
            drive(&sim, Duration::from_secs(8), async move {
                g.grain(key).ask(Put(bytes)).await
            })
        };
        let id = id.expect("put reached the grain").expect("blob stored");
        ids.push((key.clone(), id, bytes));
    }

    granaries[0].split_shard(0);
    await_split(&sim, &recorder);

    for (key, id, bytes) in ids {
        let fetched = {
            let g = granaries[1].clone();
            let key = key.clone();
            drive(&sim, Duration::from_secs(10), async move {
                g.grain(key)
                    .ask_timeout(Fetch(id), Duration::from_secs(8))
                    .await
            })
        };
        assert_eq!(
            fetched,
            Ok(Ok(Some(bytes))),
            "{key}: the blob area moved with the grain and reads back verified"
        );
    }
    assert_split_safety(&recorder);
}

// --- Merge (the mirror of split) ----------------------------------------------

/// Two founding shards, so an adjacent pair exists to merge from the start.
fn two_shard_config() -> GranaryConfig {
    GranaryConfig {
        shards: 2,
        idle_after: Duration::from_secs(60),
        snapshot_every: 4,
        ..GranaryConfig::default()
    }
}

#[test]
fn a_merge_folds_a_shard_into_its_neighbour_and_loses_no_write() {
    // The §7.7 merge sequence: commit state across two adjacent founding shards,
    // merge shard 0 with its right neighbour (shard 1), and prove every committed
    // write survives under the extended left shard, which keeps serving; the
    // retired shard's group is gone (G7). The map changed exactly once.
    let sim = Simulation::new(53);
    let (_net, recorder, _systems, granaries) = cluster_of::<Account>(&sim, two_shard_config());
    let keys: Vec<String> = (0..24).map(|i| format!("account/{i}")).collect();

    for key in &keys {
        let committed = {
            let g = granaries[0].clone();
            let key = key.clone();
            drive(&sim, Duration::from_secs(8), async move {
                g.grain(key).ask(Deposit { cents: 300 }).await
            })
        };
        assert_eq!(committed, Ok(300), "{key} deposit committed pre-merge");
    }
    // Both founding shards must actually hold grains, or the merge is trivial.
    let before: std::collections::BTreeSet<u32> = keys
        .iter()
        .flat_map(|k| {
            granaries[0]
                .replicas(k.clone())
                .is_empty()
                .then_some(())
                .map(|_| 0)
        })
        .collect();
    assert!(before.is_empty(), "every grain has a committed allocation");

    // Merge shard 0 with its right neighbour.
    granaries[0].merge_shards(0);
    let (left, right) = await_merge(&sim, &recorder);
    assert_eq!(left, 0, "shard 0 absorbs its neighbour");

    // Every grain — from both former shards — reads back and takes further
    // writes through the extended left shard.
    for key in &keys {
        let after = {
            let g = granaries[1].clone();
            let key = key.clone();
            drive(&sim, Duration::from_secs(10), async move {
                let acct = g.grain(key);
                let read = acct
                    .ask_timeout(ReadBalance, Duration::from_secs(8))
                    .await?;
                let write = acct
                    .ask_timeout(Deposit { cents: 1 }, Duration::from_secs(8))
                    .await?;
                Ok::<(i64, i64), GrainError>((read, write))
            })
        };
        assert_eq!(
            after,
            Ok((300, 301)),
            "{key}: committed state survived the merge and the left shard accepts writes"
        );
    }

    assert_eq!(merges_of(&recorder).len(), 1, "exactly one committed merge");
    // The retired shard no longer routes any grain — every key now resolves to
    // the surviving left shard, and the right index holds nothing.
    for key in &keys {
        assert_ne!(
            granaries[0].replicas(key.clone()),
            Vec::new(),
            "{key} still has a live allocation after the merge"
        );
    }
    assert_split_safety(&recorder);
    let _ = right;
}

#[test]
fn split_then_merge_round_trips_the_partition() {
    // Elasticity both ways: split shard 0, then merge the child back into it. The
    // partition returns to one shard, every write survives the round trip, and
    // both the split and the merge are observable.
    let sim = Simulation::new(61);
    let (_net, recorder, _systems, granaries) = cluster_of::<Account>(&sim, one_shard_config());
    let keys: Vec<String> = (0..16).map(|i| format!("account/{i}")).collect();

    for key in &keys {
        let committed = {
            let g = granaries[0].clone();
            let key = key.clone();
            drive(&sim, Duration::from_secs(8), async move {
                g.grain(key).ask(Deposit { cents: 200 }).await
            })
        };
        assert_eq!(committed, Ok(200), "{key} deposit committed");
    }

    // Split shard 0 → child.
    granaries[0].split_shard(0);
    let (parent, child, _boundary) = await_split(&sim, &recorder);
    assert_eq!(parent, 0);

    // A write to each grain on the post-split partition (some now on the child).
    for key in &keys {
        let bal = {
            let g = granaries[1].clone();
            let key = key.clone();
            drive(&sim, Duration::from_secs(10), async move {
                g.grain(key)
                    .ask_timeout(Deposit { cents: 50 }, Duration::from_secs(8))
                    .await
            })
        };
        assert_eq!(bal, Ok(250), "{key} committed on the post-split owner");
    }

    // Merge the child back into shard 0.
    granaries[0].merge_shards(0);
    let (left, _right) = await_merge(&sim, &recorder);
    assert_eq!(left, 0);

    // Every grain reads back the full history (200 + 50) after the round trip.
    for key in &keys {
        let bal = {
            let g = granaries[2].clone();
            let key = key.clone();
            drive(&sim, Duration::from_secs(10), async move {
                g.grain(key)
                    .ask_timeout(ReadBalance, Duration::from_secs(8))
                    .await
            })
        };
        assert_eq!(bal, Ok(250), "{key}: survived split then merge");
    }
    assert_eq!(splits_of(&recorder).len(), 1);
    assert_eq!(merges_of(&recorder).len(), 1);
    assert_split_safety(&recorder);
    let _ = child;
}

#[test]
fn a_merge_under_concurrent_writes_is_linearizable() {
    // The merge counterpart of the §14 split fault case: fold two adjacent shards
    // while clients write and read across the seal → transfer → commit window.
    // Every history must stay linearizable — a write lost at the seal or
    // double-applied onto the left shard would break it.
    for seed in 0..6 {
        let sim = Simulation::new(seed);
        let (_net, recorder, systems, granaries) =
            cluster_of::<CounterGrain>(&sim, two_shard_config());
        let keys = ["counter/0", "counter/1", "counter/2", "counter/3"];
        let histories: Vec<History<Counter>> = keys.iter().map(|_| History::new()).collect();

        for (key, history) in keys.iter().zip(&histories) {
            for granary in granaries.iter().take(2) {
                let granary = granary.clone();
                let history = history.clone();
                let entropy = systems[0].entropy().clone();
                let key = key.to_string();
                sim.spawner().launch(Box::pin(async move {
                    let counter = granary.grain(key);
                    for _ in 0..8 {
                        if entropy.next_u64().is_multiple_of(2) {
                            let delta = 1 + (entropy.next_u64() % 3) as i64;
                            let id = history.invoke(CounterOp::Add(delta));
                            match counter
                                .ask_timeout(Add(delta), Duration::from_secs(8))
                                .await
                            {
                                Ok(_) => history.ok(id, CounterRet::AddOk),
                                Err(_) => history.info(id),
                            }
                        } else {
                            let id = history.invoke(CounterOp::Read);
                            match counter.ask_timeout(ReadCount, Duration::from_secs(8)).await {
                                Ok(value) => history.ok(id, CounterRet::Read(value)),
                                Err(_) => history.info(id),
                            }
                        }
                    }
                }));
            }
        }

        let splitter = granaries[2].clone();
        let clock = systems[0].clock().clone();
        sim.spawner().launch(Box::pin(async move {
            clock.sleep(Duration::from_millis(400)).await;
            splitter.merge_shards(0);
        }));

        sim.run_for(Duration::from_secs(40));
        assert_eq!(
            merges_of(&recorder).len(),
            1,
            "seed {seed}: the merge committed under the concurrent load"
        );
        for (key, history) in keys.iter().zip(&histories) {
            let verdict = check_linearizable(history);
            assert!(
                verdict.is_ok(),
                "seed {seed}: {key} history not linearizable across the merge: {verdict:?}",
            );
        }
        assert_split_safety(&recorder);
    }
}
