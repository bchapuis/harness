//! What a disk capture costs on the `Quorum` tier, counted in round trips.
//!
//! `benches/disk_capture.rs` measured the single-node half of a machine create and
//! ruled it out: ~0.7 s to scan 512 MB, under 2 s to import it, everything flat in
//! the block count, and a trip through the journal seam costing 0.58 µs against
//! 1.4 ms of bytes per block. Against the four minutes a create was observed to take,
//! that left the whole question above the store — the quorum fan-out, its transport,
//! or the sequencing of the two.
//!
//! Sequencing is measurable here, and it needs no cluster and no hardware. The
//! simulation's clock is virtual and its transport applies a fixed minimum delivery
//! latency to every frame (§18.2), so **elapsed virtual time counts round trips**.
//! Wall-clock time on one machine cannot distinguish a path that issues five hundred
//! quorum rounds one after another from one that issues them sixteen at a time,
//! because on a loopback the rounds are nearly free. Virtual time can, and does so
//! deterministically: the same seed gives the same count on any machine.
//!
//! Both tests below measure a **slope** — the difference between two block counts,
//! not a total divided by one — because a command's total is dominated by what it
//! pays once regardless of size: routing to the shard leader, activating the grain,
//! recovering its head, committing the single manifest record. At these sizes that
//! intercept is most of the total (~56 ms against 2 ms of block), so an average
//! measures the intercept and answers the wrong question. Taking a difference cancels
//! it exactly. Between them they establish:
//!
//! - **a block costs one quorum round of its own** — the slope is a frame out and an
//!   acknowledgement back, so nothing overlaps and the round trips simply add up;
//! - **that cost is the round trip and not the bytes**, shown by a control making the
//!   same number of trips through the same seam carrying four bytes instead of a
//!   mebibyte, which produces the same slope.
//!
//! So a 512-block image is 512 serialized quorum rounds, and pipelining them is worth
//! up to `IN_FLIGHT_CHUNKS`-fold. What these tests do *not* say is what one round
//! costs on real hardware: virtual time counts rounds, it does not price them. That
//! price has since been taken on a real three-node cluster and is ~63 ms per block
//! against ~12 ms on a single node, rising linearly with each replica added — see
//! TODO.md, which also records that a create's headline cost turned out to be mostly
//! a one-time cluster warm-up rather than anything per block.
//!
//! The assertions are bands rather than equalities, and the one on the slope is a
//! characterization: it describes what the path costs *today* so the pipelining
//! change has a before to be measured against (TODO.md, "Pipeline the disk facet's
//! block puts"). When that lands this test should fail low, which is the signal that
//! it worked — re-derive the bound around the new round count rather than deleting
//! it. Failing high is the regression it guards: more exchanges per put, paid per
//! block.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::Clock;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::SimNetwork;
use actor_simulation::SimNode;
use actor_simulation::Simulation;
use granary::Disk;
use granary::DiskCaptureStats;
use granary::DiskError;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::NoEvent;
use serde::Deserialize;
use serde::Serialize;

/// The facet's fixed block size (spec §7.15). One block is one `put_blob`, so it is
/// also the unit these tests count in.
const BLOCK: usize = 1 << 20;

// --- A grain whose durable state is entirely its raw image --------------------

#[derive(Default)]
struct DiskBox;

impl Grain for DiskBox {
    type System = SimNode;
    type State = ();
    type Event = NoEvent;
    type Facets = (Disk,);
    const GRAIN_TYPE: &'static str = "test.RoundsDiskBox";

    fn apply(_state: &mut (), event: &NoEvent) {
        event.unreachable()
    }

    // On the `Quorum` tier a command crosses the wire, so each one the grain accepts
    // has to be declared — unlike the `Local` tier, where the call is direct.
    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<ImportFrom>();
        r.accept::<TinyPuts>();
    }
}

/// Provision the image from a source file — the create path, and the command whose
/// round count is the subject.
#[derive(Clone, Serialize, Deserialize)]
struct ImportFrom(String);
impl Message for ImportFrom {
    type Reply = Result<DiskCaptureStats, DiskError>;
    const MANIFEST: Manifest = Manifest::new("test.RoundsImport");
}
impl GrainHandler<ImportFrom> for DiskBox {
    async fn handle(
        &self,
        _state: &(),
        msg: ImportFrom,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Result<DiskCaptureStats, DiskError>) {
        (
            vec![],
            ctx.disk().import(std::path::Path::new(&msg.0)).await,
        )
    }
}

/// Make `0` blob puts of four bytes each from inside one command handler — the
/// control.
///
/// The same seam, the same number of trips through it, the payload taken away. If
/// this costs what an import of the same block count costs, the import's time is its
/// round trips; if it costs far less, the bytes are on the wire and the round count
/// is a red herring.
#[derive(Clone, Serialize, Deserialize)]
struct TinyPuts(u32);
impl Message for TinyPuts {
    type Reply = u32;
    const MANIFEST: Manifest = Manifest::new("test.RoundsTinyPuts");
}
impl GrainHandler<TinyPuts> for DiskBox {
    async fn handle(
        &self,
        _state: &(),
        msg: TinyPuts,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, u32) {
        let blobs = ctx.blobs();
        for i in 0..msg.0 {
            // Distinct content per put, so none of them takes the dedup path.
            blobs.put(i.to_le_bytes().to_vec()).await.expect("put");
        }
        (vec![], msg.0)
    }
}

// --- The clustered harness ----------------------------------------------------

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

/// R = 3 over the 3-node cluster, so every blob put is a real quorum round to a
/// majority. Snapshotting is off: a checkpoint in the middle of a measured import
/// would add rounds that belong to §9 rather than to the capture path.
fn config(scratch: &std::path::Path) -> GranaryConfig {
    GranaryConfig {
        shards: 1,
        replication_factor: 3,
        idle_after: Duration::from_secs(600),
        snapshot_every: 0,
        data_dir: Some(scratch.to_path_buf()),
        // Generous, because virtual time is what is being measured and a timeout
        // firing mid-import would truncate the very quantity under test.
        quorum_timeout: Duration::from_secs(120),
        recover_timeout: Duration::from_secs(120),
        ..GranaryConfig::default()
    }
}

fn cluster(
    sim: &Simulation,
    scratch: &std::path::Path,
) -> (SimNetwork, Vec<SimNode>, Vec<Granary<DiskBox>>) {
    let net = SimNetwork::new(sim).with_leader(swim(), raft(), DowningPolicy::Conservative);
    let systems = vec![
        net.join(NodeId::new(1)),
        net.join(NodeId::new(2)),
        net.join(NodeId::new(3)),
    ];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let granaries: Vec<Granary<DiskBox>> = systems
        .iter()
        .map(|system| system.granary::<DiskBox>(config(scratch)))
        .collect();
    sim.run_for(Duration::from_secs(3)); // elect the shard group's leader
    (net, systems, granaries)
}

/// Run `future` to completion and report the **virtual** time it took.
///
/// The usual `drive` in this crate's suites runs a fixed settle window and reads the
/// value out afterwards, which is right when the question is "did it happen". Here
/// the elapsed time *is* the measurement, so the clock has to be read where the
/// future finishes rather than where the window ends — otherwise every answer is the
/// settle window. The window is advanced in small steps and abandoned as soon as the
/// value lands, which also keeps the wall-clock cost of a generous cap near zero:
/// simulating a quiet cluster still costs real time, one heartbeat at a time.
fn drive_timed<T: Send + 'static>(
    sim: &Simulation,
    cap: Duration,
    future: impl std::future::Future<Output = T> + Send + 'static,
) -> (Duration, T) {
    let clock = sim.clock();
    let started = clock.now();
    let cell: Arc<Mutex<Option<(actor_core::Instant, T)>>> = Arc::new(Mutex::new(None));
    let out = Arc::clone(&cell);
    let inner = clock.clone();
    sim.spawner().launch(Box::pin(async move {
        let value = future.await;
        *out.lock().unwrap() = Some((inner.now(), value));
    }));
    let step = Duration::from_millis(20);
    let mut waited = Duration::ZERO;
    while waited < cap && cell.lock().unwrap().is_none() {
        sim.run_for(step);
        waited += step;
    }
    let (finished, value) = cell
        .lock()
        .unwrap()
        .take()
        .expect("future did not complete within the cap");
    (finished.duration_since(started), value)
}

/// A source image of `blocks` MiB, every block distinct so none of them dedups
/// against another and each is a genuine cold put.
fn write_source(path: &std::path::Path, blocks: usize) {
    let mut bytes = vec![0x5a; blocks * BLOCK];
    for idx in 0..blocks {
        bytes[idx * BLOCK..idx * BLOCK + 8].copy_from_slice(&(idx as u64).to_le_bytes());
    }
    std::fs::write(path, &bytes).expect("write the source image");
}

/// The virtual time one import of `blocks` blocks takes on the `Quorum` tier, and
/// the stats it reported.
fn import_cost(seed: u64, blocks: usize) -> (Duration, DiskCaptureStats) {
    let scratch = tempfile::tempdir().expect("tempdir");
    let src = scratch.path().join("base.img");
    write_source(&src, blocks);

    let sim = Simulation::new(seed);
    let (_net, _systems, granaries) = cluster(&sim, scratch.path());
    let granary = granaries[0].clone();

    let src_arg = src.to_string_lossy().into_owned();
    // The cap bounds the *virtual* clock and must sit far above the cost being
    // measured; `drive_timed` panics rather than returning a truncated answer if the
    // import has not finished, which is the failure this wants.
    drive_timed(&sim, Duration::from_secs(600), async move {
        granary
            .grain("box/import")
            .ask(ImportFrom(src_arg))
            .await
            .expect("ask")
            .expect("import")
    })
}

/// The virtual time `count` four-byte puts take from inside one command handler.
fn tiny_puts_cost(seed: u64, count: usize) -> Duration {
    let scratch = tempfile::tempdir().expect("tempdir");
    let sim = Simulation::new(seed);
    let (_net, _systems, granaries) = cluster(&sim, scratch.path());
    let granary = granaries[0].clone();

    let (cost, made) = drive_timed(&sim, Duration::from_secs(600), async move {
        granary
            .grain("box/puts")
            .ask(TinyPuts(count as u32))
            .await
            .expect("ask")
    });
    assert_eq!(made as usize, count, "every put must have been made");
    cost
}

// --- The measurements ---------------------------------------------------------

/// The two block counts every measurement here is a slope between.
///
/// A slope rather than an average, because a command's total is dominated by costs
/// that have nothing to do with its size: routing to the shard leader, activating the
/// grain, recovering its head, and committing the one manifest record at the end. All
/// of that is paid once and shows up as a large intercept — at these sizes it is
/// most of the total — so `total / blocks` measures the intercept and answers the
/// wrong question. The difference between two sizes cancels it exactly.
const FEW: usize = 4;
const MANY: usize = 16;

/// The marginal virtual cost of one more block, from two measurements sharing a seed.
fn per_block(few: Duration, many: Duration) -> Duration {
    assert!(
        many > few,
        "{MANY} blocks cost {many:?}, no more than {FEW} blocks' {few:?} — \
         a per-block cost cannot be recovered from that",
    );
    (many - few) / (MANY - FEW) as u32
}

#[test]
fn every_block_costs_its_own_quorum_round() {
    // The characterization this exists to produce. Import at two sizes and take the
    // slope: whatever a create pays per block, above everything it pays once.
    let (few, few_stats) = import_cost(5, FEW);
    let (many, many_stats) = import_cost(5, MANY);
    assert_eq!(few_stats.blocks as usize, FEW);
    assert_eq!(many_stats.blocks as usize, MANY);

    let marginal = per_block(few, many);
    println!(
        "quorum import: {FEW} blocks in {few:?}, {MANY} blocks in {many:?} \
         → {marginal:?} per block, {:.0} ms of fixed cost",
        (few.as_secs_f64() * 1000.0) - (marginal.as_secs_f64() * 1000.0 * FEW as f64),
    );

    // A quorum put is a frame out and an acknowledgement back, and the simulated
    // transport applies its base latency to each (§18.2). A block costing a whole
    // round trip of its own is the signature of `import`'s sequential loop: nothing
    // overlaps, so the round trips add up.
    assert!(
        marginal >= Duration::from_millis(1),
        "a block's marginal cost is {marginal:?}, under one delivery — the puts must \
         have started overlapping, which is the change TODO.md calls for; re-derive \
         this bound around the new round count rather than deleting it",
    );
    assert!(
        marginal <= Duration::from_millis(10),
        "a block's marginal cost is {marginal:?}, several round trips rather than one \
         — a put now costs more exchanges than it did, and a create pays it per block",
    );
}

#[test]
fn a_blocks_cost_is_its_round_trip_and_not_its_bytes() {
    // The same seam, the same number of trips through it, one mebibyte each against
    // four bytes each. On a path whose cost is the payload these slopes differ by a
    // factor of a quarter-million; on a path whose cost is the round trips they agree,
    // and only then is the round *count* the thing worth changing.
    let (few_bytes, stats) = import_cost(11, FEW);
    let (many_bytes, _) = import_cost(11, MANY);
    assert_eq!(stats.blocks as usize, FEW);
    let loaded = per_block(few_bytes, many_bytes);
    let empty = per_block(tiny_puts_cost(11, FEW), tiny_puts_cost(11, MANY));

    println!("quorum put: {loaded:?} per 1 MiB block, {empty:?} per 4-byte blob");

    // Within a factor of four either way — a band, not an equality. An import also
    // reads and writes the image, and the transport's per-frame latency is seeded
    // rather than fixed, so demanding agreement would be asserting the harness. The
    // hypothesis being tested is worth six orders of magnitude, so four is ample.
    let ratio = loaded.as_secs_f64() / empty.as_secs_f64();
    assert!(
        (0.25..=4.0).contains(&ratio),
        "a mebibyte per put cost {ratio:.2}x what four bytes per put cost \
         ({loaded:?} against {empty:?}) — the payload now dominates the round trip, \
         so the block size is the lever and the round count is not",
    );
}
