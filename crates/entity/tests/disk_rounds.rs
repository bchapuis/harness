//! What a disk capture costs on the `Quorum` tier, counted in round trips.
//!
//! `benches/disk_capture.rs` ruled out the single-node half of a machine create:
//! ~0.7 s to scan 512 MB, under 2 s to import it, everything flat in the block
//! count, and a trip through the journal seam costing 0.58 µs against 1.4 ms of
//! bytes per block. Against the four minutes a create was observed to take, that
//! left the question above the store: the quorum fan-out, its transport, or the
//! sequencing of the two.
//!
//! Sequencing is measurable here, with no cluster and no hardware. The simulation's
//! clock is virtual and its transport applies a fixed minimum delivery latency to
//! every frame (actor §18.2), so **elapsed virtual time counts round trips**. On a
//! loopback the rounds are nearly free, so wall-clock time cannot tell five hundred
//! serialized quorum rounds from the same rounds issued sixteen at a time. Virtual
//! time can, and the same seed gives the same count on any machine.
//!
//! Both tests below measure a **slope** (the difference between two block counts)
//! rather than an average, because a command's total is dominated by what it pays
//! once regardless of size: routing to the shard leader, activating the entity,
//! recovering its head, committing the single manifest record. At these sizes that
//! intercept is most of the total (~56 ms against 2 ms of wave), so an average
//! answers the wrong question. A difference cancels it exactly. Between them they
//! establish:
//!
//! - **a wave of `IN_FLIGHT_CHUNKS` blocks costs one quorum round between them**:
//!   the facet issues that many puts at once, so the round trips overlap instead of
//!   adding up, and the marginal block costs a round trip *divided by* the width;
//! - **pipelining is what makes it cheap, not the payload being small**, shown by a
//!   control making the same number of trips through the same seam in a serial loop
//!   carrying four bytes instead of a mebibyte, which costs a full round trip each.
//!
//! So a 512-block image is 32 waves rather than 512 serialized rounds. What these
//! tests do *not* say is what one round costs on real hardware: virtual time counts
//! rounds, it does not price them, and a codec pass costs zero ticks here. That price
//! was taken on real nodes with `scripts/bench-machine-cost.sh`, and it corrected two
//! readings taken off this file:
//!
//! - **The cost was payload, not rounds.** A four-byte blob and a mebibyte cost the
//!   same *here* whatever the bytes do, which is evidence about round counts only. On
//!   the wall clock a mebibyte was ~16 ms through serde's element-at-a-time path, at
//!   each of two encoding layers. Annotating both took the deduplicated three-node
//!   figure from ~40 ms a block to ~1.4 ms.
//! - **It does not rise with replica count.** An earlier reading said it did; that was
//!   measured on a random image, where each added replica also adds a replica's worth
//!   of cold `atomic_replace` on a laptop whose three nodes share one device. With the
//!   codec fixed, one node and three cost the same per deduplicated block. What is left
//!   in the random column is the device.
//!
//! `FEW` and `MANY` are a wave apart because of what this file measured before. While
//! `import` put its blocks in a serial loop the slope was a whole round trip *per
//! block*, and the control carrying four bytes gave the same slope; that pair
//! identified the round count rather than the payload as the lever, and pipelining was
//! made on it. When the change landed this test failed low exactly as it was written
//! to, 4 blocks and 16 blocks costing the same 58 ms because both fit in one wave, so
//! the bounds were re-derived around the wave rather than the block.
//!
//! The assertions are bands rather than equalities. Failing **high** is the
//! regression they guard: the puts have stopped overlapping and a create is back to
//! a round trip per block. Failing **low** on the wave means the concurrency bound
//! stopped being applied, which a large image pays for in memory rather than time.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_core::Clock;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::SimNetwork;
use actor_simulation::SimNode;
use actor_simulation::Simulation;
use actor_simulation::raft_config;
use actor_simulation::swim_config;
use entity::Entities;
use entity::Entity;
use entity::EntityConfig;
use entity::EntityCtx;
use entity::EntityExt;
use entity::EntityHandler;
use entity::NoEvent;
use entity::facet::Disk;
use entity::facet::DiskCaptureStats;
use entity::facet::DiskError;
use serde::Deserialize;
use serde::Serialize;

/// The facet's fixed block size (spec §7.15). One block is one `put_blob`, the
/// unit these tests count in.
const BLOCK: usize = 1 << 20;

// --- An entity whose durable state is entirely its raw image --------------------

#[derive(Default)]
struct DiskBox;

impl Entity for DiskBox {
    type System = SimNode;
    type State = ();
    type Event = NoEvent;
    type Facets = (Disk,);
    const ENTITY_TYPE: &'static str = "test.RoundsDiskBox";

    fn apply(_state: &mut (), event: &NoEvent) {
        event.unreachable()
    }

    // On the `Quorum` tier a command crosses the wire, so each one the entity accepts
    // has to be declared; on the `Local` tier the call is direct.
    fn register(r: &mut entity::EntityRegistry<Self>) {
        r.accept::<ImportFrom>();
        r.accept::<TinyPuts>();
    }
}

/// Provision the image from a source file: the create path, and the command whose
/// round count is the subject.
#[derive(Clone, Serialize, Deserialize)]
struct ImportFrom(String);
impl Message for ImportFrom {
    type Reply = Result<DiskCaptureStats, DiskError>;
    const MANIFEST: Manifest = Manifest::new("test.RoundsImport");
}
impl EntityHandler<ImportFrom> for DiskBox {
    async fn handle(
        &self,
        _state: &(),
        msg: ImportFrom,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<NoEvent>, Result<DiskCaptureStats, DiskError>) {
        (
            vec![],
            ctx.disk().import(std::path::Path::new(&msg.0)).await,
        )
    }
}

/// Make `0` blob puts of four bytes each from inside one command handler: the
/// control.
///
/// The same seam, the same number of trips through it, the payload taken away and
/// the puts left **serialized**: the shape `import` had before it pipelined. The
/// serial loop is deliberate; it is what the import is measured against, and what
/// makes the comparison read as the width rather than the bytes.
#[derive(Clone, Serialize, Deserialize)]
struct TinyPuts(u32);
impl Message for TinyPuts {
    type Reply = u32;
    const MANIFEST: Manifest = Manifest::new("test.RoundsTinyPuts");
}
impl EntityHandler<TinyPuts> for DiskBox {
    async fn handle(
        &self,
        _state: &(),
        msg: TinyPuts,
        ctx: &EntityCtx<Self>,
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

/// R = 3 over the 3-node cluster, so every blob put is a real quorum round to a
/// majority. Snapshotting is off: a checkpoint in the middle of a measured import
/// would add rounds that belong to §9 rather than to the capture path.
fn config(scratch: &std::path::Path) -> EntityConfig {
    EntityConfig {
        shards: 1,
        replication_factor: 3,
        idle_after: Duration::from_secs(600),
        snapshot_every: 0,
        data_dir: Some(scratch.to_path_buf()),
        // Generous: a timeout firing mid-import would truncate the quantity
        // under test.
        quorum_timeout: Duration::from_secs(120),
        recover_timeout: Duration::from_secs(120),
        ..EntityConfig::default()
    }
}

fn cluster(
    sim: &Simulation,
    scratch: &std::path::Path,
) -> (SimNetwork, Vec<SimNode>, Vec<Entities<DiskBox>>) {
    let net = SimNetwork::new(sim).with_leader(
        swim_config(),
        raft_config(vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)]),
        DowningPolicy::Conservative,
    );
    let systems = vec![
        net.join(NodeId::new(1)),
        net.join(NodeId::new(2)),
        net.join(NodeId::new(3)),
    ];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let hosts: Vec<Entities<DiskBox>> = systems
        .iter()
        .map(|system| system.entities::<DiskBox>(config(scratch)))
        .collect();
    sim.run_for(Duration::from_secs(3)); // elect the shard group's leader
    (net, systems, hosts)
}

/// Run `future` to completion and report the **virtual** time it took.
///
/// The usual `drive` in this crate's suites runs a fixed settle window and reads the
/// value out afterwards, which is right when the question is "did it happen". Here
/// the elapsed time *is* the measurement, so the clock is read where the future
/// finishes rather than where the window ends; otherwise every answer is the settle
/// window. The window advances in small steps and is abandoned as soon as the value
/// lands, which keeps the wall-clock cost of a generous cap near zero: simulating a
/// quiet cluster still costs real time, one heartbeat at a time.
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

/// A source image of `blocks` MiB, every block distinct, so each is a cold put
/// rather than a dedup hit.
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
    let (_net, _systems, hosts) = cluster(&sim, scratch.path());
    let entities = hosts[0].clone();

    let src_arg = src.to_string_lossy().into_owned();
    // The cap bounds the *virtual* clock and must sit far above the cost measured;
    // if the import has not finished, `drive_timed` panics rather than return a
    // truncated answer.
    drive_timed(&sim, Duration::from_secs(600), async move {
        entities
            .entity("box/import")
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
    let (_net, _systems, hosts) = cluster(&sim, scratch.path());
    let entities = hosts[0].clone();

    let (cost, made) = drive_timed(&sim, Duration::from_secs(600), async move {
        entities
            .entity("box/puts")
            .ask(TinyPuts(count as u32))
            .await
            .expect("ask")
    });
    assert_eq!(made as usize, count, "every put must have been made");
    cost
}

// --- The measurements ---------------------------------------------------------

/// `facet_blobs::IN_FLIGHT_CHUNKS`: how many block puts the disk facet keeps
/// outstanding. Private to the crate, so it is restated here the way `BLOCK` is;
/// if it moves, these measurements move with it and the assertions below say so.
const IN_FLIGHT: usize = 16;

/// The two block counts every measurement here is a slope between.
///
/// A slope rather than an average, because a command's total is dominated by costs
/// that have nothing to do with its size: routing to the shard leader, activating the
/// entity, recovering its head, and committing the one manifest record at the end.
/// That intercept is paid once and, at these sizes, is most of the total, so
/// `total / blocks` measures the intercept. A difference between two sizes cancels
/// it exactly.
///
/// Both counts are whole multiples of [`IN_FLIGHT`], one wave apart. Within a wave
/// the marginal block is free, so a pair that fits inside one wave measures nothing:
/// the old `FEW` = 4 and `MANY` = 16 both went out in a single wave and the slope
/// collapsed to a nanosecond. One wave and not two, because the difference is exact
/// rather than averaged (the transport's minimum delivery latency quantizes it) and
/// a second wave would buy no resolution for another 16 MiB through a simulated
/// three-node quorum.
const FEW: usize = IN_FLIGHT;
const MANY: usize = 2 * IN_FLIGHT;

/// The marginal virtual cost of one more block, from two measurements sharing a seed.
fn per_block(few: Duration, many: Duration) -> Duration {
    assert!(
        many > few,
        "{MANY} blocks cost {many:?}, no more than {FEW} blocks' {few:?}; \
         a per-block cost cannot be recovered from that",
    );
    (many - few) / (MANY - FEW) as u32
}

#[test]
fn a_wave_of_blocks_costs_one_quorum_round_between_them() {
    // Import at two sizes and take the slope: whatever a create pays per block,
    // above everything it pays once.
    let (few, few_stats) = import_cost(5, FEW);
    let (many, many_stats) = import_cost(5, MANY);
    assert_eq!(few_stats.blocks as usize, FEW);
    assert_eq!(many_stats.blocks as usize, MANY);

    let marginal = per_block(few, many);
    let per_wave = marginal * IN_FLIGHT as u32;
    println!(
        "quorum import: {FEW} blocks in {few:?}, {MANY} blocks in {many:?} \
         → {marginal:?} per block, {per_wave:?} per {IN_FLIGHT}-block wave, \
         {:.0} ms of fixed cost",
        (few.as_secs_f64() * 1000.0) - (marginal.as_secs_f64() * 1000.0 * FEW as f64),
    );

    // A quorum put is a frame out and an acknowledgement back, and the simulated
    // transport applies its base latency to each (actor §18.2). The facet issues
    // `IN_FLIGHT` of them at once, so a wave costs one round trip: the slope is a
    // round trip *divided by* the width, not a round trip each.
    assert!(
        per_wave >= Duration::from_millis(1),
        "a wave of {IN_FLIGHT} blocks costs {per_wave:?}, under one delivery; the \
         puts are overlapping more widely than the facet's bound allows, so either \
         `IN_FLIGHT` grew or the bound stopped being applied; a create's memory is \
         what that costs, so re-derive this around the new width rather than \
         deleting it",
    );
    assert!(
        per_wave <= Duration::from_millis(10),
        "a wave of {IN_FLIGHT} blocks costs {per_wave:?}, several round trips rather \
         than one: the puts have stopped overlapping, and a create is back to \
         paying a round trip per block",
    );
}

#[test]
fn pipelining_the_puts_is_what_makes_a_block_cheap_and_not_its_size() {
    // The same seam and the same number of trips through it, one path pipelined and
    // one not: an import carrying a mebibyte per put against the control's serial
    // loop carrying four bytes. The control is the shape `import` used to have, so
    // the heavier payload being the cheaper path can only be pipelining.
    let (few_bytes, stats) = import_cost(11, FEW);
    let (many_bytes, _) = import_cost(11, MANY);
    assert_eq!(stats.blocks as usize, FEW);
    let pipelined = per_block(few_bytes, many_bytes);
    let serial = per_block(tiny_puts_cost(11, FEW), tiny_puts_cost(11, MANY));

    println!(
        "quorum put: {pipelined:?} per pipelined 1 MiB block, \
         {serial:?} per serialized 4-byte blob"
    );

    // A band rather than a figure. In principle the win is the full `IN_FLIGHT`-fold;
    // in practice an import also reads and writes the image and the transport's
    // per-frame latency is seeded rather than fixed, so demanding the factor itself
    // would be asserting the agent runtime. A quarter of it sits far outside what
    // those account for and far inside what a regression to a serial loop shows.
    let speedup = serial.as_secs_f64() / pipelined.as_secs_f64();
    assert!(
        speedup >= IN_FLIGHT as f64 / 4.0,
        "a pipelined mebibyte-per-put block cost {pipelined:?} against the serial \
         control's {serial:?} for four bytes, only {speedup:.1}x, where overlapping \
         {IN_FLIGHT} puts should be worth several times that. The block puts are not \
         overlapping the way `put_pulled` intends.",
    );
}
