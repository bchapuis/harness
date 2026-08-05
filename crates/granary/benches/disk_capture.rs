//! What a disk-facet capture costs, block by block.
//!
//! Creating a 512 MB machine was observed to take roughly four minutes. Every
//! constant anyone reached for said it should not: the uplink floor for 512 MB is ~8
//! seconds and assumes nothing deduplicates, while a fresh image is mostly zeros that
//! all hash to one blob, and local BLAKE3 over 512 MB is ~170 ms. Almost all of that
//! four minutes was unexplained, and until it was attributed there was no honest way
//! to choose a block size or an in-flight bound — either would have been tuned
//! against a cost nobody had located.
//!
//! It has since been attributed, and this file is the layer that did it: what these
//! numbers established is that the single-node path costs what its parts cost and no
//! more, which is what made the rest of the time findable elsewhere. It was two things,
//! and neither was the fan-out — a mebibyte going through serde's element-at-a-time
//! path at each of two encoding layers, and a cold-start cost that was a Raft group
//! waiting out an election timeout built to detect a failure that had not happened.
//! Keep these numbers honest; they are the floor every later claim is checked against.
//!
//! So this file does not benchmark a knob. It measures the capture path in three
//! layers, cheapest first, so the layer that holds the time can be named:
//!
//! 1. **The constants** — every per-byte term the path has: hashing a block, reading
//!    one off the source, writing one into the image. Nothing above them can be
//!    faster than their sum times the block count, and if that sum is already minutes
//!    the block size is the whole story.
//! 2. **One blob put**, at the store, with no facet and no grain above it. Three
//!    variants, because they are three different costs wearing one name: the memory
//!    store (what the store layer itself adds), the file store on content it has
//!    never seen (an `atomic_replace` — write, fsync, rename, fsync the directory),
//!    and the file store on content it already holds (the dedup hit — one `exists`).
//!    A fresh image is mostly the third, which is why the third is measured.
//! 3. **The whole path**, through a real grain with the [`Disk`] facet on the `Local`
//!    tier: `import` for provisioning, a clean `capture` for the scan, and `puts` as
//!    their control — the same number of trips through the same seam carrying four
//!    bytes each instead of a mebibyte. The control is what separates a cost paid per
//!    `await` from one paid per byte, and those two have opposite fixes: the first is
//!    answered by concurrency, the second only by moving fewer bytes.
//!
//! Read them together, not separately. Layer 3 divided by the block count, against
//! layer 1 plus layer 2, is the number this file exists to produce: what the facet,
//! the journal seam, and the staging cost *per block* over and above the bytes.
//! Whatever is left after that is above the store — the quorum fan-out and its
//! transport — and is not measurable here, because the `Local` tier has no peers.
//! Bounding the local side is what makes the remainder attributable to the far side.
//!
//! The sizes are deliberately smaller than the 512 MB that prompted this. The path
//! is per-block and the interesting quantity is per-block cost, which a 64 MiB image
//! measures as well as a 512 MiB one and sixteen times faster; a cost that appears
//! only at 512 MB would show as a slope across the sweep.

use actor_serialization::Codec;
use actor_serialization::JsonCodec;
use std::sync::Arc;
use std::time::Duration;

use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use divan::Bencher;
use divan::black_box;
use divan::counter::BytesCount;
use divan::counter::ItemsCount;
use granary::BlobId;
use granary::Disk;
use granary::DiskCaptureStats;
use granary::DiskError;
use granary::FileGrainStore;
use granary::Grain;
use granary::GrainBlobStore;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GrainName;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::MemoryGrainStore;
use granary::NoEvent;
use serde::Deserialize;
use serde::Serialize;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// The facet's fixed block size (spec §7.15), mirrored here: every benchmark below
/// is per-block or a whole multiple of it, so it must be the same 1 MiB the facet
/// hashes and puts.
const BLOCK: usize = 1 << 20;

/// The shard and grain every store-level benchmark writes under. Which ones is
/// immaterial — a blob is keyed by `(shard, grain, id)` and none of the paths here
/// branch on the first two — but holding them fixed keeps the segment lookup on its
/// hit path, which is the steady state a capture runs in.
const SHARD: u32 = 0;

fn name() -> GrainName {
    GrainName::new("bench.DiskBox", "img-1")
}

/// One block whose content is unique to `stamp`.
///
/// Uniqueness is the point, not the pattern: a `BlobId` is BLAKE3 of the whole
/// block, so changing one byte makes the block content the store has never seen and
/// forces the cold put. A benchmark that reused one block would measure the dedup
/// hit and report it as the write.
fn block(stamp: u64) -> Vec<u8> {
    let mut bytes = vec![0x5a; BLOCK];
    bytes[..8].copy_from_slice(&stamp.to_le_bytes());
    bytes
}

// --- Layer 1: the constants ---------------------------------------------------

/// BLAKE3 over one block — the hash half of the scan, and the only cost a capture
/// pays for a block that turns out to be clean.
///
/// This is the ~170 ms per 512 MB claim, stated per block so it can be multiplied
/// out. If a capture's per-block cost is anywhere near this, the scan is the path
/// and the puts are noise; the observed four minutes says it is not, which is what
/// makes the rest of this file necessary.
#[divan::bench]
fn hash(bencher: Bencher) {
    let bytes = block(0);
    bencher
        .counter(BytesCount::new(BLOCK))
        .bench_local(|| black_box(BlobId::of(black_box(&bytes))));
}

/// Reading one block out of the image file, as `capture`'s scan does: an unbuffered
/// `read_exact` into a freshly allocated block.
///
/// Freshly allocated because that is what the facet does — it allocates a block per
/// iteration of the scan rather than reusing one buffer — so the allocation belongs
/// in the measurement. The file is small enough to sit in the page cache, which is
/// the honest case for a capture: the guest has just written the image.
#[divan::bench]
fn read(bencher: Bencher) {
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;

    let dir = tempfile::tempdir().expect("scratch dir");
    let path = dir.path().join("one.img");
    std::fs::write(&path, block(0)).expect("write the block");
    let mut file = std::fs::File::open(&path).expect("open the image");
    bencher.counter(BytesCount::new(BLOCK)).bench_local(|| {
        file.seek(SeekFrom::Start(0)).expect("seek");
        let mut bytes = vec![0u8; BLOCK];
        file.read_exact(&mut bytes).expect("read the block");
        black_box(bytes)
    });
}

/// Writing one block into the activation-local image, as `import`'s copy does: an
/// unbuffered `write_all` into a file already sized to its final length.
///
/// The last per-byte term in the path, and the only one not otherwise accounted for.
/// `set_len` first, because that is what `import` does — the image is fixed-size, so
/// every block after the first lands in a hole the filesystem has to allocate, and a
/// write into a hole is not the same cost as a write over allocated extents.
#[divan::bench]
fn write(bencher: Bencher) {
    use std::io::Write;

    let dir = tempfile::tempdir().expect("scratch dir");
    let path = dir.path().join("one.img");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .expect("open the image");
    file.set_len(64 * BLOCK as u64).expect("size the image");
    let bytes = block(0);
    bencher
        .counter(BytesCount::new(BLOCK))
        .bench_local(|| file.write_all(black_box(&bytes)).expect("write the block"));
}

// --- Layer 2: one blob put at the store ---------------------------------------

/// One cold `put_blob` against `store`: content it has never held, so a file store
/// pays the whole atomic write and a memory store pays an insert.
///
/// `with_inputs` builds each iteration's block *and its id* outside the timed region.
/// Both are the benchmark's setup rather than the store's work, and both would
/// otherwise be double-counted: a 1 MiB allocation and fill would swamp the memory
/// store entirely, and the hash is already layer 1's subject — leaving it here would
/// make every store look like it costs at least one BLAKE3.
fn put_cold(bencher: Bencher, store: impl GrainBlobStore) {
    let grain = name();
    let mut stamp = 0u64;
    bencher
        .counter(BytesCount::new(BLOCK))
        .with_inputs(|| {
            stamp += 1;
            let bytes = block(stamp);
            (BlobId::of(&bytes), bytes)
        })
        .bench_local_values(|(id, bytes)| {
            black_box(store.put_blob(SHARD, black_box(&grain), id, bytes))
        });
}

/// The in-memory store: hash, lock, insert. No disk.
///
/// The floor `put_file` is read against — everything here is cost the store layer
/// adds regardless of durability, and a capture pays it once per dirty block.
#[divan::bench]
fn put_memory(bencher: Bencher) {
    put_cold(bencher, MemoryGrainStore::new());
}

/// The file store on content it has never seen: one `atomic_replace` per block —
/// write a temp file, fsync it, rename, fsync the directory.
///
/// Two fsyncs per dirty block is the durability the acknowledgement stands for
/// (**G18**), so this number is not a defect to be optimized away; it is the price
/// of the path, and knowing it is what says whether a capture is fsync-bound. Being
/// fsync-bound makes it a property of the filesystem and the device — see
/// `docs/hardware-envelope.md` §3.3 on power-loss protection, which moves this by an
/// order of magnitude.
#[divan::bench]
fn put_file(bencher: Bencher) {
    let dir = tempfile::tempdir().expect("scratch dir");
    put_cold(
        bencher,
        FileGrainStore::open(dir.path(), JsonCodec.name()).expect("open the store"),
    );
}

/// The file store on content it already holds — the dedup hit: a path join and one
/// `exists`, then `Stored`.
///
/// This is the common case on a fresh image, and the reason a 512 MB create was
/// expected to be cheap: an image that is mostly zeros is one distinct block put
/// once and re-recognized five hundred times. It is measured because "recognized" is
/// not free at the *caller* — the bytes still had to be read and hashed (layer 1),
/// and on the `Quorum` tier they are still sent to every peer, since `put_blob` does
/// not negotiate. What this benchmark shows is only that the store side of a
/// recognized block is negligible, which localizes the cost elsewhere.
#[divan::bench]
fn put_file_present(bencher: Bencher) {
    let dir = tempfile::tempdir().expect("scratch dir");
    let store = FileGrainStore::open(dir.path(), JsonCodec.name()).expect("open the store");
    let grain = name();
    let bytes = block(0);
    let id = BlobId::of(&bytes);
    // The first put is the cold one; the measured ones are hits, so its ack is moot.
    let _warm = store.put_blob(SHARD, &grain, id, bytes.clone());
    bencher
        .counter(BytesCount::new(BLOCK))
        .with_inputs(|| bytes.clone())
        .bench_local_values(|bytes| black_box(store.put_blob(SHARD, black_box(&grain), id, bytes)));
}

// --- Layer 3: the whole path, through a grain ---------------------------------

/// A grain whose durable state is entirely its raw image — the shape `machine`'s
/// grain has, reduced to the facet under measurement.
#[derive(Default)]
struct DiskBox;

impl Grain for DiskBox {
    type System = SimSystem;
    type State = ();
    type Event = NoEvent;
    type Facets = (Disk,);
    const GRAIN_TYPE: &'static str = "bench.DiskBox";

    fn apply(_state: &mut (), event: &NoEvent) {
        event.unreachable()
    }
}

/// Provision the image from a source file — the create path (§7.15: the base image
/// *is* a capture), and the command a machine create is waiting on.
#[derive(Clone, Serialize, Deserialize)]
struct ImportFrom(String);
impl Message for ImportFrom {
    type Reply = Result<DiskCaptureStats, DiskError>;
    const MANIFEST: Manifest = Manifest::new("bench.DiskImport");
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

/// Put `0` tiny blobs from inside one command handler — the same `await` the disk
/// facet performs per block, with the block taken away.
///
/// The control for `import`: identical seam, identical number of round trips through
/// it, one byte of payload instead of a mebibyte. If the two agree per put, the cost
/// is paid per `await` and the block size is irrelevant to it; if they differ by the
/// bytes, it is paid per byte and the block size is the whole lever.
#[derive(Clone, Serialize, Deserialize)]
struct TinyPuts(u32);
impl Message for TinyPuts {
    /// The number of puts made — a value rather than `()` purely so the benchmark
    /// has something to hand `black_box`.
    type Reply = u32;
    const MANIFEST: Manifest = Manifest::new("bench.DiskTinyPuts");
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

/// Run the capture command: scan, diff block hashes against the committed index,
/// put the dirty blocks, stage one manifest.
#[derive(Clone, Serialize, Deserialize)]
struct CaptureNow;
impl Message for CaptureNow {
    type Reply = Result<DiskCaptureStats, DiskError>;
    const MANIFEST: Manifest = Manifest::new("bench.DiskCapture");
}
impl GrainHandler<CaptureNow> for DiskBox {
    async fn handle(
        &self,
        _state: &(),
        _msg: CaptureNow,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Result<DiskCaptureStats, DiskError>) {
        (vec![], ctx.disk().capture().await)
    }
}

fn sim_system(sim: &Simulation, recorder: &Recorder) -> SimSystem {
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build()
}

/// A config that hibernates nothing and snapshots nothing for the length of a run.
///
/// Both would otherwise land inside the measurement at an interval decided by how
/// many iterations divan chose, which is the one thing a benchmark must not let
/// vary. What is being measured is the capture path, not the hibernation policy;
/// what a checkpoint costs is `benches/store.rs`'s subject.
fn config(scratch: &std::path::Path, store: Option<granary::GrainStoreFactory>) -> GranaryConfig {
    GranaryConfig {
        idle_after: Duration::from_secs(3600),
        snapshot_every: 0,
        data_dir: Some(scratch.to_path_buf()),
        grain_store: store,
        ..GranaryConfig::default()
    }
}

/// A source image of `mib` MiB, each block distinguishable from its neighbours so
/// nothing dedups against anything else in the same image.
fn write_source(path: &std::path::Path, mib: usize) {
    let mut bytes = vec![0x5a; mib * BLOCK];
    for idx in 0..mib {
        bytes[idx * BLOCK..idx * BLOCK + 8].copy_from_slice(&(idx as u64).to_le_bytes());
    }
    std::fs::write(path, &bytes).expect("write the source image");
}

/// Make every block of the source image content no store has seen, by rewriting one
/// byte per block.
///
/// A generation byte per block is enough: the id is BLAKE3 of the whole block, so a
/// single differing byte makes all `mib` of them cold puts. That matters because the
/// alternative — re-importing the same file — measures the dedup hit path and would
/// report a create as far cheaper than the first one actually is. It is also cheap
/// enough to sit in untimed setup: `mib` one-byte writes, not `mib` MiB of them.
fn refresh_source(path: &std::path::Path, mib: usize, stamp: u8) {
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open the source image");
    for idx in 0..mib {
        file.seek(SeekFrom::Start((idx * BLOCK) as u64))
            .expect("seek");
        file.write_all(&[stamp]).expect("stamp the block");
    }
    file.sync_all().expect("flush the source image");
}

/// The whole `import` path against `store`, swept over image size: read a block,
/// write it into the activation-local image, hash it, put it, and — once — stage and
/// commit one full-coverage manifest.
///
/// The per-block counter is the number to read. Divided into the iteration time it
/// gives what one block costs end to end, which is directly comparable with layers 1
/// and 2 above; the byte counter is there so the same run can be read as a
/// throughput against the ~125 MB/s uplink the four-minute observation was measured
/// against.
///
/// Every iteration re-imports into the *same* grain, which is what `import` is for
/// (it replaces the image wholesale). Only the content is made fresh, so the
/// activation, the journal, and the segment stay warm across iterations — a create
/// on a live node, not a cold start.
///
/// **Keep the sample count small.** An import roots every block it puts and nothing
/// sweeps them inside one activation (a `RootSet` is union-kept, F3), so each sample
/// leaves a whole image behind in the store. Enough samples and the numbers stop
/// describing the path and start describing allocator pressure or a filling disk —
/// the two callers below hold the retained total to a few hundred megabytes.
fn import(bencher: Bencher, mib: usize, store: Option<granary::GrainStoreFactory>) {
    let scratch = tempfile::tempdir().expect("scratch dir");
    let src = scratch.path().join("base.img");
    write_source(&src, mib);
    let src_arg = src.to_string_lossy().into_owned();

    let sim = Simulation::new(17);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let boxes = system.granary::<DiskBox>(config(scratch.path(), store));
    let grain = boxes.grain("box/import");

    let mut stamp = 0u8;
    bencher
        .counter(ItemsCount::new(mib))
        .counter(BytesCount::new(mib * BLOCK))
        .with_inputs(|| {
            stamp = stamp.wrapping_add(1);
            refresh_source(&src, mib, stamp);
            src_arg.clone()
        })
        .bench_local_values(|src_arg| {
            let grain = grain.clone();
            sim.block_on(async move {
                black_box(
                    grain
                        .ask(ImportFrom(src_arg))
                        .await
                        .expect("ask")
                        .expect("import"),
                )
            })
        });
}

/// Provisioning into the in-memory store: the facet, the journal seam, and the
/// staging, with the device removed.
#[divan::bench(args = [4, 16, 64], sample_count = 5, sample_size = 1)]
fn import_memory(bencher: Bencher, mib: usize) {
    import(bencher, mib, None);
}

/// Provisioning into the file store: everything `import_memory` does, plus an
/// `atomic_replace` per block and the record's own framed, fsynced append.
///
/// The gap between the two is the durability of a create. The gap between this and
/// `put_file` times the block count is what the layers above the store add.
#[divan::bench(args = [4, 16], sample_count = 5, sample_size = 1)]
fn import_file(bencher: Bencher, mib: usize) {
    let dir = tempfile::tempdir().expect("store dir");
    import(
        bencher,
        mib,
        Some(FileGrainStore::factory(dir.path(), &JsonCodec)),
    );
    drop(dir);
}

/// The blob-put seam alone, from inside a command handler, swept over the same put
/// counts `import` performs at 4, 16 and 64 MiB.
///
/// Read this against `import_memory` at the matching size. Both make the same number
/// of trips through `GrainBlobs::put` into the same in-memory store from inside one
/// handler; the only difference is that these carry four bytes and those carry a
/// mebibyte. Whatever the two have in common is not the bytes.
#[divan::bench(args = [4, 16, 64])]
fn puts(bencher: Bencher, count: usize) {
    let scratch = tempfile::tempdir().expect("scratch dir");
    let sim = Simulation::new(21);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let boxes = system.granary::<DiskBox>(config(scratch.path(), None));
    let grain = boxes.grain("box/puts");

    bencher.counter(ItemsCount::new(count)).bench_local(|| {
        let grain = grain.clone();
        sim.block_on(
            async move { black_box(grain.ask(TinyPuts(count as u32)).await.expect("ask")) },
        )
    });
}

/// A capture of an untouched image: the full scan, every block clean, nothing put
/// and nothing staged (§7.5 — the command rides the read path).
///
/// This is the floor under *every* capture, and the one a checkpointing machine pays
/// on each quiescent point whether or not the guest wrote anything. It should come
/// out at layer 1's read plus hash times the block count; a gap means the scan
/// itself carries per-block cost the facet added, and that gap is the thing a block
/// size would be tuned against.
#[divan::bench(args = [4, 16, 64])]
fn scan(bencher: Bencher, mib: usize) {
    let scratch = tempfile::tempdir().expect("scratch dir");
    let src = scratch.path().join("base.img");
    write_source(&src, mib);

    let sim = Simulation::new(19);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let boxes = system.granary::<DiskBox>(config(scratch.path(), None));
    let grain = boxes.grain("box/scan");

    let src_arg = src.to_string_lossy().into_owned();
    {
        let grain = grain.clone();
        sim.block_on(async move {
            grain
                .ask(ImportFrom(src_arg))
                .await
                .expect("ask")
                .expect("import");
        });
    }

    bencher
        .counter(ItemsCount::new(mib))
        .counter(BytesCount::new(mib * BLOCK))
        .bench_local(|| {
            let grain = grain.clone();
            let stats =
                sim.block_on(
                    async move { grain.ask(CaptureNow).await.expect("ask").expect("capture") },
                );
            debug_assert_eq!(stats.blocks, 0, "an untouched image is clean");
            black_box(stats)
        });
}
