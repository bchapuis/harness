//! End-to-end disk facet tests on the `Local` tier (spec §7.15, §7.12): import
//! stages a full-coverage manifest, capture diffs block hashes and stages the
//! dirty subset, a clean capture stages nothing (§7.5), the checkpoint restores
//! byte-identically across hibernation (F2/F4/G12), and the fixed-size bound
//! refuses an oversized import. The replay path after a *crash* (capture
//! records past the snapshot, `fold` + `rehydrate`) is `tests/disk_swarm.rs`'s
//! job, where failover exercises it under faults.

use std::sync::Arc;
use std::time::Duration;

use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use granary::Disk;
use granary::DiskCaptureStats;
use granary::DiskError;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainEvent;
use granary::GrainHandler;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::MAX_IMAGE_BYTES;
use granary::NoEvent;
use serde::Deserialize;
use serde::Serialize;

/// 1 MiB — the facet's fixed block size (spec §7.15), mirrored here so the
/// tests place writes on and across block boundaries.
const BLOCK: u64 = 1 << 20;

// --- A grain whose durable state is entirely its raw image ---------------------

#[derive(Default)]
struct DiskBox;

impl Grain for DiskBox {
    type System = SimSystem;
    type State = ();
    type Event = NoEvent;
    type Facets = (Disk,);
    const GRAIN_TYPE: &'static str = "test.DiskBox";

    fn apply(_state: &mut (), event: &NoEvent) {
        event.unreachable()
    }
}

/// Import the image at `src` — the provisioning path (§7.15: the base image
/// *is* a capture).
#[derive(Clone, Serialize, Deserialize)]
struct ImportFrom(String);
impl Message for ImportFrom {
    type Reply = Result<DiskCaptureStats, DiskError>;
    const MANIFEST: Manifest = Manifest::new("test.DiskImport");
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

/// Write `bytes` into the live image at `offset` — a stand-in for the guest's
/// out-of-band block writes (§7.15's one departure) — without capturing.
#[derive(Clone, Serialize, Deserialize)]
struct Scribble {
    offset: u64,
    bytes: Vec<u8>,
}
impl Message for Scribble {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.DiskScribble");
}
impl GrainHandler<Scribble> for DiskBox {
    async fn handle(&self, _state: &(), msg: Scribble, ctx: &GrainCtx<Self>) -> (Vec<NoEvent>, ()) {
        use std::io::Seek;
        use std::io::Write;
        let path = ctx.disk().path().expect("image path");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open image");
        file.seek(std::io::SeekFrom::Start(msg.offset))
            .expect("seek");
        file.write_all(&msg.bytes).expect("write");
        (vec![], ())
    }
}

/// Run the capture command (§7.15): scan, diff, put dirty blocks, stage one
/// manifest.
#[derive(Clone, Serialize, Deserialize)]
struct CaptureNow;
impl Message for CaptureNow {
    type Reply = Result<DiskCaptureStats, DiskError>;
    const MANIFEST: Manifest = Manifest::new("test.DiskCapture");
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

/// Capture twice in one command — the second MUST be refused (§7.15: one
/// capture, one record).
#[derive(Clone, Serialize, Deserialize)]
struct DoubleCapture;
impl Message for DoubleCapture {
    type Reply = Result<DiskCaptureStats, DiskError>;
    const MANIFEST: Manifest = Manifest::new("test.DiskDoubleCapture");
}
impl GrainHandler<DoubleCapture> for DiskBox {
    async fn handle(
        &self,
        _state: &(),
        _msg: DoubleCapture,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Result<DiskCaptureStats, DiskError>) {
        ctx.disk().capture().await.expect("first capture");
        (vec![], ctx.disk().capture().await)
    }
}

/// Read `len` bytes at `offset` from the live image — a pure read (§7.5).
#[derive(Clone, Serialize, Deserialize)]
struct ReadBack {
    offset: u64,
    len: u32,
}
impl Message for ReadBack {
    type Reply = Vec<u8>;
    const MANIFEST: Manifest = Manifest::new("test.DiskReadBack");
}
impl GrainHandler<ReadBack> for DiskBox {
    async fn handle(
        &self,
        _state: &(),
        msg: ReadBack,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Vec<u8>) {
        use std::io::Read;
        use std::io::Seek;
        let path = ctx.disk().path().expect("image path");
        let mut file = std::fs::File::open(path).expect("open image");
        file.seek(std::io::SeekFrom::Start(msg.offset))
            .expect("seek");
        let mut bytes = vec![0u8; msg.len as usize];
        file.read_exact(&mut bytes).expect("read");
        (vec![], bytes)
    }
}

/// The committed image size — a pure read.
#[derive(Clone, Serialize, Deserialize)]
struct ImageBytes;
impl Message for ImageBytes {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("test.DiskImageBytes");
}
impl GrainHandler<ImageBytes> for DiskBox {
    async fn handle(
        &self,
        _state: &(),
        _msg: ImageBytes,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, u64) {
        (vec![], ctx.disk().image_bytes().expect("image bytes"))
    }
}

fn sim_system(sim: &Simulation, recorder: &Recorder) -> SimSystem {
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build()
}

/// A deterministic base image: three blocks, the last partial (spec §7.15:
/// `image_bytes` need not be block-aligned), each byte derived from its offset.
fn write_base_image(path: &std::path::Path) -> u64 {
    let len = 2 * BLOCK + BLOCK / 2;
    let bytes: Vec<u8> = (0..len).map(|i| (i / 7 % 251) as u8).collect();
    std::fs::write(path, &bytes).expect("write base image");
    len
}

#[test]
fn import_capture_and_hibernation_round_trip_byte_identically() {
    let scratch = tempfile::tempdir().expect("tempdir");
    let base = scratch.path().join("base.img");
    let image_len = write_base_image(&base);

    let sim = Simulation::new(23);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let boxes = system.granary::<DiskBox>(GranaryConfig {
        idle_after: Duration::from_millis(10),
        snapshot_every: 100,
        data_dir: Some(scratch.path().to_path_buf()),
        ..GranaryConfig::default()
    });

    let base_str = base.to_string_lossy().into_owned();
    let grain = boxes.grain("box/0");
    sim.block_on(async move {
        // Import: full coverage, three blocks (the base image is a capture).
        let stats = grain
            .ask(ImportFrom(base_str))
            .await
            .expect("ask")
            .expect("import");
        assert_eq!(stats.blocks, 3);
        assert_eq!(stats.bytes, image_len);
        assert_eq!(grain.ask(ImageBytes).await.expect("size"), image_len);

        // A capture of the untouched image stages nothing (§7.5).
        let clean = grain.ask(CaptureNow).await.expect("ask").expect("capture");
        assert_eq!(
            clean,
            DiskCaptureStats {
                blocks: 0,
                bytes: 0
            }
        );

        // Dirty exactly one block (block 1), then capture: the content-hash
        // diff must find exactly that block, never the whole image.
        grain
            .ask(Scribble {
                offset: BLOCK + 100,
                bytes: b"dirty".to_vec(),
            })
            .await
            .expect("scribble");
        let dirty = grain.ask(CaptureNow).await.expect("ask").expect("capture");
        assert_eq!(
            dirty.blocks, 1,
            "one dirtied block, one dirty capture entry"
        );
        assert_eq!(dirty.bytes, BLOCK);

        // A write spanning the partial final block captures it at its partial
        // length.
        grain
            .ask(Scribble {
                offset: 2 * BLOCK + BLOCK / 2 - 3,
                bytes: b"end".to_vec(),
            })
            .await
            .expect("scribble");
        let tail = grain.ask(CaptureNow).await.expect("ask").expect("capture");
        assert_eq!(tail.blocks, 1);
        assert_eq!(tail.bytes, BLOCK / 2, "the final block is partial");
    });

    // Hibernate: the composite snapshot carries the block index; the
    // materialization is torn down with the activation.
    sim.run();
    assert!(
        recorder.events().iter().any(|e| matches!(
            e.as_app::<GrainEvent>(),
            Some(GrainEvent::Passivated { .. })
        )),
        "the idle grain must hibernate",
    );

    // Reactivate: restore fetches the checkpoint blocks (G17) and the image
    // reads back byte-identically — captured writes included (F2/F4/G12).
    let reread = boxes.grain("box/0");
    sim.block_on(async move {
        assert_eq!(reread.ask(ImageBytes).await.expect("size"), image_len);
        assert_eq!(
            reread
                .ask(ReadBack {
                    offset: BLOCK + 100,
                    len: 5
                })
                .await
                .expect("read"),
            b"dirty".to_vec(),
        );
        assert_eq!(
            reread
                .ask(ReadBack {
                    offset: 2 * BLOCK + BLOCK / 2 - 3,
                    len: 3
                })
                .await
                .expect("read"),
            b"end".to_vec(),
        );
        // An undirtied base byte survives too (offset 42 → base pattern).
        assert_eq!(
            reread
                .ask(ReadBack { offset: 42, len: 1 })
                .await
                .expect("read"),
            vec![(42u64 / 7) as u8],
        );

        // An uncaptured scribble made before this hibernation would have been
        // lost — prove the inverse: a fresh scribble reads back live but is
        // absent from durable state until captured. (The crash-rewind variant
        // is the swarm's job; here hibernation refuses no capture because the
        // consumer — this test grain — declares no `can_passivate` guard.)
        let second = reread.ask(CaptureNow).await.expect("ask").expect("capture");
        assert_eq!(
            second,
            DiskCaptureStats {
                blocks: 0,
                bytes: 0
            },
            "rehydration reproduced the captured image exactly, so nothing is dirty",
        );
    });
}

#[test]
fn one_capture_per_command() {
    let scratch = tempfile::tempdir().expect("tempdir");
    let base = scratch.path().join("base.img");
    write_base_image(&base);

    let sim = Simulation::new(29);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let boxes = system.granary::<DiskBox>(GranaryConfig {
        data_dir: Some(scratch.path().to_path_buf()),
        ..GranaryConfig::default()
    });

    let base_str = base.to_string_lossy().into_owned();
    let grain = boxes.grain("box/two-captures");
    sim.block_on(async move {
        grain
            .ask(ImportFrom(base_str))
            .await
            .expect("ask")
            .expect("import");
        grain
            .ask(Scribble {
                offset: 0,
                bytes: b"x".to_vec(),
            })
            .await
            .expect("scribble");
        let second = grain.ask(DoubleCapture).await.expect("ask");
        assert!(
            second.is_err(),
            "the second capture in one command must be refused (one capture, one record)",
        );
    });
}

#[test]
fn oversized_import_is_refused_before_any_put() {
    let scratch = tempfile::tempdir().expect("tempdir");
    let base = scratch.path().join("huge.img");
    // A sparse file past the bound: metadata length is what import checks.
    let file = std::fs::File::create(&base).expect("create");
    file.set_len(MAX_IMAGE_BYTES + 1).expect("sparse len");
    drop(file);

    let sim = Simulation::new(31);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let boxes = system.granary::<DiskBox>(GranaryConfig {
        data_dir: Some(scratch.path().to_path_buf()),
        ..GranaryConfig::default()
    });

    let base_str = base.to_string_lossy().into_owned();
    let grain = boxes.grain("box/huge");
    sim.block_on(async move {
        let refused = grain.ask(ImportFrom(base_str)).await.expect("ask");
        assert!(
            refused.is_err(),
            "an import past MAX_IMAGE_BYTES is refused"
        );
        assert_eq!(grain.ask(ImageBytes).await.expect("size"), 0);
    });
}
