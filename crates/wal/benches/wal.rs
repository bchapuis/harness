//! What an append and a recovery cost.
//!
//! The three benchmarks here split the log's work along the line that matters when
//! reading a result: [`checksum`](wal::checksum) and the replay are pure CPU, while an
//! append is dominated by the fsync it ends with. That fsync is the point of the call —
//! it is not overhead to be benchmarked away — so `append` is measured *with* it, and the
//! framing work underneath is watched through the allocation counter instead, which the
//! fsync cannot drown out. A framing change that removes allocations shows up in
//! `alloc` even on a machine whose fsync costs a millisecond.
//!
//! Replay is where the checksum's throughput reaches the product: `Wal::open` verifies
//! every frame in the file before the first grain activates.

use divan::Bencher;
use divan::black_box;
use divan::counter::BytesCount;
use divan::counter::ItemsCount;
use serde::Deserialize;
use serde::Serialize;

/// Allocation counts are reported beside the timings. For this crate they are the
/// fsync-independent measure of the framing path.
#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// The record shape the benchmarks frame: a payload whose width the caller picks, so a
/// result can be read against a byte count rather than a record count alone.
#[derive(Serialize, Deserialize, Clone)]
struct Rec {
    seq: u64,
    payload: Vec<u8>,
}

impl Rec {
    fn of(bytes: usize) -> Rec {
        Rec {
            seq: 0,
            payload: vec![0x5a; bytes],
        }
    }
}

/// Wide enough for every payload below, and under the magic's `u32` reading as
/// [`wal::Wal::open`] requires.
const MAX_RECORD: u32 = 1 << 20;

/// The caller's record schema. The benchmarks never cross a revision, so any fixed
/// window does.
const RECORDS: compat::Window = compat::Window::at("bench.records", 1);

/// The sidecar digest, at the width sidecars actually are.
///
/// [`wal::checksum`](wal::checksum) is frozen — a sidecar has no header naming which
/// digest wrote it, so its value cannot change without every existing one reading back as
/// missing. It is benchmarked at eight bytes because that is the only size anything calls
/// it with (a fence term, a seal bound, a tombstone timestamp), and at that width a
/// byte-at-a-time loop is already a handful of cycles. Speed is not the interesting
/// property here; the number is here so a future change to this function is visible.
///
/// The frame digest is a different function and is deliberately private. Its throughput
/// reaches the product through `replay`, below, which is where it is measured.
#[divan::bench]
fn sidecar_checksum(bencher: Bencher) {
    let bytes = 0u64.to_le_bytes();
    bencher
        .counter(BytesCount::of_slice(&bytes))
        .bench(|| wal::checksum(black_box(&bytes)));
}

/// One `append_batch` — framing plus one write plus one fsync — against a real file.
///
/// The fsync is included because it is what the call promises; the timing is therefore a
/// property of the filesystem as much as of this crate, and the number to watch across a
/// framing change is `alloc`, not `median`. Sweeping the batch width also shows the
/// batch's own value: the fsync is paid once per call, not once per record.
#[divan::bench(args = [1, 8, 64, 1024])]
fn append(bencher: Bencher, records: usize) {
    let dir = tempfile::tempdir().expect("scratch dir");
    let batch: Vec<Rec> = (0..records).map(|_| Rec::of(256)).collect();
    let (mut log, _) =
        wal::Wal::<Rec>::open(dir.path().join("log"), MAX_RECORD, &RECORDS).expect("open");
    bencher
        .counter(ItemsCount::new(records))
        .bench_local(|| log.append_batch(black_box(&batch)).expect("append"));
}

/// Recovery: read the file, verify every frame's checksum, and decode every record.
///
/// No fsync is on this path, so the timing is the checksum and postcard decode directly —
/// and it is the latency a node pays before its first grain can serve a request.
#[divan::bench(args = [1_000, 10_000, 100_000])]
fn replay(bencher: Bencher, records: usize) {
    let dir = tempfile::tempdir().expect("scratch dir");
    let path = dir.path().join("log");
    let batch: Vec<Rec> = (0..records).map(|_| Rec::of(256)).collect();
    let (mut log, _) = wal::Wal::<Rec>::open(&path, MAX_RECORD, &RECORDS).expect("open");
    log.append_batch(&batch).expect("seed the log");
    drop(log);

    let bytes = std::fs::metadata(&path).expect("stat").len();
    bencher
        .counter(BytesCount::new(bytes))
        .counter(ItemsCount::new(records))
        .bench(|| {
            let (log, recovered) =
                wal::Wal::<Rec>::open(black_box(&path), MAX_RECORD, &RECORDS).expect("open");
            black_box((log, recovered));
        });
}
