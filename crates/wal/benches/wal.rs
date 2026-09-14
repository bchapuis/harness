//! What an append and a recovery cost.
//!
//! The three benchmarks here split the log's work along the line that matters when
//! reading a result: [`checksum`](wal::checksum) and the replay are pure CPU, while an
//! append is dominated by the fsync it ends with. That fsync is the point of the call,
//! so `append` is measured *with* it, and the framing work underneath is watched through
//! the allocation counter, which the fsync cannot drown out: a framing change that
//! removes allocations shows up in `alloc` even where an fsync costs a millisecond.
//!
//! Replay is where the checksum's throughput reaches the product: `Wal::open` verifies
//! every frame in the file before the first entity activates.

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

/// The caller's record schema; any fixed format does.
const RECORDS: compat::Format = compat::Format::new("bench.records", 1);

/// The digest at the width a sidecar value has. Sidecars are the only caller at eight
/// bytes (a fence term, a seal bound, a tombstone timestamp); the digest's throughput on
/// frames reaches the product through `replay`, below, which is where it is measured.
#[divan::bench]
fn sidecar_checksum(bencher: Bencher) {
    let bytes = 0u64.to_le_bytes();
    bencher
        .counter(BytesCount::of_slice(&bytes))
        .bench(|| wal::checksum(black_box(&bytes)));
}

/// One `append_batch` (framing plus one write plus one fsync) against a real file.
///
/// The fsync is what the call promises, so the timing is a property of the filesystem as
/// much as of this crate and the number to watch across a framing change is `alloc`, not
/// `median`. The batch sweep shows the batch's own value: the fsync is paid once per
/// call, not once per record.
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
/// No fsync is on this path, so the timing is the checksum and the postcard decode: the
/// latency a node pays before its first entity can serve a request.
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
