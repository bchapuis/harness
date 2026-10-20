//! What LZ4 would buy on the replication path, and what it would cost.
//!
//! The case for compressing replication is arithmetic (`docs/hardware-envelope.md`
//! §3.3): the uplink is ~125 MB/s and every byte of a blob crosses it `R−1` times
//! (hw §3.9), while LZ4 compresses at hundreds of megabytes per second on one core. A
//! ratio above roughly 1.2x already pays for its own CPU, so the question is whether
//! *this deployment's payloads compress*, a property of the data rather than of the
//! algorithm.
//!
//! Two numbers per payload class, because the trade needs both:
//!
//! - **ratio**: bytes off the uplink, measured **per unit** (per blob, per record
//!   batch), never over a concatenated stream. A stream lets LZ4 find redundancy
//!   *across* units that the wire will never present together, which inflates the
//!   ratio several-fold on exactly the small payloads where the answer is most
//!   marginal.
//! - **throughput**: the CPU that buys it, on the leader for the encode and on every
//!   peer for the decode.
//!
//! The payload classes are the ones that cross the path: 1 MiB disk-image blocks (the
//! bulk of a machine create), entity record batches (an agent session's transcript),
//! and the small control records that dominate by *count*.
//!
//! **Two constraints on any implementation**:
//!
//! - It must sit **below the blob id**. A `BlobId` is BLAKE3 of the *plaintext*, and
//!   it is the dedup key and the name in every durable manifest, so hashing compressed
//!   bytes would silently break both. Compress after hashing on the way out;
//!   decompress before verifying on the way in.
//! - It must keep whichever form is **smaller**, and say which it kept. Nine in ten of
//!   the small control records below come out larger, because LZ4's frame overhead
//!   exceeds what a few hundred bytes can save. A compressor applied unconditionally
//!   would make the most numerous payload class on the wire worse.
//!
//! Saying which form a payload carries needs a tag a peer will not read as blob
//! content and fail the content hash on: a wire-format change, which every peer
//! takes at once (compatibility §3.3). That is a format decision rather than a
//! measurement, which is why this file does not take it.
//!
//! Run with `cargo bench -p entity --bench compress`. If a `machine-data/` or
//! `agent-data/` directory from a real run is present at the workspace root, the blob
//! and transcript benches read it; otherwise they fall back to generated payloads
//! shaped like it and say so.

use std::path::PathBuf;

use divan::Bencher;
use divan::black_box;
use divan::counter::BytesCount;

fn main() {
    // The ratio table is not a timing measurement, so it prints once, before divan
    // takes over for the throughput half.
    report_ratios();
    divan::main();
}

/// The disk facet's block size (spec facets §5.4): the unit a machine create ships.
const BLOCK: usize = 1 << 20;

/// The workspace root, from this crate's manifest directory.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

/// Up to `limit` files under `rel` whose path and size both match, as owned byte
/// vectors: the real payloads from a previous run, when the tree has them.
///
/// The path filter keeps this honest. A run directory holds several kinds of file, and
/// the ones that never cross the wire are the most compressible of all: a machine's
/// materialized image under `facets/` is a sparse multi-megabyte file that is mostly
/// zeros, and sampling it reports a ratio no replicated payload will ever see. Only
/// the units the replication path ships are admitted.
fn sample(
    rel: &str,
    segment: &str,
    pick: impl Fn(&std::fs::Metadata) -> bool,
    limit: usize,
) -> Vec<Vec<u8>> {
    fn walk(dir: &PathBuf, out: &mut Vec<PathBuf>, depth: usize) {
        if depth == 0 {
            return;
        }
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out, depth - 1);
            } else {
                out.push(path);
            }
        }
    }
    let mut paths = Vec::new();
    walk(&workspace_root().join(rel), &mut paths, 8);
    paths.sort();
    paths
        .into_iter()
        .filter(|p| p.components().any(|c| c.as_os_str() == segment))
        .filter(|p| std::fs::metadata(p).map(|m| pick(&m)).unwrap_or(false))
        .take(limit)
        .filter_map(|p| std::fs::read(p).ok())
        .collect()
}

/// A generated stand-in for a disk-image block: mostly structured, partly
/// incompressible; the shape a provisioned filesystem image has, and not all-zeros,
/// which would report an unreachable ratio.
fn synthetic_block(seed: u64) -> Vec<u8> {
    let mut bytes = vec![0u8; BLOCK];
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    // A quarter of the block is pseudo-random; the rest keeps the zeros and repeated
    // structure that filesystem metadata and unallocated extents contribute.
    for chunk in bytes.chunks_mut(4).take(BLOCK / 16) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&(state as u32).to_le_bytes());
    }
    bytes
}

/// The blob payloads: real 1 MiB image blocks from a machine run's blob area; the
/// exact unit `PutBlob` carries, one per `put_blob`.
fn blocks() -> (Vec<Vec<u8>>, &'static str) {
    let real = sample("machine-data", "blobs", |m| m.len() == BLOCK as u64, 48);
    if real.is_empty() {
        ((0..16).map(synthetic_block).collect(), "generated")
    } else {
        (real, "machine-data")
    }
}

/// The small-control-record payloads: real Raft log files, which is where
/// `EntryPayload::App` (every entity journal record reaching a follower) lives.
///
/// Its own class rather than folded into the records above, because it is the one
/// that answers *against* compressing: hundreds of bytes each, and LZ4's frame
/// overhead exceeds what it saves on most of them.
fn control() -> (Vec<Vec<u8>>, &'static str) {
    let real = sample("agent-data", "raft", |m| m.len() > 0, 96);
    if real.is_empty() {
        let generated = (0..64u32)
            .map(|i| i.to_le_bytes().repeat(4).to_vec())
            .collect();
        (generated, "generated")
    } else {
        (real, "agent-data")
    }
}

/// The record payloads: real entity segments from an agent run; an agent session's
/// journal, the thing a snapshot and a record batch are cut from.
fn records() -> (Vec<Vec<u8>>, &'static str) {
    let real = sample("agent-data", "segments", |m| m.len() > 0, 64);
    if real.is_empty() {
        let generated = (0..64)
            .map(|i| {
                format!(
                    "{{\"turn\":{i},\"role\":\"assistant\",\"content\":\"{}\"}}",
                    "the quick brown fox jumps over the lazy dog. ".repeat(8)
                )
                .into_bytes()
            })
            .collect();
        (generated, "generated")
    } else {
        (real, "agent-data")
    }
}

/// Print the per-unit ratio table.
///
/// Reported with the count of units that came out **larger** than they went in: LZ4's
/// frame overhead exceeds the savings on small control records, so a compressor on
/// this path must keep whichever form is smaller.
fn report_ratios() {
    println!("\nper-unit LZ4 ratio (compressed individually, as the wire sends them)");
    println!(
        "{:<28} {:>6} {:>14} {:>14} {:>8} {:>6}",
        "payload", "units", "raw", "lz4", "ratio", "grew"
    );
    for (label, (units, source)) in [
        ("disk blocks", blocks()),
        ("entity records", records()),
        ("small control records", control()),
    ] {
        let mut raw = 0usize;
        let mut packed = 0usize;
        let mut grew = 0usize;
        for unit in &units {
            let out = lz4_flex::block::compress_prepend_size(unit);
            raw += unit.len();
            packed += out.len();
            if out.len() >= unit.len() {
                grew += 1;
            }
        }
        if raw == 0 {
            continue;
        }
        println!(
            "{:<28} {:>6} {:>14} {:>14} {:>7.2}x {:>6}  [{source}]",
            label,
            units.len(),
            raw,
            packed,
            raw as f64 / packed as f64,
            grew,
        );
    }
    println!();
}

/// Compressing one disk block: the cost paid once on the leader per block.
#[divan::bench]
fn compress_block(bencher: Bencher) {
    let (units, _) = blocks();
    let block = units.into_iter().next().expect("at least one block");
    bencher
        .counter(BytesCount::new(block.len()))
        .bench_local(|| black_box(lz4_flex::block::compress_prepend_size(black_box(&block))));
}

/// Decompressing one disk block: the cost paid on every peer, `R−1` times per block.
#[divan::bench]
fn decompress_block(bencher: Bencher) {
    let (units, _) = blocks();
    let block = units.into_iter().next().expect("at least one block");
    let packed = lz4_flex::block::compress_prepend_size(&block);
    bencher
        .counter(BytesCount::new(block.len()))
        .bench_local(|| {
            black_box(
                lz4_flex::block::decompress_size_prepended(black_box(&packed)).expect("round trip"),
            )
        });
}
