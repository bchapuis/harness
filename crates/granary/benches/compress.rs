//! What LZ4 would buy on the replication path, and what it would cost.
//!
//! The case for compressing replication is an arithmetic one (`docs/hardware-envelope.md`
//! §3.3): the uplink is ~125 MB/s and every byte of a blob crosses it `R−1` times
//! (§3.9), while LZ4 compresses at hundreds of megabytes per second on one core. So a
//! ratio above roughly 1.2x already pays for its own CPU, and the question was never
//! whether compression is cheap — it is whether *this deployment's payloads compress*.
//! That is a property of the data, not of the algorithm, and nothing in the tree had
//! measured it.
//!
//! Two numbers per payload class, because the trade needs both:
//!
//! - **ratio** — bytes off the uplink, measured **per unit** (per blob, per record
//!   batch), never over a concatenated stream. The distinction is not pedantic: a
//!   stream lets LZ4 find redundancy *across* units that the wire will never present
//!   together, and it inflates the ratio several-fold on exactly the small payloads
//!   where the answer is most marginal.
//! - **throughput** — the CPU that buys it, on the leader for the encode and on every
//!   peer for the decode.
//!
//! The payload classes are the ones that actually cross the path: 1 MiB disk-image
//! blocks (the bulk of a machine create), grain record batches (an agent session's
//! transcript), and the small control records that dominate by *count*.
//!
//! **Two constraints on any implementation**, both established here rather than
//! discovered later:
//!
//! - It must sit **below the blob id**. A `BlobId` is BLAKE3 of the *plaintext*, and
//!   it is the dedup key and the name in every durable manifest, so hashing compressed
//!   bytes would silently break both. Compress after hashing on the way out;
//!   decompress before verifying on the way in.
//! - It must keep whichever form is **smaller**, and say which it kept. The small
//!   control records below are not a rounding error: nine in ten of them come out
//!   larger, because LZ4's frame overhead exceeds what a few hundred bytes can save.
//!   A compressor applied unconditionally would make the most numerous payload class
//!   on the wire worse.
//!
//! What it needs from outside this file is a way to say which form a payload carries
//! without an older peer reading the tag as blob content and failing the content hash.
//! `Transport::peer_version` is that: the association reports what it settled on, and
//! a build accepting two revisions writes the tagged form only to a peer that settled
//! on the higher one. Taking it is the **V4** two-release path — widen `WIRE` to accept
//! 1..=2, write 2 in a later release — against the alternative of capability
//! negotiation, a new blob message older peers reject with `Unhandled` and the sender
//! caches per peer. Both are rollout decisions rather than measurements, which is why
//! this file takes neither.
//!
//! Run with `cargo bench -p granary --bench compress`. If a `machine-data/` or
//! `harness-data/` directory from a real run is present at the workspace root, the
//! blob and transcript benches read it; otherwise they fall back to generated payloads
//! shaped like it and say so. Real data is worth more here than anywhere else in the
//! tree, because the whole question is what the bytes look like.

use std::path::PathBuf;

use divan::Bencher;
use divan::black_box;
use divan::counter::BytesCount;

fn main() {
    // The ratio table is the point of this file and is not a timing measurement, so
    // it prints once, before divan takes over for the throughput half.
    report_ratios();
    divan::main();
}

/// The disk facet's block size (spec §7.15) — the unit a machine create ships.
const BLOCK: usize = 1 << 20;

/// The workspace root, from this crate's manifest directory.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

/// Up to `limit` files under `rel` whose path and size both match, as owned byte
/// vectors — the real payloads from a previous run, when the tree has them.
///
/// The path filter is what keeps this honest. A run directory holds several kinds of
/// file, and the ones that never cross the wire are the most compressible of all: a
/// machine's materialized image under `facets/` is a sparse multi-megabyte file that
/// is mostly zeros, and sampling it reports a ratio no replicated payload will ever
/// see. Only the units the replication path actually ships are admitted.
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
/// incompressible — the shape a provisioned filesystem image actually has, and
/// deliberately not all-zeros, which would report an unreachable ratio.
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

/// The blob payloads: real 1 MiB image blocks from a machine run's blob area — the
/// exact unit `StoreBlob` carries, one per `put_blob`.
fn blocks() -> (Vec<Vec<u8>>, &'static str) {
    let real = sample("machine-data", "blobs", |m| m.len() == BLOCK as u64, 48);
    if real.is_empty() {
        ((0..16).map(synthetic_block).collect(), "generated")
    } else {
        (real, "machine-data")
    }
}

/// The small-control-record payloads: real Raft log files, which is where
/// `EntryPayload::App` — every grain journal record reaching a follower — lives.
///
/// Kept as its own class rather than folded into the records above, because it is the
/// one that answers *against* compressing: these are hundreds of bytes each, and LZ4's
/// frame overhead exceeds what it saves on most of them.
fn control() -> (Vec<Vec<u8>>, &'static str) {
    let real = sample("harness-data", "raft", |m| m.len() > 0, 96);
    if real.is_empty() {
        let generated = (0..64u32)
            .map(|i| i.to_le_bytes().repeat(4).to_vec())
            .collect();
        (generated, "generated")
    } else {
        (real, "harness-data")
    }
}

/// The record payloads: real grain segments from a harness run — an agent session's
/// journal, the thing a snapshot and a record batch are cut from.
fn records() -> (Vec<Vec<u8>>, &'static str) {
    let real = sample("harness-data", "segments", |m| m.len() > 0, 64);
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
        (real, "harness-data")
    }
}

/// Print the per-unit ratio table — the number this file exists to produce.
///
/// Reported with the count of units that came out **larger** than they went in, which
/// is the finding that shapes the design: LZ4's frame overhead exceeds the savings on
/// small control records, so a compressor on this path must keep whichever of the two
/// forms is smaller rather than compressing unconditionally.
fn report_ratios() {
    println!("\nper-unit LZ4 ratio (compressed individually, as the wire sends them)");
    println!(
        "{:<28} {:>6} {:>14} {:>14} {:>8} {:>6}",
        "payload", "units", "raw", "lz4", "ratio", "grew"
    );
    for (label, (units, source)) in [
        ("disk blocks", blocks()),
        ("grain records", records()),
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

/// Compressing one disk block — the cost paid once on the leader per block.
#[divan::bench]
fn compress_block(bencher: Bencher) {
    let (units, _) = blocks();
    let block = units.into_iter().next().expect("at least one block");
    bencher
        .counter(BytesCount::new(block.len()))
        .bench_local(|| black_box(lz4_flex::block::compress_prepend_size(black_box(&block))));
}

/// Decompressing one disk block — the cost paid on every peer, `R−1` times per block.
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
