//! The data path: chunk a write into content blocks, and assemble a read from the
//! shadowing slice map (durable-workspace design).
//!
//! Writes go to the grain's colocated blob area before the metadata that references
//! them is journaled (§7.10), so a `BlobId` is never durable ahead of its bytes.
//! Reads resolve overlapping slices last-writer-wins (research §4.1) and fetch the
//! winning bytes, each verified against its id on the way out (G17).

use crate::BlobId;
use crate::GrainBlobs;
use crate::GrainError;

use super::meta::BLOCK_BYTES;
use super::meta::Block;
use super::meta::FileData;
use super::meta::Slice;

/// Chunk `content` into ≤[`BLOCK_BYTES`] immutable blocks, store each in the grain's
/// blob area, and return the [`Slice`] describing this write at `off` with birth
/// `seq`. The blob `put`s are durable on a write quorum before this returns, so the
/// slice the caller journals references only already-durable bytes.
pub(crate) async fn write_slice(
    blobs: &GrainBlobs,
    seq: u64,
    off: u64,
    content: &[u8],
) -> Result<Slice, GrainError> {
    let mut blocks = Vec::new();
    for chunk in content.chunks(BLOCK_BYTES.max(1)) {
        let id: BlobId = blobs.put(chunk.to_vec()).await?;
        blocks.push(Block {
            id,
            len: chunk.len() as u32,
        });
    }
    Ok(Slice {
        seq,
        off,
        len: content.len() as u64,
        blocks,
    })
}

/// Read `[start, end)` of a file: resolve the winning slice for each byte (highest
/// `seq` wins), fetch its bytes from the blob area, zero-fill holes, and clamp to the
/// file's logical size.
pub(crate) async fn read_file(
    blobs: &GrainBlobs,
    file: &FileData,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, GrainError> {
    let end = end.min(file.size);
    if start >= end {
        return Ok(Vec::new());
    }
    let mut out = vec![0u8; (end - start) as usize];
    // Paint slices in ascending `seq` order, so a later write overwrites an earlier
    // one wherever they overlap — last-writer-wins, with holes left as zero.
    let mut order: Vec<&Slice> = file.slices.iter().collect();
    order.sort_by_key(|s| s.seq);
    for slice in order {
        let lo = slice.off.max(start);
        let hi = (slice.off + slice.len).min(end);
        if lo >= hi {
            continue;
        }
        let bytes = slice_bytes(blobs, slice, lo - slice.off, hi - lo).await?;
        let dst = (lo - start) as usize;
        out[dst..dst + bytes.len()].copy_from_slice(&bytes);
    }
    Ok(out)
}

/// Fetch `len` bytes starting at offset `off` within a slice's concatenated blocks.
async fn slice_bytes(
    blobs: &GrainBlobs,
    slice: &Slice,
    off: u64,
    len: u64,
) -> Result<Vec<u8>, GrainError> {
    let mut out = Vec::with_capacity(len as usize);
    let want_end = off + len;
    let mut pos = 0u64;
    for block in &slice.blocks {
        let block_start = pos;
        let block_end = pos + block.len as u64;
        pos = block_end;
        if block_end <= off || block_start >= want_end {
            continue;
        }
        let lo = off.max(block_start) - block_start;
        let hi = want_end.min(block_end) - block_start;
        out.extend_from_slice(&blobs.get(block.id, Some(lo..hi)).await?);
    }
    Ok(out)
}
