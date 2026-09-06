//! Content-defined chunking: cut a byte string at boundaries the *content*
//! chooses, so an edit disturbs only the chunks it touches.
//!
//! The checkpointing facets cut at fixed offsets and are right to (§7.14, §7.15):
//! a SQLite page and a disk block live at a fixed address, so an unchanged page
//! occupies the same offsets in the next image and hashes to a blob already
//! stored. Facet 0's `State` has no such addressing. It is a re-encoded value —
//! an agent's folded transcript, say — and appending one turn moves every byte
//! after the insertion point, plus the length prefixes ahead of it. Cut at fixed
//! offsets, every chunk past the first change is a new blob and the whole state
//! is on the wire again.
//!
//! So the boundary is chosen by a rolling hash over a small window instead. A cut
//! lands wherever the last few dozen bytes hash into a rare pattern, which is a
//! property of the bytes around it and not of its distance from the start: insert
//! a kilobyte and the boundaries after it shift with the content, so the chunks
//! after the edit are byte-identical to before and dedup against blobs already
//! stored (§7.10).
//!
//! The hash is FastCDC's gear function without normalized chunking:
//! `h = (h << 1) + GEAR[byte]`, cut when the top bits are clear. Normalization
//! tightens the size distribution; the min/max bounds here already cap both tails,
//! which is what the transfer path cares about, so the simpler function is the one
//! in the tree.

/// The smallest chunk the splitter will emit (except the last).
///
/// A cut cannot be taken until this much has accumulated, which both suppresses
/// the run of tiny chunks a low-entropy region would otherwise produce and gives
/// the rolling hash a full window of content before its verdict counts. Under it,
/// the 32-byte id in the manifest starts to rival the bytes it names.
const MIN_CHUNK: usize = 16 * 1024;

/// The average chunk size the cut mask targets.
///
/// Bounded from both sides. Smaller chunks localize an edit better — the whole
/// point — but each is a separate quorum put with its own round trip and its own
/// 32 bytes of manifest, and past some width the per-chunk cost dominates the
/// bytes saved. 64 KiB puts a megabyte of state in ~16 chunks: an append disturbs
/// one or two of them, and the manifest costs half a kilobyte.
const AVG_CHUNK: usize = 64 * 1024;

/// The largest chunk the splitter will emit: a forced cut when the hash has not
/// produced one. Incompressible or highly repetitive content can starve the mask
/// for a long time, and the bound is what keeps one chunk from growing until it
/// is the whole payload again.
const MAX_CHUNK: usize = 256 * 1024;

/// How many bits of the hash a cut must clear: `log2(AVG_CHUNK)`, so each byte is
/// a boundary with probability `2^-MASK_BITS` and the expected run between cuts is
/// [`AVG_CHUNK`].
const MASK_BITS: u32 = AVG_CHUNK.trailing_zeros();

/// The bits a cut must clear, taken from the **top** of the hash.
///
/// Which end matters. `hash << 1` per byte means bit *j* has been fed only by the
/// last *j* bytes, so the low bits see a window of a few bytes and the high bits
/// see one of about sixty-four. Testing the top is what makes a boundary a
/// property of the surrounding content rather than of the byte under the cursor.
const MASK: u64 = ((1u64 << MASK_BITS) - 1) << (64 - MASK_BITS);

/// The gear table: one pseudo-random `u64` per byte value, mixed into the rolling
/// hash as each byte enters the window.
///
/// Derived from a fixed SplitMix64 sequence rather than written out, so the table
/// is reproducible from the seed below instead of from 256 literals nobody can
/// check. **It must not change.** Correctness does not depend on it — a manifest
/// records the ids and lengths a payload was actually cut into, so any table reads
/// back — but a different table cuts different boundaries, and every chunk written
/// before the change would then dedup against nothing.
const GEAR: [u64; 256] = {
    let mut table = [0u64; 256];
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut i = 0;
    while i < 256 {
        seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = seed;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        table[i] = z ^ (z >> 31);
        i += 1;
    }
    table
};

/// Split `bytes` into content-defined chunks, in order and covering it exactly.
///
/// Every chunk is `MIN_CHUNK..=MAX_CHUNK` bytes except the last, which is whatever
/// remains. An empty input yields no chunks.
pub(crate) fn split(bytes: &[u8]) -> Vec<&[u8]> {
    let mut chunks = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let at = cut_point(rest);
        let (chunk, tail) = rest.split_at(at);
        chunks.push(chunk);
        rest = tail;
    }
    chunks
}

/// Where the next chunk of `bytes` ends: the first hash-chosen boundary at or past
/// [`MIN_CHUNK`], else [`MAX_CHUNK`], else the whole of a shorter remainder.
fn cut_point(bytes: &[u8]) -> usize {
    let limit = bytes.len().min(MAX_CHUNK);
    if limit <= MIN_CHUNK {
        return limit;
    }
    // Hash from the start but test only past MIN_CHUNK. The hash is rolled over
    // the skipped region too: it costs a shift and an add per byte, and stopping
    // to seek would save nothing a branch-free pass does not already give.
    let mut hash = 0u64;
    for (i, &byte) in bytes[..limit].iter().enumerate() {
        hash = (hash << 1).wrapping_add(GEAR[byte as usize]);
        if i + 1 >= MIN_CHUNK && hash & MASK == 0 {
            return i + 1;
        }
    }
    limit
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random bytes — content with enough entropy for the
    /// gear hash to find boundaries in, without depending on an RNG crate.
    fn content(len: usize, seed: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut z = seed | 1;
        while out.len() < len {
            z = z
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            out.extend_from_slice(&z.to_le_bytes());
        }
        out.truncate(len);
        out
    }

    #[test]
    fn chunks_cover_the_input_in_order() {
        let bytes = content(4 * 1024 * 1024, 7);
        let chunks = split(&bytes);
        assert_eq!(chunks.concat(), bytes, "the split must be lossless");
        assert!(chunks.len() > 1, "4 MiB must cut into more than one chunk");
    }

    #[test]
    fn every_chunk_but_the_last_respects_the_bounds() {
        let bytes = content(4 * 1024 * 1024, 11);
        let chunks = split(&bytes);
        for chunk in &chunks[..chunks.len() - 1] {
            assert!(
                (MIN_CHUNK..=MAX_CHUNK).contains(&chunk.len()),
                "chunk of {} bytes is outside {MIN_CHUNK}..={MAX_CHUNK}",
                chunk.len()
            );
        }
        assert!(chunks.last().unwrap().len() <= MAX_CHUNK);
    }

    #[test]
    fn a_payload_under_the_minimum_is_one_chunk() {
        assert!(split(&[]).is_empty());
        let small = content(MIN_CHUNK - 1, 3);
        assert_eq!(split(&small).len(), 1);
    }

    #[test]
    fn incompressible_sameness_still_terminates_at_the_maximum() {
        // A constant byte string never varies the hash, so no boundary is ever
        // chosen and only MAX_CHUNK ends a chunk.
        let flat = vec![0xABu8; 1024 * 1024];
        let chunks = split(&flat);
        assert_eq!(chunks.concat(), flat);
        for chunk in &chunks[..chunks.len() - 1] {
            assert_eq!(chunk.len(), MAX_CHUNK);
        }
    }

    #[test]
    fn an_insertion_disturbs_only_the_chunks_around_it() {
        // The property the whole module exists for: splice a kilobyte into the
        // middle and the chunks after the splice must be byte-identical, not
        // shifted. Fixed-offset chunking would rewrite every one of them.
        let before = content(2 * 1024 * 1024, 23);
        let mut after = before.clone();
        after.splice(1_000_000..1_000_000, content(1024, 99));

        let old: Vec<&[u8]> = split(&before);
        let new: Vec<&[u8]> = split(&after);
        // The chunks before the splice are identical and in place; the chunks after
        // it are identical but shifted along by however many the splice added. What
        // must be small is what neither end accounts for.
        let head = zip_common(old.iter(), new.iter());
        let tail = zip_common(old.iter().rev(), new.iter().rev());
        assert!(head > 0 && tail > 0, "the splice is in the middle");
        let disturbed = old.len() - head - tail;
        assert!(
            disturbed <= 2,
            "an insertion should disturb a chunk or two, not {disturbed} of {}",
            old.len()
        );
    }

    /// How many leading elements the two iterators agree on.
    fn zip_common<'a>(
        a: impl Iterator<Item = &'a &'a [u8]>,
        b: impl Iterator<Item = &'a &'a [u8]>,
    ) -> usize {
        a.zip(b).take_while(|(a, b)| a == b).count()
    }

    #[test]
    fn an_appended_tail_leaves_the_prefix_alone() {
        // The agent-transcript shape: state grows at the end between snapshots.
        let before = content(1024 * 1024, 41);
        let mut after = before.clone();
        after.extend_from_slice(&content(200 * 1024, 42));

        let old = split(&before);
        let new = split(&after);
        let shared = old
            .iter()
            .zip(new.iter())
            .take_while(|(a, b)| a == b)
            .count();
        assert!(
            shared + 1 >= old.len(),
            "an append must leave all but the final chunk untouched: {shared} of {}",
            old.len()
        );
    }
}
