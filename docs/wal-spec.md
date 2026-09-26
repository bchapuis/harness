# Write-Ahead Log Primitive: Specification

**Status:** Draft v1
**Scope:** A generic, framed, checksummed, append-only write-ahead log on the local filesystem, and the atomic single-file replacement that goes with it. This document owns the *durable-file contract*: how a record is framed, what recovery returns, and what survives a crash. A caller owns what an I/O failure *means*.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

`wal` is the crate and namespace name. Sections of this document are cited as plain **§N**; its invariants are numbered **L1, L2, …** (L for the log).

> **Design stance.** Every file-backed durable store needs the same machinery: frame, checksum, and fsync a record; recover the valid prefix and discard a torn tail; rewrite the file atomically to compact. The write path and the recovery path are one contract read from two ends: if they disagree on what a valid record is, a node silently drops a record it acknowledged or replays one it never finished writing, while each path looks correct alone. So the machinery lives in one place, both paths built from the *same* framing (§2) and enforcing the *same* bound (§4). The crate decides only the bytes-and-durability contract, not what records mean or what a failure to persist them implies (§6).

---

## 1. Scope and layering

The crate sits **below** its callers and depends on none of them. It provides:

- **`Wal<T>`**: an append-only log of `postcard`-encoded `T` records (§2, §3).
- **Sidecars** (`Sidecar`, `atomic_replace`, `checksum`, `sync_dir`): for durable state that is a single small file rather than a log (§5).

It is **not** a database, an index, or a replication layer. It does no caching, holds no copy of the records after `open` returns them, and requires of `T` only `serde::Serialize + serde::de::DeserializeOwned` (a sidecar's body, §5 rule 4, is the crate's own shape, not a `T`). Ordering, indexing, snapshotting, replication, and compaction *policy* belong above the crate; it only frames, persists, and recovers the bytes.

The crate is **synchronous and single-handle**: one `Wal<T>` owns one open append handle to one file. Two `Wal<T>` instances on one path are outside the contract; a caller serializes access through its own single-writer discipline.

---

## 2. Framing

1. Each record MUST be framed as exactly three fields, contiguous, with no padding or alignment:

   ```
   [u32 length, little-endian] [payload] [u64 checksum, little-endian]
   ```

   where `payload` is the `postcard` encoding of the record and `length` is the payload's byte length.

2. The checksum MUST be **XXH3-64** over the payload bytes only. It detects torn and partial writes, not tampering (§8). It MUST NOT vary across platforms, crate versions, or process runs, so a file written by one build recovers identically under another; this rules out `std::hash`.

3. The log file is a **header** (§2.1) followed by the concatenation of zero or more such frames in append order. There is no trailer and no global checksum: past the header, the file's meaning is the maximal sequence of valid frames (§3.1).

### 2.1 The file header

1. Every log file MUST open with:

   ```
   [magic 8] [u16 frame version] [u16 record schema]
   ```

   all little-endian. The magic is `89 57 41 4C 0D 0A 1A 0A` (`\x89WAL\r\n\x1a\n`), the PNG convention: a high-bit byte, the name, then a CRLF/EOF trap that catches a text-mode transfer.

2. `open` MUST admit the whole header (magic, then frame version, then record schema) **before scanning a single frame** ([compatibility](compatibility-spec.md) **V2**). A file this build cannot read is never partially interpreted.

3. The header's **length and layout belong to the frame version**. Version 1 is the twelve bytes above; a later version may define a different header, since a version-1 reader refuses a version-2 file outright. Only the magic and the `u16` after it are fixed across versions, since a reader consults them *before* it knows the layout; nothing else in the crate may assume twelve bytes. The current width is exported as `HEADER_LEN`, so a caller that edits a log's bytes (a crash-point test, a corruption fixture) finds the first frame by the same definition the writer and the scan use.

4. **Frame version** versions §2's framing; version 1 is the layout above. The header carries no checksum of its own: every header byte is validated against a known value instead.

5. **Record schema** belongs to the *caller*: `open` takes the caller's `compat::Format`, stamps its version into a new file, admits the field on an existing one, and never interprets it. `postcard` is positional, so `T` cannot gain or reorder a field within a version, and its version can live only outside the payload. An enum may grow variants at its end without a bump, since no existing record carries them.

6. The check is part of `open`, not an optional accessor, because an optional check is one a caller forgets. A refused file surfaces as `std::io::ErrorKind::InvalidData`, its message naming the format and either the version found or that the file is not stamped. `rewrite` stamps the header afresh.

7. A file whose bytes are a **prefix** of the header `open` would write MUST be stamped in place rather than refused: the header is fsynced before any frame can follow it, so such a file holds no records. This covers a missing file, an empty one, and a header torn by a crash before its sync, which would otherwise leave a log that can never be opened. Any other file too short to hold a header MUST be refused, so the rule cannot overwrite a foreign file.

8. `max_record` MUST stay below `0x4C41_5789`, the magic's first four bytes read as a little-endian `u32` (`open` asserts it). A headerless file begins with a frame length at most `max_record`, so it can never begin with the magic: rule 2's refusal is sound, not merely probable.

---

## 3. The log

### 3.1 Recovery (`open`)

1. `open(path, max_record, records)` MUST read the file (treating a missing file as empty), admit its header (§2.1), **scan its maximal valid prefix** into records, and return them alongside the live `Wal<T>`. The scan MUST stop at the first frame that is any of: **incomplete** (fewer bytes remain than the frame claims), **oversized** (`length > max_record`, §4), **checksum-failing**, or **unparsable** as `T`. Everything from that frame onward is the torn tail.

2. The torn tail was never acknowledged (§3.2), so dropping it is correct. When it is non-empty, `open` MUST **truncate the file to the valid length and fsync it** before returning, so no later append can land after unacknowledged bytes.

3. When `open` *creates* the file, it MUST fsync the parent directory so the new entry survives a crash. A caller remains responsible for fsyncing any directory *it* created to hold the log (`sync_dir`, §5).

4. `open` MUST NOT allocate based on an untrusted `length`: a `length` exceeding `max_record` ends the scan as corruption rather than driving an allocation.

### 3.2 Append

5. `append(record)` and `append_batch(records)` MUST frame the record(s) into a single buffer, write it at the current end (the file is opened `O_APPEND`, so writes land at the end even right after a truncating `set_len`), and **fsync before returning**. A returned append is durable and acknowledged. `append_batch` MUST perform **one write and one fsync** for the whole batch; `append` is its single-record case. An empty batch MUST be a no-op.

### 3.3 Truncate

6. `truncate(keep)` MUST drop every record past the first `keep`, by `set_len` to record `keep`'s recorded frame offset, fsynced before returning. It is the conflict-resolution primitive: discard a diverging suffix before appending its replacement. `truncate(keep)` with `keep >= len()` MUST be a no-op.

### 3.4 Rewrite (compaction)

7. `rewrite(records)` MUST atomically replace the entire file with exactly `records` (framed per §2) and reopen the append handle. It MUST use the §5 atomic-replace discipline, so a crash during `rewrite` leaves either the whole old file or the whole new file. It is the compaction primitive: replace a log with a shorter equivalent, such as a retained suffix or a single record that subsumes the prior history.

### 3.5 Accessors

8. `len()` and `is_empty()` MUST reflect the current in-memory record count. The crate keeps the per-record frame offsets that make `truncate` a single `set_len`, but not the payloads (§1).

---

## 4. The record bound

1. `max_record` is supplied at `open` and bounds **one frame's payload at both ends**:
   - On **recovery** (§3.1 req 1), a scanned `length > max_record` is treated as corruption and ends the valid prefix.
   - On **write**, framing a payload larger than `max_record` MUST **panic** rather than write it.

2. A record that recovery would reject for size MUST NOT be writable (**L4**): otherwise `append` would acknowledge it and the next `open` would silently discard it. The panic is correct because an over-bound record is a caller bug (a mis-sized `max_record` or an unexpectedly large `T`), not a runtime condition to recover from.

---

## 5. Sidecars

Some durable state is a single small file rewritten whole: a generation counter, a pointer to the latest checkpoint. For it the crate exposes:

1. `atomic_replace(dir, name, bytes)` MUST write a scratch file in `dir`, fsync it, rename it onto `dir/<name>`, then fsync `dir`, so a reader MUST see either the old file or the whole new one. The caller supplies serialized bytes and so chooses the encoding. `rewrite` (§3.4) builds on the same discipline.

   The scratch name MUST be unique per call and MUST NOT be a name a caller could read back as content; `<name>.<pid>.<counter>.tmp` satisfies both. Uniqueness is a correctness requirement: callers replace one name concurrently (the normal case in a content-addressed store), and two calls sharing a scratch path corrupt each other (the second create truncates the first's bytes; the second rename finds the path gone and fails), which a caller treating I/O errors as fatal turns into a node that stops accepting writes.

   Concurrent calls on one `name` MUST therefore all succeed, and the file that lands MUST be exactly one caller's whole bytes; which one is unspecified (this replaces atomically, it does not serialize). A crash between create and rename MAY leave a scratch file behind, so a caller that enumerates such a directory MUST skip names it cannot decode.

2. `checksum(bytes)` is the frame digest of §2 rule 2, exposed so a caller can frame its own sidecar bytes.

3. `sync_dir(dir)` MUST make a directory entry durable: on unix, fsync the directory; elsewhere (where directories cannot be opened for sync) the rename is itself the durability point and the call is a no-op. It serves a caller that creates its own subdirectories to hold logs.

4. `Sidecar` is the stamped `u64` sidecar: one value written whole to a file of its own under a caller-owned `compat::Stamp` (a fence term, a seal bound, a deletion stamp). `write` MUST persist it through `atomic_replace`. The body behind the stamp is positional `postcard`, `{ checksum, value }`, the checksum being rule 2's `checksum` over the value's little-endian bytes. `read` MUST admit the stamp's version before reading any field (**V2**), and MUST answer `None` for an absent file only: a short file, a long file, a foreign or missing magic, or a checksum mismatch is an `InvalidData` refusal naming the format and the path, never an absence, because a durable fence read back as *no fence* loses a safety property with no diagnostic anywhere.

---

## 6. Failure policy

1. Every fallible method MUST return `std::io::Result`; `open`'s policy refusal rides it as `InvalidData` (§2.1 rule 6). The crate **MUST NOT decide what an I/O failure means**: it neither retries, nor masks, nor panics on an I/O error (the §4 over-bound panic is a caller bug, not an I/O failure). The caller decides: one that cannot persist a record it must not lose may panic with a domain-specific message; another might degrade or report.

2. The crate masks no corruption: a checksum failure, a torn tail, and a parse failure all surface as a shortened valid prefix (§3.1), never as an error and never as a silently patched record.

---

## 7. Invariants

Invariants are verified by the crate's own unit tests (`crates/wal/src/lib.rs`) and by the exhaustive crash-point suite (`crates/wal/tests/crash_points.rs`), which decides the recovery rule at *every* byte offset and every single-byte corruption rather than sampling it.

The catalogue is machine-readable beside those suites, guarded by the drift test in `conformance_catalogue.rs`, which differs from its siblings in two ways. It defines its own catalogue types, because sharing `actor-simulation`'s would make `wal` depend on a consumer of `wal` (§1). And it checks test **names** rather than test *files*, because nearly all of this crate's verification is `#[test]` functions inside `lib.rs`: every name below is scanned for in the crate's sources, and every crash-point test must be claimed by some invariant.

| # | Invariant | Defined in | Verified by |
|---|---|---|---|
| L1 | **Prefix recovery.** `open` returns exactly the maximal prefix of valid frames; the first incomplete, oversized, checksum-failing, or unparsable frame and everything after it is discarded, and the file is truncated to the valid length before any append. | §3.1 | `a_torn_tail_is_discarded_and_appends_continue`, `a_record_cut_mid_payload_is_discarded`, `a_corrupted_checksum_ends_the_valid_prefix`; exhaustively, `every_truncation_recovers_a_prefix_and_stays_recovered`, `every_single_byte_corruption_drops_that_record_and_the_rest`, `a_length_header_claiming_more_than_the_file_holds_is_refused` |
| L2 | **Acknowledged-record durability.** A record acknowledged by a returned `append`/`append_batch`/`rewrite` is recovered byte-identically across a reopen, unless a later `truncate` or `rewrite` removes it; each such call fsyncs its effect (and the directory entry on file creation) before returning. | §3.2–§3.4 | `records_round_trip_across_a_reopen`, `truncate_drops_a_conflicting_suffix`, `rewrite_replaces_the_whole_file_and_reopens`; across crash points, `every_truncation_recovers_a_prefix_and_stays_recovered` (the recovered prefix is rewritten to disk and still appends) |
| L3 | **Atomic whole-file replacement.** `rewrite` and `atomic_replace` leave, after any crash, either the whole prior file or the whole new file, never a torn intermediate. Concurrent `atomic_replace` calls on one name all succeed, and the file that lands is one caller's whole bytes. | §3.4, §5 | `rewrite_replaces_the_whole_file_and_reopens`, `atomic_replace_round_trips_a_sidecar`, `concurrent_replaces_of_one_name_all_succeed_and_none_tears`, `rewrite_restamps_the_replacement`, `a_scratch_file_left_by_a_crash_mid_rewrite_leaves_the_prior_log_whole` (the crash between create and rename, the one crash point the discipline leaves visible) |
| L4 | **Write/recovery bound agreement.** `max_record` bounds a frame's payload identically at both ends: a payload recovery would reject for size cannot be written: the write panics instead of acknowledging a record the next `open` would silently drop. | §4 | `appending_a_record_past_the_limit_panics_instead_of_losing_it` |

---

## 8. Non-goals and future work

Non-goals:

- **No concurrency.** One handle, one writer (§1); the crate adds no locking.
- **No tamper resistance.** The digest catches accidental corruption, not a malicious edit.
- **No segmentation or rotation.** A `Wal<T>` is one file. A caller that wants bounded files segments above the crate and compacts with `rewrite`.

Future work, only if a caller needs it:

- **Group commit across handles**: batching fsyncs for many small logs sharing a disk, trading a bounded latency for throughput.
