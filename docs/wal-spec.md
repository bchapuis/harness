# Write-Ahead Log Primitive: Specification

**Status:** Draft v1
**Scope:** A generic, framed, checksummed, append-only write-ahead log on the local filesystem, and the atomic single-file replacement that goes with it. This document owns the *durable-file contract*: how a record is framed, what recovery returns, and what survives a crash. A caller owns what an I/O failure *means*.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Throughout, `wal` is the crate and namespace name. Sections of this document are cited as plain **§N**. Invariants defined here are numbered **W1, W2, …**.

> **Design stance.** A file-backed durable store needs the same small machinery every time: frame a record, checksum it, fsync it, recover by scanning the valid prefix and discarding a torn tail, and rewrite the file atomically to compact. That logic is small but safety-critical, because the write path and the recovery path are one contract read from two ends: if they disagree about what a valid record is, a node mis-recovers — silently dropping a record it acknowledged, or replaying one it never finished writing. The danger is precisely that the two paths drift apart while each looks correct alone. So the machinery lives in one place, with the write path and the scan path enforcing the *same* bound (§4) and built from the *same* framing (§2). This crate decides only the bytes-and-durability contract; it takes no position on what the records mean or what a failure to persist them implies (§6).

---

## 1. Scope and layering

The crate sits **below** its callers and depends on none of them. It provides:

- **`Wal<T>`** — an append-only log of `postcard`-encoded `T` records (§2, §3).
- **Sidecar helpers** — `atomic_replace`, `checksum`, `sync_dir` — for durable state that is a single small file rather than a log (§5).

It is **not** a database, an index, or a replication layer. It does no caching, holds no in-memory copy of the records after `open` returns them, and imposes no schema on `T` beyond `serde::Serialize + serde::de::DeserializeOwned`. Ordering, indexing, snapshotting, replication, and compaction *policy* all belong above the crate: a caller assigns whatever meaning its records carry and decides when to compact; the crate only frames, persists, and recovers the bytes.

The crate is **synchronous and single-handle**: one `Wal<T>` owns one open append handle to one file. Concurrent access to the same path from two `Wal<T>` instances is outside the contract; a caller serializes access through its own single-writer discipline.

---

## 2. Framing

1. Each record MUST be framed as exactly three fields, contiguous, with no padding or alignment:

   ```
   [u32 length, little-endian] [payload] [u64 checksum, little-endian]
   ```

   where `payload` is the `postcard` encoding of the record and `length` is the payload's byte length.

2. The checksum MUST be **FNV-1a, 64-bit**, over the payload bytes only: offset basis `0xcbf29ce484222325`, prime `0x100000001b3`. It detects torn and partial writes, **not** adversarial tampering — all a local log needs. The function MUST be stable: it MUST NOT vary across platforms, crate versions, or process runs, so a file written by one build recovers identically under another. (`std::hash` and other unstable hashers are therefore ruled out.)

3. The log file is the concatenation of zero or more such frames in append order. There is no file header, no trailer, and no global checksum: the file's meaning is the maximal sequence of valid frames from its start (§3.1).

---

## 3. The log

### 3.1 Recovery (`open`)

1. `open(path, max_record)` MUST read the file (treating a missing file as empty), **scan its maximal valid prefix** into records, and return them alongside the live `Wal<T>`. The scan MUST stop at the first frame that is any of: **incomplete** (fewer bytes remain than the frame claims), **oversized** (`length > max_record`, §4), **checksum-failing**, or **unparsable** as `T`. Everything from that frame onward is the torn tail.

2. The valid prefix is the log; the torn tail was never acknowledged (§3.2), so dropping it is correct. When the torn tail is non-empty, `open` MUST **truncate the file to the valid length and fsync it** before returning — the recovery decision is made durable immediately, so no later append can land after un-acknowledged bytes.

3. When `open` *creates* the file, it MUST fsync the parent directory so the new file's directory entry survives a crash; a caller need not repeat this for the log file itself. A caller remains responsible for fsyncing any directory *it* created to hold the log (via `sync_dir`, §5).

4. `open` MUST NOT allocate based on an untrusted `length`: a `length` exceeding `max_record` ends the scan (it is treated as corruption) rather than driving an allocation.

### 3.2 Append

5. `append(record)` and `append_batch(records)` MUST frame the record(s) into a single buffer, write it at the current end (the file is opened `O_APPEND`, so writes land at the end even right after a truncating `set_len`), and **fsync before returning**. A returned append is durable and acknowledged. `append_batch` MUST perform **one write and one fsync** for the whole batch — its latency is one fsync, not one per record. `append` is the single-record case of `append_batch`. An empty batch MUST be a no-op.

### 3.3 Truncate

6. `truncate(keep)` MUST drop every record past the first `keep`, by `set_len` to record `keep`'s recorded frame offset, fsynced before returning. It is the conflict-resolution primitive: discard a diverging suffix before appending a replacement in its place. `truncate(keep)` with `keep >= len()` MUST be a no-op.

### 3.4 Rewrite (compaction)

7. `rewrite(records)` MUST atomically replace the entire file with exactly `records` (framed per §2) and reopen the append handle. It MUST use the §5 atomic-replace discipline, so a crash during `rewrite` leaves either the whole old file or the whole new file, never a mix. It is the compaction primitive: replace a log with a shorter equivalent — a retained suffix, or a single record that subsumes the prior history.

### 3.5 Accessors

8. `len()`, `is_empty()`, and `path()` MUST reflect the current in-memory record count and the file path. The crate keeps the parallel array of per-record frame offsets that makes `truncate` a single `set_len`; it does **not** retain the record payloads after `open` (the caller holds those).

---

## 4. The record bound

1. `max_record` is supplied at `open` and bounds **one frame's payload at both ends**:
   - On **recovery** (§3.1 req 1), a scanned `length > max_record` is treated as corruption and ends the valid prefix.
   - On **write**, framing a payload larger than `max_record` MUST **panic** rather than write it.

2. This two-ended enforcement is the crate's central safety property, stated as an invariant (W4). A record that recovery would reject for size MUST NOT be writable: otherwise `append` would acknowledge it and the next `open` would silently discard it — exactly the write-path/recovery-path divergence the crate exists to prevent. Failing loudly at the write keeps the asymmetry from becoming silent data loss. The panic is correct because an over-bound record is a caller bug (a mis-sized `max_record` or an unexpectedly large `T`), not a runtime condition to recover from.

---

## 5. Sidecars

Some durable state is not a log but a single small file rewritten in place: a generation counter, a small piece of metadata, a pointer to the latest checkpoint. For these the crate exposes:

1. `atomic_replace(dir, name, bytes)` MUST write `dir/<name>.tmp`, fsync it, rename it onto `dir/<name>`, then fsync `dir`. A reader MUST therefore see either the old file or the whole new one, never a torn mix. The caller supplies already-serialized bytes, so the encoding (JSON, postcard, fixed-width) stays the caller's choice. This is the same discipline `rewrite` (§3.4) builds on.

2. `checksum(bytes)` MUST be the §2 FNV-1a function, exposed so a caller that frames its own sidecar bytes (e.g. a fixed-width `[u64 value][u64 checksum]` record) checksums them the same way the log does.

3. `sync_dir(dir)` MUST make a directory entry durable: on unix, fsync the directory; elsewhere (where directories cannot be opened for sync) the rename is itself the durability point and the call is a no-op. Exposed for a caller that creates its own subdirectories to hold logs.

---

## 6. Failure policy

1. Every fallible method MUST return `std::io::Result`. The crate **MUST NOT decide what an I/O failure means** — it neither retries, nor masks, nor panics on an I/O error (the §4 over-bound panic is a caller bug, not an I/O failure). The decision belongs to the caller: one that cannot persist a record it must not lose may have no safe way to continue and so panics with a domain-specific message; another might degrade or report. Keeping the policy in the caller is deliberate, and is why `path()` is exposed (§3.5) — for the caller's own failure message.

2. The crate masks no corruption: a checksum failure, a torn tail, and a parse failure all surface as a shortened valid prefix (§3.1), never as an error and never as a silently patched record.

---

## 7. Invariants

Invariants are verified by the crate's own unit tests (`crates/wal/src/lib.rs`).

| # | Invariant | Defined in | Verified by |
|---|---|---|---|
| W1 | **Prefix recovery.** `open` returns exactly the maximal prefix of valid frames; the first incomplete, oversized, checksum-failing, or unparsable frame and everything after it is discarded, and the file is truncated to the valid length before any append. | §3.1 | `a_torn_tail_is_discarded_and_appends_continue`, `a_record_cut_mid_payload_is_discarded`, `a_corrupted_checksum_ends_the_valid_prefix` |
| W2 | **Acknowledged-record durability.** A record acknowledged by a returned `append`/`append_batch`/`rewrite` is recovered byte-identically across a reopen, unless a later `truncate` or `rewrite` removes it; each such call fsyncs its effect (and the directory entry on file creation) before returning. | §3.2–§3.4 | `records_round_trip_across_a_reopen`, `truncate_drops_a_conflicting_suffix`, `rewrite_replaces_the_whole_file_and_reopens` |
| W3 | **Atomic whole-file replacement.** `rewrite` and `atomic_replace` leave, after any crash, either the whole prior file or the whole new file — never a torn intermediate. | §3.4, §5 | `rewrite_replaces_the_whole_file_and_reopens`, `atomic_replace_round_trips_a_sidecar` |
| W4 | **Write/recovery bound agreement.** `max_record` bounds a frame's payload identically at both ends: a payload recovery would reject for size cannot be written — the write panics instead of acknowledging a record the next `open` would silently drop. | §4 | `appending_a_record_past_the_limit_panics_instead_of_losing_it` |

W4 is the crate's reason to exist: it is the one place the write path and the recovery path are forced to agree, and the property every consumer would otherwise have to re-establish by hand-rolling the machinery.

---

## 8. Non-goals and future work

Non-goals (today and by design):

- **No concurrency.** One handle, one writer; the caller provides single-writer discipline. The crate adds no locking.
- **No tamper resistance.** FNV-1a catches accidental corruption, not a malicious edit. A log on a trusted local disk needs no more.
- **No segmentation or rotation.** A `Wal<T>` is one file. A caller that wants bounded files segments above the crate (§1) and compacts with `rewrite`.

Possible future work, only if a caller needs it:

- **Group commit across handles** — batching fsyncs for many small logs sharing a disk, trading a bounded latency for throughput. Today each `append_batch` already amortizes within one log.
- **A pluggable checksum** — a stronger digest behind the same framing, were a caller ever to store a WAL on untrusted media. The framing reserves a fixed 8-byte trailer, so this is a format-version change, not a layout change.
