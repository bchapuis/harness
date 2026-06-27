//! A generic, framed, checksummed write-ahead log on the local filesystem.
//!
//! A file-backed durable store needs the same small, safety-critical machinery every
//! time: frame records the same way, checksum them the same way, recover the same way
//! (scan the valid prefix, discard a torn tail), and rewrite atomically the same way.
//! A divergence between the write path and the recovery path mis-recovers a node, so
//! that logic lives here, once.
//!
//! # The log
//!
//! [`Wal<T>`] is an append-only log of postcard-encoded `T` records, each framed
//! `[u32 little-endian length][postcard payload][u64 little-endian FNV-1a checksum]`
//! and fsynced before the call that wrote it returns. It exposes four operations:
//!
//! - [`append`](Wal::append) / [`append_batch`](Wal::append_batch) — frame and fsync
//!   one record, or many in a single write and a single fsync.
//! - [`truncate`](Wal::truncate) — drop a conflicting suffix back to the first `keep`
//!   records, a `set_len` at the record's recorded offset.
//! - [`rewrite`](Wal::rewrite) — atomically replace the whole file with exactly the
//!   given records (compaction to a retained suffix, or to a single record that
//!   subsumes the prior history).
//!
//! [`open`](Wal::open) scans the file into its records, **truncating any torn tail to
//! disk** before returning — a record whose write never completed was never
//! acknowledged, so dropping it is correct. Whatever higher-level recovery runs on top
//! operates on the returned records.
//!
//! # Sidecars
//!
//! Some durable state is not a log but a single small file rewritten in place (a
//! generation counter, a small piece of metadata, a checkpoint pointer). For those,
//! [`atomic_replace`] writes `tmp → fsync → rename → fsync dir` so a reader sees
//! either the old file or the whole new one, never a torn mix. [`checksum`] and
//! [`sync_dir`] are exposed for framing one's own sidecar bytes.
//!
//! # Failure policy
//!
//! Every method returns [`io::Result`]: this crate does not decide what an I/O
//! failure *means*. The caller does — code that cannot persist a record it must not
//! lose may have no safe way to continue, so it panics with a domain-specific message.
//! Keeping that policy out of this crate is deliberate.
//!
//! The checksum is FNV-1a: it catches torn and partial writes, not adversarial
//! tampering, which is all a local log needs.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;
use std::path::PathBuf;

use serde::Serialize;
use serde::de::DeserializeOwned;

/// FNV-1a 64. Detects torn and partial writes (not adversarial tampering), all a
/// local log needs. Exposed so a caller framing its own sidecar bytes (e.g. a
/// fixed-width value file) checksums them the same way the log does.
pub fn checksum(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

/// Make a directory entry durable (a freshly created or renamed file). File *data* is
/// covered by the file's own `sync_all`; the directory entry needs its own fsync on
/// unix for the creation/rename to survive a crash. Elsewhere (Windows) directories
/// cannot be opened for sync and the rename itself is the durability point.
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Atomically replace `dir/<name>` with `bytes`: write `<name>.tmp` → fsync → rename
/// → fsync dir, so a reader sees either the old file or the whole new one. The caller
/// supplies already-serialized bytes, so the encoding (JSON, postcard, fixed-width)
/// stays its choice.
pub fn atomic_replace(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let tmp_path = dir.join(format!("{name}.tmp"));
    let final_path = dir.join(name);
    let mut tmp = File::create(&tmp_path)?;
    tmp.write_all(bytes)?;
    tmp.sync_all()?;
    fs::rename(&tmp_path, &final_path)?;
    sync_dir(dir)
}

/// The directory holding `path`, for fsyncing the entry that names a file in it. A path
/// with no parent or an empty parent (a bare filename) resolves to the current directory.
fn parent_dir(path: &Path) -> &Path {
    match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    }
}

/// Width of the little-endian length prefix that opens every frame. Tied to the
/// prefix's own type so the write path ([`encode`]) and the recovery path ([`scan`])
/// frame from one definition and cannot drift apart on the layout — the divergence this
/// crate exists to prevent.
const LEN_BYTES: usize = size_of::<u32>();
/// Width of the little-endian FNV-1a checksum that closes every frame.
const CHECKSUM_BYTES: usize = size_of::<u64>();

/// Frame one record: `[u32 len][postcard payload][u64 checksum]`, all little-endian.
///
/// Panics if the payload exceeds `max_record`. The scan that recovers the log treats a
/// length above `max_record` as corruption and drops it (and everything after it), so a
/// record that scan would reject must never be written: it would be acknowledged here
/// and silently lost on the next open. Failing loudly at the write keeps that asymmetry
/// from becoming silent data loss.
fn encode<T: Serialize>(value: &T, max_record: u32) -> Vec<u8> {
    let payload = postcard::to_allocvec(value).expect("a WAL record always serializes");
    assert!(
        payload.len() as u64 <= u64::from(max_record),
        "WAL record of {} bytes exceeds the {max_record}-byte limit; recovery would \
         discard it, so it must not be written",
        payload.len(),
    );
    let mut record = Vec::with_capacity(LEN_BYTES + payload.len() + CHECKSUM_BYTES);
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.extend_from_slice(&payload);
    record.extend_from_slice(&checksum(&payload).to_le_bytes());
    record
}

/// Scan a framed log's bytes into `(records, per-record start offsets, valid length)`.
/// The scan stops at the first incomplete, oversized, checksum-failing, or unparsable
/// record — the recovery rule: the valid prefix is the log; the tail was never
/// acknowledged.
fn scan<T: DeserializeOwned>(bytes: &[u8], max_record: u32) -> (Vec<T>, Vec<u64>, u64) {
    let mut records = Vec::new();
    let mut offsets = Vec::new();
    let mut pos = 0usize;
    while let Some(header) = bytes.get(pos..pos + LEN_BYTES) {
        let len = u32::from_le_bytes(header.try_into().expect("length-prefix slice"));
        if len > max_record {
            break;
        }
        let len = len as usize;
        let Some(payload) = bytes.get(pos + LEN_BYTES..pos + LEN_BYTES + len) else {
            break;
        };
        let check_start = pos + LEN_BYTES + len;
        let Some(check) = bytes.get(check_start..check_start + CHECKSUM_BYTES) else {
            break;
        };
        if u64::from_le_bytes(check.try_into().expect("checksum slice")) != checksum(payload) {
            break;
        }
        let Ok(record) = postcard::from_bytes::<T>(payload) else {
            break;
        };
        offsets.push(pos as u64);
        records.push(record);
        pos += LEN_BYTES + len + CHECKSUM_BYTES;
    }
    (records, offsets, pos as u64)
}

/// A framed, checksummed, append-only log of postcard-encoded `T` records on the local
/// filesystem. See the module docs for the framing, recovery, and failure policy.
pub struct Wal<T> {
    path: PathBuf,
    /// The open append handle. With `O_APPEND`, writes land at the current end even
    /// right after a truncating `set_len`.
    file: File,
    /// The byte offset where each record's frame starts, parallel to the records the
    /// caller holds — what makes [`truncate`](Wal::truncate) a single `set_len`.
    offsets: Vec<u64>,
    /// The file's current (valid) length — where the next frame lands.
    end: u64,
    /// Upper bound on one frame's payload. Enforced on every write (a larger record is
    /// rejected loudly) and on recovery (a larger length is treated as corruption), so
    /// the write path and the scan path agree on what is a valid record.
    max_record: u32,
    _marker: PhantomData<fn() -> T>,
}

impl<T: Serialize + DeserializeOwned> Wal<T> {
    /// Open (creating if absent) the log at `path`, scan its valid prefix, truncate
    /// any torn tail to disk, and return the recovered records. `max_record` bounds one
    /// frame's payload at both ends: a scanned length above it is treated as corruption
    /// (not an allocation), and an [`append`](Wal::append) of a larger record panics
    /// rather than write something recovery would silently discard.
    ///
    /// When this creates the file, it fsyncs the parent directory so the new log's entry
    /// survives a crash — the caller need not (and should not) repeat it. The caller is
    /// still responsible for any directory *it* created to hold the log.
    ///
    /// # Errors
    ///
    /// Any filesystem error reading, opening, or truncating the file.
    pub fn open(path: impl Into<PathBuf>, max_record: u32) -> io::Result<(Wal<T>, Vec<T>)> {
        let path = path.into();
        let (bytes, existed) = match fs::read(&path) {
            Ok(bytes) => (bytes, true),
            Err(err) if err.kind() == io::ErrorKind::NotFound => (Vec::new(), false),
            Err(err) => return Err(err),
        };
        let (records, offsets, valid_end) = scan::<T>(&bytes, max_record);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        if !existed {
            // The file was just created. Its bytes become durable on the first append's
            // fsync, but the directory entry that names it needs its own fsync, or a
            // crash could lose a file whose appends were already acknowledged. Doing it
            // here makes the new log's entry durable for every caller, including one
            // that creates its logs lazily, so no caller has to remember to.
            sync_dir(parent_dir(&path))?;
        }
        if (valid_end as usize) < bytes.len() {
            // A torn tail: the write never returned, so the record was never
            // acknowledged. Truncate it away before anything is appended.
            file.set_len(valid_end)?;
            file.sync_all()?;
        }
        Ok((
            Wal {
                path,
                file,
                offsets,
                end: valid_end,
                max_record,
                _marker: PhantomData,
            },
            records,
        ))
    }

    /// The log's path (handy for a caller's domain-specific failure message).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The number of records in the log.
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Whether the log has no records.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Frame `records` into one contiguous buffer, returning it alongside each record's
    /// start offset measured from `base` — the shared body of the append and rewrite
    /// write paths, which differ only in `base` (the current end vs. zero) and in how
    /// they make the buffer durable.
    fn frame_all(&self, records: &[T], base: u64) -> (Vec<u8>, Vec<u64>) {
        let mut buf = Vec::new();
        let mut offsets = Vec::with_capacity(records.len());
        for record in records {
            offsets.push(base + buf.len() as u64);
            buf.extend_from_slice(&encode(record, self.max_record));
        }
        (buf, offsets)
    }

    /// Frame `record`, append it, and fsync — durable before the call returns. The
    /// single-record case of [`append_batch`](Wal::append_batch); the fsync dominates,
    /// so the one-element slice costs nothing measurable.
    ///
    /// # Errors
    ///
    /// Any filesystem error writing or syncing.
    pub fn append(&mut self, record: &T) -> io::Result<()> {
        self.append_batch(std::slice::from_ref(record))
    }

    /// Frame `records` into one buffer, append them, and fsync once — the batch append
    /// (its latency is one fsync, not one per record). A no-op for an empty slice.
    ///
    /// # Errors
    ///
    /// Any filesystem error writing or syncing.
    pub fn append_batch(&mut self, records: &[T]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let (buf, offsets) = self.frame_all(records, self.end);
        self.file.write_all(&buf)?;
        self.file.sync_all()?;
        self.offsets.extend(offsets);
        self.end += buf.len() as u64;
        Ok(())
    }

    /// Drop everything past the first `keep` records — a `set_len` at record `keep`'s
    /// recorded offset, made durable before any conflicting record can be appended
    /// after it. A no-op when `keep` is already the length.
    ///
    /// # Errors
    ///
    /// Any filesystem error truncating or syncing.
    pub fn truncate(&mut self, keep: usize) -> io::Result<()> {
        if keep >= self.offsets.len() {
            return Ok(());
        }
        let cut = self.offsets[keep];
        self.file.set_len(cut)?;
        self.file.sync_all()?;
        self.offsets.truncate(keep);
        self.end = cut;
        Ok(())
    }

    /// Atomically replace the whole file with exactly `records` (via `tmp` → fsync →
    /// rename → fsync dir) and reopen the append handle. Used to compact: replace the
    /// log with a retained suffix, or with a single record that subsumes the prior
    /// history. A crash leaves either the old file or the whole new one.
    ///
    /// # Errors
    ///
    /// Any filesystem error writing, renaming, or reopening.
    pub fn rewrite(&mut self, records: &[T]) -> io::Result<()> {
        let (buf, offsets) = self.frame_all(records, 0);
        let dir = parent_dir(&self.path);
        let name = self
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .expect("a WAL path always has a file name");
        atomic_replace(dir, name, &buf)?;
        self.file = OpenOptions::new().append(true).open(&self.path)?;
        self.offsets = offsets;
        self.end = buf.len() as u64;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    const MAX: u32 = 1 << 20;

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
    struct Rec {
        index: u64,
        data: Vec<u8>,
    }

    fn rec(index: u64, data: &[u8]) -> Rec {
        Rec {
            index,
            data: data.to_vec(),
        }
    }

    fn open(path: &Path) -> (Wal<Rec>, Vec<Rec>) {
        Wal::<Rec>::open(path, MAX).unwrap()
    }

    #[test]
    fn records_round_trip_across_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        {
            let (mut wal, recovered) = open(&path);
            assert!(recovered.is_empty());
            wal.append(&rec(1, b"a")).unwrap();
            wal.append_batch(&[rec(2, b"bb"), rec(3, b"ccc")]).unwrap();
            assert_eq!(wal.len(), 3);
        }
        let (_wal, recovered) = open(&path);
        assert_eq!(recovered, vec![rec(1, b"a"), rec(2, b"bb"), rec(3, b"ccc")]);
    }

    #[test]
    fn a_torn_tail_is_discarded_and_appends_continue() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        {
            let (mut wal, _) = open(&path);
            wal.append(&rec(1, b"a")).unwrap();
        }
        // Garbage after the valid record (a write that never completed).
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0x12, 0x34, 0x56]).unwrap();
        drop(file);

        let (mut wal, recovered) = open(&path);
        assert_eq!(recovered, vec![rec(1, b"a")], "the torn tail is dropped");
        // The torn tail was truncated on open, so appends land cleanly after it.
        wal.append(&rec(2, b"b")).unwrap();
        drop(wal);
        let (_wal, recovered) = open(&path);
        assert_eq!(recovered, vec![rec(1, b"a"), rec(2, b"b")]);
    }

    #[test]
    fn a_record_cut_mid_payload_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        {
            let (mut wal, _) = open(&path);
            wal.append_batch(&[rec(1, b"a"), rec(2, b"b")]).unwrap();
        }
        // Cut the file mid-record, as a crash during a write would.
        let len = fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 3).unwrap();
        drop(file);

        let (_wal, recovered) = open(&path);
        assert_eq!(
            recovered,
            vec![rec(1, b"a")],
            "the half-written record is dropped; the valid prefix survives"
        );
    }

    #[test]
    fn a_corrupted_checksum_ends_the_valid_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        {
            let (mut wal, _) = open(&path);
            wal.append_batch(&[rec(1, b"a"), rec(2, b"b")]).unwrap();
        }
        // Flip a byte inside the second record's payload.
        let mut bytes = fs::read(&path).unwrap();
        let len0 = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let second_start = 4 + len0 + 8;
        bytes[second_start + 5] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let (_wal, recovered) = open(&path);
        assert_eq!(
            recovered.len(),
            1,
            "the corrupt record and after are dropped"
        );
    }

    #[test]
    fn truncate_drops_a_conflicting_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let (mut wal, _) = open(&path);
        wal.append_batch(&[rec(1, b"a"), rec(2, b"b"), rec(3, b"c")])
            .unwrap();
        // Keep the first record, then append a different suffix.
        wal.truncate(1).unwrap();
        assert_eq!(wal.len(), 1);
        wal.append_batch(&[rec(2, b"x"), rec(3, b"y")]).unwrap();
        drop(wal);

        let (_wal, recovered) = open(&path);
        assert_eq!(recovered, vec![rec(1, b"a"), rec(2, b"x"), rec(3, b"y")]);
    }

    #[test]
    fn truncate_at_or_past_the_end_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let (mut wal, _) = open(&path);
        wal.append_batch(&[rec(1, b"a"), rec(2, b"b")]).unwrap();
        wal.truncate(2).unwrap();
        wal.truncate(9).unwrap();
        assert_eq!(wal.len(), 2);
    }

    #[test]
    fn rewrite_replaces_the_whole_file_and_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        {
            let (mut wal, _) = open(&path);
            wal.append_batch(&[rec(1, b"a"), rec(2, b"b"), rec(3, b"c")])
                .unwrap();
            let before = fs::metadata(&path).unwrap().len();
            // Compact to a single record; the file shrinks and the handle keeps working.
            wal.rewrite(&[rec(9, b"z")]).unwrap();
            let after = fs::metadata(&path).unwrap().len();
            assert!(
                after < before,
                "rewrite shrank the file: {after} < {before}"
            );
            wal.append(&rec(10, b"w")).unwrap();
        }
        let (_wal, recovered) = open(&path);
        assert_eq!(recovered, vec![rec(9, b"z"), rec(10, b"w")]);
    }

    #[test]
    #[should_panic(expected = "exceeds")]
    fn appending_a_record_past_the_limit_panics_instead_of_losing_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        // A tiny limit: scan would reject a frame larger than this, so the write must
        // reject it too rather than acknowledge a record recovery would silently drop.
        let (mut wal, _) = Wal::<Rec>::open(&path, 8).unwrap();
        wal.append(&rec(1, &[0u8; 64])).unwrap();
    }

    #[test]
    fn atomic_replace_round_trips_a_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        atomic_replace(dir.path(), "state", b"hello").unwrap();
        assert_eq!(fs::read(dir.path().join("state")).unwrap(), b"hello");
        // A second replace overwrites it whole.
        atomic_replace(dir.path(), "state", b"world!").unwrap();
        assert_eq!(fs::read(dir.path().join("state")).unwrap(), b"world!");
    }
}
