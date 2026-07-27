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
//! [`Wal<T>`] is a 16-byte header followed by an append-only run of postcard-encoded
//! `T` records, each framed
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
//! # The header
//!
//! The header is what lets any of this change later. It carries a magic, the **frame
//! layout's** revision and the **checksum kind** — this crate's own secrets — and a
//! **record-schema** revision that belongs to the caller and that this crate stores
//! and returns without interpreting. That last field is the load-bearing one: a
//! `Wal<T>`'s records are postcard, which is positional and has no field names, so `T`
//! cannot gain a field and its revision has nowhere to live except outside the payload.
//!
//! [`open`](Wal::open) therefore takes the caller's [`compat::Window`] and does the
//! refusing itself, rather than exposing the header for a caller to check. An optional
//! check is one somebody forgets, and the forgotten case is the misparse the stamp
//! exists to prevent.
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
//! [`open`](Wal::open) is the exception, returning [`OpenError`], because it has a
//! second failure that is not an I/O event at all: a file this build cannot read. The
//! bytes are intact and the disk is fine; the refusal is policy. Folding it into
//! `io::ErrorKind::InvalidData` would blur exactly the distinction this crate is
//! careful about elsewhere, so a caller that wants them collapsed says so, with
//! [`OpenError::into_io`].
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

/// The magic that opens every log file, in the PNG convention: a high-bit byte, the
/// name, then a CRLF/EOF trap that catches a text-mode transfer.
///
/// Its first four bytes read as a little-endian `u32` are `0x4C41_5789`, which
/// exceeds every `max_record` this crate permits ([`Wal::open`] asserts it). A
/// headerless file therefore can never begin with this magic — its first bytes are a
/// frame length at or below `max_record` — so an unstamped file is refused by name
/// instead of scanned for frames that happen to parse.
const MAGIC: &[u8] = b"\x89WAL\r\n\x1a\n";

/// The frame layout's revisions, and the one this build writes (compatibility spec
/// §3). Revision 1 is the `[len][payload][checksum]` framing of §2.
const FRAME: compat::Stamp =
    compat::Stamp::new(MAGIC, compat::Window::at("wal.frame", 1));

/// The checksum this build computes and requires: FNV-1a, 64-bit.
///
/// An *identity*, not a revision — there is no ordering over hash functions and
/// nothing to gain by pretending otherwise, so it is compared for equality and a
/// mismatch is refused by name. Reserving the field is what makes the stronger
/// digest a caller might want for untrusted media a header change rather than a
/// layout change.
const CHECKSUM_FNV1A: u16 = 1;

/// Width of the header **at frame revision 1** — the overhead a log written by this
/// build carries before its first frame. Public so a caller sizing a store, or
/// reaching past the header in a test, can account for it.
///
/// ```text
/// [magic 8][frame revision u16][checksum kind u16][record schema u16][reserved u16]
/// ```
///
/// The width belongs to the revision, not to the format: a later revision may define
/// a different header entirely, and can, because a revision-1 reader refuses a
/// revision-2 file before reaching its header. Only the magic and the `u16` after it
/// are fixed across revisions — a reader consults those *before* it knows which
/// layout applies. Code that must work across revisions should therefore take the
/// width from the revision it admitted rather than from this constant.
///
/// The header has no checksum of its own. Every field is validated against a known
/// value or window, so detectable damage is refused; the reserved `u16` is where a
/// header digest would go if that ever proves insufficient.
pub const HEADER_LEN: usize = 16;

/// Build the header a new log opens with. `records` is the caller's schema
/// revision — this crate stores and returns it without interpreting it (§2).
fn header(records: compat::Version) -> Vec<u8> {
    let mut tail = Vec::with_capacity(6);
    tail.extend_from_slice(&CHECKSUM_FNV1A.to_le_bytes());
    tail.extend_from_slice(&records.0.to_le_bytes());
    tail.extend_from_slice(&0u16.to_le_bytes()); // reserved
    let bytes = FRAME.stamp(&tail);
    debug_assert_eq!(bytes.len(), HEADER_LEN, "the header is a fixed width");
    bytes
}

/// Admit a log's header, returning the frame bytes that follow it.
///
/// The order is the contract: the magic, then the frame revision, then the checksum
/// kind, then the caller's record schema. Nothing is scanned until all four are
/// accepted (compatibility **V2**), so a file this build cannot read is never
/// partially interpreted.
fn admit_header<'a>(
    bytes: &'a [u8],
    records: &compat::Window,
) -> Result<&'a [u8], compat::Incompatible> {
    let (_frame, tail) = FRAME.unstamp(bytes)?;
    if tail.len() < HEADER_LEN - MAGIC.len() - size_of::<u16>() {
        // A header cut short. Unreachable through `open`, which initializes a file
        // too short to hold one, but the check keeps this function sound on its own.
        return Err(compat::Incompatible::Unstamped {
            boundary: FRAME.window().boundary(),
            accepted: FRAME.window().accepted(),
        });
    }
    let field = |i: usize| u16::from_le_bytes([tail[i * 2], tail[i * 2 + 1]]);
    if field(0) != CHECKSUM_FNV1A {
        return Err(compat::Incompatible::Version {
            boundary: "wal.checksum",
            found: compat::Version(field(0)),
            accepted: compat::Accepted::only(CHECKSUM_FNV1A),
        });
    }
    records.admit(compat::Version(field(1)))?;
    if field(2) != 0 {
        // Revision 1 defines the reserved field as zero, and the frame revision gates
        // the layout, so a later revision that gives it meaning will say so by bumping
        // that. Requiring it here costs nothing and makes all sixteen header bytes
        // self-validating: without it, corruption in this field would be the one header
        // damage that passes silently.
        return Err(compat::Incompatible::Version {
            boundary: "wal.reserved",
            found: compat::Version(field(2)),
            accepted: compat::Accepted::only(0),
        });
    }
    Ok(&bytes[HEADER_LEN..])
}

/// Opening a log failed.
///
/// Two variants because the two failures are different kinds and collapsing them
/// would lose the distinction that matters: an [`Io`](OpenError::Io) failure is a
/// filesystem event whose *meaning* is still the caller's to decide (see the
/// module's failure policy), while an [`Incompatible`](OpenError::Incompatible) file
/// is a policy refusal — the bytes are intact and simply not something this build
/// reads.
#[derive(Debug)]
pub enum OpenError {
    /// A filesystem error reading, creating, or truncating the file.
    Io(io::Error),
    /// The file is not a log this build can read: a foreign file, a frame layout
    /// from another revision, a checksum this build does not compute, or a record
    /// schema outside the caller's window.
    Incompatible(compat::Incompatible),
}

impl OpenError {
    /// Collapse into an [`io::Error`], for a caller whose own signature is
    /// `io::Result`. An incompatible file becomes `InvalidData` — the bytes are not
    /// what this build reads — with the refusal's own message carrying which
    /// boundary and which revision.
    pub fn into_io(self) -> io::Error {
        match self {
            OpenError::Io(err) => err,
            OpenError::Incompatible(err) => io::Error::new(io::ErrorKind::InvalidData, err),
        }
    }
}

impl From<io::Error> for OpenError {
    fn from(err: io::Error) -> OpenError {
        OpenError::Io(err)
    }
}

impl From<compat::Incompatible> for OpenError {
    fn from(err: compat::Incompatible) -> OpenError {
        OpenError::Incompatible(err)
    }
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::Io(err) => write!(f, "{err}"),
            OpenError::Incompatible(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for OpenError {}

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

/// Scan a framed log's frame bytes into `(records, per-record start offsets, valid
/// length)`. The scan stops at the first incomplete, oversized, checksum-failing, or
/// unparsable record — the recovery rule: the valid prefix is the log; the tail was
/// never acknowledged.
///
/// `bytes` is the frame region alone (the header is already admitted); `base` is
/// where that region starts in the file, so the returned offsets are absolute and
/// [`Wal::truncate`] stays a single `set_len`. The returned length is relative to
/// `base`.
fn scan<T: DeserializeOwned>(
    bytes: &[u8],
    max_record: u32,
    base: u64,
) -> (Vec<T>, Vec<u64>, u64) {
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
        offsets.push(base + pos as u64);
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
    /// The record-schema revision this build *writes*, held so
    /// [`rewrite`](Wal::rewrite) can stamp the replacement it produces. This crate
    /// never interprets it.
    ///
    /// Note what this is not: the revision stamped in the file that was opened. The
    /// two differ only once a caller's window spans more than one revision, and then
    /// [`append`](Wal::append) adds frames at *this* revision to a file whose header
    /// still records the older one — so the stamp understates until a
    /// [`rewrite`](Wal::rewrite) restamps it. The consequence is bounded and
    /// fail-closed: a later build whose window starts above the stale stamp refuses
    /// the file by name rather than misreading it, because every frame in it is in
    /// fact readable at the higher revision. A caller widening its window should
    /// compact affected logs; raising the stamp in place on the first append after a
    /// bump is the refinement that would remove the caveat, and it needs a second,
    /// non-append handle to reach the header.
    records: compat::Version,
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
    pub fn open(
        path: impl Into<PathBuf>,
        max_record: u32,
        records: &compat::Window,
    ) -> Result<(Wal<T>, Vec<T>), OpenError> {
        // What keeps the unstamped-file refusal sound: a headerless file begins with
        // a frame length at or below `max_record`, so it cannot begin with `MAGIC`
        // (see there). A caller permitting a larger record would break that.
        assert!(
            u64::from(max_record) < 0x4C41_5789,
            "max_record must stay below the magic's u32 reading so an unstamped file \
             is never mistaken for a stamped one"
        );
        let path = path.into();
        let (bytes, existed) = match fs::read(&path) {
            Ok(bytes) => (bytes, true),
            Err(err) if err.kind() == io::ErrorKind::NotFound => (Vec::new(), false),
            Err(err) => return Err(err.into()),
        };
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;

        // A file whose bytes are a *prefix* of the header we would write holds no
        // frames — the header is written and fsynced before any append can follow it —
        // so stamping it in place is lossless. This covers a missing file, an empty
        // one, and a header torn by a crash between creating the file and syncing it,
        // which would otherwise turn a benign crash into a log that can never be
        // opened again.
        //
        // The prefix test is what keeps that from becoming a licence to overwrite: a
        // short file that is *not* heading toward this header — a foreign file, or a
        // log from a build that framed records differently — falls through to the
        // refusal below instead of being silently truncated.
        let fresh = header(records.writes());
        if bytes.len() < HEADER_LEN {
            if !fresh.starts_with(&bytes) {
                return Err(compat::Incompatible::Unstamped {
                    boundary: FRAME.window().boundary(),
                    accepted: FRAME.window().accepted(),
                }
                .into());
            }
            if !bytes.is_empty() {
                file.set_len(0)?;
            }
            file.write_all(&fresh)?;
            file.sync_all()?;
            if !existed {
                // The file was just created. Its bytes become durable on the fsync
                // above, but the directory entry that names it needs its own fsync,
                // or a crash could lose a file whose appends were already
                // acknowledged. Doing it here makes the new log's entry durable for
                // every caller, including one that creates its logs lazily, so no
                // caller has to remember to.
                sync_dir(parent_dir(&path))?;
            }
            return Ok((
                Wal {
                    path,
                    file,
                    offsets: Vec::new(),
                    end: HEADER_LEN as u64,
                    max_record,
                    records: records.writes(),
                    _marker: PhantomData,
                },
                Vec::new(),
            ));
        }

        // The header is admitted whole before a single frame is scanned (**V2**).
        let frames = admit_header(&bytes, records)?;
        let (recovered, offsets, valid_body) = scan::<T>(frames, max_record, HEADER_LEN as u64);
        let valid_end = HEADER_LEN as u64 + valid_body;
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
                records: records.writes(),
                _marker: PhantomData,
            },
            recovered,
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
    /// The offsets are absolute and the first is the header's width, so truncating to
    /// zero records leaves the header in place rather than emptying the file.
    ///
    /// # Errors
    ///
    /// Any filesystem error truncating or syncing.
    pub fn truncate(&mut self, keep: usize) -> io::Result<()> {
        if keep >= self.offsets.len() {
            return Ok(());
        }
        let cut = self.offsets[keep];
        debug_assert!(cut >= HEADER_LEN as u64, "truncation never cuts the header");
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
        let (frames, offsets) = self.frame_all(records, HEADER_LEN as u64);
        // The replacement is a whole file, so it carries a header like any other.
        let mut buf = header(self.records);
        buf.extend_from_slice(&frames);
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

    const RECORDS: compat::Window = compat::Window::at("wal.test", 1);

    fn open(path: &Path) -> (Wal<Rec>, Vec<Rec>) {
        Wal::<Rec>::open(path, MAX, &RECORDS).unwrap()
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
        let first = HEADER_LEN;
        let len0 = u32::from_le_bytes(bytes[first..first + 4].try_into().unwrap()) as usize;
        let second_start = first + 4 + len0 + 8;
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
        let (mut wal, _) = Wal::<Rec>::open(&path, 8, &RECORDS).unwrap();
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

    #[test]
    fn a_new_log_is_stamped_and_the_stamp_survives_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        {
            let (mut wal, recovered) = open(&path);
            assert!(recovered.is_empty());
            // The header exists before any record does, so an empty log is still a
            // recognizable one.
            assert_eq!(fs::metadata(&path).unwrap().len(), HEADER_LEN as u64);
            wal.append(&rec(1, b"a")).unwrap();
        }
        let bytes = fs::read(&path).unwrap();
        assert!(bytes.starts_with(MAGIC), "the magic leads the file");
        let (_wal, recovered) = open(&path);
        assert_eq!(recovered, vec![rec(1, b"a")]);
    }

    #[test]
    fn an_unstamped_file_is_refused_rather_than_scanned() {
        // A headerless log, as a build predating the header would have written: one
        // valid-looking frame. It must be refused, not scanned — the frames may parse
        // and still mean something else.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let payload = postcard::to_allocvec(&rec(1, b"a")).unwrap();
        let mut bytes = (payload.len() as u32).to_le_bytes().to_vec();
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&checksum(&payload).to_le_bytes());
        fs::write(&path, &bytes).unwrap();

        let err = Wal::<Rec>::open(&path, MAX, &RECORDS)
            .err()
            .expect("an unstamped file must not open");
        assert!(
            matches!(err, OpenError::Incompatible(compat::Incompatible::Unstamped { .. })),
            "expected an Unstamped refusal, got {err}"
        );
    }

    #[test]
    fn a_record_schema_outside_the_window_is_refused_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        {
            let (mut wal, _) = open(&path);
            wal.append(&rec(1, b"a")).unwrap();
        }
        // Reopen under a caller whose records have moved on. The frames are intact and
        // this build simply cannot interpret them.
        const LATER: compat::Window = compat::Window::new("wal.test", 4, 5, 4);
        let err = Wal::<Rec>::open(&path, MAX, &LATER)
            .err()
            .expect("a schema outside the window must not open");
        let msg = err.to_string();
        assert!(
            msg.contains("wal.test") && msg.contains("v1"),
            "the refusal must name the boundary and what it found: {msg}"
        );
    }

    #[test]
    fn a_foreign_checksum_kind_is_refused_by_name() {
        // The reserved field doing its job: a log written with a digest this build does
        // not compute is refused, rather than every frame failing its checksum and the
        // whole log reading as one torn tail.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        {
            let (mut wal, _) = open(&path);
            wal.append(&rec(1, b"a")).unwrap();
        }
        let mut bytes = fs::read(&path).unwrap();
        bytes[10..12].copy_from_slice(&7u16.to_le_bytes());
        fs::write(&path, &bytes).unwrap();

        let msg = Wal::<Rec>::open(&path, MAX, &RECORDS)
            .err()
            .expect("a foreign checksum must not open")
            .to_string();
        assert!(msg.contains("wal.checksum"), "must name the field: {msg}");
    }

    #[test]
    fn a_header_lost_to_a_crash_before_the_first_append_is_reinitialized() {
        // A crash between creating the file and stamping it. The file holds no frames,
        // so re-stamping is lossless — and the alternative would turn a benign crash
        // into a log that can never be opened again.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        for partial in [Vec::new(), MAGIC[..3].to_vec()] {
            fs::write(&path, &partial).unwrap();
            let (mut wal, recovered) = open(&path);
            assert!(recovered.is_empty());
            wal.append(&rec(1, b"a")).unwrap();
            drop(wal);
            let (_wal, recovered) = open(&path);
            assert_eq!(recovered, vec![rec(1, b"a")], "the log works after re-stamping");
        }
    }

    #[test]
    fn truncating_every_record_keeps_the_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let (mut wal, _) = open(&path);
        wal.append_batch(&[rec(1, b"a"), rec(2, b"b")]).unwrap();
        wal.truncate(0).unwrap();
        assert_eq!(wal.len(), 0);
        assert_eq!(fs::metadata(&path).unwrap().len(), HEADER_LEN as u64);
        // And the emptied log is still a stamped one the next open accepts.
        wal.append(&rec(3, b"c")).unwrap();
        drop(wal);
        let (_wal, recovered) = open(&path);
        assert_eq!(recovered, vec![rec(3, b"c")]);
    }

    #[test]
    fn rewrite_restamps_the_replacement() {
        // `rewrite` replaces the whole file, so it must write a header too — otherwise
        // compaction would produce a log nothing can reopen.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        {
            let (mut wal, _) = open(&path);
            wal.append_batch(&[rec(1, b"a"), rec(2, b"b")]).unwrap();
            wal.rewrite(&[rec(9, b"z")]).unwrap();
        }
        assert!(fs::read(&path).unwrap().starts_with(MAGIC));
        let (_wal, recovered) = open(&path);
        assert_eq!(recovered, vec![rec(9, b"z")]);
    }

    #[test]
    #[should_panic(expected = "max_record must stay below the magic")]
    fn a_max_record_that_could_alias_the_magic_is_rejected() {
        // What keeps the unstamped-file refusal sound (see `MAGIC`).
        let dir = tempfile::tempdir().unwrap();
        let _ = Wal::<Rec>::open(dir.path().join("log"), 0x4C41_5789, &RECORDS);
    }
}
