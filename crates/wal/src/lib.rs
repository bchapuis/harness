//! A generic, framed, checksummed write-ahead log on the local filesystem.
//!
//! A divergence between the write path and the recovery path mis-recovers a node, so
//! framing, checksumming, recovery, and atomic rewrite live here, once.
//!
//! # The log
//!
//! [`Wal<T>`] is a 16-byte header followed by an append-only run of postcard-encoded
//! `T` records, each framed
//! `[u32 little-endian length][postcard payload][u64 little-endian checksum]`
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
//! The header carries a magic, the **frame layout's** revision and the **checksum
//! kind** — this crate's own secrets — and a **record-schema** revision that belongs
//! to the caller and that this crate stores and returns without interpreting. That
//! last field is the load-bearing one: a `Wal<T>`'s records are postcard, which is
//! positional and has no field names, so `T` cannot gain a field and its revision has
//! nowhere to live except outside the payload.
//!
//! [`open`](Wal::open) therefore takes the caller's [`compat::Window`] and does the
//! refusing itself, rather than exposing the header for a caller to check.
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
//!
//! [`open`](Wal::open) is the exception, returning [`OpenError`], because it has a
//! second failure that is not an I/O event at all: a file this build cannot read. The
//! bytes are intact and the disk is fine; the refusal is policy. A caller that wants
//! the two collapsed says so, with [`OpenError::into_io`].

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

/// FNV-1a 64, for a caller framing its own sidecar bytes (e.g. a fixed-width value
/// file). Detects torn and partial writes, not adversarial tampering.
///
/// **This function's output is frozen.** A sidecar is a bare `[value][checksum]` with no
/// header, so a reader has no way to ask which digest wrote it — it can only recompute
/// and compare. Callers treat a mismatch as *torn or absent* rather than as a refusal,
/// because with `atomic_replace` behind them a mismatch cannot otherwise happen. Change
/// what this returns and every existing sidecar silently reads as missing: granary's
/// durable fence would come back as *no fence* (**G15**), which is a safety property, not
/// a compatibility inconvenience.
///
/// The log's own frames do not use this. They carry a checksum-kind field in the header
/// naming which digest closed them, so they are free to move to a faster one and still be
/// read — and that field is exactly what a sidecar lacks. A digest behind a version field
/// may change; a digest without one may not.
pub fn checksum(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

/// The digest closing a log's frames — the value the header's checksum-kind field names.
///
/// A file records which digest wrote it, so a build that computes a different one can
/// still read it: the scan uses the digest the header names, never the one this build
/// prefers. That is the whole point of reserving the field, and it is what makes moving to
/// a faster digest an upgrade rather than a migration — a log written before the move
/// opens and replays untouched, and appends to it stay in its own digest so a file is
/// never a mix of two.
///
/// Private, and deliberately so: nothing outside may depend on what these return, because
/// the set may grow again. A caller framing its own bytes wants [`checksum`], which is
/// frozen precisely because it has no header to record a choice in.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Digest {
    /// Kind 1. What this crate wrote before [`Xxh3`](Digest::Xxh3); read-only in the sense
    /// that no new log is stamped with it, though a log already stamped keeps using it.
    Fnv1a,
    /// Kind 2. ~17.5 GB/s on a 4 KiB frame against FNV-1a's ~0.6 GB/s — FNV is a serial
    /// multiply chain, one byte per multiply, which made a large log's recovery
    /// digest-bound. Not the weaker check for the change: both are 64 bits wide, and XXH3
    /// has the better avalanche.
    Xxh3,
}

impl Digest {
    /// The digest a log created by this build is stamped with.
    const WRITES: Digest = Digest::Xxh3;

    /// The header value naming this digest.
    const fn kind(self) -> u16 {
        match self {
            Digest::Fnv1a => 1,
            Digest::Xxh3 => 2,
        }
    }

    /// The digest a header names, or `None` for one this build cannot compute — which is
    /// refused by name rather than left to fail every frame as if the log were corrupt.
    const fn from_kind(kind: u16) -> Option<Digest> {
        match kind {
            1 => Some(Digest::Fnv1a),
            2 => Some(Digest::Xxh3),
            _ => None,
        }
    }

    /// Digest `bytes`.
    fn of(self, bytes: &[u8]) -> u64 {
        match self {
            Digest::Fnv1a => checksum(bytes),
            Digest::Xxh3 => xxhash_rust::xxh3::xxh3_64(bytes),
        }
    }
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

/// The framing capacity a log keeps between appends, when its last batch wanted less.
///
/// Reusing one buffer is what makes an append allocation-free, but the capacity it reaches
/// is the largest batch that log has ever written. A caller holding one log per object —
/// granary keeps one per grain — would otherwise pay for every log's worst batch, forever.
/// A floor rather than a ceiling: [`append_batch`](Wal::append_batch) keeps whichever is
/// larger of this and the batch it just wrote, so a steady large writer is not punished
/// and a one-off spike is still handed back.
const SCRATCH_KEEP: usize = 16 * 1024;

/// The offset capacity a log keeps between appends, in offsets — the [`SCRATCH_KEEP`]
/// rule applied to the other buffer.
///
/// Sized on its own rather than derived from the byte floor: offsets scale with the
/// *record count* of a batch, not its width, and a batch of two thousand records is
/// already far past the spike this is meant to release.
const SCRATCH_OFFSETS: usize = 256;

/// Width of the little-endian length prefix that opens every frame. Tied to the
/// prefix's own type so the write path ([`frame_onto`]) and the recovery path
/// ([`scan`]) frame from one definition and cannot drift apart on the layout.
const LEN_BYTES: usize = size_of::<u32>();
/// Width of the little-endian checksum that closes every frame. One width for every
/// [`Digest`]: they are all 64 bits, so which one a file uses is a header question, not a
/// layout one — the reason moving between them leaves the frame revision alone.
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

/// The checksum kinds this build can compute, for the refusal that names them.
///
/// An *identity*, not a revision: there is no ordering over hash functions, so a kind is
/// matched, not compared, and one outside this set is refused by name rather than left to
/// fail every frame as if the file were corrupt.
///
/// Reserving the field is what made adding [`Digest::Xxh3`] beside [`Digest::Fnv1a`] a
/// header change rather than a layout one: both are 64 bits wide, so the frame layout
/// never had to mention which filled the field. And because a file says which digest wrote
/// it, both are *read* — a log stamped kind 1 opens and replays on a build that prefers
/// kind 2. The alternative, refusing it, would have made a routine upgrade unreadable for
/// every Raft log, grain segment, and blob log at once.
const ACCEPTED_CHECKSUMS: compat::Accepted =
    compat::Accepted::new(Digest::Fnv1a.kind(), Digest::Xxh3.kind());

/// Width of the header **at frame revision 1** — the overhead a log written by this
/// build carries before its first frame. Public so a caller sizing a store, or
/// reaching past the header in a test, can account for it.
///
/// ```text
/// [magic 8][frame revision u16][checksum kind u16][record schema u16][reserved u16]
/// ```
///
/// The width belongs to the revision, not to the format: a later revision may define
/// a different header entirely, because a revision-1 reader refuses a revision-2 file
/// before reaching its header. Only the magic and the `u16` after it are fixed across
/// revisions, since a reader consults those *before* it knows which layout applies.
/// Code that must work across revisions should take the width from the revision it
/// admitted rather than from this constant.
///
/// The header has no checksum of its own. Every field is validated against a known
/// value or window, so detectable damage is refused; the reserved `u16` is where a
/// header digest would go.
pub const HEADER_LEN: usize = 16;

/// Build the header a new log opens with. `records` is the caller's schema
/// revision — this crate stores and returns it without interpreting it (§2).
fn header(records: compat::Version, digest: Digest) -> Vec<u8> {
    let mut tail = Vec::with_capacity(6);
    tail.extend_from_slice(&digest.kind().to_le_bytes());
    tail.extend_from_slice(&records.0.to_le_bytes());
    tail.extend_from_slice(&0u16.to_le_bytes()); // reserved
    let bytes = FRAME.stamp(&tail);
    debug_assert_eq!(bytes.len(), HEADER_LEN, "the header is a fixed width");
    bytes
}

/// Admit a log's header, returning the digest it names and the frame bytes that follow it.
///
/// The order is the contract: the magic, then the frame revision, then the checksum
/// kind, then the caller's record schema. Nothing is scanned until all four are
/// accepted (compatibility **V2**), so a file this build cannot read is never
/// partially interpreted.
///
/// The digest comes back rather than being checked against a single expected value,
/// because the file's own header is the authority on which one closes its frames.
fn admit_header<'a>(
    bytes: &'a [u8],
    records: &compat::Window,
) -> Result<(Digest, &'a [u8]), compat::Incompatible> {
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
    let Some(digest) = Digest::from_kind(field(0)) else {
        return Err(compat::Incompatible::Version {
            boundary: "wal.checksum",
            found: compat::Version(field(0)),
            accepted: ACCEPTED_CHECKSUMS,
        });
    };
    records.admit(compat::Version(field(1)))?;
    if field(2) != 0 {
        // Revision 1 defines the reserved field as zero, and the frame revision gates
        // the layout, so a later revision that gives it meaning bumps that. Requiring
        // it here keeps all sixteen header bytes self-validating: without it,
        // corruption in this field would be the one header damage that passes
        // silently.
        return Err(compat::Incompatible::Version {
            boundary: "wal.reserved",
            found: compat::Version(field(2)),
            accepted: compat::Accepted::only(0),
        });
    }
    Ok((digest, &bytes[HEADER_LEN..]))
}

/// Opening a log failed.
///
/// An [`Io`](OpenError::Io) failure is a filesystem event whose *meaning* is the
/// caller's to decide (see the module's failure policy); an
/// [`Incompatible`](OpenError::Incompatible) file is a policy refusal, its bytes
/// intact and simply not something this build reads.
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

/// Frame one record — `[u32 len][postcard payload][u64 checksum]`, all little-endian —
/// onto the tail of `buf`, returning the buffer.
///
/// The payload is serialized straight onto `buf` rather than into a `Vec` of its own
/// that is then copied in: the length is written as a placeholder first, and back-patched
/// once serializing reveals it. A caller that keeps one buffer across calls therefore
/// frames without allocating at all, which is what makes an append in steady state
/// allocation-free.
///
/// The buffer moves in and out by value because postcard serializes into a sink it owns
/// (`Extend<u8>`), not through a `&mut`.
///
/// Panics if the payload exceeds `max_record`. The scan that recovers the log treats a
/// length above `max_record` as corruption and drops it (and everything after it), so a
/// record that scan would reject must never be written: it would be acknowledged here
/// and silently lost on the next open. Failing loudly at the write keeps that asymmetry
/// from becoming silent data loss. The half-framed record is discarded with `buf`, which
/// the panic unwinds past before any of it reaches the file.
fn frame_onto<T: Serialize>(
    mut buf: Vec<u8>,
    value: &T,
    max_record: u32,
    digest: Digest,
) -> Vec<u8> {
    let frame = buf.len();
    buf.extend_from_slice(&0u32.to_le_bytes()); // back-patched below
    let mut buf = postcard::to_extend(value, buf).expect("a WAL record always serializes");
    let payload = buf.len() - frame - LEN_BYTES;
    assert!(
        payload as u64 <= u64::from(max_record),
        "WAL record of {payload} bytes exceeds the {max_record}-byte limit; recovery \
         would discard it, so it must not be written",
    );
    let check = digest.of(&buf[frame + LEN_BYTES..]);
    buf[frame..frame + LEN_BYTES].copy_from_slice(&(payload as u32).to_le_bytes());
    buf.extend_from_slice(&check.to_le_bytes());
    buf
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
    digest: Digest,
) -> (Vec<T>, Vec<u64>, u64) {
    // Size both vectors from the first frame's width. A log holds one record type, so
    // that width estimates the rest well, and recovery is where it matters: growing from
    // zero across a file of millions of frames recopies the vector on every doubling.
    // Capped because the width is only a guess — a first record narrower than the rest
    // would otherwise reserve for more records than the file can hold. Past the cap they
    // grow as before, from a base that is already close.
    let estimate = match bytes.get(..LEN_BYTES) {
        Some(len) => {
            let len = u32::from_le_bytes(len.try_into().expect("length-prefix slice"));
            bytes.len() / (LEN_BYTES + len as usize + CHECKSUM_BYTES)
        }
        None => 0,
    }
    .min(1 << 16);
    let mut records = Vec::with_capacity(estimate);
    let mut offsets = Vec::with_capacity(estimate);
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
        if u64::from_le_bytes(check.try_into().expect("checksum slice")) != digest.of(payload) {
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
    /// The digest closing *this file's* frames, as its header names it — not necessarily
    /// the one this build prefers.
    ///
    /// Held per log rather than taken from a constant so a file opened at an older kind
    /// keeps being read *and appended to* in its own digest. A header names one digest for
    /// the whole file, so a mixed file would be unreadable; the way a log moves to a newer
    /// digest is [`rewrite`](Wal::rewrite), which restamps the header it replaces. That
    /// mirrors how the `records` schema below is restamped, and for the same reason.
    digest: Digest,
    /// The buffer [`append_batch`](Wal::append_batch) frames into, kept across calls so a
    /// steady-state append allocates nothing: it reaches the high-water mark of the
    /// batches this log sees and stays there. Empty between calls — it holds no state,
    /// only capacity.
    scratch: Vec<u8>,
    /// The offsets of the batch being framed, staged here until the write succeeds and
    /// they can join `offsets`. Reused like `scratch`, and for the same reason.
    ///
    /// Staged rather than pushed straight onto `offsets` because framing can panic — a
    /// record above `max_record` is refused that way — and an unwind must not leave
    /// `offsets` naming frames that were never written. Both buffers are locals for the
    /// duration of a call, so a panic drops them and leaves the log's own state untouched.
    staged: Vec<u64>,
    /// Upper bound on one frame's payload. Enforced on every write (a larger record is
    /// rejected loudly) and on recovery (a larger length is treated as corruption), so
    /// the write path and the scan path agree on what is a valid record.
    max_record: u32,
    /// The record-schema revision this build *writes*, held so
    /// [`rewrite`](Wal::rewrite) can stamp the replacement it produces. This crate
    /// never interprets it.
    ///
    /// Not the revision stamped in the file that was opened. The two differ once a
    /// caller's window spans more than one revision: [`append`](Wal::append) adds
    /// frames at *this* revision to a file whose header still records the older one,
    /// so the stamp understates until a [`rewrite`](Wal::rewrite) restamps it. The
    /// consequence is fail-closed — a later build whose window starts above the stale
    /// stamp refuses the file by name rather than misreading it. A caller widening its
    /// window should compact affected logs. Not implemented: raising the stamp in
    /// place on the first append after a bump.
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
        // The prefix test bounds that: a short file that is *not* heading toward this
        // header falls through to the refusal below instead of being silently
        // truncated.
        let fresh = header(records.writes(), Digest::WRITES);
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
                // acknowledged.
                sync_dir(parent_dir(&path))?;
            }
            return Ok((
                Wal {
                    path,
                    file,
                    offsets: Vec::new(),
                    end: HEADER_LEN as u64,
                    digest: Digest::WRITES,
                    scratch: Vec::new(),
                    staged: Vec::new(),
                    max_record,
                    records: records.writes(),
                    _marker: PhantomData,
                },
                Vec::new(),
            ));
        }

        // The header is admitted whole before a single frame is scanned (**V2**).
        let (digest, frames) = admit_header(&bytes, records)?;
        let (recovered, offsets, valid_body) =
            scan::<T>(frames, max_record, HEADER_LEN as u64, digest);
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
                digest,
                scratch: Vec::new(),
                staged: Vec::new(),
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

    /// Frame `record`, append it, and fsync — durable before the call returns. The
    /// single-record case of [`append_batch`](Wal::append_batch).
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
    /// The fsync is `sync_all`, not `sync_data`. On Linux the two are fsync and fdatasync
    /// and the latter is the tighter fit, but they do not mean the same thing everywhere:
    /// on macOS only `sync_all` is `F_FULLFSYNC`, which flushes the drive's own write
    /// cache. Taking the cheaper one would make "durable before the call returns" weaker
    /// on the platform the crash tests run on, and it measured no faster.
    ///
    /// # Errors
    ///
    /// Any filesystem error writing or syncing.
    pub fn append_batch(&mut self, records: &[T]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        // Both buffers are taken out for the call and put back after, so framing reuses
        // their capacity instead of allocating, and an unwind leaves the log untouched.
        // Both are left empty by the previous call, so they need no clearing here.
        let mut buf = std::mem::take(&mut self.scratch);
        let mut staged = std::mem::take(&mut self.staged);
        for record in records {
            staged.push(self.end + buf.len() as u64);
            buf = frame_onto(buf, record, self.max_record, self.digest);
        }
        let written = buf.len() as u64;
        let result = self
            .file
            .write_all(&buf)
            .and_then(|()| self.file.sync_all());
        // Both buffers go back whether or not the write succeeded — they are capacity,
        // not state, and a failed append should not cost the next one its allocation. The
        // offsets they staged are published only on success, so a failed write leaves the
        // log describing exactly the frames that were already there.
        if result.is_ok() {
            self.offsets.extend_from_slice(&staged);
            self.end += written;
        }
        // Release capacity an outlier left behind, but never below what this call itself
        // needed. A flat cap would shrink and regrow on every append for any log whose
        // *steady* batch is larger than it — measured at 1024 records, that was one
        // 508 KB shrink plus five grows back, per append, on the path this buffer exists
        // to keep allocation-free. Holding the high-water mark of the most recent batch
        // keeps a large steady writer stable and still hands back a one-off spike on the
        // next ordinary append.
        //
        // Emptied first: `shrink_to` will not cut capacity below the length still
        // occupying it.
        buf.clear();
        buf.shrink_to(SCRATCH_KEEP.max(written as usize));
        staged.clear();
        staged.shrink_to(SCRATCH_OFFSETS.max(records.len()));
        self.scratch = buf;
        self.staged = staged;
        result
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
        // The replacement is a whole file, so it carries a header like any other — and
        // the frames are built directly onto it, so the offsets are absolute as they are
        // produced and the frames are never staged in a second buffer.
        //
        // This one buffer is *not* the reused `scratch`. It is sized by the whole
        // compacted segment rather than by one batch, and with a log per grain, parking
        // that capacity on every `Wal` for the life of the process would trade a bounded
        // allocation on a cold path for unbounded resident memory.
        self.digest = Digest::WRITES;
        let mut buf = header(self.records, self.digest);
        let mut offsets = Vec::with_capacity(records.len());
        for record in records {
            offsets.push(buf.len() as u64);
            buf = frame_onto(buf, record, self.max_record, self.digest);
        }
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
    fn a_log_at_an_older_digest_still_opens_and_keeps_its_digest() {
        // The reserved field earning its keep. A log stamped kind 1 was written by a build
        // that computed FNV-1a; this build prefers XXH3 and must still read it, because
        // refusing would make an ordinary upgrade unreadable for every Raft log, grain
        // segment, and blob log on a node at once.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");

        // A kind-1 file, framed the way the older build framed it.
        let mut bytes = header(RECORDS.writes(), Digest::Fnv1a);
        for i in 1..=3u64 {
            bytes = frame_onto(bytes, &rec(i, b"old"), MAX, Digest::Fnv1a);
        }
        fs::write(&path, &bytes).unwrap();

        let (mut wal, recovered) = Wal::<Rec>::open(&path, MAX, &RECORDS).expect("kind 1 opens");
        assert_eq!(recovered.len(), 3, "its records replay");
        assert_eq!(wal.digest, Digest::Fnv1a, "and it keeps the digest it was written at");

        // Appending must not produce a file that is half one digest and half the other:
        // the header names one for the whole file, so the append stays at kind 1.
        wal.append(&rec(4, b"new")).unwrap();
        let (_wal, recovered) = Wal::<Rec>::open(&path, MAX, &RECORDS).expect("reopens");
        assert_eq!(recovered.len(), 4, "the appended record replays too");

        // A rewrite replaces the whole file, header included, so that is where a log
        // moves forward.
        let (mut wal, _) = Wal::<Rec>::open(&path, MAX, &RECORDS).unwrap();
        wal.rewrite(&[rec(9, b"compacted")]).unwrap();
        let (wal, recovered) = Wal::<Rec>::open(&path, MAX, &RECORDS).expect("reopens");
        assert_eq!(wal.digest, Digest::WRITES, "a rewrite restamps to this build's digest");
        assert_eq!(recovered, vec![rec(9, b"compacted")]);
    }

    #[test]
    fn the_sidecar_checksum_is_frozen() {
        // Pinned against literals, not against a second implementation, because the point
        // is the *value* — a sidecar carries no header, so a reader can only recompute and
        // compare, and a caller reads a mismatch as "torn or absent". If this ever changes,
        // granary's durable fence silently reads back as *no fence* (G15). The log's frames
        // are free to move digests precisely because their header names which one wrote
        // them; these bytes have nowhere to record that, so they cannot.
        assert_eq!(checksum(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(checksum(&0u64.to_le_bytes()), 0xa8c7_f832_281a_39c5);
        assert_eq!(checksum(b"harness"), 0x211f_cbba_fe53_31ab);
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
        bytes.extend_from_slice(&Digest::WRITES.of(&payload).to_le_bytes());
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
