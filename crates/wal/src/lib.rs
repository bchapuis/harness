//! A generic, framed, checksummed write-ahead log on the local filesystem.
//!
//! A divergence between the write path and the recovery path mis-recovers a node, so
//! framing, checksumming, recovery, and atomic rewrite live here, once.
//!
//! # The log
//!
//! [`Wal<T>`] is a 12-byte header followed by an append-only run of postcard-encoded
//! `T` records, each framed
//! `[u32 little-endian length][postcard payload][u64 little-endian checksum]`
//! and fsynced before the call that wrote it returns. It exposes four operations:
//!
//! - [`append`](Wal::append) / [`append_batch`](Wal::append_batch): frame and fsync
//!   one record, or many in a single write and a single fsync.
//! - [`truncate`](Wal::truncate): drop a conflicting suffix back to the first `keep`
//!   records, a `set_len` at the record's recorded offset.
//! - [`rewrite`](Wal::rewrite): atomically replace the whole file with exactly the
//!   given records (compaction to a retained suffix, or to a single record that
//!   subsumes the prior history).
//!
//! [`open`](Wal::open) scans the file into its records, **truncating any torn tail to
//! disk** before returning: a record whose write never completed was never
//! acknowledged, so dropping it is correct. Whatever higher-level recovery runs on top
//! operates on the returned records.
//!
//! The scan reads the whole file and verifies every frame, rather than trusting a
//! stored checkpoint offset and starting from there. That is a deliberate purchase of
//! CPU and sequential I/O (both abundant) to delete a piece of state that could be
//! wrong: at contemporary read and digest throughput a gigabyte of log opens in a
//! fraction of a second, and a checkpoint pointer is one more thing a crash can leave
//! lying (`docs/hardware-envelope.md` §3.3).
//!
//! # The header
//!
//! The header carries a magic, the **frame layout's** version (this crate's own), and
//! a **record-schema** version that belongs to the caller and that this crate admits
//! against the caller's [`compat::Format`] and never interprets. That last field is
//! the load-bearing one: a `Wal<T>`'s records are postcard, which is positional and
//! has no field names, so `T` cannot gain a field and its version has nowhere to
//! live except outside the payload.
//!
//! [`open`](Wal::open) therefore takes the caller's format and does the refusing
//! itself, rather than exposing the header for a caller to check.
//!
//! # Sidecars
//!
//! Some durable state is not a log but a single small file rewritten in place (a
//! generation counter, a small piece of metadata, a checkpoint pointer). For those,
//! [`atomic_replace`] writes `tmp → fsync → rename → fsync dir` so a reader sees
//! either the old file or the whole new one, never a torn mix. [`Sidecar`] is the
//! stamped, checksummed `u64` sidecar every such file in the tree is (a fence term,
//! a seal bound), written and read here once; [`checksum`] and
//! [`sync_dir`] are exposed for framing one's own sidecar bytes.
//!
//! # Failure policy
//!
//! Every method returns [`io::Result`]: this crate does not decide what an I/O
//! failure *means*. The caller does. Code that cannot persist a record it must not
//! lose cannot safely acknowledge anything after it, so both callers in this tree
//! poison themselves: they record the cause and refuse every later write, rather
//! than unwinding a task and leaving the node up with nothing behind it.
//!
//! [`open`](Wal::open) has a second failure that is not an I/O event at all: a file
//! this build cannot read. The bytes are intact and the disk is fine; the refusal is
//! policy. It surfaces as [`io::ErrorKind::InvalidData`] whose message names the
//! format and the version found (compatibility §2), so a caller that logs it says
//! which file and which build disagree.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// The digest closing every frame and covering every sidecar value: XXH3-64.
///
/// Detects torn and partial writes, not adversarial tampering, which is all a local
/// log needs. Exposed so a caller can frame its own sidecar bytes. About 17.5 GB/s on a
/// 4 KiB frame, so a large log's recovery is read-bound rather than digest-bound.
pub fn checksum(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(bytes)
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

/// Distinguishes one in-flight [`atomic_replace`] from every other one; see
/// [`temp_name`]. Process-wide rather than per-directory: a counter is cheaper than a
/// map keyed by path, and it only has to be unique, not dense.
static REPLACE_SEQ: AtomicU64 = AtomicU64::new(0);

/// The scratch name [`atomic_replace`] writes before renaming: `<name>.<pid>.<n>.tmp`.
///
/// Unique per call. A fixed `<name>.tmp` is safe only while one caller at a time
/// replaces a given name, and that is not a property this crate can promise on the
/// caller's behalf: entity's blob store puts concurrently through a thread pool, and
/// content addressing means two puts landing on *the same* name is the normal case,
/// not a rare one; an image full of identical blocks hashes every one of them to a
/// single id. Two calls sharing a scratch path corrupt each other in both
/// directions: the second `File::create` truncates the first's
/// half-written bytes, and whichever renames second finds the path already gone and
/// fails with `ENOENT`. Callers that treat an I/O error as unrecoverable (the entity
/// store poisons itself) turn that into a node that has stopped accepting writes.
///
/// The pid is in the name because uniqueness has to hold across processes too: two
/// nodes sharing a data directory is a misconfiguration, but it should not be one that
/// silently tears a file.
///
/// A crash between create and rename leaves the scratch file behind, where the old
/// fixed name would have been reused by the next call. Every directory this crate's
/// callers scan already skips names it cannot decode (a blob id, a shard number), so
/// a stray is inert. It is also indistinguishable from the live scratch file of a call
/// still in flight, so a caller removes strays only with the whole directory (entity's
/// blob area goes when the entity is destroyed or its range reclaimed), never by name
/// while another call may be replacing into it: that would fail the call's rename.
fn temp_name(name: &str) -> String {
    let seq = REPLACE_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{name}.{}.{seq}.tmp", std::process::id())
}

/// Atomically replace `dir/<name>` with `bytes`: write a scratch file → fsync →
/// rename → fsync dir, so a reader sees either the old file or the whole new one. The
/// caller supplies already-serialized bytes, so the encoding (JSON, postcard,
/// fixed-width) stays its choice.
///
/// Safe to call concurrently on one `name`, including from several threads and
/// processes: each call writes its own scratch file ([`temp_name`]) and the rename
/// decides the winner. Concurrent callers writing *different* bytes still race for
/// which content survives (this replaces atomically, it does not serialize), but the
/// file that lands is always exactly one caller's whole bytes.
///
/// The scratch file lives in `dir` until the rename, so a caller that deletes names in
/// `dir` it does not recognize MUST NOT do so while a call may be in flight there:
/// deleting a live scratch file fails that call with `ENOENT` (see [`temp_name`]).
pub fn atomic_replace(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let tmp_path = dir.join(temp_name(name));
    let final_path = dir.join(name);
    let mut tmp = File::create(&tmp_path)?;
    tmp.write_all(bytes)?;
    tmp.sync_all()?;
    // A failed rename leaves the scratch file behind; remove it rather than let an
    // error path accumulate them, since nothing else here will revisit this name.
    if let Err(err) = fs::rename(&tmp_path, &final_path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(err);
    }
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

/// A stamped **sidecar**: one `u64` written whole to a small file of its own (§5),
/// the shape of a fence term or a seal bound.
///
/// The body behind the stamp is positional `postcard`: a [`checksum`] over the value,
/// then the value, so a torn or bit-rotted value cannot pass unnoticed. The stamp's
/// version is admitted before any field is read (compatibility **V2**).
///
/// `None` means one thing only: **the file is absent**. A short file, a long file,
/// or a checksum mismatch is an `InvalidData` refusal naming the format and the
/// path, never an absence, because a durable fence coming back as *no fence* is a
/// safety property lost with no diagnostic anywhere.
pub struct Sidecar {
    stamp: compat::Stamp,
}

/// A sidecar's body, behind the stamp.
#[derive(Serialize, Deserialize)]
struct SidecarBody {
    checksum: u64,
    value: u64,
}

impl Sidecar {
    /// A sidecar format under `stamp`.
    pub const fn new(stamp: compat::Stamp) -> Sidecar {
        Sidecar { stamp }
    }

    /// The stamp on this sidecar's files.
    pub const fn stamp(&self) -> &compat::Stamp {
        &self.stamp
    }

    /// `value`'s bytes, stamped at this build's version.
    pub fn encode(&self, value: u64) -> Vec<u8> {
        let body = postcard::to_allocvec(&SidecarBody {
            checksum: checksum(&value.to_le_bytes()),
            value,
        })
        .expect("a sidecar body is plain owned data");
        self.stamp.stamp(&body)
    }

    /// Atomically persist `value` at `path` ([`atomic_replace`]).
    ///
    /// # Errors
    ///
    /// Any filesystem error, or `InvalidData` for a `path` with no file name.
    pub fn write(&self, path: &Path, value: u64) -> io::Result<()> {
        let name = path.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("a sidecar path names no file: {}", path.display()),
            )
        })?;
        let name = name.to_string_lossy();
        atomic_replace(parent_dir(path), &name, &self.encode(value))
    }

    /// The value the file at `path` carries, or `None` if the file is absent.
    ///
    /// # Errors
    ///
    /// Any filesystem error other than absence; or `InvalidData` for bytes that
    /// are not a stamped record this build accepts, or whose checksum does not
    /// cover their value.
    pub fn read(&self, path: &Path) -> io::Result<Option<u64>> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };
        self.decode(path, &bytes).map(Some)
    }

    fn decode(&self, path: &Path, bytes: &[u8]) -> io::Result<u64> {
        let named = |msg: String| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} at {}: {msg}",
                    self.stamp.format().name(),
                    path.display()
                ),
            )
        };
        let body = self
            .stamp
            .unstamp(bytes)
            .map_err(|err| named(err.to_string()))?;
        let (body, rest): (SidecarBody, &[u8]) = postcard::take_from_bytes(body)
            .map_err(|err| named(format!("body did not decode: {err}")))?;
        if !rest.is_empty() {
            return Err(named(format!("body has {} trailing bytes", rest.len())));
        }
        if body.checksum != checksum(&body.value.to_le_bytes()) {
            return Err(named("checksum mismatch".to_string()));
        }
        Ok(body.value)
    }
}

/// The framing capacity a log keeps between appends, when its last batch wanted less.
///
/// Reusing one buffer is what makes an append allocation-free, but the capacity it reaches
/// is the largest batch that log has ever written. A caller holding one log per object
/// (entity keeps one per entity) would otherwise pay for every log's worst batch, forever.
/// A floor rather than a ceiling: [`append_batch`](Wal::append_batch) keeps whichever is
/// larger of this and the batch it just wrote, so a steady large writer is not punished
/// and a one-off spike is still handed back.
const SCRATCH_KEEP: usize = 16 * 1024;

/// The offset capacity a log keeps between appends, in offsets: the [`SCRATCH_KEEP`]
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
/// Width of the little-endian checksum that closes every frame.
const CHECKSUM_BYTES: usize = size_of::<u64>();

/// The magic that opens every log file, in the PNG convention: a high-bit byte, the
/// name, then a CRLF/EOF trap that catches a text-mode transfer.
///
/// Its first four bytes read as a little-endian `u32` are `0x4C41_5789`, which
/// exceeds every `max_record` this crate permits ([`Wal::open`] asserts it). A
/// headerless file therefore can never begin with this magic (its first bytes are a
/// frame length at or below `max_record`), so an unstamped file is refused by name
/// instead of scanned for frames that happen to parse.
const MAGIC: &[u8] = b"\x89WAL\r\n\x1a\n";

/// The frame layout's version (compatibility spec §3). Version 1 is the
/// `[len][payload][checksum]` framing of §2.
const FRAME: compat::Stamp = compat::Stamp::new(MAGIC, compat::Format::new("wal.frame", 1));

/// The file header's width: the frames begin after it (§2.1). Public because a
/// caller that edits a log's bytes (a crash-point test, a corruption fixture)
/// needs the offset the framing starts at, and a second copy of it would not
/// move when the header does.
///
/// ```text
/// [magic 8][frame version u16][record schema u16]
/// ```
///
/// The header has no checksum of its own. Every field is validated against a known
/// value, so detectable damage is refused.
pub const HEADER_LEN: usize = 12;

/// Build the header a new log opens with. `records` is the caller's schema
/// version: this crate stores and returns it without interpreting it (§2).
fn header(records: compat::Version) -> Vec<u8> {
    let bytes = FRAME.stamp(&records.0.to_le_bytes());
    debug_assert_eq!(bytes.len(), HEADER_LEN, "the header is a fixed width");
    bytes
}

/// Admit a log's header, returning the frame bytes that follow it.
///
/// The order is the contract: the magic, then the frame version, then the caller's
/// record schema. Nothing is scanned until all three are accepted (compatibility
/// **V2**), so a file this build cannot read is never partially interpreted.
fn admit_header<'a>(
    bytes: &'a [u8],
    records: &compat::Format,
) -> Result<&'a [u8], compat::Incompatible> {
    let tail = FRAME.unstamp(bytes)?;
    let Some(schema) = tail.get(..size_of::<u16>()) else {
        // A header cut short. Unreachable through `open`, which initializes a file
        // too short to hold one, but the check keeps this function sound on its own.
        return Err(compat::Incompatible::Unstamped {
            format: FRAME.format().name(),
            expected: FRAME.format().version(),
        });
    };
    let schema = u16::from_le_bytes(schema.try_into().expect("a two-byte slice"));
    records.admit(compat::Version(schema))?;
    Ok(&bytes[HEADER_LEN..])
}

/// A policy refusal as an I/O error: `InvalidData`, because the bytes are not what
/// this build reads, with the refusal's own message naming the format and the
/// version it found.
fn refused(err: compat::Incompatible) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

/// Frame one record (`[u32 len][postcard payload][u64 checksum]`, all little-endian)
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
/// and silently lost on the next open. The half-framed record is discarded with `buf`,
/// which the panic unwinds past before any of it reaches the file.
fn frame_onto<T: Serialize>(mut buf: Vec<u8>, value: &T, max_record: u32) -> Vec<u8> {
    let frame = buf.len();
    buf.extend_from_slice(&0u32.to_le_bytes()); // back-patched below
    let mut buf = postcard::to_extend(value, buf).expect("a WAL record always serializes");
    let payload = buf.len() - frame - LEN_BYTES;
    assert!(
        payload as u64 <= u64::from(max_record),
        "WAL record of {payload} bytes exceeds the {max_record}-byte limit; recovery \
         would discard it, so it must not be written",
    );
    let check = checksum(&buf[frame + LEN_BYTES..]);
    buf[frame..frame + LEN_BYTES].copy_from_slice(&(payload as u32).to_le_bytes());
    buf.extend_from_slice(&check.to_le_bytes());
    buf
}

/// Scan a framed log's frame bytes into `(records, per-record start offsets, valid
/// length)`. The scan stops at the first incomplete, oversized, checksum-failing, or
/// unparsable record, the recovery rule: the valid prefix is the log; the tail was
/// never acknowledged.
///
/// `bytes` is the frame region alone (the header is already admitted); `base` is
/// where that region starts in the file, so the returned offsets are absolute and
/// [`Wal::truncate`] stays a single `set_len`. The returned length is relative to
/// `base`.
fn scan<T: DeserializeOwned>(bytes: &[u8], max_record: u32, base: u64) -> (Vec<T>, Vec<u64>, u64) {
    // Size both vectors from the first frame's width. A log holds one record type, so
    // that width estimates the rest well, and recovery is where it matters: growing from
    // zero across a file of millions of frames recopies the vector on every doubling.
    // Capped because the width is only a guess: a first record narrower than the rest
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
    /// caller holds: what makes [`truncate`](Wal::truncate) a single `set_len`.
    offsets: Vec<u64>,
    /// The file's current (valid) length, where the next frame lands.
    end: u64,
    /// The buffer [`append_batch`](Wal::append_batch) frames into, kept across calls so a
    /// steady-state append allocates nothing: it reaches the high-water mark of the
    /// batches this log sees and stays there. Empty between calls: it holds no state,
    /// only capacity.
    scratch: Vec<u8>,
    /// The offsets of the batch being framed, staged here until the write succeeds and
    /// they can join `offsets`. Reused like `scratch`, and for the same reason.
    ///
    /// Staged rather than pushed straight onto `offsets` because framing can panic (a
    /// record above `max_record` is refused that way), and an unwind must not leave
    /// `offsets` naming frames that were never written. Both buffers are locals for the
    /// duration of a call, so a panic drops them and leaves the log's own state untouched.
    staged: Vec<u64>,
    /// Upper bound on one frame's payload. Enforced on every write (a larger record is
    /// rejected loudly) and on recovery (a larger length is treated as corruption), so
    /// the write path and the scan path agree on what is a valid record.
    max_record: u32,
    /// The caller's record-schema version, held so [`rewrite`](Wal::rewrite) can
    /// stamp the replacement it produces. This crate never interprets it.
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
    /// survives a crash: the caller need not (and should not) repeat it. The caller is
    /// still responsible for any directory *it* created to hold the log.
    ///
    /// # Errors
    ///
    /// Any filesystem error reading, opening, or truncating the file; or
    /// `InvalidData` for a file this build cannot read (a foreign file, a frame
    /// layout from another version, or a record schema other than the caller's),
    /// its message naming the format and the version found.
    pub fn open(
        path: impl Into<PathBuf>,
        max_record: u32,
        records: &compat::Format,
    ) -> io::Result<(Wal<T>, Vec<T>)> {
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
            Err(err) => return Err(err),
        };
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        let version = records.version();

        let (recovered, offsets, end) = if bytes.len() < HEADER_LEN {
            // A file whose bytes are a *prefix* of the header we would write holds no
            // frames (the header is written and fsynced before any append can follow
            // it), so stamping it in place is lossless. This covers a missing file, an
            // empty one, and a header torn by a crash between creating the file and
            // syncing it, which would otherwise turn a benign crash into a log that
            // can never be opened again.
            //
            // The prefix test bounds that: a short file that is *not* heading toward
            // this header is refused instead of being silently truncated.
            let fresh = header(version);
            if !fresh.starts_with(&bytes) {
                return Err(refused(compat::Incompatible::Unstamped {
                    format: FRAME.format().name(),
                    expected: FRAME.format().version(),
                }));
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
            (Vec::new(), Vec::new(), HEADER_LEN as u64)
        } else {
            // The header is admitted whole before a single frame is scanned (**V2**).
            let frames = admit_header(&bytes, records).map_err(refused)?;
            let (recovered, offsets, valid_body) = scan::<T>(frames, max_record, HEADER_LEN as u64);
            let valid_end = HEADER_LEN as u64 + valid_body;
            if (valid_end as usize) < bytes.len() {
                // A torn tail: the write never returned, so the record was never
                // acknowledged. Truncate it away before anything is appended.
                file.set_len(valid_end)?;
                file.sync_all()?;
            }
            (recovered, offsets, valid_end)
        };
        Ok((
            Wal {
                path,
                file,
                offsets,
                end,
                scratch: Vec::new(),
                staged: Vec::new(),
                max_record,
                records: version,
                _marker: PhantomData,
            },
            recovered,
        ))
    }

    /// The number of records in the log.
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Whether the log has no records.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Frame `record`, append it, and fsync: durable before the call returns. The
    /// single-record case of [`append_batch`](Wal::append_batch).
    ///
    /// # Errors
    ///
    /// Any filesystem error writing or syncing.
    pub fn append(&mut self, record: &T) -> io::Result<()> {
        self.append_batch(std::slice::from_ref(record))
    }

    /// Frame `records` into one buffer, append them, and fsync once: the batch append
    /// (its latency is one fsync, not one per record). A no-op for an empty slice.
    ///
    /// The fsync is `sync_all`, not `sync_data`. On Linux the two are fsync and fdatasync
    /// and the latter is the tighter fit, but they do not mean the same thing everywhere:
    /// on macOS only `sync_all` is `F_FULLFSYNC`, which flushes the drive's own write
    /// cache. Taking the cheaper one would make "durable before the call returns" weaker
    /// on the platform the crash tests run on, and it measured no faster, which is what
    /// a device with power-loss protection predicts, since there the cache flush the
    /// stronger call adds is close to free (`docs/hardware-envelope.md` §2).
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
            buf = frame_onto(buf, record, self.max_record);
        }
        let written = buf.len() as u64;
        let result = self
            .file
            .write_all(&buf)
            .and_then(|()| self.file.sync_all());
        // Both buffers go back whether or not the write succeeded: they are capacity,
        // not state, and a failed append should not cost the next one its allocation. The
        // offsets they staged are published only on success, so a failed write leaves the
        // log describing exactly the frames that were already there.
        if result.is_ok() {
            self.offsets.extend_from_slice(&staged);
            self.end += written;
        }
        // Release capacity an outlier left behind, but never below what this call itself
        // needed. A flat cap would shrink and regrow on every append for any log whose
        // *steady* batch is larger than it: measured at 1024 records, that was one
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

    /// Drop everything past the first `keep` records: a `set_len` at record `keep`'s
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
        // The replacement is a whole file, so it carries a header like any other, and
        // the frames are built directly onto it, so the offsets are absolute as they are
        // produced and the frames are never staged in a second buffer.
        //
        // This one buffer is *not* the reused `scratch`. It is sized by the whole
        // compacted segment rather than by one batch, and with a log per entity, parking
        // that capacity on every `Wal` for the life of the process would trade a bounded
        // allocation on a cold path for unbounded resident memory.
        let mut buf = header(self.records);
        let mut offsets = Vec::with_capacity(records.len());
        for record in records {
            offsets.push(buf.len() as u64);
            buf = frame_onto(buf, record, self.max_record);
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

    const RECORDS: compat::Format = compat::Format::new("wal.test", 1);

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

    /// A sidecar file longer than the body this build writes is a refusal, not a
    /// value read out of its prefix (§5 rule 4). `postcard` stops at the end of the
    /// fields it wanted and says nothing about what follows, so a decoder that does
    /// not check the remainder reads a foreign or half-rewritten file as a good one.
    #[test]
    fn a_sidecar_file_with_trailing_bytes_is_refused() {
        const SIDECAR: Sidecar = Sidecar::new(compat::Stamp::new(
            b"WLSIDE",
            compat::Format::new("test.sidecar", 1),
        ));

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fence");
        let mut bytes = SIDECAR.encode(7);
        assert_eq!(SIDECAR.decode(&path, &bytes).unwrap(), 7);
        bytes.extend_from_slice(b"xyz");

        let err = SIDECAR
            .decode(&path, &bytes)
            .expect_err("a long file must not decode");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let message = err.to_string();
        assert!(
            message.contains("test.sidecar") && message.contains("fence"),
            "the refusal must name the format and the path: {message}",
        );
        assert!(
            message.contains("3 trailing bytes"),
            "the refusal must say what is wrong: {message}",
        );
    }

    /// Many threads replacing *one* name at once: every call succeeds, and the file
    /// left behind is one writer's whole bytes rather than a mix of several.
    ///
    /// This is entity's blob store put path, which is content-addressed and runs
    /// through a thread pool: identical content is identical id is one name, so an
    /// image whose blocks repeat sends a wave of concurrent replaces at a single
    /// path. With one shared scratch name they truncated each other's bytes and all
    /// but the first rename failed with `ENOENT`, and a store that treats a failed
    /// write as unrecoverable poisons itself on the spot, which is a live node
    /// refusing writes for the rest of its life over an image full of zeros.
    ///
    /// The bytes are long enough that a torn write is visible: a few bytes land in
    /// one `write_all` whatever the interleaving, so a short payload would pass on a
    /// broken implementation.
    #[test]
    fn concurrent_replaces_of_one_name_all_succeed_and_none_tears() {
        const WRITERS: u8 = 16;
        let dir = tempfile::tempdir().unwrap();
        let payloads: Vec<Vec<u8>> = (0..WRITERS).map(|w| vec![w; 256 * 1024]).collect();

        std::thread::scope(|scope| {
            for bytes in &payloads {
                scope.spawn(|| atomic_replace(dir.path(), "state", bytes).expect("replace"));
            }
        });

        let landed = fs::read(dir.path().join("state")).unwrap();
        assert!(
            payloads.contains(&landed),
            "the file that landed is not any single writer's bytes: {} bytes, first {:?}",
            landed.len(),
            landed.first(),
        );

        // Every scratch file was renamed away, so nothing is left but the sidecar.
        let strays: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name())
            .filter(|n| n != "state")
            .collect();
        assert!(strays.is_empty(), "scratch files left behind: {strays:?}");
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
        // valid-looking frame. It must be refused, not scanned: the frames may parse
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
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("not a stamped wal.frame record"),
            "expected an Unstamped refusal naming the format, got {err}"
        );
    }

    #[test]
    fn a_record_schema_other_than_the_callers_is_refused_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        {
            let (mut wal, _) = open(&path);
            wal.append(&rec(1, b"a")).unwrap();
        }
        // Reopen under a caller whose records have moved on. The frames are intact and
        // this build simply cannot interpret them.
        const LATER: compat::Format = compat::Format::new("wal.test", 4);
        let err = Wal::<Rec>::open(&path, MAX, &LATER)
            .err()
            .expect("another schema version must not open");
        let msg = err.to_string();
        assert!(
            msg.contains("wal.test") && msg.contains("v1") && msg.contains("v4"),
            "the refusal must name the format and both versions: {msg}"
        );
    }

    #[test]
    fn a_header_lost_to_a_crash_before_the_first_append_is_reinitialized() {
        // A crash between creating the file and stamping it. The file holds no frames,
        // so re-stamping is lossless, and the alternative would turn a benign crash
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
        // `rewrite` replaces the whole file, so it must write a header too: otherwise
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

    /// The one crash point the replace discipline leaves visible: a crash between
    /// creating the scratch file and renaming it (§5 rule 1). The scratch file is
    /// a whole or partial *replacement* that never landed, so the log a reopen
    /// finds is the whole prior file, and the stray is left where it is rather
    /// than adopted or removed (a caller that enumerates the directory skips it).
    #[test]
    fn a_scratch_file_left_by_a_crash_mid_rewrite_leaves_the_prior_log_whole() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        {
            let (mut wal, _) = open(&path);
            wal.append_batch(&[rec(1, b"a"), rec(2, b"b")]).unwrap();
        }
        let prior = fs::read(&path).unwrap();

        // Stage what a `rewrite(&[rec(9, b"z")])` had written when the crash hit:
        // a fully framed replacement under the scratch name, never renamed.
        let mut replacement = header(RECORDS.version());
        replacement = frame_onto(replacement, &rec(9, b"z"), MAX);
        let stray = dir.path().join(temp_name("log"));
        fs::write(&stray, &replacement).unwrap();

        let (_wal, recovered) = open(&path);
        assert_eq!(
            recovered,
            vec![rec(1, b"a"), rec(2, b"b")],
            "the prior file is the log; the replacement that never landed is not"
        );
        assert_eq!(
            fs::read(&path).unwrap(),
            prior,
            "the prior file is untouched"
        );
        assert_eq!(
            fs::read(&stray).unwrap(),
            replacement,
            "the stray is neither adopted nor removed"
        );
    }

    #[test]
    #[should_panic(expected = "max_record must stay below the magic")]
    fn a_max_record_that_could_alias_the_magic_is_rejected() {
        // What keeps the unstamped-file refusal sound (see `MAGIC`).
        let dir = tempfile::tempdir().unwrap();
        let _ = Wal::<Rec>::open(dir.path().join("log"), 0x4C41_5789, &RECORDS);
    }
}
