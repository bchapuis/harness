//! A file-backed, **per-grain segmented** [`GrainStore`] (spec §7.2, §7.4, §9): a
//! node's grain records on the local filesystem, durable across a process restart.
//!
//! A grain's records live **off** the leader-election group's Raft log, in each
//! replica's [`GrainStore`] (§7.2), so surviving a full-cluster cold restart needs the
//! store itself to be durable. Injected through
//! [`GranaryConfig::grain_store`](crate::GranaryConfig); the default
//! [`MemoryGrainStore`](crate::store::MemoryGrainStore) is lost on restart, while this
//! one reloads each node's records and re-establishes the per-shard fence, so a
//! re-elected leader recovers every grain's committed head from a quorum of the
//! reloaded stores (§8, **G14**).
//!
//! Both the segments and the manifest are framed, checksummed append-only logs; the
//! framing, torn-tail recovery, and atomic rewrite live in the shared [`wal`] crate,
//! so this module is just the grain store's layout and policy over it.
//!
//! **Layout.** A node's store is a directory holding:
//!
//! - `LOCK` — the single-writer guard (see below), taken before anything is read.
//! - `segments/<id>` — one **per-grain op log** ([`wal::Wal`] of [`SegOp`]). Each grain
//!   is an independent segment: its mutating calls
//!   ([`store_record`](GrainStore::store_record),
//!   [`store_snapshot`](GrainStore::store_snapshot), [`truncate`](GrainStore::truncate))
//!   are appended and fsynced before the call returns. Because segments are per grain,
//!   one grain's snapshot compaction rewrites only that grain's file, never the whole
//!   node's store. The cost: a grain owns a file, so a node's file count tracks its
//!   grain count, and no two grains' appends can share an fsync.
//! - `manifest` — an append-only map from `(shard, GrainName)` to a small integer
//!   segment **id**, so segment filenames are collision-free whatever a grain's key
//!   contains. A grain's segment is opened and replayed **lazily**, on first access,
//!   so a node holding millions of grains does not scan them all at startup — though
//!   the manifest itself is replayed and held whole, and only ever grows: an id
//!   assignment outlives the grain it names, which is why *presence* is the files',
//!   not the manifest's, to answer ([`grains`](GrainStore::grains)).
//! - `fences/<shard>` — the per-shard **fence**: the highest shard term this node has
//!   acknowledged (§8), the one piece of state shared across a shard's grains. It is
//!   rewritten (atomically) only when the term advances — on failover and recovery
//!   `prepare`, never on a steady-state append — and loaded eagerly on open, so the
//!   fence is known before any grain's records load lazily.
//! - `seals/<shard>` — the per-shard **append bound** (§7.7): refuse appends at or
//!   above this name hash. Durable and loaded eagerly for the same reason as the
//!   fence — a restart that forgot the bound could let a stale leader assemble a
//!   majority for a range a split moved away (**G15**).
//! - `blobs/<id>/<blob hex>` — a grain's content-addressed blob area (§7.10), one
//!   file per blob under the same collision-free id the manifest assigns the grain.
//!
//! **Snapshot-driven compaction (§9).** When a stored snapshot advances a grain's
//! compacted base (dropping the records it subsumes), that grain's segment is rewritten
//! to a single `Checkpoint` op holding the segment's current state, which already
//! embeds the snapshot. A snapshot that does *not* advance the base (a redundant store,
//! e.g. a re-activation re-caching the recovered snapshot) writes nothing durable, so
//! repeated activations never bloat the segment. The rewrite is atomic, so a crash
//! leaves either the old segment (replays to the same state) or the new checkpoint
//! (loads the same state).
//!
//! **Recovery.** Each log is recovered by [`wal::Wal::open`]: the first incomplete or
//! checksum-failing record ends the valid prefix and the torn tail is truncated away (a
//! record whose write never returned was never acknowledged). A segment replays
//! deterministically: a `Checkpoint` loads the whole segment state, every other op is
//! re-applied in log order.
//!
//! **Failure policy.** [`GrainStore`]'s methods are infallible by signature; a replica
//! that cannot make a write durable cannot safely acknowledge it. Like
//! [`FileRaftWAL`](actor_runtime), this panics on an I/O error after open rather than
//! risk announcing un-persisted state; peers observe the node unreachable.
//!
//! **Single writer.** A node's directory belongs to one store at a time, enforced by
//! an advisory `flock` on `LOCK` held for the store's lifetime — so a second
//! [`open`](FileGrainStore::open) of the same directory fails by name instead of
//! quietly interleaving appends into the first's segments. Within a process the
//! [`factory`](FileGrainStore::factory) caches per node, so repeated hostings share
//! one instance and never contend for it. Each grain's mutations serialize on its own
//! segment lock, so different grains persist concurrently; the shared fence sits
//! behind its own short leaf lock.
//!
//! **Durability reporting.** Every mutating method returns a
//! [`Reserved`](crate::store::Reserved): the outcome is settled under the grain's
//! segment lock before the call returns, and its stability is awaited separately.
//! This store fsyncs inside the call, so every `Reserved` it hands back is already
//! ready.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use actor_core::NodeId;
use serde::Deserialize;
use serde::Serialize;
use wal::Wal;

use crate::blobs::BlobId;
use crate::grain::GrainName;
use crate::journal::Seq;
use crate::journal::Term;
use crate::store::GrainBlobStore;
use crate::store::GrainCheckpoint;
use crate::store::GrainRecords;
use crate::store::GrainStore;
use crate::store::GrainStoreFactory;
use crate::store::ReadOutcome;
use crate::store::ReadReply;
use crate::store::Reserved;
use crate::store::StoreAck;
use crate::store::WriteGuard;
use crate::store::WriteKind;

/// Upper bound on one framed record's payload, a sanity check while scanning: a
/// length above this is treated as corruption, not an allocation. Generous, since a
/// grain's record bytes and a whole-segment `Checkpoint` record can be large.
const MAX_RECORD: u32 = 1 << 30;

/// The schema revisions of this store's two log record types, stamped into each log's
/// header (compatibility spec §3). They are separate boundaries because the two logs
/// evolve independently: adding a field to [`SegOp`] says nothing about
/// [`ManifestEntry`].
///
/// Both are `postcard`, so neither type can gain or reorder a field within a revision.
/// An enum may still grow variants at its **end**, which `postcard` encodes as a higher
/// discriminant and which no existing record can carry; that needs no bump.
const MANIFEST_RECORDS: compat::Window = compat::Window::at("granary.store.manifest", 1);
const SEGMENT_RECORDS: compat::Window = compat::Window::at("granary.store.segment", 1);

/// One mutating call on a grain's segment, as logged and replayed. Replaying a
/// segment's ops through a fresh [`GrainRecords`] reproduces its state exactly (the
/// methods are deterministic in prior state), so a reloaded segment equals the live one.
#[derive(Serialize, Deserialize)]
enum SegOp {
    /// The segment's whole state, written as the sole record when compaction rewrites
    /// it (§9). Replaying it replaces the segment's contents.
    Checkpoint(GrainCheckpoint),
    Record {
        after: Seq,
        term: Term,
        records: Vec<Vec<u8>>,
        kind: WriteKind,
    },
    Snapshot {
        at: Seq,
        term: Term,
        state: Vec<u8>,
    },
    Truncate {
        after: Seq,
        term: Term,
    },
}

/// One manifest entry: the segment id assigned to a `(shard, grain)`. Replaying the
/// manifest rebuilds the id map and the next free id.
#[derive(Serialize, Deserialize)]
struct ManifestEntry {
    shard: u32,
    grain: GrainName,
    id: u64,
}

/// One grain's segment: its in-memory records and its append log, behind one lock so
/// the durable append and the in-memory update stay atomic against concurrent callers
/// for *this* grain. Different grains hold different segment locks. `path` is kept for
/// the failure messages.
struct Segment {
    path: PathBuf,
    inner: Mutex<SegmentInner>,
}

struct SegmentInner {
    records: GrainRecords,
    log: Wal<SegOp>,
}

/// The manifest: the `(shard, grain) → id` map and the append log that persists new
/// assignments. `path` is kept for the failure messages.
struct Manifest {
    path: PathBuf,
    log: Wal<ManifestEntry>,
    ids: HashMap<(u32, GrainName), u64>,
    next: u64,
}

/// The production file-backed [`GrainStore`] (spec §7.2, §7.4), segmented per grain.
/// See the module docs for the layout, recovery, and failure policy.
pub struct FileGrainStore {
    dir: PathBuf,
    /// The single-writer guard (see the module docs), held open for this store's
    /// lifetime because the lock lasts exactly as long as the open file does.
    /// `None` where the platform has no advisory lock; the invariant is then
    /// documented rather than enforced.
    _lock: Option<fs::File>,
    /// The per-shard fence (§8), mirrored from `fences/<shard>`; its own leaf lock.
    fences: Mutex<HashMap<u32, Term>>,
    /// The per-shard append bound (§7.7), mirrored from `seals/<shard>`: refuse
    /// appends at or above this name hash. Durable like the fence (G15).
    seals: Mutex<HashMap<u32, u64>>,
    /// Loaded grain segments, keyed `(shard, grain)`. Populated lazily on first access.
    segments: Mutex<HashMap<(u32, GrainName), Arc<Segment>>>,
    manifest: Mutex<Manifest>,
}

impl FileGrainStore {
    /// Open (creating if needed) a node's store directory: load the per-shard fences
    /// and the segment manifest, truncating any torn tail. Grain segments load lazily.
    ///
    /// # Errors
    ///
    /// Any filesystem error opening the directory or its index files.
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<FileGrainStore> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        // Before anything is read (see `acquire_lock`).
        let lock = acquire_lock(&dir)?;
        fs::create_dir_all(dir.join("segments"))?;
        fs::create_dir_all(dir.join("fences"))?;
        fs::create_dir_all(dir.join("seals"))?;
        fs::create_dir_all(dir.join("blobs"))?;

        let fences = load_fences(&dir)?;
        let seals = load_seals(&dir)?;
        let manifest = load_manifest(&dir)?;
        // The files inside these subdirectories make their own entries durable
        // (`Wal::open`, `atomic_replace`); the subdirectories are this store's to fsync.
        wal::sync_dir(&dir)?;

        Ok(FileGrainStore {
            dir,
            _lock: lock,
            fences: Mutex::new(fences),
            seals: Mutex::new(seals),
            segments: Mutex::new(HashMap::new()),
            manifest: Mutex::new(manifest),
        })
    }

    /// A [`GrainStoreFactory`] rooted at `root`: each node's records live in its own
    /// `root/<node>/` directory. Caches per node so repeated hostings in one process
    /// share a single instance (single writer); a restart constructs a fresh factory
    /// and reopens from disk. Panics if a node's store cannot be opened — a replica
    /// without durable storage must not start (spec §7.4).
    pub fn factory(root: impl Into<PathBuf>) -> GrainStoreFactory {
        let root = root.into();
        let cache: Arc<Mutex<HashMap<NodeId, Arc<FileGrainStore>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        Arc::new(move |node: NodeId| {
            let mut cache = cache.lock().expect("grain store cache poisoned");
            let store = cache
                .entry(node)
                .or_insert_with(|| {
                    let dir = root.join(node.to_string());
                    Arc::new(FileGrainStore::open(&dir).unwrap_or_else(|err| {
                        panic!("cannot open grain store at {}: {err}", dir.display())
                    }))
                })
                .clone();
            store as Arc<dyn GrainStore>
        })
    }

    /// The loaded segment for `(shard, grain)`, opening and replaying it from disk on
    /// first access, allocating a new one if the grain is unknown. Holds the segment
    /// registry lock across the (one-time) load so a grain is never opened twice. When
    /// `create` is false the read path gets `None` for a grain it has not seen, rather
    /// than allocating a segment for it.
    fn segment(&self, shard: u32, grain: &GrainName, create: bool) -> Option<Arc<Segment>> {
        let mut segments = self.segments.lock().expect("grain store segments poisoned");
        if let Some(segment) = segments.get(&(shard, grain.clone())) {
            return Some(Arc::clone(segment));
        }
        let id = self.segment_id(shard, grain, create)?;
        // An id is not evidence the grain still exists: the manifest is append-only, so
        // `remove_grain` leaves the assignment behind, while `open_segment` would
        // *create* the segment file it opens. Without this check a plain `read` of a
        // removed grain resurrects it on disk — the reply is still empty, but the grain
        // reappears in `grains` and its file leaks.
        if !create && !self.segment_path(id).exists() {
            return None;
        }
        let segment = Arc::new(open_segment(&self.dir, id));
        segments.insert((shard, grain.clone()), Arc::clone(&segment));
        Some(segment)
    }

    /// The on-disk path of one grain's segment log, `segments/<segment id>`.
    fn segment_path(&self, seg_id: u64) -> PathBuf {
        self.dir.join("segments").join(seg_id.to_string())
    }

    /// The loaded segment for `(shard, grain)`, allocating one if the grain is unknown
    /// — the write path, where a segment is always available.
    fn segment_or_create(&self, shard: u32, grain: &GrainName) -> Arc<Segment> {
        self.segment(shard, grain, true)
            .expect("create allocates a segment")
    }

    /// The loaded segment for `(shard, grain)`, or `None` if the grain is unknown —
    /// the read path, which never allocates a segment for a grain it has not seen.
    fn segment_existing(&self, shard: u32, grain: &GrainName) -> Option<Arc<Segment>> {
        self.segment(shard, grain, false)
    }

    /// The segment id for `(shard, grain)`: the existing assignment, or — when
    /// `create` — a freshly allocated one, durably appended to the manifest first.
    fn segment_id(&self, shard: u32, grain: &GrainName, create: bool) -> Option<u64> {
        let mut manifest = self.manifest.lock().expect("grain store manifest poisoned");
        if let Some(id) = manifest.ids.get(&(shard, grain.clone())) {
            return Some(*id);
        }
        if !create {
            return None;
        }
        let id = manifest.next;
        manifest.next += 1;
        let path = manifest.path.clone();
        manifest
            .log
            .append(&ManifestEntry {
                shard,
                grain: grain.clone(),
                id,
            })
            .unwrap_or_else(|err| {
                panic!(
                    "grain store manifest persistence failed at {}: {err}",
                    path.display()
                )
            });
        manifest.ids.insert((shard, grain.clone()), id);
        Some(id)
    }

    /// A grain's content-addressed blob subtree, `blobs/<segment id>/` — the single
    /// place the on-disk blob layout is spelled (durable-workspace design).
    fn blob_dir(&self, seg_id: u64) -> PathBuf {
        self.dir.join("blobs").join(seg_id.to_string())
    }

    /// The on-disk path of one blob, `blobs/<segment id>/<blob hex>`.
    fn blob_path(&self, seg_id: u64, id: BlobId) -> PathBuf {
        self.blob_dir(seg_id).join(id.to_string())
    }

    /// Drop the live segment handle, its on-disk log, and the blob subtree of one
    /// grain. The manifest keeps the id assignment (append-only); a later access
    /// reopens an empty segment under the same id, which reads as absent.
    fn remove_grain_inner(&self, shard: u32, grain: &GrainName) {
        let Some(seg_id) = self.segment_id(shard, grain, false) else {
            return;
        };
        let mut segments = self.segments.lock().expect("grain store segments poisoned");
        segments.remove(&(shard, grain.clone()));
        let _ = fs::remove_file(self.segment_path(seg_id));
        let _ = fs::remove_dir_all(self.blob_dir(seg_id));
    }

    /// Rewrite a grain's segment to a single `Checkpoint` of its current state, folding
    /// away the record ops a snapshot made redundant (§9), and swap in the fresh append
    /// handle. Called under the held segment lock so no append races the rewrite.
    fn checkpoint(&self, segment: &Segment, inner: &mut SegmentInner) {
        inner
            .log
            .rewrite(&[SegOp::Checkpoint(inner.records.export())])
            .unwrap_or_else(|err| {
                panic!(
                    "grain store compaction failed at {}: {err}",
                    segment.path.display()
                )
            });
    }
}

/// Take the store directory's single-writer lock, held until the returned handle is
/// dropped (the lock lives on the open file description, not the path).
///
/// An advisory `flock` rather than a pid file, because the kernel drops it when the
/// holder's process ends: a node that crashed mid-write must be openable by its
/// replacement. Being advisory, it stops another *`FileGrainStore`*, not a stray `rm`.
#[cfg(unix)]
fn acquire_lock(dir: &Path) -> io::Result<Option<fs::File>> {
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(dir.join("LOCK"))?;
    rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive).map_err(
        |err| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "grain store at {} is already open: a node's directory belongs to one \
                     writer at a time, and two would interleave appends into each other's \
                     segments ({err})",
                    dir.display()
                ),
            )
        },
    )?;
    Ok(Some(file))
}

/// Where no advisory lock is available the single-writer rule stands as documentation
/// (as `wal::sync_dir` does for the directory fsync).
#[cfg(not(unix))]
fn acquire_lock(_dir: &Path) -> io::Result<Option<fs::File>> {
    Ok(None)
}

/// Load every `fences/<shard>` file into a shard→term map (eager: there are few shards
/// per node, and the fence must be known before any grain's records load).
fn load_fences(dir: &Path) -> io::Result<HashMap<u32, Term>> {
    let mut fences = HashMap::new();
    let fences_dir = dir.join("fences");
    for entry in fs::read_dir(&fences_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(shard) = name.to_str().and_then(|s| s.parse::<u32>().ok()) else {
            continue;
        };
        if let Some(term) = read_fence(&entry.path())? {
            fences.insert(shard, term);
        }
    }
    Ok(fences)
}

/// Open and replay the manifest, truncating any torn tail.
fn load_manifest(dir: &Path) -> io::Result<Manifest> {
    let path = dir.join("manifest");
    let (log, entries) = Wal::<ManifestEntry>::open(&path, MAX_RECORD, &MANIFEST_RECORDS)
        .map_err(|e| e.into_io())?;
    let mut ids = HashMap::new();
    let mut next = 0u64;
    for entry in entries {
        next = next.max(entry.id + 1);
        ids.insert((entry.shard, entry.grain), entry.id);
    }
    Ok(Manifest {
        path,
        log,
        ids,
        next,
    })
}

/// Open and replay a grain's segment file, truncating any torn tail. A `Checkpoint`
/// loads the whole segment state; every other op is re-applied to it in log order.
fn open_segment(dir: &Path, id: u64) -> Segment {
    let path = dir.join("segments").join(id.to_string());
    let (log, ops) = Wal::<SegOp>::open(&path, MAX_RECORD, &SEGMENT_RECORDS)
        .unwrap_or_else(|err| panic!("cannot open grain segment {}: {err}", path.display()));
    let mut records = GrainRecords::default();
    for op in ops {
        match op {
            SegOp::Checkpoint(checkpoint) => records = GrainRecords::from_checkpoint(checkpoint),
            SegOp::Record {
                after,
                term,
                records: recs,
                kind,
            } => {
                records.store_record(after, term, recs, kind);
            }
            SegOp::Snapshot { at, term, state } => {
                records.store_snapshot(at, term, state);
            }
            SegOp::Truncate { after, term } => records.truncate(after, term),
        }
    }
    Segment {
        path,
        inner: Mutex::new(SegmentInner { records, log }),
    }
}

/// Load every `seals/<shard>` file into a shard→bound map (eager, like the
/// fences: the bound must be known before any grain's appends are served).
fn load_seals(dir: &Path) -> io::Result<HashMap<u32, u64>> {
    let mut seals = HashMap::new();
    for entry in fs::read_dir(dir.join("seals"))? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(shard) = name.to_str().and_then(|s| s.parse::<u32>().ok()) else {
            continue;
        };
        if let Some(bound) = read_checksummed_u64(&entry.path())? {
            seals.insert(shard, bound);
        }
    }
    Ok(seals)
}

/// Atomically persist a shard's fence term: `[u64 term][u64 checksum]`.
fn write_fence(dir: &Path, shard: u32, term: Term) -> io::Result<()> {
    write_checksummed_u64(&dir.join("fences"), shard, term.value())
}

/// Atomically persist a shard's append bound: `[u64 bound][u64 checksum]`.
fn write_seal(dir: &Path, shard: u32, bound: u64) -> io::Result<()> {
    write_checksummed_u64(&dir.join("seals"), shard, bound)
}

/// Atomically persist one checksummed u64 under `dir/<shard>` — the shared shape
/// of the fence and seal files.
fn write_checksummed_u64(dir: &Path, shard: u32, value: u64) -> io::Result<()> {
    let raw = value.to_le_bytes();
    let mut bytes = raw.to_vec();
    bytes.extend_from_slice(&wal::checksum(&raw).to_le_bytes());
    wal::atomic_replace(dir, &shard.to_string(), &bytes)
}

/// Read a shard's fence term, or `None` if the file is absent or torn.
fn read_fence(path: &Path) -> io::Result<Option<Term>> {
    Ok(read_checksummed_u64(path)?.map(Term::new))
}

/// Read one checksummed u64, or `None` if the file is absent or torn.
fn read_checksummed_u64(path: &Path) -> io::Result<Option<u64>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    if bytes.len() != 16 {
        return Ok(None);
    }
    let raw = u64::from_le_bytes(bytes[..8].try_into().expect("8-byte slice"));
    let check = u64::from_le_bytes(bytes[8..].try_into().expect("8-byte slice"));
    if check != wal::checksum(&raw.to_le_bytes()) {
        return Ok(None);
    }
    Ok(Some(raw))
}

impl WriteGuard for FileGrainStore {
    fn sealed(&self, shard: u32, grain: &GrainName) -> bool {
        self.seals
            .lock()
            .expect("grain store seals poisoned")
            .get(&shard)
            .is_some_and(|&from| crate::system::name_at_or_above(grain, from))
    }

    /// Persists the bump before returning. The fence file is rewritten only when the
    /// term actually advances, so a steady-state append (same term) never touches it.
    fn bump_fence(&self, shard: u32, term: Term) -> Result<(), Term> {
        let mut fences = self.fences.lock().expect("grain store fences poisoned");
        let fence = *fences.get(&shard).unwrap_or(&Term::ZERO);
        if term < fence {
            return Err(fence);
        }
        if term > fence {
            write_fence(&self.dir, shard, term).unwrap_or_else(|err| {
                panic!(
                    "grain store fence persistence failed at {}: {err}",
                    self.dir.display()
                )
            });
            fences.insert(shard, term);
        }
        Ok(())
    }
}

// Every reply is `Reserved::ready`: this store fsyncs synchronously inside each call,
// so an outcome it settles is already stable when it returns.
impl GrainBlobStore for FileGrainStore {
    fn put_blob(&self, shard: u32, grain: &GrainName, id: BlobId, bytes: Vec<u8>) -> Reserved<()> {
        // One content-addressed file per blob, persisted with the same atomic
        // write-and-fsync the fence uses: no ack for a blob that is not durable.
        let seg_id = self
            .segment_id(shard, grain, true)
            .expect("create allocates an id");
        let dir = self.blob_dir(seg_id);
        let name = id.to_string();
        // Idempotent: equal content under the same id is already durable (B2).
        if dir.join(&name).exists() {
            return Reserved::ready(());
        }
        fs::create_dir_all(&dir).unwrap_or_else(|err| {
            panic!("grain store blob dir failed at {}: {err}", dir.display())
        });
        wal::atomic_replace(&dir, &name, &bytes).unwrap_or_else(|err| {
            panic!(
                "grain store blob persistence failed at {}: {err} — a replica that \
                 cannot persist a blob cannot safely acknowledge it",
                dir.display()
            )
        });
        Reserved::ready(())
    }

    fn get_blob(&self, shard: u32, grain: &GrainName, id: BlobId) -> Reserved<Option<Vec<u8>>> {
        let Some(seg_id) = self.segment_id(shard, grain, false) else {
            return Reserved::ready(None);
        };
        let path = self.blob_path(seg_id, id);
        Reserved::ready(match fs::read(&path) {
            Ok(bytes) => Some(bytes),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => panic!("grain store blob read failed at {}: {err}", path.display()),
        })
    }

    fn has_blob(&self, shard: u32, grain: &GrainName, id: BlobId) -> Reserved<bool> {
        let Some(seg_id) = self.segment_id(shard, grain, false) else {
            return Reserved::ready(false);
        };
        Reserved::ready(self.blob_path(seg_id, id).exists())
    }

    fn delete_blob(&self, shard: u32, grain: &GrainName, id: BlobId) -> Reserved<()> {
        if let Some(seg_id) = self.segment_id(shard, grain, false) {
            // Best-effort: removing a corrupt copy so the read path can re-store a
            // good one (§7.10 self-heal). A missing file is already done.
            let _ = fs::remove_file(self.blob_path(seg_id, id));
        }
        Reserved::ready(())
    }

    fn delete_blobs(&self, shard: u32, grain: &GrainName) -> Reserved<()> {
        if let Some(seg_id) = self.segment_id(shard, grain, false) {
            // Reclamation is best-effort (a leaked blob is harmless, only space): a
            // missing subtree is already-done, any other error is left for a later
            // sweep.
            let _ = fs::remove_dir_all(self.blob_dir(seg_id));
        }
        Reserved::ready(())
    }

    fn retain_blobs(
        &self,
        shard: u32,
        grain: &GrainName,
        retain: &BTreeSet<BlobId>,
    ) -> Reserved<()> {
        let Some(seg_id) = self.segment_id(shard, grain, false) else {
            return Reserved::ready(());
        };
        let dir = self.blob_dir(seg_id);
        let keep: HashSet<String> = retain.iter().map(|id| id.to_string()).collect();
        let Ok(entries) = fs::read_dir(&dir) else {
            return Reserved::ready(());
        };
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str()
                && !keep.contains(name)
            {
                let _ = fs::remove_file(entry.path());
            }
        }
        Reserved::ready(())
    }

    fn blob_ids(&self, shard: u32, grain: &GrainName) -> Reserved<Vec<BlobId>> {
        let Some(seg_id) = self.segment_id(shard, grain, false) else {
            return Reserved::ready(Vec::new());
        };
        let Ok(entries) = fs::read_dir(self.blob_dir(seg_id)) else {
            return Reserved::ready(Vec::new());
        };
        Reserved::ready(
            entries
                .flatten()
                .filter_map(|entry| BlobId::from_hex(entry.file_name().to_str()?))
                .collect(),
        )
    }
}

impl GrainStore for FileGrainStore {
    fn store_record(
        &self,
        shard: u32,
        grain: &GrainName,
        after: Seq,
        term: Term,
        records: Vec<Vec<u8>>,
        kind: WriteKind,
    ) -> Reserved<StoreAck> {
        let segment = self.segment_or_create(shard, grain);
        // Guard and apply under the segment lock, durable fence bump included, so a
        // concurrent `prepare` cannot slip between them (the fencing race, §8).
        let mut inner = segment.inner.lock().expect("grain segment poisoned");
        if let Err(ack) = self.guard_record(shard, grain, term, kind) {
            return Reserved::ready(ack);
        }
        inner
            .log
            .append(&SegOp::Record {
                after,
                term,
                records: records.clone(),
                kind,
            })
            .unwrap_or_else(|err| {
                panic!(
                    "grain store persistence failed at {}: {err} — a replica that cannot \
                     persist a record cannot safely acknowledge it",
                    segment.path.display()
                )
            });
        Reserved::ready(inner.records.store_record(after, term, records, kind))
    }

    fn read(&self, shard: u32, grain: &GrainName) -> Reserved<ReadReply> {
        Reserved::ready(match self.segment_existing(shard, grain) {
            Some(segment) => segment
                .inner
                .lock()
                .expect("grain segment poisoned")
                .records
                .read(),
            None => ReadReply {
                slots: Vec::new(),
                snapshot: None,
            },
        })
    }

    fn read_from(
        &self,
        shard: u32,
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> Reserved<Vec<(Seq, Vec<u8>)>> {
        Reserved::ready(match self.segment_existing(shard, grain) {
            Some(segment) => segment
                .inner
                .lock()
                .expect("grain segment poisoned")
                .records
                .read_from(from, limit),
            None => Vec::new(),
        })
    }

    fn prepare(&self, shard: u32, grain: &GrainName, term: Term) -> Reserved<ReadOutcome> {
        // The promise (the fence bump) must be durable before it is made, else a
        // restart could forget it and let a deposed leader commit (§8). The segment is
        // created even for a grain never seen here: holding its lock across the bump
        // and the read is what makes the promise and the returned view atomic against a
        // concurrent first append — a lock-free empty reply could miss a lower-term
        // record stored and acked in the window.
        let segment = self.segment_or_create(shard, grain);
        let inner = segment.inner.lock().expect("grain segment poisoned");
        if let Err(fence) = self.bump_fence(shard, term) {
            return Reserved::ready(ReadOutcome::Fenced(fence));
        }
        Reserved::ready(ReadOutcome::Prepared(inner.records.read()))
    }

    fn store_snapshot(
        &self,
        shard: u32,
        grain: &GrainName,
        at: Seq,
        term: Term,
        state: Vec<u8>,
        kind: WriteKind,
    ) -> Reserved<StoreAck> {
        let segment = self.segment_or_create(shard, grain);
        let mut inner = segment.inner.lock().expect("grain segment poisoned");
        if let Err(ack) = self.guard_snapshot(shard, term, kind) {
            return Reserved::ready(ack);
        }
        let (ack, advanced) = inner.records.store_snapshot(at, term, state);
        // A snapshot that advanced the base just compacted the records it subsumes
        // (§9): rewrite this grain's segment to a single checkpoint that embeds the
        // snapshot. One that did *not* advance writes nothing durable (module docs).
        if advanced {
            self.checkpoint(&segment, &mut inner);
        }
        Reserved::ready(ack)
    }

    fn truncate(&self, shard: u32, grain: &GrainName, after: Seq, term: Term) -> Reserved<()> {
        // A grain this store holds nothing for has no tail to drop, and truncating one
        // must not bring it into existence: it would then be enumerated by `grains` and
        // migrated as if it held data.
        let Some(segment) = self.segment_existing(shard, grain) else {
            return Reserved::ready(());
        };
        let mut inner = segment.inner.lock().expect("grain segment poisoned");
        inner
            .log
            .append(&SegOp::Truncate { after, term })
            .unwrap_or_else(|err| {
                panic!(
                    "grain store persistence failed at {}: {err}",
                    segment.path.display()
                )
            });
        inner.records.truncate(after, term);
        Reserved::ready(())
    }

    fn grains(&self, shard: u32) -> Vec<GrainName> {
        // The manifest assigns a segment id on the first record OR the first blob
        // (`put_blob` allocates through the same map), so its keys are the union of the
        // two areas — but only ever *grow*: the log is append-only, so `remove_grain`
        // leaves the id assignment behind (it must, to keep segment filenames
        // collision-free). The manifest is therefore only the candidate list; the
        // presence of the grain's own files is the answer.
        let ids: Vec<(GrainName, u64)> = {
            let manifest = self.manifest.lock().expect("grain store manifest poisoned");
            manifest
                .ids
                .iter()
                .filter(|((s, _), _)| *s == shard)
                .map(|((_, grain), &id)| (grain.clone(), id))
                .collect()
        };
        ids.into_iter()
            .filter(|(_, id)| {
                // Records or blobs — either makes the grain held. A blob-only grain
                // has no segment file, since `put_blob` allocates an id without
                // opening the segment.
                self.segment_path(*id).exists() || self.blob_dir(*id).exists()
            })
            .map(|(grain, _)| grain)
            .collect()
    }

    fn seal_range(&self, shard: u32, from: u64) -> Reserved<()> {
        let mut seals = self.seals.lock().expect("grain store seals poisoned");
        // Monotone tighten, persisted before it is honoured — the bound is a
        // promise (like the fence) and must survive a restart, else a stale
        // leader could assemble a majority for the moved range afterward (G15).
        let bound = seals.get(&shard).map_or(from, |&cur| cur.min(from));
        if seals.get(&shard) != Some(&bound) {
            write_seal(&self.dir, shard, bound).unwrap_or_else(|err| {
                panic!(
                    "grain store seal persistence failed at {}: {err} — a replica \
                     that cannot persist the bound cannot safely promise it",
                    self.dir.display()
                )
            });
            seals.insert(shard, bound);
        }
        Reserved::ready(())
    }

    fn unseal(&self, shard: u32) -> Reserved<()> {
        let mut seals = self.seals.lock().expect("grain store seals poisoned");
        if seals.remove(&shard).is_some() {
            // Best-effort removal: a leftover file re-seals on reopen, which a
            // re-applied merge commit clears again — conservative, never unsafe.
            let _ = fs::remove_file(self.dir.join("seals").join(shard.to_string()));
        }
        Reserved::ready(())
    }

    fn remove_grain(&self, shard: u32, grain: &GrainName) -> Reserved<()> {
        self.remove_grain_inner(shard, grain);
        Reserved::ready(())
    }

    /// Enumerate-and-remove, because a grain owns a file: the range is expressible
    /// only as the set of files in it.
    fn remove_range(&self, shard: u32, from: u64) -> Reserved<()> {
        for grain in self.grains(shard) {
            if crate::system::name_at_or_above(&grain, from) {
                self.remove_grain_inner(shard, &grain);
            }
        }
        Reserved::ready(())
    }

    fn drop_shard(&self, shard: u32) -> Reserved<()> {
        for grain in self.grains(shard) {
            self.remove_grain_inner(shard, &grain);
        }
        // The fence and the bound go with the data (see `MemoryGrainStore`), so a
        // shard index reused later starts from a clean promise rather than one this
        // shard's retired leader made.
        self.fences
            .lock()
            .expect("grain store fences poisoned")
            .remove(&shard);
        let _ = fs::remove_file(self.dir.join("fences").join(shard.to_string()));
        self.seals
            .lock()
            .expect("grain store seals poisoned")
            .remove(&shard);
        let _ = fs::remove_file(self.dir.join("seals").join(shard.to_string()));
        Reserved::ready(())
    }

    fn shard_bytes(&self, shard: u32) -> u64 {
        // File sizes, not in-memory sizes: segments load lazily, and the trigger
        // needs the durable footprint anyway.
        let ids: Vec<u64> = {
            let manifest = self.manifest.lock().expect("grain store manifest poisoned");
            manifest
                .ids
                .iter()
                .filter(|((s, _), _)| *s == shard)
                .map(|(_, &id)| id)
                .collect()
        };
        let mut total = 0u64;
        for id in ids {
            let seg_path = self.segment_path(id);
            if let Ok(meta) = fs::metadata(&seg_path) {
                total += meta.len();
            }
            if let Ok(entries) = fs::read_dir(self.blob_dir(id)) {
                for entry in entries.flatten() {
                    if let Ok(meta) = entry.metadata() {
                        total += meta.len();
                    }
                }
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryGrainStore;
    use std::fs::OpenOptions;
    use std::io::Write;

    fn name(key: &str) -> GrainName {
        GrainName::new("test.Grain", key)
    }

    /// Drive a store call to its durable outcome. These tests are synchronous by
    /// design — `a_prepare_append_race_never_hides_a_stored_record` races the store's
    /// own lock discipline (§18.1), which an async task cannot exercise — so they
    /// block rather than run a runtime.
    fn now<T: Send + 'static>(reserved: Reserved<T>) -> T {
        futures::executor::block_on(reserved.durable())
    }

    /// The single-writer rule, enforced rather than documented.
    #[cfg(unix)]
    #[test]
    fn a_second_open_of_one_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let first = FileGrainStore::open(dir.path()).expect("first open");
        assert!(
            FileGrainStore::open(dir.path()).is_err(),
            "a second store opened a directory another already holds"
        );
        // Released with the store, so a replacement can take over.
        drop(first);
        FileGrainStore::open(dir.path()).expect("reopen once the holder is gone");
    }

    #[test]
    fn records_round_trip_across_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("a");
        {
            let store = FileGrainStore::open(dir.path()).unwrap();
            assert_eq!(
                now(store.store_record(
                    0,
                    &n,
                    Seq::ZERO,
                    Term::new(1),
                    vec![b"e1".to_vec(), b"e2".to_vec()],
                    WriteKind::Append
                )),
                StoreAck::Stored(Seq::new(2))
            );
            // A snapshot below the head leaves a live tail, so records survive reopen.
            assert_eq!(
                now(store.store_snapshot(
                    0,
                    &n,
                    Seq::new(1),
                    Term::new(1),
                    b"snap".to_vec(),
                    WriteKind::Append
                )),
                StoreAck::Stored(Seq::new(1))
            );
        }
        // A fresh open recovers the retained record (e1 is compacted under the
        // snapshot at seq 1), its term, and the snapshot from disk.
        let reopened = FileGrainStore::open(dir.path()).unwrap();
        let reply = now(reopened.read(0, &n));
        assert_eq!(
            reply.slots,
            vec![(Seq::new(2), Term::new(1), b"e2".to_vec())]
        );
        assert_eq!(
            reply.snapshot,
            Some((Seq::new(1), Term::new(1), b"snap".to_vec()))
        );
    }

    #[test]
    fn a_snapshot_compacts_one_grains_segment_on_disk_and_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("a");
        let store = FileGrainStore::open(dir.path()).unwrap();
        // Grow the grain's segment with many sizeable records.
        for i in 0..50u64 {
            now(store.store_record(
                0,
                &n,
                Seq::new(i),
                Term::new(1),
                vec![vec![b'x'; 1000]],
                WriteKind::Append,
            ));
        }
        let id = *store
            .manifest
            .lock()
            .unwrap()
            .ids
            .get(&(0, n.clone()))
            .unwrap();
        let seg_path = dir.path().join("segments").join(id.to_string());
        let before = fs::metadata(&seg_path).unwrap().len();

        // A snapshot at the head subsumes every record: the segment compacts and its
        // file is rewritten to a single (small) checkpoint.
        now(store.store_snapshot(
            0,
            &n,
            Seq::new(50),
            Term::new(1),
            b"snap@50".to_vec(),
            WriteKind::Append,
        ));
        let after = fs::metadata(&seg_path).unwrap().len();
        assert!(
            after < before,
            "snapshot-driven compaction shrank the grain's segment: {after} < {before}"
        );
        drop(store);

        // The compacted segment reloads the snapshot and the (now empty) live tail.
        let reopened = FileGrainStore::open(dir.path()).unwrap();
        let reply = now(reopened.read(0, &n));
        assert!(reply.slots.is_empty());
        assert_eq!(
            reply.snapshot,
            Some((Seq::new(50), Term::new(1), b"snap@50".to_vec()))
        );
        // The next append continues contiguously from the recovered head.
        assert_eq!(
            now(reopened.store_record(
                0,
                &n,
                Seq::new(50),
                Term::new(1),
                vec![b"e51".to_vec()],
                WriteKind::Append
            )),
            StoreAck::Stored(Seq::new(51))
        );
    }

    #[test]
    fn one_grains_snapshot_leaves_another_grains_segment_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (name("a"), name("b"));
        let store = FileGrainStore::open(dir.path()).unwrap();
        now(store.store_record(
            0,
            &a,
            Seq::ZERO,
            Term::new(1),
            vec![b"a1".to_vec()],
            WriteKind::Append,
        ));
        now(store.store_record(
            0,
            &b,
            Seq::ZERO,
            Term::new(1),
            vec![b"b1".to_vec(), b"b2".to_vec()],
            WriteKind::Append,
        ));
        let id_b = *store
            .manifest
            .lock()
            .unwrap()
            .ids
            .get(&(0, b.clone()))
            .unwrap();
        let b_path = dir.path().join("segments").join(id_b.to_string());
        let b_before = fs::read(&b_path).unwrap();
        // Compacting grain `a` must not rewrite grain `b`'s segment.
        now(store.store_snapshot(
            0,
            &a,
            Seq::new(1),
            Term::new(1),
            b"snap-a".to_vec(),
            WriteKind::Append,
        ));
        assert_eq!(
            fs::read(&b_path).unwrap(),
            b_before,
            "grain b's segment was rewritten"
        );
    }

    #[test]
    fn a_redundant_snapshot_writes_nothing_and_does_not_bloat_the_segment() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("a");
        let store = FileGrainStore::open(dir.path()).unwrap();
        now(store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"e1".to_vec(), b"e2".to_vec()],
            WriteKind::Append,
        ));
        // A first snapshot advances the base and compacts to a checkpoint.
        now(store.store_snapshot(
            0,
            &n,
            Seq::new(2),
            Term::new(1),
            b"snap@2".to_vec(),
            WriteKind::Append,
        ));
        let id = *store
            .manifest
            .lock()
            .unwrap()
            .ids
            .get(&(0, n.clone()))
            .unwrap();
        let seg_path = dir.path().join("segments").join(id.to_string());
        let after_first = fs::metadata(&seg_path).unwrap().len();
        // Re-storing the same (non-advancing) snapshot many times — as repeated
        // re-activations would — must write nothing: the segment file does not grow.
        for _ in 0..20 {
            assert_eq!(
                now(store.store_snapshot(
                    0,
                    &n,
                    Seq::new(2),
                    Term::new(1),
                    b"snap@2".to_vec(),
                    WriteKind::Append
                )),
                StoreAck::Stored(Seq::new(2))
            );
        }
        assert_eq!(
            fs::metadata(&seg_path).unwrap().len(),
            after_first,
            "a redundant snapshot must not append to the segment"
        );
        // And the state still recovers correctly.
        drop(store);
        let reopened = FileGrainStore::open(dir.path()).unwrap();
        assert_eq!(
            now(reopened.read(0, &n)).snapshot,
            Some((Seq::new(2), Term::new(1), b"snap@2".to_vec()))
        );
    }

    #[test]
    fn the_append_bound_survives_a_reopen_and_unseal_lifts_it() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("a");
        {
            let store = FileGrainStore::open(dir.path()).unwrap();
            // Bound the whole space: every append on shard 0 is refused.
            now(store.seal_range(0, 0));
        }
        // The bound is a durable promise (G15): a reopen must not forget it, or a
        // stale leader could assemble a majority for the moved range afterward.
        let reopened = FileGrainStore::open(dir.path()).unwrap();
        assert_eq!(
            now(reopened.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(9),
                vec![b"e".to_vec()],
                WriteKind::Append
            )),
            StoreAck::Sealed
        );
        // A repair (the split driver's write-back) still lands.
        assert!(matches!(
            now(reopened.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(9),
                vec![b"e".to_vec()],
                WriteKind::Repair
            )),
            StoreAck::Stored(_)
        ));
        // A committed merge lifts the bound, durably.
        now(reopened.unseal(0));
        drop(reopened);
        let again = FileGrainStore::open(dir.path()).unwrap();
        assert!(matches!(
            now(again.store_record(
                0,
                &n,
                Seq::new(1),
                Term::new(9),
                vec![b"e2".to_vec()],
                WriteKind::Append
            )),
            StoreAck::Stored(_)
        ));
    }

    #[test]
    fn remove_grain_drops_the_segment_and_blobs_durably() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("moved");
        {
            let store = FileGrainStore::open(dir.path()).unwrap();
            now(store.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(1),
                vec![b"e1".to_vec()],
                WriteKind::Append,
            ));
            now(store.put_blob(0, &n, BlobId::of(b"b"), b"b".to_vec()));
            now(store.remove_grain(0, &n));
            assert!(now(store.read(0, &n)).slots.is_empty());
            assert!(!now(store.has_blob(0, &n, BlobId::of(b"b"))));
        }
        // Durable: the reopened store does not resurrect the grain.
        let reopened = FileGrainStore::open(dir.path()).unwrap();
        assert!(now(reopened.read(0, &n)).slots.is_empty());
        assert!(now(reopened.blob_ids(0, &n)).is_empty());
    }

    #[test]
    fn the_fence_survives_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("a");
        {
            let store = FileGrainStore::open(dir.path()).unwrap();
            // A recovery prepare at term 5 promises not to accept a lower term.
            assert!(matches!(
                now(store.prepare(0, &n, Term::new(5))),
                ReadOutcome::Prepared(_)
            ));
        }
        // The promise is durable: after reopen, a term-4 write is still fenced.
        let reopened = FileGrainStore::open(dir.path()).unwrap();
        assert_eq!(
            now(reopened.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(4),
                vec![b"stale".to_vec()],
                WriteKind::Append
            )),
            StoreAck::Fenced(Term::new(5))
        );
    }

    #[test]
    fn prepare_creates_and_locks_the_segment_of_an_unseen_grain() {
        // The fencing race (§8): prepare's promise and its returned (empty) view must
        // be atomic against a concurrent first append, which requires taking — and so
        // creating — the grain's segment. Without it, a term-1 first append could be
        // stored and acked after a term-2 prepare returned empty.
        let dir = tempfile::tempdir().unwrap();
        let n = name("fresh");
        let store = FileGrainStore::open(dir.path()).unwrap();
        assert!(matches!(
            now(store.prepare(0, &n, Term::new(2))),
            ReadOutcome::Prepared(_)
        ));
        // The segment now exists in the manifest: the prepare serialized on it.
        assert!(
            store
                .manifest
                .lock()
                .unwrap()
                .ids
                .contains_key(&(0, n.clone()))
        );
        // And the promise still fences a later lower-term append.
        assert_eq!(
            now(store.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(1),
                vec![b"late".to_vec()],
                WriteKind::Append
            )),
            StoreAck::Fenced(Term::new(2))
        );
    }

    #[test]
    // OS threads, not `Spawner::launch` (§18.1): this races the store's *synchronous*
    // lock discipline itself, which an async task cannot exercise — both calls must
    // genuinely contend on the segment and fence mutexes from separate threads.
    #[allow(clippy::disallowed_methods)]
    fn a_prepare_append_race_never_hides_a_stored_record() {
        // Loop a first-ever append (term 1) against a prepare (term 2) on a fresh
        // grain from two threads. Invariant: if the append was `Stored`, the
        // prepare either fenced it first or observed it — a `Prepared` EMPTY view
        // alongside a `Stored` ack is the quorum-intersection violation.
        use std::sync::Barrier;
        for round in 0..64 {
            let dir = tempfile::tempdir().unwrap();
            let store = std::sync::Arc::new(FileGrainStore::open(dir.path()).unwrap());
            let n = name(&format!("race-{round}"));
            let barrier = std::sync::Arc::new(Barrier::new(2));
            let (s1, n1, b1) = (store.clone(), n.clone(), barrier.clone());
            let append = std::thread::spawn(move || {
                b1.wait();
                now(s1.store_record(
                    0,
                    &n1,
                    Seq::ZERO,
                    Term::new(1),
                    vec![b"e1".to_vec()],
                    WriteKind::Append,
                ))
            });
            let (s2, n2, b2) = (store.clone(), n.clone(), barrier.clone());
            let prepare = std::thread::spawn(move || {
                b2.wait();
                now(s2.prepare(0, &n2, Term::new(2)))
            });
            let ack = append.join().unwrap();
            let view = prepare.join().unwrap();
            if let (StoreAck::Stored(_), ReadOutcome::Prepared(reply)) = (&ack, &view) {
                assert!(
                    !reply.slots.is_empty(),
                    "round {round}: append Stored but prepare saw an empty view"
                );
            }
        }
    }

    #[test]
    fn a_fence_promise_on_an_unseen_grain_is_durable() {
        let dir = tempfile::tempdir().unwrap();
        {
            // Prepare a grain that has no records yet: the promise is the shard fence,
            // which must survive even though no segment was ever written.
            let store = FileGrainStore::open(dir.path()).unwrap();
            assert!(matches!(
                now(store.prepare(0, &name("ghost"), Term::new(7))),
                ReadOutcome::Prepared(_)
            ));
        }
        let reopened = FileGrainStore::open(dir.path()).unwrap();
        // A different grain in the same shard is fenced by the recovered promise.
        assert_eq!(
            now(reopened.store_record(
                0,
                &name("other"),
                Seq::ZERO,
                Term::new(6),
                vec![b"x".to_vec()],
                WriteKind::Append
            )),
            StoreAck::Fenced(Term::new(7))
        );
    }

    #[test]
    fn a_torn_tail_is_discarded_and_appends_continue() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("a");
        {
            let store = FileGrainStore::open(dir.path()).unwrap();
            now(store.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(1),
                vec![b"e1".to_vec()],
                WriteKind::Append,
            ));
        }
        // A torn write: garbage lands after the valid record in the grain's segment.
        let id = {
            let store = FileGrainStore::open(dir.path()).unwrap();
            *store
                .manifest
                .lock()
                .unwrap()
                .ids
                .get(&(0, n.clone()))
                .unwrap()
        };
        let seg_path = dir.path().join("segments").join(id.to_string());
        let mut file = OpenOptions::new().append(true).open(&seg_path).unwrap();
        file.write_all(&[0x12, 0x34, 0x56]).unwrap();
        drop(file);

        let reopened = FileGrainStore::open(dir.path()).unwrap();
        assert_eq!(
            now(reopened.read(0, &n)).slots,
            vec![(Seq::new(1), Term::new(1), b"e1".to_vec())]
        );
        // The recovery truncated the garbage; appends land cleanly after it.
        assert_eq!(
            now(reopened.store_record(
                0,
                &n,
                Seq::new(1),
                Term::new(1),
                vec![b"e2".to_vec()],
                WriteKind::Append
            )),
            StoreAck::Stored(Seq::new(2))
        );
        drop(reopened);
        let again = FileGrainStore::open(dir.path()).unwrap();
        assert_eq!(now(again.read(0, &n)).slots.len(), 2);
    }

    /// The differential workhorse: drive the same op sequence through `FileGrainStore`
    /// — reopening from disk before every step — and a `MemoryGrainStore` mirror;
    /// their `read` must agree at every step. Covers replay across reopens.
    #[test]
    fn file_store_matches_memory_store_across_reopens() {
        enum Op {
            Record(Seq, Term, Vec<Vec<u8>>, WriteKind),
            Snapshot(Seq, Term, Vec<u8>),
            Prepare(Term),
            Truncate(Seq, Term),
        }
        let n = name("acct");
        let ops = [
            Op::Record(
                Seq::ZERO,
                Term::new(1),
                vec![b"a".to_vec(), b"b".to_vec()],
                WriteKind::Append,
            ),
            Op::Prepare(Term::new(2)),
            Op::Record(
                Seq::new(2),
                Term::new(2),
                vec![b"c".to_vec()],
                WriteKind::Append,
            ),
            Op::Snapshot(Seq::new(2), Term::new(2), b"snap@2".to_vec()),
            Op::Record(
                Seq::new(3),
                Term::new(2),
                vec![b"d".to_vec()],
                WriteKind::Append,
            ),
            Op::Truncate(Seq::new(3), Term::new(2)),
            Op::Record(
                Seq::new(3),
                Term::new(2),
                vec![b"d2".to_vec()],
                WriteKind::Append,
            ),
        ];

        let dir = tempfile::tempdir().unwrap();
        let mirror = MemoryGrainStore::new();
        for (step, op) in ops.iter().enumerate() {
            // A fresh open every step: the state must come back from disk.
            let file = FileGrainStore::open(dir.path()).unwrap();
            assert_eq!(
                now(file.read(0, &n)).slots,
                now(mirror.read(0, &n)).slots,
                "diverged before step {step}"
            );
            match op {
                Op::Record(after, term, recs, kind) => {
                    now(file.store_record(0, &n, *after, *term, recs.clone(), *kind));
                    now(mirror.store_record(0, &n, *after, *term, recs.clone(), *kind));
                }
                Op::Snapshot(at, term, state) => {
                    now(file.store_snapshot(0, &n, *at, *term, state.clone(), WriteKind::Append));
                    now(mirror.store_snapshot(0, &n, *at, *term, state.clone(), WriteKind::Append));
                }
                Op::Prepare(term) => {
                    now(file.prepare(0, &n, *term));
                    now(mirror.prepare(0, &n, *term));
                }
                Op::Truncate(after, term) => {
                    now(file.truncate(0, &n, *after, *term));
                    now(mirror.truncate(0, &n, *after, *term));
                }
            }
            let f = now(file.read(0, &n));
            let m = now(mirror.read(0, &n));
            assert_eq!(f.slots, m.slots, "slots diverged after step {step}");
            assert_eq!(
                f.snapshot, m.snapshot,
                "snapshot diverged after step {step}"
            );
        }
    }
}
