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
//!
//!   That second cost is the classic objection to file-per-object, and on this
//!   deployment's storage it is **not** the binding one: an fsync of a small append is
//!   tens of microseconds against a quorum round trip in the hundreds, so amortizing
//!   flushes across grains buys nothing worth a shared-log redesign
//!   (`docs/hardware-envelope.md` §3.1, §3.5, and I3 for the arithmetic; the
//!   group-commit proposal this retired is in the history). What *does* bind is
//!   filesystem metadata — inodes, directory size, and the descriptor budget below.
//!   Network-attached storage would reverse this (hw §6).
//! - `manifest` — an append-only map from `(shard, GrainName)` to a small integer
//!   segment **id**, so segment filenames are collision-free whatever a grain's key
//!   contains. A grain's segment is opened and replayed **lazily**, on first access,
//!   so a node holding millions of grains does not scan them all at startup — though
//!   the manifest itself is replayed and held whole, and only ever grows: an id
//!   assignment outlives the grain it names, which is why *presence* is the files',
//!   not the manifest's, to answer ([`grains`](GrainStore::grains)).
//!
//!   Held whole, and deliberately not compacted. An entry is a shard, a name, and a
//!   `u64` — on the order of 64 bytes resident, so ten million grains is a few hundred
//!   megabytes against a node sized in the hundreds of gigabytes, and the file itself
//!   replays at read plus digest throughput (hw §3.2). **Revisit past roughly a million
//!   grains on one node**, where the growth starts to mean something and granary §7.8's
//!   *"limited only by the shards' storage"* stops being a rounding error; until then a
//!   compaction pass would add a second format and a rewrite path for nothing.
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
//! **Failure policy.** A replica that cannot make a write durable cannot safely
//! acknowledge it — so it says so. An I/O error after open **poisons** the store: the
//! cause is recorded (readable through [`FileGrainStore::failure`]) and every
//! subsequent operation refuses, [`StoreAck::Failed`] for a write,
//! [`ReadOutcome::Failed`] for a recovery read, [`BlobAck::Failed`] for a blob. Those
//! refusals do not count toward any quorum, so the node simply drops out of its
//! shards' write and recovery quorums, which their existing majority logic already
//! covers.
//!
//! Poisoning is store-wide and one-way. Store-wide because the failures it catches — a
//! full volume, a lost mount, a failing device — belong to the directory rather than to
//! the grain whose write happened to notice first, and because a store that answered
//! for one area while unable to serve another would be lying by omission. One-way
//! because nothing here can establish that the volume recovered; a poisoned store is a
//! node to replace.
//!
//! The alternative, panicking, is what this replaced: it converts one bad volume into
//! the loss of every shard the node leads, all of which then re-elect and rehydrate
//! elsewhere — a cluster-wide event caused by a local fault. Only [`open`](
//! FileGrainStore::open) failure is still fatal at the [`factory`](
//! FileGrainStore::factory), because a replica with no storage at all has nothing to
//! degrade *from*.
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
//! **Durability reporting.** The guard, the in-memory apply, and the fsync all happen
//! under the grain's segment lock before a mutating call returns, so the outcome it
//! reports is already durable. What keeps it that way is the seam being *synchronous*:
//! a store shaped as an `async fn` would run its body at first poll, moving the
//! fencing-critical apply out from under the lock (§8).

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
use actor_serialization::Codec;
use serde::Deserialize;
use serde::Serialize;
use wal::Wal;

use crate::blobs::BlobId;
use crate::grain::GrainName;
use crate::journal::Seq;
use crate::journal::Term;
use crate::store::BlobAck;
use crate::store::BumpRefusal;
use crate::store::GrainBlobStore;
use crate::store::GrainCheckpoint;
use crate::store::GrainRecords;
use crate::store::GrainStore;
use crate::store::GrainStoreFactory;
use crate::store::ReadOutcome;
use crate::store::ReadReply;
use crate::store::StoreAck;
use crate::store::WriteGuard;
use crate::store::WriteKind;

/// Upper bound on one framed record's payload, a sanity check while scanning: a
/// length above this is treated as corruption, not an allocation. Generous, since a
/// grain's record bytes and a whole-segment `Checkpoint` record can be large.
const MAX_RECORD: u32 = 1 << 30;

/// How many grain segments a store keeps open by default.
///
/// Each is an open file descriptor, so this is really a descriptor budget. The number
/// is derived from the deployment's own limits, not inherited: a current Linux host
/// caps `fs.nr_open` at 1,048,576 and a service unit routinely raises `LimitNOFILE`
/// into the same range, and a descriptor costs a few hundred bytes of kernel memory —
/// so 65536 of them is tens of megabytes on a node sized in the hundreds of gigabytes
/// (`docs/hardware-envelope.md` §3.2, §3.6). It leaves room for the transport's
/// connections and the blob area's transient opens, and stays far above the grains a
/// node has *active* at once, which hibernation keeps to the working set (§10).
///
/// **A deployment MUST raise `LimitNOFILE` to at least ~70000.** This budget bounds
/// what the store keeps; it cannot bound what the kernel allows, and against the
/// traditional 1024 or 65536 default the process would hit the kernel's limit — as an
/// `EMFILE` on some unrelated open — before this cap ever evicted anything. See
/// `docs/standalone-deployment.md`.
///
/// The cap itself is not an optimization but a bound (hw §5): it is what keeps a node
/// that has *served* millions of grains from holding a descriptor for each. A miss
/// costs a reopen and replay of one grain's segment, bounded by compaction to a
/// checkpoint plus the records above it (§9).
const DEFAULT_SEGMENT_CAPACITY: usize = 65536;

/// A [`factory`](FileGrainStore::factory)'s open stores, keyed by the pair that names
/// one store: the hosted grain type and the node. Two hostings of the same pair share
/// an instance (the directory admits one writer); two types never do (§8.2).
type StoreCache = Mutex<HashMap<(String, NodeId), Arc<FileGrainStore>>>;

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
    /// How many grain segments — and so how many open file descriptors — this store
    /// keeps loaded (see [`FileGrainStore::evict_idle_segments`]).
    segment_capacity: usize,
    /// Why this store stopped being usable, once it has (see [`FileGrainStore::fail`]).
    /// `None` while healthy. One flag for the whole store because the failures it
    /// catches — a full volume, a lost mount, a failing device — belong to the
    /// directory, not to the grain whose write happened to notice first.
    poison: Mutex<Option<String>>,
}

impl FileGrainStore {
    /// Open (creating if needed) a node's store directory: confirm it holds records
    /// this build's codec can read, then load the per-shard fences and the segment
    /// manifest, truncating any torn tail. Grain segments load lazily.
    ///
    /// `codec` is the name of the deployment's codec
    /// ([`Codec::name`](actor_serialization::Codec::name)) — the one that encoded
    /// every event payload in here (§4.1, §5). It is checked against the store's
    /// stamp (§7.4) *before* any record is read, so a codec change is one refusal
    /// at startup rather than a corrupt-grain abort per activation.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::InvalidData`] when the store was written by another codec
    /// or at a `granary.store` revision this build does not accept; otherwise any
    /// filesystem error opening the directory or its index files.
    pub fn open(dir: impl Into<PathBuf>, codec: &str) -> io::Result<FileGrainStore> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        // Before anything is read (see `acquire_lock`).
        let lock = acquire_lock(&dir)?;
        // And before anything is *decoded*: the point of the stamp is that a store
        // this build cannot read is refused whole, in one place, rather than one
        // grain at a time as each activation fails to decode its own records.
        admit_store_stamp(&dir, codec)?;
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
            segment_capacity: DEFAULT_SEGMENT_CAPACITY,
            poison: Mutex::new(None),
        })
    }

    /// Record that this store's storage is unusable, and return `()` so a caller can
    /// tail-call it from an error arm.
    ///
    /// Poisoning is **one-way and store-wide**. A replica that cannot make a write
    /// durable cannot honestly acknowledge it (the module's failure policy), but the
    /// honest answer is [`StoreAck::Failed`], not a panic: panicking converts one bad
    /// volume into the loss of every shard this node leads, and those shards then
    /// re-elect and rehydrate elsewhere — a cluster event caused by a local fault.
    /// Refusing instead leaves the node running, dropping out of its quorums, where
    /// its peers' existing majority logic already covers it.
    ///
    /// The first cause is kept and later ones ignored: the first is the one that
    /// explains the rest.
    fn fail(&self, why: String) {
        let mut poison = self.poison.lock().expect("grain store poison mutex");
        if poison.is_none() {
            *poison = Some(why);
        }
    }

    /// Whether this store has been poisoned — checked before any operation touches
    /// the disk, so a failed store does no further I/O and answers uniformly.
    fn poisoned(&self) -> bool {
        self.poison
            .lock()
            .expect("grain store poison mutex")
            .is_some()
    }

    /// Why this store is unusable, or `None` while healthy. The deployment's health
    /// probe reads this: a poisoned store is a node to replace, and nothing recovers
    /// it in place (the flag is one-way).
    pub fn failure(&self) -> Option<String> {
        self.poison
            .lock()
            .expect("grain store poison mutex")
            .clone()
    }

    /// A [`GrainStoreFactory`] rooted at `root`: each hosted grain type's records live
    /// in its own `root/<grain_type>/<node>/` directory. Caches per `(grain_type,
    /// node)` so repeated hostings in one process share a single instance (single
    /// writer); a restart constructs a fresh factory and reopens from disk. Panics if a
    /// store cannot be opened — a replica without durable storage must not start (spec
    /// §7.4).
    ///
    /// The grain type is a directory of its own rather than a key inside one store
    /// because the whole store is shard-indexed: the fence (`fences/<shard>`), the
    /// append bound (`seals/<shard>`), and the migration driver's grain enumeration all
    /// take a bare index, which names a different consensus group under each type
    /// (§8.2). Separate roots make that mismatch unrepresentable instead of leaving it
    /// to every caller to remember.
    pub fn factory(root: impl Into<PathBuf>, codec: &dyn Codec) -> GrainStoreFactory {
        let root = root.into();
        // The codec by name, taken from the value the deployment configured its
        // system with rather than from a string beside it — the stamp is only worth
        // anything if it cannot disagree with what actually encodes the records.
        let codec = codec.name().to_string();
        let cache: Arc<StoreCache> = Arc::new(Mutex::new(HashMap::new()));
        Arc::new(move |grain_type: &str, node: NodeId| {
            let mut cache = cache.lock().expect("grain store cache poisoned");
            let store = cache
                .entry((grain_type.to_string(), node))
                .or_insert_with(|| {
                    let dir = root.join(grain_type).join(node.to_string());
                    Arc::new(FileGrainStore::open(&dir, &codec).unwrap_or_else(|err| {
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
        if self.poisoned() {
            return None;
        }
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
        // A segment that will not open poisons the store: the file is there (or should
        // be) and unreadable, which is the disk failing, not the grain being absent.
        // Returning `None` alone would read as "no such grain" and answer an empty
        // history for a grain that has one.
        let segment = match open_segment(&self.dir, id) {
            Ok(segment) => Arc::new(segment),
            Err(err) => {
                self.fail(format!(
                    "cannot open grain segment {}: {err}",
                    self.segment_path(id).display()
                ));
                return None;
            }
        };
        segments.insert((shard, grain.clone()), Arc::clone(&segment));
        Self::evict_idle_segments(&mut segments, self.segment_capacity);
        Some(segment)
    }

    /// Close segments the store is no longer using, once the loaded set exceeds
    /// `capacity`.
    ///
    /// Every loaded segment holds an **open file descriptor** (a `Wal` owns its
    /// `File`), and until this existed nothing ever closed one: a segment entered the
    /// map on first access and left only when its grain was explicitly deleted. A
    /// grain hibernating (§10) does not touch the store, so a node accumulated one fd
    /// per distinct grain it had *ever* served, for its whole lifetime — a hard
    /// ceiling at the process's descriptor limit, reached at a grain count far below
    /// anything else here is sized for, and with no back pressure before it.
    ///
    /// Closing one costs only a reopen-and-replay on the next access, which
    /// compaction bounds to a checkpoint plus the records above it (§9).
    ///
    /// **Only entries the map alone holds are evicted.** A `Segment` handed to a
    /// caller is an `Arc`, and dropping the map's copy would not close the file — but
    /// it would let the next access open a *second* `Wal` on the same path, and two
    /// appending handles on one file interleave into corruption. `strong_count == 1`
    /// under the map lock is exactly the condition that cannot happen, since the lock
    /// is the only way to obtain a clone. If every entry is in use the map is left
    /// over capacity rather than made unsafe; the callers are transient, so the next
    /// insert finds them idle.
    fn evict_idle_segments(
        segments: &mut HashMap<(u32, GrainName), Arc<Segment>>,
        capacity: usize,
    ) {
        if segments.len() <= capacity {
            return;
        }
        let excess = segments.len() - capacity;
        let idle: Vec<(u32, GrainName)> = segments
            .iter()
            .filter(|(_, segment)| Arc::strong_count(segment) == 1)
            .map(|(key, _)| key.clone())
            .take(excess)
            .collect();
        for key in idle {
            segments.remove(&key);
        }
    }

    /// The on-disk path of one grain's segment log, `segments/<segment id>`.
    fn segment_path(&self, seg_id: u64) -> PathBuf {
        self.dir.join("segments").join(seg_id.to_string())
    }

    /// The loaded segment for `(shard, grain)`, allocating one if the grain is unknown
    /// — the write path, where a segment is always available.
    /// `None` only when the store is poisoned — allocation itself always succeeds.
    fn segment_or_create(&self, shard: u32, grain: &GrainName) -> Option<Arc<Segment>> {
        self.segment(shard, grain, true)
    }

    /// The loaded segment for `(shard, grain)`, or `None` if the grain is unknown —
    /// the read path, which never allocates a segment for a grain it has not seen.
    fn segment_existing(&self, shard: u32, grain: &GrainName) -> Option<Arc<Segment>> {
        self.segment(shard, grain, false)
    }

    /// Project a known grain's records under its segment lock, or `None` if the grain is
    /// unknown. The shared body of every read this store answers, so the lookup, the lock,
    /// and its poison message are written once rather than per accessor.
    fn with_records<R>(
        &self,
        shard: u32,
        grain: &GrainName,
        project: impl FnOnce(&GrainRecords) -> R,
    ) -> Option<R> {
        self.segment_existing(shard, grain).map(|segment| {
            project(
                &segment
                    .inner
                    .lock()
                    .expect("grain segment poisoned")
                    .records,
            )
        })
    }

    /// The segment id for `(shard, grain)`: the existing assignment, or — when
    /// `create` — a freshly allocated one, durably appended to the manifest first.
    ///
    /// The poison check lives here rather than only in the callers because this is the
    /// one gate every area of the store passes through — records, snapshots, and the
    /// blob area alike. A poisoned store must not answer for the blob area either just
    /// because that subtree happens to still be writable: the failure it saw belongs
    /// to the volume, and a replica that cannot serve a grain's records has no
    /// business acknowledging its blobs.
    /// **The manifest lock spans an fsync, and that is the node's cold-create serial
    /// point.** The `create` branch below appends to the manifest log — a durable
    /// write — while holding the mutex, so every grain whose segment is being created
    /// for the first time queues behind every other one's flush. `benches/contention.rs`
    /// measures it: first writes are flat at 63–71 per second from one thread to
    /// sixteen, where `benches/flush.rs` shows the same device sustaining roughly twice
    /// as many flushes per second once they overlap. So concurrency buys nothing here
    /// and about 2x is on the table, on the path a failover walks when it brings up
    /// every grain it just inherited.
    ///
    /// Left as it stands because the obvious fix is the wrong one and the right one is
    /// not a cleanup. Sharding the map does not help: the serialization is the flush,
    /// not the hash lookup. Releasing the lock across the flush breaks what the comment
    /// below is protecting — an id must not be published before the entry naming it is
    /// durable, or a restart uses an id its manifest never recorded. What would work is
    /// group-commit: reserve ids under the lock, batch the entries, and one flush
    /// publishes them all. That is a durability-path change and wants its own review.
    ///
    /// The steady-state path is unaffected: an id already in the map returns from the
    /// hash hit above the flush, which is why `store_warm_read` is four orders faster.
    fn segment_id(&self, shard: u32, grain: &GrainName, create: bool) -> Option<u64> {
        if self.poisoned() {
            return None;
        }
        let mut manifest = self.manifest.lock().expect("grain store manifest poisoned");
        if let Some(id) = manifest.ids.get(&(shard, grain.clone())) {
            return Some(*id);
        }
        if !create {
            return None;
        }
        let id = manifest.next;
        let path = manifest.path.clone();
        if let Err(err) = manifest.log.append(&ManifestEntry {
            shard,
            grain: grain.clone(),
            id,
        }) {
            // The id is not consumed and the mapping is not published: an unrecorded
            // assignment must not survive in memory, or this store would use an id its
            // manifest never named and a restart would lose the grain's segment.
            drop(manifest);
            self.fail(format!(
                "grain store manifest persistence failed at {}: {err}",
                path.display()
            ));
            return None;
        }
        manifest.next += 1;
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
    ///
    /// Returns whether the rewrite succeeded. A failure poisons the store: the
    /// rewrite is atomic (the old segment survives on any error), so nothing is
    /// corrupt — but the volume that could not take a compaction cannot take the
    /// next append either, and the write that triggered this must not report itself
    /// durable.
    fn checkpoint(&self, segment: &Segment, inner: &mut SegmentInner) -> bool {
        match inner
            .log
            .rewrite(&[SegOp::Checkpoint(inner.records.export())])
        {
            Ok(()) => true,
            Err(err) => {
                self.fail(format!(
                    "grain store compaction failed at {}: {err}",
                    segment.path.display()
                ));
                false
            }
        }
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

/// The store's stamp (spec §7.4, compatibility spec §3).
///
/// One stamp for a whole directory rather than one per record: a store's records
/// and snapshots are all encoded by the same deployment codec, so the thing worth
/// recording is a property of the store, and recording it per record would cost
/// bytes on the hottest durable path in the tree to answer the same question a
/// million times.
pub(crate) const STORE: compat::Stamp =
    compat::Stamp::new(b"GRSTOR", compat::Window::at("granary.store", 1));

/// The stamp file's name inside the store directory.
const STORE_FILE: &str = "store";

/// The stamp's body: which codec wrote everything under this directory.
#[derive(Serialize, Deserialize)]
struct StoreBody {
    /// The deployment codec that encoded this store's event payloads (§4.1, §5).
    ///
    /// Facet payloads are `postcard` by construction and a snapshot carries its own
    /// copy of this (§7.12), but a grain's *events* are user types under the
    /// deployment's codec, and a grain with records past its last snapshot — or no
    /// snapshot at all — has nothing else that would notice the codec changing.
    codec: String,
    /// Room to grow without a revision bump (compatibility spec §2.1). The body is
    /// `postcard`, which is positional, so without this any added field would be a
    /// new revision with a second decoder to keep.
    ext: compat::Extensions,
}

/// The critical extension keys this build implements: none yet. An unknown critical
/// key is refused (**V2**) — its writer said a reader must understand it.
const STORE_EXT_KNOWN: &[u16] = &[];

/// Confirm `dir` holds records `codec` can read, stamping it if it is not stamped.
///
/// **An unstamped directory is adopted, not refused.** Refusing would make this
/// check a migration for every store that predates it, which is the opposite of
/// what a stamp is for; adoption records the codec running now. The limitation is
/// worth stating plainly: adoption cannot verify the claim it writes down, so a
/// store whose codec was *already* swapped is stamped with the wrong answer and
/// its records still fail one grain at a time. The stamp protects every swap after
/// it, and cannot retroactively protect one that already happened — which is why
/// the compatibility spec files this as worth having *before* a codec change.
fn admit_store_stamp(dir: &Path, codec: &str) -> io::Result<()> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    let path = dir.join(STORE_FILE);
    match fs::read(&path) {
        Ok(bytes) => {
            // Revision first, then the extension area, then the body: nothing reads a
            // field until the bytes have been admitted as this format at all.
            let (_, body) = STORE
                .unstamp(&bytes)
                .map_err(|err| invalid(format!("grain store at {}: {err}", dir.display())))?;
            let body: StoreBody = postcard::from_bytes(body).map_err(|err| {
                invalid(format!(
                    "grain store at {}: stamp body did not decode: {err}",
                    dir.display()
                ))
            })?;
            body.ext
                .admit(STORE.window().boundary(), STORE_EXT_KNOWN)
                .map_err(|err| invalid(format!("grain store at {}: {err}", dir.display())))?;
            if body.codec != codec {
                return Err(invalid(format!(
                    "grain store at {}: written with codec '{}', but this node runs \
                     '{codec}' — a grain's event payloads are codec-encoded (§4.1), so \
                     every record here would fail to decode. This is a configuration \
                     change, not a corrupt store: run this node with '{}', or point it \
                     at a different directory.",
                    dir.display(),
                    body.codec,
                    body.codec,
                )));
            }
            Ok(())
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            let body = postcard::to_allocvec(&StoreBody {
                codec: codec.to_string(),
                ext: compat::Extensions::new(),
            })
            .expect("the stamp body is plain owned data");
            wal::atomic_replace(dir, STORE_FILE, &STORE.stamp(&body))
        }
        Err(err) => Err(err),
    }
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
fn open_segment(dir: &Path, id: u64) -> io::Result<Segment> {
    let path = dir.join("segments").join(id.to_string());
    let (log, ops) =
        Wal::<SegOp>::open(&path, MAX_RECORD, &SEGMENT_RECORDS).map_err(wal::OpenError::into_io)?;
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
    Ok(Segment {
        path,
        inner: Mutex::new(SegmentInner { records, log }),
    })
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
    fn bump_fence(&self, shard: u32, term: Term) -> Result<(), BumpRefusal> {
        let mut fences = self.fences.lock().expect("grain store fences poisoned");
        let fence = *fences.get(&shard).unwrap_or(&Term::ZERO);
        if term < fence {
            return Err(BumpRefusal::Fenced(fence));
        }
        if term > fence {
            // The in-memory fence advances only after the file does. A bump kept only
            // in memory would be forgotten on restart, and this replica would then
            // accept a term it had already refused (§8).
            if let Err(err) = write_fence(&self.dir, shard, term) {
                self.fail(format!(
                    "grain store fence persistence failed at {}: {err}",
                    self.dir.display()
                ));
                return Err(BumpRefusal::Failed);
            }
            fences.insert(shard, term);
        }
        Ok(())
    }
}

// This store fsyncs synchronously inside each call, so an outcome it settles is
// already stable when it returns.
impl GrainBlobStore for FileGrainStore {
    fn put_blob(&self, shard: u32, grain: &GrainName, id: BlobId, bytes: Vec<u8>) -> BlobAck {
        // One content-addressed file per blob, persisted with the same atomic
        // write-and-fsync the fence uses: no ack for a blob that is not durable.
        let Some(seg_id) = self.segment_id(shard, grain, true) else {
            return BlobAck::Failed; // poisoned; `segment_id` said why
        };
        let dir = self.blob_dir(seg_id);
        let name = id.to_string();
        // Idempotent: equal content under the same id is already durable (B2).
        if dir.join(&name).exists() {
            return BlobAck::Stored;
        }
        if let Err(err) = fs::create_dir_all(&dir) {
            self.fail(format!(
                "grain store blob dir failed at {}: {err}",
                dir.display()
            ));
            return BlobAck::Failed;
        }
        if let Err(err) = wal::atomic_replace(&dir, &name, &bytes) {
            self.fail(format!(
                "grain store blob persistence failed at {}: {err} — a replica that \
                 cannot persist a blob cannot safely acknowledge it",
                dir.display()
            ));
            return BlobAck::Failed;
        }
        BlobAck::Stored
    }

    fn get_blob(&self, shard: u32, grain: &GrainName, id: BlobId) -> Option<Vec<u8>> {
        let seg_id = self.segment_id(shard, grain, false)?;
        let path = self.blob_path(seg_id, id);
        match fs::read(&path) {
            Ok(bytes) => Some(bytes),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            // Unreadable is not absent, but the read path cannot tell the caller so:
            // it answers `None`, and the caller falls through to a verifying replica
            // (**G17**). Poisoning is what keeps that honest — this store stops
            // claiming to hold anything rather than reporting a blob it cannot serve.
            Err(err) => {
                self.fail(format!(
                    "grain store blob read failed at {}: {err}",
                    path.display()
                ));
                None
            }
        }
    }

    fn has_blob(&self, shard: u32, grain: &GrainName, id: BlobId) -> bool {
        let Some(seg_id) = self.segment_id(shard, grain, false) else {
            return false;
        };
        self.blob_path(seg_id, id).exists()
    }

    fn delete_blob(&self, shard: u32, grain: &GrainName, id: BlobId) {
        if let Some(seg_id) = self.segment_id(shard, grain, false) {
            // Best-effort: removing a corrupt copy so the read path can re-store a
            // good one (§7.10 self-heal). A missing file is already done.
            let _ = fs::remove_file(self.blob_path(seg_id, id));
        }
    }

    fn delete_blobs(&self, shard: u32, grain: &GrainName) {
        if let Some(seg_id) = self.segment_id(shard, grain, false) {
            // Reclamation is best-effort (a leaked blob is harmless, only space): a
            // missing subtree is already-done, any other error is left for a later
            // sweep.
            let _ = fs::remove_dir_all(self.blob_dir(seg_id));
        }
    }

    fn retain_blobs(&self, shard: u32, grain: &GrainName, retain: &BTreeSet<BlobId>) {
        let Some(seg_id) = self.segment_id(shard, grain, false) else {
            return;
        };
        let dir = self.blob_dir(seg_id);
        let keep: HashSet<String> = retain.iter().map(|id| id.to_string()).collect();
        let Ok(entries) = fs::read_dir(&dir) else {
            return;
        };
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str()
                && !keep.contains(name)
            {
                let _ = fs::remove_file(entry.path());
            }
        }
    }

    fn blob_ids(&self, shard: u32, grain: &GrainName) -> Vec<BlobId> {
        let Some(seg_id) = self.segment_id(shard, grain, false) else {
            return Vec::new();
        };
        let Ok(entries) = fs::read_dir(self.blob_dir(seg_id)) else {
            return Vec::new();
        };

        entries
            .flatten()
            .filter_map(|entry| BlobId::from_hex(entry.file_name().to_str()?))
            .collect()
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
    ) -> StoreAck {
        let Some(segment) = self.segment_or_create(shard, grain) else {
            return StoreAck::Failed;
        };
        // Guard and apply under the segment lock, durable fence bump included, so a
        // concurrent `prepare` cannot slip between them (the fencing race, §8).
        let mut inner = segment.inner.lock().expect("grain segment poisoned");
        if let Err(ack) = self.guard_record(shard, grain, term, kind) {
            return ack;
        }
        // Durable before in-memory, so a record this store reports is one it holds. On
        // a write failure the in-memory apply is skipped and the store is poisoned:
        // refusing is the only honest answer, since acknowledging a record that is not
        // on disk shrinks a committed write's durability below its quorum (**G14**).
        // Framed once and taken apart again, rather than cloned for the frame: `append`
        // only borrows the op, and the in-memory apply below wants the same batch by
        // value. The batch is a grain's whole write — a mebibyte for a disk block — so
        // cloning it here allocated and copied one per append for the frame's sake.
        let op = SegOp::Record {
            after,
            term,
            records,
            kind,
        };
        if let Err(err) = inner.log.append(&op) {
            self.fail(format!(
                "grain store persistence failed at {}: {err} — a replica that cannot \
                 persist a record cannot safely acknowledge it",
                segment.path.display()
            ));
            return StoreAck::Failed;
        }
        let SegOp::Record { records, .. } = op else {
            unreachable!("built as SegOp::Record immediately above")
        };
        inner.records.store_record(after, term, records, kind)
    }

    fn read(&self, shard: u32, grain: &GrainName) -> ReadReply {
        self.with_records(shard, grain, GrainRecords::read)
            .unwrap_or_else(|| ReadReply {
                slots: Vec::new(),
                snapshot: None,
            })
    }

    // Answered from the segment directly, not through `read` — see the note on
    // `MemoryGrainStore`'s pair. Activation asks for both before a grain serves anything,
    // and through `read` each cloned the grain's whole record set to reach one field.

    fn head(&self, shard: u32, grain: &GrainName) -> Seq {
        self.with_records(shard, grain, GrainRecords::head)
            .unwrap_or(Seq::ZERO)
    }

    fn snapshot(&self, shard: u32, grain: &GrainName) -> Option<(Seq, Vec<u8>)> {
        self.with_records(shard, grain, GrainRecords::snapshot)
            .flatten()
    }

    fn read_from(
        &self,
        shard: u32,
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> Vec<(Seq, Vec<u8>)> {
        match self.segment_existing(shard, grain) {
            Some(segment) => segment
                .inner
                .lock()
                .expect("grain segment poisoned")
                .records
                .read_from(from, limit),
            None => Vec::new(),
        }
    }

    fn prepare(&self, shard: u32, grain: &GrainName, term: Term) -> ReadOutcome {
        // The promise (the fence bump) must be durable before it is made, else a
        // restart could forget it and let a deposed leader commit (§8). The segment is
        // created even for a grain never seen here: holding its lock across the bump
        // and the read is what makes the promise and the returned view atomic against a
        // concurrent first append — a lock-free empty reply could miss a lower-term
        // record stored and acked in the window.
        let Some(segment) = self.segment_or_create(shard, grain) else {
            return ReadOutcome::Failed;
        };
        let inner = segment.inner.lock().expect("grain segment poisoned");
        match self.bump_fence(shard, term) {
            Ok(()) => ReadOutcome::Prepared(inner.records.read()),
            Err(BumpRefusal::Fenced(fence)) => ReadOutcome::Fenced(fence),
            // The promise could not be made durable, so it was not made: answering
            // `Prepared` would offer a fence this store forgets on restart (§8).
            Err(BumpRefusal::Failed) => ReadOutcome::Failed,
        }
    }

    fn store_snapshot(
        &self,
        shard: u32,
        grain: &GrainName,
        at: Seq,
        term: Term,
        state: Vec<u8>,
        kind: WriteKind,
    ) -> StoreAck {
        let Some(segment) = self.segment_or_create(shard, grain) else {
            return StoreAck::Failed;
        };
        let mut inner = segment.inner.lock().expect("grain segment poisoned");
        if let Err(ack) = self.guard_snapshot(shard, term, kind) {
            return ack;
        }
        let (ack, advanced) = inner.records.store_snapshot(at, term, state);
        // A snapshot that advanced the base just compacted the records it subsumes
        // (§9): rewrite this grain's segment to a single checkpoint that embeds the
        // snapshot. One that did *not* advance writes nothing durable (module docs).
        if advanced && !self.checkpoint(&segment, &mut inner) {
            return StoreAck::Failed;
        }
        ack
    }

    fn truncate(&self, shard: u32, grain: &GrainName, after: Seq, term: Term) {
        // A grain this store holds nothing for has no tail to drop, and truncating one
        // must not bring it into existence: it would then be enumerated by `grains` and
        // migrated as if it held data.
        let Some(segment) = self.segment_existing(shard, grain) else {
            return;
        };
        let mut inner = segment.inner.lock().expect("grain segment poisoned");
        // A truncate that is not durable must not be applied in memory either: the
        // rollback exists so a later quorum-less recovery does not fold an uncommitted
        // record (§7.2), and dropping it only in memory would restore it on restart.
        if let Err(err) = inner.log.append(&SegOp::Truncate { after, term }) {
            self.fail(format!(
                "grain store persistence failed at {}: {err}",
                segment.path.display()
            ));
            return;
        }
        inner.records.truncate(after, term);
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

    fn seal_range(&self, shard: u32, from: u64) {
        let mut seals = self.seals.lock().expect("grain store seals poisoned");
        // Monotone tighten, persisted before it is honoured — the bound is a
        // promise (like the fence) and must survive a restart, else a stale
        // leader could assemble a majority for the moved range afterward (G15).
        let bound = seals.get(&shard).map_or(from, |&cur| cur.min(from));
        if seals.get(&shard) != Some(&bound) {
            // In memory only after the file: a bound this replica forgets on restart
            // would let a stale leader assemble a majority for the moved range (G15).
            // `seal_range` has no refusal to report, but the seal's majority stays
            // sound anyway: poisoning makes this store answer `Failed` to *every*
            // subsequent append, which refuses strictly more than the bound would, so
            // counting it toward the seal cannot let a moved-range write through.
            if let Err(err) = write_seal(&self.dir, shard, bound) {
                self.fail(format!(
                    "grain store seal persistence failed at {}: {err} — a replica \
                     that cannot persist the bound cannot safely promise it",
                    self.dir.display()
                ));
                return;
            }
            seals.insert(shard, bound);
        }
    }

    fn unseal(&self, shard: u32) {
        let mut seals = self.seals.lock().expect("grain store seals poisoned");
        if seals.remove(&shard).is_some() {
            // Best-effort removal: a leftover file re-seals on reopen, which a
            // re-applied merge commit clears again — conservative, never unsafe.
            let _ = fs::remove_file(self.dir.join("seals").join(shard.to_string()));
        }
    }

    fn remove_grain(&self, shard: u32, grain: &GrainName) {
        self.remove_grain_inner(shard, grain);
    }

    /// Enumerate-and-remove, because a grain owns a file: the range is expressible
    /// only as the set of files in it.
    fn remove_range(&self, shard: u32, from: u64) {
        for grain in self.grains(shard) {
            if crate::system::name_at_or_above(&grain, from) {
                self.remove_grain_inner(shard, &grain);
            }
        }
    }

    fn drop_shard(&self, shard: u32) {
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
    }

    fn shard_bytes(&self, shard: u32) -> u64 {
        // File sizes, not in-memory sizes: segments load lazily, and the trigger
        // needs the durable footprint anyway.
        //
        // Restating the filesystem each time, rather than maintaining a counter. A
        // `stat` served from the dentry cache is around a microsecond, so a shard
        // holding 100k grains costs ~100 ms of one core per sweep — and the sweep runs
        // every SPLIT_TRIGGER_INTERVAL (30 s) on a node with cores to spare
        // (`docs/hardware-envelope.md` §3.3). A maintained counter would be a number
        // that can drift from the filesystem, on the one path whose job is to describe
        // it. **Revisit when a sweep exceeds a second**, which is the point where the
        // trade turns.
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
    use actor_serialization::JsonCodec;
    use std::fs::OpenOptions;
    use std::io::Write;

    /// The codec these tests open every store under. Which one is immaterial to
    /// everything below except the stamp's own tests — what matters is that a
    /// reopen uses the *same* one, since disagreeing is exactly what the stamp
    /// refuses.
    const TEST_CODEC: &str = "json";

    fn name(key: &str) -> GrainName {
        GrainName::new("test.Grain", key)
    }

    // --- Golden corpus (compatibility spec §4) --------------------------------
    //
    // Both logs stamp a *record schema* into their header, and both record types
    // are `postcard`, which is positional: adding or reordering a field compiles
    // cleanly and makes every stored segment unreadable. These fixtures are the
    // only thing that notices. See `crate::corpus` for why they are never
    // regenerated.

    /// Stage checked-in bytes as a log file and hand back its path.
    ///
    /// Never opened in place: `Wal::open` truncates a torn tail, so a build that
    /// could not read the fixture would rewrite it and erase the failure.
    fn stage(dir: &tempfile::TempDir, bytes: &[u8]) -> std::path::PathBuf {
        let path = dir.path().join("log");
        std::fs::write(&path, bytes).expect("stage the fixture");
        path
    }

    /// Write `records` into a fresh log under `window` and return its bytes.
    fn produce_log<T: Serialize + serde::de::DeserializeOwned>(
        records: &[T],
        window: &compat::Window,
    ) -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let (mut log, _) = Wal::<T>::open(&path, MAX_RECORD, window).expect("open a fresh log");
        log.append_batch(records).expect("append");
        drop(log);
        std::fs::read(&path).expect("read back the produced log")
    }

    /// The corpus manifest entries, and they must never change: the checked-in
    /// bytes *are* these values.
    fn corpus_manifest() -> Vec<ManifestEntry> {
        vec![
            ManifestEntry {
                shard: 0,
                grain: name("a"),
                id: 0,
            },
            ManifestEntry {
                shard: 7,
                grain: GrainName::new("test.Other", ""),
                id: 42,
            },
        ]
    }

    #[test]
    fn granary_store_manifest_v1_still_replays_its_entries() {
        let bytes = crate::corpus::golden("granary.store.manifest", 1, || {
            produce_log(&corpus_manifest(), &MANIFEST_RECORDS)
        });

        let dir = tempfile::tempdir().unwrap();
        let (_log, entries) =
            Wal::<ManifestEntry>::open(stage(&dir, &bytes), MAX_RECORD, &MANIFEST_RECORDS)
                .expect("this build must read a granary.store.manifest v1 log it accepts");

        let found: Vec<(u32, GrainName, u64)> = entries
            .into_iter()
            .map(|e| (e.shard, e.grain, e.id))
            .collect();
        let expected: Vec<(u32, GrainName, u64)> = corpus_manifest()
            .into_iter()
            .map(|e| (e.shard, e.grain, e.id))
            .collect();
        assert_eq!(
            found, expected,
            "manifest entries decoded to different values than they were written from",
        );
    }

    /// The corpus segment ops: every [`SegOp`] variant, in an order whose replay
    /// leaves a state worth asserting — a checkpoint, two appends past it, a
    /// snapshot that advances the base, and a truncate that drops the last append.
    fn corpus_segment() -> Vec<SegOp> {
        let mut records = GrainRecords::default();
        records.store_record(
            Seq::ZERO,
            Term::new(1),
            vec![b"e1".to_vec(), b"e2".to_vec()],
            WriteKind::Append,
        );
        vec![
            SegOp::Checkpoint(records.export()),
            SegOp::Record {
                after: Seq::new(2),
                term: Term::new(1),
                records: vec![b"e3".to_vec()],
                kind: WriteKind::Append,
            },
            SegOp::Record {
                after: Seq::new(3),
                term: Term::new(1),
                records: vec![b"e4".to_vec()],
                kind: WriteKind::Append,
            },
            SegOp::Snapshot {
                at: Seq::new(2),
                term: Term::new(1),
                state: b"snap".to_vec(),
            },
            SegOp::Truncate {
                after: Seq::new(3),
                term: Term::new(1),
            },
        ]
    }

    #[test]
    fn granary_store_segment_v1_still_replays_to_the_same_segment() {
        let bytes = crate::corpus::golden("granary.store.segment", 1, || {
            produce_log(&corpus_segment(), &SEGMENT_RECORDS)
        });

        let dir = tempfile::tempdir().unwrap();
        let path = stage(&dir, &bytes);
        std::fs::create_dir_all(dir.path().join("segments")).unwrap();
        std::fs::rename(&path, dir.path().join("segments").join("0")).unwrap();

        // Replayed through the real `open_segment`, so the fixture pins the whole
        // read path — the header's schema stamp, the frame scan, and the fold —
        // rather than just `postcard`.
        let segment = open_segment(dir.path(), 0)
            .expect("this build must read a granary.store.segment v1 log it accepts");
        let inner = segment.inner.lock().unwrap();
        assert_eq!(
            inner.records.head(),
            Seq::new(3),
            "the replayed head moved: an op decoded to something else",
        );
        assert_eq!(
            inner.records.snapshot(),
            Some((Seq::new(2), b"snap".to_vec())),
            "the replayed snapshot moved: a Snapshot op decoded to something else",
        );
    }

    // --- The store stamp (spec §7.4, compatibility spec §3.4) -----------------

    #[test]
    fn a_fresh_store_is_stamped_and_reopens_under_the_same_codec() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileGrainStore::open(dir.path(), TEST_CODEC).expect("open a fresh store");
        assert!(
            dir.path().join(STORE_FILE).exists(),
            "a fresh store must record which codec wrote it",
        );
        drop(store);
        FileGrainStore::open(dir.path(), TEST_CODEC).expect("the same codec must reopen it");
    }

    #[test]
    fn a_codec_change_is_refused_once_at_open_naming_both() {
        // The whole point: a swapped codec is *one* refusal here, not a corrupt-grain
        // abort per activation for every grain with records past its last snapshot.
        let dir = tempfile::tempdir().unwrap();
        let store = FileGrainStore::open(dir.path(), "json").expect("open");
        let n = name("a");
        assert!(
            matches!(
                store.store_record(
                    0,
                    &n,
                    Seq::ZERO,
                    Term::new(1),
                    vec![b"e1".to_vec()],
                    WriteKind::Append
                ),
                StoreAck::Stored(_),
            ),
            "the record this store cannot later decode has to be there for the \
             refusal to matter",
        );
        drop(store);

        let Err(err) = FileGrainStore::open(dir.path(), "postcard") else {
            panic!("a store written under another codec must not open");
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let msg = err.to_string();
        assert!(
            msg.contains("json") && msg.contains("postcard"),
            "the refusal must name both codecs so an operator knows which end to \
             move: {msg}",
        );
    }

    #[test]
    fn a_store_from_another_revision_is_refused_as_a_revision() {
        // Not as a corrupt store, and without the body being decoded at all: the
        // revision is admitted before anything downstream sees a byte (**V2**).
        let dir = tempfile::tempdir().unwrap();
        FileGrainStore::open(dir.path(), TEST_CODEC).expect("open");
        let mut bytes = std::fs::read(dir.path().join(STORE_FILE)).expect("read the stamp");
        // The revision is a little-endian `u16` immediately after the magic.
        let head = b"GRSTOR".len();
        bytes[head..head + 2].copy_from_slice(&9u16.to_le_bytes());
        std::fs::write(dir.path().join(STORE_FILE), &bytes).expect("rewrite the stamp");

        let Err(err) = FileGrainStore::open(dir.path(), TEST_CODEC) else {
            panic!("a revision this build does not accept must not open");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("granary.store") && msg.contains("v9"),
            "the refusal must name the boundary and what it found: {msg}",
        );
    }

    #[test]
    fn an_unstamped_store_is_adopted_rather_than_refused() {
        // A store predating the stamp keeps working; adoption writes down the codec
        // running now. It cannot verify that claim — see `admit_store_stamp` — so
        // what this pins is that the check is not a migration.
        let dir = tempfile::tempdir().unwrap();
        let store = FileGrainStore::open(dir.path(), TEST_CODEC).expect("open");
        drop(store);
        std::fs::remove_file(dir.path().join(STORE_FILE)).expect("un-stamp the store");

        FileGrainStore::open(dir.path(), TEST_CODEC).expect("an unstamped store must still open");
        assert!(
            dir.path().join(STORE_FILE).exists(),
            "adoption must leave the store stamped, so the next codec change is caught",
        );
    }

    #[test]
    fn granary_store_v1_still_admits_its_codec() {
        let bytes = crate::corpus::golden("granary.store", 1, || {
            let body = postcard::to_allocvec(&StoreBody {
                codec: "json".to_string(),
                ext: compat::Extensions::new(),
            })
            .expect("encode the corpus body");
            STORE.stamp(&body)
        });

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(STORE_FILE), &bytes).expect("stage the fixture");
        admit_store_stamp(dir.path(), "json")
            .expect("this build must read a granary.store v1 stamp it accepts");

        // And the fixture pins the refusal too: the same bytes under another codec
        // are what an operator would hit after a swap.
        let err = admit_store_stamp(dir.path(), "postcard")
            .expect_err("the fixture must not admit another codec");
        assert!(
            err.to_string().contains("json"),
            "the refusal must name the codec that wrote the store: {err}",
        );
    }

    /// The single-writer rule, enforced rather than documented.
    #[cfg(unix)]
    #[test]
    fn a_second_open_of_one_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let first = FileGrainStore::open(dir.path(), TEST_CODEC).expect("first open");
        assert!(
            FileGrainStore::open(dir.path(), TEST_CODEC).is_err(),
            "a second store opened a directory another already holds"
        );
        // Released with the store, so a replacement can take over.
        drop(first);
        FileGrainStore::open(dir.path(), TEST_CODEC).expect("reopen once the holder is gone");
    }

    #[test]
    fn records_round_trip_across_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("a");
        {
            let store = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
            assert_eq!(
                store.store_record(
                    0,
                    &n,
                    Seq::ZERO,
                    Term::new(1),
                    vec![b"e1".to_vec(), b"e2".to_vec()],
                    WriteKind::Append
                ),
                StoreAck::Stored(Seq::new(2))
            );
            // A snapshot below the head leaves a live tail, so records survive reopen.
            assert_eq!(
                store.store_snapshot(
                    0,
                    &n,
                    Seq::new(1),
                    Term::new(1),
                    b"snap".to_vec(),
                    WriteKind::Append
                ),
                StoreAck::Stored(Seq::new(1))
            );
        }
        // A fresh open recovers the retained record (e1 is compacted under the
        // snapshot at seq 1), its term, and the snapshot from disk.
        let reopened = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
        let reply = reopened.read(0, &n);
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
        let store = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
        // Grow the grain's segment with many sizeable records.
        for i in 0..50u64 {
            let _ = store.store_record(
                0,
                &n,
                Seq::new(i),
                Term::new(1),
                vec![vec![b'x'; 1000]],
                WriteKind::Append,
            );
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
        let _ = store.store_snapshot(
            0,
            &n,
            Seq::new(50),
            Term::new(1),
            b"snap@50".to_vec(),
            WriteKind::Append,
        );
        let after = fs::metadata(&seg_path).unwrap().len();
        assert!(
            after < before,
            "snapshot-driven compaction shrank the grain's segment: {after} < {before}"
        );
        drop(store);

        // The compacted segment reloads the snapshot and the (now empty) live tail.
        let reopened = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
        let reply = reopened.read(0, &n);
        assert!(reply.slots.is_empty());
        assert_eq!(
            reply.snapshot,
            Some((Seq::new(50), Term::new(1), b"snap@50".to_vec()))
        );
        // The next append continues contiguously from the recovered head.
        assert_eq!(
            reopened.store_record(
                0,
                &n,
                Seq::new(50),
                Term::new(1),
                vec![b"e51".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stored(Seq::new(51))
        );
    }

    #[test]
    fn one_grains_snapshot_leaves_another_grains_segment_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (name("a"), name("b"));
        let store = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
        let _ = store.store_record(
            0,
            &a,
            Seq::ZERO,
            Term::new(1),
            vec![b"a1".to_vec()],
            WriteKind::Append,
        );
        let _ = store.store_record(
            0,
            &b,
            Seq::ZERO,
            Term::new(1),
            vec![b"b1".to_vec(), b"b2".to_vec()],
            WriteKind::Append,
        );
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
        let _ = store.store_snapshot(
            0,
            &a,
            Seq::new(1),
            Term::new(1),
            b"snap-a".to_vec(),
            WriteKind::Append,
        );
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
        let store = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
        let _ = store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"e1".to_vec(), b"e2".to_vec()],
            WriteKind::Append,
        );
        // A first snapshot advances the base and compacts to a checkpoint.
        let _ = store.store_snapshot(
            0,
            &n,
            Seq::new(2),
            Term::new(1),
            b"snap@2".to_vec(),
            WriteKind::Append,
        );
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
                store.store_snapshot(
                    0,
                    &n,
                    Seq::new(2),
                    Term::new(1),
                    b"snap@2".to_vec(),
                    WriteKind::Append
                ),
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
        let reopened = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
        assert_eq!(
            reopened.read(0, &n).snapshot,
            Some((Seq::new(2), Term::new(1), b"snap@2".to_vec()))
        );
    }

    #[test]
    fn the_append_bound_survives_a_reopen_and_unseal_lifts_it() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("a");
        {
            let store = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
            // Bound the whole space: every append on shard 0 is refused.
            store.seal_range(0, 0);
        }
        // The bound is a durable promise (G15): a reopen must not forget it, or a
        // stale leader could assemble a majority for the moved range afterward.
        let reopened = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
        assert_eq!(
            reopened.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(9),
                vec![b"e".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Sealed
        );
        // A repair (the split driver's write-back) still lands.
        assert!(matches!(
            reopened.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(9),
                vec![b"e".to_vec()],
                WriteKind::Repair
            ),
            StoreAck::Stored(_)
        ));
        // A committed merge lifts the bound, durably.
        reopened.unseal(0);
        drop(reopened);
        let again = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
        assert!(matches!(
            again.store_record(
                0,
                &n,
                Seq::new(1),
                Term::new(9),
                vec![b"e2".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stored(_)
        ));
    }

    #[test]
    fn remove_grain_drops_the_segment_and_blobs_durably() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("moved");
        {
            let store = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
            let _ = store.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(1),
                vec![b"e1".to_vec()],
                WriteKind::Append,
            );
            let _ = store.put_blob(0, &n, BlobId::of(b"b"), b"b".to_vec());
            store.remove_grain(0, &n);
            assert!(store.read(0, &n).slots.is_empty());
            assert!(!store.has_blob(0, &n, BlobId::of(b"b")));
        }
        // Durable: the reopened store does not resurrect the grain.
        let reopened = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
        assert!(reopened.read(0, &n).slots.is_empty());
        assert!(reopened.blob_ids(0, &n).is_empty());
    }

    #[test]
    fn the_fence_survives_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("a");
        {
            let store = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
            // A recovery prepare at term 5 promises not to accept a lower term.
            assert!(matches!(
                store.prepare(0, &n, Term::new(5)),
                ReadOutcome::Prepared(_)
            ));
        }
        // The promise is durable: after reopen, a term-4 write is still fenced.
        let reopened = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
        assert_eq!(
            reopened.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(4),
                vec![b"stale".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Fenced(Term::new(5))
        );
    }

    /// The fence is keyed by shard *index*, but a shard is `(grain_type, index)` and
    /// each pair is its own leader-election group with its own term (§8.2). A factory
    /// that handed both types one store would let the type that elects more often push
    /// `fences/<index>` past the other's term, and the quieter type's appends and
    /// recovery reads are then refused for good — its grains stop activating at all.
    #[test]
    fn two_grain_types_do_not_share_a_shards_fence() {
        let root = tempfile::tempdir().unwrap();
        let factory = FileGrainStore::factory(root.path().to_path_buf(), &JsonCodec);
        let node = NodeId::new(1);
        let busy = factory("busy.Type", node);
        let quiet = factory("quiet.Type", node);

        // The busy type's shard 0 reaches term 9 and fences its own store there.
        assert!(matches!(
            busy.prepare(0, &GrainName::new("busy.Type", "a"), Term::new(9)),
            ReadOutcome::Prepared(_)
        ));
        // The quiet type's shard 0 is a different group, still at term 2. Its append
        // must land: the busy type's term says nothing about who leads this shard.
        assert_eq!(
            quiet.store_record(
                0,
                &GrainName::new("quiet.Type", "a"),
                Seq::ZERO,
                Term::new(2),
                vec![b"mine".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stored(Seq::new(1))
        );
        // And its leader can still recover the grain on the next activation.
        assert!(matches!(
            quiet.prepare(0, &GrainName::new("quiet.Type", "a"), Term::new(2)),
            ReadOutcome::Prepared(_)
        ));
    }

    /// Same node, same type: one store, so a restart in-process keeps the single-writer
    /// rule (a second `open` of one directory is refused).
    #[test]
    fn one_store_per_grain_type_and_node() {
        let root = tempfile::tempdir().unwrap();
        let factory = FileGrainStore::factory(root.path().to_path_buf(), &JsonCodec);
        let first = factory("a.Type", NodeId::new(1));
        let second = factory("a.Type", NodeId::new(1));
        assert!(
            Arc::ptr_eq(&first, &second),
            "a repeated hosting of one type on one node must share its store"
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
        let store = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
        assert!(matches!(
            store.prepare(0, &n, Term::new(2)),
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
            store.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(1),
                vec![b"late".to_vec()],
                WriteKind::Append
            ),
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
            let store = std::sync::Arc::new(FileGrainStore::open(dir.path(), TEST_CODEC).unwrap());
            let n = name(&format!("race-{round}"));
            let barrier = std::sync::Arc::new(Barrier::new(2));
            let (s1, n1, b1) = (store.clone(), n.clone(), barrier.clone());
            let append = std::thread::spawn(move || {
                b1.wait();
                s1.store_record(
                    0,
                    &n1,
                    Seq::ZERO,
                    Term::new(1),
                    vec![b"e1".to_vec()],
                    WriteKind::Append,
                )
            });
            let (s2, n2, b2) = (store.clone(), n.clone(), barrier.clone());
            let prepare = std::thread::spawn(move || {
                b2.wait();
                s2.prepare(0, &n2, Term::new(2))
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
            let store = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
            assert!(matches!(
                store.prepare(0, &name("ghost"), Term::new(7)),
                ReadOutcome::Prepared(_)
            ));
        }
        let reopened = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
        // A different grain in the same shard is fenced by the recovered promise.
        assert_eq!(
            reopened.store_record(
                0,
                &name("other"),
                Seq::ZERO,
                Term::new(6),
                vec![b"x".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Fenced(Term::new(7))
        );
    }

    #[test]
    fn a_torn_tail_is_discarded_and_appends_continue() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("a");
        {
            let store = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
            let _ = store.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(1),
                vec![b"e1".to_vec()],
                WriteKind::Append,
            );
        }
        // A torn write: garbage lands after the valid record in the grain's segment.
        let id = {
            let store = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
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

        let reopened = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
        assert_eq!(
            reopened.read(0, &n).slots,
            vec![(Seq::new(1), Term::new(1), b"e1".to_vec())]
        );
        // The recovery truncated the garbage; appends land cleanly after it.
        assert_eq!(
            reopened.store_record(
                0,
                &n,
                Seq::new(1),
                Term::new(1),
                vec![b"e2".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stored(Seq::new(2))
        );
        drop(reopened);
        let again = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
        assert_eq!(again.read(0, &n).slots.len(), 2);
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
            let file = FileGrainStore::open(dir.path(), TEST_CODEC).unwrap();
            assert_eq!(
                file.read(0, &n).slots,
                mirror.read(0, &n).slots,
                "diverged before step {step}"
            );
            match op {
                Op::Record(after, term, recs, kind) => {
                    let _ = file.store_record(0, &n, *after, *term, recs.clone(), *kind);
                    let _ = mirror.store_record(0, &n, *after, *term, recs.clone(), *kind);
                }
                Op::Snapshot(at, term, state) => {
                    let _ =
                        file.store_snapshot(0, &n, *at, *term, state.clone(), WriteKind::Append);
                    let _ =
                        mirror.store_snapshot(0, &n, *at, *term, state.clone(), WriteKind::Append);
                }
                Op::Prepare(term) => {
                    let _ = file.prepare(0, &n, *term);
                    let _ = mirror.prepare(0, &n, *term);
                }
                Op::Truncate(after, term) => {
                    file.truncate(0, &n, *after, *term);
                    mirror.truncate(0, &n, *after, *term);
                }
            }
            let f = file.read(0, &n);
            let m = mirror.read(0, &n);
            assert_eq!(f.slots, m.slots, "slots diverged after step {step}");
            assert_eq!(
                f.snapshot, m.snapshot,
                "snapshot diverged after step {step}"
            );
        }
    }

    /// The descriptor bound: serving unboundedly many distinct grains must not leave
    /// unboundedly many segments — and so file descriptors — open.
    ///
    /// Before this, a segment entered the loaded set on first access and left only
    /// when its grain was deleted, so a long-lived node accumulated one open fd per
    /// grain it had ever touched. Hibernation does not help: it stops the grain's
    /// host, which never touches the store.
    #[test]
    fn the_loaded_segment_set_stays_bounded_across_many_grains() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = FileGrainStore::open(dir.path(), TEST_CODEC).expect("open");
        store.segment_capacity = 16;
        for i in 0..500 {
            let grain = name(&format!("grain-{i}"));
            let _ = store.store_record(
                0,
                &grain,
                Seq::ZERO,
                Term::new(1),
                vec![b"x".to_vec()],
                WriteKind::Append,
            );
        }
        let loaded = store.segments.lock().expect("segments").len();
        assert!(
            loaded <= 16,
            "500 grains left {loaded} segments open against a capacity of 16"
        );
    }

    /// Eviction must never close a segment a caller still holds: two `Wal` handles
    /// appending to one file interleave into corruption, which is what a second
    /// `open_segment` on an evicted-but-live path would produce.
    #[test]
    fn a_segment_in_use_is_never_evicted() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = FileGrainStore::open(dir.path(), TEST_CODEC).expect("open");
        store.segment_capacity = 1;
        let held = name("held");
        let _ = store.store_record(
            0,
            &held,
            Seq::ZERO,
            Term::new(1),
            vec![b"a".to_vec()],
            WriteKind::Append,
        );
        // Hold it the way a caller does, then push far past capacity.
        let live = store.segment(0, &held, false).expect("the held segment");
        for i in 0..50 {
            let grain = name(&format!("other-{i}"));
            let _ = store.store_record(
                0,
                &grain,
                Seq::ZERO,
                Term::new(1),
                vec![b"y".to_vec()],
                WriteKind::Append,
            );
        }
        let same = store.segment(0, &held, false).expect("still resolvable");
        assert!(
            Arc::ptr_eq(&live, &same),
            "a held segment stayed the same instance — a second Wal on its file would corrupt it"
        );
        drop(live);
        drop(same);
        // Once released it becomes evictable like any other, so holding one entry
        // does not defeat the bound.
        for i in 50..120 {
            let grain = name(&format!("other-{i}"));
            let _ = store.store_record(
                0,
                &grain,
                Seq::ZERO,
                Term::new(1),
                vec![b"z".to_vec()],
                WriteKind::Append,
            );
        }
        assert!(
            store.segments.lock().expect("segments").len() <= 2,
            "a released segment is evictable again"
        );
    }

    /// An evicted segment must reopen to exactly the state it had: eviction is a
    /// cache decision, and the file is the truth (**G3**).
    #[test]
    fn an_evicted_segment_reopens_with_its_records_intact() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = FileGrainStore::open(dir.path(), TEST_CODEC).expect("open");
        store.segment_capacity = 1;
        let grain = name("survivor");
        let _ = store.store_record(
            0,
            &grain,
            Seq::ZERO,
            Term::new(1),
            vec![b"one".to_vec(), b"two".to_vec()],
            WriteKind::Append,
        );
        // Push it out of the loaded set.
        for i in 0..20 {
            let _ = store.store_record(
                0,
                &name(&format!("filler-{i}")),
                Seq::ZERO,
                Term::new(1),
                vec![b"f".to_vec()],
                WriteKind::Append,
            );
        }
        let reply = store.read(0, &grain);
        assert_eq!(
            reply.slots.len(),
            2,
            "the reopened segment holds both records"
        );
        assert_eq!(reply.head(), Seq::new(2));
    }

    /// The failure policy: a store that cannot write **refuses**, and keeps refusing.
    ///
    /// The alternative this replaced was a panic, which took the whole process — and
    /// with it every other shard the node led — down for one bad volume. Refusing
    /// leaves the node up and simply outside its shards' quorums, which the peers'
    /// majority logic already handles.
    #[test]
    fn a_store_that_cannot_write_refuses_instead_of_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileGrainStore::open(dir.path(), TEST_CODEC).expect("open");
        let healthy = name("healthy");
        assert!(matches!(
            store.store_record(
                0,
                &healthy,
                Seq::ZERO,
                Term::new(1),
                vec![b"a".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stored(_)
        ));
        assert_eq!(store.failure(), None, "a working store is not poisoned");

        // Take the segment area away: the next grain needs a segment file and cannot
        // get one. Standing in for the full volume or failing device this guards.
        fs::remove_dir_all(dir.path().join("segments")).unwrap();

        let doomed = name("doomed");
        assert_eq!(
            store.store_record(
                0,
                &doomed,
                Seq::ZERO,
                Term::new(1),
                vec![b"b".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Failed,
            "a record that cannot be persisted must not be acknowledged"
        );
        assert!(store.failure().is_some(), "the failure is recorded");
    }

    /// Poisoning is store-wide and one-way: after the first failure the store stops
    /// claiming anything, including for grains it had already served successfully.
    /// A store that answered `Stored` again here would be acknowledging writes it can
    /// no longer persist.
    #[test]
    fn a_poisoned_store_refuses_every_later_operation() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileGrainStore::open(dir.path(), TEST_CODEC).expect("open");
        let grain = name("served");
        let _ = store.store_record(
            0,
            &grain,
            Seq::ZERO,
            Term::new(1),
            vec![b"a".to_vec()],
            WriteKind::Append,
        );
        fs::remove_dir_all(dir.path().join("segments")).unwrap();
        let _ = store.store_record(
            0,
            &name("other"),
            Seq::ZERO,
            Term::new(1),
            vec![b"b".to_vec()],
            WriteKind::Append,
        );
        assert!(store.failure().is_some(), "poisoned by the failed write");

        assert_eq!(
            store.store_record(
                0,
                &grain,
                Seq::new(1),
                Term::new(1),
                vec![b"c".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Failed,
            "a previously served grain is refused too — the volume is the unit"
        );
        // The promise, not just the view: a replica that cannot durably record the
        // term it promised could accept a lower one after a restart (§8, G14).
        assert_eq!(
            store.prepare(0, &grain, Term::new(2)),
            ReadOutcome::Failed,
            "a fence it cannot persist is a fence it has not promised"
        );
        assert_eq!(
            store.put_blob(0, &grain, BlobId::of(b"x"), b"x".to_vec()),
            BlobAck::Failed,
            "a blob copy it cannot store must not count toward a quorum"
        );
    }
}
