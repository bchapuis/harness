//! A file-backed, **per-entity segmented** [`EntityStore`] (spec §7.2, §7.4, §9): a
//! node's entity records on the local filesystem, durable across a process restart.
//!
//! An entity's records live **off** the leader-election group's Raft log, in each
//! replica's [`EntityStore`] (§7.2), so surviving a full-cluster cold restart needs the
//! store itself to be durable. Injected through
//! [`EntityConfig::entity_store`](crate::EntityConfig); the default
//! [`MemoryEntityStore`](crate::store::MemoryEntityStore) is lost on restart, while this
//! one reloads each node's records and re-establishes the per-shard fence, so a
//! re-elected leader recovers every entity's committed head from a quorum of the
//! reloaded stores (§8, **E14**).
//!
//! Both the segments and the manifest are framed, checksummed append-only logs; the
//! framing, torn-tail recovery, and atomic rewrite live in the shared [`wal`] crate,
//! so this module is just the entity store's layout and policy over it.
//!
//! **Layout.** A node's store is a directory holding:
//!
//! - `LOCK`: the single-writer guard (see below), taken before anything is read.
//! - `segments/<id>`: one **per-entity op log** ([`wal::Wal`] of [`SegOp`]). Each entity
//!   is an independent segment: its mutating calls
//!   ([`store_record`](EntityStore::store_record),
//!   [`store_snapshot`](EntityStore::store_snapshot), [`truncate`](EntityStore::truncate))
//!   are appended and fsynced before the call returns. Because segments are per entity,
//!   one entity's snapshot compaction rewrites only that entity's file, never the whole
//!   node's store. The cost: an entity owns a file, so a node's file count tracks its
//!   entity count, and no two entities' appends can share an fsync.
//!
//!   That second cost is not the binding one on this deployment's storage: an fsync of
//!   a small append is tens of microseconds against a quorum round trip in the
//!   hundreds, so amortizing flushes across entities buys nothing worth a shared-log
//!   redesign (`docs/hardware-envelope.md` §3.1, §3.5, and I3 for the arithmetic).
//!   What *does* bind is filesystem metadata: inodes, directory size, and the
//!   descriptor budget below. Network-attached storage would reverse this (hw §6).
//! - `manifest`: an append-only map from `(shard, EntityName)` to a small integer
//!   segment **id**, so segment filenames are collision-free whatever an entity's key
//!   contains. An entity's segment is opened and replayed **lazily**, on first access,
//!   so a node holding millions of entities does not scan them all at startup. The
//!   manifest itself is replayed and held whole, and only ever grows: an id assignment
//!   outlives the entity it names, which is why *presence* is the files', not the
//!   manifest's, to answer ([`entities`](EntityStore::entities)).
//!
//!   Held whole, and deliberately not compacted. An entry is a shard, a name, and a
//!   `u64`, on the order of 64 bytes resident, so ten million entities is a few hundred
//!   megabytes against a node sized in the hundreds of gigabytes, and the file itself
//!   replays at read plus digest throughput (hw §3.2). **Revisit past roughly a million
//!   entities on one node**; until then a compaction pass would add a second format and
//!   a rewrite path for nothing.
//! - `fences/<shard>`, the per-shard **fence**: the highest shard term this node has
//!   acknowledged (§8), the one piece of state shared across a shard's entities. It is
//!   rewritten (atomically) only when the term advances (on failover and recovery
//!   `prepare`, never on a steady-state append), and loaded eagerly on open, so the
//!   fence is known before any entity's records load lazily. A stamped format,
//!   `entity.store.fence` (compatibility §3). A file this build cannot read is a
//!   **refusal that fails `open`**, not an absent fence: reading a damaged fence
//!   as "no fence" would withdraw **E1** with no diagnostic, and there is nothing
//!   safe to guess.
//! - `seals/<shard>`, the per-shard **append bound** (§7.7): refuse appends at or
//!   above this name hash. Durable and loaded eagerly for the same reason as the
//!   fence: a restart that forgot the bound could let a stale leader assemble a
//!   majority for a range a split moved away (**E15**). Stamped as
//!   `entity.store.seal`, and unreadable bytes refuse for the same reason.
//! - `blobs/<id>/<blob hex>`: an entity's content-addressed blob area (§7.10), one
//!   file per blob under the same collision-free id the manifest assigns the entity.
//!
//! **Snapshot-driven compaction (§9).** When a stored snapshot advances an entity's
//! compacted base (dropping the records it subsumes), that entity's segment is rewritten
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
//! acknowledge it, so it says so. An I/O error after open **poisons** the store: the
//! cause is recorded (readable through [`FileEntityStore::failure`]) and every
//! subsequent operation refuses, [`StoreAck::Failed`] for a write,
//! [`ReadOutcome::Failed`] for a recovery read, [`BlobAck::Failed`] for a blob. Those
//! refusals do not count toward any quorum, so the node simply drops out of its
//! shards' write and recovery quorums, which their existing majority logic already
//! covers.
//!
//! Poisoning is store-wide and one-way. Store-wide because the failures it catches (a
//! full volume, a lost mount, a failing device) belong to the directory rather than to
//! the entity whose write happened to notice first. One-way because nothing here can
//! establish that the volume recovered; a poisoned store is a node to replace.
//!
//! Panicking, the alternative this replaced, converts one bad volume into the loss of
//! every shard the node leads, all of which then re-elect and rehydrate elsewhere: a
//! cluster-wide event caused by a local fault. Only [`open`](
//! FileEntityStore::open) failure is still fatal at the [`factory`](
//! FileEntityStore::factory), because a replica with no storage at all has nothing to
//! degrade *from*.
//!
//! **Single writer.** A node's directory belongs to one store at a time, enforced by
//! an advisory `flock` on `LOCK` held for the store's lifetime, so a second
//! [`open`](FileEntityStore::open) of the same directory fails by name instead of
//! quietly interleaving appends into the first's segments. Within a process the
//! [`factory`](FileEntityStore::factory) caches per node, so repeated hostings share
//! one instance and never contend for it. Each entity's mutations serialize on its own
//! segment lock, so different entities persist concurrently; the shared fence sits
//! behind its own short leaf lock.
//!
//! **Durability reporting.** The guard, the in-memory apply, and the fsync all happen
//! under the entity's segment lock before a mutating call returns, so the outcome it
//! reports is already durable. The seam is *synchronous* to keep it that way: an
//! `async fn` store would run its body at first poll, moving the fencing-critical
//! apply out from under the lock (§8).

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
use crate::entity::EntityName;
use crate::journal::RecordPage;
use crate::journal::Seq;
use crate::journal::Term;
use crate::store::BlobAck;
use crate::store::BumpRefusal;
use crate::store::EntityCheckpoint;
use crate::store::EntityRecords;
use crate::store::EntityStore;
use crate::store::EntityStoreFactory;
use crate::store::ReadOutcome;
use crate::store::ReadReply;
use crate::store::StoreAck;
use crate::store::WriteGuard;
use crate::store::WriteKind;

/// Upper bound on one framed record's payload, a sanity check while scanning: a
/// length above this is treated as corruption, not an allocation. Generous, since an
/// entity's record bytes and a whole-segment `Checkpoint` record can be large.
const MAX_RECORD: u32 = 1 << 30;

/// How many entity segments a store keeps open by default.
///
/// Each is an open file descriptor, so this is really a descriptor budget, derived
/// from the deployment's own limits: a current Linux host caps `fs.nr_open` at
/// 1,048,576 and a service unit routinely raises `LimitNOFILE` into the same range,
/// and a descriptor costs a few hundred bytes of kernel memory, so 65536 of them is
/// tens of megabytes on a node sized in the hundreds of gigabytes
/// (`docs/hardware-envelope.md` §3.2, §3.6). It leaves room for the transport's
/// connections and the blob area's transient opens, and stays far above the entities a
/// node has *active* at once, which hibernation keeps to the working set (§10).
///
/// **A deployment MUST raise `LimitNOFILE` to at least ~70000.** This budget bounds
/// what the store keeps, not what the kernel allows, and against the traditional 1024
/// or 65536 default the process would hit the kernel's limit (as an `EMFILE` on some
/// unrelated open) before this cap ever evicted anything. See
/// `docs/standalone-deployment.md`.
///
/// The cap is a bound, not an optimization (hw §5): it keeps a node that has *served*
/// millions of entities from holding a descriptor for each. A miss costs a reopen and
/// replay of one entity's segment, bounded by compaction to a checkpoint plus the
/// records above it (§9).
const DEFAULT_SEGMENT_CAPACITY: usize = 65536;

/// A [`factory`](FileEntityStore::factory)'s open stores, keyed by the pair that names
/// one store: the hosted entity type and the node. Two hostings of the same pair share
/// an instance (the directory admits one writer); two types never do (§8.2).
type StoreCache = Mutex<HashMap<(String, NodeId), Arc<FileEntityStore>>>;

/// The schema versions of this store's two log record types, stamped into each log's
/// header (compatibility spec §3). They are separate formats because the two logs
/// evolve independently: adding a field to [`SegOp`] says nothing about
/// [`ManifestEntry`].
///
/// Both are `postcard`, so neither type can gain or reorder a field within a version.
/// An enum may still grow variants at its **end**, which `postcard` encodes as a higher
/// discriminant and which no existing record can carry; that needs no bump.
const MANIFEST_RECORDS: compat::Format = compat::Format::new("entity.store.manifest", 1);
const SEGMENT_RECORDS: compat::Format = compat::Format::new("entity.store.segment", 1);

/// One mutating call on an entity's segment, as logged and replayed. Replaying a
/// segment's ops through a fresh [`EntityRecords`] reproduces its state exactly (the
/// methods are deterministic in prior state), so a reloaded segment equals the live one.
#[derive(Serialize, Deserialize)]
enum SegOp {
    /// The segment's whole state, written as the sole record when compaction rewrites
    /// it (§9). Replaying it replaces the segment's contents.
    Checkpoint(EntityCheckpoint),
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

/// One manifest entry: the segment id assigned to a `(shard, entity)`. Replaying the
/// manifest rebuilds the id map and the next free id.
#[derive(Serialize, Deserialize)]
struct ManifestEntry {
    shard: u32,
    entity: EntityName,
    id: u64,
}

/// One entity's segment: its in-memory records and its append log, behind one lock so
/// the durable append and the in-memory update stay atomic against concurrent callers
/// for *this* entity. Different entities hold different segment locks. `path` is kept for
/// the failure messages.
struct Segment {
    path: PathBuf,
    inner: Mutex<SegmentInner>,
}

struct SegmentInner {
    records: EntityRecords,
    log: Wal<SegOp>,
}

/// The manifest: the `(shard, entity) → id` map and the append log that persists new
/// assignments. `path` is kept for the failure messages.
struct Manifest {
    path: PathBuf,
    log: Wal<ManifestEntry>,
    ids: HashMap<(u32, EntityName), u64>,
    next: u64,
}

/// The production file-backed [`EntityStore`] (spec §7.2, §7.4), segmented per entity.
/// See the module docs for the layout, recovery, and failure policy.
pub struct FileEntityStore {
    dir: PathBuf,
    /// The single-writer guard (see the module docs), held open for this store's
    /// lifetime because the lock lasts exactly as long as the open file does.
    /// `None` where the platform has no advisory lock; the invariant is then
    /// documented rather than enforced.
    _lock: Option<fs::File>,
    /// The per-shard fence (§8), mirrored from `fences/<shard>`; its own leaf lock.
    fences: Mutex<HashMap<u32, Term>>,
    /// The per-shard append bound (§7.7), mirrored from `seals/<shard>`: refuse
    /// appends at or above this name hash. Durable like the fence (E15).
    seals: Mutex<HashMap<u32, u64>>,
    /// Loaded entity segments, keyed `(shard, entity)`. Populated lazily on first access.
    segments: Mutex<HashMap<(u32, EntityName), Arc<Segment>>>,
    manifest: Mutex<Manifest>,
    /// How many entity segments (and so how many open file descriptors) this store
    /// keeps loaded (see [`FileEntityStore::evict_idle_segments`]).
    segment_capacity: usize,
    /// Why this store stopped being usable, once it has (see [`FileEntityStore::fail`]).
    /// `None` while healthy. One flag for the whole store because the failures it
    /// catches (a full volume, a lost mount, a failing device) belong to the
    /// directory, not to the entity whose write happened to notice first.
    poison: Mutex<Option<String>>,
}

impl FileEntityStore {
    /// Open (creating if needed) a node's store directory: confirm it holds records
    /// this build's codec can read, then load the per-shard fences and the segment
    /// manifest, truncating any torn tail. Entity segments load lazily.
    ///
    /// `codec` is the name of the deployment's codec
    /// ([`Codec::name`](actor_serialization::Codec::name)): the one that encoded
    /// every event payload in here (§4.1, §5). It is checked against the store's
    /// stamp (§7.4) *before* any record is read, so a codec change is one refusal
    /// at startup rather than a corrupt-entity abort per activation.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::InvalidData`] when the store was written by another codec
    /// or at an `entity.store` revision this build does not accept; otherwise any
    /// filesystem error opening the directory or its index files.
    pub fn open(dir: impl Into<PathBuf>, codec: &str) -> io::Result<FileEntityStore> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        // Before anything is read (see `acquire_lock`).
        let lock = acquire_lock(&dir)?;
        // And before anything is *decoded*: the point of the stamp is that a store
        // this build cannot read is refused whole, in one place, rather than one
        // entity at a time as each activation fails to decode its own records.
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

        Ok(FileEntityStore {
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
    /// Poisoning is **one-way and store-wide** (the module's failure policy): a
    /// replica that cannot make a write durable answers [`StoreAck::Failed`] rather
    /// than panicking, dropping out of its quorums while the node keeps running.
    ///
    /// The first cause is kept and later ones ignored: the first is the one that
    /// explains the rest.
    fn fail(&self, why: String) {
        let mut poison = self.poison.lock().expect("entity store poison mutex");
        if poison.is_none() {
            *poison = Some(why);
        }
    }

    /// Whether this store has been poisoned: checked before any operation touches
    /// the disk, so a failed store does no further I/O and answers uniformly.
    fn poisoned(&self) -> bool {
        self.poison
            .lock()
            .expect("entity store poison mutex")
            .is_some()
    }

    /// Why this store is unusable, or `None` while healthy. The deployment's health
    /// probe reads this: a poisoned store is a node to replace, and nothing recovers
    /// it in place (the flag is one-way).
    pub fn failure(&self) -> Option<String> {
        self.poison
            .lock()
            .expect("entity store poison mutex")
            .clone()
    }

    /// An [`EntityStoreFactory`] rooted at `root`: each hosted entity type's records live
    /// in its own `root/<entity_type>/<node>/` directory. Caches per `(entity_type,
    /// node)` so repeated hostings in one process share a single instance (single
    /// writer); a restart constructs a fresh factory and reopens from disk. Panics if a
    /// store cannot be opened: a replica without durable storage must not start (spec
    /// §7.4).
    ///
    /// The entity type is a directory of its own rather than a key inside one store
    /// because the whole store is shard-indexed: the fence (`fences/<shard>`), the
    /// append bound (`seals/<shard>`), and the migration driver's entity enumeration all
    /// take a bare index, which names a different consensus group under each type
    /// (§8.2). Separate roots make that mismatch unrepresentable instead of leaving it
    /// to every caller to remember.
    pub fn factory(root: impl Into<PathBuf>, codec: &dyn Codec) -> EntityStoreFactory {
        let root = root.into();
        // The codec by name, taken from the value the deployment configured its
        // system with rather than from a string beside it: the stamp is only worth
        // anything if it cannot disagree with what actually encodes the records.
        let codec = codec.name().to_string();
        let cache: Arc<StoreCache> = Arc::new(Mutex::new(HashMap::new()));
        Arc::new(move |entity_type: &str, node: NodeId| {
            let mut cache = cache.lock().expect("entity store cache poisoned");
            let store = cache
                .entry((entity_type.to_string(), node))
                .or_insert_with(|| {
                    let dir = root.join(entity_type).join(node.to_string());
                    Arc::new(FileEntityStore::open(&dir, &codec).unwrap_or_else(|err| {
                        panic!("cannot open entity store at {}: {err}", dir.display())
                    }))
                })
                .clone();
            store as Arc<dyn EntityStore>
        })
    }

    /// The loaded segment for `(shard, entity)`, opening and replaying it from disk on
    /// first access, allocating a new one if the entity is unknown. Holds the segment
    /// registry lock across the (one-time) load so an entity is never opened twice. When
    /// `create` is false the read path gets `None` for an entity it has not seen, rather
    /// than allocating a segment for it.
    fn segment(&self, shard: u32, entity: &EntityName, create: bool) -> Option<Arc<Segment>> {
        if self.poisoned() {
            return None;
        }
        let mut segments = self
            .segments
            .lock()
            .expect("entity store segments poisoned");
        if let Some(segment) = segments.get(&(shard, entity.clone())) {
            return Some(Arc::clone(segment));
        }
        let id = self.segment_id(shard, entity, create)?;
        // An id is not evidence the entity still exists: the manifest is append-only, so
        // `remove_entity` leaves the assignment behind, while `open_segment` would
        // *create* the segment file it opens. Without this check a plain `read` of a
        // removed entity resurrects it on disk: the reply is still empty, but the entity
        // reappears in `entities` and its file leaks.
        if !create && !self.segment_path(id).exists() {
            return None;
        }
        // A segment that will not open poisons the store: the file is there (or should
        // be) and unreadable, which is the disk failing, not the entity being absent.
        // Returning `None` alone would read as "no such entity" and answer an empty
        // history for an entity that has one.
        let segment = match open_segment(&self.dir, id) {
            Ok(segment) => Arc::new(segment),
            Err(err) => {
                self.fail(format!(
                    "cannot open entity segment {}: {err}",
                    self.segment_path(id).display()
                ));
                return None;
            }
        };
        segments.insert((shard, entity.clone()), Arc::clone(&segment));
        Self::evict_idle_segments(&mut segments, self.segment_capacity);
        Some(segment)
    }

    /// Close segments the store is no longer using, once the loaded set exceeds
    /// `capacity`.
    ///
    /// Every loaded segment holds an **open file descriptor** (a `Wal` owns its
    /// `File`), and a hibernating entity (§10) does not touch the store, so without
    /// eviction a node accumulates one fd per distinct entity it has *ever* served: a
    /// hard ceiling at the process's descriptor limit, reached at an entity count far
    /// below anything else here is sized for, and with no back pressure before it.
    ///
    /// Closing one costs only a reopen-and-replay on the next access, which
    /// compaction bounds to a checkpoint plus the records above it (§9).
    ///
    /// **Only entries the map alone holds are evicted.** A `Segment` handed to a
    /// caller is an `Arc`: dropping the map's copy would not close the file, but it
    /// would let the next access open a *second* `Wal` on the same path, and two
    /// appending handles on one file interleave into corruption. `strong_count == 1`
    /// under the map lock rules that out, since the lock is the only way to obtain a
    /// clone. If every entry is in use the map is left over capacity rather than made
    /// unsafe; the callers are transient, so the next insert finds them idle.
    fn evict_idle_segments(
        segments: &mut HashMap<(u32, EntityName), Arc<Segment>>,
        capacity: usize,
    ) {
        if segments.len() <= capacity {
            return;
        }
        let excess = segments.len() - capacity;
        let idle: Vec<(u32, EntityName)> = segments
            .iter()
            .filter(|(_, segment)| Arc::strong_count(segment) == 1)
            .map(|(key, _)| key.clone())
            .take(excess)
            .collect();
        for key in idle {
            segments.remove(&key);
        }
    }

    /// The on-disk path of one entity's segment log, `segments/<segment id>`.
    fn segment_path(&self, seg_id: u64) -> PathBuf {
        self.dir.join("segments").join(seg_id.to_string())
    }

    /// The loaded segment for `(shard, entity)`, allocating one if the entity is unknown:
    /// the write path, where a segment is always available.
    /// `None` only when the store is poisoned: allocation itself always succeeds.
    fn segment_or_create(&self, shard: u32, entity: &EntityName) -> Option<Arc<Segment>> {
        self.segment(shard, entity, true)
    }

    /// The loaded segment for `(shard, entity)`, or `None` if the entity is unknown:
    /// the read path, which never allocates a segment for an entity it has not seen.
    fn segment_existing(&self, shard: u32, entity: &EntityName) -> Option<Arc<Segment>> {
        self.segment(shard, entity, false)
    }

    /// Project a known entity's records under its segment lock, or `None` if the entity is
    /// unknown. The shared body of every read this store answers, so the lookup, the lock,
    /// and its poison message are written once rather than per accessor.
    fn with_records<R>(
        &self,
        shard: u32,
        entity: &EntityName,
        project: impl FnOnce(&EntityRecords) -> R,
    ) -> Option<R> {
        self.segment_existing(shard, entity).map(|segment| {
            project(
                &segment
                    .inner
                    .lock()
                    .expect("entity segment poisoned")
                    .records,
            )
        })
    }

    /// The segment id for `(shard, entity)`: the existing assignment, or (when
    /// `create`) a freshly allocated one, durably appended to the manifest first.
    ///
    /// The poison check lives here rather than only in the callers because this is the
    /// one gate every area of the store passes through: records, snapshots, and the
    /// blob area alike. A poisoned store must not answer for the blob area either just
    /// because that subtree happens to still be writable: the failure it saw belongs
    /// to the volume, not to one area of it.
    ///
    /// **The manifest lock spans an fsync, and that is the node's cold-create serial
    /// point.** The `create` branch below appends to the manifest log (a durable
    /// write) while holding the mutex, so every entity whose segment is being created
    /// for the first time queues behind every other one's flush. `benches/contention.rs`
    /// measures it: first writes are flat at 63–71 per second from one thread to
    /// sixteen, where `benches/flush.rs` shows the same device sustaining roughly twice
    /// as many flushes per second once they overlap. So concurrency buys nothing here
    /// and about 2x is on the table, on the path a failover walks when it brings up
    /// every entity it just inherited.
    ///
    /// Left as it stands. Sharding the map does not help: the serialization is the
    /// flush, not the hash lookup. Releasing the lock across the flush would publish an
    /// id before the entry naming it is durable, and a restart would then use an id its
    /// manifest never recorded. What would work is group-commit: reserve ids under the
    /// lock, batch the entries, and one flush publishes them all. That is a
    /// durability-path change and wants its own review.
    ///
    /// The steady-state path is unaffected: an id already in the map returns from the
    /// hash hit above the flush, which is why `store_warm_read` is four orders faster.
    fn segment_id(&self, shard: u32, entity: &EntityName, create: bool) -> Option<u64> {
        if self.poisoned() {
            return None;
        }
        let mut manifest = self
            .manifest
            .lock()
            .expect("entity store manifest poisoned");
        if let Some(id) = manifest.ids.get(&(shard, entity.clone())) {
            return Some(*id);
        }
        if !create {
            return None;
        }
        let id = manifest.next;
        let path = manifest.path.clone();
        if let Err(err) = manifest.log.append(&ManifestEntry {
            shard,
            entity: entity.clone(),
            id,
        }) {
            // The id is not consumed and the mapping is not published: an unrecorded
            // assignment must not survive in memory, or this store would use an id its
            // manifest never named and a restart would lose the entity's segment.
            drop(manifest);
            self.fail(format!(
                "entity store manifest persistence failed at {}: {err}",
                path.display()
            ));
            return None;
        }
        manifest.next += 1;
        manifest.ids.insert((shard, entity.clone()), id);
        Some(id)
    }

    /// An entity's content-addressed blob subtree, `blobs/<segment id>/`: the single
    /// place the on-disk blob layout is spelled (durable-workspace design).
    fn blob_dir(&self, seg_id: u64) -> PathBuf {
        self.dir.join("blobs").join(seg_id.to_string())
    }

    /// The on-disk path of one blob, `blobs/<segment id>/<blob hex>`.
    fn blob_path(&self, seg_id: u64, id: BlobId) -> PathBuf {
        self.blob_dir(seg_id).join(id.to_string())
    }

    /// Rewrite an entity's segment to a single `Checkpoint` of its current state, folding
    /// away the record ops a snapshot made redundant (§9), and swap in the fresh append
    /// handle. Called under the held segment lock so no append races the rewrite.
    ///
    /// Returns whether the rewrite succeeded. A failure poisons the store: the
    /// rewrite is atomic (the old segment survives on any error), so nothing is
    /// corrupt, but the volume that could not take a compaction cannot take the
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
                    "entity store compaction failed at {}: {err}",
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
/// replacement. Being advisory, it stops another *`FileEntityStore`*, not a stray `rm`.
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
                    "entity store at {} is already open: a node's directory belongs to one \
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
    compat::Stamp::new(b"GRSTOR", compat::Format::new("entity.store", 1));

/// The stamp file's name inside the store directory.
const STORE_FILE: &str = "store";

/// The stamp's body: which codec wrote everything under this directory.
#[derive(Serialize, Deserialize)]
struct StoreBody {
    /// The deployment codec that encoded this store's event payloads (§4.1, §5).
    ///
    /// Facet payloads are `postcard` by construction and a snapshot carries its own
    /// copy of this (§7.12), but an entity's *events* are user types under the
    /// deployment's codec, and an entity with records past its last snapshot (or no
    /// snapshot at all) has nothing else that would notice the codec changing.
    codec: String,
}

/// The stamp on a `fences/<shard>` file (compatibility §3, `entity.store.fence`).
///
/// A per-file stamp rather than a field of [`STORE`]'s. A fence file is a durable
/// byte sequence in its own right (restored from a backup, copied between
/// directories), and delegating its identity to a neighbouring file is what a stamp
/// exists to stop (**V1**). The cost rounds to nothing: one file per shard,
/// rewritten only when the term advances, on a write whose real expense is the
/// fsync of a block it already occupies.
pub(crate) const FENCE: wal::Sidecar = wal::Sidecar::new(compat::Stamp::new(
    b"GRFNCE",
    compat::Format::new("entity.store.fence", 1),
));

/// The stamp on a `seals/<shard>` file (compatibility §3, `entity.store.seal`).
///
/// A separate format from [`FENCE`] for the reason [`MANIFEST_RECORDS`] and
/// [`SEGMENT_RECORDS`] are separate: a fence term (§8) and an append bound (§7.7)
/// mean different things and evolve apart. They share a body and a reader
/// (`wal::Sidecar`, wal §5), not a version.
pub(crate) const SEAL: wal::Sidecar = wal::Sidecar::new(compat::Stamp::new(
    b"GRSEAL",
    compat::Format::new("entity.store.seal", 1),
));

/// Confirm `dir` holds records `codec` can read, stamping a fresh directory.
///
/// A directory with no `store` file is a fresh store: the codec running now is
/// written down, and every later open is held to it.
fn admit_store_stamp(dir: &Path, codec: &str) -> io::Result<()> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    let path = dir.join(STORE_FILE);
    match fs::read(&path) {
        Ok(bytes) => {
            // Version first, then the body: nothing reads a field until the bytes
            // have been admitted as this format at all.
            let body = STORE
                .unstamp(&bytes)
                .map_err(|err| invalid(format!("entity store at {}: {err}", dir.display())))?;
            let body: StoreBody = postcard::from_bytes(body).map_err(|err| {
                invalid(format!(
                    "entity store at {}: stamp body did not decode: {err}",
                    dir.display()
                ))
            })?;
            if body.codec != codec {
                return Err(invalid(format!(
                    "entity store at {}: written with codec '{}', but this node runs \
                     '{codec}': an entity's event payloads are codec-encoded (§4.1), so \
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
            })
            .expect("the stamp body is plain owned data");
            wal::atomic_replace(dir, STORE_FILE, &STORE.stamp(&body))
        }
        Err(err) => Err(err),
    }
}

/// Load every `fences/<shard>` file into a shard→term map (eager: there are few shards
/// per node, and the fence must be known before any entity's records load).
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
    let (log, entries) = Wal::<ManifestEntry>::open(&path, MAX_RECORD, &MANIFEST_RECORDS)?;
    let mut ids = HashMap::new();
    let mut next = 0u64;
    for entry in entries {
        next = next.max(entry.id + 1);
        ids.insert((entry.shard, entry.entity), entry.id);
    }
    Ok(Manifest {
        path,
        log,
        ids,
        next,
    })
}

/// Open and replay an entity's segment file, truncating any torn tail. A `Checkpoint`
/// loads the whole segment state; every other op is re-applied to it in log order.
fn open_segment(dir: &Path, id: u64) -> io::Result<Segment> {
    let path = dir.join("segments").join(id.to_string());
    let (log, ops) = Wal::<SegOp>::open(&path, MAX_RECORD, &SEGMENT_RECORDS)?;
    let mut records = EntityRecords::default();
    for op in ops {
        match op {
            SegOp::Checkpoint(checkpoint) => records = EntityRecords::from_checkpoint(checkpoint),
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
/// fences: the bound must be known before any entity's appends are served).
fn load_seals(dir: &Path) -> io::Result<HashMap<u32, u64>> {
    let mut seals = HashMap::new();
    for entry in fs::read_dir(dir.join("seals"))? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(shard) = name.to_str().and_then(|s| s.parse::<u32>().ok()) else {
            continue;
        };
        if let Some(bound) = SEAL.read(&entry.path())? {
            seals.insert(shard, bound);
        }
    }
    Ok(seals)
}

/// Atomically persist a shard's fence term, stamped `entity.store.fence`.
fn write_fence(dir: &Path, shard: u32, term: Term) -> io::Result<()> {
    FENCE.write(&dir.join("fences").join(shard.to_string()), term.value())
}

/// Atomically persist a shard's append bound, stamped `entity.store.seal`.
fn write_seal(dir: &Path, shard: u32, bound: u64) -> io::Result<()> {
    SEAL.write(&dir.join("seals").join(shard.to_string()), bound)
}

/// Read a shard's fence term, or `None` if the file is absent.
///
/// `None` means one thing only: **the file is absent** (wal §5). `load_fences`
/// reads it as *no fence for this shard*, so a malformed sidecar fails
/// [`FileEntityStore::open`] rather than opening a store with a fence quietly
/// missing, which would withdraw **E1**'s single-writer guarantee with no
/// diagnostic; the same direction the Raft term file already fails in.
fn read_fence(path: &Path) -> io::Result<Option<Term>> {
    Ok(FENCE.read(path)?.map(Term::new))
}

impl WriteGuard for FileEntityStore {
    fn seal_bound(&self, shard: u32) -> Option<u64> {
        self.seals
            .lock()
            .expect("entity store seals poisoned")
            .get(&shard)
            .copied()
    }

    /// Persists the bump before returning. The fence file is rewritten only when the
    /// term actually advances, so a steady-state append (same term) never touches it.
    fn bump_fence(&self, shard: u32, term: Term) -> Result<(), BumpRefusal> {
        let mut fences = self.fences.lock().expect("entity store fences poisoned");
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
                    "entity store fence persistence failed at {}: {err}",
                    self.dir.display()
                ));
                return Err(BumpRefusal::Failed);
            }
            fences.insert(shard, term);
        }
        Ok(())
    }
}

impl FileEntityStore {
    /// Drop the live segment handle, its on-disk log, and the blob subtree of one
    /// entity. The manifest keeps the id assignment (append-only); a later access
    /// reopens an empty segment under the same id, which reads as absent.
    fn remove_entity(&self, shard: u32, entity: &EntityName) {
        let Some(seg_id) = self.segment_id(shard, entity, false) else {
            return;
        };
        let mut segments = self
            .segments
            .lock()
            .expect("entity store segments poisoned");
        segments.remove(&(shard, entity.clone()));
        let _ = fs::remove_file(self.segment_path(seg_id));
        let _ = fs::remove_dir_all(self.blob_dir(seg_id));
    }
}

// This store fsyncs synchronously inside each call, so an outcome it settles is
// already stable when it returns.
impl EntityStore for FileEntityStore {
    fn put_blob(&self, shard: u32, entity: &EntityName, id: BlobId, bytes: Vec<u8>) -> BlobAck {
        // One content-addressed file per blob, persisted with the same atomic
        // write-and-fsync the fence uses: no ack for a blob that is not durable.
        let Some(seg_id) = self.segment_id(shard, entity, true) else {
            return BlobAck::Failed; // poisoned; `segment_id` said why
        };
        let dir = self.blob_dir(seg_id);
        let name = id.to_string();
        // Idempotent: equal content under the same id is already durable (E18).
        if dir.join(&name).exists() {
            return BlobAck::Stored;
        }
        if let Err(err) = fs::create_dir_all(&dir) {
            self.fail(format!(
                "entity store blob dir failed at {}: {err}",
                dir.display()
            ));
            return BlobAck::Failed;
        }
        if let Err(err) = wal::atomic_replace(&dir, &name, &bytes) {
            self.fail(format!(
                "entity store blob persistence failed at {}: {err}; a replica that \
                 cannot persist a blob cannot safely acknowledge it",
                dir.display()
            ));
            return BlobAck::Failed;
        }
        BlobAck::Stored
    }

    fn get_blob(&self, shard: u32, entity: &EntityName, id: BlobId) -> Option<Vec<u8>> {
        let seg_id = self.segment_id(shard, entity, false)?;
        let path = self.blob_path(seg_id, id);
        match fs::read(&path) {
            Ok(bytes) => Some(bytes),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            // Unreadable is not absent, but the read path cannot tell the caller so:
            // it answers `None`, and the caller falls through to a verifying replica
            // (**E17**). Poisoning is what keeps that honest: this store stops
            // claiming to hold anything rather than reporting a blob it cannot serve.
            Err(err) => {
                self.fail(format!(
                    "entity store blob read failed at {}: {err}",
                    path.display()
                ));
                None
            }
        }
    }

    fn has_blob(&self, shard: u32, entity: &EntityName, id: BlobId) -> bool {
        let Some(seg_id) = self.segment_id(shard, entity, false) else {
            return false;
        };
        self.blob_path(seg_id, id).exists()
    }

    fn delete_blob(&self, shard: u32, entity: &EntityName, id: BlobId) {
        if let Some(seg_id) = self.segment_id(shard, entity, false) {
            // Best-effort: removing a corrupt copy so the read path can re-store a
            // good one (§7.10 self-heal). A missing file is already done.
            let _ = fs::remove_file(self.blob_path(seg_id, id));
        }
    }

    fn delete_blobs(&self, shard: u32, entity: &EntityName) {
        if let Some(seg_id) = self.segment_id(shard, entity, false) {
            // Reclamation is best-effort (a leaked blob is harmless, only space): a
            // missing subtree is already-done, any other error is left for a later
            // sweep.
            let _ = fs::remove_dir_all(self.blob_dir(seg_id));
        }
    }

    fn retain_blobs(&self, shard: u32, entity: &EntityName, retain: &BTreeSet<BlobId>) {
        let Some(seg_id) = self.segment_id(shard, entity, false) else {
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

    fn blob_ids(&self, shard: u32, entity: &EntityName) -> Vec<BlobId> {
        let Some(seg_id) = self.segment_id(shard, entity, false) else {
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

    fn store_record(
        &self,
        shard: u32,
        entity: &EntityName,
        after: Seq,
        term: Term,
        records: Vec<Vec<u8>>,
        kind: WriteKind,
    ) -> StoreAck {
        let Some(segment) = self.segment_or_create(shard, entity) else {
            return StoreAck::Failed;
        };
        // Guard and apply under the segment lock, durable fence bump included, so a
        // concurrent `prepare` cannot slip between them (the fencing race, §8).
        let mut inner = segment.inner.lock().expect("entity segment poisoned");
        if let Err(ack) = self.guard_record(shard, entity, term, kind) {
            return ack;
        }
        // Durable before in-memory, so a record this store reports is one it holds. On
        // a write failure the in-memory apply is skipped and the store is poisoned:
        // refusing is the only honest answer, since acknowledging a record that is not
        // on disk shrinks a committed write's durability below its quorum (**E14**).
        // Framed once and taken apart again, rather than cloned for the frame: `append`
        // only borrows the op, and the in-memory apply below wants the same batch by
        // value. The batch is an entity's whole write, a mebibyte for a disk block.
        let op = SegOp::Record {
            after,
            term,
            records,
            kind,
        };
        if let Err(err) = inner.log.append(&op) {
            self.fail(format!(
                "entity store persistence failed at {}: {err}; a replica that cannot \
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

    fn read(&self, shard: u32, entity: &EntityName) -> ReadReply {
        self.with_records(shard, entity, EntityRecords::read)
            .unwrap_or_default()
    }

    // Answered from the segment directly, not through `read`; see the note on
    // `MemoryEntityStore`'s pair. Activation asks for both before an entity serves anything,
    // and through `read` each cloned the entity's whole record set to reach one field.

    fn head(&self, shard: u32, entity: &EntityName) -> Seq {
        self.with_records(shard, entity, EntityRecords::head)
            .unwrap_or(Seq::ZERO)
    }

    fn snapshot(&self, shard: u32, entity: &EntityName) -> Option<(Seq, Vec<u8>)> {
        self.with_records(shard, entity, EntityRecords::snapshot)
            .flatten()
    }

    fn read_from(&self, shard: u32, entity: &EntityName, from: Seq, limit: usize) -> RecordPage {
        self.with_records(shard, entity, |records| records.read_from(from, limit))
            .unwrap_or_default()
    }

    // The reads above answer a poisoned store as an entity with no history (`segment`
    // yields `None`); this is how a journal tells that apart from a real empty one.
    fn failed(&self) -> bool {
        self.poisoned()
    }

    fn prepare(&self, shard: u32, entity: &EntityName, term: Term) -> ReadOutcome {
        // The promise (the fence bump) must be durable before it is made, else a
        // restart could forget it and let a deposed leader commit (§8). The segment is
        // created even for an entity never seen here: holding its lock across the bump
        // and the read is what makes the promise and the returned view atomic against a
        // concurrent first append; a lock-free empty reply could miss a lower-term
        // record stored and acked in the window.
        let Some(segment) = self.segment_or_create(shard, entity) else {
            return ReadOutcome::Failed;
        };
        let inner = segment.inner.lock().expect("entity segment poisoned");
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
        entity: &EntityName,
        at: Seq,
        term: Term,
        state: Vec<u8>,
        kind: WriteKind,
    ) -> StoreAck {
        let Some(segment) = self.segment_or_create(shard, entity) else {
            return StoreAck::Failed;
        };
        let mut inner = segment.inner.lock().expect("entity segment poisoned");
        if let Err(ack) = self.guard_snapshot(shard, term, kind) {
            return ack;
        }
        let advanced = inner.records.store_snapshot(at, term, state);
        // A snapshot that advanced the base just compacted the records it subsumes
        // (§9): rewrite this entity's segment to a single checkpoint that embeds the
        // snapshot. One that did *not* advance writes nothing durable (module docs).
        if advanced && !self.checkpoint(&segment, &mut inner) {
            return StoreAck::Failed;
        }
        StoreAck::Stored(at)
    }

    fn truncate(&self, shard: u32, entity: &EntityName, after: Seq, term: Term) {
        // An entity this store holds nothing for has no tail to drop, and truncating one
        // must not bring it into existence: it would then be enumerated by `entities` and
        // migrated as if it held data.
        let Some(segment) = self.segment_existing(shard, entity) else {
            return;
        };
        let mut inner = segment.inner.lock().expect("entity segment poisoned");
        // A truncate that is not durable must not be applied in memory either: the
        // rollback exists so a later quorum-less recovery does not fold an uncommitted
        // record (§7.2), and dropping it only in memory would restore it on restart.
        if let Err(err) = inner.log.append(&SegOp::Truncate { after, term }) {
            self.fail(format!(
                "entity store persistence failed at {}: {err}",
                segment.path.display()
            ));
            return;
        }
        inner.records.truncate(after, term);
    }

    fn entities(&self, shard: u32) -> Vec<EntityName> {
        // The manifest assigns a segment id on the first record OR the first blob
        // (`put_blob` allocates through the same map), so its keys are the union of the
        // two areas, but only ever *grow*: the log is append-only, so `remove_entity`
        // leaves the id assignment behind (it must, to keep segment filenames
        // collision-free). The manifest is therefore only the candidate list; the
        // presence of the entity's own files is the answer.
        let ids: Vec<(EntityName, u64)> = {
            let manifest = self
                .manifest
                .lock()
                .expect("entity store manifest poisoned");
            manifest
                .ids
                .iter()
                .filter(|((s, _), _)| *s == shard)
                .map(|((_, entity), &id)| (entity.clone(), id))
                .collect()
        };
        ids.into_iter()
            .filter(|(_, id)| {
                // Records or blobs: either makes the entity held. A blob-only entity
                // has no segment file, since `put_blob` allocates an id without
                // opening the segment.
                self.segment_path(*id).exists() || self.blob_dir(*id).exists()
            })
            .map(|(entity, _)| entity)
            .collect()
    }

    fn seal_range(&self, shard: u32, from: u64) {
        let mut seals = self.seals.lock().expect("entity store seals poisoned");
        // Monotone tighten, persisted before it is honoured: the bound is a
        // promise (like the fence) and must survive a restart, else a stale
        // leader could assemble a majority for the moved range afterward (E15).
        let bound = seals.get(&shard).map_or(from, |&cur| cur.min(from));
        if seals.get(&shard) != Some(&bound) {
            // In memory only after the file. `seal_range` has no refusal to report,
            // but the seal's majority stays sound anyway: poisoning makes this store
            // answer `Failed` to *every* subsequent append, which refuses strictly
            // more than the bound would, so counting it toward the seal cannot let a
            // moved-range write through.
            if let Err(err) = write_seal(&self.dir, shard, bound) {
                self.fail(format!(
                    "entity store seal persistence failed at {}: {err}; a replica \
                     that cannot persist the bound cannot safely promise it",
                    self.dir.display()
                ));
                return;
            }
            seals.insert(shard, bound);
        }
    }

    fn unseal(&self, shard: u32) {
        let mut seals = self.seals.lock().expect("entity store seals poisoned");
        if seals.remove(&shard).is_some() {
            // Best-effort removal: a leftover file re-seals on reopen, which a
            // re-applied merge commit clears again; conservative, never unsafe.
            let _ = fs::remove_file(self.dir.join("seals").join(shard.to_string()));
        }
    }

    /// Enumerate-and-remove, because an entity owns a file: the range is expressible
    /// only as the set of files in it.
    fn remove_range(&self, shard: u32, from: u64) {
        for entity in self.entities(shard) {
            if crate::system::name_at_or_above(&entity, from) {
                self.remove_entity(shard, &entity);
            }
        }
    }

    fn drop_shard(&self, shard: u32) {
        for entity in self.entities(shard) {
            self.remove_entity(shard, &entity);
        }
        // The fence and the bound go with the data (see `MemoryEntityStore`), so a
        // shard index reused later starts from a clean promise rather than one this
        // shard's retired leader made.
        self.fences
            .lock()
            .expect("entity store fences poisoned")
            .remove(&shard);
        let _ = fs::remove_file(self.dir.join("fences").join(shard.to_string()));
        self.seals
            .lock()
            .expect("entity store seals poisoned")
            .remove(&shard);
        let _ = fs::remove_file(self.dir.join("seals").join(shard.to_string()));
    }

    fn shard_bytes(&self, shard: u32) -> u64 {
        // File sizes, not in-memory sizes: segments load lazily, and the trigger
        // needs the durable footprint anyway.
        //
        // Restating the filesystem each time, rather than maintaining a counter. A
        // `stat` served from the dentry cache is around a microsecond, so a shard
        // holding 100k entities costs ~100 ms of one core per sweep, and the sweep runs
        // every SPLIT_TRIGGER_INTERVAL (30 s) on a node with cores to spare
        // (`docs/hardware-envelope.md` §3.3). A maintained counter would be a number
        // that can drift from the filesystem, on the one path whose job is to describe
        // it. **Revisit when a sweep exceeds a second.**
        let ids: Vec<u64> = {
            let manifest = self
                .manifest
                .lock()
                .expect("entity store manifest poisoned");
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
    use crate::store::MemoryEntityStore;
    use actor_serialization::JsonCodec;
    use std::fs::OpenOptions;
    use std::io::Write;

    /// The codec these tests open every store under. Which one is immaterial to
    /// everything below except the stamp's own tests: what matters is that a
    /// reopen uses the *same* one, since disagreeing is exactly what the stamp
    /// refuses.
    const TEST_CODEC: &str = "json";

    fn name(key: &str) -> EntityName {
        EntityName::new("test.Entity", key)
    }

    // --- The store stamp (spec §7.4, compatibility spec §3.4) -----------------

    #[test]
    fn a_fresh_store_is_stamped_and_reopens_under_the_same_codec() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileEntityStore::open(dir.path(), TEST_CODEC).expect("open a fresh store");
        assert!(
            dir.path().join(STORE_FILE).exists(),
            "a fresh store must record which codec wrote it",
        );
        drop(store);
        FileEntityStore::open(dir.path(), TEST_CODEC).expect("the same codec must reopen it");
    }

    #[test]
    fn a_codec_change_is_refused_once_at_open_naming_both() {
        // The whole point: a swapped codec is *one* refusal here, not a corrupt-entity
        // abort per activation for every entity with records past its last snapshot.
        let dir = tempfile::tempdir().unwrap();
        let store = FileEntityStore::open(dir.path(), "json").expect("open");
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

        let Err(err) = FileEntityStore::open(dir.path(), "postcard") else {
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
    fn a_store_from_another_version_is_refused_as_a_version() {
        // Not as a corrupt store, and without the body being decoded at all: the
        // version is admitted before anything downstream sees a byte (**V2**).
        let dir = tempfile::tempdir().unwrap();
        FileEntityStore::open(dir.path(), TEST_CODEC).expect("open");
        let mut bytes = std::fs::read(dir.path().join(STORE_FILE)).expect("read the stamp");
        // The version is a little-endian `u16` immediately after the magic.
        let head = b"GRSTOR".len();
        bytes[head..head + 2].copy_from_slice(&9u16.to_le_bytes());
        std::fs::write(dir.path().join(STORE_FILE), &bytes).expect("rewrite the stamp");

        let Err(err) = FileEntityStore::open(dir.path(), TEST_CODEC) else {
            panic!("a revision this build does not accept must not open");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("entity.store") && msg.contains("v9"),
            "the refusal must name the format and what it found: {msg}",
        );
    }

    #[test]
    fn an_unstamped_store_is_refused_rather_than_guessed_at() {
        // A `store` file that is not a stamped record is not a fresh store and not
        // this build's: the codec that wrote the directory is unknown, and guessing
        // it is the per-entity failure storm the stamp exists to prevent.
        let dir = tempfile::tempdir().unwrap();
        drop(FileEntityStore::open(dir.path(), TEST_CODEC).expect("open"));
        std::fs::write(dir.path().join(STORE_FILE), b"json").expect("un-stamp the store");

        let Err(err) = FileEntityStore::open(dir.path(), TEST_CODEC) else {
            panic!("an unstamped store file must not open");
        };
        assert!(
            err.to_string().contains("entity.store"),
            "the refusal must name the format: {err}",
        );
    }

    /// Stage `bytes` as shard 0's file under `dir/<kind>`, on an already-opened
    /// store directory.
    fn stage_sidecar(dir: &Path, kind: &str, bytes: &[u8]) {
        std::fs::write(dir.join(kind).join("0"), bytes).expect("stage a sidecar");
    }

    /// A stamped fence or seal, encoded the way this build writes one.
    fn stamped_sidecar(sidecar: &wal::Sidecar, value: u64) -> Vec<u8> {
        sidecar.encode(value)
    }

    /// **The load-bearing one.** Every representable way of getting a fence file
    /// wrong must fail `open` by name, not read as *no fence for this shard*, which
    /// would withdraw **E1** with no diagnostic. The store must not come up with the
    /// fence quietly missing.
    #[test]
    fn a_fence_this_build_cannot_read_is_refused_rather_than_read_as_no_fence() {
        let mut future = stamped_sidecar(&FENCE, 7);
        let head = FENCE.stamp().magic().len();
        future[head..head + 2].copy_from_slice(&9u16.to_le_bytes());

        let mut torn = stamped_sidecar(&FENCE, 7);
        let last = torn.len() - 1;
        torn[last] ^= 0xff;

        let mut bare = 7u64.to_le_bytes().to_vec();
        bare.extend_from_slice(&wal::checksum(&7u64.to_le_bytes()).to_le_bytes());

        // A good fence with something after it: the body this build writes ends where
        // its fields end, so a decoder that ignores the remainder reads a foreign or
        // half-rewritten file as a fence of its own.
        let mut long = stamped_sidecar(&FENCE, 7);
        long.extend_from_slice(b"appended");

        for (what, bytes) in [
            ("a version this build does not accept", future),
            ("a stamped record whose body is damaged", torn),
            ("a bare value with no stamp", bare),
            ("a file of no recognized shape", vec![0u8; 20]),
            ("a stamped record with bytes appended", long),
        ] {
            let dir = tempfile::tempdir().unwrap();
            drop(FileEntityStore::open(dir.path(), TEST_CODEC).expect("open"));
            stage_sidecar(dir.path(), "fences", &bytes);

            let Err(err) = FileEntityStore::open(dir.path(), TEST_CODEC) else {
                panic!("{what} must refuse, not open with no fence for the shard");
            };
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            assert!(
                err.to_string().contains("entity.store.fence"),
                "{what}: the refusal must name the format: {err}",
            );
        }
    }

    #[test]
    fn a_fence_and_a_seal_are_written_stamped() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileEntityStore::open(dir.path(), TEST_CODEC).expect("open");
        store.bump_fence(0, Term::new(3)).expect("bump");
        store.seal_range(0, 99);
        drop(store);

        for (kind, sidecar) in [("fences", &FENCE), ("seals", &SEAL)] {
            let bytes = std::fs::read(dir.path().join(kind).join("0")).expect("read the sidecar");
            sidecar
                .stamp()
                .unstamp(&bytes)
                .unwrap_or_else(|err| panic!("{kind} must be written stamped: {err}"));
        }

        // And what it wrote, it reads back.
        let store = FileEntityStore::open(dir.path(), TEST_CODEC).expect("reopen");
        assert!(
            matches!(
                store.bump_fence(0, Term::new(2)),
                Err(BumpRefusal::Fenced(fence)) if fence == Term::new(3)
            ),
            "the fence must read back as it was written",
        );
        assert_eq!(store.seals.lock().unwrap().get(&0).copied(), Some(99));
    }

    /// The single-writer rule, enforced rather than documented.
    #[cfg(unix)]
    #[test]
    fn a_second_open_of_one_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let first = FileEntityStore::open(dir.path(), TEST_CODEC).expect("first open");
        assert!(
            FileEntityStore::open(dir.path(), TEST_CODEC).is_err(),
            "a second store opened a directory another already holds"
        );
        // Released with the store, so a replacement can take over.
        drop(first);
        FileEntityStore::open(dir.path(), TEST_CODEC).expect("reopen once the holder is gone");
    }

    #[test]
    fn records_round_trip_across_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("a");
        {
            let store = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
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
        let reopened = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
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
    fn a_snapshot_compacts_one_entities_segment_on_disk_and_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("a");
        let store = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
        // Grow the entity's segment with many sizeable records.
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
            "snapshot-driven compaction shrank the entity's segment: {after} < {before}"
        );
        drop(store);

        // The compacted segment reloads the snapshot and the (now empty) live tail.
        let reopened = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
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
    fn one_entities_snapshot_leaves_another_entities_segment_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (name("a"), name("b"));
        let store = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
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
        // Compacting entity `a` must not rewrite entity `b`'s segment.
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
            "entity b's segment was rewritten"
        );
    }

    #[test]
    fn a_redundant_snapshot_writes_nothing_and_does_not_bloat_the_segment() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("a");
        let store = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
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
        // Re-storing the same (non-advancing) snapshot many times (as repeated
        // re-activations would) must write nothing: the segment file does not grow.
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
        let reopened = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
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
            let store = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
            // Bound the whole space: every append on shard 0 is refused.
            store.seal_range(0, 0);
        }
        // The bound is a durable promise (E15): a reopen must not forget it, or a
        // stale leader could assemble a majority for the moved range afterward.
        let reopened = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
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
        let again = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
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
    fn remove_entity_drops_the_segment_and_blobs_durably() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("moved");
        {
            let store = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
            let _ = store.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(1),
                vec![b"e1".to_vec()],
                WriteKind::Append,
            );
            let _ = store.put_blob(0, &n, BlobId::of(b"b"), b"b".to_vec());
            store.remove_entity(0, &n);
            assert!(store.read(0, &n).slots.is_empty());
            assert!(!store.has_blob(0, &n, BlobId::of(b"b")));
        }
        // Durable: the reopened store does not resurrect the entity.
        let reopened = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
        assert!(reopened.read(0, &n).slots.is_empty());
        assert!(reopened.blob_ids(0, &n).is_empty());
    }

    #[test]
    fn the_fence_survives_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let n = name("a");
        {
            let store = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
            // A recovery prepare at term 5 promises not to accept a lower term.
            assert!(matches!(
                store.prepare(0, &n, Term::new(5)),
                ReadOutcome::Prepared(_)
            ));
        }
        // The promise is durable: after reopen, a term-4 write is still fenced.
        let reopened = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
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

    /// The fence is keyed by shard *index*, but a shard is `(entity_type, index)` and
    /// each pair is its own leader-election group with its own term (§8.2). A factory
    /// that handed both types one store would let the type that elects more often push
    /// `fences/<index>` past the other's term, and the quieter type's appends and
    /// recovery reads are then refused for good: its entities stop activating at all.
    #[test]
    fn two_entity_types_do_not_share_a_shards_fence() {
        let root = tempfile::tempdir().unwrap();
        let factory = FileEntityStore::factory(root.path().to_path_buf(), &JsonCodec);
        let node = NodeId::new(1);
        let busy = factory("busy.Type", node);
        let quiet = factory("quiet.Type", node);

        // The busy type's shard 0 reaches term 9 and fences its own store there.
        assert!(matches!(
            busy.prepare(0, &EntityName::new("busy.Type", "a"), Term::new(9)),
            ReadOutcome::Prepared(_)
        ));
        // The quiet type's shard 0 is a different group, still at term 2. Its append
        // must land: the busy type's term says nothing about who leads this shard.
        assert_eq!(
            quiet.store_record(
                0,
                &EntityName::new("quiet.Type", "a"),
                Seq::ZERO,
                Term::new(2),
                vec![b"mine".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stored(Seq::new(1))
        );
        // And its leader can still recover the entity on the next activation.
        assert!(matches!(
            quiet.prepare(0, &EntityName::new("quiet.Type", "a"), Term::new(2)),
            ReadOutcome::Prepared(_)
        ));
    }

    /// Same node, same type: one store, so a restart in-process keeps the single-writer
    /// rule (a second `open` of one directory is refused).
    #[test]
    fn one_store_per_entity_type_and_node() {
        let root = tempfile::tempdir().unwrap();
        let factory = FileEntityStore::factory(root.path().to_path_buf(), &JsonCodec);
        let first = factory("a.Type", NodeId::new(1));
        let second = factory("a.Type", NodeId::new(1));
        assert!(
            Arc::ptr_eq(&first, &second),
            "a repeated hosting of one type on one node must share its store"
        );
    }

    #[test]
    fn prepare_creates_and_locks_the_segment_of_an_unseen_entity() {
        // The fencing race (§8): prepare's promise and its returned (empty) view must
        // be atomic against a concurrent first append, which requires taking (and so
        // creating) the entity's segment. Without it, a term-1 first append could be
        // stored and acked after a term-2 prepare returned empty.
        let dir = tempfile::tempdir().unwrap();
        let n = name("fresh");
        let store = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
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
    // lock discipline itself, which an async task cannot exercise; both calls must
    // genuinely contend on the segment and fence mutexes from separate threads.
    #[allow(clippy::disallowed_methods)]
    fn a_prepare_append_race_never_hides_a_stored_record() {
        // Loop a first-ever append (term 1) against a prepare (term 2) on a fresh
        // entity from two threads. Invariant: if the append was `Stored`, the
        // prepare either fenced it first or observed it; a `Prepared` EMPTY view
        // alongside a `Stored` ack is the quorum-intersection violation.
        use std::sync::Barrier;
        for round in 0..64 {
            let dir = tempfile::tempdir().unwrap();
            let store = std::sync::Arc::new(FileEntityStore::open(dir.path(), TEST_CODEC).unwrap());
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
    fn a_fence_promise_on_an_unseen_entity_is_durable() {
        let dir = tempfile::tempdir().unwrap();
        {
            // Prepare an entity that has no records yet: the promise is the shard fence,
            // which must survive even though no segment was ever written.
            let store = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
            assert!(matches!(
                store.prepare(0, &name("ghost"), Term::new(7)),
                ReadOutcome::Prepared(_)
            ));
        }
        let reopened = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
        // A different entity in the same shard is fenced by the recovered promise.
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
            let store = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
            let _ = store.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(1),
                vec![b"e1".to_vec()],
                WriteKind::Append,
            );
        }
        // A torn write: garbage lands after the valid record in the entity's segment.
        let id = {
            let store = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
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

        let reopened = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
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
        let again = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
        assert_eq!(again.read(0, &n).slots.len(), 2);
    }

    /// The differential workhorse: drive the same op sequence through `FileEntityStore`
    /// (reopening from disk before every step), and a `MemoryEntityStore` mirror;
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
        let mirror = MemoryEntityStore::new();
        for (step, op) in ops.iter().enumerate() {
            // A fresh open every step: the state must come back from disk.
            let file = FileEntityStore::open(dir.path(), TEST_CODEC).unwrap();
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

    /// The descriptor bound: serving unboundedly many distinct entities must not leave
    /// unboundedly many segments (and so file descriptors) open.
    ///
    /// Without eviction a long-lived node holds one open fd per entity it has ever
    /// touched: hibernation stops the entity's host, which never touches the store.
    #[test]
    fn the_loaded_segment_set_stays_bounded_across_many_entities() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = FileEntityStore::open(dir.path(), TEST_CODEC).expect("open");
        store.segment_capacity = 16;
        for i in 0..500 {
            let entity = name(&format!("entity-{i}"));
            let _ = store.store_record(
                0,
                &entity,
                Seq::ZERO,
                Term::new(1),
                vec![b"x".to_vec()],
                WriteKind::Append,
            );
        }
        let loaded = store.segments.lock().expect("segments").len();
        assert!(
            loaded <= 16,
            "500 entities left {loaded} segments open against a capacity of 16"
        );
    }

    /// Eviction must never close a segment a caller still holds: two `Wal` handles
    /// appending to one file interleave into corruption, which is what a second
    /// `open_segment` on an evicted-but-live path would produce.
    #[test]
    fn a_segment_in_use_is_never_evicted() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = FileEntityStore::open(dir.path(), TEST_CODEC).expect("open");
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
            let entity = name(&format!("other-{i}"));
            let _ = store.store_record(
                0,
                &entity,
                Seq::ZERO,
                Term::new(1),
                vec![b"y".to_vec()],
                WriteKind::Append,
            );
        }
        let same = store.segment(0, &held, false).expect("still resolvable");
        assert!(
            Arc::ptr_eq(&live, &same),
            "a held segment stayed the same instance: a second Wal on its file would corrupt it"
        );
        drop(live);
        drop(same);
        // Once released it becomes evictable like any other, so holding one entry
        // does not defeat the bound.
        for i in 50..120 {
            let entity = name(&format!("other-{i}"));
            let _ = store.store_record(
                0,
                &entity,
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
    /// cache decision, and the file is the truth (**E3**).
    #[test]
    fn an_evicted_segment_reopens_with_its_records_intact() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = FileEntityStore::open(dir.path(), TEST_CODEC).expect("open");
        store.segment_capacity = 1;
        let entity = name("survivor");
        let _ = store.store_record(
            0,
            &entity,
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
        let reply = store.read(0, &entity);
        assert_eq!(
            reply.slots.len(),
            2,
            "the reopened segment holds both records"
        );
        assert_eq!(reply.head(), Seq::new(2));
    }

    /// The failure policy: a store that cannot write **refuses**, and keeps refusing.
    /// Refusing leaves the node up and simply outside its shards' quorums, where the
    /// peers' majority logic already covers it; a panic would take the whole process
    /// down for one bad volume.
    #[test]
    fn a_store_that_cannot_write_refuses_instead_of_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileEntityStore::open(dir.path(), TEST_CODEC).expect("open");
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

        // Take the segment area away: the next entity needs a segment file and cannot
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
    /// claiming anything, including for entities it had already served successfully.
    #[test]
    fn a_poisoned_store_refuses_every_later_operation() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileEntityStore::open(dir.path(), TEST_CODEC).expect("open");
        let entity = name("served");
        let _ = store.store_record(
            0,
            &entity,
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
                &entity,
                Seq::new(1),
                Term::new(1),
                vec![b"c".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Failed,
            "a previously served entity is refused too: the volume is the unit"
        );
        // The promise, not just the view: a replica that cannot durably record the
        // term it promised could accept a lower one after a restart (§8, E14).
        assert_eq!(
            store.prepare(0, &entity, Term::new(2)),
            ReadOutcome::Failed,
            "a fence it cannot persist is a fence it has not promised"
        );
        assert_eq!(
            store.put_blob(0, &entity, BlobId::of(b"x"), b"x".to_vec()),
            BlobAck::Failed,
            "a blob copy it cannot store must not count toward a quorum"
        );
    }
}
