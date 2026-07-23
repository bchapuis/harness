//! Per-node durable grain storage — the `GrainStore` seam (spec §7.2, §7.4).
//!
//! In the per-grain quorum substrate (§7.2) the grains' records live **off** the
//! leader-election group's Raft log: each replica persists, on its own, the records
//! quorum-appended to it by the shard leader's [`Replicator`](crate::replicator).
//! `GrainStore` is that per-node durable store — the granary analogue of the Raft
//! WAL (actor §9.4.3), injected at construction and preserved across a process
//! restart, so a full-cluster cold restart recovers each grain from a quorum of the
//! replicas' reloaded stores (§8, **G14**).
//!
//! It keys records by `(shard index, GrainName)` and stamps each with the **shard
//! term** under which it was written — the fencing token (§8) and the key to
//! highest-term-per-slot read-repair on recovery. A single in-memory tier
//! ([`MemoryGrainStore`]) is the reference implementation, used by the `Local`
//! journal directly and by the `Quorum` replica store on each node; a deployment
//! that must survive total power loss supplies a file-backed `GrainStore` through
//! the same seam (the harness file-log prior art, §7.4).
//!
//! **Per-grain segmentation.** A grain's records are an independent **segment** —
//! its own [`GrainRecords`] behind its own lock — so concurrent grains never
//! serialize on a single store-wide lock, and one grain's snapshot compaction
//! touches only its own segment (§9). The one piece shared across a shard's grains
//! is the **fence**: the highest shard term the store has acknowledged (§8). It sits
//! behind its own leaf lock, taken *inside* a grain's segment lock, so a grain's
//! write and that same grain's recovery `prepare` serialize on the segment lock
//! (the only fencing-critical race, §8) while cross-grain fence bumps stay
//! monotonic and contend on nothing else.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use actor_core::NodeId;
use serde::Deserialize;
use serde::Serialize;

use crate::blobs::BlobId;
use crate::grain::GrainName;
use crate::journal::Seq;
use crate::journal::Term;

/// One stored record: the opaque event bytes and the **shard term** under which it
/// was committed. The term is what lets a recovering leader pick, per `Seq` slot,
/// the record a higher term last won — the highest-term-per-slot read-repair of §8.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordSlot {
    /// The shard term under which this record was written (the fencing token, §8).
    pub term: Term,
    /// The opaque, codec-encoded event bytes.
    pub bytes: Vec<u8>,
}

/// The outcome of a fenced store (`store_record`/`store_snapshot`, §7.2, §8).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreAck {
    /// Durable in this store; carries the replica's contiguous head after the write.
    Stored(Seq),
    /// Refused: this replica has acknowledged a higher shard term (the fence, §8).
    /// Carries that higher term so the stale leader learns it has been deposed.
    Fenced(Term),
    /// Refused: a normal append landed on a stale head — this replica already holds a
    /// different committed record at the first target slot, so the leader's head is
    /// behind (it recovered from local state without a quorum, §7.5). Carries the
    /// replica's actual head so the leader steps down and re-recovers. Optimistic
    /// concurrency on the head: it keeps a stale leader from overwriting a committed
    /// record even though its term is current (§8).
    Stale(Seq),
    /// Refused: the append targets a key at or above the shard's **append bound**
    /// (spec §7.7) — a range this shard no longer owns because a split moved it to
    /// a child (or a merge is retiring the whole shard). Refused at ANY term: the
    /// bound is what stops a leader that has not yet applied the split from
    /// assembling a majority for a moved key. The caller surfaces `NotLeader`, so
    /// the client re-resolves against the committed map (G15).
    Sealed,
}

/// The reply to a read: every occupied slot with its committing term, and the
/// latest snapshot, so a recovering leader can merge a write quorum by
/// highest-term-per-slot (**G14**).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadReply {
    /// `(seq, term, bytes)` for each occupied slot, ascending by `seq`.
    pub slots: Vec<(Seq, Term, Vec<u8>)>,
    /// The latest snapshot `(seq, term, state)`, if any.
    pub snapshot: Option<(Seq, Term, Vec<u8>)>,
}

/// The outcome of a recovery `prepare` (spec §8): the replica's records, or a
/// refusal because it has promised a higher term. `prepare` is a fenced read — a
/// Paxos-style promise — so that once a new leader has read a quorum, a deposed
/// leader can no longer commit on any of those replicas (closing the
/// commit-after-read race, §8). An ordinary [`read`](GrainStore::read) does not
/// fence and is used only for local replay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadOutcome {
    /// The replica's slots and snapshot; it has promised not to accept a lower term.
    Prepared(ReadReply),
    /// Refused: the replica has acknowledged a higher shard term (the fence, §8).
    Fenced(Term),
}

/// Whether a record store is a normal append or a recovery write-back (§8) — the
/// one bit that decides whether the optimistic head check applies. A named type
/// rather than a bare `bool` so the intent is legible at every call site.
///
/// The variant order is load-bearing: `Append` is the zero discriminant, matching
/// the `false` it replaced, so a segment log written before this type reads back
/// unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteKind {
    /// A normal append: applies the optimistic head check and may report `Stale`.
    Append,
    /// A recovery write-back (read-repair, §8): fills/repairs slots by highest term
    /// and never reports `Stale`.
    Repair,
    /// A split/merge transfer copy (spec §7.7): the driver landing a moved grain's
    /// committed prefix under the **destination** shard's keys. Skips the fence —
    /// the copy is stamped `Term::ZERO` and its destination keys belong to a range
    /// no leader serves yet (a split's child) or to grains the destination cannot
    /// hold (a merge's disjoint moved range), so there is no term to contest, and
    /// a merge destination's live fence must not refuse it. Never reports `Stale`.
    /// Safe only from the transfer driver: the source is a completed quorum
    /// recovery, so re-driven copies agree per slot (G14).
    Transfer,
}

/// A node's durable store of grain records and snapshots (spec §7.2, §7.4).
///
/// All methods are fenced by the shard **term** (§8): a write stamped with a term
/// below the highest the store has acknowledged for that shard is refused
/// (`Fenced`), so a deposed leader cannot land a write. Reads return each slot's
/// term so the leader's recovery can read-repair (§8). Implementations key by
/// `(shard, grain)` and persist durably enough to survive the restart their
/// deployment targets (in-memory for the simulator, file-backed in production).
pub trait GrainStore: Send + Sync + 'static {
    /// Store `records` for a grain beginning at the slot after `after`, fenced by
    /// `term`. Idempotent per slot: a slot already holding an equal-or-higher term
    /// is kept (a re-delivered or late append does not regress it). Returns
    /// `Stored(head)` with the replica's contiguous head, `Fenced(higher)`, or — for
    /// a [`WriteKind::Append`] onto a stale head — `Stale(head)`. A
    /// [`WriteKind::Repair`] fills/repairs slots by highest term and never reports
    /// `Stale`.
    fn store_record(
        &self,
        shard: u32,
        grain: &GrainName,
        after: Seq,
        term: Term,
        records: Vec<Vec<u8>>,
        kind: WriteKind,
    ) -> StoreAck;

    /// Every occupied slot (with its term) and the latest snapshot for a grain —
    /// a non-fencing local read, used for recovery merge and replay (§9). Empty for a
    /// grain this store has never seen.
    fn read(&self, shard: u32, grain: &GrainName) -> ReadReply;

    /// Up to `limit` records for a grain after `from` (exclusive), ascending by
    /// `Seq`, as `(Seq, bytes)` — the `load` seam (§7.3). A ranged read so paging a
    /// grain's tail on replay costs `O(limit)`, not `O(grain size)`: only the
    /// returned window's bytes are cloned. Records the snapshot already subsumes
    /// (`Seq <= base`) are absent, as in [`read`](GrainStore::read).
    fn read_from(
        &self,
        shard: u32,
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> Vec<(Seq, Vec<u8>)>;

    /// A **fenced** read for recovery (§8): promise not to accept a shard term below
    /// `term` (so a deposed leader cannot commit on this replica once a new leader
    /// has read it), then return this replica's slots and snapshot. Refuses with
    /// `Fenced(higher)` if it has already promised a higher term.
    fn prepare(&self, shard: u32, grain: &GrainName, term: Term) -> ReadOutcome;

    /// Persist a snapshot at `at` fenced by `term` (§9). Kept only if it advances
    /// the stored snapshot. Returns `Stored(at)` or `Fenced(higher)`. A
    /// [`WriteKind::Transfer`] skips the fence (the split/merge driver landing a
    /// moved grain's snapshot under the destination shard's keys, §7.7); `Append`
    /// and `Repair` are fenced identically.
    fn store_snapshot(
        &self,
        shard: u32,
        grain: &GrainName,
        at: Seq,
        term: Term,
        state: Vec<u8>,
        kind: WriteKind,
    ) -> StoreAck;

    /// Drop records past slot `after` whose term is at most `term` — the leader
    /// rolling back the tentative local write of an append that failed to reach a
    /// quorum (§7.2), so an uncommitted record never becomes visible to a later
    /// stale-local recovery (§7.5, G5). The record survives on any peers that stored
    /// it, so a quorum that does hold it can still commit it late.
    ///
    /// The term bound is what makes the rollback safe against a concurrent
    /// leadership change: while the failed append's quorum wait was in flight, a
    /// **new** leader (higher term) may have fenced this store and written back or
    /// committed records for the same grain above `after`. Those records carry the
    /// higher term and MUST NOT be dropped (G14); a term-blind truncate here could
    /// silently shrink a committed write's durability below a quorum.
    fn truncate(&self, shard: u32, grain: &GrainName, after: Seq, term: Term);

    // --- The grain-native content-addressed blob store (durable-workspace design) ----
    //
    // A grain's immutable blobs live beside its records in the same per-node store
    // but **off** the ordered, term-fenced record path: content addressing needs no
    // term (a stale leader re-storing a block writes identical bytes) and no order.
    // Keyed by `(shard, grain, BlobId)`; reclamation is grain-scoped, the grain
    // driving it from its own live id set.

    /// Store an immutable, content-addressed blob for a grain. Idempotent: an `id`
    /// already present is kept (storing equal content writes nothing new). Unfenced.
    fn put_blob(&self, shard: u32, grain: &GrainName, id: BlobId, bytes: Vec<u8>);

    /// The bytes of `id` for a grain, or `None` if this store does not hold it. The
    /// caller re-hashes and verifies the bytes against `id` before use.
    fn get_blob(&self, shard: u32, grain: &GrainName, id: BlobId) -> Option<Vec<u8>>;

    /// Whether this store holds `id` for a grain.
    fn has_blob(&self, shard: u32, grain: &GrainName, id: BlobId) -> bool;

    /// Drop a **single** blob of a grain. Idempotent (a missing blob is already
    /// done). Used by the read path to evict a copy that failed verification before
    /// re-fetching a good one (corruption self-heal, §7.10): a content-addressed
    /// [`put_blob`](GrainStore::put_blob) of an id already on disk writes nothing, so
    /// a corrupt copy must be removed before its replacement can be stored.
    fn delete_blob(&self, shard: u32, grain: &GrainName, id: BlobId);

    /// Drop **every** blob of a grain — grain-scoped reclamation on destroy, with no
    /// namespace tombstone or membership gating (the area lives only on the grain's
    /// known replicas).
    fn delete_blobs(&self, shard: u32, grain: &GrainName);

    /// Drop every blob of a grain **not** in `retain` — the grain's mark-from-roots
    /// sweep, reclaiming blocks orphaned by overwrites. Idempotent.
    fn retain_blobs(&self, shard: u32, grain: &GrainName, retain: &BTreeSet<BlobId>);

    // --- Enumeration (replica-set migration, §7.7) ---------------------------

    /// Every grain this store holds anything for under `shard` — records, a
    /// snapshot, or blobs. The migration driver enumerates a shard's grains from a
    /// read quorum of its replicas with this, so a grain committed while this node
    /// was down is still found on the others.
    fn grains(&self, shard: u32) -> Vec<GrainName>;

    /// Every blob id this store holds for one grain — the migration driver's
    /// source list when copying a grain's blob area to a new replica.
    fn blob_ids(&self, shard: u32, grain: &GrainName) -> Vec<BlobId>;

    // --- Shard split/merge (§7.7) --------------------------------------------

    /// Tighten the shard's **append bound**: refuse every future
    /// [`WriteKind::Append`] whose grain's name hash is `>= from` (`Sealed`),
    /// monotonically (`min` with any existing bound) and durably. This is the
    /// store half of G15: once a majority of the shard's replicas are bounded, no
    /// append to the moved range can assemble a write quorum at ANY term — even
    /// from a leader that has not yet applied the split — by the same
    /// intersection argument as the term fence. The bound is permanent for the
    /// shard (the moved range never returns to it) except through
    /// [`unseal`](GrainStore::unseal) on a committed merge. Recovery reads,
    /// repairs, and transfers are not bounded — the split driver itself must
    /// recover and copy the moved grains after sealing.
    fn seal_range(&self, shard: u32, from: u64);

    /// Clear the shard's append bound — only on applying a committed merge
    /// (§7.7), where the shard re-absorbs the very range its earlier split moved
    /// out and the merged data is already durable under this shard's keys.
    fn unseal(&self, shard: u32);

    /// Drop every trace of one grain under `shard` — records, snapshot, and
    /// blobs. The split driver's local GC of a moved grain's parent-keyed data
    /// after the child's copy is quorum-durable and the mapping has committed.
    /// Idempotent; never touches other shards' keys for the same grain.
    fn remove_grain(&self, shard: u32, grain: &GrainName);

    /// An estimate of the bytes this store holds under `shard` — records,
    /// snapshots, and blobs. The split trigger's size signal (§7.7,
    /// `shard_target_bytes`); an estimate is enough, so implementations may
    /// ignore framing overhead.
    fn shard_bytes(&self, shard: u32) -> u64;
}

/// How the runtime obtains a node's [`GrainStore`] (spec §7.4). Supplied on
/// [`GranaryConfig`](crate::GranaryConfig); a factory that **caches per node**, held
/// by the deployment across a restart, is what makes a grain's records survive a
/// full-cluster cold restart (the WAL-storage analogue, actor §9.4.3). The default
/// is a fresh ephemeral [`MemoryGrainStore`] per node (lost on restart).
pub type GrainStoreFactory = Arc<dyn Fn(NodeId) -> Arc<dyn GrainStore> + Send + Sync>;

/// One grain's stored records and its latest snapshot — the per-grain **segment**
/// (§7.2). Shared by [`MemoryGrainStore`] and the file-backed store, which each wrap
/// it in their own per-grain lock and add durability; the fence lives in the store,
/// not here (it is per *shard*, §8).
///
/// Records with `Seq <= base` have been compacted away — subsumed by `snapshot`
/// (§9), so `base` always equals the snapshot's seq whenever a snapshot is present.
/// `slots` is the sparse vector of records *after* `base`: slot `i` is `Seq`
/// `base + i + 1`. The head is `base` plus the leading gap-free run of `slots`.
#[derive(Clone, Default)]
pub(crate) struct GrainRecords {
    /// The compacted prefix's last seq (`ZERO` = nothing compacted), equal to the
    /// snapshot's seq when a snapshot is present. The store reports it implicitly:
    /// a reader recovers it from the snapshot's seq, so it never crosses the wire.
    base: Seq,
    slots: Vec<Option<RecordSlot>>,
    snapshot: Option<(Seq, Term, Vec<u8>)>,
}

/// A serializable checkpoint of one grain's segment (spec §9): the basis for the
/// file store's per-grain, snapshot-driven log compaction. The file store rewrites a
/// grain's segment to a single `Checkpoint` op holding this, folding away the record
/// ops the grain's snapshot made redundant — so compaction touches one grain's file,
/// never the whole node's store.
///
/// A distinct type from [`GrainRecords`] on purpose: this is the frozen on-disk
/// contract (it must deserialize old segments), while `GrainRecords` is the live
/// in-memory representation and stays free to change. [`export`](GrainRecords::export)
/// and [`from_checkpoint`](GrainRecords::from_checkpoint) are the only bridge.
#[derive(Serialize, Deserialize)]
pub(crate) struct GrainCheckpoint {
    base: Seq,
    slots: Vec<Option<RecordSlot>>,
    snapshot: Option<(Seq, Term, Vec<u8>)>,
}

impl GrainRecords {
    /// A serializable checkpoint of this segment's whole current state.
    pub(crate) fn export(&self) -> GrainCheckpoint {
        GrainCheckpoint {
            base: self.base,
            slots: self.slots.clone(),
            snapshot: self.snapshot.clone(),
        }
    }

    /// Reconstruct a segment from a [`GrainCheckpoint`] (the file store's replay of a
    /// compacted segment).
    pub(crate) fn from_checkpoint(checkpoint: GrainCheckpoint) -> GrainRecords {
        GrainRecords {
            base: checkpoint.base,
            slots: checkpoint.slots,
            snapshot: checkpoint.snapshot,
        }
    }

    /// The committed head: `base` plus the leading gap-free run of `slots`. A
    /// committed prefix is gap-free (quorum intersection, §8); a gap marks an
    /// uncommitted tail, correctly excluded from the head.
    pub(crate) fn head(&self) -> Seq {
        let mut run = 0u64;
        for slot in &self.slots {
            if slot.is_some() {
                run += 1;
            } else {
                break;
            }
        }
        Seq::new(self.base.value() + run)
    }

    /// `(seq, term, bytes)` for each occupied slot, ascending — `seq = base + i + 1`.
    /// The compacted prefix is absent (covered by the snapshot).
    fn occupied(&self) -> Vec<(Seq, Term, Vec<u8>)> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(i, slot)| {
                slot.as_ref().map(|s| {
                    (
                        Seq::new(self.base.value() + i as u64 + 1),
                        s.term,
                        s.bytes.clone(),
                    )
                })
            })
            .collect()
    }

    /// The reply to a non-fencing read: all occupied slots and the latest snapshot.
    pub(crate) fn read(&self) -> ReadReply {
        ReadReply {
            slots: self.occupied(),
            snapshot: self.snapshot.clone(),
        }
    }

    /// Up to `limit` occupied records after `from` (exclusive), ascending. Clones
    /// only the returned window — the ranged `load` read (§7.3).
    pub(crate) fn read_from(&self, from: Seq, limit: usize) -> Vec<(Seq, Vec<u8>)> {
        let base = self.base.value();
        // Records at or below `from` (and the compacted prefix) are skipped; start at
        // the first slot above `max(from, base)`.
        let start = from.value().max(base).saturating_sub(base) as usize;
        let mut out = Vec::new();
        for (i, slot) in self.slots.iter().enumerate().skip(start) {
            if out.len() == limit {
                break;
            }
            if let Some(record) = slot {
                out.push((Seq::new(base + i as u64 + 1), record.bytes.clone()));
            }
        }
        out
    }

    /// Apply a fenced record store (the fence is checked by the caller). Mirrors the
    /// idempotent-per-slot and optimistic-head-check semantics of §7.2/§8.
    pub(crate) fn store_record(
        &mut self,
        after: Seq,
        term: Term,
        records: Vec<Vec<u8>>,
        kind: WriteKind,
    ) -> StoreAck {
        let base = self.base.value();
        // Optimistic head check (§8): a normal append whose first target slot already
        // holds a *different* record means the leader's head is stale — reject so it
        // steps down and re-recovers rather than overwriting a committed record. An
        // append whose `after` is below our compacted base is stale by the same logic
        // (the leader recovered without seeing our snapshot).
        if kind == WriteKind::Append {
            if after.value() < base {
                return StoreAck::Stale(self.head());
            }
            let first_local = (after.value() - base) as usize;
            if let (Some(Some(existing)), Some(incoming)) =
                (self.slots.get(first_local), records.first())
                && &existing.bytes != incoming
            {
                return StoreAck::Stale(self.head());
            }
        }
        for (offset, bytes) in records.into_iter().enumerate() {
            // The absolute seq of this record; skip any the snapshot already subsumes
            // (a recovery write-back landing on a more-compacted replica, §8).
            let abs = after.value() + offset as u64 + 1;
            if abs <= base {
                continue;
            }
            let idx = (abs - base - 1) as usize;
            if self.slots.len() <= idx {
                self.slots.resize_with(idx + 1, || None);
            }
            // Idempotent per slot: keep an equal-or-higher-term record (a re-delivered
            // or late append, §7.2); overwrite a strictly-lower-term one (read-repair,
            // §8). An empty slot is filled.
            match &self.slots[idx] {
                Some(existing) if existing.term >= term => {}
                _ => self.slots[idx] = Some(RecordSlot { term, bytes }),
            }
        }
        StoreAck::Stored(self.head())
    }

    /// Apply a fenced snapshot store (§9). Returns the ack and whether the snapshot
    /// **advanced the base** — i.e. just compacted records — so a file store knows to
    /// rewrite the grain's segment.
    pub(crate) fn store_snapshot(
        &mut self,
        at: Seq,
        term: Term,
        state: Vec<u8>,
    ) -> (StoreAck, bool) {
        // A snapshot only ever advances (§9, G4). When it does it subsumes every
        // record up to `at`, so compact them: advance the base and drop the covered
        // slots (records past `at`, if any, shift down behind the new base). This is
        // the snapshot-driven compaction of the records the snapshot makes redundant.
        let current = self.snapshot.as_ref().map_or(0, |(s, _, _)| s.value());
        if at.value() > current {
            let drop = at
                .value()
                .saturating_sub(self.base.value())
                .min(self.slots.len() as u64) as usize;
            self.slots.drain(..drop);
            self.base = at;
            self.snapshot = Some((at, term, state));
            return (StoreAck::Stored(at), true);
        }
        (StoreAck::Stored(at), false)
    }

    /// An estimate of this segment's stored bytes — record payloads plus the
    /// snapshot state, ignoring framing. Feeds the shard-size split signal (§7.7).
    pub(crate) fn approximate_bytes(&self) -> u64 {
        let records: usize = self
            .slots
            .iter()
            .flatten()
            .map(|slot| slot.bytes.len())
            .sum();
        let snapshot = self
            .snapshot
            .as_ref()
            .map_or(0, |(_, _, state)| state.len());
        (records + snapshot) as u64
    }

    /// Drop records past slot `after` whose term is at most `term` (the rollback of
    /// an uncommitted tail, §7.2). Term-aware, per slot: a record above `after`
    /// written under a **higher** term is a newer leader's — possibly committed —
    /// write that landed on this replica while the rollback's append was in flight,
    /// and MUST survive (G14). A slot at or below `term` above the roll-backer's own
    /// head is by construction uncommitted (a committed slot there would have raised
    /// the head its recovery adopted), so dropping it is safe.
    pub(crate) fn truncate(&mut self, after: Seq, term: Term) {
        // `after` is absolute; clear matching slots above it, behind the base.
        let keep = after.value().saturating_sub(self.base.value()) as usize;
        for slot in self.slots.iter_mut().skip(keep) {
            if slot.as_ref().is_some_and(|s| s.term <= term) {
                *slot = None;
            }
        }
        // Drop any all-empty tail so the sparse vector does not grow unboundedly.
        while self.slots.last().is_some_and(Option::is_none) {
            self.slots.pop();
        }
    }
}

/// One grain's in-memory segment handle: its [`GrainRecords`] behind its own lock,
/// shared (cloned) out of the registry so an op holds only that grain's lock.
type Segment = Arc<Mutex<GrainRecords>>;

/// One grain's in-memory content-addressed blob area: its immutable blobs keyed by
/// content id (durable-workspace design), off the fenced record path.
type BlobArea = HashMap<BlobId, Vec<u8>>;

/// The per-shard fence and per-grain segment registry shared by one
/// [`MemoryGrainStore`] (and its clones).
#[derive(Default)]
struct Inner {
    /// The fence: the highest shard term this store has acknowledged (§8), behind its
    /// own leaf lock so cross-grain bumps never block a grain's data ops.
    fences: Mutex<HashMap<u32, Term>>,
    /// The per-shard **append bound** (§7.7): refuse appends at or above this name
    /// hash — the store half of split/merge safety (G15). A leaf lock like the
    /// fence, checked inside the grain's segment lock.
    seals: Mutex<HashMap<u32, u64>>,
    /// One independent segment per `(shard, grain)`, each behind its own lock.
    segments: Mutex<HashMap<(u32, GrainName), Segment>>,
    /// The grain-native content-addressed blob area (durable-workspace design): one
    /// id→bytes map per `(shard, grain)`, off the fenced record path. Behind its own
    /// lock so blob ops never contend with a grain's record segment.
    blobs: Mutex<HashMap<(u32, GrainName), BlobArea>>,
}

/// The reference in-memory [`GrainStore`] (spec §7.4). Cloning shares one store, so
/// a factory that hands the same clone to a restarted node's replica store makes
/// the records survive the restart (the simulator's stand-in for a durable disk).
#[derive(Clone, Default)]
pub struct MemoryGrainStore {
    inner: Arc<Inner>,
}

impl MemoryGrainStore {
    /// A fresh, empty store.
    pub fn new() -> MemoryGrainStore {
        MemoryGrainStore::default()
    }

    /// The segment for `(shard, grain)`, creating an empty one if absent.
    fn segment(&self, shard: u32, grain: &GrainName) -> Segment {
        let mut segments = self
            .inner
            .segments
            .lock()
            .expect("grain store segments poisoned");
        Arc::clone(
            segments
                .entry((shard, grain.clone()))
                .or_insert_with(|| Arc::new(Mutex::new(GrainRecords::default()))),
        )
    }

    /// The segment for `(shard, grain)` if it exists — no allocation for a grain
    /// this store has never seen (the read path).
    fn existing(&self, shard: u32, grain: &GrainName) -> Option<Segment> {
        let segments = self
            .inner
            .segments
            .lock()
            .expect("grain store segments poisoned");
        segments.get(&(shard, grain.clone())).map(Arc::clone)
    }

    /// Check the shard fence against `term` and bump it to the max. Returns the
    /// blocking fence on refusal. Taken *inside* a held segment lock, so it is a
    /// short leaf critical section.
    fn check_and_bump_fence(&self, shard: u32, term: Term) -> Result<(), Term> {
        let mut fences = self
            .inner
            .fences
            .lock()
            .expect("grain store fences poisoned");
        let fence = *fences.get(&shard).unwrap_or(&Term::ZERO);
        if term < fence {
            return Err(fence);
        }
        // `term >= fence` here, so only a strict advance changes the fence — skip the
        // map write in the steady-state append case where `term == fence`.
        if term > fence {
            fences.insert(shard, term);
        }
        Ok(())
    }

    /// Whether the shard's append bound refuses this grain's appends (§7.7).
    /// Taken *inside* the held segment lock, like the fence, so an append that
    /// passed the check is durably applied before any observer can act on the
    /// bound being set.
    fn sealed(&self, shard: u32, grain: &GrainName) -> bool {
        self.inner
            .seals
            .lock()
            .expect("grain store seals poisoned")
            .get(&shard)
            .is_some_and(|&from| crate::system::name_at_or_above(grain, from))
    }
}

impl GrainStore for MemoryGrainStore {
    fn store_record(
        &self,
        shard: u32,
        grain: &GrainName,
        after: Seq,
        term: Term,
        records: Vec<Vec<u8>>,
        kind: WriteKind,
    ) -> StoreAck {
        let segment = self.segment(shard, grain);
        // Hold the segment lock across the fence check and the apply, so a concurrent
        // `prepare` for *this* grain cannot slip between them (the fencing race, §8).
        let mut records_guard = segment.lock().expect("grain segment poisoned");
        // The append bound (§7.7) is checked FIRST, before the fence can bump: a
        // moved range accepts no new appends here at any term, and a refused
        // append must not advance the shard fence as a side effect (that would
        // fence the legitimate leader's own writes to the retained range).
        // Repairs and transfers pass — the split driver recovers and copies the
        // moved grains after sealing.
        if kind == WriteKind::Append && self.sealed(shard, grain) {
            return StoreAck::Sealed;
        }
        // A `Transfer` skips the fence: its destination keys have no contesting
        // leader (see `WriteKind::Transfer`), and a merge destination's live fence
        // must not refuse the copy.
        if kind != WriteKind::Transfer
            && let Err(fence) = self.check_and_bump_fence(shard, term)
        {
            return StoreAck::Fenced(fence);
        }
        records_guard.store_record(after, term, records, kind)
    }

    fn read(&self, shard: u32, grain: &GrainName) -> ReadReply {
        match self.existing(shard, grain) {
            Some(segment) => segment.lock().expect("grain segment poisoned").read(),
            None => ReadReply {
                slots: Vec::new(),
                snapshot: None,
            },
        }
    }

    fn read_from(
        &self,
        shard: u32,
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> Vec<(Seq, Vec<u8>)> {
        match self.existing(shard, grain) {
            Some(segment) => segment
                .lock()
                .expect("grain segment poisoned")
                .read_from(from, limit),
            None => Vec::new(),
        }
    }

    fn prepare(&self, shard: u32, grain: &GrainName, term: Term) -> ReadOutcome {
        let segment = self.segment(shard, grain);
        let records_guard = segment.lock().expect("grain segment poisoned");
        // Promise: bump the fence so a deposed leader at a lower term can no longer
        // commit here (the Paxos-prepare half of recovery, §8).
        if let Err(fence) = self.check_and_bump_fence(shard, term) {
            return ReadOutcome::Fenced(fence);
        }
        ReadOutcome::Prepared(records_guard.read())
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
        let segment = self.segment(shard, grain);
        let mut records_guard = segment.lock().expect("grain segment poisoned");
        if kind != WriteKind::Transfer
            && let Err(fence) = self.check_and_bump_fence(shard, term)
        {
            return StoreAck::Fenced(fence);
        }
        records_guard.store_snapshot(at, term, state).0
    }

    fn truncate(&self, shard: u32, grain: &GrainName, after: Seq, term: Term) {
        if let Some(segment) = self.existing(shard, grain) {
            segment
                .lock()
                .expect("grain segment poisoned")
                .truncate(after, term);
        }
    }

    fn put_blob(&self, shard: u32, grain: &GrainName, id: BlobId, bytes: Vec<u8>) {
        self.inner
            .blobs
            .lock()
            .expect("grain store blobs poisoned")
            .entry((shard, grain.clone()))
            .or_default()
            .entry(id)
            .or_insert(bytes);
    }

    fn get_blob(&self, shard: u32, grain: &GrainName, id: BlobId) -> Option<Vec<u8>> {
        self.inner
            .blobs
            .lock()
            .expect("grain store blobs poisoned")
            .get(&(shard, grain.clone()))
            .and_then(|area| area.get(&id).cloned())
    }

    fn has_blob(&self, shard: u32, grain: &GrainName, id: BlobId) -> bool {
        self.inner
            .blobs
            .lock()
            .expect("grain store blobs poisoned")
            .get(&(shard, grain.clone()))
            .is_some_and(|area| area.contains_key(&id))
    }

    fn delete_blob(&self, shard: u32, grain: &GrainName, id: BlobId) {
        if let Some(area) = self
            .inner
            .blobs
            .lock()
            .expect("grain store blobs poisoned")
            .get_mut(&(shard, grain.clone()))
        {
            area.remove(&id);
        }
    }

    fn delete_blobs(&self, shard: u32, grain: &GrainName) {
        self.inner
            .blobs
            .lock()
            .expect("grain store blobs poisoned")
            .remove(&(shard, grain.clone()));
    }

    fn retain_blobs(&self, shard: u32, grain: &GrainName, retain: &BTreeSet<BlobId>) {
        if let Some(area) = self
            .inner
            .blobs
            .lock()
            .expect("grain store blobs poisoned")
            .get_mut(&(shard, grain.clone()))
        {
            area.retain(|id, _| retain.contains(id));
        }
    }

    fn grains(&self, shard: u32) -> Vec<GrainName> {
        // A grain can hold blobs before its first committed record (the blob is
        // durable before the metadata that references it, §7.10), so enumerate the
        // union of the record segments and the blob areas.
        let mut names: BTreeSet<GrainName> = self
            .inner
            .segments
            .lock()
            .expect("grain store segments poisoned")
            .keys()
            .filter(|(s, _)| *s == shard)
            .map(|(_, grain)| grain.clone())
            .collect();
        names.extend(
            self.inner
                .blobs
                .lock()
                .expect("grain store blobs poisoned")
                .keys()
                .filter(|(s, _)| *s == shard)
                .map(|(_, grain)| grain.clone()),
        );
        names.into_iter().collect()
    }

    fn blob_ids(&self, shard: u32, grain: &GrainName) -> Vec<BlobId> {
        self.inner
            .blobs
            .lock()
            .expect("grain store blobs poisoned")
            .get(&(shard, grain.clone()))
            .map(|area| area.keys().copied().collect())
            .unwrap_or_default()
    }

    fn seal_range(&self, shard: u32, from: u64) {
        let mut seals = self.inner.seals.lock().expect("grain store seals poisoned");
        // Monotone: a bound only ever tightens (a re-driven seal, or a second
        // split at a lower boundary); only `unseal` (a committed merge) lifts it.
        let bound = seals.get(&shard).map_or(from, |&cur| cur.min(from));
        seals.insert(shard, bound);
    }

    fn unseal(&self, shard: u32) {
        self.inner
            .seals
            .lock()
            .expect("grain store seals poisoned")
            .remove(&shard);
    }

    fn remove_grain(&self, shard: u32, grain: &GrainName) {
        self.inner
            .segments
            .lock()
            .expect("grain store segments poisoned")
            .remove(&(shard, grain.clone()));
        self.inner
            .blobs
            .lock()
            .expect("grain store blobs poisoned")
            .remove(&(shard, grain.clone()));
    }

    fn shard_bytes(&self, shard: u32) -> u64 {
        let records: u64 = self
            .inner
            .segments
            .lock()
            .expect("grain store segments poisoned")
            .iter()
            .filter(|((s, _), _)| *s == shard)
            .map(|(_, segment)| {
                segment
                    .lock()
                    .expect("grain segment poisoned")
                    .approximate_bytes()
            })
            .sum();
        let blobs: u64 = self
            .inner
            .blobs
            .lock()
            .expect("grain store blobs poisoned")
            .iter()
            .filter(|((s, _), _)| *s == shard)
            .map(|(_, area)| area.values().map(|bytes| bytes.len() as u64).sum::<u64>())
            .sum();
        records + blobs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(key: &str) -> GrainName {
        GrainName::new("test.Grain", key)
    }

    #[test]
    fn a_sealed_range_refuses_appends_at_any_term_but_not_repairs_or_transfers() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        let hash = crate::system::name_hash(n.grain_type(), n.key());
        // A bound above the grain's hash leaves its appends unaffected.
        store.seal_range(0, hash.saturating_add(1));
        assert!(matches!(
            store.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(1),
                vec![b"e1".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stored(_)
        ));
        // Tighten the bound to cover the grain: appends refused at ANY term —
        // including one above the fence — that is what stops a leader that has
        // not yet applied the split (G15).
        store.seal_range(0, hash);
        assert_eq!(
            store.store_record(
                0,
                &n,
                Seq::new(1),
                Term::new(99),
                vec![b"e2".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Sealed
        );
        // Monotone: a later, looser seal does not lift the bound.
        store.seal_range(0, hash.saturating_add(1));
        assert_eq!(
            store.store_record(
                0,
                &n,
                Seq::new(1),
                Term::new(99),
                vec![b"e2".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Sealed
        );
        // The split driver's recovery write-back (`Repair`) still lands.
        assert!(matches!(
            store.store_record(
                0,
                &n,
                Seq::new(1),
                Term::new(2),
                vec![b"e2".to_vec()],
                WriteKind::Repair
            ),
            StoreAck::Stored(_)
        ));
        // Another shard's bound is independent.
        assert!(matches!(
            store.store_record(
                1,
                &n,
                Seq::ZERO,
                Term::new(1),
                vec![b"x".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stored(_)
        ));
        // Only a committed merge lifts the bound.
        store.unseal(0);
        assert!(matches!(
            store.store_record(
                0,
                &n,
                Seq::new(2),
                Term::new(99),
                vec![b"e3".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stored(_)
        ));
    }

    #[test]
    fn a_transfer_bypasses_the_fence_for_records_and_snapshot() {
        // The split/merge driver lands a moved grain's committed prefix under the
        // destination shard's keys at `Term::ZERO`; a merge destination's live
        // fence must not refuse it (§7.7).
        let store = MemoryGrainStore::new();
        let n = name("moved");
        assert!(matches!(
            store.prepare(0, &name("resident"), Term::new(7)),
            ReadOutcome::Prepared(_)
        ));
        // A normal zero-term write is fenced...
        assert_eq!(
            store.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::ZERO,
                vec![b"e1".to_vec()],
                WriteKind::Repair
            ),
            StoreAck::Fenced(Term::new(7))
        );
        // ...but the transfer copy lands, records and snapshot both.
        assert!(matches!(
            store.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::ZERO,
                vec![b"e1".to_vec()],
                WriteKind::Transfer
            ),
            StoreAck::Stored(_)
        ));
        assert!(matches!(
            store.store_snapshot(
                0,
                &n,
                Seq::new(1),
                Term::ZERO,
                b"snap@1".to_vec(),
                WriteKind::Transfer
            ),
            StoreAck::Stored(_)
        ));
        // And the transfer did not poison the fence: the live term still writes.
        assert!(matches!(
            store.store_record(
                0,
                &name("resident"),
                Seq::ZERO,
                Term::new(7),
                vec![b"r1".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stored(_)
        ));
    }

    #[test]
    fn remove_grain_drops_records_snapshot_and_blobs_for_one_shard_only() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"e1".to_vec()],
            WriteKind::Append,
        );
        store.store_record(
            1,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"other-shard".to_vec()],
            WriteKind::Append,
        );
        let id = BlobId::of(b"blob");
        store.put_blob(0, &n, id, b"blob".to_vec());
        store.remove_grain(0, &n);
        assert!(store.read(0, &n).slots.is_empty());
        assert!(!store.has_blob(0, &n, id));
        // The same grain under another shard index is untouched.
        assert_eq!(store.read(1, &n).slots.len(), 1);
        // Idempotent.
        store.remove_grain(0, &n);
    }

    #[test]
    fn shard_bytes_estimates_records_snapshots_and_blobs() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        assert_eq!(store.shard_bytes(0), 0);
        store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![vec![b'x'; 100]],
            WriteKind::Append,
        );
        store.put_blob(0, &n, BlobId::of(b"b"), vec![b'y'; 50]);
        assert_eq!(store.shard_bytes(0), 150);
        assert_eq!(store.shard_bytes(1), 0);
    }

    #[test]
    fn records_store_and_read_back_with_their_terms() {
        let store = MemoryGrainStore::new();
        let n = name("a");
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
        let reply = store.read(0, &n);
        assert_eq!(
            reply.slots,
            vec![
                (Seq::new(1), Term::new(1), b"e1".to_vec()),
                (Seq::new(2), Term::new(1), b"e2".to_vec())
            ]
        );
    }

    #[test]
    fn read_from_is_exclusive_of_from_and_bounded_by_limit() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"e1".to_vec(), b"e2".to_vec(), b"e3".to_vec()],
            WriteKind::Append,
        );
        assert_eq!(
            store.read_from(0, &n, Seq::ZERO, 10),
            vec![
                (Seq::new(1), b"e1".to_vec()),
                (Seq::new(2), b"e2".to_vec()),
                (Seq::new(3), b"e3".to_vec()),
            ]
        );
        // Exclusive of `from`, bounded by `limit`.
        assert_eq!(
            store.read_from(0, &n, Seq::new(1), 1),
            vec![(Seq::new(2), b"e2".to_vec())]
        );
        assert_eq!(store.read_from(0, &n, Seq::new(3), 10), Vec::new());
        // A read past a compacted base returns the live tail only.
        store.store_snapshot(
            0,
            &n,
            Seq::new(2),
            Term::new(1),
            b"snap@2".to_vec(),
            WriteKind::Append,
        );
        assert_eq!(
            store.read_from(0, &n, Seq::ZERO, 10),
            vec![(Seq::new(3), b"e3".to_vec())]
        );
    }

    #[test]
    fn a_lower_term_write_is_fenced() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(5),
            vec![b"e1".to_vec()],
            WriteKind::Append,
        );
        // A write stamped with a term below the acknowledged shard term is refused.
        assert_eq!(
            store.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(4),
                vec![b"stale".to_vec()],
                WriteKind::Repair
            ),
            StoreAck::Fenced(Term::new(5))
        );
    }

    #[test]
    fn the_fence_is_shared_across_a_shards_grains() {
        let store = MemoryGrainStore::new();
        // A prepare on grain `a` at term 5 promises the whole shard not to accept a
        // lower term; a write to grain `b` in the same shard at term 4 is then fenced.
        assert!(matches!(
            store.prepare(0, &name("a"), Term::new(5)),
            ReadOutcome::Prepared(_)
        ));
        assert_eq!(
            store.store_record(
                0,
                &name("b"),
                Seq::ZERO,
                Term::new(4),
                vec![b"stale".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Fenced(Term::new(5))
        );
        // A different shard keeps its own fence.
        assert_eq!(
            store.store_record(
                1,
                &name("b"),
                Seq::ZERO,
                Term::new(4),
                vec![b"ok".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stored(Seq::new(1))
        );
    }

    #[test]
    fn a_stale_head_append_is_rejected_but_repair_overwrites_by_term() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"e1".to_vec()],
            WriteKind::Append,
        );
        // A normal append at a stale head (slot 1 already holds a different record)
        // is rejected, so a stale leader cannot overwrite a committed record.
        assert_eq!(
            store.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(3),
                vec![b"other".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stale(Seq::new(1))
        );
        assert_eq!(
            store.read(0, &n).slots,
            vec![(Seq::new(1), Term::new(1), b"e1".to_vec())]
        );
        // A recovery write-back (repair) read-repairs the slot to the higher term.
        store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(3),
            vec![b"repaired".to_vec()],
            WriteKind::Repair,
        );
        assert_eq!(
            store.read(0, &n).slots,
            vec![(Seq::new(1), Term::new(3), b"repaired".to_vec())]
        );
    }

    #[test]
    fn contiguous_head_stops_at_the_first_gap() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"e1".to_vec(), b"e2".to_vec()],
            WriteKind::Append,
        );
        // A write that skips a slot (an uncommitted tail) does not advance the head.
        assert_eq!(
            store.store_record(
                0,
                &n,
                Seq::new(3),
                Term::new(1),
                vec![b"e4".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stored(Seq::new(2))
        );
    }

    #[test]
    fn a_snapshot_compacts_the_covered_records_and_holds_the_head() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"e1".to_vec(), b"e2".to_vec(), b"e3".to_vec()],
            WriteKind::Append,
        );
        // A snapshot at seq 2 subsumes e1, e2: they drop, the base advances to 2, and
        // only e3 remains as a live record.
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
        let reply = store.read(0, &n);
        assert_eq!(
            reply.slots,
            vec![(Seq::new(3), Term::new(1), b"e3".to_vec())]
        );
        assert_eq!(
            reply.snapshot,
            Some((Seq::new(2), Term::new(1), b"snap@2".to_vec()))
        );
        // The head still reads 3 (base 2 + the one retained record) — compaction
        // never regresses it. The next append lands contiguously at seq 4.
        assert_eq!(
            store.store_record(
                0,
                &n,
                Seq::new(3),
                Term::new(1),
                vec![b"e4".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stored(Seq::new(4))
        );
    }

    #[test]
    fn a_far_ahead_snapshot_carries_a_lagging_replica_to_its_seq() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"e1".to_vec()],
            WriteKind::Append,
        );
        // A snapshot well past this replica's records (an InstallSnapshot analogue):
        // all slots drop, the base jumps to 5, and the head follows.
        store.store_snapshot(
            0,
            &n,
            Seq::new(5),
            Term::new(2),
            b"snap@5".to_vec(),
            WriteKind::Append,
        );
        assert!(store.read(0, &n).slots.is_empty());
        // A write-back of the recovered tail lands cleanly after the new base.
        assert_eq!(
            store.store_record(
                0,
                &n,
                Seq::new(5),
                Term::new(2),
                vec![b"e6".to_vec()],
                WriteKind::Repair
            ),
            StoreAck::Stored(Seq::new(6))
        );
    }

    #[test]
    fn a_write_back_skips_records_a_higher_snapshot_already_covers() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        // This replica compacted through seq 3.
        store.store_snapshot(
            0,
            &n,
            Seq::new(3),
            Term::new(2),
            b"snap@3".to_vec(),
            WriteKind::Append,
        );
        // A recovery write-back from base 1 re-offers seqs 2..=4; seqs 2,3 are already
        // subsumed by the snapshot, so only seq 4 is stored — no gap, no regression.
        store.store_record(
            0,
            &n,
            Seq::new(1),
            Term::new(2),
            vec![b"e2".to_vec(), b"e3".to_vec(), b"e4".to_vec()],
            WriteKind::Repair,
        );
        assert_eq!(
            store.read(0, &n).slots,
            vec![(Seq::new(4), Term::new(2), b"e4".to_vec())]
        );
    }

    #[test]
    fn truncate_drops_own_term_tentative_records() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(5),
            vec![b"e1".to_vec()],
            WriteKind::Append,
        );
        // A tentative append at head 1 that failed its quorum rolls back.
        store.store_record(
            0,
            &n,
            Seq::new(1),
            Term::new(5),
            vec![b"e2".to_vec()],
            WriteKind::Append,
        );
        store.truncate(0, &n, Seq::new(1), Term::new(5));
        assert_eq!(
            store.read(0, &n).slots,
            vec![(Seq::new(1), Term::new(5), b"e1".to_vec())]
        );
        // Idempotent: nothing above the head remains.
        store.truncate(0, &n, Seq::new(1), Term::new(5));
        assert_eq!(store.read(0, &n).slots.len(), 1);
    }

    #[test]
    fn truncate_spares_a_newer_leaders_committed_records() {
        // The G14 regression (§7.2 rollback): a deposed leader's failed append
        // rolls back at its own term while a NEW leader (higher term) has already
        // written back and committed records above the same head on this replica.
        // The rollback must drop only its own tentative slot, never the newer
        // leader's records.
        let store = MemoryGrainStore::new();
        let n = name("a");
        store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(5),
            vec![b"e1".to_vec()],
            WriteKind::Append,
        );
        // The new leader (term 6) repairs slot 2 and appends slot 3 to this replica.
        store.store_record(
            0,
            &n,
            Seq::new(1),
            Term::new(6),
            vec![b"e2'".to_vec(), b"e3'".to_vec()],
            WriteKind::Repair,
        );
        // The deposed leader's rollback of its failed term-5 append above head 1.
        store.truncate(0, &n, Seq::new(1), Term::new(5));
        assert_eq!(
            store.read(0, &n).slots,
            vec![
                (Seq::new(1), Term::new(5), b"e1".to_vec()),
                (Seq::new(2), Term::new(6), b"e2'".to_vec()),
                (Seq::new(3), Term::new(6), b"e3'".to_vec()),
            ],
            "a higher-term record must survive a lower-term rollback"
        );
    }

    #[test]
    fn truncate_drops_own_term_slots_interleaved_with_higher_terms() {
        // Mixed tail: an own-term tentative slot below a higher-term record. The
        // rollback clears the tentative slot per-slot and keeps the higher-term one
        // (leaving a gap is correct: the gap marks the dropped uncommitted record,
        // and the surviving record is re-merged by the next recovery).
        let store = MemoryGrainStore::new();
        let n = name("a");
        store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(5),
            vec![b"e1".to_vec()],
            WriteKind::Append,
        );
        // Own-term tentative at slot 2; a newer leader's record at slot 3.
        store.store_record(
            0,
            &n,
            Seq::new(1),
            Term::new(5),
            vec![b"mine".to_vec()],
            WriteKind::Append,
        );
        store.store_record(
            0,
            &n,
            Seq::new(2),
            Term::new(6),
            vec![b"theirs".to_vec()],
            WriteKind::Repair,
        );
        store.truncate(0, &n, Seq::new(1), Term::new(5));
        assert_eq!(
            store.read(0, &n).slots,
            vec![
                (Seq::new(1), Term::new(5), b"e1".to_vec()),
                (Seq::new(3), Term::new(6), b"theirs".to_vec()),
            ]
        );
        // The head stops at the gap: slot 3 is above an uncommitted hole.
        assert_eq!(
            store.store_record(
                0,
                &n,
                Seq::new(1),
                Term::new(6),
                vec![b"x".to_vec()],
                WriteKind::Repair
            ),
            StoreAck::Stored(Seq::new(3))
        );
    }

    // --- The grain-native blob store (durable-workspace design) --------------

    #[test]
    fn blobs_round_trip_and_dedup_within_a_grain() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        let id = BlobId::of(b"block");
        assert!(!store.has_blob(0, &n, id));
        store.put_blob(0, &n, id, b"block".to_vec());
        // Idempotent: a second store of equal content keeps the one copy (B2).
        store.put_blob(0, &n, id, b"block".to_vec());
        assert!(store.has_blob(0, &n, id));
        assert_eq!(store.get_blob(0, &n, id), Some(b"block".to_vec()));
        // A different grain's blob area is independent.
        assert!(!store.has_blob(0, &name("b"), id));
    }

    #[test]
    fn delete_blob_evicts_one_and_lets_a_replacement_be_stored() {
        // The read path's corruption self-heal (§7.10): a content-addressed put of an
        // id already on disk is a no-op, so a corrupt copy must be evicted with
        // `delete_blob` before its good replacement can be re-stored.
        let store = MemoryGrainStore::new();
        let n = name("a");
        let id = BlobId::of(b"good");
        store.put_blob(0, &n, id, b"corrupt".to_vec()); // a copy that does not verify
        assert!(store.has_blob(0, &n, id));
        // Without eviction, a re-put keeps the corrupt copy (idempotent on the id).
        store.put_blob(0, &n, id, b"good".to_vec());
        assert_eq!(store.get_blob(0, &n, id), Some(b"corrupt".to_vec()));
        // Evict, then re-put: now the good bytes land.
        store.delete_blob(0, &n, id);
        assert!(!store.has_blob(0, &n, id));
        store.put_blob(0, &n, id, b"good".to_vec());
        assert_eq!(store.get_blob(0, &n, id), Some(b"good".to_vec()));
        // Idempotent: deleting an absent blob is a no-op, and an unrelated grain is
        // untouched.
        store.delete_blob(0, &name("b"), id);
    }

    #[test]
    fn retain_blobs_keeps_only_the_live_set() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        let a = BlobId::of(b"a");
        let b = BlobId::of(b"b");
        let c = BlobId::of(b"c");
        for (id, bytes) in [(a, &b"a"[..]), (b, &b"b"[..]), (c, &b"c"[..])] {
            store.put_blob(0, &n, id, bytes.to_vec());
        }
        // Keep only a and c: b is swept (the mark-from-roots GC).
        store.retain_blobs(0, &n, &BTreeSet::from([a, c]));
        assert!(store.has_blob(0, &n, a));
        assert!(!store.has_blob(0, &n, b));
        assert!(store.has_blob(0, &n, c));
    }

    #[test]
    fn delete_blobs_drops_the_whole_area() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        let a = BlobId::of(b"a");
        store.put_blob(0, &n, a, b"a".to_vec());
        store.delete_blobs(0, &n);
        assert!(!store.has_blob(0, &n, a));
        assert_eq!(store.get_blob(0, &n, a), None);
    }
}
