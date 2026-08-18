//! Per-node durable grain storage — the `GrainStore` seam (spec §7.2, §7.4).
//!
//! In the per-grain quorum substrate (§7.2) the grains' records live **off** the
//! leader-election group's Raft log: each replica persists, on its own, the records
//! quorum-appended to it by the shard leader's [`Replicator`](crate::replicator).
//! `GrainStore` is that per-node durable store, injected at construction and
//! preserved across a process restart, so a full-cluster cold restart recovers each
//! grain from a quorum of the replicas' reloaded stores (§8, **G14**).
//!
//! It keys records by `(shard index, GrainName)` and stamps each with the **shard
//! term** under which it was written — the fencing token (§8) and the key to
//! highest-term-per-slot read-repair on recovery. [`MemoryGrainStore`] is the
//! reference implementation, used by the `Local` journal directly and by the
//! `Quorum` replica store on each node; a deployment that must survive total power
//! loss supplies a file-backed `GrainStore` through the same seam (§7.4).
//!
//! **Per-grain segmentation.** A grain's records are an independent **segment** —
//! its own [`GrainRecords`] behind its own lock — so concurrent grains never
//! serialize on a single store-wide lock, and one grain's snapshot compaction
//! touches only its own segment (§9). The one piece shared across a shard's grains
//! is the **fence**: the highest shard term the store has acknowledged (§8). It sits
//! behind its own leaf lock, taken *inside* a grain's segment lock, so a grain's
//! write and that same grain's recovery `prepare` serialize on the segment lock
//! (the only fencing-critical race, §8).

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use actor_core::NodeId;
use serde::Deserialize;
use serde::Serialize;

use crate::blobs::BlobId;
use crate::grain::GrainName;
use crate::journal::RecordPage;
use crate::journal::Seq;
use crate::journal::Term;

/// One stored record: the opaque event bytes and the **shard term** under which it
/// was committed. The term is what a recovering leader picks by, per `Seq` slot (§8).
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
    /// concurrency on the head, keeping a stale leader from overwriting a committed
    /// record even though its term is current (§8).
    Stale(Seq),
    /// Refused: the append targets a key at or above the shard's **append bound**
    /// (spec §7.7) — a range this shard no longer owns because a split moved it to
    /// a child (or a merge is retiring the whole shard). Refused at ANY term: the
    /// bound is what stops a leader that has not yet applied the split from
    /// assembling a majority for a moved key. The caller surfaces `NotLeader`, so
    /// the client re-resolves against the committed map (G15).
    Sealed,
    /// Refused: this replica's storage is **unusable** — an I/O error (a full or
    /// failing disk, a lost mount) means it cannot make the write durable, so it
    /// cannot honestly acknowledge it.
    ///
    /// Distinct from every other refusal above, which are decisions this replica
    /// *made*; this one is a decision it could not make. The caller counts it like
    /// an unreachable replica: it does not satisfy a quorum, it carries no term or
    /// head worth believing, and it must never be read as a commit. A store that
    /// answers `Failed` once answers it for everything afterwards
    /// ([`FileGrainStore`](crate::FileGrainStore) poisons itself), so the node
    /// simply stops counting toward its shards' quorums instead of crashing and
    /// failing over every shard it happened to lead.
    Failed,
}

/// The outcome of a blob store (§7.10).
///
/// Blobs are unfenced and unordered, so unlike [`StoreAck`] there is no term or
/// head to refuse on: the only refusal is a store that cannot write at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobAck {
    /// Durable in this store (or already present — a blob put is idempotent by
    /// content, B2).
    Stored,
    /// Refused: this replica's storage is unusable, as [`StoreAck::Failed`].
    ///
    /// It must not count toward a blob write quorum. A transport success carrying
    /// this is *not* a stored copy, which is why the reply is this type rather than
    /// `()`: an ack that cannot say "no" makes a failed replica look durable.
    Failed,
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

impl ReadReply {
    /// The head this reply describes: the snapshot's seq (or zero) plus the leading
    /// gap-free run of records above it.
    ///
    /// A committed prefix is gap-free (quorum intersection, §8), so a gap marks an
    /// uncommitted tail and correctly ends the run. Defined here, beside the reply it
    /// folds, so the local tier and a recovering leader read a reply the same way.
    pub fn head(&self) -> Seq {
        let mut head = self.snapshot.as_ref().map_or(0, |(seq, _, _)| seq.value());
        for (seq, _, _) in &self.slots {
            if seq.value() != head + 1 {
                break;
            }
            head += 1;
        }
        Seq::new(head)
    }

    /// The latest snapshot as `(seq, state)` — what the `load_snapshot` seam needs,
    /// without the committing term (§9).
    pub fn into_snapshot(self) -> Option<(Seq, Vec<u8>)> {
        self.snapshot.map(|(seq, _term, state)| (seq, state))
    }
}

/// The outcome of a recovery [`prepare`](GrainStore::prepare) (spec §8): the
/// replica's records, or a refusal because it has promised a higher term. An
/// ordinary [`read`](GrainStore::read) does not fence and is used only for local
/// replay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadOutcome {
    /// The replica's slots and snapshot; it has promised not to accept a lower term.
    Prepared(ReadReply),
    /// Refused: the replica has acknowledged a higher shard term (the fence, §8).
    Fenced(Term),
    /// Refused: this replica's storage is unusable, so neither the promise nor the
    /// view can be trusted — the [`StoreAck::Failed`] of the read path.
    ///
    /// It must not count toward a recovery read quorum. Note the promise is the
    /// load-bearing half: a replica that cannot durably record the term it just
    /// promised could accept a lower one after a restart, so a failed `prepare` is
    /// not merely a missing view (**G14**).
    Failed,
}

/// Whether a record store is a normal append or a recovery write-back (§8) — the
/// one bit that decides whether the optimistic head check applies.
///
/// The variant order is load-bearing: `Append` is the zero discriminant, so an
/// already-written segment log reads back unchanged.
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

/// A grain's immutable, content-addressed blob area (durable-workspace design).
///
/// Blobs live beside a grain's records in the same per-node store but **off** the
/// ordered, term-fenced record path: content addressing needs no term (a stale leader
/// re-storing a block writes identical bytes) and no order. Keyed by
/// `(shard, grain, BlobId)`; reclamation is grain-scoped, the grain driving it from
/// its own live id set. A separate trait because it is unfenced and unordered;
/// [`GrainStore`] requires it so one per-node handle serves both.
pub trait GrainBlobStore: Send + Sync + 'static {
    /// Store an immutable, content-addressed blob for a grain. Idempotent: an `id`
    /// already present is kept (storing equal content writes nothing new). Unfenced.
    ///
    /// The acknowledgement *is* the durability (**G18**): a `Stored` means the bytes
    /// are on this node's storage, and the only other answer is
    /// [`BlobAck::Failed`] — there is no fence and no ordering to refuse against.
    #[must_use = "a store call can refuse to write (Fenced/Stale/Sealed/Failed); read the outcome (G14)"]
    fn put_blob(&self, shard: u32, grain: &GrainName, id: BlobId, bytes: Vec<u8>) -> BlobAck;

    /// The bytes of `id` for a grain, or `None` if this store does not hold it. The
    /// caller re-hashes and verifies the bytes against `id` before use.
    #[must_use = "the answer is the whole point of the call"]
    fn get_blob(&self, shard: u32, grain: &GrainName, id: BlobId) -> Option<Vec<u8>>;

    /// Whether this store holds `id` for a grain.
    #[must_use = "the answer is the whole point of the call"]
    fn has_blob(&self, shard: u32, grain: &GrainName, id: BlobId) -> bool;

    /// Drop a **single** blob of a grain. Idempotent (a missing blob is already
    /// done). Used by the read path to evict a copy that failed verification before
    /// re-fetching a good one (corruption self-heal, §7.10): a content-addressed
    /// [`put_blob`](GrainBlobStore::put_blob) of an id already on disk writes nothing, so
    /// a corrupt copy must be removed before its replacement can be stored.
    fn delete_blob(&self, shard: u32, grain: &GrainName, id: BlobId);

    /// Drop **every** blob of a grain — grain-scoped reclamation on destroy, with no
    /// namespace tombstone or membership gating (the area lives only on the grain's
    /// known replicas).
    fn delete_blobs(&self, shard: u32, grain: &GrainName);

    /// Drop every blob of a grain **not** in `retain` — the grain's mark-from-roots
    /// sweep, reclaiming blocks orphaned by overwrites. Idempotent.
    fn retain_blobs(&self, shard: u32, grain: &GrainName, retain: &BTreeSet<BlobId>);

    /// Every blob id this store holds for one grain — the migration driver's
    /// source list when copying a grain's blob area to a new replica.
    #[must_use = "the answer is the whole point of the call"]
    fn blob_ids(&self, shard: u32, grain: &GrainName) -> Vec<BlobId>;
}

/// A node's durable store of grain records and snapshots (spec §7.2, §7.4).
///
/// The record methods are fenced by the shard **term** (§8): a write stamped with a
/// term below the highest the store has acknowledged for that shard is refused
/// (`Fenced`), so a deposed leader cannot land a write. Reads return each slot's
/// term so the leader's recovery can read-repair (§8). Implementations key by
/// `(shard, grain)` and persist durably enough to survive the restart their
/// deployment targets (in-memory for the simulator, file-backed in production).
///
/// Extends [`GrainBlobStore`]: a grain's content-addressed blob area lives in the
/// same per-node store but off the fenced record path (see that trait). The
/// enumeration and reclamation methods here — `grains`, `remove_grain`,
/// `remove_range`, `drop_shard`, `shard_bytes` — span both areas.
///
/// **A store call's effect has already happened when it returns**, durably: the guard,
/// the in-memory apply, and the fsync all run before the call comes back, under the
/// grain's segment lock. That is what serializes a write against the same grain's
/// recovery `prepare` — the only fencing-critical race (§8) — and it comes from these
/// methods being *synchronous*. An `async fn` store would run its body at first poll,
/// and a caller that never polled would silently not write.
///
/// **Every method that can refuse is `#[must_use]`.** A mutating call answers `Fenced`,
/// `Stale`, `Sealed`, or `Failed` — each a *refusal to have written* — and silently
/// dropping one is how a deposed leader convinces itself it committed (**G14**). The
/// methods that report nothing return `()` and carry no such obligation.
pub trait GrainStore: GrainBlobStore {
    /// Store `records` for a grain beginning at the slot after `after`, fenced by
    /// `term`. Idempotent per slot: a slot already holding an equal-or-higher term
    /// is kept (a re-delivered or late append does not regress it). Returns
    /// `Stored(head)` with the replica's contiguous head, `Fenced(higher)`, or — for
    /// a [`WriteKind::Append`] onto a stale head — `Stale(head)`. A
    /// [`WriteKind::Repair`] fills/repairs slots by highest term and never reports
    /// `Stale`.
    #[must_use = "a store call can refuse to write (Fenced/Stale/Sealed/Failed); read the outcome (G14)"]
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
    /// grain this store has never seen. The reply may name records this store has
    /// applied but not yet made stable, which a caller must not count toward a quorum
    /// (**G14**).
    #[must_use = "the answer is the whole point of the call"]
    fn read(&self, shard: u32, grain: &GrainName) -> ReadReply;

    /// A grain's head alone (§7.3) — what activation needs before it can serve.
    ///
    /// Separate from [`read`](GrainStore::read) because the two differ by everything the
    /// grain holds: `read` clones every occupied slot's bytes, and a caller after the head
    /// then discards all of them to keep one integer. A grain with a long uncompacted tail
    /// paid that on every activation, growing with its history.
    ///
    /// The default answers through `read` and is always correct; a store that can reach
    /// its head without materializing the records overrides it. Defaulted rather than
    /// required so a store with nothing to gain — an immutable test double — need not
    /// restate it.
    #[must_use = "the answer is the whole point of the call"]
    fn head(&self, shard: u32, grain: &GrainName) -> Seq {
        self.read(shard, grain).head()
    }

    /// A grain's latest snapshot as `(seq, state)`, without its records.
    ///
    /// The [`head`](GrainStore::head) argument applies unchanged: rehydration asks for
    /// this immediately after the head, and through `read` it cloned the whole record set
    /// a second time to reach one field.
    #[must_use = "the answer is the whole point of the call"]
    fn snapshot(&self, shard: u32, grain: &GrainName) -> Option<(Seq, Vec<u8>)> {
        self.read(shard, grain).into_snapshot()
    }

    /// Up to `limit` records for a grain after `from` (exclusive), ascending by
    /// `Seq`, with the grain's compaction base — the `load` seam (§7.3), as one
    /// consistent [`RecordPage`] read under the segment's lock. Only the returned
    /// window's bytes are cloned, so paging a grain's tail on replay costs
    /// `O(limit)`. Records the snapshot already subsumes (`Seq <= base`) are
    /// absent, as in [`read`](GrainStore::read); the base is what lets a reader
    /// tell that compacted prefix from the interleaved slots it legally skips.
    #[must_use = "the answer is the whole point of the call"]
    fn read_from(
        &self,
        shard: u32,
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> RecordPage;

    /// A **fenced** read for recovery (§8): promise not to accept a shard term below
    /// `term` (so a deposed leader cannot commit on this replica once a new leader
    /// has read it), then return this replica's slots and snapshot. Refuses with
    /// `Fenced(higher)` if it has already promised a higher term.
    /// The promise is made under the grain's segment lock, before the call returns,
    /// so it fences a concurrent append either way, and the returned view is stable by
    /// the time the call returns. Both halves matter: a promise made late would let a
    /// deposed leader commit, and a view released early would let a recovering leader
    /// adopt a head no replica has on disk (**G14**).
    #[must_use = "a store call can refuse to write (Fenced/Stale/Sealed/Failed); read the outcome (G14)"]
    fn prepare(&self, shard: u32, grain: &GrainName, term: Term) -> ReadOutcome;

    /// Persist a snapshot at `at` fenced by `term` (§9). Kept only if it advances
    /// the stored snapshot. Returns `Stored(at)` or `Fenced(higher)`. A
    /// [`WriteKind::Transfer`] skips the fence (the split/merge driver landing a
    /// moved grain's snapshot under the destination shard's keys, §7.7); `Append`
    /// and `Repair` are fenced identically.
    #[must_use = "a store call can refuse to write (Fenced/Stale/Sealed/Failed); read the outcome (G14)"]
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

    // --- Enumeration (replica-set migration, §7.7) ---------------------------

    /// Every grain this store holds anything for under `shard` — records, a
    /// snapshot, or blobs. The migration driver enumerates a shard's grains from a
    /// read quorum of its replicas with this, so a grain committed while this node
    /// was down is still found on the others. Spans both areas (records and blobs),
    /// so it lives here rather than on [`GrainBlobStore`].
    fn grains(&self, shard: u32) -> Vec<GrainName>;

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

    /// Drop every trace of every grain under `shard` whose name hash is `>= from` —
    /// the same half-open range [`seal_range`](GrainStore::seal_range) bounds, and the
    /// split driver's GC of the parent-keyed data a committed split moved away.
    ///
    /// Stated as a range rather than a `grains`-then-`remove_grain` loop so a store
    /// that keys by name hash can honour it by discarding whole files. Idempotent.
    fn remove_range(&self, shard: u32, from: u64);

    /// Drop everything this store holds under `shard` — every grain's records,
    /// snapshot, and blobs, and the shard's fence and append bound.
    ///
    /// The reclamation a committed merge performs on the retired shard, whose grains
    /// now live under the surviving shard's keys (§7.7). Idempotent.
    fn drop_shard(&self, shard: u32);

    /// An estimate of the bytes this store holds under `shard` — records,
    /// snapshots, and blobs. The split trigger's size signal (§7.7,
    /// `shard_target_bytes`); an estimate is enough, so implementations may
    /// ignore framing overhead.
    fn shard_bytes(&self, shard: u32) -> u64;
}

/// How the runtime obtains a node's [`GrainStore`] (spec §7.4). Supplied on
/// [`GranaryConfig`](crate::GranaryConfig); a factory that **caches per
/// `(grain_type, node)`**, held by the deployment across a restart, is what makes a
/// grain's records survive a full-cluster cold restart. The default is a fresh
/// ephemeral [`MemoryGrainStore`] per store (lost on restart).
///
/// The `grain_type` is not decoration: a store's fence and append bound are keyed by
/// shard *index*, while a shard's identity — and the leader-election group whose term
/// the fence holds — is `(grain_type, index)` (§8.2). Two types are two independent
/// groups whose terms advance independently, so one store shared across types would
/// let the type electing more often raise the fence past the other's term, and every
/// append and recovery read of the quieter type is then refused forever: `NotLeader`
/// on the append (a live activation steps down), `Unavailable` on the read (the next
/// activation cannot rehydrate, so the grain never comes back). A deployment that
/// hosts several types therefore MUST give each its own store, and a factory that
/// keys its cache by both cannot do otherwise.
pub type GrainStoreFactory = Arc<dyn Fn(&str, NodeId) -> Arc<dyn GrainStore> + Send + Sync>;

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
/// ops the grain's snapshot made redundant.
///
/// Distinct from [`GrainRecords`] because this is the frozen on-disk contract (it must
/// deserialize old segments), while `GrainRecords` is the live in-memory
/// representation and stays free to change. [`export`](GrainRecords::export) and
/// [`from_checkpoint`](GrainRecords::from_checkpoint) are the only bridge.
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

    /// The latest snapshot as `(seq, state)`, cloning the snapshot's bytes and nothing
    /// else — the records stay where they are.
    pub(crate) fn snapshot(&self) -> Option<(Seq, Vec<u8>)> {
        self.snapshot
            .as_ref()
            .map(|(seq, _term, state)| (*seq, state.clone()))
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

    /// Up to `limit` occupied records after `from` (exclusive), ascending, with
    /// this segment's compaction base — one consistent view under the segment's
    /// lock ([`RecordPage`]). Clones only the returned window — the ranged
    /// `load` read (§7.3).
    pub(crate) fn read_from(&self, from: Seq, limit: usize) -> RecordPage {
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
        RecordPage {
            base: self.base,
            records: out,
        }
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
        // Optimistic head check (§8): a normal append landing on a slot this replica
        // has already filled means the leader's head is stale — reject so it steps
        // down and re-recovers rather than overwriting a committed record. An append
        // whose `after` is below our compacted base is stale by the same logic (the
        // leader recovered without seeing our snapshot).
        //
        // The one occupied slot an append may proceed onto is a **re-delivery of the
        // very append that filled it** (§7.2: the wire may duplicate, and a drained
        // straggler can arrive late), identified by the shard `term` as well as the
        // bytes. Bytes alone are not an identity — two clients issuing the same command
        // encode identically — so a byte-only check would read a *different* append
        // onto a stale head as a duplicate, store it over the record that slot already
        // committed, and ack: two leaders report their write durable while one record
        // exists, an acknowledged write lost (**G14**). A genuine re-delivery carries
        // the term of the append that made it; a stale-head append from a newer leader
        // never can.
        if kind == WriteKind::Append {
            if after.value() < base {
                return StoreAck::Stale(self.head());
            }
            let first_local = (after.value() - base) as usize;
            let stale = records.iter().enumerate().any(|(offset, incoming)| {
                match self.slots.get(first_local + offset) {
                    Some(Some(existing)) => existing.term != term || &existing.bytes != incoming,
                    _ => false,
                }
            });
            if stale {
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
        // slots (records past `at`, if any, shift down behind the new base).
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

    /// Drop records past slot `after` whose term is at most `term` (the rollback of an
    /// uncommitted tail, §7.2). Term-aware per slot: a higher-term record above `after`
    /// is a newer leader's, possibly committed, and MUST survive (G14). A slot at or
    /// below `term` above the roll-backer's own head is by construction uncommitted (a
    /// committed slot there would have raised the head its recovery adopted).
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

    /// Project a known grain's records under its segment lock, or `None` if the grain is
    /// unknown — the shared body of every read this store answers, so the lookup, the
    /// lock, and its poison message are written once rather than per accessor.
    fn with_records<R>(
        &self,
        shard: u32,
        grain: &GrainName,
        project: impl FnOnce(&GrainRecords) -> R,
    ) -> Option<R> {
        self.existing(shard, grain)
            .map(|segment| project(&segment.lock().expect("grain segment poisoned")))
    }
}

/// The per-shard write-guard every [`GrainStore`] shares (§7.7, §8): the append
/// bound and the term fence, and the *policy* that composes them. How the two
/// primitives persist is per-store ([`MemoryGrainStore`] keeps them in memory; the
/// file store fsyncs each fence bump); the ordering and the
/// which-write-kinds-bypass rules live here, in one place, so they cannot diverge
/// between the stores.
///
/// Every method is called *inside the grain's held segment lock* — the caller locks
/// the segment, guards, then applies — so a write and that grain's recovery
/// `prepare` serialize on the segment lock (the only fencing-critical race, §8). The
/// `sealed`/`bump_fence` leaf locks are short critical sections taken beneath it.
pub(crate) trait WriteGuard {
    /// Whether the shard's append bound refuses this grain's appends (§7.7). An
    /// append that passed this check is durably applied before any observer can act
    /// on the bound, because it runs inside the segment lock.
    fn sealed(&self, shard: u32, grain: &GrainName) -> bool;

    /// Check the shard fence against `term`, bumping it durably to `term` on a strict
    /// advance. Returns the blocking (higher, already-acknowledged) fence on refusal,
    /// so a deposed leader learns it has been fenced (§8). A same-term append does not
    /// rewrite the fence — only a strict advance changes it.
    fn bump_fence(&self, shard: u32, term: Term) -> Result<(), BumpRefusal>;

    /// Guard a fenced **record** store; `Err` is the refusal ack the store returns.
    ///
    /// The append bound (§7.7) is checked FIRST, before the fence can bump: a moved
    /// range accepts no new [`WriteKind::Append`] at any term, and a refused append
    /// must not advance the fence as a side effect (that would fence the legitimate
    /// leader's own writes to the range it still owns). Repairs and transfers are not
    /// bounded — the split driver itself recovers and copies the moved grains after
    /// sealing. The fence then applies as in [`guard_snapshot`](WriteGuard::guard_snapshot).
    fn guard_record(
        &self,
        shard: u32,
        grain: &GrainName,
        term: Term,
        kind: WriteKind,
    ) -> Result<(), StoreAck> {
        if kind == WriteKind::Append && self.sealed(shard, grain) {
            return Err(StoreAck::Sealed);
        }
        self.guard_snapshot(shard, term, kind)
    }

    /// Guard a fenced store that the append bound does **not** cover — a snapshot
    /// (§9) is not an append to the moved range, so only the fence guards it. A
    /// [`WriteKind::Transfer`] skips the fence, for the reason stated on that variant.
    fn guard_snapshot(&self, shard: u32, term: Term, kind: WriteKind) -> Result<(), StoreAck> {
        if kind != WriteKind::Transfer {
            match self.bump_fence(shard, term) {
                Ok(()) => {}
                Err(BumpRefusal::Fenced(fence)) => return Err(StoreAck::Fenced(fence)),
                Err(BumpRefusal::Failed) => return Err(StoreAck::Failed),
            }
        }
        Ok(())
    }
}

/// Why a fence bump did not happen (spec §8).
///
/// The two arms are opposites and must not be conflated: `Fenced` is this replica
/// *enforcing* the fence, and the caller learns a real, higher term from it;
/// `Failed` is this replica unable to record a fence at all, so it has promised
/// nothing and its answer carries no information about any term.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BumpRefusal {
    /// A higher term is already acknowledged here; carries it so a deposed leader
    /// learns it has been fenced.
    Fenced(Term),
    /// The bump could not be made durable, so it was not made.
    Failed,
}

impl WriteGuard for MemoryGrainStore {
    fn sealed(&self, shard: u32, grain: &GrainName) -> bool {
        self.inner
            .seals
            .lock()
            .expect("grain store seals poisoned")
            .get(&shard)
            .is_some_and(|&from| crate::system::name_at_or_above(grain, from))
    }

    /// Never `Failed`: an in-memory store has nothing that can fail to persist.
    fn bump_fence(&self, shard: u32, term: Term) -> Result<(), BumpRefusal> {
        let mut fences = self
            .inner
            .fences
            .lock()
            .expect("grain store fences poisoned");
        let fence = *fences.get(&shard).unwrap_or(&Term::ZERO);
        if term < fence {
            return Err(BumpRefusal::Fenced(fence));
        }
        if term > fence {
            fences.insert(shard, term);
        }
        Ok(())
    }
}

// An in-memory store has no stability to wait for, so its outcomes are stable the
// moment they are settled.
impl GrainBlobStore for MemoryGrainStore {
    fn put_blob(&self, shard: u32, grain: &GrainName, id: BlobId, bytes: Vec<u8>) -> BlobAck {
        self.inner
            .blobs
            .lock()
            .expect("grain store blobs poisoned")
            .entry((shard, grain.clone()))
            .or_default()
            .entry(id)
            .or_insert(bytes);
        BlobAck::Stored
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

    fn blob_ids(&self, shard: u32, grain: &GrainName) -> Vec<BlobId> {
        self.inner
            .blobs
            .lock()
            .expect("grain store blobs poisoned")
            .get(&(shard, grain.clone()))
            .map(|area| area.keys().copied().collect())
            .unwrap_or_default()
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
        // Guard and apply under the segment lock (the fencing race, §8).
        let mut records_guard = segment.lock().expect("grain segment poisoned");
        if let Err(ack) = self.guard_record(shard, grain, term, kind) {
            return ack;
        }
        records_guard.store_record(after, term, records, kind)
    }

    fn read(&self, shard: u32, grain: &GrainName) -> ReadReply {
        self.with_records(shard, grain, GrainRecords::read)
            .unwrap_or_else(|| ReadReply {
                slots: Vec::new(),
                snapshot: None,
            })
    }

    // `head` and `snapshot` read the segment directly rather than through `read`, whose
    // reply would clone every record the grain holds only for the caller to drop them.
    // `GrainRecords::head` counts the same leading gap-free run over the snapshot's seq
    // that `ReadReply::head` folds — `base` equals that seq whenever a snapshot exists —
    // so the answer is unchanged and nothing is copied to reach it.

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
    ) -> RecordPage {
        match self.existing(shard, grain) {
            Some(segment) => segment
                .lock()
                .expect("grain segment poisoned")
                .read_from(from, limit),
            None => RecordPage {
                base: Seq::ZERO,
                records: Vec::new(),
            },
        }
    }

    fn prepare(&self, shard: u32, grain: &GrainName, term: Term) -> ReadOutcome {
        let segment = self.segment(shard, grain);
        let records_guard = segment.lock().expect("grain segment poisoned");
        // The promise, under the grain's segment lock (§8). An in-memory fence cannot
        // fail to persist, so `Failed` is unreachable here — mapped rather than
        // asserted, since the arm costs a line and an assertion costs a panic.
        match self.bump_fence(shard, term) {
            Ok(()) => ReadOutcome::Prepared(records_guard.read()),
            Err(BumpRefusal::Fenced(fence)) => ReadOutcome::Fenced(fence),
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
        let segment = self.segment(shard, grain);
        let mut records_guard = segment.lock().expect("grain segment poisoned");
        if let Err(ack) = self.guard_snapshot(shard, term, kind) {
            return ack;
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

    fn remove_range(&self, shard: u32, from: u64) {
        // The same half-open hash range `seal_range` bounds, so a grain the bound
        // refuses appends for is exactly a grain this discards.
        let moved = |(s, grain): &(u32, GrainName)| {
            *s == shard && crate::system::name_at_or_above(grain, from)
        };
        self.inner
            .segments
            .lock()
            .expect("grain store segments poisoned")
            .retain(|key, _| !moved(key));
        self.inner
            .blobs
            .lock()
            .expect("grain store blobs poisoned")
            .retain(|key, _| !moved(key));
    }

    fn drop_shard(&self, shard: u32) {
        self.inner
            .segments
            .lock()
            .expect("grain store segments poisoned")
            .retain(|(s, _), _| *s != shard);
        self.inner
            .blobs
            .lock()
            .expect("grain store blobs poisoned")
            .retain(|(s, _), _| *s != shard);
        // The fence and the bound go with the data: the shard is retired, and a
        // later shard reusing this index starts from a clean promise.
        self.inner
            .fences
            .lock()
            .expect("grain store fences poisoned")
            .remove(&shard);
        self.inner
            .seals
            .lock()
            .expect("grain store seals poisoned")
            .remove(&shard);
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

    /// The property the synchronous seam exists to enforce: the write happens at
    /// **call** time, under the grain's segment lock, not when the caller gets around
    /// to reading the outcome. An `async fn` store would run its body at first poll,
    /// and a caller that never polled would silently not write — which is why the seam
    /// returns a settled value rather than a future (§8, §18.1).
    #[test]
    fn the_write_happens_when_the_call_is_made_not_when_its_outcome_is_read() {
        let store = MemoryGrainStore::new();
        let n = name("eager");
        // The outcome is deliberately discarded — no read of any kind. `drop` is what
        // discharges the `#[must_use]`, and doing so is the point of the test.
        drop(store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::ZERO,
            vec![vec![1], vec![2]],
            WriteKind::Append,
        ));
        assert_eq!(
            store.head(0, &n),
            Seq::new(2),
            "the record must be stored by the time the call returned",
        );
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
        // Tighten the bound to cover the grain: appends refused at ANY term, including
        // one above the fence, so a leader that has not applied the split is stopped
        // (G15).
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
        let _ = store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"e1".to_vec()],
            WriteKind::Append,
        );
        let _ = store.store_record(
            1,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"other-shard".to_vec()],
            WriteKind::Append,
        );
        let id = BlobId::of(b"blob");
        let _ = store.put_blob(0, &n, id, b"blob".to_vec());
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
        let _ = store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![vec![b'x'; 100]],
            WriteKind::Append,
        );
        let _ = store.put_blob(0, &n, BlobId::of(b"b"), vec![b'y'; 50]);
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
        let _ = store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"e1".to_vec(), b"e2".to_vec(), b"e3".to_vec()],
            WriteKind::Append,
        );
        assert_eq!(
            store.read_from(0, &n, Seq::ZERO, 10),
            RecordPage {
                base: Seq::ZERO,
                records: vec![
                    (Seq::new(1), b"e1".to_vec()),
                    (Seq::new(2), b"e2".to_vec()),
                    (Seq::new(3), b"e3".to_vec()),
                ],
            }
        );
        // Exclusive of `from`, bounded by `limit`.
        assert_eq!(
            store.read_from(0, &n, Seq::new(1), 1),
            RecordPage {
                base: Seq::ZERO,
                records: vec![(Seq::new(2), b"e2".to_vec())],
            }
        );
        assert_eq!(
            store.read_from(0, &n, Seq::new(3), 10),
            RecordPage {
                base: Seq::ZERO,
                records: Vec::new(),
            }
        );
        // A read past a compacted base returns the live tail only, and the page
        // reports the base — the reader's one way to tell the drained prefix
        // from slots that were never occupied.
        let _ = store.store_snapshot(
            0,
            &n,
            Seq::new(2),
            Term::new(1),
            b"snap@2".to_vec(),
            WriteKind::Append,
        );
        assert_eq!(
            store.read_from(0, &n, Seq::ZERO, 10),
            RecordPage {
                base: Seq::new(2),
                records: vec![(Seq::new(3), b"e3".to_vec())],
            }
        );
    }

    #[test]
    fn a_lower_term_write_is_fenced() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        let _ = store.store_record(
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
        let _ = store.store_record(
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
        let _ = store.store_record(
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
    fn an_identical_record_from_a_newer_term_is_still_a_stale_head() {
        // The bytes of a command are not its identity; the shard term is what tells a
        // re-delivery from a different append landing on a stale head (see
        // `GrainRecords::store_record`, **G14**).
        let store = MemoryGrainStore::new();
        let n = name("a");
        let _ = store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"deposit-1".to_vec()],
            WriteKind::Append,
        );
        assert_eq!(
            store.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(2),
                vec![b"deposit-1".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stale(Seq::new(1)),
            "a newer leader appending onto a head it recovered stale must be refused, \
             however its record happens to encode",
        );
        // The committed record is untouched, still at the term that committed it.
        assert_eq!(
            store.read(0, &n).slots,
            vec![(Seq::new(1), Term::new(1), b"deposit-1".to_vec())]
        );
        // The case this tolerance exists for still works: on a replica that has
        // seen no newer term, the very same append re-delivered (a duplicated
        // frame, or a drained straggler arriving late, §7.2) is idempotent.
        let fresh = MemoryGrainStore::new();
        let _ = fresh.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"deposit-1".to_vec()],
            WriteKind::Append,
        );
        assert_eq!(
            fresh.store_record(
                0,
                &n,
                Seq::ZERO,
                Term::new(1),
                vec![b"deposit-1".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stored(Seq::new(1))
        );
        // And a batch is judged over every slot it targets, not just the first: a
        // stale head that happens to line up on a gap must not let the rest of the
        // batch overwrite what follows it.
        let gapped = MemoryGrainStore::new();
        let _ = gapped.store_record(
            0,
            &n,
            Seq::new(2),
            Term::new(1),
            vec![b"third".to_vec()],
            WriteKind::Append,
        );
        assert_eq!(
            gapped.store_record(
                0,
                &n,
                Seq::new(1),
                Term::new(2),
                vec![b"second".to_vec(), b"third".to_vec()],
                WriteKind::Append
            ),
            StoreAck::Stale(Seq::ZERO),
            "slot 2 was empty but slot 3 is occupied under an older term",
        );
    }

    #[test]
    fn contiguous_head_stops_at_the_first_gap() {
        let store = MemoryGrainStore::new();
        let n = name("a");
        let _ = store.store_record(
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
        let _ = store.store_record(
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
        let _ = store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(1),
            vec![b"e1".to_vec()],
            WriteKind::Append,
        );
        // A snapshot well past this replica's records (an InstallSnapshot analogue):
        // all slots drop, the base jumps to 5, and the head follows.
        let _ = store.store_snapshot(
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
        let _ = store.store_snapshot(
            0,
            &n,
            Seq::new(3),
            Term::new(2),
            b"snap@3".to_vec(),
            WriteKind::Append,
        );
        // A recovery write-back from base 1 re-offers seqs 2..=4; seqs 2,3 are already
        // subsumed by the snapshot, so only seq 4 is stored — no gap, no regression.
        let _ = store.store_record(
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
        let _ = store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(5),
            vec![b"e1".to_vec()],
            WriteKind::Append,
        );
        // A tentative append at head 1 that failed its quorum rolls back.
        let _ = store.store_record(
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
        // The G14 regression (§7.2 rollback): a deposed leader's failed append rolls
        // back at its own term while a NEW leader (higher term) has already committed
        // records above the same head here. It must drop only its own tentative slot.
        let store = MemoryGrainStore::new();
        let n = name("a");
        let _ = store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(5),
            vec![b"e1".to_vec()],
            WriteKind::Append,
        );
        // The new leader (term 6) repairs slot 2 and appends slot 3 to this replica.
        let _ = store.store_record(
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
        let _ = store.store_record(
            0,
            &n,
            Seq::ZERO,
            Term::new(5),
            vec![b"e1".to_vec()],
            WriteKind::Append,
        );
        // Own-term tentative at slot 2; a newer leader's record at slot 3.
        let _ = store.store_record(
            0,
            &n,
            Seq::new(1),
            Term::new(5),
            vec![b"mine".to_vec()],
            WriteKind::Append,
        );
        let _ = store.store_record(
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
        let _ = store.put_blob(0, &n, id, b"block".to_vec());
        // Idempotent: a second store of equal content keeps the one copy (B2).
        let _ = store.put_blob(0, &n, id, b"block".to_vec());
        assert!(store.has_blob(0, &n, id));
        assert_eq!(store.get_blob(0, &n, id), Some(b"block".to_vec()));
        // A different grain's blob area is independent.
        assert!(!store.has_blob(0, &name("b"), id));
    }

    #[test]
    fn delete_blob_evicts_one_and_lets_a_replacement_be_stored() {
        // The read path's corruption self-heal (§7.10): a content-addressed put of an
        // id already on disk is a no-op, so a corrupt copy must be evicted first.
        let store = MemoryGrainStore::new();
        let n = name("a");
        let id = BlobId::of(b"good");
        let _ = store.put_blob(0, &n, id, b"corrupt".to_vec()); // a copy that does not verify
        assert!(store.has_blob(0, &n, id));
        // Without eviction, a re-put keeps the corrupt copy (idempotent on the id).
        let _ = store.put_blob(0, &n, id, b"good".to_vec());
        assert_eq!(store.get_blob(0, &n, id), Some(b"corrupt".to_vec()));
        // Evict, then re-put: now the good bytes land.
        store.delete_blob(0, &n, id);
        assert!(!store.has_blob(0, &n, id));
        let _ = store.put_blob(0, &n, id, b"good".to_vec());
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
            let _ = store.put_blob(0, &n, id, bytes.to_vec());
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
        let _ = store.put_blob(0, &n, a, b"a".to_vec());
        store.delete_blobs(0, &n);
        assert!(!store.has_blob(0, &n, a));
        assert_eq!(store.get_blob(0, &n, a), None);
    }
}
