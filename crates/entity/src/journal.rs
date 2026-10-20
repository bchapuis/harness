//! The journal seam (spec storage §3.1).
//!
//! An entity's journal is its durable, totally-ordered, append-only log of events:
//! the **source of truth** (§1, invariant **E3**). The [`EntityJournal`] trait is a
//! simulation and deployment seam like `Transport` and `Clock` (actor §11.1, cluster §4),
//! operating on opaque, codec-encoded event bytes so it stays codec-agnostic.
//!
//! Two reference tiers implement it (storage §3.2): the single-node `Local`
//! [`LocalEntityJournal`](crate::memory::LocalEntityJournal) and the clustered `Quorum`
//! (`EntityJournal for Arc<QuorumReplicator>`, replicator.rs). Because the methods
//! return `impl Future`, the trait is not object-safe; it is threaded as a concrete
//! type (actor §11.1).

use std::future::Future;

use actor_core::BoxFuture;
use serde::Deserialize;
use serde::Serialize;

use crate::blobs::BlobId;
use crate::entity::EntityName;

/// The position of an event in one entity's total order (spec storage §3.1). The first
/// event commits at `1`; [`Seq::ZERO`] is the empty head.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default, Serialize, Deserialize,
)]
pub struct Seq(u64);

impl Seq {
    /// The empty head: an entity with no committed events (spec storage §3.1).
    pub const ZERO: Seq = Seq(0);

    /// Wrap a raw sequence value.
    pub const fn new(value: u64) -> Seq {
        Seq(value)
    }

    /// The raw sequence value.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for Seq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "seq-{}", self.0)
    }
}

/// The **shard term**: the single-writer fencing token every per-entity append
/// carries (spec storage §6). It is the shard leader-election group's Raft term, so it only
/// ever advances; the store refuses any write stamped below the highest term it has
/// acknowledged. `Ord` is the fence comparison. `Term::ZERO` is the single-node
/// `Local` tier, which never elects and so never fences.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default, Serialize, Deserialize,
)]
pub struct Term(u64);

impl Term {
    /// The `Local` tier's constant term: it never elects, so it never fences (storage §3.2).
    pub const ZERO: Term = Term(0);

    /// Wrap a raw term value (e.g. a leader-election group's Raft term).
    pub const fn new(value: u64) -> Term {
        Term(value)
    }

    /// The raw term value.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "term-{}", self.0)
    }
}

/// The outcome of an [`append`](EntityJournal::append) or
/// [`save_snapshot`](EntityJournal::save_snapshot) (spec storage §3.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppendOutcome {
    /// Durable on a quorum; carries the new head (for a snapshot, the snapshot
    /// seq).
    Committed(Seq),
    /// This node no longer leads the shard; redirect to the hinted leader (storage §6).
    /// Produced only when no replica can hold the batch, so the command provably did
    /// not commit and a caller may re-issue it (§3.2); a deposed append that a peer
    /// may have stored is `Unavailable` instead. Never produced by the `Local`
    /// single-node journal.
    NotLeader(actor_core::NodeId),
    /// The shard cannot reach a quorum; the entity pauses writes (storage §11). The
    /// `Local` single-node journal produces it only when its one store has failed.
    Unavailable(String),
}

/// One [`load`](EntityJournal::load) page (spec storage §3.1): up to `limit` records
/// after `from`, plus the store's compaction **base** at the moment of the read
/// (storage §9), both read under one segment lock. Slots at `Seq <= base` are compacted:
/// subsumed by the entity's snapshot and gone from every read path. Without the
/// base a reader below it cannot tell truncated history from the interleaved
/// slots a projection legally skips (facets §4), so every read reply carries it
/// (§7).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecordPage {
    /// The compaction base: records at `Seq <= base` are no longer readable.
    /// `Seq::ZERO` when nothing has been compacted.
    pub base: Seq,
    /// The records after `max(from, base)`, ascending by `Seq`.
    pub records: Vec<(Seq, Vec<u8>)>,
}

/// A failure of a *local* journal read (spec storage §3.1): I/O or corruption.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntityJournalError {
    /// A local read could not complete (I/O or corruption).
    Unavailable(String),
}

impl std::fmt::Display for EntityJournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntityJournalError::Unavailable(why) => write!(f, "journal read failed: {why}"),
        }
    }
}

impl std::error::Error for EntityJournalError {}

/// An entity's durable, append-only event log and snapshot store (spec storage §3.1).
///
/// All methods address one entity by name; the implementation keys each entity's
/// events and snapshot independently. The host calls [`append`](EntityJournal::append)
/// only from the shard leader and behind the entity's input gate (§6), so `after`
/// always equals the entity's known head.
pub trait EntityJournal: Clone + Send + Sync + 'static {
    /// Append `events` for one entity immediately after `after`, as one atomic
    /// entry. Commits on a write quorum of the shard's replicas (the `Quorum` tier)
    /// or a local store (the `Local` tier).
    fn append(
        &self,
        entity: &EntityName,
        after: Seq,
        events: Vec<Vec<u8>>,
    ) -> impl Future<Output = AppendOutcome> + Send;

    /// Up to `limit` committed events for one entity from `from` (exclusive)
    /// toward its head, in ascending `Seq` order, with the store's compaction
    /// base ([`RecordPage`]). A local, fence-free read on the leader.
    fn load(
        &self,
        entity: &EntityName,
        from: Seq,
        limit: usize,
    ) -> impl Future<Output = Result<RecordPage, EntityJournalError>> + Send;

    /// The entity's committed head: the authoritative source of `head` on
    /// rehydration (storage §9, invariant **E3**), and the **rehydration barrier** itself.
    /// `Seq::ZERO` for an entity with no committed events.
    ///
    /// On the `Quorum` tier this recovers the head from a write quorum of the
    /// shard's replicas by read-repair (highest-term record per slot, written back
    /// under the leader's own term, storage §6), so a fresh leader never folds onto a stale
    /// head and subsequent `load`s read locally. Short of a quorum within
    /// `recover_timeout` it adopts the merged view without write-back, which MAY be
    /// stale but cannot fork a write (storage §6). On the `Local` tier it reads locally. Rehydration derives `head` from this, never from memory.
    fn head(
        &self,
        entity: &EntityName,
    ) -> impl Future<Output = Result<Seq, EntityJournalError>> + Send;

    /// The shard term this journal is writing under **right now** (spec storage §6): the
    /// leader-election group's term while this node leads the shard, `None`
    /// otherwise. A cheap local read, never a network round.
    ///
    /// This is the *validity token* of the gateway's non-activating read (§7):
    /// a [`head`](EntityJournal::head) recovery is trustworthy only while the
    /// leadership it ran under persists; leadership regained is a **higher**
    /// term, so an unchanged term proves no other node could have appended in
    /// between. The `Local` tier never elects and so always leads at
    /// [`Term::ZERO`].
    fn term(&self) -> Option<Term>;

    /// Persist a snapshot for one entity at a committed seq (storage §9). Returns
    /// `Committed(at)` on success, or `NotLeader` if this node no longer leads.
    fn save_snapshot(
        &self,
        entity: &EntityName,
        at: Seq,
        state: Vec<u8>,
    ) -> impl Future<Output = AppendOutcome> + Send;

    /// The latest snapshot for one entity, if any (storage §9), read from the **local**
    /// store on every tier. On the `Quorum` tier the quorum-best snapshot is
    /// chosen during `head`'s recovery (storage §6) and cached locally there, so this
    /// read (like `load`) costs no round trip and follows that recovery.
    fn load_snapshot(
        &self,
        entity: &EntityName,
    ) -> impl Future<Output = Result<Option<(Seq, Vec<u8>)>, EntityJournalError>> + Send;

    // --- The entity-native content-addressed blob store (storage §10) ---
    //
    // An entity's immutable blobs, replicated to the *same* shard replicas as its
    // records but off the ordered/fenced path (no `Seq`, no term). On the journal seam
    // so the host needs no extra dependency to hand the entity a
    // [`EntityBlobs`](crate::blobs::EntityBlobs) handle, and because blobs share
    // everything else the journal already carries per entity: its replicas, its
    // recovery, the migration and split passes, and the simulation seed.

    /// Store an immutable blob for one entity, durable on a write quorum of its
    /// replicas (one local copy on `Local`). Idempotent and dedup'd by content.
    fn put_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
        bytes: Vec<u8>,
    ) -> impl Future<Output = Result<(), EntityJournalError>> + Send;

    /// Fetch a verified blob for one entity, or `None` if no replica holds it.
    fn get_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, EntityJournalError>> + Send;

    /// Whether one entity's blob is present on **any reachable replica** (the local
    /// copy, else the first peer that holds it, not a quorum count). A `true` means a
    /// [`get_blob`](EntityJournal::get_blob) can source the bytes, not that they are
    /// quorum-durable (durability is established at [`put_blob`](EntityJournal::put_blob)).
    fn has_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
    ) -> impl Future<Output = Result<bool, EntityJournalError>> + Send;

    /// Keep only the listed blobs of one entity, dropping the rest: the entity's
    /// mark-from-roots GC. Best-effort.
    fn retain_blobs(
        &self,
        entity: &EntityName,
        retain: Vec<BlobId>,
    ) -> impl Future<Output = ()> + Send;

    /// Drop **all** of one entity's blobs: entity-scoped reclamation on destroy.
    /// Best-effort.
    fn delete_blobs(&self, entity: &EntityName) -> impl Future<Output = ()> + Send;
}

/// The boxed result of [`DynEntityJournal::load`].
pub type LoadFuture = BoxFuture<'static, Result<RecordPage, EntityJournalError>>;
/// The boxed result of [`DynEntityJournal::load_snapshot`].
pub type LoadSnapshotFuture =
    BoxFuture<'static, Result<Option<(Seq, Vec<u8>)>, EntityJournalError>>;

/// The object-safe form of [`EntityJournal`], so the runtime can hold a journal as
/// `Arc<dyn DynEntityJournal>` and **select the durability tier at construction**
/// without threading a `J` type parameter through `Host`/`Gateway`/`EntityRef`/
/// `Entities` (which would leak `J` into the user-facing `EntityCtx` and handler
/// signatures). The blanket impl below clones the journal and the entity name into
/// each boxed future, so the returned future is `'static` and the caller borrows
/// nothing.
pub trait DynEntityJournal: Send + Sync + 'static {
    fn append(
        &self,
        entity: &EntityName,
        after: Seq,
        events: Vec<Vec<u8>>,
    ) -> BoxFuture<'static, AppendOutcome>;

    fn load(&self, entity: &EntityName, from: Seq, limit: usize) -> LoadFuture;

    fn head(&self, entity: &EntityName) -> BoxFuture<'static, Result<Seq, EntityJournalError>>;

    fn term(&self) -> Option<Term>;

    fn save_snapshot(
        &self,
        entity: &EntityName,
        at: Seq,
        state: Vec<u8>,
    ) -> BoxFuture<'static, AppendOutcome>;

    fn load_snapshot(&self, entity: &EntityName) -> LoadSnapshotFuture;

    fn put_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, Result<(), EntityJournalError>>;

    fn get_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, EntityJournalError>>;

    fn has_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
    ) -> BoxFuture<'static, Result<bool, EntityJournalError>>;

    fn retain_blobs(&self, entity: &EntityName, retain: Vec<BlobId>) -> BoxFuture<'static, ()>;

    fn delete_blobs(&self, entity: &EntityName) -> BoxFuture<'static, ()>;
}

impl<J: EntityJournal> DynEntityJournal for J {
    fn append(
        &self,
        entity: &EntityName,
        after: Seq,
        events: Vec<Vec<u8>>,
    ) -> BoxFuture<'static, AppendOutcome> {
        let journal = self.clone();
        let entity = entity.clone();
        Box::pin(async move { journal.append(&entity, after, events).await })
    }

    fn load(&self, entity: &EntityName, from: Seq, limit: usize) -> LoadFuture {
        let journal = self.clone();
        let entity = entity.clone();
        Box::pin(async move { journal.load(&entity, from, limit).await })
    }

    fn head(&self, entity: &EntityName) -> BoxFuture<'static, Result<Seq, EntityJournalError>> {
        let journal = self.clone();
        let entity = entity.clone();
        Box::pin(async move { journal.head(&entity).await })
    }

    fn term(&self) -> Option<Term> {
        EntityJournal::term(self)
    }

    fn save_snapshot(
        &self,
        entity: &EntityName,
        at: Seq,
        state: Vec<u8>,
    ) -> BoxFuture<'static, AppendOutcome> {
        let journal = self.clone();
        let entity = entity.clone();
        Box::pin(async move { journal.save_snapshot(&entity, at, state).await })
    }

    fn load_snapshot(&self, entity: &EntityName) -> LoadSnapshotFuture {
        let journal = self.clone();
        let entity = entity.clone();
        Box::pin(async move { journal.load_snapshot(&entity).await })
    }

    fn put_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, Result<(), EntityJournalError>> {
        let journal = self.clone();
        let entity = entity.clone();
        Box::pin(async move { journal.put_blob(&entity, id, bytes).await })
    }

    fn get_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, EntityJournalError>> {
        let journal = self.clone();
        let entity = entity.clone();
        Box::pin(async move { journal.get_blob(&entity, id).await })
    }

    fn has_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
    ) -> BoxFuture<'static, Result<bool, EntityJournalError>> {
        let journal = self.clone();
        let entity = entity.clone();
        Box::pin(async move { journal.has_blob(&entity, id).await })
    }

    fn retain_blobs(&self, entity: &EntityName, retain: Vec<BlobId>) -> BoxFuture<'static, ()> {
        let journal = self.clone();
        let entity = entity.clone();
        Box::pin(async move { journal.retain_blobs(&entity, retain).await })
    }

    fn delete_blobs(&self, entity: &EntityName) -> BoxFuture<'static, ()> {
        let journal = self.clone();
        let entity = entity.clone();
        Box::pin(async move { journal.delete_blobs(&entity).await })
    }
}
