//! The journal seam (spec §7.3).
//!
//! A grain's journal is its durable, totally-ordered, append-only log of events
//! — the **source of truth** (§1, invariant **G3**). The [`GrainJournal`] trait is a
//! simulation and deployment seam like `Transport` and `Clock` (actor §4.6, §7),
//! operating on opaque, codec-encoded event bytes so it stays codec-agnostic.
//!
//! Two reference tiers implement it (§7.4): the single-node `Local`
//! [`LocalGrainJournal`](crate::memory::LocalGrainJournal) and the clustered `Quorum`
//! [`QuorumGrainJournal`](crate::shard::QuorumGrainJournal). Because the methods
//! return `impl Future`, the trait is not object-safe; it is threaded as a concrete
//! type, the way the framework threads its other seams (actor §4.6).

use std::future::Future;

use actor_core::BoxFuture;
use serde::Deserialize;
use serde::Serialize;

use crate::grain::GrainName;

/// The position of an event in one grain's total order (spec §7.3). The first
/// event commits at `1`; [`Seq::ZERO`] is the empty head.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default, Serialize, Deserialize,
)]
pub struct Seq(u64);

impl Seq {
    /// The empty head: a grain with no committed events (spec §7.3).
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

/// The outcome of an [`append`](GrainJournal::append) or
/// [`save_snapshot`](GrainJournal::save_snapshot) (spec §7.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppendOutcome {
    /// Durable on a quorum; carries the new head (for a snapshot, the snapshot
    /// seq).
    Committed(Seq),
    /// This node no longer leads the shard; redirect to the hinted leader (§8).
    /// Never produced by the `Local` single-node journal.
    NotLeader(actor_core::NodeId),
    /// The shard cannot reach a quorum; the grain pauses writes (§11). Never
    /// produced by the `Local` single-node journal.
    Unavailable(String),
}

/// A failure of a *local* journal read (spec §7.3): I/O or corruption.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GrainJournalError {
    /// A local read could not complete (I/O or corruption).
    Unavailable(String),
}

impl std::fmt::Display for GrainJournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GrainJournalError::Unavailable(why) => write!(f, "journal read failed: {why}"),
        }
    }
}

impl std::error::Error for GrainJournalError {}

/// A grain's durable, append-only event log and snapshot store (spec §7.3).
///
/// All methods address one grain by name; the implementation keys each grain's
/// events and snapshot independently. The host calls [`append`](GrainJournal::append)
/// only from the shard leader and behind the grain's input gate (§6), so `after`
/// always equals the grain's known head.
pub trait GrainJournal: Clone + Send + Sync + 'static {
    /// Append `events` for one grain immediately after `after`, as one atomic
    /// entry. Commits on a Raft quorum (the `Quorum` tier) or a local store (the `Local` tier).
    fn append(
        &self,
        grain: &GrainName,
        after: Seq,
        events: Vec<Vec<u8>>,
    ) -> impl Future<Output = AppendOutcome> + Send;

    /// Up to `limit` committed events for one grain from `from` (exclusive)
    /// toward its head, in ascending `Seq` order. A local, fence-free read on the
    /// leader.
    fn load(
        &self,
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<(Seq, Vec<u8>)>, GrainJournalError>> + Send;

    /// The grain's committed head — the authoritative source of `head` on
    /// rehydration (§9, invariant **G3**), and the **rehydration barrier** itself.
    /// `Seq::ZERO` for a grain with no committed events.
    ///
    /// On the `Quorum` tier this recovers the head from a write quorum of the
    /// shard's replicas by read-repair (highest-term record per slot, written back
    /// under the leader's own term, §8), so a fresh leader never folds onto a stale
    /// head and subsequent `load`s read locally; on the `Local` tier it reads
    /// locally. Rehydration derives `head` from this, never from memory.
    fn head(
        &self,
        grain: &GrainName,
    ) -> impl Future<Output = Result<Seq, GrainJournalError>> + Send;

    /// Persist a snapshot for one grain at a committed seq (§9). Returns
    /// `Committed(at)` on success, or `NotLeader` if this node no longer leads.
    fn save_snapshot(
        &self,
        grain: &GrainName,
        at: Seq,
        state: Vec<u8>,
    ) -> impl Future<Output = AppendOutcome> + Send;

    /// The latest snapshot for one grain, if any (§9). On the `Quorum` tier this
    /// recovers the latest durable snapshot from a write quorum (the snapshot
    /// analogue of `head`'s record recovery, §8); on the `Local` tier it reads
    /// locally.
    fn load_snapshot(
        &self,
        grain: &GrainName,
    ) -> impl Future<Output = Result<Option<(Seq, Vec<u8>)>, GrainJournalError>> + Send;
}

/// The object-safe form of [`GrainJournal`], so the runtime can hold a journal as
/// `Arc<dyn DynGrainJournal>` and **select the durability tier at construction** —
/// the `Local` [`LocalGrainJournal`](crate::memory::LocalGrainJournal) or the `Quorum`
/// [`QuorumGrainJournal`](crate::shard::QuorumGrainJournal) — without threading a `J`
/// type parameter through `Host`/`Gateway`/`GrainRef`/`Granary` (which would leak `J`
/// into the user-facing `GrainCtx` and handler signatures).
///
/// `GrainJournal`'s `impl Future` returns are not object-safe; this mirror boxes them
/// (`BoxFuture`). The blanket impl below adapts any [`GrainJournal`] — it clones the
/// journal (a [`GrainJournal`] is `Clone`) and the grain name into each boxed future,
/// so the returned future is `'static` and the caller borrows nothing.
/// The boxed result of [`DynGrainJournal::load`].
pub type LoadFuture = BoxFuture<'static, Result<Vec<(Seq, Vec<u8>)>, GrainJournalError>>;
/// The boxed result of [`DynGrainJournal::load_snapshot`].
pub type LoadSnapshotFuture = BoxFuture<'static, Result<Option<(Seq, Vec<u8>)>, GrainJournalError>>;

pub trait DynGrainJournal: Send + Sync + 'static {
    fn append(
        &self,
        grain: &GrainName,
        after: Seq,
        events: Vec<Vec<u8>>,
    ) -> BoxFuture<'static, AppendOutcome>;

    fn load(&self, grain: &GrainName, from: Seq, limit: usize) -> LoadFuture;

    fn head(&self, grain: &GrainName) -> BoxFuture<'static, Result<Seq, GrainJournalError>>;

    fn save_snapshot(
        &self,
        grain: &GrainName,
        at: Seq,
        state: Vec<u8>,
    ) -> BoxFuture<'static, AppendOutcome>;

    fn load_snapshot(&self, grain: &GrainName) -> LoadSnapshotFuture;
}

impl<J: GrainJournal> DynGrainJournal for J {
    fn append(
        &self,
        grain: &GrainName,
        after: Seq,
        events: Vec<Vec<u8>>,
    ) -> BoxFuture<'static, AppendOutcome> {
        let journal = self.clone();
        let grain = grain.clone();
        Box::pin(async move { journal.append(&grain, after, events).await })
    }

    fn load(&self, grain: &GrainName, from: Seq, limit: usize) -> LoadFuture {
        let journal = self.clone();
        let grain = grain.clone();
        Box::pin(async move { journal.load(&grain, from, limit).await })
    }

    fn head(&self, grain: &GrainName) -> BoxFuture<'static, Result<Seq, GrainJournalError>> {
        let journal = self.clone();
        let grain = grain.clone();
        Box::pin(async move { journal.head(&grain).await })
    }

    fn save_snapshot(
        &self,
        grain: &GrainName,
        at: Seq,
        state: Vec<u8>,
    ) -> BoxFuture<'static, AppendOutcome> {
        let journal = self.clone();
        let grain = grain.clone();
        Box::pin(async move { journal.save_snapshot(&grain, at, state).await })
    }

    fn load_snapshot(&self, grain: &GrainName) -> LoadSnapshotFuture {
        let journal = self.clone();
        let grain = grain.clone();
        Box::pin(async move { journal.load_snapshot(&grain).await })
    }
}
