//! The journal seam (spec §7.3).
//!
//! A grain's journal is its durable, totally-ordered, append-only log of events
//! — the **source of truth** (§1, invariant **G3**). The [`GrainJournal`] trait is a
//! simulation and deployment seam like `Transport` and `Clock` (actor §4.6, §7),
//! operating on opaque, codec-encoded event bytes so it stays codec-agnostic.
//!
//! Two reference tiers implement it (§7.4): the Tier-1 single-node
//! [`MemoryGrainJournal`](crate::memory::MemoryGrainJournal) and the Tier-2 sharded Raft
//! [`RaftGrainJournal`](crate::shard::RaftGrainJournal). Because the methods return
//! `impl Future`, the trait is not object-safe; it is threaded as a concrete type,
//! the way the framework threads its other seams (actor §4.6).

use std::future::Future;

use actor_core::BoxFuture;
use serde::Deserialize;
use serde::Serialize;

use crate::grain::GrainName;

/// The position of an event in one grain's total order (spec §7.3). The first
/// event commits at `1`; [`Seq::ZERO`] is the empty head.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
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
    /// Never produced by the Tier-1 single-node journal.
    NotLeader(actor_core::NodeId),
    /// The shard cannot reach a quorum; the grain pauses writes (§11). Never
    /// produced by the Tier-1 single-node journal.
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

/// Up to `limit` of a grain's events after `from` (exclusive), each tagged with
/// its 1-based [`Seq`] (the event at vec index `i` is `Seq` `i + 1`). The one
/// read primitive shared by both journal tiers' [`load`](GrainJournal::load) (§7.3),
/// so the slice arithmetic lives in a single place.
pub(crate) fn slice(events: &[Vec<u8>], from: Seq, limit: usize) -> Vec<(Seq, Vec<u8>)> {
    let start = from.value() as usize;
    events
        .iter()
        .enumerate()
        .skip(start)
        .take(limit)
        .map(|(i, bytes)| (Seq::new(i as u64 + 1), bytes.clone()))
        .collect()
}

/// A grain's committed head from its event vec: the [`Seq`] of the last event, or
/// [`Seq::ZERO`] when empty. Shared by both tiers (§7.3).
pub(crate) fn head_of(events: &[Vec<u8>]) -> Seq {
    Seq::new(events.len() as u64)
}

/// A grain's durable, append-only event log and snapshot store (spec §7.3).
///
/// All methods address one grain by name; the implementation keys each grain's
/// events and snapshot independently. The host calls [`append`](GrainJournal::append)
/// only from the shard leader and behind the grain's input gate (§6), so `after`
/// always equals the grain's known head.
pub trait GrainJournal: Clone + Send + Sync + 'static {
    /// Append `events` for one grain immediately after `after`, as one atomic
    /// entry. Commits on a Raft quorum (Tier 2) or a local store (Tier 1).
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
    /// rehydration (§9, invariant **G3**). `Seq::ZERO` for a grain with no
    /// committed events.
    ///
    /// Not in the literal §7.3 trait sketch, but required so rehydration derives
    /// `head` (and the snapshot guard of **G4**) from the journal rather than
    /// trusting memory; the log always knows its head, and Tier-2 Raft knows the
    /// per-grain commit index.
    fn head(&self, grain: &GrainName)
    -> impl Future<Output = Result<Seq, GrainJournalError>> + Send;

    /// Persist a snapshot for one grain at a committed seq (§9). Returns
    /// `Committed(at)` on success, or `NotLeader` if this node no longer leads.
    fn save_snapshot(
        &self,
        grain: &GrainName,
        at: Seq,
        state: Vec<u8>,
    ) -> impl Future<Output = AppendOutcome> + Send;

    /// The latest snapshot for one grain, if any (§9).
    fn load_snapshot(
        &self,
        grain: &GrainName,
    ) -> impl Future<Output = Result<Option<(Seq, Vec<u8>)>, GrainJournalError>> + Send;

    /// Block until this node's local view reflects every event committed *as of
    /// now* — the **rehydration barrier** (spec §9, invariant **G3**). The host
    /// awaits this before reading [`head`](GrainJournal::head)/[`load`](GrainJournal::load)
    /// on activation, so a grain never rebuilds its state from a still-draining
    /// view and then serves stale reads or folds onto a short head.
    ///
    /// Tier 1 is synchronous (its store *is* the committed state), so the default
    /// is a no-op. Tier 2 overrides it to wait for its commit-stream projection to
    /// reach the shard leader's commit index.
    fn catch_up(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// The object-safe form of [`GrainJournal`], so the runtime can hold a journal as
/// `Arc<dyn DynGrainJournal>` and **select the durability tier at construction** —
/// the Tier-1 [`MemoryGrainJournal`](crate::memory::MemoryGrainJournal) or the Tier-2
/// [`RaftGrainJournal`](crate::shard::RaftGrainJournal) — without threading a `J` type
/// parameter through `Host`/`Gateway`/`GrainRef`/`Granary` (which would leak `J`
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

    fn catch_up(&self) -> BoxFuture<'static, ()>;
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

    fn catch_up(&self) -> BoxFuture<'static, ()> {
        let journal = self.clone();
        Box::pin(async move { journal.catch_up().await })
    }
}
