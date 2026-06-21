//! The application consensus seam (spec §9.4.3).
//!
//! [`RaftConsensus`] is the narrow capability a layer built *on* the cluster — granary's
//! sharded journal — needs from a Raft-hosting [`ClusterSystem`](crate::ClusterSystem):
//! create an application group, propose opaque command bytes to its leader, and
//! observe the committed stream. It is the consensus analogue of granary's own
//! `GranarySystem` seam: it lets that crate stay generic over `R: RaftConsensus`
//! instead of naming the concrete `ClusterSystem<C, E, S, T>` and its four type
//! parameters.
//!
//! It is used as a **generic bound** (`R: RaftConsensus`), never as `dyn RaftConsensus`, so
//! the `impl Future` / concrete-`Receiver` return types are fine — no object
//! safety is required and the futures are never boxed on the hot path.

use std::future::Future;
use std::time::Duration;

use actor_core::BoxFuture;
use actor_core::NodeId;
use async_channel::Receiver;

use crate::raft::Committed;
use crate::raft::GroupId;

/// The consensus capability granary's sharded journal builds on (spec §9.4.3).
/// Implemented by [`ClusterSystem`](crate::ClusterSystem) in leader-based mode;
/// outside it, the methods are inert (no group, no leader).
pub trait RaftConsensus: Clone + Send + Sync + 'static {
    /// This node's identity (the `NotLeader` fallback when no leader is known).
    fn node(&self) -> NodeId;

    /// The cluster's consensus-agreed control-plane voter set (spec §9.4.3): the
    /// control group's *current* committed voters, which grows/shrinks as nodes are
    /// added/removed. A layer above uses it as the target membership for a metadata
    /// group (reconciled via [`reconfigure_group`](RaftConsensus::reconfigure_group)).
    /// Empty outside leader-based mode.
    fn cluster_voters(&self) -> Vec<NodeId>;

    /// The cluster's **statically configured** voter set (the founding
    /// `RaftConfig.voters`, spec §9.4.3) — identical on every node and unchanging,
    /// regardless of when the node joined. A layer above uses it as the **creation
    /// seed** for a metadata group so every node forms the *same* group no matter
    /// when it calls in (a late joiner that seeded from the live `cluster_voters`
    /// would form a divergent group and disrupt elections). Empty outside
    /// leader-based mode.
    fn configured_voters(&self) -> Vec<NodeId>;

    /// Drive `group`'s voter set toward `voters` (spec §9.4.3 item 2): if this node
    /// leads `group`, propose the `AddVoter`/`RemoveVoter` deltas between the
    /// group's current voters and `voters`. A no-op on a non-leader. The engine
    /// replicates the full committed log to a newly added voter, which then catches
    /// up — so a layer above can keep an application group's membership in sync with
    /// the cluster's.
    fn reconfigure_group(&self, group: GroupId, voters: Vec<NodeId>);

    /// Create an application Raft group with `voters` and non-voting `learners`
    /// (spec §9.4.3, §7.1). Voters elect, lead, and form the commit quorum;
    /// learners only replicate the log (so they can route and serve reads) and are
    /// absent from the quorum — bounding `voters` to `R` keeps write quorum at
    /// `⌈R/2⌉` regardless of cluster size. The membership control group is created
    /// at startup; this is for application groups. To see a group's log from its
    /// first entry, call [`subscribe_commits`](RaftConsensus::subscribe_commits) right
    /// after this, before the engine next ticks.
    fn create_group(&self, group: GroupId, voters: Vec<NodeId>, learners: Vec<NodeId>);

    /// Subscribe to `group`'s committed observation stream ([`Committed`]). The
    /// receiver observes every entry committed **after** this call (a late
    /// subscriber misses earlier commits — replay-from-index is a future
    /// extension), interleaved in commit order with any state-machine snapshot
    /// installed on this node (`Committed::Snapshot`), which the consumer applies
    /// by replacing its state.
    fn subscribe_commits(&self, group: GroupId) -> Receiver<Committed>;

    /// Compact `group`'s log up to `index` against the application's state-machine
    /// `snapshot` (spec §9): the engine discards the committed prefix `≤ index` and
    /// keeps `snapshot` so it can bootstrap a lagging or freshly added replica via
    /// an install instead of full-log replay. Local and idempotent — the caller
    /// supplies a snapshot of its own applied prefix, deterministic across replicas;
    /// a stale or not-yet-applied `index` is ignored. Inert outside leader-based mode.
    fn compact(&self, group: GroupId, index: u64, snapshot: Vec<u8>);

    /// Offer an application command to `group`'s leader (spec §9.4.3 item 1):
    /// append locally when leading, else forward to the known leader, else fan
    /// out to the group's voters.
    fn propose_to(&self, group: GroupId, command: Vec<u8>) -> impl Future<Output = ()> + Send;

    /// `group`'s rehydration target: the highest committed application index,
    /// available only once the current leader has committed in its term, else
    /// `None` (spec §9). A consumer of [`subscribe_commits`](RaftConsensus::subscribe_commits)
    /// that just (re)subscribed waits until the watermark it has folded from the
    /// stream (`Committed::commit`) reaches this, so a grain activating right after
    /// a cluster restart rebuilds from a projection that reflects the whole
    /// restored, re-committed log rather than a still-empty prefix. `None` while a
    /// fresh election is still settling (or on a leaderless minority) — the
    /// consumer keeps waiting, bounded, then falls back to its local projection.
    fn group_ready_commit(&self, group: GroupId) -> Option<u64>;

    /// Whether this node currently leads `group`.
    fn group_is_leader(&self, group: GroupId) -> bool;

    /// The leader of `group` as this node believes it.
    fn group_leader(&self, group: GroupId) -> Option<NodeId>;

    /// A draw from the system's entropy source (the same deterministic seam used
    /// for membership timing, spec §13). A layer above uses it to mint an identity
    /// that must be unique across process restarts of the *same* node — e.g. a
    /// per-instance epoch tagging proposals, so a re-started, re-elected node never
    /// reuses a prior incarnation's proposal id (granary §7.2). Deterministic under
    /// simulation, so seeded runs stay reproducible.
    fn next_u64(&self) -> u64;

    /// A future that completes after `dur` of the system's (virtual) time —
    /// used by the journal to bound a commit wait.
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()>;

    /// Launch a detached background task on the system's executor — used by the
    /// journal to run its commit-applying loop over
    /// [`subscribe_commits`](RaftConsensus::subscribe_commits).
    fn launch(&self, task: BoxFuture<'static, ()>);
}
