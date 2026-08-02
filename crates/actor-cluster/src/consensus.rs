//! The application consensus seam (spec §9.4.3).
//!
//! [`RaftConsensus`] is the narrow capability a layer built *on* the cluster — granary's
//! sharded journal — needs from a Raft-hosting [`ClusterSystem`](crate::ClusterSystem):
//! create an application group, propose opaque command bytes to its leader, and
//! observe the committed stream. A consumer stays generic over `R: RaftConsensus`
//! instead of naming the concrete `ClusterSystem<C, E, S, T>`.
//!
//! It is used as a **generic bound** (`R: RaftConsensus`), never as
//! `dyn RaftConsensus`: no object safety is required, so the `impl Future` /
//! concrete-`Receiver` return types are permitted.

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
    /// regardless of when the node joined. It, not the live `cluster_voters`, is
    /// the **creation seed** for a metadata group: a late joiner seeding from the
    /// live set would form a divergent group and disrupt elections. Empty outside
    /// leader-based mode.
    fn configured_voters(&self) -> Vec<NodeId>;

    /// Drive `group`'s voter set toward `voters` (spec §9.4.3 item 2): if this node
    /// leads `group`, propose the `AddVoter`/`RemoveVoter` deltas between the
    /// group's current voters and `voters`. A no-op on a non-leader. The engine
    /// replicates the full committed log to a newly added voter, which then
    /// catches up.
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

    /// Retire an application Raft group (spec §7.7, G7): stop running it so it no
    /// longer elects, heartbeats, or commits. A group that holds no data — a
    /// granary shard's leader-election group is placement only (§7.1) — needs no
    /// in-group consensus to retire: each node drops it as it applies the
    /// committed merge. Idempotent; a no-op on a group this node never ran and
    /// outside leader-based mode.
    fn remove_group(&self, group: GroupId);

    /// Subscribe to `group`'s committed observation stream ([`Committed`]). The
    /// receiver observes every entry committed **after** this call (a late
    /// subscriber misses earlier commits; there is no replay-from-index),
    /// interleaved in commit order with any state-machine snapshot
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

    /// Hand `group`'s leadership to `target`, returning whether the handoff was
    /// initiated (Raft §3.10).
    ///
    /// `false` means "not yet": this node does not lead `group`, `target` is not one
    /// of its voters, or `target` has not yet replicated this leader's last entry —
    /// a peer in that state would lose the election it was invited to hold, so the
    /// caller retries while replication catches it up.
    ///
    /// Initiating a handoff does **not** end this node's term; only another node
    /// winning a higher one does. The call is an invitation to a caught-up peer to
    /// stand immediately rather than after its election timeout, which is what makes
    /// a planned departure cheap: a node leading many groups otherwise costs one full
    /// election timeout per group on every drain.
    fn transfer_group_leadership(
        &self,
        group: GroupId,
        target: NodeId,
    ) -> impl Future<Output = bool> + Send;

    /// Whether this node currently leads `group`.
    fn group_is_leader(&self, group: GroupId) -> bool;

    /// `group`'s current Raft term, or `None` if this node has no engine / is not
    /// in the group. A layer above (granary's per-shard leader-election group, §8)
    /// stamps it on every per-grain append as the single-writer fencing token: one
    /// leader per term, refused by any replica that has acknowledged a higher term.
    fn group_term(&self, group: GroupId) -> Option<u64>;

    /// The leader of `group` as this node believes it.
    fn group_leader(&self, group: GroupId) -> Option<NodeId>;

    /// A draw from the system's entropy source (the same deterministic seam used
    /// for membership timing, spec §13). Used to mint an identity that must be
    /// unique across process restarts of the *same* node, e.g. the per-instance
    /// epoch tagging proposals so a re-started, re-elected node never reuses a
    /// prior incarnation's proposal id (granary §7.2). Deterministic under
    /// simulation.
    fn next_u64(&self) -> u64;

    /// A future that completes after `dur` of the system's (virtual) time —
    /// used by the journal to bound a commit wait.
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()>;

    /// Launch a detached background task on the system's executor — used by the
    /// journal to run its commit-applying loop over
    /// [`subscribe_commits`](RaftConsensus::subscribe_commits).
    fn launch(&self, task: BoxFuture<'static, ()>);
}
