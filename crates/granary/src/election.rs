//! The per-shard leader-election group (spec §7.1, §8).
//!
//! A shard's placement is a small Raft group of its *R* replicas — the
//! **leader-election group** — that elects one leader per term and records the
//! replica set. It holds **no grain data** (§7.1): granary never proposes a grain's
//! records to it. Its only role is to answer "who may write this shard's grains, and
//! under which term," and that term is the single-writer fencing token every
//! per-grain append carries (§8).
//!
//! [`LeaderElection`] is the thin wrapper the journal sees over that group, built on
//! the same `actor-cluster` Raft the control plane uses (actor §9.4.3), so the
//! simulator drives the real consensus code. The group is created and reconfigured
//! by [`shardmap`](crate::shardmap); this wrapper only reads its leadership and term.

use actor_cluster::GroupId;
use actor_cluster::RaftConsensus;
use actor_core::NodeId;

/// The leadership and term of one shard's leader-election group (spec §8). Cheap to
/// clone — it holds only the consensus handle and the group id.
pub(crate) struct LeaderElection<R: RaftConsensus> {
    consensus: R,
    group: GroupId,
    self_node: NodeId,
}

impl<R: RaftConsensus> Clone for LeaderElection<R> {
    fn clone(&self) -> Self {
        LeaderElection {
            consensus: self.consensus.clone(),
            group: self.group,
            self_node: self.self_node,
        }
    }
}

impl<R: RaftConsensus> LeaderElection<R> {
    /// Wrap the shard's leader-election `group` on `consensus`. The group must
    /// already be created (`shardmap` does so as the allocation commits, §7.6).
    pub(crate) fn new(consensus: R, group: GroupId) -> LeaderElection<R> {
        let self_node = consensus.node();
        LeaderElection {
            consensus,
            group,
            self_node,
        }
    }

    /// Whether this node currently leads the shard — the single-writer gate (§8):
    /// only the leader appends; a follower's append is fenced with `NotLeader`.
    pub(crate) fn is_leader(&self) -> bool {
        self.consensus.group_is_leader(self.group)
    }

    /// The shard's current term, the fencing token stamped on every per-grain
    /// append (§8). `None` on a node with no engine or not in the group.
    pub(crate) fn term(&self) -> Option<u64> {
        self.consensus.group_term(self.group)
    }

    /// The best `NotLeader` redirect hint: the believed leader, or this node when
    /// none is known yet (a settling election).
    pub(crate) fn leader_hint(&self) -> NodeId {
        self.consensus
            .group_leader(self.group)
            .unwrap_or(self.self_node)
    }
}
