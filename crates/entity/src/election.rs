//! The per-shard leader-election group (spec storage §4.1, §6).
//!
//! A shard's placement is a small Raft group of its *R* replicas (the
//! **leader-election group**) that elects one leader per term; its voters follow
//! the replica set the shard map records. It holds **no entity data** (storage §4.1). Its only role is to answer "who
//! may write this shard's entities, and under which term," and that term is the
//! single-writer fencing token every per-entity append carries (storage §6).
//!
//! [`LeaderElection`] is the thin wrapper the journal sees over that group, built on
//! the same `actor-cluster` Raft the control plane uses (cluster §5.4.3). The group is
//! created and reconfigured by [`shardmap`](crate::shardmap); this wrapper only reads
//! its leadership and term.

use actor_cluster::GroupId;
use actor_cluster::RaftConsensus;
use actor_core::NodeId;

use crate::journal::Term;

/// The leadership and term of one shard's leader-election group (spec storage §6).
pub(crate) struct LeaderElection<R: RaftConsensus> {
    consensus: R,
    group: GroupId,
    self_node: NodeId,
}

impl<R: RaftConsensus> LeaderElection<R> {
    /// Wrap the shard's leader-election `group` on `consensus`. The group must
    /// already be created (`shardmap` does so as the allocation commits, storage §7).
    pub(crate) fn new(consensus: R, group: GroupId) -> LeaderElection<R> {
        let self_node = consensus.node();
        LeaderElection {
            consensus,
            group,
            self_node,
        }
    }

    /// Whether this node currently leads the shard, the single-writer gate (storage §6):
    /// only the leader appends; a follower's append is fenced with `NotLeader`.
    pub(crate) fn is_leader(&self) -> bool {
        self.consensus.group_is_leader(self.group)
    }

    /// The shard's current term, the fencing token stamped on every per-entity
    /// append (storage §6). `None` on a node with no engine or not in the group.
    pub(crate) fn term(&self) -> Option<Term> {
        self.consensus.group_term(self.group).map(Term::new)
    }

    /// The best `NotLeader` redirect hint: the believed leader, or this node when
    /// none is known yet (a settling election).
    pub(crate) fn leader_hint(&self) -> NodeId {
        self.consensus
            .group_leader(self.group)
            .unwrap_or(self.self_node)
    }
}
