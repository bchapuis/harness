//! The `GranarySystem` capability seam.
//!
//! A grain's activation needs three runtime capabilities the bare
//! [`ActorSystem`] trait does not expose: the virtual clock (for hibernation
//! timing, §10), task launching (to drive the idle timer), and a typed channel
//! for grain events (§13). Rather than thread the concrete `Clock`/`Entropy`/
//! `Spawner` type parameters of [`LocalSystem`] through every grain type — which
//! would leak them onto `GrainRef`, `Granary`, and the host — Granary requires a
//! grain's system to implement this one **object-friendly** trait. The host,
//! gateway, and ref are then generic over just `G: Grain`, and `G::System`
//! supplies these capabilities.
//!
//! This is the seam Tier 2 reuses: a clustered system that can host shards
//! implements `GranarySystem` the same way, and no grain code changes.

use std::sync::Arc;
use std::time::Duration;

use actor_cluster::ClusterSystem;
use actor_cluster::GroupId;
use actor_cluster::RaftLog;
use actor_cluster::Transport;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::Instant;
use actor_core::LocalSystem;
use actor_core::NodeId;
use actor_core::Spawner;

use crate::event::GrainEvent;
use crate::shardmap::LocalShardMap;
use crate::shardmap::RaftShardMap;
use crate::shardmap::ShardMapSource;

/// A shard of one grain type's namespace (spec §7.1): the granary-local handle a
/// [`GranarySystem`] maps to its backing store and consensus group. Kept distinct
/// from `actor_cluster::GroupId` so the seam signature stays system-agnostic — a
/// Tier-1 single-node system has no Raft group at all.
///
/// A grain's name maps to one `ShardId` by a stable hash ([`shard_for`]); the
/// number of shards per type is fixed at `granary()` time (control-plane-stored
/// shard maps and dynamic split/merge, §7.6/§7.7, are deferred).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardId {
    /// The owning grain type (`G::GRAIN_TYPE`); distinguishes shards of different
    /// types that share an `index`, so their consensus groups never collide.
    pub grain_type: &'static str,
    /// The shard's index within its type's shard set, `0..shards`.
    pub index: u32,
}

/// FNV-1a over `bytes` — a small, allocation-free, deterministic hash. Used for
/// name→shard and shard→group, both of which MUST agree on every node and across
/// runs (the simulator replays a seed exactly, §14), so a randomized hasher is
/// unusable here.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// A reserved shard index, never assigned to a real shard (a deployment cannot
/// have `u32::MAX` shards), used to derive the per-type **map group**'s id from
/// [`group_id_for`] — so the map group never collides with a data shard's group.
pub(crate) const MAP_SHARD_INDEX: u32 = u32::MAX;

/// Map a grain name to its shard (spec §5.1): a stable hash of `(grain_type, key)`
/// onto `0..shards`. Stable across nodes and runs, so resolution is consistent
/// cluster-wide; it changes only if the shard count changes.
pub fn shard_for(grain_type: &'static str, key: &str, shards: usize) -> ShardId {
    let mixed = fnv1a(grain_type.as_bytes())
        .wrapping_mul(0x0000_0100_0000_01b3)
        ^ fnv1a(key.as_bytes());
    ShardId {
        grain_type,
        index: (mixed % shards.max(1) as u64) as u32,
    }
}

/// Map a shard to its `actor_cluster` consensus group: a stable hash of
/// `(grain_type, index)`, forced nonzero so it never aliases the membership
/// control group ([`GroupId::CONTROL`] = 0, spec §8.2). Every node derives the
/// same group id for the same shard, so they form one Raft group.
pub(crate) fn group_id_for(shard: ShardId) -> GroupId {
    let id = fnv1a(shard.grain_type.as_bytes())
        .wrapping_mul(0x0000_0100_0000_01b3)
        ^ (shard.index as u64).wrapping_add(1);
    GroupId(id.max(1))
}

/// Split `members` into a shard's `replicas` voters and its remaining learners
/// (spec §7.1) by **rendezvous hashing**: each member is scored by a stable hash
/// of `(node, group)`, the top `replicas` become voters, the rest learners. This
/// is a deterministic function of the members and the group — so every node
/// computes the identical split — and it spreads each shard's voters across the
/// cluster while moving only `~1/N` of shards when membership changes. `members`
/// is assumed sorted (the tie-break) and non-empty; `replicas` is clamped to it.
pub(crate) fn select_replicas(members: &[NodeId], group: GroupId, replicas: usize) -> (Vec<NodeId>, Vec<NodeId>) {
    let mut scored: Vec<(u64, NodeId)> = members
        .iter()
        .map(|&node| {
            let score = fnv1a(&node.uid().to_le_bytes())
                .wrapping_mul(0x0000_0100_0000_01b3)
                ^ group.0;
            (score, node)
        })
        .collect();
    scored.sort_unstable(); // by score, then node id (the deterministic tie-break)
    let voter_count = replicas.clamp(1, members.len());
    let mut voters: Vec<NodeId> = scored[..voter_count].iter().map(|&(_, n)| n).collect();
    let mut learners: Vec<NodeId> = scored[voter_count..].iter().map(|&(_, n)| n).collect();
    voters.sort_unstable();
    learners.sort_unstable();
    (voters, learners)
}

/// An [`ActorSystem`] that can host grains: it exposes virtual time, task
/// launching, the grain event channel (§10, §13), and the shard seam that places
/// a grain's durable storage and resolves its leader (§5.1, §5.2, §7). Implemented
/// for [`LocalSystem`] (Tier 1, single-node) and [`ClusterSystem`] (Tier 2,
/// sharded Raft); a grain is generic over just `G`, and `G::System` supplies these.
pub trait GranarySystem: ActorSystem {
    /// The current virtual time (for the idle/hibernation clock, §10).
    fn now(&self) -> Instant;

    /// A future that completes after `dur` of virtual time — the host's
    /// hibernation timer (§10).
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()>;

    /// Launch a detached background task (drives the idle timer).
    fn launch(&self, task: BoxFuture<'static, ()>);

    /// Emit a grain event onto the framework's observability stream (§13),
    /// wrapped as an application event so the checkers and the reproducibility
    /// recorder observe it in the one ordered stream.
    fn emit_grain_event(&self, event: GrainEvent);

    /// Build the **shard map** for a grain type (spec §7.6): the consensus-agreed
    /// record of which nodes replicate each of its `shards`, and this node's local
    /// store for shards it replicates. Tier 1 returns a single-node map (this node
    /// replicates everything); Tier 2 creates a per-type Raft group whose committed
    /// log is the allocation, so every node agrees regardless of join order. The
    /// allocation targets `replicas` nodes per shard. Created once at `granary()`
    /// time; routing reads it through the [`ShardMapSource`] seam.
    fn shard_map(
        &self,
        grain_type: &'static str,
        shards: usize,
        replicas: usize,
    ) -> Arc<dyn ShardMapSource>;

    /// The node that currently leads `shard`, where its grains activate (§5.2), or
    /// `None` if this node does not replicate the shard (so cannot know) or a shard
    /// election is in flight. Tier 1 is always its own leader.
    fn shard_leader(&self, shard: ShardId) -> Option<NodeId>;

    /// Whether this node leads `shard` — the single-writer fence (§8) and the gate
    /// for local activation (§5.4). `false` on a node that does not replicate the
    /// shard. Tier 1 leads every shard.
    fn leads_shard(&self, shard: ShardId) -> bool;
}

impl<C: Clock, E: Entropy, S: Spawner> GranarySystem for LocalSystem<C, E, S> {
    fn now(&self) -> Instant {
        self.clock().now()
    }

    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        let clock = self.clock().clone();
        Box::pin(async move { clock.sleep(dur).await })
    }

    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.spawner().launch(task);
    }

    fn emit_grain_event(&self, event: GrainEvent) {
        self.emit(Event::app(event));
    }

    fn shard_map(
        &self,
        _grain_type: &'static str,
        shards: usize,
        _replicas: usize,
    ) -> Arc<dyn ShardMapSource> {
        // Tier 1: the single node replicates every shard, each an in-memory log.
        Arc::new(LocalShardMap::new(self.node(), shards))
    }

    fn shard_leader(&self, _shard: ShardId) -> Option<NodeId> {
        Some(self.node())
    }

    fn leads_shard(&self, _shard: ShardId) -> bool {
        true
    }
}

impl<C, E, S, T> GranarySystem for ClusterSystem<C, E, S, T>
where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    fn now(&self) -> Instant {
        self.clock().now()
    }

    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        let clock = self.clock().clone();
        Box::pin(async move { clock.sleep(dur).await })
    }

    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.launch_task(task);
    }

    fn emit_grain_event(&self, event: GrainEvent) {
        self.emit(Event::app(event));
    }

    fn shard_map(
        &self,
        grain_type: &'static str,
        shards: usize,
        replicas: usize,
    ) -> Arc<dyn ShardMapSource> {
        // Tier 2: a per-type Raft group whose committed log is the allocation, so
        // every node agrees on each shard's replica set (§7.6). It creates each
        // assigned shard's group + journal as the allocation commits.
        Arc::new(RaftShardMap::new(self.clone(), grain_type, shards, replicas))
    }

    fn shard_leader(&self, shard: ShardId) -> Option<NodeId> {
        self.group_leader(group_id_for(shard))
    }

    fn leads_shard(&self, shard: ShardId) -> bool {
        self.group_is_leader(group_id_for(shard))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(ids: &[u64]) -> Vec<NodeId> {
        ids.iter().map(|&i| NodeId::new(i)).collect()
    }

    #[test]
    fn select_replicas_bounds_voters_and_splits_the_rest_as_learners() {
        let members = nodes(&[1, 2, 3, 4, 5]);
        let (voters, learners) = select_replicas(&members, GroupId(7), 3);
        // Voter set is bounded to the replication factor; the rest are learners.
        assert_eq!(voters.len(), 3, "voters bounded to R, not the cluster size");
        assert_eq!(learners.len(), 2);
        // Disjoint, and together they are exactly the membership.
        let mut all: Vec<NodeId> = voters.iter().chain(learners.iter()).copied().collect();
        all.sort_unstable();
        assert_eq!(all, members, "every member is a voter or a learner, none both");
    }

    #[test]
    fn select_replicas_is_deterministic_so_every_node_agrees() {
        // The split is a pure function of (members, group): every node computes the
        // identical voter/learner sets, so they form one consistent Raft group.
        let members = nodes(&[10, 20, 30, 40, 50]);
        let a = select_replicas(&members, GroupId(42), 3);
        let b = select_replicas(&members, GroupId(42), 3);
        assert_eq!(a, b);
        // A different shard (group) generally lands on a different voter set, so
        // shards spread their leadership/replication across the cluster.
        let other = select_replicas(&members, GroupId(43), 3);
        assert_eq!(other.0.len(), 3);
    }

    #[test]
    fn select_replicas_clamps_when_replicas_exceeds_membership() {
        let members = nodes(&[1, 2, 3]);
        let (voters, learners) = select_replicas(&members, GroupId(1), 5);
        assert_eq!(voters.len(), 3, "cannot have more voters than members");
        assert!(learners.is_empty());
    }
}
