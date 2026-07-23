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
//! This is the seam the `Quorum` tier reuses: a clustered system that can host shards
//! implements `GranarySystem` the same way, and no grain code changes.

use std::sync::Arc;
use std::time::Duration;

use actor_cluster::ClusterSystem;
use actor_cluster::GroupId;
use actor_cluster::RaftConsensus;
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
use crate::replica_store::ReplicaTransport;
use crate::shardmap::LocalShardMap;
use crate::shardmap::RaftShardMap;
use crate::shardmap::ShardMapSource;
use crate::store::GrainStore;

/// A shard of one grain type's namespace (spec §7.1): the granary-local handle a
/// [`GranarySystem`] maps to its backing store and consensus group. Kept distinct
/// from `actor_cluster::GroupId` so the seam signature stays system-agnostic — a
/// `Local` single-node system has no Raft group at all.
///
/// A grain's name maps to one `ShardId` by a stable hash of the name onto a
/// key-range partition of the type's hash space (§5.1): `granary()` founds
/// `shards` equal ranges ([`shard_for`]), and a split or merge (§7.7) is the only
/// thing that changes the partition afterward — never membership change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardId {
    /// The owning grain type (`G::GRAIN_TYPE`); distinguishes shards of different
    /// types that share an `index`, so their consensus groups never collide.
    pub grain_type: &'static str,
    /// The shard's index within its type's shard set: `0..shards` for the
    /// founding partition, minted fresh (max allocated + 1) for a split's child.
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

/// The stable hash of a grain name onto the type's 64-bit key space (spec §5.1).
/// Every routing decision starts here: the hash locates the name's point in the
/// space, and the shard owning the surrounding key range serves it.
///
/// The FNV mix is finalized with MurmurHash3's `fmix64` because the partition is
/// by key **range**, i.e. the hash's high bits pick the shard. Raw FNV-1a has
/// weak high-bit avalanche on short, similar keys (each multiply lifts a
/// final-byte difference by only ~40 bits), so sequential keys like `user/1`,
/// `user/2` would pile into one range; the finalizer avalanches every input bit
/// into every output bit, restoring a uniform spread. Deterministic, so every
/// node and every run agrees (§14).
pub(crate) fn name_hash(grain_type: &str, key: &str) -> u64 {
    let mixed =
        fnv1a(grain_type.as_bytes()).wrapping_mul(0x0000_0100_0000_01b3) ^ fnv1a(key.as_bytes());
    // MurmurHash3 fmix64.
    let mut hash = mixed;
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^= hash >> 33;
    hash
}

/// Whether `grain`'s name-hash falls at or above the bound `from` (§7.7). The
/// moving side of a split/merge boundary and the append-refusing side of a
/// shard's seal are the same half-open test, so both stores' `sealed` check and
/// the split driver's keep-predicate route through here — one place the
/// comparison lives, like [`founding_index`].
pub(crate) fn name_at_or_above(grain: &crate::grain::GrainName, from: u64) -> bool {
    name_hash(grain.grain_type(), grain.key()) >= from
}

/// A contiguous, inclusive range of the 64-bit name-hash space (spec §7.1) — the
/// keys a shard owns. The founding partition slices the space into `shards` equal
/// ranges ([`initial_ranges`]); a split divides a range in two and a merge fuses
/// two adjacent ranges (§7.7), the only operations that change the partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyRange {
    /// The lowest hash this range owns.
    pub start: u64,
    /// The highest hash this range owns (inclusive, so `[0, u64::MAX]` covers the
    /// whole space without 2^64 arithmetic).
    pub end: u64,
}

impl KeyRange {
    /// Whether `hash` falls in this range.
    pub fn contains(&self, hash: u64) -> bool {
        self.start <= hash && hash <= self.end
    }
}

/// The `i`-th boundary of the founding `shards`-way partition: `ceil(i·2^64/n)`,
/// so boundary 0 is 0 and boundary `n` is 2^64 (one past `u64::MAX`).
fn initial_boundary(i: usize, shards: usize) -> u128 {
    ((i as u128) << 64).div_ceil(shards as u128)
}

/// The founding partition (spec §5.1): `shards` near-equal contiguous ranges
/// covering the whole hash space. Deterministic, so every node founds the
/// identical partition; [`shard_for`] is its O(1) point lookup.
pub(crate) fn initial_ranges(shards: usize) -> Vec<KeyRange> {
    let shards = shards.max(1);
    (0..shards)
        .map(|i| KeyRange {
            start: initial_boundary(i, shards) as u64,
            end: (initial_boundary(i + 1, shards) - 1) as u64,
        })
        .collect()
}

/// Map a grain name to its **founding** shard (spec §5.1): the range of the
/// initial `shards`-way partition ([`initial_ranges`]) containing the name's
/// hash, computed in O(1) as `⌊hash·n/2^64⌋`. Stable across nodes and runs, so
/// resolution is consistent cluster-wide. After a split or merge (§7.7) the
/// committed shard map, not this function, is the authority — routing consults
/// the map first and falls back here only while the map bootstraps.
pub fn shard_for(grain_type: &'static str, key: &str, shards: usize) -> ShardId {
    ShardId {
        grain_type,
        index: founding_index(name_hash(grain_type, key), shards),
    }
}

/// The founding-partition bucket for a name `hash` under a `shards`-way split:
/// `⌊hash·shards/2^64⌋` (Lemire's fixed-point map). The one place this formula
/// lives — every routing path that falls back to the founding partition must
/// agree byte-for-byte, so they all call here.
pub(crate) fn founding_index(hash: u64, shards: usize) -> u32 {
    ((hash as u128 * shards.max(1) as u128) >> 64) as u32
}

/// Map a shard to its `actor_cluster` consensus group: a stable hash of
/// `(grain_type, index)`, forced nonzero so it never aliases the membership
/// control group ([`GroupId::CONTROL`] = 0, spec §8.2). Every node derives the
/// same group id for the same shard, so they form one Raft group.
pub(crate) fn group_id_for(shard: ShardId) -> GroupId {
    let id = fnv1a(shard.grain_type.as_bytes()).wrapping_mul(0x0000_0100_0000_01b3)
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
pub(crate) fn select_replicas(
    members: &[NodeId],
    group: GroupId,
    replicas: usize,
) -> (Vec<NodeId>, Vec<NodeId>) {
    // Rendezvous (HRW) score per node. The group id MUST be hashed, not XORed in
    // raw: `group_id_for` derives a type's shard groups as `BASE ^ (index+1)`, so
    // consecutive shards' ids differ only in the low ~4 bits. XORing that raw into a
    // per-node constant leaves the high-order bits — which dominate the sort — fixed,
    // so every shard would rank the nodes identically and pile onto the same R
    // replicas with the same leader. Diffusing the id through `fnv1a` first spreads
    // those low-bit differences across all 64 bits, so each shard gets an independent
    // ranking and shards spread their replicas and leadership across the cluster.
    let mut scored: Vec<(u64, NodeId)> = members
        .iter()
        .map(|&node| {
            let score = fnv1a(&node.uid().to_le_bytes()).wrapping_mul(0x0000_0100_0000_01b3)
                ^ fnv1a(&group.0.to_le_bytes());
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
/// for [`LocalSystem`] (the `Local` tier, single-node) and [`ClusterSystem`] (the `Quorum` tier,
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
    /// store for shards it replicates. The `Local` tier returns a single-node map
    /// (this node replicates everything); the `Quorum` tier creates a per-type Raft
    /// group whose committed log is the allocation, so every node agrees regardless
    /// of join order. The allocation targets `replicas` nodes per shard, and the
    /// leader auto-splits a shard past `split_target_bytes` (§7.7; `0` disables the
    /// size trigger). `local` is this node's durable [`GrainStore`] and `transport`
    /// reaches peer replicas' stores (§7.2). Created once at `granary()` time;
    /// routing reads it through the [`ShardMapSource`] seam.
    fn shard_map(
        &self,
        grain_type: &'static str,
        shards: usize,
        replicas: usize,
        split_target_bytes: u64,
        local: Arc<dyn GrainStore>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Arc<dyn ShardMapSource>;

    /// The node that currently leads `shard`, where its grains activate (§5.2), or
    /// `None` if this node does not replicate the shard (so cannot know) or a shard
    /// election is in flight. the `Local` tier is always its own leader.
    fn shard_leader(&self, shard: ShardId) -> Option<NodeId>;

    /// Whether this node leads `shard` — the single-writer fence (§8) and the gate
    /// for local activation (§5.4). `false` on a node that does not replicate the
    /// shard. the `Local` tier leads every shard.
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
        _split_target_bytes: u64,
        local: Arc<dyn GrainStore>,
        _transport: Arc<dyn ReplicaTransport>,
    ) -> Arc<dyn ShardMapSource> {
        // `Local` tier: the single node replicates every shard, all keyed in one
        // local store (the peer transport is unused — there are no peers). Split
        // and merge are `Quorum` elasticity, so the size trigger is inert here.
        Arc::new(LocalShardMap::new(self.node(), shards, local))
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
        split_target_bytes: u64,
        local: Arc<dyn GrainStore>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Arc<dyn ShardMapSource> {
        // `Quorum` tier: a per-type Raft group whose committed log is the allocation,
        // so every node agrees on each shard's replica set (§7.6). It creates each
        // assigned shard's leader-election group + per-grain quorum journal as the
        // allocation commits. The emitter closure puts the map's shard events
        // (§13: LeaderChanged, ShardSplit, ShardMerged) onto this system's stream.
        let system = self.clone();
        Arc::new(RaftShardMap::new(
            self.clone(),
            grain_type,
            shards,
            replicas,
            split_target_bytes,
            local,
            transport,
            Arc::new(move |event| system.emit_grain_event(event)),
        ))
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
    fn initial_ranges_tile_the_hash_space_exactly() {
        for shards in [1usize, 2, 3, 4, 7, 16, 100] {
            let ranges = initial_ranges(shards);
            assert_eq!(ranges.len(), shards);
            assert_eq!(ranges[0].start, 0, "the partition starts at 0");
            assert_eq!(
                ranges[shards - 1].end,
                u64::MAX,
                "the partition ends at u64::MAX"
            );
            for pair in ranges.windows(2) {
                assert_eq!(
                    pair[0].end.wrapping_add(1),
                    pair[1].start,
                    "adjacent ranges abut with no gap or overlap ({shards} shards)"
                );
            }
        }
    }

    #[test]
    fn sequential_keys_spread_across_the_range_partition() {
        // Regression for the high-bit avalanche bug: the partition is by key
        // range (the hash's HIGH bits pick the shard), but raw FNV-1a barely
        // avalanches a short key's final bytes into the high bits, so sequential
        // keys piled into one range. The fmix64 finalizer restores the spread;
        // this pins it with the least favourable input — short keys differing
        // only in a trailing counter.
        let shards = 8usize;
        let mut per_shard = vec![0usize; shards];
        for i in 0..800u64 {
            let key = format!("account/{i}");
            per_shard[shard_for("bank.Account", &key, shards).index as usize] += 1;
        }
        for (index, &count) in per_shard.iter().enumerate() {
            assert!(
                count >= 25, // a quarter of the uniform 100 per shard
                "shard {index} got only {count}/800 sequential keys ({per_shard:?}) — \
                 the range partition's high bits are not avalanching"
            );
        }
    }

    #[test]
    fn shard_for_agrees_with_the_founding_partition() {
        // `shard_for` is the O(1) fallback while the shard map bootstraps; the
        // committed founding `Assign`s carry `initial_ranges`. The two MUST agree
        // on every name, or a caller routed by the fallback and one routed by the
        // map would resolve the same grain to different shards.
        for shards in [1usize, 2, 3, 4, 7, 16] {
            let ranges = initial_ranges(shards);
            for i in 0..500u64 {
                let key = format!("grain/{i}");
                let index = shard_for("bank.Account", &key, shards).index;
                let hash = name_hash("bank.Account", &key);
                assert!(
                    ranges[index as usize].contains(hash),
                    "shard_for({key}) = {index} but hash {hash} is outside that \
                     founding range ({shards} shards)"
                );
            }
            // The boundary hashes themselves resolve inside their range too.
            for (index, range) in ranges.iter().enumerate() {
                for hash in [range.start, range.end] {
                    let computed = founding_index(hash, shards) as usize;
                    assert_eq!(computed, index, "boundary {hash} of range {index}");
                }
            }
        }
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
        assert_eq!(
            all, members,
            "every member is a voter or a learner, none both"
        );
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
    fn select_replicas_spreads_shards_across_the_cluster() {
        // Regression for the rendezvous bug: the prior code XORed the raw group id
        // into a per-node constant, but `group_id_for` derives a type's shard groups
        // as `BASE ^ (index+1)` — differing only in the low ~4 bits — so every shard
        // ranked the nodes identically and piled onto the SAME R replicas, leaving
        // other nodes idle. Verified here with the *realistic* group ids (not the
        // far-apart literals the deterministic test uses): distinct shards must land
        // on more than one replica set, and every node must replicate some shard, or
        // the §7.1/§7.8 load-spreading premise is broken.
        let members = nodes(&[1, 2, 3, 4, 5]);
        let mut sets = std::collections::BTreeSet::new();
        let mut covered = std::collections::BTreeSet::new();
        for index in 0..16u32 {
            let group = group_id_for(ShardId {
                grain_type: "bank.Account",
                index,
            });
            let (voters, _) = select_replicas(&members, group, 3);
            for v in &voters {
                covered.insert(*v);
            }
            sets.insert(voters);
        }
        assert!(
            sets.len() > 1,
            "shards must not all collapse onto one replica set (got {} distinct sets)",
            sets.len(),
        );
        assert_eq!(
            covered.len(),
            5,
            "every node must replicate at least one shard — no idle nodes (covered {covered:?})",
        );
    }

    #[test]
    fn select_replicas_clamps_when_replicas_exceeds_membership() {
        let members = nodes(&[1, 2, 3]);
        let (voters, learners) = select_replicas(&members, GroupId(1), 5);
        assert_eq!(voters.len(), 3, "cannot have more voters than members");
        assert!(learners.is_empty());
    }
}
