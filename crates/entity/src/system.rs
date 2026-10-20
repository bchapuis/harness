//! The `EntitySystem` capability seam.
//!
//! An entity's activation needs three runtime capabilities the bare
//! [`ActorSystem`] trait does not expose: the virtual clock (for hibernation
//! timing, §8), task launching (to drive the idle timer), and a typed channel
//! for entity events (§11). Threading the concrete `Clock`/`Entropy`/`Spawner`
//! type parameters of [`LocalSystem`] through every entity type would leak them
//! onto `EntityRef`, `Entities`, and the host, so an entity's system implements this
//! one **object-friendly** trait instead: the host, gateway, and ref are generic
//! over just `G: Entity`, and `G::System` supplies these capabilities. A clustered
//! system that can host shards implements it the same way (the `Quorum` tier).

use std::sync::Arc;

use actor_cluster::ClusterSystem;
use actor_cluster::GroupId;
use actor_cluster::RaftConsensus;
use actor_cluster::Transport;
use actor_core::ActorSystem;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::LocalSystem;
use actor_core::NodeId;
use actor_core::Spawner;

use crate::event::EntityEvent;
use crate::replica_store::ReplicaTransport;
use crate::shardmap::LocalShardMap;
use crate::shardmap::RaftShardMap;
use crate::shardmap::ShardMapSource;
use crate::store::NodeStore;

/// A shard of one entity type's namespace (spec storage §4.1): the entity-local handle a
/// [`EntitySystem`] maps to its backing store and consensus group. Kept distinct
/// from `actor_cluster::GroupId` so the seam signature stays system-agnostic: a
/// `Local` single-node system has no Raft group at all.
///
/// An entity's name maps to one `ShardId` by a stable hash of the name onto a
/// key-range partition of the type's hash space (§5.1): `entities()` founds
/// `shards` equal ranges ([`shard_for`]), and a split or merge (storage §8) is the only
/// thing that changes the partition afterward, never membership change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardId {
    /// The owning entity type (`G::ENTITY_TYPE`); distinguishes shards of different
    /// types that share an `index`, so their consensus groups never collide.
    pub entity_type: &'static str,
    /// The shard's index within its type's shard set: `0..shards` for the
    /// founding partition, minted fresh (max allocated + 1) for a split's child.
    pub index: u32,
}

/// FNV-1a over `bytes`: a small, allocation-free, deterministic hash. Used for
/// name→shard and shard→group, both of which MUST agree on every node and across
/// runs (the simulator replays a seed exactly, §10), so a randomized hasher is
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
/// [`group_id_for`], so the map group never collides with a data shard's group.
pub(crate) const MAP_SHARD_INDEX: u32 = u32::MAX;

/// The stable hash of an entity name onto the type's 64-bit key space (spec §5.1).
/// Every routing decision starts here: the hash locates the name's point in the
/// space, and the shard owning the surrounding key range serves it.
///
/// The FNV mix is finalized with MurmurHash3's `fmix64` because the partition is
/// by key **range**, i.e. the hash's high bits pick the shard. Raw FNV-1a has
/// weak high-bit avalanche on short, similar keys (each multiply lifts a
/// final-byte difference by only ~40 bits), so sequential keys like `user/1`,
/// `user/2` would pile into one range; the finalizer avalanches every input bit
/// into every output bit, restoring a uniform spread.
pub(crate) fn name_hash(entity_type: &str, key: &str) -> u64 {
    let mixed =
        fnv1a(entity_type.as_bytes()).wrapping_mul(0x0000_0100_0000_01b3) ^ fnv1a(key.as_bytes());
    // MurmurHash3 fmix64.
    let mut hash = mixed;
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^= hash >> 33;
    hash
}

/// Whether `entity`'s name-hash falls at or above the bound `from` (storage §8). The
/// moving side of a split/merge boundary and the append-refusing side of a
/// shard's seal are the same half-open test, so both stores' `sealed` check and
/// the split driver's keep-predicate route through here.
pub(crate) fn name_at_or_above(entity: &crate::entity::EntityName, from: u64) -> bool {
    name_hash(entity.entity_type(), entity.key()) >= from
}

/// A contiguous, inclusive range of the 64-bit name-hash space (spec storage §4.1): the
/// keys a shard owns. The founding partition slices the space into `shards` equal
/// ranges (`initial_ranges`); a split divides a range in two and a merge fuses
/// two adjacent ranges (storage §8).
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

/// Map an entity name to its **founding** shard (spec §5.1): the range of the
/// initial `shards`-way partition (`initial_ranges`) containing the name's
/// hash, computed in O(1) as `⌊hash·n/2^64⌋`. Stable across nodes and runs, so
/// resolution is consistent cluster-wide. After a split or merge (storage §8) the
/// committed shard map, not this function, is the authority, routing consults
/// the map first and falls back here only while the map bootstraps.
pub fn shard_for(entity_type: &'static str, key: &str, shards: usize) -> ShardId {
    ShardId {
        entity_type,
        index: founding_index(name_hash(entity_type, key), shards),
    }
}

/// The founding-partition bucket for a name `hash` under a `shards`-way split:
/// `⌊hash·shards/2^64⌋` (Lemire's fixed-point map). The one place this formula
/// lives: every routing path that falls back to the founding partition must
/// agree byte-for-byte, so they all call here.
pub(crate) fn founding_index(hash: u64, shards: usize) -> u32 {
    ((hash as u128 * shards.max(1) as u128) >> 64) as u32
}

/// Map a shard to its `actor_cluster` consensus group: a stable hash of
/// `(entity_type, index)`, forced nonzero so it never aliases the membership
/// control group ([`GroupId::CONTROL`] = 0, spec storage §4.2). Every node derives the
/// same group id for the same shard, so they form one Raft group.
pub(crate) fn group_id_for(shard: ShardId) -> GroupId {
    let id = fnv1a(shard.entity_type.as_bytes()).wrapping_mul(0x0000_0100_0000_01b3)
        ^ (shard.index as u64).wrapping_add(1);
    GroupId(id.max(1))
}

/// How a deployment groups nodes into failure domains: racks, availability zones,
/// whatever fails together (spec storage §4.1).
///
/// Replica selection without this spreads voters across the *cluster* but is blind to
/// how the cluster is arranged, so nothing stops all R replicas of a shard landing in
/// one zone. For a design whose entire safety argument is quorum intersection, that is
/// the difference between a zone loss being survivable and it taking shards below
/// quorum while the cluster still looks healthy.
///
/// **Every node MUST map a given node to the same domain.** The allocation is agreed
/// by every node computing the identical rendezvous choice (storage §7); a mapping that
/// disagreed across nodes would make them propose different sets. Deriving it from
/// static deployment metadata (the zone label a scheduler sets) satisfies this;
/// deriving it from anything node-local does not.
pub type FailureDomains = Arc<dyn Fn(NodeId) -> u64 + Send + Sync>;

/// Split `members` into voters and learners, spreading voters across failure domains
/// (spec storage §4.1). See [`select_replicas`] for the domain-blind case.
///
/// Walks the rendezvous order and takes a node only while its domain is under the
/// per-domain cap: `ceil(replicas / distinct domains)`, so R=3 over 3 zones takes one
/// each, and R=3 over 2 zones takes at most two from either. A second pass fills from
/// whatever is left if the caps could not be met, because a shard with fewer replicas
/// than configured is strictly worse than an imperfectly spread one.
///
/// Still a pure, deterministic function of `(members, group, replicas, domains)`, and
/// still minimal-movement: the rendezvous order does the choosing, the cap only skips.
fn select_spread(
    members: &[NodeId],
    group: GroupId,
    replicas: usize,
    domains: &FailureDomains,
) -> (Vec<NodeId>, Vec<NodeId>) {
    let ranked = rendezvous_order(members, group);
    let voter_count = replicas.clamp(1, members.len());
    let distinct: std::collections::BTreeSet<u64> = members.iter().map(|&n| domains(n)).collect();
    let per_domain = voter_count.div_ceil(distinct.len().max(1));

    let mut taken: std::collections::BTreeMap<u64, usize> = std::collections::BTreeMap::new();
    let mut voters = Vec::with_capacity(voter_count);
    for &node in &ranked {
        if voters.len() == voter_count {
            break;
        }
        let slot = taken.entry(domains(node)).or_default();
        if *slot < per_domain {
            *slot += 1;
            voters.push(node);
        }
    }
    // Lopsided domains (four nodes in one zone and one in another, R=3) can leave
    // the caps short. Fill from what is left, in rendezvous order.
    if voters.len() < voter_count {
        for &node in &ranked {
            if voters.len() == voter_count {
                break;
            }
            if !voters.contains(&node) {
                voters.push(node);
            }
        }
    }
    let mut learners: Vec<NodeId> = ranked.into_iter().filter(|n| !voters.contains(n)).collect();
    voters.sort_unstable();
    learners.sort_unstable();
    (voters, learners)
}

/// The members in rendezvous (HRW) order for `group`: the shared ranking both
/// selection strategies choose from. Every node must derive the same order for
/// the allocation to converge (storage §7), so the score is the cluster's one
/// rendezvous weight (utilities §4.1), keyed by the group id: consecutive shards'
/// ids differ only in their low bits, and the weight's finalizer spreads that
/// difference across all 64, so each shard ranks the nodes independently.
fn rendezvous_order(members: &[NodeId], group: GroupId) -> Vec<NodeId> {
    actor_cluster::placement::top(members, &group.0.to_le_bytes(), members.len())
}

/// Split `members` into a shard's voters and learners, optionally spreading the
/// voters across failure domains (spec storage §4.1). `domains` of `None` is the
/// domain-blind rendezvous split.
pub(crate) fn select_replicas_in(
    members: &[NodeId],
    group: GroupId,
    replicas: usize,
    domains: Option<&FailureDomains>,
) -> (Vec<NodeId>, Vec<NodeId>) {
    match domains {
        Some(domains) => select_spread(members, group, replicas, domains),
        None => select_replicas(members, group, replicas),
    }
}

/// Split `members` into a shard's `replicas` voters and its remaining learners
/// (spec storage §4.1) by **rendezvous hashing**: each member is scored by a stable hash
/// of `(node, group)`, the top `replicas` become voters, the rest learners. This
/// is a deterministic function of the members and the group (so every node
/// computes the identical split), and it spreads each shard's voters across the
/// cluster while moving only `~1/N` of shards when membership changes. `members`
/// is assumed sorted (the tie-break) and non-empty; `replicas` is clamped to it.
fn select_replicas(
    members: &[NodeId],
    group: GroupId,
    replicas: usize,
) -> (Vec<NodeId>, Vec<NodeId>) {
    let ranked = rendezvous_order(members, group);
    let voter_count = replicas.clamp(1, members.len());
    let (voters, learners) = ranked.split_at(voter_count);
    let (mut voters, mut learners) = (voters.to_vec(), learners.to_vec());
    voters.sort_unstable();
    learners.sort_unstable();
    (voters, learners)
}

/// An [`ActorSystem`] that can host entities (spec §3.3): it exposes virtual time, task
/// launching, the entity event channel (§8, §11), and the shard seam that places
/// an entity's durable storage and resolves its leader (§5.1, §5.2, storage §1). Implemented
/// for [`LocalSystem`] (the `Local` tier, single-node) and [`ClusterSystem`] (the
/// `Quorum` tier, sharded Raft).
pub trait EntitySystem: ActorSystem {
    /// Emit an entity event onto the framework's observability stream (§11),
    /// wrapped as an application event so the checkers and the reproducibility
    /// recorder observe it in the one ordered stream. (Time, sleeping, and
    /// task launching come from the [`ActorSystem`] supertrait.)
    fn emit_entity_event(&self, event: EntityEvent) {
        self.emit(Event::app(event));
    }

    /// Build the **shard map** for an entity type (spec storage §7): the consensus-agreed
    /// record of which nodes replicate each of its `shards`, and this node's local
    /// store for shards it replicates. The `Local` tier returns a single-node map
    /// (this node replicates everything); the `Quorum` tier creates a per-type Raft
    /// group whose committed log is the allocation, so every node agrees regardless
    /// of join order. The allocation targets `replicas` nodes per shard, and the
    /// leader auto-splits a shard past `split_target_bytes` (storage §8; `0` disables the
    /// size trigger). `local` is this node's durable store, its writes on the node's
    /// blocking pool ([`NodeStore`]), and `transport` reaches peer replicas' stores
    /// (storage §5). Created once at `entities()` time; routing reads it through the
    /// [`ShardMapSource`] seam.
    #[allow(clippy::too_many_arguments)] // one call site, from `entities()`
    fn shard_map(
        &self,
        entity_type: &'static str,
        shards: usize,
        replicas: usize,
        split_target_bytes: u64,
        local: NodeStore,
        transport: Arc<dyn ReplicaTransport>,
        deadlines: crate::replicator::Deadlines,
        domains: Option<crate::system::FailureDomains>,
    ) -> Arc<dyn ShardMapSource>;

    /// The node that currently leads `shard`, where its entities activate (§5.2), or
    /// `None` if this node does not replicate the shard (so cannot know) or a shard
    /// election is in flight. the `Local` tier is always its own leader.
    fn shard_leader(&self, shard: ShardId) -> Option<NodeId>;

    /// Whether this node leads `shard`: the single-writer fence (storage §6) and the gate
    /// for local activation (§5.4). `false` on a node that does not replicate the
    /// shard. the `Local` tier leads every shard.
    fn leads_shard(&self, shard: ShardId) -> bool;
}

impl<C: Clock, E: Entropy, S: Spawner> EntitySystem for LocalSystem<C, E, S> {
    fn shard_map(
        &self,
        _entity_type: &'static str,
        shards: usize,
        _replicas: usize,
        _split_target_bytes: u64,
        local: NodeStore,
        _transport: Arc<dyn ReplicaTransport>,
        // The `Local` tier has no quorum and no recovery read to bound.
        _deadlines: crate::replicator::Deadlines,
        // One node replicates everything, so there is nothing to spread.
        _domains: Option<crate::system::FailureDomains>,
    ) -> Arc<dyn ShardMapSource> {
        // `Local` tier: the single node replicates every shard, all keyed in one
        // local store (the peer transport is unused; there are no peers). Split
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

impl<C, E, S, T> EntitySystem for ClusterSystem<C, E, S, T>
where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    fn shard_map(
        &self,
        entity_type: &'static str,
        shards: usize,
        replicas: usize,
        split_target_bytes: u64,
        local: NodeStore,
        transport: Arc<dyn ReplicaTransport>,
        deadlines: crate::replicator::Deadlines,
        domains: Option<crate::system::FailureDomains>,
    ) -> Arc<dyn ShardMapSource> {
        // `Quorum` tier: a per-type Raft group whose committed log is the allocation,
        // so every node agrees on each shard's replica set (storage §7). It creates each
        // assigned shard's leader-election group + per-entity quorum journal as the
        // allocation commits. The emitter closure puts the map's shard events
        // (§11: LeaderChanged, ShardSplit, ShardMerged) onto this system's stream.
        let system = self.clone();
        Arc::new(RaftShardMap::new(
            self.clone(),
            entity_type,
            shards,
            replicas,
            split_target_bytes,
            local,
            transport,
            deadlines,
            domains,
            Arc::new(move |event| system.emit_entity_event(event)),
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
        // The partition is by key range (the hash's HIGH bits pick the shard),
        // and raw FNV-1a barely avalanches a short key's final bytes into the
        // high bits. Pinned with the least favourable input: short keys
        // differing only in a trailing counter.
        let shards = 8usize;
        let mut per_shard = vec![0usize; shards];
        for i in 0..800u64 {
            let key = format!("account/{i}");
            per_shard[shard_for("bank.Account", &key, shards).index as usize] += 1;
        }
        for (index, &count) in per_shard.iter().enumerate() {
            assert!(
                count >= 25, // a quarter of the uniform 100 per shard
                "shard {index} got only {count}/800 sequential keys ({per_shard:?}); \
                 the range partition's high bits are not avalanching"
            );
        }
    }

    #[test]
    fn shard_for_agrees_with_the_founding_partition() {
        // `shard_for` is the O(1) fallback while the shard map bootstraps; the
        // committed founding `Assign`s carry `initial_ranges`. The two MUST agree
        // on every name, or a caller routed by the fallback and one routed by the
        // map would resolve the same entity to different shards.
        for shards in [1usize, 2, 3, 4, 7, 16] {
            let ranges = initial_ranges(shards);
            for i in 0..500u64 {
                let key = format!("entity/{i}");
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
        // `group_id_for` derives a type's shard groups as `BASE ^ (index+1)`,
        // differing only in the low ~4 bits, so this uses the *realistic* group
        // ids (not the far-apart literals the deterministic test uses): distinct
        // shards must land on more than one replica set, and every node must
        // replicate some shard, or the storage §4.1/§4.2 load-spreading premise is broken.
        let members = nodes(&[1, 2, 3, 4, 5]);
        let mut sets = std::collections::BTreeSet::new();
        let mut covered = std::collections::BTreeSet::new();
        for index in 0..16u32 {
            let group = group_id_for(ShardId {
                entity_type: "bank.Account",
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
            "every node must replicate at least one shard, no idle nodes (covered {covered:?})",
        );
    }

    /// The property a zone loss depends on: R replicas of a shard must not all land
    /// in one failure domain. Domain-blind rendezvous can and does put them there:
    /// this checks the spread actually prevents it, across every shard, not on average.
    #[test]
    fn replicas_spread_across_failure_domains() {
        // Nine nodes, three per zone: the shape a three-AZ deployment has.
        let members: Vec<NodeId> = (1..=9).map(NodeId::new).collect();
        let zones: FailureDomains = Arc::new(|node: NodeId| (node.uid() - 1) % 3);
        let mut blind_concentrated = 0;
        for shard in 0..200u64 {
            let group = GroupId(shard.max(1));
            let (voters, _) = select_replicas_in(&members, group, 3, Some(&zones));
            let distinct: std::collections::BTreeSet<u64> =
                voters.iter().map(|&n| zones(n)).collect();
            assert_eq!(
                distinct.len(),
                3,
                "shard {shard}: voters {voters:?} must cover three zones, not {distinct:?}"
            );
            let (blind, _) = select_replicas(&members, group, 3);
            let blind_zones: std::collections::BTreeSet<u64> =
                blind.iter().map(|&n| zones(n)).collect();
            if blind_zones.len() < 3 {
                blind_concentrated += 1;
            }
        }
        assert!(
            blind_concentrated > 0,
            "the domain-blind split should concentrate some shards, or this test proves nothing"
        );
    }

    /// Lopsided domains must not leave a shard under-replicated: fewer replicas than
    /// configured is strictly worse than an imperfect spread.
    #[test]
    fn a_lopsided_domain_layout_still_yields_the_full_replica_count() {
        let members: Vec<NodeId> = (1..=5).map(NodeId::new).collect();
        // Four nodes in zone 0, one in zone 1: R=3 cannot be spread evenly.
        let zones: FailureDomains = Arc::new(|node: NodeId| u64::from(node.uid() == 5));
        for shard in 0..50u64 {
            let (voters, _) = select_replicas_in(&members, GroupId(shard.max(1)), 3, Some(&zones));
            assert_eq!(
                voters.len(),
                3,
                "shard {shard} must still get three replicas"
            );
        }
    }

    /// The spread is still a pure function every node computes identically: the
    /// property the consensus-agreed allocation rests on (storage §7).
    #[test]
    fn the_spread_is_deterministic() {
        let members: Vec<NodeId> = (1..=6).map(NodeId::new).collect();
        let zones: FailureDomains = Arc::new(|node: NodeId| node.uid() % 3);
        let a = select_replicas_in(&members, GroupId(11), 3, Some(&zones));
        let b = select_replicas_in(&members, GroupId(11), 3, Some(&zones));
        assert_eq!(a, b);
    }

    /// No domains configured must behave exactly as before, so the default deployment
    /// is unchanged.
    #[test]
    fn no_domains_is_the_domain_blind_split() {
        let members: Vec<NodeId> = (1..=7).map(NodeId::new).collect();
        for shard in 0..50u64 {
            let group = GroupId(shard.max(1));
            assert_eq!(
                select_replicas_in(&members, group, 3, None),
                select_replicas(&members, group, 3)
            );
        }
    }

    #[test]
    fn select_replicas_clamps_when_replicas_exceeds_membership() {
        let members = nodes(&[1, 2, 3]);
        let (voters, learners) = select_replicas(&members, GroupId(1), 5);
        assert_eq!(voters.len(), 3, "cannot have more voters than members");
        assert!(learners.is_empty());
    }
}
