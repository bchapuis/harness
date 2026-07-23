//! The shard map: a consensus-agreed record of which nodes replicate each shard
//! (spec §7.6).
//!
//! Storage distribution needs every node to agree on a shard's replica set. The
//! rendezvous-derived map (the prior step) snapshots that per node from its *live*
//! membership view at `granary()` time, so nodes that join at different membership
//! epochs compute divergent sets. This module replaces that with a **stored,
//! consensus-agreed** map: a granary-owned Raft group per grain type whose
//! committed log *is* the allocation. Every node applies the identical committed
//! entries, so the cluster agrees on where each shard lives regardless of join
//! order.
//!
//! Reached through the object-safe [`ShardMapSource`] seam — the map analogue of
//! [`DynGrainJournal`](crate::journal::DynGrainJournal) — so the gateway and `Granary` stay
//! free of the consensus type. Two implementations: the single-node
//! [`LocalShardMap`] (the `Local` tier) and the [`RaftShardMap`] over a clustered system
//! (the `Quorum` tier). The map records each shard's key range and replica set;
//! the allocator rebalances replica sets as membership changes, and the partition
//! itself changes on a shard **split** or **merge** (§7.7) — driven by the
//! `split_loop`/`merge_loop` on the shard leader, committed through this same log
//! (`SplitStarted`/`SplitCommitted`, `MergeStarted`/`MergeCommitted`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use actor_cluster::Committed;
use actor_cluster::GroupId;
use actor_cluster::RaftConsensus;
use actor_core::NodeId;
use async_channel::Receiver;
use serde::Deserialize;
use serde::Serialize;

use crate::event::GrainEvent;
use crate::journal::DynGrainJournal;
use crate::memory::LocalGrainJournal;
use crate::replica_store::ReplicaTransport;
use crate::replicator::QuorumReplicator;
use crate::replicator::ReplicaSets;
use crate::replicator::ShardControl;
use crate::shard::QuorumGrainJournal;
use crate::store::GrainStore;
use crate::system::KeyRange;
use crate::system::MAP_SHARD_INDEX;
use crate::system::ShardId;
use crate::system::group_id_for;
use crate::system::initial_ranges;
use crate::system::select_replicas;

/// How often the map leader re-checks for unallocated shards to propose. Short
/// relative to commit latency; the work is one-shot per shard, so the loop is idle
/// once the allocation is complete.
const ALLOCATE_INTERVAL: Duration = Duration::from_millis(100);

/// A node's read access to the shard map (spec §7.6): which nodes replicate a
/// shard, and — if this node is one of them — the local store for it. The map
/// analogue of [`DynGrainJournal`]; erases the consensus type so the gateway stays
/// generic over just the grain.
pub trait ShardMapSource: Send + Sync + 'static {
    /// The shard's replica set once the allocation has committed, else `None`
    /// (the map is still bootstrapping).
    fn replicas(&self, shard: u32) -> Option<Vec<NodeId>>;

    /// This node's durable store for the shard — `Some` iff this node replicates
    /// it and has built the journal.
    fn journal(&self, shard: u32) -> Option<Arc<dyn DynGrainJournal>>;

    /// The shard whose committed key range contains `hash` (spec §5.1), or `None`
    /// while the map is still bootstrapping (or on a routing-only client, which
    /// never holds the allocation). The authority once ranges have committed —
    /// after a split or merge (§7.7) only the map knows the partition; the
    /// founding [`shard_for`](crate::shard_for) fallback covers only bootstrap.
    fn shard_of(&self, hash: u64) -> Option<u32>;

    /// The committed shard indices, in order — the live partition's shards, for
    /// loops that sweep every shard (the alarm driver, metrics). Empty while the
    /// map bootstraps or on a routing-only client.
    fn shard_indices(&self) -> Vec<u32>;

    /// Request a split of `shard` at its range midpoint (§7.7) — the admin/test
    /// seam behind [`Granary::split_shard`](crate::Granary::split_shard); the
    /// size trigger proposes through the same path. Best-effort: the request is
    /// picked up by the split proposer, validated against committed state, and
    /// silently dropped if the shard is unknown, mid-migration, mid-split, or
    /// its range is a single point. A no-op on the `Local` tier (split/merge is
    /// `Quorum` elasticity) and on a routing-only client.
    fn request_split(&self, _shard: u32) {}

    /// Request a merge of `shard` with its **right** neighbour — the shard whose
    /// range begins just past `shard`'s (§7.7), the mirror of a split. The seam
    /// behind [`Granary::merge_shards`](crate::Granary::merge_shards). Best-
    /// effort: validated against committed state and silently dropped if either
    /// shard is unknown, not adjacent, or not idle. A no-op on the `Local` tier
    /// and on a routing-only client.
    fn request_merge(&self, _left: u32) {}
}

/// Resolve a grain key to its shard (spec §5.1): the committed key-range
/// partition once the map has it ([`ShardMapSource::shard_of`]), else the
/// founding partition ([`shard_for`](crate::shard_for)) while the map
/// bootstraps — the two agree until the first split/merge, which only ever
/// commits through the map. The one name→shard function behind the gateway, the
/// host cache's pre-send guard, and the `Granary` observability reads.
pub(crate) fn resolve_shard(
    map: &dyn ShardMapSource,
    grain_type: &'static str,
    key: &str,
    shards: usize,
) -> ShardId {
    match map.shard_of(crate::system::name_hash(grain_type, key)) {
        Some(index) => ShardId { grain_type, index },
        None => crate::system::shard_for(grain_type, key, shards),
    }
}

/// The map group's id for a grain type: [`group_id_for`] at the reserved
/// [`MAP_SHARD_INDEX`], so it never collides with a data shard's group.
fn map_group_id_for(grain_type: &'static str) -> GroupId {
    group_id_for(ShardId {
        grain_type,
        index: MAP_SHARD_INDEX,
    })
}

/// The consensus group id for a data shard — the counterpart of
/// [`map_group_id_for`] for `index` in `0..shards`.
fn shard_group_id(grain_type: &'static str, index: u32) -> GroupId {
    group_id_for(ShardId { grain_type, index })
}

/// One committed allocation command in the map group's log (spec §7.6, §7.7).
#[derive(Serialize, Deserialize)]
enum ShardMapCommand {
    /// Shard `shard` owns the key range `range` and is replicated by `replicas` —
    /// the only nodes that hold its data and can lead it (§7.1). The **founding**
    /// `Assign` sets the shard's range and replica set outright; a later `Assign`
    /// onto an already-allocated shard starts a **migration** toward `replicas`
    /// (the committed `target`, §7.7): writes and recoveries switch to the joint
    /// quorum, and the leader's migration driver catches every grain up on the
    /// target set. Migration never moves the range — only a split or merge (§7.7)
    /// changes the partition — so a non-founding `Assign`'s `range` is ignored.
    Assign {
        shard: u32,
        replicas: Vec<NodeId>,
        range: KeyRange,
    },
    /// The migration for `shard` is complete — every grain's records, snapshot,
    /// and blobs are on the target set — so the target becomes the current set
    /// and the old-set members drop out (§7.7). Proposed only by the shard
    /// leader's migration driver, only after a successful catch-up pass.
    Migrated { shard: u32 },
    /// A shard split began (§7.7): `parent`'s range will divide at `boundary` —
    /// the parent keeps `[start, boundary)` in place, the fresh `child` index
    /// will own `[boundary, end]` on the same replicas. From this commit the
    /// parent's replicas freeze appends to the moving range (the durable store
    /// seal is fanned by the driver; applying this sets the leader-local fast
    /// path) and the parent leader's split driver transfers the moved grains.
    /// Validated deterministically against committed state at apply; an invalid
    /// proposal (parent migrating, another split in flight, boundary outside
    /// the range, child taken) is a no-op on every node.
    SplitStarted {
        parent: u32,
        child: u32,
        boundary: u64,
    },
    /// The split for `parent` is complete — every moved grain's committed
    /// prefix, snapshot, and blobs are quorum-durable under the child's keys —
    /// so the mapping flips: the parent's range shrinks, the child's allocation
    /// commits, and the child's replicas create its leader-election group.
    /// Proposed only by the parent leader's split driver after a full transfer
    /// pass. Only from this commit does any node route the moved range to the
    /// child (§7.7's ordering: the map commits BEFORE either side serves it).
    SplitCommitted { parent: u32 },
    /// A shard merge began (§7.7): the adjacent `right` shard will fold into
    /// `left` (`left.end + 1 == right.start`), the mirror of a split. From this
    /// commit `right`'s replicas freeze their whole range and the `right` leader's
    /// merge driver transfers every grain into `left`'s keys. Validated
    /// deterministically at apply (both allocated, idle, adjacent, same replica
    /// set); an invalid proposal is a no-op on every node.
    MergeStarted { left: u32, right: u32 },
    /// The merge for `(left, right)` is complete — every grain of `right` is
    /// quorum-durable under `left`'s keys — so the mapping flips: `left`'s range
    /// extends over `right`'s, `right`'s allocation is dropped and its
    /// leader-election group retired (G7). Proposed only by the `right` leader's
    /// merge driver after a full transfer pass.
    MergeCommitted { left: u32, right: u32 },
}

/// An in-flight split's committed plan (§7.7), recorded on the parent's
/// [`Allocation`] between `SplitStarted` and `SplitCommitted`.
#[derive(Clone, Copy)]
struct SplitPlan {
    child: u32,
    boundary: u64,
}

/// A shard's role in an in-flight merge (§7.7), recorded on its [`Allocation`]
/// between `MergeStarted` and `MergeCommitted`. Mutually exclusive with a
/// migration `target` and a `split`.
#[derive(Clone, Copy)]
enum MergeRole {
    /// The **left** shard, absorbing the adjacent `right` (carries its index).
    Absorbing(u32),
    /// The **right** shard, folding into the adjacent `left` (carries its
    /// index) — the frozen, driving side.
    Absorbed(u32),
}

/// One shard's committed allocation: the key range it owns (§5.1), the `current`
/// replica set, and — one at a time, mutually exclusive — a migration `target`,
/// an in-flight `split`, or an in-flight `merge` role (§7.7). The range changes
/// only on a committed split or merge, never on migration.
#[derive(Clone)]
struct Allocation {
    range: KeyRange,
    current: Vec<NodeId>,
    target: Option<Vec<NodeId>>,
    split: Option<SplitPlan>,
    merge: Option<MergeRole>,
}

impl Allocation {
    /// Whether this shard is free to start a new split or merge — no migration,
    /// split, or merge already in flight (§7.7's one-op-at-a-time rule).
    fn idle(&self) -> bool {
        self.target.is_none() && self.split.is_none() && self.merge.is_none()
    }

    /// The nodes involved in the shard right now: `current ∪ target`.
    fn union(&self) -> Vec<NodeId> {
        let mut nodes = self.current.clone();
        if let Some(target) = &self.target {
            for node in target {
                if !nodes.contains(node) {
                    nodes.push(*node);
                }
            }
        }
        nodes
    }
}

fn encode(command: &ShardMapCommand) -> Vec<u8> {
    serde_json::to_vec(command).expect("a ShardMapCommand always serializes")
}

fn decode(bytes: &[u8]) -> Option<ShardMapCommand> {
    serde_json::from_slice(bytes).ok()
}

// --- the `Local` tier: the single node replicates everything ---------------------------

/// The single-node shard map (`Local` tier): this node is the sole replica of every
/// shard, all keyed in one local [`GrainStore`] by shard index.
pub(crate) struct LocalShardMap {
    node: NodeId,
    journals: Vec<Arc<dyn DynGrainJournal>>,
}

impl LocalShardMap {
    pub(crate) fn new(node: NodeId, shards: usize, store: Arc<dyn GrainStore>) -> LocalShardMap {
        let journals = (0..shards)
            .map(|shard| {
                Arc::new(LocalGrainJournal::over(Arc::clone(&store), shard as u32))
                    as Arc<dyn DynGrainJournal>
            })
            .collect();
        LocalShardMap { node, journals }
    }
}

impl ShardMapSource for LocalShardMap {
    fn replicas(&self, shard: u32) -> Option<Vec<NodeId>> {
        (shard < self.journals.len() as u32).then(|| vec![self.node])
    }

    fn journal(&self, shard: u32) -> Option<Arc<dyn DynGrainJournal>> {
        self.journals.get(shard as usize).cloned()
    }

    fn shard_of(&self, hash: u64) -> Option<u32> {
        // The `Local` tier keeps the founding partition forever (split/merge is a
        // `Quorum`-tier elasticity mechanism): the O(1) founding lookup.
        Some(crate::system::founding_index(hash, self.journals.len()))
    }

    fn shard_indices(&self) -> Vec<u32> {
        (0..self.journals.len() as u32).collect()
    }
}

// --- the client view: no allocation, no local journals ----------------------------------

/// The shard map of a routing-only **client** (an Orleans-style cluster client): it
/// hosts nothing, so it knows no allocation and holds no journal. A client never
/// reads `replicas`/`journal` on the data path — it routes through a host's gateway
/// — so both answer `None`. The handle exists only to satisfy [`Granary`]'s field.
pub(crate) struct EmptyShardMap;

impl ShardMapSource for EmptyShardMap {
    fn replicas(&self, _shard: u32) -> Option<Vec<NodeId>> {
        None
    }

    fn journal(&self, _shard: u32) -> Option<Arc<dyn DynGrainJournal>> {
        None
    }

    fn shard_of(&self, _hash: u64) -> Option<u32> {
        None
    }

    fn shard_indices(&self) -> Vec<u32> {
        Vec::new()
    }
}

// --- the `Quorum` tier: the consensus-agreed map over a Raft group ----------------------

/// The committed allocation and the local journals built from it. Shared between
/// the apply loop (writer) and the [`ShardMapSource`] reads.
#[derive(Default)]
struct Inner {
    /// shard index → the committed allocation (`current`, plus the migration
    /// `target` or split plan while one is in flight, §7.7).
    allocation: BTreeMap<u32, Allocation>,
    /// shard index → local store, for shards this node replicates (or is a
    /// migration target of).
    journals: BTreeMap<u32, Arc<dyn DynGrainJournal>>,
    /// Node-local pending split requests ([`ShardMapSource::request_split`]):
    /// drained by the split proposer, which turns each into a `SplitStarted`
    /// proposal forwarded to the map leader. Not committed state.
    split_requests: std::collections::BTreeSet<u32>,
    /// Node-local pending merge requests ([`ShardMapSource::request_merge`]),
    /// keyed by the **left** shard: merge it with its right neighbour. Drained
    /// by the merge proposer. Not committed state.
    merge_requests: std::collections::BTreeSet<u32>,
}

/// The shard-event emitter (spec §13): how the map's loops put `ShardSplit`,
/// `ShardMerged`, and `LeaderChanged` onto the framework's `Event` stream. A
/// closure seam because the loops are generic over just `R: RaftConsensus`,
/// which does not carry the `GranarySystem` event channel.
type EmitEvent = Arc<dyn Fn(GrainEvent) + Send + Sync>;

/// The typed per-shard handles the loops share (the [`ShardMapSource`] seam is
/// object-safe, so these cannot live in [`Inner`]): each hosted shard's **live**
/// [`ShardControl`] — replica sets, owned range, split freeze; the apply loop
/// mutates it in place as commands commit and the shard's replicator reads it
/// per operation (§7.7) — and its replicator, the migration/split driver's
/// handle.
type ShardHandles<R> = BTreeMap<
    u32,
    (
        Arc<std::sync::Mutex<ShardControl>>,
        Arc<QuorumReplicator<R>>,
    ),
>;

/// A [`ShardMapSource`] backed by a per-type Raft group (the `Quorum` tier). The group's
/// committed log is the allocation. The consensus handle lives only in the
/// spawned loops, so the source itself is just the shared `Inner`.
pub(crate) struct RaftShardMap {
    inner: Arc<Mutex<Inner>>,
}

impl RaftShardMap {
    /// Create the map group and start tracking the allocation (spec §7.6). The
    /// group's voters are the cluster's consensus-agreed control-plane voters
    /// ([`RaftConsensus::cluster_voters`]), so every node forms the identical group.
    /// Subscribes before driving and launches the apply loop and the leader-only
    /// allocator.
    #[allow(clippy::too_many_arguments)] // one call site, from `GranarySystem::shard_map`
    pub(crate) fn new<R: RaftConsensus>(
        consensus: R,
        grain_type: &'static str,
        shards: usize,
        replicas: usize,
        split_target_bytes: u64,
        local: Arc<dyn GrainStore>,
        transport: Arc<dyn ReplicaTransport>,
        emit: EmitEvent,
    ) -> RaftShardMap {
        let group = map_group_id_for(grain_type);
        let self_node = consensus.node();
        // the `Quorum` tier rides Raft: the map group and every shard group elect through the
        // system's consensus engine. A clustered system with no configured voters
        // has no engine at all (only `MembershipMode::Leader` builds one) — so no
        // group would ever elect, the gateway's redirect would hint this node back
        // at itself, and every grain call would loop on `NotLeader`. Fail loud at
        // host-construction (`granary()`), not silently at the first call. the `Local` tier
        // never reaches here: `LocalSystem` serves a `LocalShardMap` instead.
        assert!(
            !consensus.configured_voters().is_empty(),
            "granary `Quorum` requires leader-based consensus, but the system reports \
             no configured Raft voters: start the node in MembershipMode::Leader \
             (the only mode that builds the Raft engine). Static, Registry, and \
             Gossip modes have no engine, so no shard can elect a leader and every \
             grain call would loop on NotLeader."
        );
        let commits = consensus.subscribe_commits(group);
        // Seed the map group from the **configured** (founding) voters, identical
        // on every node regardless of when it calls `granary()`, so all nodes form
        // the same group. A node that joined after the founding set is not in it —
        // it forms the group as a non-member (it never disrupts elections) and the
        // reconcile loop on the leader adds it, after which it catches up the
        // committed allocation via replication and routes (spec §7.6).
        consensus.create_group(group, consensus.configured_voters(), Vec::new());
        let inner: Arc<Mutex<Inner>> = Arc::new(Mutex::new(Inner::default()));
        let handles: Arc<Mutex<ShardHandles<R>>> = Arc::new(Mutex::new(BTreeMap::new()));

        consensus.launch(Box::pin(apply_loop(
            consensus.clone(),
            grain_type,
            self_node,
            commits,
            Arc::clone(&inner),
            Arc::clone(&handles),
            Arc::clone(&local),
            transport,
            Arc::clone(&emit),
        )));
        consensus.launch(Box::pin(allocator_loop(
            consensus.clone(),
            grain_type,
            group,
            shards,
            replicas,
            Arc::downgrade(&inner),
        )));
        consensus.launch(Box::pin(reconcile_loop(
            consensus.clone(),
            grain_type,
            group,
            Arc::downgrade(&inner),
        )));
        consensus.launch(Box::pin(migrate_loop(
            consensus.clone(),
            grain_type,
            group,
            Arc::downgrade(&inner),
            Arc::downgrade(&handles),
        )));
        consensus.launch(Box::pin(split_loop(
            consensus.clone(),
            grain_type,
            group,
            Arc::downgrade(&inner),
            Arc::downgrade(&handles),
        )));
        consensus.launch(Box::pin(merge_loop(
            consensus.clone(),
            grain_type,
            group,
            Arc::downgrade(&inner),
            Arc::downgrade(&handles),
        )));
        consensus.launch(Box::pin(leader_watch_loop(
            consensus.clone(),
            grain_type,
            Arc::downgrade(&inner),
            emit,
        )));
        if split_target_bytes > 0 {
            consensus.launch(Box::pin(split_trigger_loop(
                consensus.clone(),
                grain_type,
                split_target_bytes,
                local,
                Arc::downgrade(&inner),
            )));
        }

        RaftShardMap { inner }
    }
}

impl ShardMapSource for RaftShardMap {
    fn replicas(&self, shard: u32) -> Option<Vec<NodeId>> {
        self.inner
            .lock()
            .expect("shard map mutex poisoned")
            .allocation
            .get(&shard)
            .map(|alloc| alloc.current.clone())
    }

    fn journal(&self, shard: u32) -> Option<Arc<dyn DynGrainJournal>> {
        self.inner
            .lock()
            .expect("shard map mutex poisoned")
            .journals
            .get(&shard)
            .cloned()
    }

    fn shard_of(&self, hash: u64) -> Option<u32> {
        // O(shards) scan of the committed ranges. Shard counts are dozens, not
        // thousands (G7 bounds them), and the hot path caches its resolution, so
        // a sorted-range index is not yet worth the bookkeeping.
        self.inner
            .lock()
            .expect("shard map mutex poisoned")
            .allocation
            .iter()
            .find(|(_, alloc)| alloc.range.contains(hash))
            .map(|(&shard, _)| shard)
    }

    fn shard_indices(&self) -> Vec<u32> {
        self.inner
            .lock()
            .expect("shard map mutex poisoned")
            .allocation
            .keys()
            .copied()
            .collect()
    }

    fn request_split(&self, shard: u32) {
        self.inner
            .lock()
            .expect("shard map mutex poisoned")
            .split_requests
            .insert(shard);
    }

    fn request_merge(&self, left: u32) {
        self.inner
            .lock()
            .expect("shard map mutex poisoned")
            .merge_requests
            .insert(left);
    }
}

/// Apply the map group's committed allocation records (spec §7.6, §7.7).
///
/// The **founding** `Assign` for a shard records its replica set; every node in it
/// creates the shard's leader-election group and builds its
/// [`QuorumGrainJournal`]. A later `Assign` onto an already-allocated shard starts
/// a **migration** (§7.7): it commits the `target` set — from that point every
/// write and recovery uses the joint quorum (majority of current AND of target,
/// enforced by the shared [`ReplicaSets`]) — and a node newly involved creates the
/// shard group as a non-member over the *current* voters (no election disruption;
/// the reconcile loop promotes it) and builds a journal so it can serve once
/// leadership reaches it. `Migrated` — proposed by the shard leader's driver only
/// after every grain is caught up on the target — flips `current = target`; nodes
/// leaving the set drop their journal (an active `Host` keeps its own `Arc`, so an
/// in-flight append still completes or fails cleanly as `NotLeader`).
///
/// Runs until the map group's commit stream closes.
#[allow(clippy::too_many_arguments)]
async fn apply_loop<R: RaftConsensus>(
    consensus: R,
    grain_type: &'static str,
    self_node: NodeId,
    commits: Receiver<Committed>,
    inner: Arc<Mutex<Inner>>,
    handles: Arc<Mutex<ShardHandles<R>>>,
    local: Arc<dyn GrainStore>,
    transport: Arc<dyn ReplicaTransport>,
    emit: EmitEvent,
) {
    // Build this node's journal + live control for `shard` and register them.
    let build = |shard: u32, sets: ReplicaSets, range: KeyRange| {
        let shard_group = shard_group_id(grain_type, shard);
        let control = Arc::new(std::sync::Mutex::new(ShardControl::new(sets, range)));
        let journal = QuorumGrainJournal::new(
            consensus.clone(),
            shard_group,
            shard,
            Arc::clone(&control),
            Arc::clone(&local),
            Arc::clone(&transport),
        );
        handles
            .lock()
            .expect("shard handles mutex poisoned")
            .insert(shard, (control, journal.replicator()));
        inner
            .lock()
            .expect("shard map mutex poisoned")
            .journals
            .insert(shard, Arc::new(journal) as Arc<dyn DynGrainJournal>);
    };

    while let Ok(observation) = commits.recv().await {
        // The map group is tiny and never compacts, so it only ever applies
        // commands; a snapshot install (were one to occur) carries no allocation
        // this loop can act on and is ignored.
        let Committed::Apply { command: bytes, .. } = observation else {
            continue;
        };
        let Some(command) = decode(&bytes) else {
            continue; // a command this map cannot parse is defensively ignored
        };
        match command {
            ShardMapCommand::Assign {
                shard,
                replicas,
                range,
            } => {
                // Record the transition under the map lock; act on it below.
                enum Change {
                    /// The founding allocation for this shard.
                    Founding,
                    /// A migration toward `replicas` began; carries the current
                    /// set and the shard's committed range (migration never
                    /// moves the range, so the command's `range` is ignored).
                    Migration(Vec<NodeId>, KeyRange),
                    /// A re-proposed identical allocation — nothing changed.
                    Noop,
                }
                let change = {
                    let mut guard = inner.lock().expect("shard map mutex poisoned");
                    match guard.allocation.get_mut(&shard) {
                        None => {
                            guard.allocation.insert(
                                shard,
                                Allocation {
                                    range,
                                    current: replicas.clone(),
                                    target: None,
                                    split: None,
                                    merge: None,
                                },
                            );
                            Change::Founding
                        }
                        Some(alloc)
                            if alloc.current == replicas && alloc.target.is_none()
                                || alloc.target.as_ref() == Some(&replicas) =>
                        {
                            Change::Noop
                        }
                        Some(alloc) => {
                            alloc.target = Some(replicas.clone());
                            Change::Migration(alloc.current.clone(), alloc.range)
                        }
                    }
                };
                let shard_group = shard_group_id(grain_type, shard);
                match change {
                    Change::Noop => {}
                    Change::Founding => {
                        if replicas.contains(&self_node) {
                            consensus.create_group(shard_group, replicas.clone(), Vec::new());
                            build(shard, ReplicaSets::new(replicas), range);
                        }
                    }
                    Change::Migration(current, committed_range) => {
                        let involved =
                            current.contains(&self_node) || replicas.contains(&self_node);
                        let has_handles = handles
                            .lock()
                            .expect("shard handles mutex poisoned")
                            .contains_key(&shard);
                        if has_handles {
                            // A continuing participant: flip its live sets so every
                            // in-flight journal switches to the joint quorum (§7.7).
                            if let Some((control, _)) = handles
                                .lock()
                                .expect("shard handles mutex poisoned")
                                .get(&shard)
                            {
                                control.lock().expect("shard control poisoned").sets =
                                    ReplicaSets {
                                        current: current.clone(),
                                        target: Some(replicas.clone()),
                                    };
                            }
                        } else if involved {
                            // Newly a target member: form the shard group as a
                            // non-member over the CURRENT voters (no election
                            // disruption; the reconcile loop promotes it) and build
                            // the journal over the joint sets.
                            consensus.create_group(shard_group, current.clone(), Vec::new());
                            build(
                                shard,
                                ReplicaSets {
                                    current,
                                    target: Some(replicas),
                                },
                                committed_range,
                            );
                        }
                    }
                }
            }
            ShardMapCommand::Migrated { shard } => {
                // Flip `current = target` (a duplicate `Migrated` finds no target
                // and is a no-op).
                let flipped = {
                    let mut guard = inner.lock().expect("shard map mutex poisoned");
                    match guard.allocation.get_mut(&shard) {
                        Some(alloc) => {
                            let target = alloc.target.take();
                            if let Some(target) = &target {
                                alloc.current = target.clone();
                            }
                            target
                        }
                        None => None,
                    }
                };
                let Some(current) = flipped else { continue };
                if current.contains(&self_node) {
                    if let Some((control, _)) = handles
                        .lock()
                        .expect("shard handles mutex poisoned")
                        .get(&shard)
                    {
                        control.lock().expect("shard control poisoned").sets =
                            ReplicaSets::new(current);
                    }
                } else {
                    // Leaving the set: drop the journal and handles (the reconcile
                    // loop removes this node from the group's voters).
                    inner
                        .lock()
                        .expect("shard map mutex poisoned")
                        .journals
                        .remove(&shard);
                    handles
                        .lock()
                        .expect("shard handles mutex poisoned")
                        .remove(&shard);
                }
            }
            ShardMapCommand::SplitStarted {
                parent,
                child,
                boundary,
            } => {
                // Deterministic validation against committed state — a pure
                // function of (command, map state), so every node accepts or
                // rejects identically. Rejection is a silent no-op: the proposer
                // raced a migration, another split, or a stale request.
                let started = {
                    let mut guard = inner.lock().expect("shard map mutex poisoned");
                    let child_free = !guard.allocation.contains_key(&child)
                        && child != MAP_SHARD_INDEX
                        && guard
                            .allocation
                            .values()
                            .all(|a| a.split.is_none_or(|p| p.child != child));
                    match guard.allocation.get_mut(&parent) {
                        Some(alloc)
                            if alloc.idle()
                                && alloc.range.start < boundary
                                && boundary <= alloc.range.end
                                && child_free =>
                        {
                            alloc.split = Some(SplitPlan { child, boundary });
                            true
                        }
                        _ => false,
                    }
                };
                if started {
                    // A parent replica arms the leader-local freeze and bounds
                    // its own store as apply-time catch-up; the driver's
                    // majority-acked seal fan is the authoritative barrier.
                    if let Some((control, _)) = handles
                        .lock()
                        .expect("shard handles mutex poisoned")
                        .get(&parent)
                    {
                        control.lock().expect("shard control poisoned").frozen_from =
                            Some(boundary);
                        local.seal_range(parent, boundary);
                    }
                }
            }
            ShardMapCommand::SplitCommitted { parent } => {
                // Flip the mapping: shrink the parent's range, allocate the
                // child on the parent's replicas. A duplicate commit finds no
                // plan and is a no-op.
                let committed = {
                    let mut guard = inner.lock().expect("shard map mutex poisoned");
                    match guard.allocation.get_mut(&parent) {
                        Some(alloc) => match alloc.split.take() {
                            Some(plan) => {
                                let old_end = alloc.range.end;
                                alloc.range.end = plan.boundary - 1;
                                let current = alloc.current.clone();
                                let child_range = KeyRange {
                                    start: plan.boundary,
                                    end: old_end,
                                };
                                guard.allocation.insert(
                                    plan.child,
                                    Allocation {
                                        range: child_range,
                                        current: current.clone(),
                                        target: None,
                                        split: None,
                                        merge: None,
                                    },
                                );
                                Some((plan, child_range, current))
                            }
                            None => None,
                        },
                        None => None,
                    }
                };
                let Some((plan, child_range, current)) = committed else {
                    continue;
                };
                // Parent replicas: shrink the live control range (appends to the
                // moved range now refuse `NotLeader` at the leader), drop the
                // freeze (the shrunken range subsumes it), catch up the durable
                // bound in case this replica missed the driver's fan, and GC the
                // moved grains' parent-keyed data — the child copy is
                // quorum-durable, that is what licensed `SplitCommitted`.
                if let Some((control, _)) = handles
                    .lock()
                    .expect("shard handles mutex poisoned")
                    .get(&parent)
                {
                    {
                        let mut control = control.lock().expect("shard control poisoned");
                        control.range.end = plan.boundary - 1;
                        control.frozen_from = None;
                    }
                    local.seal_range(parent, plan.boundary);
                    for grain in local.grains(parent) {
                        if crate::system::name_hash(grain.grain_type(), grain.key())
                            >= plan.boundary
                        {
                            local.remove_grain(parent, &grain);
                        }
                    }
                }
                // Child replicas (the parent's set, inherited): create the
                // child's leader-election group and journal — only now, so no
                // child term exists during the transfer and neither side served
                // the moved range early (§7.7's commit-before-serve ordering).
                if current.contains(&self_node) {
                    let child_group = shard_group_id(grain_type, plan.child);
                    consensus.create_group(child_group, current.clone(), Vec::new());
                    build(plan.child, ReplicaSets::new(current), child_range);
                }
                emit(GrainEvent::ShardSplit {
                    node: self_node,
                    grain_type,
                    parent,
                    child: plan.child,
                    boundary: plan.boundary,
                });
            }
            ShardMapCommand::MergeStarted { left, right } => {
                // Deterministic validation: both allocated, both idle, adjacent
                // (left's range ends just below right's). Replica sets need not
                // match — the right driver recovers from right's quorum and
                // transfers to left's replicas, exactly as migration crosses
                // sets. An invalid proposal is a silent no-op on every node.
                let started = {
                    let mut guard = inner.lock().expect("shard map mutex poisoned");
                    let ok = match (guard.allocation.get(&left), guard.allocation.get(&right)) {
                        (Some(l), Some(r)) => {
                            l.idle()
                                && r.idle()
                                && l.range.end.checked_add(1) == Some(r.range.start)
                        }
                        _ => false,
                    };
                    if ok {
                        guard.allocation.get_mut(&left).expect("checked").merge =
                            Some(MergeRole::Absorbing(right));
                        guard.allocation.get_mut(&right).expect("checked").merge =
                            Some(MergeRole::Absorbed(left));
                    }
                    ok
                };
                if started {
                    // Right replicas: freeze the whole right range and bound the
                    // store (the driver's majority-acked seal is authoritative).
                    let right_start = {
                        let guard = inner.lock().expect("shard map mutex poisoned");
                        guard.allocation.get(&right).map(|a| a.range.start)
                    };
                    if let (Some(start), Some((control, _))) = (
                        right_start,
                        handles
                            .lock()
                            .expect("shard handles mutex poisoned")
                            .get(&right),
                    ) {
                        control.lock().expect("shard control poisoned").frozen_from = Some(start);
                        local.seal_range(right, start);
                    }
                }
            }
            ShardMapCommand::MergeCommitted { left, right } => {
                // Flip the mapping: extend left over right, drop right. A
                // duplicate commit finds no merge role and is a no-op.
                let committed = {
                    let mut guard = inner.lock().expect("shard map mutex poisoned");
                    // Left must still be absorbing exactly this right (a duplicate
                    // or crossed commit otherwise finds a mismatched role).
                    let matches = matches!(
                        guard.allocation.get(&left).and_then(|l| l.merge),
                        Some(MergeRole::Absorbing(r)) if r == right
                    );
                    if matches && guard.allocation.contains_key(&right) {
                        let right_end = guard.allocation[&right].range.end;
                        let left_alloc = guard.allocation.get_mut(&left).expect("checked");
                        left_alloc.range.end = right_end;
                        left_alloc.merge = None;
                        let new_end = right_end;
                        guard.allocation.remove(&right);
                        Some(new_end)
                    } else {
                        None
                    }
                };
                let Some(new_end) = committed else { continue };
                // Left replicas: extend the live range, then re-establish the
                // store bound. `unseal` clears any prior split boundary; a fresh
                // seal at `new_end + 1` protects whatever shards still lie above
                // left (a no-op-safe backstop when none do), so a stale
                // higher-term leader can never append past left's committed range
                // (G15). Left keeps its journal/group.
                if let Some((control, _)) = handles
                    .lock()
                    .expect("shard handles mutex poisoned")
                    .get(&left)
                {
                    control.lock().expect("shard control poisoned").range.end = new_end;
                    local.unseal(left);
                    if let Some(bound) = new_end.checked_add(1) {
                        local.seal_range(left, bound);
                    }
                }
                // Right replicas: GC the now-unreachable right-keyed data (the
                // grains live under left's keys after the transfer), drop the
                // journal/handles, and retire the group — it holds no data
                // (§7.1), so no in-group consensus is needed; every node retires
                // it as it applies this commit (G7).
                let had_right = inner
                    .lock()
                    .expect("shard map mutex poisoned")
                    .journals
                    .remove(&right)
                    .is_some();
                if had_right {
                    for grain in local.grains(right) {
                        local.remove_grain(right, &grain);
                    }
                    local.unseal(right);
                }
                handles
                    .lock()
                    .expect("shard handles mutex poisoned")
                    .remove(&right);
                consensus.remove_group(shard_group_id(grain_type, right));
                emit(GrainEvent::ShardMerged {
                    node: self_node,
                    grain_type,
                    left,
                    right,
                });
            }
        }
    }
}

/// The leader-only allocator (spec §7.6, §7.7): while this node leads the map
/// group, keep each shard's committed replica set equal to its rendezvous choice
/// over the **current cluster voters**. It re-proposes an [`Assign`](ShardMapCommand::Assign)
/// whenever a shard's desired set differs from the committed one — so as the
/// cluster grows or shrinks, shards rebalance onto/off the changed members. All
/// nodes see the same committed `cluster_voters()`, so the rendezvous is agreed;
/// only the map leader proposes. Exits when the map is dropped.
async fn allocator_loop<R: RaftConsensus>(
    consensus: R,
    grain_type: &'static str,
    group: GroupId,
    shards: usize,
    replicas: usize,
    inner: Weak<Mutex<Inner>>,
) {
    loop {
        consensus.sleep(ALLOCATE_INTERVAL).await;
        let Some(inner) = inner.upgrade() else {
            return; // the map was dropped — stop the loop
        };
        if !consensus.group_is_leader(group) {
            continue;
        }
        let voters = consensus.cluster_voters();
        if voters.is_empty() {
            continue; // control plane not settled yet; nothing to allocate over
        }
        // The proposal set: the founding shards (allocated with their initial
        // ranges if absent) plus every committed shard — a split-minted child
        // (index ≥ `shards`) rebalances exactly like a founding shard once its
        // allocation commits. `None` range ⇒ founding; `Some` ⇒ the committed
        // range, carried unchanged (migration never moves a range).
        let founding = initial_ranges(shards);
        let candidates: Vec<(u32, KeyRange)> = {
            let guard = inner.lock().expect("shard map mutex poisoned");
            let mut candidates: Vec<(u32, KeyRange)> = founding
                .iter()
                .enumerate()
                .filter(|(shard, _)| !guard.allocation.contains_key(&(*shard as u32)))
                .map(|(shard, &range)| (shard as u32, range))
                .collect();
            candidates.extend(
                guard
                    .allocation
                    .iter()
                    .map(|(&shard, alloc)| (shard, alloc.range)),
            );
            candidates
        };
        for (shard, range) in candidates {
            let desired = select_replicas(&voters, shard_group_id(grain_type, shard), replicas).0;
            // Propose the founding allocation, or — when the desired set has
            // drifted from the committed one — start a migration toward it
            // (§7.7). Never re-propose while a migration is already in flight:
            // retargeting mid-migration would flip `current` to a set the driver
            // never caught up (the `Migrated` guard relies on this). Nor while a
            // split is in flight — migration and split are mutually exclusive
            // per shard (the `SplitStarted` validation enforces the converse).
            let proposable = match inner
                .lock()
                .expect("shard map mutex poisoned")
                .allocation
                .get(&shard)
            {
                None => true,
                Some(alloc) => alloc.idle() && alloc.current != desired,
            };
            if proposable {
                consensus
                    .propose_to(
                        group,
                        encode(&ShardMapCommand::Assign {
                            shard,
                            replicas: desired,
                            range,
                        }),
                    )
                    .await;
            }
        }
    }
}

/// The reconcile loop (spec §7.6, §7.7): drive each group **this node leads**
/// toward its intended membership.
/// - The **map group** → the cluster's current `cluster_voters()`, so a node that
///   joins the cluster after the map formed is added (it then catches up the
///   committed allocation via replication and can route), and a removed node is
///   dropped.
/// - Each **shard group** → its committed allocation `allocation[shard]`, so a
///   rebalance actually moves the shard's Raft membership in place (the shard
///   leader `AddVoter`s the new replicas — which catch up via replication — and
///   `RemoveVoter`s the departed ones).
///
/// `reconfigure_group` proposes the deltas as separate single-server changes and is
/// leader-only, so only one node reconfigures a given group. Exits when the map is
/// dropped. (Bounding the map group to fewer than all voters — learners for
/// routing-only nodes — remains a refinement.)
async fn reconcile_loop<R: RaftConsensus>(
    consensus: R,
    grain_type: &'static str,
    map_group: GroupId,
    inner: Weak<Mutex<Inner>>,
) {
    loop {
        consensus.sleep(ALLOCATE_INTERVAL).await;
        let Some(inner) = inner.upgrade() else {
            return; // the map was dropped — stop the loop
        };
        // Map group → current cluster voters.
        if consensus.group_is_leader(map_group) {
            let target = consensus.cluster_voters();
            if !target.is_empty() {
                consensus.reconfigure_group(map_group, target);
            }
        }
        // Each shard group this node leads → its committed allocation: the union
        // (current ∪ target) while a migration is in flight — target members must
        // become voters so leadership can reach them, and current members must
        // stay voters until `Migrated` — then the (new) current set.
        let allocation: Vec<(u32, Vec<NodeId>)> = {
            let guard = inner.lock().expect("shard map mutex poisoned");
            guard
                .allocation
                .iter()
                .map(|(&s, alloc)| (s, alloc.union()))
                .collect()
        };
        for (shard, voters) in allocation {
            let shard_group = shard_group_id(grain_type, shard);
            if consensus.group_is_leader(shard_group) {
                consensus.reconfigure_group(shard_group, voters);
            }
        }
    }
}

/// The migration driver (spec §7.7): while this node leads a shard whose
/// allocation carries a `target`, catch every grain up on the target set —
/// records via the joint-quorum recovery write-back, the snapshot via a joint
/// `save_snapshot`, blobs via verified copy — and, once a full pass succeeds,
/// propose [`Migrated`](ShardMapCommand::Migrated) to flip the set. Every step is
/// idempotent, so a leader change mid-migration just re-drives on the new leader;
/// a failed step leaves `target` in place and the next tick retries. Exits when
/// the map is dropped.
async fn migrate_loop<R: RaftConsensus>(
    consensus: R,
    grain_type: &'static str,
    map_group: GroupId,
    inner: Weak<Mutex<Inner>>,
    handles: Weak<Mutex<ShardHandles<R>>>,
) {
    loop {
        consensus.sleep(ALLOCATE_INTERVAL).await;
        let (Some(inner), Some(handles)) = (inner.upgrade(), handles.upgrade()) else {
            return; // the map was dropped — stop the loop
        };
        let migrating: Vec<u32> = {
            let guard = inner.lock().expect("shard map mutex poisoned");
            guard
                .allocation
                .iter()
                .filter(|(_, alloc)| alloc.target.is_some())
                .map(|(&shard, _)| shard)
                .collect()
        };
        for shard in migrating {
            let shard_group = shard_group_id(grain_type, shard);
            if !consensus.group_is_leader(shard_group) {
                continue; // another (leading) node drives this shard's migration
            }
            let Some((_, replicator)) = handles
                .lock()
                .expect("shard handles mutex poisoned")
                .get(&shard)
                .cloned()
            else {
                continue; // handles not built yet; retry next tick
            };
            // One catch-up pass: enumerate from a read quorum, then migrate each
            // grain. Any failure aborts the pass; `target` stays and we retry.
            let Ok(grains) = replicator.migration_grains().await else {
                continue;
            };
            let mut complete = true;
            for grain in grains {
                if replicator.migrate_grain(&grain).await.is_err() {
                    complete = false;
                    break;
                }
            }
            // Re-check the target is still the one we migrated toward, then flip.
            if complete && replicator.migration_target().is_some() {
                consensus
                    .propose_to(map_group, encode(&ShardMapCommand::Migrated { shard }))
                    .await;
            }
        }
    }
}

/// One transfer pass shared by the split and merge drivers (spec §7.7): seal the
/// moving range durably at `seal_from` (from here no append to it reaches a write
/// quorum, G15), enumerate the shard's grains from a read quorum, and transfer
/// every grain the `moves` predicate keeps to `dest` on `dest_replicas`. Returns
/// whether a full pass succeeded — a failed seal, enumerate, or transfer yields
/// `false`, leaving the plan in place for the next tick to re-drive. Every step is
/// idempotent, so a re-driven pass re-copies equal slots.
async fn transfer_pass<R: RaftConsensus>(
    replicator: &QuorumReplicator<R>,
    seal_from: u64,
    dest: u32,
    dest_replicas: &[NodeId],
    moves: impl Fn(&crate::grain::GrainName) -> bool,
) -> bool {
    if replicator.seal_shard(seal_from).await.is_err() {
        return false; // seal short of a majority; retry next tick
    }
    let Ok(grains) = replicator.migration_grains().await else {
        return false;
    };
    for grain in grains {
        if moves(&grain)
            && replicator
                .transfer_grain(&grain, dest, dest_replicas)
                .await
                .is_err()
        {
            return false;
        }
    }
    true
}

/// The split proposer + driver (spec §7.7).
///
/// **Proposer** (any node): drain locally-queued split requests
/// ([`ShardMapSource::request_split`] — the admin/test seam and the size
/// trigger's path), derive the plan from committed state — child index = one
/// past the highest index in use, boundary = the range midpoint — and propose
/// `SplitStarted` (forwarded to the map leader by `propose_to`). The apply-time
/// validation is the arbiter; a raced or stale proposal is a committed no-op.
///
/// **Driver** (the parent shard's leader): for each shard with a committed
/// split plan, run one transfer pass —
/// 1. **seal**: durably bound appends at `boundary` on a majority of replicas
///    (from here no append to the moving range can reach a write quorum, G15);
/// 2. **enumerate** the shard's grains from a read quorum, keep the moving ones;
/// 3. **transfer** each moved grain's committed prefix, snapshot, and blobs to
///    the child's keys on a majority (the child inherits the parent's replicas);
/// 4. **commit**: still leading with the plan still pending, propose
///    `SplitCommitted`, which flips routing (§7.7's commit-before-serve).
///
/// Every step is idempotent and derived from committed state: a crash, leader
/// change, or failed step just re-drives on the next tick (a re-driven transfer
/// re-copies equal slots). Exits when the map is dropped.
async fn split_loop<R: RaftConsensus>(
    consensus: R,
    grain_type: &'static str,
    map_group: GroupId,
    inner: Weak<Mutex<Inner>>,
    handles: Weak<Mutex<ShardHandles<R>>>,
) {
    loop {
        consensus.sleep(ALLOCATE_INTERVAL).await;
        let (Some(inner), Some(handles)) = (inner.upgrade(), handles.upgrade()) else {
            return; // the map was dropped — stop the loop
        };
        // Proposer: (re)propose `SplitStarted` for each requested shard. A
        // request is RETAINED until the plan is observably committed
        // (`split.is_some()`) or the shard becomes unsplittable/gone — so a
        // proposal lost to a crashing map leader is retried, not dropped (the
        // request queues node-locally and is not itself replicated). Two nodes
        // proposing the same shard derive the identical `(child, boundary)` from
        // committed state, and apply dedups the loser.
        enum Action {
            /// Propose (or re-propose) the split with this plan.
            Propose(u32, u64),
            /// Keep the request queued but do not propose this tick (busy).
            Wait,
            /// The request is done or moot — drop it.
            Clear,
        }
        let requests: Vec<u32> = {
            let guard = inner.lock().expect("shard map mutex poisoned");
            guard.split_requests.iter().copied().collect()
        };
        for shard in requests {
            let action = {
                let guard = inner.lock().expect("shard map mutex poisoned");
                match guard.allocation.get(&shard) {
                    None => Action::Clear, // shard gone (merged away)
                    Some(alloc) if alloc.split.is_some() => Action::Clear, // started — handed off
                    Some(alloc) if alloc.range.start >= alloc.range.end => Action::Clear, // one point
                    // Migrating or merging: not idle. Retry once the shard frees up.
                    Some(alloc) if !alloc.idle() => Action::Wait,
                    Some(alloc) => {
                        // One past the highest index in use — committed shards
                        // and in-flight splits' children both count, so two
                        // concurrent splits cannot mint the same child (the
                        // apply validation still arbitrates a propose race).
                        let highest = guard
                            .allocation
                            .iter()
                            .flat_map(|(&s, a)| std::iter::once(s).chain(a.split.map(|p| p.child)))
                            .max()
                            .unwrap_or(0);
                        let range = alloc.range;
                        let boundary = range.start + (range.end - range.start) / 2 + 1;
                        Action::Propose(highest + 1, boundary)
                    }
                }
            };
            match action {
                Action::Clear => {
                    inner
                        .lock()
                        .expect("shard map mutex poisoned")
                        .split_requests
                        .remove(&shard);
                }
                Action::Wait => {}
                Action::Propose(child, boundary) => {
                    consensus
                        .propose_to(
                            map_group,
                            encode(&ShardMapCommand::SplitStarted {
                                parent: shard,
                                child,
                                boundary,
                            }),
                        )
                        .await;
                }
            }
        }
        // Driver: one transfer pass per split this node leads.
        let splitting: Vec<(u32, SplitPlan, Vec<NodeId>)> = {
            let guard = inner.lock().expect("shard map mutex poisoned");
            guard
                .allocation
                .iter()
                .filter_map(|(&shard, alloc)| {
                    alloc.split.map(|plan| (shard, plan, alloc.current.clone()))
                })
                .collect()
        };
        for (parent, plan, replicas) in splitting {
            if !consensus.group_is_leader(shard_group_id(grain_type, parent)) {
                continue; // another (leading) node drives this split
            }
            let Some((_, replicator)) = handles
                .lock()
                .expect("shard handles mutex poisoned")
                .get(&parent)
                .cloned()
            else {
                continue; // handles not built yet; retry next tick
            };
            // Seal at the boundary and transfer only the grains above it — those
            // below are retained by the parent.
            let complete =
                transfer_pass(&replicator, plan.boundary, plan.child, &replicas, |grain| {
                    crate::system::name_at_or_above(grain, plan.boundary)
                })
                .await;
            // Re-check the plan is still pending (a duplicate commit is a no-op
            // anyway — apply takes the plan exactly once), then flip the map.
            if complete
                && inner
                    .lock()
                    .expect("shard map mutex poisoned")
                    .allocation
                    .get(&parent)
                    .is_some_and(|alloc| alloc.split.is_some())
            {
                consensus
                    .propose_to(
                        map_group,
                        encode(&ShardMapCommand::SplitCommitted { parent }),
                    )
                    .await;
            }
        }
    }
}

/// The merge proposer + driver (spec §7.7) — the mirror of [`split_loop`].
///
/// **Proposer** (any node): for each requested `left` (retained until the merge
/// is observably started or moot), find its right neighbour and propose
/// `MergeStarted` when both are idle and adjacent.
///
/// **Driver** (the **right** shard's leader — it holds the term to fence right's
/// deposed leaders): for each shard with an `Absorbed(left)` role, run one pass —
/// seal the whole right range, enumerate right's grains, transfer each into
/// `left`'s keys on `left`'s replicas, then propose `MergeCommitted`, which
/// extends `left`, drops `right`, and retires its group (G7). Idempotent and
/// re-drivable, exactly like the split driver. Exits when the map is dropped.
async fn merge_loop<R: RaftConsensus>(
    consensus: R,
    grain_type: &'static str,
    map_group: GroupId,
    inner: Weak<Mutex<Inner>>,
    handles: Weak<Mutex<ShardHandles<R>>>,
) {
    loop {
        consensus.sleep(ALLOCATE_INTERVAL).await;
        let (Some(inner), Some(handles)) = (inner.upgrade(), handles.upgrade()) else {
            return; // the map was dropped — stop the loop
        };
        // Proposer: (re)propose `MergeStarted` for each requested left shard.
        enum Action {
            Propose(u32), // the right neighbour to merge in
            Wait,
            Clear,
        }
        let requests: Vec<u32> = {
            let guard = inner.lock().expect("shard map mutex poisoned");
            guard.merge_requests.iter().copied().collect()
        };
        for left in requests {
            let action = {
                let guard = inner.lock().expect("shard map mutex poisoned");
                match guard.allocation.get(&left) {
                    None => Action::Clear,                         // gone
                    Some(l) if l.merge.is_some() => Action::Clear, // started — handed off
                    Some(l) => {
                        // The right neighbour: the shard whose range begins just
                        // past left's. `None` (left owns the top) → nothing to merge.
                        let right = l.range.end.checked_add(1).and_then(|start| {
                            guard
                                .allocation
                                .iter()
                                .find(|(_, r)| r.range.start == start)
                                .map(|(&idx, _)| idx)
                        });
                        match right {
                            None => Action::Clear,
                            Some(right) => {
                                let both_idle = l.idle()
                                    && guard.allocation.get(&right).is_some_and(|r| r.idle());
                                if both_idle {
                                    Action::Propose(right)
                                } else {
                                    Action::Wait
                                }
                            }
                        }
                    }
                }
            };
            match action {
                Action::Clear => {
                    inner
                        .lock()
                        .expect("shard map mutex poisoned")
                        .merge_requests
                        .remove(&left);
                }
                Action::Wait => {}
                Action::Propose(right) => {
                    consensus
                        .propose_to(
                            map_group,
                            encode(&ShardMapCommand::MergeStarted { left, right }),
                        )
                        .await;
                }
            }
        }
        // Driver: one transfer pass per merge whose RIGHT shard this node leads.
        let merging: Vec<(u32, u32, Vec<NodeId>)> = {
            let guard = inner.lock().expect("shard map mutex poisoned");
            guard
                .allocation
                .iter()
                .filter_map(|(&right, alloc)| match alloc.merge {
                    Some(MergeRole::Absorbed(left)) => {
                        let left_replicas = guard.allocation.get(&left)?.current.clone();
                        Some((right, left, left_replicas))
                    }
                    _ => None,
                })
                .collect()
        };
        for (right, left, left_replicas) in merging {
            if !consensus.group_is_leader(shard_group_id(grain_type, right)) {
                continue; // the right leader drives its own merge
            }
            let right_start = {
                let guard = inner.lock().expect("shard map mutex poisoned");
                match guard.allocation.get(&right) {
                    Some(alloc) => alloc.range.start,
                    None => continue,
                }
            };
            let Some((_, replicator)) = handles
                .lock()
                .expect("shard handles mutex poisoned")
                .get(&right)
                .cloned()
            else {
                continue; // handles not built yet; retry next tick
            };
            // Seal the whole right range, then transfer every grain into left.
            let complete =
                transfer_pass(&replicator, right_start, left, &left_replicas, |_| true).await;
            if complete
                && inner
                    .lock()
                    .expect("shard map mutex poisoned")
                    .allocation
                    .get(&right)
                    .is_some_and(|alloc| alloc.merge.is_some())
            {
                consensus
                    .propose_to(
                        map_group,
                        encode(&ShardMapCommand::MergeCommitted { left, right }),
                    )
                    .await;
            }
        }
    }
}

/// How often the size trigger re-measures the shards it leads. Slower than the
/// allocator's cadence — a shard's size drifts gradually, and a split is
/// expensive, so there is no need to poll it tightly.
const SPLIT_TRIGGER_INTERVAL: Duration = Duration::from_millis(500);

/// The size-based split trigger (spec §7.7): for each shard this node **leads**,
/// if its local durable footprint exceeds `target_bytes` and the shard is idle
/// (no migration or split in flight) with a splittable range, request a split.
/// The request rides the same [`request_split`](ShardMapSource::request_split)
/// queue the admin/test seam uses, so the `split_loop` proposer and its
/// committed-state validation are the single arbiter — this loop only supplies
/// the size signal. Measured on the leader because it holds the shard's data
/// locally and is the only node that can drive the resulting split. Exits when
/// the map is dropped.
async fn split_trigger_loop<R: RaftConsensus>(
    consensus: R,
    grain_type: &'static str,
    target_bytes: u64,
    local: Arc<dyn GrainStore>,
    inner: Weak<Mutex<Inner>>,
) {
    loop {
        consensus.sleep(SPLIT_TRIGGER_INTERVAL).await;
        let Some(inner) = inner.upgrade() else {
            return; // the map was dropped — stop the loop
        };
        // The idle, splittable shards this node's allocation knows about.
        let candidates: Vec<u32> = {
            let guard = inner.lock().expect("shard map mutex poisoned");
            guard
                .allocation
                .iter()
                .filter(|(_, alloc)| alloc.idle() && alloc.range.start < alloc.range.end)
                .map(|(&shard, _)| shard)
                .collect()
        };
        for shard in candidates {
            if !consensus.group_is_leader(shard_group_id(grain_type, shard)) {
                continue; // only the leader measures and drives its shards
            }
            if local.shard_bytes(shard) > target_bytes {
                inner
                    .lock()
                    .expect("shard map mutex poisoned")
                    .split_requests
                    .insert(shard);
            }
        }
    }
}

/// The leadership observer (spec §13): emit `LeaderChanged` once per (shard,
/// term) this node wins. Emitted by the winner itself — the node that will
/// actually serve the shard's grains — so the stream carries one event per
/// elected term without cross-node dedup. Exits when the map is dropped.
async fn leader_watch_loop<R: RaftConsensus>(
    consensus: R,
    grain_type: &'static str,
    inner: Weak<Mutex<Inner>>,
    emit: EmitEvent,
) {
    let self_node = consensus.node();
    let mut last: BTreeMap<u32, u64> = BTreeMap::new();
    loop {
        consensus.sleep(ALLOCATE_INTERVAL).await;
        let Some(inner) = inner.upgrade() else {
            return; // the map was dropped — stop the loop
        };
        let shards: Vec<u32> = {
            let guard = inner.lock().expect("shard map mutex poisoned");
            guard.allocation.keys().copied().collect()
        };
        for shard in shards {
            let group = shard_group_id(grain_type, shard);
            if consensus.group_is_leader(group)
                && let Some(term) = consensus.group_term(group)
                && last.get(&shard) != Some(&term)
            {
                last.insert(shard, term);
                emit(GrainEvent::LeaderChanged {
                    node: self_node,
                    grain_type,
                    shard,
                    term,
                });
            }
        }
    }
}
