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
//! (the `Quorum` tier). The allocation is static after bootstrap (rebalancing and split/merge
//! are deferred).

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

use crate::journal::DynGrainJournal;
use crate::memory::LocalGrainJournal;
use crate::replica_store::ReplicaTransport;
use crate::replicator::QuorumReplicator;
use crate::replicator::ReplicaSets;
use crate::shard::QuorumGrainJournal;
use crate::store::GrainStore;
use crate::system::MAP_SHARD_INDEX;
use crate::system::ShardId;
use crate::system::group_id_for;
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
    /// Shard `shard` is replicated by `replicas` — the only nodes that hold its
    /// data and can lead it (§7.1). The **founding** `Assign` sets the shard's
    /// replica set outright; a later `Assign` onto an already-allocated shard
    /// starts a **migration** toward `replicas` (the committed `target`, §7.7):
    /// writes and recoveries switch to the joint quorum, and the leader's
    /// migration driver catches every grain up on the target set.
    Assign { shard: u32, replicas: Vec<NodeId> },
    /// The migration for `shard` is complete — every grain's records, snapshot,
    /// and blobs are on the target set — so the target becomes the current set
    /// and the old-set members drop out (§7.7). Proposed only by the shard
    /// leader's migration driver, only after a successful catch-up pass.
    Migrated { shard: u32 },
}

/// One shard's committed allocation: the `current` replica set and, while a
/// migration is in flight, the committed `target` (§7.7).
#[derive(Clone)]
struct Allocation {
    current: Vec<NodeId>,
    target: Option<Vec<NodeId>>,
}

impl Allocation {
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
}

// --- the `Quorum` tier: the consensus-agreed map over a Raft group ----------------------

/// The committed allocation and the local journals built from it. Shared between
/// the apply loop (writer) and the [`ShardMapSource`] reads.
#[derive(Default)]
struct Inner {
    /// shard index → the committed allocation (`current`, plus the migration
    /// `target` while one is in flight, §7.7).
    allocation: BTreeMap<u32, Allocation>,
    /// shard index → local store, for shards this node replicates (or is a
    /// migration target of).
    journals: BTreeMap<u32, Arc<dyn DynGrainJournal>>,
}

/// The typed per-shard handles the loops share (the [`ShardMapSource`] seam is
/// object-safe, so these cannot live in [`Inner`]): each hosted shard's **live**
/// replica sets — the apply loop mutates them in place as `Assign`/`Migrated`
/// commit, and the shard's replicator reads them per operation (§7.7) — and its
/// replicator, the migration driver's handle.
type ShardHandles<R> =
    BTreeMap<u32, (Arc<std::sync::Mutex<ReplicaSets>>, Arc<QuorumReplicator<R>>)>;

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
    pub(crate) fn new<R: RaftConsensus>(
        consensus: R,
        grain_type: &'static str,
        shards: usize,
        replicas: usize,
        local: Arc<dyn GrainStore>,
        transport: Arc<dyn ReplicaTransport>,
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
            local,
            transport,
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
) {
    // Build this node's journal + live sets for `shard` and register them.
    let build = |shard: u32, sets: ReplicaSets| {
        let shard_group = shard_group_id(grain_type, shard);
        let sets = Arc::new(std::sync::Mutex::new(sets));
        let journal = QuorumGrainJournal::new(
            consensus.clone(),
            shard_group,
            shard,
            Arc::clone(&sets),
            Arc::clone(&local),
            Arc::clone(&transport),
        );
        handles
            .lock()
            .expect("shard handles mutex poisoned")
            .insert(shard, (sets, journal.replicator()));
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
            ShardMapCommand::Assign { shard, replicas } => {
                // Record the transition under the map lock; act on it below.
                enum Change {
                    /// The founding allocation for this shard.
                    Founding,
                    /// A migration toward `replicas` began; carries the current set.
                    Migration(Vec<NodeId>),
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
                                    current: replicas.clone(),
                                    target: None,
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
                            Change::Migration(alloc.current.clone())
                        }
                    }
                };
                let shard_group = shard_group_id(grain_type, shard);
                match change {
                    Change::Noop => {}
                    Change::Founding => {
                        if replicas.contains(&self_node) {
                            consensus.create_group(shard_group, replicas.clone(), Vec::new());
                            build(shard, ReplicaSets::new(replicas));
                        }
                    }
                    Change::Migration(current) => {
                        let involved =
                            current.contains(&self_node) || replicas.contains(&self_node);
                        let has_handles = handles
                            .lock()
                            .expect("shard handles mutex poisoned")
                            .contains_key(&shard);
                        if has_handles {
                            // A continuing participant: flip its live sets so every
                            // in-flight journal switches to the joint quorum (§7.7).
                            if let Some((sets, _)) = handles
                                .lock()
                                .expect("shard handles mutex poisoned")
                                .get(&shard)
                            {
                                *sets.lock().expect("replica sets poisoned") = ReplicaSets {
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
                    if let Some((sets, _)) = handles
                        .lock()
                        .expect("shard handles mutex poisoned")
                        .get(&shard)
                    {
                        *sets.lock().expect("replica sets poisoned") = ReplicaSets::new(current);
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
        for shard in 0..shards as u32 {
            let desired = select_replicas(&voters, shard_group_id(grain_type, shard), replicas).0;
            // Propose the founding allocation, or — when the desired set has
            // drifted from the committed one — start a migration toward it
            // (§7.7). Never re-propose while a migration is already in flight:
            // retargeting mid-migration would flip `current` to a set the driver
            // never caught up (the `Migrated` guard relies on this).
            let proposable = match inner
                .lock()
                .expect("shard map mutex poisoned")
                .allocation
                .get(&shard)
            {
                None => true,
                Some(alloc) => alloc.target.is_none() && alloc.current != desired,
            };
            if proposable {
                consensus
                    .propose_to(
                        group,
                        encode(&ShardMapCommand::Assign {
                            shard,
                            replicas: desired,
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
