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
//! [`LocalShardMap`] (Tier 1) and the [`RaftShardMap`] over a clustered system
//! (Tier 2). The allocation is static after bootstrap (rebalancing and split/merge
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
use crate::memory::MemoryGrainJournal;
use crate::shard::RaftGrainJournal;
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

/// One committed allocation command in the map group's log (spec §7.6).
#[derive(Serialize, Deserialize)]
enum ShardMapCommand {
    /// Shard `shard` is replicated by `replicas` — the only nodes that hold its
    /// data and can lead it (§7.1).
    Assign { shard: u32, replicas: Vec<NodeId> },
}

fn encode(command: &ShardMapCommand) -> Vec<u8> {
    serde_json::to_vec(command).expect("a ShardMapCommand always serializes")
}

fn decode(bytes: &[u8]) -> Option<ShardMapCommand> {
    serde_json::from_slice(bytes).ok()
}

// --- Tier 1: the single node replicates everything ---------------------------

/// The single-node shard map (Tier 1): this node is the sole replica of every
/// shard, each backed by an independent in-memory store.
pub(crate) struct LocalShardMap {
    node: NodeId,
    journals: Vec<Arc<dyn DynGrainJournal>>,
}

impl LocalShardMap {
    pub(crate) fn new(node: NodeId, shards: usize) -> LocalShardMap {
        let journals = (0..shards)
            .map(|_| Arc::new(MemoryGrainJournal::new()) as Arc<dyn DynGrainJournal>)
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

// --- Tier 2: the consensus-agreed map over a Raft group ----------------------

/// The committed allocation and the local journals built from it. Shared between
/// the apply loop (writer) and the [`ShardMapSource`] reads.
#[derive(Default)]
struct Inner {
    /// shard index → replica set, as committed.
    allocation: BTreeMap<u32, Vec<NodeId>>,
    /// shard index → local store, for shards this node replicates.
    journals: BTreeMap<u32, Arc<dyn DynGrainJournal>>,
}

/// A [`ShardMapSource`] backed by a per-type Raft group (Tier 2). The group's
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
    ) -> RaftShardMap {
        let group = map_group_id_for(grain_type);
        let self_node = consensus.node();
        // Tier 2 rides Raft: the map group and every shard group elect through the
        // system's consensus engine. A clustered system with no configured voters
        // has no engine at all (only `MembershipMode::Leader` builds one) — so no
        // group would ever elect, the gateway's redirect would hint this node back
        // at itself, and every grain call would loop on `NotLeader`. Fail loud at
        // host-construction (`granary()`), not silently at the first call. Tier 1
        // never reaches here: `LocalSystem` serves a `LocalShardMap` instead.
        assert!(
            !consensus.configured_voters().is_empty(),
            "granary Tier-2 requires leader-based consensus, but the system reports \
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

        consensus.launch(Box::pin(apply_loop(
            consensus.clone(),
            grain_type,
            self_node,
            commits,
            Arc::clone(&inner),
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

        RaftShardMap { inner }
    }
}

impl ShardMapSource for RaftShardMap {
    fn replicas(&self, shard: u32) -> Option<Vec<NodeId>> {
        self.inner.lock().expect("shard map mutex poisoned").allocation.get(&shard).cloned()
    }

    fn journal(&self, shard: u32) -> Option<Arc<dyn DynGrainJournal>> {
        self.inner.lock().expect("shard map mutex poisoned").journals.get(&shard).cloned()
    }
}

/// Apply the map group's committed allocation records (spec §7.6, §7.7): record
/// each shard's **latest** replica set and reconcile this node's local store with
/// it. The first `Assign` is the founding allocation; later `Assign`s are
/// rebalances (membership changed). On each change this node:
/// - **becomes a replica** (in new, not old): builds the shard's store. Subscribe
///   before create (`RaftGrainJournal::new` registers the commit sink, then
///   `create_group`), so the projection sees the shard log from the start. The
///   group is created over the **old** replica set so a node newly joining the
///   shard is a non-member that does not disrupt the shard's election — the shard
///   leader's reconcile loop then `AddVoter`s it and it catches up via replication.
///   (On the founding `Assign` there is no old set, so it is created over the
///   founding replicas, of which this node is one.)
/// - **stops being a replica** (in old, not new): drops the store from the registry.
///   An active `Host` keeps its own `Arc`, so its in-flight append still completes
///   or fails cleanly as `NotLeader` when the shard leader removes it; the next call
///   re-activates the grain on a current replica.
///
/// Runs until the map group's commit stream closes.
async fn apply_loop<R: RaftConsensus>(
    consensus: R,
    grain_type: &'static str,
    self_node: NodeId,
    commits: Receiver<Committed>,
    inner: Arc<Mutex<Inner>>,
) {
    while let Ok(observation) = commits.recv().await {
        // The map group is tiny and never compacts, so it only ever applies
        // commands; a snapshot install (were one to occur) carries no allocation
        // this loop can act on and is ignored.
        let Committed::Apply { command: bytes, .. } = observation else {
            continue;
        };
        let Some(ShardMapCommand::Assign { shard, replicas }) = decode(&bytes) else {
            continue; // a command this map cannot parse is defensively ignored
        };
        // Record the latest allocation; capture the prior set to diff against.
        let old = {
            let mut guard = inner.lock().expect("shard map mutex poisoned");
            guard.allocation.insert(shard, replicas.clone())
        };
        if old.as_ref() == Some(&replicas) {
            continue; // a re-proposed identical allocation — nothing changed
        }
        let was_replica = old.as_ref().is_some_and(|o| o.contains(&self_node));
        let now_replica = replicas.contains(&self_node);
        let shard_group = group_id_for(ShardId {
            grain_type,
            index: shard,
        });
        if now_replica && !was_replica {
            // Newly a replica: subscribe, then create the group over the *old* set
            // (a non-member join — no election disruption); the shard leader's
            // reconcile loop adds this node, which then catches up.
            let journal: Arc<dyn DynGrainJournal> =
                Arc::new(RaftGrainJournal::new(consensus.clone(), shard_group));
            let create_voters = old.unwrap_or_else(|| replicas.clone());
            consensus.create_group(shard_group, create_voters, Vec::new());
            inner
                .lock()
                .expect("shard map mutex poisoned")
                .journals
                .insert(shard, journal);
        } else if was_replica && !now_replica {
            // No longer a replica: drop the store from the registry (the shard
            // leader removes this node from the group via reconfigure).
            inner.lock().expect("shard map mutex poisoned").journals.remove(&shard);
        }
        // (Still a replica with a changed peer set: the shard leader's reconcile
        // loop drives the membership; this node's store is unchanged.)
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
            let desired = select_replicas(
                &voters,
                group_id_for(ShardId {
                    grain_type,
                    index: shard,
                }),
                replicas,
            )
            .0;
            // Re-propose only when the desired set differs from what is committed —
            // the steady state proposes nothing.
            let committed = inner.lock().expect("shard map mutex poisoned").allocation.get(&shard).cloned();
            if committed.as_ref() != Some(&desired) {
                consensus
                    .propose_to(group, encode(&ShardMapCommand::Assign { shard, replicas: desired }))
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
        // Each shard group this node leads → its committed allocation.
        let allocation: Vec<(u32, Vec<NodeId>)> = {
            let guard = inner.lock().expect("shard map mutex poisoned");
            guard.allocation.iter().map(|(&s, v)| (s, v.clone())).collect()
        };
        for (shard, replicas) in allocation {
            let shard_group = group_id_for(ShardId {
                grain_type,
                index: shard,
            });
            if consensus.group_is_leader(shard_group) {
                consensus.reconfigure_group(shard_group, replicas);
            }
        }
    }
}
