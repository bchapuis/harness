//! The Replicator: per-entity durability (spec §7.2, §7.4, §8).
//!
//! An entity's records are made durable not by a shared log but by a **per-entity
//! quorum append** (§7.2): the shard leader assigns each record the next `Seq` (free,
//! since it is the single writer) and the Replicator fans it to the shard's replicas,
//! reporting it durable once a quorum has stored it, fenced by the shard term (§8).
//! On activation a fresh leader **recovers** each entity's head from a write quorum by
//! read-repair (highest-term record per slot, written back under its own term), so
//! no acknowledged write is lost across a leadership change (**E14**).
//!
//! This is the clustered `Quorum` tier's durability, and only that: a
//! [`QuorumReplicator`] per shard, over a [`LeaderElection`] group and the
//! [`ReplicaTransport`] to the shard's replicas. The single-node `Local` tier needs
//! none of it (one store, no term, no quorum), so it is written out in full as
//! [`LocalEntityJournal`](crate::memory::LocalEntityJournal) rather than as a second
//! replicator here. Both rest on the [`EntityStore`] seam for per-node durability.

use std::sync::Arc;
use std::time::Duration;

use actor_cluster::GroupId;

use actor_cluster::RaftConsensus;
use actor_core::NodeId;
use futures::StreamExt;
use futures::future::join_all;
use futures::stream::FuturesUnordered;

use crate::blobs::BlobId;
use crate::blocking::BlockingIo;
use crate::blocking::on_store;
use crate::election::LeaderElection;
use crate::entity::EntityName;
use crate::journal::AppendOutcome;
use crate::journal::EntityJournal;
use crate::journal::EntityJournalError;
use crate::journal::RecordPage;
use crate::journal::Seq;
use crate::journal::Term;
use crate::replica_store::ReplicaTransport;
use crate::store::BlobAck;
use crate::store::EntityStore;
use crate::store::ReadOutcome;
use crate::store::StoreAck;
use crate::store::WriteKind;

/// A pending per-replica store ack from the [`ReplicaTransport`] fan-out, tagged
/// with the replica it came from so a joint quorum can attribute it to the right
/// set(s) during a replica-set migration (§7.7).
type StoreAckFuture =
    actor_core::BoxFuture<'static, (NodeId, Result<StoreAck, actor_core::CallError>)>;

/// How a blob-area copy treats an unreachable destination replica: the one
/// difference between the migration and split-transfer paths, kept as a named
/// policy because it is load-bearing: the migration barrier must not proceed
/// past a target missing its blobs (E17), while a transfer's skipped replica
/// heals via recovery-on-access (E18).
#[derive(Clone, Copy)]
enum OnUnreachable {
    Fail,
    Skip,
}

/// This node's own store outcome as one more replica's ack, so a quorum is counted
/// uniformly over all R replicas (the leader is a replica, §5.2).
///
/// Already resolved, because the local store completed the write (fsync included)
/// before it returned. It is still shaped as a future so the leader's own ack and its
/// peers' asks collect through one code path; the peers are the ones with a wait in
/// them (**E14**).
fn local_ack(node: NodeId, ack: StoreAck) -> StoreAckFuture {
    Box::pin(std::future::ready((node, Ok(ack))))
}

/// A handle on this node's own in-flight store call, held by the caller so it can
/// wait for the write it already handed to the quorum. See [`local_store_ack`].
type LocalWrite = futures::future::Shared<actor_core::BoxFuture<'static, StoreAck>>;

/// This node's own store as one more replica's ack: **started with** the peer
/// fan-out rather than ahead of it, so a commit costs `max(local, RTT)` instead of
/// `local + RTT`. Noise at the median; the whole of the local flush at the tail.
///
/// Two handles onto one write, and both are needed. The [`StoreAckFuture`] goes into
/// the replica set, where the local ack counts toward the quorum the moment it lands,
/// exactly as a peer's does: the set is polled in insertion order, so pushing this
/// last puts the peer asks on the wire before the flush is submitted. The
/// [`LocalWrite`] stays with the caller, which **must** await it before returning.
///
/// That second obligation is not belt-and-braces. [`BlockingIo`] promises threads and
/// explicitly *not* order (see its trait doc), and `ThreadPoolIo` runs several workers
/// off one queue, so two store calls for the same entity that are in flight together
/// may execute in either order. Today nothing exercises that, because every caller
/// awaits its store call before issuing the next one: the serialized flush this
/// change removes is exactly what has been enforcing an entity's write order. Awaiting
/// here keeps that property (one outstanding store call per entity) while still
/// overlapping the flush with the peers, which is the whole point: the ordering
/// guarantee costs a wait only when the peers beat the disk, and never a round trip.
fn local_store_ack(
    node: NodeId,
    io: &Arc<dyn BlockingIo>,
    store: &Arc<dyn crate::store::EntityStore>,
    call: impl FnOnce(&dyn crate::store::EntityStore) -> StoreAck + Send + 'static,
) -> (StoreAckFuture, LocalWrite) {
    let io = Arc::clone(io);
    let store = Arc::clone(store);
    // `offload` submits on first poll, not on construction, so the job reaches the
    // pool when the quorum's stream first polls this: after the peer asks.
    let write: actor_core::BoxFuture<'static, StoreAck> =
        Box::pin(async move { on_store(&io, &store, call).await });
    let write = futures::FutureExt::shared(write);
    let ack: StoreAckFuture = {
        let write = write.clone();
        Box::pin(async move { (node, Ok(write.await)) })
    };
    (ack, write)
}

/// A pending per-replica blob store from the [`ReplicaTransport`] blob fan-out: it
/// resolves once that peer has stored the blob or reported it could not. Tagged with
/// the replica for joint-quorum attribution (§7.7).
type BlobAckFuture =
    actor_core::BoxFuture<'static, (NodeId, Result<BlobAck, actor_core::CallError>)>;

/// The result of [`merge`]: the contiguous record prefix, its head, the best
/// snapshot `(seq, term, state)`, and whether any kept record's term is below the
/// recovering leader's term (so a write-back is needed).
type Merged = (Vec<Vec<u8>>, Seq, Option<(Seq, Term, Vec<u8>)>, bool);

/// The deadlines a replicator applies, from [`EntityConfig`](crate::EntityConfig).
///
/// Carried as a pair rather than read from the config at each use so the two are
/// visibly a policy the deployment sets, not constants the code assumes. They were
/// compile-time constants; the right values depend on the deployment's network and
/// storage, and setting them too low is worse than slow: every timeout here is
/// ambiguous (§7.2), so it steps the activation down and forces a rehydration.
///
/// Deliberately without a `Default`: the defaults live once, in
/// [`EntityConfig`](crate::EntityConfig), and a second set here would be a second
/// place to edit one tuning number, which is how such a pair silently drifts apart.
/// Every construction comes from a config.
#[derive(Clone, Copy, Debug)]
pub struct Deadlines {
    /// A quorum append, snapshot, or blob put (§11).
    pub quorum: Duration,
    /// A recovery read quorum, before the read-your-leader fallback (§7.5, §8).
    pub recover: Duration,
}

// --- Quorum tier: per-entity quorum append over the shard's replicas ----------

/// A shard's replica sets (§7.6, §7.7): the committed `current` set, and (while a
/// replica-set migration is in flight) the committed `target` set. Shared between
/// the shard map's apply loop (the writer, updating it as `Assign`/`Migrated`
/// commit) and the shard's [`QuorumReplicator`] (the reader).
///
/// While `target` is present every write and recovery uses a **joint quorum** (a
/// majority of `current` AND a majority of `target`), so no committed record's
/// durability ever rests on a set that lacks it: old-set quorums still intersect
/// every pre-migration write, new-set quorums intersect every in-migration write,
/// and the flip to `target`-only happens only after the migration driver has
/// caught every entity up on the target set (§7.7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReplicaSets {
    pub(crate) current: Vec<NodeId>,
    pub(crate) target: Option<Vec<NodeId>>,
}

/// The live control state one shard's apply loop shares with its replicator
/// (§7.6, §7.7): the replica sets (the quorum domain), the key range the shard
/// currently owns, and (while a split or merge is sealing the moving range)
/// the frozen-from bound. One mutex, read per operation, written only by the
/// shard map's apply loop and split/merge driver.
pub(crate) struct ShardControl {
    /// The committed `current`/`target` replica sets (§7.7).
    pub(crate) sets: ReplicaSets,
    /// The key range this shard owns (§5.1): shrinks on a committed split,
    /// extends on a committed merge. An append outside it is refused
    /// `NotLeader` before any store attempt: the leader-local half of E15.
    pub(crate) range: crate::system::KeyRange,
    /// The in-flight split/merge seal (§7.7): refuse appends at or above this
    /// hash. A fast path only: the authoritative barrier is the replica
    /// stores' durable append bound, which refuses at any term.
    pub(crate) frozen_from: Option<u64>,
}

impl ShardControl {
    pub(crate) fn new(sets: ReplicaSets, range: crate::system::KeyRange) -> ShardControl {
        ShardControl {
            sets,
            range,
            frozen_from: None,
        }
    }

    /// Whether this shard currently accepts appends for an entity at `hash`.
    fn accepts(&self, hash: u64) -> bool {
        self.range.contains(hash) && self.frozen_from.is_none_or(|from| hash < from)
    }
}

impl ReplicaSets {
    pub(crate) fn new(current: Vec<NodeId>) -> ReplicaSets {
        ReplicaSets {
            current,
            target: None,
        }
    }

    /// Every node that must receive the fan-out: [`joint_members`].
    pub(crate) fn union(&self) -> Vec<NodeId> {
        joint_members(&self.current, self.target.as_deref())
    }
}

/// The nodes involved in a shard right now: `current ∪ target`, deduplicated,
/// order-preserving; the one definition of the mid-migration joint membership
/// (§7.7), shared by the replicator's fan-out and the shard map's reconcile loop
/// so the two cannot drift.
pub(crate) fn joint_members(current: &[NodeId], target: Option<&[NodeId]>) -> Vec<NodeId> {
    let mut nodes = current.to_vec();
    if let Some(target) = target {
        for node in target {
            if !nodes.contains(node) {
                nodes.push(*node);
            }
        }
    }
    nodes
}

/// A majority of `n` replicas (§7.2).
fn majority(n: usize) -> usize {
    n / 2 + 1
}

/// Per-set ack counting toward a joint quorum (§7.7): an ack from a node counts
/// toward every set that contains it.
struct JointCount<'a> {
    sets: &'a ReplicaSets,
    current: usize,
    target: usize,
}

impl<'a> JointCount<'a> {
    fn new(sets: &'a ReplicaSets) -> JointCount<'a> {
        JointCount {
            sets,
            current: 0,
            target: 0,
        }
    }

    fn ack(&mut self, node: NodeId) {
        if self.sets.current.contains(&node) {
            self.current += 1;
        }
        if let Some(target) = &self.sets.target
            && target.contains(&node)
        {
            self.target += 1;
        }
    }

    fn satisfied(&self) -> bool {
        self.current >= majority(self.sets.current.len())
            && self
                .sets
                .target
                .as_ref()
                .is_none_or(|target| self.target >= majority(target.len()))
    }
}

/// The outcome of a recovery read phase (§7.2, §8): the fenced replies that survived
/// the read fan-out, this node's local head taken before the reply was merged, and
/// whether the acks reached a joint read quorum.
struct ReadQuorum {
    replies: Vec<crate::store::ReadReply>,
    local_head: Seq,
    confirmed: bool,
}

/// The clustered `Quorum` replicator (spec §7.2, §7.4, §8). Holds the shard's
/// leader-election group (for the term and leadership gate), this node's local
/// [`EntityStore`] (the leader is one of the replicas, §5.2), and the
/// [`ReplicaTransport`] to the other replicas.
pub(crate) struct QuorumReplicator<R: RaftConsensus> {
    election: LeaderElection<R>,
    local: Arc<dyn EntityStore>,
    transport: Arc<dyn ReplicaTransport>,
    /// The shard's live control state (§7.1, §7.7): the replica sets (the
    /// write/recovery quorum domain), owned key range, and split/merge freeze. The
    /// shard map's apply loop updates it in place as commands commit, so quorums always
    /// count over the committed allocation, never a snapshot from construction time.
    control: Arc<std::sync::Mutex<ShardControl>>,
    shard: u32,
    self_node: NodeId,
    /// Where this node's own replica writes run (§7.4). The leader is a replica, so
    /// every committed write fsyncs here; inline that fsync blocks the async worker
    /// driving this node's heartbeats (see [`crate::blocking`]).
    io: Arc<dyn BlockingIo>,
    deadlines: Deadlines,
}

impl<R: RaftConsensus> QuorumReplicator<R> {
    /// Build one shard's replicator over its leader-election group (spec §7.3,
    /// §7.4): the group supplies placement (who may write, under which term)
    /// holding no entity data (§7.1, §8); this replicator supplies durability.
    /// `control` is the shard's **live** control state (replica sets, owned
    /// range, split freeze) shared with the shard map's apply loop so a
    /// committed reallocation or split reaches in-flight journals (§7.7). The
    /// returned handle is the shard's `EntityJournal` (the `Quorum` tier) and
    /// the migration driver's replicator in one.
    #[allow(clippy::too_many_arguments)] // one call site, from the shard map
    pub(crate) fn for_shard(
        consensus: R,
        group: GroupId,
        shard: u32,
        control: Arc<std::sync::Mutex<ShardControl>>,
        local: Arc<dyn EntityStore>,
        transport: Arc<dyn ReplicaTransport>,
        io: Arc<dyn BlockingIo>,
        deadlines: Deadlines,
    ) -> Arc<QuorumReplicator<R>> {
        let self_node = consensus.node();
        Arc::new(QuorumReplicator {
            election: LeaderElection::new(consensus, group),
            local,
            transport,
            control,
            shard,
            self_node,
            io,
            deadlines,
        })
    }

    /// A point-in-time snapshot of the replica sets: one fan-out uses one snapshot,
    /// so its ack counting is coherent even if the allocation commits mid-flight
    /// (the next operation picks up the new sets).
    fn sets(&self) -> ReplicaSets {
        self.control
            .lock()
            .expect("shard control poisoned")
            .sets
            .clone()
    }

    /// The target set of an in-flight migration, if any (§7.7).
    pub(crate) fn migration_target(&self) -> Option<Vec<NodeId>> {
        self.control
            .lock()
            .expect("shard control poisoned")
            .sets
            .target
            .clone()
    }

    fn not_leader(&self) -> AppendOutcome {
        AppendOutcome::NotLeader(self.election.leader_hint())
    }

    /// The term this node leads the shard under right now, or `None` when it
    /// does not lead: the journal seam's leadership token
    /// ([`EntityJournal::term`](crate::journal::EntityJournal::term)).
    pub(crate) fn leadership_term(&self) -> Option<Term> {
        self.election.term().filter(|_| self.election.is_leader())
    }

    /// The fan-out peers of `sets` other than this node (the leader writes its own
    /// store locally, §5.2): `current ∪ target` during a migration.
    fn peers_of(&self, sets: &ReplicaSets) -> Vec<NodeId> {
        sets.union()
            .into_iter()
            .filter(|&n| n != self.self_node)
            .collect()
    }

    /// Per-entity quorum append (spec §7.2): stamp the shard term, write the local
    /// replica, fan out to the peers, and commit once a quorum has stored. A
    /// `Fenced` reply means a higher term exists (we are deposed) → `NotLeader`; a
    /// missed quorum within the timeout → `Unavailable` (§11). The record's identity
    /// is its `(entity, Seq)` slot, so a timed-out append that lands later is applied
    /// once on recovery with no dedup token (§7.2).
    pub(crate) async fn append(
        &self,
        entity: &EntityName,
        after: Seq,
        events: Vec<Vec<u8>>,
    ) -> AppendOutcome {
        let Some(term) = self.leadership_term() else {
            return self.not_leader();
        };
        // The split/merge gate (§7.7, E15): an append for a key this shard no longer
        // owns, or that is frozen mid-move, is refused BEFORE any store attempt, so it
        // provably never ran and the caller can re-resolve against the committed map.
        // The authoritative barrier is the replica stores' durable append bound.
        let sets = {
            let control = self.control.lock().expect("shard control poisoned");
            let hash = crate::system::name_hash(entity.entity_type(), entity.key());
            if !control.accepts(hash) {
                return self.not_leader();
            }
            control.sets.clone()
        };
        let events_len = events.len();
        // Fan out to the remote peers first, each cloning the payload for its own wire
        // message; then write this node's own replica, *moving* the payload in (§5.2).
        // The batch is deep-cloned R-1 times, never R.
        let mut replicas: Vec<StoreAckFuture> = self.fan(self.peers_of(&sets), |node| {
            self.transport.store_record(
                node,
                self.shard,
                entity.clone(),
                after,
                term,
                events.clone(),
                WriteKind::Append,
                self.deadlines.quorum,
            )
        });
        // The local write joins the fan-out as one more replica's ack (the leader is a
        // replica, §5.2). All R acks are gathered the same way, so this node counts
        // toward the quorum exactly when its own bytes are stable.
        //
        // Offloaded, not inline: this is an fsync, and running it on the async worker
        // would stall that worker's Raft heartbeats and every other shard's quorum wait
        // (see [`crate::blocking`]). Handed to the quorum *unresolved* rather than
        // awaited first: the peer asks are lazy futures, so awaiting the flush here
        // would keep every `StoreRecord` off the wire until this node's disk answered,
        // making a commit cost `local + RTT`. `local_store_ack` gives back a handle so
        // the write is still awaited before this returns (which is what preserves an
        // entity's write order), but the peers travel while it runs.
        let (name, shard) = (entity.clone(), self.shard);
        let (local, local_write) =
            local_store_ack(self.self_node, &self.io, &self.local, move |store| {
                store.store_record(shard, &name, after, term, events, WriteKind::Append)
            });
        replicas.push(local);
        let (outcome, pending) = self.collect_store_quorum(&sets, replicas).await;
        // Before anything is decided on the outcome, and in particular before the
        // rollback below touches the same slot: this node's own write must have landed
        // (see `local_store_ack`). Free on the failure path, where the quorum loop has
        // already drained every future including this one.
        local_write.await;
        if matches!(outcome, QuorumOutcome::Committed) {
            // Committed on a quorum: return now and drain the slower replicas off the
            // hot path (§7.2), so the append's latency is the quorum's, not the slowest
            // replica's.
            self.drain(pending);
        } else {
            // The append did not commit: roll back this node's tentative local write
            // so a later stale-local recovery never folds an uncommitted record
            // (§7.2, E5). Peers that stored it keep it, so a quorum can still commit
            // it late (the ambiguous-timeout case, §7.2). `pending` is already drained.
            // Bounded by our own term: while the quorum wait was in flight a newer
            // leader may have fenced this store and landed committed records above
            // `after`; those carry a higher term and must survive (E14).
            self.local.truncate(self.shard, entity, after, term);
        }
        match outcome {
            QuorumOutcome::Committed => {
                AppendOutcome::Committed(Seq::new(after.value() + events_len as u64))
            }
            QuorumOutcome::Fenced => self.not_leader(),
            // A stale head: an up-to-date replica rejected the append (§8). Step down
            // (ambiguous) and re-recover from a quorum on the next activation.
            QuorumOutcome::Stale => AppendOutcome::Unavailable("stale head; reactivating".into()),
            // A replica's append bound refused the moved range (§7.7): this
            // leader's map is behind a committed split. AMBIGUOUS, not
            // `NotLeader`: unbounded replicas may hold the record and the
            // split's transfer can adopt it, so an auto-retry against the child
            // could double-apply. `Unavailable` puts the outcome under the
            // caller's §2.2 idempotence discipline, like any quorum timeout.
            QuorumOutcome::Sealed => {
                AppendOutcome::Unavailable("shard sealed for a split/merge".into())
            }
            QuorumOutcome::Unavailable => {
                AppendOutcome::Unavailable("append did not reach a write quorum".into())
            }
        }
    }

    /// Recover an entity's head from a write quorum by read-repair (spec §8, **E14**):
    /// the rehydration barrier. Fence-read a quorum
    /// (a Paxos prepare that bars a deposed leader from committing after we read),
    /// take the highest-term record per slot, write the recovered tail back under our
    /// own term so the adopted head is itself quorum-durable, and leave the records
    /// and snapshot in the local store so subsequent `load`/`load_snapshot` read
    /// locally. Returns the recovered head, or `Unavailable` while the shard is
    /// electing or a quorum is unreachable (the failover window, §8.3).
    ///
    /// Short of a read quorum this falls back to the local view (§7.5,
    /// read-your-leader): acceptable for serving reads, never for a decision
    /// that moves data; those paths use [`recover_quorum`](Self::recover_quorum).
    pub(crate) async fn recover(&self, entity: &EntityName) -> Result<Seq, EntityJournalError> {
        self.recover_with(entity, false).await
    }

    /// [`recover`](Self::recover) that REQUIRES the read quorum: `Err` instead of
    /// the local fallback. The migration and split drivers (§7.7) use this: a
    /// transfer or `Migrated`/`SplitCommitted` proposal must never be based on a
    /// possibly-stale local view (E14/E15), and during a migration the confirmed
    /// joint quorum is also what makes the write-back run at all.
    pub(crate) async fn recover_quorum(
        &self,
        entity: &EntityName,
    ) -> Result<Seq, EntityJournalError> {
        self.recover_with(entity, true).await
    }

    async fn recover_with(
        &self,
        entity: &EntityName,
        require_quorum: bool,
    ) -> Result<Seq, EntityJournalError> {
        let Some(term) = self.leadership_term() else {
            return Err(EntityJournalError::Unavailable("shard electing".into()));
        };
        let sets = self.sets();

        let read = self.fence_read(entity, term, &sets).await?;
        // A joint read quorum during a migration (§7.7): every pre-migration commit
        // sits on a majority of `current`, every in-migration commit additionally on
        // a majority of `target`, so requiring both majorities intersects them all.
        if require_quorum && !read.confirmed {
            return Err(EntityJournalError::Unavailable(
                "recovery did not reach a read quorum".into(),
            ));
        }

        // Merge: highest-term record per slot, and the best snapshot. When a quorum
        // was reached this is the authoritative head; otherwise it is just this node's
        // local view: a read-your-leader fallback (§7.5) that may be stale but cannot
        // fork a write, since a write from it still needs a quorum and a stale-head
        // append is rejected by an up-to-date replica's optimistic check (§8).
        //
        // One read anomaly beyond staleness is possible in this fallback: a crash
        // after a failed append's local write but before its rollback truncate can
        // leave an uncommitted record in the local store; a quorum-less recovery
        // adopts it into the served state until the next quorum recovery drops it.
        // The record was never acknowledged, so no durability claim is violated (the
        // relaxed-read window of §7.5).
        let (records, head, snapshot, any_below) = merge(read.replies, term);
        // The recovered head's compacted base: the seq of the best snapshot, which
        // the recovered tail records sit above (§9).
        let base = snapshot.as_ref().map_or(Seq::ZERO, |(s, _, _)| *s);

        // Cache the recovered snapshot locally first, so the local store's base is
        // aligned to `base` before the write-back lands the tail above it. Records
        // remain the authority, so the snapshot need not be quorum-durable here.
        if let Some((at, snap_term, state)) = snapshot {
            // The ack is deliberately dropped, and this is the one place on the
            // recovery path where that is sound: the snapshot is a local cache and the
            // merged *records* remain the authority (§9), so a refusal here costs a
            // longer replay on the next activation rather than a durability claim.
            // Every other store call on this path reads its outcome (**E14**).
            let _cached = self.local.store_snapshot(
                self.shard,
                entity,
                at,
                snap_term.max(term),
                state,
                WriteKind::Repair,
            );
        }

        if read.confirmed {
            self.write_back(
                entity,
                term,
                &sets,
                base,
                head,
                &records,
                any_below,
                read.local_head,
            )
            .await?;
        }

        Ok(head)
    }

    /// Recovery read phase (§7.2, §8): fence-read the local store and every peer,
    /// awaiting all reads so no in-flight ask is dropped (no-silent-loss, §14).
    /// Each read is bounded by `self.deadlines.recover`, so an unreachable peer just falls
    /// out of the quorum. `Err` when the local store or a peer has fenced us behind a
    /// higher term.
    async fn fence_read(
        &self,
        entity: &EntityName,
        term: Term,
        sets: &ReplicaSets,
    ) -> Result<ReadQuorum, EntityJournalError> {
        // The promise is made under the entity's segment lock, before `prepare`
        // returns, so it fences a concurrent append either way. The *view* is another
        // matter and must not be released before it is stable: `write_back` below
        // skips the quorum re-write when nothing changed, so a reply naming a record
        // this replica has applied but not yet persisted could be merged into the
        // adopted head and then never re-written anywhere; a crash here would lose a
        // record already reported committed (**E14**).
        // Offloaded: `prepare` rewrites the shard's fence file whenever the term
        // advances, so on every failover this is a durable write on the recovery path.
        let (name, shard) = (entity.clone(), self.shard);
        let local = on_store(&self.io, &self.local, move |store| {
            store.prepare(shard, &name, term)
        })
        .await;
        // Naming both terms, because the interesting failure is not the ordinary one.
        // A higher term from a *peer* is a deposed leader learning it lost an election,
        // and the next activation on the winner recovers. A higher term in our **own**
        // store is the pathological one (the local fence outran this shard's group, so
        // no leader of it can ever read again), and only the two numbers tell them
        // apart.
        let local_reply = match local {
            ReadOutcome::Prepared(reply) => reply,
            ReadOutcome::Fenced(fence) => {
                return Err(EntityJournalError::Unavailable(format!(
                    "local store fenced shard {} at {fence:?}, above our {term:?}",
                    self.shard
                )));
            }
            // Our own storage is unusable, so this node cannot lead the shard at all:
            // it can neither read the entity's records nor durably promise the term
            // recovery depends on. Refusing here is what keeps a leader on a dead
            // disk from serving; the shard elects around it.
            ReadOutcome::Failed => {
                return Err(EntityJournalError::Unavailable(format!(
                    "local store for shard {} is unusable",
                    self.shard
                )));
            }
        };
        let peer_nodes = self.peers_of(sets);
        let peer_reads = peer_nodes.iter().map(|&node| {
            self.transport.read_entity(
                node,
                self.shard,
                entity.clone(),
                term,
                self.deadlines.recover,
            )
        });
        // Take our local head before moving the reply into the quorum set, so the
        // write-back below can skip the network on a stable re-activation without a
        // second read and without deep-cloning the entity's records.
        let local_head = local_reply.head();
        let mut count = JointCount::new(sets);
        count.ack(self.self_node);
        let mut replies = vec![local_reply];
        for (node, result) in peer_nodes.iter().copied().zip(join_all(peer_reads).await) {
            match result {
                Ok(ReadOutcome::Prepared(reply)) => {
                    count.ack(node);
                    replies.push(reply);
                }
                // A peer promised a higher term: we are deposed, do not serve.
                Ok(ReadOutcome::Fenced(_)) => {
                    return Err(EntityJournalError::Unavailable(
                        "fenced by a higher term".into(),
                    ));
                }
                // A peer whose storage failed promised nothing and holds nothing we
                // can believe, so it is not part of the read quorum: the same
                // treatment as one we could not reach at all.
                Ok(ReadOutcome::Failed) | Err(_) => {}
            }
        }
        Ok(ReadQuorum {
            replies,
            local_head,
            confirmed: count.satisfied(),
        })
    }

    /// Recovery write-back phase (§8): make the recovered tail quorum-durable under
    /// our term, so no later recovery regresses it and the local store can serve
    /// `load`. The tail sits after `base`; a replica already compacted past it skips
    /// the covered records (§8). Skips the network (returning `Ok` without a write)
    /// when nothing changed (a stable re-activation: no record below our term, head
    /// not advanced), except during a migration, when the write-back is exactly how a
    /// target replica receives the entity's records (§7.7), so it always runs.
    #[allow(clippy::too_many_arguments)] // mirrors the raft.rs handler signatures
    async fn write_back(
        &self,
        entity: &EntityName,
        term: Term,
        sets: &ReplicaSets,
        base: Seq,
        head: Seq,
        records: &[Vec<u8>],
        any_below: bool,
        local_head: Seq,
    ) -> Result<(), EntityJournalError> {
        let migrating = sets.target.is_some();
        if head.value() <= base.value()
            || !(any_below || local_head.value() < head.value() || migrating)
        {
            return Ok(());
        }
        let (name, shard, repaired) = (entity.clone(), self.shard, records.to_vec());
        let local = on_store(&self.io, &self.local, move |store| {
            store.store_record(shard, &name, base, term, repaired, WriteKind::Repair)
        })
        .await;
        let mut replicas: Vec<StoreAckFuture> = self.fan(self.peers_of(sets), |node| {
            self.transport.store_record(
                node,
                self.shard,
                entity.clone(),
                base,
                term,
                records.to_vec(),
                WriteKind::Repair,
                self.deadlines.recover,
            )
        });
        replicas.push(local_ack(self.self_node, local));
        let (outcome, pending) = self.collect_store_quorum(sets, replicas).await;
        match outcome {
            QuorumOutcome::Committed => {
                self.drain(pending);
                Ok(())
            }
            QuorumOutcome::Fenced => Err(EntityJournalError::Unavailable(
                "fenced by a higher term".into(),
            )),
            // `Sealed` cannot occur on a `Repair` (the bound refuses only appends);
            // folded with the quorum-miss arm for completeness.
            QuorumOutcome::Stale | QuorumOutcome::Sealed | QuorumOutcome::Unavailable => {
                Err(EntityJournalError::Unavailable(
                    "recovery write-back did not reach a quorum".into(),
                ))
            }
        }
    }

    /// Records from the **local** store, not from a quorum: the quorum read on the
    /// activation path is `head`'s recovery (§8), which runs once and backfills
    /// whatever this replica was missing. So a rehydration's replay, however long,
    /// costs no round trips: it is local reads over records this node already holds.
    /// That is why §9's snapshot cadence can afford to be sparse
    /// (`docs/hardware-envelope.md` §3.1).
    pub(crate) async fn load(
        &self,
        entity: &EntityName,
        from: Seq,
        limit: usize,
    ) -> Result<RecordPage, EntityJournalError> {
        Ok(self.local.read_from(self.shard, entity, from, limit))
    }

    pub(crate) async fn load_snapshot(
        &self,
        entity: &EntityName,
    ) -> Result<Option<(Seq, Vec<u8>)>, EntityJournalError> {
        Ok(self.local.snapshot(self.shard, entity))
    }

    // --- The entity-native content-addressed blob store (clustered) ----------------
    //
    // An entity's immutable blobs ride the *same* shard replica set as its records, with
    // no term and no order. The leader always keeps a local copy, so a `get` is a
    // local, verified read in steady state; a fresh leader after a migration that
    // lacks a block faults it from a peer and backfills locally (lazy hydration).

    /// Store an immutable blob on a write quorum of the entity's replicas, always
    /// including this local replica (so subsequent reads are local). No term, no
    /// leadership gate: an orphan blob from a deposed writer is harmless (content-
    /// addressed) and reclaimed by the entity's sweep. Returns `Unavailable` if a
    /// quorum is unreachable, so the caller learns the bytes are not yet durable.
    ///
    /// **It puts the bytes unconditionally, and an offer round here would not pay.**
    /// The obvious refinement (`has` the id on each peer first, then put only where
    /// it is absent) was proposed and withdrawn, and is recorded here because the
    /// shape of this function is what invites it again. Two reasons it loses:
    ///
    /// - It catches nothing new. The facets above already filter what they have seen,
    ///   and the case an offer round *would* catch (a chunk two different entities
    ///   produced) is unreachable, because an entity's blob area is per-entity all the
    ///   way down (`FileEntityStore::segment_id` keys on `(shard, entity)`, and
    ///   `has_blob` with it). The round can only ask about *this* entity, so
    ///   centralizing the question here would not create cross-entity dedup.
    /// - It costs a round on exactly the path that cannot afford one. On a create
    ///   every chunk is cold, so every put would pay the probe and then put anyway;
    ///   with the puts pipelined into waves, a put is one round per wave and an offer
    ///   round makes it two, doubling what the pipelining had just halved.
    ///
    /// Reopen it only alongside a **shared** (not per-entity) blob namespace, where the
    /// question would have an answer worth a round trip.
    pub(crate) async fn put_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
        bytes: Vec<u8>,
    ) -> Result<(), EntityJournalError> {
        let sets = self.sets();
        // Local copy first (the leader is a replica, §5.2): move the bytes into peers'
        // wire messages by clone, but the local write needs no copy beyond the fan-out.
        let pending: FuturesUnordered<BlobAckFuture> = self
            .fan(self.peers_of(&sets), |node| {
                self.transport.store_blob(
                    node,
                    self.shard,
                    entity.clone(),
                    id,
                    bytes.clone(),
                    self.deadlines.quorum,
                )
            })
            .into_iter()
            .collect();
        // Awaited *before* the peers are polled, rather than joining the fan-out as
        // one more ack the way the record path does: **E18** requires a blob's quorum
        // to always include the leader, because that is what makes a later `get` a
        // local read (§7.10 colocation). That requirement is also why a failed local
        // write ends the put here, no number of peer copies can substitute for the
        // one E18 names, and the caller must not hear success without it.
        let (name, shard) = (entity.clone(), self.shard);
        let stored = on_store(&self.io, &self.local, move |store| {
            store.put_blob(shard, &name, id, bytes)
        })
        .await;
        if stored == BlobAck::Failed {
            self.drain(pending);
            return Err(EntityJournalError::Unavailable(
                "local store could not persist the blob".into(),
            ));
        }
        // A blob has no fence or order, so any peer that reports it stored counts.
        let (satisfied, pending) = self
            .accumulate_quorum(&sets, true, pending, |result| {
                matches!(result, Ok(BlobAck::Stored))
            })
            .await;
        if satisfied {
            self.drain(pending);
            Ok(())
        } else {
            Err(EntityJournalError::Unavailable(
                "blob did not reach a write quorum".into(),
            ))
        }
    }

    /// Fetch a verified blob (B1): the local copy if present and verifying, else the
    /// first peer that returns verifying bytes (rank order), backfilled locally for
    /// the next read. `None` if no replica holds it; `Unavailable` if a copy was
    /// found but none verified (corruption on every reachable replica).
    pub(crate) async fn get_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
    ) -> Result<Option<Vec<u8>>, EntityJournalError> {
        let sets = self.sets();
        let mut corrupt = false;
        if let Some(bytes) = self.local.get_blob(self.shard, entity, id) {
            if id.verifies(&bytes) {
                return Ok(Some(bytes));
            }
            // The local copy exists but is corrupt (on-disk bit-rot). Evict it so the
            // peer-sourced backfill below can replace it in place: a content-addressed
            // `put_blob` of an id already on disk writes nothing, so without this the
            // bad copy would persist, forcing a network fetch on every future read and
            // leaving this replica's durability margin permanently one short (§7.10
            // self-heal). Safe unconditionally: a copy that fails verification can
            // never be returned.
            corrupt = true;
            let (name, shard) = (entity.clone(), self.shard);
            on_store(&self.io, &self.local, move |store| {
                store.delete_blob(shard, &name, id)
            })
            .await;
        }
        for node in self.peers_of(&sets) {
            match self
                .transport
                .fetch_blob(node, self.shard, entity.clone(), id, self.deadlines.quorum)
                .await
            {
                Ok(Some(bytes)) if id.verifies(&bytes) => {
                    // Backfill locally so the next read is local (lazy hydration), and
                    // repair a corrupt local copy evicted above (self-heal).
                    let (name, shard, copy) = (entity.clone(), self.shard, bytes.clone());
                    on_store(&self.io, &self.local, move |store| {
                        store.put_blob(shard, &name, id, copy)
                    })
                    .await;
                    return Ok(Some(bytes));
                }
                Ok(Some(_)) => corrupt = true,
                Ok(None) | Err(_) => {}
            }
        }
        if corrupt {
            Err(EntityJournalError::Unavailable(format!(
                "blob {id} failed verification on every reachable replica"
            )))
        } else {
            Ok(None)
        }
    }

    /// Whether any reachable replica holds the blob: short-circuit on the first holder
    /// (the local copy, else a peer), not a quorum count; a `true` says a `get` can
    /// source the bytes, not that they are quorum-durable (that is `put_blob`'s job).
    pub(crate) async fn has_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
    ) -> Result<bool, EntityJournalError> {
        if self.local.has_blob(self.shard, entity, id) {
            return Ok(true);
        }
        for node in self.peers_of(&self.sets()) {
            if let Ok(true) = self
                .transport
                .has_blob(node, self.shard, entity.clone(), id, self.deadlines.quorum)
                .await
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Sweep the entity's blobs on every replica, keeping only `retain` (the
    /// mark-from-roots GC). Best-effort: a missed replica keeps its garbage until the
    /// next sweep, never a correctness issue.
    pub(crate) async fn retain_blobs(&self, entity: &EntityName, retain: Vec<BlobId>) {
        let (name, shard, keep) = (entity.clone(), self.shard, retain.clone());
        on_store(&self.io, &self.local, move |store| {
            store.retain_blobs(shard, &name, &keep.into_iter().collect())
        })
        .await;
        let sweeps = self.peers_of(&self.sets()).into_iter().map(|node| {
            self.transport.sweep_blobs(
                node,
                self.shard,
                entity.clone(),
                Some(retain.clone()),
                self.deadlines.quorum,
            )
        });
        let _ = join_all(sweeps).await;
    }

    /// Drop the entity's whole blob area on every replica (destroy). Best-effort.
    pub(crate) async fn delete_blobs(&self, entity: &EntityName) {
        let (name, shard) = (entity.clone(), self.shard);
        on_store(&self.io, &self.local, move |store| {
            store.delete_blobs(shard, &name)
        })
        .await;
        let sweeps = self.peers_of(&self.sets()).into_iter().map(|node| {
            self.transport.sweep_blobs(
                node,
                self.shard,
                entity.clone(),
                None,
                self.deadlines.quorum,
            )
        });
        let _ = join_all(sweeps).await;
    }

    /// Persist a snapshot on a quorum (spec §9), fenced by the shard term. Quorum-
    /// blocking so a later compaction can safely truncate the covered records.
    pub(crate) async fn save_snapshot(
        &self,
        entity: &EntityName,
        at: Seq,
        state: Vec<u8>,
    ) -> AppendOutcome {
        let Some(term) = self.leadership_term() else {
            return self.not_leader();
        };
        let sets = self.sets();
        // Clone the state for each remote peer's wire message, then move it into this
        // node's own replica write (§5.2): R-1 copies, not R.
        let mut replicas: Vec<StoreAckFuture> = self.fan(self.peers_of(&sets), |node| {
            self.transport.store_snapshot(
                node,
                self.shard,
                entity.clone(),
                at,
                term,
                state.clone(),
                WriteKind::Append,
                self.deadlines.quorum,
            )
        });
        // Offloaded for the same reason as the record write above, and overlapped with
        // the fan-out for the same reason, more so here: a snapshot is the largest
        // single thing this node fsyncs, so it is both the worst one to run on an async
        // worker and the one whose flush the peers would spend the longest waiting on.
        let (name, shard) = (entity.clone(), self.shard);
        let (local, local_write) =
            local_store_ack(self.self_node, &self.io, &self.local, move |store| {
                store.store_snapshot(shard, &name, at, term, state, WriteKind::Append)
            });
        replicas.push(local);
        let (outcome, pending) = self.collect_store_quorum(&sets, replicas).await;
        // This node's own write has landed before the outcome is acted on, as in
        // `append`: one outstanding store call per entity (see `local_store_ack`).
        local_write.await;
        match outcome {
            QuorumOutcome::Committed => {
                self.drain(pending);
                AppendOutcome::Committed(at)
            }
            QuorumOutcome::Fenced => self.not_leader(),
            QuorumOutcome::Stale | QuorumOutcome::Sealed | QuorumOutcome::Unavailable => {
                AppendOutcome::Unavailable("snapshot did not reach a quorum".into())
            }
        }
    }

    // --- Replica-set migration (§7.7) -----------------------------------------
    //
    // The shard's migration driver (a leader-only loop in `shardmap`) uses these to
    // catch every entity up on the `target` set before the map flips to it. All are
    // idempotent, so a crashed or deposed driver simply re-drives.

    /// Enumerate the shard's entities from a read quorum of its replicas: the union
    /// of every reachable replica's local list, valid once the replies cover a
    /// majority of `current` (a committed record lives on a majority of `current`,
    /// so any such union misses no committed entity). `Err` while short of that.
    pub(crate) async fn migration_entities(&self) -> Result<Vec<EntityName>, EntityJournalError> {
        let sets = self.sets();
        let mut count = JointCount::new(&sets);
        count.ack(self.self_node);
        let mut names: std::collections::BTreeSet<EntityName> =
            self.local.entities(self.shard).into_iter().collect();
        let peer_nodes = self.peers_of(&sets);
        let lists = peer_nodes.iter().map(|&node| {
            self.transport
                .list_entities(node, self.shard, self.deadlines.quorum)
        });
        for (node, result) in peer_nodes.iter().copied().zip(join_all(lists).await) {
            if let Ok(list) = result {
                count.ack(node);
                names.extend(list);
            }
        }
        // Enumeration needs only a majority of `current` (the pre-migration commit
        // domain); target members contribute names but are not required.
        if count.current >= majority(sets.current.len()) {
            Ok(names.into_iter().collect())
        } else {
            Err(EntityJournalError::Unavailable(
                "entity enumeration did not reach a read quorum".into(),
            ))
        }
    }

    /// Catch one entity up on the target set (§7.7): recover its head (the joint
    /// write-back lands the records on the target replicas), then re-persist its
    /// best snapshot on the joint quorum (a compacted entity's prefix exists only in
    /// the snapshot, so the records alone are not enough), then copy its blob area.
    ///
    /// Uses the quorum-required `recover_quorum`, never the read-your-leader
    /// `recover`: the write-back that lands the records on the target replicas runs
    /// only when the joint read quorum is confirmed, and `recover`'s local fallback
    /// returns `Ok` without it. A pass built on that fallback could complete (the
    /// targets still answer the blob listing), and the driver would propose
    /// `Migrated` for a set that never received the records, so a later loss of the
    /// departed replicas would lose committed writes (E14). Short of the quorum the
    /// pass fails instead, `target` stays, and the driver re-drives it next tick.
    pub(crate) async fn migrate_entity(
        &self,
        entity: &EntityName,
    ) -> Result<(), EntityJournalError> {
        self.recover_quorum(entity).await?;
        if let Some((at, state)) = self.local.snapshot(self.shard, entity)
            && let AppendOutcome::NotLeader(_) | AppendOutcome::Unavailable(_) =
                self.save_snapshot(entity, at, state).await
        {
            return Err(EntityJournalError::Unavailable(
                "snapshot did not reach the joint quorum".into(),
            ));
        }
        self.migrate_blobs(entity).await
    }

    /// Copy an entity's blob area to every target replica that lacks any of it: the
    /// source list is the union of the current replicas' ids, each blob fetched
    /// verified (through [`get_blob`](Self::get_blob)'s local-then-peers path) and
    /// stored on the lacking peers. Idempotent (content-addressed).
    async fn migrate_blobs(&self, entity: &EntityName) -> Result<(), EntityJournalError> {
        let sets = self.sets();
        let Some(target) = sets.target.clone() else {
            return Ok(());
        };
        // Source: this replica plus the reachable *current* peers. Strict on an
        // unreachable target: the flip must not proceed past a target missing
        // its blobs (E17), so the failure is the migration's, not healed later.
        let current_peers: Vec<NodeId> = sets
            .current
            .iter()
            .copied()
            .filter(|&n| n != self.self_node)
            .collect();
        self.copy_blob_area(
            entity,
            self.shard,
            target.into_iter().filter(|&n| n != self.self_node),
            current_peers,
            OnUnreachable::Fail,
        )
        .await
    }

    // --- Shard split/merge transfer (§7.7) -------------------------------------
    //
    // The split (and merge) driver (a leader-only loop in `shardmap`) uses these
    // to move a key range's entities to their destination shard's keys before the
    // partition change commits. All idempotent: a crashed or deposed driver
    // re-drives, and re-copied slots agree (the source is a quorum recovery, E14).

    /// Durably tighten the append bound on a majority of this shard's replicas:
    /// refuse every future append at or above `from`, at ANY term (E15). The
    /// driver's first step; only after this returns may the transfer read the
    /// committed prefix, because from here on no append to the moved range can
    /// assemble a write quorum: a majority of acks would have to include a
    /// bounded store. Idempotent and monotone.
    pub(crate) async fn seal_shard(&self, from: u64) -> Result<(), EntityJournalError> {
        let sets = self.sets();
        // Durable before this node counts itself toward the barrier: the bound is a
        // promise, and a majority that included an unpersisted one could be forgotten
        // by a restart, letting a stale leader assemble a quorum for the moved range
        // afterward (**E15**).
        let shard = self.shard;
        on_store(&self.io, &self.local, move |store| {
            store.seal_range(shard, from)
        })
        .await;
        let mut count = JointCount::new(&sets);
        count.ack(self.self_node);
        // Return as soon as a majority has sealed, never block on a dead replica
        // for the full timeout. A split's dest inherits the parent's replicas, so
        // one may be down; the seal barrier is a majority, not unanimity (E15).
        let mut pending: FuturesUnordered<_> = self
            .fan(self.peers_of(&sets), |node| {
                self.transport
                    .seal_range(node, self.shard, from, self.deadlines.quorum)
            })
            .into_iter()
            .collect();
        while !count.satisfied()
            && let Some((node, result)) = pending.next().await
        {
            if result.is_ok() {
                count.ack(node);
            }
        }
        if count.satisfied() {
            self.drain(pending);
            Ok(())
        } else {
            Err(EntityJournalError::Unavailable(
                "seal did not reach a quorum".into(),
            ))
        }
    }

    /// Land one moved entity's committed prefix (snapshot, then records, then
    /// blobs) under `dest` shard keys on `dest_replicas` (§7.7). The source is
    /// a quorum recovery under our own term (fencing deposed leaders of this
    /// shard), after which the local store holds the authoritative prefix; the
    /// copy is a [`WriteKind::Transfer`] at `Term::ZERO`, majority-acked on
    /// `dest_replicas` for records and snapshot, every-replica for blobs
    /// (mirroring the migration copy's strictness). Snapshot before records so
    /// the destination segment's base aligns (as `recover`'s own write-back
    /// does).
    pub(crate) async fn transfer_entity(
        &self,
        entity: &EntityName,
        dest: u32,
        dest_replicas: &[NodeId],
    ) -> Result<(), EntityJournalError> {
        self.recover_quorum(entity).await?;
        let reply = self.local.read(self.shard, entity);
        let head = reply.head();
        let base = reply.snapshot.as_ref().map_or(Seq::ZERO, |(s, _, _)| *s);
        // The committed prefix: the snapshot plus the contiguous records above
        // it, up to the recovered head, never the uncommitted tail beyond it.
        let records: Vec<Vec<u8>> = reply
            .slots
            .iter()
            .filter(|(seq, _, _)| seq.value() > base.value() && seq.value() <= head.value())
            .map(|(_, _, bytes)| bytes.clone())
            .collect();
        if let Some((at, _, state)) = reply.snapshot {
            // Spelled as an `if` rather than `bool::then` because the write is
            // offloaded and so has to be awaited, which a synchronous closure cannot
            // do. The condition and the `Option` it produces are unchanged.
            let local = if dest_replicas.contains(&self.self_node) {
                let (name, copy) = (entity.clone(), state.clone());
                Some(
                    on_store(&self.io, &self.local, move |store| {
                        store.store_snapshot(dest, &name, at, Term::ZERO, copy, WriteKind::Transfer)
                    })
                    .await,
                )
            } else {
                None
            };
            let peers = self.fan(dest_replicas.iter().copied(), |node| {
                self.transport.store_snapshot(
                    node,
                    dest,
                    entity.clone(),
                    at,
                    Term::ZERO,
                    state.clone(),
                    WriteKind::Transfer,
                    self.deadlines.quorum,
                )
            });
            if !self
                .transfer_to_majority(dest_replicas.len(), local, peers)
                .await
            {
                return Err(EntityJournalError::Unavailable(
                    "transfer snapshot did not reach a majority of the destination".into(),
                ));
            }
        }
        if !records.is_empty() {
            let local = if dest_replicas.contains(&self.self_node) {
                let (name, copy) = (entity.clone(), records.clone());
                Some(
                    on_store(&self.io, &self.local, move |store| {
                        store.store_record(dest, &name, base, Term::ZERO, copy, WriteKind::Transfer)
                    })
                    .await,
                )
            } else {
                None
            };
            let peers = self.fan(dest_replicas.iter().copied(), |node| {
                self.transport.store_record(
                    node,
                    dest,
                    entity.clone(),
                    base,
                    Term::ZERO,
                    records.clone(),
                    WriteKind::Transfer,
                    self.deadlines.quorum,
                )
            });
            if !self
                .transfer_to_majority(dest_replicas.len(), local, peers)
                .await
            {
                return Err(EntityJournalError::Unavailable(
                    "transfer records did not reach a majority of the destination".into(),
                ));
            }
        }
        self.transfer_blobs(entity, dest, dest_replicas).await
    }

    /// Await `Stored` acks from a majority of `total` destination replicas:
    /// the transfer copy's plain-majority accounting (the destination set is
    /// explicit, unlike the joint quorum over this shard's own sets). Stragglers
    /// of a satisfied majority drain off the hot path.
    async fn transfer_to_majority(
        &self,
        total: usize,
        local: Option<StoreAck>,
        peers: Vec<StoreAckFuture>,
    ) -> bool {
        let mut acked = 0usize;
        let need = majority(total);
        // As in `collect_store_quorum`, this node's copy counts through the same
        // stream as the peers': it is a destination replica like any other.
        let mut replicas = peers;
        if let Some(local) = local {
            replicas.push(local_ack(self.self_node, local));
        }
        let mut pending: FuturesUnordered<StoreAckFuture> = replicas.into_iter().collect();
        while acked < need {
            match pending.next().await {
                Some((_, Ok(StoreAck::Stored(_)))) => acked += 1,
                Some(_) => {}
                None => return false,
            }
        }
        self.drain(pending);
        true
    }

    /// Fan one per-node store out to `nodes`, skipping this node and tagging
    /// each ack with its node for the quorum count. The differing store call is
    /// supplied as `mk`; every fan in this file (append, repair, blob,
    /// snapshot, seal, transfer) is this shape around a different call.
    fn fan<T: Send + 'static>(
        &self,
        nodes: impl IntoIterator<Item = NodeId>,
        mk: impl Fn(NodeId) -> actor_core::BoxFuture<'static, T>,
    ) -> Vec<actor_core::BoxFuture<'static, (NodeId, T)>> {
        nodes
            .into_iter()
            .filter(|&n| n != self.self_node)
            .map(|node| {
                let ack = mk(node);
                Box::pin(async move { (node, ack.await) })
                    as actor_core::BoxFuture<'static, (NodeId, T)>
            })
            .collect()
    }

    /// Copy a moved entity's blob area to the reachable destination replicas'
    /// `dest`-keyed areas (§7.7, E17/E18): the shared copy behind
    /// [`copy_blob_area`], best-effort per destination node; an unreachable
    /// dest replica (a split's child inherits the parent's replicas, which may
    /// include a crashed one) is skipped rather than stalling the split; its
    /// copies heal via recovery-on-access when it returns. The committed
    /// records and snapshot already reached a majority
    /// (`transfer_to_majority`), and blobs reach every reachable dest,
    /// including this leader's own `dest`-keyed copy, so the child leader can
    /// always serve.
    async fn transfer_blobs(
        &self,
        entity: &EntityName,
        dest: u32,
        dest_replicas: &[NodeId],
    ) -> Result<(), EntityJournalError> {
        let sets = self.sets();
        self.copy_blob_area(
            entity,
            dest,
            dest_replicas.iter().copied(),
            self.peers_of(&sets),
            OnUnreachable::Skip,
        )
        .await
    }

    /// The one blob-area copy (§7.7): gather the source id set (this
    /// replica's list under this shard's keys plus every reachable
    /// `source_peers` list) then, per destination node, list what it holds
    /// under `dest` and ship each missing blob through a verified
    /// [`get_blob`] fetch (`None` means no replica holds it any more, an
    /// orphan swept mid-copy, safely skipped). A destination that is this
    /// node writes through the local store's blocking pool. Idempotent
    /// (content-addressed). `on_unreachable` is the migration-vs-transfer
    /// policy split, kept explicit because it is load-bearing (E17 vs E18).
    async fn copy_blob_area(
        &self,
        entity: &EntityName,
        dest: u32,
        dest_nodes: impl IntoIterator<Item = NodeId>,
        source_peers: Vec<NodeId>,
        on_unreachable: OnUnreachable,
    ) -> Result<(), EntityJournalError> {
        let mut ids: std::collections::BTreeSet<BlobId> = self
            .local
            .blob_ids(self.shard, entity)
            .into_iter()
            .collect();
        let lists = source_peers.iter().map(|&node| {
            self.transport
                .list_blobs(node, self.shard, entity.clone(), self.deadlines.quorum)
        });
        for list in join_all(lists).await.into_iter().flatten() {
            ids.extend(list);
        }
        for node in dest_nodes {
            let held: std::collections::BTreeSet<BlobId> = if node == self.self_node {
                self.local.blob_ids(dest, entity).into_iter().collect()
            } else {
                match self
                    .transport
                    .list_blobs(node, dest, entity.clone(), self.deadlines.quorum)
                    .await
                {
                    Ok(list) => list.into_iter().collect(),
                    Err(_) => match on_unreachable {
                        OnUnreachable::Fail => {
                            return Err(EntityJournalError::Unavailable(
                                "target replica unreachable".into(),
                            ));
                        }
                        OnUnreachable::Skip => continue,
                    },
                }
            };
            for &id in ids.difference(&held) {
                let Some(bytes) = self.get_blob(entity, id).await? else {
                    continue;
                };
                if node == self.self_node {
                    let name = entity.clone();
                    on_store(&self.io, &self.local, move |store| {
                        store.put_blob(dest, &name, id, bytes)
                    })
                    .await;
                } else if self
                    .transport
                    .store_blob(node, dest, entity.clone(), id, bytes, self.deadlines.quorum)
                    .await
                    .is_err()
                {
                    match on_unreachable {
                        OnUnreachable::Fail => {
                            return Err(EntityJournalError::Unavailable(
                                "blob copy to target failed".into(),
                            ));
                        }
                        // Lost the dest replica mid-copy: skip; the rest of
                        // its blobs heal on access. Records/snapshot durability
                        // is unaffected (they committed on a majority).
                        OnUnreachable::Skip => break,
                    }
                }
            }
        }
        Ok(())
    }

    /// Seed the local ack, then poll `pending` until a joint quorum has acked (spec
    /// §7.2), returning as soon as it is reached: the commit waits on the quorum, not
    /// the slowest replica. `is_ack` decides whether a peer's reply counts: a `Stored`
    /// [`StoreAck`] on the record path, an `Ok(())` on the blob path. During a
    /// migration the quorum is JOINT (§7.7): a majority of `current` AND of `target`.
    /// The unresolved stragglers come back for [`drain`](Self::drain), so each still
    /// closes its `AskIssued`/`AskOutcome` bracket off the hot path (no-silent-loss,
    /// §14). When the quorum is not reached the loop has drained every peer, so the
    /// returned set is empty.
    async fn accumulate_quorum<Reply>(
        &self,
        sets: &ReplicaSets,
        local_acked: bool,
        mut pending: FuturesUnordered<actor_core::BoxFuture<'static, (NodeId, Reply)>>,
        mut is_ack: impl FnMut(&Reply) -> bool,
    ) -> (
        bool,
        FuturesUnordered<actor_core::BoxFuture<'static, (NodeId, Reply)>>,
    ) {
        let mut count = JointCount::new(sets);
        if local_acked {
            count.ack(self.self_node);
        }
        if count.satisfied() {
            return (true, pending);
        }
        while let Some((node, reply)) = pending.next().await {
            if is_ack(&reply) {
                count.ack(node);
            }
            if count.satisfied() {
                return (true, pending);
            }
        }
        (false, pending)
    }

    /// Count every replica's ack toward a quorum (spec §7.2) on the record path, over
    /// [`accumulate_quorum`](Self::accumulate_quorum). A `Stored` counts; a
    /// `Fenced`/`Stale` reply does not but is remembered, so short of a quorum a single
    /// `Fenced` means we are deposed and a `Stale` means the head was stale, running
    /// out of replies with neither is `Unavailable`. A quorum that stored wins even if
    /// a lagging replica also reported a higher term: had a higher-term leader prepared
    /// a quorum, the intersection would have fenced this store (§8).
    async fn collect_store_quorum(
        &self,
        sets: &ReplicaSets,
        replicas: Vec<StoreAckFuture>,
    ) -> (QuorumOutcome, FuturesUnordered<StoreAckFuture>) {
        let mut fenced = false;
        let mut stale = false;
        let mut sealed = false;
        // Every replica including this node arrives through the stream (see
        // [`local_ack`]), so nothing is counted before the poll loop.
        let pending: FuturesUnordered<StoreAckFuture> = replicas.into_iter().collect();
        let (satisfied, pending) = self
            .accumulate_quorum(sets, false, pending, |reply| match reply {
                Ok(StoreAck::Stored(_)) => true,
                Ok(StoreAck::Fenced(_)) => {
                    fenced = true;
                    false
                }
                Ok(StoreAck::Stale(_)) => {
                    stale = true;
                    false
                }
                Ok(StoreAck::Sealed) => {
                    sealed = true;
                    false
                }
                // A replica whose storage failed is exactly an absent replica: it
                // did not store, and (unlike the refusals above) it decided
                // nothing, so it sets no flag and steers the outcome nowhere. If the
                // remaining replicas still make a quorum the write commits normally;
                // otherwise this falls through to `Unavailable`, which is the honest
                // report of a write whose fate the caller must not assume.
                Ok(StoreAck::Failed) => false,
                Err(_) => false,
            })
            .await;
        if satisfied {
            return (QuorumOutcome::Committed, pending);
        }
        let outcome = if fenced {
            QuorumOutcome::Fenced
        } else if sealed {
            QuorumOutcome::Sealed
        } else if stale {
            QuorumOutcome::Stale
        } else {
            QuorumOutcome::Unavailable
        };
        (outcome, pending)
    }

    /// Drive the leftover peer asks of a committed quorum (a record store or a blob
    /// store) to completion off the hot path (spec §7.2). Launched as a detached task,
    /// so the commit returns at quorum latency while every issued ask still closes its
    /// `AskIssued`/`AskOutcome` bracket (no-silent-loss, §14). A late `Stored` is
    /// harmless (the slot already holds the record); a late `Fenced` cannot un-commit a
    /// quorum-durable write (§8).
    fn drain<F>(&self, mut pending: FuturesUnordered<F>)
    where
        F: Future + Send + 'static,
    {
        if pending.is_empty() {
            return;
        }
        self.transport.launch(Box::pin(
            async move { while pending.next().await.is_some() {} },
        ));
    }
}

/// The outcome of a quorum store/append (spec §7.2, §8, §11, §7.7).
enum QuorumOutcome {
    Committed,
    Fenced,
    Stale,
    /// A replica's append bound refused the moved key range (§7.7): the shard is
    /// sealed for a split/merge this leader has not yet applied.
    Sealed,
    Unavailable,
}

/// Merge a quorum of recovery reads by **highest-term-per-slot** (spec §8): for each
/// `Seq` slot, keep the record carried under the highest term any replica holds.
/// Returns the contiguous record prefix (ascending bytes), its head, the best
/// snapshot, and whether any kept record's term is below `our_term` (so a write-back
/// under our term is needed). A gap ends the prefix: an uncommitted tail, dropped.
fn merge(replies: Vec<crate::store::ReadReply>, our_term: Term) -> Merged {
    use std::collections::BTreeMap;
    let mut best: BTreeMap<u64, (Term, Vec<u8>)> = BTreeMap::new();
    let mut snapshot: Option<(Seq, Term, Vec<u8>)> = None;
    // The replies are owned and used only here, so record and snapshot bytes are moved
    // into the merge, never cloned (recovery runs on every activation).
    for reply in replies {
        for (seq, term, bytes) in reply.slots {
            let slot = seq.value();
            match best.get(&slot) {
                Some((t, _)) if *t >= term => {}
                _ => {
                    best.insert(slot, (term, bytes));
                }
            }
        }
        if let Some((s, t, state)) = reply.snapshot {
            let better = match &snapshot {
                Some((cur_s, cur_t, _)) => (s.value(), t) > (cur_s.value(), *cur_t),
                None => true,
            };
            if better {
                snapshot = Some((s, t, state));
            }
        }
    }
    // The head base is the best snapshot's seq: records it subsumes were compacted
    // away on the replicas that hold it (§9), so the contiguous scan starts just
    // above it. A snapshot is only ever taken at a committed head, so using its seq
    // as the base can never drop a committed record (E14).
    let base = snapshot.as_ref().map_or(0, |(s, _, _)| s.value());
    // The longest contiguous run of records after the base.
    let mut records = Vec::new();
    let mut any_below = false;
    let mut expected = base + 1;
    while let Some((term, bytes)) = best.remove(&expected) {
        if term < our_term {
            any_below = true;
        }
        records.push(bytes);
        expected += 1;
    }
    let head = Seq::new(base + records.len() as u64);
    (records, head, snapshot, any_below)
}

/// The clustered `Quorum` journal (spec §7, §7.4): the shard's replicator
/// handle *is* the [`EntityJournal`], no layer relays between them, because
/// the composition (election + replicator) already happened in
/// `QuorumReplicator::for_shard`. An entity write is an independent per-entity
/// quorum append (§7.2), not an entry in a shared shard log, so a write-heavy
/// entity never queues behind its shard-mates. On `Arc` because a journal
/// clones per activation, sharing one replicator.
impl<R: RaftConsensus> EntityJournal for Arc<QuorumReplicator<R>> {
    async fn append(&self, entity: &EntityName, after: Seq, events: Vec<Vec<u8>>) -> AppendOutcome {
        (**self).append(entity, after, events).await
    }

    async fn load(
        &self,
        entity: &EntityName,
        from: Seq,
        limit: usize,
    ) -> Result<RecordPage, EntityJournalError> {
        (**self).load(entity, from, limit).await
    }

    async fn head(&self, entity: &EntityName) -> Result<Seq, EntityJournalError> {
        // On the `Quorum` tier `head` *is* the rehydration barrier (§8, §9): it
        // recovers the entity's head from a write quorum by read-repair, so a fresh
        // leader never folds onto a stale head.
        (**self).recover(entity).await
    }

    fn term(&self) -> Option<Term> {
        (**self).leadership_term()
    }

    async fn save_snapshot(&self, entity: &EntityName, at: Seq, state: Vec<u8>) -> AppendOutcome {
        (**self).save_snapshot(entity, at, state).await
    }

    async fn load_snapshot(
        &self,
        entity: &EntityName,
    ) -> Result<Option<(Seq, Vec<u8>)>, EntityJournalError> {
        (**self).load_snapshot(entity).await
    }

    async fn put_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
        bytes: Vec<u8>,
    ) -> Result<(), EntityJournalError> {
        (**self).put_blob(entity, id, bytes).await
    }

    async fn get_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
    ) -> Result<Option<Vec<u8>>, EntityJournalError> {
        (**self).get_blob(entity, id).await
    }

    async fn has_blob(&self, entity: &EntityName, id: BlobId) -> Result<bool, EntityJournalError> {
        (**self).has_blob(entity, id).await
    }

    async fn retain_blobs(&self, entity: &EntityName, retain: Vec<BlobId>) {
        (**self).retain_blobs(entity, retain).await
    }

    async fn delete_blobs(&self, entity: &EntityName) {
        (**self).delete_blobs(entity).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::FutureExt;

    use super::*;
    use crate::blocking::Job;
    use crate::store::EntityStore;
    use crate::testing::StaticEntityStore;

    /// A pool that accepts jobs and runs none of them until told, standing in for a
    /// device that has not answered yet. `InlineIo` cannot express that, because it
    /// runs the job inside `submit` and so can never be observed mid-flight.
    #[derive(Default)]
    struct DeferredIo {
        queued: Mutex<Vec<Job>>,
        submitted: std::sync::atomic::AtomicUsize,
    }

    impl DeferredIo {
        fn submitted(&self) -> usize {
            self.submitted.load(std::sync::atomic::Ordering::SeqCst)
        }

        /// Let the device answer.
        fn run_queued(&self) {
            let jobs: Vec<Job> = std::mem::take(&mut *self.queued.lock().unwrap());
            for job in jobs {
                job();
            }
        }
    }

    impl BlockingIo for DeferredIo {
        fn submit(&self, job: Job) -> bool {
            self.submitted
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.queued.lock().unwrap().push(job);
            true
        }
    }

    fn stored_store() -> Arc<dyn EntityStore> {
        Arc::new(StaticEntityStore::new(StoreAck::Stored(Seq::new(1))))
    }

    #[test]
    fn the_local_write_reaches_the_pool_when_the_quorum_polls_it_and_not_before() {
        // The property the overlap rests on. `append` builds the peer asks, then this,
        // then hands the whole set to `collect_store_quorum`, so if constructing the
        // local ack submitted the flush, the flush would again precede every peer ask
        // and the commit would be back to `local + RTT`.
        let io = Arc::new(DeferredIo::default());
        let store = stored_store();
        let (ack, write) = local_store_ack(
            NodeId::new(1),
            &(io.clone() as Arc<dyn BlockingIo>),
            &store,
            |store| {
                store.store_record(
                    0,
                    &EntityName::new("t", "k"),
                    Seq::new(0),
                    Term::new(1),
                    vec![vec![7]],
                    WriteKind::Append,
                )
            },
        );
        assert_eq!(
            io.submitted(),
            0,
            "building the local ack submitted the write: it would run before the peer \
             asks are on the wire, which is the serialization this exists to remove",
        );

        // The first poll is the quorum's, and it is what submits.
        assert!(
            write.clone().now_or_never().is_none(),
            "the write completed without the pool having run it",
        );
        assert_eq!(io.submitted(), 1, "the first poll must reach the pool");

        io.run_queued();
        assert_eq!(
            ack.now_or_never(),
            Some((NodeId::new(1), Ok(StoreAck::Stored(Seq::new(1))))),
            "the quorum's copy must see the store's answer",
        );
    }

    #[test]
    fn the_callers_handle_observes_the_same_write_the_quorum_counted() {
        // What keeps an entity's writes ordered. `BlockingIo` promises no ordering, so
        // `append` must not return while its own flush is still queued: the next
        // append for the entity would race it. It awaits this handle to prevent that,
        // which only works if the handle tracks the very write the quorum counted
        // rather than starting a second one.
        let io = Arc::new(DeferredIo::default());
        let store = stored_store();
        let (ack, write) = local_store_ack(
            NodeId::new(2),
            &(io.clone() as Arc<dyn BlockingIo>),
            &store,
            |store| {
                store.store_record(
                    0,
                    &EntityName::new("t", "k"),
                    Seq::new(0),
                    Term::new(1),
                    vec![vec![7]],
                    WriteKind::Append,
                )
            },
        );

        // Drive it the way the quorum does, then release the device.
        assert!(ack.now_or_never().is_none());
        assert_eq!(io.submitted(), 1);
        assert!(
            write.clone().now_or_never().is_none(),
            "the caller's handle must still be waiting on the same unfinished write",
        );
        io.run_queued();

        assert_eq!(
            io.submitted(),
            1,
            "the two handles started two writes: the caller would be awaiting one the \
             quorum never counted, and the entity would have two stores in flight",
        );
        assert_eq!(
            write.now_or_never(),
            Some(StoreAck::Stored(Seq::new(1))),
            "the caller's handle did not observe the completed write",
        );
    }

    // --- The migration barrier (§7.7, E14) ----------------------------------------

    /// A consensus that always leads: placement is not what these tests exercise.
    #[derive(Clone)]
    struct AlwaysLeading(NodeId);

    impl RaftConsensus for AlwaysLeading {
        fn node(&self) -> NodeId {
            self.0
        }
        fn cluster_voters(&self) -> Vec<NodeId> {
            Vec::new()
        }
        fn configured_voters(&self) -> Vec<NodeId> {
            Vec::new()
        }
        fn reconfigure_group(&self, _: GroupId, _: Vec<NodeId>) {}
        fn create_group(&self, _: GroupId, _: Vec<NodeId>, _: Vec<NodeId>) {}
        fn remove_group(&self, _: GroupId) {}
        fn subscribe_commits(
            &self,
            _: GroupId,
        ) -> async_channel::Receiver<actor_cluster::Committed> {
            async_channel::bounded(1).1
        }
        async fn propose_to(&self, _: GroupId, _: Vec<u8>) {}
        async fn transfer_group_leadership(&self, _: GroupId, _: NodeId) -> bool {
            false
        }
        fn group_is_leader(&self, _: GroupId) -> bool {
            true
        }
        fn group_term(&self, _: GroupId) -> Option<u64> {
            Some(3)
        }
        fn group_leader(&self, _: GroupId) -> Option<NodeId> {
            Some(self.0)
        }
        fn sleep(&self, _: Duration) -> actor_core::BoxFuture<'static, ()> {
            Box::pin(async {})
        }
        fn launch(&self, _: actor_core::BoxFuture<'static, ()>) {}
    }

    /// Peers whose stores are empty and answer everything at once: except the
    /// recovery read on the nodes in `unreadable`, which times out. The blob
    /// listing on those same nodes still answers: the shape of a target replica
    /// that was slow under `recover_timeout` yet reachable a moment later, the
    /// case a pass that tolerated an unconfirmed read quorum would sail through.
    struct PeersWithSlowReads {
        unreadable: Vec<NodeId>,
    }

    impl ReplicaTransport for PeersWithSlowReads {
        fn store_record(
            &self,
            _: NodeId,
            _: u32,
            _: EntityName,
            after: Seq,
            _: Term,
            records: Vec<Vec<u8>>,
            _: WriteKind,
            _: Duration,
        ) -> actor_core::BoxFuture<'static, Result<StoreAck, actor_core::CallError>> {
            let head = Seq::new(after.value() + records.len() as u64);
            Box::pin(async move { Ok(StoreAck::Stored(head)) })
        }
        fn read_entity(
            &self,
            node: NodeId,
            _: u32,
            _: EntityName,
            _: Term,
            _: Duration,
        ) -> actor_core::BoxFuture<'static, Result<ReadOutcome, actor_core::CallError>> {
            let answers = !self.unreadable.contains(&node);
            Box::pin(async move {
                if answers {
                    Ok(ReadOutcome::Prepared(crate::store::ReadReply::default()))
                } else {
                    Err(actor_core::CallError::Timeout)
                }
            })
        }
        fn store_snapshot(
            &self,
            _: NodeId,
            _: u32,
            _: EntityName,
            at: Seq,
            _: Term,
            _: Vec<u8>,
            _: WriteKind,
            _: Duration,
        ) -> actor_core::BoxFuture<'static, Result<StoreAck, actor_core::CallError>> {
            Box::pin(async move { Ok(StoreAck::Stored(at)) })
        }
        fn seal_range(
            &self,
            _: NodeId,
            _: u32,
            _: u64,
            _: Duration,
        ) -> actor_core::BoxFuture<'static, Result<(), actor_core::CallError>> {
            Box::pin(async { Ok(()) })
        }
        fn store_blob(
            &self,
            _: NodeId,
            _: u32,
            _: EntityName,
            _: BlobId,
            _: Vec<u8>,
            _: Duration,
        ) -> actor_core::BoxFuture<'static, Result<BlobAck, actor_core::CallError>> {
            Box::pin(async { Ok(BlobAck::Stored) })
        }
        fn fetch_blob(
            &self,
            _: NodeId,
            _: u32,
            _: EntityName,
            _: BlobId,
            _: Duration,
        ) -> actor_core::BoxFuture<'static, Result<Option<Vec<u8>>, actor_core::CallError>>
        {
            Box::pin(async { Ok(None) })
        }
        fn has_blob(
            &self,
            _: NodeId,
            _: u32,
            _: EntityName,
            _: BlobId,
            _: Duration,
        ) -> actor_core::BoxFuture<'static, Result<bool, actor_core::CallError>> {
            Box::pin(async { Ok(false) })
        }
        fn sweep_blobs(
            &self,
            _: NodeId,
            _: u32,
            _: EntityName,
            _: Option<Vec<BlobId>>,
            _: Duration,
        ) -> actor_core::BoxFuture<'static, Result<(), actor_core::CallError>> {
            Box::pin(async { Ok(()) })
        }
        fn list_entities(
            &self,
            _: NodeId,
            _: u32,
            _: Duration,
        ) -> actor_core::BoxFuture<'static, Result<Vec<EntityName>, actor_core::CallError>>
        {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn list_blobs(
            &self,
            _: NodeId,
            _: u32,
            _: EntityName,
            _: Duration,
        ) -> actor_core::BoxFuture<'static, Result<Vec<BlobId>, actor_core::CallError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn launch(&self, _: actor_core::BoxFuture<'static, ()>) {}
    }

    /// A migrating shard led by `a`: `current` {a, b, c}, `target` {a, d, e}.
    fn migrating_replicator(unreadable: Vec<NodeId>) -> Arc<QuorumReplicator<AlwaysLeading>> {
        let [a, b, c, d, e] = [1, 2, 3, 4, 5].map(NodeId::new);
        let mut sets = ReplicaSets::new(vec![a, b, c]);
        sets.target = Some(vec![a, d, e]);
        let control = Arc::new(Mutex::new(ShardControl::new(
            sets,
            crate::system::KeyRange {
                start: 0,
                end: u64::MAX,
            },
        )));
        QuorumReplicator::for_shard(
            AlwaysLeading(a),
            GroupId(7),
            0,
            control,
            stored_store(),
            Arc::new(PeersWithSlowReads { unreadable }),
            Arc::new(crate::blocking::InlineIo),
            Deadlines {
                quorum: Duration::from_secs(1),
                recover: Duration::from_secs(1),
            },
        )
    }

    #[test]
    fn a_migration_pass_short_of_the_joint_read_quorum_fails_instead_of_completing() {
        // §7.6 step 3 / E14: `Migrated` may be proposed only after a complete pass,
        // and a pass is complete only if the joint write-back ran, which it does
        // only under a confirmed joint read quorum. With the target majority (d, e)
        // not answering the recovery read, the read-your-leader fallback would
        // have returned `Ok`, the targets still answer the blob listing, and the
        // pass would have completed with the records on no target replica. The
        // pass must fail instead, so `migrate_loop` keeps `target` and re-drives.
        let [d, e] = [4, 5].map(NodeId::new);
        let replicator = migrating_replicator(vec![d, e]);
        let outcome = replicator
            .migrate_entity(&EntityName::new("t", "k"))
            .now_or_never()
            .expect("every peer answers immediately");
        assert!(
            matches!(outcome, Err(EntityJournalError::Unavailable(_))),
            "a pass whose joint read quorum was not confirmed completed: {outcome:?}",
        );

        // The same pass completes once the targets answer the read, so the failure
        // above is the quorum's and not the fixture's.
        let replicator = migrating_replicator(Vec::new());
        let outcome = replicator
            .migrate_entity(&EntityName::new("t", "k"))
            .now_or_never()
            .expect("every peer answers immediately");
        assert!(
            outcome.is_ok(),
            "a pass with the joint read quorum confirmed failed: {outcome:?}",
        );
    }
}
