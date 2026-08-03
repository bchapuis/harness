//! The Replicator: per-grain durability (spec §7.2, §7.4, §8).
//!
//! A grain's records are made durable not by a shared log but by a **per-grain
//! quorum append** (§7.2): the shard leader assigns each record the next `Seq` (free,
//! since it is the single writer) and the Replicator fans it to the shard's replicas,
//! reporting it durable once a quorum has stored it, fenced by the shard term (§8).
//! On activation a fresh leader **recovers** each grain's head from a write quorum by
//! read-repair — highest-term record per slot, written back under its own term — so
//! no acknowledged write is lost across a leadership change (**G14**).
//!
//! Two tiers (§7.4): [`LocalReplicator`] is one local store, no term, no quorum — the
//! single-node `Local` journal; [`QuorumReplicator`] is the clustered `Quorum` path
//! over a [`LeaderElection`] group and the [`ReplicaTransport`] to the shard's
//! replicas. Both rest on the [`GrainStore`] seam for per-node durability.

use std::sync::Arc;
use std::time::Duration;

use actor_cluster::RaftConsensus;
use actor_core::NodeId;
use futures::StreamExt;
use futures::future::join_all;
use futures::stream::FuturesUnordered;

use crate::blobs::BlobId;
use crate::blocking::BlockingIo;
use crate::blocking::on_store;
use crate::election::LeaderElection;
use crate::grain::GrainName;
use crate::journal::AppendOutcome;
use crate::journal::GrainJournalError;
use crate::journal::Seq;
use crate::journal::Term;
use crate::replica_store::ReplicaTransport;
use crate::store::BlobAck;
use crate::store::GrainStore;
use crate::store::ReadOutcome;
use crate::store::Reserved;
use crate::store::StoreAck;
use crate::store::WriteKind;

/// A pending per-replica store ack from the [`ReplicaTransport`] fan-out, tagged
/// with the replica it came from so a joint quorum can attribute it to the right
/// set(s) during a replica-set migration (§7.7).
type StoreAckFuture =
    actor_core::BoxFuture<'static, (NodeId, Result<StoreAck, actor_core::CallError>)>;

/// This node's own store outcome as one more replica's ack, so a quorum is counted
/// uniformly over all R replicas (the leader is a replica, §5.2).
///
/// Already resolved, because the local store completed the write — fsync included —
/// before it returned the [`Reserved`]. It is still shaped as a future so the leader's
/// own ack and its peers' asks collect through one code path; the peers are the ones
/// with a wait in them (**G14**).
fn local_ack(node: NodeId, reserved: Reserved<StoreAck>) -> StoreAckFuture {
    Box::pin(std::future::ready((node, Ok(reserved.durable()))))
}

/// A handle on this node's own in-flight store call, held by the caller so it can
/// wait for the write it already handed to the quorum. See [`local_store_ack`].
type LocalWrite = futures::future::Shared<actor_core::BoxFuture<'static, StoreAck>>;

/// This node's own store as one more replica's ack — **started with** the peer
/// fan-out rather than ahead of it, so a commit costs `max(local, RTT)` instead of
/// `local + RTT`. Noise at the median; the whole of the local flush at the tail.
///
/// Two handles onto one write, and both are needed. The [`StoreAckFuture`] goes into
/// the replica set, where the local ack counts toward the quorum the moment it lands,
/// exactly as a peer's does — the set is polled in insertion order, so pushing this
/// last puts the peer asks on the wire before the flush is submitted. The
/// [`LocalWrite`] stays with the caller, which **must** await it before returning.
///
/// That second obligation is not belt-and-braces. [`BlockingIo`] promises threads and
/// explicitly *not* order (see its trait doc), and `ThreadPoolIo` runs several workers
/// off one queue, so two store calls for the same grain that are in flight together
/// may execute in either order. Today nothing exercises that, because every caller
/// awaits its store call before issuing the next one — the serialized flush this
/// change removes is exactly what has been enforcing a grain's write order. Awaiting
/// here keeps that property (one outstanding store call per grain) while still
/// overlapping the flush with the peers, which is the whole point: the ordering
/// guarantee costs a wait only when the peers beat the disk, and never a round trip.
fn local_store_ack(
    node: NodeId,
    io: &Arc<dyn BlockingIo>,
    store: &Arc<dyn crate::store::GrainStore>,
    call: impl FnOnce(&dyn crate::store::GrainStore) -> Reserved<StoreAck> + Send + 'static,
) -> (StoreAckFuture, LocalWrite) {
    let io = Arc::clone(io);
    let store = Arc::clone(store);
    // `offload` submits on first poll, not on construction, so the job reaches the
    // pool when the quorum's stream first polls this — after the peer asks.
    let write: actor_core::BoxFuture<'static, StoreAck> =
        Box::pin(async move { on_store(&io, &store, call).await.durable() });
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

/// The deadlines a replicator applies, from [`GranaryConfig`](crate::GranaryConfig).
///
/// Carried as a pair rather than read from the config at each use so the two are
/// visibly a policy the deployment sets, not constants the code assumes. They were
/// compile-time constants; the right values depend on the deployment's network and
/// storage, and setting them too low is worse than slow — every timeout here is
/// ambiguous (§7.2), so it steps the activation down and forces a rehydration.
///
/// Deliberately without a `Default`: the defaults live once, in
/// [`GranaryConfig`](crate::GranaryConfig), and a second set here would be a second
/// place to edit one tuning number — which is how such a pair silently drifts apart.
/// Every construction comes from a config.
#[derive(Clone, Copy, Debug)]
pub struct Deadlines {
    /// A quorum append, snapshot, or blob put (§11).
    pub quorum: Duration,
    /// A recovery read quorum, before the read-your-leader fallback (§7.5, §8).
    pub recover: Duration,
}

// --- Local tier: one node, one store -----------------------------------------

/// The single-node `Local` replicator (spec §7.4): one [`GrainStore`], no term, no
/// quorum. An append commits on the local store; recovery is a local head read.
///
/// It mirrors [`QuorumReplicator`]'s shape so both journal tiers wrap a replicator
/// behind the same seam.
pub(crate) struct LocalReplicator {
    store: Arc<dyn GrainStore>,
    shard: u32,
    /// Where the store's blocking writes run (§7.4). On this tier the local fsync is
    /// the commit, so it is the entire cost of an append.
    io: Arc<dyn BlockingIo>,
}

impl LocalReplicator {
    pub(crate) fn new(
        store: Arc<dyn GrainStore>,
        shard: u32,
        io: Arc<dyn BlockingIo>,
    ) -> LocalReplicator {
        LocalReplicator { store, shard, io }
    }

    pub(crate) async fn append(
        &self,
        grain: &GrainName,
        after: Seq,
        events: Vec<Vec<u8>>,
    ) -> AppendOutcome {
        // A single writer at term 0 is never fenced or stale (its fence stays 0 and
        // `after` always equals the head behind the input gate, §6). On this tier the
        // local fsync IS the commit (§7.4), so the await is the durability the
        // `Committed` outcome asserts.
        let (name, shard) = (grain.clone(), self.shard);
        let stored = on_store(&self.io, &self.store, move |store| {
            store.store_record(shard, &name, after, Term::ZERO, events, WriteKind::Append)
        })
        .await;
        match stored.durable() {
            StoreAck::Stored(head) => AppendOutcome::Committed(head),
            other => {
                AppendOutcome::Unavailable(format!("local store rejected the append: {other:?}"))
            }
        }
    }

    pub(crate) async fn head(&self, grain: &GrainName) -> Result<Seq, GrainJournalError> {
        Ok(self.store.head(self.shard, grain).durable())
    }

    pub(crate) async fn load(
        &self,
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<(Seq, Vec<u8>)>, GrainJournalError> {
        Ok(self
            .store
            .read_from(self.shard, grain, from, limit)
            .durable())
    }

    pub(crate) async fn save_snapshot(
        &self,
        grain: &GrainName,
        at: Seq,
        state: Vec<u8>,
    ) -> AppendOutcome {
        let (name, shard) = (grain.clone(), self.shard);
        let stored = on_store(&self.io, &self.store, move |store| {
            store.store_snapshot(shard, &name, at, Term::ZERO, state, WriteKind::Append)
        })
        .await;
        match stored.durable() {
            StoreAck::Stored(seq) => AppendOutcome::Committed(seq),
            other => {
                AppendOutcome::Unavailable(format!("local store rejected the snapshot: {other:?}"))
            }
        }
    }

    pub(crate) async fn load_snapshot(
        &self,
        grain: &GrainName,
    ) -> Result<Option<(Seq, Vec<u8>)>, GrainJournalError> {
        Ok(self.store.snapshot(self.shard, grain).durable())
    }

    // --- The grain-native content-addressed blob store (single-node) --------------

    pub(crate) async fn put_blob(
        &self,
        grain: &GrainName,
        id: BlobId,
        bytes: Vec<u8>,
    ) -> Result<(), GrainJournalError> {
        let (name, shard) = (grain.clone(), self.shard);
        match on_store(&self.io, &self.store, move |store| {
            store.put_blob(shard, &name, id, bytes)
        })
        .await
        .durable()
        {
            BlobAck::Stored => Ok(()),
            // The single store IS the durability on this tier (§7.4), so a store that
            // could not write means the blob is not durable anywhere.
            BlobAck::Failed => Err(GrainJournalError::Unavailable(
                "local store could not persist the blob".into(),
            )),
        }
    }

    pub(crate) async fn get_blob(
        &self,
        grain: &GrainName,
        id: BlobId,
    ) -> Result<Option<Vec<u8>>, GrainJournalError> {
        // Verify the stored bytes against the id (B1): a single store can still suffer
        // on-disk bit-rot, which must surface as an error, never as wrong bytes.
        match self.store.get_blob(self.shard, grain, id).durable() {
            Some(bytes) if id.verifies(&bytes) => Ok(Some(bytes)),
            Some(_) => Err(GrainJournalError::Unavailable(format!(
                "blob {id} failed verification"
            ))),
            None => Ok(None),
        }
    }

    pub(crate) async fn has_blob(
        &self,
        grain: &GrainName,
        id: BlobId,
    ) -> Result<bool, GrainJournalError> {
        Ok(self.store.has_blob(self.shard, grain, id).durable())
    }

    pub(crate) async fn retain_blobs(&self, grain: &GrainName, retain: Vec<BlobId>) {
        let (name, shard) = (grain.clone(), self.shard);
        on_store(&self.io, &self.store, move |store| {
            store.retain_blobs(shard, &name, &retain.into_iter().collect())
        })
        .await
        .durable();
    }

    pub(crate) async fn delete_blobs(&self, grain: &GrainName) {
        let (name, shard) = (grain.clone(), self.shard);
        on_store(&self.io, &self.store, move |store| {
            store.delete_blobs(shard, &name)
        })
        .await
        .durable();
    }
}

// --- Quorum tier: per-grain quorum append over the shard's replicas ----------

/// A shard's replica sets (§7.6, §7.7): the committed `current` set, and — while a
/// replica-set migration is in flight — the committed `target` set. Shared between
/// the shard map's apply loop (the writer, updating it as `Assign`/`Migrated`
/// commit) and the shard's [`QuorumReplicator`] (the reader).
///
/// While `target` is present every write and recovery uses a **joint quorum** (a
/// majority of `current` AND a majority of `target`), so no committed record's
/// durability ever rests on a set that lacks it: old-set quorums still intersect
/// every pre-migration write, new-set quorums intersect every in-migration write,
/// and the flip to `target`-only happens only after the migration driver has
/// caught every grain up on the target set (§7.7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReplicaSets {
    pub(crate) current: Vec<NodeId>,
    pub(crate) target: Option<Vec<NodeId>>,
}

/// The live control state one shard's apply loop shares with its replicator
/// (§7.6, §7.7): the replica sets (the quorum domain), the key range the shard
/// currently owns, and — while a split or merge is sealing the moving range —
/// the frozen-from bound. One mutex, read per operation, written only by the
/// shard map's apply loop and split/merge driver.
pub(crate) struct ShardControl {
    /// The committed `current`/`target` replica sets (§7.7).
    pub(crate) sets: ReplicaSets,
    /// The key range this shard owns (§5.1): shrinks on a committed split,
    /// extends on a committed merge. An append outside it is refused
    /// `NotLeader` before any store attempt — the leader-local half of G15.
    pub(crate) range: crate::system::KeyRange,
    /// The in-flight split/merge seal (§7.7): refuse appends at or above this
    /// hash. A fast path only — the authoritative barrier is the replica
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

    /// Whether this shard currently accepts appends for a grain at `hash`.
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

    /// Every node that must receive the fan-out: `current ∪ target`, deduplicated.
    pub(crate) fn union(&self) -> Vec<NodeId> {
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
/// [`GrainStore`] (the leader is one of the replicas, §5.2), and the
/// [`ReplicaTransport`] to the other replicas.
pub(crate) struct QuorumReplicator<R: RaftConsensus> {
    election: LeaderElection<R>,
    local: Arc<dyn GrainStore>,
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
    #[allow(clippy::too_many_arguments)] // one call site, from the shard map
    pub(crate) fn new(
        election: LeaderElection<R>,
        local: Arc<dyn GrainStore>,
        transport: Arc<dyn ReplicaTransport>,
        control: Arc<std::sync::Mutex<ShardControl>>,
        shard: u32,
        self_node: NodeId,
        io: Arc<dyn BlockingIo>,
        deadlines: Deadlines,
    ) -> QuorumReplicator<R> {
        QuorumReplicator {
            election,
            local,
            transport,
            control,
            shard,
            self_node,
            io,
            deadlines,
        }
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

    /// The fan-out peers of `sets` other than this node (the leader writes its own
    /// store locally, §5.2): `current ∪ target` during a migration.
    fn peers_of(&self, sets: &ReplicaSets) -> Vec<NodeId> {
        sets.union()
            .into_iter()
            .filter(|&n| n != self.self_node)
            .collect()
    }

    /// Per-grain quorum append (spec §7.2): stamp the shard term, write the local
    /// replica, fan out to the peers, and commit once a quorum has stored. A
    /// `Fenced` reply means a higher term exists (we are deposed) → `NotLeader`; a
    /// missed quorum within the timeout → `Unavailable` (§11). The record's identity
    /// is its `(grain, Seq)` slot, so a timed-out append that lands later is applied
    /// once on recovery with no dedup token (§7.2).
    pub(crate) async fn append(
        &self,
        grain: &GrainName,
        after: Seq,
        events: Vec<Vec<u8>>,
    ) -> AppendOutcome {
        let Some(term) = self.election.term() else {
            return self.not_leader();
        };
        if !self.election.is_leader() {
            return self.not_leader();
        }
        // The split/merge gate (§7.7, G15): an append for a key this shard no longer
        // owns, or that is frozen mid-move, is refused BEFORE any store attempt, so it
        // provably never ran and the caller can re-resolve against the committed map.
        // The authoritative barrier is the replica stores' durable append bound.
        let sets = {
            let control = self.control.lock().expect("shard control poisoned");
            let hash = crate::system::name_hash(grain.grain_type(), grain.key());
            if !control.accepts(hash) {
                return self.not_leader();
            }
            control.sets.clone()
        };
        let events_len = events.len();
        // Fan out to the remote peers first, each cloning the payload for its own wire
        // message; then write this node's own replica, *moving* the payload in (§5.2).
        // The batch is deep-cloned R-1 times, never R.
        let mut replicas: Vec<StoreAckFuture> = self
            .peers_of(&sets)
            .into_iter()
            .map(|node| {
                let ack = self.transport.store_record(
                    node,
                    self.shard,
                    grain.clone(),
                    after,
                    term,
                    events.clone(),
                    WriteKind::Append,
                    self.deadlines.quorum,
                );
                Box::pin(async move { (node, ack.await) }) as StoreAckFuture
            })
            .collect();
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
        // the write is still awaited before this returns — which is what preserves a
        // grain's write order — but the peers travel while it runs.
        let (name, shard) = (grain.clone(), self.shard);
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
            // (§7.2, G5). Peers that stored it keep it, so a quorum can still commit
            // it late (the ambiguous-timeout case, §7.2). `pending` is already drained.
            // Bounded by our own term: while the quorum wait was in flight a newer
            // leader may have fenced this store and landed committed records above
            // `after` — those carry a higher term and must survive (G14).
            self.local
                .truncate(self.shard, grain, after, term)
                .durable();
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

    /// Recover a grain's head from a write quorum by read-repair (spec §8, **G14**) —
    /// the rehydration barrier. Fence-read a quorum
    /// (a Paxos prepare that bars a deposed leader from committing after we read),
    /// take the highest-term record per slot, write the recovered tail back under our
    /// own term so the adopted head is itself quorum-durable, and leave the records
    /// and snapshot in the local store so subsequent `load`/`load_snapshot` read
    /// locally. Returns the recovered head, or `Unavailable` while the shard is
    /// electing or a quorum is unreachable (the failover window, §8.3).
    ///
    /// Short of a read quorum this falls back to the local view (§7.5,
    /// read-your-leader) — acceptable for serving reads, never for a decision
    /// that moves data; those paths use [`recover_quorum`](Self::recover_quorum).
    pub(crate) async fn recover(&self, grain: &GrainName) -> Result<Seq, GrainJournalError> {
        self.recover_with(grain, false).await
    }

    /// [`recover`](Self::recover) that REQUIRES the read quorum: `Err` instead of
    /// the local fallback. The migration and split drivers (§7.7) use this — a
    /// transfer or `Migrated`/`SplitCommitted` proposal must never be based on a
    /// possibly-stale local view (G14/G15).
    pub(crate) async fn recover_quorum(&self, grain: &GrainName) -> Result<Seq, GrainJournalError> {
        self.recover_with(grain, true).await
    }

    async fn recover_with(
        &self,
        grain: &GrainName,
        require_quorum: bool,
    ) -> Result<Seq, GrainJournalError> {
        let Some(term) = self.election.term().filter(|_| self.election.is_leader()) else {
            return Err(GrainJournalError::Unavailable("shard electing".into()));
        };
        let sets = self.sets();

        let read = self.fence_read(grain, term, &sets).await?;
        // A joint read quorum during a migration (§7.7): every pre-migration commit
        // sits on a majority of `current`, every in-migration commit additionally on
        // a majority of `target`, so requiring both majorities intersects them all.
        if require_quorum && !read.confirmed {
            return Err(GrainJournalError::Unavailable(
                "recovery did not reach a read quorum".into(),
            ));
        }

        // Merge: highest-term record per slot, and the best snapshot. When a quorum
        // was reached this is the authoritative head; otherwise it is just this node's
        // local view — a read-your-leader fallback (§7.5) that may be stale but cannot
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
        // The recovered head's compacted base — the seq of the best snapshot, which
        // the recovered tail records sit above (§9).
        let base = snapshot.as_ref().map_or(Seq::ZERO, |(s, _, _)| *s);

        // Cache the recovered snapshot locally first, so the local store's base is
        // aligned to `base` before the write-back lands the tail above it. Records
        // remain the authority, so the snapshot need not be quorum-durable here.
        if let Some((at, snap_term, state)) = snapshot {
            self.local
                .store_snapshot(
                    self.shard,
                    grain,
                    at,
                    snap_term.max(term),
                    state,
                    WriteKind::Repair,
                )
                .durable();
        }

        if read.confirmed {
            self.write_back(
                grain,
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
        grain: &GrainName,
        term: Term,
        sets: &ReplicaSets,
    ) -> Result<ReadQuorum, GrainJournalError> {
        // The promise is made under the grain's segment lock, before `prepare`
        // returns, so it fences a concurrent append either way. The *view* is another
        // matter and must not be released before it is stable: `write_back` below
        // skips the quorum re-write when nothing changed, so a reply naming a record
        // this replica has applied but not yet persisted could be merged into the
        // adopted head and then never re-written anywhere — a crash here would lose a
        // record already reported committed (**G14**).
        // Offloaded: `prepare` rewrites the shard's fence file whenever the term
        // advances, so on every failover this is a durable write on the recovery path.
        let (name, shard) = (grain.clone(), self.shard);
        let local = on_store(&self.io, &self.local, move |store| {
            store.prepare(shard, &name, term)
        })
        .await;
        // Naming both terms, because the interesting failure is not the ordinary one.
        // A higher term from a *peer* is a deposed leader learning it lost an election,
        // and the next activation on the winner recovers. A higher term in our **own**
        // store is the pathological one — the local fence outran this shard's group, so
        // no leader of it can ever read again — and only the two numbers tell them
        // apart.
        let local_reply = match local.durable() {
            ReadOutcome::Prepared(reply) => reply,
            ReadOutcome::Fenced(fence) => {
                return Err(GrainJournalError::Unavailable(format!(
                    "local store fenced shard {} at {fence:?}, above our {term:?}",
                    self.shard
                )));
            }
            // Our own storage is unusable, so this node cannot lead the shard at all:
            // it can neither read the grain's records nor durably promise the term
            // recovery depends on. Refusing here is what keeps a leader on a dead
            // disk from serving; the shard elects around it.
            ReadOutcome::Failed => {
                return Err(GrainJournalError::Unavailable(format!(
                    "local store for shard {} is unusable",
                    self.shard
                )));
            }
        };
        let peer_nodes = self.peers_of(sets);
        let peer_reads = peer_nodes.iter().map(|&node| {
            self.transport.read_grain(
                node,
                self.shard,
                grain.clone(),
                term,
                self.deadlines.recover,
            )
        });
        // Take our local head before moving the reply into the quorum set, so the
        // write-back below can skip the network on a stable re-activation without a
        // second read and without deep-cloning the grain's records.
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
                    return Err(GrainJournalError::Unavailable(
                        "fenced by a higher term".into(),
                    ));
                }
                // A peer whose storage failed promised nothing and holds nothing we
                // can believe, so it is not part of the read quorum — the same
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
    /// the covered records (§8). Skips the network — returning `Ok` without a write —
    /// when nothing changed (a stable re-activation: no record below our term, head
    /// not advanced), except during a migration, when the write-back is exactly how a
    /// target replica receives the grain's records (§7.7), so it always runs.
    #[allow(clippy::too_many_arguments)] // mirrors the raft.rs handler signatures
    async fn write_back(
        &self,
        grain: &GrainName,
        term: Term,
        sets: &ReplicaSets,
        base: Seq,
        head: Seq,
        records: &[Vec<u8>],
        any_below: bool,
        local_head: Seq,
    ) -> Result<(), GrainJournalError> {
        let migrating = sets.target.is_some();
        if head.value() <= base.value()
            || !(any_below || local_head.value() < head.value() || migrating)
        {
            return Ok(());
        }
        let (name, shard, repaired) = (grain.clone(), self.shard, records.to_vec());
        let local = on_store(&self.io, &self.local, move |store| {
            store.store_record(shard, &name, base, term, repaired, WriteKind::Repair)
        })
        .await;
        let mut replicas: Vec<StoreAckFuture> = self
            .peers_of(sets)
            .into_iter()
            .map(|node| {
                let ack = self.transport.store_record(
                    node,
                    self.shard,
                    grain.clone(),
                    base,
                    term,
                    records.to_vec(),
                    WriteKind::Repair,
                    self.deadlines.recover,
                );
                Box::pin(async move { (node, ack.await) }) as StoreAckFuture
            })
            .collect();
        replicas.push(local_ack(self.self_node, local));
        let (outcome, pending) = self.collect_store_quorum(sets, replicas).await;
        match outcome {
            QuorumOutcome::Committed => {
                self.drain(pending);
                Ok(())
            }
            QuorumOutcome::Fenced => Err(GrainJournalError::Unavailable(
                "fenced by a higher term".into(),
            )),
            // `Sealed` cannot occur on a `Repair` (the bound refuses only appends);
            // folded with the quorum-miss arm for completeness.
            QuorumOutcome::Stale | QuorumOutcome::Sealed | QuorumOutcome::Unavailable => Err(
                GrainJournalError::Unavailable("recovery write-back did not reach a quorum".into()),
            ),
        }
    }

    /// Records from the **local** store, not from a quorum — the quorum read on the
    /// activation path is `head`'s recovery (§8), which runs once and backfills
    /// whatever this replica was missing. So a rehydration's replay, however long,
    /// costs no round trips: it is local reads over records this node already holds.
    /// That is why §9's snapshot cadence can afford to be sparse
    /// (`docs/hardware-envelope.md` §3.1).
    pub(crate) async fn load(
        &self,
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<(Seq, Vec<u8>)>, GrainJournalError> {
        Ok(self
            .local
            .read_from(self.shard, grain, from, limit)
            .durable())
    }

    pub(crate) async fn load_snapshot(
        &self,
        grain: &GrainName,
    ) -> Result<Option<(Seq, Vec<u8>)>, GrainJournalError> {
        Ok(self.local.snapshot(self.shard, grain).durable())
    }

    // --- The grain-native content-addressed blob store (clustered) ----------------
    //
    // A grain's immutable blobs ride the *same* shard replica set as its records, with
    // no term and no order. The leader always keeps a local copy, so a `get` is a
    // local, verified read in steady state; a fresh leader after a migration that
    // lacks a block faults it from a peer and backfills locally (lazy hydration).

    /// Store an immutable blob on a write quorum of the grain's replicas, always
    /// including this local replica (so subsequent reads are local). No term, no
    /// leadership gate: an orphan blob from a deposed writer is harmless (content-
    /// addressed) and reclaimed by the grain's sweep. Returns `Unavailable` if a
    /// quorum is unreachable, so the caller learns the bytes are not yet durable.
    pub(crate) async fn put_blob(
        &self,
        grain: &GrainName,
        id: BlobId,
        bytes: Vec<u8>,
    ) -> Result<(), GrainJournalError> {
        let sets = self.sets();
        // Local copy first (the leader is a replica, §5.2): move the bytes into peers'
        // wire messages by clone, but the local write needs no copy beyond the fan-out.
        let pending: FuturesUnordered<BlobAckFuture> = self
            .peers_of(&sets)
            .into_iter()
            .map(|node| {
                let ack = self.transport.store_blob(
                    node,
                    self.shard,
                    grain.clone(),
                    id,
                    bytes.clone(),
                    self.deadlines.quorum,
                );
                Box::pin(async move { (node, ack.await) }) as BlobAckFuture
            })
            .collect();
        // Awaited *before* the peers are polled, rather than joining the fan-out as
        // one more ack the way the record path does: **G18** requires a blob's quorum
        // to always include the leader, because that is what makes a later `get` a
        // local read (§7.10 colocation). That requirement is also why a failed local
        // write ends the put here — no number of peer copies can substitute for the
        // one G18 names, and the caller must not hear success without it.
        let (name, shard) = (grain.clone(), self.shard);
        let stored = on_store(&self.io, &self.local, move |store| {
            store.put_blob(shard, &name, id, bytes)
        })
        .await
        .durable();
        if stored == BlobAck::Failed {
            self.drain(pending);
            return Err(GrainJournalError::Unavailable(
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
            Err(GrainJournalError::Unavailable(
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
        grain: &GrainName,
        id: BlobId,
    ) -> Result<Option<Vec<u8>>, GrainJournalError> {
        let sets = self.sets();
        let mut corrupt = false;
        if let Some(bytes) = self.local.get_blob(self.shard, grain, id).durable() {
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
            let (name, shard) = (grain.clone(), self.shard);
            on_store(&self.io, &self.local, move |store| {
                store.delete_blob(shard, &name, id)
            })
            .await
            .durable();
        }
        for node in self.peers_of(&sets) {
            match self
                .transport
                .fetch_blob(node, self.shard, grain.clone(), id, self.deadlines.quorum)
                .await
            {
                Ok(Some(bytes)) if id.verifies(&bytes) => {
                    // Backfill locally so the next read is local (lazy hydration), and
                    // repair a corrupt local copy evicted above (self-heal).
                    let (name, shard, copy) = (grain.clone(), self.shard, bytes.clone());
                    on_store(&self.io, &self.local, move |store| {
                        store.put_blob(shard, &name, id, copy)
                    })
                    .await
                    .durable();
                    return Ok(Some(bytes));
                }
                Ok(Some(_)) => corrupt = true,
                Ok(None) | Err(_) => {}
            }
        }
        if corrupt {
            Err(GrainJournalError::Unavailable(format!(
                "blob {id} failed verification on every reachable replica"
            )))
        } else {
            Ok(None)
        }
    }

    /// Whether any reachable replica holds the blob: short-circuit on the first holder
    /// (the local copy, else a peer), not a quorum count — a `true` says a `get` can
    /// source the bytes, not that they are quorum-durable (that is `put_blob`'s job).
    pub(crate) async fn has_blob(
        &self,
        grain: &GrainName,
        id: BlobId,
    ) -> Result<bool, GrainJournalError> {
        if self.local.has_blob(self.shard, grain, id).durable() {
            return Ok(true);
        }
        for node in self.peers_of(&self.sets()) {
            if let Ok(true) = self
                .transport
                .has_blob(node, self.shard, grain.clone(), id, self.deadlines.quorum)
                .await
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Sweep the grain's blobs on every replica, keeping only `retain` (the
    /// mark-from-roots GC). Best-effort: a missed replica keeps its garbage until the
    /// next sweep, never a correctness issue.
    pub(crate) async fn retain_blobs(&self, grain: &GrainName, retain: Vec<BlobId>) {
        let (name, shard, keep) = (grain.clone(), self.shard, retain.clone());
        on_store(&self.io, &self.local, move |store| {
            store.retain_blobs(shard, &name, &keep.into_iter().collect())
        })
        .await
        .durable();
        let sweeps = self.peers_of(&self.sets()).into_iter().map(|node| {
            self.transport.sweep_blobs(
                node,
                self.shard,
                grain.clone(),
                Some(retain.clone()),
                self.deadlines.quorum,
            )
        });
        let _ = join_all(sweeps).await;
    }

    /// Drop the grain's whole blob area on every replica (destroy). Best-effort.
    pub(crate) async fn delete_blobs(&self, grain: &GrainName) {
        let (name, shard) = (grain.clone(), self.shard);
        on_store(&self.io, &self.local, move |store| {
            store.delete_blobs(shard, &name)
        })
        .await
        .durable();
        let sweeps = self.peers_of(&self.sets()).into_iter().map(|node| {
            self.transport
                .sweep_blobs(node, self.shard, grain.clone(), None, self.deadlines.quorum)
        });
        let _ = join_all(sweeps).await;
    }

    /// Persist a snapshot on a quorum (spec §9), fenced by the shard term. Quorum-
    /// blocking so a later compaction can safely truncate the covered records.
    pub(crate) async fn save_snapshot(
        &self,
        grain: &GrainName,
        at: Seq,
        state: Vec<u8>,
    ) -> AppendOutcome {
        let Some(term) = self.election.term() else {
            return self.not_leader();
        };
        if !self.election.is_leader() {
            return self.not_leader();
        }
        let sets = self.sets();
        // Clone the state for each remote peer's wire message, then move it into this
        // node's own replica write (§5.2) — R-1 copies, not R.
        let mut replicas: Vec<StoreAckFuture> = self
            .peers_of(&sets)
            .into_iter()
            .map(|node| {
                let ack = self.transport.store_snapshot(
                    node,
                    self.shard,
                    grain.clone(),
                    at,
                    term,
                    state.clone(),
                    WriteKind::Append,
                    self.deadlines.quorum,
                );
                Box::pin(async move { (node, ack.await) }) as StoreAckFuture
            })
            .collect();
        // Offloaded for the same reason as the record write above, and overlapped with
        // the fan-out for the same reason — more so here: a snapshot is the largest
        // single thing this node fsyncs, so it is both the worst one to run on an async
        // worker and the one whose flush the peers would spend the longest waiting on.
        let (name, shard) = (grain.clone(), self.shard);
        let (local, local_write) =
            local_store_ack(self.self_node, &self.io, &self.local, move |store| {
                store.store_snapshot(shard, &name, at, term, state, WriteKind::Append)
            });
        replicas.push(local);
        let (outcome, pending) = self.collect_store_quorum(&sets, replicas).await;
        // This node's own write has landed before the outcome is acted on, as in
        // `append` — one outstanding store call per grain (see `local_store_ack`).
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
    // catch every grain up on the `target` set before the map flips to it. All are
    // idempotent, so a crashed or deposed driver simply re-drives.

    /// Enumerate the shard's grains from a read quorum of its replicas: the union
    /// of every reachable replica's local list, valid once the replies cover a
    /// majority of `current` (a committed record lives on a majority of `current`,
    /// so any such union misses no committed grain). `Err` while short of that.
    pub(crate) async fn migration_grains(&self) -> Result<Vec<GrainName>, GrainJournalError> {
        let sets = self.sets();
        let mut count = JointCount::new(&sets);
        count.ack(self.self_node);
        let mut names: std::collections::BTreeSet<GrainName> =
            self.local.grains(self.shard).into_iter().collect();
        let peer_nodes = self.peers_of(&sets);
        let lists = peer_nodes.iter().map(|&node| {
            self.transport
                .list_grains(node, self.shard, self.deadlines.quorum)
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
            Err(GrainJournalError::Unavailable(
                "grain enumeration did not reach a read quorum".into(),
            ))
        }
    }

    /// Catch one grain up on the target set (§7.7): recover its head (the joint
    /// write-back lands the records on the target replicas), then re-persist its
    /// best snapshot on the joint quorum (a compacted grain's prefix exists only in
    /// the snapshot, so the records alone are not enough), then copy its blob area.
    ///
    /// Uses the read-your-leader `recover`, not the quorum-required variant: a
    /// migration only ever advances a `target` toward becoming `current`, gated by the
    /// joint-quorum write-back and the final `Migrated` flip, so a pass on a
    /// possibly-stale local view still cannot flip the set without a quorum, and it
    /// does not retry-storm against a partitioned peer (§14). `recover_quorum` is
    /// reserved for split/merge, where a transfer decision is irreversible before any
    /// consensus gate (G15).
    pub(crate) async fn migrate_grain(&self, grain: &GrainName) -> Result<(), GrainJournalError> {
        self.recover(grain).await?;
        if let Some((at, state)) = self.local.snapshot(self.shard, grain).durable()
            && let AppendOutcome::NotLeader(_) | AppendOutcome::Unavailable(_) =
                self.save_snapshot(grain, at, state).await
        {
            return Err(GrainJournalError::Unavailable(
                "snapshot did not reach the joint quorum".into(),
            ));
        }
        self.migrate_blobs(grain).await
    }

    /// Copy a grain's blob area to every target replica that lacks any of it: the
    /// source list is the union of the current replicas' ids, each blob fetched
    /// verified (through [`get_blob`](Self::get_blob)'s local-then-peers path) and
    /// stored on the lacking peers. Idempotent (content-addressed).
    async fn migrate_blobs(&self, grain: &GrainName) -> Result<(), GrainJournalError> {
        let sets = self.sets();
        let Some(target) = sets.target.clone() else {
            return Ok(());
        };
        // Source ids: this replica's plus every reachable current peer's.
        let mut ids: std::collections::BTreeSet<BlobId> = self
            .local
            .blob_ids(self.shard, grain)
            .durable()
            .into_iter()
            .collect();
        let current_peers: Vec<NodeId> = sets
            .current
            .iter()
            .copied()
            .filter(|&n| n != self.self_node)
            .collect();
        let lists = current_peers.iter().map(|&node| {
            self.transport
                .list_blobs(node, self.shard, grain.clone(), self.deadlines.quorum)
        });
        for list in join_all(lists).await.into_iter().flatten() {
            ids.extend(list);
        }
        // Per target peer: ship what it lacks.
        for node in target.into_iter().filter(|&n| n != self.self_node) {
            let held: std::collections::BTreeSet<BlobId> = self
                .transport
                .list_blobs(node, self.shard, grain.clone(), self.deadlines.quorum)
                .await
                .map_err(|_| GrainJournalError::Unavailable("target replica unreachable".into()))?
                .into_iter()
                .collect();
            for &id in ids.difference(&held) {
                // A verified fetch: local copy, else the first current peer holding
                // it. `None` means no replica holds it any more (swept mid-copy) —
                // an orphan, safely skipped.
                let Some(bytes) = self.get_blob(grain, id).await? else {
                    continue;
                };
                self.transport
                    .store_blob(
                        node,
                        self.shard,
                        grain.clone(),
                        id,
                        bytes,
                        self.deadlines.quorum,
                    )
                    .await
                    .map_err(|_| {
                        GrainJournalError::Unavailable("blob copy to target failed".into())
                    })?;
            }
        }
        Ok(())
    }

    // --- Shard split/merge transfer (§7.7) -------------------------------------
    //
    // The split (and merge) driver — a leader-only loop in `shardmap` — uses these
    // to move a key range's grains to their destination shard's keys before the
    // partition change commits. All idempotent: a crashed or deposed driver
    // re-drives, and re-copied slots agree (the source is a quorum recovery, G14).

    /// Durably tighten the append bound on a majority of this shard's replicas:
    /// refuse every future append at or above `from`, at ANY term (G15). The
    /// driver's first step; only after this returns may the transfer read the
    /// committed prefix, because from here on no append to the moved range can
    /// assemble a write quorum — a majority of acks would have to include a
    /// bounded store. Idempotent and monotone.
    pub(crate) async fn seal_shard(&self, from: u64) -> Result<(), GrainJournalError> {
        let sets = self.sets();
        // Durable before this node counts itself toward the barrier: the bound is a
        // promise, and a majority that included an unpersisted one could be forgotten
        // by a restart, letting a stale leader assemble a quorum for the moved range
        // afterward (**G15**).
        let shard = self.shard;
        on_store(&self.io, &self.local, move |store| {
            store.seal_range(shard, from)
        })
        .await
        .durable();
        let mut count = JointCount::new(&sets);
        count.ack(self.self_node);
        // Return as soon as a majority has sealed — never block on a dead replica
        // for the full timeout. A split's dest inherits the parent's replicas, so
        // one may be down; the seal barrier is a majority, not unanimity (G15).
        let mut pending: FuturesUnordered<_> = self
            .peers_of(&sets)
            .into_iter()
            .map(|node| {
                let ack = self
                    .transport
                    .seal_range(node, self.shard, from, self.deadlines.quorum);
                Box::pin(async move { (node, ack.await) })
            })
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
            Err(GrainJournalError::Unavailable(
                "seal did not reach a quorum".into(),
            ))
        }
    }

    /// Land one moved grain's committed prefix — snapshot, then records, then
    /// blobs — under `dest` shard keys on `dest_replicas` (§7.7). The source is
    /// a quorum recovery under our own term (fencing deposed leaders of this
    /// shard), after which the local store holds the authoritative prefix; the
    /// copy is a [`WriteKind::Transfer`] at `Term::ZERO`, majority-acked on
    /// `dest_replicas` for records and snapshot, every-replica for blobs
    /// (mirroring the migration copy's strictness). Snapshot before records so
    /// the destination segment's base aligns (as `recover`'s own write-back
    /// does).
    pub(crate) async fn transfer_grain(
        &self,
        grain: &GrainName,
        dest: u32,
        dest_replicas: &[NodeId],
    ) -> Result<(), GrainJournalError> {
        self.recover_quorum(grain).await?;
        let reply = self.local.read(self.shard, grain).durable();
        let head = reply.head();
        let base = reply.snapshot.as_ref().map_or(Seq::ZERO, |(s, _, _)| *s);
        // The committed prefix: the snapshot plus the contiguous records above
        // it, up to the recovered head — never the uncommitted tail beyond it.
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
                let (name, copy) = (grain.clone(), state.clone());
                Some(
                    on_store(&self.io, &self.local, move |store| {
                        store.store_snapshot(dest, &name, at, Term::ZERO, copy, WriteKind::Transfer)
                    })
                    .await,
                )
            } else {
                None
            };
            let peers = self.fan_to_peers(dest_replicas, |node| {
                self.transport.store_snapshot(
                    node,
                    dest,
                    grain.clone(),
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
                return Err(GrainJournalError::Unavailable(
                    "transfer snapshot did not reach a majority of the destination".into(),
                ));
            }
        }
        if !records.is_empty() {
            let local = if dest_replicas.contains(&self.self_node) {
                let (name, copy) = (grain.clone(), records.clone());
                Some(
                    on_store(&self.io, &self.local, move |store| {
                        store.store_record(dest, &name, base, Term::ZERO, copy, WriteKind::Transfer)
                    })
                    .await,
                )
            } else {
                None
            };
            let peers = self.fan_to_peers(dest_replicas, |node| {
                self.transport.store_record(
                    node,
                    dest,
                    grain.clone(),
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
                return Err(GrainJournalError::Unavailable(
                    "transfer records did not reach a majority of the destination".into(),
                ));
            }
        }
        self.transfer_blobs(grain, dest, dest_replicas).await
    }

    /// Await `Stored` acks from a majority of `total` destination replicas —
    /// the transfer copy's plain-majority accounting (the destination set is
    /// explicit, unlike the joint quorum over this shard's own sets). Stragglers
    /// of a satisfied majority drain off the hot path.
    async fn transfer_to_majority(
        &self,
        total: usize,
        local: Option<Reserved<StoreAck>>,
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

    /// Fan a per-node `Transfer` store out to every destination replica but this
    /// leader, tagging each ack with its node for the majority count. The differing
    /// store call is supplied as `mk`.
    fn fan_to_peers(
        &self,
        dest_replicas: &[NodeId],
        mk: impl Fn(NodeId) -> actor_core::BoxFuture<'static, Result<StoreAck, actor_core::CallError>>,
    ) -> Vec<StoreAckFuture> {
        dest_replicas
            .iter()
            .copied()
            .filter(|&n| n != self.self_node)
            .map(|node| {
                let ack = mk(node);
                Box::pin(async move { (node, ack.await) }) as StoreAckFuture
            })
            .collect()
    }

    /// Copy a moved grain's blob area to the reachable destination replicas'
    /// `dest`-keyed areas (§7.7, G17/G18): source ids are the union of this
    /// shard's replicas' lists, each blob fetched verified and stored where
    /// missing. Best-effort per destination node — an unreachable dest replica
    /// (a split's child inherits the parent's replicas, which may include a
    /// crashed one) is skipped rather than stalling the split; its copies heal
    /// via recovery-on-access when it returns. The committed records and snapshot
    /// already reached a majority (`transfer_to_majority`), and blobs reach every
    /// reachable dest. Idempotent (content-addressed); requires this leader's own
    /// local copy to land, so the child leader can always serve.
    async fn transfer_blobs(
        &self,
        grain: &GrainName,
        dest: u32,
        dest_replicas: &[NodeId],
    ) -> Result<(), GrainJournalError> {
        let sets = self.sets();
        // Source ids under THIS shard's keys: local plus every reachable peer.
        let mut ids: std::collections::BTreeSet<BlobId> = self
            .local
            .blob_ids(self.shard, grain)
            .durable()
            .into_iter()
            .collect();
        let source_peers = self.peers_of(&sets);
        let lists = source_peers.iter().map(|&node| {
            self.transport
                .list_blobs(node, self.shard, grain.clone(), self.deadlines.quorum)
        });
        for list in join_all(lists).await.into_iter().flatten() {
            ids.extend(list);
        }
        for &node in dest_replicas {
            let held: std::collections::BTreeSet<BlobId> = if node == self.self_node {
                self.local
                    .blob_ids(dest, grain)
                    .durable()
                    .into_iter()
                    .collect()
            } else {
                match self
                    .transport
                    .list_blobs(node, dest, grain.clone(), self.deadlines.quorum)
                    .await
                {
                    Ok(list) => list.into_iter().collect(),
                    // Unreachable dest replica: skip it (heals on access later).
                    Err(_) => continue,
                }
            };
            for &id in ids.difference(&held) {
                // A verified fetch from this shard's keys: local copy, else the
                // first peer holding it. `None` means no replica holds it any
                // more (swept mid-copy) — an orphan, safely skipped.
                let Some(bytes) = self.get_blob(grain, id).await? else {
                    continue;
                };
                if node == self.self_node {
                    let name = grain.clone();
                    on_store(&self.io, &self.local, move |store| {
                        store.put_blob(dest, &name, id, bytes)
                    })
                    .await
                    .durable();
                } else if self
                    .transport
                    .store_blob(node, dest, grain.clone(), id, bytes, self.deadlines.quorum)
                    .await
                    .is_err()
                {
                    // Lost the dest replica mid-copy — skip; the rest of its
                    // blobs heal on access. Records/snapshot durability is
                    // unaffected (they committed on a majority).
                    break;
                }
            }
        }
        Ok(())
    }

    /// Seed the local ack, then poll `pending` until a joint quorum has acked (spec
    /// §7.2), returning as soon as it is reached — the commit waits on the quorum, not
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
    /// `Fenced` means we are deposed and a `Stale` means the head was stale — running
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
                // did not store, and — unlike the refusals above — it decided
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
/// under our term is needed). A gap ends the prefix — an uncommitted tail, dropped.
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
    // as the base can never drop a committed record (G14).
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::FutureExt;

    use super::*;
    use crate::blocking::Job;
    use crate::store::GrainStore;
    use crate::testing::StaticGrainStore;

    /// A pool that accepts jobs and runs none of them until told — standing in for a
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

    fn stored_store() -> Arc<dyn GrainStore> {
        Arc::new(StaticGrainStore::new(StoreAck::Stored(Seq::new(1))))
    }

    #[test]
    fn the_local_write_reaches_the_pool_when_the_quorum_polls_it_and_not_before() {
        // The property the overlap rests on. `append` builds the peer asks, then this,
        // then hands the whole set to `collect_store_quorum` — so if constructing the
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
                    &GrainName::new("t", "k"),
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
        // What keeps a grain's writes ordered. `BlockingIo` promises no ordering, so
        // `append` must not return while its own flush is still queued — the next
        // append for the grain would race it. It awaits this handle to prevent that,
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
                    &GrainName::new("t", "k"),
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
             quorum never counted, and the grain would have two stores in flight",
        );
        assert_eq!(
            write.now_or_never(),
            Some(StoreAck::Stored(Seq::new(1))),
            "the caller's handle did not observe the completed write",
        );
    }
}
