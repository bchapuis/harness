//! The Replicator: per-grain durability (spec §7.2, §7.4, §8).
//!
//! A grain's records are made durable not by a shared log but by a **per-grain
//! quorum append** (§7.2): the shard leader assigns each record the next `Seq` (free,
//! since it is the single writer) and the Replicator fans it to the shard's replicas,
//! reporting it durable once a quorum has stored it, fenced by the shard term (§8).
//! On activation a fresh leader **recovers** each grain's head from a write quorum by
//! read-repair — highest-term record per slot, written back under its own term — so
//! no acknowledged write is lost across a leadership change (**G14**), in place of a
//! shared log's leader-completeness.
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
use crate::election::LeaderElection;
use crate::grain::GrainName;
use crate::journal::AppendOutcome;
use crate::journal::GrainJournalError;
use crate::journal::Seq;
use crate::journal::Term;
use crate::replica_store::ReplicaTransport;
use crate::store::GrainStore;
use crate::store::ReadOutcome;
use crate::store::StoreAck;
use crate::store::WriteKind;

/// A pending per-replica store ack from the [`ReplicaTransport`] fan-out, tagged
/// with the replica it came from so a joint quorum can attribute it to the right
/// set(s) during a replica-set migration (§7.7).
type StoreAckFuture =
    actor_core::BoxFuture<'static, (NodeId, Result<StoreAck, actor_core::CallError>)>;

/// A pending per-replica blob store from the [`ReplicaTransport`] blob fan-out: it
/// resolves `Ok(())` once that peer has durably stored the blob (no ack variants —
/// an immutable blob has nothing to fence or order). Tagged with the replica for
/// joint-quorum attribution (§7.7).
type BlobAckFuture = actor_core::BoxFuture<'static, (NodeId, Result<(), actor_core::CallError>)>;

/// The result of [`merge`]: the contiguous record prefix, its head, the best
/// snapshot `(seq, term, state)`, and whether any kept record's term is below the
/// recovering leader's term (so a write-back is needed).
type Merged = (Vec<Vec<u8>>, Seq, Option<(Seq, Term, Vec<u8>)>, bool);

/// How long a quorum append/snapshot waits before reporting `Unavailable` (§11).
/// Comfortably above a healthy quorum round-trip (milliseconds) yet short enough
/// that a write to an unreachable shard fails fast rather than pinning the host's
/// serial executor: a quorum not reached within it means the shard cannot reach one.
const QUORUM_TIMEOUT: Duration = Duration::from_secs(2);

/// How long recovery waits for a read quorum before falling back to local state
/// (§7.5, read-your-leader). Short, so an activation on the minority side of a
/// partition serves a stale read promptly rather than blocking — a write from it
/// still cannot commit (no quorum), and a stale-head append is caught by the
/// replicas' optimistic head check (§8), so the fallback stays safe.
const RECOVER_TIMEOUT: Duration = Duration::from_secs(2);

// --- Local tier: one node, one store -----------------------------------------

/// The single-node `Local` replicator (spec §7.4): one [`GrainStore`], no term, no
/// quorum. An append commits on the local store; recovery is a local head read.
///
/// It deliberately mirrors [`QuorumReplicator`]'s shape so both journal tiers wrap a
/// replicator behind the same seam, even though the local body is thin — the symmetry
/// keeps [`LocalGrainJournal`](crate::LocalGrainJournal) and
/// [`QuorumGrainJournal`](crate::QuorumGrainJournal) structurally identical.
pub(crate) struct LocalReplicator {
    store: Arc<dyn GrainStore>,
    shard: u32,
}

impl LocalReplicator {
    pub(crate) fn new(store: Arc<dyn GrainStore>, shard: u32) -> LocalReplicator {
        LocalReplicator { store, shard }
    }

    pub(crate) async fn append(
        &self,
        grain: &GrainName,
        after: Seq,
        events: Vec<Vec<u8>>,
    ) -> AppendOutcome {
        // A single writer at term 0 is never fenced or stale (its fence stays 0 and
        // `after` always equals the head behind the input gate, §6).
        match self.store.store_record(
            self.shard,
            grain,
            after,
            Term::ZERO,
            events,
            WriteKind::Append,
        ) {
            StoreAck::Stored(head) => AppendOutcome::Committed(head),
            other => {
                AppendOutcome::Unavailable(format!("local store rejected the append: {other:?}"))
            }
        }
    }

    pub(crate) async fn head(&self, grain: &GrainName) -> Result<Seq, GrainJournalError> {
        Ok(head_from_reply(&self.store.read(self.shard, grain)))
    }

    pub(crate) async fn load(
        &self,
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<(Seq, Vec<u8>)>, GrainJournalError> {
        Ok(self.store.read_from(self.shard, grain, from, limit))
    }

    pub(crate) async fn save_snapshot(
        &self,
        grain: &GrainName,
        at: Seq,
        state: Vec<u8>,
    ) -> AppendOutcome {
        match self
            .store
            .store_snapshot(self.shard, grain, at, Term::ZERO, state, WriteKind::Append)
        {
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
        Ok(snapshot_of(self.store.read(self.shard, grain)))
    }

    // --- The grain-native content-addressed blob store (single-node) --------------

    pub(crate) async fn put_blob(
        &self,
        grain: &GrainName,
        id: BlobId,
        bytes: Vec<u8>,
    ) -> Result<(), GrainJournalError> {
        self.store.put_blob(self.shard, grain, id, bytes);
        Ok(())
    }

    pub(crate) async fn get_blob(
        &self,
        grain: &GrainName,
        id: BlobId,
    ) -> Result<Option<Vec<u8>>, GrainJournalError> {
        // Verify the stored bytes against the id (B1): a single store can still suffer
        // on-disk bit-rot, which must surface as an error, never as wrong bytes.
        match self.store.get_blob(self.shard, grain, id) {
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
        Ok(self.store.has_blob(self.shard, grain, id))
    }

    pub(crate) async fn retain_blobs(&self, grain: &GrainName, retain: Vec<BlobId>) {
        self.store
            .retain_blobs(self.shard, grain, &retain.into_iter().collect());
    }

    pub(crate) async fn delete_blobs(&self, grain: &GrainName) {
        self.store.delete_blobs(self.shard, grain);
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
/// toward every set that contains it; the quorum is satisfied when a majority of
/// `current` AND (when migrating) a majority of `target` have acked.
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

/// The clustered `Quorum` replicator (spec §7.2, §7.4, §8). Holds the shard's
/// leader-election group (for the term and leadership gate), this node's local
/// [`GrainStore`] (the leader is one of the replicas, §5.2), and the
/// [`ReplicaTransport`] to the other replicas.
pub(crate) struct QuorumReplicator<R: RaftConsensus> {
    election: LeaderElection<R>,
    local: Arc<dyn GrainStore>,
    transport: Arc<dyn ReplicaTransport>,
    /// The shard's live control state (§7.1, §7.7): the replica sets (the
    /// write/recovery quorum domain), owned key range, and split/merge freeze.
    /// The shard map's apply loop updates it in place as commands commit, so a
    /// continuing replica's quorums always count over the committed allocation,
    /// never a stale snapshot from construction time.
    control: Arc<std::sync::Mutex<ShardControl>>,
    shard: u32,
    self_node: NodeId,
}

impl<R: RaftConsensus> QuorumReplicator<R> {
    pub(crate) fn new(
        election: LeaderElection<R>,
        local: Arc<dyn GrainStore>,
        transport: Arc<dyn ReplicaTransport>,
        control: Arc<std::sync::Mutex<ShardControl>>,
        shard: u32,
        self_node: NodeId,
    ) -> QuorumReplicator<R> {
        QuorumReplicator {
            election,
            local,
            transport,
            control,
            shard,
            self_node,
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
        // The split/merge gate (§7.7, G15): an append for a key this shard no
        // longer owns (a committed split moved it) or that is frozen mid-move is
        // refused BEFORE any store attempt, so it provably never ran and the
        // caller's redirect can safely re-resolve against the committed map. The
        // authoritative barrier is the replica stores' durable append bound; this
        // is the leader-local fast path that spares the quorum round.
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
        // message; then write this node's own replica directly, *moving* the payload in
        // — the leader is a replica (§5.2), so its write needs no copy. The batch is
        // deep-cloned R-1 times (once per peer), never R.
        let peers = self
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
                    QUORUM_TIMEOUT,
                );
                Box::pin(async move { (node, ack.await) }) as StoreAckFuture
            })
            .collect();
        let local =
            self.local
                .store_record(self.shard, grain, after, term, events, WriteKind::Append);
        let (outcome, pending) = self.collect_store_quorum(&sets, local, peers).await;
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
            self.local.truncate(self.shard, grain, after, term);
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
    /// the rehydration barrier, in place of the old `catch_up`. Fence-read a quorum
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

        // Read phase: fence-read local and every peer (awaiting all, so no in-flight
        // ask is dropped — no-silent-loss, §14). Each read is bounded by
        // `RECOVER_TIMEOUT`, so an unreachable peer just falls out of the quorum.
        let local = self.local.prepare(self.shard, grain, term);
        let ReadOutcome::Prepared(local_reply) = local else {
            return Err(GrainJournalError::Unavailable(
                "fenced by a higher term".into(),
            ));
        };
        let peer_nodes = self.peers_of(&sets);
        let peer_reads = peer_nodes.iter().map(|&node| {
            self.transport
                .read_grain(node, self.shard, grain.clone(), term, RECOVER_TIMEOUT)
        });
        // Take our local head before moving the reply into the quorum set, so the
        // write-back below can skip the network on a stable re-activation without a
        // second read — and the recovery path never deep-clones the grain's records.
        let local_head = head_from_reply(&local_reply);
        let mut count = JointCount::new(&sets);
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
                Err(_) => {}
            }
        }
        // A joint read quorum during a migration (§7.7): every pre-migration commit
        // sits on a majority of `current`, every in-migration commit additionally on
        // a majority of `target`, so requiring both majorities intersects them all.
        let confirmed = count.satisfied();
        if require_quorum && !confirmed {
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
        // The record was never acknowledged, so no durability claim is violated —
        // it is a transient dirty read on a partitioned minority leader, the same
        // relaxed-read window §7.5 already documents.
        let (records, head, snapshot, any_below) = merge(replies, term);
        // The recovered head's compacted base — the seq of the best snapshot, which
        // the recovered tail records sit above (§9).
        let base = snapshot.as_ref().map_or(Seq::ZERO, |(s, _, _)| *s);

        // Cache the recovered snapshot locally first, so the local store's base is
        // aligned to `base` before the write-back lands the tail above it. Records
        // remain the authority, so the snapshot need not be quorum-durable here.
        if let Some((at, snap_term, state)) = snapshot {
            self.local.store_snapshot(
                self.shard,
                grain,
                at,
                snap_term.max(term),
                state,
                WriteKind::Repair,
            );
        }

        if confirmed {
            // Write-back phase: make the recovered tail quorum-durable under our term,
            // so no later recovery regresses it (§8) and the local store can serve
            // `load`. The tail sits after `base`; a replica already compacted past it
            // skips the covered records (§8). Skip the network when nothing changed
            // (a stable re-activation: no record below our term, head not advanced) —
            // except during a migration, when the write-back is exactly how a target
            // replica receives the grain's records (§7.7), so it always runs.
            let migrating = sets.target.is_some();
            if head.value() > base.value()
                && (any_below || local_head.value() < head.value() || migrating)
            {
                let local = self.local.store_record(
                    self.shard,
                    grain,
                    base,
                    term,
                    records.clone(),
                    WriteKind::Repair,
                );
                let peers = self
                    .peers_of(&sets)
                    .into_iter()
                    .map(|node| {
                        let ack = self.transport.store_record(
                            node,
                            self.shard,
                            grain.clone(),
                            base,
                            term,
                            records.clone(),
                            WriteKind::Repair,
                            RECOVER_TIMEOUT,
                        );
                        Box::pin(async move { (node, ack.await) }) as StoreAckFuture
                    })
                    .collect();
                let (outcome, pending) = self.collect_store_quorum(&sets, local, peers).await;
                match outcome {
                    QuorumOutcome::Committed => self.drain(pending),
                    QuorumOutcome::Fenced => {
                        return Err(GrainJournalError::Unavailable(
                            "fenced by a higher term".into(),
                        ));
                    }
                    // `Sealed` cannot occur on a `Repair` (the bound refuses only
                    // appends); folded with the quorum-miss arm for completeness.
                    QuorumOutcome::Stale | QuorumOutcome::Sealed | QuorumOutcome::Unavailable => {
                        return Err(GrainJournalError::Unavailable(
                            "recovery write-back did not reach a quorum".into(),
                        ));
                    }
                }
            }
        }

        Ok(head)
    }

    pub(crate) async fn load(
        &self,
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<(Seq, Vec<u8>)>, GrainJournalError> {
        Ok(self.local.read_from(self.shard, grain, from, limit))
    }

    pub(crate) async fn load_snapshot(
        &self,
        grain: &GrainName,
    ) -> Result<Option<(Seq, Vec<u8>)>, GrainJournalError> {
        Ok(snapshot_of(self.local.read(self.shard, grain)))
    }

    // --- The grain-native content-addressed blob store (clustered) ----------------
    //
    // A grain's immutable blobs ride the *same* shard replica set as its records,
    // but with no term and no order: a content hash names exactly one byte sequence,
    // so there is nothing to fence and nothing to agree on (the immutable subset of
    // the record path). The leader always keeps a local copy, so a `get` is a local,
    // verified read in steady state; a fresh leader after a migration that lacks a
    // block faults it from a peer and backfills locally (lazy hydration).

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
                    QUORUM_TIMEOUT,
                );
                Box::pin(async move { (node, ack.await) }) as BlobAckFuture
            })
            .collect();
        // The local write always succeeds (the leader is a replica), so it always acks;
        // a blob has no fence or order, so any `Ok` from a peer counts.
        self.local.put_blob(self.shard, grain, id, bytes);
        let (satisfied, pending) = self
            .accumulate_quorum(&sets, true, pending, |result| result.is_ok())
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
        if let Some(bytes) = self.local.get_blob(self.shard, grain, id) {
            if id.verifies(&bytes) {
                return Ok(Some(bytes));
            }
            // The local copy exists but is corrupt (on-disk bit-rot). Evict it so the
            // peer-sourced backfill below can replace it in place: a content-addressed
            // `put_blob` of an id already on disk writes nothing, so without this the
            // bad copy would persist — forcing a network fetch on every future read
            // and leaving this replica's durability margin permanently one short
            // (§7.10 self-heal). It is safe to drop unconditionally: a copy that fails
            // verification can never be returned, so it carries no value to lose.
            corrupt = true;
            self.local.delete_blob(self.shard, grain, id);
        }
        for node in self.peers_of(&sets) {
            match self
                .transport
                .fetch_blob(node, self.shard, grain.clone(), id, QUORUM_TIMEOUT)
                .await
            {
                Ok(Some(bytes)) if id.verifies(&bytes) => {
                    // Backfill locally so the next read is local (lazy hydration), and
                    // repair a corrupt local copy evicted above (self-heal).
                    self.local.put_blob(self.shard, grain, id, bytes.clone());
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
        if self.local.has_blob(self.shard, grain, id) {
            return Ok(true);
        }
        for node in self.peers_of(&self.sets()) {
            if let Ok(true) = self
                .transport
                .has_blob(node, self.shard, grain.clone(), id, QUORUM_TIMEOUT)
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
        self.local
            .retain_blobs(self.shard, grain, &retain.iter().copied().collect());
        let sweeps = self.peers_of(&self.sets()).into_iter().map(|node| {
            self.transport.sweep_blobs(
                node,
                self.shard,
                grain.clone(),
                Some(retain.clone()),
                QUORUM_TIMEOUT,
            )
        });
        let _ = join_all(sweeps).await;
    }

    /// Drop the grain's whole blob area on every replica (destroy). Best-effort.
    pub(crate) async fn delete_blobs(&self, grain: &GrainName) {
        self.local.delete_blobs(self.shard, grain);
        let sweeps = self.peers_of(&self.sets()).into_iter().map(|node| {
            self.transport
                .sweep_blobs(node, self.shard, grain.clone(), None, QUORUM_TIMEOUT)
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
        let peers = self
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
                    QUORUM_TIMEOUT,
                );
                Box::pin(async move { (node, ack.await) }) as StoreAckFuture
            })
            .collect();
        let local =
            self.local
                .store_snapshot(self.shard, grain, at, term, state, WriteKind::Append);
        let (outcome, pending) = self.collect_store_quorum(&sets, local, peers).await;
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
        let lists = peer_nodes
            .iter()
            .map(|&node| self.transport.list_grains(node, self.shard, QUORUM_TIMEOUT));
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
    /// Uses the read-your-leader `recover` (not the quorum-required variant): a
    /// migration only ever advances a `target` toward becoming `current`, gated
    /// by the joint-quorum write-back and the final `Migrated` flip, so a pass
    /// that runs on a possibly-stale local view still cannot flip the set without
    /// a quorum — and it does not retry-storm against a partitioned peer, which
    /// would leave in-flight recovery asks pending at quiescence (§14). The
    /// stricter `recover_quorum` is reserved for split/merge, where a transfer
    /// decision is irreversible before any consensus gate (G15).
    pub(crate) async fn migrate_grain(&self, grain: &GrainName) -> Result<(), GrainJournalError> {
        self.recover(grain).await?;
        if let Some((at, state)) = snapshot_of(self.local.read(self.shard, grain))
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
        let mut ids: std::collections::BTreeSet<BlobId> =
            self.local.blob_ids(self.shard, grain).into_iter().collect();
        let current_peers: Vec<NodeId> = sets
            .current
            .iter()
            .copied()
            .filter(|&n| n != self.self_node)
            .collect();
        let lists = current_peers.iter().map(|&node| {
            self.transport
                .list_blobs(node, self.shard, grain.clone(), QUORUM_TIMEOUT)
        });
        for list in join_all(lists).await.into_iter().flatten() {
            ids.extend(list);
        }
        // Per target peer: ship what it lacks.
        for node in target.into_iter().filter(|&n| n != self.self_node) {
            let held: std::collections::BTreeSet<BlobId> = self
                .transport
                .list_blobs(node, self.shard, grain.clone(), QUORUM_TIMEOUT)
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
                    .store_blob(node, self.shard, grain.clone(), id, bytes, QUORUM_TIMEOUT)
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
        self.local.seal_range(self.shard, from);
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
                    .seal_range(node, self.shard, from, QUORUM_TIMEOUT);
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
        let reply = self.local.read(self.shard, grain);
        let head = head_from_reply(&reply);
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
            let local = dest_replicas.contains(&self.self_node).then(|| {
                self.local.store_snapshot(
                    dest,
                    grain,
                    at,
                    Term::ZERO,
                    state.clone(),
                    WriteKind::Transfer,
                )
            });
            let peers = self.fan_to_peers(dest_replicas, |node| {
                self.transport.store_snapshot(
                    node,
                    dest,
                    grain.clone(),
                    at,
                    Term::ZERO,
                    state.clone(),
                    WriteKind::Transfer,
                    QUORUM_TIMEOUT,
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
            let local = dest_replicas.contains(&self.self_node).then(|| {
                self.local.store_record(
                    dest,
                    grain,
                    base,
                    Term::ZERO,
                    records.clone(),
                    WriteKind::Transfer,
                )
            });
            let peers = self.fan_to_peers(dest_replicas, |node| {
                self.transport.store_record(
                    node,
                    dest,
                    grain.clone(),
                    base,
                    Term::ZERO,
                    records.clone(),
                    WriteKind::Transfer,
                    QUORUM_TIMEOUT,
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
        local: Option<StoreAck>,
        peers: Vec<StoreAckFuture>,
    ) -> bool {
        let mut acked = usize::from(matches!(local, Some(StoreAck::Stored(_))));
        let need = majority(total);
        let mut pending: FuturesUnordered<StoreAckFuture> = peers.into_iter().collect();
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
    /// leader, tagging each ack with its node for the majority count. The one
    /// differing store call is supplied as `mk`; the filter-self/box/collect
    /// scaffolding is shared by the snapshot and records arms of `transfer_grain`.
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
    /// via recovery-on-access when it returns (the spec's blob-replication path,
    /// proactive re-replication being deferred). The committed records and
    /// snapshot already reached a majority (`transfer_to_majority`), and blobs
    /// reach every reachable dest — a majority whenever one is reachable, which
    /// is exactly when the split can commit at all. Idempotent
    /// (content-addressed); requires this leader's own local copy to land, so
    /// the child leader can always serve.
    async fn transfer_blobs(
        &self,
        grain: &GrainName,
        dest: u32,
        dest_replicas: &[NodeId],
    ) -> Result<(), GrainJournalError> {
        let sets = self.sets();
        // Source ids under THIS shard's keys: local plus every reachable peer.
        let mut ids: std::collections::BTreeSet<BlobId> =
            self.local.blob_ids(self.shard, grain).into_iter().collect();
        let source_peers = self.peers_of(&sets);
        let lists = source_peers.iter().map(|&node| {
            self.transport
                .list_blobs(node, self.shard, grain.clone(), QUORUM_TIMEOUT)
        });
        for list in join_all(lists).await.into_iter().flatten() {
            ids.extend(list);
        }
        for &node in dest_replicas {
            let held: std::collections::BTreeSet<BlobId> = if node == self.self_node {
                self.local.blob_ids(dest, grain).into_iter().collect()
            } else {
                match self
                    .transport
                    .list_blobs(node, dest, grain.clone(), QUORUM_TIMEOUT)
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
                    self.local.put_blob(dest, grain, id, bytes);
                } else if self
                    .transport
                    .store_blob(node, dest, grain.clone(), id, bytes, QUORUM_TIMEOUT)
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

    /// Count a local ack plus the peers' acks toward a quorum (spec §7.2) on the
    /// record path, over [`accumulate_quorum`](Self::accumulate_quorum). A `Stored`
    /// counts; a `Fenced`/`Stale` reply does not but is remembered, so short of a
    /// quorum a single `Fenced` means we are deposed and a `Stale` means the head was
    /// stale — running out of replies with neither is `Unavailable`. A quorum that
    /// stored wins even if a lagging replica also reported a higher term: had a
    /// higher-term leader prepared a quorum, the intersection would have fenced this
    /// store (§8).
    async fn collect_store_quorum(
        &self,
        sets: &ReplicaSets,
        local: StoreAck,
        peers: Vec<StoreAckFuture>,
    ) -> (QuorumOutcome, FuturesUnordered<StoreAckFuture>) {
        let mut fenced = false;
        let mut stale = false;
        let mut sealed = false;
        let local_acked = match local {
            StoreAck::Stored(_) => true,
            StoreAck::Fenced(_) => {
                fenced = true;
                false
            }
            StoreAck::Stale(_) => {
                stale = true;
                false
            }
            StoreAck::Sealed => {
                sealed = true;
                false
            }
        };
        let pending: FuturesUnordered<StoreAckFuture> = peers.into_iter().collect();
        let (satisfied, pending) = self
            .accumulate_quorum(sets, local_acked, pending, |reply| match reply {
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
    // The replies are owned and used only here, so the record and snapshot bytes are
    // moved into the merge, never cloned (recovery runs on every activation).
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

/// A store reply's committed head: the snapshot's seq (the compacted base, `0` if
/// none) plus the leading gap-free run of records above it. Measures both a Local
/// store's head and a recovering leader's local head, each of which may sit over a
/// compacted prefix (§9).
fn head_from_reply(reply: &crate::store::ReadReply) -> Seq {
    let base = reply.snapshot.as_ref().map_or(0, |(s, _, _)| s.value());
    // `slots` is ascending by seq with the compacted prefix absent, so the leading
    // gap-free run above the base ends at the first slot that is not the next seq —
    // a single linear walk, no set to build.
    let mut head = base;
    for (seq, _, _) in &reply.slots {
        if seq.value() != head + 1 {
            break;
        }
        head += 1;
    }
    Seq::new(head)
}

/// A store reply's latest snapshot as `(seq, state)` — the `load_snapshot` seam needs
/// only the seq and the state, not the committing term (§9).
fn snapshot_of(reply: crate::store::ReadReply) -> Option<(Seq, Vec<u8>)> {
    reply.snapshot.map(|(seq, _term, state)| (seq, state))
}
