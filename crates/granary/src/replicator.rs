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

use crate::election::LeaderElection;
use crate::grain::GrainName;
use crate::journal::AppendOutcome;
use crate::journal::GrainJournalError;
use crate::journal::Seq;
use crate::replica_store::ReplicaTransport;
use crate::store::GrainStore;
use crate::store::ReadOutcome;
use crate::store::StoreAck;

/// A pending per-replica store ack from the [`ReplicaTransport`] fan-out.
type StoreAckFuture = actor_core::BoxFuture<'static, Result<StoreAck, actor_core::CallError>>;

/// The result of [`merge`]: the contiguous record prefix, its head, the best
/// snapshot `(seq, term, state)`, and whether any kept record's term is below the
/// recovering leader's term (so a write-back is needed).
type Merged = (Vec<Vec<u8>>, Seq, Option<(Seq, u64, Vec<u8>)>, bool);

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
        match self.store.store_record(self.shard, grain, after, 0, events, false) {
            StoreAck::Stored(head) => AppendOutcome::Committed(head),
            other => AppendOutcome::Unavailable(format!("local store rejected the append: {other:?}")),
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
        match self.store.store_snapshot(self.shard, grain, at, 0, state) {
            StoreAck::Stored(seq) => AppendOutcome::Committed(seq),
            other => AppendOutcome::Unavailable(format!("local store rejected the snapshot: {other:?}")),
        }
    }

    pub(crate) async fn load_snapshot(
        &self,
        grain: &GrainName,
    ) -> Result<Option<(Seq, Vec<u8>)>, GrainJournalError> {
        Ok(snapshot_of(self.store.read(self.shard, grain)))
    }
}

// --- Quorum tier: per-grain quorum append over the shard's replicas ----------

/// The clustered `Quorum` replicator (spec §7.2, §7.4, §8). Holds the shard's
/// leader-election group (for the term and leadership gate), this node's local
/// [`GrainStore`] (the leader is one of the replicas, §5.2), and the
/// [`ReplicaTransport`] to the other replicas.
pub(crate) struct QuorumReplicator<R: RaftConsensus> {
    election: LeaderElection<R>,
    local: Arc<dyn GrainStore>,
    transport: Arc<dyn ReplicaTransport>,
    /// The shard's replica set — the write/recovery quorum domain (§7.1). Fixed for
    /// now (dynamic reconfiguration with grain-data movement is deferred, §7.6).
    replicas: Vec<NodeId>,
    shard: u32,
    self_node: NodeId,
}

impl<R: RaftConsensus> QuorumReplicator<R> {
    pub(crate) fn new(
        election: LeaderElection<R>,
        local: Arc<dyn GrainStore>,
        transport: Arc<dyn ReplicaTransport>,
        replicas: Vec<NodeId>,
        shard: u32,
        self_node: NodeId,
    ) -> QuorumReplicator<R> {
        QuorumReplicator {
            election,
            local,
            transport,
            replicas,
            shard,
            self_node,
        }
    }

    /// The majority of the shard's replicas (§7.2).
    fn quorum(&self) -> usize {
        self.replicas.len() / 2 + 1
    }

    fn not_leader(&self) -> AppendOutcome {
        AppendOutcome::NotLeader(self.election.leader_hint())
    }

    /// The replica nodes other than this one (the leader writes its own store
    /// locally, §5.2).
    fn peers(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.replicas.iter().copied().filter(|&n| n != self.self_node)
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
        let events_len = events.len();
        // Fan out to the remote peers first, each cloning the payload for its own wire
        // message; then write this node's own replica directly, *moving* the payload in
        // — the leader is a replica (§5.2), so its write needs no copy. The batch is
        // deep-cloned R-1 times (once per peer), never R.
        let peers = self
            .peers()
            .map(|node| {
                self.transport.store_record(
                    node,
                    self.shard,
                    grain.clone(),
                    after,
                    term,
                    events.clone(),
                    false,
                    QUORUM_TIMEOUT,
                )
            })
            .collect();
        let local = self.local.store_record(self.shard, grain, after, term, events, false);
        let (outcome, pending) = self.collect_store_quorum(local, peers).await;
        if matches!(outcome, QuorumOutcome::Committed) {
            // Committed on a quorum: return now and drain the slower replicas off the
            // hot path (§7.2), so the append's latency is the quorum's, not the slowest
            // replica's.
            self.drain_in_background(pending);
        } else {
            // The append did not commit: roll back this node's tentative local write
            // so a later stale-local recovery never folds an uncommitted record
            // (§7.2, G5). Peers that stored it keep it, so a quorum can still commit
            // it late (the ambiguous-timeout case, §7.2). `pending` is already drained.
            self.local.truncate(self.shard, grain, after);
        }
        match outcome {
            QuorumOutcome::Committed => {
                AppendOutcome::Committed(Seq::new(after.value() + events_len as u64))
            }
            QuorumOutcome::Fenced => self.not_leader(),
            // A stale head: an up-to-date replica rejected the append (§8). Step down
            // (ambiguous) and re-recover from a quorum on the next activation.
            QuorumOutcome::Stale => {
                AppendOutcome::Unavailable("stale head; reactivating".into())
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
    pub(crate) async fn recover(&self, grain: &GrainName) -> Result<Seq, GrainJournalError> {
        let Some(term) = self.election.term().filter(|_| self.election.is_leader()) else {
            return Err(GrainJournalError::Unavailable("shard electing".into()));
        };

        // Read phase: fence-read local and every peer (awaiting all, so no in-flight
        // ask is dropped — no-silent-loss, §14). Each read is bounded by
        // `RECOVER_TIMEOUT`, so an unreachable peer just falls out of the quorum.
        let local = self.local.prepare(self.shard, grain, term);
        let ReadOutcome::Prepared(local_reply) = local else {
            return Err(GrainJournalError::Unavailable("fenced by a higher term".into()));
        };
        let peer_reads = self.peers().map(|node| {
            self.transport
                .read_grain(node, self.shard, grain.clone(), term, RECOVER_TIMEOUT)
        });
        // Take our local head before moving the reply into the quorum set, so the
        // write-back below can skip the network on a stable re-activation without a
        // second read — and the recovery path never deep-clones the grain's records.
        let local_head = head_from_reply(&local_reply);
        let mut replies = vec![local_reply];
        for result in join_all(peer_reads).await {
            match result {
                Ok(ReadOutcome::Prepared(reply)) => replies.push(reply),
                // A peer promised a higher term: we are deposed, do not serve.
                Ok(ReadOutcome::Fenced(_)) => {
                    return Err(GrainJournalError::Unavailable("fenced by a higher term".into()));
                }
                Err(_) => {}
            }
        }
        let confirmed = replies.len() >= self.quorum();

        // Merge: highest-term record per slot, and the best snapshot. When a quorum
        // was reached this is the authoritative head; otherwise it is just this node's
        // local view — a read-your-leader fallback (§7.5) that may be stale but cannot
        // be unsafe, since a write from it still needs a quorum and a stale-head
        // append is rejected by an up-to-date replica's optimistic check (§8).
        let (records, head, snapshot, any_below) = merge(replies, term);
        // The recovered head's compacted base — the seq of the best snapshot, which
        // the recovered tail records sit above (§9).
        let base = snapshot.as_ref().map_or(Seq::ZERO, |(s, _, _)| *s);

        // Cache the recovered snapshot locally first, so the local store's base is
        // aligned to `base` before the write-back lands the tail above it. Records
        // remain the authority, so the snapshot need not be quorum-durable here.
        if let Some((at, snap_term, state)) = snapshot {
            self.local
                .store_snapshot(self.shard, grain, at, snap_term.max(term), state);
        }

        if confirmed {
            // Write-back phase: make the recovered tail quorum-durable under our term,
            // so no later recovery regresses it (§8) and the local store can serve
            // `load`. The tail sits after `base`; a replica already compacted past it
            // skips the covered records (§8). Skip the network when nothing changed
            // (a stable re-activation: no record below our term, head not advanced).
            if head.value() > base.value() && (any_below || local_head.value() < head.value()) {
                let local =
                    self.local
                        .store_record(self.shard, grain, base, term, records.clone(), true);
                let peers = self
                    .peers()
                    .map(|node| {
                        self.transport.store_record(
                            node,
                            self.shard,
                            grain.clone(),
                            base,
                            term,
                            records.clone(),
                            true,
                            RECOVER_TIMEOUT,
                        )
                    })
                    .collect();
                let (outcome, pending) = self.collect_store_quorum(local, peers).await;
                match outcome {
                    QuorumOutcome::Committed => self.drain_in_background(pending),
                    QuorumOutcome::Fenced => {
                        return Err(GrainJournalError::Unavailable("fenced by a higher term".into()));
                    }
                    QuorumOutcome::Stale | QuorumOutcome::Unavailable => {
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
        // Clone the state for each remote peer's wire message, then move it into this
        // node's own replica write (§5.2) — R-1 copies, not R.
        let peers = self
            .peers()
            .map(|node| {
                self.transport.store_snapshot(
                    node,
                    self.shard,
                    grain.clone(),
                    at,
                    term,
                    state.clone(),
                    QUORUM_TIMEOUT,
                )
            })
            .collect();
        let local = self.local.store_snapshot(self.shard, grain, at, term, state);
        let (outcome, pending) = self.collect_store_quorum(local, peers).await;
        match outcome {
            QuorumOutcome::Committed => {
                self.drain_in_background(pending);
                AppendOutcome::Committed(at)
            }
            QuorumOutcome::Fenced => self.not_leader(),
            QuorumOutcome::Stale | QuorumOutcome::Unavailable => {
                AppendOutcome::Unavailable("snapshot did not reach a quorum".into())
            }
        }
    }

    /// Count a local ack plus the peers' acks toward a quorum (spec §7.2), returning as
    /// soon as a quorum has stored — the commit waits on the quorum, never on the
    /// slowest replica. The not-yet-resolved peer asks come back with the outcome for
    /// the caller to [`drain_in_background`](Self::drain_in_background): each still runs
    /// to completion off the hot path, so its `AskIssued`/`AskOutcome` bracket closes
    /// (no-silent-loss, §14). A single `Fenced` short of a quorum means we are deposed;
    /// running out of replies short of a quorum is `Unavailable`. On any non-committed
    /// outcome the loop has drained every peer, so the returned set is empty.
    async fn collect_store_quorum(
        &self,
        local: StoreAck,
        peers: Vec<StoreAckFuture>,
    ) -> (QuorumOutcome, FuturesUnordered<StoreAckFuture>) {
        let quorum = self.quorum();
        let mut stored = 0usize;
        let mut fenced = false;
        let mut stale = false;
        match local {
            StoreAck::Stored(_) => stored += 1,
            StoreAck::Fenced(_) => fenced = true,
            StoreAck::Stale(_) => stale = true,
        }
        let mut pending: FuturesUnordered<StoreAckFuture> = peers.into_iter().collect();
        // A quorum that stored wins even if a lagging replica also reported a higher
        // term: had a higher-term leader prepared a quorum, the intersection would
        // have fenced this store (§8). The local write alone may already satisfy a
        // single-replica quorum.
        if stored >= quorum {
            return (QuorumOutcome::Committed, pending);
        }
        while let Some(result) = pending.next().await {
            match result {
                Ok(StoreAck::Stored(_)) => stored += 1,
                Ok(StoreAck::Fenced(_)) => fenced = true,
                Ok(StoreAck::Stale(_)) => stale = true,
                Err(_) => {}
            }
            if stored >= quorum {
                return (QuorumOutcome::Committed, pending);
            }
        }
        let outcome = if fenced {
            QuorumOutcome::Fenced
        } else if stale {
            QuorumOutcome::Stale
        } else {
            QuorumOutcome::Unavailable
        };
        (outcome, pending)
    }

    /// Drive the leftover peer asks of a committed quorum store to completion off the
    /// hot path (spec §7.2). Launched as a detached task, so the commit returns at
    /// quorum latency while every issued ask still closes its `AskIssued`/`AskOutcome`
    /// bracket (no-silent-loss, §14). A late `Stored` is harmless (the slot already
    /// holds the record); a late `Fenced` cannot un-commit a quorum-durable write (§8).
    fn drain_in_background(&self, mut pending: FuturesUnordered<StoreAckFuture>) {
        if pending.is_empty() {
            return;
        }
        self.transport.launch(Box::pin(async move {
            while pending.next().await.is_some() {}
        }));
    }
}

/// The outcome of a quorum store/append (spec §7.2, §8, §11).
enum QuorumOutcome {
    Committed,
    Fenced,
    Stale,
    Unavailable,
}

/// Merge a quorum of recovery reads by **highest-term-per-slot** (spec §8): for each
/// `Seq` slot, keep the record carried under the highest term any replica holds.
/// Returns the contiguous record prefix (ascending bytes), its head, the best
/// snapshot, and whether any kept record's term is below `our_term` (so a write-back
/// under our term is needed). A gap ends the prefix — an uncommitted tail, dropped.
fn merge(replies: Vec<crate::store::ReadReply>, our_term: u64) -> Merged {
    use std::collections::BTreeMap;
    let mut best: BTreeMap<u64, (u64, Vec<u8>)> = BTreeMap::new();
    let mut snapshot: Option<(Seq, u64, Vec<u8>)> = None;
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
