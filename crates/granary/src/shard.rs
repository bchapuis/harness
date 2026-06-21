//! The Tier-2 Raft-backed journal (spec §7, §7.4 tier 2).
//!
//! A shard is one Raft group; [`RaftJournal`] implements the [`Journal`] seam
//! over it via the [`RaftLog`] capability of a clustered system. A grain write
//! becomes one committed log entry (the engine's `EntryPayload::App` bytes, the
//! opaque-bytes seam granary already uses), so it is durable on a quorum (§7.2)
//! and survives leader failover (leader completeness, invariant **G14**).
//!
//! **The append protocol.** `append` runs only on the shard leader (a follower
//! returns `NotLeader`, the §8 single-writer fence). It tags the record with a
//! [`ProposalId`], **registers a waiter before proposing** (lost-wakeup guard),
//! proposes, and awaits the commit — bounded by a timeout that surfaces quorum
//! loss as `Unavailable` (§11). A background task consumes the committed stream
//! and builds a per-grain **projection** (the same on every replica, so a new
//! leader is already caught up); applying a committed `Append` completes its
//! waiter. The projection dedups by [`ProposalId`], so a timed-out append whose
//! entry commits later is not double-applied (the idempotence analogue of the
//! membership merge's stamp rule).
//!
//! **Restart-unique proposal ids.** A [`ProposalId`] is `(proposer, epoch,
//! nonce)`. The `nonce` counts from zero per journal instance; the `epoch` is
//! drawn once from the system entropy at construction, so a node that crashes and
//! re-starts (reusing its stable `NodeId`) and is re-elected gets a *fresh* epoch
//! and never collides with a prior incarnation's already-applied ids — which would
//! otherwise make the dedup silently swallow its new writes.
//!
//! **The rehydration barrier.** [`catch_up`](RaftJournal::catch_up) holds a
//! grain's head read until the apply loop has folded the commit stream up to the
//! leader's *established* commit — the highest committed index once the current
//! leader has committed in its term (`group_ready_commit`). It compares that
//! target against a commit watermark the apply loop publishes (`Committed::commit`),
//! both indices on the same ordered stream, so a freshly-activated grain never
//! rebuilds from a still-draining projection (spec §9, invariant **G3**/**G14**).
//! This holds across a **full cluster restart**, where every group must re-elect
//! before its restored log is re-committed: the barrier waits out that window
//! rather than reading an as-yet-empty projection.
//!
//! This journal backs a running `Granary` over a cluster: grain activation on the
//! shard leader and cross-node routing go through the [`Gateway`](crate::gateway)
//! and the consensus-agreed [`ShardMapSource`](crate::ShardMapSource).

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_cluster::Committed;
use actor_cluster::GroupId;
use actor_cluster::RaftLog;
use actor_core::NodeId;
use async_channel::Receiver;
use futures::channel::oneshot;
use futures::future::Either;
use futures::future::select;
use serde::Deserialize;
use serde::Serialize;

use crate::grain::GrainName;
use crate::journal::AppendOutcome;
use crate::journal::Journal;
use crate::journal::JournalError;
use crate::journal::Seq;
use crate::journal::head_of;
use crate::journal::slice;

/// How long `append` waits for its entry to commit before reporting
/// `Unavailable` (§11). Generous relative to election timeouts: a commit that
/// has not landed within it means the shard cannot reach a quorum.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(10);

/// How often [`catch_up`](RaftJournal::catch_up) re-checks whether the commit
/// stream has drained. Short relative to a commit, so the rehydration barrier adds
/// negligible latency.
const CATCH_UP_POLL: Duration = Duration::from_millis(10);

/// The bound on the rehydration barrier's polls — `COMMIT_TIMEOUT` worth — so a
/// pathological backlog can never wedge an activation indefinitely.
const CATCH_UP_MAX_POLLS: u32 = (COMMIT_TIMEOUT.as_millis() / CATCH_UP_POLL.as_millis()) as u32;

/// The identity of one append proposal (spec §7.2): `(proposer, epoch, nonce)`.
/// The `nonce` counts from zero within one journal instance; the `epoch`, drawn
/// from entropy at construction, makes the id unique across restarts of the same
/// `NodeId`, so a re-elected node never reuses a prior incarnation's id. It is the
/// commit-once dedup key, the analogue of the membership merge's stamp rule.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct ProposalId {
    proposer: NodeId,
    epoch: u64,
    nonce: u64,
}

/// One grain's slice of the shard projection: its committed events (in `Seq`
/// order), latest snapshot, and the [`ProposalId`]s already applied (commit-once
/// dedup).
///
/// `Serialize`/`Deserialize` so the whole projection can be captured as a Raft
/// state-machine snapshot (§9) and shipped to a lagging replica via InstallSnapshot
/// or reloaded after compaction.
#[derive(Default, Serialize, Deserialize)]
struct GrainLog {
    events: Vec<Vec<u8>>,
    snapshot: Option<(Seq, Vec<u8>)>,
    applied: HashSet<ProposalId>,
}

/// How many committed records accumulate before a replica compacts its shard's
/// Raft log against a fresh projection snapshot (§9). Local and uncoordinated:
/// every replica compacts its own log, so the engine can bootstrap a new replica
/// from one snapshot install instead of replaying the whole history.
const COMPACT_EVERY: u64 = 64;

/// One journal command. The application command bytes of the group's Raft log.
#[derive(Serialize, Deserialize)]
enum JournalCommand {
    /// A grain's atomic event batch, tagged with its [`ProposalId`] so the
    /// proposer's `append` waiter can be completed (and re-applies deduped) on
    /// commit. The id is unique shard-wide, even across leaders and restarts.
    Append {
        grain: GrainName,
        events: Vec<Vec<u8>>,
        id: ProposalId,
    },
    /// A grain snapshot at a committed seq (§9).
    Snapshot {
        grain: GrainName,
        at: u64,
        state: Vec<u8>,
    },
}

fn encode(command: &JournalCommand) -> Vec<u8> {
    serde_json::to_vec(command).expect("a JournalCommand always serializes")
}

fn decode(bytes: &[u8]) -> Option<JournalCommand> {
    serde_json::from_slice(bytes).ok()
}

type Projection = Arc<Mutex<BTreeMap<GrainName, GrainLog>>>;
type Waiters = Arc<Mutex<HashMap<u64, oneshot::Sender<Seq>>>>;

struct Inner<R: RaftLog> {
    consensus: R,
    group: GroupId,
    self_node: NodeId,
    /// This instance's epoch (spec §7.2): drawn from entropy at construction so a
    /// re-started node's proposal ids never collide with a prior incarnation's.
    epoch: u64,
    projection: Projection,
    waiters: Waiters,
    next_nonce: AtomicU64,
    /// The commit high-water mark the apply loop has folded from the stream
    /// (`Committed::commit`). The rehydration barrier
    /// ([`catch_up`](RaftJournal::catch_up)) waits for it to reach the leader's
    /// established commit, so a freshly-activated grain never rebuilds from a
    /// still-draining projection — the cold-restart race (spec §9, invariant
    /// **G3**/**G14**). Folded monotonically, so it never regresses.
    seen: Arc<AtomicU64>,
}

/// A [`Journal`] backed by a Raft group (spec §7.4 tier 2). Cloning shares one
/// projection and one consensus handle.
pub struct RaftJournal<R: RaftLog> {
    inner: Arc<Inner<R>>,
}

impl<R: RaftLog> Clone for RaftJournal<R> {
    fn clone(&self) -> Self {
        RaftJournal {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<R: RaftLog> RaftJournal<R> {
    /// Build a journal over `group` on `consensus`. Subscribes to the group's
    /// committed stream and launches the apply task **now**, so the projection
    /// sees the log from its first entry (subscribe-before-drive, per
    /// [`RaftLog::subscribe_commits`]). The caller must have `create_group`'d the
    /// group first.
    pub fn new(consensus: R, group: GroupId) -> RaftJournal<R> {
        let self_node = consensus.node();
        let epoch = consensus.next_u64();
        let projection: Projection = Arc::new(Mutex::new(BTreeMap::new()));
        let waiters: Waiters = Arc::new(Mutex::new(HashMap::new()));
        // The commit watermark the apply loop folds from the stream; the
        // rehydration barrier reads it to know the projection is current.
        let seen = Arc::new(AtomicU64::new(0));
        let commits = consensus.subscribe_commits(group);
        consensus.launch(Box::pin(apply_loop(
            consensus.clone(),
            group,
            self_node,
            epoch,
            commits,
            Arc::clone(&projection),
            Arc::clone(&waiters),
            Arc::clone(&seen),
        )));
        RaftJournal {
            inner: Arc::new(Inner {
                consensus,
                group,
                self_node,
                epoch,
                projection,
                waiters,
                next_nonce: AtomicU64::new(0),
                seen,
            }),
        }
    }

    /// The `NotLeader` outcome with the best leader hint (the known leader, or
    /// this node if none is known yet).
    fn not_leader(&self) -> AppendOutcome {
        let hint = self
            .inner
            .consensus
            .group_leader(self.inner.group)
            .unwrap_or(self.inner.self_node);
        AppendOutcome::NotLeader(hint)
    }
}

/// Serialize the whole projection as a Raft state-machine snapshot (§9). A `Vec`
/// of pairs rather than a map so the codec needs no string-keyed-map support; the
/// `BTreeMap` iteration is ordered, but correctness does not depend on it (each
/// replica deserializes its own copy).
fn snapshot_projection(projection: &BTreeMap<GrainName, GrainLog>) -> Vec<u8> {
    let pairs: Vec<(&GrainName, &GrainLog)> = projection.iter().collect();
    serde_json::to_vec(&pairs).expect("a projection always serializes")
}

/// Rebuild a projection from a snapshot produced by [`snapshot_projection`].
fn restore_projection(bytes: &[u8]) -> BTreeMap<GrainName, GrainLog> {
    let pairs: Vec<(GrainName, GrainLog)> =
        serde_json::from_slice(bytes).unwrap_or_default();
    pairs.into_iter().collect()
}

/// Consume the committed stream and fold each observation into the projection,
/// completing the matching `append` waiter (spec §7.2). Periodically compacts the
/// shard's Raft log against a fresh projection snapshot (§9), and on a
/// `Committed::Snapshot` installed by the engine replaces the projection wholesale
/// (a freshly added or lagging replica catching up without replaying the log).
/// Runs until the journal is dropped and the channel closes.
#[allow(clippy::too_many_arguments)]
async fn apply_loop<R: RaftLog>(
    consensus: R,
    group: GroupId,
    self_node: NodeId,
    self_epoch: u64,
    commits: Receiver<Committed>,
    projection: Projection,
    waiters: Waiters,
    seen: Arc<AtomicU64>,
) {
    // The highest index covered by our latest local compaction; bounds how often
    // we re-snapshot.
    let mut last_compacted: u64 = 0;
    while let Ok(observation) = commits.recv().await {
        // The commit high-water mark this observation carries; published to `seen`
        // only *after* the projection has folded it, so the rehydration barrier
        // ([`catch_up`]) never observes "caught up" ahead of the state it gates on.
        // Monotonic by the stream's commit order; `fetch_max` guards regardless.
        let watermark = observation.commit();
        let (index, bytes) = match observation {
            Committed::Apply { index, command, .. } => (index, command),
            Committed::Snapshot { index, snapshot, .. } => {
                // The engine installed a leader's snapshot: replace our state with
                // it. Any pending local waiters cannot be completed from a remote
                // snapshot — they fall back to their commit timeout.
                *projection.lock().expect("projection mutex poisoned") =
                    restore_projection(&snapshot);
                last_compacted = index;
                seen.fetch_max(watermark, Ordering::Release);
                continue;
            }
        };
        let Some(command) = decode(&bytes) else {
            // Unparseable, but still committed state: advance the watermark so the
            // barrier counts it rather than waiting on an entry it will never fold.
            seen.fetch_max(watermark, Ordering::Release);
            continue;
        };
        match command {
            JournalCommand::Append { grain, events, id } => {
                // Apply once per `ProposalId`: a timed-out append whose entry
                // commits later must not double-apply (commit-once, the analogue
                // of the membership stamp rule).
                let head = {
                    let mut projection = projection.lock().expect("projection mutex poisoned");
                    let log = projection.entry(grain).or_default();
                    if log.applied.insert(id) {
                        log.events.extend(events);
                        Some(head_of(&log.events))
                    } else {
                        None
                    }
                };
                // Complete the waiter only for *our own* live proposals: this
                // instance is the proposer and the epoch matches, so a prior
                // incarnation's replayed id (same node, older epoch) never
                // spuriously completes a current waiter that reused its nonce.
                if let Some(head) = head {
                    if id.proposer == self_node && id.epoch == self_epoch {
                        if let Some(waiter) =
                            waiters.lock().expect("waiters mutex poisoned").remove(&id.nonce)
                        {
                            let _ = waiter.send(head);
                        }
                    }
                }
            }
            JournalCommand::Snapshot { grain, at, state } => {
                let mut projection = projection.lock().expect("projection mutex poisoned");
                projection.entry(grain).or_default().snapshot = Some((Seq::new(at), state));
            }
        }
        // The projection now reflects this observation; publish the watermark so
        // the rehydration barrier can count it.
        seen.fetch_max(watermark, Ordering::Release);
        // Compact once enough records have accumulated since the last snapshot.
        // The projection now reflects exactly the committed prefix through
        // `index` (the stream is sequential), so it is a valid snapshot at it.
        if index >= last_compacted + COMPACT_EVERY {
            let snapshot = {
                let projection = projection.lock().expect("projection mutex poisoned");
                snapshot_projection(&projection)
            };
            consensus.compact(group, index, snapshot);
            last_compacted = index;
        }
    }
}

impl<R: RaftLog> Journal for RaftJournal<R> {
    async fn append(&self, grain: &GrainName, _after: Seq, events: Vec<Vec<u8>>) -> AppendOutcome {
        // Tier 2 ignores `after` (which `MemoryJournal` asserts against its head):
        // commit order in the shard's Raft log is the authority for `Seq`, the
        // single-writer fence is leadership, and re-applies are deduped by nonce —
        // so the caller's optimistic head is neither needed nor trusted here.
        // The single-writer fence (§8): only the leader appends.
        if !self.inner.consensus.group_is_leader(self.inner.group) {
            return self.not_leader();
        }
        let nonce = self.inner.next_nonce.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel::<Seq>();
        // Register the waiter BEFORE proposing, or a fast commit could complete a
        // nonce not yet registered (lost wakeup).
        self.inner
            .waiters
            .lock()
            .expect("waiters mutex poisoned")
            .insert(nonce, tx);

        let command = JournalCommand::Append {
            grain: grain.clone(),
            events,
            id: ProposalId {
                proposer: self.inner.self_node,
                epoch: self.inner.epoch,
                nonce,
            },
        };
        self.inner
            .consensus
            .propose_to(self.inner.group, encode(&command))
            .await;

        // Await the commit, bounded by the timeout (quorum loss → Unavailable).
        let timeout = self.inner.consensus.sleep(COMMIT_TIMEOUT);
        match select(rx, timeout).await {
            Either::Left((Ok(head), _)) => AppendOutcome::Committed(head),
            // The waiter sender was dropped without sending — the journal is
            // shutting down; report it as a non-commit rather than hang.
            Either::Left((Err(_), _)) => AppendOutcome::Unavailable("commit waiter canceled".into()),
            Either::Right(((), _)) => {
                self.inner
                    .waiters
                    .lock()
                    .expect("waiters mutex poisoned")
                    .remove(&nonce);
                AppendOutcome::Unavailable("commit timeout".into())
            }
        }
    }

    async fn load(
        &self,
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<(Seq, Vec<u8>)>, JournalError> {
        let projection = self.inner.projection.lock().expect("projection mutex poisoned");
        let Some(log) = projection.get(grain) else {
            return Ok(Vec::new());
        };
        Ok(slice(&log.events, from, limit))
    }

    async fn head(&self, grain: &GrainName) -> Result<Seq, JournalError> {
        let projection = self.inner.projection.lock().expect("projection mutex poisoned");
        Ok(projection
            .get(grain)
            .map(|log| head_of(&log.events))
            .unwrap_or(Seq::ZERO))
    }

    async fn save_snapshot(&self, grain: &GrainName, at: Seq, state: Vec<u8>) -> AppendOutcome {
        if !self.inner.consensus.group_is_leader(self.inner.group) {
            return self.not_leader();
        }
        // Best-effort (§9, G4): the snapshot is only an optimization, so we
        // propose it and report success without blocking on its commit. The
        // projection applies it when it lands; the journal stays the authority.
        let command = JournalCommand::Snapshot {
            grain: grain.clone(),
            at: at.value(),
            state,
        };
        self.inner
            .consensus
            .propose_to(self.inner.group, encode(&command))
            .await;
        AppendOutcome::Committed(at)
    }

    async fn load_snapshot(
        &self,
        grain: &GrainName,
    ) -> Result<Option<(Seq, Vec<u8>)>, JournalError> {
        let projection = self.inner.projection.lock().expect("projection mutex poisoned");
        Ok(projection.get(grain).and_then(|log| log.snapshot.clone()))
    }

    async fn catch_up(&self) {
        // The rehydration barrier (spec §9, G3/G14): wait until this projection
        // reflects the leader's *established* commit, so a grain that just
        // activated rebuilds from the whole committed log rather than a prefix.
        //
        // [`ready_commit`](actor_cluster::RaftLog::group_ready_commit) is the
        // highest committed application index, available only once the current
        // leader has committed in its term; `seen` is the commit watermark the
        // apply loop has folded from the stream. Both are indices on the same
        // ordered stream, so comparing them is race-free — unlike the old "is the
        // stream momentarily empty?" check, which a cold restart defeated: a
        // restarted group's restored log is re-committed only after its new leader
        // commits its term-opening Noop, well after a grain might first activate,
        // so the stream is briefly empty while still far behind.
        //
        // While no current-term commit exists (a fresh election still settling, or
        // a leaderless minority under partition) the target is `None` and we keep
        // polling. The bound stops a partition from wedging the activation: it then
        // falls through and the host serves the read from the local projection
        // (the §7.5 stale-read contract); the §6 contiguity guard catches residue.
        for _ in 0..CATCH_UP_MAX_POLLS {
            if let Some(target) = self.inner.consensus.group_ready_commit(self.inner.group) {
                if self.inner.seen.load(Ordering::Acquire) >= target {
                    return;
                }
            }
            self.inner.consensus.sleep(CATCH_UP_POLL).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The commit-once dedup (spec §7.2) must distinguish a re-started node's
    /// proposals from a prior incarnation's. Both reuse `nonce` 0 (each instance
    /// counts from zero), and a re-started node reuses its stable `NodeId`, so only
    /// the per-instance `epoch` keeps the id spaces disjoint. Without it the third
    /// insert below would be deduped and the restarted node's first write swallowed.
    #[test]
    fn commit_once_dedup_distinguishes_incarnations_by_epoch() {
        let node = NodeId::new(1);
        let mut log = GrainLog::default();

        // The old incarnation (epoch 100) commits its first event.
        assert!(log.applied.insert(ProposalId { proposer: node, epoch: 100, nonce: 0 }));
        // Re-applying the SAME id is deduped — a timed-out append that commits late
        // must not double-apply.
        assert!(!log.applied.insert(ProposalId { proposer: node, epoch: 100, nonce: 0 }));
        // The re-started incarnation (same node, same nonce, FRESH epoch) is a
        // distinct id: it is NOT deduped, so its first write is not swallowed.
        assert!(log.applied.insert(ProposalId { proposer: node, epoch: 200, nonce: 0 }));
    }
}
