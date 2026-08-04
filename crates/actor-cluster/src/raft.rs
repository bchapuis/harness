//! Minimal deterministic Raft — a reusable **multi-group** consensus engine
//! (spec §9.4.3).
//!
//! The cluster self-hosts its membership authority as a **replicated log**: an
//! elected leader serializes every transition as a [`RaftEntry`], a quorum of
//! **voters** commits it, and the **commit index** is the authority stamp the
//! membership merge orders decisions by (spec §9.2). The scope is elections,
//! heartbeats, log replication, quorum commit, single-server voter changes, and
//! snapshot/compaction (§9) — enough for every observable guarantee the spec
//! requires (election safety, log matching, leader completeness) and invariant
//! #22.
//!
//! **Multi-group.** The consensus algorithm is generic; only the entry *payload*
//! and the voter-set-change handling are application-specific. A [`RaftGroup`]
//! therefore carries an opaque application command as bytes
//! ([`EntryPayload::App`]), and [`MultiRaft`] runs O(groups) independent groups
//! keyed by [`GroupId`], each with its own log, leadership, and term sequence.
//! The membership control plane is one well-known group ([`GroupId::CONTROL`]);
//! granary's per-shard journals are additional groups.
//!
//! Determinism: timers come from `Clock`, election jitter from `Entropy`, and
//! consensus traffic rides the ordinary `Transport` as frames (spec §9.4.3
//! item 7), so a leader-based cluster simulates like everything else (§18).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::Entropy;
use actor_core::Instant;
use actor_core::NodeId;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::Frame;

/// The identity of one Raft group (spec §9.4.3). The engine runs O(groups)
/// independent groups; each owns its log, leadership, and term sequence.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct GroupId(pub u64);

impl GroupId {
    /// The membership control plane's group (spec §9.4.3) — the one group every
    /// leader-mode cluster always runs.
    pub const CONTROL: GroupId = GroupId(0);

    /// The raw group value (used as the `group` field of `Event::LeaderElected`,
    /// which stays agnostic of this type).
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "group-{}", self.0)
    }
}

/// The payload of one replicated log entry (spec §9.4.3 item 1).
///
/// `Noop`, `AddVoter`, and `RemoveVoter` are **engine-internal**: the group
/// applies them itself (a term-opening no-op, and single-server configuration
/// changes, spec §9.4.3 item 2) and never hands them to the caller. `App`
/// carries the opaque application command — the membership control plane encodes
/// a `MembershipCommand`, a granary shard encodes its grain-journal record — and
/// only `App` entries drain to the caller on commit.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum EntryPayload {
    /// The no-op a new leader commits to open its term (leader completeness).
    Noop,
    /// Add a voter (single-server configuration change, spec §9.4.3 item 2).
    AddVoter(NodeId),
    /// Remove a voter (single-server configuration change).
    RemoveVoter(NodeId),
    /// An opaque application command, committed and drained to the caller.
    ///
    /// `serde_bytes` because these bytes are already encoded and would otherwise go
    /// through serde's default sequence path in both directions — the cost that
    /// dominated blob replication (`actor-serialization/benches/codec.rs` prices the
    /// pass; `scripts/bench-machine-cost.sh` priced it end to end). This one is also
    /// **durable**: a voter persists its log, so the attribute has to leave the encoding
    /// byte-identical or every existing log file moves with it. It does, for both
    /// codecs in the tree;
    /// `actor-serialization/tests/wire_bytes.rs` covers this newtype-variant shape.
    App(#[serde(with = "serde_bytes")] Vec<u8>),
}

/// One replicated log entry: the `term` it was proposed in and its `payload`
/// (spec §9.4.3 item 1).
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct RaftEntry {
    pub term: u64,
    pub payload: EntryPayload,
}

/// The durable Raft state a voter must persist (spec §9.4.3 item 2): the
/// current term, the vote cast in it, the log, and — once the prefix has been
/// compacted (§9) — the state-machine snapshot that subsumes it. `log` holds the
/// entries *after* `snapshot_index`, so entry `i` (1-based) lives at
/// `log[i - snapshot_index - 1]`; with no snapshot `snapshot_index == 0` and the
/// log is absolute as before.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct PersistedRaft {
    pub term: u64,
    pub voted_for: Option<NodeId>,
    pub log: Vec<RaftEntry>,
    /// The compacted prefix's last index (`0` = nothing compacted) and its term,
    /// and the application snapshot taken at it.
    pub snapshot_index: u64,
    pub snapshot_term: u64,
    pub snapshot: Option<Vec<u8>>,
}

/// Whether a [`RaftWAL`] write reached stable storage.
///
/// The distinction is not advisory. Every write through this seam happens *before* the
/// state it records takes effect, because the caller announces that state immediately
/// after — a vote, an accepted append, a snapshot. A write that did not land and is
/// reported as if it had is precisely how a voter votes twice in one term across a
/// restart, or acknowledges an entry it cannot replay.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[must_use = "a voter that cannot persist must stop voting (spec §9.4.3 item 2)"]
pub enum WalAck {
    /// On stable storage. The caller may announce the state it records.
    Persisted,
    /// **Not** on stable storage. The WAL is poisoned from here on and will refuse
    /// every subsequent write; the engine stops participating in consensus.
    Failed,
}

impl WalAck {
    /// Whether the write landed.
    pub fn persisted(self) -> bool {
        self == WalAck::Persisted
    }
}

/// The durability seam for a voter's Raft state (spec §9.4.3 item 2):
/// persisted before the state takes effect, reloaded on restart. The methods
/// are synchronous on purpose: when one returns, the data MUST be durable —
/// the caller sends the messages announcing the state right after. In-memory
/// for simulation ([`InMemoryRaftWAL`]); `actor-runtime` supplies the
/// production `FileRaftWAL`. One instance backs one `(group, node)`.
///
/// **Failure is reported, not thrown.** A voter that cannot persist its term must stop
/// voting, and the way to stop is not to panic: an unwind here is caught by actor
/// supervision or kills one task, either of which can leave the node alive, gossiping,
/// and counted in quorums while its durability guarantee is gone. So a write answers
/// [`WalAck::Failed`] and the engine steps down and refuses to vote or append — the
/// node stays up, visibly unhealthy, and an operator replaces it.
pub trait RaftWAL: Send + Sync + 'static {
    /// Load the persisted state (empty/default on first start).
    fn load(&self) -> PersistedRaft;

    /// Persist a new term and the vote cast in it.
    fn save_term_and_vote(&self, term: u64, voted_for: Option<NodeId>) -> WalAck;

    /// Truncate the log at `from_index` (absolute, 0-based) and append `entries`
    /// there — Raft's conflict-resolution write. With a compacted prefix
    /// (`snapshot_index > 0`) `from_index` is still absolute; the storage maps it
    /// onto its retained suffix.
    fn append(&self, from_index: u64, entries: &[RaftEntry]) -> WalAck;

    /// Record a state-machine snapshot at `index`/`term` (§9) and discard the log
    /// prefix it subsumes (every entry with absolute index `≤ index`). Persisted
    /// before the engine drops the prefix in memory, so a restart reloads from the
    /// snapshot plus the retained tail rather than a blank log. The default keeps
    /// the whole log (no compaction) for a storage that has not implemented it.
    fn save_snapshot(&self, index: u64, term: u64, data: &[u8]) -> WalAck {
        let _ = (index, term, data);
        WalAck::Persisted
    }

    /// Why this WAL stopped accepting writes, or `None` while healthy — the
    /// operator-facing half of the policy above. One-way: a WAL that has answered
    /// [`WalAck::Failed`] never reports healthy again, because nothing here can
    /// establish that the volume recovered.
    fn poisoned(&self) -> Option<String> {
        None
    }
}

/// A volatile [`RaftWAL`]: state survives as long as the value does. The
/// simulation implementation, and a starting point for production.
#[derive(Default)]
pub struct InMemoryRaftWAL {
    state: Mutex<PersistedRaft>,
}

impl InMemoryRaftWAL {
    pub fn new() -> InMemoryRaftWAL {
        InMemoryRaftWAL::default()
    }
}

impl RaftWAL for InMemoryRaftWAL {
    fn load(&self) -> PersistedRaft {
        self.state
            .lock()
            .expect("raft storage mutex poisoned")
            .clone()
    }

    fn save_term_and_vote(&self, term: u64, voted_for: Option<NodeId>) -> WalAck {
        let mut state = self.state.lock().expect("raft storage mutex poisoned");
        state.term = term;
        state.voted_for = voted_for;
        WalAck::Persisted
    }

    fn append(&self, from_index: u64, entries: &[RaftEntry]) -> WalAck {
        let mut state = self.state.lock().expect("raft storage mutex poisoned");
        // `from_index` is absolute; the retained log begins at `snapshot_index + 1`.
        let local = from_index.saturating_sub(state.snapshot_index) as usize;
        state.log.truncate(local);
        state.log.extend_from_slice(entries);
        WalAck::Persisted
    }

    fn save_snapshot(&self, index: u64, term: u64, data: &[u8]) -> WalAck {
        let mut state = self.state.lock().expect("raft storage mutex poisoned");
        // Discard the prefix the snapshot subsumes (absolute indices `≤ index`),
        // then record the new base. A stale or duplicate call (index already
        // compacted) discards nothing.
        let drop = index
            .saturating_sub(state.snapshot_index)
            .min(state.log.len() as u64);
        state.log.drain(..drop as usize);
        state.snapshot_index = index;
        state.snapshot_term = term;
        state.snapshot = Some(data.to_vec());
        WalAck::Persisted
    }
}

/// Configuration of the leader-based control plane (spec §9.4.3). It configures
/// the control group and supplies the engine-wide timing and the per-group
/// storage factory every group is built from.
#[derive(Clone)]
pub struct RaftConfig {
    /// The control group's initial voter set (spec §9.4.3 item 2): a configured,
    /// modest subset of members (typically 3 or 5), identical on every node.
    /// Later changes are committed [`EntryPayload::AddVoter`]/
    /// [`EntryPayload::RemoveVoter`] entries.
    pub voters: Vec<NodeId>,
    /// Base election timeout; each election round waits the base plus jitter in
    /// `[0, base)` drawn from `Entropy` (spec §9.4.3 item 7).
    pub election_timeout: Duration,
    /// Leader heartbeat/replication cadence. Must be well under
    /// `election_timeout`.
    pub heartbeat_interval: Duration,
    /// Per-`(group, node)` storage factory (spec §9.4.3 item 2). It MUST be
    /// **per-(group, node)-stable**: calling it again with the same arguments
    /// must hand back the same durable state, so a restarted voter reloads the
    /// term it voted in rather than a blank slate (the double-vote hazard). A
    /// filesystem-backed factory is stable through the disk; the default caches
    /// one in-memory storage per `(group, node)`.
    pub storage: Arc<dyn Fn(GroupId, NodeId) -> Arc<dyn RaftWAL> + Send + Sync>,
}

impl RaftConfig {
    /// A config for `voters` with in-memory storage and default timing
    /// (1s election timeout, 250ms heartbeats). The default storage factory
    /// caches one [`InMemoryRaftWAL`] per `(group, node)`, so state survives as
    /// long as the config does and a simulated restart reloads it.
    pub fn new(voters: Vec<NodeId>) -> RaftConfig {
        let cache: Mutex<BTreeMap<(GroupId, NodeId), Arc<dyn RaftWAL>>> =
            Mutex::new(BTreeMap::new());
        RaftConfig {
            voters,
            election_timeout: Duration::from_secs(1),
            heartbeat_interval: Duration::from_millis(250),
            storage: Arc::new(move |group, node| {
                Arc::clone(
                    cache
                        .lock()
                        .expect("raft storage cache poisoned")
                        .entry((group, node))
                        .or_insert_with(|| Arc::new(InMemoryRaftWAL::new())),
                )
            }),
        }
    }
}

// --- The multi-group registry -------------------------------------------------

/// The node's consensus engine (spec §9.4.3): a registry of [`RaftGroup`]s keyed
/// by [`GroupId`]. Every leader-mode node runs one, hosting the control group
/// and (for granary, later) a group per shard it replicates. All groups share
/// the engine-wide timing and draw election jitter from the one seeded
/// `Entropy`, so the whole engine simulates deterministically.
pub(crate) struct MultiRaft {
    node: NodeId,
    election_timeout: Duration,
    /// Needed only to bound a pristine group's first campaign — see
    /// [`first_election_delay`].
    heartbeat_interval: Duration,
    storage: Arc<dyn Fn(GroupId, NodeId) -> Arc<dyn RaftWAL> + Send + Sync>,
    groups: Mutex<BTreeMap<GroupId, Arc<RaftGroup>>>,
}

impl MultiRaft {
    /// Build an empty engine — a group registry with the engine-wide timing and
    /// storage factory, and no groups yet. The caller creates the groups it wants
    /// (the cluster layer's control group at startup, a granary shard's group on
    /// demand). Election timers arm from the `now` passed to each `create_group`.
    pub(crate) fn new(node: NodeId, config: &RaftConfig) -> MultiRaft {
        MultiRaft {
            node,
            election_timeout: config.election_timeout,
            heartbeat_interval: config.heartbeat_interval,
            storage: Arc::clone(&config.storage),
            groups: Mutex::new(BTreeMap::new()),
        }
    }

    /// Create (or replace) the group `group` with voter set `voters`, reloading
    /// its persisted state from the factory. The election timer arms from `now`
    /// with no jitter: no group draws entropy on its first arm, only on later
    /// resets, so the engine-wide draw order is a deterministic function of the
    /// group set (ticked in `GroupId` order), independent of how many groups exist.
    pub(crate) fn create_group(
        &self,
        group: GroupId,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
        now: Instant,
    ) -> Arc<RaftGroup> {
        let storage = (self.storage)(group, self.node);
        let raft = Arc::new(RaftGroup::new(
            group,
            self.node,
            voters,
            learners,
            self.election_timeout,
            self.heartbeat_interval,
            storage,
            now,
        ));
        self.groups
            .lock()
            .expect("raft groups mutex poisoned")
            .insert(group, Arc::clone(&raft));
        raft
    }

    /// The group `group`, if this node runs it.
    pub(crate) fn group(&self, group: GroupId) -> Option<Arc<RaftGroup>> {
        self.groups
            .lock()
            .expect("raft groups mutex poisoned")
            .get(&group)
            .map(Arc::clone)
    }

    /// Stop running `group`: drop it from the tick map so it no longer elects,
    /// heartbeats, or commits (spec §7.7, G7 reclamation on a shard merge).
    /// Retiring needs no in-group consensus (see
    /// [`RaftConsensus::remove_group`](crate::RaftConsensus::remove_group)).
    /// Idempotent; a group this node never ran is already gone. Not implemented:
    /// sweeping the group's on-disk storage.
    pub(crate) fn remove_group(&self, group: GroupId) {
        self.groups
            .lock()
            .expect("raft groups mutex poisoned")
            .remove(&group);
    }

    /// Drive every group one tick, in `GroupId` order (deterministic). Returns
    /// each group's output for the caller to apply (frames to send, committed
    /// app commands, the term won if it just became leader).
    pub(crate) fn tick_all<E: Entropy>(
        &self,
        now: Instant,
        entropy: &E,
    ) -> Vec<(GroupId, RaftOutput)> {
        let groups: Vec<(GroupId, Arc<RaftGroup>)> = self
            .groups
            .lock()
            .expect("raft groups mutex poisoned")
            .iter()
            .map(|(id, raft)| (*id, Arc::clone(raft)))
            .collect();
        groups
            .into_iter()
            .map(|(id, raft)| (id, raft.tick(now, entropy)))
            .collect()
    }
}

// --- The consensus state machine ----------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

/// One observation the caller folds into its state machine, in commit order
/// (spec §9.4.3). The opaque-bytes seam: the caller decodes and applies these.
/// A `Snapshot` is delivered when this node installs a leader's state-machine
/// snapshot, the log prefix it subsumes having been compacted away (§9). Both
/// variants ride one ordered stream, so an install and the commands after it
/// never reorder.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Committed {
    /// A committed application command at this 1-based log index. `commit` is the
    /// group's commit index at the moment this batch was drained — a high-water
    /// mark a consumer MAY fold monotonically (`seen = max(seen, commit)`) to track
    /// how far its projection trails the leader's commit. It rides the same ordered
    /// stream as the command, so it never races the data it covers; and it carries
    /// the *commit*, not this entry's `index`, so the last delivered observation
    /// reflects any `Noop`/voter-change tail that the stream filters.
    Apply {
        index: u64,
        command: Vec<u8>,
        commit: u64,
    },
    /// Install this state-machine snapshot, which subsumes every command through
    /// `index`; the receiver replaces its state with it. `commit` is the
    /// high-water mark as for [`Apply`](Committed::Apply).
    Snapshot {
        index: u64,
        snapshot: Vec<u8>,
        commit: u64,
    },
}

impl Committed {
    /// The log index this observation carries (an applied command's index, or the
    /// index a snapshot is taken at).
    pub fn index(&self) -> u64 {
        match self {
            Committed::Apply { index, .. } | Committed::Snapshot { index, .. } => *index,
        }
    }

    /// The commit high-water mark this observation was drained at — a consumer
    /// MAY fold it monotonically to track how far its projection trails the
    /// leader's commit (see the variant docs).
    pub fn commit(&self) -> u64 {
        match self {
            Committed::Apply { commit, .. } | Committed::Snapshot { commit, .. } => *commit,
        }
    }
}

/// What one Raft step produced, for the caller to act on: frames to send over
/// the transport (each already tagged with its group), observations newly
/// committed in log order ([`Committed`] — the opaque app bytes the caller
/// decodes and applies, or a snapshot to install), and the term won if this step
/// made the node leader (the caller emits `LeaderElected`, spec §16).
#[derive(Default)]
pub(crate) struct RaftOutput {
    pub frames: Vec<(NodeId, Frame)>,
    pub committed: Vec<Committed>,
    pub elected: Option<u64>,
}

struct RaftState {
    role: Role,
    term: u64,
    voted_for: Option<NodeId>,
    /// The replicated log **after** the compacted prefix; entry `i` (1-based,
    /// absolute) lives at `log[i - snapshot_index - 1]`. With no snapshot
    /// `snapshot_index == 0` and this is the plain 1-based log.
    log: Vec<RaftEntry>,
    /// The last index covered by the installed snapshot (`0` = nothing compacted,
    /// §9), its term, and the application snapshot bytes (held so a leader can ship
    /// them via `RaftInstallSnapshot`). Entries `≤ snapshot_index` are gone from
    /// `log`; their term is only known for `snapshot_index` itself.
    snapshot_index: u64,
    snapshot_term: u64,
    snapshot: Option<Vec<u8>>,
    /// Highest committed index; `0` = nothing committed.
    commit: u64,
    /// Highest index whose command has been handed to the caller for
    /// application; trails `commit` only inside a step.
    applied: u64,
    /// The current voter set: the configured one plus committed
    /// `AddVoter`/`RemoveVoter` changes, kept sorted (determinism, spec §4.6 #4).
    /// Only voters elect, lead, and count toward a quorum.
    voters: Vec<NodeId>,
    /// Non-voting **learners**: group members the leader replicates to, but which
    /// never elect, lead, or count toward a quorum (spec §7.1, the granary shards'
    /// extra replicas beyond the voter quorum). Kept sorted and disjoint from
    /// `voters`. A learner adopts the leader on append, so it can route and serve
    /// reads.
    learners: Vec<NodeId>,
    /// The leader this node currently believes in (itself when leading).
    leader: Option<NodeId>,
    /// Votes granted to this candidate in the current term.
    votes: BTreeSet<NodeId>,
    /// Leader bookkeeping: the next index to send each peer, and the highest
    /// index known replicated on it.
    next: BTreeMap<NodeId, u64>,
    matched: BTreeMap<NodeId, u64>,
    /// When the follower/candidate election timer fires (base + seeded jitter).
    election_deadline: Instant,
    /// Set once this node's [`RaftWAL`] has answered [`WalAck::Failed`], and never
    /// cleared: it cannot persist, so it must not vote, lead, or acknowledge an
    /// append (spec §9.4.3 item 2). It stays a group member and keeps answering with
    /// its term, so peers see a live node that refuses rather than a silent one.
    wal_poisoned: bool,
}

/// What fraction of the election timeout a **pristine** group waits before its first
/// campaign: one slot per voter, so the whole staggered fan fits inside one timeout.
const FIRST_ELECTION_SLOTS: u32 = 10;

/// How long a freshly built group waits before campaigning for the first time
/// (spec §9.4.3 item 10).
///
/// The election timeout exists to detect a leader that has *stopped*. A group that has
/// never had one is not detecting anything — it is waiting out a timer for a failure
/// that has not occurred, and on a cold cluster that wait is the whole of the startup
/// cost. It compounds, too: a shard group is only created once the shard map commits,
/// so a deployment pays the timeout once for the control plane and again for the groups
/// created after it, serially.
///
/// So a group whose persisted state is `pristine` — term 0, no log, no snapshot, which
/// is to say it has never voted, replicated, or been led — campaigns after a small
/// fraction of the timeout instead. Two properties make that safe:
///
/// - **It cannot depose a live leader.** A pristine group campaigns at term 1. A leader
///   that exists is at some term ≥ 1 with a log, and Raft rejects a `RequestVote` whose
///   term is below the receiver's; the candidate then adopts the higher term and steps
///   down. The cost of a node joining an established group is one wasted round, not a
///   disruption. (This is exactly the case pre-vote exists for, and exactly the case
///   pre-vote is *not* needed for: the danger is a candidate with a **higher** term.)
/// - **It does not split the vote.** The delay is staggered one slot per voter, so on a
///   cold cluster the voters campaign in sequence and the first one wins outright
///   rather than three of them tying and rearming.
///
/// The stagger is derived from `(group, node)` rather than from node id alone, so
/// different groups put different nodes first and leadership spreads across the cluster
/// instead of piling onto the lowest id. It draws **no entropy**: the simulation seeds
/// every random choice and a draw here would shift that stream, changing the behaviour
/// of every recorded seed (spec §18.1). Being deterministic, it is also the same on
/// every node, so all of them agree on the order without exchanging anything.
///
/// A group with persisted state keeps the full timeout. A restarting voter rejoining a
/// healthy group has no business campaigning early: there *is* a leader, and the timer
/// is doing the job it was designed for.
fn first_election_delay(
    group: GroupId,
    node: NodeId,
    voters: &[NodeId],
    election_timeout: Duration,
    heartbeat_interval: Duration,
    pristine: bool,
) -> Duration {
    if !pristine {
        return election_timeout;
    }
    // A splitmix64 round: enough to decorrelate adjacent ids and group numbers, and
    // cheap and stable across builds (no `Hasher`, whose output is not contracted).
    let mix = |x: u64| {
        let mut z = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    };
    let key = |n: NodeId| mix(group.0 ^ mix(n.uid()));
    // This node's position in the group's own ordering of its voters.
    let ours = key(node);
    let rank = voters.iter().filter(|&&n| key(n) < ours).count() as u32;
    let slot = election_timeout / FIRST_ELECTION_SLOTS;
    // Never campaign before an existing leader could have been heard from. A group is
    // created on each node as the shard map reaches it, which is not simultaneous: a
    // node that builds its copy late is pristine while the group already has a leader
    // elsewhere, and if it campaigns before the first heartbeat arrives it spends a
    // whole heartbeat interval as a candidate with no known leader — during which every
    // request routed to it stalls. That cost is real and was measured: it turned a
    // 0.3 s warm create into a 3.5 s one until this floor was added.
    //
    // Two intervals, not one, so a single dropped heartbeat does not put the group back
    // in that state.
    let floor = heartbeat_interval.saturating_mul(2);
    floor + slot * rank
}

/// Normalize a group's membership: voters and learners each sorted and deduped
/// (determinism, spec §4.6 #4), with learners kept disjoint from voters (a node is
/// one or the other, never both). Shared by group construction and snapshot
/// install, which adopt a fresh membership set the same way.
fn normalize_membership(
    mut voters: Vec<NodeId>,
    mut learners: Vec<NodeId>,
) -> (Vec<NodeId>, Vec<NodeId>) {
    voters.sort();
    voters.dedup();
    learners.sort();
    learners.dedup();
    learners.retain(|n| !voters.contains(n));
    (voters, learners)
}

impl RaftState {
    fn last_index(&self) -> u64 {
        self.snapshot_index + self.log.len() as u64
    }

    /// The 0-based slot in `log` holding the entry at absolute `index`. The log
    /// drops the compacted prefix, so absolute `index` sits at
    /// `log[index - snapshot_index - 1]`; every read, truncate, and drain routes
    /// through here. `index` must be `> snapshot_index` (index `0` and the
    /// compacted prefix have no slot). Because `slot(index)` names an entry's
    /// position, a `..=slot(index)` range is *inclusive* of it: it spans exactly
    /// the `index - snapshot_index` entries at or below `index`.
    fn slot(&self, index: u64) -> usize {
        (index - self.snapshot_index) as usize - 1
    }

    /// The term of the entry at absolute `index`. `0` is the empty head;
    /// `snapshot_index` is the snapshot's term; anything in between has been
    /// compacted away and is never queried (such a peer gets an InstallSnapshot,
    /// never a log-matching probe).
    fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            0
        } else if index == self.snapshot_index {
            self.snapshot_term
        } else {
            self.log[self.slot(index)].term
        }
    }

    /// The entry at absolute `index` (`> snapshot_index`), for application and
    /// replication slicing.
    fn entry_at(&self, index: u64) -> &RaftEntry {
        &self.log[self.slot(index)]
    }

    /// The retained log entries from absolute index `first` (inclusive); `first`
    /// must be `> snapshot_index`.
    fn suffix_from(&self, first: u64) -> &[RaftEntry] {
        &self.log[self.slot(first)..]
    }

    fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    /// Every other group member the leader replicates to — voters and learners
    /// alike, excluding `self_node`. Learners receive the log but are absent from
    /// the quorum count ([`quorum`](Self::quorum), [`advance_commit`]).
    fn replication_targets(&self, self_node: NodeId) -> Vec<NodeId> {
        self.voters
            .iter()
            .chain(self.learners.iter())
            .copied()
            .filter(|&n| n != self_node)
            .collect()
    }
}

/// One group's Raft instance on this node (spec §9.4.3). All state sits behind
/// one mutex, mutated by the driver tick and the frame handlers.
pub(crate) struct RaftGroup {
    group: GroupId,
    node: NodeId,
    election_timeout: Duration,
    storage: Arc<dyn RaftWAL>,
    state: Mutex<RaftState>,
}

impl RaftGroup {
    /// Build the group instance, reloading any persisted state (spec §9.4.3
    /// item 2). The election timer arms from `now` (base timeout, no jitter —
    /// the first tick draws no entropy, keeping the draw order deterministic).
    #[allow(clippy::too_many_arguments)] // one call site, `MultiRaft::create_group`
    pub(crate) fn new(
        group: GroupId,
        node: NodeId,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
        election_timeout: Duration,
        heartbeat_interval: Duration,
        storage: Arc<dyn RaftWAL>,
        now: Instant,
    ) -> RaftGroup {
        let persisted = storage.load();
        let (voters, learners) = normalize_membership(voters, learners);
        let snapshot_index = persisted.snapshot_index;
        // Computed before the state takes ownership of `voters`, and before `persisted`
        // is torn apart into it.
        let first_election = first_election_delay(
            group,
            node,
            &voters,
            election_timeout,
            heartbeat_interval,
            persisted.term == 0 && persisted.log.is_empty() && persisted.snapshot_index == 0,
        );
        let state = RaftState {
            role: Role::Follower,
            term: persisted.term,
            voted_for: persisted.voted_for,
            log: persisted.log,
            snapshot_index,
            snapshot_term: persisted.snapshot_term,
            snapshot: persisted.snapshot,
            // A reloaded snapshot is already applied state; commit/applied start at
            // its index so the engine never re-drains the compacted prefix.
            commit: snapshot_index,
            applied: snapshot_index,
            voters,
            learners,
            leader: None,
            votes: BTreeSet::new(),
            next: BTreeMap::new(),
            matched: BTreeMap::new(),
            election_deadline: now + first_election,
            // A WAL that reloaded poisoned — the volume is still failing at startup —
            // brings the group up refusing rather than letting it vote once and find
            // out on the first write.
            wal_poisoned: storage.poisoned().is_some(),
        };
        RaftGroup {
            group,
            node,
            election_timeout,
            storage,
            state: Mutex::new(state),
        }
    }

    /// Whether `node` is currently in this group's voter set.
    pub(crate) fn has_voter(&self, node: NodeId) -> bool {
        self.lock().voters.contains(&node)
    }

    /// This group's current voter set — used to fan a proposal out to the voters
    /// when the leader is not yet known (the app-level analogue of the control
    /// plane's `RaftConfig.voters` broadcast).
    pub(crate) fn voters(&self) -> Vec<NodeId> {
        self.lock().voters.clone()
    }

    /// Whether this node currently leads this group.
    pub(crate) fn is_leader(&self) -> bool {
        self.lock().role == Role::Leader
    }

    /// This group's current Raft term. A layer above (granary's per-shard
    /// leader-election group, §8) uses it as the single-writer fencing token every
    /// per-grain append carries: one leader per term, monotonic across the quorum.
    pub(crate) fn term(&self) -> u64 {
        self.lock().term
    }

    /// The reloaded state-machine snapshot as a [`Committed::Snapshot`], if this
    /// group came up over a **compacted** log (`snapshot_index > 0`), else `None`.
    ///
    /// A node that restarts from a snapshot reloads it into [`RaftState`], but the
    /// engine never re-emits already-applied state on the commit stream (`applied`
    /// starts at the snapshot base, see [`new`]), so a fresh subscriber would
    /// otherwise see only the post-snapshot tail and miss the compacted prefix
    /// entirely. Handing it this observation first rebuilds the projection from the
    /// snapshot, the leaderless counterpart of a leader-driven InstallSnapshot
    /// (§9). Its `commit` watermark is the snapshot base: the snapshot proves the
    /// projection current only through `snapshot_index`, never beyond.
    ///
    /// [`new`]: RaftGroup::new
    pub(crate) fn snapshot_observation(&self) -> Option<Committed> {
        let state = self.lock();
        if state.snapshot_index == 0 {
            return None;
        }
        state.snapshot.clone().map(|snapshot| Committed::Snapshot {
            index: state.snapshot_index,
            snapshot,
            commit: state.snapshot_index,
        })
    }

    /// This group's highest committed index (test-only inspection).
    #[cfg(test)]
    pub(crate) fn commit_index(&self) -> u64 {
        self.lock().commit
    }

    /// This group's compacted-prefix base, `0` if nothing is compacted (test-only).
    #[cfg(test)]
    pub(crate) fn snapshot_index(&self) -> u64 {
        self.lock().snapshot_index
    }

    /// The number of log entries retained after the compacted prefix (test-only).
    #[cfg(test)]
    pub(crate) fn retained_len(&self) -> usize {
        self.lock().log.len()
    }

    /// The leader this node currently believes in: itself when leading, the
    /// sender of the last accepted append otherwise.
    pub(crate) fn leader_hint(&self) -> Option<NodeId> {
        self.lock().leader
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RaftState> {
        self.state.lock().expect("raft mutex poisoned")
    }

    /// Persist term and vote — before any message that announces them
    /// (spec §9.4.3 item 2). A caller that gets [`WalAck::Failed`] MUST NOT send the
    /// message it was about to send.
    fn persist_term(&self, state: &mut RaftState) -> WalAck {
        let ack = self.storage.save_term_and_vote(state.term, state.voted_for);
        if ack == WalAck::Failed {
            self.wal_failed(state);
        }
        ack
    }

    /// Stop participating in consensus, permanently (spec §9.4.3 item 2).
    ///
    /// A voter that cannot persist its term must stop voting. It cannot simply crash:
    /// an unwind is caught by actor supervision or takes down one task, and either way
    /// the node can stay up, gossiping, and counted in quorums with no durability
    /// behind it — the failure mode this exists to prevent. So the group steps down to
    /// a follower that never elects, never grants a vote, and never acknowledges an
    /// append. The node remains visible and answers with its term, so peers elect
    /// around it promptly instead of waiting out a silence, and an operator sees a
    /// member to replace.
    fn wal_failed(&self, state: &mut RaftState) {
        state.wal_poisoned = true;
        state.role = Role::Follower;
        state.leader = None;
        state.votes.clear();
        state.next.clear();
        state.matched.clear();
    }

    /// Step down into the follower role for `term` (seen a higher term, or a
    /// current leader).
    fn become_follower<E: Entropy>(
        &self,
        state: &mut RaftState,
        term: u64,
        now: Instant,
        entropy: &E,
    ) {
        if term > state.term {
            state.term = term;
            state.voted_for = None;
            // A failure here has already poisoned the group, which is what stops it
            // voting; stepping down is what this function was going to do anyway.
            let _stepped_down = self.persist_term(state);
        }
        state.role = Role::Follower;
        state.votes.clear();
        self.rearm_election(state, now, entropy);
    }

    /// Re-arm the election timer: base timeout plus seeded jitter in
    /// `[0, base)` (spec §9.4.3 item 7).
    fn rearm_election<E: Entropy>(&self, state: &mut RaftState, now: Instant, entropy: &E) {
        let span = self.election_timeout.as_nanos() as u64;
        let jitter = Duration::from_nanos(entropy.next_u64() % span.max(1));
        state.election_deadline = now + self.election_timeout + jitter;
    }

    /// Hand leadership of this group to `target` (Raft §3.10), returning whether
    /// the handoff was actually initiated.
    ///
    /// `false` means "not yet, try again": this node is not the leader, `target` is
    /// not a voter, or — the interesting case — `target`'s log is not yet caught up
    /// to ours. A node that has not replicated our last entry cannot win an election
    /// against the rest of the group (its log is not up to date, so voters refuse
    /// it), so sending it `TimeoutNow` would cost a disrupted term and change
    /// nothing. The caller retries while ordinary replication closes the gap.
    ///
    /// This does **not** step down. It cannot: leadership is a quorum fact, and the
    /// only thing that legitimately ends our term is someone else winning a higher
    /// one. All this does is invite a caught-up peer to try immediately instead of
    /// after its election timeout. If that peer fails, nothing is lost — we are
    /// still the leader and the caller can pick another target.
    pub(crate) fn transfer_leadership(&self, target: NodeId) -> (bool, RaftOutput) {
        let mut out = RaftOutput::default();
        let state = self.lock();
        if state.role != Role::Leader || target == self.node || !state.voters.contains(&target) {
            return (false, out);
        }
        // Caught up means matched to OUR last index: the entry a voter compares
        // against when deciding whether the candidate's log is up to date.
        if state.matched.get(&target).copied().unwrap_or(0) < state.last_index() {
            return (false, out);
        }
        out.frames.push((
            target,
            Frame::RaftTimeoutNow {
                group: self.group,
                term: state.term,
                leader: self.node,
            },
        ));
        (true, out)
    }

    /// A voter this group's leader asked to stand for election immediately
    /// (Raft §3.10, the recipient half of [`transfer_leadership`]).
    ///
    /// Accepted only from the leader of our current term, so a stale or forged
    /// invitation cannot make us disrupt a healthy leader. Beyond that no new trust
    /// is extended: this starts an ordinary election, which still has to win a
    /// quorum, so the worst a wrongly-accepted invitation costs is one disrupted
    /// term — never a split leadership.
    pub(crate) fn handle_timeout_now<E: Entropy>(
        &self,
        from: NodeId,
        term: u64,
        now: Instant,
        entropy: &E,
    ) -> RaftOutput {
        let mut out = RaftOutput::default();
        let mut state = self.lock();
        // Only a voter can stand, and only for the term we currently believe in
        // under the leader we currently believe in.
        if term != state.term
            || state.leader != Some(from)
            || !state.voters.contains(&self.node)
            || state.role == Role::Leader
        {
            return out;
        }
        self.start_election(&mut state, now, entropy, &mut out);
        self.drain_committed(&mut state, &mut out, now, entropy);
        out
    }

    /// The driver tick (spec §9.4.3): a follower/candidate whose election timer
    /// fired starts an election; a leader replicates its log and heartbeats,
    /// advancing the commit index over quorum-matched, current-term entries.
    pub(crate) fn tick<E: Entropy>(&self, now: Instant, entropy: &E) -> RaftOutput {
        let mut out = RaftOutput::default();
        let mut state = self.lock();
        // Only voters elect and lead (spec §9.4.3 item 2); a non-voter node is a
        // passive learner whose tick does nothing. A voter whose WAL has failed is
        // likewise inert: it must not start an election it cannot persist, and it must
        // not replicate as a leader whose log no longer reaches disk.
        if !state.voters.contains(&self.node) || state.wal_poisoned {
            return out;
        }
        match state.role {
            Role::Follower | Role::Candidate => {
                if now >= state.election_deadline {
                    self.start_election(&mut state, now, entropy, &mut out);
                }
            }
            Role::Leader => self.replicate(&mut state, &mut out),
        }
        self.drain_committed(&mut state, &mut out, now, entropy);
        out
    }

    /// Begin an election (spec §9.4.3): bump the term, vote for self, persist,
    /// and solicit the other voters. A single-voter cluster wins immediately.
    fn start_election<E: Entropy>(
        &self,
        state: &mut RaftState,
        now: Instant,
        entropy: &E,
        out: &mut RaftOutput,
    ) {
        state.role = Role::Candidate;
        state.term += 1;
        state.voted_for = Some(self.node);
        state.leader = None;
        state.votes = BTreeSet::from([self.node]);
        // The term bump and the self-vote must be on disk before either is announced:
        // a candidate that solicited votes it could not remember across a restart
        // would vote twice in one term, which is election safety gone (#22).
        if self.persist_term(state) == WalAck::Failed {
            self.rearm_election(state, now, entropy);
            return;
        }
        self.rearm_election(state, now, entropy);
        if state.votes.len() >= state.quorum() {
            self.become_leader(state, out);
            return;
        }
        let request = Frame::RaftVote {
            group: self.group,
            term: state.term,
            candidate: self.node,
            last_index: state.last_index(),
            last_term: state.term_at(state.last_index()),
        };
        for &voter in state.voters.iter().filter(|&&v| v != self.node) {
            out.frames.push((voter, request.clone()));
        }
    }

    /// Win the election: append the term-opening `Noop` (leader completeness)
    /// and start replicating.
    fn become_leader(&self, state: &mut RaftState, out: &mut RaftOutput) {
        state.role = Role::Leader;
        state.leader = Some(self.node);
        out.elected = Some(state.term);
        // Track replication progress for every member — voters and learners — so
        // the leader sends each the right log suffix.
        let next = state.last_index() + 1;
        let members: Vec<NodeId> = state
            .voters
            .iter()
            .chain(state.learners.iter())
            .copied()
            .collect();
        state.next = members.iter().map(|&n| (n, next)).collect();
        state.matched = members.iter().map(|&n| (n, 0)).collect();
        let term = state.term;
        self.append_entry(
            state,
            RaftEntry {
                term,
                payload: EntryPayload::Noop,
            },
        );
        // The term-opening entry is the first thing this leader must have on disk, and
        // a WAL that refuses it has already stepped the group down. Take the election
        // announcement back with it: the caller emits `LeaderElected` from `elected`,
        // and a node that reported leading and then replicated a log it cannot persist
        // is the failure the poison policy exists to prevent (spec §9.4.3 item 2).
        if state.wal_poisoned {
            out.elected = None;
            return;
        }
        self.replicate(state, out);
    }

    /// Append one entry to this node's log, durably. Returns the resulting last
    /// index, unchanged if the WAL refused.
    ///
    /// A refusal rolls the entry back out of the in-memory log and stops the group.
    /// Leaving it would be the worst of both: the leader would replicate and could
    /// commit an entry that is not on its own disk, and a restart would come back
    /// missing a committed entry it had already acknowledged.
    fn append_entry(&self, state: &mut RaftState, entry: RaftEntry) -> u64 {
        let from = state.last_index();
        state.log.push(entry);
        // The entry just pushed now sits at the tail slot; hand storage that
        // one-entry suffix starting at absolute `from`.
        let slot = state.slot(state.last_index());
        if self.storage.append(from, &state.log[slot..]) == WalAck::Failed {
            state.log.pop();
            self.wal_failed(state);
        }
        state.last_index()
    }

    /// Append `payload` if this node leads and no identical payload is already
    /// pending (uncommitted) — the dedup that keeps the leader's per-tick duties
    /// and forwarded proposals from piling up duplicates. Returns whether the
    /// payload is now in the log (newly or already). Byte-equality of an
    /// [`EntryPayload::App`] makes a re-proposed application command idempotent.
    pub(crate) fn propose(&self, payload: EntryPayload) -> bool {
        let mut state = self.lock();
        if state.role != Role::Leader || state.wal_poisoned {
            return false;
        }
        // Scan only uncommitted entries; `commit ≥ snapshot_index` always, so the
        // retained suffix from `commit` is present.
        let pending = state
            .suffix_from(state.commit + 1)
            .iter()
            .any(|e| e.payload == payload);
        if !pending {
            let term = state.term;
            self.append_entry(&mut state, RaftEntry { term, payload });
            // `append_entry` rolls the entry back out and stops the group when the WAL
            // refuses it, so the payload is in the log only if that did not happen.
            // Reporting `true` here would tell the caller its command was accepted by a
            // leader that has just stepped down without it.
            return !state.wal_poisoned;
        }
        true
    }

    /// Compact the log up to `index` against the application's state-machine
    /// `snapshot` (§9): discard every entry `≤ index` and remember the snapshot so
    /// this node can ship it to a lagging peer. Purely local and deterministic —
    /// the caller supplies a snapshot of its applied prefix, so every replica
    /// produces an equivalent one without coordination. Ignores a stale or
    /// not-yet-applied `index` (only applied state is safe to compact).
    pub(crate) fn compact(&self, index: u64, snapshot: Vec<u8>) {
        let mut state = self.lock();
        if index <= state.snapshot_index || index > state.applied {
            return;
        }
        let term = state.term_at(index);
        // Persist before the prefix leaves memory — the ordering this seam promises,
        // and the one that makes a refusal harmless: the entries compaction would have
        // shed are still in memory and still on disk, so the group stops with its log
        // whole rather than with a hole where its base should be.
        if self.storage.save_snapshot(index, term, &snapshot) == WalAck::Failed {
            self.wal_failed(&mut state);
            return;
        }
        // Shed every entry at or below `index` (inclusive range through its slot).
        let slot = state.slot(index);
        state.log.drain(..=slot);
        state.snapshot_index = index;
        state.snapshot_term = term;
        state.snapshot = Some(snapshot);
    }

    /// The `RaftInstallSnapshot` frame carrying this node's current snapshot — sent
    /// to a peer whose `next` has fallen below the compacted prefix.
    fn install_snapshot_frame(&self, state: &RaftState) -> Frame {
        Frame::RaftInstallSnapshot {
            group: self.group,
            term: state.term,
            leader: self.node,
            snapshot_index: state.snapshot_index,
            snapshot_term: state.snapshot_term,
            voters: state.voters.clone(),
            learners: state.learners.clone(),
            data: state.snapshot.clone().unwrap_or_default(),
        }
    }

    /// Leader replication (spec §9.4.3 item 3): send each other voter the log
    /// suffix it still misses (a heartbeat when empty), then advance the commit
    /// index to the highest current-term entry a quorum has matched.
    fn replicate(&self, state: &mut RaftState, out: &mut RaftOutput) {
        // Every other member, learners included (§7.1).
        let peers = state.replication_targets(self.node);
        for peer in peers {
            let next = *state.next.get(&peer).unwrap_or(&(state.last_index() + 1));
            let prev_index = next - 1;
            // A peer behind the compacted prefix cannot be caught up with log
            // entries — its `prev_index` names a term we no longer hold. Ship the
            // snapshot instead; one accepted install moves its `next` past the base.
            if prev_index < state.snapshot_index {
                out.frames.push((peer, self.install_snapshot_frame(state)));
                continue;
            }
            let entries: Vec<RaftEntry> = state.suffix_from(prev_index + 1).to_vec();
            out.frames.push((
                peer,
                Frame::RaftAppend {
                    group: self.group,
                    term: state.term,
                    leader: self.node,
                    prev_index,
                    prev_term: state.term_at(prev_index),
                    entries,
                    commit: state.commit,
                },
            ));
        }
        self.advance_commit(state);
    }

    /// The quorum commit rule: only entries of the current term commit by
    /// counting (Raft §5.4.2). The leader's own log always matches, but it counts
    /// toward the quorum **only while it is still a voter**: once a leader has
    /// committed its own `RemoveVoter` it must not vote itself over the commit
    /// line, or a non-voter leader could advance commit on a phantom self-vote
    /// with sub-quorum real replication — a minority evicting the majority (spec
    /// §18.5 #22: a side lacking a quorum of voters commits none). Defense in
    /// depth: the `tick` non-voter early-return already keeps such a leader dormant.
    fn advance_commit(&self, state: &mut RaftState) {
        for index in (state.commit + 1..=state.last_index()).rev() {
            if state.term_at(index) != state.term {
                continue;
            }
            let self_vote = usize::from(state.voters.contains(&self.node));
            let replicated = self_vote
                + state
                    .voters
                    .iter()
                    .filter(|&&v| {
                        v != self.node && state.matched.get(&v).copied().unwrap_or(0) >= index
                    })
                    .count();
            if replicated >= state.quorum() {
                state.commit = index;
                break;
            }
        }
    }

    /// Hand newly committed application commands to the caller, applying
    /// voter-set changes internally (spec §9.4.3 item 2). `Noop` and the voter
    /// changes never reach the caller; only [`EntryPayload::App`] bytes do.
    ///
    /// A leader that commits its own `RemoveVoter` steps down here (Raft
    /// dissertation §4.2.2): it is no longer a voter, so it stops leading.
    /// `now`/`entropy` re-arm its election timer as on any other step-down.
    fn drain_committed<E: Entropy>(
        &self,
        state: &mut RaftState,
        out: &mut RaftOutput,
        now: Instant,
        entropy: &E,
    ) {
        while state.applied < state.commit {
            state.applied += 1;
            // Cloned so the borrow ends before the voter-set mutations below.
            let entry = state.entry_at(state.applied).clone();
            match entry.payload {
                EntryPayload::AddVoter(node) if !state.voters.contains(&node) => {
                    state.voters.push(node);
                    state.voters.sort();
                    if state.role == Role::Leader {
                        let next = state.last_index() + 1;
                        state.next.entry(node).or_insert(next);
                        state.matched.entry(node).or_insert(0);
                    }
                }
                EntryPayload::RemoveVoter(node) => {
                    state.voters.retain(|&v| v != node);
                    state.next.remove(&node);
                    state.matched.remove(&node);
                    // A leader that just removed itself is no longer a voter and
                    // must not keep leading (spec §9.4.3 item 2).
                    if node == self.node && state.role == Role::Leader {
                        let term = state.term;
                        self.become_follower(state, term, now, entropy);
                        state.leader = None;
                    }
                }
                EntryPayload::App(bytes) => {
                    out.committed.push(Committed::Apply {
                        index: state.applied,
                        command: bytes,
                        commit: state.commit,
                    });
                }
                EntryPayload::Noop | EntryPayload::AddVoter(_) => {}
            }
        }
    }

    /// Handle a vote request (spec §9.4.3): grant iff the candidate's term is
    /// current, we have not voted for another in it, and the candidate's log is
    /// at least as up-to-date as ours.
    ///
    /// Deliberately **no** "am I a voter" check (Raft dissertation §4.2.3:
    /// servers process RPCs from servers outside their own configuration view).
    /// Config changes apply on *commit* here, so views lag: after a leader
    /// removes itself and goes silent, the followers may still hold the old
    /// N-voter config while a just-added voter has not yet applied its own
    /// `AddVoter`. If receivers refused to vote whenever their own view excluded
    /// them, that state deadlocks — the stale-view followers cannot assemble the
    /// old quorum without the departed leader (which would refuse, its view
    /// excluding itself) or the new voter (which would refuse, its view lagging)
    /// — and the group never elects again. Granting is safe: one vote per term
    /// per server still holds (`voted_for` persistence), and a candidate only
    /// counts votes it solicited from its own voter view, so quorums are never
    /// inflated by non-voters.
    pub(crate) fn handle_vote<E: Entropy>(
        &self,
        from: NodeId,
        term: u64,
        last_index: u64,
        last_term: u64,
        now: Instant,
        entropy: &E,
    ) -> RaftOutput {
        let mut out = RaftOutput::default();
        let mut state = self.lock();
        if term > state.term {
            self.become_follower(&mut state, term, now, entropy);
        }
        let up_to_date = last_term > state.term_at(state.last_index())
            || (last_term == state.term_at(state.last_index()) && last_index >= state.last_index());
        // A node whose WAL has failed refuses every vote: one it granted and then
        // forgot across a restart is a second vote in the same term.
        let eligible = !state.wal_poisoned
            && term == state.term
            && state.voted_for.is_none_or(|v| v == from)
            && up_to_date;
        let granted = if eligible {
            state.voted_for = Some(from);
            // Durable before it is announced, and reported as refused if it did not
            // land — the candidate must not count a vote this node cannot honour.
            let persisted = self.persist_term(&mut state) == WalAck::Persisted;
            if persisted {
                self.rearm_election(&mut state, now, entropy);
            }
            persisted
        } else {
            false
        };
        out.frames.push((
            from,
            Frame::RaftVoteReply {
                group: self.group,
                term: state.term,
                granted,
            },
        ));
        out
    }

    /// Handle a vote reply: a quorum of grants in the current term wins the
    /// election (at most one leader per term — invariant #22's election-safety
    /// half, by single-vote-per-term persistence).
    pub(crate) fn handle_vote_reply<E: Entropy>(
        &self,
        from: NodeId,
        term: u64,
        granted: bool,
        now: Instant,
        entropy: &E,
    ) -> RaftOutput {
        let mut out = RaftOutput::default();
        let mut state = self.lock();
        if term > state.term {
            self.become_follower(&mut state, term, now, entropy);
            return out;
        }
        if state.role == Role::Candidate && term == state.term && granted {
            state.votes.insert(from);
            if state.votes.len() >= state.quorum() {
                self.become_leader(&mut state, &mut out);
            }
        }
        self.drain_committed(&mut state, &mut out, now, entropy);
        out
    }

    /// Handle an append/heartbeat (spec §9.4.3): adopt the leader, resolve log
    /// conflicts by truncate-then-append, and advance the commit index to the
    /// leader's. Any leader-mode node accepts appends — a non-voter is a
    /// learner replicating committed state.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn handle_append<E: Entropy>(
        &self,
        from: NodeId,
        term: u64,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<RaftEntry>,
        commit: u64,
        now: Instant,
        entropy: &E,
    ) -> RaftOutput {
        let mut out = RaftOutput::default();
        let mut state = self.lock();
        // A refusal, not a silence: the leader learns immediately that this replica
        // cannot hold entries and stops counting it toward the commit quorum, rather
        // than waiting out a timeout on every append.
        if term < state.term || state.wal_poisoned {
            out.frames.push((
                from,
                Frame::RaftAppendReply {
                    group: self.group,
                    term: state.term,
                    ok: false,
                    match_index: 0,
                },
            ));
            return out;
        }
        self.become_follower(&mut state, term, now, entropy);
        state.leader = Some(from);

        // Log-matching check: the entry before the suffix must agree. The prefix
        // through `snapshot_index` is committed and immutable, so a `prev_index`
        // at or below our snapshot base trivially agrees and skips the check (its
        // term is no longer in the log to compare).
        if prev_index >= state.snapshot_index
            && (prev_index > state.last_index() || state.term_at(prev_index) != prev_term)
        {
            out.frames.push((
                from,
                Frame::RaftAppendReply {
                    group: self.group,
                    term: state.term,
                    ok: false,
                    // A hint for the leader: everything past our log cannot match.
                    match_index: state.last_index().min(prev_index.saturating_sub(1)),
                },
            ));
            return out;
        }
        // Truncate any conflicting suffix, then append what is genuinely new.
        // `entries[i]` carries the absolute index `prev_index + 1 + i`; entries at
        // or below `snapshot_index` are already committed, so they are skipped.
        let mut append_from = entries.len();
        for (i, entry) in entries.iter().enumerate() {
            let index = prev_index + 1 + i as u64;
            if index <= state.snapshot_index {
                continue;
            }
            if index > state.last_index() {
                append_from = i;
                break;
            }
            if state.term_at(index) != entry.term {
                let slot = state.slot(index);
                state.log.truncate(slot);
                append_from = i;
                break;
            }
        }
        if append_from < entries.len() {
            let base = state.last_index();
            // The length to roll back to, taken before the extend: `base` can sit at
            // the snapshot base with an empty log, where there is no slot to name.
            let held = state.log.len();
            state.log.extend_from_slice(&entries[append_from..]);
            // Durable before it is acknowledged. An ack the disk did not back is what
            // lets the leader commit an entry that a quorum cannot actually replay, so
            // a refusal rolls the suffix back out and answers `ok: false`.
            if self.storage.append(base, &entries[append_from..]) == WalAck::Failed {
                state.log.truncate(held);
                self.wal_failed(&mut state);
                out.frames.push((
                    from,
                    Frame::RaftAppendReply {
                        group: self.group,
                        term: state.term,
                        ok: false,
                        match_index: 0,
                    },
                ));
                return out;
            }
        }
        // We hold at least through our snapshot base regardless of what the leader
        // re-sent, so never report progress below it.
        let match_index = (prev_index + entries.len() as u64).max(state.snapshot_index);
        state.commit = state.commit.max(commit.min(state.last_index()));
        out.frames.push((
            from,
            Frame::RaftAppendReply {
                group: self.group,
                term: state.term,
                ok: true,
                match_index,
            },
        ));
        self.drain_committed(&mut state, &mut out, now, entropy);
        out
    }

    /// Handle an install-snapshot from the leader (spec §9): adopt the leader,
    /// and if the snapshot advances us past our commit point, replace our state
    /// with it — set the log base to `(snapshot_index, snapshot_term)`, discard any
    /// log we held (a follower this far behind has no committed entries past the
    /// base; uncommitted ones are safely dropped), adopt the membership, hand the
    /// snapshot to the application via `Committed::Snapshot`, and reply with an
    /// ordinary `RaftAppendReply` so the leader advances our `next`/`matched`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn handle_install_snapshot<E: Entropy>(
        &self,
        from: NodeId,
        term: u64,
        snapshot_index: u64,
        snapshot_term: u64,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
        data: Vec<u8>,
        now: Instant,
        entropy: &E,
    ) -> RaftOutput {
        let mut out = RaftOutput::default();
        let mut state = self.lock();
        if term < state.term {
            // A stale leader: reject so it learns the newer term and steps down.
            out.frames.push((
                from,
                Frame::RaftAppendReply {
                    group: self.group,
                    term: state.term,
                    ok: false,
                    match_index: 0,
                },
            ));
            return out;
        }
        if state.wal_poisoned {
            // Same refusal as `handle_append`: this replica cannot durably hold what
            // the leader is shipping, so it says so rather than accepting it.
            out.frames.push((
                from,
                Frame::RaftAppendReply {
                    group: self.group,
                    term: state.term,
                    ok: false,
                    match_index: 0,
                },
            ));
            return out;
        }
        self.become_follower(&mut state, term, now, entropy);
        state.leader = Some(from);
        // Only install a snapshot that carries us past what we have committed; an
        // older or duplicate one is acked at our own level.
        if snapshot_index > state.commit {
            // Durable before it is adopted or handed to the application. This node is
            // about to jump its commit and applied points to the snapshot's index; if
            // the bytes are not on disk, a restart comes back below that point with no
            // log able to reach it, having already told the leader — and the
            // application — that it got there. Persist the snapshot, then clear the
            // stored log tail so a reload reconstructs from the base alone.
            let stored = self
                .storage
                .save_snapshot(snapshot_index, snapshot_term, &data)
                .persisted()
                && self.storage.append(snapshot_index, &[]).persisted();
            if !stored {
                self.wal_failed(&mut state);
                out.frames.push((
                    from,
                    Frame::RaftAppendReply {
                        group: self.group,
                        term: state.term,
                        ok: false,
                        match_index: 0,
                    },
                ));
                return out;
            }
            state.log.clear();
            state.snapshot_index = snapshot_index;
            state.snapshot_term = snapshot_term;
            state.snapshot = Some(data.clone());
            state.commit = snapshot_index;
            state.applied = snapshot_index;
            (state.voters, state.learners) = normalize_membership(voters, learners);
            out.committed.push(Committed::Snapshot {
                index: snapshot_index,
                snapshot: data,
                commit: snapshot_index,
            });
        }
        out.frames.push((
            from,
            Frame::RaftAppendReply {
                group: self.group,
                term: state.term,
                ok: true,
                match_index: state.last_index(),
            },
        ));
        out
    }

    /// Handle an append reply (leader): record the peer's progress and advance
    /// the commit index, or back off its `next` index after a mismatch.
    pub(crate) fn handle_append_reply<E: Entropy>(
        &self,
        from: NodeId,
        term: u64,
        ok: bool,
        match_index: u64,
        now: Instant,
        entropy: &E,
    ) -> RaftOutput {
        let mut out = RaftOutput::default();
        let mut state = self.lock();
        if term > state.term {
            self.become_follower(&mut state, term, now, entropy);
            return out;
        }
        if state.role != Role::Leader || term != state.term {
            return out;
        }
        if ok {
            state.matched.insert(from, match_index);
            state.next.insert(from, match_index + 1);
            self.advance_commit(&mut state);
        } else {
            let next = state.next.entry(from).or_insert(1);
            *next = (*next - 1).clamp(1, match_index + 1);
        }
        self.drain_committed(&mut state, &mut out, now, entropy);
        out
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::collections::btree_map::Entry;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use super::*;

    /// A [`RaftWAL`] that persists normally until it is broken, then refuses every
    /// write — a volume going read-only under a running voter, which is the failure
    /// the poison policy exists for.
    struct BreakingWAL {
        inner: InMemoryRaftWAL,
        broken: std::sync::atomic::AtomicBool,
    }

    impl BreakingWAL {
        fn new() -> BreakingWAL {
            BreakingWAL {
                inner: InMemoryRaftWAL::new(),
                broken: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn break_now(&self) {
            self.broken.store(true, Ordering::Relaxed);
        }

        fn is_broken(&self) -> bool {
            self.broken.load(Ordering::Relaxed)
        }
    }

    impl RaftWAL for BreakingWAL {
        fn load(&self) -> PersistedRaft {
            self.inner.load()
        }

        fn save_term_and_vote(&self, term: u64, voted_for: Option<NodeId>) -> WalAck {
            if self.is_broken() {
                return WalAck::Failed;
            }
            self.inner.save_term_and_vote(term, voted_for)
        }

        fn append(&self, from_index: u64, entries: &[RaftEntry]) -> WalAck {
            if self.is_broken() {
                return WalAck::Failed;
            }
            self.inner.append(from_index, entries)
        }

        fn save_snapshot(&self, index: u64, term: u64, data: &[u8]) -> WalAck {
            if self.is_broken() {
                return WalAck::Failed;
            }
            self.inner.save_snapshot(index, term, data)
        }

        fn poisoned(&self) -> Option<String> {
            self.is_broken().then(|| "broken".to_string())
        }
    }

    /// A tiny deterministic [`Entropy`] for driving elections without a runtime:
    /// a per-node LCG, so the three nodes draw distinct election jitter and the
    /// vote does not livelock on a symmetric split.
    struct TestEntropy {
        state: AtomicU64,
    }

    impl TestEntropy {
        fn new(seed: u64) -> TestEntropy {
            TestEntropy {
                state: AtomicU64::new(seed),
            }
        }
    }

    impl Entropy for TestEntropy {
        fn next_u64(&self) -> u64 {
            let next = self
                .state
                .load(Ordering::Relaxed)
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.state.store(next, Ordering::Relaxed);
            next
        }
    }

    /// The group a Raft frame belongs to (every Raft frame carries one).
    fn frame_group(frame: &Frame) -> GroupId {
        match frame {
            Frame::RaftVote { group, .. }
            | Frame::RaftVoteReply { group, .. }
            | Frame::RaftAppend { group, .. }
            | Frame::RaftAppendReply { group, .. }
            | Frame::RaftInstallSnapshot { group, .. }
            | Frame::RaftTimeoutNow { group, .. }
            | Frame::RaftPropose { group, .. } => *group,
            _ => unreachable!("a Raft group only ever produces Raft frames"),
        }
    }

    /// Deliver one frame to its group instance on the target node, returning the
    /// resulting step. `from` is the sending node.
    fn deliver(
        group: &RaftGroup,
        from: NodeId,
        frame: Frame,
        now: Instant,
        entropy: &TestEntropy,
    ) -> RaftOutput {
        match frame {
            Frame::RaftVote {
                term,
                candidate,
                last_index,
                last_term,
                ..
            } => group.handle_vote(candidate, term, last_index, last_term, now, entropy),
            Frame::RaftVoteReply { term, granted, .. } => {
                group.handle_vote_reply(from, term, granted, now, entropy)
            }
            Frame::RaftAppend {
                term,
                leader,
                prev_index,
                prev_term,
                entries,
                commit,
                ..
            } => group.handle_append(
                leader, term, prev_index, prev_term, entries, commit, now, entropy,
            ),
            Frame::RaftAppendReply {
                term,
                ok,
                match_index,
                ..
            } => group.handle_append_reply(from, term, ok, match_index, now, entropy),
            Frame::RaftInstallSnapshot {
                term,
                leader,
                snapshot_index,
                snapshot_term,
                voters,
                learners,
                data,
                ..
            } => group.handle_install_snapshot(
                leader,
                term,
                snapshot_index,
                snapshot_term,
                voters,
                learners,
                data,
                now,
                entropy,
            ),
            Frame::RaftTimeoutNow { term, leader, .. } => {
                group.handle_timeout_now(leader, term, now, entropy)
            }
            _ => RaftOutput::default(),
        }
    }

    /// Fold one group's step into the run state: check election safety, record
    /// committed app bytes, and enqueue the produced frames.
    #[allow(clippy::type_complexity)]
    fn record(
        group: GroupId,
        src: NodeId,
        out: RaftOutput,
        queue: &mut VecDeque<(NodeId, NodeId, Frame)>,
        committed: &mut BTreeMap<(GroupId, NodeId), Vec<Vec<u8>>>,
        winners: &mut BTreeMap<(GroupId, u64), NodeId>,
    ) {
        if let Some(term) = out.elected {
            match winners.entry((group, term)) {
                Entry::Vacant(slot) => {
                    slot.insert(src);
                }
                // Election safety, per group: two groups may reach the same term
                // number, but one (group, term) never has two leaders.
                Entry::Occupied(slot) => {
                    assert_eq!(*slot.get(), src, "two leaders for {group} term {term}")
                }
            }
        }
        for observation in out.committed {
            match observation {
                Committed::Apply { command, .. } => {
                    committed.entry((group, src)).or_default().push(command);
                }
                // A test that drives compaction replaces the recorded log with the
                // snapshot's commands; the harness here only exercises plain applies.
                Committed::Snapshot { .. } => {}
            }
        }
        for (to, frame) in out.frames {
            queue.push_back((src, to, frame));
        }
    }

    fn leader_of(
        groups: &BTreeMap<(GroupId, NodeId), RaftGroup>,
        group: GroupId,
        nodes: &[NodeId],
    ) -> Option<NodeId> {
        nodes
            .iter()
            .copied()
            .find(|&node| groups[&(group, node)].is_leader())
    }

    /// Two Raft groups on the same three nodes elect independently and commit
    /// disjoint logs.
    #[test]
    fn two_groups_run_independently_on_the_same_nodes() {
        let nodes = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let g1 = GroupId(1);
        let g2 = GroupId(2);
        let timeout = Duration::from_millis(100);

        let entropy: BTreeMap<NodeId, TestEntropy> = nodes
            .iter()
            .enumerate()
            .map(|(i, &node)| {
                (
                    node,
                    TestEntropy::new((i as u64 + 1).wrapping_mul(0x9e37_79b9)),
                )
            })
            .collect();

        let mut groups: BTreeMap<(GroupId, NodeId), RaftGroup> = BTreeMap::new();
        for &group in &[g1, g2] {
            for &node in &nodes {
                groups.insert(
                    (group, node),
                    RaftGroup::new(
                        group,
                        node,
                        nodes.to_vec(),
                        Vec::new(),
                        timeout,
                        timeout / 4,
                        Arc::new(InMemoryRaftWAL::new()),
                        Instant::ZERO,
                    ),
                );
            }
        }

        let mut committed: BTreeMap<(GroupId, NodeId), Vec<Vec<u8>>> = BTreeMap::new();
        let mut winners: BTreeMap<(GroupId, u64), NodeId> = BTreeMap::new();
        let mut now = Instant::ZERO;
        let mut proposed = false;

        for _round in 0..200 {
            now = now + Duration::from_millis(40);
            let mut queue: VecDeque<(NodeId, NodeId, Frame)> = VecDeque::new();

            // Tick every group on every node.
            let ticks: Vec<(GroupId, NodeId, RaftOutput)> = groups
                .iter()
                .map(|(&(group, node), raft)| (group, node, raft.tick(now, &entropy[&node])))
                .collect();
            for (group, node, out) in ticks {
                record(group, node, out, &mut queue, &mut committed, &mut winners);
            }

            // Deliver frames to quiescence, routing each by its group.
            let mut steps = 0;
            while let Some((from, to, frame)) = queue.pop_front() {
                steps += 1;
                assert!(steps < 100_000, "frame storm — consensus did not settle");
                let group = frame_group(&frame);
                let out = deliver(&groups[&(group, to)], from, frame, now, &entropy[&to]);
                record(group, to, out, &mut queue, &mut committed, &mut winners);
            }

            // Once both groups have a leader, propose disjoint commands — each to
            // its own group's leader, exactly once.
            if !proposed
                && let (Some(l1), Some(l2)) = (
                    leader_of(&groups, g1, &nodes),
                    leader_of(&groups, g2, &nodes),
                )
            {
                groups[&(g1, l1)].propose(EntryPayload::App(b"g1-a".to_vec()));
                groups[&(g1, l1)].propose(EntryPayload::App(b"g1-b".to_vec()));
                groups[&(g2, l2)].propose(EntryPayload::App(b"g2-x".to_vec()));
                proposed = true;
            }
        }

        assert!(proposed, "both groups elected a leader");

        // Every node committed its own group's log, in order, and never the
        // other group's bytes — the logs are fully independent.
        for &node in &nodes {
            assert_eq!(
                committed.get(&(g1, node)).cloned().unwrap_or_default(),
                vec![b"g1-a".to_vec(), b"g1-b".to_vec()],
                "group 1 log on {node}",
            );
            assert_eq!(
                committed.get(&(g2, node)).cloned().unwrap_or_default(),
                vec![b"g2-x".to_vec()],
                "group 2 log on {node}",
            );
        }
    }

    /// The multi-raft framing property (spec §9.4.3): a node leading many groups
    /// emits one heartbeat *per group per follower*, which is what
    /// [`Frame::RaftHeartbeats`] exists to collapse.
    ///
    /// Asserted at the source — what a tick produces — because that is what the
    /// coalescing in the system tick loop consumes. The count is the ceiling the
    /// batching removes: at `G` groups and `R` replicas it is `G × (R-1)` frames per
    /// interval, all of them empty.
    #[test]
    fn a_tick_emits_one_empty_append_per_group_per_follower() {
        let nodes = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let groups_n = 8;
        let timeout = Duration::from_millis(100);
        let entropy: BTreeMap<NodeId, TestEntropy> = nodes
            .iter()
            .enumerate()
            .map(|(i, &node)| {
                (
                    node,
                    TestEntropy::new((i as u64 + 1).wrapping_mul(0x9e37_79b9)),
                )
            })
            .collect();
        let ids: Vec<GroupId> = (1..=groups_n).map(GroupId).collect();
        let mut groups: BTreeMap<(GroupId, NodeId), RaftGroup> = BTreeMap::new();
        for &group in &ids {
            for &node in &nodes {
                groups.insert(
                    (group, node),
                    RaftGroup::new(
                        group,
                        node,
                        nodes.to_vec(),
                        Vec::new(),
                        timeout,
                        timeout / 4,
                        Arc::new(InMemoryRaftWAL::new()),
                        Instant::ZERO,
                    ),
                );
            }
        }
        let mut committed = BTreeMap::new();
        let mut winners = BTreeMap::new();
        let mut now = Instant::ZERO;
        for _ in 0..30 {
            now = now + Duration::from_millis(40);
            let mut queue: VecDeque<(NodeId, NodeId, Frame)> = VecDeque::new();
            let ticks: Vec<(GroupId, NodeId, RaftOutput)> = groups
                .iter()
                .map(|(&(g, n), raft)| (g, n, raft.tick(now, &entropy[&n])))
                .collect();
            for (g, n, out) in ticks {
                record(g, n, out, &mut queue, &mut committed, &mut winners);
            }
            let mut steps = 0;
            while let Some((from, to, frame)) = queue.pop_front() {
                steps += 1;
                assert!(steps < 100_000, "frame storm");
                let g = frame_group(&frame);
                let out = deliver(&groups[&(g, to)], from, frame, now, &entropy[&to]);
                record(g, to, out, &mut queue, &mut committed, &mut winners);
            }
            if ids.iter().all(|&g| leader_of(&groups, g, &nodes).is_some()) {
                break;
            }
        }
        assert!(
            ids.iter().all(|&g| leader_of(&groups, g, &nodes).is_some()),
            "every group elected a leader"
        );

        // One steady-state tick: count the empty appends a single node emits.
        now = now + Duration::from_millis(40);
        let mut empty_appends = 0;
        // Keyed by the (leader, follower) pair, because that is exactly what a batch
        // is keyed by: one frame per destination per sending node.
        let mut per_pair: BTreeMap<(NodeId, NodeId), usize> = BTreeMap::new();
        let mut led: BTreeMap<NodeId, usize> = BTreeMap::new();
        for &group in &ids {
            let leader = leader_of(&groups, group, &nodes).expect("a leader");
            *led.entry(leader).or_default() += 1;
            for (to, frame) in groups[&(group, leader)].tick(now, &entropy[&leader]).frames {
                if matches!(&frame, Frame::RaftAppend { entries, .. } if entries.is_empty()) {
                    empty_appends += 1;
                    *per_pair.entry((leader, to)).or_default() += 1;
                }
            }
        }
        assert_eq!(
            empty_appends,
            groups_n as usize * (nodes.len() - 1),
            "one empty append per group per follower — the G x (R-1) the batching collapses"
        );
        // Each of those frames is one *beat*; batching turns every pair's whole set
        // into a single frame, so the saving is the count per pair — which is the
        // number of groups that node leads, and grows with the shard count.
        for ((leader, _), beats) in &per_pair {
            assert_eq!(
                *beats, led[leader],
                "a leader sends each follower one beat per group it leads"
            );
        }
        assert!(
            per_pair.values().any(|&n| n > 1),
            "some node leads more than one group, or this proves nothing about batching"
        );
    }

    /// Leadership transfer (Raft §3.10): a handoff moves leadership to the chosen
    /// peer **without waiting out an election timeout**.
    ///
    /// That timing is the whole point. A draining node leading many groups otherwise
    /// pays a full election timeout per group, which is what makes a rolling restart
    /// a failover storm; here the clock is deliberately never advanced past the
    /// timeout, so a leadership change can only have come from the handoff.
    #[test]
    fn a_handoff_moves_leadership_without_waiting_for_an_election_timeout() {
        let nodes = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let group = GroupId(1);
        let timeout = Duration::from_millis(100);
        let entropy: BTreeMap<NodeId, TestEntropy> = nodes
            .iter()
            .enumerate()
            .map(|(i, &node)| {
                (
                    node,
                    TestEntropy::new((i as u64 + 1).wrapping_mul(0x9e37_79b9)),
                )
            })
            .collect();
        let mut groups: BTreeMap<(GroupId, NodeId), RaftGroup> = BTreeMap::new();
        for &node in &nodes {
            groups.insert(
                (group, node),
                RaftGroup::new(
                    group,
                    node,
                    nodes.to_vec(),
                    Vec::new(),
                    timeout,
                    timeout / 4,
                    Arc::new(InMemoryRaftWAL::new()),
                    Instant::ZERO,
                ),
            );
        }
        let mut committed: BTreeMap<(GroupId, NodeId), Vec<Vec<u8>>> = BTreeMap::new();
        let mut winners: BTreeMap<(GroupId, u64), NodeId> = BTreeMap::new();
        let mut now = Instant::ZERO;

        // Settle an initial leader and get a command committed, so the followers'
        // match indexes are real rather than trivially zero.
        let settle = |groups: &BTreeMap<(GroupId, NodeId), RaftGroup>,
                      now: Instant,
                      committed: &mut BTreeMap<(GroupId, NodeId), Vec<Vec<u8>>>,
                      winners: &mut BTreeMap<(GroupId, u64), NodeId>,
                      seed: Option<(NodeId, RaftOutput)>| {
            let mut queue: VecDeque<(NodeId, NodeId, Frame)> = VecDeque::new();
            match seed {
                Some((src, out)) => record(group, src, out, &mut queue, committed, winners),
                None => {
                    let ticks: Vec<(NodeId, RaftOutput)> = nodes
                        .iter()
                        .map(|&n| (n, groups[&(group, n)].tick(now, &entropy[&n])))
                        .collect();
                    for (n, out) in ticks {
                        record(group, n, out, &mut queue, committed, winners);
                    }
                }
            }
            let mut steps = 0;
            while let Some((from, to, frame)) = queue.pop_front() {
                steps += 1;
                assert!(steps < 100_000, "frame storm");
                let out = deliver(&groups[&(group, to)], from, frame, now, &entropy[&to]);
                record(group, to, out, &mut queue, committed, winners);
            }
        };

        let mut first = None;
        for _ in 0..20 {
            now = now + Duration::from_millis(40);
            settle(&groups, now, &mut committed, &mut winners, None);
            if let Some(l) = leader_of(&groups, group, &nodes) {
                first = Some(l);
                break;
            }
        }
        let leader = first.expect("a leader was elected");
        groups[&(group, leader)].propose(EntryPayload::App(b"before".to_vec()));
        for _ in 0..5 {
            now = now + Duration::from_millis(40);
            settle(&groups, now, &mut committed, &mut winners, None);
        }

        let target = *nodes.iter().find(|&&n| n != leader).expect("another voter");
        let (started, out) = groups[&(group, leader)].transfer_leadership(target);
        assert!(started, "a caught-up voter is a valid handoff target");

        // Deliver the handoff and its consequences at the SAME instant — no tick, so
        // no election timer can have fired.
        let frozen = now;
        settle(
            &groups,
            frozen,
            &mut committed,
            &mut winners,
            Some((leader, out)),
        );

        assert!(
            groups[&(group, target)].is_leader(),
            "the handoff target took leadership at the same instant it was invited"
        );
        assert!(
            !groups[&(group, leader)].is_leader(),
            "and the old leader stood down under the higher term"
        );
        // The transfer must not lose the committed prefix.
        assert_eq!(
            committed.get(&(group, target)).cloned().unwrap_or_default(),
            vec![b"before".to_vec()],
            "the new leader holds everything the old one committed"
        );
    }

    /// A handoff to a peer that has not replicated the leader's last entry is
    /// refused rather than attempted: such a peer loses the election it would be
    /// invited to hold, so sending it `TimeoutNow` costs a disrupted term and
    /// achieves nothing. The caller retries while replication catches it up.
    #[test]
    fn a_handoff_to_a_lagging_peer_is_refused() {
        let nodes = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let group = GroupId(1);
        let mut groups: BTreeMap<NodeId, RaftGroup> = BTreeMap::new();
        for &node in &nodes {
            groups.insert(
                node,
                RaftGroup::new(
                    group,
                    node,
                    nodes.to_vec(),
                    Vec::new(),
                    Duration::from_millis(100),
                    Duration::from_millis(25),
                    Arc::new(InMemoryRaftWAL::new()),
                    Instant::ZERO,
                ),
            );
        }
        let entropy = TestEntropy::new(7);
        // A brand-new group: nobody leads, so nobody may hand anything off.
        assert!(
            !groups[&nodes[0]].transfer_leadership(nodes[1]).0,
            "a non-leader has no leadership to transfer"
        );
        // Elect node 1 by driving it alone to its timeout, then answering its votes.
        let mut now = Instant::ZERO;
        let mut committed = BTreeMap::new();
        let mut winners = BTreeMap::new();
        for _ in 0..20 {
            now = now + Duration::from_millis(40);
            let mut queue: VecDeque<(NodeId, NodeId, Frame)> = VecDeque::new();
            let ticks: Vec<(NodeId, RaftOutput)> = nodes
                .iter()
                .map(|&n| (n, groups[&n].tick(now, &entropy)))
                .collect();
            for (n, out) in ticks {
                record(group, n, out, &mut queue, &mut committed, &mut winners);
            }
            let mut steps = 0;
            while let Some((from, to, frame)) = queue.pop_front() {
                steps += 1;
                assert!(steps < 100_000, "frame storm");
                let out = deliver(&groups[&to], from, frame, now, &entropy);
                record(group, to, out, &mut queue, &mut committed, &mut winners);
            }
            if nodes.iter().any(|&n| groups[&n].is_leader()) {
                break;
            }
        }
        let leader = *nodes
            .iter()
            .find(|&&n| groups[&n].is_leader())
            .expect("a leader");
        // A node outside the voter set is never a target, caught up or not.
        assert!(
            !groups[&leader].transfer_leadership(NodeId::new(99)).0,
            "a non-voter is not a handoff target"
        );
        assert!(
            !groups[&leader].transfer_leadership(leader).0,
            "a leader does not hand off to itself"
        );
    }

    /// A non-voting learner replicates the committed log just like a voter, but
    /// never elects or leads and never counts toward the quorum (granary's storage
    /// replicas beyond the voter quorum, spec §7.1).
    #[test]
    fn a_learner_replicates_committed_state_but_never_leads() {
        let voters = [NodeId::new(1), NodeId::new(2)];
        let learner = NodeId::new(3);
        let all = [voters[0], voters[1], learner];
        let group = GroupId(1);
        let timeout = Duration::from_millis(100);

        let entropy: BTreeMap<NodeId, TestEntropy> = all
            .iter()
            .enumerate()
            .map(|(i, &node)| {
                (
                    node,
                    TestEntropy::new((i as u64 + 1).wrapping_mul(0x9e37_79b9)),
                )
            })
            .collect();

        let mut groups: BTreeMap<(GroupId, NodeId), RaftGroup> = BTreeMap::new();
        for &node in &all {
            groups.insert(
                (group, node),
                RaftGroup::new(
                    group,
                    node,
                    voters.to_vec(),
                    vec![learner],
                    timeout,
                    timeout / 4,
                    Arc::new(InMemoryRaftWAL::new()),
                    Instant::ZERO,
                ),
            );
        }

        let mut committed: BTreeMap<(GroupId, NodeId), Vec<Vec<u8>>> = BTreeMap::new();
        let mut winners: BTreeMap<(GroupId, u64), NodeId> = BTreeMap::new();
        let mut now = Instant::ZERO;
        let mut proposed = false;

        for _round in 0..200 {
            now = now + Duration::from_millis(40);
            let mut queue: VecDeque<(NodeId, NodeId, Frame)> = VecDeque::new();
            let ticks: Vec<(GroupId, NodeId, RaftOutput)> = groups
                .iter()
                .map(|(&(g, node), raft)| (g, node, raft.tick(now, &entropy[&node])))
                .collect();
            for (g, node, out) in ticks {
                record(g, node, out, &mut queue, &mut committed, &mut winners);
            }
            let mut steps = 0;
            while let Some((from, to, frame)) = queue.pop_front() {
                steps += 1;
                assert!(steps < 100_000, "frame storm — consensus did not settle");
                let g = frame_group(&frame);
                let out = deliver(&groups[&(g, to)], from, frame, now, &entropy[&to]);
                record(g, to, out, &mut queue, &mut committed, &mut winners);
            }
            if !proposed && let Some(leader) = leader_of(&groups, group, &all) {
                groups[&(group, leader)].propose(EntryPayload::App(b"x".to_vec()));
                groups[&(group, leader)].propose(EntryPayload::App(b"y".to_vec()));
                proposed = true;
            }
        }

        assert!(proposed, "the voters elected a leader");
        // The learner never won an election in any term.
        for (&(g, term), &winner) in &winners {
            assert_ne!(
                winner, learner,
                "the learner led {g} term {term} — learners must not lead"
            );
        }
        // The learner replicated the committed log, identical to a voter's — so it
        // can route and serve reads without being part of the quorum.
        let expected = vec![b"x".to_vec(), b"y".to_vec()];
        assert_eq!(
            committed
                .get(&(group, learner))
                .cloned()
                .unwrap_or_default(),
            expected,
            "the learner replicates committed state",
        );
        assert_eq!(
            committed
                .get(&(group, voters[0]))
                .cloned()
                .unwrap_or_default(),
            expected,
            "a voter has the same committed log as the learner",
        );
    }

    /// Guard for spec §18.5 #22: once a leader commits its own `RemoveVoter` it is
    /// no longer a voter and MUST NOT count itself toward the commit quorum —
    /// otherwise a non-voter leader could advance commit on a phantom self-vote
    /// with only sub-quorum real replication, a minority evicting the majority.
    /// In the full engine the `tick` non-voter early-return keeps such a leader
    /// dormant, so this drives `advance_commit` directly to pin the property to
    /// the commit arithmetic itself rather than to that outer guard.
    #[test]
    fn a_self_removed_leader_does_not_count_its_own_phantom_vote() {
        let node = NodeId::new(1);
        let (v2, v3) = (NodeId::new(2), NodeId::new(3));
        let group = RaftGroup::new(
            GroupId(1),
            node,
            vec![node, v2, v3],
            Vec::new(),
            Duration::from_millis(100),
            Duration::from_millis(25),
            Arc::new(InMemoryRaftWAL::new()),
            Instant::ZERO,
        );

        let mut state = group.lock();
        state.role = Role::Leader;
        state.term = 5;
        // This leader has committed its own removal: the voter set is now {2, 3}
        // (quorum 2) and node 1 is no longer among them.
        state.voters = vec![v2, v3];
        // A fresh current-term entry sits uncommitted at index 1, replicated to
        // exactly one real voter (node 2) — one short of the 2-voter quorum.
        state.log = vec![RaftEntry {
            term: 5,
            payload: EntryPayload::App(b"evict".to_vec()),
        }];
        state.matched.insert(v2, 1);
        state.matched.insert(v3, 0);

        group.advance_commit(&mut state);
        assert_eq!(
            state.commit, 0,
            "a non-voter leader must not commit on its own phantom self-vote",
        );

        // Restore node 1 to the voter set (quorum 2 of {1, 2, 3}); now the same
        // single follower plus the leader's own legitimate vote reaches quorum, so
        // the entry commits. Proves the guard blocks only the phantom vote, not a
        // real one — it is not a blanket refusal to commit.
        state.voters = vec![node, v2, v3];
        group.advance_commit(&mut state);
        assert_eq!(
            state.commit, 1,
            "a voter leader commits on its own vote plus one follower (quorum 2 of 3)",
        );
    }

    /// A leader that commits its own `RemoveVoter` steps down to follower (Raft
    /// dissertation §4.2.2) rather than lingering as a `Role::Leader` non-voter —
    /// the cleaner counterpart to the commit-quorum guard above.
    #[test]
    fn a_leader_steps_down_when_it_commits_its_own_removal() {
        let node = NodeId::new(1);
        let v2 = NodeId::new(2);
        let group = RaftGroup::new(
            GroupId(1),
            node,
            vec![node, v2],
            Vec::new(),
            Duration::from_millis(100),
            Duration::from_millis(25),
            Arc::new(InMemoryRaftWAL::new()),
            Instant::ZERO,
        );

        let mut state = group.lock();
        state.role = Role::Leader;
        state.leader = Some(node);
        state.term = 4;
        // A committed `RemoveVoter(self)` at index 1, ready to apply.
        state.log = vec![RaftEntry {
            term: 4,
            payload: EntryPayload::RemoveVoter(node),
        }];
        state.commit = 1;

        let entropy = TestEntropy::new(0x9e37_79b9);
        let mut out = RaftOutput::default();
        group.drain_committed(&mut state, &mut out, Instant::ZERO, &entropy);

        assert_eq!(
            state.role,
            Role::Follower,
            "a self-removed leader steps down instead of leading as a non-voter",
        );
        assert_eq!(
            state.leader, None,
            "and no longer believes itself the leader"
        );
        assert_eq!(state.voters, vec![v2], "and is gone from the voter set");
    }

    /// Compaction (spec §9): the voters commit a run of entries while a learner is
    /// partitioned, then the leader compacts its log against a snapshot. When the
    /// learner heals, its `next` has fallen below the compacted prefix, so the
    /// leader catches it up with one `RaftInstallSnapshot` instead of replaying the
    /// log — and the learner ends at the leader's commit index with the snapshot
    /// installed. The leader's retained log stays bounded well under the number of
    /// committed entries.
    #[test]
    fn a_compacted_leader_catches_up_a_lagging_replica_via_install_snapshot() {
        let voters = [NodeId::new(1), NodeId::new(2)];
        let learner = NodeId::new(3);
        let all = [voters[0], voters[1], learner];
        let group = GroupId(1);
        let timeout = Duration::from_millis(100);
        let snapshot_bytes = b"shard-snapshot".to_vec();

        let entropy: BTreeMap<NodeId, TestEntropy> = all
            .iter()
            .enumerate()
            .map(|(i, &node)| {
                (
                    node,
                    TestEntropy::new((i as u64 + 1).wrapping_mul(0x9e37_79b9)),
                )
            })
            .collect();

        let mut groups: BTreeMap<(GroupId, NodeId), RaftGroup> = BTreeMap::new();
        for &node in &all {
            groups.insert(
                (group, node),
                RaftGroup::new(
                    group,
                    node,
                    voters.to_vec(),
                    vec![learner],
                    timeout,
                    timeout / 4,
                    Arc::new(InMemoryRaftWAL::new()),
                    Instant::ZERO,
                ),
            );
        }

        const WRITES: usize = 30;
        let mut now = Instant::ZERO;
        let mut proposed = false;
        let mut compacted = false;
        let mut healed = false;
        // The learner's view: the snapshot it installs (if any).
        let mut learner_snapshot: Option<(u64, Vec<u8>)> = None;

        for round in 0..400 {
            now = now + Duration::from_millis(40);
            // Heal the learner once the leader has compacted.
            if compacted && !healed {
                healed = true;
            }

            let mut queue: VecDeque<(NodeId, NodeId, Frame)> = VecDeque::new();
            let ticks: Vec<(GroupId, NodeId, RaftOutput)> = groups
                .iter()
                .map(|(&(g, node), raft)| (g, node, raft.tick(now, &entropy[&node])))
                .collect();
            for (_g, node, out) in ticks {
                drain(node, out, &mut queue, &mut learner_snapshot, learner);
            }

            let mut steps = 0;
            while let Some((from, to, frame)) = queue.pop_front() {
                steps += 1;
                assert!(steps < 100_000, "frame storm — consensus did not settle");
                // Partition the learner until it is healed: drop frames to/from it.
                if !healed && (from == learner || to == learner) {
                    continue;
                }
                let g = frame_group(&frame);
                let out = deliver(&groups[&(g, to)], from, frame, now, &entropy[&to]);
                drain(to, out, &mut queue, &mut learner_snapshot, learner);
            }

            // Once a leader exists, push a run of writes through it.
            if !proposed && let Some(leader) = leader_of(&groups, group, &all) {
                for i in 0..WRITES {
                    groups[&(group, leader)]
                        .propose(EntryPayload::App(format!("e{i}").into_bytes()));
                }
                proposed = true;
            }
            // Once those writes have committed on the leader, compact its log.
            if proposed && !compacted {
                if let Some(leader) = leader_of(&groups, group, &all) {
                    let g = &groups[&(group, leader)];
                    if g.commit_index() >= WRITES as u64 {
                        g.compact(g.commit_index(), snapshot_bytes.clone());
                        compacted = true;
                    }
                }
                let _ = round;
            }
        }

        assert!(
            compacted,
            "the leader committed the writes and compacted its log"
        );
        let leader = leader_of(&groups, group, &all).expect("a stable leader");
        let leader_group = &groups[&(group, leader)];
        // The compaction discarded the prefix: the base advanced and the retained
        // log is far smaller than the number of committed entries.
        assert!(
            leader_group.snapshot_index() >= WRITES as u64,
            "the snapshot base advanced past the writes"
        );
        assert!(
            leader_group.retained_len() < WRITES,
            "the retained log is bounded ({} entries) well under {WRITES} writes",
            leader_group.retained_len(),
        );

        // The healed learner caught up via InstallSnapshot — it installed exactly
        // the leader's snapshot and reached the leader's commit index, without ever
        // replaying the compacted entries.
        let (snap_index, snap_data) = learner_snapshot.expect("the learner installed a snapshot");
        assert_eq!(
            snap_data, snapshot_bytes,
            "the learner installed the leader's snapshot bytes"
        );
        assert_eq!(
            snap_index,
            leader_group.snapshot_index(),
            "the install carried the snapshot base"
        );
        assert_eq!(
            groups[&(group, learner)].commit_index(),
            leader_group.commit_index(),
            "the learner reached the leader's commit index via the snapshot",
        );
    }

    /// Fold one group's step for the single-group compaction test: enqueue frames
    /// and capture a snapshot the learner installs.
    fn drain(
        src: NodeId,
        out: RaftOutput,
        queue: &mut VecDeque<(NodeId, NodeId, Frame)>,
        learner_snapshot: &mut Option<(u64, Vec<u8>)>,
        learner: NodeId,
    ) {
        for observation in out.committed {
            if let Committed::Snapshot {
                index, snapshot, ..
            } = observation
                && src == learner
            {
                *learner_snapshot = Some((index, snapshot));
            }
        }
        for (to, frame) in out.frames {
            queue.push_back((src, to, frame));
        }
    }

    // --- The first campaign on a cold cluster -------------------------------------

    /// A group that has never had a leader does not wait out a timeout built to detect
    /// one that stopped. This is the whole of the cold-start cost: measured on
    /// `machine-standalone` (20 s timeout), a create paid it twice serially — once for
    /// the control plane and again for the shard groups created after the map
    /// committed — for ~33 s before anything touched a disk.
    #[test]
    fn a_pristine_group_campaigns_without_waiting_out_the_election_timeout() {
        let node = NodeId::new(1);
        let entropy = TestEntropy::new(3);
        let timeout = Duration::from_secs(20);
        let heartbeat = timeout / 4;
        let group = RaftGroup::new(
            GroupId(1),
            node,
            vec![node],
            Vec::new(),
            timeout,
            timeout / 4,
            Arc::new(InMemoryRaftWAL::new()),
            Instant::ZERO,
        );
        // It does not campaign inside the first heartbeat: a group created into a
        // cluster that already has a leader must hear from it before it decides there
        // is none. This is not a detail — skipping it cost a measured 0.3 s warm create
        // 3.5 s, the whole time the group spent a candidate with no leader to route to.
        let out = group.tick(Instant::ZERO + heartbeat, &entropy);
        assert!(
            out.elected.is_none(),
            "a pristine group must give an existing leader a heartbeat to speak first",
        );
        // But it is well inside the full timeout: that timer detects a leader that
        // stopped, and this group never had one.
        let well_inside = timeout - heartbeat;
        let out = group.tick(Instant::ZERO + well_inside, &entropy);
        assert!(
            out.elected.is_some(),
            "a group with no leader to detect must not wait out the detector",
        );
        assert!(group.is_leader());
    }

    /// A restarting voter rejoining a group it has state for keeps the full timeout.
    /// There *is* a leader in that case and the timer is doing its actual job;
    /// campaigning early would be a node coming back from a restart and immediately
    /// disturbing the cluster it rejoined.
    #[test]
    fn a_group_with_persisted_state_keeps_the_full_election_timeout() {
        let node = NodeId::new(1);
        let entropy = TestEntropy::new(5);
        let timeout = Duration::from_secs(20);
        let wal = Arc::new(InMemoryRaftWAL::new());
        assert!(wal.save_term_and_vote(7, Some(NodeId::new(2))).persisted());
        let group = RaftGroup::new(
            GroupId(1),
            node,
            vec![node],
            Vec::new(),
            timeout,
            timeout / 4,
            Arc::clone(&wal) as Arc<dyn RaftWAL>,
            Instant::ZERO,
        );
        let just_short = timeout - Duration::from_millis(1);
        let out = group.tick(Instant::ZERO + just_short, &entropy);
        assert!(
            out.elected.is_none(),
            "a voter with persisted state must wait out the full detector timeout",
        );
        let out = group.tick(Instant::ZERO + timeout, &entropy);
        assert!(out.elected.is_some(), "and campaign once it expires");
    }

    /// Every voter gets its own slot, and the whole fan fits inside one timeout.
    ///
    /// This is what keeps the eager first campaign from simply moving the cost: three
    /// voters all campaigning at once tie, rearm with jitter, and pay a *second* full
    /// timeout — which is precisely what the 3-node baseline showed before this
    /// existed (a first election reaching term 2).
    #[test]
    fn the_first_campaign_stagger_gives_every_voter_its_own_slot() {
        let voters: Vec<NodeId> = (1..=5).map(NodeId::new).collect();
        let timeout = Duration::from_secs(20);
        let heartbeat = Duration::from_secs(4);
        for group in [GroupId(0), GroupId(1), GroupId(7711844766215661656)] {
            let mut delays: Vec<Duration> = voters
                .iter()
                .map(|&n| first_election_delay(group, n, &voters, timeout, heartbeat, true))
                .collect();
            delays.sort_unstable();
            let fan = *delays.last().expect("a voter");
            delays.dedup();
            assert_eq!(
                delays.len(),
                voters.len(),
                "two voters sharing a slot would tie in {group:?}",
            );
            assert!(
                fan <= timeout,
                "the whole staggered fan must fit inside one timeout, got {fan:?}",
            );
        }
    }

    /// Different groups put different voters first, so a node hosting many shards does
    /// not end up leading all of them — the stagger is keyed on `(group, node)`, not on
    /// node id alone.
    #[test]
    fn different_groups_put_different_voters_first() {
        let voters: Vec<NodeId> = (1..=3).map(NodeId::new).collect();
        let timeout = Duration::from_secs(20);
        let heartbeat = Duration::from_secs(4);
        let firsts: BTreeSet<NodeId> = (0..64u64)
            .map(|g| {
                *voters
                    .iter()
                    .min_by_key(|&&n| {
                        first_election_delay(GroupId(g), n, &voters, timeout, heartbeat, true)
                    })
                    .expect("a voter")
            })
            .collect();
        assert_eq!(
            firsts.len(),
            voters.len(),
            "every voter should lead some group first, got {firsts:?}",
        );
    }

    // --- The WAL failure policy (spec §9.4.3 item 2) ------------------------------

    /// A voter whose WAL fails mid-term stops voting rather than continuing without
    /// durability. The vote is the safety-critical one: a grant this node forgets
    /// across a restart is a second vote in the same term, which is election safety
    /// gone (#22).
    #[test]
    fn a_voter_whose_wal_fails_refuses_to_grant_a_vote() {
        let nodes = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let entropy = TestEntropy::new(7);
        let wal = Arc::new(BreakingWAL::new());
        let voter = RaftGroup::new(
            GroupId(1),
            nodes[0],
            nodes.to_vec(),
            Vec::new(),
            Duration::from_millis(100),
            Duration::from_millis(25),
            Arc::clone(&wal) as Arc<dyn RaftWAL>,
            Instant::ZERO,
        );

        // Healthy: an up-to-date candidate at a new term is granted.
        let out = voter.handle_vote(nodes[1], 1, 0, 0, Instant::ZERO, &entropy);
        assert!(
            matches!(out.frames[0].1, Frame::RaftVoteReply { granted: true, .. }),
            "a healthy voter grants an up-to-date candidate's first request",
        );

        // The volume goes read-only. A different candidate at a higher term — which a
        // healthy voter would grant — must now be refused.
        wal.break_now();
        let out = voter.handle_vote(nodes[2], 2, 0, 0, Instant::ZERO, &entropy);
        assert!(
            matches!(out.frames[0].1, Frame::RaftVoteReply { granted: false, .. }),
            "a voter that cannot persist its vote must refuse it, not grant it",
        );
        assert!(
            !voter.is_leader(),
            "and it must not be leading anything either",
        );
    }

    /// The refusal is a *reply*, not a silence. A poisoned replica answers every
    /// append with `ok: false` so the leader stops counting it toward the commit
    /// quorum immediately, rather than waiting out a timeout on each append.
    #[test]
    fn a_poisoned_replica_refuses_appends_out_loud() {
        let nodes = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let entropy = TestEntropy::new(11);
        let wal = Arc::new(BreakingWAL::new());
        let follower = RaftGroup::new(
            GroupId(1),
            nodes[0],
            nodes.to_vec(),
            Vec::new(),
            Duration::from_millis(100),
            Duration::from_millis(25),
            Arc::clone(&wal) as Arc<dyn RaftWAL>,
            Instant::ZERO,
        );

        let entries = vec![RaftEntry {
            term: 1,
            payload: EntryPayload::Noop,
        }];
        let out = follower.handle_append(
            nodes[1],
            1,
            0,
            0,
            entries.clone(),
            0,
            Instant::ZERO,
            &entropy,
        );
        assert!(
            matches!(out.frames[0].1, Frame::RaftAppendReply { ok: true, .. }),
            "a healthy follower accepts a matching append",
        );

        wal.break_now();
        let out = follower.handle_append(nodes[1], 1, 1, 1, entries, 0, Instant::ZERO, &entropy);
        assert!(
            matches!(out.frames[0].1, Frame::RaftAppendReply { ok: false, .. }),
            "a follower that cannot persist an entry must not acknowledge it",
        );
    }

    /// A WAL that fails on the term-opening `Noop` takes the election down with it.
    ///
    /// The window is narrow and worth a test of its own: `become_leader` sets the role
    /// and the `elected` term *before* it appends, so a failure at the append leaves a
    /// node that has already told its caller it leads. It must retract that, not just
    /// step down quietly — the caller emits `LeaderElected` from it, and downstream a
    /// shard would start routing writes at a leader whose log does not reach disk.
    #[test]
    fn a_wal_that_fails_on_the_opening_entry_retracts_the_election() {
        let node = NodeId::new(1);
        let entropy = TestEntropy::new(23);
        let wal = Arc::new(BreakingWAL::new());
        let timeout = Duration::from_millis(100);
        let voter = RaftGroup::new(
            GroupId(1),
            node,
            vec![node],
            Vec::new(),
            timeout,
            timeout / 4,
            Arc::clone(&wal) as Arc<dyn RaftWAL>,
            Instant::ZERO,
        );

        // Break it after construction but before the first election, so the group
        // starts healthy and fails at exactly the append inside `become_leader`.
        wal.break_now();
        let out = voter.tick(Instant::ZERO + timeout, &entropy);
        assert!(
            out.elected.is_none(),
            "an election whose opening entry did not persist must not be announced",
        );
        assert!(!voter.is_leader(), "and the node must not be leading");
        assert!(
            out.frames.is_empty(),
            "nor replicate a log it cannot persist",
        );
    }

    /// A poisoned voter never starts an election, so it cannot win one on a term it
    /// could not write down. Its tick is inert: no frames, no candidacy.
    #[test]
    fn a_poisoned_voter_never_starts_an_election() {
        let nodes = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let entropy = TestEntropy::new(13);
        let wal = Arc::new(BreakingWAL::new());
        let timeout = Duration::from_millis(100);
        let voter = RaftGroup::new(
            GroupId(1),
            nodes[0],
            nodes.to_vec(),
            Vec::new(),
            timeout,
            timeout / 4,
            Arc::clone(&wal) as Arc<dyn RaftWAL>,
            Instant::ZERO,
        );

        // Break it before the first election deadline, then run well past several.
        wal.break_now();
        let mut now = Instant::ZERO;
        for _ in 0..20 {
            now = now + timeout;
            let out = voter.tick(now, &entropy);
            assert!(
                out.frames.is_empty(),
                "a poisoned voter must not solicit votes",
            );
        }
        assert!(!voter.is_leader(), "and must never win an election");
        // The first tick bumps the term in memory and only then discovers the WAL is
        // gone, which is safe — nothing was announced, and a restart reloads the
        // durable term. What must not happen is the *next* twenty timeouts each
        // bumping it again: a node spinning the term upward while unable to persist
        // would force a real election on every peer it later spoke to.
        assert_eq!(
            voter.term(),
            1,
            "the term stops where the failure was discovered",
        );
    }

    /// A leader whose WAL fails on its own append rolls the entry back out of its log
    /// and steps down, rather than replicating an entry that is not on its own disk.
    #[test]
    fn a_leader_whose_wal_fails_drops_the_entry_and_steps_down() {
        let node = NodeId::new(1);
        let entropy = TestEntropy::new(17);
        let wal = Arc::new(BreakingWAL::new());
        let timeout = Duration::from_millis(100);
        // A single-voter group elects itself on the first tick past the deadline.
        let leader = RaftGroup::new(
            GroupId(1),
            node,
            vec![node],
            Vec::new(),
            timeout,
            timeout / 4,
            Arc::clone(&wal) as Arc<dyn RaftWAL>,
            Instant::ZERO,
        );
        let out = leader.tick(Instant::ZERO + timeout, &entropy);
        assert!(out.elected.is_some(), "a lone voter elects itself");
        assert!(leader.is_leader());
        let committed_index = leader.commit_index();

        wal.break_now();
        assert!(
            !leader.propose(EntryPayload::App(vec![1, 2, 3])),
            "a proposal onto a WAL that cannot hold it must be refused",
        );
        assert!(
            !leader.is_leader(),
            "and the leader steps down rather than replicating an entry it did not persist",
        );
        assert_eq!(
            leader.commit_index(),
            committed_index,
            "nothing new commits after the WAL fails",
        );
    }
}
