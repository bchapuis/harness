//! Minimal deterministic Raft — a reusable **multi-group** consensus engine
//! (spec §9.4.3).
//!
//! The cluster self-hosts its membership authority as a **replicated log**: an
//! elected leader serializes every transition as a [`RaftEntry`], a quorum of
//! **voters** commits it, and the **commit index** is the authority stamp the
//! membership merge orders decisions by (spec §9.2). The scope is deliberately
//! minimal — elections, heartbeats, log replication, quorum commit, and
//! single-server voter changes; no snapshots or log compaction — enough for
//! every observable guarantee the spec requires (election safety, log matching,
//! leader completeness) and invariant #22.
//!
//! **Multi-group.** The consensus algorithm is generic; only the entry *payload*
//! and the voter-set-change handling are application-specific. So a single
//! [`RaftGroup`] carries an opaque application command as bytes
//! ([`EntryPayload::App`]) — the same opaque-bytes seam philosophy as `Transport`
//! and granary's `GrainJournal` — and [`MultiRaft`] runs O(groups) independent groups
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
    App(Vec<u8>),
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

/// The durability seam for a voter's Raft state (spec §9.4.3 item 2):
/// persisted before the state takes effect, reloaded on restart. The methods
/// are synchronous on purpose: when one returns, the data MUST be durable —
/// the caller sends the messages announcing the state right after. In-memory
/// for simulation ([`InMemoryRaftWAL`]); `actor-runtime` supplies the
/// production `FileRaftWAL`. One instance backs one `(group, node)`.
pub trait RaftWAL: Send + Sync + 'static {
    /// Load the persisted state (empty/default on first start).
    fn load(&self) -> PersistedRaft;

    /// Persist a new term and the vote cast in it.
    fn save_term_and_vote(&self, term: u64, voted_for: Option<NodeId>);

    /// Truncate the log at `from_index` (absolute, 0-based) and append `entries`
    /// there — Raft's conflict-resolution write. With a compacted prefix
    /// (`snapshot_index > 0`) `from_index` is still absolute; the storage maps it
    /// onto its retained suffix.
    fn append(&self, from_index: u64, entries: &[RaftEntry]);

    /// Record a state-machine snapshot at `index`/`term` (§9) and discard the log
    /// prefix it subsumes (every entry with absolute index `≤ index`). Persisted
    /// before the engine drops the prefix in memory, so a restart reloads from the
    /// snapshot plus the retained tail rather than a blank log. The default keeps
    /// the whole log (no compaction) for a storage that has not implemented it.
    fn save_snapshot(&self, index: u64, term: u64, data: &[u8]) {
        let _ = (index, term, data);
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

    fn save_term_and_vote(&self, term: u64, voted_for: Option<NodeId>) {
        let mut state = self.state.lock().expect("raft storage mutex poisoned");
        state.term = term;
        state.voted_for = voted_for;
    }

    fn append(&self, from_index: u64, entries: &[RaftEntry]) {
        let mut state = self.state.lock().expect("raft storage mutex poisoned");
        // `from_index` is absolute; the retained log begins at `snapshot_index + 1`.
        let local = from_index.saturating_sub(state.snapshot_index) as usize;
        state.log.truncate(local);
        state.log.extend_from_slice(entries);
    }

    fn save_snapshot(&self, index: u64, term: u64, data: &[u8]) {
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
    /// caches one [`InMemoryRaftWAL`] per `(group, node)`, so state survives
    /// as long as the config does — under simulation, a restarted node reloads
    /// its persisted Raft state exactly as a production node reloads it from disk.
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
    storage: Arc<dyn Fn(GroupId, NodeId) -> Arc<dyn RaftWAL> + Send + Sync>,
    groups: Mutex<BTreeMap<GroupId, Arc<RaftGroup>>>,
}

impl MultiRaft {
    /// Build the engine and create the control group from `config.voters`,
    /// reloading its persisted state (spec §9.4.3 item 2). The election timer
    /// arms from `now`.
    pub(crate) fn new(node: NodeId, config: &RaftConfig, now: Instant) -> MultiRaft {
        let engine = MultiRaft {
            node,
            election_timeout: config.election_timeout,
            storage: Arc::clone(&config.storage),
            groups: Mutex::new(BTreeMap::new()),
        };
        engine.create_group(GroupId::CONTROL, config.voters.clone(), Vec::new(), now);
        engine
    }

    /// Create (or replace) the group `group` with voter set `voters`, reloading
    /// its persisted state from the factory. Used at startup for the control
    /// group, and (later) per shard. The election timer arms from `now` with no
    /// jitter — the same first-tick behavior as a single group, so the entropy
    /// draw order stays identical on the control-only path.
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
///
/// `Apply` is one committed application command at its log `index` (the membership
/// merge stamps decisions by it, spec §9.2; a granary shard appends its journal
/// record). `Snapshot` is delivered when this node **installs** a leader's state-
/// machine snapshot (the log prefix it subsumes was compacted away, §9): the
/// caller must *replace* its state with `snapshot`, which reflects every command
/// through `index`. The two share one ordered stream, so an install and the
/// commands after it never reorder.
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
    /// reads; bounding `voters` to `R` keeps write quorum at `⌈R/2⌉` independent of
    /// cluster size.
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
            self.log[(index - self.snapshot_index) as usize - 1].term
        }
    }

    /// The entry at absolute `index` (`> snapshot_index`), for application and
    /// replication slicing.
    fn entry_at(&self, index: u64) -> &RaftEntry {
        &self.log[(index - self.snapshot_index) as usize - 1]
    }

    /// The retained log entries from absolute index `first` (inclusive); `first`
    /// must be `> snapshot_index` (entry `first` lives at local
    /// `first - snapshot_index - 1`).
    fn suffix_from(&self, first: u64) -> &[RaftEntry] {
        &self.log[(first - self.snapshot_index - 1) as usize..]
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

/// One group's Raft instance on this node (spec §9.4.3). A voter elects and
/// leads; a non-voter is a passive learner that only replicates committed
/// state. All state sits behind one mutex, mutated by the driver tick and the
/// frame handlers.
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
    pub(crate) fn new(
        group: GroupId,
        node: NodeId,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
        election_timeout: Duration,
        storage: Arc<dyn RaftWAL>,
        now: Instant,
    ) -> RaftGroup {
        let persisted = storage.load();
        let (voters, learners) = normalize_membership(voters, learners);
        let snapshot_index = persisted.snapshot_index;
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
            // The base timeout without jitter; re-armed with jitter on every
            // subsequent reset (the first tick draws no entropy, keeping the
            // draw order simple and deterministic).
            election_deadline: now + election_timeout,
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
    /// A node that restarts from a snapshot reloads it into [`RaftState`] but the
    /// engine never re-emits already-applied state on the commit stream (`applied`
    /// starts at the snapshot base, see [`new`]). So a fresh subscriber — a granary
    /// journal rebuilding its projection after a full cluster restart — would
    /// otherwise see only the post-snapshot tail and miss the whole compacted
    /// prefix. Handing it this observation first rebuilds the projection from the
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
    /// (spec §9.4.3 item 2).
    fn persist_term(&self, state: &RaftState) {
        self.storage.save_term_and_vote(state.term, state.voted_for);
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
            self.persist_term(state);
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

    /// The driver tick (spec §9.4.3): a follower/candidate whose election timer
    /// fired starts an election; a leader replicates its log and heartbeats,
    /// advancing the commit index over quorum-matched, current-term entries.
    pub(crate) fn tick<E: Entropy>(&self, now: Instant, entropy: &E) -> RaftOutput {
        let mut out = RaftOutput::default();
        let mut state = self.lock();
        // Only voters elect and lead (spec §9.4.3 item 2); a non-voter node is a
        // passive learner whose tick does nothing.
        if !state.voters.contains(&self.node) {
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
        self.drain_committed(&mut state, &mut out);
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
        self.persist_term(state);
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
        // the leader sends each the right log suffix. Only voters' progress is
        // consulted for the commit quorum (`advance_commit`).
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
        self.replicate(state, out);
    }

    fn append_entry(&self, state: &mut RaftState, entry: RaftEntry) -> u64 {
        let from = state.last_index();
        state.log.push(entry);
        let local = (from - state.snapshot_index) as usize;
        self.storage.append(from, &state.log[local..]);
        state.last_index()
    }

    /// Append `payload` if this node leads and no identical payload is already
    /// pending (uncommitted) — the dedup that keeps the leader's per-tick duties
    /// and forwarded proposals from piling up duplicates. Returns whether the
    /// payload is now in the log (newly or already). Byte-equality of an
    /// [`EntryPayload::App`] makes a re-proposed application command idempotent,
    /// exactly as a repeated config command was.
    pub(crate) fn propose(&self, payload: EntryPayload) -> bool {
        let mut state = self.lock();
        if state.role != Role::Leader {
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
        let drop = (index - state.snapshot_index) as usize;
        state.log.drain(..drop);
        state.snapshot_index = index;
        state.snapshot_term = term;
        state.snapshot = Some(snapshot.clone());
        self.storage.save_snapshot(index, term, &snapshot);
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
        // Replicate to every other member, learners included (§7.1): a learner
        // gets the committed log so it can route and serve reads, but never votes
        // or counts toward the commit quorum (`advance_commit`).
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
    /// counting (Raft §5.4.2), and the leader itself always matches its log.
    fn advance_commit(&self, state: &mut RaftState) {
        for index in (state.commit + 1..=state.last_index()).rev() {
            if state.term_at(index) != state.term {
                continue;
            }
            let replicated = 1 + state
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
    fn drain_committed(&self, state: &mut RaftState, out: &mut RaftOutput) {
        while state.applied < state.commit {
            state.applied += 1;
            // Cloned out of the log (a `RaftEntry` is no longer `Copy`); the
            // borrow ends before the voter-set mutations below.
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
    /// current, we have not voted for another in it, the candidate's log is at
    /// least as up-to-date as ours, and we are a voter.
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
        let granted = state.voters.contains(&self.node)
            && term == state.term
            && state.voted_for.is_none_or(|v| v == from)
            && up_to_date;
        if granted {
            state.voted_for = Some(from);
            self.persist_term(&state);
            self.rearm_election(&mut state, now, entropy);
        }
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
        self.drain_committed(&mut state, &mut out);
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
        if term < state.term {
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
                let local = (index - state.snapshot_index) as usize - 1;
                state.log.truncate(local);
                append_from = i;
                break;
            }
        }
        if append_from < entries.len() {
            let base = state.last_index();
            state.log.extend_from_slice(&entries[append_from..]);
            self.storage.append(base, &entries[append_from..]);
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
        self.drain_committed(&mut state, &mut out);
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
        self.become_follower(&mut state, term, now, entropy);
        state.leader = Some(from);
        // Only install a snapshot that carries us past what we have committed; an
        // older or duplicate one is acked at our own level.
        if snapshot_index > state.commit {
            state.log.clear();
            state.snapshot_index = snapshot_index;
            state.snapshot_term = snapshot_term;
            state.snapshot = Some(data.clone());
            state.commit = snapshot_index;
            state.applied = snapshot_index;
            (state.voters, state.learners) = normalize_membership(voters, learners);
            // Persist the snapshot, then clear the stored log tail so a reload
            // reconstructs from the base alone (the in-memory log is now empty).
            self.storage
                .save_snapshot(snapshot_index, snapshot_term, &data);
            self.storage.append(snapshot_index, &[]);
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
        self.drain_committed(&mut state, &mut out);
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
    /// disjoint logs — the multi-group capability the engine exists to provide.
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
}
