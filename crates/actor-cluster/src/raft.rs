//! Minimal deterministic Raft for the leader-based control plane (spec §9.4.3).
//!
//! The cluster self-hosts its membership authority as a **replicated log**: an
//! elected leader serializes every membership transition as a [`LogEntry`], a
//! quorum of **voters** commits it, and the **commit index** is the authority
//! stamp the membership merge orders decisions by (spec §9.2). The scope is
//! deliberately minimal — elections, heartbeats, log replication, quorum
//! commit, and single-server voter changes; no snapshots or log compaction —
//! enough for every observable guarantee the spec requires (election safety,
//! log matching, leader completeness) and invariant #22.
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

/// A membership transition carried as one log entry (spec §9.4.3 item 1): the
/// state machine the replicated log drives is the member set itself.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum RaftCommand {
    /// Admit a node as a full `up` member (`joining → up`, or a new member).
    Admit(NodeId),
    /// Cordon a member for maintenance — the reversible `draining` state.
    Drain(NodeId),
    /// Return a drained member to `up`.
    Resume(NodeId),
    /// Finalize a graceful leave (`leaving → down`), committed at the departing
    /// node's request (spec §9.3).
    Leave(NodeId),
    /// Declare a member terminally `down` — the detector-fed, policy-gated,
    /// quorum-committed downing decision (spec §9.4.3 item 4, invariant #22).
    Down(NodeId),
    /// Add a voter (single-server configuration change, spec §9.4.3 item 2).
    AddVoter(NodeId),
    /// Remove a voter (single-server configuration change).
    RemoveVoter(NodeId),
    /// The no-op a new leader commits to open its term (leader completeness).
    Noop,
}

/// One replicated log entry: the `term` it was proposed in and the membership
/// `command` it carries (spec §9.4.3 item 1).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub command: RaftCommand,
}

/// The durable Raft state a voter must persist (spec §9.4.3 item 2): the
/// current term, the vote cast in it, and the log.
#[derive(Clone, Debug, Default)]
pub struct PersistedRaft {
    pub term: u64,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,
}

/// The durability seam for a voter's Raft state (spec §9.4.3 item 2):
/// persisted before the state takes effect, reloaded on restart. In-memory for
/// simulation ([`InMemoryRaftStorage`]); a production implementation persists
/// to disk.
pub trait RaftStorage: Send + Sync + 'static {
    /// Load the persisted state (empty/default on first start).
    fn load(&self) -> PersistedRaft;

    /// Persist a new term and the vote cast in it.
    fn save_term_and_vote(&self, term: u64, voted_for: Option<NodeId>);

    /// Truncate the log at `from_index` (0-based) and append `entries` there —
    /// Raft's conflict-resolution write.
    fn append(&self, from_index: u64, entries: &[LogEntry]);
}

/// A volatile [`RaftStorage`]: state survives as long as the value does. The
/// simulation implementation, and a starting point for production.
#[derive(Default)]
pub struct InMemoryRaftStorage {
    state: Mutex<PersistedRaft>,
}

impl InMemoryRaftStorage {
    pub fn new() -> InMemoryRaftStorage {
        InMemoryRaftStorage::default()
    }
}

impl RaftStorage for InMemoryRaftStorage {
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

    fn append(&self, from_index: u64, entries: &[LogEntry]) {
        let mut state = self.state.lock().expect("raft storage mutex poisoned");
        state.log.truncate(from_index as usize);
        state.log.extend_from_slice(entries);
    }
}

/// Configuration of the leader-based control plane (spec §9.4.3).
#[derive(Clone)]
pub struct RaftConfig {
    /// The initial voter set (spec §9.4.3 item 2): a configured, modest subset
    /// of members (typically 3 or 5), identical on every node. Later changes are
    /// committed [`RaftCommand::AddVoter`]/[`RaftCommand::RemoveVoter`] entries.
    pub voters: Vec<NodeId>,
    /// Base election timeout; each election round waits the base plus jitter in
    /// `[0, base)` drawn from `Entropy` (spec §9.4.3 item 7).
    pub election_timeout: Duration,
    /// Leader heartbeat/replication cadence. Must be well under
    /// `election_timeout`.
    pub heartbeat_interval: Duration,
    /// Per-node storage factory (spec §9.4.3 item 2). Defaults to in-memory.
    pub storage: Arc<dyn Fn(NodeId) -> Arc<dyn RaftStorage> + Send + Sync>,
}

impl RaftConfig {
    /// A config for `voters` with in-memory storage and default timing
    /// (1s election timeout, 250ms heartbeats).
    pub fn new(voters: Vec<NodeId>) -> RaftConfig {
        RaftConfig {
            voters,
            election_timeout: Duration::from_secs(1),
            heartbeat_interval: Duration::from_millis(250),
            storage: Arc::new(|_| Arc::new(InMemoryRaftStorage::new())),
        }
    }
}

// --- The consensus state machine ----------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

/// What one Raft step produced, for the caller to act on: frames to send over
/// the transport, entries newly committed (in log order — the caller applies
/// each membership command via `Membership::apply_stamped` with the entry's
/// index as the authority stamp, spec §9.2), and the term won if this step made
/// the node leader (the caller emits `LeaderElected`, spec §16).
#[derive(Default)]
pub(crate) struct RaftOutput {
    pub frames: Vec<(NodeId, Frame)>,
    pub committed: Vec<(u64, RaftCommand)>,
    pub elected: Option<u64>,
}

struct RaftState {
    role: Role,
    term: u64,
    voted_for: Option<NodeId>,
    /// The replicated log; entry `i` (1-based) lives at `log[i - 1]`.
    log: Vec<LogEntry>,
    /// Highest committed index; `0` = nothing committed.
    commit: u64,
    /// Highest index whose command has been handed to the caller for
    /// application; trails `commit` only inside a step.
    applied: u64,
    /// The current voter set: the configured one plus committed
    /// `AddVoter`/`RemoveVoter` changes, kept sorted (determinism, spec §4.6 #4).
    voters: Vec<NodeId>,
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

impl RaftState {
    fn last_index(&self) -> u64 {
        self.log.len() as u64
    }

    fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            0
        } else {
            self.log[index as usize - 1].term
        }
    }

    fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }
}

/// One node's Raft instance (spec §9.4.3). Every leader-mode node runs one —
/// non-voters as passive learners that only forward proposals — and all state
/// sits behind one mutex, mutated by the driver tick and the frame handlers.
pub(crate) struct Raft {
    node: NodeId,
    election_timeout: Duration,
    storage: Arc<dyn RaftStorage>,
    state: Mutex<RaftState>,
}

impl Raft {
    /// Build the instance from `config`, reloading any persisted state
    /// (spec §9.4.3 item 2). The election timer arms from `now`.
    pub(crate) fn new(node: NodeId, config: &RaftConfig, now: Instant) -> Raft {
        let storage = (config.storage)(node);
        let persisted = storage.load();
        let mut voters = config.voters.clone();
        voters.sort();
        voters.dedup();
        let state = RaftState {
            role: Role::Follower,
            term: persisted.term,
            voted_for: persisted.voted_for,
            log: persisted.log,
            commit: 0,
            applied: 0,
            voters,
            leader: None,
            votes: BTreeSet::new(),
            next: BTreeMap::new(),
            matched: BTreeMap::new(),
            // The base timeout without jitter; re-armed with jitter on every
            // subsequent reset (the first tick draws no entropy, keeping the
            // draw order simple and deterministic).
            election_deadline: now + config.election_timeout,
        };
        Raft {
            node,
            election_timeout: config.election_timeout,
            storage,
            state: Mutex::new(state),
        }
    }

    /// Whether `node` is currently in the voter set.
    pub(crate) fn has_voter(&self, node: NodeId) -> bool {
        self.lock().voters.contains(&node)
    }

    /// Whether this node currently leads.
    pub(crate) fn is_leader(&self) -> bool {
        self.lock().role == Role::Leader
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
        let next = state.last_index() + 1;
        state.next = state.voters.iter().map(|&v| (v, next)).collect();
        state.matched = state.voters.iter().map(|&v| (v, 0)).collect();
        let term = state.term;
        self.append_entry(
            state,
            LogEntry {
                term,
                command: RaftCommand::Noop,
            },
        );
        self.replicate(state, out);
    }

    fn append_entry(&self, state: &mut RaftState, entry: LogEntry) -> u64 {
        let from = state.last_index();
        state.log.push(entry);
        self.storage.append(from, &state.log[from as usize..]);
        state.last_index()
    }

    /// Append `command` if this node leads and no identical command is already
    /// pending (uncommitted) — the dedup that keeps the leader's per-tick
    /// control-plane duties and forwarded proposals from piling up duplicates.
    /// Returns whether the command is now in the log (newly or already).
    pub(crate) fn propose(&self, command: RaftCommand) -> bool {
        let mut state = self.lock();
        if state.role != Role::Leader {
            return false;
        }
        let pending = state.log[state.commit as usize..]
            .iter()
            .any(|e| e.command == command);
        if !pending {
            let term = state.term;
            self.append_entry(&mut state, LogEntry { term, command });
        }
        true
    }

    /// Leader replication (spec §9.4.3 item 3): send each other voter the log
    /// suffix it still misses (a heartbeat when empty), then advance the commit
    /// index to the highest current-term entry a quorum has matched.
    fn replicate(&self, state: &mut RaftState, out: &mut RaftOutput) {
        let peers: Vec<NodeId> = state
            .voters
            .iter()
            .copied()
            .filter(|&v| v != self.node)
            .collect();
        for peer in peers {
            let next = *state.next.get(&peer).unwrap_or(&(state.last_index() + 1));
            let prev_index = next - 1;
            let entries: Vec<LogEntry> = state.log[prev_index as usize..].to_vec();
            out.frames.push((
                peer,
                Frame::RaftAppend {
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

    /// Hand newly committed commands to the caller, applying voter-set changes
    /// internally (spec §9.4.3 item 2: a committed configuration entry).
    fn drain_committed(&self, state: &mut RaftState, out: &mut RaftOutput) {
        while state.applied < state.commit {
            state.applied += 1;
            let entry = state.log[state.applied as usize - 1];
            match entry.command {
                RaftCommand::AddVoter(node) if !state.voters.contains(&node) => {
                    state.voters.push(node);
                    state.voters.sort();
                    if state.role == Role::Leader {
                        let next = state.last_index() + 1;
                        state.next.entry(node).or_insert(next);
                        state.matched.entry(node).or_insert(0);
                    }
                }
                RaftCommand::RemoveVoter(node) => {
                    state.voters.retain(|&v| v != node);
                    state.next.remove(&node);
                    state.matched.remove(&node);
                }
                _ => {}
            }
            out.committed.push((state.applied, entry.command));
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
        entries: Vec<LogEntry>,
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
                    term: state.term,
                    ok: false,
                    match_index: 0,
                },
            ));
            return out;
        }
        self.become_follower(&mut state, term, now, entropy);
        state.leader = Some(from);

        // Log-matching check: the entry before the suffix must agree.
        if prev_index > state.last_index() || state.term_at(prev_index) != prev_term {
            out.frames.push((
                from,
                Frame::RaftAppendReply {
                    term: state.term,
                    ok: false,
                    // A hint for the leader: everything past our log cannot match.
                    match_index: state.last_index().min(prev_index.saturating_sub(1)),
                },
            ));
            return out;
        }
        // Truncate any conflicting suffix, then append what is genuinely new.
        // `entries[i]` carries the 1-based index `prev_index + 1 + i`.
        let mut append_from = entries.len();
        for (i, entry) in entries.iter().enumerate() {
            let index = prev_index + 1 + i as u64;
            if index > state.last_index() {
                append_from = i;
                break;
            }
            if state.term_at(index) != entry.term {
                state.log.truncate(index as usize - 1);
                append_from = i;
                break;
            }
        }
        if append_from < entries.len() {
            let base = state.last_index();
            state.log.extend_from_slice(&entries[append_from..]);
            self.storage.append(base, &entries[append_from..]);
        }
        let match_index = prev_index + entries.len() as u64;
        state.commit = state.commit.max(commit.min(state.last_index()));
        out.frames.push((
            from,
            Frame::RaftAppendReply {
                term: state.term,
                ok: true,
                match_index,
            },
        ));
        self.drain_committed(&mut state, &mut out);
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
