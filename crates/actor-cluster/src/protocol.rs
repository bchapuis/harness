//! The wire protocol: frames exchanged over an association (spec §7.1).
//!
//! This is the vocabulary every subsystem speaks across the network — actor
//! envelopes and replies, SWIM probes, death-watch, and receptionist
//! replication. It is kept apart from the [`Transport`](crate::Transport) trait
//! (the *mechanism* that carries frames, in [`crate::transport`]): the protocol
//! references domain types (e.g. [`MemberDigest`]), while the transport trait
//! stays a thin carrier that need not know any subsystem's payload.

use actor_core::ActorId;
use actor_core::NodeId;
use actor_core::ReplyResult;
use actor_core::TerminationReason;
use serde::Deserialize;
use serde::Serialize;

use crate::membership::MemberDigest;
use crate::raft::GroupId;
use crate::raft::RaftEntry;

/// A correlation id pairing a request with its reply on an association (spec
/// §7.1).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct CallId(pub u64);

impl From<u64> for CallId {
    fn from(n: u64) -> CallId {
        CallId(n)
    }
}

/// One receptionist registration: an actor registered under `key` by `origin`
/// (spec §13). Carried in bulk by [`Frame::ReceptionistSync`] for anti-entropy.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReceptionistEntry {
    pub key: String,
    pub origin: NodeId,
    pub actor: ActorId,
}

/// A frame exchanged over an association (spec §7.1). The message `payload` is
/// already codec-encoded; under simulation the frame itself travels in-memory,
/// so only the payload exercises the wire codec (spec §18.2). In production the
/// whole frame is codec-encoded onto the wire, hence `Serialize`/`Deserialize`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Frame {
    /// An actor envelope: `correlation` is `Some` for an `ask`, `None` for a
    /// one-way `tell`.
    Envelope {
        recipient: ActorId,
        manifest: String,
        correlation: Option<CallId>,
        payload: Vec<u8>,
    },
    /// The reply to an `ask`, referencing its correlation id. The outcome is the
    /// encoded reply bytes, or a transport/system `CallError`.
    Reply {
        correlation: CallId,
        outcome: ReplyResult,
    },
    /// A SWIM failure-detector probe (spec §10). Carries the sender's
    /// `incarnation` (direct liveness evidence) and a gossip `digest` piggybacked
    /// to disseminate membership (spec §9.2, §10 #6); `seq` correlates the `Ack`.
    Ping {
        seq: u64,
        incarnation: u64,
        digest: Vec<MemberDigest>,
    },
    /// The reply to a `Ping`, echoing its `seq` and carrying the sender's own
    /// incarnation and gossip digest (spec §10).
    Ack {
        seq: u64,
        incarnation: u64,
        digest: Vec<MemberDigest>,
    },
    /// A request to indirectly probe `target` on the sender's behalf (spec §10
    /// #2): the helper pings `target` and, on success, returns an `IndirectAck`
    /// echoing `seq`. Carries the requester's `incarnation` and a gossip `digest`.
    PingReq {
        seq: u64,
        target: NodeId,
        incarnation: u64,
        digest: Vec<MemberDigest>,
    },
    /// A helper's relay that `target` answered an indirect probe (spec §10 #2):
    /// it echoes the requester's `seq` and carries the `target`'s `incarnation`
    /// (so the requester can clear its suspicion) plus a gossip `digest`.
    IndirectAck {
        seq: u64,
        target: NodeId,
        incarnation: u64,
        digest: Vec<MemberDigest>,
    },
    /// Register cross-node death watch (spec §12): `watcher` (on the sender's
    /// node) wants to be notified when `target` (on this frame's destination
    /// node) terminates.
    Watch { target: ActorId, watcher: ActorId },
    /// Cancel a cross-node death watch (spec §12).
    Unwatch { target: ActorId, watcher: ActorId },
    /// Notify a remote `watcher` that `target` has terminated (spec §12). Sent
    /// from the target's node to the watcher's node.
    Terminated {
        target: ActorId,
        watcher: ActorId,
        reason: TerminationReason,
    },
    /// A receptionist registration replicated from `origin` (spec §13) —
    /// broadcast on change, when a registration first happens.
    Receptionist {
        key: String,
        origin: NodeId,
        actor: ActorId,
    },
    /// A node's full receptionist registry, pushed periodically to a random peer
    /// for anti-entropy (spec §13): it reconciles registrations a node missed —
    /// because it joined late or a broadcast was lost — without the registrant
    /// having to re-broadcast.
    ReceptionistSync { entries: Vec<ReceptionistEntry> },
    /// A Raft vote request (leader-based mode, spec §9.4.3): `candidate` asks
    /// for the vote in `term` for Raft `group`, proving its log is up to date
    /// with its last entry's index and term.
    RaftVote {
        group: GroupId,
        term: u64,
        candidate: NodeId,
        last_index: u64,
        last_term: u64,
    },
    /// The reply to a [`RaftVote`](Frame::RaftVote).
    RaftVoteReply {
        group: GroupId,
        term: u64,
        granted: bool,
    },
    /// Raft log replication and heartbeat (spec §9.4.3): the `leader` sends
    /// `group`'s log suffix after `(prev_index, prev_term)` plus its commit
    /// index.
    RaftAppend {
        group: GroupId,
        term: u64,
        leader: NodeId,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<RaftEntry>,
        commit: u64,
    },
    /// The reply to a [`RaftAppend`](Frame::RaftAppend): on success,
    /// `match_index` is the highest replicated index; on a log mismatch it is a
    /// back-off hint.
    RaftAppendReply {
        group: GroupId,
        term: u64,
        ok: bool,
        match_index: u64,
    },
    /// A state-machine snapshot the `leader` sends a follower whose `next` has
    /// fallen below the leader's compacted prefix (spec §9): the log entries that
    /// would catch it up no longer exist, so the leader ships the snapshot that
    /// subsumes them. The follower installs it (replacing its state through
    /// `snapshot_index`) and replies with an ordinary
    /// [`RaftAppendReply`](Frame::RaftAppendReply). `data` is the opaque
    /// application snapshot; `voters`/`learners` are the membership as of the base.
    RaftInstallSnapshot {
        group: GroupId,
        term: u64,
        leader: NodeId,
        snapshot_index: u64,
        snapshot_term: u64,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
        data: Vec<u8>,
    },
    /// An application command offered to `group`'s leader (spec §9.4.3 item 1):
    /// a non-leader node sends it to a voter, which forwards it to its leader.
    /// The command is the opaque app payload (the engine's `EntryPayload::App`
    /// bytes — for the control group, an encoded `MembershipCommand`).
    /// `forwarded` stops a stale-leader loop — a forwarded proposal landing on
    /// a non-leader is dropped, and the proposer's bounded wait reports failure.
    RaftPropose {
        group: GroupId,
        command: Vec<u8>,
        forwarded: bool,
    },
}
