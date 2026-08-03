//! The wire protocol: frames exchanged over an association (spec §7.1).
//!
//! This is the vocabulary every subsystem speaks across the network — actor
//! envelopes and replies, SWIM probes, death-watch, and receptionist
//! replication. It references domain types (e.g. [`MemberDigest`]); the
//! [`Transport`](crate::Transport) that carries it does not.

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
        /// Already codec-encoded by the sender, and encoded *again* as part of this
        /// frame — so bulk bytes cross the wire through two encoders, and both have
        /// to be told they are bytes. Without the attribute serde takes its default
        /// sequence path and the decoder grows a `Vec` one `u8` at a time: ~16 ms a
        /// mebibyte, which measured as the whole per-block cost of replicating a
        /// disk image (TODO.md). Neutral on the wire for both codecs in the tree —
        /// `actor-serialization/tests/wire_bytes.rs` is that claim, and covers this
        /// struct-variant shape specifically.
        #[serde(with = "serde_bytes")]
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
        /// The opaque application snapshot — bulk bytes, so tagged for the same
        /// reason as `Envelope::payload`.
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    /// Many groups' heartbeats in one frame, from one leader to one follower
    /// (spec §9.4.3).
    ///
    /// A heartbeat is an empty [`RaftAppend`](Frame::RaftAppend), and a node that
    /// leads `G` groups sends one to each of `R-1` followers every heartbeat
    /// interval. Sent individually that is `G × (R-1)` frames — each its own
    /// serialization, its own transport send — repeated several times a second, for
    /// traffic that carries no data at all. Since granary runs one Raft group per
    /// shard (grain §8.2) and shard count is the elasticity knob (grain §7.7), `G`
    /// grows with the cluster, and this becomes the ceiling on how far a node can be
    /// packed long before storage or CPU does.
    ///
    /// Coalescing costs nothing in semantics: each beat is dispatched to its own
    /// group exactly as a lone `RaftAppend` would be, so every group's state machine
    /// sees the identical sequence. Only the framing changes.
    RaftHeartbeats { beats: Vec<RaftBeat> },
    /// The replies to a [`RaftHeartbeats`](Frame::RaftHeartbeats), batched the same
    /// way and for the same reason: one reply per beat would undo half the saving.
    RaftHeartbeatReplies { replies: Vec<RaftBeatReply> },
    /// A leadership handoff: `leader` asks the recipient to stand for election in
    /// `group` **now**, without waiting out its election timeout (Raft §3.10).
    ///
    /// Sent only to a voter the leader has confirmed is caught up to its own last
    /// index, so the recipient can win immediately rather than being rejected for a
    /// stale log. It is safe by construction — it starts an ordinary election, which
    /// still requires a quorum of votes, so it can move leadership but never split
    /// it. Its purpose is to make a *planned* departure cheap: without it a draining
    /// node's groups each wait a full election timeout, which for a node leading many
    /// shards is a failover storm on every rolling restart.
    RaftTimeoutNow {
        group: GroupId,
        term: u64,
        leader: NodeId,
    },
    /// An application command offered to `group`'s leader (spec §9.4.3 item 1):
    /// a non-leader node sends it to a voter, which forwards it to its leader.
    /// The command is the opaque app payload (the engine's `EntryPayload::App`
    /// bytes — for the control group, an encoded `MembershipCommand`).
    /// `forwarded` stops a stale-leader loop — a forwarded proposal landing on
    /// a non-leader is dropped, and the proposer's bounded wait reports failure.
    RaftPropose {
        group: GroupId,
        /// Already-encoded app bytes, tagged for the same reason as
        /// `Envelope::payload`.
        #[serde(with = "serde_bytes")]
        command: Vec<u8>,
        forwarded: bool,
    },
}

/// One group's heartbeat inside a [`Frame::RaftHeartbeats`] — the fields of an
/// empty [`Frame::RaftAppend`], minus the `entries` that make it empty.
///
/// `leader` is carried per beat rather than once per frame because a node can lead
/// some groups and merely follow others; only the groups it leads produce beats, but
/// nothing in the wire format should assume the sender leads all of them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftBeat {
    pub group: GroupId,
    pub term: u64,
    pub leader: NodeId,
    pub prev_index: u64,
    pub prev_term: u64,
    pub commit: u64,
}

/// One group's reply inside a [`Frame::RaftHeartbeatReplies`] — the fields of a
/// [`Frame::RaftAppendReply`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftBeatReply {
    pub group: GroupId,
    pub term: u64,
    pub ok: bool,
    pub match_index: u64,
}
