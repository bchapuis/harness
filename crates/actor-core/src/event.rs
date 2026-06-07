//! The observability event stream (spec §16).
//!
//! A conforming system emits structured events for lifecycle transitions,
//! mailbox activity, and call outcomes. The same stream is what a deterministic
//! simulator subscribes to in order to check invariants and to assert
//! seed-reproducibility: two runs with the same seed must produce byte-identical
//! event streams (spec §18.1).
//!
//! The enum is `#[non_exhaustive]`; later slices add membership, supervision,
//! and `Terminated` variants without breaking matches.

use crate::id::ActorId;
use crate::id::NodeId;

/// A structured observability event (spec §16).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// An identity was reserved for a new actor (spec §4.2 step 1).
    AssignId { id: ActorId },
    /// An actor became resolvable and may now receive messages (step 3).
    ActorReady { id: ActorId },
    /// An actor's identity was released on stop or failure (step 5).
    ResignId { id: ActorId },
    /// A faulted actor was re-created by supervision (spec §11.2). Distinct from
    /// `ActorReady`, which fires once at first start, so the lifecycle invariant
    /// holds across restarts.
    Restarted { actor: ActorId },
    /// A message was enqueued onto an actor's mailbox (spec §6).
    Enqueue {
        actor: ActorId,
        manifest: &'static str,
    },
    /// An actor's executor began handling a message (spec §6). Paired with
    /// [`Event::DispatchEnd`]; between the two, the actor is busy and must not
    /// start another message (serial, non-reentrant execution, invariant #4).
    DispatchStart {
        actor: ActorId,
        manifest: &'static str,
    },
    /// An actor's executor finished handling a message (or it panicked).
    DispatchEnd {
        actor: ActorId,
        manifest: &'static str,
    },
    /// A request/response call was issued to an actor (spec §3.3). Paired with
    /// [`Event::AskOutcome`]; an issued ask that never reaches an outcome is a
    /// silently-lost call (invariant #1).
    AskIssued {
        actor: ActorId,
        manifest: &'static str,
    },
    /// A request/response call terminated, in success or a [`CallError`]
    /// (spec §14). `failed` distinguishes the two.
    ///
    /// [`CallError`]: crate::CallError
    AskOutcome {
        actor: ActorId,
        manifest: &'static str,
        failed: bool,
    },
    /// `observer` marked `node` suspect (spec §10). The observer is carried
    /// because reachability is per-node until gossip disseminates it.
    Suspected { observer: NodeId, node: NodeId },
    /// `observer` confirmed `node` unreachable (a suspicion unrefuted for
    /// `T_suspect`, spec §10).
    Unreachable { observer: NodeId, node: NodeId },
    /// `observer` saw `node` become reachable again (a probe succeeded, spec §10).
    Reachable { observer: NodeId, node: NodeId },
    /// `observer` declared `node` down — terminal for that observer (spec §9.1,
    /// §8.1).
    NodeDown { observer: NodeId, node: NodeId },
    /// `observer` first saw `node` as `joining` — handshake done, not yet a full
    /// member (spec §9.1, §9.3).
    MemberJoining { observer: NodeId, node: NodeId },
    /// `observer` saw `node` become a full `up` member — the leader admitted it
    /// on convergence (spec §9.1, §9.3).
    MemberUp { observer: NodeId, node: NodeId },
    /// A diagnostic marker, used by tests to punctuate an event stream.
    Mark(String),
}

/// A sink the runtime emits [`Event`]s to (spec §16).
///
/// Production may discard them; the simulator records them for invariant
/// checking. Implemented for `()` as a no-op so a system can run without
/// observability wired up.
pub trait EventSink: Send + Sync + 'static {
    fn emit(&self, event: Event);
}

impl EventSink for () {
    fn emit(&self, _event: Event) {}
}
