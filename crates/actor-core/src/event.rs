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

use crate::actor::TerminationReason;
use crate::id::ActorId;
use crate::id::NodeId;
use crate::supervision::Fault;

/// The decision supervision applied to a faulted actor (spec §11.2), as seen on
/// the event stream (spec §16). This is the *effective* decision after the
/// restart window and backoff are applied — e.g. exceeding `max` restarts
/// surfaces as [`Stop`](SupervisionDecision::Stop), not `Restart`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SupervisionDecision {
    /// Keep the actor, drop the failed message.
    Resume,
    /// Re-create the actor, keeping its id and mailbox.
    Restart,
    /// Terminate the actor with `Failed`.
    Stop,
    /// Terminate the actor and fail its parent (spec §11.1).
    Escalate,
}

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
    /// `observer` saw `node` become a full `up` member — admitted by the mode's
    /// authority (spec §9.1, §9.3, §9.4).
    MemberUp { observer: NodeId, node: NodeId },
    /// `observer` saw `node` enter the reversible `draining` state — the mode's
    /// authority (a registry entry or a committed log entry, spec §9.4) cordoned
    /// it for maintenance. Unlike `down`, this is not terminal: the node stays a
    /// member and a later `resume` returns it to `up`.
    MemberDraining { observer: NodeId, node: NodeId },
    /// `observer` saw a `draining` `node` return to `up` — the operator resumed it
    /// after maintenance (spec §9.4). The reverse of [`Event::MemberDraining`].
    MemberResumed { observer: NodeId, node: NodeId },
    /// `observer` applied the external registry's state at `revision`
    /// (registry-based mode, spec §9.4.2). Emitted when a sync first lands a new
    /// revision, so tests can await convergence on a registry mutation.
    RegistrySynced { observer: NodeId, revision: u64 },
    /// `node` won the leader election for `term` (leader-based mode, spec
    /// §9.4.3). At most one node may ever announce a given term — the
    /// election-safety half of invariant #22 a continuous checker enforces.
    LeaderElected { node: NodeId, term: u64 },
    /// Supervision chose a directive for a faulted actor (spec §11.2, §16).
    Supervised {
        actor: ActorId,
        fault: Fault,
        decision: SupervisionDecision,
    },
    /// A singleton manager activated `actor` as its node's instance of the
    /// singleton `name` (utilities spec §4). At most one activation per name may
    /// be live on a node at a time — the per-node half of invariant U2 a
    /// continuous checker enforces; cross-node overlap during view divergence is
    /// legal until convergence.
    SingletonStarted { name: &'static str, actor: ActorId },
    /// A singleton manager observed its activation `actor` of `name` terminated
    /// (utilities spec §4) — by handoff, supervision, or its own stop. Pairs
    /// with the [`Event::SingletonStarted`] that activated it.
    SingletonStopped { name: &'static str, actor: ActorId },
    /// A `Terminated` signal was delivered to a watcher (spec §12, §16): emitted
    /// when the signal is enqueued onto the watcher's mailbox, on the watcher's
    /// own node. Forwarding a signal to a remote watcher's node is not a delivery
    /// (it is emitted there when the frame lands), so this fires once per actual
    /// delivery — including a watch-after-death (invariant #12).
    TerminatedDelivered {
        target: ActorId,
        watcher: ActorId,
        reason: TerminationReason,
    },
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
