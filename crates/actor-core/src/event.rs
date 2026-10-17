//! The observability event stream (spec §2.7).
//!
//! A deterministic simulator subscribes to this stream to check invariants and
//! to assert seed-reproducibility: two runs with the same seed must produce
//! byte-identical event streams (spec §2.12.2).

use crate::actor::TerminationReason;
use crate::error::CallError;
use crate::id::ActorId;
use crate::id::NodeId;
use crate::message::Manifest;
use crate::supervision::Fault;

/// The decision supervision applied to a faulted actor (spec §2.4), as seen on
/// the event stream (spec §2.7). This is the *effective* decision after the
/// restart window and backoff are applied, e.g. exceeding `max` restarts
/// surfaces as [`Stop`](SupervisionDecision::Stop), not `Restart`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SupervisionDecision {
    /// Keep the actor, drop the failed message.
    Resume,
    /// Re-create the actor, keeping its id and mailbox.
    Restart,
    /// Terminate the actor with `Failed`.
    Stop,
    /// Terminate the actor and fail its parent (spec §2.4).
    Escalate,
}

/// A structured observability event (spec §2.7).
///
/// **Where a new event belongs.** A named variant is for an invariant the
/// framework catalogue checks by variant: lifecycle, ask pairing, membership,
/// Raft, and the singleton activation discipline (which is why the singleton
/// pair sits here, though `actor-cluster`'s singleton manager emits it).
/// Everything a *layered* spec defines, such as entity's entity events or the
/// agent runtime's stream (agent §2.8), rides [`Event::App`] instead, so core is
/// never edited for a layer above it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// An identity was reserved for a new actor (spec §3.1 step 1).
    AssignId { id: ActorId },
    /// An actor became resolvable and may now receive messages (step 3).
    ActorReady { id: ActorId },
    /// An actor's identity was released on stop or failure (step 5).
    ResignId { id: ActorId },
    /// A faulted actor was re-created by supervision (spec §2.4). Distinct from
    /// `ActorReady`, which fires once at first start, so the lifecycle invariant
    /// holds across restarts.
    Restarted { actor: ActorId },
    /// A message was enqueued onto an actor's mailbox (spec §3.3).
    Enqueue { actor: ActorId, manifest: Manifest },
    /// An inbound remote message was refused at delivery to `actor` with
    /// `error` (spec §3.3 rule 4, §5): no live actor, no registration for the
    /// manifest, an undecodable payload, a full mailbox, or a sender the
    /// authorizer refuses (spec §9). An `ask` also sees
    /// the error as its reply; a one-way `tell` has no reply, so this event is
    /// the only trace that its message was not accepted. `manifest` is the
    /// claim the frame carried: untrusted wire text, so not a [`Manifest`].
    Undelivered {
        actor: ActorId,
        manifest: String,
        error: CallError,
    },
    /// An actor's executor began handling a message (spec §3.3). Paired with
    /// [`Event::DispatchEnd`]; between the two, the actor is busy and must not
    /// start another message (serial, non-reentrant execution, invariant #4).
    DispatchStart { actor: ActorId, manifest: Manifest },
    /// An actor's executor finished handling a message (or it panicked).
    DispatchEnd { actor: ActorId, manifest: Manifest },
    /// A request/response call was issued to an actor (spec §2.2). Paired with
    /// [`Event::AskOutcome`]; an issued ask that never reaches an outcome is a
    /// silently-lost call (invariant #1).
    ///
    /// `actor` is the **target**; `caller` is the node that issued the call. The
    /// pairing is the caller's property: it is the caller's death that makes an
    /// unanswered ask expected rather than lost.
    AskIssued {
        actor: ActorId,
        caller: NodeId,
        manifest: Manifest,
    },
    /// A request/response call terminated, in success or a [`CallError`]
    /// (spec §8). `failed` distinguishes the two.
    ///
    /// [`CallError`]: crate::CallError
    AskOutcome {
        actor: ActorId,
        caller: NodeId,
        manifest: Manifest,
        failed: bool,
    },
    /// `observer` marked `node` suspect (spec §7). Reachability is per-observer
    /// until gossip disseminates it.
    Suspected { observer: NodeId, node: NodeId },
    /// `observer` confirmed `node` unreachable (a suspicion unrefuted for
    /// `T_suspect`, spec §7).
    Unreachable { observer: NodeId, node: NodeId },
    /// `observer` saw `node` become reachable again (a probe succeeded, spec §7).
    Reachable { observer: NodeId, node: NodeId },
    /// `observer` declared `node` down: terminal for that observer (spec §6.1,
    /// §5.1).
    NodeDown { observer: NodeId, node: NodeId },
    /// `observer` first saw `node` as `joining`: handshake done, not yet a full
    /// member (spec §6.1, §6.3).
    MemberJoining { observer: NodeId, node: NodeId },
    /// `observer` saw `node` become a full `up` member: admitted by the mode's
    /// authority (spec §6.1, §6.3, §6.4).
    MemberUp { observer: NodeId, node: NodeId },
    /// `observer` saw `node` enter the reversible `draining` state: the mode's
    /// authority (a registry entry or a committed log entry, spec §6.4) cordoned
    /// it for maintenance. Unlike `down`, this is not terminal: the node stays a
    /// member and a later `resume` returns it to `up`.
    MemberDraining { observer: NodeId, node: NodeId },
    /// `observer` saw a `draining` `node` return to `up` (spec §6.4).
    MemberResumed { observer: NodeId, node: NodeId },
    /// `observer` applied the external registry's state at `revision`
    /// (registry-based mode, spec §6.4.2). Emitted when a sync *first* lands a
    /// given revision.
    RegistrySynced { observer: NodeId, revision: u64 },
    /// `node` won the leader election for `term` in Raft `group` (leader-based
    /// mode, spec §6.4.3). At most one node may ever announce a given
    /// `(group, term)`: the election-safety half of invariant #22 a continuous
    /// checker enforces. Terms are **per group**, so the group is part of the
    /// identity; it is the `GroupId` value as a plain `u64`. The membership
    /// control plane is group `0`.
    LeaderElected { node: NodeId, term: u64, group: u64 },
    /// Supervision chose a directive for a faulted actor (spec §2.4, §2.7).
    Supervised {
        actor: ActorId,
        fault: Fault,
        decision: SupervisionDecision,
    },
    /// A singleton manager activated `actor` as its node's instance of the
    /// singleton `name` (utilities spec §2.3). At most one activation per name may
    /// be live on a node at a time: the per-node half of invariant U2 a
    /// continuous checker enforces; cross-node overlap during view divergence is
    /// legal until convergence.
    SingletonStarted { name: &'static str, actor: ActorId },
    /// A singleton manager observed its activation `actor` of `name` terminated
    /// (utilities spec §2.3). Pairs with the [`Event::SingletonStarted`] that
    /// activated it.
    SingletonStopped { name: &'static str, actor: ActorId },
    /// A `Terminated` signal was delivered to a watcher (spec §2.5, §2.7): emitted
    /// when the signal is enqueued onto the watcher's mailbox, on the watcher's
    /// own node. Forwarding a signal to a remote watcher's node is not a delivery
    /// (it is emitted there when the frame lands), so this fires once per actual
    /// delivery, including a watch-after-death (invariant #12).
    TerminatedDelivered {
        target: ActorId,
        watcher: ActorId,
        reason: TerminationReason,
    },
    /// An application-layer event riding the framework's stream (spec §2.7).
    ///
    /// A runtime layered *on* the framework (the agent runtime, agent spec
    /// §2.8) defines its own typed event enum and emits it here, so its
    /// checkers observe one totally ordered stream interleaved with the core
    /// events, under the same seed-reproducibility contract (spec §2.12.2 #1).
    /// Read it back with [`Event::as_app`].
    App(Box<dyn AppEvent>),
}

impl Event {
    /// Whether this is a membership or reachability transition (spec §6, §7):
    /// the block an operator-facing stderr sink prints while swallowing
    /// dispatch-level noise. Named here, beside the enum, so a new membership
    /// variant extends every such sink in one place.
    pub fn is_membership(&self) -> bool {
        matches!(
            self,
            Event::Suspected { .. }
                | Event::Unreachable { .. }
                | Event::Reachable { .. }
                | Event::NodeDown { .. }
                | Event::MemberJoining { .. }
                | Event::MemberUp { .. }
                | Event::MemberDraining { .. }
                | Event::MemberResumed { .. }
        )
    }

    /// Wrap an application event for emission onto the stream.
    pub fn app<E: AppEvent>(event: E) -> Event {
        Event::App(Box::new(event))
    }

    /// The application event inside an [`Event::App`], if it is one and is of
    /// type `E`.
    pub fn as_app<E: AppEvent>(&self) -> Option<&E> {
        match self {
            // Dispatch through the inner `dyn AppEvent` explicitly: the blanket
            // impl also covers the box itself, so `event.as_any()` would return
            // the box, never the event.
            Event::App(event) => (**event).as_any().downcast_ref::<E>(),
            _ => None,
        }
    }
}

/// An application-defined event (spec §2.7): anything `Debug + Clone +
/// PartialEq + Send + Sync` qualifies via the blanket impl; applications never
/// implement this by hand. Those bounds are what let [`Event`] stay `Clone` and
/// `Eq` for the reproducibility recorder's comparison (spec §2.12.2 #1).
pub trait AppEvent: std::fmt::Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn std::any::Any;
    fn clone_app(&self) -> Box<dyn AppEvent>;
    fn eq_app(&self, other: &dyn AppEvent) -> bool;
}

impl<E> AppEvent for E
where
    E: std::fmt::Debug + Clone + PartialEq + Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn clone_app(&self) -> Box<dyn AppEvent> {
        Box::new(self.clone())
    }

    fn eq_app(&self, other: &dyn AppEvent) -> bool {
        other
            .as_any()
            .downcast_ref::<E>()
            .is_some_and(|other| self == other)
    }
}

// Both impls dispatch through the inner `dyn AppEvent` explicitly. Calling
// `self.clone_app()` / `self.eq_app(..)` on the box receiver would resolve
// to the blanket impl on `Box<dyn AppEvent>` itself (these very impls make
// the box satisfy its bounds) turning clone into infinite recursion and eq
// into a constant `false`.
impl Clone for Box<dyn AppEvent> {
    fn clone(&self) -> Self {
        (**self).clone_app()
    }
}

impl PartialEq for Box<dyn AppEvent> {
    fn eq(&self, other: &Self) -> bool {
        (**self).eq_app(&**other)
    }
}

impl Eq for Box<dyn AppEvent> {}

/// A sink the runtime emits [`Event`]s to (spec §2.7). Implemented for `()` as a
/// no-op, so a system can run with no observability wired up.
pub trait EventSink: Send + Sync + 'static {
    fn emit(&self, event: Event);
}

impl EventSink for () {
    fn emit(&self, _event: Event) {}
}
