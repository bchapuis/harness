//! Harness observability events (harness spec §10.4): checker-facing,
//! ephemeral, describing the *machinery around* sessions — activation,
//! fencing, run pairing — so the H-invariants are checkable over a run's
//! stream. They carry nothing about a session's content: records are the
//! durable, user-facing account (§10.1).
//!
//! The vocabulary is the harness's; the stream is the framework's: each
//! event rides the core stream as [`Event::App`] (core spec §16), so
//! checkers observe one totally ordered sequence interleaved with the core
//! events, and the seed-reproducibility contract (core spec §18.1 #1) covers
//! these for free. Recover them with `event.as_app::<HarnessEvent>()`.

use actor_core::Event;
use actor_core::NodeId;

use crate::session::SessionId;
use crate::session::TurnId;

/// One harness event (harness spec §10.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HarnessEvent {
    /// The host activated the session: journal folded, actor live. Per
    /// session and node, strictly alternates with
    /// [`SessionDeactivated`](HarnessEvent::SessionDeactivated) (invariant
    /// H6, per-node half).
    SessionActivated { session: SessionId, node: NodeId },
    /// That activation stopped — ownership moved, fence rejection, idle
    /// stop, or fault.
    SessionDeactivated { session: SessionId, node: NodeId },
    /// A fenced append lost the race (§6.2): the activation must now
    /// deactivate with no further harness activity for the session (H2).
    AppendRejected { session: SessionId, node: NodeId },
    /// A run began for a newly journaled turn: fires once per
    /// `(session, turn)` — on the activation whose append committed the turn
    /// — under any duplication or retry (H7). `parent` is the delegating
    /// session for a run started by delegation (§8.1).
    RunStarted {
        session: SessionId,
        turn: TurnId,
        parent: Option<SessionId>,
    },
    /// An activation picked up a journaled, unfinished run (§7.5): a resume,
    /// never a second `RunStarted`.
    RunResumed {
        session: SessionId,
        turn: TurnId,
        node: NodeId,
    },
    /// One model call finished and its response was accepted by a live,
    /// unfenced activation. `usage` is the model-reported total token count
    /// feeding the H4 checker; a discarded straggler (run already ended) or
    /// a fenced activation's speculative call emits nothing, keeping the
    /// event scoped to journaled spend (§9.1.4, §10.4).
    ModelCompleted {
        session: SessionId,
        turn: TurnId,
        node: NodeId,
        usage: u64,
    },
    /// The run's exactly-one terminal outcome was journaled (H3). `outcome`
    /// is the terminal kind: `"ok"`, `"budget"`, `"cancelled"`, `"model"`,
    /// or `"journal"` (§3.1).
    RunEnded {
        session: SessionId,
        turn: TurnId,
        outcome: &'static str,
    },
    /// The activation opened its sandbox — first sandboxed call (§5.3).
    /// Alternates with [`SandboxReleased`](HarnessEvent::SandboxReleased)
    /// within the activation (H8).
    SandboxBound { session: SessionId, node: NodeId },
    /// That sandbox was torn down — deactivation, loss, or release.
    SandboxReleased { session: SessionId, node: NodeId },
}

impl From<HarnessEvent> for Event {
    fn from(event: HarnessEvent) -> Event {
        Event::app(event)
    }
}
