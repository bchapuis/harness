//! Harness observability events (agent spec §10.4): checker-facing,
//! ephemeral, describing the *machinery around* sessions (run pairing, model
//! spend, the sandbox bind/release pair), so the A-invariants are checkable over
//! a run's stream. They carry nothing about a session's content: records are the
//! durable, user-facing account (§10.1). Activation, deactivation, and the
//! single-writer fence are the entity's events (`Activated`/`Deactivated`/
//! `LeaderChanged`, entity §13), not duplicated here.
//!
//! Each event rides the core stream as [`Event::App`] (core spec §16), so
//! checkers observe one totally ordered sequence interleaved with the core
//! events and the seed-reproducibility contract (core spec §18.1 #1) covers
//! these too. Recover them with `event.as_app::<AgentEvent>()`.

use actor_core::Event;
use actor_core::NodeId;

use crate::session::SessionId;
use crate::session::TurnId;

/// One agent event (agent spec §10.4): the events a stream checker cannot
/// reconstruct from the entity's own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentEvent {
    /// A run began for a newly journaled turn: fires once per `(session, turn)`
    /// (on the activation whose append committed the turn) under any
    /// duplication or retry (A7). A resume emits **no** second `RunStarted`: a
    /// checker recognizes a resume as the entity's `Activated` followed by a
    /// `ModelCompleted` on an already-started turn (§10.4). `parent` is the
    /// delegating session for a run started by delegation (§8.1).
    RunStarted {
        session: SessionId,
        turn: TurnId,
        parent: Option<SessionId>,
    },
    /// One model call finished and its response committed on a live activation.
    /// `usage` is the model-reported total token count feeding the A4 checker;
    /// a discarded straggler (run already ended) emits nothing, keeping the
    /// event scoped to journaled spend (§9.1 item 4, §10.4). `node` attributes the
    /// call to its enclosing activation.
    ModelCompleted {
        session: SessionId,
        turn: TurnId,
        node: NodeId,
        usage: u64,
    },
    /// One tool call's `ToolOutcome` committed on a live activation (§5.4,
    /// §6.4). Scoped to a live run: a straggler of an ended or cancelled run
    /// commits no record, so emits nothing (§3.2, §9.2). `node` attributes it to
    /// its enclosing activation.
    ToolCompleted {
        session: SessionId,
        turn: TurnId,
        node: NodeId,
    },
    /// The run's exactly-one terminal outcome was journaled (A3). `outcome`
    /// is the terminal kind: `"ok"`, `"budget"`, `"cancelled"`, or `"model"`
    /// (§3.1).
    RunEnded {
        session: SessionId,
        turn: TurnId,
        outcome: &'static str,
    },
    /// The activation opened its sandbox: first sandboxed call (§5.3).
    /// Alternates with [`SandboxReleased`](AgentEvent::SandboxReleased)
    /// within the entity's activation window (A8).
    SandboxBound { session: SessionId, node: NodeId },
    /// That sandbox was torn down: deactivation, loss, or step-down.
    SandboxReleased { session: SessionId, node: NodeId },
}

impl From<AgentEvent> for Event {
    fn from(event: AgentEvent) -> Event {
        Event::app(event)
    }
}
