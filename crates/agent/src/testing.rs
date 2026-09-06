//! Shared simulation invariants over harness events (agent spec §11).
//!
//! The continuous H-checkers: the event grammar that bounds effects to a live
//! activation (A6/A8), and run discipline over `(session, turn)` (A3/A7). Test
//! support behind the `testing` feature, not part of the harness API.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use actor_core::Event;
use actor_core::NodeId;
use actor_simulation::Invariant;
use actor_simulation::default_invariants;
use entity::EntityEvent;
use entity::EntityName;

use crate::AgentEvent;
use crate::SessionId;
use crate::TurnId;

/// The session an entity event names, by its key (§2.2: the `EntityName` key is the
/// `SessionId`). Tests use unique session keys, so the key identifies the run.
fn session_of(name: &EntityName) -> SessionId {
    SessionId::new(name.key())
}

/// **Effect containment and single per-node activation** (A6 per-node half, A8):
/// activation is the entity's (`Activated`/`Deactivated` strictly alternate per
/// session and node, entity §13); the harness's `SandboxBound`/`SandboxReleased`
/// alternate **within** that window and the sandbox is released before
/// deactivation; and a `ModelCompleted` only fires inside a live activation.
#[derive(Default)]
pub struct AgentEventGrammar {
    /// (session, node) → (active, sandbox_bound)
    windows: BTreeMap<(SessionId, NodeId), (bool, bool)>,
}

impl AgentEventGrammar {
    fn window(&mut self, session: SessionId, node: NodeId) -> &mut (bool, bool) {
        self.windows
            .entry((session, node))
            .or_insert((false, false))
    }
}

impl Invariant for AgentEventGrammar {
    fn name(&self) -> &'static str {
        "agent-event-grammar"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        // Activation lifecycle from the entity's own events (entity §13).
        if let Some(entity) = event.as_app::<EntityEvent>() {
            match entity {
                EntityEvent::Activated { node, name } => {
                    let w = self.window(session_of(name), *node);
                    if w.0 {
                        return Err(format!(
                            "second activation of {name} on {node} without deactivation (E6)"
                        ));
                    }
                    *w = (true, false);
                }
                EntityEvent::Deactivated { node, name } => {
                    let w = self.window(session_of(name), *node);
                    if w.1 {
                        return Err(format!(
                            "deactivation of {name} on {node} with the sandbox still bound (A8)"
                        ));
                    }
                    *w = (false, false);
                }
                _ => {}
            }
            return Ok(());
        }
        let Some(event) = event.as_app::<AgentEvent>() else {
            return Ok(());
        };
        match event {
            AgentEvent::SandboxBound { session, node } => {
                let w = self.window(session.clone(), *node);
                if !w.0 {
                    return Err(format!(
                        "sandbox bound for {session} on {node} outside an activation (A8)"
                    ));
                }
                if w.1 {
                    return Err(format!("second sandbox bound for {session} on {node} (A8)"));
                }
                w.1 = true;
            }
            AgentEvent::SandboxReleased { session, node } => {
                let w = self.window(session.clone(), *node);
                if !w.1 {
                    return Err(format!(
                        "sandbox released for {session} on {node} without a bind (A8)"
                    ));
                }
                w.1 = false;
            }
            AgentEvent::ModelCompleted { session, node, .. } => {
                let w = self.window(session.clone(), *node);
                if !w.0 {
                    return Err(format!(
                        "model completion for {session} on {node} outside an activation (A6)"
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// **Run discipline** (A3 pairing, A7): per `(session, turn)` exactly one
/// `RunStarted` and at most one `RunEnded`, never an end without a start. A
/// resume emits no second `RunStarted` (§10.4), and `ModelCompleted` is scoped to
/// journaled spend (emitted only after the response commits, §9.1 item 4), so no
/// completion follows a run's end.
#[derive(Default)]
pub struct RunDiscipline {
    started: BTreeSet<(SessionId, TurnId)>,
    ended: BTreeSet<(SessionId, TurnId)>,
}

impl Invariant for RunDiscipline {
    fn name(&self) -> &'static str {
        "agent-run-discipline"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        let Some(event) = event.as_app::<AgentEvent>() else {
            return Ok(());
        };
        match event {
            AgentEvent::RunStarted { session, turn, .. }
                if !self.started.insert((session.clone(), turn.clone())) =>
            {
                return Err(format!("second RunStarted for {session}/{turn} (A7)"));
            }
            AgentEvent::RunEnded { session, turn, .. } => {
                let key = (session.clone(), turn.clone());
                if !self.started.contains(&key) {
                    return Err(format!(
                        "RunEnded without RunStarted for {session}/{turn} (A3)"
                    ));
                }
                if !self.ended.insert(key) {
                    return Err(format!("second RunEnded for {session}/{turn} (A3)"));
                }
            }
            AgentEvent::ModelCompleted { session, turn, .. }
                if self.ended.contains(&(session.clone(), turn.clone())) =>
            {
                return Err(format!(
                    "model call for {session}/{turn} completed after the run ended (A4/A5)"
                ));
            }
            AgentEvent::ToolCompleted { session, turn, .. }
                if self.ended.contains(&(session.clone(), turn.clone())) =>
            {
                return Err(format!(
                    "tool call for {session}/{turn} completed after the run ended (§3.2)"
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

/// The default checker set for harness workloads: the core and entity invariants
/// plus the harness's continuous H-checkers (§11).
pub fn agent_invariants() -> Vec<Box<dyn Invariant>> {
    let mut invariants = default_invariants();
    invariants.push(Box::new(AgentEventGrammar::default()));
    invariants.push(Box::new(RunDiscipline::default()));
    invariants
}

/// Which H-invariant each harness checker enforces, as `(checker name, H
/// number)` — the pairing §11's "Verified by" column asserts, stated from the
/// checker's side.
///
/// Declared here, beside the checkers, because this is the claim an edit to an
/// `observe` body falsifies: stop watching `ModelCompleted` outside an
/// activation and the row saying [`AgentEventGrammar`] carries A6 is what
/// became false. `conformance_catalogue` holds these pairs equal to the
/// catalogue's, **per pair** and in both directions.
///
/// Two names cover six invariants here, which is why the pairing has to be
/// compared rather than the two sets of checker *names*: a name set cannot see
/// A6 dropping its `Verify::Checker` while A8 still names the same checker.
///
/// A bare `(name, number)` rather than `actor_simulation::CheckerCoverage`
/// because that type carries which of *its* two catalogues a number belongs to,
/// and every number here is an H.
pub fn checker_coverage() -> &'static [(&'static str, u8)] {
    &[
        ("agent-event-grammar", 6),
        ("agent-event-grammar", 8),
        ("agent-run-discipline", 3),
        ("agent-run-discipline", 4),
        ("agent-run-discipline", 5),
        ("agent-run-discipline", 7),
    ]
}
