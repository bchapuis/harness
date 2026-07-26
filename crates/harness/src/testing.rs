//! Shared simulation invariants over harness events (harness spec §11).
//!
//! These are the continuous H-checkers every harness swarm wants: the event
//! grammar that bounds effects to a live activation (H6/H8), and run discipline
//! over `(session, turn)` (H3/H7). They live here rather than in one test binary
//! because they are claims about *the harness's* contract, not about any one
//! suite — and because independently-maintained copies drift apart, so that one
//! of them quietly stops checking what its name says.
//!
//! Behind the `testing` feature: this is test support, not part of the harness
//! API, and it should not ship in a production build of the crate.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use actor_core::Event;
use actor_core::NodeId;
use actor_simulation::Invariant;
use actor_simulation::default_invariants;
use granary::GrainEvent;
use granary::GrainName;

use crate::HarnessEvent;
use crate::SessionId;
use crate::TurnId;

/// The session a grain event names, by its key (§2.2: the `GrainName` key is the
/// `SessionId`). Tests use unique session keys, so the key identifies the run.
fn session_of(name: &GrainName) -> SessionId {
    SessionId::new(name.key())
}

/// **Effect containment and single per-node activation** (H6 per-node half, H8):
/// activation is the grain's (`Activated`/`Passivated` strictly alternate per
/// session and node, granary §13); the harness's `SandboxBound`/`SandboxReleased`
/// alternate **within** that window and the sandbox is released before
/// deactivation; and a `ModelCompleted` only fires inside a live activation.
#[derive(Default)]
pub struct HarnessEventGrammar {
    /// (session, node) → (active, sandbox_bound)
    windows: BTreeMap<(SessionId, NodeId), (bool, bool)>,
}

impl HarnessEventGrammar {
    fn window(&mut self, session: SessionId, node: NodeId) -> &mut (bool, bool) {
        self.windows
            .entry((session, node))
            .or_insert((false, false))
    }
}

impl Invariant for HarnessEventGrammar {
    fn name(&self) -> &'static str {
        "harness-event-grammar"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        // Activation lifecycle from the grain's own events (granary §13).
        if let Some(grain) = event.as_app::<GrainEvent>() {
            match grain {
                GrainEvent::Activated { node, name } => {
                    let w = self.window(session_of(name), *node);
                    if w.0 {
                        return Err(format!(
                            "second activation of {name} on {node} without passivation (G6)"
                        ));
                    }
                    *w = (true, false);
                }
                GrainEvent::Passivated { node, name } => {
                    let w = self.window(session_of(name), *node);
                    if w.1 {
                        return Err(format!(
                            "passivation of {name} on {node} with the sandbox still bound (H8)"
                        ));
                    }
                    *w = (false, false);
                }
                _ => {}
            }
            return Ok(());
        }
        let Some(event) = event.as_app::<HarnessEvent>() else {
            return Ok(());
        };
        match event {
            HarnessEvent::SandboxBound { session, node } => {
                let w = self.window(session.clone(), *node);
                if !w.0 {
                    return Err(format!(
                        "sandbox bound for {session} on {node} outside an activation (H8)"
                    ));
                }
                if w.1 {
                    return Err(format!("second sandbox bound for {session} on {node} (H8)"));
                }
                w.1 = true;
            }
            HarnessEvent::SandboxReleased { session, node } => {
                let w = self.window(session.clone(), *node);
                if !w.1 {
                    return Err(format!(
                        "sandbox released for {session} on {node} without a bind (H8)"
                    ));
                }
                w.1 = false;
            }
            HarnessEvent::ModelCompleted { session, node, .. } => {
                let w = self.window(session.clone(), *node);
                if !w.0 {
                    return Err(format!(
                        "model completion for {session} on {node} outside an activation (H6)"
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// **Run discipline** (H3 pairing, H7): per `(session, turn)` exactly one
/// `RunStarted` and at most one `RunEnded`, never an end without a start. A
/// resume emits no second `RunStarted` (§10.4), and `ModelCompleted` is scoped to
/// journaled spend (emitted only after the response commits, §9.1.4), so no
/// completion follows a run's end.
#[derive(Default)]
pub struct RunDiscipline {
    started: BTreeSet<(SessionId, TurnId)>,
    ended: BTreeSet<(SessionId, TurnId)>,
}

impl Invariant for RunDiscipline {
    fn name(&self) -> &'static str {
        "harness-run-discipline"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        let Some(event) = event.as_app::<HarnessEvent>() else {
            return Ok(());
        };
        match event {
            HarnessEvent::RunStarted { session, turn, .. }
                if !self.started.insert((session.clone(), turn.clone())) =>
            {
                return Err(format!("second RunStarted for {session}/{turn} (H7)"));
            }
            HarnessEvent::RunEnded { session, turn, .. } => {
                let key = (session.clone(), turn.clone());
                if !self.started.contains(&key) {
                    return Err(format!(
                        "RunEnded without RunStarted for {session}/{turn} (H3)"
                    ));
                }
                if !self.ended.insert(key) {
                    return Err(format!("second RunEnded for {session}/{turn} (H3)"));
                }
            }
            HarnessEvent::ModelCompleted { session, turn, .. }
                if self.ended.contains(&(session.clone(), turn.clone())) =>
            {
                return Err(format!(
                    "model call for {session}/{turn} completed after the run ended (H4/H5)"
                ));
            }
            HarnessEvent::ToolCompleted { session, turn, .. }
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

/// The default checker set for harness workloads: the core and grain invariants
/// plus the harness's continuous H-checkers (§11).
pub fn harness_invariants() -> Vec<Box<dyn Invariant>> {
    let mut invariants = default_invariants();
    invariants.push(Box::new(HarnessEventGrammar::default()));
    invariants.push(Box::new(RunDiscipline::default()));
    invariants
}
