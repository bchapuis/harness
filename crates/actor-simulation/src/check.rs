//! The checking event sink (spec §18.5).
//!
//! A [`Checker`] wraps a set of [`Invariant`]s and acts as the system's
//! [`EventSink`]: every emitted event is fed to each invariant, and any
//! violation is recorded. Crucially it **never panics inside `emit`** — that
//! would unwind through the actor executor's panic guard and be mistaken for a
//! handler fault. Violations are collected and surfaced by the runner after the
//! run completes.

use std::sync::Arc;
use std::sync::Mutex;

use actor_core::Event;
use actor_core::EventSink;

use crate::invariant::Invariant;

/// A single invariant violation observed during a run.
#[derive(Clone, Debug)]
pub struct Violation {
    /// The invariant that was violated.
    pub invariant: &'static str,
    /// A human-readable description of how it was violated.
    pub detail: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.invariant, self.detail)
    }
}

struct Inner {
    invariants: Vec<Box<dyn Invariant>>,
    violations: Vec<Violation>,
}

/// Feeds the event stream to a set of invariants and collects their violations.
/// Clone to obtain another handle to the same checker (it is its own
/// [`EventSink`]).
#[derive(Clone)]
pub struct Checker {
    inner: Arc<Mutex<Inner>>,
}

impl Checker {
    /// Build a checker over the given invariants.
    pub fn new(invariants: Vec<Box<dyn Invariant>>) -> Checker {
        Checker {
            inner: Arc::new(Mutex::new(Inner {
                invariants,
                violations: Vec::new(),
            })),
        }
    }

    /// An [`EventSink`] handle to hand to
    /// [`LocalSystemBuilder::events`](actor_core::LocalSystemBuilder::events).
    pub fn sink(&self) -> Arc<dyn EventSink> {
        Arc::new(self.clone())
    }

    /// Run the at-quiescence checks and return every violation observed during
    /// the run (empty ⇒ the run upheld all invariants).
    pub fn finish(&self) -> Vec<Violation> {
        let mut inner = self.inner.lock().expect("checker mutex poisoned");
        let Inner {
            invariants,
            violations,
        } = &mut *inner;
        for inv in invariants.iter_mut() {
            if let Err(detail) = inv.at_quiescence() {
                violations.push(Violation {
                    invariant: inv.name(),
                    detail,
                });
            }
        }
        violations.clone()
    }
}

impl EventSink for Checker {
    fn emit(&self, event: Event) {
        let mut inner = self.inner.lock().expect("checker mutex poisoned");
        let Inner {
            invariants,
            violations,
        } = &mut *inner;
        for inv in invariants.iter_mut() {
            if let Err(detail) = inv.observe(&event) {
                violations.push(Violation {
                    invariant: inv.name(),
                    detail,
                });
            }
        }
    }
}
