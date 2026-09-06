//! An activation-scoped external resource (drive spec §6).
//!
//! A consumer frequently holds something for the life of an activation — the
//! harness's sandbox (harness §5.3), the machine's runtime binding (machine
//! §2.1) — with an open-once lifecycle, a release on every deactivation, and a
//! hibernation veto while the open is in flight.
//!
//! The drive supplies this as a **component rather than a trait method**,
//! because the resource's type and its open path are the consumer's: the drive
//! owns only the state machine and the rule that a failed open settles the
//! effects that needed it rather than retrying invisibly. A consumer that does
//! not need one declares nothing.

/// Where an activation-scoped resource stands (spec §6).
pub enum BindingState<T> {
    /// Nothing held, nothing in flight. The next effect that needs the resource
    /// triggers the open (lazily, so an entity that never needs one never pays).
    Closed,
    /// An open is in flight. **Joins the hibernation veto**: an open in flight
    /// is an outcome the activation still owes itself, and it is the one piece
    /// of in-flight work that is not an effect.
    Opening,
    /// Held and usable.
    Open(T),
    /// The last open failed. The next drive settles the effects that needed the
    /// resource with this reason rather than retrying invisibly, so the failure
    /// reaches the consumer's fold instead of spinning under it.
    Failed(String),
}

impl<T> std::fmt::Debug for BindingState<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BindingState::Closed => write!(f, "Closed"),
            BindingState::Opening => write!(f, "Opening"),
            BindingState::Open(_) => write!(f, "Open"),
            BindingState::Failed(why) => write!(f, "Failed({why})"),
        }
    }
}

/// An activation-scoped external resource (spec §6): opened once on demand,
/// released on every deactivation, and never journaled.
pub struct Binding<T> {
    state: BindingState<T>,
}

impl<T> Default for Binding<T> {
    fn default() -> Self {
        Binding {
            state: BindingState::Closed,
        }
    }
}

impl<T> Binding<T> {
    pub fn state(&self) -> &BindingState<T> {
        &self.state
    }

    /// The held resource, or `None` in any other state.
    pub fn get(&self) -> Option<&T> {
        match &self.state {
            BindingState::Open(t) => Some(t),
            _ => None,
        }
    }

    /// Claim the right to open, moving `Closed` → `Opening`. Returns `true` the
    /// first time, so the caller launches exactly one open task; `false` if the
    /// resource is already opening, open, or failed.
    pub fn begin_open(&mut self) -> bool {
        if matches!(self.state, BindingState::Closed) {
            self.state = BindingState::Opening;
            true
        } else {
            false
        }
    }

    /// The open succeeded.
    pub fn opened(&mut self, resource: T) {
        self.state = BindingState::Open(resource);
    }

    /// The open failed. The reason is surfaced, not swallowed: the next drive
    /// settles the effects that needed the resource with it.
    pub fn failed(&mut self, why: impl Into<String>) {
        self.state = BindingState::Failed(why.into());
    }

    /// The failure this binding is holding, if any — read by the drive when it
    /// settles the effects that needed the resource.
    pub fn failure(&self) -> Option<&str> {
        match &self.state {
            BindingState::Failed(why) => Some(why),
            _ => None,
        }
    }

    /// Whether an open is in flight, and therefore whether this binding vetoes
    /// hibernation (§6). A `Failed` binding does **not** veto: the failure is
    /// the activation's to surface through a settle, not to wait on.
    pub fn is_opening(&self) -> bool {
        matches!(self.state, BindingState::Opening)
    }

    /// Take the resource for release, leaving the binding closed. Called from
    /// `on_deactivate` on **every** deactivation path — idle hibernation,
    /// migration, or a forced step-down — so nothing is owed at deactivation.
    pub fn take(&mut self) -> Option<T> {
        match std::mem::replace(&mut self.state, BindingState::Closed) {
            BindingState::Open(t) => Some(t),
            _ => None,
        }
    }

    /// Return to `Closed` from a failed open, so the next demand retries. The
    /// consumer calls this once it has settled the effects that failure
    /// concerned — never before, or the failure spins invisibly.
    pub fn clear_failure(&mut self) {
        if matches!(self.state, BindingState::Failed(_)) {
            self.state = BindingState::Closed;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_first_demand_claims_the_open() {
        let mut b: Binding<u8> = Binding::default();
        assert!(b.begin_open(), "the first claim wins");
        assert!(
            !b.begin_open(),
            "a second demand does not launch a second open"
        );
        assert!(b.is_opening());
        b.opened(7);
        assert!(!b.begin_open(), "an open binding is not re-opened");
        assert_eq!(b.get(), Some(&7));
        assert!(!b.is_opening(), "an open binding does not veto hibernation");
    }

    #[test]
    fn a_failed_open_surfaces_and_does_not_veto() {
        let mut b: Binding<u8> = Binding::default();
        b.begin_open();
        b.failed("no sandbox");
        assert_eq!(b.failure(), Some("no sandbox"));
        assert!(
            !b.is_opening(),
            "a failure is settled through the fold, never waited on"
        );
        assert!(
            !b.begin_open(),
            "the failure must be settled before a retry"
        );
        b.clear_failure();
        assert!(b.begin_open(), "cleared, the next demand retries");
    }

    #[test]
    fn take_releases_and_closes_on_every_path() {
        let mut b: Binding<u8> = Binding::default();
        b.begin_open();
        b.opened(7);
        assert_eq!(b.take(), Some(7));
        assert!(matches!(b.state(), BindingState::Closed));
        assert_eq!(b.take(), None, "release is idempotent");
        // A forced step-down mid-open owes nothing either.
        b.begin_open();
        assert_eq!(b.take(), None);
        assert!(matches!(b.state(), BindingState::Closed));
    }
}
