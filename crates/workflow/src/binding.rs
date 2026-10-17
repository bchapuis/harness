//! An activation-scoped external resource (workflow spec §2.6).
//!
//! A consumer frequently holds something for the life of an activation, such as
//! the agent runtime's sandbox (agent §2.3): opened once, released on every
//! deactivation, and vetoing hibernation while the open is in flight.
//!
//! The workflow supplies this as a **component rather than a trait method**,
//! because the resource's type and its open path are the consumer's. The type is
//! passive: it holds the state machine, and the engine never sees it. The rules
//! are the holder's to apply: conjoin [`Binding::is_opening`] into its
//! `can_hibernate`, settle the effects that needed a resource whose open failed
//! ([`Binding::take_failure`]) rather than retrying invisibly, and release it from
//! `on_deactivate` ([`Binding::take`]). A consumer that does not need one declares
//! nothing.

/// Where an activation-scoped resource stands (spec §2.6).
pub enum BindingState<T> {
    /// Nothing held, nothing in flight. The next effect that needs the resource
    /// triggers the open (lazily, so an entity that never needs one never pays).
    Closed,
    /// An open is in flight. The holder's `can_hibernate` must veto while a
    /// binding is here ([`Binding::is_opening`]): an open in flight is an outcome
    /// the activation still owes itself, and it is the one piece of in-flight
    /// work that is not an effect, so the engine's quiescence cannot see it.
    Opening,
    /// Held and usable.
    Open(T),
    /// The last open failed. The holder's next tick settles the effects that
    /// needed the resource with this reason rather than retrying invisibly, so the
    /// failure reaches the consumer's fold instead of spinning under it.
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

/// An activation-scoped external resource (spec §2.6): opened once on demand,
/// released by its holder on every deactivation, and never journaled.
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

    /// The open failed. The reason is surfaced, not swallowed: the holder's next
    /// tick settles the effects that needed the resource with it.
    pub fn failed(&mut self, why: impl Into<String>) {
        self.state = BindingState::Failed(why.into());
    }

    /// Whether an open is in flight, and therefore whether this binding vetoes
    /// hibernation (§6). A `Failed` binding does **not** veto: the failure is
    /// the activation's to surface through a settle, not to wait on.
    pub fn is_opening(&self) -> bool {
        matches!(self.state, BindingState::Opening)
    }

    /// Take the resource for release, leaving the binding closed whatever it
    /// held. Called from `on_deactivate` on **every** deactivation path (idle
    /// hibernation, migration, or a forced step-down), so nothing is owed at
    /// deactivation; and by a consumer giving up a claimed open it could not
    /// launch, so the claim does not outlive the attempt.
    pub fn take(&mut self) -> Option<T> {
        match std::mem::replace(&mut self.state, BindingState::Closed) {
            BindingState::Open(t) => Some(t),
            _ => None,
        }
    }

    /// Take a failed open's reason, returning the binding to `Closed` so the
    /// next demand retries; `None` in any other state. Taking is settling: the
    /// consumer fails the effects that needed the resource with the reason in
    /// the same round, or the failure spins invisibly.
    pub fn take_failure(&mut self) -> Option<String> {
        if !matches!(self.state, BindingState::Failed(_)) {
            return None;
        }
        match std::mem::replace(&mut self.state, BindingState::Closed) {
            BindingState::Failed(why) => Some(why),
            _ => None,
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
        assert!(
            !b.is_opening(),
            "a failure is settled through the fold, never waited on"
        );
        assert!(
            !b.begin_open(),
            "the failure must be settled before a retry"
        );
        assert_eq!(b.take_failure().as_deref(), Some("no sandbox"));
        assert_eq!(b.take_failure(), None, "taken once");
        assert!(b.begin_open(), "taken, the next demand retries");
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
