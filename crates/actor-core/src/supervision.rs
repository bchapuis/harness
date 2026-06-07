//! Supervision (spec §11).
//!
//! Supervision governs what happens when a **local** actor faults — a handler
//! panics, or [`started`](crate::Actor::started) fails. An actor declares its
//! strategy by overriding [`Actor::supervision`](crate::Actor::supervision); the
//! executor catches the fault and applies the resulting
//! [`SupervisionDirective`] (spec §11.2). The default is [`Stop`].
//!
//! [`Stop`]: SupervisionDirective::Stop

use std::sync::Arc;
use std::time::Duration;

/// Why an actor faulted, so a decider can choose a directive per cause (spec
/// §11.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// `started` returned `Err`.
    Started,
    /// A message handler panicked.
    Message,
    /// A child actor escalated its failure to this (parent) actor (spec §11.1).
    Escalation,
}

/// Delay between restarts (spec §11.2). Exponential with a cap is RECOMMENDED to
/// avoid hot-restart loops; jitter is a follow-up.
#[derive(Clone, Copy, Debug)]
pub enum Backoff {
    /// Restart immediately.
    None,
    /// A constant delay.
    Fixed(Duration),
    /// `base · 2^(attempt-1)`, capped at `max`.
    Exponential { base: Duration, max: Duration },
}

impl Backoff {
    /// The delay before the `attempt`-th restart (1-based).
    pub fn delay(&self, attempt: u32) -> Duration {
        match self {
            Backoff::None => Duration::ZERO,
            Backoff::Fixed(d) => *d,
            Backoff::Exponential { base, max } => {
                let factor = 2u32.saturating_pow(attempt.saturating_sub(1));
                (*base * factor).min(*max)
            }
        }
    }
}

/// What to do when an actor faults (spec §11.2).
#[derive(Clone, Copy, Debug)]
pub enum SupervisionDirective {
    /// Terminate the actor; notify watchers (spec §12). The default.
    Stop,
    /// Keep state, drop the failed message, and carry on (use sparingly).
    Resume,
    /// Re-create the actor (fresh state) keeping its id and mailbox. Exceeding
    /// `max` restarts within `within` escalates to [`Stop`].
    ///
    /// [`Stop`]: SupervisionDirective::Stop
    Restart {
        max: u32,
        within: Duration,
        backoff: Backoff,
    },
    /// Fail the parent, applying the parent's strategy (spec §11.2). Parent
    /// hierarchy propagation is a follow-up; treated as [`Stop`] for now.
    ///
    /// [`Stop`]: SupervisionDirective::Stop
    Escalate,
}

/// A per-actor supervision strategy: a decider mapping a [`Fault`] to a
/// [`SupervisionDirective`] (spec §11.2).
#[derive(Clone)]
pub struct Supervision {
    decider: Arc<dyn Fn(Fault) -> SupervisionDirective + Send + Sync>,
}

impl Supervision {
    /// Always stop (the default, spec §11.2).
    pub fn stop() -> Supervision {
        Supervision::with(|_| SupervisionDirective::Stop)
    }

    /// Always resume.
    pub fn resume() -> Supervision {
        Supervision::with(|_| SupervisionDirective::Resume)
    }

    /// Always restart with the given limits and backoff.
    pub fn restart(max: u32, within: Duration, backoff: Backoff) -> Supervision {
        Supervision::with(move |_| SupervisionDirective::Restart {
            max,
            within,
            backoff,
        })
    }

    /// A custom decider, choosing a directive per fault.
    pub fn with<F>(decider: F) -> Supervision
    where
        F: Fn(Fault) -> SupervisionDirective + Send + Sync + 'static,
    {
        Supervision {
            decider: Arc::new(decider),
        }
    }

    /// Decide the directive for a fault.
    pub fn decide(&self, fault: Fault) -> SupervisionDirective {
        (self.decider)(fault)
    }
}

impl Default for Supervision {
    fn default() -> Self {
        Supervision::stop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff_doubles_and_caps() {
        let backoff = Backoff::Exponential {
            base: Duration::from_millis(100),
            max: Duration::from_secs(1),
        };
        assert_eq!(backoff.delay(1), Duration::from_millis(100));
        assert_eq!(backoff.delay(2), Duration::from_millis(200));
        assert_eq!(backoff.delay(3), Duration::from_millis(400));
        assert_eq!(backoff.delay(10), Duration::from_secs(1));
    }

    #[test]
    fn a_decider_can_branch_on_fault() {
        let supervision = Supervision::with(|fault| match fault {
            Fault::Started => SupervisionDirective::Stop,
            Fault::Message => SupervisionDirective::Resume,
            Fault::Escalation => SupervisionDirective::Escalate,
        });
        assert!(matches!(
            supervision.decide(Fault::Started),
            SupervisionDirective::Stop
        ));
        assert!(matches!(
            supervision.decide(Fault::Message),
            SupervisionDirective::Resume
        ));
    }
}
