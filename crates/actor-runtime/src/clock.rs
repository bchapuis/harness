//! The production [`Clock`]: real wall-clock time backed by tokio (spec §4.6).
//!
//! The framework's [`Instant`] is a logical u64-nanos value, deliberately
//! independent of [`std::time::Instant`]. [`TokioClock`] bridges the two: it
//! captures a monotonic baseline at construction and reports `now()` as
//! nanoseconds elapsed since that baseline, so the instants it hands out are
//! monotonic and comparable just like the simulator's. Sleeping defers to
//! [`tokio::time`].

use std::sync::Arc;
use std::time::Duration;

use actor_core::Clock;
use actor_core::Instant;

/// A wall-clock [`Clock`] for the production runtime. Cheap to clone (shares an
/// `Arc` to the monotonic baseline), as the seam requires.
#[derive(Clone)]
pub struct TokioClock {
    epoch: Arc<std::time::Instant>,
}

impl TokioClock {
    /// Start a clock whose epoch is now. Two clocks constructed at different
    /// moments report different `now()` values, which is fine: instants are only
    /// ever compared within one clock.
    pub fn new() -> TokioClock {
        TokioClock {
            epoch: Arc::new(std::time::Instant::now()),
        }
    }
}

impl Default for TokioClock {
    fn default() -> TokioClock {
        TokioClock::new()
    }
}

impl Clock for TokioClock {
    fn now(&self) -> Instant {
        // Monotonic by construction; saturating cast covers the (~585-year)
        // overflow of u64 nanoseconds.
        let nanos = u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX);
        Instant::from_nanos(nanos)
    }

    fn sleep(&self, dur: Duration) -> impl Future<Output = ()> + Send {
        tokio::time::sleep(dur)
    }
}
