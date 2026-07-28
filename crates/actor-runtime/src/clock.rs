//! The production [`Clock`]: real wall-clock time backed by tokio (spec §4.6).
//!
//! The framework's [`Instant`] is a logical u64-nanos value, independent of
//! [`std::time::Instant`]. [`TokioClock`] captures a monotonic baseline at
//! construction and reports `now()` as nanoseconds elapsed since it. Sleeping
//! defers to [`tokio::time`].

use std::sync::Arc;
use std::time::Duration;

use actor_core::Clock;
use actor_core::Instant;

/// A wall-clock [`Clock`] for the production runtime. Cheap to clone (shares an
/// `Arc` to the monotonic baseline).
#[derive(Clone)]
pub struct TokioClock {
    epoch: Arc<std::time::Instant>,
}

impl TokioClock {
    /// Start a clock whose epoch is now. Instants are comparable only within
    /// one clock: two clocks constructed at different moments report different
    /// `now()` values.
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
