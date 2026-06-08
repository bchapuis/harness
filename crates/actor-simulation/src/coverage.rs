//! Fault-injection coverage telemetry (spec §18.3).
//!
//! A sweep that *configures* faults but, by seed luck, never *triggers* one
//! gives false confidence. [`FaultStats`] is the *output* side of fault
//! injection: a tally of what a run actually exercised, so a swarm can assert
//! each fault type fired at least once. The *input* side is [`crate::faults`].

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// A tally of the faults a run actually exercised (spec §18.3). A swarm asserts,
/// across its seed range, that each fault type fired at least once — so a green
/// sweep provably covered loss, duplication, reordering, and partition/crash,
/// not just the happy path.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FaultStats {
    /// Frames dropped by a seeded loss roll (excludes partition/crash blocking).
    pub dropped: u64,
    /// Frames delivered twice by a seeded duplication roll.
    pub duplicated: u64,
    /// Frames delayed by a non-zero seeded latency (i.e. reordered in time).
    pub delayed: u64,
    /// Frames dropped because their directed pair was partitioned or crashed.
    pub blocked: u64,
}

impl FaultStats {
    /// Total number of fault events of any kind. Zero means the run exercised
    /// only the happy path.
    pub fn total(&self) -> u64 {
        self.dropped + self.duplicated + self.delayed + self.blocked
    }
}

impl std::ops::Add for FaultStats {
    type Output = FaultStats;

    fn add(self, rhs: FaultStats) -> FaultStats {
        FaultStats {
            dropped: self.dropped + rhs.dropped,
            duplicated: self.duplicated + rhs.duplicated,
            delayed: self.delayed + rhs.delayed,
            blocked: self.blocked + rhs.blocked,
        }
    }
}

/// The live counters the network increments as it injects faults. Kept behind
/// `record_*`/`snapshot` so the atomic representation stays this module's secret;
/// the network records events, it does not reach into the counters.
#[derive(Default)]
pub(crate) struct FaultCounters {
    dropped: AtomicU64,
    duplicated: AtomicU64,
    delayed: AtomicU64,
    blocked: AtomicU64,
}

impl FaultCounters {
    pub(crate) fn record_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_duplicated(&self) {
        self.duplicated.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_delayed(&self) {
        self.delayed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_blocked(&self) {
        self.blocked.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> FaultStats {
        FaultStats {
            dropped: self.dropped.load(Ordering::Relaxed),
            duplicated: self.duplicated.load(Ordering::Relaxed),
            delayed: self.delayed.load(Ordering::Relaxed),
            blocked: self.blocked.load(Ordering::Relaxed),
        }
    }
}
