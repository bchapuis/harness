//! Seed-controlled transport faults (spec §18.3).
//!
//! A [`FaultPolicy`] is the *input* side of fault injection: the per-frame drop,
//! duplication, and latency a run applies to the in-memory network. The *output*
//! side — what a run actually exercised — is the coverage tally in
//! [`crate::coverage`].

use std::time::Duration;

/// Seed-controlled transport faults (spec §18.3). All zero by default — a
/// no-fault run is the simplest case and must still pass. Per-pair FIFO is
/// preserved even under latency (#3); loss surfaces as `Timeout`/`Unreachable`;
/// duplication is tolerated (the framework gives at-most-once *at the caller*,
/// not exactly-once delivery, §7.2).
#[derive(Clone, Copy)]
pub struct FaultPolicy {
    /// Probability `drop_num / drop_den` that a frame is lost.
    pub drop_num: u64,
    pub drop_den: u64,
    /// Probability `duplicate_num / duplicate_den` that a frame is delivered twice.
    pub duplicate_num: u64,
    pub duplicate_den: u64,
    /// Frames are delayed by a seeded amount in `0..=max_latency`.
    pub max_latency: Duration,
}

impl Default for FaultPolicy {
    fn default() -> Self {
        FaultPolicy {
            drop_num: 0,
            drop_den: 1,
            duplicate_num: 0,
            duplicate_den: 1,
            max_latency: Duration::ZERO,
        }
    }
}

impl FaultPolicy {
    /// Whether any fault dimension is enabled. The network takes a synchronous,
    /// in-order fast path when this is false, so a no-fault run pays nothing.
    pub(crate) fn active(&self) -> bool {
        self.drop_num > 0 || self.duplicate_num > 0 || !self.max_latency.is_zero()
    }
}
