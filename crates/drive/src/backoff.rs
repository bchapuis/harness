//! Per-key backoff (drive spec §3.4), and the cap that puts a key **at rest**
//! (§6).
//!
//! An effect that settles unproductively — `Err`, or a settle that returns no
//! records — and stays in the desired set would otherwise be relaunched by the
//! very next drive, at the rate the drive cycles. So the drive holds a per-key
//! count of consecutive unproductive settles and delays that key's next launch
//! by a capped exponential backoff, timed by `Clock` and jittered from
//! `Entropy`, so the delay is virtual and seed-reproducible under simulation.
//!
//! The count is activation state, like the claim: a re-activation starts at
//! zero. The drive imposes **no attempt bound** — bounding the work is the
//! consumer's, because only the consumer knows what the work is worth. What the
//! drive bounds is the *delay*, and reaching the cap has a second meaning: a key
//! backed off to the cap is at rest, no longer holding the activation resident.
//! Without that, "no attempt bound" and the quiescence rule would together pin
//! an activation forever on one permanently failing effect.

use std::collections::BTreeMap;
use std::time::Duration;

use actor_core::Instant;

/// The schedule a drive backs off on (spec §3.4).
///
/// The defaults are chosen so a transient failure retries promptly and a
/// permanent one stops holding the activation within one idle window (entity
/// §10 puts `idle_after` in minutes): four doublings from 500 ms reach the 8 s
/// cap, at which point the key is at rest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackoffPolicy {
    /// The first delay, doubled per consecutive unproductive settle.
    pub base: Duration,
    /// The ceiling. A key delayed by the cap is **at rest** (§6): the drive
    /// stops treating it as work that must keep this activation resident.
    pub cap: Duration,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        BackoffPolicy {
            base: Duration::from_millis(500),
            cap: Duration::from_secs(8),
        }
    }
}

impl BackoffPolicy {
    /// The delay after `failures` consecutive unproductive settles, before
    /// jitter: the core's capped exponential schedule (core §11.2), so the drive
    /// and the supervisor back off the same way. Saturates at
    /// [`cap`](BackoffPolicy::cap) rather than overflowing: the drive has no
    /// attempt bound, so `failures` is unbounded by design.
    fn delay(&self, failures: u32) -> Duration {
        if failures == 0 {
            return Duration::ZERO;
        }
        actor_core::Backoff::Exponential {
            base: self.base,
            max: self.cap,
        }
        .delay(failures)
    }
}

/// One key's backoff state.
#[derive(Clone, Copy, Debug)]
struct Entry {
    failures: u32,
    /// When the key may next be launched. Absent means the key is **at rest**
    /// (§6): its delay reached the cap and no timer was armed.
    until: Option<Instant>,
}

/// The per-key backoff table: ephemeral activation state, rebuilt from nothing
/// on every activation (spec §3.4).
///
/// `K` is the consumer's [`Driven::Key`](crate::Driven::Key).
pub struct Backoff<K: Ord> {
    policy: BackoffPolicy,
    entries: BTreeMap<K, Entry>,
}

impl<K: Ord> Default for Backoff<K> {
    fn default() -> Self {
        Backoff::new(BackoffPolicy::default())
    }
}

impl<K: Ord> Backoff<K> {
    pub fn new(policy: BackoffPolicy) -> Self {
        Backoff {
            policy,
            entries: BTreeMap::new(),
        }
    }

    /// Record one unproductive settle of `key` and return how long the drive
    /// must wait before relaunching it — or `None` if the key has reached the
    /// cap and is now **at rest**.
    ///
    /// `now` and `jitter` come from the system's `Clock` and `Entropy`, so the
    /// resulting instant is virtual and one seed reproduces it. The jitter is
    /// drawn from `[0, delay/2)` and *added*, so a burst of failures across many
    /// keys spreads rather than resonating.
    ///
    /// **At the cap the drive arms no timer**, and that is the whole of the
    /// at-rest rule (§6). Arming one anyway would be worse than useless: the
    /// activation would become quiescent, hibernate, and then be woken by its own
    /// stale timer a moment later, so the entity would churn instead of resting
    /// and the clause would buy nothing. Giving up the timer is giving up
    /// *unattended* progress, which is exactly what §6 trades away — the debt is
    /// a fold, so a caller's next contact ([`wake`](Backoff::wake)) or the next
    /// activation re-derives and re-drives it.
    pub fn failed(&mut self, key: K, now: Instant, jitter: u64) -> Option<Duration> {
        let failures = self
            .entries
            .get(&key)
            .map(|e| e.failures.saturating_add(1))
            .unwrap_or(1);
        let base = self.policy.delay(failures);
        if base >= self.policy.cap {
            self.entries.insert(
                key,
                Entry {
                    failures,
                    until: None,
                },
            );
            return None;
        }
        let spread = base / 2;
        let extra = if spread.is_zero() {
            Duration::ZERO
        } else {
            Duration::from_nanos(jitter % spread.as_nanos() as u64)
        };
        let delay = base + extra;
        self.entries.insert(
            key,
            Entry {
                failures,
                until: Some(now + delay),
            },
        );
        Some(delay)
    }

    /// Clear every at-rest key, so a caller-driven drive retries it.
    ///
    /// This is the difference between the drive's own timer re-entering the cycle
    /// and *someone asking*. The harness draws the same line today: it abandons a
    /// propagation launch after bounded retries, and "the next message that drives
    /// the session — on this same activation or a later one — re-launches the
    /// send" (harness §9.2 item 3). A key at rest is waiting for exactly that
    /// message, so the count starts over when it arrives.
    pub fn wake(&mut self) {
        self.entries.retain(|_, e| e.until.is_some());
    }

    /// Clear `key`'s count: a settle whose records committed is productive, and
    /// the next failure starts the schedule again.
    pub fn cleared(&mut self, key: &K) {
        self.entries.remove(key);
    }

    /// Drop every key absent from the desired set — the drive's step 2 (§3.2),
    /// which bounds this table by the work outstanding rather than by the work
    /// ever done.
    pub fn retain_keys(&mut self, live: impl Fn(&K) -> bool) {
        self.entries.retain(|k, _| live(k));
    }

    /// Whether `key` may be launched at `now`. A key with no entry always may;
    /// a key at rest never may, for the remainder of this activation.
    pub fn ready(&self, key: &K, now: Instant) -> bool {
        match self.entries.get(key) {
            None => true,
            Some(Entry { until: None, .. }) => false,
            Some(Entry {
                until: Some(until), ..
            }) => now >= *until,
        }
    }

    /// Whether `key` is **at rest** (§6): backed off to the cap, so the drive
    /// has stopped arming a timer for it and it no longer holds the activation
    /// resident.
    ///
    /// A key merely waiting out an early, short delay is *not* at rest — the
    /// activation should stay resident for it, because it is about to retry.
    pub fn at_rest(&self, key: &K) -> bool {
        self.entries.get(key).is_some_and(|e| e.until.is_none())
    }

    #[cfg(test)]
    fn failures(&self, key: &K) -> u32 {
        self.entries.get(key).map(|e| e.failures).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> Instant {
        Instant::from_nanos(0)
    }

    #[test]
    fn the_delay_doubles_from_the_base_and_stops_at_the_cap() {
        let p = BackoffPolicy {
            base: Duration::from_millis(500),
            cap: Duration::from_secs(8),
        };
        assert_eq!(p.delay(0), Duration::ZERO);
        assert_eq!(p.delay(1), Duration::from_millis(500));
        assert_eq!(p.delay(2), Duration::from_secs(1));
        assert_eq!(p.delay(3), Duration::from_secs(2));
        assert_eq!(p.delay(4), Duration::from_secs(4));
        assert_eq!(p.delay(5), Duration::from_secs(8));
        assert_eq!(
            p.delay(60),
            Duration::from_secs(8),
            "saturates, never wraps"
        );
        assert_eq!(p.delay(u32::MAX), Duration::from_secs(8));
    }

    #[test]
    fn a_key_is_not_at_rest_until_its_delay_reaches_the_cap() {
        let mut b: Backoff<u8> = Backoff::default();
        // The first failure waits out a short delay: the activation stays
        // resident, because the retry is imminent (§6).
        assert!(
            b.failed(1, t0(), 0).is_some(),
            "an early failure arms a timer"
        );
        assert!(!b.ready(&1, t0()));
        assert!(!b.at_rest(&1), "an early delay is not rest");
        // The delay elapsing makes it launchable again, and still not rest.
        let later = t0() + Duration::from_secs(60);
        assert!(b.ready(&1, later));
        assert!(!b.at_rest(&1));
    }

    #[test]
    fn at_the_cap_the_drive_arms_nothing_and_the_key_comes_to_rest() {
        // The whole of the at-rest rule (§6). Arming a timer here would be worse
        // than useless: the activation would hibernate and then be woken by its
        // own stale timer, churning instead of resting.
        let mut b: Backoff<u8> = Backoff::default();
        let mut armed = 0;
        for _ in 0..8 {
            if b.failed(1, t0(), 0).is_some() {
                armed += 1;
            }
        }
        assert_eq!(armed, 4, "four doublings from 500ms reach the 8s cap");
        assert!(b.at_rest(&1));
        assert!(
            !b.ready(&1, t0() + Duration::from_secs(600)),
            "no amount of waiting relaunches an at-rest key: only a caller \
             contact or the next activation does"
        );
    }

    #[test]
    fn a_caller_contact_wakes_an_at_rest_key_and_starts_its_count_over() {
        let mut b: Backoff<u8> = Backoff::default();
        for _ in 0..8 {
            b.failed(1, t0(), 0);
        }
        assert!(b.at_rest(&1));
        b.wake();
        assert!(!b.at_rest(&1));
        assert!(b.ready(&1, t0()), "someone asked, so the drive tries again");
        assert_eq!(b.failures(&1), 0, "the retry budget starts over");
    }

    #[test]
    fn waking_leaves_a_key_still_serving_an_early_delay_alone() {
        let mut b: Backoff<u8> = Backoff::default();
        b.failed(1, t0(), 0);
        b.wake();
        assert!(
            !b.ready(&1, t0()),
            "an imminent retry is already scheduled; a contact must not \
             stampede it"
        );
    }

    #[test]
    fn a_committed_settle_clears_the_count() {
        let mut b: Backoff<u8> = Backoff::default();
        b.failed(1, t0(), 0);
        b.failed(1, t0(), 0);
        assert_eq!(b.failures(&1), 2);
        b.cleared(&1);
        assert_eq!(b.failures(&1), 0);
        assert!(b.ready(&1, t0()));
    }

    #[test]
    fn jitter_only_ever_extends_the_delay_and_stays_inside_half_of_it() {
        let mut b: Backoff<u8> = Backoff::default();
        let base = Duration::from_millis(500);
        for jitter in [0u64, 1, 7, u64::MAX] {
            let mut b2: Backoff<u8> = Backoff::default();
            let d = b2.failed(1, t0(), jitter).expect("the first failure arms");
            assert!(d >= base, "jitter never shortens: {d:?}");
            assert!(d < base + base / 2 + Duration::from_nanos(1));
        }
        // And it is a pure function of the drawn value: same seed, same delay.
        assert_eq!(b.failed(1, t0(), 42), {
            let mut c: Backoff<u8> = Backoff::default();
            c.failed(1, t0(), 42)
        });
    }

    #[test]
    fn keys_absent_from_the_desired_set_are_dropped() {
        let mut b: Backoff<u8> = Backoff::default();
        b.failed(1, t0(), 0);
        b.failed(2, t0(), 0);
        b.retain_keys(|k| *k == 1);
        assert!(!b.ready(&1, t0()), "kept");
        assert!(b.ready(&2, t0()), "dropped, so unencumbered");
    }
}
