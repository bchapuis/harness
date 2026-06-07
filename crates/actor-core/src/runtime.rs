//! The runtime environment seam (spec §4.6).
//!
//! Time, randomness, and task spawning are the three capabilities the runtime
//! needs from its host. Each is an ordinary trait, and **no subsystem may read
//! any of them from the host directly**. This indirection is exactly what lets
//! the same actor code run under the production runtime and under deterministic
//! simulation (spec §18): only the trait implementations differ.
//!
//! The trait methods return `impl Future + Send` rather than using `async fn`
//! so that code generic over a [`Clock`] can rely on the returned futures being
//! `Send` — required because executors are spawned through [`Spawner`] as
//! `Send` futures.

use std::future::Future;
use std::ops::Add;
use std::time::Duration;

pub use futures::future::BoxFuture;

/// A logical point in time, measured as a duration since an unspecified epoch.
///
/// Deliberately independent of [`std::time::Instant`]: a virtual clock must be
/// able to manufacture and advance instants freely, which the std type forbids.
/// The production clock maps wall-clock deltas onto this type.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Instant {
    nanos: u64,
}

impl Instant {
    /// The epoch; the instant a freshly started clock reports.
    pub const ZERO: Instant = Instant { nanos: 0 };

    /// Construct an instant a fixed number of nanoseconds past the epoch.
    pub const fn from_nanos(nanos: u64) -> Instant {
        Instant { nanos }
    }

    /// Nanoseconds since the epoch.
    pub const fn as_nanos(self) -> u64 {
        self.nanos
    }

    /// The amount of time elapsed from `earlier` to `self`, saturating at zero.
    pub fn duration_since(self, earlier: Instant) -> Duration {
        Duration::from_nanos(self.nanos.saturating_sub(earlier.nanos))
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;

    fn add(self, rhs: Duration) -> Instant {
        // Saturate rather than panic: a deadline far in the future is benign.
        let add = u64::try_from(rhs.as_nanos()).unwrap_or(u64::MAX);
        Instant {
            nanos: self.nanos.saturating_add(add),
        }
    }
}

/// Returned by [`Clock::timeout`] when the future did not complete in time.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Elapsed;

impl std::fmt::Display for Elapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("operation timed out")
    }
}

impl std::error::Error for Elapsed {}

/// Virtual or real time. No subsystem may read wall-clock time directly
/// (spec §4.6). `Clone` so the executor can own a handle for supervision
/// backoff (spec §11.2); implementations are cheap to clone (an `Arc` inside).
pub trait Clock: Clone + Send + Sync + 'static {
    /// The current logical time.
    fn now(&self) -> Instant;

    /// Complete after `dur` of logical time has elapsed.
    fn sleep(&self, dur: Duration) -> impl Future<Output = ()> + Send;

    /// Run `f`, failing with [`Elapsed`] if it does not finish within `within`.
    ///
    /// Provided in terms of [`Clock::sleep`]; implementations need only supply
    /// `now` and `sleep`.
    fn timeout<F>(
        &self,
        within: Duration,
        f: F,
    ) -> impl Future<Output = Result<F::Output, Elapsed>> + Send
    where
        Self: Sized,
        F: Future + Send,
        F::Output: Send,
    {
        async move {
            let sleep = self.sleep(within);
            futures::pin_mut!(f, sleep);
            match futures::future::select(f, sleep).await {
                futures::future::Either::Left((out, _)) => Ok(out),
                futures::future::Either::Right(((), _)) => Err(Elapsed),
            }
        }
    }
}

/// The single source of randomness (spec §4.6). Seedable in simulation; the
/// only randomness anywhere in the system. Uses interior mutability so a shared
/// `&self` can advance the stream.
pub trait Entropy: Send + Sync + 'static {
    /// Draw the next 64 bits from the stream.
    fn next_u64(&self) -> u64;

    /// Uniformly pick an index in `0..len`, or `None` if `len == 0`.
    ///
    /// The one place index selection over a collection is centralized, so peer
    /// selection, SWIM's `k` members, and scheduler tie-breaks all draw the
    /// same way.
    fn pick_index(&self, len: usize) -> Option<usize> {
        if len == 0 {
            None
        } else {
            Some((self.next_u64() % len as u64) as usize)
        }
    }

    /// A fault gate for deterministic fault injection (spec §18.3), in the style
    /// of FoundationDB's "buggify". Returns `true` with probability
    /// `numerator / denominator`, drawn from this stream.
    ///
    /// Production entropy leaves it **off** (the default always returns
    /// `false`), so buggify call-sites in the runtime cost nothing outside
    /// simulation; a simulated `Entropy` overrides it to enable faults.
    fn buggify(&self, _numerator: u64, _denominator: u64) -> bool {
        false
    }
}

/// Task spawning (spec §4.6). Mailbox executors and, later, the gossip and
/// failure-detector loops run through this.
pub trait Spawner: Send + Sync + 'static {
    /// Named `launch`, not `spawn`, so a raw task is never confused with
    /// spawning an actor (`ActorSystem::spawn` / `Ctx::spawn`).
    fn launch(&self, task: BoxFuture<'static, ()>);
}
