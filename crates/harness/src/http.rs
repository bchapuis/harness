//! The HTTP seam shared by production model adapters (harness spec §4.2,
//! §12.1): the one socket operation an adapter needs from its host, and the
//! bounded retry engine every adapter drives it with.
//!
//! An adapter crate owns its **protocol** — the wire JSON and the `classify`
//! taxonomy mapping a response onto [`ModelError`] — and nothing else. The
//! plumbing that is not protocol lives here, once: a deployment implements
//! [`HttpPost`] over its HTTP stack of choice (hyper, reqwest, raw rustls) and
//! the same value serves every adapter. Backoff and jitter draw from the
//! injected [`Clock`] and [`Entropy`] (§4.2 rule 1), never the wall clock or
//! an OS RNG, so a simulated run reproduces the schedule. A completion
//! request is side-effect-free, so these internal retries do not violate the
//! core no-transparent-retry rule (core spec §1.2, §4.3); a failure that
//! survives the policy surfaces as [`ModelError`] for the run to end on,
//! never silently.

use std::sync::Arc;
use std::time::Duration;

use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;

use crate::model::ModelError;
use crate::model::ModelResponse;

/// An HTTP response, reduced to what a protocol layer decides on.
#[derive(Clone, Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// A transport-level failure to complete the POST (connection refused, TLS
/// failure, timeout): retried like an overload, surfaced as
/// [`ModelError::Api`] past the policy.
#[derive(Clone, Debug)]
pub struct HttpError(pub String);

/// The one operation an adapter needs from its host: POST `body` to `path`
/// with `headers`, return the status and body. Implementations supply the
/// socket and the base URL; the adapter supplies everything else.
pub trait HttpPost: Send + Sync + 'static {
    fn post(
        &self,
        path: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> BoxFuture<'static, Result<HttpResponse, HttpError>>;
}

/// Retry tuning: attempts and the backoff curve, timed by the injected
/// [`Clock`] with jitter from the injected [`Entropy`] (§4.2 rule 1).
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub attempts: u32,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            attempts: 4,
            base_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(8),
        }
    }
}

/// Which failures the bounded policy absorbs (§4.3): pressure and transport
/// blips, never a request the API called malformed.
fn retryable(error: &ModelError) -> bool {
    matches!(
        error,
        ModelError::RateLimited | ModelError::Overloaded | ModelError::Api(_)
    )
}

/// Backoff before the `attempt`-th retry (1-based): exponential, capped, with
/// up to one base of seeded jitter.
fn backoff<E: Entropy + ?Sized>(retry: &RetryPolicy, entropy: &E, attempt: u32) -> Duration {
    let factor = 2u32.saturating_pow(attempt.saturating_sub(1).min(16));
    let base = (retry.base_backoff * factor).min(retry.max_backoff);
    let jitter_nanos = entropy.next_u64() % retry.base_backoff.as_nanos().max(1) as u64;
    (base + Duration::from_nanos(jitter_nanos)).min(retry.max_backoff)
}

/// Drive one encoded request through the bounded policy: POST, `classify`,
/// and retry pressure and transport blips with capped exponential backoff.
/// `classify` is the adapter's protocol: it maps a completed HTTP exchange
/// onto the [`ModelError`] taxonomy (§4.3); which of those errors retry is
/// this engine's shared judgment.
#[allow(clippy::too_many_arguments)] // one call site per adapter; the width IS the seam's contract
pub fn complete_with_retry<C: Clock, E: Entropy + ?Sized>(
    http: Arc<dyn HttpPost>,
    path: &'static str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    clock: &C,
    entropy: &E,
    retry: &RetryPolicy,
    classify: fn(&HttpResponse) -> Result<ModelResponse, ModelError>,
) -> BoxFuture<'static, Result<ModelResponse, ModelError>> {
    let clock = clock.clone();
    let attempts = retry.attempts.max(1);
    // Pre-draw the jitter schedule so the future owns no entropy handle.
    let backoffs: Vec<Duration> = (1..attempts).map(|a| backoff(retry, entropy, a)).collect();
    Box::pin(async move {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let outcome = match http.post(path, &headers, body.clone()).await {
                Ok(response) => classify(&response),
                Err(HttpError(e)) => Err(ModelError::Api(format!("transport: {e}"))),
            };
            match outcome {
                Ok(response) => return Ok(response),
                Err(error) if attempt < attempts && retryable(&error) => {
                    clock.sleep(backoffs[(attempt - 1) as usize]).await;
                }
                Err(error) => return Err(error),
            }
        }
    })
}
