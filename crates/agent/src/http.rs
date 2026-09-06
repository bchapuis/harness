//! The HTTP seam shared by production model adapters (agent spec §4.2,
//! §12.1): the one socket operation an adapter needs from its host, and the
//! shared adapter shell that drives a protocol through the bounded retry
//! engine.
//!
//! An adapter crate owns its **protocol** — the [`Protocol`] four: path,
//! headers, the wire JSON, and the `classify` taxonomy mapping a response
//! onto [`ModelError`] — and nothing else. The plumbing that is not protocol
//! lives here, once: a deployment implements [`HttpPost`] over its HTTP stack
//! of choice (hyper, reqwest, raw rustls) and the same value serves every
//! adapter; [`ProtocolModel`] holds the socket, the key, the seams, and the
//! retry policy for all of them. Backoff and jitter draw from the injected
//! [`Clock`] and [`Entropy`] (§4.2 rule 1), never the wall clock or an OS
//! RNG, so a simulated run reproduces the schedule. A completion request is
//! side-effect-free, so these internal retries do not violate the core
//! no-transparent-retry rule (core spec §1.2, §4.3); a failure that survives
//! the policy surfaces as [`ModelError`] for the run to end on, never
//! silently.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use serde_json::Value;

use crate::model::Model;
use crate::model::ModelError;
use crate::model::ModelRequest;
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
/// [`Clock`] with jitter from the injected [`Entropy`] (§4.2 rule 1). The
/// shell's judgment, not the caller's: no adapter or deployment tunes it.
#[derive(Clone, Debug)]
struct RetryPolicy {
    attempts: u32,
    base_backoff: Duration,
    max_backoff: Duration,
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

/// A provider protocol: the wire-specific four an adapter crate owns — the
/// path, the headers carrying the key, the request JSON, and the `classify`
/// taxonomy mapping a completed exchange onto [`ModelError`] (§4.3). Which of
/// those errors retry is the shared shell's judgment, not the protocol's.
pub trait Protocol: Send + Sync + 'static {
    /// The API path every completion posts to.
    const PATH: &'static str;
    /// The request headers, carrying `api_key` the protocol's way.
    fn headers(api_key: &str) -> Vec<(String, String)>;
    /// Encode a [`ModelRequest`] as the protocol's wire JSON.
    fn encode(req: &ModelRequest) -> Value;
    /// Map a completed HTTP exchange onto the [`ModelError`] taxonomy (§4.3).
    fn classify(response: &HttpResponse) -> Result<ModelResponse, ModelError>;
}

/// The shared adapter shell (§4.2): the socket, the key, the injected seams,
/// and the retry policy, held once for every protocol. Each completion
/// encodes through `P`, then drives the bounded policy — POST, classify,
/// retry pressure and transport blips with capped exponential backoff. An
/// adapter crate defines a [`Protocol`] and exports an alias, e.g.
/// `pub type AnthropicModel<C, E> = ProtocolModel<Messages, C, E>;`.
pub struct ProtocolModel<P: Protocol, C: Clock, E: Entropy> {
    http: Arc<dyn HttpPost>,
    api_key: String,
    clock: C,
    entropy: Arc<E>,
    retry: RetryPolicy,
    _protocol: PhantomData<P>,
}

impl<P: Protocol, C: Clock, E: Entropy> ProtocolModel<P, C, E> {
    pub fn new(
        http: Arc<dyn HttpPost>,
        api_key: impl Into<String>,
        clock: C,
        entropy: Arc<E>,
    ) -> Self {
        ProtocolModel {
            http,
            api_key: api_key.into(),
            clock,
            entropy,
            retry: RetryPolicy::default(),
            _protocol: PhantomData,
        }
    }

}

impl<P: Protocol, C: Clock, E: Entropy> Model for ProtocolModel<P, C, E> {
    fn complete(&self, req: ModelRequest) -> BoxFuture<'static, Result<ModelResponse, ModelError>> {
        let body = serde_json::to_vec(&P::encode(&req)).expect("request encodes");
        let headers = P::headers(&self.api_key);
        let http = Arc::clone(&self.http);
        let clock = self.clock.clone();
        let attempts = self.retry.attempts.max(1);
        // Pre-draw the jitter schedule so the future owns no entropy handle.
        let backoffs: Vec<Duration> = (1..attempts)
            .map(|a| backoff(&self.retry, self.entropy.as_ref(), a))
            .collect();
        Box::pin(async move {
            let mut attempt = 0;
            loop {
                attempt += 1;
                let outcome = match http.post(P::PATH, &headers, body.clone()).await {
                    Ok(response) => P::classify(&response),
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
}

/// A tool result's wire form, shared across protocols: a bare string rides
/// as-is, anything structured as JSON text.
pub fn tool_result_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}
