//! The HTTP seam shared by production model adapters (agent spec §2.1,
//! §2.2): the one socket operation an adapter needs from its host, and the
//! shared adapter shell that drives a protocol through the bounded retry
//! engine.
//!
//! An adapter crate owns its **protocol** ([`Protocol`]) and nothing else. The
//! plumbing that is not protocol lives here, once: a deployment implements
//! [`HttpPost`] over its HTTP stack of choice (hyper, reqwest, raw rustls) and
//! the same value serves every adapter; [`ProtocolModel`] holds the socket, the
//! key, the seams, and the retry policy for all of them. Backoff and jitter
//! draw from the injected [`Clock`] and [`Entropy`] (§2.1), never the
//! wall clock or an OS RNG, so a simulated run reproduces the schedule. A
//! completion request is side-effect-free, so these internal retries do not
//! violate the core no-transparent-retry rule (core spec §1.1, §3.2); a failure
//! that survives the policy surfaces as [`ModelError`] for the run to end on,
//! never silently.

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
/// socket and the base URL; the adapter supplies everything else (agent spec
/// §2.2).
pub trait HttpPost: Send + Sync + 'static {
    fn post(
        &self,
        path: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> BoxFuture<'static, Result<HttpResponse, HttpError>>;
}

/// Retry tuning: attempts and the backoff curve, timed by the injected
/// [`Clock`] with jitter from the injected [`Entropy`] (§2.1). The
/// shell's judgment, not the caller's: no adapter or deployment tunes it.
const ATTEMPTS: u32 = 4;
const BASE_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(8);

/// Which failures the bounded policy absorbs (§2.1): pressure and transport
/// blips, never a request the API called malformed.
fn retryable(error: &ModelError) -> bool {
    matches!(
        error,
        ModelError::RateLimited | ModelError::Overloaded | ModelError::Api(_)
    )
}

/// Backoff before the `attempt`-th retry (1-based): exponential, capped, with
/// up to one base of seeded jitter.
fn backoff<E: Entropy + ?Sized>(entropy: &E, attempt: u32) -> Duration {
    let factor = 2u32.saturating_pow(attempt.saturating_sub(1).min(16));
    let base = (BASE_BACKOFF * factor).min(MAX_BACKOFF);
    let jitter_nanos = entropy.next_u64() % BASE_BACKOFF.as_nanos().max(1) as u64;
    (base + Duration::from_nanos(jitter_nanos)).min(MAX_BACKOFF)
}

/// A provider protocol: the wire-specific four an adapter crate owns; the
/// path, the headers carrying the key, the request JSON, and the `classify`
/// taxonomy mapping a completed exchange onto [`ModelError`] (§2.1). Which of
/// those errors retry is the shared shell's judgment, not the protocol's.
pub trait Protocol: Send + Sync + 'static {
    /// The API path every completion posts to.
    const PATH: &'static str;
    /// The request headers, carrying `api_key` the protocol's way.
    fn headers(api_key: &str) -> Vec<(String, String)>;
    /// Encode a [`ModelRequest`] as the protocol's wire JSON.
    fn encode(req: &ModelRequest) -> Value;
    /// Map a completed HTTP exchange onto the [`ModelError`] taxonomy (§2.1).
    fn classify(response: &HttpResponse) -> Result<ModelResponse, ModelError>;
}

/// The shared adapter shell (§2.1): the socket, the key, the injected seams,
/// and the retry policy, held once for every protocol. Each completion
/// encodes through `P`, then drives the bounded policy: POST, classify,
/// retry pressure and transport blips with capped exponential backoff. An
/// adapter crate defines a [`Protocol`] and exports an alias, e.g.
/// `pub type AnthropicModel<C, E> = ProtocolModel<Messages, C, E>;`.
///
/// `complete` draws the whole jitter schedule from [`Entropy`] before it
/// returns the future, so the future owns no entropy handle: the schedule is
/// fixed by the seed and the order of calls, whatever the future's polling.
pub struct ProtocolModel<P: Protocol, C: Clock, E: Entropy> {
    http: Arc<dyn HttpPost>,
    api_key: String,
    clock: C,
    entropy: Arc<E>,
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
        // Pre-draw the jitter schedule so the future owns no entropy handle.
        let backoffs: Vec<Duration> = (1..ATTEMPTS)
            .map(|a| backoff(self.entropy.as_ref(), a))
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
                    Err(error) if attempt < ATTEMPTS && retryable(&error) => {
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use actor_simulation::SimEntropy;
    use actor_simulation::Simulation;
    use serde_json::json;

    use super::*;
    use crate::model::ModelParams;
    use crate::budget::Usage;

    /// The smallest protocol: no headers, an empty body, and a status-only
    /// classification, so the test exercises the shell's policy and nothing
    /// of any wire format.
    struct StatusOnly;

    impl Protocol for StatusOnly {
        const PATH: &'static str = "/complete";
        fn headers(_api_key: &str) -> Vec<(String, String)> {
            Vec::new()
        }
        fn encode(_req: &ModelRequest) -> Value {
            json!({})
        }
        fn classify(response: &HttpResponse) -> Result<ModelResponse, ModelError> {
            match response.status {
                200 => Ok(ModelResponse {
                    content: String::new(),
                    calls: Vec::new(),
                    usage: Usage::default(),
                }),
                429 => Err(ModelError::RateLimited),
                503 => Err(ModelError::Overloaded),
                _ => Err(ModelError::InvalidRequest("bad field".to_string())),
            }
        }
    }

    /// A scripted transport: pops the next canned status per call and counts
    /// the calls.
    struct ScriptedHttp {
        statuses: Mutex<Vec<u16>>,
        calls: Mutex<usize>,
    }

    impl ScriptedHttp {
        fn new(mut statuses: Vec<u16>) -> Arc<Self> {
            statuses.reverse();
            Arc::new(ScriptedHttp {
                statuses: Mutex::new(statuses),
                calls: Mutex::new(0),
            })
        }

        fn calls(&self) -> usize {
            *self.calls.lock().expect("calls")
        }
    }

    impl HttpPost for ScriptedHttp {
        fn post(
            &self,
            _path: &str,
            _headers: &[(String, String)],
            _body: Vec<u8>,
        ) -> BoxFuture<'static, Result<HttpResponse, HttpError>> {
            *self.calls.lock().expect("calls") += 1;
            let status = self
                .statuses
                .lock()
                .expect("statuses")
                .pop()
                .expect("scripted status");
            Box::pin(async move {
                Ok(HttpResponse {
                    status,
                    body: Vec::new(),
                })
            })
        }
    }

    fn request() -> ModelRequest {
        ModelRequest {
            system_prompt: String::new(),
            params: ModelParams::default(),
            tools: Vec::new(),
            transcript: Arc::new(Vec::new()),
        }
    }

    fn model(
        seed: u64,
        sim: &Simulation,
        http: Arc<ScriptedHttp>,
    ) -> ProtocolModel<StatusOnly, actor_simulation::SimClock, SimEntropy> {
        ProtocolModel::new(http, "key", sim.clock(), Arc::new(SimEntropy::new(seed)))
    }

    #[test]
    fn pressure_is_retried_with_backoff_and_a_hard_error_is_not() {
        // Rate limit, then overload, then success: absorbed by the policy
        // (agent spec §2.1), each retry after a backoff on the logical clock.
        let sim = Simulation::new(7);
        let http = ScriptedHttp::new(vec![429, 503, 200]);
        let started = sim.now();
        let outcome = sim.block_on(model(7, &sim, Arc::clone(&http)).complete(request()));
        assert!(outcome.is_ok());
        assert_eq!(http.calls(), 3);
        assert!(sim.now() > started, "the retries took logical time");

        // A malformed request is the caller's bug: never retried (§2.1).
        let sim = Simulation::new(8);
        let http = ScriptedHttp::new(vec![400]);
        let outcome = sim.block_on(model(8, &sim, Arc::clone(&http)).complete(request()));
        assert_eq!(
            outcome,
            Err(ModelError::InvalidRequest("bad field".to_string()))
        );
        assert_eq!(http.calls(), 1);
    }

    #[test]
    fn the_policy_gives_up_after_its_attempts() {
        let sim = Simulation::new(9);
        let http = ScriptedHttp::new(vec![429; 8]);
        let outcome = sim.block_on(model(9, &sim, Arc::clone(&http)).complete(request()));
        assert_eq!(outcome, Err(ModelError::RateLimited));
        assert_eq!(http.calls(), ATTEMPTS as usize);
    }
}
