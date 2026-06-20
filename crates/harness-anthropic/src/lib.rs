//! The production [`Model`] seam (harness spec §4, §12.1): the Anthropic
//! Messages API client.
//!
//! This crate is the single place the Messages API protocol exists: the
//! request and response JSON, the transcript encoding, the error taxonomy,
//! and the retry policy — with backoff and jitter drawn from `Clock` and
//! `Entropy` (§4.2 rule 1), never from the wall clock or an OS RNG. A
//! completion request is side-effect-free, so internal retries do not
//! violate the core no-transparent-retry rule (core spec §1.2, §4.3); a
//! failure that survives the policy surfaces as `ModelError` for the run to
//! end on, never silently.
//!
//! The socket is a seam: [`HttpPost`] is the one operation the client needs
//! from its host — POST these bytes to this path with these headers — the
//! same move the core makes with `Transport` (core spec §7). The deployment
//! supplies the two-liner over its HTTP stack of choice; everything that
//! could be wrong about *talking to Anthropic* lives here, deterministic and
//! tested, while the byte transport stays swappable (and the simulator's
//! scripted model replaces this crate entirely, §12.1).

use std::sync::Arc;
use std::time::Duration;

use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use harness::Entry;
use harness::Model;
use harness::ModelError;
use harness::ModelRequest;
use harness::ModelResponse;
use harness::ToolCall;
use harness::Usage;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;

/// The API path every completion posts to.
pub const MESSAGES_PATH: &str = "/v1/messages";
/// The API version header this client speaks.
pub const ANTHROPIC_VERSION: &str = "2023-06-01";

/// An HTTP response, reduced to what the protocol layer decides on.
#[derive(Clone, Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// A transport-level failure to complete the POST (connection refused, TLS
/// failure, timeout): retried like an overload, surfaced as `Api` past the
/// policy.
#[derive(Clone, Debug)]
pub struct HttpError(pub String);

/// The one operation the client needs from its host: POST `body` to `path`
/// with `headers`, return the status and body. Implementations supply the
/// socket (hyper, reqwest, raw rustls — the deployment's choice); this crate
/// supplies everything else.
pub trait HttpPost: Send + Sync + 'static {
    fn post(
        &self,
        path: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> BoxFuture<'static, Result<HttpResponse, HttpError>>;
}

/// Retry tuning: attempts and the backoff curve, timed by the injected
/// `Clock` with jitter from the injected `Entropy` (§4.2 rule 1).
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

/// The Anthropic Messages API model (harness spec §4.1).
pub struct AnthropicModel<C: Clock, E: Entropy> {
    http: Arc<dyn HttpPost>,
    api_key: String,
    clock: C,
    entropy: Arc<E>,
    retry: RetryPolicy,
}

impl<C: Clock, E: Entropy> AnthropicModel<C, E> {
    pub fn new(
        http: Arc<dyn HttpPost>,
        api_key: impl Into<String>,
        clock: C,
        entropy: Arc<E>,
    ) -> Self {
        AnthropicModel {
            http,
            api_key: api_key.into(),
            clock,
            entropy,
            retry: RetryPolicy::default(),
        }
    }

    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    fn headers(&self) -> Vec<(String, String)> {
        vec![
            ("x-api-key".to_string(), self.api_key.clone()),
            (
                "anthropic-version".to_string(),
                ANTHROPIC_VERSION.to_string(),
            ),
            ("content-type".to_string(), "application/json".to_string()),
        ]
    }

    /// Backoff before the `attempt`-th retry (1-based): exponential, capped,
    /// with up to one base of seeded jitter.
    fn backoff(&self, attempt: u32) -> Duration {
        let factor = 2u32.saturating_pow(attempt.saturating_sub(1).min(16));
        let base = (self.retry.base_backoff * factor).min(self.retry.max_backoff);
        let jitter_nanos =
            self.entropy.next_u64() % self.retry.base_backoff.as_nanos().max(1) as u64;
        (base + Duration::from_nanos(jitter_nanos)).min(self.retry.max_backoff)
    }
}

impl<C: Clock, E: Entropy> Model for AnthropicModel<C, E> {
    fn complete(&self, req: ModelRequest) -> BoxFuture<'static, Result<ModelResponse, ModelError>> {
        let body = serde_json::to_vec(&encode_request(&req)).expect("request encodes");
        let headers = self.headers();
        let http = Arc::clone(&self.http);
        let clock = self.clock.clone();
        let attempts = self.retry.attempts.max(1);
        // Pre-draw the jitter schedule so the future owns no entropy handle.
        let backoffs: Vec<Duration> = (1..attempts).map(|a| self.backoff(a)).collect();
        Box::pin(async move {
            let mut attempt = 0;
            loop {
                attempt += 1;
                let outcome = match http.post(MESSAGES_PATH, &headers, body.clone()).await {
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
}

/// Which failures the bounded policy absorbs (§4.3): pressure and transport
/// blips, never a request the API called malformed.
fn retryable(error: &ModelError) -> bool {
    matches!(
        error,
        ModelError::RateLimited | ModelError::Overloaded | ModelError::Api(_)
    )
}

// ---------------------------------------------------------------------------
// Request encoding (harness Entry → Messages API)
// ---------------------------------------------------------------------------

/// Encode a [`ModelRequest`] as a Messages API request body. Public for
/// golden tests; the encoding is part of this crate's contract.
pub fn encode_request(req: &ModelRequest) -> Value {
    // Each transcript entry becomes content blocks for a role; consecutive
    // same-role blocks merge into one message, as the API prefers.
    let mut messages: Vec<(&str, Vec<Value>)> = Vec::new();
    let mut push = |role: &'static str, block: Value| match messages.last_mut() {
        Some((last, blocks)) if *last == role => blocks.push(block),
        _ => messages.push((role, vec![block])),
    };
    for entry in req.transcript.iter() {
        match entry {
            Entry::User(text) => push("user", json!({"type": "text", "text": text})),
            Entry::Assistant { content, calls } => {
                if !content.is_empty() {
                    push("assistant", json!({"type": "text", "text": content}));
                }
                for call in calls {
                    push(
                        "assistant",
                        json!({
                            "type": "tool_use",
                            "id": call.id.as_str(),
                            "name": call.name,
                            "input": call.input,
                        }),
                    );
                }
            }
            Entry::ToolResult { call, outcome } => {
                let (content, is_error) = match outcome {
                    Ok(value) => (render(value), false),
                    Err(error) => (error.to_string(), true),
                };
                push(
                    "user",
                    json!({
                        "type": "tool_result",
                        "tool_use_id": call.as_str(),
                        "content": content,
                        "is_error": is_error,
                    }),
                );
            }
            // The workspace-loss notice (§5.5): it answers no CallId, so it
            // enters as input content the harness authors — the encoding's
            // analogue of a user message.
            Entry::WorkspaceReset => push(
                "user",
                json!({
                    "type": "text",
                    "text": "[harness] The workspace was reset: files, processes, and \
                             servers from earlier in this conversation no longer exist. \
                             Re-derive anything you need before relying on it.",
                }),
            ),
        }
    }
    let messages: Vec<Value> = messages
        .into_iter()
        .map(|(role, content)| json!({"role": role, "content": content}))
        .collect();

    let mut body = json!({
        "model": req.params.model,
        "max_tokens": req.max_tokens,
        "system": req.system_prompt,
        "messages": messages,
    });
    if !req.tools.is_empty() {
        body["tools"] = Value::Array(
            req.tools
                .iter()
                .map(|tool| {
                    json!({
                        "name": tool.name,
                        "description": tool.description,
                        "input_schema": tool.input_schema,
                    })
                })
                .collect(),
        );
    }
    body
}

/// A tool result's wire form: a bare string rides as-is, anything structured
/// as JSON text.
fn render(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Response decoding and the error taxonomy (§4.3)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ApiResponse {
    content: Vec<ContentBlock>,
    usage: ApiUsage,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum ContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct ApiUsage {
    input_tokens: u64,
    output_tokens: u64,
}

#[derive(Deserialize, Serialize)]
struct ApiError {
    error: ApiErrorBody,
}

#[derive(Deserialize, Serialize)]
struct ApiErrorBody {
    #[serde(rename = "type")]
    kind: String,
    message: String,
}

/// Map an HTTP outcome onto the harness's [`ModelError`] taxonomy (§4.3).
fn classify(response: &HttpResponse) -> Result<ModelResponse, ModelError> {
    if response.status == 200 {
        let parsed: ApiResponse = serde_json::from_slice(&response.body)
            .map_err(|e| ModelError::Api(format!("malformed response body: {e}")))?;
        let mut content = String::new();
        let mut calls = Vec::new();
        for block in parsed.content {
            match block {
                ContentBlock::Text { text } => content.push_str(&text),
                ContentBlock::ToolUse { id, name, input } => calls.push(ToolCall {
                    id: harness::CallId::new(id),
                    name,
                    input,
                }),
                ContentBlock::Other => {}
            }
        }
        return Ok(ModelResponse {
            content,
            calls,
            usage: Usage {
                input_tokens: parsed.usage.input_tokens,
                output_tokens: parsed.usage.output_tokens,
            },
        });
    }
    let detail = serde_json::from_slice::<ApiError>(&response.body)
        .map(|e| e.error)
        .unwrap_or(ApiErrorBody {
            kind: format!("http_{}", response.status),
            message: String::from_utf8_lossy(&response.body).into_owned(),
        });
    Err(match (response.status, detail.kind.as_str()) {
        (429, _) => ModelError::RateLimited,
        (529, _) | (_, "overloaded_error") => ModelError::Overloaded,
        // The API reports an over-long prompt as an invalid request; the
        // harness distinguishes it because the run must fail explicitly on
        // it (§1.1, §4.3).
        (400, _) if detail.message.contains("prompt is too long") => ModelError::ContextOverflow,
        (400, _) | (_, "invalid_request_error") => ModelError::InvalidRequest(detail.message),
        _ => ModelError::Api(format!("{}: {}", detail.kind, detail.message)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use actor_simulation::SimEntropy;
    use actor_simulation::Simulation;
    use harness::CallId;
    use harness::ModelParams;
    use harness::ToolError;
    use harness::ToolSpec;

    /// A scripted transport: pops the next canned reply per call and records
    /// every request.
    struct ScriptedHttp {
        replies: Mutex<Vec<Result<HttpResponse, HttpError>>>,
        seen: Mutex<Vec<Value>>,
    }

    impl ScriptedHttp {
        fn new(mut replies: Vec<Result<HttpResponse, HttpError>>) -> Arc<Self> {
            replies.reverse();
            Arc::new(ScriptedHttp {
                replies: Mutex::new(replies),
                seen: Mutex::new(Vec::new()),
            })
        }
    }

    impl HttpPost for ScriptedHttp {
        fn post(
            &self,
            path: &str,
            headers: &[(String, String)],
            body: Vec<u8>,
        ) -> BoxFuture<'static, Result<HttpResponse, HttpError>> {
            assert_eq!(path, MESSAGES_PATH);
            assert!(headers.iter().any(|(k, _)| k == "x-api-key"));
            self.seen
                .lock()
                .expect("seen")
                .push(serde_json::from_slice(&body).expect("json body"));
            let reply = self
                .replies
                .lock()
                .expect("replies")
                .pop()
                .expect("scripted reply");
            Box::pin(async move { reply })
        }
    }

    fn ok_body(json: Value) -> Result<HttpResponse, HttpError> {
        Ok(HttpResponse {
            status: 200,
            body: serde_json::to_vec(&json).expect("encode"),
        })
    }

    fn request() -> ModelRequest {
        ModelRequest {
            system_prompt: "be terse".to_string(),
            params: ModelParams::default(),
            tools: vec![ToolSpec {
                name: "shell".to_string(),
                description: "run".to_string(),
                input_schema: json!({"type": "object"}),
            }],
            transcript: std::sync::Arc::new(vec![
                Entry::User("hello".to_string()),
                Entry::Assistant {
                    content: "using a tool".to_string(),
                    calls: vec![ToolCall {
                        id: CallId::new("tu_1"),
                        name: "shell".to_string(),
                        input: json!({"cmd": "ls"}),
                    }],
                },
                Entry::ToolResult {
                    call: CallId::new("tu_1"),
                    outcome: Err(ToolError::Timeout),
                },
                Entry::WorkspaceReset,
            ]),
            max_tokens: 1234,
        }
    }

    #[test]
    fn the_request_encoding_matches_the_messages_api() {
        let body = encode_request(&request());
        assert_eq!(body["model"], "claude-sonnet-4-6");
        assert_eq!(body["max_tokens"], 1234);
        assert_eq!(body["system"], "be terse");
        let messages = body["messages"].as_array().expect("messages");
        // user(hello) → assistant(text + tool_use) → user(tool_result + reset
        // notice): consecutive same-role blocks merged into one message.
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[1]["role"], "assistant");
        assert_eq!(messages[1]["content"][1]["type"], "tool_use");
        assert_eq!(messages[2]["content"][0]["type"], "tool_result");
        assert_eq!(messages[2]["content"][0]["is_error"], true);
        assert_eq!(messages[2]["content"][1]["type"], "text");
        assert_eq!(body["tools"][0]["name"], "shell");
    }

    #[test]
    fn a_successful_response_decodes_content_calls_and_usage() {
        let sim = Simulation::new(1);
        let http = ScriptedHttp::new(vec![ok_body(json!({
            "content": [
                {"type": "text", "text": "running it"},
                {"type": "tool_use", "id": "tu_9", "name": "shell", "input": {"cmd": "ls"}}
            ],
            "usage": {"input_tokens": 50, "output_tokens": 9}
        }))]);
        let model = AnthropicModel::new(http, "sk-test", sim.clock(), Arc::new(SimEntropy::new(1)));
        let response = sim.block_on(model.complete(request())).expect("completes");
        assert_eq!(response.content, "running it");
        assert_eq!(response.calls.len(), 1);
        assert_eq!(response.calls[0].id, CallId::new("tu_9"));
        assert_eq!(response.usage.total(), 59);
    }

    #[test]
    fn pressure_is_retried_with_backoff_and_a_hard_error_is_not() {
        let sim = Simulation::new(7);
        // 429, then 529, then success: absorbed by the policy (§4.3).
        let http = ScriptedHttp::new(vec![
            Ok(HttpResponse {
                status: 429,
                body: b"{}".to_vec(),
            }),
            Ok(HttpResponse {
                status: 529,
                body: b"{}".to_vec(),
            }),
            ok_body(json!({"content": [], "usage": {"input_tokens": 1, "output_tokens": 1}})),
        ]);
        let model = AnthropicModel::new(
            Arc::clone(&http) as Arc<dyn HttpPost>,
            "sk-test",
            sim.clock(),
            Arc::new(SimEntropy::new(7)),
        );
        let started = sim.now();
        let response = sim.block_on(model.complete(request()));
        assert!(response.is_ok());
        assert_eq!(http.seen.lock().expect("seen").len(), 3);
        assert!(sim.now() > started, "the retries took logical time");

        // A malformed request is the caller's bug: never retried (§4.3).
        let sim = Simulation::new(8);
        let http = ScriptedHttp::new(vec![Ok(HttpResponse {
            status: 400,
            body: serde_json::to_vec(&json!({
                "error": {"type": "invalid_request_error", "message": "bad field"}
            }))
            .expect("encode"),
        })]);
        let model = AnthropicModel::new(
            Arc::clone(&http) as Arc<dyn HttpPost>,
            "sk-test",
            sim.clock(),
            Arc::new(SimEntropy::new(8)),
        );
        let outcome = sim.block_on(model.complete(request()));
        assert_eq!(
            outcome,
            Err(ModelError::InvalidRequest("bad field".to_string()))
        );
        assert_eq!(http.seen.lock().expect("seen").len(), 1);
    }

    #[test]
    fn an_overlong_prompt_is_a_context_overflow() {
        let sim = Simulation::new(9);
        let http = ScriptedHttp::new(vec![Ok(HttpResponse {
            status: 400,
            body: serde_json::to_vec(&json!({
                "error": {"type": "invalid_request_error",
                           "message": "prompt is too long: 250000 tokens > 200000"}
            }))
            .expect("encode"),
        })]);
        let model = AnthropicModel::new(http, "sk-test", sim.clock(), Arc::new(SimEntropy::new(9)));
        let outcome = sim.block_on(model.complete(request()));
        assert_eq!(outcome, Err(ModelError::ContextOverflow));
    }
}
