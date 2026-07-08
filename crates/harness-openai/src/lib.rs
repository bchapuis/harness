//! The OpenAI-compatible [`Model`] seam (harness spec §4, §12.1): a Chat
//! Completions API client.
//!
//! This crate is the single place the Chat Completions protocol exists: the
//! request and response JSON, the transcript encoding, the error taxonomy,
//! and the retry policy — with backoff and jitter drawn from `Clock` and
//! `Entropy` (§4.2 rule 1), never from the wall clock or an OS RNG. It is the
//! sibling of `harness-anthropic`: same seam, same retry shape, a different
//! wire protocol. A completion request is side-effect-free, so internal
//! retries do not violate the core no-transparent-retry rule (core spec §1.2,
//! §4.3); a failure that survives the policy surfaces as `ModelError` for the
//! run to end on, never silently.
//!
//! "OpenAI-compatible" is the point: the socket is a seam ([`HttpPost`]) and
//! the host owns the base URL, so the same client speaks to OpenAI, xAI (Grok),
//! OpenRouter, Together, Groq, vLLM, llama.cpp, Ollama, or any server that
//! serves `/v1/chat/completions`. Everything that could be wrong about
//! *talking* the protocol lives here, deterministic and tested, while the byte
//! transport and the endpoint stay the deployment's choice ([`base_url`] lists
//! the known ones).
//!
//! The transport plumbing (`HttpPost`, `RetryPolicy`, the backoff curve) is
//! duplicated from `harness-anthropic` rather than shared: the two providers
//! only look alike, and each retry policy is part of its own protocol contract
//! (Anthropic's `529`/`overloaded_error`, OpenAI's `context_length_exceeded`).
//! A third provider is the trigger to lift the common half into a shared crate.

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

/// The API path every completion posts to. It carries the `/v1` prefix, so a
/// host's [`HttpPost`] points at a base URL *without* one — see [`base_url`].
pub const COMPLETIONS_PATH: &str = "/v1/chat/completions";

/// Base URLs of well-known OpenAI-compatible endpoints, for a host to point its
/// [`HttpPost`] at. Each is the origin (plus any provider-specific prefix) that
/// [`COMPLETIONS_PATH`] appends to; none ends in `/v1`, because the path already
/// carries it — the one join easy to get wrong. The base URL is the
/// deployment's to choose (§12.1); these are convenience data, not policy, and
/// self-hosted servers (vLLM, llama.cpp, Ollama) supply their own.
pub mod base_url {
    /// OpenAI: `https://api.openai.com/v1/chat/completions`.
    pub const OPENAI: &str = "https://api.openai.com";
    /// xAI (Grok): `https://api.x.ai/v1/chat/completions`.
    pub const XAI: &str = "https://api.x.ai";
    /// OpenRouter: `https://openrouter.ai/api/v1/chat/completions`.
    pub const OPENROUTER: &str = "https://openrouter.ai/api";
    /// Together: `https://api.together.xyz/v1/chat/completions`.
    pub const TOGETHER: &str = "https://api.together.xyz";
    /// Groq: `https://api.groq.com/openai/v1/chat/completions`.
    pub const GROQ: &str = "https://api.groq.com/openai";
}

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
/// socket and the base URL (hyper, reqwest, raw rustls — and OpenAI, a local
/// server, or any compatible endpoint); this crate supplies everything else.
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

/// The OpenAI-compatible Chat Completions model (harness spec §4.1).
pub struct OpenAiModel<C: Clock, E: Entropy> {
    http: Arc<dyn HttpPost>,
    api_key: String,
    clock: C,
    entropy: Arc<E>,
    retry: RetryPolicy,
}

impl<C: Clock, E: Entropy> OpenAiModel<C, E> {
    pub fn new(
        http: Arc<dyn HttpPost>,
        api_key: impl Into<String>,
        clock: C,
        entropy: Arc<E>,
    ) -> Self {
        OpenAiModel {
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
            (
                "authorization".to_string(),
                format!("Bearer {}", self.api_key),
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

impl<C: Clock, E: Entropy> Model for OpenAiModel<C, E> {
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
                let outcome = match http.post(COMPLETIONS_PATH, &headers, body.clone()).await {
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
// Request encoding (harness Entry → Chat Completions API)
// ---------------------------------------------------------------------------

/// Encode a [`ModelRequest`] as a Chat Completions request body. Public for
/// golden tests; the encoding is part of this crate's contract.
///
/// Unlike the Messages API, Chat Completions carries the system prompt as the
/// first message, folds an assistant turn's text and tool calls into one
/// message (arguments as a JSON *string*), and answers a call with a distinct
/// `role: "tool"` message keyed by `tool_call_id`.
pub fn encode_request(req: &ModelRequest) -> Value {
    let mut messages: Vec<Value> = Vec::new();
    if !req.system_prompt.is_empty() {
        messages.push(json!({"role": "system", "content": req.system_prompt}));
    }
    for entry in req.transcript.iter() {
        match entry {
            Entry::User(text) => messages.push(json!({"role": "user", "content": text})),
            Entry::Assistant { content, calls } => {
                let mut message = json!({"role": "assistant", "content": content});
                if !calls.is_empty() {
                    // Chat Completions carries tool arguments as a JSON string,
                    // not a structured object.
                    message["tool_calls"] = Value::Array(
                        calls
                            .iter()
                            .map(|call| {
                                json!({
                                    "id": call.id.as_str(),
                                    "type": "function",
                                    "function": {
                                        "name": call.name,
                                        "arguments": call.input.to_string(),
                                    },
                                })
                            })
                            .collect(),
                    );
                }
                messages.push(message);
            }
            Entry::ToolResult { call, outcome } => {
                // The protocol has no `is_error` flag on a tool message, so a
                // failure rides as marked content the model can react to.
                let content = match outcome {
                    Ok(value) => render(value),
                    Err(error) => format!("[error] {error}"),
                };
                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": call.as_str(),
                    "content": content,
                }));
            }
            // The workspace-loss notice (§5.5): it answers no CallId, so it
            // enters as a user message the harness authors.
            Entry::WorkspaceReset => messages.push(json!({
                "role": "user",
                "content": "[harness] The workspace was reset: files, processes, and \
                            servers from earlier in this conversation no longer exist. \
                            Re-derive anything you need before relying on it.",
            })),
        }
    }

    let mut body = json!({
        "model": req.params.model,
        "max_tokens": req.max_tokens,
        "messages": messages,
    });
    if !req.tools.is_empty() {
        body["tools"] = Value::Array(
            req.tools
                .iter()
                .map(|tool| {
                    json!({
                        "type": "function",
                        "function": {
                            "name": tool.name,
                            "description": tool.description,
                            "parameters": tool.input_schema,
                        },
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
    choices: Vec<Choice>,
    usage: ApiUsage,
}

#[derive(Deserialize)]
struct Choice {
    message: ChoiceMessage,
}

#[derive(Deserialize)]
struct ChoiceMessage {
    /// Null when the turn is tool calls only.
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<ApiToolCall>,
}

#[derive(Deserialize)]
struct ApiToolCall {
    id: String,
    function: ApiFunction,
}

#[derive(Deserialize)]
struct ApiFunction {
    name: String,
    /// A JSON string; parsed back to a value, or kept as a string if the
    /// server sent something that is not valid JSON.
    arguments: String,
}

#[derive(Deserialize)]
struct ApiUsage {
    prompt_tokens: u64,
    completion_tokens: u64,
}

#[derive(Deserialize, Serialize)]
struct ApiError {
    error: ApiErrorBody,
}

#[derive(Deserialize, Serialize)]
struct ApiErrorBody {
    #[serde(rename = "type")]
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    code: Option<String>,
    message: String,
}

/// Map an HTTP outcome onto the harness's [`ModelError`] taxonomy (§4.3).
fn classify(response: &HttpResponse) -> Result<ModelResponse, ModelError> {
    if response.status == 200 {
        let parsed: ApiResponse = serde_json::from_slice(&response.body)
            .map_err(|e| ModelError::Api(format!("malformed response body: {e}")))?;
        let message = parsed
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| ModelError::Api("response had no choices".to_string()))?
            .message;
        let content = message.content.unwrap_or_default();
        let calls = message
            .tool_calls
            .into_iter()
            .map(|call| ToolCall {
                id: harness::CallId::new(call.id),
                name: call.function.name,
                // Arguments arrive as a JSON string; parse to a value so tool
                // dispatch sees structured input, falling back to a string.
                input: serde_json::from_str(&call.function.arguments)
                    .unwrap_or(Value::String(call.function.arguments)),
            })
            .collect();
        return Ok(ModelResponse {
            content,
            calls,
            usage: Usage {
                input_tokens: parsed.usage.prompt_tokens,
                output_tokens: parsed.usage.completion_tokens,
            },
        });
    }
    let detail = serde_json::from_slice::<ApiError>(&response.body)
        .map(|e| e.error)
        .unwrap_or(ApiErrorBody {
            kind: Some(format!("http_{}", response.status)),
            code: None,
            message: String::from_utf8_lossy(&response.body).into_owned(),
        });
    let code = detail.code.as_deref().unwrap_or("");
    Err(match response.status {
        429 => ModelError::RateLimited,
        // Server-side pressure and transient failures: retried like an
        // overload. Compatible servers use 500/502/503 (and Anthropic-style
        // 529) for the same conditions.
        500 | 502 | 503 | 529 => ModelError::Overloaded,
        // An over-long prompt is reported as an invalid request; the harness
        // distinguishes it because the run must fail explicitly on it
        // (§1.1, §4.3). Detection is by code, or by message across the servers
        // that omit it — the leakiest part of "compatible".
        400 | 413 if code == "context_length_exceeded" || is_context_message(&detail.message) => {
            ModelError::ContextOverflow
        }
        400 | 404 | 413 | 422 => ModelError::InvalidRequest(detail.message),
        _ => ModelError::Api(format!(
            "{}: {}",
            detail.kind.as_deref().unwrap_or("error"),
            detail.message
        )),
    })
}

/// Whether an error message names an over-long context, for the servers that
/// report it without a machine code.
fn is_context_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("context length")
        || lower.contains("context window")
        || lower.contains("maximum context")
        || lower.contains("too many tokens")
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
            assert_eq!(path, COMPLETIONS_PATH);
            assert!(
                headers
                    .iter()
                    .any(|(k, v)| k == "authorization" && v.starts_with("Bearer "))
            );
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
    fn base_urls_join_with_the_path_without_doubling_the_version() {
        // The path owns `/v1`; a base URL that also ended in it would produce
        // `/v1/v1/chat/completions`. Guard the one join easy to get wrong.
        for base in [
            base_url::OPENAI,
            base_url::XAI,
            base_url::OPENROUTER,
            base_url::TOGETHER,
            base_url::GROQ,
        ] {
            assert!(!base.ends_with("/v1"), "{base} must not carry the path");
            assert!(!base.ends_with('/'), "{base} must not end in a slash");
            assert!(format!("{base}{COMPLETIONS_PATH}").ends_with("/v1/chat/completions"));
        }
    }

    #[test]
    fn the_request_encoding_matches_the_chat_completions_api() {
        let body = encode_request(&request());
        assert_eq!(body["model"], "claude-sonnet-4-6");
        assert_eq!(body["max_tokens"], 1234);
        let messages = body["messages"].as_array().expect("messages");
        // system → user(hello) → assistant(text + tool_calls) →
        // tool(result) → user(reset notice): one message per entry, the system
        // prompt lifted to the front.
        assert_eq!(messages.len(), 5);
        assert_eq!(messages[0]["role"], "system");
        assert_eq!(messages[0]["content"], "be terse");
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(messages[2]["role"], "assistant");
        assert_eq!(messages[2]["tool_calls"][0]["id"], "tu_1");
        assert_eq!(messages[2]["tool_calls"][0]["type"], "function");
        assert_eq!(messages[2]["tool_calls"][0]["function"]["name"], "shell");
        // Arguments are a JSON string, not an object.
        assert_eq!(
            messages[2]["tool_calls"][0]["function"]["arguments"],
            "{\"cmd\":\"ls\"}"
        );
        assert_eq!(messages[3]["role"], "tool");
        assert_eq!(messages[3]["tool_call_id"], "tu_1");
        assert_eq!(messages[3]["content"], "[error] tool call timed out");
        assert_eq!(messages[4]["role"], "user");
        assert_eq!(body["tools"][0]["type"], "function");
        assert_eq!(body["tools"][0]["function"]["name"], "shell");
        assert_eq!(body["tools"][0]["function"]["parameters"]["type"], "object");
    }

    #[test]
    fn a_successful_response_decodes_content_calls_and_usage() {
        let sim = Simulation::new(1);
        let http = ScriptedHttp::new(vec![ok_body(json!({
            "choices": [{
                "message": {
                    "content": "running it",
                    "tool_calls": [{
                        "id": "tu_9",
                        "type": "function",
                        "function": {"name": "shell", "arguments": "{\"cmd\":\"ls\"}"}
                    }]
                }
            }],
            "usage": {"prompt_tokens": 50, "completion_tokens": 9}
        }))]);
        let model = OpenAiModel::new(http, "sk-test", sim.clock(), Arc::new(SimEntropy::new(1)));
        let response = sim.block_on(model.complete(request())).expect("completes");
        assert_eq!(response.content, "running it");
        assert_eq!(response.calls.len(), 1);
        assert_eq!(response.calls[0].id, CallId::new("tu_9"));
        // The arguments string decodes back to structured input.
        assert_eq!(response.calls[0].input, json!({"cmd": "ls"}));
        assert_eq!(response.usage.total(), 59);
    }

    #[test]
    fn a_tool_only_turn_decodes_with_empty_content() {
        let sim = Simulation::new(2);
        let http = ScriptedHttp::new(vec![ok_body(json!({
            "choices": [{
                "message": {
                    "content": null,
                    "tool_calls": [{
                        "id": "tu_3",
                        "type": "function",
                        "function": {"name": "shell", "arguments": "{}"}
                    }]
                }
            }],
            "usage": {"prompt_tokens": 5, "completion_tokens": 2}
        }))]);
        let model = OpenAiModel::new(http, "sk-test", sim.clock(), Arc::new(SimEntropy::new(2)));
        let response = sim.block_on(model.complete(request())).expect("completes");
        assert_eq!(response.content, "");
        assert_eq!(response.calls.len(), 1);
        assert_eq!(response.calls[0].input, json!({}));
    }

    #[test]
    fn pressure_is_retried_with_backoff_and_a_hard_error_is_not() {
        let sim = Simulation::new(7);
        // 429, then 503, then success: absorbed by the policy (§4.3).
        let http = ScriptedHttp::new(vec![
            Ok(HttpResponse {
                status: 429,
                body: b"{}".to_vec(),
            }),
            Ok(HttpResponse {
                status: 503,
                body: b"{}".to_vec(),
            }),
            ok_body(json!({
                "choices": [{"message": {"content": "ok"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1}
            })),
        ]);
        let model = OpenAiModel::new(
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
        let model = OpenAiModel::new(
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
        // By machine code.
        let sim = Simulation::new(9);
        let http = ScriptedHttp::new(vec![Ok(HttpResponse {
            status: 400,
            body: serde_json::to_vec(&json!({
                "error": {
                    "type": "invalid_request_error",
                    "code": "context_length_exceeded",
                    "message": "This model's maximum context length is 128000 tokens"
                }
            }))
            .expect("encode"),
        })]);
        let model = OpenAiModel::new(http, "sk-test", sim.clock(), Arc::new(SimEntropy::new(9)));
        assert_eq!(
            sim.block_on(model.complete(request())),
            Err(ModelError::ContextOverflow)
        );

        // By message, for a server that omits the code.
        let sim = Simulation::new(10);
        let http = ScriptedHttp::new(vec![Ok(HttpResponse {
            status: 400,
            body: serde_json::to_vec(&json!({
                "error": {"message": "requested tokens exceed the context window"}
            }))
            .expect("encode"),
        })]);
        let model = OpenAiModel::new(http, "sk-test", sim.clock(), Arc::new(SimEntropy::new(10)));
        assert_eq!(
            sim.block_on(model.complete(request())),
            Err(ModelError::ContextOverflow)
        );
    }
}
