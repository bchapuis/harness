//! The production [`Model`] seam (agent spec §4, §12.1): the Anthropic
//! Messages API client.
//!
//! This crate is the single place the Messages API protocol exists: the
//! request and response JSON, the transcript encoding, and the error
//! taxonomy. Everything that is not protocol (the socket, the key, the
//! `Clock`/`Entropy` seams, the bounded retry engine, §4.2 rule 1) is the
//! shared [`ProtocolModel`] shell from `agent::http`, held there once for
//! every sibling adapter.
//!
//! The socket is a seam: [`HttpPost`] is the one operation the client needs
//! from its host, POST these bytes to this path with these headers (the same
//! move the core makes with `Transport`, core spec §7), supplied once by the
//! deployment over its HTTP stack of choice. The simulator's scripted model
//! replaces this crate entirely (§12.1).

use agent::Entry;
use agent::ModelError;
use agent::ModelRequest;
use agent::ModelResponse;
use agent::ToolCall;
use agent::Usage;
use agent::WORKSPACE_RESET_NOTICE;
use agent::http::HttpResponse;
use agent::http::Protocol;
use agent::http::ProtocolModel;
use agent::http::tool_result_text;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;

/// The API path every completion posts to.
const MESSAGES_PATH: &str = "/v1/messages";
/// The API version header this client speaks.
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// The Messages API protocol: the wire-specific four this crate owns.
pub struct Messages;

impl Protocol for Messages {
    const PATH: &'static str = MESSAGES_PATH;

    fn headers(api_key: &str) -> Vec<(String, String)> {
        vec![
            ("x-api-key".to_string(), api_key.to_string()),
            (
                "anthropic-version".to_string(),
                ANTHROPIC_VERSION.to_string(),
            ),
            ("content-type".to_string(), "application/json".to_string()),
        ]
    }

    fn encode(req: &ModelRequest) -> Value {
        encode_request(req)
    }

    fn classify(response: &HttpResponse) -> Result<ModelResponse, ModelError> {
        classify(response)
    }
}

/// The Anthropic Messages API model (agent spec §4.1): the shared shell
/// bound to this crate's protocol.
pub type AnthropicModel<C, E> = ProtocolModel<Messages, C, E>;

// ---------------------------------------------------------------------------
// Request encoding (agent Entry → Messages API)
// ---------------------------------------------------------------------------

/// Encode a [`ModelRequest`] as a Messages API request body. The encoding is
/// part of this crate's contract, pinned by the golden tests below.
fn encode_request(req: &ModelRequest) -> Value {
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
                    Ok(value) => (tool_result_text(value), false),
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
            // enters as input content the agent runtime authors; the encoding's
            // analogue of a user message.
            Entry::WorkspaceReset => push(
                "user",
                json!({"type": "text", "text": WORKSPACE_RESET_NOTICE}),
            ),
        }
    }
    let messages: Vec<Value> = messages
        .into_iter()
        .map(|(role, content)| json!({"role": role, "content": content}))
        .collect();

    let mut body = json!({
        "model": req.params.model,
        "max_tokens": req.params.max_tokens,
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

/// Map an HTTP outcome onto the agent runtime's [`ModelError`] taxonomy (§4.3).
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
                    id: agent::CallId::new(id),
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
        // The API reports an over-long prompt as an invalid request; the agent
        // runtime distinguishes it because the run must fail explicitly on it
        // (§1.1, §4.3).
        (400, _) if detail.message.contains("prompt is too long") => ModelError::ContextOverflow,
        (400, _) | (_, "invalid_request_error") => ModelError::InvalidRequest(detail.message),
        _ => ModelError::Api(format!("{}: {}", detail.kind, detail.message)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Mutex;

    use actor_simulation::SimEntropy;
    use actor_simulation::Simulation;
    use agent::BoxFuture;
    use agent::CallId;
    use agent::Model;
    use agent::ModelParams;
    use agent::ToolError;
    use agent::ToolSpec;
    use agent::http::HttpError;
    use agent::http::HttpPost;

    /// A scripted transport: pops the next canned reply per call, asserting the
    /// protocol's path and key header on the way.
    struct ScriptedHttp {
        replies: Mutex<Vec<Result<HttpResponse, HttpError>>>,
    }

    impl ScriptedHttp {
        fn new(mut replies: Vec<Result<HttpResponse, HttpError>>) -> Arc<Self> {
            replies.reverse();
            Arc::new(ScriptedHttp {
                replies: Mutex::new(replies),
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
            serde_json::from_slice::<Value>(&body).expect("json body");
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
            params: ModelParams {
                model: "claude-sonnet-4-6".to_string(),
                max_tokens: 1234,
            },
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
