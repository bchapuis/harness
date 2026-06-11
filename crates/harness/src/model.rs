//! The model seam (harness spec §4): one inference request, one response.
//!
//! The harness core depends only on the [`Model`] trait, exactly as it depends
//! on `Transport` (core spec §7): `harness-anthropic` implements it over the
//! Anthropic Messages API in production, and the simulator supplies a scripted
//! model — a deterministic function of the request and the run's seed (§12.2).
//! A completion request is side-effect-free, so an implementation MAY retry
//! internally with backoff from `Clock`/`Entropy` without violating the core
//! no-transparent-retry rule (core spec §1.2); a failure that survives the
//! policy ends the run as `RunError::Model` (§4.3), never silently.
//!
//! The trait returns a [`BoxFuture`] rather than using `async fn` so it stays
//! object-safe: the harness injects seams as `Arc<dyn Model>`, the same shape
//! the core gives its codec (core spec §5).

use actor_core::BoxFuture;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::budget::Usage;
use crate::session::CallId;
use crate::session::Entry;

/// Model parameters of a kind (harness spec §7.1): deployment configuration
/// the harness stores and transmits, not interprets.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelParams {
    /// The provider-side model identifier.
    pub model: String,
    /// The per-call output ceiling, before budget clamping (§9.1 item 2).
    pub max_tokens: u64,
}

impl Default for ModelParams {
    fn default() -> Self {
        ModelParams {
            model: "claude-sonnet-4-6".to_string(),
            max_tokens: 4_096,
        }
    }
}

/// One declared tool as the model sees it (harness spec §4.1): name,
/// description, input schema — the interface half of a [`ToolDecl`]
/// (§5.2), without the harness-side recovery policy.
///
/// [`ToolDecl`]: crate::tool::ToolDecl
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

/// One inference request (harness spec §4.1): the kind's system prompt and
/// tool declarations, the folded transcript, and the budget-clamped output
/// ceiling. A deterministic function of session state (§10.5), so the exact
/// request issued at any step is reconstructible from the journal prefix.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelRequest {
    pub system_prompt: String,
    pub params: ModelParams,
    pub tools: Vec<ToolSpec>,
    pub transcript: Vec<Entry>,
    /// `params.max_tokens` clamped to the run's remaining token budget
    /// (§9.1 item 2).
    pub max_tokens: u64,
}

/// One tool call the model requested (harness spec §5.2). The `id` is the
/// model API's tool-use id, or one the harness assigned on receipt; it is the
/// key dangling-call resolution (§5.5) and child derivation (§8.1) match by.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: CallId,
    pub name: String,
    pub input: Value,
}

/// One inference response (harness spec §4.1): assistant content, zero or
/// more requested tool calls, and the reported usage that feeds budget
/// accounting (§9.1).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelResponse {
    pub content: String,
    pub calls: Vec<ToolCall>,
    pub usage: Usage,
}

impl ModelResponse {
    /// Whether this is a final assistant message — no tool calls, so the run
    /// ends with it (harness spec §3.1 step 3).
    pub fn is_final(&self) -> bool {
        self.calls.is_empty()
    }
}

/// A model failure (harness spec §4.3). Serializable because an unabsorbed
/// failure travels inside the run's terminal outcome (`RunError::Model`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelError {
    RateLimited,
    Overloaded,
    /// The transcript exceeds the model's context window; fails the run
    /// explicitly — compaction is future work (§1.1, §13).
    ContextOverflow,
    InvalidRequest(String),
    Api(String),
}

impl std::fmt::Display for ModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModelError::RateLimited => f.write_str("rate limited"),
            ModelError::Overloaded => f.write_str("overloaded"),
            ModelError::ContextOverflow => f.write_str("context window exceeded"),
            ModelError::InvalidRequest(e) => write!(f, "invalid request: {e}"),
            ModelError::Api(e) => write!(f, "api failure: {e}"),
        }
    }
}

/// Inference: one request, one response; no streaming in v1 (harness spec
/// §13). The first harness seam.
pub trait Model: Send + Sync + 'static {
    fn complete(&self, req: ModelRequest) -> BoxFuture<'static, Result<ModelResponse, ModelError>>;
}
