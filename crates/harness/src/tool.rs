//! Tool declarations and the registry-as-allowlist (harness spec §5.2).
//!
//! Every tool is **declared**: the model and the loop need its interface
//! regardless of where it executes. A kind's [`ToolRegistry`] is a hand-built
//! list in the spirit of `HandlerRegistry` (core spec §4.4): explicit,
//! inspectable, and the allowlist — a model's tool call dispatches by name
//! against it and nothing else, so no path leads from model output to code
//! outside the declared set. An unknown name or schema-rejected input is a
//! synthesized [`ToolError`] outcome (§5.4), not a protocol failure: nothing
//! was executed.
//!
//! Every declared tool is sandboxed (§5.3). The single built-in exception is
//! [`DELEGATE`] (§8): a delegation is control flow — a child `Submit`,
//! confined to the seams — not an effect, so it executes in the loop. v1
//! deliberately exposes no extension point for further loop-executing tools
//! (§13).

use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::budget::Budget;
use crate::model::ToolSpec;
use crate::sandbox::Tier;
use crate::session::RunError;

/// The one built-in, loop-executing tool (harness spec §8.1): present in a
/// kind's registry iff the kind permits sub-agents.
pub const DELEGATE: &str = "delegate";

/// Resume's policy for a dangling call: intent journaled, outcome not
/// (harness spec §5.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnDangling {
    /// Blind re-execution is safe: the call is idempotent, or dedups
    /// (`delegate`, §8.1).
    Reexecute,
    /// Resolve as [`ToolError::Interrupted`]; the model decides whether to
    /// retry the side effect.
    Interrupt,
}

/// One declared tool (harness spec §5.2).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolDecl {
    /// Stable, author-chosen; the model selects by it (cf. manifests, core
    /// spec §4.4).
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    /// The capability set the call requires (§5.6): checked against the
    /// kind's tier cap at registration (§5.3 item 4), passed to
    /// `Sandbox::call`, and covered by the kind digest (§7.1).
    #[serde(default)]
    pub tier: Tier,
    /// The declared recovery policy for a dangling call (§5.5).
    pub on_dangling: OnDangling,
    /// Per-call execution bound, timed by `Clock` (§5.3 item 3); `None` uses
    /// the harness default.
    pub timeout: Option<Duration>,
}

impl ToolDecl {
    /// The interface half the model sees (§4.1).
    pub fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name.clone(),
            description: self.description.clone(),
            input_schema: self.input_schema.clone(),
        }
    }
}

/// A kind's hand-built tool list — the allowlist (harness spec §5.2).
#[derive(Clone, Debug, Default)]
pub struct ToolRegistry {
    decls: Vec<ToolDecl>,
}

impl ToolRegistry {
    pub fn new() -> ToolRegistry {
        ToolRegistry::default()
    }

    /// Declare a tool. Panics on a duplicate name: the registry is built once
    /// at deployment configuration time, where a collision is a bug to surface
    /// loudly, not handle.
    pub fn declare(&mut self, decl: ToolDecl) {
        assert!(
            self.get(&decl.name).is_none(),
            "duplicate tool declaration: {}",
            decl.name
        );
        self.decls.push(decl);
    }

    /// The declaration for `name`, or `None` — the allowlist check (§5.2).
    pub fn get(&self, name: &str) -> Option<&ToolDecl> {
        self.decls.iter().find(|d| d.name == name)
    }

    /// The declarations, in declaration order.
    pub fn iter(&self) -> impl Iterator<Item = &ToolDecl> {
        self.decls.iter()
    }

    /// The interface halves the model sees, in declaration order (§4.1).
    pub fn specs(&self) -> Vec<ToolSpec> {
        self.decls.iter().map(ToolDecl::spec).collect()
    }
}

/// The input of the built-in `delegate` tool (harness spec §8.1): a child
/// kind — which must belong to the parent kind's delegation allowlist (§7.1)
/// — a prompt, and optionally a budget request carved from the parent's
/// remainder (§9.1).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DelegateInput {
    pub kind: String,
    pub prompt: String,
    #[serde(default)]
    pub budget: Option<Budget>,
}

/// A tool call's failure, journaled as the call's outcome and returned to the
/// model as the tool result (harness spec §5.4): a failing tool never fails
/// the run — the only abnormal run endings are the four of §3.1.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ToolError {
    /// The per-tool timeout elapsed (§5.3 item 3). The call's effects may
    /// still land; only its outcome is bounded.
    Timeout,
    /// A dangling call declared [`OnDangling::Interrupt`] was resolved on
    /// resume (§5.5); the model decides whether to retry the side effect.
    Interrupted,
    /// The model named a tool outside the registry — synthesized, nothing was
    /// executed (§5.4).
    UnknownTool { name: String },
    /// The model's arguments failed the declared schema — synthesized,
    /// nothing was executed (§5.4).
    InvalidArguments(String),
    /// The sandbox failed the call (open failure, or a sandbox-side crash).
    Sandbox(String),
    /// The environment itself is gone: the provider lost the workspace
    /// mid-call. The agent releases the binding and journals a
    /// `WorkspaceReset` before the next model call (§5.5).
    EnvironmentLost(String),
    /// A delegated child run ended in failure; the child's terminal error,
    /// for the parent's model to react to (§8.2).
    Delegation(RunError),
}

impl std::fmt::Display for ToolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ToolError::Timeout => f.write_str("tool call timed out"),
            ToolError::Interrupted => f.write_str("tool call interrupted by a resume"),
            ToolError::UnknownTool { name } => write!(f, "unknown tool: {name}"),
            ToolError::InvalidArguments(e) => write!(f, "invalid arguments: {e}"),
            ToolError::Sandbox(e) => write!(f, "sandbox failure: {e}"),
            ToolError::EnvironmentLost(e) => write!(f, "environment lost: {e}"),
            ToolError::Delegation(e) => write!(f, "delegated run failed: {e:?}"),
        }
    }
}
