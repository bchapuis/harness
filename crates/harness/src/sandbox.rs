//! The sandbox seam (harness spec §5.3): isolated execution environments.
//!
//! The third harness seam, and the isolation boundary of §5.1: every declared
//! tool except the built-in `delegate` executes inside its session's sandbox
//! and nowhere else. The spec mandates *that* effects run behind this seam,
//! not *how*: process, container, or microVM is the provider's secret (§1.1);
//! the simulator's scripted sandbox is one more implementation of the same
//! trait (§12.1).
//!
//! A sandbox is **working state, not session state** (§5.5): the fold never
//! reads it, no record depends on its contents, and losing it never loses the
//! session — the loss surfaces to the model as a journaled `WorkspaceReset`,
//! never as silent corruption (invariant H8).

use std::sync::Arc;

use actor_core::BoxFuture;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::session::SessionId;
use crate::tool::ToolError;

/// A kind's sandbox configuration (harness spec §5.3 item 4): deployment
/// configuration agreed cluster-wide like the kind itself (§7.1). What an
/// `image` means is the provider's business.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxProfile {
    pub image: String,
}

impl SandboxProfile {
    /// A profile naming a provider-interpreted environment image.
    pub fn image(image: impl Into<String>) -> SandboxProfile {
        SandboxProfile {
            image: image.into(),
        }
    }
}

/// A failure to provision an environment (harness spec §5.3). Surfaces to the
/// model as `ToolError`s on the calls that needed the sandbox (§5.4), never
/// as a run failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxError(pub String);

impl std::fmt::Display for SandboxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sandbox open failed: {}", self.0)
    }
}

/// One environment, bound to one session activation (harness spec §5.3).
///
/// Object-safe (`BoxFuture` rather than `async fn`) so the agent can hold it
/// as `Arc<dyn Sandbox>` behind the seam.
pub trait Sandbox: Send + Sync + 'static {
    /// Execute one declared, sandboxed tool call to completion inside the
    /// environment.
    fn call(&self, name: &str, input: Value) -> BoxFuture<'static, Result<Value, ToolError>>;

    /// Tear down processes and working state. Idempotent.
    fn release(&self) -> BoxFuture<'static, ()>;
}

/// Provisioning of isolated execution environments (harness spec §5.3). One
/// sandbox per session activation, opened lazily on the first sandboxed call.
pub trait SandboxProvider: Send + Sync + 'static {
    fn open(
        &self,
        session: &SessionId,
        profile: &SandboxProfile,
    ) -> BoxFuture<'static, Result<Arc<dyn Sandbox>, SandboxError>>;
}
