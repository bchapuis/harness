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

use std::collections::BTreeSet;
use std::sync::Arc;

use actor_core::BoxFuture;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::session::SessionId;
use crate::tool::ToolError;

/// The capability set a tool call requires (harness spec §5.2, §5.6; sandbox
/// spec §2). Each tier grants one additional capability over the workspace
/// and withholds the rest.
///
/// `Ord` exists only so tiers can live in a `BTreeSet` (the cap) and iterate
/// in a canonical order (the digest, §7.1). The derived order happens to
/// match today's inclusion chain, but the cap is a *set*, never a maximum
/// (sandbox spec §2.2): no grant decision may compare tiers with `<` or
/// `max` — a future peripheral tier would sit beside the chain, not on it.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub enum Tier {
    /// The session's scoped filesystem, through host-implemented typed tools;
    /// no guest code, no network, no ambient clock or entropy (sandbox §3.1).
    #[default]
    Workspace,
    /// Arbitrary guest code over the workspace; no network, no ambient
    /// clock, entropy, or OS identity (sandbox §3.2).
    Compute,
    /// Compute, plus egress to the profile's allowlist (sandbox §3.3).
    Network,
    /// OS processes and native binaries inside the confined environment
    /// (sandbox §3.4).
    Native,
}

/// The compute tier's resource limits (sandbox spec §3.2: REQUIRED; their
/// values are profile configuration). Digest-covered like the rest of the
/// profile (§7.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeLimits {
    /// Guest memory ceiling, in bytes.
    pub memory_bytes: u64,
    /// Deterministic CPU bound, in wasmtime's fuel vocabulary (sandbox §3.5).
    pub fuel: u64,
}

impl Default for ComputeLimits {
    fn default() -> ComputeLimits {
        ComputeLimits {
            memory_bytes: 64 * 1024 * 1024,
            fuel: 1_000_000_000,
        }
    }
}

/// A kind's sandbox configuration (harness spec §5.3 item 4): deployment
/// configuration agreed cluster-wide like the kind itself (§7.1). What an
/// `image` means is the provider's business.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxProfile {
    pub image: String,
    /// The kind's tier cap: the *set* of tiers its sessions may hold (§5.6;
    /// sandbox §2.2). `None` derives the spec default at registration —
    /// exactly the tiers the kind's declared tools require (§5.3 item 4).
    #[serde(default)]
    pub tier_cap: Option<BTreeSet<Tier>>,
    /// The egress allowlist the `Network` tier grants — by reference from a
    /// `TierAcquired` record, never an inline list (sandbox §3.3). Digest-
    /// covered (§7.1); meaningful only when the cap includes `Network`.
    #[serde(default)]
    pub egress: Vec<String>,
    /// Compute-tier resource limits (sandbox §3.2).
    #[serde(default)]
    pub compute: ComputeLimits,
}

impl SandboxProfile {
    /// A profile naming a provider-interpreted environment image.
    pub fn image(image: impl Into<String>) -> SandboxProfile {
        SandboxProfile {
            image: image.into(),
            ..SandboxProfile::default()
        }
    }

    /// Set an explicit tier cap (§5.3 item 4), overriding the derived
    /// default.
    pub fn cap(mut self, tiers: impl IntoIterator<Item = Tier>) -> SandboxProfile {
        self.tier_cap = Some(tiers.into_iter().collect());
        self
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
    /// environment, at the call's declared tier (§5.2, §5.6). The harness
    /// passes the tier — the provider holds no registry and cannot derive it
    /// (§5.3 item 1) — and passes only declared, cap-checked tiers, so a
    /// conforming pair grants nothing the journal does not show (sandbox
    /// spec §2.3, S4).
    fn call(
        &self,
        tier: Tier,
        name: &str,
        input: Value,
    ) -> BoxFuture<'static, Result<Value, ToolError>>;

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
