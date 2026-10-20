//! The sandbox seam (agent spec §3.3): isolated execution environments.
//!
//! The isolation boundary of §5.1: every declared tool except the built-in
//! `delegate` executes inside its session's sandbox and nowhere else. The spec
//! mandates *that* effects run behind this seam, not *how*: process, container,
//! or microVM is the provider's secret (§1.1).
//!
//! A sandbox is **working state, not session state** (§5.4): the fold never
//! reads it, no record depends on its contents, and losing it never loses the
//! session; the loss surfaces to the model as a journaled `WorkspaceReset`,
//! never as silent corruption (invariant A7).

use std::collections::BTreeSet;
use std::sync::Arc;

use actor_core::BoxFuture;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::session::SessionId;
use crate::tool::ToolError;

/// The capability set a tool call requires (agent spec §6.1, §5.5; sandbox
/// spec §4). Each tier grants one additional capability over the workspace
/// and withholds the rest.
///
/// `Ord` exists only so tiers can live in a `BTreeSet` (the cap) and iterate
/// in a canonical order (the digest, §6.2). The derived order happens to match
/// today's inclusion chain, but the cap is a *set*, never a maximum (sandbox
/// spec §4.2): no grant decision may compare tiers with `<` or `max`.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub enum Tier {
    /// The session's scoped filesystem, through host-implemented typed tools;
    /// no guest code, no network, no ambient clock or entropy (sandbox §5.1).
    #[default]
    Workspace,
    /// Arbitrary guest code over the workspace; no network, no ambient
    /// clock, entropy, or OS identity (sandbox §5.2).
    Compute,
    /// Compute, plus egress to the profile's allowlist (sandbox §5.3).
    Network,
    /// OS processes and native binaries inside the confined environment
    /// (sandbox §5.4).
    Native,
}

/// The compute tier's resource limits (sandbox spec §5.2: REQUIRED; their
/// values are profile configuration). Digest-covered like the rest of the
/// profile (§6.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeLimits {
    /// Guest memory ceiling, in bytes.
    pub memory_bytes: u64,
    /// Deterministic CPU bound, in wasmtime's fuel vocabulary (sandbox §5).
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

/// A kind's sandbox configuration (agent spec §6.1): deployment
/// configuration agreed cluster-wide like the kind itself (§6.2). What an
/// `image` means is the provider's business.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxProfile {
    pub image: String,
    /// The kind's tier cap: the *set* of tiers its sessions may hold (§5.5;
    /// sandbox §4.2). `None` derives the spec default at registration:
    /// exactly the tiers the kind's declared tools require (§6.1).
    #[serde(default)]
    pub tier_cap: Option<BTreeSet<Tier>>,
    /// The egress allowlist the `Network` tier grants, by reference from a
    /// `TierAcquired` record, never an inline list (sandbox §5.3). Digest-
    /// covered (§6.2); meaningful only when the cap includes `Network`.
    #[serde(default)]
    pub egress: Vec<String>,
    /// Compute-tier resource limits (sandbox §5.2).
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

    /// Set an explicit tier cap (§6.1), overriding the derived
    /// default.
    pub fn cap(mut self, tiers: impl IntoIterator<Item = Tier>) -> SandboxProfile {
        self.tier_cap = Some(tiers.into_iter().collect());
        self
    }
}

/// A failure to provision an environment (agent spec §3.3). Surfaces to the
/// model as `ToolError`s on the calls that needed the sandbox (§5.2), never
/// as a run failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxError(pub String);

impl std::fmt::Display for SandboxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sandbox open failed: {}", self.0)
    }
}

/// One environment, bound to one session activation (agent spec §3.3).
///
/// Object-safe (`BoxFuture` rather than `async fn`) so the agent can hold it
/// as `Arc<dyn Sandbox>` behind the seam.
pub trait Sandbox: Send + Sync + 'static {
    /// Execute one declared, sandboxed tool call to completion inside the
    /// environment, at the call's declared tier (§6.1, §5.5). The agent runtime
    /// passes the tier: the seam hands the sandbox no registry, so the tier
    /// must travel with the call (§3.3), and only declared, cap-checked
    /// tiers are passed, so a
    /// conforming pair grants nothing the journal does not show (sandbox
    /// spec §4.3, S4).
    fn call(
        &self,
        tier: Tier,
        name: &str,
        input: Value,
    ) -> BoxFuture<'static, Result<Value, ToolError>>;

    /// Tear down processes and working state. Idempotent.
    fn release(&self) -> BoxFuture<'static, ()>;
}

/// Provisioning of isolated execution environments (agent spec §3.3). One
/// sandbox per session activation, opened lazily on the first sandboxed call.
pub trait SandboxProvider: Send + Sync + 'static {
    /// Open the session's environment over `workspace`: the session's durable
    /// workspace directory, owned by the caller (the agent's workspace facet,
    /// facets §5.1). The caller normally materializes it (durable content
    /// and all) before this call; a provider creates a missing directory
    /// rather than failing, and MUST NOT delete it on `release`: durability
    /// is the caller's property, captured after each tool call, never the
    /// provider's.
    fn open(
        &self,
        session: &SessionId,
        profile: &SandboxProfile,
        workspace: &std::path::Path,
    ) -> BoxFuture<'static, Result<Arc<dyn Sandbox>, SandboxError>>;
}
