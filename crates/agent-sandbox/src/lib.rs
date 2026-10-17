//! The tiered sandbox provider: [`TieredSandboxes`] implements the agent
//! runtime's [`SandboxProvider`](agent::SandboxProvider) seam, one environment
//! per session activation, each tier under the per-tier obligations of sandbox
//! spec §4. Specified by `docs/sandbox-spec.md`.
//!
//! Its two seams are the provider itself and the toolset it answers, published
//! as [`TieredSandboxes::tools`] for a deployment to register on its kinds
//! verbatim, so a kind declares exactly the names and tiers this provider
//! executes. A name no tier provides is answered as a `ToolError` outcome,
//! with nothing executed.
//!
//! What this provider offers (sandbox spec §4):
//!
//! - **`Workspace`** (default feature `workspace`): the session's filesystem
//!   behind a cap-std [`Dir`](cap_std::fs::Dir) capability handle; a path
//!   outside the workspace is *unrepresentable*, never merely rejected
//!   (sandbox spec §4.1, S1). Declared by [`workspace_tools`](crate::workspace::workspace_tools).
//! - **`Compute`** (feature `compute`): hermetic wasmtime guests over the
//!   same workspace handle, no ambient clock, entropy, network, or OS
//!   identity; every capability enters through a host function this crate
//!   chose to expose, and outputs are a function of the call, the workspace,
//!   and the injected seed (sandbox spec §4.2, S2). A guest arrives as a
//!   `.wasm` module (`run_module`), or, with the `quickjs` feature, as
//!   JavaScript (`run_js`).
//! - **JavaScript** (feature `quickjs`): [`run_js_tool`](crate::compute::run_js_tool) plus
//!   [`TieredSandboxes::with_quickjs`] register the committed
//!   `guest/qjs-runner` interpreter under the reserved module name. The JS
//!   environment is deterministic: seeded `Math.random`, frozen `Date`,
//!   workspace I/O only.
//! - **`Native`** (feature `native`): OS processes inside an OCI container
//!   driven through the `docker` CLI; the session workspace bind-mounted at
//!   `/workspace`, `--network none`, one container per activation,
//!   provisioned lazily on the first `Native` call and removed on release.
//!   This is shared-kernel confinement, sandbox spec §4.4's SHOULD grade and
//!   §4's development fallback, not the microVM grade. Configured per
//!   provider by [`TieredSandboxes::with_container_cli`]; a provider that
//!   names neither a container CLI nor a Firecracker profile has no `Native`
//!   tier, and declares no `shell`. Native calls require a tokio runtime
//!   (`tokio::process`); the other tiers stay runtime-agnostic.
//! - **`Native`, microVM grade** (feature `firecracker`): the same tier
//!   behind a Firecracker VM instead; §4's reference choice. Configured
//!   per provider by [`TieredSandboxes::with_firecracker`]; one VM per
//!   activation, booted lazily, killed on release; the workspace travels as
//!   a capped tar stream over vsock around each call (push → exec → pull,
//!   all through the same cap-std handle). Its `shell` declaration differs
//!   from the docker one because the sync semantics are model-visible.
//!   Builds everywhere; *runs* on Linux with `/dev/kvm`,
//!   against the assets `guest/fc-rootfs/build.sh` produces.
//! - **`Network`**: not offered. A call carrying it fails as a `ToolError`
//!   outcome the model reacts to (agent spec §4.2), and a profile that names
//!   egress is refused at [`open`](agent::SandboxProvider::open), whose doc on
//!   [`TieredSandboxes`] states the rule.
//!
//! The S-catalogue (sandbox spec §6) is machine-readable beside this crate's
//! conformance suite, in `tests/support/mod.rs`.
//!
//! **Execution is synchronous inside the returned future.** Workspace tools
//! are small, capped operations; a compute guest is bounded by fuel, not by
//! the agent runtime's per-tool timeout: the timeout bounds the *outcome*, and a
//! fuel-exhausted guest traps deterministically (sandbox spec §4.2). Not
//! implemented: async fuel-yield and epoch interruption (sandbox spec §7),
//! whose epoch ticking needs an ambient timer thread the determinism
//! contract forbids under simulation (core spec §2.12.2).

// The docs above link each feature's surface, which resolves only with that
// feature on. CI checks them with every feature, so a narrower build warns.
#![cfg_attr(
    not(all(feature = "quickjs", feature = "firecracker")),
    warn(rustdoc::broken_intra_doc_links)
)]

#[cfg(feature = "compute")]
mod compute;
#[cfg(feature = "firecracker")]
mod firecracker;
#[cfg(feature = "native")]
mod native;
#[cfg(feature = "workspace")]
mod provider;
#[cfg(feature = "workspace")]
mod workspace;

#[cfg(feature = "firecracker")]
pub use firecracker::FirecrackerConfig;
#[cfg(feature = "workspace")]
pub use provider::TierStats;
#[cfg(feature = "workspace")]
pub use provider::TieredSandboxes;
#[cfg(feature = "quickjs")]
pub use provider::quickjs_module;

/// FNV-1a 64 over bytes: the compute module cache key, the per-session guest
/// seed, and the digest in a session's container name and VM control
/// directory. Seeds and names are observable outside the process (a guest's
/// output, a container's name), so the function must stay stable. Fast, not
/// collision-resistant: no use is a security boundary.
#[cfg(feature = "workspace")]
pub(crate) fn fnv64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
