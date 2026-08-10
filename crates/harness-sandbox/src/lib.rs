//! The tiered sandbox provider (sandbox-spec.md): each tier under the
//! per-tier obligations of sandbox spec §3.
//!
//! What this provider offers (sandbox spec §3.5):
//!
//! - **`Workspace`** (default feature `workspace`): the session's filesystem
//!   behind a cap-std [`Dir`](cap_std::fs::Dir) capability handle — a path
//!   outside the workspace is *unrepresentable*, never merely rejected
//!   (sandbox spec §3.1, S1). Declared by [`workspace_tools`].
//! - **`Compute`** (feature `compute`): hermetic wasmtime guests over the
//!   same workspace handle — no ambient clock, entropy, network, or OS
//!   identity; every capability enters through a host function this crate
//!   chose to expose, and outputs are a function of the call, the workspace,
//!   and the injected seed (sandbox spec §3.2, S2). A guest arrives as a
//!   `.wasm` module (`run_module`), or, with the `quickjs` feature, as
//!   JavaScript (`run_js`).
//! - **JavaScript** (feature `quickjs`): [`run_js_tool`] plus
//!   [`TieredSandboxes::with_quickjs`] register the committed
//!   `guest/qjs-runner` interpreter under the reserved module name. The JS
//!   environment is deterministic: seeded `Math.random`, frozen `Date`,
//!   workspace I/O only.
//! - **`Native`** (feature `native`): OS processes inside an OCI container
//!   driven through the `docker` CLI — the session workspace bind-mounted at
//!   `/workspace`, `--network none`, one container per activation,
//!   provisioned lazily on the first `Native` call and removed on release.
//!   This is shared-kernel confinement, sandbox spec §3.4's SHOULD grade and
//!   §3.5's development fallback, not the microVM grade. Declared by
//!   [`shell_tool`]. Native calls require a tokio runtime
//!   (`tokio::process`); the other tiers stay runtime-agnostic.
//! - **`Native`, microVM grade** (feature `firecracker`): the same tier
//!   behind a Firecracker VM instead — §3.5's reference choice. Configured
//!   per provider by [`TieredSandboxes::with_firecracker`]; one VM per
//!   activation, booted lazily, killed on release; the workspace travels as
//!   a capped tar stream over vsock around each call (push → exec → pull,
//!   all through the same cap-std handle). Declared by [`fc_shell_tool`] —
//!   distinct from the docker declaration because the sync semantics are
//!   model-visible. Builds everywhere; *runs* on Linux with `/dev/kvm`,
//!   against the assets `guest/fc-rootfs/build.sh` produces.
//! - **`Network`**: not offered. A call carrying it fails as a `ToolError`
//!   outcome the model reacts to (harness spec §5.4), and the
//!   registration-time cap check makes such a call unreachable in a
//!   correctly configured deployment (harness spec §5.3 item 4). With
//!   `native` enabled, a profile that *names* egress (or caps in `Network`)
//!   fails at `open` instead: holding `Native` implies `Network`'s grants
//!   (sandbox spec §2.2), and `--network none` delivers exactly an empty
//!   allowlist.
//!
//! The S-catalogue (sandbox spec §6) is machine-readable beside this crate's
//! conformance suite, in `tests/support/mod.rs`.
//!
//! **Execution is synchronous inside the returned future.** Workspace tools
//! are small, capped operations; a compute guest is bounded by fuel, not by
//! the harness's per-tool timeout — the timeout bounds the *outcome*, and a
//! fuel-exhausted guest traps deterministically (sandbox spec §3.2). Not
//! implemented: async fuel-yield and epoch interruption (sandbox spec §7),
//! whose epoch ticking needs an ambient timer thread the determinism
//! contract forbids under simulation (core spec §18.1).

#[cfg(feature = "compute")]
mod compute;
#[cfg(feature = "firecracker")]
mod firecracker;
// Gated on `native`, not `workspace`: the container's name is the only thing that
// needs a filesystem-safe session id, and `native` implies `workspace` anyway. On
// the default feature set the wider gate compiled the module with nothing calling
// it, which `-D warnings` reads as dead code — true, and only on the builds no CI
// job runs.
#[cfg(feature = "native")]
mod ids;
#[cfg(feature = "native")]
mod native;
#[cfg(feature = "workspace")]
mod provider;
#[cfg(feature = "workspace")]
mod workspace;

#[cfg(feature = "compute")]
pub use compute::run_js_tool;
#[cfg(feature = "compute")]
pub use compute::run_module_tool;
#[cfg(feature = "firecracker")]
pub use firecracker::FirecrackerConfig;
#[cfg(feature = "firecracker")]
pub use firecracker::fc_shell_tool;
#[cfg(feature = "native")]
pub use native::shell_tool;
#[cfg(feature = "workspace")]
pub use provider::TierStats;
#[cfg(feature = "workspace")]
pub use provider::TieredSandboxes;
#[cfg(feature = "quickjs")]
pub use provider::quickjs_module;
#[cfg(feature = "workspace")]
pub use workspace::workspace_tools;
