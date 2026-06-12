//! The tiered sandbox provider (sandbox-spec.md): the first production-shaped
//! implementation of the harness's sandbox seam, offering each tier under the
//! per-tier obligations of sandbox spec §3.
//!
//! What this provider offers, and how (the reference realization, sandbox
//! spec §3.5):
//!
//! - **`Workspace`** (default feature `workspace`): the session's filesystem
//!   behind a cap-std [`Dir`](cap_std::fs::Dir) capability handle — a path
//!   outside the workspace is *unrepresentable*, never merely rejected
//!   (sandbox spec §3.1, S1). The typed tools are exported as ready-made
//!   declarations by [`workspace_tools`].
//! - **`Compute`** (feature `compute`): hermetic wasmtime guests over the
//!   same workspace handle — no ambient clock, entropy, network, or OS
//!   identity; every capability enters through a host function this crate
//!   chose to expose, and outputs are a function of the call, the workspace,
//!   and the injected seed (sandbox spec §3.2, S2). The model delivers a
//!   guest as a `.wasm` module (`run_module`), or, with the `quickjs`
//!   feature, as JavaScript (`run_js`) the embedded QuickJS runner executes.
//! - **JavaScript** (feature `quickjs`): [`run_js_tool`] plus
//!   [`TieredSandboxes::with_quickjs`] register a hermetic QuickJS
//!   interpreter (the committed `guest/qjs-runner` artifact) under the
//!   reserved module name. The JS environment is deterministic — seeded
//!   `Math.random`, frozen `Date`, workspace I/O only — so a script's output
//!   is a function of the call, the workspace, and the seed, exactly as a
//!   raw module's is.
//! - **`Network` and `Native`**: not offered. A call carrying either tier
//!   fails as a `ToolError` outcome the model reacts to (harness spec §5.4)
//!   — and the registration-time cap check makes such a call unreachable in
//!   a correctly configured deployment (harness spec §5.3 item 4). The
//!   standalone deployment's `LocalSandboxes` remains the degenerate
//!   `Native`-only provider (sandbox spec §5).
//!
//! The S-catalogue (sandbox spec §6) is machine-readable beside this crate's
//! conformance suite, in `tests/support/mod.rs`, guarded by the same drift
//! test pattern as its siblings.
//!
//! **Execution is synchronous inside the returned future.** Workspace tools
//! are small, capped operations; a compute guest is bounded by fuel, not by
//! the harness's per-tool timeout — the timeout bounds the *outcome*, and a
//! fuel-exhausted guest traps deterministically (sandbox spec §3.2). Async
//! fuel-yield and epoch interruption are the production-hardening follow-up
//! (sandbox spec §7): epoch ticking needs an ambient timer thread, which the
//! determinism contract forbids under simulation (core spec §18.1).

#[cfg(feature = "compute")]
mod compute;
#[cfg(feature = "workspace")]
mod ids;
#[cfg(feature = "workspace")]
mod provider;
#[cfg(feature = "workspace")]
mod workspace;

#[cfg(feature = "compute")]
pub use compute::run_js_tool;
#[cfg(feature = "compute")]
pub use compute::run_module_tool;
#[cfg(feature = "workspace")]
pub use provider::TierStats;
#[cfg(feature = "workspace")]
pub use provider::TieredSandboxes;
#[cfg(feature = "quickjs")]
pub use provider::quickjs_module;
#[cfg(feature = "workspace")]
pub use workspace::workspace_tools;
