//! Shared fixtures for granary's test binaries (docs/simulation-testing.md).
//!
//! A fixture lives here when more than one suite in this crate drives it, so a
//! sweep covers the same grain its scenarios specify. Each fixture is its own
//! module; `mod.rs` only wires them up.
//!
//! - [`counter`] — the event-sourced `CounterGrain`, shared by `grains.rs`
//!   (scenarios) and `grain_swarm.rs` (sweeps).
//! - [`ledger`] — the SQL-facet `Ledger`, shared by `sql.rs` and `sql_swarm.rs`.
//!   Gated on the `sql` feature, like both suites that use it.
//! - [`exercised`] — coverage accounting for hibernation and process restarts,
//!   shared by every `*_swarm.rs` with a hibernating variant.
//! - [`log`] — the appendable `LogGrain` and the reference subscription
//!   reconciler, shared by `subscription_faults.rs` (scripted §14 cases) and
//!   `subscription_swarm.rs` (the same object under the nemesis).
//! - [`timer`] — the alarm-bearing `Timer`, generic over the hosting system, so
//!   `alarm_index.rs` drives it on the `Local` tier and `alarm_cluster.rs` and
//!   `alarm_loss.rs` drive the same grain on the clustered one.
//! - [`pipeline`] — the workflow-facet `Pipeline`, likewise generic over the
//!   system, shared by `workflow.rs` (scenarios) and `workflow_swarm.rs` (the
//!   sweep). Its shape varies by `PipelineConfig` rather than by a second grain,
//!   so the sweep asserts against the object the scenarios specify.
//!
//! `counter` is re-exported flat; `ledger` is not. Both define an `Add`, so two
//! globs would make the name ambiguous — and because `ledger` is feature-gated,
//! that break only appeared once `--features sql` was on, which nothing in CI or
//! `soak.yml` turns on. The `sql` suites name their module instead.

// Every test binary compiles this module in full but uses only the fixtures it
// drives, so unused items and re-exports here are the normal case, not a smell.
#![allow(dead_code, unused_imports)]

pub mod catalogue;

pub mod counter;
pub use counter::*;

pub mod exercised;
pub use exercised::Exercised;

// Not re-exported flat: `log` defines an `Append`, as `ledger` does, and the
// suites that use it name the module.
pub mod log;

pub mod timer;

// Not re-exported flat: `pipeline` defines a `Read`, and so would any other
// fixture with a query command.
pub mod pipeline;

#[cfg(feature = "sql")]
pub mod ledger;
