//! Shared fixtures for entity's test binaries (docs/simulation-testing.md).
//!
//! A fixture lives here when more than one suite in this crate drives it, so a
//! sweep covers the same entity its scenarios specify.
//!
//! - [`counter`]: the event-sourced `CounterEntity`, shared by `entities.rs`
//!   (scenarios) and `entity_swarm.rs` (sweeps).
//! - [`ledger`]: the SQL-facet `Ledger`, shared by `sql.rs` and `sql_swarm.rs`.
//!   Gated on the `sql` feature, like both suites that use it.
//! - [`exercised`]: coverage accounting for hibernation and process restarts,
//!   shared by every `*_swarm.rs` with a hibernating variant.
//! - [`log`]: the appendable `LogEntity` and the reference subscription
//!   reconciler, shared by `subscription_faults.rs` (scripted §11 cases) and
//!   `subscription_swarm.rs` (the same object under the nemesis).
//! - [`timer`]: the alarm-bearing `Timer`, generic over the hosting system, so
//!   `alarm_index.rs` drives it on the `Local` tier and `alarm_cluster.rs` and
//!   `alarm_loss.rs` on the clustered one.
//! - [`pipeline`]: the steps-facet `Pipeline`, likewise generic over the
//!   system, shared by `steps.rs` (scenarios) and `steps_swarm.rs` (the sweep).
//!   Its shape varies by `PipelineConfig` rather than by a second entity.
//! - [`drive`]: the fixed-window call driver the clustered suites step their
//!   scenarios with.
//!
//! `counter` is re-exported flat; `ledger` is not. Both define an `Add`, so two
//! globs would make the name ambiguous, and since `ledger` is feature-gated the
//! break appeared only under `--features sql`, which neither CI nor `soak.yml`
//! turns on. The `sql` suites name their module instead.

// Every test binary compiles this module in full but uses only the fixtures it
// drives, so unused items and re-exports here are the normal case, not a smell.
#![allow(dead_code, unused_imports)]

use std::future::Future;
use std::time::Duration;

use actor_simulation::Simulation;

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

/// Drive an async call under the cluster's perpetually running loops for a
/// fixed `settle` window; the contract is [`Simulation::run_for_output`]'s.
pub fn drive<T: Send + 'static>(
    sim: &Simulation,
    settle: Duration,
    future: impl Future<Output = T> + Send + 'static,
) -> T {
    sim.run_for_output(settle, future)
}
