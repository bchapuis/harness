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
//!
//! `counter` is re-exported flat; `ledger` is not. Both define an `Add`, so two
//! globs would make the name ambiguous — and because `ledger` is feature-gated,
//! that break only appeared once `--features sql` was on, which nothing in CI or
//! `soak.yml` turns on. The `sql` suites name their module instead.

#![allow(dead_code)]

pub mod counter;
pub use counter::*;

#[cfg(feature = "sql")]
pub mod ledger;
