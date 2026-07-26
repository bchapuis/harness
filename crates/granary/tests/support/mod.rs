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

#![allow(dead_code)]

pub mod counter;
pub use counter::*;

#[cfg(feature = "sql")]
pub mod ledger;
#[cfg(feature = "sql")]
pub use ledger::*;
