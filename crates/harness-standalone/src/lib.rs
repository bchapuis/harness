//! `harness-standalone` as a library: the node (silo) runtime. The `main.rs`
//! binary is a thin argument parser over it.
//!
//! A node hosts grains and votes in Raft; it has no client-facing protocol. The
//! public multi-tenant edge lives in `harness-gateway`.

pub mod http;
pub mod node;
