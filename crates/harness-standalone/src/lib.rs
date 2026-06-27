//! `harness-standalone` as a library: the node (silo) runtime. The `main.rs`
//! binary is a thin argument parser over it.
//!
//! A node hosts grains and votes in Raft; it has no client-facing protocol. The
//! public multi-tenant edge lives in `harness-gateway`, a trusted cluster client
//! that joins the transport as a non-voting member and addresses the grains
//! directly over `GrainRef`.

pub mod http;
pub mod ids;
pub mod node;
pub mod sandbox;
