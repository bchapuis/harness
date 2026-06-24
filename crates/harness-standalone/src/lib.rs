//! `harness-standalone` as a library: the node runtime, the control protocol,
//! and the two client-facing front-ends (REPL and ACP). The `main.rs` binary is
//! a thin argument parser over these modules.
//!
//! Exposing the modules as a library is what lets the integration tests under
//! `tests/` drive [`acp::serve`] over an in-process pipe and stand up a fake
//! control port from the real [`proto`] types — the same wire the node serves.

pub mod acp;
pub mod http;
pub mod ids;
pub mod node;
pub mod proto;
pub mod repl;
pub mod sandbox;
