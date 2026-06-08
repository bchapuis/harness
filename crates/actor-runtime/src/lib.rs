//! The production runtime for the actor framework (spec §4.6, §7, §15).
//!
//! The framework is runtime-agnostic: time, randomness, scheduling, and the
//! wire all sit behind traits ([`Clock`], [`Entropy`], [`Spawner`],
//! [`Transport`]), and the simulator (`actor-simulation`) supplies deterministic
//! implementations. This crate supplies the **production** counterparts so a
//! [`ClusterSystem`] can run for real across processes and hosts — and nothing
//! else in the framework changes (spec §4.6):
//!
//! - [`TokioClock`] — wall-clock time on a tokio runtime.
//! - [`OsEntropy`] — an OS-seeded PRNG (`buggify` stays off).
//! - [`TokioSpawner`] — tasks on a tokio runtime.
//! - [`TcpTransport`] — length-delimited frames over TCP, behind a mutual-TLS
//!   association handshake with a cluster secret and a node allowlist (§7, §15).
//!
//! tokio and rustls live here and only here, so the core and cluster crates stay
//! free of any specific async runtime.
//!
//! [`Clock`]: actor_core::Clock
//! [`Entropy`]: actor_core::Entropy
//! [`Spawner`]: actor_core::Spawner
//! [`Transport`]: actor_cluster::Transport
//! [`ClusterSystem`]: actor_cluster::ClusterSystem

// This crate IS the host seam (spec §4.6, §18.1): it is the one place allowed to
// read the wall clock, spawn OS/tokio tasks, and seed from the OS. The workspace
// `clippy.toml` forbids those host APIs everywhere else to keep the simulation
// build deterministic; this crate-level allow is the explicit determinism
// boundary they are permitted to cross.
#![allow(clippy::disallowed_methods)]

mod clock;
mod entropy;
mod spawner;
mod transport;
mod wire;

pub use clock::TokioClock;
pub use entropy::OsEntropy;
pub use spawner::TokioSpawner;
pub use transport::DEFAULT_CONNECT_TIMEOUT;
pub use transport::DEFAULT_HANDSHAKE_TIMEOUT;
pub use transport::DEFAULT_OUTBOUND_CAPACITY;
pub use transport::PROTO_VERSION;
pub use transport::TcpConfig;
pub use transport::TcpTransport;
pub use transport::TlsConfig;

/// A cluster node wired to the production runtime: tokio time and scheduling,
/// OS-seeded entropy, and the TCP transport. The production counterpart of
/// `actor_simulation::SimCluster`.
pub type TcpCluster =
    actor_cluster::ClusterSystem<TokioClock, OsEntropy, TokioSpawner, TcpTransport>;
