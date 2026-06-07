//! Deterministic simulation for the actor framework (spec §18).
//!
//! Reuses the very traits the production runtime uses ([`Clock`], [`Entropy`],
//! [`Spawner`]) — only the implementations differ — so a simulation runs the
//! real system code rather than a model of it. One seed drives time,
//! randomness, and scheduling, making an entire run reproducible.
//!
//! Construct a [`Simulation`], hand its [`clock`], [`entropy`], and [`spawner`]
//! to a system, then drive it with [`Simulation::run`] or
//! [`Simulation::block_on`].
//!
//! Fault injection and the invariant catalogue (spec §18.3, §18.5) arrive in a
//! later slice.
//!
//! [`Clock`]: actor_core::Clock
//! [`Entropy`]: actor_core::Entropy
//! [`Spawner`]: actor_core::Spawner
//! [`clock`]: Simulation::clock
//! [`entropy`]: Simulation::entropy
//! [`spawner`]: Simulation::spawner

mod check;
mod clock;
mod cluster_swarm;
mod entropy;
mod executor;
mod invariant;
mod recorder;
mod transport;
mod workload;

pub use check::Checker;
pub use check::Violation;
pub use clock::SimClock;
pub use cluster_swarm::ClusterCtx;
pub use cluster_swarm::ClusterFailure;
pub use cluster_swarm::ClusterWorkload;
pub use cluster_swarm::run_cluster_seed;
pub use cluster_swarm::run_cluster_swarm;
pub use entropy::SimEntropy;
pub use executor::SimSpawner;
pub use executor::Simulation;
pub use invariant::Invariant;
pub use invariant::LifecycleExactlyOnce;
pub use invariant::NoSilentLoss;
pub use invariant::SerialExecution;
pub use invariant::default_invariants;
pub use recorder::Recorder;
pub use transport::FaultPolicy;
pub use transport::SimCluster;
pub use transport::SimNetwork;
pub use transport::SimTransport;
pub use workload::FaultConfig;
pub use workload::RunFailure;
pub use workload::SimSystem;
pub use workload::Workload;
pub use workload::run_seed;
pub use workload::run_swarm;
