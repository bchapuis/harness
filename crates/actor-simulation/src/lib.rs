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
//! Correctness is checked from three complementary angles:
//!
//! - **Continuous invariants** (spec §18.5): a small set of always-on
//!   [`Invariant`]s — safety predicates over the §16 event stream — checked on
//!   every run by a [`Checker`], with the [`catalogue`] tying each spec invariant
//!   to how it is verified. [`run_swarm`]/[`run_cluster_swarm`] sweep workloads
//!   across seeds under seeded [`FaultPolicy`] faults and a nemesis.
//! - **Seed-reproducibility** (spec §18.1 #1): the determinism contract enforced
//!   over the *real* system — [`check_reproducible`]/[`replay_cluster_swarm`] run
//!   a workload twice under one seed and assert byte-identical event streams,
//!   pinpointing any [`Divergence`].
//! - **Linearizability** (spec §18.4): record a client-observed [`History`] and
//!   decide it against a reference [`Model`] ([`Register`], [`Counter`]) with
//!   [`check_linearizable`], a Wing & Gong search.
//!
//! Fault injection is tallied as [`FaultStats`] so a sweep can assert it actually
//! exercised loss, duplication, reordering, and partitions — coverage, not just
//! configuration.
//!
//! [`Clock`]: actor_core::Clock
//! [`Entropy`]: actor_core::Entropy
//! [`Spawner`]: actor_core::Spawner
//! [`clock`]: Simulation::clock
//! [`entropy`]: Simulation::entropy
//! [`spawner`]: Simulation::spawner

mod catalogue;
mod check;
mod clock;
mod cluster_swarm;
mod coverage;
mod entropy;
mod executor;
mod faults;
mod invariant;
mod linearizability;
mod recorder;
mod registry;
mod replay;
mod transport;
mod workload;

pub use catalogue::CatalogueEntry;
pub use catalogue::Verify;
pub use catalogue::catalogue;
pub use catalogue::utilities_catalogue;
pub use check::Checker;
pub use check::Violation;
pub use clock::SimClock;
pub use cluster_swarm::ClusterCtx;
pub use cluster_swarm::ClusterModeSpec;
pub use cluster_swarm::ClusterWorkload;
pub use cluster_swarm::run_cluster_seed;
pub use cluster_swarm::run_cluster_swarm;
pub use cluster_swarm::run_cluster_swarm_coverage;
pub use coverage::FaultStats;
pub use entropy::SimEntropy;
pub use executor::SimSpawner;
pub use executor::Simulation;
pub use faults::FaultPolicy;
pub use invariant::Invariant;
pub use invariant::LifecycleExactlyOnce;
pub use invariant::NoSilentLoss;
pub use invariant::OneLeaderPerTerm;
pub use invariant::SerialExecution;
pub use invariant::SingletonAtMostOnePerNode;
pub use invariant::default_invariants;
pub use linearizability::Counter;
pub use linearizability::CounterOp;
pub use linearizability::CounterRet;
pub use linearizability::History;
pub use linearizability::Linearization;
pub use linearizability::MAX_HISTORY;
pub use linearizability::Model;
pub use linearizability::OpId;
pub use linearizability::Register;
pub use linearizability::RegisterOp;
pub use linearizability::RegisterRet;
pub use linearizability::check as check_linearizable;
pub use recorder::Recorder;
pub use registry::RegistryFaultPolicy;
pub use registry::SimRegistry;
pub use replay::Divergence;
pub use replay::check_cluster_reproducible;
pub use replay::check_reproducible;
pub use replay::record_cluster_seed;
pub use replay::record_seed;
pub use replay::replay_cluster_swarm;
pub use replay::replay_swarm;
pub use transport::SimCluster;
pub use transport::SimNetwork;
pub use transport::SimTransport;
pub use workload::FaultConfig;
pub use workload::RunFailure;
pub use workload::SimSystem;
pub use workload::Workload;
pub use workload::run_seed;
pub use workload::run_swarm;
