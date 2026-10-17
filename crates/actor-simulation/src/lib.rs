//! Deterministic simulation for the actor framework: a whole cluster in one
//! process, on one logical thread, over virtual time, network, and randomness,
//! so one seed reproduces a multi-node run, failures included (spec §2.12.2).
//!
//! # Seams
//!
//! The simulator implements the production seams rather than modelling them,
//! so a simulation runs the real system code:
//!
//! - [`SimClock`], [`SimEntropy`], [`SimSpawner`]: the host environment
//!   ([`Clock`], [`Entropy`], [`Spawner`]), sharing one scheduler that advances
//!   virtual time only at quiescence.
//! - [`SimTransport`] over a [`SimNetwork`]: the cluster `Transport`, with
//!   seeded loss, duplication, delay, reordering, and partitions; [`SimNode`] is
//!   a `ClusterSystem` over it, [`SimSystem`] a single-node `LocalSystem`.
//! - [`SimRegistry`]: the `RegistryClient` of registry-based mode, with seeded
//!   latency, staleness, and outages.
//!
//! What it offers its own callers, the test suites of every crate above, is the
//! [`Workload`] and [`ClusterWorkload`] shapes, their seeded runners, the
//! [`Invariant`] checkers, and the core and utilities catalogues.
//!
//! # Mechanism
//!
//! Construct a [`Simulation`], hand its [`clock`], [`entropy`], and [`spawner`]
//! to a system, then drive it with [`Simulation::run`] or
//! [`Simulation::block_on`]. Correctness is checked from three angles:
//!
//! - **Continuous invariants** (spec §10): safety predicates over the §2.7
//!   event stream, checked on every run by a [`Checker`].
//!   [`run_swarm`]/[`run_cluster_swarm`] sweep workloads across seeds under
//!   seeded [`FaultPolicy`] faults and a nemesis; every sweep also replays its
//!   workload's [`regression_seeds`].
//! - **Seed-reproducibility** (spec §2.12.2 #1): [`check_reproducible`] and
//!   [`replay_cluster_swarm`] run a workload twice under one seed and assert
//!   byte-identical event streams, pinpointing any [`Divergence`].
//! - **Linearizability**: a client-observed [`History`] decided against a
//!   reference [`Model`] by [`check_linearizable`], a Wing & Gong search (see
//!   `docs/simulation-testing.md`, reference-model testing).
//!
//! Specified by `docs/distributed-actor-spec.md` (the determinism contract and
//! the invariant catalogue); `docs/simulation-testing.md` is the guide to
//! writing suites on it.
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
mod cluster;
mod cluster_swarm;
mod config;
mod corpus;
mod coverage;
mod entropy;
mod executor;
mod faults;
mod invariant;
mod linearizability;
mod recorder;
mod registry;
mod replay;
mod sweep;
mod transport;
mod workload;

// --- The runtime seam ----------------------------------------------------------
// The simulated halves of the same `Clock`/`Entropy`/`Spawner`/`Transport`
// traits production uses (spec §2.12.1, §2.13, §2.12.2).
pub use clock::SimClock;
pub use cluster::NodeRestarted;
pub use entropy::SimEntropy;
pub use executor::SimSpawner;
pub use executor::Simulation;
pub use registry::SimRegistry;
pub use transport::SimNetwork;
pub use transport::SimNode;
pub use transport::SimTransport;
pub use workload::SimSystem;

// --- The control-plane timings a simulated cluster runs under -----------------
pub use config::raft_config;
pub use config::swim_config;

// --- Workloads and their runners ----------------------------------------------
// A workload drives the system through its public API; a runner executes it
// under one seed (docs/simulation-testing.md).
pub use cluster_swarm::ClusterCtx;
pub use cluster_swarm::ClusterModeSpec;
pub use cluster_swarm::ClusterWorkload;
pub use cluster_swarm::Rehost;
pub use cluster_swarm::run_cluster_seed;
pub use cluster_swarm::run_cluster_swarm;
pub use cluster_swarm::run_cluster_swarm_coverage;
pub use workload::RunFailure;
pub use workload::SweepFailure;
pub use workload::Workload;
pub use workload::run_seed;
pub use workload::run_swarm;

// --- Reproducibility: the determinism contract over the event stream (§2.12.2 #1)
pub use recorder::Recorder;
pub use replay::Divergence;
pub use replay::SweepDivergence;
pub use replay::check_cluster_reproducible;
pub use replay::check_reproducible;
pub use replay::record_cluster_seed;
pub use replay::replay_cluster_swarm;
pub use replay::replay_swarm;

// --- Which seeds a run spends: sizing by cost class, plus the pinned corpus
// that ignores sizing entirely (spec §2.12.2).
pub use corpus::regression_seeds;
pub use sweep::coverage_seeds;
pub use sweep::scenario_sweep;
pub use sweep::slow_seeds;
pub use sweep::sweep_seeds;

// --- Faults: the input side, and the coverage that proves they fired ----------
pub use coverage::FaultStats;
pub use faults::FaultPolicy;
pub use faults::RegistryFaultPolicy;

// --- Invariants and the checker that feeds them (spec §10) ------------------
pub use check::Checker;
pub use check::Violation;
pub use invariant::Invariant;
pub use invariant::LifecycleExactlyOnce;
pub use invariant::NoSilentLoss;
pub use invariant::OneLeaderPerTerm;
pub use invariant::SerialExecution;
pub use invariant::SingletonAtMostOnePerNode;
pub use invariant::checker_coverage;
pub use invariant::default_invariants;

// --- Conformance traceability: which invariant is verified how (spec §10) -----
pub use catalogue::Catalogue;
pub use catalogue::CatalogueEntry;
pub use catalogue::CheckerCoverage;
pub use catalogue::Verify;
pub use catalogue::core_catalogue;
pub use catalogue::utilities_catalogue;

// --- Linearizability: a client history against a sequential model
pub use linearizability::Counter;
pub use linearizability::CounterOp;
pub use linearizability::CounterRet;
pub use linearizability::History;
pub use linearizability::Linearization;
pub use linearizability::Model;
pub use linearizability::OpId;
pub use linearizability::Register;
pub use linearizability::RegisterOp;
pub use linearizability::RegisterRet;
pub use linearizability::check as check_linearizable;
