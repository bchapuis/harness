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
//!   every run by a [`Checker`], with the [`core_catalogue`] tying each spec invariant
//!   to how it is verified. [`run_swarm`]/[`run_cluster_swarm`] sweep workloads
//!   across seeds under seeded [`FaultPolicy`] faults and a nemesis, at the
//!   width [`sweep_seeds`] resolves for the run — the whole corpus under CI, a
//!   few seeds in the edit loop, and fresh seeds from a `SWARM_SEED_BASE` when
//!   soaking for undiscovered ones. Whatever the sizing, every sweep also
//!   replays its workload's [`regression_seeds`] — the fixed-seed corpus that
//!   makes a bug found once stay found.
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
mod cluster;
mod cluster_swarm;
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

// --- The runtime seam: what a system is built on ------------------------------
// The simulated halves of the same `Clock`/`Entropy`/`Spawner`/`Transport`
// traits production uses (spec §4.6, §7, §18.2).
pub use clock::SimClock;
pub use cluster::NodeRestarted;
pub use entropy::SimEntropy;
pub use executor::SimSpawner;
pub use executor::Simulation;
pub use registry::SimRegistry;
pub use transport::SimNode;
pub use transport::SimNetwork;
pub use transport::SimTransport;
pub use workload::SimSystem;

// --- Workloads and their runners ----------------------------------------------
// A workload drives the system through its public API; a runner executes it
// under one seed. Single-node and cluster shapes, paired (spec §18.4).
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

// --- Shape 4: reproducibility -------------------------------------------------
// The determinism contract enforced over the real event stream (spec §18.1 #1).
pub use recorder::Recorder;
pub use replay::Divergence;
pub use replay::SweepDivergence;
pub use replay::check_cluster_reproducible;
pub use replay::check_reproducible;
pub use replay::record_cluster_seed;
pub use replay::record_seed;
pub use replay::replay_cluster_swarm;
pub use replay::replay_swarm;

// --- Which seeds a run spends -------------------------------------------------
// Sizing by declared cost class, plus the pinned corpus that ignores sizing
// entirely (spec §18.6).
pub use corpus::regression_seeds;
pub use sweep::collect_all_failures;
pub use sweep::coverage_seeds;
pub use sweep::scenario_sweep;
pub use sweep::slow_seeds;
pub use sweep::sweep_seeds;

// --- Faults: the input side, and the coverage that proves they fired ----------
pub use coverage::FaultStats;
pub use faults::FaultPolicy;
pub use faults::RegistryFaultPolicy;
pub use workload::FaultConfig;

// --- Invariants and the checker that feeds them -------------------------------
// Safety predicates over the §16 event stream, checked on every run (spec §18.5).
pub use check::Checker;
pub use check::Violation;
pub use invariant::Invariant;
pub use invariant::LifecycleExactlyOnce;
pub use invariant::NoSilentLoss;
pub use invariant::OneLeaderPerTerm;
pub use invariant::SerialExecution;
pub use invariant::SingletonAtMostOnePerNode;
pub use invariant::default_invariants;

// --- Conformance traceability -------------------------------------------------
// Which invariant is verified how, machine-readable and drift-tested (spec §17).
pub use catalogue::CatalogueEntry;
pub use catalogue::Verify;
pub use catalogue::core_catalogue;
pub use catalogue::utilities_catalogue;

// --- Linearizability ----------------------------------------------------------
// The other angle on correctness: a client-observed history decided against a
// sequential reference model (spec §18.4).
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
