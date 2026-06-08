//! Workloads and the swarm runner (spec §18.4, §18.6).
//!
//! A [`Workload`] drives the cluster through its public API; the runner executes
//! it under a seeded [`Simulation`], with a per-seed [`FaultConfig`] sampled from
//! the same stream, while a [`Checker`] watches the event stream. A failing run
//! is reported as a [`RunFailure`] carrying the `(seed, faults)` needed to
//! replay it deterministically (spec §18.6).
//!
//! The swarm loop: define a few workloads and invariants, then sweep many
//! seeds ([`run_swarm`]); coverage is cluster-time exercised, not test count.

use std::sync::Arc;

use actor_core::BoxFuture;
use actor_core::EventSink;
use actor_core::LocalSystem;
use actor_core::LocalSystemBuilder;

use crate::SimClock;
use crate::SimEntropy;
use crate::SimSpawner;
use crate::Simulation;
use crate::check::Checker;
use crate::check::Violation;
use crate::invariant::Invariant;
use crate::invariant::default_invariants;

/// The concrete system a simulated workload runs on.
pub type SimSystem = LocalSystem<SimClock, SimEntropy, SimSpawner>;

/// A test scenario expressed against the cluster's public API (spec §18.4).
///
/// `run` builds actors and drives traffic; the runner then advances the
/// simulation to quiescence and checks the workload's [`invariants`]. A workload
/// MUST observe the cluster only through the public API and the event stream,
/// never through actor state directly.
///
/// [`invariants`]: Workload::invariants
pub trait Workload: Send + 'static {
    /// A stable name for reporting.
    fn name(&self) -> &'static str;

    /// Build actors and drive traffic to completion. The returned future
    /// resolves when the workload's own traffic is done; the runner still drives
    /// the simulation to full quiescence afterwards.
    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()>;

    /// Invariants checked continuously and at quiescence (spec §18.5). Defaults
    /// to [`default_invariants`].
    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        default_invariants()
    }
}

/// A seed-sampled fault configuration for a single-node run (spec §18.3).
///
/// A single-node workload runs on a [`LocalSystem`] with no transport or
/// membership, so the only fault dimension here is the bounded mailbox capacity,
/// randomized so invariants are exercised across the backpressure spectrum.
/// Transport faults (drop/duplicate/latency) are a *cluster* concern and live in
/// [`FaultPolicy`](crate::FaultPolicy), applied to the in-memory network.
#[derive(Clone, Copy, Debug)]
pub struct FaultConfig {
    /// Per-actor bounded mailbox capacity for this run.
    pub mailbox_capacity: usize,
}

impl FaultConfig {
    /// Sample a configuration from the run's entropy. Drawing here keeps the
    /// choice deterministic per seed.
    pub fn sample(entropy: &SimEntropy) -> FaultConfig {
        use actor_core::Entropy;
        FaultConfig {
            mailbox_capacity: 1 + (entropy.next_u64() % 64) as usize,
        }
    }
}

/// A failing run, with everything needed to replay it (spec §18.6). One type
/// covers both single-node and cluster runs: the `(workload, seed)` pair alone
/// replays either deterministically — the seed regenerates the run's faults — so
/// there is nothing run-shaped to carry beyond it.
#[derive(Clone, Debug)]
pub struct RunFailure {
    pub workload: &'static str,
    pub seed: u64,
    pub violations: Vec<Violation>,
}

impl std::fmt::Display for RunFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "workload '{}' failed at seed {} (replay from seed {}):",
            self.workload, self.seed, self.seed
        )?;
        for v in &self.violations {
            writeln!(f, "  - {v}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RunFailure {}

/// Build and run a workload once under `seed`, routing its event stream to
/// `events`. Shared by [`run_seed`] (which feeds a [`Checker`]) and the
/// reproducibility harness (which feeds a [`Recorder`](crate::Recorder)), so both
/// observe the *identical* run — the construction lives in exactly one place.
pub(crate) fn drive_local<W: Workload>(workload: &W, seed: u64, events: Arc<dyn EventSink>) {
    let sim = Simulation::new(seed);
    let faults = FaultConfig::sample(&sim.entropy());

    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .mailbox_capacity(faults.mailbox_capacity)
        .events(events)
        .build();

    sim.block_on(workload.run(system));
}

/// Run a workload once under a given seed, returning any invariant violations.
pub fn run_seed<W: Workload>(workload: &W, seed: u64) -> Result<(), RunFailure> {
    let checker = Checker::new(workload.invariants());
    drive_local(workload, seed, checker.sink());

    let violations = checker.finish();
    if violations.is_empty() {
        Ok(())
    } else {
        Err(RunFailure {
            workload: workload.name(),
            seed,
            violations,
        })
    }
}

/// Sweep a workload across many seeds (swarm testing, spec §18.6), stopping at
/// the first failing seed so it can be replayed.
pub fn run_swarm<W: Workload>(
    workload: &W,
    seeds: impl IntoIterator<Item = u64>,
) -> Result<(), RunFailure> {
    for seed in seeds {
        run_seed(workload, seed)?;
    }
    Ok(())
}
