//! Workloads and the swarm runner (spec §18.4, §18.6).
//!
//! A [`Workload`] drives the cluster through its public API; the runner executes
//! it under a seeded [`Simulation`], with a per-seed [`FaultConfig`] sampled from
//! the same stream, while a [`Checker`] watches the event stream. A failing run
//! is reported as a [`RunFailure`] carrying the seed that replays it
//! deterministically (spec §18.6).

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
/// randomized to exercise invariants across the backpressure spectrum. Transport
/// faults (drop/duplicate/latency) live in
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

/// A failing run, with everything needed to replay it (spec §18.6). The
/// `(workload, seed)` pair alone replays a single-node or cluster run
/// deterministically, since the seed regenerates the run's faults.
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

/// Every seed a sweep failed at, and how many it ran (spec §18.6).
///
/// CI stops at the first failure, so this carries one [`RunFailure`] and prints
/// exactly as one; under [`collect_all_failures`](crate::collect_all_failures)
/// it carries every failing seed and prints them ready to paste into
/// `corpus.txt`.
#[derive(Clone, Debug)]
pub struct SweepFailure {
    pub workload: &'static str,
    /// How many seeds ran, including the ones that failed. When the sweep stops
    /// at the first failure this is where it stopped, not the width.
    pub seeds_run: u64,
    /// The failing seeds, in the order the sweep reached them. Never empty.
    pub failures: Vec<RunFailure>,
}

impl std::fmt::Display for SweepFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // One failure prints as the bare `RunFailure`, so it still names its own
        // corpus key.
        if let [only] = self.failures.as_slice() {
            return write!(f, "{only}");
        }
        writeln!(
            f,
            "workload '{}' failed at {} of the {} seeds run:\n",
            self.workload,
            self.failures.len(),
            self.seeds_run
        )?;
        for failure in &self.failures {
            writeln!(f, "{failure}")?;
        }
        // In the `<workload> <seed>` format corpus.txt parses.
        writeln!(f, "corpus.txt lines for every seed above:\n")?;
        for failure in &self.failures {
            writeln!(f, "{} {}", failure.workload, failure.seed)?;
        }
        Ok(())
    }
}

impl std::error::Error for SweepFailure {}

/// Run one seed, turning a panic into a [`RunFailure`] against that seed.
///
/// Sweeps fail two ways: an invariant returns a violation, or the workload
/// itself asserts. Catching the second kind here keeps the remaining seeds
/// running and names the seed, which the workload's own panic message does not.
pub(crate) fn caught(
    workload: &'static str,
    seed: u64,
    run: impl FnOnce() -> Result<(), RunFailure>,
) -> Result<(), RunFailure> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)) {
        Ok(result) => result,
        Err(payload) => Err(RunFailure {
            workload,
            seed,
            violations: vec![Violation {
                invariant: "panic",
                detail: panic_detail(payload.as_ref()),
            }],
        }),
    }
}

/// The message a caught panic carried, for the two payload types `panic!`
/// produces. Anything else is reported by shape.
pub(crate) fn panic_detail(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "panicked with a non-string payload".to_string()
    }
}

/// The sweep loop shared by every invariant runner: sweep the seeds, and decide
/// whether a failure ends the run or joins a list.
///
/// `collect` is a parameter rather than an environment read, so the loop is a
/// pure function of its inputs; the runners read
/// [`collect_all_failures`](crate::collect_all_failures) once, at the edge.
pub(crate) fn sweep_collecting(
    workload: &'static str,
    seeds: impl IntoIterator<Item = u64>,
    collect: bool,
    mut run: impl FnMut(u64) -> Result<(), RunFailure>,
) -> Result<(), SweepFailure> {
    let mut failures: Vec<RunFailure> = Vec::new();
    let mut seeds_run = 0;
    for seed in seeds {
        seeds_run += 1;
        if let Err(failure) = caught(workload, seed, || run(seed)) {
            // A seed the corpus replay and the sweep range both reach ran twice;
            // it reports once.
            if !failures.iter().any(|seen| seen.seed == seed) {
                failures.push(failure);
            }
            if !collect {
                break;
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(SweepFailure {
            workload,
            seeds_run,
            failures,
        })
    }
}

/// Build and run a workload once under `seed`, routing its event stream to
/// `events`. Shared by [`run_seed`] (which feeds a [`Checker`]) and the
/// reproducibility harness (which feeds a [`Recorder`](crate::Recorder)), so both
/// observe the *identical* run.
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
/// the first failing seed so it can be replayed — or, under
/// [`collect_all_failures`](crate::collect_all_failures), running to the end and
/// reporting every one.
///
/// The workload's [`regression_seeds`](crate::regression_seeds) run first,
/// ahead of `seeds` and whatever sizing produced them: a seed that failed once
/// is checked on every run, however narrow the sweep.
pub fn run_swarm<W: Workload>(
    workload: &W,
    seeds: impl IntoIterator<Item = u64>,
) -> Result<(), SweepFailure> {
    sweep_collecting(
        workload.name(),
        crate::corpus::regression_seeds(workload.name()).chain(seeds),
        crate::sweep::collect_all_failures(),
        |seed| run_seed(workload, seed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A run that fails on the seeds named, and passes on the rest.
    fn failing_on(bad: &[u64]) -> impl Fn(u64) -> Result<(), RunFailure> + '_ {
        move |seed| {
            if bad.contains(&seed) {
                Err(RunFailure {
                    workload: "w",
                    seed,
                    violations: vec![Violation {
                        invariant: "inv",
                        detail: format!("seed {seed} is bad"),
                    }],
                })
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn stopping_reports_only_the_first_failure() {
        let run = failing_on(&[3, 5, 7]);
        let failure = sweep_collecting("w", 0..10, false, run).expect_err("3 fails");
        assert_eq!(failure.failures.len(), 1);
        assert_eq!(failure.failures[0].seed, 3);
        // Four seeds ran: 0, 1, 2, and the one that stopped it.
        assert_eq!(failure.seeds_run, 4);
    }

    #[test]
    fn collecting_reports_every_failing_seed_and_runs_them_all() {
        let run = failing_on(&[3, 5, 7]);
        let failure = sweep_collecting("w", 0..10, true, run).expect_err("three fail");
        let seeds: Vec<u64> = failure.failures.iter().map(|f| f.seed).collect();
        assert_eq!(seeds, vec![3, 5, 7]);
        assert_eq!(failure.seeds_run, 10, "collecting runs the whole sweep");
    }

    #[test]
    fn a_seed_reached_twice_is_reported_once() {
        // What the corpus replay does: seed 3 runs ahead of the sweep, and the
        // sweep covers it again.
        let seeds = [3].into_iter().chain(0..5);
        let failure = sweep_collecting("w", seeds, true, failing_on(&[3])).expect_err("seed 3");
        assert_eq!(failure.failures.len(), 1);
        assert_eq!(failure.seeds_run, 6, "it still ran twice; it reports once");
    }

    #[test]
    fn a_clean_sweep_is_ok() {
        assert!(sweep_collecting("w", 0..10, true, failing_on(&[])).is_ok());
    }

    #[test]
    fn a_panicking_seed_is_caught_and_named() {
        let failure = sweep_collecting("w", 0..4, true, |seed| {
            assert_ne!(seed, 2, "the workload asserted");
            Ok(())
        })
        .expect_err("seed 2 panics");
        assert_eq!(failure.failures[0].seed, 2);
        assert_eq!(failure.failures[0].violations[0].invariant, "panic");
        assert!(
            failure.failures[0].violations[0]
                .detail
                .contains("the workload asserted"),
            "the panic message is kept: {}",
            failure.failures[0].violations[0].detail
        );
        assert_eq!(failure.seeds_run, 4, "a panic does not end the sweep");
    }

    #[test]
    fn one_failure_prints_exactly_as_a_run_failure() {
        let run = failing_on(&[3]);
        let failure = sweep_collecting("w", 0..10, false, run).expect_err("3 fails");
        assert_eq!(failure.to_string(), failure.failures[0].to_string());
    }

    #[test]
    fn many_failures_print_pasteable_corpus_lines() {
        let run = failing_on(&[3, 5]);
        let failure = sweep_collecting("w", 0..10, true, run).expect_err("two fail");
        let report = failure.to_string();
        let summary = "failed at 2 of the 10 seeds run";
        assert!(report.contains(summary), "{report}");
        // The block the soak exists to produce, in the format corpus.txt parses.
        assert!(report.contains("\nw 3\nw 5\n"), "{report}");
    }
}
