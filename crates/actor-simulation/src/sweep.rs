//! Seed-sweep sizing for swarm tests (spec §2.12.2).
//!
//! A sweep serves two jobs that want different seeds: **regression** against a
//! pinned corpus, which is what CI runs, and **discovery** across fresh seeds in
//! bulk, whose every failure is a `(base, seed)` pair that reproduces exactly
//! and can then join the corpus. Neither fits the edit loop, so a sweep declares
//! its range *and* what a seed costs it, and the run decides the rest:
//!
//! | run   | seeds                          | pinned            |
//! |-------|--------------------------------|-------------------|
//! | local | a few, by cost class           | yes               |
//! | CI    | from 0, the declared width     | yes               |
//! | soak  | from a fresh base, many        | by `(base, seed)` |
//!
//! Sizing is a cost class, not a wall clock: reading the host clock to decide
//! how much to run would make the seeds that executed depend on the machine that
//! ran them (spec §2.12.2), and `clippy.toml` bans the call in this crate. Seeds
//! stay independent under all three sizings, so a narrow run is a smaller sample
//! of the same corpus rather than a different one.
//!
//! Per-seed cost spans about four orders of magnitude across these workloads, so
//! the call site says which class it is: [`sweep_seeds`] for ordinary sweeps,
//! [`slow_seeds`] where a single seed costs seconds. [`coverage_seeds`] is a
//! correctness class, not a cost one.
//!
//! # Environment
//!
//! - `SWARM_SEEDS=<n>`: take exactly `n` seeds, whatever the class. May exceed
//!   the declared width, which is how a soak asks for more than the corpus holds.
//! - `SWARM_SEEDS=full` (or `0`): the declared width, the corpus as written.
//! - `SWARM_SEED_BASE=<n>`: offset every sweep by `n`. A soak sets this fresh
//!   per run so it explores seeds the corpus has never covered.
//! - `SWARM_CONTINUE=1`; do not stop at the first failing seed; run the whole
//!   sweep and report every seed that failed. See [`collect_all_failures`].
//!
//! Unset, a run takes its cost class's width; CI pins `SWARM_SEEDS=full` in its
//! workflow.
//!
//! # Choosing a cost class
//!
//! Reclassify a sweep by measuring, not guessing. Run its test binary at two
//! widths and take the difference, so the binary's fixed start-up cost (about
//! 0.6 s cold, enough to swamp a cheap sweep) cancels out; warm the binary
//! first:
//!
//! ```text
//! # per-seed cost = (t32 - t2) / 30
//! SWARM_SEEDS=2  ./target/debug/deps/<binary>
//! SWARM_SEEDS=32 ./target/debug/deps/<binary>
//! ```

use std::ops::Range;
use std::sync::Once;

const LOCAL_SEEDS: u64 = 8;

/// One seed is a smoke test: the workload still runs and its invariants still
/// hold somewhere.
const LOCAL_SLOW_SEEDS: u64 = 1;

const ENV_WIDTH: &str = "SWARM_SEEDS";
const ENV_BASE: &str = "SWARM_SEED_BASE";
const ENV_CONTINUE: &str = "SWARM_CONTINUE";

/// The seeds to sweep for a declared range whose seeds are cheap.
///
/// The declared range stays in the test as the corpus of record; offsetting by
/// `SWARM_SEED_BASE` keeps its shape, so a soak failure at `(base, seed)`
/// replays by setting the same two variables.
pub fn sweep_seeds(declared: Range<u64>) -> Range<u64> {
    resolve(declared, LOCAL_SEEDS)
}

/// The seeds to sweep for a declared range whose seeds cost seconds apiece:
/// workloads that touch the filesystem, a database, or a machine binding.
///
/// Same contract as [`sweep_seeds`]; only the local width differs, so one slow
/// workload cannot dominate the edit loop.
pub fn slow_seeds(declared: Range<u64>) -> Range<u64> {
    resolve(declared, LOCAL_SLOW_SEEDS)
}

/// The seeds to sweep for a fault-coverage assertion.
///
/// A coverage sweep asserts that each fault type fired at least once *across
/// the declared width* (docs/simulation-testing.md), so narrowing it would change what the test
/// claims. These run at their declared width everywhere; only an explicit
/// `SWARM_SEEDS` changes that.
pub fn coverage_seeds(declared: Range<u64>) -> Range<u64> {
    let declared_width = declared.end - declared.start;
    let width = match width_request() {
        Width::Explicit(n) => n,
        Width::Declared | Width::Unset => declared_width,
    };
    offset(declared.start, width)
}

fn resolve(declared: Range<u64>, local: u64) -> Range<u64> {
    let declared_width = declared.end - declared.start;
    let width = match width_request() {
        Width::Explicit(n) => n,
        Width::Declared => declared_width,
        Width::Unset => local.min(declared_width),
    };
    offset(declared.start, width)
}

fn offset(start: u64, width: u64) -> Range<u64> {
    announce_once();
    let start = start.saturating_add(base_offset());
    start..start.saturating_add(width)
}

/// What `SWARM_SEEDS` asked for. `Declared` and `Unset` are distinct: naming
/// `full` pins the corpus width everywhere; saying nothing takes the cost
/// class's width, except for a coverage sweep.
enum Width {
    Explicit(u64),
    Declared,
    Unset,
}

fn width_request() -> Width {
    match std::env::var(ENV_WIDTH) {
        Ok(raw) => match raw.trim() {
            "full" | "0" => Width::Declared,
            // A malformed value is a typo on a command line, not a request for a
            // narrower sweep: fall back to the declared width, which cannot
            // under-test.
            other => other
                .parse::<u64>()
                .map_or(Width::Declared, Width::Explicit),
        },
        Err(_) => Width::Unset,
    }
}

fn base_offset() -> u64 {
    std::env::var(ENV_BASE)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

/// Whether a sweep runs to the end and reports **every** failing seed, rather
/// than stopping at the first.
///
/// CI wants the first failure fast. A soak is mining the seed space for
/// `(workload, seed)` pairs to write into `corpus.txt`, and halting on the first
/// one throws away the rest of the run. Worse, corpus seeds replay *ahead* of
/// every sweep, so without this a workload with a failing seed on record fails
/// that replay first and explores nothing new until the bug is fixed.
///
/// Off by default (stop at the first failure); `soak.yml` turns it on.
pub(crate) fn collect_all_failures() -> bool {
    match std::env::var(ENV_CONTINUE) {
        Ok(raw) => !matches!(raw.trim(), "" | "0" | "false" | "no"),
        Err(_) => false,
    }
}

/// The sweep loop every runner shares, generic over its failure type: run each
/// seed, dedupe failures by seed (a seed the corpus replay and the sweep range
/// both reach runs twice and reports once), and either stop at the first
/// failure or collect them all. Returns how many seeds ran and the failures in
/// sweep order; an empty list is success. What "failure" means (an invariant
/// violation, a determinism divergence, a caught panic) stays the runner's,
/// as does its report.
pub(crate) fn sweep_generic<F>(
    seeds: impl IntoIterator<Item = u64>,
    collect: bool,
    seed_of: impl Fn(&F) -> u64,
    mut run: impl FnMut(u64) -> Result<(), F>,
) -> (u64, Vec<F>) {
    let mut failures: Vec<F> = Vec::new();
    let mut seeds_run = 0;
    for seed in seeds {
        seeds_run += 1;
        if let Err(failure) = run(seed) {
            if !failures.iter().any(|seen| seed_of(seen) == seed) {
                failures.push(failure);
            }
            if !collect {
                break;
            }
        }
    }
    (seeds_run, failures)
}

/// Say once per test binary how sweeps are sized, so a narrow run is never
/// mistaken for the corpus.
fn announce_once() {
    static ANNOUNCED: Once = Once::new();
    ANNOUNCED.call_once(|| {
        let base = base_offset();
        let from = if base == 0 {
            String::new()
        } else {
            format!(", from base {base}")
        };
        let sizing = match width_request() {
            Width::Explicit(n) => format!("{n} seed(s) per sweep"),
            Width::Declared => "the declared width".to_string(),
            Width::Unset => {
                format!("{LOCAL_SEEDS} seed(s) per sweep, {LOCAL_SLOW_SEEDS} for slow ones")
            }
        };
        let stopping = if collect_all_failures() {
            ", reporting every failing seed"
        } else {
            ""
        };
        eprintln!(
            "note: swarm sweeps run {sizing}{from}{stopping}; \
             set {ENV_WIDTH}=full for the declared corpus, \
             {ENV_CONTINUE}=1 to collect every failure"
        );
    });
}

/// Sweep a hand-built scenario across seeds under an explicit name.
///
/// A [`Workload`](crate::Workload)'s `name()` is its corpus key; a scenario that
/// builds a [`Simulation`](crate::Simulation) directly has none, so the caller
/// supplies one and it ratchets like any other sweep.
///
/// ```ignore
/// scenario_sweep("partition-safety/quorum-reads", sweep_seeds(0..12), |seed| {
///     let sim = Simulation::new(seed);
///     // ... drive it, assert on the outcome ...
/// });
/// ```
///
/// The body panics to fail; the sweep stops at the first seed that does, or,
/// under [`collect_all_failures`], catches each panic, runs the rest of the
/// seeds, and fails at the end naming every one. Prefer a `Workload` when the
/// scenario fits one: the trait buys invariant checking and reproducibility
/// sweeps as well as a name.
pub fn scenario_sweep(name: &str, seeds: impl IntoIterator<Item = u64>, mut run: impl FnMut(u64)) {
    let seeds = crate::corpus::regression_seeds(name).chain(seeds);
    // The stop-at-first mode runs bare, so the failing seed's own panic (and
    // backtrace) surfaces unwrapped.
    if !collect_all_failures() {
        for seed in seeds {
            run(seed);
        }
        return;
    }
    let (seeds_run, failed) = sweep_generic(
        seeds,
        true,
        |(seen, _): &(u64, String)| *seen,
        |seed| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(seed)))
                .map_err(|payload| (seed, crate::workload::panic_detail(payload.as_ref())))
        },
    );
    if failed.is_empty() {
        return;
    }
    let mut report = format!(
        "scenario '{name}' failed at {} of the {seeds_run} seeds run:\n\n",
        failed.len()
    );
    for (seed, detail) in &failed {
        report.push_str(&format!("  seed {seed}: {detail}\n"));
    }
    report.push('\n');
    report.push_str(&crate::corpus::corpus_block(
        failed.iter().map(|(seed, _)| (name, *seed)),
    ));
    panic!("{report}");
}
