//! Seed-sweep sizing for swarm tests (spec §18.6).
//!
//! A swarm sweep serves two different jobs, and they want different seeds.
//!
//! - **Regression.** A pinned corpus — the same seeds on every run — so a bug
//!   once fixed cannot come back unnoticed. This is what CI runs, and its value
//!   comes precisely from *not* varying.
//! - **Discovery.** Fresh seeds nobody has run before, in bulk, to find the
//!   corner cases the corpus does not contain yet. Every failure it turns up is
//!   a `(base, seed)` pair that reproduces exactly and can then join the corpus.
//!
//! Neither job fits the edit loop, which wants an answer in seconds. So a sweep
//! declares its range *and* what a seed costs it, and the run decides the rest:
//!
//! | run   | seeds                          | pinned            |
//! |-------|--------------------------------|-------------------|
//! | local | a few, by cost class           | yes               |
//! | CI    | from 0, the declared width     | yes               |
//! | soak  | from a fresh base, many        | by `(base, seed)` |
//!
//! Sizing is deliberately *deterministic* — a cost class, not a wall clock.
//! Reading the host clock to decide how much to run would make the seeds that
//! executed depend on the machine that ran them, which is the property spec
//! §18.1 exists to deny, and `clippy.toml` bans the call outright in this crate.
//! Wall-time budgeting belongs to whatever drives the suite (`soak.yml` sizes
//! its sweeps per crate); it does not belong under a seed.
//!
//! Seeds stay independent under all three sizings, so a narrow run is a smaller
//! sample of the same corpus rather than a different one — never something else.
//!
//! # Cost classes
//!
//! Per-seed cost spans about four orders of magnitude across these workloads —
//! roughly a millisecond for a local actor sweep, seconds for one that touches
//! the disk or drives a machine. One local width cannot fit both, so the call
//! site says which it is: [`sweep_seeds`] for ordinary sweeps, [`slow_seeds`]
//! where a single seed costs seconds. [`coverage_seeds`] is not a cost class but
//! a correctness one — see its own note.
//!
//! # Environment
//!
//! - `SWARM_SEEDS=<n>` — take exactly `n` seeds, whatever the class. May exceed
//!   the declared width, which is how a soak asks for more than the corpus holds.
//! - `SWARM_SEEDS=full` (or `0`) — the declared width, the corpus as written.
//! - `SWARM_SEED_BASE=<n>` — offset every sweep by `n`. A soak sets this fresh
//!   per run so it explores seeds the corpus has never covered.
//! - `SWARM_CONTINUE=1` — do not stop at the first failing seed; run the whole
//!   sweep and report every seed that failed. See [`collect_all_failures`].
//!
//! Unset, a run under `CI` takes the declared width and a local run takes its
//! cost class's width.

use std::ops::Range;
use std::sync::Once;

/// Seeds per ordinary sweep in the edit loop.
const LOCAL_SEEDS: u64 = 8;

/// Seeds per expensive sweep in the edit loop. One seed is a smoke test — it
/// proves the workload still runs and its invariants still hold somewhere. The
/// corpus is CI's job, and the seed space is soak's.
const LOCAL_SLOW_SEEDS: u64 = 1;

const ENV_WIDTH: &str = "SWARM_SEEDS";
const ENV_BASE: &str = "SWARM_SEED_BASE";
const ENV_CONTINUE: &str = "SWARM_CONTINUE";

/// The seeds to sweep for a declared range whose seeds are cheap.
///
/// The declared range stays in the test as the corpus of record; this decides
/// how much of it — or how far past it — the run in hand covers. Offsetting by
/// `SWARM_SEED_BASE` keeps the range's shape, so a soak failure at
/// `(base, seed)` replays by setting the same two variables.
pub fn sweep_seeds(declared: Range<u64>) -> Range<u64> {
    resolve(declared, LOCAL_SEEDS)
}

/// The seeds to sweep for a declared range whose seeds cost seconds apiece —
/// workloads that touch the filesystem, a database, or a machine binding.
///
/// Same contract as [`sweep_seeds`]; only the local width differs, so one slow
/// workload cannot dominate the edit loop. CI and soak treat the two alike.
pub fn slow_seeds(declared: Range<u64>) -> Range<u64> {
    resolve(declared, LOCAL_SLOW_SEEDS)
}

/// The seeds to sweep for a fault-coverage assertion.
///
/// A coverage sweep asserts that each fault type fired at least once *across
/// the declared width* (spec §18.3). The claim is about the whole range, so
/// narrowing it would not weaken the assertion, it would make it mean something
/// the test does not say. These run at their declared width everywhere; only an
/// explicit `SWARM_SEEDS` can change that, and a soak that widens them is still
/// asserting something true.
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
        Width::Unset if is_ci() => declared_width,
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
/// `full` pins the corpus width everywhere, while saying nothing leaves the
/// choice to the run — the cost class locally, the corpus under CI.
enum Width {
    Explicit(u64),
    Declared,
    Unset,
}

fn width_request() -> Width {
    match std::env::var(ENV_WIDTH) {
        Ok(raw) => match raw.trim() {
            "full" | "0" => Width::Declared,
            // A malformed value is a typo on a command line, not a request to
            // silently drop to a narrower sweep: fall back to the declared
            // width, the reading that cannot under-test.
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

fn is_ci() -> bool {
    std::env::var_os("CI").is_some_and(|value| !value.is_empty())
}

/// Whether a sweep runs to the end and reports **every** failing seed, rather
/// than stopping at the first.
///
/// The two runs want opposite things here. CI wants the first failure and wants
/// it fast: the build is already red, and the seconds spent proving the other
/// 1,999 seeds also fail buy nothing. A soak wants the opposite — it is mining
/// the seed space for `(workload, seed)` pairs to write into `corpus.txt`, and a
/// sweep that halts on the first one throws away the rest of the run. Worse, it
/// compounds: once a corpus seed fails, the regression replay that runs *ahead*
/// of every sweep (`corpus.txt`) fails first, and that workload stops exploring
/// new ground entirely until someone fixes the bug.
///
/// So this is off by default — stop at the first failure, the behaviour every
/// call site was written against — and `soak.yml` turns it on.
pub fn collect_all_failures() -> bool {
    match std::env::var(ENV_CONTINUE) {
        Ok(raw) => !matches!(raw.trim(), "" | "0" | "false" | "no"),
        Err(_) => false,
    }
}

/// Say once per test binary how sweeps are sized, so a narrow run is never
/// mistaken for the corpus. Visible with `--nocapture` and on failure.
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
            Width::Unset if is_ci() => "the declared width".to_string(),
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

// --- Naming a sweep that is not a `Workload` ----------------------------------

/// Sweep a hand-built scenario across seeds under an explicit name.
///
/// Most sweeps drive a [`Workload`](crate::Workload), whose `name()` is the
/// corpus key. Some build a [`Simulation`](crate::Simulation) directly instead —
/// a scenario too bespoke to fit the trait — and those have no name, so nothing
/// could be recorded against them. This gives them one: the caller names the
/// scenario, and from there it ratchets like any other sweep.
///
/// ```ignore
/// scenario_sweep("partition-safety/quorum-reads", sweep_seeds(0..12), |seed| {
///     let sim = Simulation::new(seed);
///     // ... drive it, assert on the outcome ...
/// });
/// ```
///
/// The body panics to fail, as a test body does; the sweep stops at the first
/// seed that does — or, under [`collect_all_failures`], catches each panic, runs
/// the rest of the seeds, and fails at the end naming every one. Prefer a
/// `Workload` when the scenario fits one — the trait buys invariant checking and
/// reproducibility sweeps as well as a name.
pub fn scenario_sweep(name: &str, seeds: impl IntoIterator<Item = u64>, mut run: impl FnMut(u64)) {
    let seeds = crate::corpus::regression_seeds(name).chain(seeds);
    if !collect_all_failures() {
        for seed in seeds {
            run(seed);
        }
        return;
    }
    // A scenario body reports by panicking and has no `RunFailure` to return, so
    // collecting means catching the panic, keeping its message against the seed,
    // and re-raising the lot once the sweep is done.
    let mut failed: Vec<(u64, String)> = Vec::new();
    let mut seeds_run = 0;
    for seed in seeds {
        seeds_run += 1;
        let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(seed)));
        if let Err(payload) = attempt
            && !failed.iter().any(|(seen, _)| *seen == seed)
        {
            // One line per seed, even when the corpus replay and the sweep range
            // both reach it.
            failed.push((seed, crate::workload::panic_detail(payload.as_ref())));
        }
    }
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
    report.push_str("\ncorpus.txt lines for every seed above:\n\n");
    for (seed, _) in &failed {
        report.push_str(&format!("{name} {seed}\n"));
    }
    panic!("{report}");
}
