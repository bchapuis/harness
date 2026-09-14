//! Seed-reproducibility: the determinism contract, enforced (spec §18.1 #1).
//!
//! Two runs of the same workload under the same seed MUST produce byte-identical
//! event streams (spec §18.1 #1). A single leak of ambient nondeterminism (a
//! wall-clock read, an OS thread, a non-seeded RNG, `HashMap` iteration order)
//! silently breaks it, so it is checked directly rather than assumed.
//!
//! This module runs a [`Workload`] or [`ClusterWorkload`] twice under one seed
//! with a [`Recorder`] on each, then diffs the two [`Event`] streams, reporting
//! the first point of divergence as a [`Divergence`].
//! [`check_reproducible`] and [`check_cluster_reproducible`] are the single-run
//! gates; [`replay_swarm`] and [`replay_cluster_swarm`] sweep them across seeds,
//! the standing determinism corpus (spec §18.6).

use std::sync::Arc;

use actor_core::Event;

use crate::ClusterWorkload;
use crate::Recorder;
use crate::Workload;
use crate::cluster_swarm::drive_cluster;
use crate::workload::drive_local;

/// The first point at which two same-seed runs disagreed (spec §18.1 #1). A
/// `None` event means that run's stream ended first.
///
/// A sweep also reports a workload that panicked while being recorded as one of
/// these, so a soak attributes the panic to its seed and carries on: `detail`
/// then carries the panic message, and there are no events to show.
#[derive(Clone, PartialEq, Eq)]
pub struct Divergence {
    /// The name of the workload that diverged.
    pub workload: &'static str,
    /// The seed under which the two runs disagreed.
    pub seed: u64,
    /// The index of the first differing event (or the length of the shorter
    /// stream, if one run simply emitted fewer events).
    pub index: usize,
    /// The event the first run emitted at `index` (`None` if it ended early).
    pub left: Option<Event>,
    /// The event the second run emitted at `index` (`None` if it ended early).
    pub right: Option<Event>,
    /// The total length of each run's stream.
    pub left_len: usize,
    pub right_len: usize,
    /// The panic message, when the workload panicked under this seed instead
    /// of producing two streams to compare.
    pub detail: Option<String>,
}

impl std::fmt::Display for Divergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(detail) = &self.detail {
            return write!(
                f,
                "workload '{}' panicked at seed {} while being recorded: {detail}",
                self.workload, self.seed
            );
        }
        write!(
            f,
            "workload '{}' is non-deterministic at seed {}: event streams of \
             length {} and {} first differ at index {}\n  run A: {:?}\n  run B: {:?}",
            self.workload,
            self.seed,
            self.left_len,
            self.right_len,
            self.index,
            self.left,
            self.right,
        )
    }
}

impl std::error::Error for Divergence {}

/// `Debug` is the report too (see `RunFailure`), so `.unwrap()` prints the
/// crafted account rather than two dumped events.
impl std::fmt::Debug for Divergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

/// Diff two event streams, returning the first index at which they differ:
/// past the shorter stream's end when one is a prefix of the other, so a `get`
/// there is the `None` the report shows for the run that ended early.
fn first_divergence(
    workload: &'static str,
    seed: u64,
    left: &[Event],
    right: &[Event],
) -> Option<Divergence> {
    (0..left.len().max(right.len()))
        .find(|&i| left.get(i) != right.get(i))
        .map(|index| Divergence {
            workload,
            seed,
            index,
            left: left.get(index).cloned(),
            right: right.get(index).cloned(),
            left_len: left.len(),
            right_len: right.len(),
            detail: None,
        })
}

/// Check one seed, turning a panic into a [`Divergence`] against that seed (see
/// [`caught`](crate::workload::caught) for the invariant sweeps' twin): a
/// workload that asserts inside `drive` names no seed on its own, and an
/// uncaught panic would end a collecting sweep at that seed.
fn caught_divergence(
    workload: &'static str,
    seed: u64,
    check: impl FnOnce() -> Result<(), Box<Divergence>>,
) -> Result<(), Box<Divergence>> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(check)) {
        Ok(result) => result,
        Err(payload) => Err(Box::new(Divergence {
            workload,
            seed,
            index: 0,
            left: None,
            right: None,
            left_len: 0,
            right_len: 0,
            detail: Some(crate::workload::panic_detail(payload.as_ref())),
        })),
    }
}

/// Run a workload twice under one seed and diff the two event streams, reporting
/// the first [`Divergence`] (spec §18.1 #1). The `record` closure supplies the
/// driver, single-node or cluster. Boxed because a `Divergence` carries two full
/// events and dwarfs the `Ok` path.
fn check_twice(
    name: &'static str,
    seed: u64,
    record: impl Fn(u64) -> Vec<Event>,
) -> Result<(), Box<Divergence>> {
    let first = record(seed);
    let second = record(seed);
    match first_divergence(name, seed, &first, &second) {
        Some(d) => Err(Box::new(d)),
        None => Ok(()),
    }
}

/// Sweep a per-seed determinism check across many seeds, stopping at the first
/// divergence, or, when `collect`, running to the end and reporting every seed
/// that diverged or panicked. The standing reproducibility corpus (spec §18.6).
///
/// The workload's [`regression_seeds`](crate::regression_seeds) run ahead of
/// `seeds`, exactly as they do for an invariant sweep: a seed that once broke
/// determinism is checked on every run, however narrow the sweep. `collect` is
/// a parameter so the loop is a pure function of its inputs; the public runners
/// read the environment once, at the edge.
fn sweep(
    workload: &'static str,
    seeds: impl IntoIterator<Item = u64>,
    collect: bool,
    mut check: impl FnMut(u64) -> Result<(), Box<Divergence>>,
) -> Result<(), Box<SweepDivergence>> {
    let (seeds_run, divergences) = crate::sweep::sweep_generic::<Box<Divergence>>(
        crate::corpus::regression_seeds(workload).chain(seeds),
        collect,
        |divergence| divergence.seed,
        // Swept boxed (the per-seed result stays thin), and unboxed only on
        // the cold path below, where divergence is already the whole story.
        |seed| caught_divergence(workload, seed, || check(seed)),
    );
    if divergences.is_empty() {
        Ok(())
    } else {
        Err(Box::new(SweepDivergence {
            seeds_run,
            divergences: divergences.into_iter().map(|d| *d).collect(),
        }))
    }
}

/// Every seed at which a reproducibility sweep found a divergence. Boxed by its
/// callers: each [`Divergence`] carries two full events, so the error dwarfs the
/// `Ok` path.
#[derive(Clone, PartialEq, Eq)]
pub struct SweepDivergence {
    /// How many seeds ran, including the ones that diverged.
    pub seeds_run: u64,
    /// The diverging seeds, in sweep order. Never empty.
    pub divergences: Vec<Divergence>,
}

impl std::fmt::Display for SweepDivergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // One divergence prints exactly as a bare `Divergence`.
        if let [only] = self.divergences.as_slice() {
            return write!(f, "{only}");
        }
        writeln!(
            f,
            "{} of the {} seeds run are non-deterministic:\n",
            self.divergences.len(),
            self.seeds_run
        )?;
        for divergence in &self.divergences {
            writeln!(f, "{divergence}\n")?;
        }
        f.write_str(&crate::corpus::corpus_block(
            self.divergences.iter().map(|d| (d.workload, d.seed)),
        ))
    }
}

impl std::error::Error for SweepDivergence {}

/// `Debug` is the report too (see `RunFailure`).
impl std::fmt::Debug for SweepDivergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

/// Record the event stream of a single-node workload run under `seed`.
pub(crate) fn record_seed<W: Workload>(workload: &W, seed: u64) -> Vec<Event> {
    let recorder = Recorder::new();
    drive_local(workload, seed, Arc::new(recorder.clone()));
    recorder.events()
}

/// Verify the determinism contract for a single-node workload at `seed`: two runs
/// must emit byte-identical event streams (spec §18.1 #1). Returns the first
/// [`Divergence`] otherwise (boxed: it carries two full events, so it dwarfs the
/// `Ok` path).
pub fn check_reproducible<W: Workload>(workload: &W, seed: u64) -> Result<(), Box<Divergence>> {
    check_twice(workload.name(), seed, |s| record_seed(workload, s))
}

/// Sweep the determinism contract across seeds for a single-node workload: the
/// standing reproducibility corpus (spec §18.6). Stops at the first divergence
/// unless the run asks for every one.
pub fn replay_swarm<W: Workload>(
    workload: &W,
    seeds: impl IntoIterator<Item = u64>,
) -> Result<(), Box<SweepDivergence>> {
    sweep(
        workload.name(),
        seeds,
        crate::sweep::collect_all_failures(),
        |s| check_reproducible(workload, s),
    )
}

/// Record the event stream of a cluster workload run under `seed`. The cluster
/// stream interleaves every node's events, so reproducing it byte-for-byte pins
/// down multi-node scheduling, the seeded transport faults, and the nemesis at
/// once.
pub fn record_cluster_seed<W: ClusterWorkload>(workload: &W, seed: u64) -> Vec<Event> {
    let recorder = Recorder::new();
    drive_cluster(workload, seed, Arc::new(recorder.clone()));
    recorder.events()
}

/// Verify the determinism contract for a cluster workload at `seed` (spec
/// §18.1 #1): two runs over the multi-node network, seeded faults, and nemesis
/// must emit byte-identical event streams.
pub fn check_cluster_reproducible<W: ClusterWorkload>(
    workload: &W,
    seed: u64,
) -> Result<(), Box<Divergence>> {
    check_twice(workload.name(), seed, |s| record_cluster_seed(workload, s))
}

/// Sweep the determinism contract across seeds for a cluster workload (spec
/// §18.6). Stops at the first divergence unless the run asks for every one.
pub fn replay_cluster_swarm<W: ClusterWorkload>(
    workload: &W,
    seeds: impl IntoIterator<Item = u64>,
) -> Result<(), Box<SweepDivergence>> {
    sweep(
        workload.name(),
        seeds,
        crate::sweep::collect_all_failures(),
        |s| check_cluster_reproducible(workload, s),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use actor_core::ActorId;
    use actor_core::NodeId;
    use actor_core::Path;

    fn ev(n: u64) -> Event {
        Event::AssignId {
            id: ActorId::new(NodeId::new(0), Path::new(format!("/user/{n}")), 0),
        }
    }

    #[test]
    fn identical_streams_do_not_diverge() {
        let a = vec![ev(0), ev(1), ev(2)];
        assert_eq!(first_divergence("w", 0, &a, &a.clone()), None);
    }

    #[test]
    fn a_differing_event_is_pinpointed() {
        let a = vec![ev(0), ev(1), ev(2)];
        let b = vec![ev(0), ev(9), ev(2)];
        let d = first_divergence("w", 0, &a, &b).expect("streams differ");
        assert_eq!(d.index, 1);
        assert_eq!(d.left, Some(ev(1)));
        assert_eq!(d.right, Some(ev(9)));
    }

    #[test]
    fn a_truncated_stream_is_caught() {
        let a = vec![ev(0), ev(1)];
        let b = vec![ev(0)];
        let d = first_divergence("w", 0, &a, &b).expect("streams differ in length");
        assert_eq!(d.index, 1);
        assert_eq!(d.left, Some(ev(1)));
        assert_eq!(d.right, None);
        assert_eq!((d.left_len, d.right_len), (2, 1));
    }

    #[test]
    fn a_panicking_seed_is_caught_and_named() {
        let failure = sweep("w", 0..4, true, |seed| {
            assert_ne!(seed, 2, "the workload asserted");
            Ok(())
        })
        .expect_err("seed 2 panics");
        assert_eq!(failure.seeds_run, 4, "a panic does not end the sweep");
        let [only] = failure.divergences.as_slice() else {
            panic!("one seed panicked: {failure}");
        };
        assert_eq!(only.seed, 2);
        assert_eq!((only.left.as_ref(), only.right.as_ref()), (None, None));
        let report = only.to_string();
        assert!(
            report.contains("panicked at seed 2") && report.contains("the workload asserted"),
            "the panic message is kept: {report}"
        );
    }
}
