//! Seed-reproducibility: the determinism contract, enforced (spec §18.1 #1).
//!
//! The bedrock property of deterministic simulation is that a seed reproduces an
//! entire run *exactly*: two runs of the same workload under the same seed MUST
//! produce byte-identical event streams (spec §18.1 #1). Everything else — replaying a
//! failure from its seed, shrinking, a regression corpus — rests on it. A single
//! leak of ambient nondeterminism (a wall-clock read, an OS thread, a non-seeded
//! RNG, `HashMap` iteration order) silently breaks it, so it deserves to be
//! checked directly rather than assumed.
//!
//! This module runs a [`Workload`] or [`ClusterWorkload`] twice under one seed
//! with a [`Recorder`] on each, then diffs the two [`Event`] streams. The first
//! point of divergence is reported as a [`Divergence`] — the exact index and the
//! two differing events — so a determinism regression names itself instead of
//! showing up as a mysterious flaky swarm. [`check_reproducible`] and
//! [`check_cluster_reproducible`] are the single-run gates; [`replay_swarm`] and
//! [`replay_cluster_swarm`] sweep them across seeds, the standing determinism
//! corpus (spec §18.6).

use std::sync::Arc;

use actor_core::Event;

use crate::ClusterWorkload;
use crate::Recorder;
use crate::Workload;
use crate::cluster_swarm::drive_cluster;
use crate::workload::drive_local;

/// The first point at which two same-seed runs disagreed (spec §18.1 #1). A
/// `None` event means that run's stream ended first.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    /// The total length of each run's stream — a quick signal of how far they
    /// agreed before diverging.
    pub left_len: usize,
    pub right_len: usize,
}

impl std::fmt::Display for Divergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

/// Diff two event streams, returning the first index at which they differ.
fn first_divergence(
    workload: &'static str,
    seed: u64,
    left: &[Event],
    right: &[Event],
) -> Option<Divergence> {
    let common = left.len().min(right.len());
    for i in 0..common {
        if left[i] != right[i] {
            return Some(Divergence {
                workload,
                seed,
                index: i,
                left: Some(left[i].clone()),
                right: Some(right[i].clone()),
                left_len: left.len(),
                right_len: right.len(),
            });
        }
    }
    if left.len() != right.len() {
        return Some(Divergence {
            workload,
            seed,
            index: common,
            left: left.get(common).cloned(),
            right: right.get(common).cloned(),
            left_len: left.len(),
            right_len: right.len(),
        });
    }
    None
}

/// Run a workload twice under one seed and diff the two event streams, reporting
/// the first [`Divergence`] (spec §18.1 #1). The `record` closure captures which
/// driver — single-node or cluster — produced the stream, so the determinism
/// check itself lives once for both. Boxed because a `Divergence` carries two
/// full events and dwarfs the `Ok` path.
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
/// divergence — the standing reproducibility corpus (spec §18.6).
fn sweep(
    seeds: impl IntoIterator<Item = u64>,
    mut check: impl FnMut(u64) -> Result<(), Box<Divergence>>,
) -> Result<(), Box<Divergence>> {
    for seed in seeds {
        check(seed)?;
    }
    Ok(())
}

/// Record the event stream of a single-node workload run under `seed`.
pub fn record_seed<W: Workload>(workload: &W, seed: u64) -> Vec<Event> {
    let recorder = Recorder::new();
    drive_local(workload, seed, Arc::new(recorder.clone()));
    recorder.events()
}

/// Verify the determinism contract for a single-node workload at `seed`: two runs
/// must emit byte-identical event streams (spec §18.1 #1). Returns the first
/// [`Divergence`] otherwise (boxed — it carries two full events, so it dwarfs the
/// `Ok` path).
pub fn check_reproducible<W: Workload>(workload: &W, seed: u64) -> Result<(), Box<Divergence>> {
    check_twice(workload.name(), seed, |s| record_seed(workload, s))
}

/// Sweep the determinism contract across seeds for a single-node workload — the
/// standing reproducibility corpus (spec §18.6). Stops at the first divergence.
pub fn replay_swarm<W: Workload>(
    workload: &W,
    seeds: impl IntoIterator<Item = u64>,
) -> Result<(), Box<Divergence>> {
    sweep(seeds, |s| check_reproducible(workload, s))
}

/// Record the event stream of a cluster workload run under `seed`. The cluster
/// stream interleaves every node's events; reproducing it byte-for-byte is the
/// stronger contract, because it pins down multi-node scheduling, the seeded
/// transport faults, and the nemesis all at once.
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
/// §18.6). Stops at the first divergence.
pub fn replay_cluster_swarm<W: ClusterWorkload>(
    workload: &W,
    seeds: impl IntoIterator<Item = u64>,
) -> Result<(), Box<Divergence>> {
    sweep(seeds, |s| check_cluster_reproducible(workload, s))
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
}
