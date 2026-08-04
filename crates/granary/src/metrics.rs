//! What a deployment measures about a node's grains (spec §13).
//!
//! The [`Event`](actor_core::Event) stream (§13) is the *checker's* interface: it
//! carries one structured record per lifecycle transition so the simulator can assert
//! invariants over an exact history. That is the wrong shape for production. It emits
//! per message, its volume scales with traffic, and answering "is commit latency
//! rising?" from it means reconstructing a distribution from a firehose.
//!
//! This is the operator's interface: a fixed, small set of aggregates that cost O(1)
//! per operation and answer the questions an on-call engineer actually has — are
//! commits landing, how long are they taking, what does a cold access cost, how much
//! is resident. The two are complementary and neither replaces the other.
//!
//! What is *not* here is as deliberate. Leadership changes, splits and merges are
//! transitions, not rates: the event stream carries each exactly once, with the term
//! or the boundary attached, and a counter beside it would restate the same fact less
//! precisely.
//!
//! The vocabulary is closed on purpose. An open `incr(name, labels)` surface invites
//! unbounded label cardinality, which is the standard way a metrics pipeline falls
//! over; every metric here is either a counter or a fixed-bucket histogram, keyed by
//! nothing wider than a grain type. Not by shard: shard count is this substrate's
//! elasticity knob (§7.7), so a per-shard key is unbounded by design.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// How a commit ended (§6 steps 4–6) — the label on every commit measurement.
///
/// A latency number without this is misleading: a fast `Unavailable` and a fast
/// `Committed` mean opposite things, and averaging them hides both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    /// Durable on a quorum; the grain folded and replied (§6 step 4).
    Committed,
    /// Leadership moved off this node mid-append (§6 step 5).
    NotLeader,
    /// No quorum, or the commit timed out — the ambiguous outcome (§6 step 6, §11).
    Unavailable,
}

impl CommitOutcome {
    /// The label text used when rendering.
    fn as_str(self) -> &'static str {
        match self {
            CommitOutcome::Committed => "committed",
            CommitOutcome::NotLeader => "not_leader",
            CommitOutcome::Unavailable => "unavailable",
        }
    }
}

/// The operator-facing measurements a granary node reports (spec §13).
///
/// Implemented for `()` as a no-op, matching [`EventSink`](actor_core::EventSink), so
/// a system runs with no metrics wired up and pays only a virtual call.
pub trait GrainMetrics: Send + Sync + 'static {
    /// One command's durability round completed, with how long the append took and
    /// how it ended. The single most important signal a granary node produces: it is
    /// the latency the caller actually waited and the outcome the caller actually got.
    fn commit(&self, grain_type: &'static str, outcome: CommitOutcome, took: Duration);

    /// One activation rehydrated (§9): the quorum head recovery plus snapshot and
    /// replay. Separate from `commit` because it is the cost of a *cold* access, which
    /// is what hibernation tuning (§10) trades against memory.
    fn rehydrated(&self, grain_type: &'static str, took: Duration, replayed: u64);

    /// A grain activated (`+1`) or passivated (`-1`) on this node — the resident
    /// working set (§7.8), the thing hibernation is meant to bound.
    fn activation_delta(&self, grain_type: &'static str, delta: i64);
}

impl GrainMetrics for () {
    fn commit(&self, _: &'static str, _: CommitOutcome, _: Duration) {}
    fn rehydrated(&self, _: &'static str, _: Duration, _: u64) {}
    fn activation_delta(&self, _: &'static str, _: i64) {}
}

/// Latency bucket upper bounds, in microseconds.
///
/// Chosen around what the numbers mean rather than as a round decade ladder: a local
/// fsync lands in the low hundreds of microseconds, a healthy same-zone quorum in the
/// low milliseconds, a cross-zone one in the tens, and anything past a second is
/// heading for the append timeout (§11). The buckets are dense where the decisions
/// are.
const BUCKETS_US: [u64; 11] = [
    100,       // in-memory / page cache
    500,       // a local fsync on good storage
    1_000,     //
    5_000,     // a healthy same-zone quorum
    10_000,    //
    50_000,    // cross-zone, or a loaded device
    100_000,   //
    500_000,   //
    1_000_000, // a second: something is wrong
    2_000_000, // at the default append timeout
    u64::MAX,  // beyond it
];

/// A cumulative histogram plus its sum and count — the three things a rate/quantile
/// query needs, in the shape Prometheus expects.
#[derive(Default)]
struct Histogram {
    /// `le` buckets, **not** cumulative in storage; rendering accumulates them.
    buckets: [AtomicU64; BUCKETS_US.len()],
    sum_us: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    fn observe(&self, took: Duration) {
        let us = u64::try_from(took.as_micros()).unwrap_or(u64::MAX);
        let slot = BUCKETS_US.iter().position(|&b| us <= b).unwrap_or(0);
        self.buckets[slot].fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// `(le, cumulative_count)` pairs, plus the sum and total.
    fn snapshot(&self) -> (Vec<(u64, u64)>, u64, u64) {
        let mut running = 0;
        let points = BUCKETS_US
            .iter()
            .zip(&self.buckets)
            .map(|(&le, count)| {
                running += count.load(Ordering::Relaxed);
                (le, running)
            })
            .collect();
        (
            points,
            self.sum_us.load(Ordering::Relaxed),
            self.count.load(Ordering::Relaxed),
        )
    }
}

/// One histogram in Prometheus exposition form: the cumulative `le` buckets, then the
/// `_sum` and `_count` lines.
///
/// One copy, because the exposition format is a contract with the scraper and the
/// microsecond-to-second conversion is easy to get subtly wrong — two copies means a
/// fix that lands on one of them. `labels` is the label set every line carries, already
/// formatted and without braces (`grain_type="x",outcome="y"`); `le` is appended last,
/// so the caller's labels and the bucket label compose in the order they always had.
fn render_histogram(out: &mut String, metric: &str, labels: &str, hist: &Histogram) {
    let (points, sum_us, count) = hist.snapshot();
    for (le, cumulative) in points {
        let le = if le == u64::MAX {
            "+Inf".to_string()
        } else {
            format!("{:.6}", le as f64 / 1e6)
        };
        out.push_str(&format!(
            "{metric}_bucket{{{labels},le=\"{le}\"}} {cumulative}\n"
        ));
    }
    out.push_str(&format!(
        "{metric}_sum{{{labels}}} {:.6}\n{metric}_count{{{labels}}} {count}\n",
        sum_us as f64 / 1e6
    ));
}

/// One grain type's measurements.
#[derive(Default)]
struct TypeMetrics {
    commit_latency: [Histogram; 3],
    commits: [AtomicU64; 3],
    rehydrate_latency: Histogram,
    replayed: AtomicU64,
    /// Signed, but held unsigned: activations only ever exceed passivations, since a
    /// passivation follows an activation of the same grain (§10).
    active: AtomicU64,
}

/// A dependency-free [`GrainMetrics`] a deployment can scrape.
///
/// No metrics library: the whole surface is a handful of atomics and a text renderer,
/// which keeps the build offline-clean and costs one relaxed add per operation once
/// the type's entry is in hand.
///
/// **Getting that entry is the part worth being careful about.** Every commit,
/// rehydrate and activation on the node looks its grain type up here, and there is one
/// of these per *node*, shared across every type it hosts — so this lookup is on the
/// hot path of the whole process, taken from as many threads as the box has cores. The
/// map it looks in is effectively fixed: a type's entry is created by its first metric
/// and then never changes, so after the first commit of each type every subsequent
/// lookup is a pure read. An [`RwLock`](std::sync::RwLock) is what makes that read
/// shared; a `Mutex` would serialize every core through one lock to read a map that is
/// no longer being written.
#[derive(Default)]
pub struct AtomicGrainMetrics {
    types: std::sync::RwLock<std::collections::BTreeMap<&'static str, Arc<TypeMetrics>>>,
}

impl AtomicGrainMetrics {
    /// An empty registry.
    pub fn new() -> AtomicGrainMetrics {
        AtomicGrainMetrics::default()
    }

    /// This grain type's counters, creating them on the first metric it reports.
    ///
    /// Read-locked first and write-locked only on a miss, which happens once per grain
    /// type for the life of the node. The miss path re-checks under the write lock
    /// rather than assuming the entry is still absent: two threads can both miss the
    /// read and both take the write, and `or_default` on the second must find the
    /// first's entry rather than replace it — a replaced entry would silently reset
    /// counters and drop whatever the other thread had already recorded into its `Arc`.
    fn entry(&self, grain_type: &'static str) -> Arc<TypeMetrics> {
        if let Some(metrics) = self
            .types
            .read()
            .expect("grain metrics poisoned")
            .get(grain_type)
        {
            return Arc::clone(metrics);
        }
        Arc::clone(
            self.types
                .write()
                .expect("grain metrics poisoned")
                .entry(grain_type)
                .or_default(),
        )
    }

    /// Render every metric in the Prometheus text exposition format, ready to serve
    /// from a `/metrics` endpoint.
    ///
    /// Rendering takes a consistent view per metric but not across metrics; a scrape
    /// is a sample, not a transaction, and Prometheus already assumes as much.
    pub fn render(&self) -> String {
        let types: Vec<(&'static str, Arc<TypeMetrics>)> = {
            // A read lock: a scrape must not stand between a commit and its counter.
            let guard = self.types.read().expect("grain metrics poisoned");
            guard.iter().map(|(&k, v)| (k, Arc::clone(v))).collect()
        };
        let mut out = String::new();
        out.push_str(
            "# HELP granary_commit_seconds Time a grain command waited for its append to commit.\n\
             # TYPE granary_commit_seconds histogram\n",
        );
        for (name, m) in &types {
            for (idx, outcome) in [
                CommitOutcome::Committed,
                CommitOutcome::NotLeader,
                CommitOutcome::Unavailable,
            ]
            .into_iter()
            .enumerate()
            {
                render_histogram(
                    &mut out,
                    "granary_commit_seconds",
                    &format!("grain_type=\"{name}\",outcome=\"{}\"", outcome.as_str()),
                    &m.commit_latency[idx],
                );
            }
        }
        out.push_str(
            "# HELP granary_commits_total Grain command durability rounds by outcome.\n\
             # TYPE granary_commits_total counter\n",
        );
        for (name, m) in &types {
            for (idx, outcome) in [
                CommitOutcome::Committed,
                CommitOutcome::NotLeader,
                CommitOutcome::Unavailable,
            ]
            .into_iter()
            .enumerate()
            {
                out.push_str(&format!(
                    "granary_commits_total{{grain_type=\"{name}\",outcome=\"{}\"}} {}\n",
                    outcome.as_str(),
                    m.commits[idx].load(Ordering::Relaxed)
                ));
            }
        }
        out.push_str(
            "# HELP granary_rehydrate_seconds Time an activation spent recovering and replaying.\n\
             # TYPE granary_rehydrate_seconds histogram\n",
        );
        for (name, m) in &types {
            render_histogram(
                &mut out,
                "granary_rehydrate_seconds",
                &format!("grain_type=\"{name}\""),
                &m.rehydrate_latency,
            );
        }
        out.push_str(
            "# HELP granary_records_replayed_total Records folded on activation.\n\
             # TYPE granary_records_replayed_total counter\n",
        );
        for (name, m) in &types {
            out.push_str(&format!(
                "granary_records_replayed_total{{grain_type=\"{name}\"}} {}\n",
                m.replayed.load(Ordering::Relaxed)
            ));
        }
        out.push_str(
            "# HELP granary_active_grains Grains currently activated on this node.\n\
             # TYPE granary_active_grains gauge\n",
        );
        for (name, m) in &types {
            out.push_str(&format!(
                "granary_active_grains{{grain_type=\"{name}\"}} {}\n",
                m.active.load(Ordering::Relaxed)
            ));
        }
        out
    }
}

impl GrainMetrics for AtomicGrainMetrics {
    fn commit(&self, grain_type: &'static str, outcome: CommitOutcome, took: Duration) {
        let m = self.entry(grain_type);
        let idx = match outcome {
            CommitOutcome::Committed => 0,
            CommitOutcome::NotLeader => 1,
            CommitOutcome::Unavailable => 2,
        };
        m.commit_latency[idx].observe(took);
        m.commits[idx].fetch_add(1, Ordering::Relaxed);
    }

    fn rehydrated(&self, grain_type: &'static str, took: Duration, replayed: u64) {
        let m = self.entry(grain_type);
        m.rehydrate_latency.observe(took);
        m.replayed.fetch_add(replayed, Ordering::Relaxed);
    }

    fn activation_delta(&self, grain_type: &'static str, delta: i64) {
        let m = self.entry(grain_type);
        if delta >= 0 {
            m.active.fetch_add(delta.unsigned_abs(), Ordering::Relaxed);
        } else {
            // Saturating, not wrapping: a passivation without a matching activation
            // is a bug in the caller, and a gauge that wraps to u64::MAX turns that
            // bug into a page.
            let _ = m
                .active
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                    Some(cur.saturating_sub(delta.unsigned_abs()))
                });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commits_are_counted_and_bucketed_by_outcome() {
        let m = AtomicGrainMetrics::new();
        m.commit(
            "test.Grain",
            CommitOutcome::Committed,
            Duration::from_micros(400),
        );
        m.commit(
            "test.Grain",
            CommitOutcome::Committed,
            Duration::from_millis(3),
        );
        m.commit(
            "test.Grain",
            CommitOutcome::Unavailable,
            Duration::from_secs(2),
        );
        let text = m.render();
        assert!(
            text.contains(
                "granary_commits_total{grain_type=\"test.Grain\",outcome=\"committed\"} 2"
            )
        );
        assert!(
            text.contains(
                "granary_commits_total{grain_type=\"test.Grain\",outcome=\"unavailable\"} 1"
            ),
            "a failed commit is counted separately, not averaged into the good ones"
        );
        // Cumulative: the 500us bucket holds the 400us sample, the 5ms bucket both.
        assert!(text.contains("outcome=\"committed\",le=\"0.000500\"} 1"));
        assert!(text.contains("outcome=\"committed\",le=\"0.005000\"} 2"));
    }

    #[test]
    fn the_active_gauge_tracks_activations_and_never_wraps() {
        let m = AtomicGrainMetrics::new();
        m.activation_delta("test.Grain", 1);
        m.activation_delta("test.Grain", 1);
        m.activation_delta("test.Grain", -1);
        assert!(
            m.render()
                .contains("granary_active_grains{grain_type=\"test.Grain\"} 1")
        );
        // An unmatched passivation must not underflow into a gauge reading 1.8e19.
        m.activation_delta("test.Grain", -5);
        assert!(
            m.render()
                .contains("granary_active_grains{grain_type=\"test.Grain\"} 0")
        );
    }

    #[test]
    fn a_types_first_metric_from_many_threads_at_once_loses_none_of_them() {
        // The lookup reads under a shared lock and only takes the write lock on a
        // miss, so several threads reporting a grain type's *first* metric can all
        // miss the read and all reach the write. If the miss path replaced the entry
        // instead of re-checking, every thread but the last would increment an `Arc`
        // no longer in the map and its commits would vanish — a race that only shows
        // on the first touch of a type, which is exactly when a node is starting and
        // nobody is looking at the numbers.
        //
        // Two things make this a test rather than a coin flip. The barrier, because
        // otherwise the first thread spawned wins the write and installs the entry
        // before the others start, leaving every later lookup an uncontended read hit
        // and the miss path never raced. And the repetition, because even barriered
        // the threads only collide inside the miss window some of the time — against a
        // deliberately broken `entry` a single round catches it only a few percent of
        // the time, which is a detector nobody should rely on. Measured against that
        // broken version, 64 rounds caught it in five runs out of six and 512 in every
        // run; correct code passes every round, so the repetition costs a quarter of a
        // second and is not a source of flakiness in the direction that matters.
        const ROUNDS: usize = 512;
        const THREADS: u64 = 8;
        const EACH: u64 = 50;
        for _ in 0..ROUNDS {
            let m = Arc::new(AtomicGrainMetrics::new());
            let gate = Arc::new(std::sync::Barrier::new(THREADS as usize));
            std::thread::scope(|scope| {
                for _ in 0..THREADS {
                    let (m, gate) = (Arc::clone(&m), Arc::clone(&gate));
                    scope.spawn(move || {
                        gate.wait();
                        for _ in 0..EACH {
                            m.commit(
                                "test.Racy",
                                CommitOutcome::Committed,
                                Duration::from_micros(100),
                            );
                        }
                    });
                }
            });
            assert!(
                m.render().contains(&format!(
                    "granary_commits_total{{grain_type=\"test.Racy\",outcome=\"committed\"}} {}",
                    THREADS * EACH
                )),
                "every commit must land in the one entry the type ends up with",
            );
        }
    }

    #[test]
    fn the_no_op_sink_costs_nothing_and_compiles_as_a_sink() {
        let sink: Arc<dyn GrainMetrics> = Arc::new(());
        sink.commit("t", CommitOutcome::Committed, Duration::from_secs(1));
        sink.rehydrated("t", Duration::from_secs(1), 5);
        sink.activation_delta("t", 1);
    }
}
