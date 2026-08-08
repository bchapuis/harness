//! Durable workflows under the cluster fault swarm (granary §7.17).
//!
//! `workflow.rs` asserts the workflow facet's properties through *scripted*
//! scenarios on one node. This file applies the one that matters most the V&V way:
//! a [`ClusterWorkload`] swept across seeds while the nemesis injects partitions,
//! crashes, restarts, pauses, heals, loss, duplication, and delay.
//!
//! **The property is that the memo is write-once.** It is *not* "the effect ran
//! once": [`granary::LaunchGuard`] is per-activation and never journaled, so a
//! re-activation legitimately re-launches an unresolved step and the effect may
//! run many times (§7.17). What must hold is that `complete_step` records only a
//! step that is not already done — so the **first committed result wins**, and every
//! later drive, re-activation and failover resolves from it.
//!
//! **What makes it observable.** The fixture's `fetch` effect returns a fresh
//! ordinal on every launch (`support::pipeline`), so a memo that was overwritten
//! reads differently from one that was kept; a constant-valued effect cannot tell
//! the two apart. The check is therefore over *observations*: two reads of one
//! grain's memo that both answer `Some` must answer the same `Some`. That needs no
//! knowledge of which launch won, which is what makes it decidable under faults —
//! a read the nemesis refuses is simply not an observation.
//!
//! **What makes it reachable.** The property needs a chain — commit a step, be
//! interrupted, re-launch, still be readable — and the seeds that get furthest are
//! the calm ones, which are exactly the seeds that never re-launch. So the
//! interruption is **driven rather than waited for**: the grain may hibernate with
//! a step in flight, the effect takes longer than the idle window, and the read
//! rounds below both observe the memo and re-activate a cold grain whose step is
//! still outstanding — which is what re-launches it. The nemesis then adds
//! failovers and process deaths on top. An earlier attempt left the interruption to
//! the nemesis alone and measured roughly two seeds in twenty-four observing a memo
//! at all, none of which had re-launched; that workload was never committed.
//!
//! **What it still cannot reach, measured rather than assumed.** Driving the chain
//! does not make every seed usable: on roughly two seeds in five the nemesis leaves
//! the shard without a leader this node can route to for the whole run, every read
//! comes back `NotLeader`, and the run observes nothing at all. That is why the
//! coverage assertions sit on their own `coverage_seeds` sweep below rather than on
//! the invariant sweep, which narrows to 8 seeds locally. Both are green across the
//! declared 24, and the invariant sweep was additionally run clean across 200.
//!
//! **Why there is no `sleep` step.** The fixture's alarm-backed sleep is switched
//! off here (`PipelineConfig::sleep: None`). A sleep needs a hosted `AlarmIndex`
//! whose per-shard driver never quiesces, which costs two default checkers
//! (`alarm_swarm.rs` carries that argument) and buys this check nothing: write-once
//! is decided by `fetch`'s first commit, and everything after it is a longer chain
//! to the same claim. Shortening the chain is what got this sweep landed.

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Event;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::Rehost;
use actor_simulation::SimNode;
use actor_simulation::coverage_seeds;
use actor_simulation::default_invariants;
use actor_simulation::replay_cluster_swarm;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_cluster_swarm_coverage;
use actor_simulation::sweep_seeds;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::testing::CommitMonotonic;

use support::exercised::Exercised;
use support::pipeline::Effects;
use support::pipeline::PipelineConfig;
use support::pipeline::ReadMemo;
use support::pipeline::STEP_FETCH;

/// The `Pipeline` at this suite's tier.
type Pipeline = support::pipeline::Pipeline<SimNode>;

const SHARDS: usize = 2;

/// How many workflow grains one run drives.
const GRAINS: usize = 6;

/// How long a step's effect takes before it self-`tell`s its result back. Longer
/// than [`IDLE_AFTER`] by enough that a grain reliably hibernates under it, which
/// is what leaves a step outstanding for a later touch to re-launch.
const EFFECT_LATENCY: Duration = Duration::from_millis(800);

/// The hibernation window. Short, and crossed on a fixed cadence rather than a
/// seeded coin, so a re-launch is something this workload drives at any width
/// (docs/simulation-testing.md, *Adding a sweep*).
const IDLE_AFTER: Duration = Duration::from_millis(150);

/// Per-call deadline. Short so a refused read fails fast and the run keeps moving.
const CALL_DEADLINE: Duration = Duration::from_secs(3);

/// How many read rounds a run makes over every grain. Each round is both an
/// observation and — on a grain whose step is still outstanding — the touch that
/// re-launches it.
const ROUNDS: usize = 16;

/// The gap between read rounds. Comfortably past [`IDLE_AFTER`] and well short of
/// [`EFFECT_LATENCY`], which is the window a re-launch happens in.
const ROUND_GAP: Duration = Duration::from_millis(500);

// --- The write-once check -----------------------------------------------------

/// Every memo value one run observed, per grain, plus the sweep-wide tallies that
/// say how much it managed to observe. Filled by `drive`; checked by
/// [`MemoWriteOnce`] once the run is quiescent.
#[derive(Clone, Default)]
struct Memos {
    /// This run's `grain key → observed memo values`, in the order they were read.
    /// Cleared at the start of each `drive`, so a check sees one run's observations
    /// rather than the sweep's — the run-scoped *expectation* the shared
    /// `Exercised` tallies below are deliberately not.
    run: Arc<Mutex<BTreeMap<String, Vec<u32>>>>,
    /// Grains whose memo was read at least once, across the whole sweep — the guard
    /// against a green run that observed nothing.
    observed: Arc<AtomicUsize>,
    /// Grains that were both **re-launched and observed** in the same run, across
    /// the sweep. This is the chain the property needs: without it the sweep would
    /// be asserting write-once over memos nothing ever tried to overwrite.
    chained: Arc<AtomicUsize>,
}

impl Memos {
    fn observations(&self) -> usize {
        self.observed.load(Ordering::Relaxed)
    }

    fn chains(&self) -> usize {
        self.chained.load(Ordering::Relaxed)
    }

    fn record(&self, key: &str, value: u32) {
        self.run
            .lock()
            .expect("memo mutex poisoned")
            .entry(key.to_string())
            .or_default()
            .push(value);
        self.observed.fetch_add(1, Ordering::Relaxed);
    }
}

/// **The memo is write-once**, checked at quiescence against what the run read
/// back.
///
/// A checker rather than a bare assertion in `drive` so a violation is reported
/// through the same channel as every other invariant, naming the seed to replay.
struct MemoWriteOnce {
    memos: Memos,
}

impl Invariant for MemoWriteOnce {
    fn name(&self) -> &'static str {
        "workflow-memo-write-once"
    }

    fn observe(&mut self, _event: &Event) -> Result<(), String> {
        Ok(())
    }

    fn at_quiescence(&mut self) -> Result<(), String> {
        let run = self.memos.run.lock().expect("memo mutex poisoned");
        let changed: Vec<(&String, &Vec<u32>)> = run
            .iter()
            .filter(|(_, seen)| seen.iter().any(|v| *v != seen[0]))
            .collect();
        if !changed.is_empty() {
            return Err(format!(
                "a committed step memo changed after the fact (§7.17): {changed:?} — \
                 `complete_step` must record only a step that is not already done, so \
                 every read after the first commit resolves to that first result",
            ));
        }
        Ok(())
    }
}

// --- The workload -------------------------------------------------------------

struct WorkflowSwarm {
    nodes: usize,
    fx: Effects,
    memos: Memos,
    exercised: Exercised,
}

fn config() -> GranaryConfig {
    GranaryConfig {
        shards: SHARDS,
        idle_after: IDLE_AFTER,
        // Every commit checkpoints. A run commits only a handful of records per
        // grain — the two step memos and the terminal event — so a wider trigger
        // would leave every rehydration replaying from an empty base and the
        // snapshot-restore path untested (`Exercised::assert_hibernated`).
        snapshot_every: 1,
        ..GranaryConfig::default()
    }
}

/// The shape this sweep hosts: no alarm-backed sleep, hibernating under an
/// in-flight step, and an effect slow enough to still be outstanding when the next
/// touch arrives.
fn shape() -> PipelineConfig {
    PipelineConfig {
        sleep: None,
        hibernate_mid_workflow: true,
        effect_latency: Some(EFFECT_LATENCY),
    }
}

fn host(node: &SimNode, fx: &Effects) -> Granary<Pipeline> {
    node.granary_named::<Pipeline>(
        support::pipeline::PIPELINE_TYPE,
        config(),
        support::pipeline::Pipeline::factory(fx.clone(), shape()),
    )
}

impl ClusterWorkload for WorkflowSwarm {
    fn name(&self) -> &'static str {
        "granary-workflow-swarm"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn rehost(&self) -> Option<Rehost> {
        // A restarted node comes up empty: it no longer hosts `Pipeline`, so without
        // this the run would shrink the cluster rather than fault it.
        let fx = self.fx.clone();
        Some(Arc::new(move |node: &SimNode| {
            host(node, &fx);
        }))
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(300),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        // Granary requires the leader-based control plane to host the shard map
        // (§7.6); every node is a control voter so the map group can form.
        ClusterModeSpec::Leader {
            swim: self.swim(),
            voters: self.nodes,
            election_timeout: Duration::from_millis(500),
            heartbeat_interval: Duration::from_millis(100),
            downing: DowningPolicy::Conservative,
        }
    }

    fn setup(&self, _ctx: &ClusterCtx) {}

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let fx = self.fx.clone();
        let memos = self.memos.clone();
        Box::pin(async move {
            // One run's observations and one run's launch ordinals, not the sweep's.
            // Both are emptied here, before any grain can be touched: the ordinals
            // must be a function of the seed alone or a reproducibility replay would
            // hand the same run different step values on its second pass.
            memos.run.lock().expect("memo mutex poisoned").clear();
            fx.reset();

            let hosts: Vec<Granary<Pipeline>> = nodes.iter().map(|n| host(n, &fx)).collect();
            let clock = nodes[0].clock().clone();
            // Let the control-plane and shard groups elect before traffic.
            clock.sleep(Duration::from_secs(3)).await;

            // Read rounds, every one of them in flight at once and each opening at
            // a fixed offset from the last. Each read is three things: the touch
            // that *starts* a workflow on a grain never seen before, the
            // re-activation that re-launches a step left outstanding by a
            // hibernation, and the observation the check is made of. A read the
            // faults refuse is simply not an observation — write-once is a claim
            // about what committed, and a grain this node cannot reach committed
            // whatever it committed either way.
            //
            // **The cadence must not depend on the reads.** Awaiting a round before
            // starting the next one lets a refused read stretch a [`ROUND_GAP`]
            // round into a [`CALL_DEADLINE`] one, and the whole drive shares one 30 s
            // virtual-time budget — so on exactly the faulty seeds this sweep is
            // for, the later rounds would never be issued at all. Sleeping to the
            // round's offset instead keeps the schedule the workload's own.
            let rounds = (0..ROUNDS).map(|round| {
                let reads = (0..GRAINS).map(|g| {
                    let key = format!("p/{g}");
                    // Every read is issued from the **first** node's handle, which
                    // the nemesis never restarts: a handle on any other node is
                    // dead the moment its process is replaced, and an ask issued
                    // from one is an ask nothing is left waiting for
                    // (docs/simulation-testing.md, *Invariant sweep*). Routing is
                    // unaffected — the gateway sends it to whichever node leads
                    // the grain's shard.
                    let grain = hosts[0].grain(key.clone());
                    let memos = memos.clone();
                    async move {
                        if let Ok(Some(value)) = grain.ask_timeout(ReadMemo, CALL_DEADLINE).await {
                            memos.record(&key, value);
                        }
                    }
                });
                let clock = clock.clone();
                async move {
                    clock.sleep(ROUND_GAP * round as u32).await;
                    futures::future::join_all(reads).await;
                }
            });
            futures::future::join_all(rounds).await;

            // Which grains completed the chain this run: re-launched *and* observed.
            // Counted here rather than in the checker because it is a coverage
            // question, and a run that failed the chain is a weaker run, not a
            // wrong one.
            let relaunched = fx.relaunched(STEP_FETCH);
            let run = memos.run.lock().expect("memo mutex poisoned");
            let chained = relaunched
                .iter()
                .filter(|key| run.contains_key(*key))
                .count();
            memos.chained.fetch_add(chained, Ordering::Relaxed);
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        let mut invariants: Vec<Box<dyn Invariant>> = default_invariants();
        invariants.push(Box::new(MemoWriteOnce {
            memos: self.memos.clone(),
        }));
        invariants.push(Box::new(CommitMonotonic::new(
            "workflow-commit-monotonic",
            support::pipeline::PIPELINE_TYPE,
        )));
        invariants.push(Box::new(self.exercised.clone()));
        invariants
    }
}

fn workload() -> WorkflowSwarm {
    WorkflowSwarm {
        nodes: 3,
        fx: Effects::default(),
        memos: Memos::default(),
        exercised: Exercised::default(),
    }
}

// --- The conformance tests ----------------------------------------------------

#[test]
fn a_step_memo_is_write_once_under_the_cluster_swarm() {
    // The safety claim, and only it. What this sweep *observed* is asserted
    // separately below, on a range that never narrows — see there for why it
    // cannot ride here.
    let workload = workload();
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..24)) {
        panic!("{failure}");
    }
    println!(
        "memo observed {} time(s), chained on {} grain(s)",
        workload.memos.observations(),
        workload.memos.chains(),
    );
}

/// What the sweep actually reached, on a range that never narrows.
///
/// Three ways the sweep above could be green while checking nothing: no memo was
/// ever read, no memo anything tried to overwrite was ever read, or no activation
/// ever hibernated. All three are asserted here rather than there because **none of
/// them holds per seed**. A read only observes a memo if the grain's shard has an
/// elected leader this node can route to, and on roughly two seeds in five the
/// nemesis leaves the cluster without one for the whole run — every read comes back
/// `NotLeader` and the run observes nothing. That is a property of the seed range,
/// not of the workload, so it belongs on `coverage_seeds`, which never narrows; the
/// same assertion on the invariant sweep would not be stricter, only flaky at its
/// local width of 8 (docs/simulation-testing.md, *Adding a sweep*, and
/// `Exercised`'s own note, which `disk_swarm.rs` reached the same way).
#[test]
fn the_workflow_swarm_observes_a_memo_something_tried_to_overwrite() {
    let workload = workload();
    let stats = match run_cluster_swarm_coverage(&workload, coverage_seeds(0..24)) {
        Ok(stats) => stats,
        Err(failure) => panic!("{failure}"),
    };
    let observed = workload.memos.observations();
    let chains = workload.memos.chains();
    println!("memo observed {observed} time(s), chained on {chains} grain(s)");
    assert!(
        observed > 0,
        "no grain's memo could be read on any seed, so the sweep asserted \
         write-once against an empty set",
    );
    assert!(
        chains > 0,
        "no grain was both re-launched and observed, so nothing ever tried to \
         overwrite a memo and write-once was satisfied vacuously",
    );
    workload.exercised.assert_hibernated();
    // And the transport faults the wire *can* count (§18.3), so a green run is not
    // a silently happy-path one.
    assert!(
        stats.dropped > 0 && stats.duplicated > 0 && stats.delayed > 0 && stats.blocked > 0,
        "the sweep did not fire every fault type: {stats:?}",
    );
}

/// The determinism contract over this workload (actor §18.1 #1): the same seed
/// twice, byte-identical event streams. It is what makes the launch ordinals
/// meaningful — a step value that varied run to run would be nondeterminism the
/// check itself introduced, and the memo values the check reads are drawn from
/// them.
#[test]
fn the_workflow_swarm_replays_identically() {
    if let Err(divergence) = replay_cluster_swarm(&workload(), sweep_seeds(0..8)) {
        panic!("{divergence}");
    }
}
