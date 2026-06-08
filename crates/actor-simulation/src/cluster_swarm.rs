//! Swarm testing for the cluster (spec §18.3, §18.6).
//!
//! Runs a [`ClusterWorkload`] over a multi-node [`SimNetwork`] while a seeded
//! [`Nemesis`](nemesis) injects partitions, crashes, and heals, and a
//! [`Checker`](crate::Checker) watches the event stream. Each run is bounded in
//! virtual time (the failure detector never quiesces) and reproducible from its
//! seed; a failure is reported with the seed for replay.
//!
//! This is the swarm loop applied to the distributed paths: faults across
//! seeds, invariants attached, coverage measured in cluster-time exercised.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_cluster::SwimConfig;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::NodeId;
use actor_core::Spawner;

use crate::Checker;
use crate::FaultPolicy;
use crate::FaultStats;
use crate::RunFailure;
use crate::SimClock;
use crate::SimCluster;
use crate::SimEntropy;
use crate::SimNetwork;
use crate::Simulation;
use crate::Violation;
use crate::invariant::Invariant;
use crate::invariant::default_invariants;

// Swarm intensity for the cluster harness (spec §18.3): how hard each run is
// faulted and how long it may take. Collected here as named constants so the
// driver reads as policy, not scattered magic numbers — and so the one place to
// retune the sweep is obvious.
//
/// Denominator of the per-frame drop and duplication probabilities.
const CLUSTER_FAULT_DEN: u64 = 20;
/// A run draws a drop probability in `0..CLUSTER_MAX_DROP_NUM / CLUSTER_FAULT_DEN`.
const CLUSTER_MAX_DROP_NUM: u64 = 4;
/// A run draws a duplication probability in `0..CLUSTER_MAX_DUP_NUM / CLUSTER_FAULT_DEN`.
const CLUSTER_MAX_DUP_NUM: u64 = 3;
/// Frames are delayed by a seeded amount in `0..CLUSTER_MAX_LATENCY_MS` ms.
const CLUSTER_MAX_LATENCY_MS: u64 = 30;
/// Partition/crash/heal rounds the nemesis runs per run.
const CLUSTER_NEMESIS_ROUNDS: usize = 6;
/// Upper bound on a run's virtual time, so a hung call cannot loop forever (the
/// failure detector itself never quiesces).
const CLUSTER_TIME_BUDGET: Duration = Duration::from_secs(120);
/// Virtual-time step between workload-completion checks while driving.
const CLUSTER_STEP: Duration = Duration::from_millis(500);
/// Window after traffic completes for post-traffic signals (terminations,
/// prunes) to flush.
const CLUSTER_FLUSH: Duration = Duration::from_secs(2);

/// The running cluster handed to a [`ClusterWorkload`].
pub struct ClusterCtx {
    nodes: Vec<SimCluster>,
    net: SimNetwork,
}

impl ClusterCtx {
    /// The nodes of the cluster, indexed in join order.
    pub fn nodes(&self) -> &[SimCluster] {
        &self.nodes
    }

    /// The underlying network (for inspection; faults are the nemesis's job).
    pub fn net(&self) -> &SimNetwork {
        &self.net
    }
}

/// A distributed test scenario (spec §18.4). `setup` builds actors and
/// registrations; `drive` issues traffic and resolves when its work is done; the
/// runner injects faults and checks `invariants` continuously and at the end.
pub trait ClusterWorkload {
    /// A stable name for reporting.
    fn name(&self) -> &'static str;

    /// How many nodes to bring up.
    fn node_count(&self) -> usize;

    /// SWIM configuration for the run.
    fn swim(&self) -> SwimConfig;

    /// Build actors and registrations before traffic starts.
    fn setup(&self, ctx: &ClusterCtx);

    /// Drive traffic; the returned future resolves when the workload is done.
    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()>;

    /// Invariants checked continuously and at the end (spec §18.5).
    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        default_invariants()
    }
}

/// A seeded fault injector (spec §18.3): over several rounds it partitions,
/// crashes, and heals at random, so a run exercises the failure paths.
async fn nemesis(
    net: SimNetwork,
    entropy: SimEntropy,
    clock: SimClock,
    nodes: Vec<NodeId>,
    rounds: usize,
) {
    for _ in 0..rounds {
        let wait = 200 + entropy.next_u64() % 600;
        clock.sleep(Duration::from_millis(wait)).await;
        match entropy.next_u64() % 4 {
            // Partition the nodes into two random groups.
            0 => {
                let mut left = Vec::new();
                let mut right = Vec::new();
                for &node in &nodes {
                    if entropy.next_u64() % 2 == 0 {
                        left.push(node);
                    } else {
                        right.push(node);
                    }
                }
                if !left.is_empty() && !right.is_empty() {
                    net.partition(&left, &right);
                }
            }
            // Crash a random node.
            1 => {
                if let Some(i) = entropy.pick_index(nodes.len()) {
                    net.crash(nodes[i]);
                }
            }
            // Heal all partitions/crashes.
            2 => net.heal(),
            // A quiet round.
            _ => {}
        }
    }
}

/// The outcome of driving one cluster run: whether the workload's traffic
/// completed within the time budget, and the fault activity the run exercised
/// (so a swarm can assert faults actually fired — spec §18.3).
pub(crate) struct ClusterRun {
    pub completed: bool,
    pub faults: FaultStats,
}

/// Build and drive a cluster workload once under `seed`, routing every node's
/// event stream to `events`. Shared by [`run_cluster_seed`] (which feeds a
/// [`Checker`]) and the reproducibility harness (which feeds a
/// [`Recorder`](crate::Recorder)), so both observe the *identical* run.
pub(crate) fn drive_cluster<W: ClusterWorkload>(
    workload: &W,
    seed: u64,
    events: Arc<dyn actor_core::EventSink>,
) -> ClusterRun {
    let sim = Simulation::new(seed);
    // Seed-sampled transport faults: modest drop, duplication, and latency, so
    // the run exercises loss, dups, and reordering on top of the nemesis's
    // partitions/crashes (spec §18.3). Sampled from the run's entropy, so it
    // stays deterministic per seed.
    let entropy = sim.entropy();
    let faults = FaultPolicy {
        drop_num: entropy.next_u64() % CLUSTER_MAX_DROP_NUM,
        drop_den: CLUSTER_FAULT_DEN,
        duplicate_num: entropy.next_u64() % CLUSTER_MAX_DUP_NUM,
        duplicate_den: CLUSTER_FAULT_DEN,
        max_latency: Duration::from_millis(entropy.next_u64() % CLUSTER_MAX_LATENCY_MS),
    };
    let net = SimNetwork::new(&sim)
        .with_swim(workload.swim())
        .with_events(events)
        .with_faults(faults);

    let nodes: Vec<SimCluster> = (1..=workload.node_count() as u64)
        .map(|i| net.join(NodeId::new(i)))
        .collect();
    let ctx = ClusterCtx {
        nodes: nodes.clone(),
        net: net.clone(),
    };

    workload.setup(&ctx);

    let done = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&done);
    let traffic = workload.drive(&ctx);
    sim.spawner().launch(Box::pin(async move {
        traffic.await;
        flag.store(true, Ordering::SeqCst);
    }));

    let node_ids: Vec<NodeId> = nodes.iter().map(|n| n.node()).collect();
    sim.spawner().launch(Box::pin(nemesis(
        net.clone(),
        sim.entropy(),
        sim.clock(),
        node_ids,
        CLUSTER_NEMESIS_ROUNDS,
    )));

    // Drive until the traffic completes, bounded so a hung call cannot loop
    // forever (the failure detector itself never quiesces).
    let deadline = sim.now() + CLUSTER_TIME_BUDGET;
    while !done.load(Ordering::SeqCst) && sim.now() < deadline {
        sim.run_for(CLUSTER_STEP);
    }
    // Let post-traffic signals (terminations, prunes) flush.
    sim.run_for(CLUSTER_FLUSH);

    ClusterRun {
        completed: done.load(Ordering::SeqCst),
        faults: net.fault_stats(),
    }
}

/// Drive one run and evaluate it: the invariant violations observed (plus a
/// synthesized liveness violation if the workload hung) and the faults the run
/// exercised. Shared by the seed runner and the coverage sweep.
fn eval_cluster<W: ClusterWorkload>(workload: &W, seed: u64) -> (Vec<Violation>, FaultStats) {
    let checker = Checker::new(workload.invariants());
    let run = drive_cluster(workload, seed, checker.sink());

    let mut violations = checker.finish();
    if !run.completed {
        violations.push(Violation {
            invariant: "liveness",
            detail: "workload did not complete within the time budget (a call may hang)".into(),
        });
    }
    (violations, run.faults)
}

/// Run a cluster workload once under `seed`, returning any invariant violations.
pub fn run_cluster_seed<W: ClusterWorkload>(workload: &W, seed: u64) -> Result<(), RunFailure> {
    let (violations, _) = eval_cluster(workload, seed);
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

/// Sweep a cluster workload across many seeds, stopping at the first failure.
pub fn run_cluster_swarm<W: ClusterWorkload>(
    workload: &W,
    seeds: impl IntoIterator<Item = u64>,
) -> Result<(), RunFailure> {
    for seed in seeds {
        run_cluster_seed(workload, seed)?;
    }
    Ok(())
}

/// Sweep a cluster workload across many seeds, checking invariants on each run
/// and returning the *aggregate* fault activity the sweep exercised (spec
/// §18.3). A test asserts each fault type fired at least once, so a green sweep
/// provably covered loss, duplication, reordering, and partition/crash — not
/// just the happy path (fault-injection coverage).
pub fn run_cluster_swarm_coverage<W: ClusterWorkload>(
    workload: &W,
    seeds: impl IntoIterator<Item = u64>,
) -> Result<FaultStats, RunFailure> {
    let mut total = FaultStats::default();
    for seed in seeds {
        let (violations, faults) = eval_cluster(workload, seed);
        if !violations.is_empty() {
            return Err(RunFailure {
                workload: workload.name(),
                seed,
                violations,
            });
        }
        total = total + faults;
    }
    Ok(total)
}
