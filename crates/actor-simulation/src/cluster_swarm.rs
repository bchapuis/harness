//! Swarm testing for the cluster (spec §18.3, §18.6).
//!
//! Runs a [`ClusterWorkload`] over a multi-node [`SimNetwork`] while a seeded
//! [`Nemesis`](nemesis) injects partitions, crashes, and heals, and a
//! [`Checker`](crate::Checker) watches the event stream. Each run is bounded in
//! virtual time (the failure detector never quiesces) and reproducible from its
//! seed; a failure is reported with the seed for replay.
//!
//! This is the FoundationDB loop applied to the distributed paths: faults across
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
use crate::SimClock;
use crate::SimCluster;
use crate::SimEntropy;
use crate::SimNetwork;
use crate::Simulation;
use crate::Violation;
use crate::invariant::Invariant;
use crate::invariant::default_invariants;

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

/// A failing cluster run, with the seed needed to replay it (spec §18.6).
#[derive(Clone, Debug)]
pub struct ClusterFailure {
    pub workload: &'static str,
    pub seed: u64,
    pub violations: Vec<Violation>,
}

impl std::fmt::Display for ClusterFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "cluster workload '{}' failed at seed {} (replay with run_cluster_seed(.., {})):",
            self.workload, self.seed, self.seed
        )?;
        for v in &self.violations {
            writeln!(f, "  - {v}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ClusterFailure {}

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

/// Run a cluster workload once under `seed`, returning any invariant violations.
pub fn run_cluster_seed<W: ClusterWorkload>(workload: &W, seed: u64) -> Result<(), ClusterFailure> {
    let sim = Simulation::new(seed);
    let checker = Checker::new(workload.invariants());
    // Seed-sampled transport faults: modest drop, duplication, and latency, so
    // the run exercises loss, dups, and reordering on top of the nemesis's
    // partitions/crashes (spec §18.3). Sampled from the run's entropy, so it
    // stays deterministic per seed.
    let entropy = sim.entropy();
    let faults = FaultPolicy {
        drop_num: entropy.next_u64() % 4,
        drop_den: 20,
        duplicate_num: entropy.next_u64() % 3,
        duplicate_den: 20,
        max_latency: Duration::from_millis(entropy.next_u64() % 30),
    };
    let net = SimNetwork::new(&sim)
        .with_swim(workload.swim())
        .with_events(checker.sink())
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
        net,
        sim.entropy(),
        sim.clock(),
        node_ids,
        6,
    )));

    // Drive until the traffic completes, bounded so a hung call cannot loop
    // forever (the failure detector itself never quiesces).
    let budget = Duration::from_secs(120);
    let deadline = sim.now() + budget;
    while !done.load(Ordering::SeqCst) && sim.now() < deadline {
        sim.run_for(Duration::from_millis(500));
    }
    // Let post-traffic signals (terminations, prunes) flush.
    sim.run_for(Duration::from_secs(2));

    let mut violations = checker.finish();
    if !done.load(Ordering::SeqCst) {
        violations.push(Violation {
            invariant: "liveness",
            detail: "workload did not complete within the time budget (a call may hang)".into(),
        });
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(ClusterFailure {
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
) -> Result<(), ClusterFailure> {
    for seed in seeds {
        run_cluster_seed(workload, seed)?;
    }
    Ok(())
}
