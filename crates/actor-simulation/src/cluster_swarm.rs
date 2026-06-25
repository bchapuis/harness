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

use actor_cluster::DowningPolicy;
use actor_cluster::GossipMode;
use actor_cluster::LeaderMode;
use actor_cluster::MembershipMode;
use actor_cluster::RaftConfig;
use actor_cluster::RegistryMode;
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
use crate::registry::RegistryFaultPolicy;
use crate::registry::SimRegistry;

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
    /// The simulated external registry, in registry-based mode (spec §9.4.2):
    /// the operator handle a workload mutates and outages under seed control.
    registry: Option<SimRegistry>,
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

    /// The simulated registry, when the run is in registry-based mode
    /// ([`ClusterModeSpec::Registry`]).
    pub fn registry(&self) -> Option<&SimRegistry> {
        self.registry.as_ref()
    }
}

/// A declarative membership-mode choice for a [`ClusterWorkload`] (spec §9.4).
/// Declarative because the registry- and leader-based modes need per-run
/// resources (the simulated registry, the voter set) that only the driver — with
/// the run's [`Simulation`] in hand — can materialize.
#[derive(Clone, Copy, Debug)]
pub enum ClusterModeSpec {
    /// Fixed roster (spec §9.4.1); `detector` enables the observe-only SWIM loop.
    Static { detector: Option<SwimConfig> },
    /// Peer-to-peer gossip with a coordinator (spec §9.4.4).
    Gossip {
        swim: SwimConfig,
        downing: DowningPolicy,
    },
    /// An external registry, simulated with seeded faults (spec §9.4.2). The
    /// driver registers every node up front and hands the operator handle to the
    /// workload via [`ClusterCtx::registry`].
    Registry {
        swim: SwimConfig,
        sync_interval: Duration,
        faults: RegistryFaultPolicy,
    },
    /// A self-hosted Raft log (spec §9.4.3): the first `voters` nodes (by join
    /// order) form the voter set, with in-memory storage.
    Leader {
        swim: SwimConfig,
        voters: usize,
        election_timeout: Duration,
        heartbeat_interval: Duration,
        downing: DowningPolicy,
    },
}

impl ClusterModeSpec {
    /// A short name for reporting, so one workload swept across modes yields
    /// distinguishable run names.
    pub fn name(&self) -> &'static str {
        match self {
            ClusterModeSpec::Static { .. } => "static",
            ClusterModeSpec::Gossip { .. } => "gossip",
            ClusterModeSpec::Registry { .. } => "registry",
            ClusterModeSpec::Leader { .. } => "leader",
        }
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

    /// SWIM configuration for the run (used by the default
    /// [`mode`](Self::mode), the gossip-based control plane).
    fn swim(&self) -> SwimConfig;

    /// The membership mode to sweep under (spec §9.4). Defaults to
    /// **gossip-based** with conservative downing; a workload overrides this to
    /// exercise the static, registry-based, or leader-based control plane under
    /// the same nemesis and fault injection.
    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::Gossip {
            swim: self.swim(),
            downing: DowningPolicy::Conservative,
        }
    }

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
/// crashes, and heals at random, so a run exercises the failure paths. In
/// registry-based mode a fifth action opens a bounded registry **outage**
/// window — the "stalled, lagging, or unavailable registry sync" fault.
async fn nemesis(
    net: SimNetwork,
    entropy: SimEntropy,
    clock: SimClock,
    nodes: Vec<NodeId>,
    rounds: usize,
    registry: Option<SimRegistry>,
) {
    let actions = if registry.is_some() { 5 } else { 4 };
    for _ in 0..rounds {
        let wait = 200 + entropy.next_u64() % 600;
        clock.sleep(Duration::from_millis(wait)).await;
        match entropy.next_u64() % actions {
            // Partition the nodes into two random groups.
            0 => {
                let mut left = Vec::new();
                let mut right = Vec::new();
                for &node in &nodes {
                    if entropy.next_u64().is_multiple_of(2) {
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
            3 => {}
            // A bounded registry outage window (spec §9.4.2 item 6, §18.3).
            _ => {
                if let Some(registry) = &registry {
                    registry.set_available(false);
                    let outage = 100 + entropy.next_u64() % 300;
                    clock.sleep(Duration::from_millis(outage)).await;
                    registry.set_available(true);
                }
            }
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
    // Materialize the workload's mode spec into a concrete control plane: the
    // registry- and leader-based modes need per-run resources (the simulated
    // registry, the voter set) only the driver can build.
    let (mode, registry) = match workload.mode() {
        ClusterModeSpec::Static { detector } => (MembershipMode::Static { detector }, None),
        ClusterModeSpec::Gossip { swim, downing } => {
            (MembershipMode::Gossip(GossipMode { swim, downing }), None)
        }
        ClusterModeSpec::Registry {
            swim,
            sync_interval,
            faults,
        } => {
            let registry = SimRegistry::new(&sim).with_faults(faults);
            // The platform registers every node up front (spec §9.4.2 item 2);
            // runtime mutations are the workload's and nemesis's job.
            for i in 1..=workload.node_count() as u64 {
                registry.register(NodeId::new(i));
            }
            (
                MembershipMode::Registry(RegistryMode {
                    swim,
                    client: registry.client(),
                    sync_interval,
                }),
                Some(registry),
            )
        }
        ClusterModeSpec::Leader {
            swim,
            voters,
            election_timeout,
            heartbeat_interval,
            downing,
        } => {
            let voter_ids: Vec<NodeId> = (1..=voters.min(workload.node_count()) as u64)
                .map(NodeId::new)
                .collect();
            let mut raft = RaftConfig::new(voter_ids);
            raft.election_timeout = election_timeout;
            raft.heartbeat_interval = heartbeat_interval;
            (
                MembershipMode::Leader(LeaderMode {
                    swim,
                    raft,
                    downing,
                }),
                None,
            )
        }
    };
    let net = SimNetwork::new(&sim)
        .with_mode(mode)
        .with_events(events)
        .with_faults(faults);

    let nodes: Vec<SimCluster> = (1..=workload.node_count() as u64)
        .map(|i| net.join(NodeId::new(i)))
        .collect();
    let ctx = ClusterCtx {
        nodes: nodes.clone(),
        net: net.clone(),
        registry,
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
        ctx.registry.clone(),
    )));

    // Drive until the traffic completes, bounded so a hung call cannot loop
    // forever (the failure detector itself never quiesces).
    let deadline = sim.now() + CLUSTER_TIME_BUDGET;
    while !done.load(Ordering::SeqCst) && sim.now() < deadline {
        sim.run_for(CLUSTER_STEP);
    }
    // Let post-traffic signals (terminations, prunes) flush.
    sim.run_for(CLUSTER_FLUSH);

    let mut faults = net.fault_stats();
    if let Some(registry) = &ctx.registry {
        faults = faults + registry.fault_stats();
    }
    ClusterRun {
        completed: done.load(Ordering::SeqCst),
        faults,
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
