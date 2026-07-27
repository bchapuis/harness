//! Swarm testing for the cluster (spec §18.3, §18.6).
//!
//! Runs a [`ClusterWorkload`] over a multi-node [`SimNetwork`] while a seeded
//! [`Nemesis`](nemesis) injects partitions (symmetric and one-way), crashes,
//! process freezes, restarts, and heals, and a
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
use crate::SimEntropy;
use crate::SimNetwork;
use crate::SimNode;
use crate::Simulation;
use crate::Violation;
use crate::faults::RegistryFaultPolicy;
use crate::invariant::Invariant;
use crate::invariant::default_invariants;
use crate::registry::SimRegistry;
use crate::workload::SweepFailure;
use crate::workload::sweep_collecting;

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
/// Rounds the nemesis runs per run, one action drawn from its vocabulary each.
const CLUSTER_NEMESIS_ROUNDS: usize = 6;
/// Upper bound on a run's virtual time, so a hung call cannot loop forever (the
/// failure detector itself never quiesces).
const CLUSTER_TIME_BUDGET: Duration = Duration::from_secs(120);
/// Virtual-time step between workload-completion checks while driving.
const CLUSTER_STEP: Duration = Duration::from_millis(500);
/// Window after traffic completes for post-traffic signals (terminations,
/// prunes) to flush.
const CLUSTER_FLUSH: Duration = Duration::from_secs(2);
/// Cap on the extra time [`drive_cluster`] will spend waiting for in-flight
/// `ask`s to close after the flush window (see `AskTally`). Generous next to any
/// per-call deadline a workload configures, and finite so a genuinely lost ask
/// still reaches the no-silent-loss invariant instead of spinning here.
const CLUSTER_SETTLE_MAX: Duration = Duration::from_secs(30);

/// The running cluster handed to a [`ClusterWorkload`].
pub struct ClusterCtx {
    nodes: Vec<SimNode>,
    net: SimNetwork,
    /// The simulated external registry, in registry-based mode (spec §9.4.2):
    /// the operator handle a workload mutates and outages under seed control.
    registry: Option<SimRegistry>,
}

impl ClusterCtx {
    /// The nodes of the cluster, indexed in join order.
    pub fn nodes(&self) -> &[SimNode] {
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

/// A workload's hook for putting a restarted node back to work, run by the
/// driver on the fresh system the moment it is up — see
/// [`ClusterWorkload::rehost`]. Shared and `'static` because the nemesis outlives
/// the borrow of the workload that produced it.
pub type Rehost = Arc<dyn Fn(&SimNode) + Send + Sync>;

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

    /// What it takes to bring a **restarted** node back into service — and, by
    /// being `Some` at all, this workload's consent for the nemesis to restart
    /// one (spec §18.3).
    ///
    /// A restart is process death: volatile state lost, durable state reloaded
    /// through the storage seam. It is the fault that reaches
    /// recovery-on-activation, where `crash` only isolates a process that keeps
    /// running. But the fresh process comes up **empty**, so everything
    /// [`setup`](Self::setup) installed on that node — a granary host, a spawned
    /// actor, a receptionist registration — is gone. Left that way a restarted
    /// node silently stops participating, and the run quietly shrinks its
    /// cluster instead of faulting it. The hook is where a workload puts the
    /// node back to work; a workload with nothing to re-install still opts in
    /// with a hook that does nothing.
    ///
    /// Two things a consenting workload must tolerate. A restart shuts the old
    /// system down, so any [`SimNode`] cloned out of [`ClusterCtx::nodes`] for
    /// that id is dead afterwards — the nemesis never restarts the first node,
    /// so one handle always stays live. And every call should be bounded
    /// (`ask_timeout`), so one issued into a shut-down node resolves as a
    /// failure rather than sitting pending at quiescence.
    fn rehost(&self) -> Option<Rehost> {
        None
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

/// One action in the nemesis's vocabulary (spec §18.3). Named rather than
/// numbered because the vocabulary is now built per run — a registry-based run
/// can open an outage, a restart-tolerant workload can lose a process — and
/// modular arithmetic over a conditional tail stops reading as policy.
#[derive(Clone, Copy)]
enum Fault {
    /// Sever two random groups in both directions.
    Partition,
    /// Sever one random group's frames *to* another, leaving the reverse
    /// flowing: the asymmetric case `Partition` cannot express, and the source
    /// of zombie leaders (§8.1).
    PartitionOneWay,
    /// Isolate a node from every peer — a crash as the rest of the cluster sees
    /// it. The process keeps running; [`Fault::Restart`] is the one that kills it.
    Crash,
    /// Freeze a node's tasks for a bounded window, then thaw: a GC stall or VM
    /// pause. State and inbound frames survive, and overdue timers fire at once
    /// on resume, so a paused leader wakes already deposed.
    Pause,
    /// Kill a node's process and bring a fresh one up under the same identity:
    /// volatile state lost, durable state reloaded through the storage seam.
    Restart,
    /// Clear every partition and crash.
    Heal,
    /// A quiet round.
    Quiet,
    /// A bounded registry outage window — the "stalled, lagging, or unavailable
    /// registry sync" fault (spec §9.4.2 item 6).
    RegistryOutage,
}

/// The actions available to one run. The network-only ones — both partitions,
/// crash, freeze, heal, quiet — are always in it, since every workload survives
/// them by construction. The two that touch more than the wire are gated on the
/// run saying it can take them.
fn vocabulary(registry: bool, restarts: bool) -> Vec<Fault> {
    let mut faults = vec![
        Fault::Partition,
        Fault::PartitionOneWay,
        Fault::Crash,
        Fault::Pause,
        Fault::Heal,
        Fault::Quiet,
    ];
    if registry {
        faults.push(Fault::RegistryOutage);
    }
    if restarts {
        faults.push(Fault::Restart);
    }
    faults
}

/// Split `nodes` into two non-empty random groups, or `None` if the coin came up
/// all one way. Shared by the symmetric and one-way partition actions so they
/// divide the cluster the same way and differ only in which directions block.
fn two_groups(entropy: &SimEntropy, nodes: &[NodeId]) -> Option<(Vec<NodeId>, Vec<NodeId>)> {
    let mut left = Vec::new();
    let mut right = Vec::new();
    for &node in nodes {
        if entropy.next_u64().is_multiple_of(2) {
            left.push(node);
        } else {
            right.push(node);
        }
    }
    if left.is_empty() || right.is_empty() {
        None
    } else {
        Some((left, right))
    }
}

/// A seeded fault injector (spec §18.3): over several rounds it draws from the
/// run's [`vocabulary`] at random, so a run exercises the failure paths.
///
/// Every action is either instantaneous or bounded within its own round. A pause
/// that outlived the nemesis would stall the run rather than fault it, so the
/// freeze and its thaw are one action, exactly as the registry outage is.
async fn nemesis(
    net: SimNetwork,
    entropy: SimEntropy,
    clock: SimClock,
    nodes: Vec<NodeId>,
    rounds: usize,
    registry: Option<SimRegistry>,
    rehost: Option<Rehost>,
) {
    let faults = vocabulary(registry.is_some(), rehost.is_some());
    for _ in 0..rounds {
        let wait = 200 + entropy.next_u64() % 600;
        clock.sleep(Duration::from_millis(wait)).await;
        let Some(action) = entropy.pick_index(faults.len()) else {
            continue;
        };
        match faults[action] {
            Fault::Partition => {
                if let Some((left, right)) = two_groups(&entropy, &nodes) {
                    net.partition(&left, &right);
                }
            }
            Fault::PartitionOneWay => {
                if let Some((from, to)) = two_groups(&entropy, &nodes) {
                    net.partition_one_way(&from, &to);
                }
            }
            Fault::Crash => {
                if let Some(i) = entropy.pick_index(nodes.len()) {
                    net.crash(nodes[i]);
                }
            }
            Fault::Pause => {
                if let Some(i) = entropy.pick_index(nodes.len()) {
                    let node = nodes[i];
                    net.pause(node);
                    let freeze = 100 + entropy.next_u64() % 300;
                    clock.sleep(Duration::from_millis(freeze)).await;
                    net.resume(node);
                }
            }
            Fault::Restart => {
                // Never the first node. A restart shuts the old system down, so
                // every `SimNode` a workload cloned out of `ClusterCtx::nodes()`
                // for that id is dead afterwards; leaving node 1 alone leaves a
                // workload one handle it can count on, which is the discipline
                // `restart-churn` already keeps by hand.
                if let Some(i) = entropy.pick_index(nodes.len() - 1) {
                    let system = net.restart(nodes[i + 1]);
                    // The fresh process is empty. Put it back to work before the
                    // next round, or the run has shrunk the cluster rather than
                    // faulted it (`ClusterWorkload::rehost`).
                    if let Some(rehost) = &rehost {
                        rehost(&system);
                    }
                }
            }
            Fault::Heal => net.heal(),
            Fault::Quiet => {}
            Fault::RegistryOutage => {
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
    let tally = Arc::new(AskTally::default());
    let events: Arc<dyn actor_core::EventSink> = Arc::new(TallyingSink {
        tally: Arc::clone(&tally),
        inner: events,
    });
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

    let nodes: Vec<SimNode> = (1..=workload.node_count() as u64)
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
        workload.rehost(),
    )));

    // Drive until the traffic completes, bounded so a hung call cannot loop
    // forever (the failure detector itself never quiesces).
    let deadline = sim.now() + CLUSTER_TIME_BUDGET;
    while !done.load(Ordering::SeqCst) && sim.now() < deadline {
        sim.run_for(CLUSTER_STEP);
    }
    // Let post-traffic signals (terminations, prunes) flush, then keep going
    // while any `ask` is still in flight. A fixed window cannot be right here:
    // the last call a workload issues carries its own deadline, and a subsystem
    // may fan out further calls behind it — granary's quorum append returns at
    // quorum latency and drains the slower replicas afterwards, each with a
    // seconds-long timeout of its own. Stopping the clock inside that deadline
    // and then asking "is anything still pending?" reports a live call as a lost
    // one. Waiting for the answer is what makes the question meaningful.
    let settle_by = sim.now() + CLUSTER_SETTLE_MAX;
    sim.run_for(CLUSTER_FLUSH);
    while tally.in_flight() && sim.now() < settle_by {
        sim.run_for(CLUSTER_STEP);
    }

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

/// Sweep a cluster workload across many seeds, stopping at the first failure —
/// or, under [`collect_all_failures`](crate::collect_all_failures), running to
/// the end and reporting every failing seed.
///
/// The workload's [`regression_seeds`](crate::regression_seeds) run first,
/// ahead of `seeds` and whatever sizing produced them: a seed that failed once
/// is checked on every run, however narrow the sweep.
pub fn run_cluster_swarm<W: ClusterWorkload>(
    workload: &W,
    seeds: impl IntoIterator<Item = u64>,
) -> Result<(), SweepFailure> {
    sweep_collecting(
        workload.name(),
        crate::corpus::regression_seeds(workload.name()).chain(seeds),
        crate::sweep::collect_all_failures(),
        |seed| run_cluster_seed(workload, seed),
    )
}

/// Sweep a cluster workload across many seeds, checking invariants on each run
/// and returning the *aggregate* fault activity the sweep exercised (spec
/// §18.3). A test asserts each fault type fired at least once, so a green sweep
/// provably covered loss, duplication, reordering, and partition/crash — not
/// just the happy path (fault-injection coverage).
pub fn run_cluster_swarm_coverage<W: ClusterWorkload>(
    workload: &W,
    seeds: impl IntoIterator<Item = u64>,
) -> Result<FaultStats, SweepFailure> {
    let mut total = FaultStats::default();
    sweep_collecting(
        workload.name(),
        crate::corpus::regression_seeds(workload.name()).chain(seeds),
        crate::sweep::collect_all_failures(),
        |seed| {
            let (violations, faults) = eval_cluster(workload, seed);
            if !violations.is_empty() {
                return Err(RunFailure {
                    workload: workload.name(),
                    seed,
                    violations,
                });
            }
            // Only a clean seed contributes coverage, so the fault totals stay a
            // claim about runs that actually held their invariants.
            total = total + faults;
            Ok(())
        },
    )?;
    Ok(total)
}

/// How many `ask`s the run has issued but not yet resolved.
///
/// The driver reads this to decide when the run has actually gone quiet (see
/// [`drive_cluster`]). It is deliberately the same bracket the no-silent-loss
/// invariant counts (spec §18.5 #1), so "the driver stopped waiting" and "the
/// invariant is satisfied" cannot disagree about what pending means.
#[derive(Default)]
struct AskTally {
    outstanding: std::sync::atomic::AtomicI64,
}

impl AskTally {
    fn in_flight(&self) -> bool {
        self.outstanding.load(Ordering::SeqCst) > 0
    }
}

/// The run's event sink, counting the `ask` bracket on its way through to the
/// [`Checker`] or [`Recorder`](crate::Recorder) the caller supplied.
struct TallyingSink {
    tally: Arc<AskTally>,
    inner: Arc<dyn actor_core::EventSink>,
}

impl actor_core::EventSink for TallyingSink {
    fn emit(&self, event: actor_core::Event) {
        match event {
            actor_core::Event::AskIssued { .. } => {
                self.tally.outstanding.fetch_add(1, Ordering::SeqCst);
            }
            actor_core::Event::AskOutcome { .. } => {
                self.tally.outstanding.fetch_sub(1, Ordering::SeqCst);
            }
            _ => {}
        }
        self.inner.emit(event);
    }
}
