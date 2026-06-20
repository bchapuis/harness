//! Conformance: determinism harness (spec §18.1) — the *event stream* is
//! byte-identical for a fixed seed, and `run_for` stops cleanly at its bound
//! even with perpetual work outstanding. Also covers the deterministic
//! executor's seed-driven replay (timers firing in deadline order, virtual time
//! advancing for free) and the byte-identical event-stream reproducibility
//! contract enforced over the real system, swept across seeds for single-node
//! and multi-node-under-nemesis workloads.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RouteStrategy;
use actor_cluster::Router;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Instant;
use actor_core::Key;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::Spawner;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::RegistryFaultPolicy;
use actor_simulation::SimCluster;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use actor_simulation::Workload;
use actor_simulation::check_cluster_reproducible;
use actor_simulation::check_reproducible;
use actor_simulation::replay_cluster_swarm;
use actor_simulation::replay_swarm;
use serde::Deserialize;
use serde::Serialize;
use support::Greet;
use support::Greeter;

// === Event-stream determinism harness (spec §18.1) ============================

/// Run a small workload under a recorded single-node system and return its event
/// stream.
fn event_stream(seed: u64) -> Vec<Event> {
    let (sim, system, recorder) = support::local_recorded(seed);
    sim.block_on(async move {
        let greeter = system.spawn(Greeter::<SimSystem>::new("Hi"));
        for name in ["a", "b", "c"] {
            let _ = greeter.ask(Greet { name: name.into() }).await;
        }
    });
    recorder.events()
}

#[test]
fn the_same_seed_yields_an_identical_event_stream() {
    assert_eq!(event_stream(42), event_stream(42));
}

#[test]
fn different_seeds_can_diverge_but_each_is_stable() {
    // Each seed is internally reproducible (the core determinism guarantee).
    assert_eq!(event_stream(7), event_stream(7));
    assert_eq!(event_stream(8), event_stream(8));
}

#[test]
fn run_for_stops_at_the_time_bound_with_work_outstanding() {
    let sim = Simulation::new(1);
    let clock = sim.clock();
    // A task that sleeps forever — never quiesces.
    sim.spawner().launch(Box::pin(async move {
        loop {
            clock.sleep(Duration::from_millis(100)).await;
        }
    }));

    sim.run_for(Duration::from_secs(1));
    // Returns at exactly the bound rather than running the perpetual task forever.
    assert_eq!(sim.now(), Instant::ZERO + Duration::from_secs(1));
}

// === Deterministic executor: seed replay, timer order, virtual time ==========
// (spec §18.1): the deterministic executor reproduces runs from a seed, fires
// timers in deadline order, and advances virtual time for free.

/// A workload whose interleaving depends on both scheduling and application
/// randomness — every draw flows through the one seeded stream.
fn run_workload(seed: u64) -> Vec<String> {
    let sim = Simulation::new(seed);
    let log = Arc::new(Mutex::new(Vec::new()));

    for task in 0..4u64 {
        let log = Arc::clone(&log);
        let clock = sim.clock();
        let entropy = sim.entropy();
        sim.spawner().launch(Box::pin(async move {
            for step in 0..3u64 {
                let r = entropy.next_u64() % 100;
                clock.sleep(Duration::from_millis(r + 1)).await;
                log.lock()
                    .unwrap()
                    .push(format!("task{task}-step{step}-r{r}"));
            }
        }));
    }

    sim.run();
    log.lock().unwrap().clone()
}

#[test]
fn same_seed_reproduces_run() {
    // Byte-identical results from the same seed (spec §18.1 #1).
    assert_eq!(run_workload(42), run_workload(42));
    assert_eq!(run_workload(7), run_workload(7));
}

#[test]
fn different_seeds_diverge() {
    assert_ne!(run_workload(1), run_workload(2));
}

#[test]
fn timers_fire_in_deadline_order() {
    let sim = Simulation::new(99);
    let log = Arc::new(Mutex::new(Vec::new()));

    // Launch in scrambled order; each task sleeps a distinct duration.
    for ms in [50u64, 10, 30, 20, 40] {
        let log = Arc::clone(&log);
        let clock = sim.clock();
        sim.spawner().launch(Box::pin(async move {
            clock.sleep(Duration::from_millis(ms)).await;
            log.lock().unwrap().push(ms);
        }));
    }

    sim.run();
    assert_eq!(*log.lock().unwrap(), vec![10, 20, 30, 40, 50]);
}

#[test]
fn sleep_advances_virtual_time() {
    let sim = Simulation::new(0);
    let clock = sim.clock();
    // An hour of logical time, paid for in zero wall-clock time (spec §18.1 #2).
    let elapsed = sim.block_on(async move {
        let start = clock.now();
        clock.sleep(Duration::from_secs(3600)).await;
        clock.now().duration_since(start)
    });
    assert_eq!(elapsed, Duration::from_secs(3600));
}

#[test]
fn timeout_elapses_on_slow_future() {
    let sim = Simulation::new(0);
    let clock = sim.clock();
    let slow = sim.clock();
    let res = sim.block_on(async move {
        clock
            .timeout(
                Duration::from_millis(100),
                slow.sleep(Duration::from_millis(500)),
            )
            .await
    });
    assert!(res.is_err(), "slow future should elapse");
}

#[test]
fn timeout_passes_through_fast_future() {
    let sim = Simulation::new(0);
    let clock = sim.clock();
    let fast = sim.clock();
    let res = sim.block_on(async move {
        clock
            .timeout(Duration::from_millis(500), async move {
                fast.sleep(Duration::from_millis(100)).await;
                7u32
            })
            .await
    });
    assert_eq!(res, Ok(7));
}

// === Event-stream reproducibility over the real system (spec §18.1 #1) =======
//
// The foundational guarantee of deterministic simulation is that a seed
// reproduces an entire run exactly. Here it is checked the strong way: a
// workload is run twice under one seed and the two recorded *event streams*
// (spec §16) must be byte-identical — swept across many seeds, for both a
// single-node and a multi-node-under-faults workload. A `negative` test then
// proves the check has teeth: a workload with a deliberate determinism leak
// (ambient state outside the seed) is caught, with the first divergence
// pinpointed.

// --- A single-node workload ---------------------------------------------------

struct Echo;

impl Actor for Echo {
    type System = SimSystem;
}

#[derive(Serialize, Deserialize)]
struct Ping(u64);

impl Message for Ping {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("repro.Ping");
}

impl Handler<Ping> for Echo {
    async fn handle(&mut self, msg: Ping, _ctx: &Ctx<Self>) -> u64 {
        msg.0
    }
}

struct AskStorm {
    actors: usize,
    asks: u64,
}

impl Workload for AskStorm {
    fn name(&self) -> &'static str {
        "repro-ask-storm"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let actors = self.actors;
        let asks = self.asks;
        Box::pin(async move {
            let refs: Vec<_> = (0..actors).map(|_| system.spawn(Echo)).collect();
            let mut futs = Vec::new();
            for r in &refs {
                for i in 0..asks {
                    futs.push(r.ask(Ping(i)));
                }
            }
            let _ = futures::future::join_all(futs).await;
        })
    }
}

#[test]
fn single_node_event_stream_is_byte_identical_across_seeds() {
    let workload = AskStorm { actors: 4, asks: 8 };
    if let Err(divergence) = replay_swarm(&workload, 0..128) {
        panic!("{divergence}");
    }
}

#[test]
fn single_node_one_seed_replays_identically() {
    let workload = AskStorm { actors: 3, asks: 5 };
    assert!(check_reproducible(&workload, 7).is_ok());
}

// --- A cluster workload (the stronger contract: multi-node + faults) ----------

struct ReproGreeter;

impl Actor for ReproGreeter {
    type System = SimCluster;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<ReproGreet>();
    }
}

#[derive(Serialize, Deserialize)]
struct ReproGreet;

impl Message for ReproGreet {
    type Reply = String;
    const MANIFEST: Manifest = Manifest::new("repro.Greet");
}

impl Handler<ReproGreet> for ReproGreeter {
    async fn handle(&mut self, _msg: ReproGreet, _ctx: &Ctx<Self>) -> String {
        "hi".into()
    }
}

const GREETERS: Key<ReproGreeter> = Key::new("repro.greeters");

struct DiscoverAndCall {
    nodes: usize,
    rounds: u64,
}

impl ClusterWorkload for DiscoverAndCall {
    fn name(&self) -> &'static str {
        "repro-discover-and-call"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(200),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::Gossip {
            swim: self.swim(),
            downing: DowningPolicy::Timeout(Duration::from_millis(300)),
        }
    }

    fn setup(&self, ctx: &ClusterCtx) {
        for node in ctx.nodes() {
            let greeter = node.spawn(ReproGreeter);
            node.receptionist().register(GREETERS, &greeter);
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let caller = ctx.nodes()[0].clone();
        let rounds = self.rounds;
        Box::pin(async move {
            let clock = caller.clock().clone();
            for _ in 0..rounds {
                clock.sleep(Duration::from_millis(200)).await;
                for service in caller.receptionist().lookup(GREETERS).iter() {
                    let _ = service.ask_timeout(ReproGreet, Duration::from_millis(500)).await;
                }
            }
        })
    }
}

#[test]
fn cluster_event_stream_is_byte_identical_under_faults_across_seeds() {
    // The strong contract: even with seeded transport faults and a nemesis
    // partitioning and crashing nodes, the whole multi-node event stream
    // reproduces byte-for-byte from the seed.
    let workload = DiscoverAndCall {
        nodes: 3,
        rounds: 8,
    };
    if let Err(divergence) = replay_cluster_swarm(&workload, 0..24) {
        panic!("{divergence}");
    }
}

#[test]
fn cluster_one_seed_replays_identically() {
    let workload = DiscoverAndCall {
        nodes: 3,
        rounds: 6,
    };
    assert!(check_cluster_reproducible(&workload, 99).is_ok());
}

/// A **registry**-mode workload whose operator churns a node `draining ⇄ up`
/// every round — through the simulated external registry, with seeded sync
/// latency and stale reads — while also discovering and calling. This exercises
/// registry mode's *new* deterministic state — registry revisions, the sync
/// loop, and the `MemberDraining`/`MemberResumed` event ordering — so the
/// reproducibility check has something mode-specific to pin (spec §9.4.2,
/// §18.1).
struct RegistryChurn {
    nodes: usize,
    rounds: u64,
}

impl ClusterWorkload for RegistryChurn {
    fn name(&self) -> &'static str {
        "repro-registry-churn"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(200),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::Registry {
            swim: self.swim(),
            sync_interval: Duration::from_millis(100),
            faults: RegistryFaultPolicy {
                max_latency: Duration::from_millis(40),
                stale_num: 1,
                stale_den: 4,
                max_staleness: 3,
            },
        }
    }

    fn setup(&self, ctx: &ClusterCtx) {
        for node in ctx.nodes() {
            let greeter = node.spawn(ReproGreeter);
            node.receptionist().register(GREETERS, &greeter);
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let caller = ctx.nodes()[0].clone();
        let registry = ctx.registry().expect("registry mode").clone();
        let victim = ctx.nodes()[1].node(); // node 2
        let rounds = self.rounds;
        Box::pin(async move {
            let clock = caller.clock().clone();
            for round in 0..rounds {
                clock.sleep(Duration::from_millis(200)).await;
                // Cordon and uncordon the victim on alternating rounds — registry
                // mutations whose sync must reproduce byte-for-byte from the seed.
                if round % 2 == 0 {
                    registry.drain(victim);
                } else {
                    registry.resume(victim);
                }
                for service in caller.receptionist().lookup(GREETERS).iter() {
                    let _ = service.ask_timeout(ReproGreet, Duration::from_millis(500)).await;
                }
            }
        })
    }
}

#[test]
fn registry_mode_event_stream_is_byte_identical_across_seeds() {
    // Registry mode's sync loop, seeded registry faults, and revision stamps
    // must be as deterministic as everything else: the whole event stream
    // reproduces byte-for-byte, even with the nemesis and transport faults in
    // play.
    let workload = RegistryChurn {
        nodes: 3,
        rounds: 8,
    };
    if let Err(divergence) = replay_cluster_swarm(&workload, 0..24) {
        panic!("{divergence}");
    }
}

/// A **leader**-mode workload whose operator churns a node `draining ⇄ up`
/// every round — each a Raft proposal forwarded, committed, and applied — while
/// the nemesis crashes and partitions nodes, forcing elections mid-transition.
/// This pins leader mode's *new* deterministic state: election timers and
/// jitter, log replication, commit-index stamps, and `LeaderElected` ordering
/// (spec §9.4.3, §18.1) — the highest-risk determinism surface of the mode.
struct LeaderChurn {
    nodes: usize,
    rounds: u64,
}

impl ClusterWorkload for LeaderChurn {
    fn name(&self) -> &'static str {
        "repro-leader-churn"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(200),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::Leader {
            swim: self.swim(),
            voters: 3,
            election_timeout: Duration::from_millis(500),
            heartbeat_interval: Duration::from_millis(100),
            downing: DowningPolicy::Timeout(Duration::from_millis(300)),
        }
    }

    fn setup(&self, ctx: &ClusterCtx) {
        for node in ctx.nodes() {
            let greeter = node.spawn(ReproGreeter);
            node.receptionist().register(GREETERS, &greeter);
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let proposer = ctx.nodes()[0].clone();
        let victim = ctx.nodes()[1].node(); // node 2
        let rounds = self.rounds;
        Box::pin(async move {
            let clock = proposer.clock().clone();
            for round in 0..rounds {
                clock.sleep(Duration::from_millis(200)).await;
                // Cordon and uncordon the victim on alternating rounds — Raft
                // proposals whose elections, commits, and (under the nemesis)
                // failures must reproduce byte-for-byte from the seed.
                if round % 2 == 0 {
                    let _ = proposer.drain(victim).await;
                } else {
                    let _ = proposer.resume(victim).await;
                }
                for service in proposer.receptionist().lookup(GREETERS).iter() {
                    let _ = service.ask_timeout(ReproGreet, Duration::from_millis(500)).await;
                }
            }
        })
    }
}

/// Leader-mode churn with a voter **restarting** every other round: the
/// restart seam (abrupt stop, storage reload, fresh loops drawing from the
/// shared entropy stream) is itself part of the deterministic surface and must
/// replay byte-for-byte (spec §18.1).
struct RestartReplay {
    nodes: usize,
    rounds: u64,
}

impl ClusterWorkload for RestartReplay {
    fn name(&self) -> &'static str {
        "repro-restart-churn"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(200),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::Leader {
            swim: self.swim(),
            voters: 3,
            election_timeout: Duration::from_millis(500),
            heartbeat_interval: Duration::from_millis(100),
            downing: DowningPolicy::Conservative,
        }
    }

    fn setup(&self, ctx: &ClusterCtx) {
        for node in ctx.nodes() {
            let greeter = node.spawn(ReproGreeter);
            node.receptionist().register(GREETERS, &greeter);
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let caller = ctx.nodes()[0].clone();
        let net = ctx.net().clone();
        let victim = ctx.nodes()[1].node();
        let rounds = self.rounds;
        Box::pin(async move {
            let clock = caller.clock().clone();
            for round in 0..rounds {
                clock.sleep(Duration::from_millis(300)).await;
                if round % 2 == 0 {
                    net.restart(victim);
                }
                for service in caller.receptionist().lookup(GREETERS).iter() {
                    let _ = service.ask_timeout(ReproGreet, Duration::from_millis(500)).await;
                }
            }
        })
    }
}

#[test]
fn restart_churn_event_stream_is_byte_identical_across_seeds() {
    let workload = RestartReplay {
        nodes: 3,
        rounds: 8,
    };
    if let Err(divergence) = replay_cluster_swarm(&workload, 0..24) {
        panic!("{divergence}");
    }
}

#[test]
fn leader_mode_event_stream_is_byte_identical_across_seeds() {
    // Raft's timers, jitter, replication, and commit application must be as
    // deterministic as everything else: the whole event stream reproduces
    // byte-for-byte, even with the nemesis crashing leaders mid-transition.
    let workload = LeaderChurn {
        nodes: 3,
        rounds: 8,
    };
    if let Err(divergence) = replay_cluster_swarm(&workload, 0..24) {
        panic!("{divergence}");
    }
}

// --- Negative: a determinism leak is caught -----------------------------------

/// Process-global state, *outside* the seed: the kind of ambient nondeterminism
/// (a wall-clock read, a non-seeded RNG, `HashMap` order) §18.1 #3 forbids.
static RUNS: AtomicUsize = AtomicUsize::new(0);

/// A workload that consults ambient state to decide how much traffic to drive,
/// so two runs under the *same* seed diverge — exactly what the contract forbids.
struct NonDeterministic;

impl Workload for NonDeterministic {
    fn name(&self) -> &'static str {
        "non-deterministic"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        // Ambient: alternate 1 vs 2 asks on successive runs, regardless of seed.
        let extra = RUNS.fetch_add(1, Ordering::SeqCst) % 2;
        Box::pin(async move {
            let echo = system.spawn(Echo);
            for i in 0..(1 + extra as u64) {
                let _ = echo.ask(Ping(i)).await;
            }
        })
    }
}

#[test]
fn the_checker_catches_a_determinism_leak() {
    // Mirrors `harness_detects_a_silently_lost_ask`: prove the harness fails on a
    // genuinely non-reproducible run rather than passing everything.
    let divergence = check_reproducible(&NonDeterministic, 1)
        .expect_err("ambient nondeterminism must be caught");
    assert_eq!(divergence.workload, "non-deterministic");
    // The two runs emitted different-length streams (one extra ask's worth of
    // events), and the divergence names the first differing index.
    assert_ne!(divergence.left_len, divergence.right_len);
}

// --- Cluster utilities (utilities spec §2–§4, core §18.1) ----------------------

/// The singleton's handoff message; delivered locally by the manager, so it
/// needs no `register` entry.
#[derive(Clone, Serialize, Deserialize)]
struct Halt;

impl Message for Halt {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("repro.Halt");
}

impl Handler<Halt> for ReproGreeter {
    async fn handle(&mut self, _msg: Halt, ctx: &Ctx<Self>) {
        ctx.stop();
    }
}

const UTILITIES_SINGLETON: &str = "repro-singleton";

/// A workload over the cluster utilities, pinning *their* new deterministic
/// state: the singleton manager's tick loop and `SingletonStarted`/`Stopped`
/// event ordering as anchors move under the nemesis, the router's seeded
/// `Random` draws, and rendezvous selection (utilities spec §2–§4). Like every
/// utility, these must reproduce byte-for-byte from the seed (core §18.1 #1).
struct UtilitiesChurn {
    nodes: usize,
    rounds: u64,
}

impl ClusterWorkload for UtilitiesChurn {
    fn name(&self) -> &'static str {
        "repro-utilities-churn"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(200),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::Gossip {
            swim: self.swim(),
            downing: DowningPolicy::Conservative,
        }
    }

    fn setup(&self, ctx: &ClusterCtx) {
        for node in ctx.nodes() {
            let greeter = node.spawn(ReproGreeter);
            node.receptionist().register(GREETERS, &greeter);
            node.singleton(UTILITIES_SINGLETON, || ReproGreeter, Halt);
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimCluster> = ctx.nodes().to_vec();
        let rounds = self.rounds;
        Box::pin(async move {
            let clock = nodes[0].clock().clone();
            for round in 0..rounds {
                clock.sleep(Duration::from_millis(200)).await;
                for node in &nodes {
                    // A seeded-random pick, a rendezvous-hashed pick, and the
                    // singleton instance — every utility selection path runs
                    // every round, so a nondeterministic draw anywhere diverges
                    // the stream.
                    let random = Router::new(node, GREETERS, RouteStrategy::Random);
                    if let Some(routee) = random.route() {
                        let _ = routee.ask_timeout(ReproGreet, Duration::from_millis(500)).await;
                    }
                    let key = format!("k-{round}");
                    if let Some(routee) = random.route_by(key.as_bytes()) {
                        let _ = routee.ask_timeout(ReproGreet, Duration::from_millis(500)).await;
                    }
                    let proxy = node.singleton_proxy::<ReproGreeter>(UTILITIES_SINGLETON);
                    if let Some(instance) = proxy.resolve() {
                        let _ = instance
                            .ask_timeout(ReproGreet, Duration::from_millis(500))
                            .await;
                    }
                }
            }
        })
    }
}

#[test]
fn utilities_event_stream_is_byte_identical_across_seeds() {
    let workload = UtilitiesChurn {
        nodes: 3,
        rounds: 8,
    };
    if let Err(divergence) = replay_cluster_swarm(&workload, 0..24) {
        panic!("{divergence}");
    }
}
