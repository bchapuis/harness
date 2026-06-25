//! Conformance: swarm testing (spec §18.4, §18.6) — single-node and cluster
//! workloads swept across seeds under fault injection, with continuous invariant
//! checking and fault-coverage assertions.

use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::BoxError;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Key;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::Terminated;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::RegistryFaultPolicy;
use actor_simulation::SimCluster;
use actor_simulation::SimSystem;
use actor_simulation::Workload;
use actor_simulation::record_cluster_seed;
use actor_simulation::run_cluster_seed;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_seed;
use actor_simulation::run_swarm;
use serde::Deserialize;
use serde::Serialize;

// === Single-node swarm (spec §18.4, §18.6) ====================================

// --- Actors used by the workloads ---------------------------------------------

/// Echoes a counter back; also tallies how many it served (private state).
struct Echo {
    served: u64,
}

impl Actor for Echo {
    type System = SimSystem;
}

#[derive(Serialize, Deserialize)]
struct Ping(u64);

impl Message for Ping {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("swarm.Ping");
}

impl Handler<Ping> for Echo {
    async fn handle(&mut self, msg: Ping, _ctx: &Ctx<Self>) -> u64 {
        self.served += 1;
        msg.0
    }
}

/// Sleeps mid-handler, so concurrent asks would overlap if execution were not
/// serial — exercising the serial-execution invariant under random scheduling.
struct Worker {
    clock: actor_simulation::SimClock,
}

impl Actor for Worker {
    type System = SimSystem;
}

#[derive(Serialize, Deserialize)]
struct Job(u64);

impl Message for Job {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("swarm.Job");
}

impl Handler<Job> for Worker {
    async fn handle(&mut self, msg: Job, _ctx: &Ctx<Self>) -> u64 {
        self.clock.sleep(Duration::from_millis(1)).await;
        msg.0
    }
}

// --- Workloads ----------------------------------------------------------------

/// Fan out asks across several echo actors and verify every reply.
struct AskStorm {
    actors: usize,
    asks_per_actor: u64,
}

impl Workload for AskStorm {
    fn name(&self) -> &'static str {
        "ask-storm"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let actors = self.actors;
        let asks = self.asks_per_actor;
        Box::pin(async move {
            let refs: Vec<_> = (0..actors)
                .map(|_| system.spawn(Echo { served: 0 }))
                .collect();
            let mut futures = Vec::new();
            for r in &refs {
                for i in 0..asks {
                    futures.push(r.ask(Ping(i)));
                }
            }
            let replies = futures::future::join_all(futures).await;
            // Per-actor FIFO: replies for each actor come back in send order.
            for chunk in replies.chunks(asks as usize) {
                let got: Vec<u64> = chunk.iter().map(|r| r.clone().unwrap()).collect();
                assert_eq!(got, (0..asks).collect::<Vec<_>>());
            }
        })
    }
}

/// Hammer a single slow worker with concurrent asks; serial execution must hold
/// regardless of scheduling order.
struct ConcurrentLoad {
    asks: u64,
}

impl Workload for ConcurrentLoad {
    fn name(&self) -> &'static str {
        "concurrent-load"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let asks = self.asks;
        Box::pin(async move {
            let clock = system.clock().clone();
            let worker = system.spawn(Worker { clock });
            let futures: Vec<_> = (0..asks).map(|i| worker.ask(Job(i))).collect();
            let replies = futures::future::join_all(futures).await;
            for (i, reply) in replies.into_iter().enumerate() {
                assert_eq!(reply, Ok(i as u64));
            }
        })
    }
}

// --- Tests --------------------------------------------------------------------

#[test]
fn ask_storm_holds_across_seeds() {
    let workload = AskStorm {
        actors: 4,
        asks_per_actor: 8,
    };
    // Each seed perturbs scheduling order and mailbox capacity; all must pass.
    if let Err(failure) = run_swarm(&workload, 0..256) {
        panic!("{failure}");
    }
}

#[test]
fn concurrent_load_stays_serial_across_seeds() {
    let workload = ConcurrentLoad { asks: 16 };
    if let Err(failure) = run_swarm(&workload, 0..256) {
        panic!("{failure}");
    }
}

#[test]
fn a_single_seed_replays_identically() {
    // Reproduction (spec §18.6): the same seed yields the same outcome.
    let workload = AskStorm {
        actors: 3,
        asks_per_actor: 5,
    };
    assert!(run_seed(&workload, 12345).is_ok());
    assert!(run_seed(&workload, 12345).is_ok());
}

/// A workload that abandons an in-flight ask, to prove the harness actually
/// catches a silently-lost call rather than passing everything.
struct DropsAnAsk;

impl Workload for DropsAnAsk {
    fn name(&self) -> &'static str {
        "drops-an-ask"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let clock = system.clock().clone();
            let worker = system.spawn(Worker {
                clock: system.clock().clone(),
            });
            // Issue an ask, then drop its future before it can complete by
            // racing it against an immediately-ready branch.
            let ask = worker.ask(Job(1));
            let done = std::future::ready(());
            futures::pin_mut!(ask, done);
            let _ = futures::future::select(ask, done).await;
            // Give the worker's own timer somewhere to land so the run is not
            // empty; the abandoned ask never reaches an outcome.
            clock.sleep(Duration::from_millis(5)).await;
        })
    }
}

#[test]
fn harness_detects_a_silently_lost_ask() {
    let failure = run_seed(&DropsAnAsk, 1).expect_err("abandoned ask must be caught");
    assert!(
        failure
            .violations
            .iter()
            .any(|v| v.invariant == "no-silent-loss"),
        "expected a no-silent-loss violation, got: {failure}",
    );
}

// --- Fault-injecting supervision workload (spec §18.3) ------------------------

/// A service whose handler panics at seed-controlled points (`buggify`) and
/// restarts. Whatever the scheduling and fault timing, every call must still
/// complete and the invariants must hold.
struct Flaky;

impl Actor for Flaky {
    type System = SimSystem;

    fn supervision() -> actor_core::Supervision {
        // A generous window so injected faults restart rather than escalate.
        actor_core::Supervision::restart(1000, Duration::from_secs(3600), actor_core::Backoff::None)
    }
}

#[derive(Serialize, Deserialize)]
struct Work(u64);

impl Message for Work {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("swarm.Work");
}

impl Handler<Work> for Flaky {
    async fn handle(&mut self, msg: Work, ctx: &Ctx<Self>) -> u64 {
        use actor_core::Entropy;
        // Inject a fault on roughly one call in four (spec §18.3).
        if ctx.system().entropy().buggify(1, 4) {
            panic!("injected fault");
        }
        msg.0
    }
}

struct FlakyService {
    rounds: u64,
}

impl Workload for FlakyService {
    fn name(&self) -> &'static str {
        "flaky-service"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let rounds = self.rounds;
        Box::pin(async move {
            let service = system.spawn_with(|| Flaky);
            for i in 0..rounds {
                // Each call completes — `Ok` or `DeadLetter` (its handler
                // faulted) — never hanging; the restart keeps the service alive.
                let _ = service.ask(Work(i)).await;
            }
        })
    }
}

#[test]
fn flaky_service_survives_injected_faults_across_seeds() {
    let workload = FlakyService { rounds: 24 };
    if let Err(failure) = run_swarm(&workload, 0..256) {
        panic!("{failure}");
    }
}

// === Cluster swarm (spec §18.3, §18.6) ========================================

struct Greeter;

impl Actor for Greeter {
    type System = SimCluster;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Greet>();
    }
}

#[derive(Serialize, Deserialize)]
struct Greet;

impl Message for Greet {
    type Reply = String;
    const MANIFEST: Manifest = Manifest::new("swarm.Greet");
}

impl Handler<Greet> for Greeter {
    async fn handle(&mut self, _msg: Greet, _ctx: &Ctx<Self>) -> String {
        "hello".into()
    }
}

const GREETERS: Key<Greeter> = Key::new("greeters");

/// The SWIM parameters the swarm runs under (when a mode uses a detector).
fn swarm_swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(200),
        indirect_count: 2,
    }
}

/// Each node hosts and publishes a greeter; node 0 repeatedly discovers and
/// calls them, tolerating whatever failures the nemesis induces. Parameterized by
/// membership `mode` so the *same* workload and nemesis sweep the static,
/// gossip-based, registry-based, and leader-based control planes (spec §9.4) —
/// the safety invariants must hold under every mode.
struct DiscoverAndCall {
    nodes: usize,
    rounds: u64,
    mode: ClusterModeSpec,
}

impl ClusterWorkload for DiscoverAndCall {
    fn name(&self) -> &'static str {
        match self.mode {
            ClusterModeSpec::Static { .. } => "discover-and-call/static",
            ClusterModeSpec::Gossip { .. } => "discover-and-call/gossip",
            ClusterModeSpec::Registry { .. } => "discover-and-call/registry",
            ClusterModeSpec::Leader { .. } => "discover-and-call/leader",
        }
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        swarm_swim()
    }

    fn mode(&self) -> ClusterModeSpec {
        self.mode
    }

    fn setup(&self, ctx: &ClusterCtx) {
        for node in ctx.nodes() {
            let greeter = node.spawn(Greeter);
            node.receptionist().register(GREETERS, &greeter);
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let caller = ctx.nodes()[0].clone();
        let rounds = self.rounds;
        Box::pin(async move {
            let clock = caller.clock().clone();
            for _ in 0..rounds {
                // Yield so replication and the nemesis make progress.
                clock.sleep(Duration::from_millis(200)).await;
                let listing = caller.receptionist().lookup(GREETERS);
                for service in listing.iter() {
                    // Every outcome (Ok / Unreachable / Timeout / DeadLetter) is
                    // acceptable; the invariant is that the call *completes*.
                    let _ = service.ask_timeout(Greet, Duration::from_millis(500)).await;
                }
            }
        })
    }
}

#[test]
fn discover_and_call_holds_across_seeds_under_faults() {
    let workload = DiscoverAndCall {
        nodes: 3,
        rounds: 12,
        mode: ClusterModeSpec::Gossip {
            swim: swarm_swim(),
            downing: DowningPolicy::Timeout(Duration::from_millis(300)),
        },
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..48) {
        panic!("{failure}");
    }
}

#[test]
fn discover_and_call_holds_across_seeds_in_registry_mode() {
    // The same chaos under the **registry-based** control plane (spec §9.4.2):
    // the detector is observe-only, so a crashed node is never auto-downed — its
    // in-flight calls complete by timeout rather than the node-down cascade. The
    // safety invariants (no silent loss, serial, lifecycle, down-terminal) must
    // still hold on every seed, with the registry sync itself faulted (latency
    // and stale reads, spec §18.3).
    let workload = DiscoverAndCall {
        nodes: 3,
        rounds: 12,
        mode: ClusterModeSpec::Registry {
            swim: swarm_swim(),
            sync_interval: Duration::from_millis(100),
            faults: RegistryFaultPolicy {
                max_latency: Duration::from_millis(50),
                stale_num: 1,
                stale_den: 4,
                max_staleness: 4,
            },
        },
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..48) {
        panic!("{failure}");
    }
}

#[test]
fn discover_and_call_holds_across_seeds_in_leader_mode() {
    // The same chaos under the **leader-based** control plane (spec §9.4.3):
    // the nemesis crashes and partitions voters, forcing elections and quorum
    // loss mid-traffic. The safety invariants — now including one-leader-per-term
    // (invariant #22's election-safety half) — must hold on every seed.
    let workload = DiscoverAndCall {
        nodes: 3,
        rounds: 12,
        mode: ClusterModeSpec::Leader {
            swim: swarm_swim(),
            voters: 3,
            election_timeout: Duration::from_millis(500),
            heartbeat_interval: Duration::from_millis(100),
            downing: DowningPolicy::Timeout(Duration::from_millis(300)),
        },
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..48) {
        panic!("{failure}");
    }
}

#[test]
fn discover_and_call_holds_across_seeds_in_static_mode() {
    // And under the **static** control plane (spec §9.4): no detector at all, so
    // membership never changes under the nemesis and discovery never re-converges.
    // Calls to crashed nodes complete by timeout; the safety invariants must hold
    // with no failure detection in the loop.
    let workload = DiscoverAndCall {
        nodes: 3,
        rounds: 12,
        mode: ClusterModeSpec::Static { detector: None },
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..48) {
        panic!("{failure}");
    }
}

// --- Death-watch under the nemesis (spec §12, §8.1; invariant #11) ------------

// A remotely-addressable actor that stops or fails on demand.
struct Target;
impl Actor for Target {
    type System = SimCluster;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Poke>();
    }
}

#[derive(Serialize, Deserialize)]
enum Poke {
    Stop,
    Fail,
}
impl Message for Poke {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("swarm.Poke");
}
impl Handler<Poke> for Target {
    async fn handle(&mut self, msg: Poke, ctx: &Ctx<Self>) {
        match msg {
            Poke::Stop => ctx.stop(),
            Poke::Fail => panic!("induced fault"),
        }
    }
}

// Watches one remote target from `started`; the `Terminated` it receives is
// observed by the continuous `DeathWatchAtMostOnce` checker, not asserted here.
struct WatchProbe {
    target: ActorRef<Target>,
}
impl Actor for WatchProbe {
    type System = SimCluster;
    async fn started(&mut self, ctx: &Ctx<Self>) -> Result<(), BoxError> {
        ctx.watch(&self.target);
        Ok(())
    }
}
impl Handler<Terminated> for WatchProbe {
    async fn handle(&mut self, _signal: Terminated, _ctx: &Ctx<Self>) {}
}

const TARGETS: Key<Target> = Key::new("watch-targets");

/// Node 0 watches every other node's target, then drives those targets to stop
/// or fail while the nemesis independently crashes and partitions nodes. Three
/// causes of `Terminated` then race per target — a graceful stop frame, a fault,
/// and a synthesized `NodeDown` (spec §8.1 step 4). This drives the death-watch
/// paths — local delivery, cross-node frame forwarding, and node-down synthesis —
/// across the whole fault space, where the standing safety invariants
/// (no-silent-loss, serial, lifecycle, down-terminal) must still hold.
/// Parameterized by `mode` so both the gossip coordinator's downing and the
/// Raft leader's quorum-committed downing feed the node-down synthesis path.
struct WatchUnderChaos {
    nodes: usize,
    rounds: u64,
    mode: ClusterModeSpec,
}

impl ClusterWorkload for WatchUnderChaos {
    fn name(&self) -> &'static str {
        "watch-under-chaos"
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
        self.mode
    }

    fn setup(&self, ctx: &ClusterCtx) {
        for node in ctx.nodes() {
            let target = node.spawn(Target);
            node.receptionist().register(TARGETS, &target);
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let watcher_node = ctx.nodes()[0].clone();
        let rounds = self.rounds;
        Box::pin(async move {
            let clock = watcher_node.clock().clone();
            // Let registrations replicate, then watch every discovered target.
            clock.sleep(Duration::from_millis(300)).await;
            for target in watcher_node.receptionist().lookup(TARGETS).iter() {
                watcher_node.spawn(WatchProbe {
                    target: target.clone(),
                });
            }
            // Poke targets to stop/fail while the nemesis crashes nodes; whatever
            // outcome occurs, a watcher never sees two `Terminated` for one target.
            for round in 0..rounds {
                clock.sleep(Duration::from_millis(200)).await;
                for (i, target) in watcher_node
                    .receptionist()
                    .lookup(TARGETS)
                    .iter()
                    .enumerate()
                {
                    let poke = if (round as usize + i).is_multiple_of(2) {
                        Poke::Stop
                    } else {
                        Poke::Fail
                    };
                    let _ = target.ask_timeout(poke, Duration::from_millis(300)).await;
                }
            }
        })
    }
}

fn watch_gossip_mode(workload_swim: SwimConfig) -> ClusterModeSpec {
    ClusterModeSpec::Gossip {
        swim: workload_swim,
        downing: DowningPolicy::Timeout(Duration::from_millis(300)),
    }
}

fn watch_leader_mode(workload_swim: SwimConfig) -> ClusterModeSpec {
    ClusterModeSpec::Leader {
        swim: workload_swim,
        voters: 3,
        election_timeout: Duration::from_millis(500),
        heartbeat_interval: Duration::from_millis(100),
        downing: DowningPolicy::Timeout(Duration::from_millis(300)),
    }
}

fn watch_swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(200),
        indirect_count: 2,
    }
}

#[test]
fn watch_under_chaos_upholds_safety_invariants_across_seeds() {
    let workload = WatchUnderChaos {
        nodes: 3,
        rounds: 12,
        mode: watch_gossip_mode(watch_swim()),
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..48) {
        panic!("{failure}");
    }
}

#[test]
fn watch_under_chaos_upholds_safety_invariants_in_leader_mode() {
    // The leader-mode twin: `NodeDown` synthesis is driven by quorum-committed
    // `Down` entries instead of the coordinator's policy (spec §9.4.3 item 4),
    // racing graceful stops and faults across the same fault space.
    let workload = WatchUnderChaos {
        nodes: 3,
        rounds: 12,
        mode: watch_leader_mode(watch_swim()),
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..48) {
        panic!("{failure}");
    }
}

#[test]
fn watch_under_chaos_never_double_delivers_a_terminated() {
    // Each watcher here watches its target exactly once, so it must observe at
    // most one `Terminated` — even as graceful-stop, fault, and node-down causes
    // race across the fault space. (This is *not* the general invariant: watching
    // the same target twice legitimately yields two signals, spec §12. It is a
    // workload-specific guard against `TerminatedDelivered` being emitted more
    // than once per real delivery — e.g. once where a node forwards the signal to
    // a remote watcher and again where it actually lands.)
    use std::collections::BTreeMap;
    let workload = WatchUnderChaos {
        nodes: 3,
        rounds: 12,
        mode: watch_gossip_mode(watch_swim()),
    };
    for seed in 0..48u64 {
        let events = record_cluster_seed(&workload, seed);
        let mut delivered: BTreeMap<(String, String), usize> = BTreeMap::new();
        for event in &events {
            if let actor_core::Event::TerminatedDelivered {
                target, watcher, ..
            } = event
            {
                *delivered
                    .entry((format!("{watcher}"), format!("{target}")))
                    .or_default() += 1;
            }
        }
        for ((watcher, target), count) in &delivered {
            assert!(
                *count == 1,
                "seed {seed}: watcher {watcher} received {count} Terminated for {target}, expected 1",
            );
        }
    }
}

// --- Restart churn (spec §9.4.3 item 2, §18.3) ---------------------------------

/// Leader-mode traffic while voters restart every round: each restarted voter
/// reloads its persisted term, vote, and log through the per-node storage seam.
/// This is the regression guard for the classic persistence bug — a voter that
/// forgets `voted_for` across a restart can grant a second vote in the same
/// term and elect two leaders, which the continuous `OneLeaderPerTerm` checker
/// (invariant #22) would catch on some seed of the sweep.
struct RestartChurn {
    nodes: usize,
    rounds: u64,
}

impl ClusterWorkload for RestartChurn {
    fn name(&self) -> &'static str {
        "restart-churn"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        swarm_swim()
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
            let greeter = node.spawn(Greeter);
            node.receptionist().register(GREETERS, &greeter);
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        // Node 1 stays up as the caller; voters 2 and 3 restart on alternating
        // rounds — often mid-election, thanks to the nemesis's partitions.
        let caller = ctx.nodes()[0].clone();
        let net = ctx.net().clone();
        let victims = [ctx.nodes()[1].node(), ctx.nodes()[2].node()];
        let rounds = self.rounds;
        Box::pin(async move {
            let clock = caller.clock().clone();
            for round in 0..rounds {
                clock.sleep(Duration::from_millis(300)).await;
                net.restart(victims[(round % 2) as usize]);
                for service in caller.receptionist().lookup(GREETERS).iter() {
                    // Any outcome is acceptable (a restarted host loses its
                    // actors); the invariant is completion, and — via the
                    // checker — election safety across all the churn.
                    let _ = service.ask_timeout(Greet, Duration::from_millis(500)).await;
                }
            }
        })
    }
}

#[test]
fn restart_churn_upholds_election_safety_across_seeds() {
    let workload = RestartChurn {
        nodes: 3,
        rounds: 10,
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..48) {
        panic!("{failure}");
    }
}

// --- Cluster singleton under the nemesis (utilities spec §4, invariant U2) ----

/// A singleton instance that greets and stops on its handoff message.
struct Highlander;
impl Actor for Highlander {
    type System = SimCluster;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Greet>();
        r.accept::<Halt>();
    }
}
impl Handler<Greet> for Highlander {
    async fn handle(&mut self, _msg: Greet, _ctx: &Ctx<Self>) -> String {
        "there can be only one".into()
    }
}

/// The singleton's handoff message (utilities spec §4 item 2).
#[derive(Clone, Serialize, Deserialize)]
struct Halt;
impl Message for Halt {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("swarm.Halt");
}
impl Handler<Halt> for Highlander {
    async fn handle(&mut self, _msg: Halt, ctx: &Ctx<Self>) {
        ctx.stop();
    }
}

const SINGLETON: &str = "swarm-highlander";

/// The converged-exactly-one half of U2, checked at quiescence: after the drive
/// ends with a heal and a settle window, exactly one activation per name is
/// live. (The per-node half rides along continuously in `default_invariants`;
/// mid-run dual activation across nodes is legal divergence and not flagged.)
#[derive(Default)]
struct SingletonConverged {
    open: Vec<actor_core::ActorId>,
    ever_started: bool,
}

impl actor_simulation::Invariant for SingletonConverged {
    fn name(&self) -> &'static str {
        "singleton-converged-exactly-one"
    }

    fn observe(&mut self, event: &actor_core::Event) -> Result<(), String> {
        match event {
            actor_core::Event::SingletonStarted { actor, .. } => {
                self.ever_started = true;
                self.open.push(actor.clone());
            }
            actor_core::Event::SingletonStopped { actor, .. } => {
                self.open.retain(|a| a != actor);
            }
            _ => {}
        }
        Ok(())
    }

    fn at_quiescence(&mut self) -> Result<(), String> {
        if !self.ever_started {
            return Err("no singleton activation ever happened (liveness)".into());
        }
        if self.open.len() != 1 {
            return Err(format!(
                "{} live activations at quiescence on a healed, converged cluster \
                 (expected exactly 1): {:?}",
                self.open.len(),
                self.open
            ));
        }
        Ok(())
    }
}

/// Every node hosts the singleton while every node calls it through its proxy
/// and the nemesis crashes and partitions at will. The drive outlasts the
/// nemesis (whose rounds end within ~7s of virtual time), then heals and lets
/// views reconverge, so the at-quiescence exactly-one check is sound — a run
/// that ended mid-partition could legally hold two instances. Parameterized by
/// `mode` so the same chaos sweeps all four control planes (spec §9.4); in
/// static mode the anchor simply never moves, and the checks still hold.
struct SingletonChaos {
    nodes: usize,
    mode: ClusterModeSpec,
}

impl ClusterWorkload for SingletonChaos {
    fn name(&self) -> &'static str {
        match self.mode {
            ClusterModeSpec::Static { .. } => "singleton-chaos/static",
            ClusterModeSpec::Gossip { .. } => "singleton-chaos/gossip",
            ClusterModeSpec::Registry { .. } => "singleton-chaos/registry",
            ClusterModeSpec::Leader { .. } => "singleton-chaos/leader",
        }
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        swarm_swim()
    }

    fn mode(&self) -> ClusterModeSpec {
        self.mode
    }

    fn invariants(&self) -> Vec<Box<dyn actor_simulation::Invariant>> {
        let mut invariants = actor_simulation::default_invariants();
        invariants.push(Box::new(SingletonConverged::default()));
        invariants
    }

    fn setup(&self, ctx: &ClusterCtx) {
        for node in ctx.nodes() {
            node.singleton(SINGLETON, || Highlander, Halt);
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimCluster> = ctx.nodes().to_vec();
        let net = ctx.net().clone();
        Box::pin(async move {
            let clock = nodes[0].clock().clone();
            // Chaos phase: every node keeps calling its proxy; any outcome —
            // Ok, DeadLetter in a handoff gap, Timeout across a partition — is
            // acceptable, the invariant is that calls complete and activations
            // stay disciplined.
            for _ in 0..12 {
                clock.sleep(Duration::from_millis(400)).await;
                for node in &nodes {
                    let proxy = node.singleton_proxy::<Highlander>(SINGLETON);
                    if let Some(instance) = proxy.resolve() {
                        let _ = instance
                            .ask_timeout(Greet, Duration::from_millis(500))
                            .await;
                    }
                }
            }
            // Outlast the nemesis entirely, then heal and reconverge so the
            // at-quiescence exactly-one check is meaningful.
            clock.sleep(Duration::from_secs(3)).await;
            net.heal();
            clock.sleep(Duration::from_secs(3)).await;
        })
    }
}

#[test]
fn singleton_chaos_converges_to_one_instance_across_seeds() {
    // Conservative downing: with an aggressive gossip timeout, the nemesis's
    // total partitions make every isolated coordinator *terminally* down the
    // others, legally fracturing one cluster into several permanent one-node
    // clusters — each then correctly runs its own singleton, and a global
    // exactly-one no longer applies. The downing-driven handoff path is pinned
    // by `conformance_singleton.rs` (anchor crash) under controlled faults.
    let workload = SingletonChaos {
        nodes: 3,
        mode: ClusterModeSpec::Gossip {
            swim: swarm_swim(),
            downing: DowningPolicy::Conservative,
        },
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..48) {
        panic!("{failure}");
    }
}

#[test]
fn singleton_chaos_converges_in_registry_mode() {
    let workload = SingletonChaos {
        nodes: 3,
        mode: ClusterModeSpec::Registry {
            swim: swarm_swim(),
            sync_interval: Duration::from_millis(100),
            faults: RegistryFaultPolicy {
                max_latency: Duration::from_millis(50),
                stale_num: 1,
                stale_den: 4,
                max_staleness: 4,
            },
        },
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..48) {
        panic!("{failure}");
    }
}

#[test]
fn singleton_chaos_converges_in_leader_mode() {
    let workload = SingletonChaos {
        nodes: 3,
        mode: ClusterModeSpec::Leader {
            swim: swarm_swim(),
            voters: 3,
            election_timeout: Duration::from_millis(500),
            heartbeat_interval: Duration::from_millis(100),
            downing: DowningPolicy::Timeout(Duration::from_millis(300)),
        },
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..48) {
        panic!("{failure}");
    }
}

#[test]
fn singleton_chaos_converges_in_static_mode() {
    // No detector: reachability never changes, the anchor never moves, and the
    // nemesis's crashes are invisible to membership — but the partition still
    // blocks frames, so calls fail and must fail *fast and completely*.
    let workload = SingletonChaos {
        nodes: 3,
        mode: ClusterModeSpec::Static { detector: None },
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..48) {
        panic!("{failure}");
    }
}

#[test]
fn a_cluster_seed_replays() {
    let workload = DiscoverAndCall {
        nodes: 3,
        rounds: 8,
        mode: ClusterModeSpec::Gossip {
            swim: swarm_swim(),
            downing: DowningPolicy::Timeout(Duration::from_millis(300)),
        },
    };
    assert!(run_cluster_seed(&workload, 123).is_ok());
    assert!(run_cluster_seed(&workload, 123).is_ok());
}
