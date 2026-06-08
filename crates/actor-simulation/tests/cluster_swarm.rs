//! Cluster swarm test (spec §18.3, §18.6): a discover-and-call workload swept
//! across seeds while a nemesis partitions and crashes nodes. The invariants —
//! no silent loss, serial execution, lifecycle, down-is-terminal — must hold on
//! every seed, and every call must complete (no hang).

use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::MembershipMode;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::NodeId;
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
use actor_simulation::ClusterWorkload;
use actor_simulation::SimCluster;
use actor_simulation::record_cluster_seed;
use actor_simulation::run_cluster_seed;
use actor_simulation::run_cluster_swarm;
use serde::Deserialize;
use serde::Serialize;

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
        downing: DowningPolicy::Timeout(Duration::from_millis(300)),
    }
}

/// Each node hosts and publishes a greeter; node 0 repeatedly discovers and
/// calls them, tolerating whatever failures the nemesis induces. Parameterized by
/// membership `mode` so the *same* workload and nemesis sweep static, autonomous,
/// and managed control planes (spec §9.4) — the safety invariants must hold under
/// every mode.
struct DiscoverAndCall {
    nodes: usize,
    rounds: u64,
    mode: MembershipMode,
}

impl ClusterWorkload for DiscoverAndCall {
    fn name(&self) -> &'static str {
        match self.mode {
            MembershipMode::Static => "discover-and-call/static",
            MembershipMode::Autonomous(_) => "discover-and-call/autonomous",
            MembershipMode::Managed { .. } => "discover-and-call/managed",
        }
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        swarm_swim()
    }

    fn mode(&self) -> MembershipMode {
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
        mode: MembershipMode::Autonomous(swarm_swim()),
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..48) {
        panic!("{failure}");
    }
}

#[test]
fn discover_and_call_holds_across_seeds_in_managed_mode() {
    // The same chaos under the **managed** control plane (spec §9.4): the detector
    // is observe-only, so a crashed node is never auto-downed — its in-flight calls
    // complete by timeout rather than the node-down cascade. The safety invariants
    // (no silent loss, serial, lifecycle, down-terminal) must still hold on every
    // seed. Node 1 is the designated leader.
    let workload = DiscoverAndCall {
        nodes: 3,
        rounds: 12,
        mode: MembershipMode::Managed {
            swim: swarm_swim(),
            leader: NodeId::new(1),
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
        mode: MembershipMode::Static,
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
struct WatchUnderChaos {
    nodes: usize,
    rounds: u64,
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
            downing: DowningPolicy::Timeout(Duration::from_millis(300)),
        }
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
                for (i, target) in watcher_node.receptionist().lookup(TARGETS).iter().enumerate() {
                    let poke = if (round as usize + i) % 2 == 0 {
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

#[test]
fn watch_under_chaos_upholds_safety_invariants_across_seeds() {
    let workload = WatchUnderChaos {
        nodes: 3,
        rounds: 12,
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

#[test]
fn a_cluster_seed_replays() {
    let workload = DiscoverAndCall {
        nodes: 3,
        rounds: 8,
        mode: MembershipMode::Autonomous(swarm_swim()),
    };
    assert!(run_cluster_seed(&workload, 123).is_ok());
    assert!(run_cluster_seed(&workload, 123).is_ok());
}
