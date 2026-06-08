//! The determinism contract, enforced over the real system (spec §18.1 #1).
//!
//! The foundational guarantee of deterministic simulation is that a seed
//! reproduces an entire run exactly. Here it is checked the strong way: a
//! workload is run twice under one
//! seed and the two recorded *event streams* (spec §16) must be byte-identical —
//! swept across many seeds, for both a single-node and a multi-node-under-faults
//! workload. A `negative` test then proves the check has teeth: a workload with a
//! deliberate determinism leak (ambient state outside the seed) is caught, with
//! the first divergence pinpointed.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Key;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterWorkload;
use actor_simulation::SimCluster;
use actor_simulation::SimSystem;
use actor_simulation::Workload;
use actor_simulation::check_cluster_reproducible;
use actor_simulation::check_reproducible;
use actor_simulation::replay_cluster_swarm;
use actor_simulation::replay_swarm;
use serde::Deserialize;
use serde::Serialize;

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
    const MANIFEST: Manifest = Manifest::new("repro.Greet");
}

impl Handler<Greet> for Greeter {
    async fn handle(&mut self, _msg: Greet, _ctx: &Ctx<Self>) -> String {
        "hi".into()
    }
}

const GREETERS: Key<Greeter> = Key::new("repro.greeters");

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
            downing: DowningPolicy::Timeout(Duration::from_millis(300)),
        }
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
                clock.sleep(Duration::from_millis(200)).await;
                for service in caller.receptionist().lookup(GREETERS).iter() {
                    let _ = service.ask_timeout(Greet, Duration::from_millis(500)).await;
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
