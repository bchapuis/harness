//! Shared test support for the spec-conformance suite.
//!
//! Defines reusable actors and messages that work on *both* the single-node
//! `SimSystem` and the multi-node `SimCluster` (the actors are generic over the
//! system type — generic actors are allowed by the spec, §1.2), plus builders
//! for the common system topologies. Each conformance test file pulls this in
//! with `mod support;`.

#![allow(dead_code)]

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::LocalSystem;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::TerminationReason;
use actor_simulation::Recorder;
use actor_simulation::SimClock;
use actor_simulation::SimNetwork;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

/// Collected `Terminated` reasons a watcher observed.
pub type Reasons = Arc<Mutex<Vec<TerminationReason>>>;

// --- Messages ----------------------------------------------------------------

#[derive(Serialize, Deserialize)]
pub struct Greet {
    pub name: String,
}
impl Message for Greet {
    type Reply = String;
    const MANIFEST: Manifest = Manifest::new("conf.Greet");
}

/// Ask the actor to stop itself after handling this message.
#[derive(Serialize, Deserialize)]
pub struct Stop;
impl Message for Stop {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("conf.Stop");
}

/// Make the handler panic (a fault).
#[derive(Serialize, Deserialize)]
pub struct Boom;
impl Message for Boom {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("conf.Boom");
}

#[derive(Serialize, Deserialize)]
pub struct Inc;
impl Message for Inc {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("conf.Inc");
}

#[derive(Serialize, Deserialize)]
pub struct Get;
impl Message for Get {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("conf.Get");
}

// --- Greeter: greets, stops, or panics on demand -----------------------------

pub struct Greeter<S> {
    pub greeting: String,
    _system: PhantomData<fn() -> S>,
}

impl<S> Greeter<S> {
    pub fn new(greeting: impl Into<String>) -> Greeter<S> {
        Greeter {
            greeting: greeting.into(),
            _system: PhantomData,
        }
    }
}

impl<S: ActorSystem> Actor for Greeter<S> {
    type System = S;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Greet>();
        r.accept::<Stop>();
        r.accept::<Boom>();
    }
}

impl<S: ActorSystem> Handler<Greet> for Greeter<S> {
    async fn handle(&mut self, msg: Greet, _ctx: &Ctx<Self>) -> String {
        format!("{}, {}!", self.greeting, msg.name)
    }
}

impl<S: ActorSystem> Handler<Stop> for Greeter<S> {
    async fn handle(&mut self, _msg: Stop, ctx: &Ctx<Self>) {
        ctx.stop();
    }
}

impl<S: ActorSystem> Handler<Boom> for Greeter<S> {
    async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
        panic!("boom");
    }
}

// --- Counter: counts the messages it serves ----------------------------------

pub struct Counter<S> {
    pub count: u64,
    _system: PhantomData<fn() -> S>,
}

impl<S> Counter<S> {
    pub fn new() -> Counter<S> {
        Counter {
            count: 0,
            _system: PhantomData,
        }
    }
}

impl<S> Default for Counter<S> {
    fn default() -> Self {
        Counter::new()
    }
}

impl<S: ActorSystem> Actor for Counter<S> {
    type System = S;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Inc>();
        r.accept::<Get>();
    }
}

impl<S: ActorSystem> Handler<Inc> for Counter<S> {
    async fn handle(&mut self, _msg: Inc, _ctx: &Ctx<Self>) {
        self.count += 1;
    }
}

impl<S: ActorSystem> Handler<Get> for Counter<S> {
    async fn handle(&mut self, _msg: Get, _ctx: &Ctx<Self>) -> u64 {
        self.count
    }
}

// --- Slow: a handler that sleeps, for timeout and backpressure tests ---------

/// Sleeps for the requested time before replying, and tallies how many messages
/// it has served. Holds a `SimClock` directly so it works on any system type.
pub struct Slow<S> {
    clock: SimClock,
    served: u64,
    _system: PhantomData<fn() -> S>,
}

impl<S> Slow<S> {
    pub fn new(clock: SimClock) -> Slow<S> {
        Slow {
            clock,
            served: 0,
            _system: PhantomData,
        }
    }
}

/// Sleep `ms` of virtual time, then reply with the running served count.
#[derive(Serialize, Deserialize)]
pub struct Work {
    pub ms: u64,
}
impl Message for Work {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("conf.Work");
}

impl<S: ActorSystem> Actor for Slow<S> {
    type System = S;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Work>();
        r.accept::<Get>();
    }
}

impl<S: ActorSystem> Handler<Work> for Slow<S> {
    async fn handle(&mut self, msg: Work, _ctx: &Ctx<Self>) -> u64 {
        self.clock.sleep(Duration::from_millis(msg.ms)).await;
        self.served += 1;
        self.served
    }
}

impl<S: ActorSystem> Handler<Get> for Slow<S> {
    async fn handle(&mut self, _msg: Get, _ctx: &Ctx<Self>) -> u64 {
        self.served
    }
}

// --- Builders ----------------------------------------------------------------

/// A single-node system on the simulator.
pub fn local(seed: u64) -> (Simulation, SimSystem) {
    let sim = Simulation::new(seed);
    let system = LocalSystem::new(sim.clock(), sim.entropy(), sim.spawner());
    (sim, system)
}

/// A single-node system whose event stream is recorded (spec §16).
pub fn local_recorded(seed: u64) -> (Simulation, SimSystem, Recorder) {
    let sim = Simulation::new(seed);
    let recorder = Recorder::new();
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(Arc::new(recorder.clone()))
        .build();
    (sim, system, recorder)
}

/// A network for a multi-node cluster; call `.join(node)` to bring nodes up.
/// `swim` enables gossip-based membership (spec §9.4.4) with conservative
/// downing when `Some`; `None` runs static mode without a detector.
pub fn cluster(seed: u64, swim: Option<SwimConfig>) -> (Simulation, SimNetwork) {
    let sim = Simulation::new(seed);
    let mut net = SimNetwork::new(&sim);
    if let Some(config) = swim {
        net = net.with_gossip(config, DowningPolicy::Conservative);
    }
    (sim, net)
}
