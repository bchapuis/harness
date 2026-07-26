//! Shared test support for the spec-conformance suite.
//!
//! Defines reusable actors and messages that work on *both* the single-node
//! `SimSystem` and the multi-node `SimNode` (the actors are generic over the
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
use actor_core::Entropy;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::LocalSystem;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::TerminationReason;
use actor_simulation::History;
use actor_simulation::Recorder;
use actor_simulation::Register;
use actor_simulation::RegisterOp;
use actor_simulation::RegisterRet;
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

/// Ask the actor to stop itself after handling this message. `Clone` so it can
/// serve as a singleton's reusable handoff message.
#[derive(Clone, Serialize, Deserialize)]
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

// --- Linearizability: the shared register object (spec §18.4) ----------------
//
// Shared by `conformance_linearizability.rs` (single-node scenarios) and
// `conformance_linearizability_swarm.rs` (the cluster sweep), so both decide a
// history against the *same* object rather than two copies that can drift.
// --- The register actor -------------------------------------------------------

// A system-generic register so the same actor runs on `SimSystem` and
// `SimNode` (generic actors are allowed by the spec, §1.2).

pub struct RegisterActorIn<S> {
    value: i64,
    _s: PhantomData<fn() -> S>,
}

impl<S> RegisterActorIn<S> {
    pub fn new() -> Self {
        RegisterActorIn {
            value: 0,
            _s: PhantomData,
        }
    }
}

impl<S: ActorSystem> Actor for RegisterActorIn<S> {
    type System = S;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Read>();
        r.accept::<Write>();
        r.accept::<Cas>();
    }
}

#[derive(Serialize, Deserialize)]
pub struct Read;
impl Message for Read {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("lin.Read");
}

#[derive(Serialize, Deserialize)]
pub struct Write(pub i64);
impl Message for Write {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("lin.Write");
}

#[derive(Serialize, Deserialize)]
pub struct Cas(pub i64, pub i64);
impl Message for Cas {
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("lin.Cas");
}

impl<S: ActorSystem> Handler<Read> for RegisterActorIn<S> {
    async fn handle(&mut self, _msg: Read, _ctx: &Ctx<Self>) -> i64 {
        self.value
    }
}

impl<S: ActorSystem> Handler<Write> for RegisterActorIn<S> {
    async fn handle(&mut self, msg: Write, _ctx: &Ctx<Self>) {
        self.value = msg.0;
    }
}

impl<S: ActorSystem> Handler<Cas> for RegisterActorIn<S> {
    async fn handle(&mut self, msg: Cas, _ctx: &Ctx<Self>) -> bool {
        if self.value == msg.0 {
            self.value = msg.1;
            true
        } else {
            false
        }
    }
}

// --- A client process: picks ops from the seeded stream and records them ------

/// One client's traffic: `ops` operations against the shared register, each
/// recorded into the shared history. Values are drawn from a small domain so
/// reads, writes, and CASes actually interact (CASes sometimes match).
pub async fn client<R>(
    reg: R,
    history: History<Register>,
    entropy: actor_simulation::SimEntropy,
    ops: u64,
) where
    R: RegisterRef,
{
    for _ in 0..ops {
        match entropy.next_u64() % 3 {
            0 => {
                let id = history.invoke(RegisterOp::Read);
                match reg.read().await {
                    Ok(v) => history.ok(id, RegisterRet::Read(v)),
                    Err(()) => history.info(id),
                }
            }
            1 => {
                let v = (entropy.next_u64() % 4) as i64;
                let id = history.invoke(RegisterOp::Write(v));
                match reg.write(v).await {
                    Ok(()) => history.ok(id, RegisterRet::WriteOk),
                    Err(()) => history.info(id),
                }
            }
            _ => {
                let old = (entropy.next_u64() % 4) as i64;
                let new = (entropy.next_u64() % 4) as i64;
                let id = history.invoke(RegisterOp::Cas(old, new));
                match reg.cas(old, new).await {
                    Ok(b) => history.ok(id, RegisterRet::Cas(b)),
                    Err(()) => history.info(id),
                }
            }
        }
    }
}

/// A uniform calling surface over the register, so the same client code drives it
/// both locally and across the network. `Err(())` means the outcome is unknown
/// (any `CallError`) — recorded as a pending operation.
pub trait RegisterRef: Clone {
    fn read(&self) -> BoxFuture<'static, Result<i64, ()>>;
    fn write(&self, v: i64) -> BoxFuture<'static, Result<(), ()>>;
    fn cas(&self, old: i64, new: i64) -> BoxFuture<'static, Result<bool, ()>>;
}

