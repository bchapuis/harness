//! Multi-node routing over the simulated network (spec §4, §7). The same
//! `ask`/`tell` call site works whether the target is local or remote
//! (invariant #21), real JSON crosses every hop, and node/actor failures surface
//! as the right `CallError`.

use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Path;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

type Sys = SimCluster;

struct Greeter {
    greeting: String,
}

impl Actor for Greeter {
    type System = Sys;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Greet>();
    }
}

#[derive(Serialize, Deserialize)]
struct Greet {
    name: String,
}

impl Message for Greet {
    type Reply = String;
    const MANIFEST: Manifest = Manifest::new("test.Greet");
}

impl Handler<Greet> for Greeter {
    async fn handle(&mut self, msg: Greet, _ctx: &Ctx<Self>) -> String {
        format!("{}, {}!", self.greeting, msg.name)
    }
}

/// A two-node network on one simulation.
fn two_nodes(seed: u64) -> (Simulation, SimCluster, SimCluster) {
    let sim = Simulation::new(seed);
    let net = SimNetwork::new(&sim);
    let a = net.join(NodeId::new(1));
    let b = net.join(NodeId::new(2));
    (sim, a, b)
}

#[test]
fn remote_ask_crosses_nodes() {
    let (sim, node_a, node_b) = two_nodes(1);
    let reply = sim.block_on(async move {
        let greeter = node_a.spawn(Greeter {
            greeting: "Hello".into(),
        });
        // node_b holds no local actor for this id, so the call routes to node_a.
        let remote = node_b.resolve::<Greeter>(greeter.id().clone());
        remote
            .ask(Greet {
                name: "world".into(),
            })
            .await
    });
    assert_eq!(reply, Ok("Hello, world!".to_string()));
}

#[test]
fn location_transparency_local_vs_remote() {
    // Differential check (invariant #21): identical replies from the same call
    // site, target local versus remote.
    let (sim, node_a, node_b) = two_nodes(2);
    let (local, remote) = sim.block_on(async move {
        let greeter = node_a.spawn(Greeter {
            greeting: "Hi".into(),
        });
        let id = greeter.id().clone();
        let local = greeter.ask(Greet { name: "x".into() }).await;
        let remote = node_b
            .resolve::<Greeter>(id)
            .ask(Greet { name: "x".into() })
            .await;
        (local, remote)
    });
    assert_eq!(local, remote);
    assert_eq!(local, Ok("Hi, x!".to_string()));
}

// --- A counter to exercise remote tell + per-pair FIFO ------------------------

struct Counter {
    n: u64,
}

impl Actor for Counter {
    type System = Sys;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Inc>();
        r.accept::<Get>();
    }
}

#[derive(Serialize, Deserialize)]
struct Inc;

impl Message for Inc {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.Inc");
}

#[derive(Serialize, Deserialize)]
struct Get;

impl Message for Get {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("test.Get");
}

impl Handler<Inc> for Counter {
    async fn handle(&mut self, _msg: Inc, _ctx: &Ctx<Self>) {
        self.n += 1;
    }
}

impl Handler<Get> for Counter {
    async fn handle(&mut self, _msg: Get, _ctx: &Ctx<Self>) -> u64 {
        self.n
    }
}

#[test]
fn remote_tells_then_ask_observe_fifo() {
    let (sim, node_a, node_b) = two_nodes(3);
    let count = sim.block_on(async move {
        let counter = node_a.spawn(Counter { n: 0 });
        let remote = node_b.resolve::<Counter>(counter.id().clone());
        // tell/ask from one sender to one recipient are FIFO (spec §6, §7.2).
        for _ in 0..5 {
            remote.tell(Inc).await.unwrap();
        }
        remote.ask(Get).await
    });
    assert_eq!(count, Ok(5));
}

// --- Failure modes over the wire ----------------------------------------------

#[test]
fn ask_to_missing_remote_actor_is_dead_letter() {
    let (sim, _node_a, node_b) = two_nodes(4);
    let result = sim.block_on(async move {
        // A well-formed id on node 1, but no such actor was ever spawned.
        let ghost = actor_core::ActorId::new(NodeId::new(1), Path::new("/user/404"), 0);
        node_b
            .resolve::<Greeter>(ghost)
            .ask(Greet { name: "x".into() })
            .await
    });
    assert_eq!(result, Err(CallError::DeadLetter));
}

#[test]
fn ask_to_unknown_node_is_unreachable() {
    let (sim, _node_a, node_b) = two_nodes(5);
    let result = sim.block_on(async move {
        // Node 99 never joined the network.
        let nowhere = actor_core::ActorId::new(NodeId::new(99), Path::new("/user/0"), 0);
        node_b
            .resolve::<Greeter>(nowhere)
            .ask(Greet { name: "x".into() })
            .await
    });
    assert_eq!(result, Err(CallError::Unreachable));
}

// An actor that handles a message but never registered it for remote dispatch.
struct Closed;

impl Actor for Closed {
    type System = Sys;
    // register() left as the default: accepts nothing over the wire.
}

impl Handler<Greet> for Closed {
    async fn handle(&mut self, _msg: Greet, _ctx: &Ctx<Self>) -> String {
        "unreachable by wire".into()
    }
}

#[test]
fn unregistered_message_over_wire_is_unhandled() {
    let (sim, node_a, node_b) = two_nodes(6);
    let result = sim.block_on(async move {
        let closed = node_a.spawn(Closed);
        // Allowlist (spec §5, §15): the manifest is not registered on the target.
        node_b
            .resolve::<Closed>(closed.id().clone())
            .ask(Greet { name: "x".into() })
            .await
    });
    assert_eq!(result, Err(CallError::Unhandled));
}
