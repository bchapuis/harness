//! The wire dispatch path, exercised single-node via loopback (spec §4.4, §5):
//! encode a message → look it up in the dispatch registry → decode and enqueue →
//! the real handler runs → its reply is encoded → decode it back. No transport
//! yet; this validates the codec + registry + `ReplyHandle` before the network.

use std::sync::Arc;

use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::LocalSystem;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::ReplyHandle;
use actor_serialization::Codec;
use actor_serialization::JsonCodec;
use actor_serialization::decode;
use actor_serialization::encode;
use actor_simulation::SimClock;
use actor_simulation::SimEntropy;
use actor_simulation::SimSpawner;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

type Sys = LocalSystem<SimClock, SimEntropy, SimSpawner>;

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

#[test]
fn message_round_trips_through_the_dispatch_registry() {
    let sim = Simulation::new(0);
    let system = LocalSystem::new(sim.clock(), sim.entropy(), sim.spawner());

    let reply = sim.block_on(async move {
        let greeter = system.spawn(Greeter {
            greeting: "Hello".into(),
        });
        let mailbox = system
            .resolve_local::<Greeter>(greeter.id())
            .expect("greeter is local");

        let mut registry = HandlerRegistry::<Greeter>::default();
        Greeter::register(&mut registry);

        let codec: Arc<dyn Codec> = Arc::new(JsonCodec);
        let payload = encode(
            &*codec,
            &Greet {
                name: "world".into(),
            },
        )
        .unwrap();

        // Inbound path: look up the manifest, decode, enqueue, route the reply.
        let dispatch = registry
            .dispatch(Greet::MANIFEST.as_str())
            .expect("Greet is registered");
        let (handle, reply_rx) = ReplyHandle::channel(Arc::clone(&codec));
        dispatch(&*codec, &payload, handle, &mailbox).unwrap();

        let bytes = reply_rx.await.unwrap().unwrap();
        decode::<String>(&*codec, &bytes).unwrap()
    });

    assert_eq!(reply, "Hello, world!");
}

#[test]
fn unregistered_manifest_is_rejected() {
    // The registry is the allowlist: an unknown manifest has no entry, which the
    // receive loop turns into CallError::Unhandled (spec §4.4, §5).
    let mut registry = HandlerRegistry::<Greeter>::default();
    Greeter::register(&mut registry);
    assert!(registry.dispatch("test.Greet").is_some());
    assert!(registry.dispatch("unknown.Message").is_none());
}

#[test]
fn malformed_payload_is_a_serialization_error() {
    let sim = Simulation::new(0);
    let system = LocalSystem::new(sim.clock(), sim.entropy(), sim.spawner());

    let outcome = sim.block_on(async move {
        let greeter = system.spawn(Greeter {
            greeting: "Hello".into(),
        });
        let mailbox = system.resolve_local::<Greeter>(greeter.id()).unwrap();
        let mut registry = HandlerRegistry::<Greeter>::default();
        Greeter::register(&mut registry);

        let codec: Arc<dyn Codec> = Arc::new(JsonCodec);
        let dispatch = registry.dispatch(Greet::MANIFEST.as_str()).unwrap();
        let (handle, _rx) = ReplyHandle::channel(Arc::clone(&codec));
        dispatch(&*codec, b"not valid json", handle, &mailbox)
    });

    assert!(matches!(
        outcome,
        Err(actor_core::CallError::Serialization(_))
    ));
}
