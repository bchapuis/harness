//! Conformance: robustness to hostile input (spec §5.4, §7.3). Decoding bad
//! bytes is reported as `Serialization`, never a panic; the receive loop
//! survives hostile frames a well-behaved peer would never send and keeps
//! serving legitimate traffic.

mod support;

use std::sync::Arc;

use actor_cluster::CallId;
use actor_cluster::Frame;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::HandlerRegistry;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::ReplyHandle;
use actor_serialization::Codec;
use actor_serialization::JsonCodec;
use actor_serialization::decode;
use actor_serialization::encode;
use actor_simulation::SimCluster;
use actor_simulation::SimSystem;
use support::Greet;
use support::Greeter;

#[test]
fn decode_failure_is_serialization_and_the_actor_keeps_serving() {
    let (sim, system) = support::local(1);
    let (garbage, good) = sim.block_on(async move {
        let greeter = system.spawn(Greeter::<SimSystem>::new("Hello"));
        let mailbox = system
            .resolve_local::<Greeter<SimSystem>>(greeter.id())
            .unwrap();
        let mut registry = HandlerRegistry::<Greeter<SimSystem>>::default();
        Greeter::<SimSystem>::register(&mut registry);
        let dispatch = registry.dispatch(Greet::MANIFEST.as_str()).unwrap();
        let codec: Arc<dyn Codec> = Arc::new(JsonCodec);

        // Garbage bytes for a registered manifest must be rejected, not panic.
        let (reply, _rx) = ReplyHandle::channel(Arc::clone(&codec));
        let garbage = dispatch(&*codec, b"not json at all", reply, &mailbox);

        // The actor still serves a well-formed message afterwards.
        let payload = encode(
            &*codec,
            &Greet {
                name: "world".into(),
            },
        )
        .unwrap();
        let (reply, rx) = ReplyHandle::channel(Arc::clone(&codec));
        dispatch(&*codec, &payload, reply, &mailbox).unwrap();
        let good = decode::<String>(&*codec, &rx.await.unwrap().unwrap()).unwrap();

        (garbage, good)
    });

    assert!(matches!(garbage, Err(CallError::Serialization(_))));
    assert_eq!(good, "Hello, world!");
}

#[test]
fn the_receive_loop_survives_hostile_frames() {
    let (sim, net) = support::cluster(2, None);
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));
    let greeter = node_a.spawn(Greeter::<SimCluster>::new("Hello"));
    let id = greeter.id().clone();

    // Frames a well-behaved peer would never send, from a stranger node.
    let stranger = NodeId::new(99);
    net.inject(
        stranger,
        NodeId::new(1),
        Frame::Envelope {
            recipient: id.clone(),
            manifest: "conf.Greet".to_string(),
            correlation: Some(CallId(7)),
            payload: b"not json".to_vec(),
        },
    );
    net.inject(
        stranger,
        NodeId::new(1),
        Frame::Envelope {
            recipient: id.clone(),
            manifest: "totally.unknown".to_string(),
            correlation: Some(CallId(8)),
            payload: b"{}".to_vec(),
        },
    );

    // A legitimate call after the hostile burst still succeeds (the node and its
    // receive loop survived — spec §5.4).
    let reply = sim.block_on(async move {
        node_b
            .resolve::<Greeter<SimCluster>>(id)
            .ask(Greet {
                name: "world".into(),
            })
            .await
    });
    assert_eq!(reply, Ok("Hello, world!".to_string()));
}
