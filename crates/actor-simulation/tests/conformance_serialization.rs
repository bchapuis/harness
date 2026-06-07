//! Conformance: serialization & dispatch (spec §4.4, §5, §15) — the dispatch
//! registry is the deserialization allowlist.

mod support;

use std::sync::Arc;

use actor_cluster::Authorizer;
use actor_core::Actor;
use actor_core::ActorId;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::HandlerRegistry;
use actor_core::NodeId;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use support::Counter;
use support::Get;
use support::Greeter;
use support::Inc;

/// An authorizer that rejects exactly one message manifest (spec §15).
struct Deny(&'static str);
impl Authorizer for Deny {
    fn authorize(&self, _peer: NodeId, _recipient: &ActorId, manifest: &str) -> bool {
        manifest != self.0
    }
}

#[test]
fn the_dispatch_registry_is_an_allowlist() {
    let mut registry = HandlerRegistry::<Greeter<SimSystem>>::default();
    Greeter::<SimSystem>::register(&mut registry);

    // Registered manifests dispatch; an unregistered one has no entry, so the
    // receive loop turns it into `Unhandled` and never builds an off-list type
    // from network bytes (spec §5, §15, invariant #8).
    assert!(registry.dispatch("conf.Greet").is_some());
    assert!(registry.dispatch("conf.Stop").is_some());
    assert!(registry.dispatch("conf.Get").is_none());
    assert!(registry.dispatch("totally.Unknown").is_none());
}

#[test]
fn an_unauthorized_ask_is_rejected_as_a_system_failure() {
    // The authorizer denies `conf.Get`; the call is rejected as a system failure
    // and never reaches the actor (spec §15).
    let sim = Simulation::new(1);
    let net = SimNetwork::new(&sim).with_authorizer(Arc::new(Deny("conf.Get")));
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let outcome = sim.block_on(async move {
        let counter = node_b.spawn(Counter::<SimCluster>::new());
        node_a
            .resolve::<Counter<SimCluster>>(counter.id().clone())
            .ask(Get)
            .await
    });
    assert!(
        matches!(outcome, Err(CallError::System(_))),
        "an unauthorized ask is a system failure, got {outcome:?}",
    );
}

#[test]
fn an_authorized_message_is_delivered_a_denied_one_is_not() {
    // The authorizer denies `conf.Inc` but permits `conf.Get`. The denied `tell`
    // is dropped at the recipient (never applied); the permitted `ask` succeeds.
    let sim = Simulation::new(2);
    let net = SimNetwork::new(&sim).with_authorizer(Arc::new(Deny("conf.Inc")));
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let count = sim.block_on(async move {
        let counter = node_b.spawn(Counter::<SimCluster>::new());
        let remote = node_a.resolve::<Counter<SimCluster>>(counter.id().clone());
        remote.tell(Inc).await.unwrap(); // denied at B → dropped, never applied
        remote.ask(Get).await // permitted
    });
    assert_eq!(count, Ok(0), "the denied Inc never reached the actor");
}

// Codec agreement at the association handshake (spec §5 #2, §7.1) is a property
// of the real transport's handshake, which the in-memory simulator does not have
// (it routes frames directly, and only the payload exercises the wire codec —
// spec §18.2). It is covered against the production TCP transport by
// `actor-runtime`'s `a_codec_disagreement_is_rejected_at_the_handshake`.
