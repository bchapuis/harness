//! Conformance: **static** membership (spec §9.4) — the fixed-roster control
//! plane.
//!
//! In static mode the member set is fixed at startup and no failure detector
//! runs: membership never changes at runtime, a vanished node is never declared
//! `down`, and there is no control plane to issue operator commands. Failure
//! surfaces only to the *caller* (a `CallError`), never to membership. (Static
//! mode is also the default the messaging/location-transparency tests in
//! `cluster.rs` already run under; here we pin the membership properties.)

use std::sync::Arc;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Event;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_cluster::MemberStatus;
use actor_cluster::Reachability;
use actor_simulation::Recorder;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

struct Echo;
impl Actor for Echo {
    type System = SimCluster;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Ping>();
    }
}

#[derive(Serialize, Deserialize)]
struct Ping;
impl Message for Ping {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("static.Ping");
}
impl Handler<Ping> for Echo {
    async fn handle(&mut self, _msg: Ping, _ctx: &Ctx<Self>) -> u64 {
        1
    }
}

#[test]
fn static_mode_runs_no_failure_detector() {
    // With no detector, crashing a node produces no membership reaction at all:
    // no reachability transition, no `down`, and the roster is exactly as wired.
    let sim = Simulation::new(1);
    let recorder = Recorder::new();
    let net = SimNetwork::new(&sim).with_events(Arc::new(recorder.clone())); // static = default
    let a = net.join(A);
    let _b = net.join(B);
    sim.run_for(Duration::from_millis(500));

    net.crash(B);
    sim.run_for(Duration::from_secs(30)); // a long outage; nothing reacts

    let detector_events = recorder
        .events()
        .iter()
        .filter(|e| {
            matches!(
                e,
                Event::Suspected { .. }
                    | Event::Unreachable { .. }
                    | Event::Reachable { .. }
                    | Event::NodeDown { .. }
            )
        })
        .count();
    assert_eq!(detector_events, 0, "static mode runs no failure detector");

    // B's membership is exactly what startup wired — never suspected, never downed.
    assert_eq!(a.membership().status(B), Some(MemberStatus::Up));
    assert_eq!(a.membership().reachability(B), Some(Reachability::Reachable));
    assert!(!a.membership().is_down(B));
}

#[test]
fn a_send_to_a_crashed_node_fails_without_changing_membership() {
    // Failure surfaces to the caller as a `CallError`, and the call still
    // completes (never hangs) — but membership is untouched: nothing downs B
    // (spec §8, §9.4).
    let sim = Simulation::new(2);
    let net = SimNetwork::new(&sim);
    let a = net.join(A);
    let b = net.join(B);
    sim.run_for(Duration::from_millis(200));

    let echo = b.spawn(Echo);
    let echo_on_a: ActorRef<Echo> = a.resolve(echo.id().clone());
    net.crash(B);

    // Static mode has no perpetual detector loop, so the run quiesces and
    // `block_on` is the natural driver here.
    let caller = echo_on_a.clone();
    let result = sim.block_on(async move { caller.ask_timeout(Ping, Duration::from_secs(1)).await });
    assert!(
        matches!(result, Err(CallError::Timeout) | Err(CallError::Unreachable)),
        "the call completes as a transport failure, never hangs",
    );
    assert!(!a.membership().is_down(B), "but B is never declared down");
    assert_eq!(a.membership().status(B), Some(MemberStatus::Up));
}

#[test]
fn operator_commands_are_noops_without_a_control_plane() {
    // Static mode has no designated leader, so no node is a control plane: every
    // operator command is a no-op and the roster stays fixed.
    let sim = Simulation::new(3);
    let net = SimNetwork::new(&sim);
    let a = net.join(A);
    let _b = net.join(B);
    sim.run_for(Duration::from_millis(200));

    assert!(!a.admit(C), "no control plane to admit a node");
    assert!(!a.drain(B));
    assert!(!a.resume(B));
    let node = a.clone();
    assert!(!sim.block_on(async move { node.decommission(B).await }));

    assert_eq!(a.membership().status(B), Some(MemberStatus::Up), "B untouched");
    assert!(a.membership().status(C).is_none(), "C was never admitted");
}
