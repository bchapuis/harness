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

use actor_cluster::MemberStatus;
use actor_cluster::MembershipMode;
use actor_cluster::Reachability;
use actor_cluster::SwimConfig;
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
    assert_eq!(
        a.membership().reachability(B),
        Some(Reachability::Reachable)
    );
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
    let result =
        sim.block_on(async move { caller.ask_timeout(Ping, Duration::from_secs(1)).await });
    assert!(
        matches!(
            result,
            Err(CallError::Timeout) | Err(CallError::Unreachable)
        ),
        "the call completes as a transport failure, never hangs",
    );
    assert!(!a.membership().is_down(B), "but B is never declared down");
    assert_eq!(a.membership().status(B), Some(MemberStatus::Up));
}

#[test]
fn operator_commands_are_noops_without_a_control_plane() {
    // Static mode has no membership authority at all: every operator command is
    // a no-op and the roster stays fixed (spec §9.4.1).
    let sim = Simulation::new(3);
    let net = SimNetwork::new(&sim);
    let a = net.join(A);
    let _b = net.join(B);
    sim.run_for(Duration::from_millis(200));

    let node = a.clone();
    let (admitted, drained, resumed, decommissioned) = sim.block_on(async move {
        (
            node.admit(C).await,
            node.drain(B).await,
            node.resume(B).await,
            node.decommission(B).await,
        )
    });
    assert!(!admitted, "no control plane to admit a node");
    assert!(!drained);
    assert!(!resumed);
    assert!(!decommissioned);

    assert_eq!(
        a.membership().status(B),
        Some(MemberStatus::Up),
        "B untouched"
    );
    assert!(a.membership().status(C).is_none(), "C was never admitted");
}

#[test]
fn the_observe_only_detector_reports_reachability_but_never_downs() {
    // Spec §9.4.1: a static deployment MAY enable the detector observe-only,
    // trading probe traffic for reachability events and discovery routing.
    // Nothing else changes: there is still no down authority — no `down`, no
    // membership transition; the member set stays exactly the configured roster.
    let sim = Simulation::new(4);
    let recorder = Recorder::new();
    let net = SimNetwork::new(&sim)
        .with_mode(MembershipMode::Static {
            detector: Some(SwimConfig {
                probe_interval: Duration::from_millis(100),
                rtt: Duration::from_millis(50),
                suspect_timeout: Duration::from_millis(300),
                indirect_count: 2,
            }),
        })
        .with_events(Arc::new(recorder.clone()));
    let a = net.join(A);
    let _b = net.join(B);
    sim.run_for(Duration::from_millis(500));

    net.crash(B);
    sim.run_for(Duration::from_secs(30)); // a long outage under an active detector

    assert_eq!(
        a.membership().reachability(B),
        Some(Reachability::Unreachable),
        "the observe-only detector reports the outage",
    );
    assert!(
        recorder.events().iter().any(
            |e| matches!(e, Event::Unreachable { observer, node } if *observer == A && *node == B)
        ),
        "reachability transitions are observable (spec §16)",
    );
    assert!(!a.membership().is_down(B), "but nothing ever declares down");
    assert_eq!(
        a.membership().status(B),
        Some(MemberStatus::Up),
        "no membership transition occurs — the roster is fixed",
    );
    assert!(
        !recorder
            .events()
            .iter()
            .any(|e| matches!(e, Event::NodeDown { .. })),
        "no NodeDown is ever emitted in static mode",
    );

    net.heal();
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        a.membership().reachability(B),
        Some(Reachability::Reachable),
        "reachability is reversible — B recovers as the same member",
    );
}
