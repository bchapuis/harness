//! Conformance: **registry-based** membership (spec §9.4.2) — the control plane
//! delegated to an external registry.
//!
//! The authoritative member set lives in a registry the cluster reads but does
//! not operate; every node runs a sync loop against it and applies its
//! revision-stamped state, and the SWIM detector is **observe-only** — only a
//! registry mutation ever declares `down`. The operator drives the lifecycle by
//! mutating the registry: register (admission *is* the entry), the reversible
//! `drain`/`resume` cordon, and deregistration (the terminal removal, spec
//! §8.1). These tests pin that behavior — the Kubernetes node-lifecycle model —
//! against the simulated registry with seeded latency, staleness, and outage
//! (spec §18.2, §18.3).

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::MemberStatus;
use actor_cluster::Reachability;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::BoxError;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Event;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Key;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_core::Terminated;
use actor_core::TerminationReason;
use actor_simulation::FaultPolicy;
use actor_simulation::Recorder;
use actor_simulation::RegistryFaultPolicy;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::SimRegistry;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

/// Fast SWIM so a few simulated seconds cover several probe/gossip rounds.
fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

/// The sync-loop cadence these tests run with.
const SYNC: Duration = Duration::from_millis(100);

/// A fault-free simulated registry with `nodes` registered up front, and a
/// network whose nodes sync against it.
fn registry_cluster(sim: &Simulation, nodes: &[NodeId]) -> (SimRegistry, SimNetwork) {
    let registry = SimRegistry::new(sim);
    for &node in nodes {
        registry.register(node);
    }
    let net = SimNetwork::new(sim).with_registry(swim(), registry.client(), SYNC);
    (registry, net)
}

/// Drive an async system call (an operator command, an `ask`) to completion
/// under a running detector. `Simulation::block_on` runs to *quiescence*, which
/// a SWIM cluster never reaches (the detector probes forever), so instead we
/// launch the future and advance a bounded `settle` span for it to finish in.
fn drive<T: Send + 'static>(
    sim: &Simulation,
    settle: Duration,
    future: impl Future<Output = T> + Send + 'static,
) -> T {
    let cell: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let out = Arc::clone(&cell);
    sim.spawner().launch(Box::pin(async move {
        *out.lock().unwrap() = Some(future.await);
    }));
    sim.run_for(settle);
    cell.lock()
        .unwrap()
        .take()
        .expect("future did not complete")
}

// --- A trivial addressable actor, to exercise the cascade and routing ---------

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
    const MANIFEST: Manifest = Manifest::new("registry.Ping");
}
impl Handler<Ping> for Echo {
    async fn handle(&mut self, _msg: Ping, _ctx: &Ctx<Self>) -> u64 {
        1
    }
}

type Reasons = Arc<Mutex<Vec<TerminationReason>>>;

/// Watches `target` from start, recording the reason of every `Terminated`.
struct Watcher {
    target: ActorRef<Echo>,
    got: Reasons,
}
impl Actor for Watcher {
    type System = SimCluster;
    async fn started(&mut self, ctx: &Ctx<Self>) -> Result<(), BoxError> {
        ctx.watch(&self.target);
        Ok(())
    }
}
impl Handler<Terminated> for Watcher {
    async fn handle(&mut self, signal: Terminated, _ctx: &Ctx<Self>) {
        self.got.lock().unwrap().push(signal.reason);
    }
}

// --- Tests -------------------------------------------------------------------

#[test]
fn admission_is_the_registry_entry() {
    // Spec §9.4.2 item 2: a node is admitted by *appearing in the registry* — the
    // `joining` state is unused. Register C, bring it up knowing only seed A, and
    // the whole cluster converges on C `Up` at the entry's revision; no node ever
    // observes C as `joining`.
    let sim = Simulation::new(1);
    let recorder = Recorder::new();
    let registry = SimRegistry::new(&sim);
    registry.register(A);
    registry.register(B);
    let net = SimNetwork::new(&sim)
        .with_registry(swim(), registry.client(), SYNC)
        .with_events(Arc::new(recorder.clone()));
    let a = net.join(A);
    let b = net.join(B);
    sim.run_for(Duration::from_millis(500));

    let revision = registry.register(C);
    let c = net.join_seeded(C, &[A]);
    sim.run_for(Duration::from_secs(2));

    assert_eq!(
        c.membership().self_status(),
        MemberStatus::Up,
        "admission is the registry entry — C is a full member",
    );
    for (who, sys) in [("A", &a), ("B", &b)] {
        assert_eq!(
            sys.membership().status(C),
            Some(MemberStatus::Up),
            "{who} sees C up"
        );
        assert_eq!(
            sys.membership().stamp(C),
            Some(revision),
            "{who} holds C's status at the registry revision that admitted it",
        );
    }
    assert!(
        !recorder
            .events()
            .iter()
            .any(|e| matches!(e, Event::MemberJoining { node, .. } if *node == C)),
        "the joining state is unused in registry mode (spec §9.4.2 item 2)",
    );
}

#[test]
fn drain_then_resume_round_trips_cluster_wide() {
    // The reversible cordon (spec §9.4.2 item 5): the operator drains B in the
    // registry, the whole cluster — B included — converges on `Draining`; a later
    // resume returns it to `Up`.
    let sim = Simulation::new(2);
    let recorder = Recorder::new();
    let registry = SimRegistry::new(&sim);
    for node in [A, B, C] {
        registry.register(node);
    }
    let net = SimNetwork::new(&sim)
        .with_registry(swim(), registry.client(), SYNC)
        .with_events(Arc::new(recorder.clone()));
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_millis(500));

    registry.drain(B);
    sim.run_for(Duration::from_secs(1)); // every node syncs the decision

    for (who, sys) in [("A", &a), ("C", &c)] {
        assert_eq!(
            sys.membership().status(B),
            Some(MemberStatus::Draining),
            "{who} sees B draining",
        );
    }
    assert_eq!(
        b.membership().self_status(),
        MemberStatus::Draining,
        "B itself adopts the drain decision and would shed load",
    );
    assert!(
        !a.membership().is_down(B),
        "draining is not down — B stays a member"
    );

    registry.resume(B);
    sim.run_for(Duration::from_secs(1));

    for (who, sys) in [("A", &a), ("C", &c)] {
        assert_eq!(
            sys.membership().status(B),
            Some(MemberStatus::Up),
            "{who} sees B back up",
        );
    }
    assert_eq!(b.membership().self_status(), MemberStatus::Up);

    // The transition is observable end-to-end: C, a third party, recorded
    // draining then resuming for B.
    let seen: Vec<&str> = recorder
        .events()
        .iter()
        .filter_map(|e| match e {
            Event::MemberDraining { observer, node } if *observer == C && *node == B => {
                Some("draining")
            }
            Event::MemberResumed { observer, node } if *observer == C && *node == B => {
                Some("resumed")
            }
            _ => None,
        })
        .collect();
    assert_eq!(seen, vec!["draining", "resumed"]);
}

#[test]
fn a_node_under_maintenance_keeps_its_membership() {
    // The motivating case (spec §9.4.2 item 4): a node down for maintenance must
    // not be evicted. Drain B, then take it fully offline (a maintenance outage).
    // It goes `unreachable` on the detector axis but stays a member — never
    // `down`, never pruned — and its status is preserved for when it returns.
    let sim = Simulation::new(3);
    let (registry, net) = registry_cluster(&sim, &[A, B, C]);
    let a = net.join(A);
    let _b = net.join(B);
    let _c = net.join(C);
    sim.run_for(Duration::from_millis(500));

    registry.drain(B);
    sim.run_for(Duration::from_secs(1));

    net.crash(B); // B goes offline for maintenance
    sim.run_for(Duration::from_secs(30)); // a long outage

    assert_eq!(
        a.membership().reachability(B),
        Some(Reachability::Unreachable),
        "the detector observes the outage",
    );
    assert_eq!(
        a.membership().status(B),
        Some(MemberStatus::Draining),
        "but B keeps its membership — maintenance never evicts it",
    );
    assert!(
        !a.membership().is_down(B),
        "the detector never downs a node in registry mode",
    );

    net.heal(); // maintenance done; B comes back under the same identity
    sim.run_for(Duration::from_secs(2));

    assert_eq!(
        a.membership().reachability(B),
        Some(Reachability::Reachable),
        "B is reachable again, still the same member",
    );
}

#[test]
fn the_detector_never_downs_a_node_in_registry_mode() {
    // Spec §9.4.2 item 4 and invariant #16, unconditionally: the registry mode
    // carries no downing policy at all — there is nothing to even misconfigure.
    // A partitioned node only ever becomes `unreachable`; downing is the
    // registry's alone.
    let sim = Simulation::new(4);
    let (_registry, net) = registry_cluster(&sim, &[A, B]);
    let a = net.join(A);
    let _b = net.join(B);
    sim.run_for(Duration::from_millis(500));

    net.partition(&[A], &[B]);
    sim.run_for(Duration::from_secs(10));

    assert_eq!(
        a.membership().reachability(B),
        Some(Reachability::Unreachable)
    );
    assert!(
        !a.membership().is_down(B),
        "only a registry mutation declares down (spec §9.4.2 item 4)",
    );
}

#[test]
fn deregistration_is_terminal_and_runs_the_cascade() {
    // Deregistration is the deliberate, terminal removal (spec §9.4.2 items 3–4,
    // invariant #15): its revision finalizes `down` and runs the node-down
    // cascade (spec §8.1) — watchers get `NodeDown`, in-flight and subsequent
    // calls fail `Unreachable` — and the node never returns, not even if the
    // same `NodeId` is re-registered.
    let sim = Simulation::new(5);
    let (registry, net) = registry_cluster(&sim, &[A, B]);
    let a = net.join(A);
    let b = net.join(B);
    sim.run_for(Duration::from_millis(500));

    let echo = b.spawn(Echo);
    let echo_on_a: ActorRef<Echo> = a.resolve(echo.id().clone());
    let got: Reasons = Arc::new(Mutex::new(Vec::new()));
    let _watcher = a.spawn(Watcher {
        target: echo_on_a.clone(),
        got: Arc::clone(&got),
    });
    sim.run_for(Duration::from_millis(200)); // the remote watch registers

    // The operator API on any node dispatches to the registry (spec §9.4.2).
    let operator = a.clone();
    let acknowledged = drive(&sim, Duration::from_millis(200), async move {
        operator.decommission(B).await
    });
    assert!(acknowledged, "the registry acknowledged the deregistration");
    sim.run_for(Duration::from_secs(1)); // every node syncs the removal

    assert_eq!(
        *got.lock().unwrap(),
        vec![TerminationReason::NodeDown],
        "the watcher is notified its target's node went down",
    );
    assert!(a.membership().is_down(B), "B is terminally down");

    // A call to an actor on the dead node fails fast, it never hangs (invariant #2).
    let caller = echo_on_a.clone();
    let result = drive(&sim, Duration::from_millis(200), async move {
        caller.ask(Ping).await
    });
    assert!(matches!(result, Err(CallError::Unreachable)));

    // `down` is terminal (invariant #15): re-registering the same `NodeId` does
    // not resurrect it — terminal stickiness orders before the stamp rule.
    // Rejoining after down is a *new identity* (spec §9.1).
    registry.register(B);
    sim.run_for(Duration::from_secs(1));
    assert!(
        a.membership().is_down(B),
        "a higher-revision re-registration cannot revive a downed member",
    );
}

const ECHOES: Key<Echo> = Key::new("echoes");

#[test]
fn draining_a_node_routes_service_discovery_away_from_it() {
    // Load-shedding (spec §9.4.2 item 5, §13 #4): an `Echo` registered on B is
    // discoverable cluster-wide, but once the operator drains B a lookup routes
    // around it — the registration survives (it is not pruned like a `down`
    // node's), so a later resume restores it. The node stays a member throughout.
    let sim = Simulation::new(7);
    let (registry, net) = registry_cluster(&sim, &[A, B]);
    let a = net.join(A);
    let b = net.join(B);
    sim.run_for(Duration::from_millis(500));

    let echo = b.spawn(Echo);
    b.receptionist().register(ECHOES, &echo);
    sim.run_for(Duration::from_secs(1)); // replicate the registration to A

    assert_eq!(
        a.receptionist().lookup(ECHOES).len(),
        1,
        "A discovers the Echo on B"
    );

    registry.drain(B);
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        a.receptionist().lookup(ECHOES).len(),
        0,
        "a drained node is routed around — its actors are not handed out",
    );

    registry.resume(B);
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        a.receptionist().lookup(ECHOES).len(),
        1,
        "resume restores B to rotation; the registration was never lost",
    );
}

#[test]
fn the_operator_api_works_from_any_node() {
    // There is no designated leader to command: the registry is the single
    // writer, so the operator API dispatches to it from *any* node (spec
    // §9.4.2). B's drain of C is as authoritative as anyone's.
    let sim = Simulation::new(6);
    let (_registry, net) = registry_cluster(&sim, &[A, B, C]);
    let a = net.join(A);
    let b = net.join(B);
    let _c = net.join(C);
    sim.run_for(Duration::from_millis(500));

    let operator = b.clone();
    let acknowledged = drive(&sim, Duration::from_millis(200), async move {
        operator.drain(C).await
    });
    assert!(
        acknowledged,
        "any node's operator call reaches the registry"
    );
    sim.run_for(Duration::from_secs(1));

    assert_eq!(a.membership().status(C), Some(MemberStatus::Draining));
    assert_eq!(b.membership().status(C), Some(MemberStatus::Draining));
}

#[test]
fn registry_unavailability_pauses_changes_only() {
    // Spec §9.4.2 item 6: while the registry is unreachable no membership
    // *changes* land, but the data plane keeps running on the last-synced view,
    // and the outage is never treated as evidence about peer liveness.
    let sim = Simulation::new(8);
    let (registry, net) = registry_cluster(&sim, &[A, B]);
    let a = net.join(A);
    let b = net.join(B);
    sim.run_for(Duration::from_millis(500));

    registry.set_available(false);
    registry.drain(B); // written in the registry, invisible to the cluster
    sim.run_for(Duration::from_secs(5));

    assert_eq!(
        a.membership().status(B),
        Some(MemberStatus::Up),
        "the change is paused while the registry is unavailable",
    );
    assert_eq!(
        a.membership().reachability(B),
        Some(Reachability::Reachable),
        "registry outage is not evidence about peer liveness",
    );

    // The data plane keeps working on the last-synced view.
    let echo = b.spawn(Echo);
    let caller: ActorRef<Echo> = a.resolve(echo.id().clone());
    let result = drive(&sim, Duration::from_millis(500), async move {
        caller.ask(Ping).await
    });
    assert_eq!(result, Ok(1), "actor traffic is unaffected by the outage");

    registry.set_available(true);
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        a.membership().status(B),
        Some(MemberStatus::Draining),
        "the paused change lands once the registry recovers",
    );
}

#[test]
fn a_stale_and_laggy_registry_still_converges() {
    // Invariant #14 under registry faults (spec §18.3): with every fetch
    // delayed and half of them served stale, the sync loop's monotonic-revision
    // guard absorbs the staleness and the cluster still converges on the
    // registry's latest state — a stale read can never revert a newer decision.
    let sim = Simulation::new(9);
    let registry = SimRegistry::new(&sim).with_faults(RegistryFaultPolicy {
        max_latency: Duration::from_millis(80),
        stale_num: 1,
        stale_den: 2,
        max_staleness: 4,
    });
    for node in [A, B, C] {
        registry.register(node);
    }
    let net = SimNetwork::new(&sim).with_registry(swim(), registry.client(), SYNC);
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_secs(1));

    registry.drain(B);
    sim.run_for(Duration::from_secs(20)); // many sync rounds, many stale reads

    for (who, sys) in [("A", &a), ("C", &c)] {
        assert_eq!(
            sys.membership().status(B),
            Some(MemberStatus::Draining),
            "{who} converges on the drain despite stale and laggy reads",
        );
    }
    assert_eq!(b.membership().self_status(), MemberStatus::Draining);

    registry.resume(B);
    sim.run_for(Duration::from_secs(20));
    for (who, sys) in [("A", &a), ("C", &c)] {
        assert_eq!(
            sys.membership().status(B),
            Some(MemberStatus::Up),
            "{who} converges on the resume — staleness never wins backwards",
        );
    }
}

#[test]
fn a_registry_decision_converges_under_transport_faults() {
    // Registry decisions reach nodes by sync, and stamped entries also ride the
    // ordinary gossip as a safe accelerant (spec §9.4.2 item 1) — under heavy
    // transport faults both paths merge to the same stamped state.
    let sim = Simulation::new(10);
    let faults = FaultPolicy {
        drop_num: 1,
        drop_den: 4, // ~25% of frames dropped
        duplicate_num: 1,
        duplicate_den: 8,
        max_latency: Duration::from_millis(80),
    };
    let registry = SimRegistry::new(&sim);
    for node in [A, B, C] {
        registry.register(node);
    }
    let net = SimNetwork::new(&sim)
        .with_registry(swim(), registry.client(), SYNC)
        .with_faults(faults);
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_secs(1));

    let revision = registry.drain(B);
    sim.run_for(Duration::from_secs(20));

    for (who, sys) in [("A", &a), ("C", &c)] {
        assert_eq!(
            sys.membership().status(B),
            Some(MemberStatus::Draining),
            "{who} converges on the drain despite faults",
        );
        assert_eq!(
            sys.membership().stamp(B),
            Some(revision),
            "{who} holds the decision at its registry revision",
        );
    }
    assert_eq!(b.membership().self_status(), MemberStatus::Draining);
}

#[test]
fn a_network_partitioned_node_still_syncs_the_registry() {
    // The registry is an *external* authority: a node partitioned from its peers
    // still reaches it, so a terminal decision lands even where gossip cannot —
    // unlike gossip-based mode, peer connectivity is not the control plane's
    // dissemination path (spec §9.4.2).
    let sim = Simulation::new(11);
    let (registry, net) = registry_cluster(&sim, &[A, B, C]);
    let a = net.join(A);
    let _b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_millis(500));

    net.partition(&[C], &[A, B]); // C is severed from every peer
    sim.run_for(Duration::from_secs(1));

    registry.deregister(B);
    sim.run_for(Duration::from_secs(2));

    assert!(a.membership().is_down(B), "A synced the removal");
    assert!(
        c.membership().is_down(B),
        "C, though partitioned from its peers, synced the removal directly",
    );

    net.heal();
    sim.run_for(Duration::from_secs(2));
    assert!(
        c.membership().is_down(B),
        "down stays terminal after healing (#15)"
    );
}
