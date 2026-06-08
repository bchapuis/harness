//! Conformance: **managed** membership (spec §9.4) — the operator-run
//! control plane.
//!
//! In managed mode the member set is governed by an external authority through a
//! *designated* leader (not elected), and the SWIM detector is demoted to a
//! read-only reachability sensor that never decides `down`. The operator drives
//! the lifecycle by command: [`admit`](actor_cluster::ClusterSystem::admit),
//! [`drain`](actor_cluster::ClusterSystem::drain) /
//! [`resume`](actor_cluster::ClusterSystem::resume) (the reversible maintenance
//! cordon), and [`decommission`](actor_cluster::ClusterSystem::decommission) (the
//! terminal removal). Each decision is revision-stamped and disseminates by
//! gossip. These tests pin that behavior — the k8s node-lifecycle model.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
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
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

/// Fast SWIM so a few simulated seconds cover several probe/gossip rounds.
fn swim(downing: DowningPolicy) -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
        downing,
    }
}

/// Drive an async system call (`decommission`, an `ask`) to completion under a
/// running detector. `Simulation::block_on` runs to *quiescence*, which a SWIM
/// cluster never reaches (the detector probes forever), so instead we launch the
/// future and advance a bounded `settle` span for it to finish in.
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
    cell.lock().unwrap().take().expect("future did not complete")
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
    const MANIFEST: Manifest = Manifest::new("managed.Ping");
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
fn the_control_plane_leader_is_designated_not_elected() {
    // Unlike autonomous mode (lowest-id elected, spec §9.2), the managed leader is
    // provisioned. Designate C — the *highest* id — and every node must still
    // agree it leads, never electing A.
    let sim = Simulation::new(1);
    let net = SimNetwork::new(&sim).with_managed(swim(DowningPolicy::Conservative), C);
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_millis(500));

    assert_eq!(a.leader(), Some(C), "the designated leader, not the lowest id");
    assert_eq!(b.leader(), Some(C));
    assert_eq!(c.leader(), Some(C));
    assert!(c.membership().is_leader());
    assert!(!a.membership().is_leader());
}

#[test]
fn drain_then_resume_round_trips_cluster_wide() {
    // The reversible cordon (spec §9.4): the leader drains B, the whole cluster —
    // B included — converges on `Draining`; a later resume returns it to `Up`.
    let sim = Simulation::new(2);
    let recorder = Recorder::new();
    let net = SimNetwork::new(&sim)
        .with_managed(swim(DowningPolicy::Conservative), A)
        .with_events(Arc::new(recorder.clone()));
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_millis(500));

    assert!(a.drain(B), "the leader's drain command takes effect");
    sim.run_for(Duration::from_secs(1)); // gossip the decision cluster-wide

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
    assert!(!a.membership().is_down(B), "draining is not down — B stays a member");

    assert!(a.resume(B), "the leader resumes B after maintenance");
    sim.run_for(Duration::from_secs(1));

    for (who, sys) in [("A", &a), ("C", &c)] {
        assert_eq!(
            sys.membership().status(B),
            Some(MemberStatus::Up),
            "{who} sees B back up",
        );
    }
    assert_eq!(b.membership().self_status(), MemberStatus::Up);

    // The transition is observable on the wire end-to-end: C, a third party,
    // recorded draining then resuming for B.
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
    // The motivating case: a node down for maintenance must not be evicted. Drain
    // B, then take it fully offline (a maintenance outage). It goes `unreachable`
    // on the detector axis but stays a member — never `down`, never pruned — and
    // its status is preserved for when it returns.
    let sim = Simulation::new(3);
    let net = SimNetwork::new(&sim).with_managed(swim(DowningPolicy::Conservative), A);
    let a = net.join(A);
    let _b = net.join(B);
    let _c = net.join(C);
    sim.run_for(Duration::from_millis(500));

    assert!(a.drain(B));
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
    assert!(!a.membership().is_down(B), "the detector never downs a node in managed mode");

    net.heal(); // maintenance done; B comes back under the same identity
    sim.run_for(Duration::from_secs(2));

    assert_eq!(
        a.membership().reachability(B),
        Some(Reachability::Reachable),
        "B is reachable again, still the same member",
    );
}

#[test]
fn the_detector_never_downs_a_node_in_managed_mode() {
    // Managed mode forces the conservative downing policy: even handed an
    // aggressive `Timeout`, the detector only ever marks a partitioned node
    // `unreachable`, never `down`. Downing is the operator's call alone.
    let sim = Simulation::new(4);
    let net = SimNetwork::new(&sim)
        .with_managed(swim(DowningPolicy::Timeout(Duration::from_millis(200))), A);
    let a = net.join(A);
    let _b = net.join(B);
    sim.run_for(Duration::from_millis(500));

    net.partition(&[A], &[B]);
    sim.run_for(Duration::from_secs(10));

    assert_eq!(a.membership().reachability(B), Some(Reachability::Unreachable));
    assert!(
        !a.membership().is_down(B),
        "the Timeout policy is ignored in managed mode — only the operator downs",
    );
}

#[test]
fn decommission_is_terminal_and_runs_the_cascade() {
    // Decommission is the deliberate, terminal removal (spec §9.4, invariant #15):
    // it runs the node-down cascade (spec §8.1) — watchers get `NodeDown`, in-flight
    // and subsequent calls fail `Unreachable` — and the node never returns.
    let sim = Simulation::new(5);
    let net = SimNetwork::new(&sim).with_managed(swim(DowningPolicy::Conservative), A);
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

    let leader = a.clone();
    let applied = drive(&sim, Duration::from_millis(200), async move {
        leader.decommission(B).await
    });
    assert!(applied, "the leader decommissions B");

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

    // `down` is terminal: the operator cannot revive it (invariant #15).
    assert!(!a.admit(B), "a decommissioned node cannot be re-admitted");
    assert!(a.membership().is_down(B));
}

const ECHOES: Key<Echo> = Key::new("echoes");

#[test]
fn draining_a_node_routes_service_discovery_away_from_it() {
    // Load-shedding (spec §9.4, §13): an `Echo` registered on B is
    // discoverable cluster-wide, but once the operator drains B a lookup routes
    // around it — the registration survives (it is not pruned like a `down` node),
    // so a later resume restores it. The node stays a member throughout.
    let sim = Simulation::new(7);
    let net = SimNetwork::new(&sim).with_managed(swim(DowningPolicy::Conservative), A);
    let a = net.join(A);
    let b = net.join(B);
    sim.run_for(Duration::from_millis(500));

    let echo = b.spawn(Echo);
    b.receptionist().register(ECHOES, &echo);
    sim.run_for(Duration::from_secs(1)); // replicate the registration to A

    assert_eq!(
        a.receptionist().lookup(ECHOES).len(),
        1,
        "A discovers the Echo on B",
    );

    assert!(a.drain(B));
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        a.receptionist().lookup(ECHOES).len(),
        0,
        "a drained node is routed around — its actors are not handed out",
    );

    assert!(a.resume(B));
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        a.receptionist().lookup(ECHOES).len(),
        1,
        "resume restores B to rotation; the registration was never lost",
    );
}

#[test]
fn operator_commands_only_take_effect_on_the_leader() {
    // The member set is single-writer: only the designated leader's commands take
    // effect, which is what keeps managed-mode membership decisions split-brain
    // free. A command on any other node is a no-op.
    let sim = Simulation::new(6);
    let net = SimNetwork::new(&sim).with_managed(swim(DowningPolicy::Conservative), A);
    let a = net.join(A);
    let b = net.join(B);
    let _c = net.join(C);
    sim.run_for(Duration::from_millis(500));

    // B is not the leader: its commands are rejected and change nothing.
    assert!(!b.drain(C), "a non-leader command is a no-op");
    let non_leader = b.clone();
    let downed = drive(&sim, Duration::from_secs(1), async move {
        non_leader.decommission(C).await
    });
    assert!(!downed, "a non-leader decommission is a no-op");
    assert_eq!(
        a.membership().status(C),
        Some(MemberStatus::Up),
        "C is untouched by the non-leader's commands",
    );

    // The leader's command, by contrast, takes effect and disseminates.
    assert!(a.drain(C));
    sim.run_for(Duration::from_secs(1));
    assert_eq!(b.membership().status(C), Some(MemberStatus::Draining));
}

#[test]
fn an_operator_decision_converges_under_transport_faults() {
    // Operator decisions ride the same gossip as everything else, so they MUST
    // converge even when the network drops, duplicates, and delays frames — there
    // is no separate reliable channel for the control plane.
    let sim = Simulation::new(8);
    let faults = FaultPolicy {
        drop_num: 1,
        drop_den: 4, // ~25% of frames dropped
        duplicate_num: 1,
        duplicate_den: 8,
        max_latency: Duration::from_millis(80),
    };
    let net = SimNetwork::new(&sim)
        .with_managed(swim(DowningPolicy::Conservative), A)
        .with_faults(faults);
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_secs(1));

    assert!(a.drain(B), "the leader drains B");
    sim.run_for(Duration::from_secs(20)); // many gossip rounds despite the loss

    for (who, sys) in [("A", &a), ("C", &c)] {
        assert_eq!(
            sys.membership().status(B),
            Some(MemberStatus::Draining),
            "{who} converges on the drain despite faults",
        );
    }
    assert_eq!(b.membership().self_status(), MemberStatus::Draining);
}

#[test]
fn a_terminal_decision_reaches_a_reconnecting_node() {
    // A node cut off from the control plane misses a decommission while it is
    // partitioned — its observe-only detector only marks the target `unreachable`,
    // never `down` — and then catches up to the terminal decision once it
    // reconnects (spec §9.4, invariant #15).
    let sim = Simulation::new(9);
    let net = SimNetwork::new(&sim).with_managed(swim(DowningPolicy::Conservative), A);
    let a = net.join(A);
    let _b = net.join(B);
    let c = net.join(C);
    sim.run_for(Duration::from_millis(500));

    net.partition(&[C], &[A, B]); // C is severed from the leader
    sim.run_for(Duration::from_secs(1));

    let leader = a.clone();
    let downed = drive(&sim, Duration::from_secs(1), async move {
        leader.decommission(B).await
    });
    assert!(downed, "the leader decommissions B");
    sim.run_for(Duration::from_secs(2));
    assert!(a.membership().is_down(B), "A downed B");
    assert!(
        !c.membership().is_down(B),
        "C, partitioned, has not heard the decision — only that B is unreachable",
    );

    net.heal();
    sim.run_for(Duration::from_secs(5));
    assert!(
        c.membership().is_down(B),
        "on reconnect, C catches up to the terminal decision",
    );
}
