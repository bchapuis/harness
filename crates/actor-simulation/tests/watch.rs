//! Death watch (spec §12): a watcher receives exactly one [`Terminated`] when a
//! watched actor stops, fails, or — the distributed case — has its node declared
//! `down` (the node-down cascade, spec §8.1 step 4).

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::BoxError;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Terminated;
use actor_core::TerminationReason;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

type Sys = SimCluster;
type Reasons = Arc<Mutex<Vec<TerminationReason>>>;

struct Target;

impl Actor for Target {
    type System = Sys;
    fn register(r: &mut HandlerRegistry<Self>) {
        // Addressable across the wire so a remote node can drive (and fail) it.
        r.accept::<Stop>();
        r.accept::<Boom>();
    }
}

#[derive(Serialize, Deserialize)]
struct Stop;

impl Message for Stop {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("watch.Stop");
}

impl Handler<Stop> for Target {
    async fn handle(&mut self, _msg: Stop, ctx: &Ctx<Self>) {
        ctx.stop();
    }
}

#[derive(Serialize, Deserialize)]
struct Boom;

impl Message for Boom {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("watch.Boom");
}

impl Handler<Boom> for Target {
    async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
        panic!("target blew up");
    }
}

/// Watches `target` from `started`, recording every `Terminated` it observes.
struct Watcher {
    target: ActorRef<Target>,
    got: Reasons,
}

impl Actor for Watcher {
    type System = Sys;

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

/// A single-node cluster with SWIM off — so `block_on` reaches quiescence.
fn one_node(seed: u64) -> (Simulation, SimCluster) {
    let sim = Simulation::new(seed);
    let node = SimNetwork::new(&sim).join(NodeId::new(1));
    (sim, node)
}

fn fast_swim(downing: DowningPolicy) -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(200),
        indirect_count: 2,
        downing,
    }
}

#[test]
fn local_watch_yields_terminated_once_on_stop() {
    let (sim, node) = one_node(1);
    let got: Reasons = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&got);

    sim.block_on(async move {
        let clock = node.clock().clone();
        let target = node.spawn(Target);
        let _watcher = node.spawn(Watcher {
            target: target.clone(),
            got: recorded,
        });
        // Let the watcher's `started` register the watch before the target stops.
        clock.sleep(Duration::from_millis(1)).await;
        target.tell(Stop).await.unwrap();
    });

    assert_eq!(*got.lock().unwrap(), vec![TerminationReason::Stopped]);
}

#[test]
fn watching_an_already_dead_actor_yields_terminated_immediately() {
    // Invariant #12.
    let (sim, node) = one_node(2);
    let got: Reasons = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&got);

    sim.block_on(async move {
        let clock = node.clock().clone();
        let target = node.spawn(Target);
        target.tell(Stop).await.unwrap();
        // Let the target stop and resign before anyone watches it.
        clock.sleep(Duration::from_millis(5)).await;
        let _watcher = node.spawn(Watcher {
            target: target.clone(),
            got: recorded,
        });
        clock.sleep(Duration::from_millis(1)).await;
    });

    assert_eq!(*got.lock().unwrap(), vec![TerminationReason::Stopped]);
}

#[test]
fn watching_an_already_failed_actor_reports_failed_not_stopped() {
    // Invariant #12 with reason fidelity: a watch placed *after* the target has
    // already failed must report `Failed`, not a blanket `Stopped` (spec §12).
    let (sim, node) = one_node(5);
    let got: Reasons = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&got);

    sim.block_on(async move {
        let clock = node.clock().clone();
        let target = node.spawn(Target);
        target.tell(Boom).await.unwrap(); // panics → terminates with Failed
        // Let the target fail and resign before anyone watches it.
        clock.sleep(Duration::from_millis(5)).await;
        let _watcher = node.spawn(Watcher {
            target: target.clone(),
            got: recorded,
        });
        clock.sleep(Duration::from_millis(1)).await;
    });

    assert_eq!(*got.lock().unwrap(), vec![TerminationReason::Failed]);
}

#[test]
fn remote_watch_after_death_reports_the_true_reason() {
    // A remote watcher that registers interest after the target has already
    // failed must learn it `Failed`, not a default `Stopped`: the target's node
    // reports the reason it recorded at resignation (spec §12).
    let sim = Simulation::new(6);
    let net = SimNetwork::new(&sim); // SWIM off: the node stays up, the actor dies
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let got: Reasons = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&got);

    sim.block_on(async move {
        let clock = node_a.clock().clone();
        let target = node_b.spawn(Target);
        // Fail the target on B from A, then let it resign.
        node_a
            .resolve::<Target>(target.id().clone())
            .tell(Boom)
            .await
            .unwrap();
        clock.sleep(Duration::from_millis(5)).await;
        // Now A watches the already-dead remote target; B replies with the true
        // reason rather than a blanket `Stopped`.
        let _watcher = node_a.spawn(Watcher {
            target: node_a.resolve::<Target>(target.id().clone()),
            got: recorded,
        });
        clock.sleep(Duration::from_millis(20)).await;
    });

    assert_eq!(*got.lock().unwrap(), vec![TerminationReason::Failed]);
}

#[test]
fn watch_reports_failure_when_handler_panics() {
    let (sim, node) = one_node(3);
    let got: Reasons = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&got);

    sim.block_on(async move {
        let clock = node.clock().clone();
        let target = node.spawn(Target);
        let _watcher = node.spawn(Watcher {
            target: target.clone(),
            got: recorded,
        });
        clock.sleep(Duration::from_millis(1)).await;
        target.tell(Boom).await.unwrap();
    });

    assert_eq!(*got.lock().unwrap(), vec![TerminationReason::Failed]);
}

#[test]
fn node_down_synthesizes_terminated_for_remote_watch() {
    // The cascade (spec §8.1 step 4): a watcher of an actor on a downed node
    // receives `Terminated { NodeDown }`, even though no stop message can arrive.
    let sim = Simulation::new(4);
    let net = SimNetwork::new(&sim).with_swim(fast_swim(DowningPolicy::Timeout(
        Duration::from_millis(200),
    )));
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let target = node_b.spawn(Target);
    let got: Reasons = Arc::new(Mutex::new(Vec::new()));
    let _watcher = node_a.spawn(Watcher {
        target: node_a.resolve::<Target>(target.id().clone()),
        got: Arc::clone(&got),
    });

    net.crash(NodeId::new(2));
    sim.run_for(Duration::from_secs(5));

    assert_eq!(*got.lock().unwrap(), vec![TerminationReason::NodeDown]);
}
