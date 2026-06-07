//! Supervision hierarchy (spec §11.1): a child that faults with `Escalate` fails
//! its parent, which then applies its own strategy — stop, restart, or escalate
//! further up the tree.

use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorId;
use actor_core::ActorSystem;
use actor_core::Backoff;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::LocalSystem;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::Supervision;
use actor_simulation::SimClock;
use actor_simulation::SimEntropy;
use actor_simulation::SimSpawner;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

type Sys = LocalSystem<SimClock, SimEntropy, SimSpawner>;

fn sys(seed: u64) -> (Simulation, Sys) {
    let sim = Simulation::new(seed);
    let system = LocalSystem::new(sim.clock(), sim.entropy(), sim.spawner());
    (sim, system)
}

// --- Messages ----------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct Boom;
impl Message for Boom {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("esc.Boom");
}

#[derive(Serialize, Deserialize)]
struct Ping;
impl Message for Ping {
    type Reply = u32;
    const MANIFEST: Manifest = Manifest::new("esc.Ping");
}

/// Spawn a child and return its id (so a test can reach it).
#[derive(Serialize, Deserialize)]
struct SpawnChild;
impl Message for SpawnChild {
    type Reply = ActorId;
    const MANIFEST: Manifest = Manifest::new("esc.SpawnChild");
}

// --- A child that escalates on fault -----------------------------------------

struct Child;
impl Actor for Child {
    type System = Sys;
    fn supervision() -> Supervision {
        Supervision::with(|_| actor_core::SupervisionDirective::Escalate)
    }
}
impl Handler<Boom> for Child {
    async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
        panic!("child boom");
    }
}

// --- A parent that stops when a child escalates ------------------------------

struct StopParent;
impl Actor for StopParent {
    type System = Sys;
    // Default supervision is Stop.
}
impl Handler<SpawnChild> for StopParent {
    async fn handle(&mut self, _msg: SpawnChild, ctx: &Ctx<Self>) -> ActorId {
        ctx.spawn(Child).id().clone()
    }
}
impl Handler<Ping> for StopParent {
    async fn handle(&mut self, _msg: Ping, _ctx: &Ctx<Self>) -> u32 {
        0
    }
}

#[test]
fn child_escalation_stops_a_stop_parent() {
    let (sim, system) = sys(1);
    let result = sim.block_on(async move {
        let clock = system.clock().clone();
        let parent = system.spawn(StopParent);
        let child_id = parent.ask(SpawnChild).await.unwrap();
        system.resolve::<Child>(child_id).tell(Boom).await.unwrap();
        clock.sleep(Duration::from_millis(10)).await; // let the escalation land
        parent.ask(Ping).await
    });
    // The parent stopped because the child escalated, so it dead-letters.
    assert_eq!(result, Err(actor_core::CallError::DeadLetter));
}

// --- A parent that restarts when a child escalates ---------------------------

struct RestartParent {
    starts: Arc<AtomicU32>,
}
impl Actor for RestartParent {
    type System = Sys;
    async fn started(&mut self, _ctx: &Ctx<Self>) -> Result<(), actor_core::BoxError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn supervision() -> Supervision {
        Supervision::restart(5, Duration::from_secs(10), Backoff::None)
    }
}
impl Handler<SpawnChild> for RestartParent {
    async fn handle(&mut self, _msg: SpawnChild, ctx: &Ctx<Self>) -> ActorId {
        ctx.spawn(Child).id().clone()
    }
}
impl Handler<Ping> for RestartParent {
    async fn handle(&mut self, _msg: Ping, _ctx: &Ctx<Self>) -> u32 {
        self.starts.load(Ordering::SeqCst)
    }
}

#[test]
fn child_escalation_restarts_a_restart_parent() {
    let (sim, system) = sys(2);
    let starts = Arc::new(AtomicU32::new(0));
    let factory_starts = Arc::clone(&starts);

    let started_count = sim.block_on(async move {
        let clock = system.clock().clone();
        let parent = system.spawn_with(move || RestartParent {
            starts: Arc::clone(&factory_starts),
        });
        let child_id = parent.ask(SpawnChild).await.unwrap();
        system.resolve::<Child>(child_id).tell(Boom).await.unwrap();
        clock.sleep(Duration::from_millis(10)).await; // escalation → parent restart
        parent.ask(Ping).await
    });

    // `started` ran once initially and once after the restart.
    assert_eq!(started_count, Ok(2));
}

// --- Chained escalation: child → parent (escalate) → grandparent (stop) ------

struct EscalatingParent;
impl Actor for EscalatingParent {
    type System = Sys;
    fn supervision() -> Supervision {
        Supervision::with(|_| actor_core::SupervisionDirective::Escalate)
    }
}
impl Handler<SpawnChild> for EscalatingParent {
    async fn handle(&mut self, _msg: SpawnChild, ctx: &Ctx<Self>) -> ActorId {
        ctx.spawn(Child).id().clone()
    }
}

#[derive(Serialize, Deserialize)]
struct SpawnParent;
impl Message for SpawnParent {
    type Reply = ActorId;
    const MANIFEST: Manifest = Manifest::new("esc.SpawnParent");
}

struct Grandparent;
impl Actor for Grandparent {
    type System = Sys;
    // Default Stop.
}
impl Handler<SpawnParent> for Grandparent {
    async fn handle(&mut self, _msg: SpawnParent, ctx: &Ctx<Self>) -> ActorId {
        ctx.spawn(EscalatingParent).id().clone()
    }
}
impl Handler<Ping> for Grandparent {
    async fn handle(&mut self, _msg: Ping, _ctx: &Ctx<Self>) -> u32 {
        0
    }
}

#[test]
fn escalation_chains_up_to_the_grandparent() {
    let (sim, system) = sys(3);
    let result = sim.block_on(async move {
        let clock = system.clock().clone();
        let grandparent = system.spawn(Grandparent);
        let parent_id = grandparent.ask(SpawnParent).await.unwrap();
        let child_id = system
            .resolve::<EscalatingParent>(parent_id)
            .ask(SpawnChild)
            .await
            .unwrap();
        system.resolve::<Child>(child_id).tell(Boom).await.unwrap();
        clock.sleep(Duration::from_millis(10)).await;
        // child → parent escalates → grandparent stops.
        grandparent.ask(Ping).await
    });
    assert_eq!(result, Err(actor_core::CallError::DeadLetter));
}
