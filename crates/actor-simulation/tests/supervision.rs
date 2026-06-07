//! Supervision (spec §11): a faulting actor is restarted (re-running `started`
//! on a fresh instance, keeping its mailbox), resumed, or stopped per its
//! strategy; exceeding the restart window escalates to stop.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::Backoff;
use actor_core::BoxError;
use actor_core::CallError;
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

fn system(seed: u64) -> (Simulation, Sys) {
    let sim = Simulation::new(seed);
    let system = LocalSystem::new(sim.clock(), sim.entropy(), sim.spawner());
    (sim, system)
}

#[derive(Serialize, Deserialize)]
struct Boom;
impl Message for Boom {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("sup.Boom");
}

// --- Restart: fresh instance, re-runs `started`, keeps the mailbox -----------

struct Flaky {
    starts: Arc<AtomicU32>,
}

impl Actor for Flaky {
    type System = Sys;

    async fn started(&mut self, _ctx: &Ctx<Self>) -> Result<(), BoxError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn supervision() -> Supervision {
        Supervision::restart(5, Duration::from_secs(10), Backoff::None)
    }
}

#[derive(Serialize, Deserialize)]
struct Starts;
impl Message for Starts {
    type Reply = u32;
    const MANIFEST: Manifest = Manifest::new("sup.Starts");
}

impl Handler<Boom> for Flaky {
    async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
        panic!("flaky handler");
    }
}
impl Handler<Starts> for Flaky {
    async fn handle(&mut self, _msg: Starts, _ctx: &Ctx<Self>) -> u32 {
        self.starts.load(Ordering::SeqCst)
    }
}

#[test]
fn restart_recreates_the_actor_and_reruns_started() {
    let (sim, system) = system(1);
    let starts = Arc::new(AtomicU32::new(0));
    let factory_starts = Arc::clone(&starts);

    let after = sim.block_on(async move {
        let actor = system.spawn_with(move || Flaky {
            starts: Arc::clone(&factory_starts),
        });
        let _ = actor.tell(Boom).await; // panic → restart, started runs again
        // `Starts` was enqueued before the restart and survives it (same mailbox).
        actor.ask(Starts).await
    });

    // `started` ran once at spawn and once after the restart.
    assert_eq!(after, Ok(2));
    assert_eq!(starts.load(Ordering::SeqCst), 2);
}

#[test]
fn exceeding_the_restart_window_escalates_to_stop() {
    let (sim, system) = system(2);
    let starts = Arc::new(AtomicU32::new(0));
    let factory_starts = Arc::clone(&starts);

    let result = sim.block_on(async move {
        // Allow at most 2 restarts; a 3rd fault stops the actor.
        let actor = system.spawn_with(move || Bounded {
            starts: Arc::clone(&factory_starts),
        });
        for _ in 0..3 {
            let _ = actor.tell(Boom).await;
        }
        actor.ask(Starts).await
    });

    assert_eq!(result, Err(CallError::DeadLetter));
}

// A variant with a tight restart limit.
struct Bounded {
    starts: Arc<AtomicU32>,
}
impl Actor for Bounded {
    type System = Sys;
    async fn started(&mut self, _ctx: &Ctx<Self>) -> Result<(), BoxError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn supervision() -> Supervision {
        Supervision::restart(2, Duration::from_secs(10), Backoff::None)
    }
}
impl Handler<Boom> for Bounded {
    async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
        panic!("bounded handler");
    }
}
impl Handler<Starts> for Bounded {
    async fn handle(&mut self, _msg: Starts, _ctx: &Ctx<Self>) -> u32 {
        self.starts.load(Ordering::SeqCst)
    }
}

// --- Resume: keep state, drop the failed message -----------------------------

struct Accumulator {
    sum: u32,
}
impl Actor for Accumulator {
    type System = Sys;
    fn supervision() -> Supervision {
        Supervision::resume()
    }
}

#[derive(Serialize, Deserialize)]
struct Add(u32);
impl Message for Add {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("sup.Add");
}
#[derive(Serialize, Deserialize)]
struct Total;
impl Message for Total {
    type Reply = u32;
    const MANIFEST: Manifest = Manifest::new("sup.Total");
}

impl Handler<Add> for Accumulator {
    async fn handle(&mut self, msg: Add, _ctx: &Ctx<Self>) {
        self.sum += msg.0;
    }
}
impl Handler<Boom> for Accumulator {
    async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
        panic!("accumulator handler");
    }
}
impl Handler<Total> for Accumulator {
    async fn handle(&mut self, _msg: Total, _ctx: &Ctx<Self>) -> u32 {
        self.sum
    }
}

#[test]
fn resume_keeps_state_and_drops_the_failed_message() {
    let (sim, system) = system(3);
    let total = sim.block_on(async move {
        let actor = system.spawn(Accumulator { sum: 0 });
        actor.tell(Add(1)).await.unwrap();
        let _ = actor.tell(Boom).await; // panics → Resume keeps the actor + state
        actor.tell(Add(2)).await.unwrap();
        actor.ask(Total).await
    });
    assert_eq!(total, Ok(3));
}

// --- Backoff is honored (restart still recovers after a delay) ---------------

#[test]
fn restart_with_backoff_still_recovers() {
    let (sim, system) = system(4);
    let starts = Arc::new(AtomicU32::new(0));
    let factory_starts = Arc::clone(&starts);
    let observed = Arc::new(Mutex::new(0u32));
    let sink = Arc::clone(&observed);

    sim.block_on(async move {
        let actor = system.spawn_with(move || Backed {
            starts: Arc::clone(&factory_starts),
        });
        let _ = actor.tell(Boom).await;
        *sink.lock().unwrap() = actor.ask(Starts).await.unwrap();
    });

    // Recovered after the backoff delay; started ran twice.
    assert_eq!(*observed.lock().unwrap(), 2);
}

struct Backed {
    starts: Arc<AtomicU32>,
}
impl Actor for Backed {
    type System = Sys;
    async fn started(&mut self, _ctx: &Ctx<Self>) -> Result<(), BoxError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn supervision() -> Supervision {
        Supervision::restart(
            5,
            Duration::from_secs(10),
            Backoff::Exponential {
                base: Duration::from_millis(50),
                max: Duration::from_secs(1),
            },
        )
    }
}
impl Handler<Boom> for Backed {
    async fn handle(&mut self, _msg: Boom, _ctx: &Ctx<Self>) {
        panic!("backed handler");
    }
}
impl Handler<Starts> for Backed {
    async fn handle(&mut self, _msg: Starts, _ctx: &Ctx<Self>) -> u32 {
        self.starts.load(Ordering::SeqCst)
    }
}
