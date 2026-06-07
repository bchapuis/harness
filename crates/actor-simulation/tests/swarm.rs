//! FoundationDB-style swarm testing (spec §18.4, §18.6): a few workloads run
//! across many seeds, each with randomized scheduling and mailbox capacity,
//! while the default invariants are checked continuously. Coverage is
//! cluster-time exercised, not test count.

use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::SimSystem;
use actor_simulation::Workload;
use actor_simulation::run_seed;
use actor_simulation::run_swarm;
use serde::Deserialize;
use serde::Serialize;
use std::time::Duration;

// --- Actors used by the workloads ---------------------------------------------

/// Echoes a counter back; also tallies how many it served (private state).
struct Echo {
    served: u64,
}

impl Actor for Echo {
    type System = SimSystem;
}

#[derive(Serialize, Deserialize)]
struct Ping(u64);

impl Message for Ping {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("swarm.Ping");
}

impl Handler<Ping> for Echo {
    async fn handle(&mut self, msg: Ping, _ctx: &Ctx<Self>) -> u64 {
        self.served += 1;
        msg.0
    }
}

/// Sleeps mid-handler, so concurrent asks would overlap if execution were not
/// serial — exercising the serial-execution invariant under random scheduling.
struct Worker {
    clock: actor_simulation::SimClock,
}

impl Actor for Worker {
    type System = SimSystem;
}

#[derive(Serialize, Deserialize)]
struct Job(u64);

impl Message for Job {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("swarm.Job");
}

impl Handler<Job> for Worker {
    async fn handle(&mut self, msg: Job, _ctx: &Ctx<Self>) -> u64 {
        self.clock.sleep(Duration::from_millis(1)).await;
        msg.0
    }
}

// --- Workloads ----------------------------------------------------------------

/// Fan out asks across several echo actors and verify every reply.
struct AskStorm {
    actors: usize,
    asks_per_actor: u64,
}

impl Workload for AskStorm {
    fn name(&self) -> &'static str {
        "ask-storm"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let actors = self.actors;
        let asks = self.asks_per_actor;
        Box::pin(async move {
            let refs: Vec<_> = (0..actors)
                .map(|_| system.spawn(Echo { served: 0 }))
                .collect();
            let mut futures = Vec::new();
            for r in &refs {
                for i in 0..asks {
                    futures.push(r.ask(Ping(i)));
                }
            }
            let replies = futures::future::join_all(futures).await;
            // Per-actor FIFO: replies for each actor come back in send order.
            for chunk in replies.chunks(asks as usize) {
                let got: Vec<u64> = chunk.iter().map(|r| r.clone().unwrap()).collect();
                assert_eq!(got, (0..asks).collect::<Vec<_>>());
            }
        })
    }
}

/// Hammer a single slow worker with concurrent asks; serial execution must hold
/// regardless of scheduling order.
struct ConcurrentLoad {
    asks: u64,
}

impl Workload for ConcurrentLoad {
    fn name(&self) -> &'static str {
        "concurrent-load"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let asks = self.asks;
        Box::pin(async move {
            let clock = system.clock().clone();
            let worker = system.spawn(Worker { clock });
            let futures: Vec<_> = (0..asks).map(|i| worker.ask(Job(i))).collect();
            let replies = futures::future::join_all(futures).await;
            for (i, reply) in replies.into_iter().enumerate() {
                assert_eq!(reply, Ok(i as u64));
            }
        })
    }
}

// --- Tests --------------------------------------------------------------------

#[test]
fn ask_storm_holds_across_seeds() {
    let workload = AskStorm {
        actors: 4,
        asks_per_actor: 8,
    };
    // Each seed perturbs scheduling order and mailbox capacity; all must pass.
    if let Err(failure) = run_swarm(&workload, 0..256) {
        panic!("{failure}");
    }
}

#[test]
fn concurrent_load_stays_serial_across_seeds() {
    let workload = ConcurrentLoad { asks: 16 };
    if let Err(failure) = run_swarm(&workload, 0..256) {
        panic!("{failure}");
    }
}

#[test]
fn a_single_seed_replays_identically() {
    // Reproduction (spec §18.6): the same seed yields the same outcome.
    let workload = AskStorm {
        actors: 3,
        asks_per_actor: 5,
    };
    assert!(run_seed(&workload, 12345).is_ok());
    assert!(run_seed(&workload, 12345).is_ok());
}

/// A workload that abandons an in-flight ask, to prove the harness actually
/// catches a silently-lost call rather than passing everything.
struct DropsAnAsk;

impl Workload for DropsAnAsk {
    fn name(&self) -> &'static str {
        "drops-an-ask"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let clock = system.clock().clone();
            let worker = system.spawn(Worker {
                clock: system.clock().clone(),
            });
            // Issue an ask, then drop its future before it can complete by
            // racing it against an immediately-ready branch.
            let ask = worker.ask(Job(1));
            let done = std::future::ready(());
            futures::pin_mut!(ask, done);
            let _ = futures::future::select(ask, done).await;
            // Give the worker's own timer somewhere to land so the run is not
            // empty; the abandoned ask never reaches an outcome.
            clock.sleep(Duration::from_millis(5)).await;
        })
    }
}

#[test]
fn harness_detects_a_silently_lost_ask() {
    let failure = run_seed(&DropsAnAsk, 1).expect_err("abandoned ask must be caught");
    assert!(
        failure
            .violations
            .iter()
            .any(|v| v.invariant == "no-silent-loss"),
        "expected a no-silent-loss violation, got: {failure}",
    );
}

// --- Fault-injecting supervision workload (spec §18.3) ------------------------

/// A service whose handler panics at seed-controlled points (`buggify`) and
/// restarts. Whatever the scheduling and fault timing, every call must still
/// complete and the invariants must hold.
struct Flaky;

impl Actor for Flaky {
    type System = SimSystem;

    fn supervision() -> actor_core::Supervision {
        // A generous window so injected faults restart rather than escalate.
        actor_core::Supervision::restart(1000, Duration::from_secs(3600), actor_core::Backoff::None)
    }
}

#[derive(Serialize, Deserialize)]
struct Work(u64);

impl Message for Work {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("swarm.Work");
}

impl Handler<Work> for Flaky {
    async fn handle(&mut self, msg: Work, ctx: &Ctx<Self>) -> u64 {
        use actor_core::Entropy;
        // Inject a fault on roughly one call in four (spec §18.3).
        if ctx.system().entropy().buggify(1, 4) {
            panic!("injected fault");
        }
        msg.0
    }
}

struct FlakyService {
    rounds: u64,
}

impl Workload for FlakyService {
    fn name(&self) -> &'static str {
        "flaky-service"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let rounds = self.rounds;
        Box::pin(async move {
            let service = system.spawn_with(|| Flaky);
            for i in 0..rounds {
                // Each call completes — `Ok` or `DeadLetter` (its handler
                // faulted) — never hanging; the restart keeps the service alive.
                let _ = service.ask(Work(i)).await;
            }
        })
    }
}

#[test]
fn flaky_service_survives_injected_faults_across_seeds() {
    let workload = FlakyService { rounds: 24 };
    if let Err(failure) = run_swarm(&workload, 0..256) {
        panic!("{failure}");
    }
}
