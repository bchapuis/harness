//! Conformance: determinism harness (spec §18.1) — the *event stream* is
//! byte-identical for a fixed seed, and `run_for` stops cleanly at its bound
//! even with perpetual work outstanding.

mod support;

use std::time::Duration;

use actor_core::ActorSystem;
use actor_core::Clock;
use actor_core::Event;
use actor_core::Instant;
use actor_core::Spawner;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use support::Greet;
use support::Greeter;

/// Run a small workload under a recorded single-node system and return its event
/// stream.
fn event_stream(seed: u64) -> Vec<Event> {
    let (sim, system, recorder) = support::local_recorded(seed);
    sim.block_on(async move {
        let greeter = system.spawn(Greeter::<SimSystem>::new("Hi"));
        for name in ["a", "b", "c"] {
            let _ = greeter.ask(Greet { name: name.into() }).await;
        }
    });
    recorder.events()
}

#[test]
fn the_same_seed_yields_an_identical_event_stream() {
    assert_eq!(event_stream(42), event_stream(42));
}

#[test]
fn different_seeds_can_diverge_but_each_is_stable() {
    // Each seed is internally reproducible (the core determinism guarantee).
    assert_eq!(event_stream(7), event_stream(7));
    assert_eq!(event_stream(8), event_stream(8));
}

#[test]
fn run_for_stops_at_the_time_bound_with_work_outstanding() {
    let sim = Simulation::new(1);
    let clock = sim.clock();
    // A task that sleeps forever — never quiesces.
    sim.spawner().launch(Box::pin(async move {
        loop {
            clock.sleep(Duration::from_millis(100)).await;
        }
    }));

    sim.run_for(Duration::from_secs(1));
    // Returns at exactly the bound rather than running the perpetual task forever.
    assert_eq!(sim.now(), Instant::ZERO + Duration::from_secs(1));
}
