//! Phase A gate (spec §18.1): the deterministic executor reproduces runs from a
//! seed, fires timers in deadline order, and advances virtual time for free.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Spawner;
use actor_simulation::Simulation;

/// A workload whose interleaving depends on both scheduling and application
/// randomness — every draw flows through the one seeded stream.
fn run_workload(seed: u64) -> Vec<String> {
    let sim = Simulation::new(seed);
    let log = Arc::new(Mutex::new(Vec::new()));

    for task in 0..4u64 {
        let log = Arc::clone(&log);
        let clock = sim.clock();
        let entropy = sim.entropy();
        sim.spawner().launch(Box::pin(async move {
            for step in 0..3u64 {
                let r = entropy.next_u64() % 100;
                clock.sleep(Duration::from_millis(r + 1)).await;
                log.lock()
                    .unwrap()
                    .push(format!("task{task}-step{step}-r{r}"));
            }
        }));
    }

    sim.run();
    log.lock().unwrap().clone()
}

#[test]
fn same_seed_reproduces_run() {
    // Byte-identical results from the same seed (spec §18.1 #1).
    assert_eq!(run_workload(42), run_workload(42));
    assert_eq!(run_workload(7), run_workload(7));
}

#[test]
fn different_seeds_diverge() {
    assert_ne!(run_workload(1), run_workload(2));
}

#[test]
fn timers_fire_in_deadline_order() {
    let sim = Simulation::new(99);
    let log = Arc::new(Mutex::new(Vec::new()));

    // Launch in scrambled order; each task sleeps a distinct duration.
    for ms in [50u64, 10, 30, 20, 40] {
        let log = Arc::clone(&log);
        let clock = sim.clock();
        sim.spawner().launch(Box::pin(async move {
            clock.sleep(Duration::from_millis(ms)).await;
            log.lock().unwrap().push(ms);
        }));
    }

    sim.run();
    assert_eq!(*log.lock().unwrap(), vec![10, 20, 30, 40, 50]);
}

#[test]
fn sleep_advances_virtual_time() {
    let sim = Simulation::new(0);
    let clock = sim.clock();
    // An hour of logical time, paid for in zero wall-clock time (spec §18.1 #2).
    let elapsed = sim.block_on(async move {
        let start = clock.now();
        clock.sleep(Duration::from_secs(3600)).await;
        clock.now().duration_since(start)
    });
    assert_eq!(elapsed, Duration::from_secs(3600));
}

#[test]
fn timeout_elapses_on_slow_future() {
    let sim = Simulation::new(0);
    let clock = sim.clock();
    let slow = sim.clock();
    let res = sim.block_on(async move {
        clock
            .timeout(
                Duration::from_millis(100),
                slow.sleep(Duration::from_millis(500)),
            )
            .await
    });
    assert!(res.is_err(), "slow future should elapse");
}

#[test]
fn timeout_passes_through_fast_future() {
    let sim = Simulation::new(0);
    let clock = sim.clock();
    let fast = sim.clock();
    let res = sim.block_on(async move {
        clock
            .timeout(Duration::from_millis(500), async move {
                fast.sleep(Duration::from_millis(100)).await;
                7u32
            })
            .await
    });
    assert_eq!(res, Ok(7));
}
