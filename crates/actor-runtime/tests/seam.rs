//! Unit-level checks of the production runtime seam (spec §4.6): the tokio clock
//! advances and sleeps, OS entropy yields independent streams, and the spawner
//! actually runs launched tasks to completion.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Spawner;
use actor_runtime::OsEntropy;
use actor_runtime::TokioClock;
use actor_runtime::TokioSpawner;

#[tokio::test]
async fn clock_advances_across_a_sleep() {
    let clock = TokioClock::new();
    let before = clock.now();
    clock.sleep(Duration::from_millis(20)).await;
    let after = clock.now();
    assert!(
        after.duration_since(before) >= Duration::from_millis(20),
        "clock must advance by at least the slept duration"
    );
}

#[tokio::test]
async fn timeout_fires_on_a_stalled_future() {
    let clock = TokioClock::new();
    // A future that never resolves must be cut off by the deadline.
    let outcome = clock
        .timeout(Duration::from_millis(20), std::future::pending::<()>())
        .await;
    assert!(outcome.is_err());
}

#[test]
fn entropy_streams_are_independent_and_nonconstant() {
    let a = OsEntropy::new();
    let b = OsEntropy::new();
    // Distinct OS seeds: the two streams should not march in lockstep, and a
    // single stream should not return a constant.
    let xs: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
    let ys: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
    assert_ne!(xs, ys);
    assert!(xs.iter().any(|&x| x != xs[0]));
}

#[tokio::test]
async fn spawner_runs_launched_tasks() {
    let spawner = TokioSpawner::current();
    let ran = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&ran);
    spawner.launch(Box::pin(async move {
        flag.store(true, Ordering::SeqCst);
    }));
    // Yield until the spawned task has had a chance to run.
    for _ in 0..100 {
        if ran.load(Ordering::SeqCst) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(ran.load(Ordering::SeqCst), "launched task must run");
}
