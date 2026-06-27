//! Determinism conformance (harness spec §12; core spec §18.1): the same
//! `(seed, configuration)` reproduces the full event stream byte-for-byte —
//! including harness *and* grain events — with model and sandbox faults firing
//! under seed control; and a swarm sweep holds the H-invariants across seeds
//! while those faults flow. The journal, the fence, and resume are the grain's,
//! so simulating the harness runs granary's real consensus/rehydration code.

mod support;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_core::BoxFuture;
use actor_core::Clock;
use actor_simulation::Invariant;
use actor_simulation::SimSystem;
use actor_simulation::Workload;
use actor_simulation::check_reproducible;
use actor_simulation::run_swarm;
use harness::Budget;
use harness::Harness;
use harness::Kind;
use harness::Kinds;
use harness::SessionId;
use harness::Tier;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::FaultyModel;
use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::boxed;
use support::brisk_idle;
use support::final_message;
use support::harness_invariants;
use support::tool_call;

/// A workload that runs several sessions through the tool loop with seeded model
/// failures (§12.2). Callers tolerate honest failures — a model error that
/// survives the policy is a journaled terminal outcome, not a hang — and
/// re-submit on transport errors, the H3 contact discipline.
struct FaultyWorkload {
    name: &'static str,
    sessions: usize,
    model_fail_num: u64,
    /// Coverage accounting (§11): model faults the sweep actually fired while
    /// agent traffic flowed, aggregated across seeds.
    model_fired: Arc<AtomicUsize>,
}

impl FaultyWorkload {
    fn new(name: &'static str, sessions: usize, model_fail_num: u64) -> Self {
        FaultyWorkload {
            name,
            sessions,
            model_fail_num,
            model_fired: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Workload for FaultyWorkload {
    fn name(&self) -> &'static str {
        self.name
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let kinds = Kinds::new().register(
            "worker",
            Kind::new("worker")
                .sandboxed(
                    "shell",
                    "run",
                    &json!({ "type": "object" }),
                    Tier::Workspace,
                )
                .budget(Budget::new(10_000, 10))
                .grain(brisk_idle()),
        );
        let script = ScriptedModel::steps(vec![
            Ok(tool_call("c1", "shell", json!({}))),
            Ok(final_message("done")),
        ]);
        let model = FaultyModel {
            inner: Arc::new(script),
            clock: system.clock().clone(),
            entropy: system.entropy().clone(),
            max_latency: Duration::from_millis(200),
            fail_num: self.model_fail_num,
            fail_den: 4,
            fired: Arc::clone(&self.model_fired),
        };
        let harness = Harness::cluster(
            system.clone(),
            &kinds,
            Arc::new(model),
            Arc::new(ScriptedSandboxes::echo()),
        );
        let sessions = self.sessions;
        boxed(async move {
            let clock = system.clock().clone();
            let mut waiting = Vec::new();
            for i in 0..sessions {
                let session = harness.session("worker", SessionId::new(format!("s-{i}")));
                let clock = clock.clone();
                waiting.push(async move {
                    let turn = Turn::new(TurnId::new("t-1"), "go");
                    // Caller-driven resumption (§7.5): re-submit the same TurnId
                    // on a reported transport failure, bounded.
                    for _ in 0..20 {
                        match session.prompt(turn.clone()).await {
                            Ok(_outcome) => return,
                            Err(_) => clock.sleep(Duration::from_millis(500)).await,
                        }
                    }
                    panic!("a run stayed unreachable past every retry");
                });
            }
            futures::future::join_all(waiting).await;
            clock.sleep(Duration::from_secs(10)).await;
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        harness_invariants()
    }
}

#[test]
fn the_event_stream_reproduces_byte_for_byte_per_seed() {
    // The determinism contract over the real stream (core spec §18.1 #1), harness
    // and grain events included: one leak — a wall clock, an OS thread, an
    // unseeded draw — breaks this, which is the point.
    for seed in [3, 17, 5151] {
        let faultless = FaultyWorkload::new("repro-faultless", 3, 0);
        check_reproducible(&faultless, seed).expect("faultless run reproduces");
        let faulted = FaultyWorkload::new("repro-faulted", 3, 1);
        check_reproducible(&faulted, seed).expect("faulted run reproduces");
    }
}

#[test]
fn the_h_invariants_hold_across_a_faulted_seed_sweep() {
    let workload = FaultyWorkload::new("harness-swarm", 4, 1);
    run_swarm(&workload, 1..=25).expect("every seed upholds the H-invariants");
    // Coverage accounting (§11): a green sweep proves model faults actually fired
    // while agent traffic flowed, not that seed luck dodged them.
    assert!(
        workload.model_fired.load(Ordering::SeqCst) > 0,
        "the sweep never injected a model fault"
    );
}
