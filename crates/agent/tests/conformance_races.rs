//! Run-loop race conformance: the entity's serial mailbox gives no ordering
//! guarantee between a client's `Submit` and the workflow's own self-driving
//! messages (`Tick`, `Settled`), so what the loop keeps outside the fold must
//! stay correct under any interleaving (§3.2). Two races regress here:
//!
//! 1. A `Submit` processed between a run's terminal commit and the tick that
//!    follows it starts the next run directly: it must not inherit anything of
//!    the ended run's, which is why an effect's key names its turn (`StepKey`,
//!    workflow §3.1) even though a synthesized call id repeats across runs.
//! 2. A straggler outcome of an ended run must not release the successor run's
//!    launch claim, and a superseded call's response must never journal a second
//!    `ModelResponse` for a step (§3.2 step 2, §7.1 item 4).

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::ActorSystem;
use actor_core::Clock;
use actor_simulation::SimSystem;
use actor_simulation::run_seed;
use actor_simulation::scenario_sweep;
use actor_simulation::sweep_seeds;
use agent::Kind;
use agent::Kinds;
use agent::Model;
use agent::Record;
use agent::RunError;
use agent::SessionId;
use agent::Tier;
use agent::Turn;
use agent::TurnId;
use serde_json::json;

use support::Scenario;
use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::SlowModel;
use support::final_message;
use support::record_kinds;
use support::tail_records;
use support::tool_call;

/// A kind with one Workspace-tier tool, so runs exercise the tool step without
/// journaling `TierAcquired` noise.
fn probing_kind() -> Kinds {
    Kinds::new().register(
        "echo",
        Kind::new("agent").tool(
            "probe",
            "probe the workspace",
            &json!({ "type": "object" }),
            Tier::Workspace,
        ),
    )
}

/// One id-less tool call per turn, then a final message: the agent runtime
/// synthesizes `call-{step}-{i}` ids, so every run's first call is
/// `call-1-0`; consecutive runs collide exactly when stale per-run state
/// leaks across the run boundary.
fn probing_script() -> ScriptedModel {
    ScriptedModel::new(|req| match req.transcript.last() {
        Some(agent::Entry::ToolResult { .. }) => Ok(final_message("done")),
        _ => Ok(tool_call("", "probe", json!({}))),
    })
}

#[test]
fn a_submit_racing_the_post_end_advance_starts_clean() {
    // Run A takes two 10s model calls, so its final outcome (the message whose
    // processing commits `RunEnded` and brings the tick that follows it) lands
    // at exactly t=20. Submitting B at t=20 puts three enqueues at one virtual
    // instant (A's outcome, B's `Submit`, the tick), and the executor's
    // seed-randomized scheduling explores their orderings. On the interleaving
    // where B's `Submit` is processed after the terminal commit but before the
    // tick, the turn starts directly and must not inherit anything of the ended
    // run's: B's synthesized `call-1-0` is A's `call-1-0` by name, and a loop
    // that keyed a claim by the id alone would skip B's call forever, hanging
    // the run until the prompt times out.
    scenario_sweep(
        "agent-races/submit-post-end",
        sweep_seeds(0..96),
        |seed| {
            let make_model = |system: &SimSystem| -> Arc<dyn Model> {
                Arc::new(SlowModel {
                    inner: Arc::new(probing_script()),
                    clock: system.clock().clone(),
                    delay: Duration::from_secs(10),
                })
            };
            let workload = Scenario::from_factories(
                "submit-vs-post-end-advance",
                probing_kind(),
                Arc::new(make_model),
                Arc::new(|_| Arc::new(ScriptedSandboxes::echo())),
                move |harness, system| {
                    Box::pin(async move {
                        let clock = system.clock().clone();
                        let session = harness.session("echo", SessionId::new("s-race")).unwrap();
                        let a = session.prompt(Turn::new(TurnId::new("t-a"), "go"));
                        let b = {
                            let session = session.clone();
                            let clock = clock.clone();
                            async move {
                                clock.sleep(Duration::from_secs(20)).await;
                                session.prompt(Turn::new(TurnId::new("t-b"), "go")).await
                            }
                        };
                        let (a, b) = futures::join!(a, b);
                        assert_eq!(a.expect("submit a").expect("run a").text(), "done");
                        let b = b
                            .unwrap_or_else(|e| panic!("seed {seed}: run b never finished: {e:?}"))
                            .expect("run b");
                        assert_eq!(b.text(), "done");
                        clock.sleep(Duration::from_secs(60)).await;
                    })
                },
            );
            run_seed(&workload, seed).unwrap_or_else(|e| panic!("seed {seed}: {e:?}"));
        },
    );
}

#[test]
fn a_straggler_model_done_never_unlocks_a_second_call() {
    // Deterministic timeline (10s model calls, 30s tool calls):
    //   t=0   run A starts; its model call flies until t=10.
    //   t=1   run A is cancelled; the call keeps flying (§7.2 item 4).
    //   t=2   run B starts; its own model call flies until t=12.
    //   t=9   the first wait on B lapses (7s deadline).
    //   t=10  run A's straggler outcome lands, after B's call launched,
    //         before B's response: it must not release B's launch claim.
    //   t=11  the re-attach nudges a tick: a loop that let the straggler
    //         release the claim issues a second concurrent call for B here, and
    //         its response would journal a second `ModelResponse` for the step.
    //   t=12  B's real response journals; the tool runs to t=42; B finishes.
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let make_model = |system: &SimSystem| -> Arc<dyn Model> {
        Arc::new(SlowModel {
            inner: Arc::new(probing_script()),
            clock: system.clock().clone(),
            delay: Duration::from_secs(10),
        })
    };
    let make_sandboxes = |system: &SimSystem| -> Arc<dyn agent::SandboxProvider> {
        Arc::new(
            ScriptedSandboxes::echo().with_delay(system.clock().clone(), Duration::from_secs(30)),
        )
    };
    let workload = Scenario::from_factories(
        "straggler-model-done",
        probing_kind(),
        Arc::new(make_model),
        Arc::new(make_sandboxes),
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let clock = system.clock().clone();
                let session = harness
                    .session("echo", SessionId::new("s-straggler"))
                    .unwrap();
                let prompt_a = session.prompt(Turn::new(TurnId::new("t-a"), "go-a"));
                let driver = {
                    let session = session.clone();
                    let clock = clock.clone();
                    async move {
                        clock.sleep(Duration::from_secs(1)).await;
                        session.cancel(&TurnId::new("t-a")).await.expect("cancel");
                        clock.sleep(Duration::from_secs(1)).await;
                        let turn_b = Turn::new(TurnId::new("t-b"), "go-b");
                        let first = session
                            .prompt_within(turn_b.clone(), Duration::from_secs(7))
                            .await;
                        assert!(first.is_err(), "the 7s wait lapses before B finishes");
                        clock.sleep(Duration::from_secs(2)).await;
                        let outcome = session
                            .prompt_within(turn_b, Duration::from_secs(3_600))
                            .await
                            .expect("re-attach")
                            .expect("run b");
                        assert_eq!(outcome.text(), "done");
                    }
                };
                let (a, ()) = futures::join!(prompt_a, driver);
                assert_eq!(a.expect("submit a"), Err(RunError::Cancelled));
                clock.sleep(Duration::from_secs(120)).await;
                *sink.lock().unwrap() = tail_records(&session).await;
            })
        },
    );
    run_seed(&workload, 61).expect("invariants hold");

    // Exactly one `ModelResponse` per step of run B (§3.2 step 2): the
    // straggler neither journaled nor unlocked a duplicate call.
    let records = records.lock().unwrap();
    assert_eq!(
        record_kinds(&records),
        vec![
            "created", "turn", "started", "ended", "turn", "started", "model", "tool", "model",
            "ended"
        ],
        "one model response per step; journal was: {records:#?}"
    );
}
