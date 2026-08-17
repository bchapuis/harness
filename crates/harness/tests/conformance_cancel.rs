//! Cancellation conformance (harness spec §9.2; invariant H5): message
//! granularity, idempotence, straggler discard, and propagation across the
//! delegation tree.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::Clock;
use actor_simulation::SimSystem;
use actor_simulation::run_seed;
use harness::Budget;
use harness::HarnessConfig;
use harness::Kind;
use harness::Kinds;
use harness::Model;
use harness::Record;
use harness::RecordBody;
use harness::RunError;
use harness::SessionId;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::Scenario;
use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::SlowModel;
use support::final_message;
use support::tail_records;
use support::tool_call;

async fn flush(system: &SimSystem) {
    system.clock().sleep(Duration::from_secs(60)).await;
}

fn slow_final(system: &SimSystem) -> Arc<dyn Model> {
    Arc::new(SlowModel {
        inner: Arc::new(ScriptedModel::steps(vec![Ok(final_message("too late"))])),
        clock: system.clock().clone(),
        delay: Duration::from_secs(60),
    })
}

#[test]
fn a_cancel_takes_effect_during_a_model_call() {
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let workload = Scenario::from_factories(
        "cancel-mid-call",
        Kinds::new().register("echo", Kind::new("agent")),
        Arc::new(slow_final),
        Arc::new(|_| Arc::new(ScriptedSandboxes::echo())),
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let session = harness.session("echo", SessionId::new("s-1"));
                let prompting = session.prompt(Turn::new(TurnId::new("t-1"), "go"));
                let canceller = {
                    let session = session.clone();
                    let clock = system.clock().clone();
                    async move {
                        // Cancel while the 60s model call is in flight: the
                        // mailbox is live during it (§3.2), so the cancel lands at
                        // message granularity, not after the call.
                        clock.sleep(Duration::from_secs(5)).await;
                        session.cancel(&TurnId::new("t-1")).await.expect("cancel")
                    }
                };
                let (outcome, ()) = futures::join!(prompting, canceller);
                assert_eq!(outcome.expect("call"), Err(RunError::Cancelled));
                flush(&system).await;
                *sink.lock().unwrap() = tail_records(&session).await;
            })
        },
    );
    run_seed(&workload, 43).expect("invariants hold");

    // The straggling model response was discarded, not journaled (§9.2 item 4):
    // the cancelled run's terminal record is the last word.
    let records = records.lock().unwrap();
    assert!(
        records
            .iter()
            .all(|r| !matches!(r.body, RecordBody::ModelResponse { .. })),
        "no model response journaled for the cancelled run"
    );
    assert!(matches!(
        records.last().expect("records").body,
        RecordBody::RunEnded {
            outcome: Err(RunError::Cancelled),
            ..
        }
    ));
}

#[test]
fn a_cancel_of_a_queued_turn_is_a_journaled_terminal_without_a_run() {
    // Timeline (10s model calls): t=0 run A starts and its call flies to t=10;
    // t=1 turn B is accepted behind it — fold state, not activation memory
    // (§7.3); t=2 a re-submission of B with different content is rejected off
    // the journal (§7.4 — the dedup covers queued turns); t=3 B is cancelled:
    // its `RunEnded { Cancelled }` journals with no run ever starting (§9.2),
    // and B's waiting prompt resolves from the recorded outcome. A is
    // untouched and completes at t=10.
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let make_model = |system: &SimSystem| -> Arc<dyn Model> {
        Arc::new(SlowModel {
            inner: Arc::new(ScriptedModel::new(|_| Ok(final_message("done")))),
            clock: system.clock().clone(),
            delay: Duration::from_secs(10),
        })
    };
    let workload = Scenario::from_factories(
        "cancel-queued",
        Kinds::new().register("echo", Kind::new("agent")),
        Arc::new(make_model),
        Arc::new(|_| Arc::new(ScriptedSandboxes::echo())),
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let clock = system.clock().clone();
                let session = harness.session("echo", SessionId::new("s-q"));
                let prompt_a = session.prompt(Turn::new(TurnId::new("t-a"), "a"));
                let prompt_b = {
                    let session = session.clone();
                    let clock = clock.clone();
                    async move {
                        clock.sleep(Duration::from_secs(1)).await;
                        session.prompt(Turn::new(TurnId::new("t-b"), "b")).await
                    }
                };
                let driver = {
                    let session = session.clone();
                    let clock = clock.clone();
                    async move {
                        clock.sleep(Duration::from_secs(2)).await;
                        // The queued turn is dedup-checked like any other (§7.4):
                        // same TurnId, different content — a caller bug, rejected.
                        let conflict = session.prompt(Turn::new(TurnId::new("t-b"), "not b")).await;
                        assert!(conflict.is_err(), "queued re-submit content-checked");
                        clock.sleep(Duration::from_secs(1)).await;
                        session.cancel(&TurnId::new("t-b")).await.expect("cancel");
                    }
                };
                let (a, b, ()) = futures::join!(prompt_a, prompt_b, driver);
                assert_eq!(a.expect("submit a").expect("run a").text(), "done");
                assert_eq!(b.expect("submit b"), Err(RunError::Cancelled));
                // The recorded outcome survives for any later re-contact (H7).
                let again = session.prompt(Turn::new(TurnId::new("t-b"), "b")).await;
                assert_eq!(again.expect("re-submit b"), Err(RunError::Cancelled));
                flush(&system).await;
                *sink.lock().unwrap() = tail_records(&session).await;
            })
        },
    );
    run_seed(&workload, 59).expect("invariants hold");

    // B ends by record without ever starting: its terminal commits mid-run-A,
    // and only A has a `TurnStarted` (§9.2; the checkers verify no event pair
    // fired for B).
    let records = records.lock().unwrap();
    assert_eq!(
        support::record_kinds(&records),
        vec![
            "created", "turn", "started", "turn", "ended", "model", "ended"
        ],
        "the queued cancel is a journaled transition; journal was: {records:#?}"
    );
    let started: Vec<_> = records
        .iter()
        .filter_map(|r| match &r.body {
            RecordBody::TurnStarted { turn } => Some(turn.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        started,
        vec!["t-a"],
        "the cancelled queued turn never started"
    );
}

#[test]
fn a_cancel_for_an_ended_or_unknown_run_is_a_no_op() {
    let model = Arc::new(ScriptedModel::steps(vec![Ok(final_message("done"))]));
    let sandboxes = Arc::new(ScriptedSandboxes::echo());
    let workload = Scenario::new(
        "cancel-idempotent",
        Kinds::new().register("echo", Kind::new("agent")),
        model,
        sandboxes,
        move |harness, system| {
            Box::pin(async move {
                let session = harness.session("echo", SessionId::new("s-2"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "done");
                // A delayed cancel never kills the named run's successor (§9.2
                // item 1): it is a no-op on the ended run.
                session.cancel(&TurnId::new("t-1")).await.expect("cancel");
                session.cancel(&TurnId::new("t-x")).await.expect("unknown");
                let again = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(again.text(), "done", "the recorded outcome survives");
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 47).expect("invariants hold");
}

#[test]
fn a_cancel_propagates_down_the_delegation_tree() {
    let kinds = || {
        Kinds::new()
            .register(
                "parent",
                Kind::new("parent agent")
                    .delegates_to(&["child"])
                    .budget(Budget::new(10_000, 10)),
            )
            .register(
                "child",
                Kind::new("child agent").budget(Budget::new(2_000, 4)),
            )
    };
    let make_model = |system: &SimSystem| -> Arc<dyn Model> {
        // The parent delegates immediately; the child's model call hangs for an
        // hour — only cancellation ends it.
        let script = ScriptedModel::new(|req| {
            if req.system_prompt == "child agent" {
                Ok(final_message("child-answer"))
            } else {
                let step = req
                    .transcript
                    .iter()
                    .filter(|e| matches!(e, harness::Entry::Assistant { .. }))
                    .count();
                if step == 0 {
                    Ok(tool_call(
                        "d1",
                        "delegate",
                        json!({ "kind": "child", "prompt": "sub-task" }),
                    ))
                } else {
                    Ok(final_message("parent-answer"))
                }
            }
        });
        Arc::new(SlowModel {
            inner: Arc::new(script),
            clock: system.clock().clone(),
            delay: Duration::from_secs(3_600),
        })
    };
    let child_records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let parent_records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&child_records);
    let parent_sink = Arc::clone(&parent_records);
    let workload = Scenario::from_factories(
        "cancel-propagation",
        kinds(),
        Arc::new(make_model),
        Arc::new(|_| Arc::new(ScriptedSandboxes::echo())),
        move |harness, system| {
            let sink = Arc::clone(&sink);
            let parent_sink = Arc::clone(&parent_sink);
            Box::pin(async move {
                let session = harness.session("parent", SessionId::new("root"));
                let prompting = session.prompt_within(
                    Turn::new(TurnId::new("t-1"), "go"),
                    Duration::from_secs(20_000),
                );
                let canceller = {
                    let session = session.clone();
                    let clock = system.clock().clone();
                    async move {
                        // The parent's first call completes at 3600s and journals
                        // the delegation; the child's hour-long model call is then
                        // in flight. Cancel mid-flight.
                        clock.sleep(Duration::from_secs(4_000)).await;
                        session.cancel(&TurnId::new("t-1")).await.expect("cancel")
                    }
                };
                let (outcome, ()) = futures::join!(prompting, canceller);
                assert_eq!(outcome.expect("call"), Err(RunError::Cancelled));
                flush(&system).await;

                // The parent's journal names the child (§8.1); read the child's
                // journal through its own session.
                let records = tail_records(&session).await;
                let (child_kind, child_session) = records
                    .iter()
                    .find_map(|r| match &r.body {
                        RecordBody::ChildRun {
                            child_kind,
                            child_session,
                            ..
                        } => Some((child_kind.clone(), child_session.clone())),
                        _ => None,
                    })
                    .expect("journaled delegation");
                let child = harness.session(child_kind.as_str(), child_session);
                *sink.lock().unwrap() = tail_records(&child).await;
                *parent_sink.lock().unwrap() = records;
            })
        },
    )
    // The child (2_000-token budget) must reach its hour-long model call so
    // cancellation is what ends it; pin the floor to 0 so it isn't stopped first.
    .with_config(HarnessConfig {
        budget_floor: 0,
        ..HarnessConfig::default()
    });
    run_seed(&workload, 53).expect("invariants hold");

    // The child's run ended Cancelled within bounded logical time (H5, §9.2 item 2).
    assert!(
        child_records.lock().unwrap().iter().any(|r| matches!(
            &r.body,
            RecordBody::RunEnded {
                outcome: Err(RunError::Cancelled),
                ..
            }
        )),
        "the child's run ended Cancelled (§9.2 item 2)"
    );

    // The delivered propagation cleared its owed fact durably: the parent
    // journaled `CancelDelivered` for the delegation once the child acked
    // (§9.2), so no later activation re-owes the send.
    assert!(
        parent_records
            .lock()
            .unwrap()
            .iter()
            .any(|r| matches!(&r.body, RecordBody::CancelDelivered { .. })),
        "the parent journaled CancelDelivered for the propagated cancel (§9.2)"
    );
}
