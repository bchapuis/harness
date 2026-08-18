//! Run-loop conformance (harness spec §3, §5.4, §6.4, §7.4): the happy path,
//! the tool loop, write-ahead order, idempotent submission (H7), and serialized
//! turns — each asserted against the journal, the single source of truth
//! (§10.1), under the continuous harness checkers (§11).

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::CallError;
use actor_core::Clock;
use actor_simulation::SimSystem;
use actor_simulation::run_seed;
use granary::GrainError;
use harness::Budget;
use harness::Kind;
use harness::Kinds;
use harness::Record;
use harness::RecordBody;
use harness::SessionId;
use harness::Tier;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::Scenario;
use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::brisk_idle;
use support::final_message;
use support::record_kinds;
use support::tail_records;
use support::tool_call;

fn echo_kind() -> Kinds {
    Kinds::new().register(
        "echo",
        Kind::new("You are a test agent.")
            .sandboxed(
                "shell",
                "Run a command",
                &json!({ "type": "object" }),
                Tier::Workspace,
            )
            .budget(Budget::new(10_000, 10))
            .grain(brisk_idle()),
    )
}

/// Let idle hibernation and stragglers flush before quiescence checks.
async fn flush(system: &SimSystem) {
    system.clock().sleep(Duration::from_secs(10)).await;
}

#[test]
fn a_final_message_completes_the_run() {
    let model = Arc::new(ScriptedModel::steps(vec![Ok(final_message("the answer"))]));
    let sandboxes = Arc::new(ScriptedSandboxes::echo());
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let workload = Scenario::new(
        "happy-path",
        echo_kind(),
        model,
        sandboxes,
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let session = harness.session("echo", SessionId::new("s-1")).unwrap();
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "the answer");
                assert_eq!(outcome.tokens(), 120);
                *sink.lock().unwrap() = tail_records(&session).await;
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 7).expect("invariants hold");

    // The journal is the session (§2.1): audit the record order (§6.4).
    assert_eq!(
        record_kinds(&records.lock().unwrap()),
        vec!["created", "turn", "started", "model", "ended"],
        "write-ahead order (§6.4)"
    );
}

#[test]
fn the_tool_loop_journals_intent_before_effect() {
    let model = Arc::new(ScriptedModel::steps(vec![
        Ok(tool_call("c1", "shell", json!({ "cmd": "ls" }))),
        Ok(final_message("done")),
    ]));
    let sandboxes = Arc::new(ScriptedSandboxes::echo());
    let stats = sandboxes.stats.clone();
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let workload = Scenario::new(
        "tool-loop",
        echo_kind(),
        model,
        sandboxes,
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let session = harness.session("echo", SessionId::new("s-2")).unwrap();
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "use the tool"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "done");
                *sink.lock().unwrap() = tail_records(&session).await;
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 11).expect("invariants hold");

    assert_eq!(
        record_kinds(&records.lock().unwrap()),
        vec![
            "created", "turn", "started", "model", "tool", "model", "ended"
        ],
        "intent precedes effect; outcome precedes the next step (§6.4)"
    );
    // The workspace executed exactly the declared call (§5.2) and was released by
    // the idle stop (§7.2, H8).
    assert_eq!(stats.calls().len(), 1);
    assert_eq!(stats.opened(), 1);
    assert_eq!(stats.released(), 1);
}

#[test]
fn resubmitting_a_turn_never_starts_a_second_run() {
    let model = Arc::new(ScriptedModel::steps(vec![Ok(final_message("once"))]));
    let sandboxes = Arc::new(ScriptedSandboxes::echo());
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let workload = Scenario::new(
        "idempotent-submit",
        echo_kind(),
        model,
        sandboxes,
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let session = harness.session("echo", SessionId::new("s-3")).unwrap();
                let turn = Turn::new(TurnId::new("t-1"), "go");
                // Concurrent duplicate submissions attach to one run (§7.4).
                let (a, b) =
                    futures::join!(session.prompt(turn.clone()), session.prompt(turn.clone()));
                assert_eq!(a.expect("call").expect("run").text(), "once");
                assert_eq!(b.expect("call").expect("run").text(), "once");
                // A later re-submission returns the recorded outcome (§7.4).
                let again = session
                    .prompt(turn.clone())
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(again.text(), "once");
                // Same TurnId, different content: a caller bug, rejected without
                // journaling (§7.4).
                let mismatch = session
                    .prompt(Turn::new(TurnId::new("t-1"), "different"))
                    .await;
                assert!(matches!(
                    mismatch,
                    Err(GrainError::Call(CallError::System(_)))
                ));
                *sink.lock().unwrap() = tail_records(&session).await;
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 13).expect("invariants hold");

    let turns = records
        .lock()
        .unwrap()
        .iter()
        .filter(|r| matches!(r.body, RecordBody::TurnSubmitted { .. }))
        .count();
    assert_eq!(turns, 1, "one journaled turn under duplication (H7)");
}

#[test]
fn a_resubmission_that_changes_the_budget_is_a_conflict() {
    // §7.4: the turn-equality check covers the literal budget field, so a
    // re-submitted TurnId with a different budget is a caller bug, not a
    // silent attach — and `None` is not `Some(default)`, so the rejection
    // never depends on the kind's current default.
    let model = Arc::new(ScriptedModel::new(|_| Ok(final_message("once"))));
    let sandboxes = Arc::new(ScriptedSandboxes::echo());
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let workload = Scenario::new(
        "budget-conflict",
        echo_kind(),
        model,
        sandboxes,
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let session = harness.session("echo", SessionId::new("s-6")).unwrap();
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "once");
                // Same content, explicit budget equal to the kind's default:
                // the literal differs (None vs Some), so it conflicts.
                let some_default = session
                    .prompt(
                        Turn::new(TurnId::new("t-1"), "go").with_budget(Budget::new(10_000, 10)),
                    )
                    .await;
                assert!(
                    matches!(some_default, Err(GrainError::Call(CallError::System(_)))),
                    "Some(default) is not None (§7.4)"
                );
                // An identical literal budget re-attaches and returns the
                // recorded outcome; a changed one conflicts.
                let budgeted =
                    Turn::new(TurnId::new("t-2"), "go").with_budget(Budget::new(9_000, 9));
                let first = session
                    .prompt(budgeted.clone())
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(first.text(), "once");
                let again = session.prompt(budgeted).await.expect("call").expect("run");
                assert_eq!(again.text(), "once");
                let changed = session
                    .prompt(Turn::new(TurnId::new("t-2"), "go").with_budget(Budget::new(8_000, 9)))
                    .await;
                assert!(matches!(
                    changed,
                    Err(GrainError::Call(CallError::System(_)))
                ));
                *sink.lock().unwrap() = tail_records(&session).await;
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 29).expect("invariants hold");

    let turns = records
        .lock()
        .unwrap()
        .iter()
        .filter(|r| matches!(r.body, RecordBody::TurnSubmitted { .. }))
        .count();
    assert_eq!(turns, 2, "the conflicting re-submissions journaled nothing");
}

#[test]
fn turns_are_serialized_by_the_journal() {
    let model = Arc::new(ScriptedModel::new(|req| {
        let user_turns = req
            .transcript
            .iter()
            .filter(|e| matches!(e, harness::Entry::User(_)))
            .count();
        Ok(final_message(if user_turns == 1 {
            "first"
        } else {
            "second"
        }))
    }));
    let sandboxes = Arc::new(ScriptedSandboxes::echo());
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let workload = Scenario::new(
        "serialized-turns",
        echo_kind(),
        model,
        sandboxes,
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let session = harness.session("echo", SessionId::new("s-4")).unwrap();
                let (a, b) = futures::join!(
                    session.prompt(Turn::new(TurnId::new("t-1"), "one")),
                    session.prompt(Turn::new(TurnId::new("t-2"), "two"))
                );
                let a = a.expect("call").expect("run");
                let b = b.expect("call").expect("run");
                // Both ran; the journal's total order serialized them (§2.2).
                assert_eq!(a.text(), "first");
                assert_eq!(b.text(), "second");
                *sink.lock().unwrap() = tail_records(&session).await;
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 17).expect("invariants hold");

    // Acceptance and start are distinct facts (§7.3): both `TurnSubmitted`s
    // commit as they arrive (this seed acks both before either run begins),
    // while the runs themselves stay serialized — the second `TurnStarted`
    // commits only after the first's terminal record (§3.1).
    let records = records.lock().unwrap();
    assert_eq!(
        record_kinds(&records),
        vec![
            "created", "turn", "turn", "started", "model", "ended", "started", "model", "ended"
        ],
        "acceptance interleaves; starts serialize (§3.1)"
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
        vec!["t-1", "t-2"],
        "queue order is acceptance order"
    );
}

#[test]
fn an_unknown_tool_is_a_transcript_value_not_a_run_failure() {
    let model = Arc::new(ScriptedModel::steps(vec![
        Ok(tool_call("c1", "no_such_tool", json!({}))),
        Ok(tool_call("c2", "shell", json!("not an object"))),
        Ok(final_message("recovered")),
    ]));
    let sandboxes = Arc::new(ScriptedSandboxes::echo());
    let stats = sandboxes.stats.clone();
    let workload = Scenario::new(
        "unknown-tool",
        echo_kind(),
        model,
        sandboxes,
        move |harness, system| {
            Box::pin(async move {
                let session = harness.session("echo", SessionId::new("s-5")).unwrap();
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run never fails because a tool misbehaved (§5.4)");
                assert_eq!(outcome.text(), "recovered");
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 19).expect("invariants hold");

    // Both synthesized outcomes are journaled errors; nothing was executed (§5.4).
    assert_eq!(stats.calls().len(), 0);
    assert_eq!(stats.opened(), 0);
}
