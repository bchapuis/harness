//! Run-loop conformance (harness spec §3, §5.4, §6.4, §7.4): the happy path,
//! the tool loop, write-ahead order, idempotent submission (H7), and
//! serialized turns — each asserted against the journal, the single source of
//! truth (§10.1), under the continuous harness checkers (§11).

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::BoxFuture;
use actor_core::CallError;
use actor_core::Clock;
use actor_simulation::Invariant;
use actor_simulation::SimSystem;
use actor_simulation::Workload;
use actor_simulation::run_seed;
use harness::Budget;
use harness::Harness;
use harness::HarnessConfig;
use harness::InMemoryJournal;
use harness::Kind;
use harness::Kinds;
use harness::RecordBody;
use harness::SessionId;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::final_message;
use support::harness_invariants;
use support::tool_call;

/// Short timeouts so a single test run exercises idle deactivation (§7.2).
fn test_config() -> HarnessConfig {
    HarnessConfig {
        idle_timeout: Duration::from_secs(2),
        tick_interval: Duration::from_millis(500),
        submit_deadline: Duration::from_secs(120),
        ..HarnessConfig::default()
    }
}

fn echo_kind() -> Kinds {
    Kinds::new().register(
        "echo",
        Kind::new("You are a test agent.")
            .sandboxed("shell", "Run a command", &json!({"type": "object"}))
            .budget(Budget::new(10_000, 10)),
    )
}

/// A workload shell: builds the harness inside `run` (so every run is
/// self-contained and seed-deterministic) and stashes the journal for the
/// test body's audit at quiescence.
struct HarnessWorkload<F> {
    name: &'static str,
    model: ScriptedModel,
    body: F,
    journal: Mutex<Option<InMemoryJournal>>,
    sandboxes: Mutex<Option<ScriptedSandboxes>>,
}

impl<F> HarnessWorkload<F>
where
    F: Fn(Harness<SimSystem>, SimSystem) -> BoxFuture<'static, ()> + Send + Sync + 'static,
{
    fn new(name: &'static str, model: ScriptedModel, body: F) -> Self {
        HarnessWorkload {
            name,
            model,
            body,
            journal: Mutex::new(None),
            sandboxes: Mutex::new(None),
        }
    }

    fn journal(&self) -> InMemoryJournal {
        self.journal
            .lock()
            .expect("journal")
            .clone()
            .expect("run first")
    }

    fn sandboxes(&self) -> ScriptedSandboxes {
        self.sandboxes
            .lock()
            .expect("sandboxes")
            .clone()
            .expect("run first")
    }
}

impl<F> Workload for HarnessWorkload<F>
where
    F: Fn(Harness<SimSystem>, SimSystem) -> BoxFuture<'static, ()> + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let journal = InMemoryJournal::new();
        let sandboxes = ScriptedSandboxes::echo();
        *self.journal.lock().expect("journal") = Some(journal.clone());
        *self.sandboxes.lock().expect("sandboxes") = Some(sandboxes.clone());
        let harness = Harness::with_config(
            system.clone(),
            echo_kind(),
            Arc::new(journal),
            Arc::new(self.model.clone()),
            Arc::new(sandboxes),
            test_config(),
        );
        (self.body)(harness, system)
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        harness_invariants()
    }
}

/// Let idle deactivation and stragglers flush before quiescence checks.
async fn flush(system: &SimSystem) {
    system.clock().sleep(Duration::from_secs(10)).await;
}

#[test]
fn a_final_message_completes_the_run() {
    let workload = HarnessWorkload::new(
        "happy-path",
        ScriptedModel::steps(vec![Ok(final_message("the answer"))]),
        |harness, system| {
            Box::pin(async move {
                let session = harness.session("echo", SessionId::new("s-1"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "the answer");
                assert_eq!(outcome.tokens, 120);
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 7).expect("invariants hold");

    // The journal is the session (§2.1): audit the record order (§6.4).
    let records = workload.journal().records(&SessionId::new("s-1"));
    let kinds: Vec<&str> = records.iter().map(|r| record_kind(&r.body)).collect();
    assert_eq!(
        kinds,
        vec!["created", "turn", "model", "ended"],
        "write-ahead order (§6.4)"
    );
}

#[test]
fn the_tool_loop_journals_intent_before_effect() {
    let workload = HarnessWorkload::new(
        "tool-loop",
        ScriptedModel::steps(vec![
            Ok(tool_call("c1", "shell", json!({"cmd": "ls"}))),
            Ok(final_message("done")),
        ]),
        |harness, system| {
            Box::pin(async move {
                let session = harness.session("echo", SessionId::new("s-2"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "use the tool"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "done");
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 11).expect("invariants hold");

    let records = workload.journal().records(&SessionId::new("s-2"));
    let kinds: Vec<&str> = records.iter().map(|r| record_kind(&r.body)).collect();
    assert_eq!(
        kinds,
        vec!["created", "turn", "model", "tool", "model", "ended"],
        "intent precedes effect; outcome precedes the next step (§6.4)"
    );
    // The workspace executed exactly the declared call (§5.2) and was
    // released by the idle stop (§7.2, H8).
    let sandboxes = workload.sandboxes();
    assert_eq!(sandboxes.stats.calls().len(), 1);
    assert_eq!(sandboxes.stats.opened(), 1);
    assert_eq!(sandboxes.stats.released(), 1);
}

#[test]
fn resubmitting_a_turn_never_starts_a_second_run() {
    let workload = HarnessWorkload::new(
        "idempotent-submit",
        ScriptedModel::steps(vec![Ok(final_message("once"))]),
        |harness, system| {
            Box::pin(async move {
                let session = harness.session("echo", SessionId::new("s-3"));
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
                // Same TurnId, different content: a caller bug, rejected with
                // CallError::System and journaling nothing (§7.4).
                let mismatch = session
                    .prompt(Turn::new(TurnId::new("t-1"), "different"))
                    .await;
                assert!(matches!(mismatch, Err(CallError::System(_))));
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 13).expect("invariants hold");

    let records = workload.journal().records(&SessionId::new("s-3"));
    let turns = records
        .iter()
        .filter(|r| matches!(r.body, RecordBody::TurnSubmitted { .. }))
        .count();
    assert_eq!(turns, 1, "one journaled turn under duplication (H7)");
}

#[test]
fn turns_are_serialized_by_the_journal() {
    let workload = HarnessWorkload::new(
        "serialized-turns",
        // Both runs complete in one step each.
        ScriptedModel::new(|req| {
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
        }),
        |harness, system| {
            Box::pin(async move {
                let session = harness.session("echo", SessionId::new("s-4"));
                let (a, b) = futures::join!(
                    session.prompt(Turn::new(TurnId::new("t-1"), "one")),
                    session.prompt(Turn::new(TurnId::new("t-2"), "two"))
                );
                let a = a.expect("call").expect("run");
                let b = b.expect("call").expect("run");
                // Both ran; the journal's total order serialized them (§2.2).
                assert_eq!(a.text(), "first");
                assert_eq!(b.text(), "second");
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 17).expect("invariants hold");

    let records = workload.journal().records(&SessionId::new("s-4"));
    let kinds: Vec<&str> = records.iter().map(|r| record_kind(&r.body)).collect();
    assert_eq!(
        kinds,
        vec![
            "created", "turn", "model", "ended", "turn", "model", "ended"
        ],
        "the second run starts only after the first's terminal record (§3.1)"
    );
}

#[test]
fn an_unknown_tool_is_a_transcript_value_not_a_run_failure() {
    let workload = HarnessWorkload::new(
        "unknown-tool",
        ScriptedModel::steps(vec![
            Ok(tool_call("c1", "no_such_tool", json!({}))),
            Ok(tool_call("c2", "shell", json!("not an object"))),
            Ok(final_message("recovered")),
        ]),
        |harness, system| {
            Box::pin(async move {
                let session = harness.session("echo", SessionId::new("s-5"));
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

    // Both synthesized outcomes are journaled errors; nothing was executed
    // (§5.4: the registry-as-allowlist).
    let sandboxes = workload.sandboxes();
    assert_eq!(sandboxes.stats.calls().len(), 0);
    assert_eq!(sandboxes.stats.opened(), 0);
}

fn record_kind(body: &RecordBody) -> &'static str {
    match body {
        RecordBody::SessionCreated { .. } => "created",
        RecordBody::TurnSubmitted { .. } => "turn",
        RecordBody::ModelResponse { .. } => "model",
        RecordBody::ToolOutcome { .. } => "tool",
        RecordBody::ChildRun { .. } => "child",
        RecordBody::WorkspaceReset => "reset",
        RecordBody::RunEnded { .. } => "ended",
    }
}
