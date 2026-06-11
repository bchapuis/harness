//! Sandbox conformance (harness spec §5.3, §5.5; invariant H8): lazy open,
//! provisioning failure as a transcript value, per-tool timeouts, and
//! environment loss surfacing as a journaled `WorkspaceReset` — never as
//! silent corruption.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_core::BoxFuture;
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
use harness::OnDangling;
use harness::RecordBody;
use harness::SessionId;
use harness::ToolDecl;
use harness::ToolError;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::boxed;
use support::final_message;
use support::harness_invariants;
use support::tool_call;

fn test_config() -> HarnessConfig {
    HarnessConfig {
        idle_timeout: Duration::from_secs(2),
        tick_interval: Duration::from_millis(500),
        submit_deadline: Duration::from_secs(300),
        ..HarnessConfig::default()
    }
}

struct SandboxWorkload<F> {
    name: &'static str,
    kinds: fn() -> Kinds,
    model: ScriptedModel,
    make_sandboxes: fn(&SimSystem) -> ScriptedSandboxes,
    body: F,
    journal: Mutex<Option<InMemoryJournal>>,
    sandboxes: Mutex<Option<ScriptedSandboxes>>,
}

impl<F> SandboxWorkload<F> {
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

impl<F> Workload for SandboxWorkload<F>
where
    F: Fn(Harness<SimSystem>, SimSystem, ScriptedSandboxes) -> BoxFuture<'static, ()>
        + Send
        + Sync
        + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let journal = InMemoryJournal::new();
        let sandboxes = (self.make_sandboxes)(&system);
        *self.journal.lock().expect("journal") = Some(journal.clone());
        *self.sandboxes.lock().expect("sandboxes") = Some(sandboxes.clone());
        let harness = Harness::with_config(
            system.clone(),
            (self.kinds)(),
            Arc::new(journal),
            Arc::new(self.model.clone()),
            Arc::new(sandboxes.clone()),
            test_config(),
        );
        (self.body)(harness, system, sandboxes)
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        harness_invariants()
    }
}

async fn flush(system: &SimSystem) {
    system.clock().sleep(Duration::from_secs(10)).await;
}

fn shell_kind() -> Kinds {
    Kinds::new().register(
        "worker",
        Kind::new("worker")
            .sandboxed("shell", "run", &json!({"type": "object"}))
            .budget(Budget::new(10_000, 10)),
    )
}

#[test]
fn a_failed_open_fails_the_calls_not_the_run() {
    // Provisioning recovers when the model reacts to the first failure: the
    // script flips the switch on seeing the failed tool result — a
    // deterministic function of the request plus the scripted environment.
    let recovering = Arc::new(Mutex::new(None::<ScriptedSandboxes>));
    let switch = Arc::clone(&recovering);
    let workload = SandboxWorkload {
        name: "sandbox-open-failure",
        kinds: shell_kind,
        model: ScriptedModel::new(move |req| {
            let step = req
                .transcript
                .iter()
                .filter(|e| matches!(e, harness::Entry::Assistant { .. }))
                .count();
            match step {
                0 => Ok(tool_call("c1", "shell", json!({}))),
                1 => {
                    // The model saw the provisioning failure (§5.4) and
                    // retries; the environment is back.
                    if let Some(sandboxes) = switch.lock().expect("switch").as_ref() {
                        sandboxes.set_fail_open(false);
                    }
                    Ok(tool_call("c2", "shell", json!({})))
                }
                _ => Ok(final_message("done")),
            }
        }),
        make_sandboxes: |_| ScriptedSandboxes::echo(),
        body: move |harness: Harness<SimSystem>,
                    system: SimSystem,
                    sandboxes: ScriptedSandboxes| {
            // Make the per-run environment visible to the model script.
            *recovering.lock().expect("switch") = Some(sandboxes.clone());
            boxed(async move {
                sandboxes.set_fail_open(true);
                let session = harness.session("worker", SessionId::new("s-1"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "done");
                flush(&system).await;
            })
        },
        journal: Mutex::new(None),
        sandboxes: Mutex::new(None),
    };
    run_seed(&workload, 59).expect("invariants hold");

    let records = workload.journal().records(&SessionId::new("s-1"));
    let outcomes: Vec<bool> = records
        .iter()
        .filter_map(|r| match &r.body {
            RecordBody::ToolOutcome { outcome, .. } => Some(outcome.is_ok()),
            _ => None,
        })
        .collect();
    assert_eq!(
        outcomes,
        vec![false, true],
        "the provisioning failure is the first call's outcome (§5.4)"
    );
    assert_eq!(
        workload.sandboxes().stats.opened(),
        1,
        "the retry opened lazily"
    );
}

#[test]
fn a_lost_environment_surfaces_as_a_journaled_workspace_reset() {
    let workload = SandboxWorkload {
        name: "environment-loss",
        kinds: shell_kind,
        model: ScriptedModel::steps(vec![
            Ok(tool_call("c1", "shell", json!({"lose": true}))),
            Ok(tool_call("c2", "shell", json!({}))),
            Ok(final_message("done")),
        ]),
        make_sandboxes: |_| {
            ScriptedSandboxes::new(|_, input| {
                if input.get("lose").is_some() {
                    Err(ToolError::EnvironmentLost("scripted loss".to_string()))
                } else {
                    Ok(json!("ok"))
                }
            })
        },
        body: |harness: Harness<SimSystem>, system: SimSystem, _| {
            boxed(async move {
                let session = harness.session("worker", SessionId::new("s-2"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "done");
                flush(&system).await;
            })
        },
        journal: Mutex::new(None),
        sandboxes: Mutex::new(None),
    };
    run_seed(&workload, 61).expect("invariants hold");

    // The loss enters the record before the model acts on state that is gone
    // (§5.5): tool error → WorkspaceReset → next model call.
    let records = workload.journal().records(&SessionId::new("s-2"));
    let kinds: Vec<&str> = records
        .iter()
        .map(|r| match &r.body {
            RecordBody::SessionCreated { .. } => "created",
            RecordBody::TurnSubmitted { .. } => "turn",
            RecordBody::ModelResponse { .. } => "model",
            RecordBody::ToolOutcome { .. } => "tool",
            RecordBody::ChildRun { .. } => "child",
            RecordBody::WorkspaceReset => "reset",
            RecordBody::RunEnded { .. } => "ended",
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            "created", "turn", "model", "tool", "reset", "model", "tool", "model", "ended"
        ],
    );
    // The loss tore down one environment; the next call opened a fresh one
    // (§5.5) — and both were released exactly once (H8).
    let stats = workload.sandboxes().stats;
    assert_eq!(stats.opened(), 2);
    assert_eq!(stats.released(), 2);
}

#[test]
fn a_slow_tool_is_bounded_by_its_declared_timeout() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let workload = SandboxWorkload {
        name: "tool-timeout",
        kinds: || {
            Kinds::new().register(
                "worker",
                Kind::new("worker")
                    .tool(ToolDecl {
                        name: "slow".to_string(),
                        description: "slow tool".to_string(),
                        input_schema: json!({"type": "object"}),
                        on_dangling: OnDangling::Interrupt,
                        timeout: Some(Duration::from_secs(1)),
                    })
                    .budget(Budget::new(10_000, 10)),
            )
        },
        model: ScriptedModel::steps(vec![
            Ok(tool_call("c1", "slow", json!({}))),
            Ok(final_message("done")),
        ]),
        make_sandboxes: |system| {
            CALLS.store(0, Ordering::SeqCst);
            ScriptedSandboxes::new(|_, _| {
                CALLS.fetch_add(1, Ordering::SeqCst);
                Ok(json!("eventually"))
            })
            // Every call takes 60s of logical time against a 1s bound.
            .with_delay(system.clock().clone(), Duration::from_secs(60))
        },
        body: |harness: Harness<SimSystem>, system: SimSystem, _| {
            boxed(async move {
                let session = harness.session("worker", SessionId::new("s-3"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "done");
                flush(&system).await;
            })
        },
        journal: Mutex::new(None),
        sandboxes: Mutex::new(None),
    };
    run_seed(&workload, 67).expect("invariants hold");

    // The timeout is the journaled outcome (§5.3 item 3); the straggling
    // real result, arriving at 60s, is a duplicate and is discarded.
    let records = workload.journal().records(&SessionId::new("s-3"));
    let tool_outcomes: Vec<_> = records
        .iter()
        .filter_map(|r| match &r.body {
            RecordBody::ToolOutcome { outcome, .. } => Some(outcome.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(tool_outcomes, vec![Err(ToolError::Timeout)]);
}
