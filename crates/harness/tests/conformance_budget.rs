//! Budget and delegation conformance (harness spec §8, §9.1; invariant H4):
//! pre-call enforcement, carve-outs, the compositional tree bound, and child
//! failure surfacing as a tool outcome — audited against the journals, where
//! spend is defined (§9.1.4).

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
use harness::ModelError;
use harness::Record;
use harness::RecordBody;
use harness::RunError;
use harness::SessionId;
use harness::Tier;
use harness::ToolError;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::Scenario;
use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::brisk_idle;
use support::final_message;
use support::tail_records;
use support::tool_call;

async fn flush(system: &SimSystem) {
    system.clock().sleep(Duration::from_secs(10)).await;
}

/// Sum journaled spend from a session's records: own model usage plus `ChildRun`
/// carve-outs (§9.1).
fn spend_of(records: &[Record]) -> (u64, u32) {
    let mut tokens = 0;
    let mut steps = 0;
    for record in records {
        match &record.body {
            RecordBody::ModelResponse { usage, .. } => {
                tokens += usage.total();
                steps += 1;
            }
            RecordBody::ChildRun { budget, .. } => {
                tokens += budget.tokens;
                steps += budget.steps;
            }
            _ => {}
        }
    }
    (tokens, steps)
}

/// Run a single-session budget scenario and return its journal.
fn run_single(
    name: &'static str,
    kinds: Kinds,
    model: ScriptedModel,
    session: &'static str,
    seed: u64,
) -> Vec<Record> {
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let workload = Scenario::new(
        name,
        kinds,
        Arc::new(model),
        Arc::new(ScriptedSandboxes::echo()),
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let session = harness.session("looper", SessionId::new(session));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call");
                assert_eq!(outcome, Err(RunError::BudgetExhausted));
                *sink.lock().unwrap() = tail_records(&session).await;
                flush(&system).await;
            })
        },
    )
    // These tests exercise exhaustion at the exact remainder with tiny budgets;
    // pin the floor to 0 so the default floor doesn't stop the run first.
    .with_config(HarnessConfig {
        budget_floor: 0,
        ..HarnessConfig::default()
    });
    run_seed(&workload, seed).expect("invariants hold");
    let records = records.lock().unwrap();
    records.clone()
}

#[test]
fn an_exhausted_budget_ends_the_run_with_no_further_calls() {
    // The model loops tool calls forever (§12.2's pathological loop); only the
    // budget stops it.
    let kinds = Kinds::new().register(
        "looper",
        Kind::new("loop forever")
            .sandboxed(
                "shell",
                "run",
                &json!({ "type": "object" }),
                Tier::Workspace,
            )
            .budget(Budget::new(100_000, 3))
            .grain(brisk_idle()),
    );
    let model = ScriptedModel::new(|req| {
        let step = req
            .transcript
            .iter()
            .filter(|e| matches!(e, harness::Entry::Assistant { .. }))
            .count();
        Ok(tool_call(
            &format!("c{step}"),
            "shell",
            json!({ "n": step }),
        ))
    });
    let records = run_single("budget-exhaustion", kinds, model, "s-loop", 23);

    let (_, steps) = spend_of(&records);
    assert_eq!(steps, 3, "exactly the budgeted steps were issued (H4)");
    assert!(matches!(
        records.last().expect("records").body,
        RecordBody::RunEnded {
            outcome: Err(RunError::BudgetExhausted),
            ..
        }
    ));
}

#[test]
fn token_exhaustion_is_enforced_before_the_call() {
    // Each step reports 130 tokens against a 250-token budget: the third call is
    // never issued (§9.1 item 2).
    let kinds = Kinds::new().register(
        "looper",
        Kind::new("loop forever")
            .sandboxed(
                "shell",
                "run",
                &json!({ "type": "object" }),
                Tier::Workspace,
            )
            .budget(Budget::new(250, 100))
            .grain(brisk_idle()),
    );
    let model = ScriptedModel::new(|req| {
        let step = req
            .transcript
            .iter()
            .filter(|e| matches!(e, harness::Entry::Assistant { .. }))
            .count();
        Ok(tool_call(&format!("c{step}"), "shell", json!({})))
    });
    let records = run_single("token-exhaustion", kinds, model, "s-tok", 29);

    let (tokens, steps) = spend_of(&records);
    assert_eq!(steps, 2, "the third call was never issued (§9.1 item 2)");
    // Overshoot is bounded by one call (§9.1 item 2): 130 over at worst.
    assert!(
        tokens <= 250 + 130,
        "journaled spend {tokens} within the bound"
    );
}

/// Parent/child kinds for the delegation tests.
fn tree_kinds() -> Kinds {
    Kinds::new()
        .register(
            "parent",
            Kind::new("parent agent")
                .delegates_to(&["child"])
                .budget(Budget::new(10_000, 10))
                .grain(brisk_idle()),
        )
        .register(
            "child",
            Kind::new("child agent")
                .budget(Budget::new(2_000, 4))
                .grain(brisk_idle()),
        )
}

fn tree_model(child_fails: bool) -> ScriptedModel {
    ScriptedModel::new(move |req| {
        if req.system_prompt == "child agent" {
            if child_fails {
                return Err(ModelError::Api("child model down".to_string()));
            }
            return Ok(final_message("child-answer"));
        }
        let step = req
            .transcript
            .iter()
            .filter(|e| matches!(e, harness::Entry::Assistant { .. }))
            .count();
        if step == 0 {
            Ok(tool_call(
                "d1",
                "delegate",
                json!({ "kind": "child", "prompt": "sub-task", "budget": { "tokens": 500, "steps": 3 } }),
            ))
        } else {
            Ok(final_message("parent-answer"))
        }
    })
}

/// Run a delegation tree and return (parent records, child records).
fn run_tree(
    name: &'static str,
    model: ScriptedModel,
    root: &'static str,
    seed: u64,
) -> (Vec<Record>, Vec<Record>) {
    let out: Arc<Mutex<(Vec<Record>, Vec<Record>)>> = Arc::default();
    let sink = Arc::clone(&out);
    let workload = Scenario::new(
        name,
        tree_kinds(),
        Arc::new(model),
        Arc::new(ScriptedSandboxes::echo()),
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let session = harness.session("parent", SessionId::new(root));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "do the task"))
                    .await
                    .expect("call")
                    .expect("the parent's run never fails because a child did (§8.2)");
                assert_eq!(outcome.text(), "parent-answer");
                let parent = tail_records(&session).await;
                let child = match parent.iter().find_map(|r| match &r.body {
                    RecordBody::ChildRun {
                        child_kind,
                        child_session,
                        ..
                    } => Some((child_kind.clone(), child_session.clone())),
                    _ => None,
                }) {
                    Some((kind, id)) => tail_records(&harness.session(kind.as_str(), id)).await,
                    None => Vec::new(),
                };
                *sink.lock().unwrap() = (parent, child);
                flush(&system).await;
            })
        },
    )
    // Small carved child budgets (500 tokens) test the carve mechanics, not the
    // floor; pin it to 0 so the default floor doesn't stop the child first.
    .with_config(HarnessConfig {
        budget_floor: 0,
        ..HarnessConfig::default()
    });
    run_seed(&workload, seed).expect("invariants hold");
    let guard = out.lock().unwrap();
    (guard.0.clone(), guard.1.clone())
}

#[test]
fn a_delegation_is_a_full_session_with_a_carved_budget() {
    let (parent, child) = run_tree("delegation-tree", tree_model(false), "root-1", 31);
    let root = SessionId::new("root-1");

    // The parent journaled the delegation's intent with the carved budget (§8.1
    // step 1), then the child's completion as the tool outcome.
    let (child_session, carved) = parent
        .iter()
        .find_map(|r| match &r.body {
            RecordBody::ChildRun {
                child_session,
                budget,
                ..
            } => Some((child_session.clone(), *budget)),
            _ => None,
        })
        .expect("a journaled ChildRun");
    assert_eq!(carved, Budget::new(500, 3), "the requested slice (§9.1)");
    let child_outcome = parent.iter().find_map(|r| match &r.body {
        RecordBody::ToolOutcome { outcome: Ok(v), .. } => Some(v.clone()),
        _ => None,
    });
    assert_eq!(
        child_outcome,
        Some(serde_json::Value::String("child-answer".to_string()))
    );

    // The child is a full session (§8.1): its lineage and root are recorded (§10.3).
    let (parent_lineage, child_root) = child
        .iter()
        .find_map(|r| match &r.body {
            RecordBody::SessionCreated { parent, root, .. } => Some((parent.clone(), root.clone())),
            _ => None,
        })
        .expect("child SessionCreated");
    assert_eq!(parent_lineage.expect("lineage").session, root);
    assert_eq!(child_root, root, "the root names the tree (§10.3)");
    assert!(
        !child.is_empty(),
        "the child {child_session} has a journal of its own"
    );

    // The compositional bound (H4): own spend + carve-outs ≤ budget, child within slice.
    let (parent_tokens, parent_steps) = spend_of(&parent);
    assert!(parent_tokens <= 10_000 && parent_steps <= 10);
    let (child_tokens, child_steps) = spend_of(&child);
    assert!(
        child_tokens <= 500 && child_steps <= 3,
        "the child enforces its slice locally (§9.1 item 3)"
    );
}

#[test]
fn a_failing_child_is_a_tool_outcome_not_a_parent_failure() {
    let (parent, _child) = run_tree("delegation-failure", tree_model(true), "root-2", 37);
    // The child's failure reached the parent's transcript as a tool value (§5.4).
    let failed = parent.iter().any(|r| {
        matches!(
            &r.body,
            RecordBody::ToolOutcome {
                outcome: Err(ToolError::Delegation(_)),
                ..
            }
        )
    });
    assert!(failed, "the child's terminal error is the tool outcome");
}

#[test]
fn delegating_outside_the_allowlist_is_synthesized_not_executed() {
    let model = ScriptedModel::new(|req| {
        if req.system_prompt == "child agent" {
            return Ok(final_message("child-answer"));
        }
        let step = req
            .transcript
            .iter()
            .filter(|e| matches!(e, harness::Entry::Assistant { .. }))
            .count();
        if step == 0 {
            // "parent" is not in its own allowlist: a locked-down kind cannot
            // escalate (§8.1).
            Ok(tool_call(
                "d1",
                "delegate",
                json!({ "kind": "parent", "prompt": "escalate" }),
            ))
        } else {
            Ok(final_message("recovered"))
        }
    });
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let workload = Scenario::new(
        "delegation-allowlist",
        tree_kinds(),
        Arc::new(model),
        Arc::new(ScriptedSandboxes::echo()),
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let session = harness.session("parent", SessionId::new("root-3"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "recovered");
                *sink.lock().unwrap() = tail_records(&session).await;
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 41).expect("invariants hold");

    let records = records.lock().unwrap();
    assert!(
        records
            .iter()
            .all(|r| !matches!(r.body, RecordBody::ChildRun { .. })),
        "no child run was journaled"
    );
    // The disallowed delegation is a synthesized tool failure, never executed (§5.4).
    assert!(
        records.iter().any(|r| matches!(
            &r.body,
            RecordBody::ToolOutcome {
                outcome: Err(ToolError::InvalidArguments(_)),
                ..
            }
        )),
        "the disallowed delegation is a synthesized InvalidArguments outcome"
    );
}
