//! Budget and delegation conformance (harness spec §8, §9.1; invariant H4):
//! pre-call enforcement, carve-outs, the compositional tree bound, and child
//! failure surfacing as a tool outcome — audited against the journals, where
//! spend is defined (§9.1.4).

mod support;

use std::sync::Arc;
use std::sync::Mutex;
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
use harness::ModelError;
use harness::RecordBody;
use harness::RunError;
use harness::SessionId;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::ScriptedModel;
use support::ScriptedSandboxes;
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

struct BudgetWorkload<F> {
    name: &'static str,
    kinds: fn() -> Kinds,
    model: ScriptedModel,
    body: F,
    journal: Mutex<Option<InMemoryJournal>>,
}

impl<F> BudgetWorkload<F> {
    fn journal(&self) -> InMemoryJournal {
        self.journal
            .lock()
            .expect("journal")
            .clone()
            .expect("run first")
    }
}

impl<F> Workload for BudgetWorkload<F>
where
    F: Fn(Harness<SimSystem>, SimSystem) -> BoxFuture<'static, ()> + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let journal = InMemoryJournal::new();
        *self.journal.lock().expect("journal") = Some(journal.clone());
        let harness = Harness::with_config(
            system.clone(),
            (self.kinds)(),
            Arc::new(journal),
            Arc::new(self.model.clone()),
            Arc::new(ScriptedSandboxes::echo()),
            test_config(),
        );
        (self.body)(harness, system)
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        harness_invariants()
    }
}

async fn flush(system: &SimSystem) {
    system.clock().sleep(Duration::from_secs(10)).await;
}

/// Sum a session's journaled spend: own model usage plus `ChildRun`
/// carve-outs (§9.1).
fn journaled_spend(journal: &InMemoryJournal, session: &SessionId) -> (u64, u32) {
    let mut tokens = 0;
    let mut steps = 0;
    for record in journal.records(session) {
        match record.body {
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

#[test]
fn an_exhausted_budget_ends_the_run_with_no_further_calls() {
    // The model loops tool calls forever (§12.2's pathological loop); only
    // the budget stops it.
    let workload = BudgetWorkload {
        name: "budget-exhaustion",
        kinds: || {
            Kinds::new().register(
                "looper",
                Kind::new("loop forever")
                    .sandboxed("shell", "run", &json!({"type": "object"}))
                    .budget(Budget::new(100_000, 3)),
            )
        },
        model: ScriptedModel::new(|req| {
            let step = req
                .transcript
                .iter()
                .filter(|e| matches!(e, harness::Entry::Assistant { .. }))
                .count();
            Ok(tool_call(&format!("c{step}"), "shell", json!({"n": step})))
        }),
        body: |harness: Harness<SimSystem>, system: SimSystem| {
            support::boxed(async move {
                let session = harness.session("looper", SessionId::new("s-loop"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call");
                assert_eq!(outcome, Err(RunError::BudgetExhausted));
                flush(&system).await;
            })
        },
        journal: Mutex::new(None),
    };
    run_seed(&workload, 23).expect("invariants hold");

    let journal = workload.journal();
    let session = SessionId::new("s-loop");
    let (_, steps) = journaled_spend(&journal, &session);
    assert_eq!(steps, 3, "exactly the budgeted steps were issued (H4)");
    let last = journal.records(&session).pop().expect("records");
    assert!(matches!(
        last.body,
        RecordBody::RunEnded {
            outcome: Err(RunError::BudgetExhausted),
            ..
        }
    ));
}

#[test]
fn token_exhaustion_is_enforced_before_the_call() {
    // Each step reports 130 tokens against a 250-token budget: the third
    // call is never issued (§9.1 item 2).
    let workload = BudgetWorkload {
        name: "token-exhaustion",
        kinds: || {
            Kinds::new().register(
                "looper",
                Kind::new("loop forever")
                    .sandboxed("shell", "run", &json!({"type": "object"}))
                    .budget(Budget::new(250, 100)),
            )
        },
        model: ScriptedModel::new(|req| {
            let step = req
                .transcript
                .iter()
                .filter(|e| matches!(e, harness::Entry::Assistant { .. }))
                .count();
            Ok(tool_call(&format!("c{step}"), "shell", json!({})))
        }),
        body: |harness: Harness<SimSystem>, system: SimSystem| {
            support::boxed(async move {
                let session = harness.session("looper", SessionId::new("s-tok"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call");
                assert_eq!(outcome, Err(RunError::BudgetExhausted));
                flush(&system).await;
            })
        },
        journal: Mutex::new(None),
    };
    run_seed(&workload, 29).expect("invariants hold");

    let journal = workload.journal();
    let (tokens, steps) = journaled_spend(&journal, &SessionId::new("s-tok"));
    assert_eq!(steps, 2, "the third call was never issued (§9.1 item 2)");
    // Overshoot is bounded by one call (§9.1 item 2): 130 over at worst.
    assert!(
        tokens <= 250 + 130,
        "journaled spend {tokens} within the bound"
    );
}

/// Parent/child kinds for the delegation tests: one scripted model serves
/// both, branching on the system prompt (a pure function of the request).
fn tree_kinds() -> Kinds {
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
                json!({"kind": "child", "prompt": "sub-task", "budget": {"tokens": 500, "steps": 3}}),
            ))
        } else {
            Ok(final_message("parent-answer"))
        }
    })
}

#[test]
fn a_delegation_is_a_full_session_with_a_carved_budget() {
    let workload = BudgetWorkload {
        name: "delegation-tree",
        kinds: tree_kinds,
        model: tree_model(false),
        body: |harness: Harness<SimSystem>, system: SimSystem| {
            support::boxed(async move {
                let session = harness.session("parent", SessionId::new("root-1"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "do the task"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "parent-answer");
                flush(&system).await;
            })
        },
        journal: Mutex::new(None),
    };
    run_seed(&workload, 31).expect("invariants hold");

    let journal = workload.journal();
    let root = SessionId::new("root-1");

    // The parent journaled the delegation's intent with the carved budget
    // (§8.1 step 1), then the child's completion as the tool outcome.
    let parent_records = journal.records(&root);
    let (child_session, carved) = parent_records
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
    let child_outcome = parent_records.iter().find_map(|r| match &r.body {
        RecordBody::ToolOutcome { outcome: Ok(v), .. } => Some(v.clone()),
        _ => None,
    });
    assert_eq!(
        child_outcome,
        Some(serde_json::Value::String("child-answer".to_string()))
    );

    // The child is a full session (§8.1): journaled, with its lineage and
    // root recorded (§10.3).
    let child_records = journal.records(&child_session);
    let (parent_lineage, child_root) = child_records
        .iter()
        .find_map(|r| match &r.body {
            RecordBody::SessionCreated { parent, root, .. } => Some((parent.clone(), root.clone())),
            _ => None,
        })
        .expect("child SessionCreated");
    assert_eq!(parent_lineage.expect("lineage").session, root);
    assert_eq!(child_root, root, "the root names the tree (§10.3)");

    // The compositional bound (H4): own spend + carve-outs ≤ budget, and the
    // child within its slice.
    let (parent_tokens, parent_steps) = journaled_spend(&journal, &root);
    assert!(parent_tokens <= 10_000 && parent_steps <= 10);
    let (child_tokens, child_steps) = journaled_spend(&journal, &child_session);
    assert!(
        child_tokens <= 500 && child_steps <= 3,
        "the child enforces its slice locally (§9.1 item 3)"
    );
}

#[test]
fn a_failing_child_is_a_tool_outcome_not_a_parent_failure() {
    let workload = BudgetWorkload {
        name: "delegation-failure",
        kinds: tree_kinds,
        model: tree_model(true),
        body: |harness: Harness<SimSystem>, system: SimSystem| {
            support::boxed(async move {
                let session = harness.session("parent", SessionId::new("root-2"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "do the task"))
                    .await
                    .expect("call")
                    .expect("the parent's run never fails because a child did (§8.2)");
                assert_eq!(outcome.text(), "parent-answer");
                flush(&system).await;
            })
        },
        journal: Mutex::new(None),
    };
    run_seed(&workload, 37).expect("invariants hold");

    // The child's RunError reached the parent's transcript as a value (§5.4).
    let records = workload.journal().records(&SessionId::new("root-2"));
    let failed = records.iter().any(|r| {
        matches!(
            &r.body,
            RecordBody::ToolOutcome {
                outcome: Err(harness::ToolError::Delegation(RunError::Model(_))),
                ..
            }
        )
    });
    assert!(failed, "the child's terminal error is the tool outcome");
}

#[test]
fn delegating_outside_the_allowlist_is_synthesized_not_executed() {
    let workload = BudgetWorkload {
        name: "delegation-allowlist",
        kinds: tree_kinds,
        model: ScriptedModel::new(|req| {
            if req.system_prompt == "child agent" {
                return Ok(final_message("child-answer"));
            }
            let step = req
                .transcript
                .iter()
                .filter(|e| matches!(e, harness::Entry::Assistant { .. }))
                .count();
            if step == 0 {
                // "parent" is not in its own allowlist: a locked-down kind
                // cannot escalate (§8.1).
                Ok(tool_call(
                    "d1",
                    "delegate",
                    json!({"kind": "parent", "prompt": "escalate"}),
                ))
            } else {
                Ok(final_message("recovered"))
            }
        }),
        body: |harness: Harness<SimSystem>, system: SimSystem| {
            support::boxed(async move {
                let session = harness.session("parent", SessionId::new("root-3"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "recovered");
                flush(&system).await;
            })
        },
        journal: Mutex::new(None),
    };
    run_seed(&workload, 41).expect("invariants hold");

    let journal = workload.journal();
    let records = journal.records(&SessionId::new("root-3"));
    assert!(
        records
            .iter()
            .all(|r| !matches!(r.body, RecordBody::ChildRun { .. })),
        "no child run was journaled"
    );
    assert_eq!(
        journal.session_ids(),
        vec![SessionId::new("root-3")],
        "no child session exists"
    );
}
