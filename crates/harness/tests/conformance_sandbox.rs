//! Sandbox conformance (harness spec §5.3, §5.5, §5.6; invariant H8; sandbox
//! spec S4): lazy open, provisioning failure as a transcript value, per-tool
//! timeouts, environment loss surfacing as a journaled `WorkspaceReset` — never
//! as silent corruption — and the tier acquisition discipline: journaled,
//! monotone, capped, restarted by a reset.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::Clock;
use actor_simulation::SimSystem;
use actor_simulation::run_seed;
use harness::Budget;
use harness::CallId;
use harness::Kind;
use harness::KindId;
use harness::Kinds;
use harness::OnDangling;
use harness::Record;
use harness::RecordBody;
use harness::SandboxProfile;
use harness::SessionId;
use harness::Tier;
use harness::ToolCall;
use harness::ToolDecl;
use harness::ToolError;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::Scenario;
use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::audit_tier_acquisition;
use support::brisk_idle;
use support::final_message;
use support::record_kinds;
use support::tail_records;
use support::tool_call;

async fn flush(system: &SimSystem) {
    system.clock().sleep(Duration::from_secs(10)).await;
}

fn shell_kind() -> Kinds {
    Kinds::new().register(
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
    )
}

#[test]
fn a_failed_open_fails_the_calls_not_the_run() {
    // The provider's open fails until the model reacts to the first failure: the
    // script flips the switch on seeing the failed tool result.
    let sandboxes = ScriptedSandboxes::echo();
    sandboxes.set_fail_open(true);
    let stats = sandboxes.stats.clone();
    let for_model = sandboxes.clone();
    let model = ScriptedModel::new(move |req| {
        let step = req
            .transcript
            .iter()
            .filter(|e| matches!(e, harness::Entry::Assistant { .. }))
            .count();
        match step {
            0 => Ok(tool_call("c1", "shell", json!({}))),
            1 => {
                // The model saw the provisioning failure (§5.4) and retries; the
                // environment is back.
                for_model.set_fail_open(false);
                Ok(tool_call("c2", "shell", json!({})))
            }
            _ => Ok(final_message("done")),
        }
    });
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let workload = Scenario::new(
        "sandbox-open-failure",
        shell_kind(),
        Arc::new(model),
        Arc::new(sandboxes),
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let session = harness.session("worker", SessionId::new("s-1"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "done");
                *sink.lock().unwrap() = tail_records(&session).await;
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 59).expect("invariants hold");

    let outcomes: Vec<bool> = records
        .lock()
        .unwrap()
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
    assert_eq!(stats.opened(), 1, "the retry opened lazily");
}

#[test]
fn a_lost_environment_surfaces_as_a_journaled_workspace_reset() {
    let sandboxes = ScriptedSandboxes::new(|_, input| {
        if input.get("lose").is_some() {
            Err(ToolError::EnvironmentLost("scripted loss".to_string()))
        } else {
            Ok(json!("ok"))
        }
    });
    let stats = sandboxes.stats.clone();
    let model = ScriptedModel::steps(vec![
        Ok(tool_call("c1", "shell", json!({ "lose": true }))),
        Ok(tool_call("c2", "shell", json!({}))),
        Ok(final_message("done")),
    ]);
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let workload = Scenario::new(
        "environment-loss",
        shell_kind(),
        Arc::new(model),
        Arc::new(sandboxes),
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let session = harness.session("worker", SessionId::new("s-2"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "done");
                *sink.lock().unwrap() = tail_records(&session).await;
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 61).expect("invariants hold");

    // The loss enters the record before the model acts on state that is gone
    // (§5.5): tool error → WorkspaceReset → next model call.
    assert_eq!(
        record_kinds(&records.lock().unwrap()),
        vec![
            "created", "turn", "model", "tool", "reset", "model", "tool", "model", "ended"
        ],
    );
    // The loss tore down one environment; the next call opened a fresh one (§5.5)
    // — and both were released exactly once (H8).
    assert_eq!(stats.opened(), 2);
    assert_eq!(stats.released(), 2);
}

#[test]
fn a_durable_workspace_still_resets_on_a_genuine_mid_run_loss() {
    // The routine reactivation reset is gone (the workspace is the agent's own
    // durable facet), but a real `EnvironmentLost` during the run is not
    // routine — that environment's working state is gone, so the reset must
    // still be journaled (the `lost_this_activation` gate).
    let sandboxes = ScriptedSandboxes::new(|_, input| {
        if input.get("lose").is_some() {
            Err(ToolError::EnvironmentLost("scripted loss".to_string()))
        } else {
            Ok(json!("ok"))
        }
    });
    let model = ScriptedModel::steps(vec![
        Ok(tool_call("c1", "shell", json!({ "lose": true }))),
        Ok(tool_call("c2", "shell", json!({}))),
        Ok(final_message("done")),
    ]);
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let workload = Scenario::new(
        "durable-environment-loss",
        shell_kind(),
        Arc::new(model),
        Arc::new(sandboxes),
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let session = harness.session("worker", SessionId::new("s-durable-loss"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "done");
                *sink.lock().unwrap() = tail_records(&session).await;
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 61).expect("invariants hold");

    // The reset is present despite the durable provider: the loss was real.
    assert_eq!(
        record_kinds(&records.lock().unwrap()),
        vec![
            "created", "turn", "model", "tool", "reset", "model", "tool", "model", "ended"
        ],
    );
}

#[test]
fn a_slow_tool_is_bounded_by_its_declared_timeout() {
    let kinds = Kinds::new().register(
        "worker",
        Kind::new("worker")
            .tool(ToolDecl {
                name: "slow".to_string(),
                description: "slow tool".to_string(),
                input_schema: json!({ "type": "object" }),
                tier: Tier::Workspace,
                on_dangling: OnDangling::Interrupt,
                timeout: Some(Duration::from_secs(1)),
            })
            .budget(Budget::new(10_000, 10))
            .grain(brisk_idle()),
    );
    let model = ScriptedModel::steps(vec![
        Ok(tool_call("c1", "slow", json!({}))),
        Ok(final_message("done")),
    ]);
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    // The sandbox needs the run's clock for its 60s delay against a 1s bound.
    let workload = Scenario::from_factories(
        "tool-timeout",
        kinds,
        Arc::new(move |_| Arc::new(model.clone())),
        Arc::new(|system: &SimSystem| {
            Arc::new(
                ScriptedSandboxes::new(|_, _| Ok(json!("eventually")))
                    .with_delay(system.clock().clone(), Duration::from_secs(60)),
            )
        }),
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let session = harness.session("worker", SessionId::new("s-3"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "done");
                *sink.lock().unwrap() = tail_records(&session).await;
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 67).expect("invariants hold");

    // The timeout is the journaled outcome (§5.3 item 3); the straggling real
    // result, arriving at 60s, is a duplicate and is discarded.
    let tool_outcomes: Vec<_> = records
        .lock()
        .unwrap()
        .iter()
        .filter_map(|r| match &r.body {
            RecordBody::ToolOutcome { outcome, .. } => Some(outcome.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(tool_outcomes, vec![Err(ToolError::Timeout)]);
}

// ---------------------------------------------------------------------------
// Tier acquisition (harness spec §5.6; sandbox spec S4)
// ---------------------------------------------------------------------------

/// A kind spanning two tiers: `read` needs only the workspace the open grants
/// (§5.6 item 1); `run` needs `Compute`, acquired on first use. Its cap admits
/// both, so the registration is valid.
fn tiered_kind() -> Kinds {
    Kinds::new().register(
        "worker",
        Kind::new("worker")
            .sandboxed(
                "read",
                "read a file",
                &json!({ "type": "object" }),
                Tier::Workspace,
            )
            .sandboxed(
                "run",
                "run guest code",
                &json!({ "type": "object" }),
                Tier::Compute,
            )
            .sandbox(SandboxProfile::default().cap([Tier::Workspace, Tier::Compute]))
            .budget(Budget::new(10_000, 10))
            .grain(brisk_idle()),
    )
}

fn worker_kind() -> Arc<Kind> {
    tiered_kind()
        .get(&KindId::new("worker"))
        .expect("registered")
}

/// Run a tiered-kind scenario over `steps` and return the session's journal.
fn run_tiered(
    name: &'static str,
    model: ScriptedModel,
    session: &'static str,
    seed: u64,
) -> (Vec<Record>, support::SandboxStats) {
    let sandboxes = ScriptedSandboxes::echo();
    let stats = sandboxes.stats.clone();
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let workload = Scenario::new(
        name,
        tiered_kind(),
        Arc::new(model),
        Arc::new(sandboxes),
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let s = harness.session("worker", SessionId::new(session));
                let outcome = s
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "done");
                *sink.lock().unwrap() = tail_records(&s).await;
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, seed).expect("invariants hold");
    let out = records.lock().unwrap().clone();
    (out, stats)
}

#[test]
fn a_tier_is_acquired_under_a_journaled_record_before_its_first_effect() {
    let model = ScriptedModel::steps(vec![
        Ok(tool_call("c1", "read", json!({}))),
        Ok(tool_call("c2", "run", json!({}))),
        Ok(final_message("done")),
    ]);
    let (records, stats) = run_tiered("tier-acquisition", model, "s-4", 71);

    // The Workspace call needs no record — opening grants Workspace and nothing
    // else (§5.6 item 1); the Compute call's acquisition is journaled before its
    // outcome: intent before effect (§6.4).
    assert_eq!(
        record_kinds(&records),
        vec![
            "created", "turn", "model", "tool", "model", "tier", "tool", "model", "ended"
        ],
    );
    let acquired: Vec<Tier> = records
        .iter()
        .filter_map(|r| match &r.body {
            RecordBody::TierAcquired { tier, .. } => Some(*tier),
            _ => None,
        })
        .collect();
    assert_eq!(acquired, vec![Tier::Compute]);
    // The provider saw each call at its declared tier (§5.3 item 1).
    let calls: Vec<(Tier, String)> = stats
        .calls()
        .into_iter()
        .map(|(tier, name, _)| (tier, name))
        .collect();
    assert_eq!(
        calls,
        vec![
            (Tier::Workspace, "read".to_string()),
            (Tier::Compute, "run".to_string())
        ],
    );
    audit_tier_acquisition(&records, &worker_kind());
}

#[test]
fn two_same_step_calls_at_one_new_tier_journal_one_acquisition() {
    let mut both = tool_call("c1", "run", json!({}));
    both.calls.push(ToolCall {
        id: CallId::new("c2"),
        name: "run".to_string(),
        input: json!({}),
    });
    let model = ScriptedModel::steps(vec![Ok(both), Ok(final_message("done"))]);
    let (records, _) = run_tiered("tier-acquisition-dedup", model, "s-5", 73);

    let acquisitions = records
        .iter()
        .filter(|r| matches!(&r.body, RecordBody::TierAcquired { .. }))
        .count();
    assert_eq!(
        acquisitions, 1,
        "the second sibling parks on the first's record, no duplicate (§5.6)"
    );
    let outcomes = records
        .iter()
        .filter(|r| matches!(&r.body, RecordBody::ToolOutcome { outcome: Ok(_), .. }))
        .count();
    assert_eq!(outcomes, 2, "both calls executed once the record committed");
    audit_tier_acquisition(&records, &worker_kind());
}

#[test]
fn a_reset_restarts_the_held_set_and_reacquisition_is_rejournaled() {
    let sandboxes = ScriptedSandboxes::new(|_, input| {
        if input.get("lose").is_some() {
            Err(ToolError::EnvironmentLost("scripted loss".to_string()))
        } else {
            Ok(json!("ok"))
        }
    });
    let model = ScriptedModel::steps(vec![
        Ok(tool_call("c1", "run", json!({ "lose": true }))),
        Ok(tool_call("c2", "run", json!({}))),
        Ok(final_message("done")),
    ]);
    let records: Arc<Mutex<Vec<Record>>> = Arc::default();
    let sink = Arc::clone(&records);
    let workload = Scenario::new(
        "tier-reacquisition",
        tiered_kind(),
        Arc::new(model),
        Arc::new(sandboxes),
        move |harness, system| {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                let s = harness.session("worker", SessionId::new("s-6"));
                let outcome = s
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "done");
                *sink.lock().unwrap() = tail_records(&s).await;
                flush(&system).await;
            })
        },
    );
    run_seed(&workload, 79).expect("invariants hold");

    let records = records.lock().unwrap();
    // The fresh environment resets every held tier (§5.5): after the journaled
    // reset, the held set is back to Workspace and the second Compute call
    // re-journals its acquisition — never silently inherited.
    assert_eq!(
        record_kinds(&records),
        vec![
            "created", "turn", "model", "tier", "tool", "reset", "model", "tier", "tool", "model",
            "ended"
        ],
    );
    audit_tier_acquisition(&records, &worker_kind());
}

#[test]
#[should_panic(expected = "outside the tier cap")]
fn a_tool_beyond_the_cap_is_a_registration_error() {
    // Declaring a tool whose tier the cap excludes is a deployment configuration
    // error, surfaced at registration as loudly as a duplicate name (§5.3 item 4).
    let _ = Kinds::new().register(
        "worker",
        Kind::new("worker")
            .sandbox(SandboxProfile::default().cap([Tier::Workspace]))
            .sandboxed(
                "run",
                "run guest code",
                &json!({ "type": "object" }),
                Tier::Compute,
            ),
    );
}
