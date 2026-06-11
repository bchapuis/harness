//! Cancellation conformance (harness spec §9.2; invariant H5): message
//! granularity, idempotence, straggler discard, and propagation across the
//! delegation tree.

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
use harness::Model;
use harness::RecordBody;
use harness::RunError;
use harness::SessionId;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::SlowModel;
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

/// A workload that needs the run's clock to build its model (the slow model
/// sleeps on it), so it builds everything from the system it is handed.
struct CancelWorkload<F> {
    name: &'static str,
    kinds: fn() -> Kinds,
    make_model: fn(&SimSystem) -> Arc<dyn Model>,
    body: F,
    journal: Mutex<Option<InMemoryJournal>>,
}

impl<F> CancelWorkload<F> {
    fn journal(&self) -> InMemoryJournal {
        self.journal
            .lock()
            .expect("journal")
            .clone()
            .expect("run first")
    }
}

impl<F> Workload for CancelWorkload<F>
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
            (self.make_model)(&system),
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
    system.clock().sleep(Duration::from_secs(30)).await;
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
    let workload = CancelWorkload {
        name: "cancel-mid-call",
        kinds: || Kinds::new().register("echo", Kind::new("agent")),
        make_model: slow_final,
        body: |harness: Harness<SimSystem>, system: SimSystem| {
            boxed(async move {
                let session = harness.session("echo", SessionId::new("s-1"));
                let prompting = session.prompt(Turn::new(TurnId::new("t-1"), "go"));
                let canceller = {
                    let session = session.clone();
                    let clock = system.clock().clone();
                    async move {
                        // Cancel while the 60s model call is in flight: the
                        // mailbox is live during it (§3.2), so the cancel
                        // lands at message granularity, not after the call.
                        clock.sleep(Duration::from_secs(5)).await;
                        session.cancel(&TurnId::new("t-1")).await.expect("cancel")
                    }
                };
                let (outcome, ()) = futures::join!(prompting, canceller);
                assert_eq!(outcome.expect("call"), Err(RunError::Cancelled));
                flush(&system).await;
            })
        },
        journal: Mutex::new(None),
    };
    run_seed(&workload, 43).expect("invariants hold");

    // The straggling model response was discarded, not journaled (§9.2 item
    // 4): the cancelled run's terminal record is the last word.
    let records = workload.journal().records(&SessionId::new("s-1"));
    let kinds: Vec<bool> = records
        .iter()
        .map(|r| matches!(r.body, RecordBody::ModelResponse { .. }))
        .collect();
    assert!(
        kinds.iter().all(|is_model| !is_model),
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
fn a_cancel_for_an_ended_or_unknown_run_is_a_no_op() {
    let workload = CancelWorkload {
        name: "cancel-idempotent",
        kinds: || Kinds::new().register("echo", Kind::new("agent")),
        make_model: |_| Arc::new(ScriptedModel::steps(vec![Ok(final_message("done"))])),
        body: |harness: Harness<SimSystem>, system: SimSystem| {
            boxed(async move {
                let session = harness.session("echo", SessionId::new("s-2"));
                let outcome = session
                    .prompt(Turn::new(TurnId::new("t-1"), "go"))
                    .await
                    .expect("call")
                    .expect("run");
                assert_eq!(outcome.text(), "done");
                // A delayed cancel never kills the named run's successor
                // (§9.2 item 1): it is a no-op on the ended run.
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
        journal: Mutex::new(None),
    };
    run_seed(&workload, 47).expect("invariants hold");
}

#[test]
fn a_cancel_propagates_down_the_delegation_tree() {
    let workload = CancelWorkload {
        name: "cancel-propagation",
        kinds: || {
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
        },
        make_model: |system| {
            // The parent delegates immediately; the child's model call hangs
            // for an hour — only cancellation ends it.
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
                            json!({"kind": "child", "prompt": "sub-task"}),
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
        },
        body: |harness: Harness<SimSystem>, system: SimSystem| {
            boxed(async move {
                let session = harness.session("parent", SessionId::new("root"));
                let prompting = session.prompt_within(
                    Turn::new(TurnId::new("t-1"), "go"),
                    Duration::from_secs(20_000),
                );
                let canceller = {
                    let session = session.clone();
                    let clock = system.clock().clone();
                    async move {
                        // The parent's first call completes at 3600s and
                        // journals the delegation; the child's hour-long
                        // model call is then in flight. Cancel mid-flight.
                        clock.sleep(Duration::from_secs(4_000)).await;
                        session.cancel(&TurnId::new("t-1")).await.expect("cancel")
                    }
                };
                let (outcome, ()) = futures::join!(prompting, canceller);
                assert_eq!(outcome.expect("call"), Err(RunError::Cancelled));
                flush(&system).await;
            })
        },
        journal: Mutex::new(None),
    };
    run_seed(&workload, 53).expect("invariants hold");

    // The parent's journal names the child (§8.1); the child's run ended
    // Cancelled within bounded logical time (H5).
    let journal = workload.journal();
    let child_session = journal
        .records(&SessionId::new("root"))
        .iter()
        .find_map(|r| match &r.body {
            RecordBody::ChildRun { child_session, .. } => Some(child_session.clone()),
            _ => None,
        })
        .expect("journaled delegation");
    let ended_cancelled = journal.records(&child_session).iter().any(|r| {
        matches!(
            &r.body,
            RecordBody::RunEnded {
                outcome: Err(RunError::Cancelled),
                ..
            }
        )
    });
    assert!(
        ended_cancelled,
        "the child's run ended Cancelled (§9.2 item 2)"
    );
}
