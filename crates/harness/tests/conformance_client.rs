//! Client/delegation plumbing conformance (harness spec §7.4, §8.1): the
//! ephemeral reply mailbox's lifetime is its caller's wait — no submit path
//! leaks the actor — and a delegating parent's wait on a slow child scales
//! with the child's carved budget rather than a flat attempt cap.

mod support;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use actor_core::ActorId;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Event;
use actor_core::LocalSystemBuilder;
use actor_simulation::SimClock;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use harness::Budget;
use harness::CallId;
use harness::Harness;
use harness::HarnessConfig;
use harness::Kind;
use harness::Kinds;
use harness::Model;
use harness::ModelError;
use harness::ModelRequest;
use harness::ModelResponse;
use harness::RecordBody;
use harness::SessionId;
use harness::ToolError;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::CollectingSink;
use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::check_events;
use support::final_message;
use support::tail_records;
use support::tool_call;

/// The live actors implied by a stream prefix: every `ActorReady` id without a
/// matching `ResignId` (core §4.2) — the leak observable behind §7.4's mailbox
/// lifetime.
fn live_actors(events: &[Event]) -> BTreeSet<ActorId> {
    let mut live = BTreeSet::new();
    for event in events {
        match event {
            Event::ActorReady { id } => {
                live.insert(id.clone());
            }
            Event::ResignId { id } => {
                live.remove(id);
            }
            _ => {}
        }
    }
    live
}

/// A kind config whose idle window outlives the test, so the live-actor set is
/// not churned by hibernation between snapshots.
fn calm_idle() -> granary::GranaryConfig {
    granary::GranaryConfig {
        idle_after: Duration::from_secs(100_000),
        data_dir: Some(
            tempfile::tempdir()
                .expect("workspace scratch tempdir")
                .keep(),
        ),
        ..granary::GranaryConfig::default()
    }
}

/// Parent/child kinds for the delegation tests, calm-idled so actor sets stay
/// comparable at quiescence.
fn tree_kinds() -> Kinds {
    Kinds::new()
        .register(
            "parent",
            Kind::new("parent agent")
                .delegates_to(&["child"])
                .budget(Budget::new(10_000, 10))
                .grain(calm_idle()),
        )
        .register(
            "child",
            Kind::new("child agent")
                .budget(Budget::new(2_000, 4))
                .grain(calm_idle()),
        )
}

/// Slows only the child kind's model calls (system prompt "child agent"), so
/// the parent's own steps stay prompt while the child crawls. The delay is a
/// timer, never a forever-pending future: the simulation drains to quiescence
/// through every armed timer, and a child that can never finish would keep its
/// activation's idle check re-arming forever — the drain would never end.
struct SlowChildModel {
    inner: Arc<dyn Model>,
    clock: SimClock,
    delay: Duration,
}

impl Model for SlowChildModel {
    fn complete(&self, req: ModelRequest) -> BoxFuture<'static, Result<ModelResponse, ModelError>> {
        if req.system_prompt != "child agent" {
            return self.inner.complete(req);
        }
        let delay = self.delay;
        let clock = self.clock.clone();
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            clock.sleep(delay).await;
            inner.complete(req).await
        })
    }
}

#[test]
fn a_rejected_resubmission_leaks_no_reply_mailbox() {
    // A rejected re-submission spawns a reply mailbox nothing will ever notify;
    // the caller's own wait ending must stop it (§7.4). Differential: before
    // the mailbox was tied to the wait, every rejected attempt leaked one
    // actor, and this set comparison caught eight of them.
    let sim = Simulation::new(41);
    let sink = CollectingSink::default();
    let system: SimSystem = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(Arc::new(sink.clone()))
        .build();
    let kinds = Kinds::new().register(
        "worker",
        Kind::new("worker")
            .budget(Budget::new(10_000, 5))
            .grain(calm_idle()),
    );
    let harness = Harness::cluster(
        system.clone(),
        &kinds,
        Arc::new(ScriptedModel::steps(Vec::new())),
        Arc::new(ScriptedSandboxes::echo()),
    );
    let clock = system.clock().clone();
    let observer = sink.clone();
    sim.block_on(async move {
        let s = harness.session("worker", SessionId::new("s-reject")).unwrap();
        s.prompt(Turn::new(TurnId::new("t-1"), "one"))
            .await
            .expect("call")
            .expect("run");
        clock.sleep(Duration::from_secs(10)).await;
        let before = live_actors(&observer.events());
        for _ in 0..8 {
            let rejected = s.prompt(Turn::new(TurnId::new("t-1"), "changed")).await;
            assert!(
                rejected.is_err(),
                "a content conflict is a rejection (§7.4)"
            );
        }
        clock.sleep(Duration::from_secs(10)).await;
        let after = live_actors(&observer.events());
        assert_eq!(before, after, "rejected attempts leak no actor (§7.4)");
    });
    let violations = check_events(&sink.events());
    assert!(violations.is_empty(), "checkers: {violations:?}");
}

/// One delegation to a child whose model stalls far past the wait: the parent
/// gives up at the configured bound (`cycles` re-attach lapses of the 30s
/// cadence), its own run completing with the delegation's failure journaled
/// (§5.4, §8.1). Returns the count of live actors just after the give-up —
/// while the child is still mid-call, so a mailbox its lapsed attempts left
/// subscribed is still visibly alive (the drain would eventually complete the
/// child and notify even a leaked mailbox, hiding the leak at quiescence).
fn run_abandoned_child(cycles: u32) -> usize {
    let sim = Simulation::new(43);
    let sink = CollectingSink::default();
    let system: SimSystem = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(Arc::new(sink.clone()))
        .build();
    let parent_script = ScriptedModel::new(|req| {
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
    });
    let model = SlowChildModel {
        inner: Arc::new(parent_script),
        clock: sim.clock(),
        delay: Duration::from_secs(100_000),
    };
    let harness = Harness::builder(system.clone(), &tree_kinds())
        .config(HarnessConfig {
            budget_floor: 0,
            child_wait_floor: Duration::from_secs(30) * cycles,
            child_wait_per_step: Duration::ZERO,
            ..HarnessConfig::default()
        })
        .host_all(Arc::new(model), Arc::new(ScriptedSandboxes::echo()))
        .build();
    let clock = system.clock().clone();
    let observer = sink.clone();
    sim.block_on(async move {
        let s = harness.session("parent", SessionId::new("root-wait")).unwrap();
        let outcome = s
            .prompt_within(
                Turn::new(TurnId::new("t-1"), "go"),
                Duration::from_secs(50_000),
            )
            .await
            .expect("call")
            .expect("the parent's run never fails because a child did (§8.2)");
        assert_eq!(outcome.text(), "parent-answer");
        let records = tail_records(&s).await;
        assert!(
            records.iter().any(|r| matches!(
                &r.body,
                RecordBody::ToolOutcome {
                    outcome: Err(ToolError::Delegation(_)),
                    ..
                }
            )),
            "the give-up is the delegation's journaled tool failure (§5.4)"
        );
        // Let post-give-up deliveries settle, then count while the child is
        // still mid-call.
        clock.sleep(Duration::from_secs(10)).await;
        live_actors(&observer.events()).len()
    })
}

#[test]
fn an_abandoned_child_wait_accumulates_no_mailboxes() {
    // The same scenario at 2 vs 8 lapse cycles: the live-actor set at
    // quiescence must not grow with the number of re-attach attempts (§7.4).
    // Differential: before the mailbox guard, every lapsed attempt left its
    // mailbox subscribed forever on the stalled child — six more actors here.
    assert_eq!(run_abandoned_child(2), run_abandoned_child(8));
}

#[test]
fn the_child_wait_scales_with_the_carved_steps() {
    // A child answering after 700s of virtual time, delegated twice: a 3-step
    // carve waits 60 + 3×300 = 960s ≥ 700 and collects the answer; a 1-step
    // carve waits 60 + 300 = 360s < 700 and resolves as the give-up failure.
    // The wait is a function of the carve (§8.1), not a flat cap — under the
    // old 32×30s cap both would have completed identically.
    let sim = Simulation::new(47);
    let sink = CollectingSink::default();
    let system: SimSystem = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(Arc::new(sink.clone()))
        .build();
    let parent_script = ScriptedModel::new(|req| {
        if req.system_prompt == "child agent" {
            return Ok(final_message("child-answer"));
        }
        let step = req
            .transcript
            .iter()
            .filter(|e| matches!(e, harness::Entry::Assistant { .. }))
            .count();
        match step {
            0 => Ok(tool_call(
                "d1",
                "delegate",
                json!({ "kind": "child", "prompt": "big sub-task", "budget": { "tokens": 500, "steps": 3 } }),
            )),
            1 => Ok(tool_call(
                "d2",
                "delegate",
                json!({ "kind": "child", "prompt": "small sub-task", "budget": { "tokens": 500, "steps": 1 } }),
            )),
            _ => Ok(final_message("parent-answer")),
        }
    });
    let model = SlowChildModel {
        inner: Arc::new(parent_script),
        clock: sim.clock(),
        delay: Duration::from_secs(700),
    };
    let harness = Harness::builder(system.clone(), &tree_kinds())
        .config(HarnessConfig {
            budget_floor: 0,
            child_wait_floor: Duration::from_secs(60),
            child_wait_per_step: Duration::from_secs(300),
            ..HarnessConfig::default()
        })
        .host_all(Arc::new(model), Arc::new(ScriptedSandboxes::echo()))
        .build();
    let clock = system.clock().clone();
    sim.block_on(async move {
        let s = harness.session("parent", SessionId::new("root-scale")).unwrap();
        let outcome = s
            .prompt_within(
                Turn::new(TurnId::new("t-1"), "go"),
                Duration::from_secs(100_000),
            )
            .await
            .expect("call")
            .expect("the parent's run never fails because a child did (§8.2)");
        assert_eq!(outcome.text(), "parent-answer");
        let records = tail_records(&s).await;
        let outcomes: BTreeMap<CallId, Result<serde_json::Value, ToolError>> = records
            .iter()
            .filter_map(|r| match &r.body {
                RecordBody::ToolOutcome { call, outcome, .. } => {
                    Some((call.clone(), outcome.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            outcomes.get(&CallId::new("d1")),
            Some(&Ok(serde_json::Value::String("child-answer".to_string()))),
            "the 3-step carve's 960s wait outlasts the 700s child (§8.1)"
        );
        assert!(
            matches!(
                outcomes.get(&CallId::new("d2")),
                Some(&Err(ToolError::Delegation(_)))
            ),
            "the 1-step carve's 360s wait lapses first (§8.1): {:?}",
            outcomes.get(&CallId::new("d2"))
        );
        clock.sleep(Duration::from_secs(10)).await;
    });
    let violations = check_events(&sink.events());
    assert!(violations.is_empty(), "checkers: {violations:?}");
}
