//! Resume conformance (harness spec §6.2, §7.5; invariant H1): a session
//! hibernates between turns and rehydrates from the journal on the next contact
//! (granary §9, §10); the resumed journal matches an uninterrupted control run,
//! record for record — the differential test behind H1. (Mid-run crash/migration
//! resume of a *dangling* call, §5.5, is a `Quorum`-tier leadership-move phenomenon,
//! exercised under the clustered simulation; the `Local` tier is faultless by design.)

mod support;

use std::sync::Arc;
use std::time::Duration;

use actor_core::Clock;
use actor_core::Event;
use actor_core::LocalSystemBuilder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use granary::GrainEvent;
use harness::Budget;
use harness::Harness;
use harness::Kind;
use harness::Kinds;
use harness::Record;
use harness::RecordBody;
use harness::SessionId;
use harness::Tier;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::CollectingSink;
use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::brisk_idle;
use support::check_events;
use support::final_message;
use support::tail_records;
use support::tool_call;

fn worker_kinds() -> Kinds {
    Kinds::new().register(
        "worker",
        Kind::new("worker")
            .sandboxed(
                "fetch",
                "an idempotent read",
                &json!({ "type": "object" }),
                Tier::Workspace,
            )
            .budget(Budget::new(10_000, 10))
            .grain(brisk_idle()),
    )
}

/// Two turns; each does one tool step then completes. A pure function of the
/// request, so a replay reproduces it (§4.2): at a turn's start (last entry is
/// the user prompt) it calls the tool; after the tool result, it completes.
fn two_turn_model() -> ScriptedModel {
    ScriptedModel::new(|req| match req.transcript.last() {
        Some(harness::Entry::ToolResult { .. }) => Ok(final_message("done")),
        // Empty call id: the harness assigns a deterministic per-step id.
        _ => Ok(tool_call("", "fetch", json!({}))),
    })
}

/// Run two turns on one session, optionally idling between them so the session
/// hibernates and the second turn rehydrates from the journal (§7.5). Returns
/// the journal and the observed event stream.
fn run_two_turns(seed: u64, session: &'static str, hibernate: bool) -> (Vec<Record>, Vec<Event>) {
    let sim = Simulation::new(seed);
    let sink = CollectingSink::default();
    let system: SimSystem = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(Arc::new(sink.clone()))
        .build();
    let sandboxes = ScriptedSandboxes::new(|_, _| Ok(json!("fetched")));
    let harness = Harness::cluster(
        system.clone(),
        &worker_kinds(),
        Arc::new(two_turn_model()),
        Arc::new(sandboxes),
    );
    let clock = system.clock().clone();
    let records = sim.block_on(async move {
        let s = harness.session("worker", SessionId::new(session));
        let first = s
            .prompt(Turn::new(TurnId::new("t-1"), "one"))
            .await
            .expect("call")
            .expect("run");
        assert_eq!(first.text(), "done");
        if hibernate {
            // Idle past the brisk window so the activation hibernates; the next
            // turn reactivates and rehydrates from the journal (§7.5).
            clock.sleep(Duration::from_secs(5)).await;
        }
        let second = s
            .prompt(Turn::new(TurnId::new("t-2"), "two"))
            .await
            .expect("call")
            .expect("run");
        assert_eq!(second.text(), "done");
        let records = tail_records(&s).await;
        clock.sleep(Duration::from_secs(5)).await;
        records
    });
    (records, sink.events())
}

#[test]
fn a_hibernated_session_resumes_to_the_control_transcript() {
    let (resumed, events) = run_two_turns(71, "s-resume", true);
    let (control, _) = run_two_turns(71, "s-resume", false);

    // The harness checkers hold over the resumed run's full stream (§11).
    let violations = check_events(&events);
    assert!(violations.is_empty(), "checkers: {violations:?}");

    // The session activated more than once: it hibernated and rehydrated (§10).
    let activations = events
        .iter()
        .filter_map(|e| e.as_app::<GrainEvent>())
        .filter(|e| matches!(e, GrainEvent::Activated { .. }))
        .count();
    assert!(
        activations >= 2,
        "the session hibernated and reactivated, got {activations} activations"
    );

    // A resume emits no second RunStarted (§10.4): one per turn, two total.
    let starts = events
        .iter()
        .filter_map(|e| e.as_app::<harness::HarnessEvent>())
        .filter(|e| matches!(e, harness::HarnessEvent::RunStarted { .. }))
        .count();
    assert_eq!(starts, 2, "one RunStarted per turn, none for the resume");

    // No `WorkspaceReset` on resume: the workspace is the agent's own durable
    // facet (granary §7.11), rematerialized on reactivation — a routine resume
    // is never a loss (§5.5). Only a mid-activation `EnvironmentLost` resets.
    assert!(
        !resumed
            .iter()
            .any(|r| matches!(r.body, RecordBody::WorkspaceReset)),
        "a routine resume must not surface a workspace reset (§5.5)"
    );

    // H1 with no exception: the rehydrated journal equals the uninterrupted
    // control's, record for record. Same session id, so even `SessionCreated`
    // matches.
    let bodies =
        |records: Vec<Record>| -> Vec<RecordBody> { records.into_iter().map(|r| r.body).collect() };
    assert_eq!(
        bodies(resumed),
        bodies(control),
        "fold-equivalence after resume (H1), no mandated divergence"
    );
}
