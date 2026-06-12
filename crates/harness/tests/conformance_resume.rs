//! Resume conformance (harness spec §5.5, §6.5, §7.5; invariant H1): a
//! journal outage interrupts a run mid-step; the caller's re-submitted
//! `TurnId` resumes it on a fresh activation, dangling calls resolve per
//! their declared policy, and the resumed journal matches an uninterrupted
//! control run — the differential test behind H1.

mod support;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_core::Clock;
use actor_core::Event;
use actor_core::LocalSystemBuilder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use harness::Budget;
use harness::Harness;
use harness::HarnessConfig;
use harness::InMemoryJournal;
use harness::Kind;
use harness::Kinds;
use harness::OnDangling;
use harness::RecordBody;
use harness::RunError;
use harness::SessionId;
use harness::Tier;
use harness::ToolDecl;
use harness::ToolError;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::CollectingSink;
use support::FaultedJournal;
use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::check_events;
use support::final_message;
use support::tool_call;

fn test_config() -> HarnessConfig {
    HarnessConfig {
        idle_timeout: Duration::from_secs(2),
        tick_interval: Duration::from_millis(500),
        submit_deadline: Duration::from_secs(60),
        journal_attempts: 2,
        journal_backoff: Duration::from_millis(50),
        ..HarnessConfig::default()
    }
}

fn fetch_kind(on_dangling: OnDangling) -> Kinds {
    Kinds::new().register(
        "worker",
        Kind::new("worker")
            .tool(ToolDecl {
                name: "fetch".to_string(),
                description: "an idempotent read".to_string(),
                input_schema: json!({"type": "object"}),
                tier: Tier::Workspace,
                on_dangling,
                timeout: None,
            })
            .budget(Budget::new(10_000, 10)),
    )
}

fn fetch_model() -> ScriptedModel {
    ScriptedModel::steps(vec![
        Ok(tool_call("c1", "fetch", json!({}))),
        Ok(final_message("done")),
    ])
}

/// One manually assembled run: a session whose tool execution trips a
/// one-shot journal outage, so the tool's outcome cannot be journaled — a
/// dangling call (§5.5). Returns the journal, the collected events, and the
/// sandbox stats.
fn run_interrupted(
    seed: u64,
    on_dangling: OnDangling,
) -> (InMemoryJournal, Vec<Event>, support::SandboxStats) {
    let sim = Simulation::new(seed);
    let sink = CollectingSink::default();
    let system: SimSystem = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(Arc::new(sink.clone()))
        .build();
    let store = InMemoryJournal::new();
    let journal = FaultedJournal::new(store.clone(), sim.clock(), sim.entropy());
    let outage = journal.clone();
    let trips = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&trips);
    // The first `fetch` executes its effect and *then* the journal goes
    // down: intent journaled, outcome not — the §5.5 dangling shape.
    let sandboxes = ScriptedSandboxes::new(move |_, _| {
        if counter.fetch_add(1, Ordering::SeqCst) == 0 {
            outage.set_unavailable(true);
        }
        Ok(json!("fetched"))
    });
    let stats = sandboxes.stats.clone();
    let harness = Harness::with_config(
        system.clone(),
        fetch_kind(on_dangling),
        Arc::new(journal.clone()),
        Arc::new(fetch_model()),
        Arc::new(sandboxes),
        test_config(),
    );
    let clock = system.clock().clone();
    sim.block_on(async move {
        let session = harness.session("worker", SessionId::new("s-resume"));
        let first = session
            .prompt(Turn::new(TurnId::new("t-1"), "go"))
            .await
            .expect("call");
        // The session cannot record (§6.5): the run pauses, the caller is
        // told so best-effort.
        assert_eq!(first, Err(RunError::Journal("injected outage".to_string())));

        // The store recovers; the re-submitted TurnId is the resumption
        // contact (§7.5).
        journal.set_unavailable(false);
        clock.sleep(Duration::from_secs(5)).await;
        let second = session
            .prompt(Turn::new(TurnId::new("t-1"), "go"))
            .await
            .expect("call")
            .expect("run resumes and completes");
        assert_eq!(second.text(), "done");
        clock.sleep(Duration::from_secs(10)).await;
    });
    (store, sink.events(), stats)
}

/// The uninterrupted control run, identical but for the outage.
fn run_control(seed: u64) -> InMemoryJournal {
    let sim = Simulation::new(seed);
    let system: SimSystem =
        LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let store = InMemoryJournal::new();
    let harness = Harness::with_config(
        system.clone(),
        fetch_kind(OnDangling::Reexecute),
        Arc::new(store.clone()),
        Arc::new(fetch_model()),
        Arc::new(ScriptedSandboxes::new(|_, _| Ok(json!("fetched")))),
        test_config(),
    );
    let clock = system.clock().clone();
    sim.block_on(async move {
        let session = harness.session("worker", SessionId::new("s-control"));
        let outcome = session
            .prompt(Turn::new(TurnId::new("t-1"), "go"))
            .await
            .expect("call")
            .expect("run");
        assert_eq!(outcome.text(), "done");
        clock.sleep(Duration::from_secs(10)).await;
    });
    store
}

#[test]
fn a_reexecuted_dangling_call_resumes_to_the_control_run_transcript() {
    let (store, events, stats) = run_interrupted(71, OnDangling::Reexecute);
    let control = run_control(71);

    // The harness checkers hold over the interrupted run's full stream, and
    // the resume is visible on it (§10.4): a RunResumed, never a second
    // RunStarted.
    let violations = check_events(&events);
    assert!(violations.is_empty(), "checkers: {violations:?}");
    assert!(
        events
            .iter()
            .filter_map(|e| e.as_app::<harness::HarnessEvent>())
            .any(|e| matches!(e, harness::HarnessEvent::RunResumed { .. })),
        "the second activation resumed the journaled run"
    );
    assert_eq!(
        events
            .iter()
            .filter_map(|e| e.as_app::<harness::HarnessEvent>())
            .filter(|e| matches!(e, harness::HarnessEvent::RunStarted { .. }))
            .count(),
        1
    );

    // The effect ran twice — blind re-execution is the declared policy
    // (§5.5) — but the journal records one outcome.
    assert_eq!(stats.calls().len(), 2);

    // H1, differentially: the resumed journal equals the uninterrupted
    // control's, record for record, except for (a) the recorded session
    // identity inside `SessionCreated` and (b) the `WorkspaceReset` the spec
    // *requires* the fresh activation to journal for a transcript that
    // asserts sandboxed state (§5.5) — the one mandated divergence.
    let resumed: Vec<RecordBody> = store
        .records(&SessionId::new("s-resume"))
        .into_iter()
        .map(|r| r.body)
        .filter(|b| {
            !matches!(
                b,
                RecordBody::SessionCreated { .. } | RecordBody::WorkspaceReset
            )
        })
        .collect();
    let control: Vec<RecordBody> = control
        .records(&SessionId::new("s-control"))
        .into_iter()
        .map(|r| r.body)
        .filter(|b| {
            !matches!(
                b,
                RecordBody::SessionCreated { .. } | RecordBody::WorkspaceReset
            )
        })
        .collect();
    assert_eq!(resumed, control, "fold-equivalence after resume (H1)");
}

#[test]
fn an_interrupted_dangling_call_resolves_for_the_model_to_decide() {
    let (store, events, stats) = run_interrupted(73, OnDangling::Interrupt);

    let violations = check_events(&events);
    assert!(violations.is_empty(), "checkers: {violations:?}");

    // The effect ran once; the harness never re-fired it (§5.5 — the model,
    // not the harness, decides whether to retry a side effect).
    assert_eq!(stats.calls().len(), 1);
    let records = store.records(&SessionId::new("s-resume"));
    let outcomes: Vec<_> = records
        .iter()
        .filter_map(|r| match &r.body {
            RecordBody::ToolOutcome { outcome, .. } => Some(outcome.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        outcomes,
        vec![Err(ToolError::Interrupted)],
        "the dangling call resolved as Interrupted, journaled once"
    );
    assert!(matches!(
        records.last().expect("records").body,
        RecordBody::RunEnded { outcome: Ok(_), .. }
    ));
}
