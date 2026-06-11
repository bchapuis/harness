//! Fence conformance (harness spec §6.2; invariant H2): two activations of
//! one session on two "nodes" with divergent views — each a `LocalSystem`
//! whose placement names itself owner — race their fenced appends on one
//! shared journal. The journal accepts one writer per record; the loser
//! deactivates with nothing further journaled, and the transcript never
//! forks. Divergence costs duplicated speculative work, never a forked
//! record (§6.2 item 4).

mod support;

use std::sync::Arc;
use std::time::Duration;

use actor_core::Clock;
use actor_core::LocalSystemBuilder;
use actor_core::NodeId;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
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

use support::CollectingSink;
use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::SlowModel;
use support::check_events;
use support::final_message;

#[test]
fn divergent_owners_race_on_the_fence_and_the_transcript_never_forks() {
    let sim = Simulation::new(79);
    let sink = CollectingSink::default();
    let store = InMemoryJournal::new();

    let config = HarnessConfig {
        idle_timeout: Duration::from_secs(2),
        tick_interval: Duration::from_millis(500),
        submit_deadline: Duration::from_secs(120),
        ..HarnessConfig::default()
    };
    // Two nodes, each believing it owns every session (a LocalSystem's
    // placement is itself): the divergent-view shape of util §2.3, distilled.
    // They share the one logical journal (§6.1) — and, for the checkers, one
    // event stream.
    let mut harnesses = Vec::new();
    for node in [1u64, 2u64] {
        let system: SimSystem = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
            .node(NodeId::new(node))
            .events(Arc::new(sink.clone()))
            .build();
        let model = SlowModel {
            inner: Arc::new(ScriptedModel::steps(vec![Ok(final_message("done"))])),
            clock: system.clock().clone(),
            // Slow enough that both activations are mid-step when their
            // appends race.
            delay: Duration::from_secs(2),
        };
        let kinds =
            Kinds::new().register("echo", Kind::new("agent").budget(Budget::new(10_000, 10)));
        harnesses.push((
            system.clone(),
            Harness::with_config(
                system,
                kinds,
                Arc::new(store.clone()),
                Arc::new(model),
                Arc::new(ScriptedSandboxes::echo()),
                config.clone(),
            ),
        ));
    }

    let session_id = SessionId::new("contested");
    let clock = harnesses[0].0.clock().clone();
    let a = harnesses[0].1.session("echo", session_id.clone());
    let b = harnesses[1].1.session("echo", session_id.clone());
    sim.block_on(async move {
        // Both nodes serve the same session concurrently: turn t-a lands on
        // node 1's activation, t-b on node 2's. Their first appends race on
        // `after = 0`; exactly one wins, the loser deactivates (H2) and its
        // caller's re-submission lands wherever the journal's order allows.
        let (ra, rb) = futures::join!(
            a.prompt(Turn::new(TurnId::new("t-a"), "from node 1")),
            b.prompt(Turn::new(TurnId::new("t-b"), "from node 2"))
        );
        let ra = ra.expect("call a").expect("run a");
        let rb = rb.expect("call b").expect("run b");
        assert_eq!(ra.text(), "done");
        assert_eq!(rb.text(), "done");
        clock.sleep(Duration::from_secs(15)).await;
    });

    // The race actually happened (this seed produces it; a regression that
    // stops fencing fails loudly) …
    let events = sink.events();
    assert!(
        events
            .iter()
            .filter_map(|e| e.as_app::<harness::HarnessEvent>())
            .any(|e| matches!(e, harness::HarnessEvent::AppendRejected { .. })),
        "the divergence produced at least one fence rejection"
    );
    // … and the checkers held across it: the loser deactivated with no
    // further activity (H2), activations alternated per node (H6), one
    // RunStarted per turn (H7).
    let violations = check_events(&events);
    assert!(violations.is_empty(), "checkers: {violations:?}");

    // One total order, both runs complete in it: the transcript never forks
    // (§6.2). Each turn has exactly one TurnSubmitted and one RunEnded.
    let records = store.records(&session_id);
    for turn in ["t-a", "t-b"] {
        let submitted = records
            .iter()
            .filter(|r| {
                matches!(&r.body, RecordBody::TurnSubmitted { turn: t, .. } if t.as_str() == turn)
            })
            .count();
        let ended = records
            .iter()
            .filter(
                |r| matches!(&r.body, RecordBody::RunEnded { turn: t, .. } if t.as_str() == turn),
            )
            .count();
        assert_eq!((submitted, ended), (1, 1), "turn {turn} ran exactly once");
    }
}
