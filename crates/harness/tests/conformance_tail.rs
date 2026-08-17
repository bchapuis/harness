//! Tail conformance (harness spec §10.2): reading a run is a journal read at
//! the granary gateway, not a session command — so observation neither wakes a
//! hibernated session (rehydration plus workspace rematerialization) nor loses
//! records across the hibernation it leaves undisturbed.

mod support;

use std::sync::Arc;
use std::time::Duration;

use actor_core::Clock;
use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_simulation::Recorder;
use actor_simulation::Simulation;
use granary::GrainEvent;
use granary::Seq;
use harness::Harness;
use harness::Kind;
use harness::Kinds;
use harness::SandboxProfile;
use harness::SessionId;
use harness::Tier;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::brisk_idle;
use support::final_message;
use support::record_kinds;
use support::tool_call;

#[test]
fn tail_leaves_a_hibernated_session_asleep() {
    let sim = Simulation::new(3);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();

    let model = Arc::new(ScriptedModel::steps(vec![
        Ok(tool_call("c1", "shell", json!({ "cmd": "ls" }))),
        Ok(final_message("done")),
    ]));
    let sandboxes = Arc::new(ScriptedSandboxes::echo());
    let kinds = Kinds::new().register(
        "worker",
        Kind::new("worker")
            .sandboxed(
                "shell",
                "run a command",
                &json!({ "type": "object" }),
                Tier::Workspace,
            )
            .sandbox(SandboxProfile::image("base"))
            .grain(brisk_idle()),
    );
    let harness = Harness::cluster(system.clone(), &kinds, model, sandboxes);
    let session = harness.session("worker", SessionId::new("s1"));

    let out = sim.block_on({
        let session = session.clone();
        async move { session.prompt(Turn::new(TurnId::new("t1"), "go")).await }
    });
    assert!(matches!(out, Ok(Ok(_))), "the run completes: {out:?}");

    // Drive past the brisk idle window: the session hibernates.
    sim.block_on({
        let clock = sim.clock();
        async move { clock.sleep(Duration::from_secs(10)).await }
    });
    let session_events = |recorder: &Recorder| {
        recorder
            .events()
            .iter()
            .filter_map(|e| e.as_app::<GrainEvent>().cloned())
            .filter(|e| match e {
                GrainEvent::Activated { name, .. } | GrainEvent::Passivated { name, .. } => {
                    name.key() == "s1"
                }
                _ => false,
            })
            .collect::<Vec<_>>()
    };
    assert!(
        session_events(&recorder)
            .iter()
            .any(|e| matches!(e, GrainEvent::Passivated { .. })),
        "the idle session must hibernate",
    );

    // Tail the whole run from the hibernated journal: the gateway serves it
    // without get-or-activating the session (§10.2).
    let page = sim
        .block_on({
            let session = session.clone();
            async move { session.tail(Seq::ZERO, harness::TAIL_PAGE).await }
        })
        .expect("tail after hibernation succeeds");
    let kinds_seen = record_kinds(&page.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>());
    assert_eq!(
        kinds_seen,
        vec!["created", "turn", "model", "tool", "model", "ended"],
        "the full record sequence reads back across hibernation",
    );
    assert!(
        page.windows(2).all(|w| w[0].0 < w[1].0),
        "the Seqs are the journal's own, strictly ascending",
    );

    // The read woke nothing: exactly one activation — the run's — and no
    // re-activation after the hibernation the poll left undisturbed.
    sim.block_on({
        let clock = sim.clock();
        async move { clock.sleep(Duration::from_secs(5)).await }
    });
    let activated = session_events(&recorder)
        .iter()
        .filter(|e| matches!(e, GrainEvent::Activated { .. }))
        .count();
    assert_eq!(
        activated, 1,
        "a tail poll must not wake a hibernated session"
    );
}
