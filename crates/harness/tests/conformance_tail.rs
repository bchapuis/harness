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
    let session = harness.session("worker", SessionId::new("s1")).unwrap();

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
    let kinds_seen = record_kinds(&page.events.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>());
    assert_eq!(
        kinds_seen,
        vec!["created", "turn", "started", "model", "tool", "model", "ended"],
        "the full record sequence reads back across hibernation",
    );
    assert!(
        page.events.windows(2).all(|w| w[0].0 < w[1].0),
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

/// A kind config that snapshots aggressively, so a short run crosses the
/// compaction trigger (granary §9) and truncates the journal's prefix.
fn compacting(snapshot_every: u64) -> granary::GranaryConfig {
    granary::GranaryConfig {
        snapshot_every,
        // The workspace facet materializes a real directory per grain; a fresh
        // tempdir keeps parallel tests from sharing scratch paths (see
        // `support::brisk_idle`).
        data_dir: Some(
            tempfile::tempdir()
                .expect("workspace scratch tempdir")
                .keep(),
        ),
        ..granary::GranaryConfig::default()
    }
}

#[test]
fn a_compacted_session_announces_truncation_to_tail_and_follower() {
    let sim = Simulation::new(7);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();

    // Every model call ends the turn; each turn appends a handful of records,
    // so four turns cross the snapshot trigger mid-history and the tail of the
    // journal stays readable past the last snapshot.
    let model = Arc::new(ScriptedModel::steps(Vec::new()));
    let sandboxes = Arc::new(ScriptedSandboxes::echo());
    let kinds = Kinds::new().register("worker", Kind::new("worker").grain(compacting(6)));
    let harness = Harness::cluster(system.clone(), &kinds, model, sandboxes);
    let session = harness.session("worker", SessionId::new("s1")).unwrap();

    for turn in ["t1", "t2", "t3", "t4"] {
        let out = sim.block_on({
            let session = session.clone();
            let turn = Turn::new(TurnId::new(turn), "go");
            async move { session.prompt(turn).await }
        });
        assert!(matches!(out, Ok(Ok(_))), "the run completes: {out:?}");
    }

    // A tail from ZERO asked for records the session's snapshot has subsumed:
    // the reply announces the truncation (§10.2) instead of serving a short
    // history a reader would mistake for the whole transcript.
    let page = sim
        .block_on({
            let session = session.clone();
            async move { session.tail(Seq::ZERO, harness::TAIL_PAGE).await }
        })
        .expect("tail of a compacted session succeeds");
    assert!(
        page.base > Seq::ZERO,
        "compaction ran and the reply reports its base",
    );
    assert!(
        !page.events.is_empty(),
        "the post-snapshot suffix is still readable (else lower snapshot_every's crossings)",
    );
    assert!(
        page.events.iter().all(|(seq, _)| *seq > page.base),
        "every record served lies above the base",
    );

    // Completeness above the base: reading from the base returns byte-identical
    // history, and its reply says nothing after `from` was compacted.
    let from_base = sim
        .block_on({
            let session = session.clone();
            let base = page.base;
            async move { session.tail(base, harness::TAIL_PAGE).await }
        })
        .expect("tail from the base succeeds");
    assert!(
        from_base.base <= page.base,
        "nothing after the base was compacted",
    );
    assert_eq!(
        from_base.events, page.events,
        "the readable suffix is identical from ZERO and from the base",
    );

    // The follower surfaces the same fact as an explicit step — told *before*
    // the stream resumes past the gap — then yields the surviving suffix.
    let (first, second) = sim.block_on({
        let session = session.clone();
        async move {
            let mut follower = session.follow(Seq::ZERO);
            let first = follower.next().await.expect("follow attaches");
            let second = follower.next().await.expect("follow resumes");
            (first, second)
        }
    });
    assert_eq!(
        first,
        harness::Followed::Truncated { base: page.base },
        "the follower announces the truncation first",
    );
    assert_eq!(
        second,
        harness::Followed::Batch(page.events.clone()),
        "the stream resumes with the records after the base",
    );
}
