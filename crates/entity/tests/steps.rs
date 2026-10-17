//! Durable-workflow scenarios under deterministic simulation (facets §4.6).
//!
//! Exercises the [`Steps`](entity::facet::Steps) step memo + the
//! [`Alarm`](entity::facet::Alarm)-backed `sleep` through the self-driving
//! [`Pipeline`](support::pipeline::Pipeline) entity: the reference shape a linear
//! DSL would generate. One seed each, hand-built: `sleep` resumes the workflow with
//! no caller (the alarm), a `retry` step re-launches after a failed effect, and the
//! memo carries completed steps across hibernation.
//!
//! The property these state and `steps_swarm.rs` sweeps is the memo's
//! **write-once** rule: `complete_step` records only a step that is not already
//! done, so the first committed result wins. Here it is asserted on a clean
//! single-node run, the counterpart to the sweep, which bounds what can change and
//! would be satisfied by a fixture whose workflow never commits at all.

mod support;

use std::sync::Arc;
use std::time::Duration;

use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_core::Spawner;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use entity::EntityConfig;
use entity::EntityEvent;
use entity::EntityExt;
use entity::facet::LaunchGuard;

use support::pipeline::Effects;
use support::pipeline::PipelineConfig;
use support::pipeline::Read;
use support::pipeline::ReadMemo;
use support::pipeline::STEP_FETCH;

/// The `Pipeline` at this suite's tier.
type Pipeline = support::pipeline::Pipeline<SimSystem>;

// --- Test rig -----------------------------------------------------------------

fn rig(
    seed: u64,
    idle_after: Duration,
    fx: Effects,
    cfg: PipelineConfig,
) -> (Simulation, Recorder, entity::Entities<Pipeline>) {
    let sim = Simulation::new(seed);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let entities = system.entity_node().entities_named::<Pipeline>(
        support::pipeline::PIPELINE_TYPE,
        EntityConfig {
            idle_after,
            ..EntityConfig::default()
        },
        support::pipeline::Pipeline::factory(fx, cfg),
    );
    (sim, recorder, entities)
}

fn hibernated(recorder: &Recorder) -> bool {
    recorder.events().iter().any(|e| {
        matches!(
            e.as_app::<EntityEvent>(),
            Some(EntityEvent::Deactivated { .. })
        )
    })
}

// --- Tests --------------------------------------------------------------------

#[test]
fn steps_run_sleep_and_finish() {
    let fx = Effects::default();
    let (sim, _rec, entities) = rig(
        1,
        Duration::from_secs(3600),
        fx.clone(),
        PipelineConfig::default(),
    );
    // The touch that starts it: the entity drives from `on_activate`, so a read is
    // enough and nothing has to succeed before the first step commits.
    let g = entities.entity("p/0");
    sim.block_on(async move {
        let _ = g.ask(ReadMemo).await;
    });
    // The read returns at t=0; the workflow drives on its own timers.
    sim.run();

    let g = entities.entity("p/0");
    let result = sim.block_on(async move { g.ask(Read).await.expect("read") });
    assert_eq!(result, Some(2), "fetch(1) → sleep → double = 2");
    assert_eq!(
        fx.launches("p/0", STEP_FETCH),
        1,
        "a calm run launches the fetch effect once",
    );
}

#[test]
fn steps_memoize_across_hibernation() {
    // Aggressive idle window: the entity hibernates in every gap the workflow
    // allows (the alarm veto holds during the sleep, so the gap is after Finished),
    // and the memo must still carry completed steps across the re-activations the
    // drive triggers. The witness is the terminal value: `finished` is twice
    // whatever `fetch` first committed, so a re-launch that overwrote the memo
    // would carry a later ordinal into it.
    let fx = Effects::default();
    let (sim, recorder, entities) = rig(
        2,
        Duration::from_millis(5),
        fx.clone(),
        PipelineConfig::default(),
    );
    let g = entities.entity("p/0");
    sim.block_on(async move {
        let _ = g.ask(ReadMemo).await;
    });
    sim.run();

    let g = entities.entity("p/0");
    let result = sim.block_on(async move { g.ask(Read).await.expect("read") });
    assert_eq!(result, Some(2));
    assert!(
        hibernated(&recorder),
        "the aggressive idle window must have hibernated the entity at least once",
    );
    let g = entities.entity("p/0");
    let memo = sim.block_on(async move { g.ask(ReadMemo).await.expect("read") });
    assert_eq!(
        memo,
        Some(1),
        "the memo still holds the first committed result after the re-activations",
    );
}

#[test]
fn a_relaunched_step_does_not_overwrite_the_memo() {
    // The write-once rule, driven rather than waited for. The effect takes time and
    // the entity may hibernate under it, so the second touch below re-activates an
    // entity whose `fetch` is still outstanding: the drive that follows finds an
    // unresolved step and launches a *second* effect carrying a different ordinal.
    // `complete_step` refuses the second result, so the memo (and the terminal
    // value derived from it) stay at the first.
    //
    // The second touch has to be a separate one: a `StepDone` re-activating the
    // entity is already in the mailbox ahead of the drive it triggers, so that path
    // commits the step before anything could re-launch it.
    //
    // The touches are **launched, not blocked on**: `block_on` drives the scheduler
    // to quiescence, so blocking on the first read would run the whole workflow to
    // completion (deadline and all) before the second read was issued, leaving no
    // in-flight window to interrupt.
    //
    // This is the shape `steps_swarm.rs` sweeps under the nemesis; here it is
    // pinned on one seed with no faults at all, so a failure names an interleaving
    // rather than a distribution.
    let fx = Effects::default();
    let (sim, _rec, entities) = rig(
        4,
        Duration::from_millis(1),
        fx.clone(),
        PipelineConfig {
            sleep: None,
            hibernate_mid_workflow: true,
            effect_latency: Some(Duration::from_millis(200)),
        },
    );
    let touch = |sim: &Simulation, entities: &entity::Entities<Pipeline>| {
        let g = entities.entity("p/0");
        sim.spawner().launch(Box::pin(async move {
            let _ = g.ask(ReadMemo).await;
        }));
    };
    touch(&sim, &entities);
    // Past `idle_after`, well short of the effect's latency: the entity hibernates
    // with its step in flight.
    sim.run_for(Duration::from_millis(50));
    touch(&sim, &entities);
    sim.run_for(Duration::from_millis(50));
    // Both effects now land, the first winning the memo.
    sim.run();

    let g = entities.entity("p/0");
    let memo = sim
        .block_on(async move { g.ask(ReadMemo).await.expect("read") })
        .expect("the workflow committed its first step");
    let launches = fx.launches("p/0", STEP_FETCH);
    assert!(
        launches > 1,
        "the fixture did not re-launch, so this run did not exercise the rule \
         (launches={launches})",
    );
    assert_eq!(
        memo, 1,
        "a re-launched step overwrote the memo: {launches} launches drew ordinals \
         1..={launches}, and the memo must hold the one that committed first",
    );
    let g = entities.entity("p/0");
    let result = sim.block_on(async move { g.ask(Read).await.expect("read") });
    assert_eq!(
        result,
        Some(2),
        "the terminal value is twice the memo, so it moves with any overwrite",
    );
}

#[test]
fn launch_guard_claims_by_arbitrary_key() {
    // The guard is generic over the consumer's step key (the agent runtime keys
    // tool steps by the model's call id): claims are per-key, readable without
    // claiming, and reset restores idleness.
    let mut guard: LaunchGuard<String> = LaunchGuard::default();
    assert!(guard.is_idle(), "a fresh guard holds no claims");
    assert!(guard.claim("call-1-0".to_string()));
    assert!(!guard.claim("call-1-0".to_string()), "claim-once per key");
    assert!(guard.is_claimed(&"call-1-0".to_string()));
    assert!(
        !guard.is_claimed(&"call-2-0".to_string()),
        "is_claimed reads without claiming"
    );
    assert!(!guard.is_idle(), "an outstanding claim vetoes idleness");
    guard.reset();
    assert!(guard.is_idle());
    assert!(guard.claim("call-1-0".to_string()), "reset forgets claims");
}

#[test]
fn retry_relaunches_after_a_failed_step() {
    // The first fetch launch fails (records no result); the re-drive re-launches it
    // and the second attempt records its own ordinal. A failed launch draws no
    // ordinal, so the memo lands at 1 and the workflow still finishes at 2.
    let fx = Effects::default();
    fx.fail_next_fetch();
    let (sim, _rec, entities) = rig(
        3,
        Duration::from_secs(3600),
        fx.clone(),
        PipelineConfig::default(),
    );
    let g = entities.entity("p/0");
    sim.block_on(async move {
        let _ = g.ask(ReadMemo).await;
    });
    sim.run();

    let g = entities.entity("p/0");
    let result = sim.block_on(async move { g.ask(Read).await.expect("read") });
    assert_eq!(
        result,
        Some(2),
        "a failed step re-launches and the workflow completes"
    );
    assert_eq!(
        fx.launches("p/0", STEP_FETCH),
        1,
        "one successful fetch after the failed attempt"
    );
}
