//! Durable-alarm tests under deterministic simulation (facets §5.5).
//!
//! Drives the [`Alarm`] facet and the [`on_alarm`](Entity::on_alarm) seam through
//! the public API on the single-node `Local` tier: an armed deadline fires exactly
//! once with no caller present, is consumed on fire, honours cancel and re-arm, and
//! keeps its entity resident until it fires (no alarm index is wired here, so the
//! hibernation veto is unconditional; `alarm_index.rs` covers hibernation under an
//! acked registration).

use std::sync::Arc;
use std::time::Duration;

use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use entity::Entity;
use entity::EntityConfig;
use entity::EntityCtx;
use entity::EntityEvent;
use entity::EntityExt;
use entity::EntityHandler;
use entity::facet::Alarm;
use serde::Deserialize;
use serde::Serialize;

// --- An entity whose only state is how many times its alarm has fired -----------

#[derive(Default)]
struct AlarmEntity;

#[derive(Default, Serialize, Deserialize)]
struct AlarmState {
    fired: u64,
}

#[derive(Serialize, Deserialize)]
enum AlarmEvent {
    Fired,
}

impl Entity for AlarmEntity {
    type System = SimSystem;
    type State = AlarmState;
    type Event = AlarmEvent;
    type Facets = (Alarm,);
    const ENTITY_TYPE: &'static str = "test.Alarm";

    fn apply(state: &mut AlarmState, event: &AlarmEvent) {
        match event {
            AlarmEvent::Fired => state.fired += 1,
        }
    }

    fn register(r: &mut entity::EntityRegistry<Self>) {
        r.accept::<Arm>();
        r.accept::<Clear>();
        r.accept::<ReadFired>();
        r.accept::<ReadPending>();
    }

    // Fire once; return no re-arm, so the consume-on-fire cancel stands and the
    // alarm does not repeat (spec facets §5.5).
    async fn on_alarm(&self, _state: &AlarmState, _ctx: &EntityCtx<Self>) -> Vec<AlarmEvent> {
        vec![AlarmEvent::Fired]
    }

    // Regression net: activation has no command stage armed, so this read must
    // answer from the committed form rather than panic about command scope
    // (facets §5.5).
    fn on_activate(
        &mut self,
        ctx: &EntityCtx<Self>,
    ) -> impl std::future::Future<Output = Result<(), actor_core::BoxError>> + Send {
        let _ = ctx.alarm().pending();
        async { Ok(()) }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Arm {
    after_ms: u64,
}
impl Message for Arm {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.Arm");
}
impl EntityHandler<Arm> for AlarmEntity {
    async fn handle(
        &self,
        _state: &AlarmState,
        msg: Arm,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<AlarmEvent>, ()) {
        ctx.alarm().set_after(Duration::from_millis(msg.after_ms));
        (vec![], ())
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Clear;
impl Message for Clear {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.Clear");
}
impl EntityHandler<Clear> for AlarmEntity {
    async fn handle(
        &self,
        _state: &AlarmState,
        _msg: Clear,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<AlarmEvent>, ()) {
        ctx.alarm().clear();
        (vec![], ())
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadFired;
impl Message for ReadFired {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("test.ReadFired");
}
impl EntityHandler<ReadFired> for AlarmEntity {
    async fn handle(
        &self,
        state: &AlarmState,
        _msg: ReadFired,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<AlarmEvent>, u64) {
        (vec![], state.fired)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadPending;
impl Message for ReadPending {
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("test.ReadPending");
}
impl EntityHandler<ReadPending> for AlarmEntity {
    async fn handle(
        &self,
        _state: &AlarmState,
        _msg: ReadPending,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<AlarmEvent>, bool) {
        (vec![], ctx.alarm().pending().is_some())
    }
}

// --- Test rig -----------------------------------------------------------------

fn rig(seed: u64, idle_after: Duration) -> (Simulation, Recorder, entity::Entities<AlarmEntity>) {
    let sim = Simulation::new(seed);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let entities = system.entities::<AlarmEntity>(EntityConfig {
        idle_after,
        ..EntityConfig::default()
    });
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

fn activated(recorder: &Recorder) -> bool {
    recorder.events().iter().any(|e| {
        matches!(
            e.as_app::<EntityEvent>(),
            Some(EntityEvent::Activated { .. })
        )
    })
}

// --- Tests --------------------------------------------------------------------

#[test]
fn alarm_fires_once_past_due() {
    // A generous idle window so the entity does not hibernate before the alarm,
    // though the veto would prevent it anyway.
    let (sim, _rec, entities) = rig(1, Duration::from_secs(10));
    let g = entities.entity("a/0");
    sim.block_on(async move {
        g.ask(Arm { after_ms: 100 }).await.expect("arm commits");
    });

    // Advance virtual time past the deadline; the callerless timer delivers.
    sim.run();

    let g = entities.entity("a/0");
    let (fired, pending) = sim.block_on(async move {
        let fired = g.ask(ReadFired).await.expect("read");
        let pending = g.ask(ReadPending).await.expect("read");
        (fired, pending)
    });
    assert_eq!(
        fired, 1,
        "the alarm fires exactly once with no caller present"
    );
    assert!(!pending, "firing consumes the alarm (spec facets §5.5)");
}

#[test]
fn cleared_alarm_does_not_fire() {
    let (sim, _rec, entities) = rig(2, Duration::from_secs(10));
    let g = entities.entity("a/0");
    sim.block_on(async move {
        g.ask(Arm { after_ms: 100 }).await.expect("arm");
        g.ask(Clear).await.expect("clear"); // both commit at t=0, before the deadline
    });

    sim.run();

    let g = entities.entity("a/0");
    let fired = sim.block_on(async move { g.ask(ReadFired).await.expect("read") });
    assert_eq!(fired, 0, "a cancelled alarm never fires");
}

#[test]
fn re_arm_fires_latest_only() {
    let (sim, _rec, entities) = rig(3, Duration::from_secs(10));
    let g = entities.entity("a/0");
    sim.block_on(async move {
        g.ask(Arm { after_ms: 100 }).await.expect("arm");
        g.ask(Arm { after_ms: 300 }).await.expect("re-arm"); // supersedes the 100ms timer
    });

    sim.run();

    let g = entities.entity("a/0");
    let fired = sim.block_on(async move { g.ask(ReadFired).await.expect("read") });
    assert_eq!(
        fired, 1,
        "the superseded timer is ignored by the epoch guard; the alarm fires once",
    );
}

#[test]
fn pending_alarm_vetoes_hibernation() {
    use actor_core::Spawner;

    // Aggressive idle window, far-future alarm, and NO alarm index wired: with
    // nothing to wake it, the entity must stay resident so the in-activation timer
    // survives to fire it (the unconditional half of the veto, spec facets §5.5). Drive
    // with `launch` + `run_for`, not `block_on`: a vetoing entity never quiesces,
    // so `block_on` would run to the deadline instead of stopping at the bound.
    let (sim, recorder, entities) = rig(4, Duration::from_millis(10));
    sim.spawner().launch(Box::pin(async move {
        let _ = entities.entity("a/0").ask(Arm { after_ms: 10_000 }).await;
    }));

    // Bounded run well past many idle windows but before the 10s deadline.
    sim.run_for(Duration::from_secs(1));

    assert!(activated(&recorder), "the entity activated");
    assert!(
        !hibernated(&recorder),
        "a pending alarm must veto idle hibernation until it fires",
    );
}
