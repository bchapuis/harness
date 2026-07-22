//! Durable-alarm tests under deterministic simulation (granary §16).
//!
//! Drives the [`Alarm`] facet and the [`on_alarm`](Grain::on_alarm) seam through
//! the public API on the single-node `Local` tier: an armed deadline fires exactly
//! once with no caller present, is consumed on fire, honours cancel and re-arm, and
//! keeps its grain resident until it fires (no alarm index is wired here, so the
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
use granary::Alarm;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainEvent;
use granary::GrainHandler;
use granary::GranaryConfig;
use granary::GranaryExt;
use serde::Deserialize;
use serde::Serialize;

// --- A grain whose only state is how many times its alarm has fired -----------

#[derive(Default)]
struct AlarmGrain;

#[derive(Default, Serialize, Deserialize)]
struct AlarmState {
    fired: u64,
}

#[derive(Serialize, Deserialize)]
enum AlarmEvent {
    Fired,
}

impl Grain for AlarmGrain {
    type System = SimSystem;
    type State = AlarmState;
    type Event = AlarmEvent;
    type Facets = (Alarm,);
    const GRAIN_TYPE: &'static str = "test.Alarm";

    fn apply(state: &mut AlarmState, event: &AlarmEvent) {
        match event {
            AlarmEvent::Fired => state.fired += 1,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Arm>();
        r.accept::<Clear>();
        r.accept::<ReadFired>();
        r.accept::<ReadPending>();
    }

    // Fire once; return no re-arm, so the consume-on-fire cancel stands and the
    // alarm does not repeat (spec §16).
    async fn on_alarm(&self, _state: &AlarmState, _ctx: &GrainCtx<Self>) -> Vec<AlarmEvent> {
        vec![AlarmEvent::Fired]
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
impl GrainHandler<Arm> for AlarmGrain {
    async fn handle(
        &self,
        _state: &AlarmState,
        msg: Arm,
        ctx: &GrainCtx<Self>,
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
impl GrainHandler<Clear> for AlarmGrain {
    async fn handle(
        &self,
        _state: &AlarmState,
        _msg: Clear,
        ctx: &GrainCtx<Self>,
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
impl GrainHandler<ReadFired> for AlarmGrain {
    async fn handle(
        &self,
        state: &AlarmState,
        _msg: ReadFired,
        _ctx: &GrainCtx<Self>,
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
impl GrainHandler<ReadPending> for AlarmGrain {
    async fn handle(
        &self,
        _state: &AlarmState,
        _msg: ReadPending,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<AlarmEvent>, bool) {
        (vec![], ctx.alarm().pending().is_some())
    }
}

// --- Test rig -----------------------------------------------------------------

fn rig(seed: u64, idle_after: Duration) -> (Simulation, Recorder, granary::Granary<AlarmGrain>) {
    let sim = Simulation::new(seed);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let grains = system.granary::<AlarmGrain>(GranaryConfig {
        idle_after,
        ..GranaryConfig::default()
    });
    (sim, recorder, grains)
}

fn passivated(recorder: &Recorder) -> bool {
    recorder.events().iter().any(|e| {
        matches!(
            e.as_app::<GrainEvent>(),
            Some(GrainEvent::Passivated { .. })
        )
    })
}

fn activated(recorder: &Recorder) -> bool {
    recorder
        .events()
        .iter()
        .any(|e| matches!(e.as_app::<GrainEvent>(), Some(GrainEvent::Activated { .. })))
}

// --- Tests --------------------------------------------------------------------

#[test]
fn alarm_fires_once_past_due() {
    // A generous idle window so the grain does not hibernate before the alarm; the
    // veto would prevent it anyway, but this keeps the intent local to the test.
    let (sim, _rec, grains) = rig(1, Duration::from_secs(10));
    let g = grains.grain("a/0");
    sim.block_on(async move {
        g.ask(Arm { after_ms: 100 }).await.expect("arm commits");
    });

    // Advance virtual time past the deadline; the callerless timer delivers.
    sim.run();

    let g = grains.grain("a/0");
    let (fired, pending) = sim.block_on(async move {
        let fired = g.ask(ReadFired).await.expect("read");
        let pending = g.ask(ReadPending).await.expect("read");
        (fired, pending)
    });
    assert_eq!(
        fired, 1,
        "the alarm fires exactly once with no caller present"
    );
    assert!(!pending, "firing consumes the alarm (spec §16)");
}

#[test]
fn cleared_alarm_does_not_fire() {
    let (sim, _rec, grains) = rig(2, Duration::from_secs(10));
    let g = grains.grain("a/0");
    sim.block_on(async move {
        g.ask(Arm { after_ms: 100 }).await.expect("arm");
        g.ask(Clear).await.expect("clear"); // both commit at t=0, before the deadline
    });

    sim.run();

    let g = grains.grain("a/0");
    let fired = sim.block_on(async move { g.ask(ReadFired).await.expect("read") });
    assert_eq!(fired, 0, "a cancelled alarm never fires");
}

#[test]
fn re_arm_fires_latest_only() {
    let (sim, _rec, grains) = rig(3, Duration::from_secs(10));
    let g = grains.grain("a/0");
    sim.block_on(async move {
        g.ask(Arm { after_ms: 100 }).await.expect("arm");
        g.ask(Arm { after_ms: 300 }).await.expect("re-arm"); // supersedes the 100ms timer
    });

    sim.run();

    let g = grains.grain("a/0");
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
    // nothing to wake it, the grain must stay resident so the in-activation timer
    // survives to fire it (the unconditional half of the veto, spec §7.16). Drive
    // with `launch` + `run_for`, not `block_on`: a vetoing grain never quiesces, so
    // `block_on` (which runs to quiescence) would advance all the way to the
    // deadline instead of stopping at the bound.
    let (sim, recorder, grains) = rig(4, Duration::from_millis(10));
    sim.spawner().launch(Box::pin(async move {
        let _ = grains.grain("a/0").ask(Arm { after_ms: 10_000 }).await;
    }));

    // Bounded run well past many idle windows but before the 10s deadline.
    sim.run_for(Duration::from_secs(1));

    assert!(activated(&recorder), "the grain activated");
    assert!(
        !passivated(&recorder),
        "a pending alarm must veto idle hibernation until it fires",
    );
}
