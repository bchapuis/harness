//! Per-shard alarm index + driver tests (granary §16): the callerless-across-
//! failover half of durable alarms.
//!
//! Covers the three mechanisms that compose it: the [`AlarmIndex`] grain's
//! register/clear/query behaviour, a host **registering** its pending deadline in
//! the index as it arms (`granary_with_alarms`), and the per-type **driver**
//! re-activating an indexed grain that is no longer resident — the reactivation a
//! new leader performs after a failover, exercised here on the `Local` tier by
//! letting a grain hibernate and driving the sweep.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::Spawner;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use granary::Alarm;
use granary::AlarmIndex;
use granary::AlarmSync;
use granary::AllPending;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainEvent;
use granary::GrainHandler;
use granary::GrainName;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::index_key;
use granary::shard_for;
use serde::Deserialize;
use serde::Serialize;

const SHARDS: usize = 4;

// --- A minimal alarm-bearing grain -------------------------------------------

#[derive(Default)]
struct Timer;

#[derive(Default, Serialize, Deserialize)]
struct TimerState {
    fired: u64,
}

#[derive(Serialize, Deserialize)]
enum TimerEvent {
    Fired,
}

impl Grain for Timer {
    type System = SimSystem;
    type State = TimerState;
    type Event = TimerEvent;
    type Facets = (Alarm,);
    const GRAIN_TYPE: &'static str = "test.Timer";

    fn apply(state: &mut TimerState, event: &TimerEvent) {
        match event {
            TimerEvent::Fired => state.fired += 1,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Arm>();
        r.accept::<ReadFired>();
    }

    async fn on_alarm(&self, _s: &TimerState, _ctx: &GrainCtx<Self>) -> Vec<TimerEvent> {
        vec![TimerEvent::Fired]
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Arm {
    after_ms: u64,
}
impl Message for Arm {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.timer.Arm");
}
impl GrainHandler<Arm> for Timer {
    async fn handle(&self, _s: &TimerState, m: Arm, ctx: &GrainCtx<Self>) -> (Vec<TimerEvent>, ()) {
        ctx.alarm().set_after(Duration::from_millis(m.after_ms));
        (vec![], ())
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadFired;
impl Message for ReadFired {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("test.timer.ReadFired");
}
impl GrainHandler<ReadFired> for Timer {
    async fn handle(&self, s: &TimerState, _m: ReadFired, _ctx: &GrainCtx<Self>) -> (Vec<TimerEvent>, u64) {
        (vec![], s.fired)
    }
}

// --- A plain grain the driver can re-activate from the index ------------------

#[derive(Default)]
struct Poke;

impl Grain for Poke {
    type System = SimSystem;
    type State = TimerState;
    type Event = TimerEvent;
    type Facets = ();
    const GRAIN_TYPE: &'static str = "test.Poke";

    fn apply(_s: &mut TimerState, _e: &TimerEvent) {}

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Touch>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Touch;
impl Message for Touch {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.poke.Touch");
}
impl GrainHandler<Touch> for Poke {
    async fn handle(&self, _s: &TimerState, _m: Touch, _ctx: &GrainCtx<Self>) -> (Vec<TimerEvent>, ()) {
        (vec![], ())
    }
}

// --- Helpers ------------------------------------------------------------------

/// Ask a grain off the main thread and drain the short chain of ready work, so a
/// query completes without running the whole sim to quiescence (which a pending
/// alarm's hibernation veto would forbid). Returns the reply.
fn ask<G, M>(sim: &Simulation, g: granary::GrainRef<G>, msg: M) -> M::Reply
where
    G: Grain<System = SimSystem> + GrainHandler<M>,
    M: Message + Clone + Send + 'static,
    M::Reply: Send + 'static,
{
    let cell: Arc<Mutex<Option<M::Reply>>> = Arc::new(Mutex::new(None));
    let out = Arc::clone(&cell);
    sim.spawner().launch(Box::pin(async move {
        if let Ok(reply) = g.ask(msg).await {
            *out.lock().unwrap() = Some(reply);
        }
    }));
    sim.run_for(Duration::from_millis(1));
    cell.lock().unwrap().take().expect("ask completed")
}

fn activations_of(recorder: &Recorder, key: &str) -> usize {
    recorder
        .events()
        .iter()
        .filter(|e| {
            matches!(e.as_app::<GrainEvent>(), Some(GrainEvent::Activated { name, .. }) if name.key() == key)
        })
        .count()
}

// --- Tests --------------------------------------------------------------------

#[test]
fn alarm_index_registers_clears_and_queries() {
    let sim = Simulation::new(1);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let index = system.granary::<AlarmIndex<SimSystem>>(GranaryConfig {
        shards: SHARDS,
        ..GranaryConfig::default()
    });

    let a = GrainName::new("test.Timer", "a");
    let b = GrainName::new("test.Timer", "b");
    let all = sim.block_on({
        let idx = index.grain("test.Timer/0");
        let (a, b) = (a.clone(), b.clone());
        async move {
            idx.ask(AlarmSync { grain: a.clone(), due: Some(100), head: 1 }).await.unwrap();
            idx.ask(AlarmSync { grain: b.clone(), due: Some(300), head: 1 }).await.unwrap();
            // A stale lower-head clear must NOT drop a live entry.
            idx.ask(AlarmSync { grain: a.clone(), due: None, head: 0 }).await.unwrap();
            // A current clear removes b.
            idx.ask(AlarmSync { grain: b, due: None, head: 2 }).await.unwrap();
            idx.ask(AllPending).await.unwrap()
        }
    });
    assert_eq!(all, vec![(a, 100)], "b cleared at a higher head; a survived a stale clear");
}

#[test]
fn host_registers_pending_alarm_in_the_index() {
    let sim = Simulation::new(2);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let index = system.granary::<AlarmIndex<SimSystem>>(GranaryConfig {
        shards: SHARDS,
        ..GranaryConfig::default()
    });
    let timers = system.granary_with_alarms::<Timer>(
        GranaryConfig { shards: SHARDS, ..GranaryConfig::default() },
        index.clone(),
    );

    // Arm a far-future alarm; the host registers it in the index as it arms.
    ask(&sim, timers.grain("t/0"), Arm { after_ms: 10_000 });

    let shard = shard_for("test.Timer", "t/0", SHARDS).index as usize;
    let pending = ask(&sim, index.grain(index_key("test.Timer", shard)), AllPending);
    assert_eq!(
        pending,
        vec![(GrainName::new("test.Timer", "t/0"), 10_000_000_000)],
        "arming registers the grain's deadline (10s in ns) in its shard's index",
    );
}

#[test]
fn driver_reactivates_an_indexed_grain() {
    let sim = Simulation::new(3);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let index = system.granary::<AlarmIndex<SimSystem>>(GranaryConfig {
        shards: SHARDS,
        ..GranaryConfig::default()
    });
    // A short idle window so the grain hibernates once touched — standing in for the
    // passivation a failover forces. The driver must bring it back from the index.
    let pokes = system.granary_with_alarms::<Poke>(
        GranaryConfig { shards: SHARDS, idle_after: Duration::from_millis(10), ..GranaryConfig::default() },
        index.clone(),
    );

    // Activate then let it hibernate.
    ask(&sim, pokes.grain("p/0"), Touch);
    sim.run_for(Duration::from_millis(100));
    assert_eq!(activations_of(&recorder, "p/0"), 1, "activated once, then hibernated");

    // Register it as due (deadline 0, already past) in its shard's index, as a host
    // would have before its leader died.
    let shard = shard_for("test.Poke", "p/0", SHARDS).index as usize;
    ask(
        &sim,
        index.grain(index_key("test.Poke", shard)),
        AlarmSync { grain: GrainName::new("test.Poke", "p/0"), due: Some(0), head: 1 },
    );

    // Let the driver sweep (its cadence is 500ms): it reads the index and re-activates.
    sim.run_for(Duration::from_millis(1200));
    assert!(
        activations_of(&recorder, "p/0") >= 2,
        "the driver re-activated the indexed grain with no caller (got {} activations)",
        activations_of(&recorder, "p/0"),
    );
}
