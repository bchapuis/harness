//! Per-shard alarm index + driver tests (entity §16): the callerless-across-
//! failover half of durable alarms.
//!
//! Covers the mechanisms that compose it: the [`AlarmIndex`] entity's
//! register/clear/query behaviour, a host **registering** its pending deadline in
//! the index as it arms (`entities_with_alarms`), the per-type **driver**
//! re-activating an indexed entity that is no longer resident (the reactivation a
//! new leader performs after a failover, exercised here on the `Local` tier by
//! letting an entity hibernate and driving the sweep), and **alarm hibernation**:
//! an alarmed entity hibernates once (and only once) the index has acknowledged
//! its deadline, the driver waking it when the alarm falls due.

mod support;

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
use entity::AlarmIndex;
use entity::AlarmSync;
use entity::AllPending;
use entity::Entity;
use entity::EntityConfig;
use entity::EntityCtx;
use entity::EntityEvent;
use entity::EntityExt;
use entity::EntityHandler;
use entity::EntityName;
use entity::StoreAck;
use entity::Term;
use entity::index_key;
use entity::shard_for;
use entity::testing::StaticEntityStore;
use serde::Deserialize;
use serde::Serialize;

use support::timer::Arm;
use support::timer::ReadFired;
// `Poke` below reuses the timer's state and event types: it is an entity the driver
// re-activates, not one that fires, so its own vocabulary would be two empty types.
use support::timer::TimerEvent;
use support::timer::TimerState;

/// The `Timer` at this suite's tier.
type Timer = support::timer::Timer<SimSystem>;

const SHARDS: usize = 4;

// --- A plain entity the driver can re-activate from the index ------------------

#[derive(Default)]
struct Poke;

impl Entity for Poke {
    type System = SimSystem;
    type State = TimerState;
    type Event = TimerEvent;
    type Facets = ();
    const ENTITY_TYPE: &'static str = "test.Poke";

    fn apply(_s: &mut TimerState, _e: &TimerEvent) {}

    fn register(r: &mut entity::EntityRegistry<Self>) {
        r.accept::<Touch>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Touch;
impl Message for Touch {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.poke.Touch");
}
impl EntityHandler<Touch> for Poke {
    async fn handle(
        &self,
        _s: &TimerState,
        _m: Touch,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<TimerEvent>, ()) {
        (vec![], ())
    }
}

// --- Helpers ------------------------------------------------------------------

/// Ask an entity off the main thread and drain the short chain of ready work, so a
/// query completes without running the whole sim to quiescence (which a pending
/// alarm's hibernation veto would forbid). Returns the reply.
fn ask<G, M>(sim: &Simulation, g: entity::EntityRef<G>, msg: M) -> M::Reply
where
    G: Entity<System = SimSystem> + EntityHandler<M>,
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
            matches!(e.as_app::<EntityEvent>(), Some(EntityEvent::Activated { name, .. }) if name.key() == key)
        })
        .count()
}

fn deactivations_of(recorder: &Recorder, key: &str) -> usize {
    recorder
        .events()
        .iter()
        .filter(|e| {
            matches!(e.as_app::<EntityEvent>(), Some(EntityEvent::Deactivated { name, .. }) if name.key() == key)
        })
        .count()
}

// --- Tests --------------------------------------------------------------------

#[test]
fn alarm_index_registers_clears_and_queries() {
    let sim = Simulation::new(1);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let index = system.entities::<AlarmIndex<SimSystem>>(EntityConfig {
        shards: SHARDS,
        ..EntityConfig::default()
    });

    let a = EntityName::new("test.Timer", "a");
    let b = EntityName::new("test.Timer", "b");
    let all = sim.block_on({
        let idx = index.entity("test.Timer/0");
        let (a, b) = (a.clone(), b.clone());
        async move {
            idx.ask(AlarmSync {
                entity: a.clone(),
                due: Some(100),
                head: 1,
            })
            .await
            .unwrap();
            idx.ask(AlarmSync {
                entity: b.clone(),
                due: Some(300),
                head: 1,
            })
            .await
            .unwrap();
            // A stale lower-head clear must NOT drop a live entry.
            idx.ask(AlarmSync {
                entity: a.clone(),
                due: None,
                head: 0,
            })
            .await
            .unwrap();
            // A current clear removes b.
            idx.ask(AlarmSync {
                entity: b,
                due: None,
                head: 2,
            })
            .await
            .unwrap();
            idx.ask(AllPending).await.unwrap()
        }
    });
    assert_eq!(
        all,
        vec![(a, 100)],
        "b cleared at a higher head; a survived a stale clear"
    );
}

#[test]
fn host_registers_pending_alarm_in_the_index() {
    let sim = Simulation::new(2);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let index = system.entities::<AlarmIndex<SimSystem>>(EntityConfig {
        shards: SHARDS,
        ..EntityConfig::default()
    });
    let timers = system.entities_with_alarms::<Timer>(
        EntityConfig {
            shards: SHARDS,
            ..EntityConfig::default()
        },
        index.clone(),
    );

    // Arm a far-future alarm; the host registers it in the index as it arms.
    ask(&sim, timers.entity("t/0"), Arm { after_ms: 10_000 });

    let shard = shard_for("test.Timer", "t/0", SHARDS).index as usize;
    let pending = ask(
        &sim,
        index.entity(index_key("test.Timer", shard)),
        AllPending,
    );
    assert_eq!(
        pending,
        vec![(EntityName::new("test.Timer", "t/0"), 10_000_000_000)],
        "arming registers the entity's deadline (10s in ns) in its shard's index",
    );
}

#[test]
fn driver_reactivates_an_indexed_entity() {
    let sim = Simulation::new(3);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let index = system.entities::<AlarmIndex<SimSystem>>(EntityConfig {
        shards: SHARDS,
        ..EntityConfig::default()
    });
    // A short idle window so the entity hibernates once touched, standing in for the
    // deactivation a failover forces. The driver must bring it back from the index.
    let pokes = system.entities_with_alarms::<Poke>(
        EntityConfig {
            shards: SHARDS,
            idle_after: Duration::from_millis(10),
            ..EntityConfig::default()
        },
        index.clone(),
    );

    // Activate then let it hibernate.
    ask(&sim, pokes.entity("p/0"), Touch);
    sim.run_for(Duration::from_millis(100));
    assert_eq!(
        activations_of(&recorder, "p/0"),
        1,
        "activated once, then hibernated"
    );

    // Register it as due (deadline 0, already past) in its shard's index, as a host
    // would have before its leader died.
    let shard = shard_for("test.Poke", "p/0", SHARDS).index as usize;
    ask(
        &sim,
        index.entity(index_key("test.Poke", shard)),
        AlarmSync {
            entity: EntityName::new("test.Poke", "p/0"),
            due: Some(0),
            head: 1,
        },
    );

    // Let the driver sweep (its cadence is 500ms): it reads the index and re-activates.
    sim.run_for(Duration::from_millis(1200));
    assert!(
        activations_of(&recorder, "p/0") >= 2,
        "the driver re-activated the indexed entity with no caller (got {} activations)",
        activations_of(&recorder, "p/0"),
    );
}

#[test]
fn acked_registration_lets_an_alarmed_entity_hibernate_and_the_driver_fires_it() {
    // The alarm-hibernation half of §7.16: once the index has ACKED the deadline
    // (the registration ask returned from the index entity's commit), the standing
    // alarm no longer pins the entity resident. The entity hibernates well before
    // its deadline; the driver's sweep re-activates it when the alarm falls due,
    // and its own re-armed timer fires it, no caller at any point.
    let sim = Simulation::new(4);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let index = system.entities::<AlarmIndex<SimSystem>>(EntityConfig {
        shards: SHARDS,
        ..EntityConfig::default()
    });
    let timers = system.entities_with_alarms::<Timer>(
        EntityConfig {
            shards: SHARDS,
            idle_after: Duration::from_millis(50),
            ..EntityConfig::default()
        },
        index.clone(),
    );

    // Arm a 5s alarm; the host registers the deadline and the index acks it.
    ask(&sim, timers.entity("t/1"), Arm { after_ms: 5_000 });

    // Well before the deadline the entity hibernates: the ack lifted the veto.
    sim.run_for(Duration::from_secs(1));
    assert_eq!(activations_of(&recorder, "t/1"), 1, "activated once so far");
    assert_eq!(
        deactivations_of(&recorder, "t/1"),
        1,
        "the alarmed entity hibernated once the index acked its deadline",
    );

    // Cross the deadline with NO caller. The driver sweeps the index, re-activates
    // the entity, and its re-armed timer fires the alarm.
    sim.run_for(Duration::from_secs(6));
    assert!(
        activations_of(&recorder, "t/1") >= 2,
        "the driver re-activated the hibernated entity at its deadline (got {} activations)",
        activations_of(&recorder, "t/1"),
    );
    let fired = ask(&sim, timers.entity("t/1"), ReadFired);
    assert_eq!(
        fired, 1,
        "the alarm fired exactly once, callerlessly, across hibernation"
    );
}

#[test]
fn unacked_registration_keeps_an_alarmed_entity_resident() {
    // The safety half: with the index's every commit refused, no registration ask
    // can ever ack, so the host has no evidence the driver would wake the entity;
    // the veto must hold and the entity stay resident (where its own timer covers
    // the fire), however long it idles. The idle tick retries the registration;
    // each retry fails the same way, and none of that reaches the command path.
    let sim = Simulation::new(5);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let index = system.entities::<AlarmIndex<SimSystem>>(EntityConfig {
        shards: SHARDS,
        // An index whose registrations never acknowledge: reads succeed, so
        // activation gets far enough for the refusal to land at the commit, exactly
        // where it does in production.
        entity_store: Some(StaticEntityStore::factory(StoreAck::Fenced(Term::new(
            u64::MAX,
        )))),
        ..EntityConfig::default()
    });
    let timers = system.entities_with_alarms::<Timer>(
        EntityConfig {
            shards: SHARDS,
            idle_after: Duration::from_millis(50),
            ..EntityConfig::default()
        },
        index,
    );

    // Arm far beyond the run below, then idle across many eviction windows.
    ask(&sim, timers.entity("t/2"), Arm { after_ms: 60_000 });
    sim.run_for(Duration::from_secs(3));

    assert_eq!(
        activations_of(&recorder, "t/2"),
        1,
        "still the first activation"
    );
    assert_eq!(
        deactivations_of(&recorder, "t/2"),
        0,
        "an alarmed entity whose registration never acked must stay resident",
    );
}
