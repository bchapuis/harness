//! The `CounterEntity` fixture: an event-sourced counter matching the
//! linearizability `Counter` model (entity §14).
//!
//! Shared by `entities.rs` (single-node scenarios) and `entity_swarm.rs` (the
//! sweeps), so the sweep exercises the *same* entity the scenarios specify: a
//! fixture that drifted between them would make the sweep stop covering what
//! the scenarios pin down.

use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::SimSystem;
use entity::Entity;
use entity::EntityCtx;
use entity::EntityHandler;
use serde::Deserialize;
use serde::Serialize;

// --- A counter entity matching the linearizability `Counter` model -------------

#[derive(Default)]
pub struct CounterEntity;

#[derive(Default, Serialize, Deserialize)]
pub struct CounterState {
    pub value: i64,
}

#[derive(Serialize, Deserialize)]
pub enum CounterEvent {
    Added(i64),
}

impl Entity for CounterEntity {
    type System = SimSystem;
    type State = CounterState;
    type Event = CounterEvent;
    type Facets = ();
    const ENTITY_TYPE: &'static str = "test.Counter";

    fn apply(state: &mut CounterState, event: &CounterEvent) {
        match event {
            CounterEvent::Added(d) => state.value += *d,
        }
    }

    fn register(r: &mut entity::EntityRegistry<Self>) {
        r.accept::<Add>();
        r.accept::<ReadCount>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Add(pub i64);
impl Message for Add {
    type Reply = i64; // the post-command value
    const MANIFEST: Manifest = Manifest::new("test.Add");
}

impl EntityHandler<Add> for CounterEntity {
    async fn handle(
        &self,
        state: &CounterState,
        msg: Add,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<CounterEvent>, i64) {
        // Non-idempotent: a double-fold shows up as a wrong Read, which the
        // linearizability checker catches (E2).
        (vec![CounterEvent::Added(msg.0)], state.value + msg.0)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ReadCount;
impl Message for ReadCount {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("test.ReadCount");
}

impl EntityHandler<ReadCount> for CounterEntity {
    async fn handle(
        &self,
        state: &CounterState,
        _msg: ReadCount,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<CounterEvent>, i64) {
        (vec![], state.value) // read path: no events, commits nothing (§7.5)
    }
}
