//! E10: an entity that does not implement `EntityHandler<Unhandled>` must reject a
//! call carrying `Unhandled` — at compile time, via the `G: EntityHandler<M>`
//! bound on `EntityRef::ask`.

use actor_core::LocalSystem;
use actor_core::Manifest;
use actor_core::Message;
use entity::Entity;
use entity::EntityCtx;
use entity::EntityHandler;
use entity::EntityRef;
use serde::Deserialize;
use serde::Serialize;

#[derive(Default)]
struct Switch;

#[derive(Default, Serialize, Deserialize)]
struct State {
    on: bool,
}

#[derive(Serialize, Deserialize)]
enum Event {
    Toggled,
}

impl Entity for Switch {
    type System = LocalSystem<
        actor_simulation::SimClock,
        actor_simulation::SimEntropy,
        actor_simulation::SimSpawner,
    >;
    type State = State;
    type Event = Event;
    type Facets = ();
    const ENTITY_TYPE: &'static str = "test.Switch";

    fn apply(state: &mut State, _event: &Event) {
        state.on = !state.on;
    }
}

#[derive(Serialize, Deserialize)]
struct Toggle;
impl Message for Toggle {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.Toggle");
}

impl EntityHandler<Toggle> for Switch {
    async fn handle(
        &self,
        _state: &State,
        _msg: Toggle,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<Event>, ()) {
        (vec![Event::Toggled], ())
    }
}

#[derive(Serialize, Deserialize)]
struct IsOn;
impl Message for IsOn {
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("test.IsOn");
}

// A second handler, so the command type is inferred from the argument (not
// unified with a lone impl) — making `Switch: EntityHandler<Unhandled>` the
// unsatisfied bound the error names.
impl EntityHandler<IsOn> for Switch {
    async fn handle(&self, state: &State, _msg: IsOn, _ctx: &EntityCtx<Self>) -> (Vec<Event>, bool) {
        (vec![], state.on)
    }
}

// A command the `Switch` entity has no `EntityHandler` for. `Clone` so the only
// unsatisfied bound on `ask` is `EntityHandler` (the E10 check), not `M: Clone`.
#[derive(Clone, Serialize, Deserialize)]
struct Unhandled;
impl Message for Unhandled {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.Unhandled");
}

async fn nope(switch: EntityRef<Switch>) {
    // Must not compile: `Switch: EntityHandler<Unhandled>` is unsatisfied.
    let _ = switch.ask(Unhandled).await;
}

fn main() {}
