//! G10: a grain that does not implement `GrainHandler<Unhandled>` must reject a
//! call carrying `Unhandled` — at compile time, via the `G: GrainHandler<M>`
//! bound on `GrainRef::ask`.

use actor_core::LocalSystem;
use actor_core::Manifest;
use actor_core::Message;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GrainRef;
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

impl Grain for Switch {
    type System = LocalSystem<
        actor_simulation::SimClock,
        actor_simulation::SimEntropy,
        actor_simulation::SimSpawner,
    >;
    type State = State;
    type Event = Event;
    type Facets = ();
    const GRAIN_TYPE: &'static str = "test.Switch";

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

impl GrainHandler<Toggle> for Switch {
    async fn handle(
        &self,
        _state: &State,
        _msg: Toggle,
        _ctx: &GrainCtx<Self>,
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
// unified with a lone impl) — making `Switch: GrainHandler<Unhandled>` the
// unsatisfied bound the error names.
impl GrainHandler<IsOn> for Switch {
    async fn handle(&self, state: &State, _msg: IsOn, _ctx: &GrainCtx<Self>) -> (Vec<Event>, bool) {
        (vec![], state.on)
    }
}

// A command the `Switch` grain has no `GrainHandler` for. `Clone` so the only
// unsatisfied bound on `ask` is `GrainHandler` (the G10 check), not `M: Clone`.
#[derive(Clone, Serialize, Deserialize)]
struct Unhandled;
impl Message for Unhandled {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.Unhandled");
}

async fn nope(switch: GrainRef<Switch>) {
    // Must not compile: `Switch: GrainHandler<Unhandled>` is unsatisfied.
    let _ = switch.ask(Unhandled).await;
}

fn main() {}
