//! §7.12 / G10-for-storage: `ctx.kv()` on a grain that does not declare the
//! `Kv` facet must not compile — the accessor is gated by the compile-time
//! containment proof (`G::Facets: HasFacet<Kv, I>`).

use actor_core::Manifest;
use actor_core::Message;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use serde::Deserialize;
use serde::Serialize;

#[derive(Default)]
struct Plain;

#[derive(Default, Serialize, Deserialize)]
struct State;

#[derive(Serialize, Deserialize)]
enum Event {
    Happened,
}

impl Grain for Plain {
    type System = actor_simulation::SimSystem;
    type State = State;
    type Event = Event;
    type Facets = (); // no facets: no kv accessor
    const GRAIN_TYPE: &'static str = "test.Plain";

    fn apply(_state: &mut State, _event: &Event) {}
}

#[derive(Clone, Serialize, Deserialize)]
struct Touch;
impl Message for Touch {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.Touch");
}

impl GrainHandler<Touch> for Plain {
    async fn handle(
        &self,
        _state: &State,
        _msg: Touch,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Event>, ()) {
        let _ = ctx.kv(); // ERROR: `()` does not contain the Kv facet
        (vec![], ())
    }
}

fn main() {}
