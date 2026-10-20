//! facets §4 / E10-for-storage: `ctx.kv()` on an entity that does not declare the
//! `Kv` facet must not compile; the accessor is gated by the compile-time
//! containment proof (`G::Facets: HasFacet<Kv, I>`).

use actor_core::Manifest;
use actor_core::Message;
use entity::Entity;
use entity::EntityCtx;
use entity::EntityHandler;
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

impl Entity for Plain {
    type System = actor_simulation::SimSystem;
    type State = State;
    type Event = Event;
    type Facets = (); // no facets: no kv accessor
    const ENTITY_TYPE: &'static str = "test.Plain";

    fn apply(_state: &mut State, _event: &Event) {}
}

#[derive(Clone, Serialize, Deserialize)]
struct Touch;
impl Message for Touch {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.Touch");
}

impl EntityHandler<Touch> for Plain {
    async fn handle(
        &self,
        _state: &State,
        _msg: Touch,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<Event>, ()) {
        let _ = ctx.kv(); // ERROR: `()` does not contain the Kv facet
        (vec![], ())
    }
}

fn main() {}
