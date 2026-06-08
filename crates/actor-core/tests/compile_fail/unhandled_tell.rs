//! Invariant #20 (spec §3.3): `tell`-ing a message the actor has no `Handler`
//! for MUST NOT compile. `Greeter` handles `Hello` but not `Goodbye`.

use core::marker::PhantomData;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::Manifest;
use actor_core::Message;
use serde::Deserialize;
use serde::Serialize;

struct Greeter<S>(PhantomData<fn() -> S>);

impl<S: ActorSystem> Actor for Greeter<S> {
    type System = S;
}

#[derive(Serialize, Deserialize)]
struct Hello;
impl Message for Hello {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("cf.Hello");
}
impl<S: ActorSystem> Handler<Hello> for Greeter<S> {
    async fn handle(&mut self, _msg: Hello, _ctx: &Ctx<Self>) {}
}

#[derive(Serialize, Deserialize)]
struct Goodbye;
impl Message for Goodbye {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("cf.Goodbye");
}

// `Greeter` has no `Handler<Goodbye>`, so this `tell` must fail to compile.
fn must_not_compile<S: ActorSystem>(greeter: ActorRef<Greeter<S>>) {
    let _ = greeter.tell(Goodbye);
}

fn main() {}
