//! Invariant #20 (spec §3.3): `ask`-ing a message the actor has no `Handler`
//! for MUST NOT compile. `Greeter` handles `Hello` but not `Question`.

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
struct Question;
impl Message for Question {
    type Reply = u32;
    const MANIFEST: Manifest = Manifest::new("cf.Question");
}

// `Greeter` has no `Handler<Question>`, so this `ask` must fail to compile.
fn must_not_compile<S: ActorSystem>(greeter: ActorRef<Greeter<S>>) {
    let _ = greeter.ask(Question);
}

fn main() {}
