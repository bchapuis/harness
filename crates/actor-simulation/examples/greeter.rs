//! The Appendix A `Greeter`, run locally on a `LocalSystem` under the
//! deterministic simulator. Mirrors the spec's end-to-end example minus the
//! cluster: the call site `greeter.ask(Greet { .. })` is identical to what it
//! will be for a remote target.
//!
//! Run with: `cargo run --example greeter -p actor-simulation`

use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::LocalSystem;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::Simulation;
use serde::Deserialize;
use serde::Serialize;

// --- Define the actor (plain struct + trait impl, no macros) ---
struct Greeter {
    greeting: String,
}

impl Actor for Greeter {
    type System = LocalSystem<
        actor_simulation::SimClock,
        actor_simulation::SimEntropy,
        actor_simulation::SimSpawner,
    >;

    // --- List the messages Greeter accepts over the network (macro-free) ---
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Greet>();
    }
}

// --- Define a message (serde derive only; wire identity is a hand-written const) ---
#[derive(Serialize, Deserialize)]
struct Greet {
    name: String,
}

impl Message for Greet {
    type Reply = String;
    const MANIFEST: Manifest = Manifest::new("myapp.Greet");
}

impl Handler<Greet> for Greeter {
    async fn handle(&mut self, msg: Greet, _ctx: &Ctx<Self>) -> String {
        format!("{}, {}!", self.greeting, msg.name)
    }
}

fn main() {
    // One seed drives the whole run deterministically.
    let sim = Simulation::new(42);
    let system = LocalSystem::new(sim.clock(), sim.entropy(), sim.spawner());

    let reply = sim.block_on(async move {
        let greeter = system.spawn(Greeter {
            greeting: "Hello".into(),
        });
        // Identical call site whether `greeter` is local or remote.
        match greeter
            .ask(Greet {
                name: "world".into(),
            })
            .await
        {
            Ok(msg) => msg,
            Err(CallError::Unreachable) => "<peer down>".to_string(),
            Err(e) => format!("<call failed: {e}>"),
        }
    });

    println!("{reply}");
    assert_eq!(reply, "Hello, world!");
}
