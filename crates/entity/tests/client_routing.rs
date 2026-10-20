//! Spike (§5.3/§5.4): an Orleans-style cluster **client** (a member that hosts
//! no entities and is not a Raft voter) addresses and calls an entity hosted on
//! the cluster, via `entities_client`. It proves a non-hosting participant
//! discovers a host's gateway through the receptionist gossip and routes an `ask`
//! to the shard leader, getting the same reply a host would.

mod support;

use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_core::ActorSystem;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_simulation::SimNetwork;
use actor_simulation::SimNode;
use actor_simulation::Simulation;
use actor_simulation::raft_config;
use actor_simulation::swim_config;
use entity::Entity;
use entity::EntityConfig;
use entity::EntityCtx;
use entity::EntityExt;
use entity::EntityHandler;
use entity::EntityRegistry;
use serde::Deserialize;
use serde::Serialize;
use support::drive;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);
/// The client's id: outside the Raft voter roster {A,B,C}, so it never votes.
const CLIENT: NodeId = NodeId::new(99);

// --- A minimal counter entity hosted on the cluster ----------------------------

#[derive(Default)]
struct Counter;

#[derive(Default, Serialize, Deserialize)]
struct CounterState {
    value: i64,
}

#[derive(Serialize, Deserialize)]
enum CounterEvent {
    Added(i64),
}

impl Entity for Counter {
    type System = SimNode;
    type State = CounterState;
    type Event = CounterEvent;
    type Facets = ();
    const ENTITY_TYPE: &'static str = "client.Counter";

    fn apply(state: &mut CounterState, event: &CounterEvent) {
        match event {
            CounterEvent::Added(d) => state.value += *d,
        }
    }

    fn register(r: &mut EntityRegistry<Self>) {
        r.accept::<Add>();
        r.accept::<Read>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Add(i64);
impl Message for Add {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("client.Add");
}
impl EntityHandler<Add> for Counter {
    async fn handle(
        &self,
        state: &CounterState,
        msg: Add,
        _: &EntityCtx<Self>,
    ) -> (Vec<CounterEvent>, i64) {
        (vec![CounterEvent::Added(msg.0)], state.value + msg.0)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Read;
impl Message for Read {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("client.Read");
}
impl EntityHandler<Read> for Counter {
    async fn handle(
        &self,
        state: &CounterState,
        _: Read,
        _: &EntityCtx<Self>,
    ) -> (Vec<CounterEvent>, i64) {
        (vec![], state.value)
    }
}

// --- Harness (mirrors tests/clustered_entities.rs) ------------------------------

fn config() -> EntityConfig {
    EntityConfig {
        shards: 2,
        idle_after: Duration::from_secs(60),
        snapshot_every: 8,
        ..EntityConfig::default()
    }
}

#[test]
fn a_non_hosting_client_routes_to_a_cluster_entity() {
    let sim = Simulation::new(1);
    let net = SimNetwork::new(&sim).with_leader(
        swim_config(),
        raft_config(vec![A, B, C]),
        DowningPolicy::Conservative,
    );

    // Hosts: a 3-node leader cluster hosting the Counter entity.
    let hosts = [net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let _granaries: Vec<_> = hosts
        .iter()
        .map(|s| s.entities::<Counter>(config()))
        .collect();
    sim.run_for(Duration::from_secs(3)); // elect each shard group's leader

    // The client: wired into everyone's membership view by `join`, but NOT in the
    // Raft voter roster {A,B,C} and never a host (it never calls `entity`).
    let client: SimNode = net.join(CLIENT);

    // Poll until the hosts' gateway has gossiped into the client's receptionist,
    // as a node waits for its peers before serving.
    let client_entities = {
        let mut found = None;
        for _ in 0..20 {
            if let Some(entity) = client.entities_client::<Counter>("client.Counter") {
                found = Some(entity);
                break;
            }
            sim.run_for(Duration::from_millis(500));
        }
        found.expect("the client discovered a host gateway via the receptionist gossip")
    };

    // From the non-hosting client: an Add (a write that must reach the shard
    // leader and commit), a Read, and a gateway-level event read (§7) that
    // never touches the activation.
    let (after_add, after_read, tailed) = drive(&sim, Duration::from_secs(8), async move {
        let counter = client_entities.entity("counter/1");
        let added = counter.ask(Add(7)).await;
        let read = counter.ask(Read).await;
        let tailed = counter.events(entity::Seq::ZERO, 16).await;
        (added, read, tailed)
    });

    assert_eq!(
        after_add,
        Ok(7),
        "the client's Add routed to the leader and committed"
    );
    assert_eq!(after_read, Ok(7), "the client reads the committed value");
    let tailed = tailed.expect("the client's event read routed to the leader's gateway");
    assert!(
        matches!(tailed.events.as_slice(), [(_, CounterEvent::Added(7))]),
        "the non-activating read returns the committed event over the wire",
    );
}

#[test]
fn ask_timeout_is_bounded_by_its_deadline_even_while_the_shard_cannot_elect() {
    // One node of a three-voter roster: no quorum, so neither the map group nor
    // any shard ever elects, and every resolution redirect-loops. The declared
    // deadline must bound the whole call, resolution and ask sharing one budget,
    // never a per-step window that stacks.
    let sim = Simulation::new(3);
    let net = SimNetwork::new(&sim).with_leader(
        swim_config(),
        raft_config(vec![A, B, C]),
        DowningPolicy::Conservative,
    );
    let a = net.join(A);
    sim.run_for(Duration::from_secs(1));
    let entities = a.entities::<Counter>(config());
    sim.run_for(Duration::from_secs(1));

    let system = a.clone();
    let (result, elapsed) = drive(&sim, Duration::from_secs(30), async move {
        let g = entities.entity("counter/1");
        let start = system.now();
        let result = g.ask_timeout(Add(1), Duration::from_millis(300)).await;
        (result, system.now().duration_since(start))
    });

    assert!(result.is_err(), "no leader can exist, so the ask must fail");
    assert!(
        elapsed >= Duration::from_millis(250),
        "the redirect loop should wait out most of the deadline, not fail fast: {elapsed:?}"
    );
    assert!(
        elapsed <= Duration::from_millis(400),
        "the 300ms deadline bounds the whole call, resolve and ask together: {elapsed:?}"
    );
}
