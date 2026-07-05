//! Spike (§5.3/§5.4): an Orleans-style cluster **client** — a member that hosts
//! no grains and is not a Raft voter — addresses and calls a grain hosted on the
//! cluster, via `granary_client`. This is the linchpin of the gateway-as-client
//! architecture: it proves a non-hosting participant discovers a host's gateway
//! through the receptionist gossip and routes an `ask` to the shard leader,
//! getting the same reply a host would.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GrainRegistry;
use granary::GranaryConfig;
use granary::GranaryExt;
use serde::Deserialize;
use serde::Serialize;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);
/// The client's id — outside the Raft voter roster {A,B,C}, so it never votes.
const CLIENT: NodeId = NodeId::new(99);

// --- A minimal counter grain hosted on the cluster ----------------------------

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

impl Grain for Counter {
    type System = SimCluster;
    type State = CounterState;
    type Event = CounterEvent;
    type Facets = ();
    const GRAIN_TYPE: &'static str = "client.Counter";

    fn apply(state: &mut CounterState, event: &CounterEvent) {
        match event {
            CounterEvent::Added(d) => state.value += *d,
        }
    }

    fn register(r: &mut GrainRegistry<Self>) {
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
impl GrainHandler<Add> for Counter {
    async fn handle(
        &self,
        state: &CounterState,
        msg: Add,
        _: &GrainCtx<Self>,
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
impl GrainHandler<Read> for Counter {
    async fn handle(
        &self,
        state: &CounterState,
        _: Read,
        _: &GrainCtx<Self>,
    ) -> (Vec<CounterEvent>, i64) {
        (vec![], state.value)
    }
}

// --- Harness (mirrors tests/clustered_grains.rs) ------------------------------

fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

fn raft() -> RaftConfig {
    let mut config = RaftConfig::new(vec![A, B, C]);
    config.election_timeout = Duration::from_millis(500);
    config.heartbeat_interval = Duration::from_millis(100);
    config
}

fn config() -> GranaryConfig {
    GranaryConfig {
        shards: 2,
        idle_after: Duration::from_secs(60),
        snapshot_every: 8,
        ..GranaryConfig::default()
    }
}

fn drive<T: Send + 'static>(
    sim: &Simulation,
    settle: Duration,
    future: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    let cell: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let out = Arc::clone(&cell);
    sim.spawner().launch(Box::pin(async move {
        *out.lock().unwrap() = Some(future.await);
    }));
    sim.run_for(settle);
    cell.lock()
        .unwrap()
        .take()
        .expect("future did not complete")
}

#[test]
fn a_non_hosting_client_routes_to_a_cluster_grain() {
    let sim = Simulation::new(1);
    let net = SimNetwork::new(&sim).with_leader(swim(), raft(), DowningPolicy::Conservative);

    // Hosts: a 3-node leader cluster hosting the Counter grain.
    let hosts = [net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let _granaries: Vec<_> = hosts
        .iter()
        .map(|s| s.granary::<Counter>(config()))
        .collect();
    sim.run_for(Duration::from_secs(3)); // elect each shard group's leader

    // The client: wired into everyone's membership view by `join`, but NOT in the
    // Raft voter roster {A,B,C} and never a host (it never calls `granary`).
    let client: SimCluster = net.join(CLIENT);

    // Poll until the hosts' gateway has gossiped into the client's receptionist —
    // exactly as a node waits for its peers before serving.
    let client_granary = {
        let mut found = None;
        for _ in 0..20 {
            if let Some(granary) = client.granary_client::<Counter>("client.Counter", 2) {
                found = Some(granary);
                break;
            }
            sim.run_for(Duration::from_millis(500));
        }
        found.expect("the client discovered a host gateway via the receptionist gossip")
    };

    // Route an Add (a write that must reach the shard leader and commit) and a
    // Read, from the non-hosting client.
    let (after_add, after_read) = drive(&sim, Duration::from_secs(8), async move {
        let counter = client_granary.grain("counter/1");
        let added = counter.ask(Add(7)).await;
        let read = counter.ask(Read).await;
        (added, read)
    });

    assert_eq!(
        after_add,
        Ok(7),
        "the client's Add routed to the leader and committed"
    );
    assert_eq!(after_read, Ok(7), "the client reads the committed value");
}

#[test]
fn ask_timeout_is_bounded_by_its_deadline_even_while_the_shard_cannot_elect() {
    // One node of a three-voter roster: no quorum, so neither the map group nor
    // any shard ever elects, and every resolution redirect-loops. The declared
    // deadline must bound the WHOLE call — resolution and ask, across both
    // dispatch attempts share one budget — never a per-step window that stacks.
    let sim = Simulation::new(3);
    let net = SimNetwork::new(&sim).with_leader(swim(), raft(), DowningPolicy::Conservative);
    let a = net.join(A);
    sim.run_for(Duration::from_secs(1));
    let granary = a.granary::<Counter>(config());
    sim.run_for(Duration::from_secs(1));

    let system = a.clone();
    let (result, elapsed) = drive(&sim, Duration::from_secs(30), async move {
        use granary::GranarySystem;
        let g = granary.grain("counter/1");
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
