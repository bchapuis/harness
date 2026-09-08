//! Regression: hosting entities on a clustered system that has **no consensus
//! engine** must fail loud at construction, not silently at the first call.
//!
//! entity's `Quorum` tier rides Raft: a shard elects its leader through the system's
//! consensus engine, and only `MembershipMode::Leader` builds one. A clustered
//! deployment wired in any other mode (Static, Registry, Gossip) reports no
//! configured voters, so no shard ever elects: the gateway's redirect then hints
//! the receiving node back at itself and every entity call loops on `NotLeader`.
//! That was the exact symptom of the standalone harness booting in `Static` mode.
//!
//! The guard lives in `RaftShardMap::new` (the cluster shard-map path), so it
//! catches the whole class regardless of the deployment on top: here through the
//! public `entities()` entry point, the same call the standalone node makes.

use std::time::Duration;

use actor_simulation::SimNetwork;
use actor_simulation::SimNode;
use actor_simulation::Simulation;
use entity::Entity;
use entity::EntityConfig;
use entity::EntityExt;
use entity::EntityRegistry;
use serde::Deserialize;
use serde::Serialize;

const A: actor_core::NodeId = actor_core::NodeId::new(1);

#[derive(Default)]
struct Probe;

#[derive(Default, Serialize, Deserialize)]
struct Empty;

#[derive(Serialize, Deserialize)]
enum Never {}

impl Entity for Probe {
    type System = SimNode;
    type State = Empty;
    type Event = Never;
    type Facets = ();
    const ENTITY_TYPE: &'static str = "test.Probe";

    fn apply(_state: &mut Empty, _event: &Never) {}

    fn register(_r: &mut EntityRegistry<Self>) {}
}

/// A clustered system left in the default `Static` membership mode (no
/// `.with_leader(...)`) has no Raft engine. Hosting an entity on it must panic at
/// `entities()` with a message that names the fix, rather than returning a handle
/// whose every call would loop on `NotLeader`.
#[test]
#[should_panic(expected = "leader-based consensus")]
fn hosting_a_entity_without_consensus_panics_at_construction() {
    let sim = Simulation::new(1);
    // No `.with_leader(...)`: `SimNetwork::new` defaults to `MembershipMode::Static`.
    let system = SimNetwork::new(&sim).join(A);
    let config = EntityConfig {
        shards: 2,
        idle_after: Duration::from_secs(60),
        ..EntityConfig::default()
    };
    // Eager: `entities()` builds the shard map synchronously, so the guard fires
    // here; before any turn, any sim step, any network traffic.
    let _ = system.entities::<Probe>(config);
}
