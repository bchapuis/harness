//! Regression: hosting grains on a clustered system that has **no consensus
//! engine** must fail loud at construction, not silently at the first call.
//!
//! granary's `Quorum` tier rides Raft: a shard elects its leader through the system's
//! consensus engine, and only `MembershipMode::Leader` builds one. A clustered
//! deployment wired in any other mode (Static, Registry, Gossip) reports no
//! configured voters, so no shard ever elects — the gateway's redirect then hints
//! the receiving node back at itself and every grain call loops on `NotLeader`.
//! That was the exact symptom of the standalone harness booting in `Static` mode.
//!
//! The guard lives in `RaftShardMap::new` (the cluster shard-map path), so it
//! catches the whole class regardless of the deployment on top — here through the
//! public `granary()` entry point, the same call the standalone node makes.

use std::time::Duration;

use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use granary::Grain;
use granary::GrainRegistry;
use granary::GranaryConfig;
use granary::GranaryExt;
use serde::Deserialize;
use serde::Serialize;

const A: actor_core::NodeId = actor_core::NodeId::new(1);

#[derive(Default)]
struct Probe;

#[derive(Default, Serialize, Deserialize)]
struct Empty;

#[derive(Serialize, Deserialize)]
enum Never {}

impl Grain for Probe {
    type System = SimCluster;
    type State = Empty;
    type Event = Never;
    const GRAIN_TYPE: &'static str = "test.Probe";

    fn apply(_state: &mut Empty, _event: &Never) {}

    fn register(_r: &mut GrainRegistry<Self>) {}
}

/// A clustered system left in the default `Static` membership mode (no
/// `.with_leader(...)`) has no Raft engine. Hosting a grain on it must panic at
/// `granary()` with a message that names the fix, rather than returning a handle
/// whose every call would loop on `NotLeader`.
#[test]
#[should_panic(expected = "leader-based consensus")]
fn hosting_a_grain_without_consensus_panics_at_construction() {
    let sim = Simulation::new(1);
    // No `.with_leader(...)`: `SimNetwork::new` defaults to `MembershipMode::Static`.
    let system = SimNetwork::new(&sim).join(A);
    let config = GranaryConfig {
        shards: 2,
        idle_after: Duration::from_secs(60),
        ..GranaryConfig::default()
    };
    // Eager: `granary()` builds the shard map synchronously, so the guard fires
    // here — before any turn, any sim step, any network traffic.
    let _ = system.granary::<Probe>(config);
}
