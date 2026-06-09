//! Fault-injection coverage (spec §18.3).
//!
//! A swarm that *configures* faults but, by seed luck, never *triggers* one gives
//! false confidence: a green sweep that secretly only ever ran the happy path
//! proves much less than it appears to. So the cluster swarm tallies the faults
//! it actually exercised across its seed range, and this test asserts that every
//! fault type fired at least once. If a future change to the fault sampling or
//! the network silently stopped injecting (say) duplication, this test goes red
//! even though no invariant was violated — surfacing a coverage regression that
//! the invariant checks alone could never see.

use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Key;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::RegistryFaultPolicy;
use actor_simulation::SimCluster;
use actor_simulation::run_cluster_swarm_coverage;
use serde::Deserialize;
use serde::Serialize;

struct Greeter;

impl Actor for Greeter {
    type System = SimCluster;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Greet>();
    }
}

#[derive(Serialize, Deserialize)]
struct Greet;

impl Message for Greet {
    type Reply = String;
    const MANIFEST: Manifest = Manifest::new("cov.Greet");
}

impl Handler<Greet> for Greeter {
    async fn handle(&mut self, _msg: Greet, _ctx: &Ctx<Self>) -> String {
        "hi".into()
    }
}

const GREETERS: Key<Greeter> = Key::new("cov.greeters");

struct Chatter {
    nodes: usize,
    rounds: u64,
}

impl ClusterWorkload for Chatter {
    fn name(&self) -> &'static str {
        "cov-chatter"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(200),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::Gossip {
            swim: self.swim(),
            downing: DowningPolicy::Timeout(Duration::from_millis(300)),
        }
    }

    fn setup(&self, ctx: &ClusterCtx) {
        for node in ctx.nodes() {
            let greeter = node.spawn(Greeter);
            node.receptionist().register(GREETERS, &greeter);
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let caller = ctx.nodes()[0].clone();
        let rounds = self.rounds;
        Box::pin(async move {
            let clock = caller.clock().clone();
            for _ in 0..rounds {
                clock.sleep(Duration::from_millis(150)).await;
                for service in caller.receptionist().lookup(GREETERS).iter() {
                    let _ = service.ask_timeout(Greet, Duration::from_millis(500)).await;
                }
            }
        })
    }
}

#[test]
fn the_cluster_swarm_actually_exercises_every_fault_type() {
    let workload = Chatter {
        nodes: 3,
        rounds: 10,
    };
    // Sweep a range wide enough that the seed-sampled fault policy and the
    // nemesis between them trigger every kind of fault. The sweep also checks
    // invariants on each run, so this is coverage *and* correctness.
    let stats = match run_cluster_swarm_coverage(&workload, 0..64) {
        Ok(stats) => stats,
        Err(failure) => panic!("{failure}"),
    };

    assert!(
        stats.dropped > 0,
        "no frame was ever dropped across the sweep: {stats:?}",
    );
    assert!(
        stats.duplicated > 0,
        "no frame was ever duplicated across the sweep: {stats:?}",
    );
    assert!(
        stats.delayed > 0,
        "no frame was ever delayed/reordered across the sweep: {stats:?}",
    );
    assert!(
        stats.blocked > 0,
        "no frame was ever blocked by a partition/crash across the sweep: {stats:?}",
    );
}

/// The same chatter under the **registry-based** control plane (spec §9.4.2):
/// proves the registry-specific faults — outage windows opened by the nemesis,
/// stale snapshots served by the seeded policy — actually fired across the
/// sweep, on top of the transport faults (spec §18.3).
struct RegistryChatter {
    nodes: usize,
    rounds: u64,
}

impl ClusterWorkload for RegistryChatter {
    fn name(&self) -> &'static str {
        "cov-registry-chatter"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(200),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::Registry {
            swim: self.swim(),
            sync_interval: Duration::from_millis(100),
            faults: RegistryFaultPolicy {
                max_latency: Duration::from_millis(40),
                stale_num: 1,
                stale_den: 3,
                max_staleness: 4,
            },
        }
    }

    fn setup(&self, ctx: &ClusterCtx) {
        for node in ctx.nodes() {
            let greeter = node.spawn(Greeter);
            node.receptionist().register(GREETERS, &greeter);
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let caller = ctx.nodes()[0].clone();
        let registry = ctx.registry().expect("registry mode").clone();
        let victim = ctx.nodes()[1].node();
        let rounds = self.rounds;
        Box::pin(async move {
            let clock = caller.clock().clone();
            for round in 0..rounds {
                clock.sleep(Duration::from_millis(150)).await;
                // Churn the registry so stale reads have revisions to lag behind.
                if round % 2 == 0 {
                    registry.drain(victim);
                } else {
                    registry.resume(victim);
                }
                for service in caller.receptionist().lookup(GREETERS).iter() {
                    let _ = service.ask_timeout(Greet, Duration::from_millis(500)).await;
                }
            }
        })
    }
}

#[test]
fn the_registry_swarm_actually_exercises_registry_faults() {
    let workload = RegistryChatter {
        nodes: 3,
        rounds: 10,
    };
    let stats = match run_cluster_swarm_coverage(&workload, 0..64) {
        Ok(stats) => stats,
        Err(failure) => panic!("{failure}"),
    };

    assert!(
        stats.registry_outages > 0,
        "the nemesis never opened a registry outage across the sweep: {stats:?}",
    );
    assert!(
        stats.registry_stale > 0,
        "no stale registry snapshot was ever served across the sweep: {stats:?}",
    );
}
