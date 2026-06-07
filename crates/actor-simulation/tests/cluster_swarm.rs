//! Cluster swarm test (spec §18.3, §18.6): a discover-and-call workload swept
//! across seeds while a nemesis partitions and crashes nodes. The invariants —
//! no silent loss, serial execution, lifecycle, down-is-terminal — must hold on
//! every seed, and every call must complete (no hang).

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
use actor_simulation::ClusterWorkload;
use actor_simulation::SimCluster;
use actor_simulation::run_cluster_seed;
use actor_simulation::run_cluster_swarm;
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
    const MANIFEST: Manifest = Manifest::new("swarm.Greet");
}

impl Handler<Greet> for Greeter {
    async fn handle(&mut self, _msg: Greet, _ctx: &Ctx<Self>) -> String {
        "hello".into()
    }
}

const GREETERS: Key<Greeter> = Key::new("greeters");

/// Each node hosts and publishes a greeter; node 0 repeatedly discovers and
/// calls them, tolerating whatever failures the nemesis induces.
struct DiscoverAndCall {
    nodes: usize,
    rounds: u64,
}

impl ClusterWorkload for DiscoverAndCall {
    fn name(&self) -> &'static str {
        "discover-and-call"
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
                // Yield so replication and the nemesis make progress.
                clock.sleep(Duration::from_millis(200)).await;
                let listing = caller.receptionist().lookup(GREETERS);
                for service in listing.iter() {
                    // Every outcome (Ok / Unreachable / Timeout / DeadLetter) is
                    // acceptable; the invariant is that the call *completes*.
                    let _ = service.ask_timeout(Greet, Duration::from_millis(500)).await;
                }
            }
        })
    }
}

#[test]
fn discover_and_call_holds_across_seeds_under_faults() {
    let workload = DiscoverAndCall {
        nodes: 3,
        rounds: 12,
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..48) {
        panic!("{failure}");
    }
}

#[test]
fn a_cluster_seed_replays() {
    let workload = DiscoverAndCall {
        nodes: 3,
        rounds: 8,
    };
    assert!(run_cluster_seed(&workload, 123).is_ok());
    assert!(run_cluster_seed(&workload, 123).is_ok());
}
