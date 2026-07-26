//! Conformance: linearizability of a **remote** register under the cluster
//! swarm (spec §18.4).
//!
//! The single-node scenarios in `conformance_linearizability.rs` prove the
//! checker has teeth and that a local register is linearizable. This is the
//! distributed claim: the same register, reached across the network, stays
//! linearizable while a seeded nemesis injects partitions, crashes, loss,
//! duplication, and delay — with unknown-outcome (`info`) operations carrying
//! the weight, since an `ask` that returns `Unreachable` may or may not have
//! taken effect.
//!
//! Split from the scenario file because this is a sweep: it runs at
//! `sweep_seeds` width and fails by naming a seed rather than a sequence
//! (docs/simulation-testing.md).

mod support;

use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Key;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::History;
use actor_simulation::Register;
use actor_simulation::SimNode;
use actor_simulation::check_linearizable;
use actor_simulation::run_cluster_swarm;
use actor_simulation::sweep_seeds;

use support::Cas;
use support::Read;
use support::RegisterActorIn;
use support::RegisterRef;
use support::Write;
use support::client;

// --- Cluster workload: a remote register under faults -------------------------

const REG: Key<RegisterActorIn<SimNode>> = Key::new("lin.register");

struct RemoteRegisterWorkload {
    nodes: usize,
    clients: usize,
    ops: u64,
}

#[derive(Clone)]
struct RemoteReg(actor_core::ActorRef<RegisterActorIn<SimNode>>);

impl RegisterRef for RemoteReg {
    fn read(&self) -> BoxFuture<'static, Result<i64, ()>> {
        let r = self.0.clone();
        Box::pin(async move {
            r.ask_timeout(Read, Duration::from_millis(500))
                .await
                .map_err(|_| ())
        })
    }
    fn write(&self, v: i64) -> BoxFuture<'static, Result<(), ()>> {
        let r = self.0.clone();
        Box::pin(async move {
            r.ask_timeout(Write(v), Duration::from_millis(500))
                .await
                .map_err(|_| ())
        })
    }
    fn cas(&self, old: i64, new: i64) -> BoxFuture<'static, Result<bool, ()>> {
        let r = self.0.clone();
        Box::pin(async move {
            r.ask_timeout(Cas(old, new), Duration::from_millis(500))
                .await
                .map_err(|_| ())
        })
    }
}

impl ClusterWorkload for RemoteRegisterWorkload {
    fn name(&self) -> &'static str {
        "linearizable-remote-register"
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
        // A single register lives on node 0 — the one linearizable object.
        let host = &ctx.nodes()[0];
        let reg = host.spawn(RegisterActorIn::<SimNode>::new());
        host.receptionist().register(REG, &reg);
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        // Clients run on the *other* nodes, so their calls cross the faulted
        // network: a drop/partition/crash surfaces as Unreachable/Timeout and is
        // recorded as a pending (info) op — exactly the unknown-outcome case the
        // checker must handle.
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        Box::pin(async move {
            let caller = nodes[0].clone();
            // Discover the register from a peer (location-transparent ref).
            let entropy = caller.entropy().clone();
            let clock = caller.clock().clone();
            // Let membership and registry replication settle so the lookup lands.
            clock.sleep(Duration::from_millis(400)).await;

            let history: History<Register> = History::new();
            let mut tasks = Vec::new();
            for c in 0..clients {
                let node = nodes[c % nodes.len()].clone();
                let history = history.clone();
                let entropy = entropy.clone();
                tasks.push(async move {
                    // Re-discover from this client's own node.
                    let listing = node.receptionist().lookup(REG);
                    if let Some(reg) = listing.iter().next() {
                        let reg = RemoteReg(reg.clone());
                        client(reg, history, entropy, ops).await;
                    }
                });
            }
            futures::future::join_all(tasks).await;

            // Whatever the faults did, the observed history must be linearizable:
            // unknown-outcome calls are pending ops the checker may place or drop.
            let verdict = check_linearizable(&history);
            assert!(
                verdict.is_ok(),
                "remote register history was not linearizable: {verdict:?}",
            );
        })
    }
}

#[test]
fn remote_register_is_linearizable_under_faults() {
    let workload = RemoteRegisterWorkload {
        nodes: 3,
        clients: 3,
        ops: 6,
    };
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..24)) {
        panic!("{failure}");
    }
}
