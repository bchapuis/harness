//! A three-node leader cluster hosting `Fanout`, for the suites whose scenarios
//! are shard-leader moves: a crash between a launch and its settled record
//! (§4.4), a migration under an effect in flight (§6), and the fault swarm.
//!
//! Mirrors entity's `clustered_entities.rs` harness: SWIM plus a self-hosted Raft
//! control plane, two shards so names distribute across leader-election groups,
//! and a long idle window so nothing hibernates mid-scenario unless a test asks.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_core::EventSink;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::Recorder;
use actor_simulation::SimNetwork;
use actor_simulation::SimNode;
use actor_simulation::Simulation;
use actor_simulation::raft_config;
use actor_simulation::swim_config;
use entity::Entities;
use entity::EntityConfig;
use entity::EntityExt;
use entity::EntityHandler;

use super::FANOUT_TYPE;
use super::Fanout;
use super::Seam;

pub const A: NodeId = NodeId::new(1);
pub const B: NodeId = NodeId::new(2);
pub const C: NodeId = NodeId::new(3);

/// The shard groups' Raft timings, shared by the scenario cluster and the
/// swarm's `ClusterModeSpec` so the two tiers cannot drift apart.
/// Two shards, no idle hibernation within a scenario, and no snapshots: every
/// rehydration replays the journal, and `conformance_resume.rs`'s offline fold
/// reads every record through the public read path, which a snapshot would
/// truncate beneath it (entity §9).
pub fn config() -> EntityConfig {
    EntityConfig {
        shards: 2,
        idle_after: Duration::from_secs(600),
        snapshot_every: 0,
        ..EntityConfig::default()
    }
}

/// Host `Fanout` on `node` under `config`, every activation sharing `seam`.
pub fn host_with(node: &SimNode, seam: &Seam, config: EntityConfig) -> Entities<Fanout<SimNode>> {
    let seam = seam.clone();
    node.entities_named::<Fanout<SimNode>>(
        FANOUT_TYPE,
        config,
        Arc::new(move || Fanout::new(seam.clone())),
    )
}

/// Host `Fanout` on `node` under the scenario config.
pub fn host(node: &SimNode, seam: &Seam) -> Entities<Fanout<SimNode>> {
    host_with(node, seam, config())
}

/// A cluster the suites drive: the network (for faults), its nodes, and one
/// `Entities` handle per node, with every node's events on the recorder.
///
/// The first node is the anchor: every call is issued through its handle and it
/// is never restarted — the rule the swarm runner applies to its own nemesis
/// (`ClusterWorkload::rehost`) — so a scenario crashes a leader elsewhere
/// ([`key_led_elsewhere`](Cluster::key_led_elsewhere)).
pub struct Cluster {
    pub net: SimNetwork,
    pub nodes: Vec<SimNode>,
    pub hosts: Vec<Entities<Fanout<SimNode>>>,
}

/// Bring up the three nodes, elect the control-plane leader and each shard's
/// leader, and host `Fanout` everywhere.
pub fn cluster(sim: &Simulation, recorder: &Recorder, seam: &Seam) -> Cluster {
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let net = SimNetwork::new(sim)
        .with_leader(
            swim_config(),
            raft_config(vec![A, B, C]),
            DowningPolicy::Conservative,
        )
        .with_events(sink);
    let nodes = vec![net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let hosts: Vec<_> = nodes.iter().map(|n| host(n, seam)).collect();
    sim.run_for(Duration::from_secs(3)); // elect each shard group's leader
    Cluster { net, nodes, hosts }
}

impl Cluster {
    /// The anchor's handle: the first node, which no scenario restarts.
    pub fn anchor(&self) -> &Entities<Fanout<SimNode>> {
        &self.hosts[0]
    }

    /// The node currently leading `key`'s shard.
    pub fn leader_of(&self, key: &str) -> NodeId {
        self.anchor()
            .leader(key)
            .expect("the shard elected a leader")
    }

    /// An entity key under `prefix` whose shard is led by a node other than the
    /// anchor, so its leader can be crashed while the anchor keeps routing.
    pub fn key_led_elsewhere(&self, prefix: &str) -> String {
        (0..32)
            .map(|i| format!("{prefix}/{i}"))
            .find(|key| self.leader_of(key) != A)
            .expect("two shards across three nodes: some key is led off the first node")
    }

    /// Kill the process leading `key`'s shard — volatile state, tasks in flight,
    /// and all — and bring the node back empty, re-hosting `Fanout` on it. The
    /// returned handles replace the dead node's; every earlier clone of them is
    /// dead with the old process.
    pub fn crash_leader_of(&mut self, key: &str, seam: &Seam) -> NodeId {
        let leader = self.leader_of(key);
        let index = self
            .nodes
            .iter()
            .position(|n| n.node() == leader)
            .expect("the leader is one of ours");
        let fresh = self.net.restart(leader);
        self.hosts[index] = host(&fresh, seam);
        self.nodes[index] = fresh;
        leader
    }

    /// Ask `key`'s entity through the anchor, retrying across the window in which
    /// a restarted node's gateway is still coming up and a stale redirect names a
    /// leader that is gone (entity §5.4).
    ///
    /// Every attempt costs the same four seconds of virtual time whether it
    /// replies at once or is refused: the scenarios' crash points are tuned
    /// against that envelope, and a restart that lands a few seconds earlier in
    /// a shard group's life can leave the survivors disagreeing about its leader
    /// (see `conformance_orphan.rs` on seed 101). That is the cluster's to
    /// resolve, not the workflow's; the fixed step keeps the scenarios on the side of
    /// it they were written for.
    ///
    /// Retrying a write after an ambiguous failure may commit it twice, which is
    /// why `Fanout::apply` folds a repeated `Requested` idempotently (entity
    /// §2.2): the fixture is safe to re-ask by construction, not by luck.
    pub fn ask<M>(&self, sim: &Simulation, key: &str, msg: M) -> M::Reply
    where
        M: Message + Clone + Send + 'static,
        M::Reply: Send + 'static,
        Fanout<SimNode>: EntityHandler<M>,
    {
        let mut last = None;
        for _ in 0..10 {
            let entity = self.anchor().entity(key);
            let msg = msg.clone();
            let reply = drive(sim, Duration::from_secs(4), async move {
                entity.ask_timeout(msg, Duration::from_secs(3)).await
            });
            match reply {
                Ok(reply) => return reply,
                Err(err) => last = Some(err),
            }
            sim.run_for(Duration::from_secs(1));
        }
        panic!("the entity {key} refused every attempt: {last:?}");
    }
}

/// Engine a call for exactly `settle` of virtual time under the perpetually-running
/// cluster loops, then read it back. Unlike `Simulation::block_on` it does not
/// return early, which is what gives every step of a scenario the same envelope.
pub fn drive<T: Send + 'static>(
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
        .expect("future did not complete within the settle window")
}
