//! The in-memory simulated network (spec §7, §18.2, §18.3).
//!
//! [`SimNetwork`] routes [`Frame`]s between [`ClusterSystem`] nodes running on
//! one simulation, implementing the real [`Transport`] trait — so a simulated
//! cluster runs the real routing, dispatch, codec, and failure detection, with
//! only the wire in-memory. It also injects faults under seed control (spec
//! §18.3): [`partition`](SimNetwork::partition), [`crash`](SimNetwork::crash),
//! and [`heal`](SimNetwork::heal) drop frames on blocked directed pairs, which
//! the SWIM detector then observes as unreachability.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::Authorizer;
use actor_cluster::ClusterConfig;
use actor_cluster::ClusterSystem;
use actor_cluster::Frame;
use actor_cluster::SwimConfig;
use actor_cluster::Transport;
use actor_cluster::TransportError;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::EventSink;
use actor_core::Instant;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_serialization::Codec;
use actor_serialization::JsonCodec;

use crate::SimClock;
use crate::SimEntropy;
use crate::SimSpawner;
use crate::Simulation;
use crate::coverage::FaultCounters;
use crate::coverage::FaultStats;
use crate::faults::FaultPolicy;

/// A cluster node running under the simulator.
pub type SimCluster = ClusterSystem<SimClock, SimEntropy, SimSpawner, SimTransport>;

struct NetInner {
    /// Each node's inbound frame sender (its receive loop holds the receiver).
    nodes: BTreeMap<NodeId, async_channel::Sender<(NodeId, Frame)>>,
    /// Directed pairs whose frames are dropped (partitions and crashes).
    blocked: BTreeSet<(NodeId, NodeId)>,
    /// Last scheduled delivery time per directed pair, kept strictly increasing
    /// so per-pair FIFO survives latency jitter (spec §7.2, invariant #3).
    pair_clock: BTreeMap<(NodeId, NodeId), Instant>,
    /// Joined systems, so a new node can be wired into every roster.
    joined: Vec<SimCluster>,
}

/// An in-memory network shared by the nodes of one simulation (spec §18.2).
#[derive(Clone)]
pub struct SimNetwork {
    inner: Arc<Mutex<NetInner>>,
    clock: SimClock,
    entropy: SimEntropy,
    spawner: SimSpawner,
    mailbox_capacity: usize,
    swim: Option<SwimConfig>,
    events: Arc<dyn EventSink>,
    authorizer: Option<Arc<dyn Authorizer>>,
    faults: FaultPolicy,
    stats: Arc<FaultCounters>,
}

impl SimNetwork {
    /// Create a network backed by a simulation's runtime seam (SWIM off,
    /// no faults, no-op observability).
    pub fn new(sim: &Simulation) -> SimNetwork {
        SimNetwork {
            inner: Arc::new(Mutex::new(NetInner {
                nodes: BTreeMap::new(),
                blocked: BTreeSet::new(),
                pair_clock: BTreeMap::new(),
                joined: Vec::new(),
            })),
            clock: sim.clock(),
            entropy: sim.entropy(),
            spawner: sim.spawner(),
            mailbox_capacity: 64,
            swim: None,
            events: Arc::new(()),
            authorizer: None,
            faults: FaultPolicy::default(),
            stats: Arc::new(FaultCounters::default()),
        }
    }

    /// A snapshot of the faults this network has exercised so far (spec §18.3).
    pub fn fault_stats(&self) -> FaultStats {
        self.stats.snapshot()
    }

    /// Enable seed-controlled transport faults (spec §18.3).
    pub fn with_faults(mut self, faults: FaultPolicy) -> SimNetwork {
        self.faults = faults;
        self
    }

    /// Enable SWIM failure detection on every node of this network (spec §10).
    pub fn with_swim(mut self, config: SwimConfig) -> SimNetwork {
        self.swim = Some(config);
        self
    }

    /// Gate inbound messages on every node with `authorizer` (spec §15).
    pub fn with_authorizer(mut self, authorizer: Arc<dyn Authorizer>) -> SimNetwork {
        self.authorizer = Some(authorizer);
        self
    }

    /// Route every node's events to `events` (spec §16).
    pub fn with_events(mut self, events: Arc<dyn EventSink>) -> SimNetwork {
        self.events = events;
        self
    }

    /// Set the per-actor bounded mailbox capacity on every node (spec §6). A
    /// small capacity makes backpressure — `MailboxFull` on the inbound remote
    /// path (invariant #5) — observable in a test without flooding the default.
    pub fn with_mailbox_capacity(mut self, capacity: usize) -> SimNetwork {
        self.mailbox_capacity = capacity;
        self
    }

    /// Bring up a node's system on the network, registering it for routing.
    /// `joining` selects founding (`Up`) vs joiner (`Joining`) startup (spec §9.3).
    fn bring_up(&self, node: NodeId, joining: bool) -> SimCluster {
        let (tx, rx) = async_channel::unbounded();
        let transport = SimTransport {
            net: self.clone(),
            from: node,
        };
        let codec: Arc<dyn Codec> = Arc::new(JsonCodec);
        let config = ClusterConfig {
            codec,
            mailbox_capacity: self.mailbox_capacity,
            events: Arc::clone(&self.events),
            swim: self.swim,
            joining,
            authorizer: self.authorizer.clone(),
        };
        let system = ClusterSystem::start(
            node,
            self.clock.clone(),
            self.entropy.clone(),
            self.spawner.clone(),
            transport,
            rx,
            config,
        );
        self.inner
            .lock()
            .expect("network mutex poisoned")
            .nodes
            .insert(node, tx);
        system
    }

    /// Bring up a founding node and return its running system, wiring it into
    /// every existing node's roster as a full `Up` member (spec §9.3 join,
    /// pre-wired — the simple path when the whole roster is known up front).
    pub fn join(&self, node: NodeId) -> SimCluster {
        let system = self.bring_up(node, false);
        let mut inner = self.inner.lock().expect("network mutex poisoned");
        for existing in &inner.joined {
            existing.add_member(node);
            system.add_member(existing.node());
        }
        inner.joined.push(system.clone());
        system
    }

    /// Bring up a node as a *joiner* (spec §9.3): it starts `Joining` and is told
    /// only its `seeds`, which it contacts to gossip itself into the cluster. The
    /// cluster discovers it and the leader admits it to `Up` — no pre-wiring, so
    /// this exercises the real join protocol.
    pub fn join_seeded(&self, node: NodeId, seeds: &[NodeId]) -> SimCluster {
        let system = self.bring_up(node, true);
        for &seed in seeds {
            system.add_member(seed);
        }
        self.inner
            .lock()
            .expect("network mutex poisoned")
            .joined
            .push(system.clone());
        system
    }

    /// Sever communication between two groups of nodes (spec §18.3): frames on
    /// any cross pair are dropped, in both directions.
    pub fn partition(&self, side_a: &[NodeId], side_b: &[NodeId]) {
        let mut inner = self.inner.lock().expect("network mutex poisoned");
        for &a in side_a {
            for &b in side_b {
                inner.blocked.insert((a, b));
                inner.blocked.insert((b, a));
            }
        }
    }

    /// Isolate a node from every peer (spec §18.3) — a crash, as seen by the
    /// rest of the cluster: its frames are dropped in both directions.
    pub fn crash(&self, node: NodeId) {
        let mut inner = self.inner.lock().expect("network mutex poisoned");
        let others: Vec<NodeId> = inner.nodes.keys().copied().filter(|&n| n != node).collect();
        for other in others {
            inner.blocked.insert((node, other));
            inner.blocked.insert((other, node));
        }
    }

    /// Inject a raw frame directly into a node's inbound queue, bypassing
    /// routing — for negative tests that feed hostile or corrupt frames a
    /// well-behaved peer would never send (spec §5.4, §7.3). A no-op if the
    /// target node is unknown.
    pub fn inject(&self, from: NodeId, to: NodeId, frame: Frame) {
        let sender = {
            let inner = self.inner.lock().expect("network mutex poisoned");
            inner.nodes.get(&to).cloned()
        };
        if let Some(sender) = sender {
            let _ = sender.try_send((from, frame));
        }
    }

    /// Clear all partitions/crashes — the network heals (spec §9.2).
    pub fn heal(&self) {
        self.inner
            .lock()
            .expect("network mutex poisoned")
            .blocked
            .clear();
    }

    /// Route a frame from `from` to `to`. A blocked pair drops the frame
    /// silently (a partition is loss, not an error); an unknown node is
    /// unreachable. With no faults the push is synchronous and in-order; under a
    /// [`FaultPolicy`] the frame may be dropped, duplicated, or delayed, with
    /// per-pair delivery kept strictly ordered so per-pair FIFO survives (#3).
    fn route(&self, from: NodeId, to: NodeId, frame: Frame) -> Result<(), TransportError> {
        let sender = {
            let inner = self.inner.lock().expect("network mutex poisoned");
            if inner.blocked.contains(&(from, to)) {
                self.stats.record_blocked();
                return Ok(());
            }
            match inner.nodes.get(&to) {
                Some(sender) => sender.clone(),
                None => return Err(TransportError::Unreachable),
            }
        };

        if !self.faults.active() {
            return sender
                .try_send((from, frame))
                .map_err(|_| TransportError::Unreachable);
        }

        // Seeded loss (also models corruption / association loss): the node never
        // sees the frame, so it cannot be wedged by it (spec §7.3).
        if self
            .entropy
            .buggify(self.faults.drop_num, self.faults.drop_den)
        {
            self.stats.record_dropped();
            return Ok(());
        }

        // Seeded duplication (spec §18.3): the framework tolerates it; the caller
        // still sees a single outcome (§7.2).
        let copies = if self
            .entropy
            .buggify(self.faults.duplicate_num, self.faults.duplicate_den)
        {
            self.stats.record_duplicated();
            2
        } else {
            1
        };
        for _ in 0..copies {
            let deliver_at = self.reserve_pair_slot(from, to);
            if deliver_at > self.clock.now() {
                self.stats.record_delayed();
            }
            let now = self.clock.now();
            let clock = self.clock.clone();
            let sender = sender.clone();
            let frame = frame.clone();
            self.spawner.launch(Box::pin(async move {
                clock.sleep(deliver_at.duration_since(now)).await;
                let _ = sender.try_send((from, frame));
            }));
        }
        Ok(())
    }

    /// Reserve the next strictly-increasing delivery instant for `(from, to)`,
    /// applying seeded latency. Strict monotonicity is what preserves per-pair
    /// FIFO under jitter: later-sent frames never get an earlier delivery time.
    fn reserve_pair_slot(&self, from: NodeId, to: NodeId) -> Instant {
        let delay = if self.faults.max_latency.is_zero() {
            Duration::ZERO
        } else {
            let span = self.faults.max_latency.as_nanos() as u64 + 1;
            Duration::from_nanos(self.entropy.next_u64() % span)
        };
        let earliest = self.clock.now() + delay;
        let mut inner = self.inner.lock().expect("network mutex poisoned");
        let deliver_at = match inner.pair_clock.get(&(from, to)) {
            Some(last) => earliest.max(*last + Duration::from_nanos(1)),
            None => earliest,
        };
        inner.pair_clock.insert((from, to), deliver_at);
        deliver_at
    }
}

/// A [`Transport`] handle bound to one node's outbound side (spec §7).
#[derive(Clone)]
pub struct SimTransport {
    net: SimNetwork,
    from: NodeId,
}

impl Transport for SimTransport {
    async fn send(&self, peer: NodeId, frame: Frame) -> Result<(), TransportError> {
        self.net.route(self.from, peer, frame)
    }
}
