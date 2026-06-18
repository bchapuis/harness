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
use actor_cluster::DowningPolicy;
use actor_cluster::Frame;
use actor_cluster::GossipMode;
use actor_cluster::LeaderMode;
use actor_cluster::MembershipMode;
use actor_cluster::RaftConfig;
use actor_cluster::RegistryClient;
use actor_cluster::RegistryMode;
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
    mode: MembershipMode,
    events: Arc<dyn EventSink>,
    authorizer: Option<Arc<dyn Authorizer>>,
    faults: FaultPolicy,
    /// A fixed minimum delivery latency applied to every frame (spec §18.2). It is
    /// **not** a fault and draws no entropy — it exists so virtual time always
    /// advances on delivery. Without it, zero-latency delivery completes
    /// synchronously at the current instant (`SimClock::sleep(0)` is immediately
    /// ready), and a burst of same-instant traffic — e.g. simultaneous re-election
    /// of every Raft group a crashed node led, plus concurrent routing — can pin
    /// the clock and starve future election timers. A small floor (a realistic LAN
    /// latency) keeps the run deterministic while guaranteeing progress.
    base_latency: Duration,
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
            mode: MembershipMode::Static { detector: None },
            events: Arc::new(()),
            authorizer: None,
            faults: FaultPolicy::default(),
            // A small, realistic default so virtual time always advances on
            // delivery (see the field doc). Deterministic and entropy-free.
            base_latency: Duration::from_millis(1),
            stats: Arc::new(FaultCounters::default()),
        }
    }

    /// A snapshot of the faults this network has exercised so far (spec §18.3).
    pub fn fault_stats(&self) -> FaultStats {
        self.stats.snapshot()
    }

    /// Override the fixed minimum delivery latency (default 1 ms; see
    /// [`base_latency`](Self::base_latency)). Set `Duration::ZERO` only for a test
    /// that needs the old synchronous, same-instant delivery and is known not to
    /// generate a starving message burst.
    pub fn with_base_latency(mut self, base_latency: Duration) -> SimNetwork {
        self.base_latency = base_latency;
        self
    }

    /// Enable seed-controlled transport faults (spec §18.3).
    pub fn with_faults(mut self, faults: FaultPolicy) -> SimNetwork {
        self.faults = faults;
        self
    }

    /// Run every node in **gossip-based** mode (spec §9.4.4): full SWIM failure
    /// detection, with the coordinator driving the lifecycle and applying
    /// `downing`.
    pub fn with_gossip(mut self, swim: SwimConfig, downing: DowningPolicy) -> SimNetwork {
        self.mode = MembershipMode::Gossip(GossipMode { swim, downing });
        self
    }

    /// Run every node in **registry-based** mode (spec §9.4.2): the SWIM
    /// detector observes reachability, but the external registry behind
    /// `client` is the authority — every node syncs against it each
    /// `sync_interval`, and only a registry mutation declares `down`.
    pub fn with_registry(
        mut self,
        swim: SwimConfig,
        client: Arc<dyn RegistryClient>,
        sync_interval: Duration,
    ) -> SimNetwork {
        self.mode = MembershipMode::Registry(RegistryMode {
            swim,
            client,
            sync_interval,
        });
        self
    }

    /// Run every node in **leader-based** mode (spec §9.4.3): the SWIM detector
    /// is the leader's sensor, membership transitions are quorum-committed Raft
    /// log entries, and `downing` is applied by the elected leader alone.
    pub fn with_leader(
        mut self,
        swim: SwimConfig,
        raft: RaftConfig,
        downing: DowningPolicy,
    ) -> SimNetwork {
        self.mode = MembershipMode::Leader(LeaderMode {
            swim,
            raft,
            downing,
        });
        self
    }

    /// Run every node in the given membership [`mode`](MembershipMode) (spec §9.4)
    /// — the general form of the per-mode builders, so a swarm can sweep the same
    /// workload across all four control planes.
    pub fn with_mode(mut self, mode: MembershipMode) -> SimNetwork {
        self.mode = mode;
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
            membership: self.mode.clone(),
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

    /// Restart `node` (spec §18.3, §9.4.3 item 2): stop its current system —
    /// an **abrupt** stop, not a graceful leave — and bring up a fresh one
    /// under the same identity and mode. Volatile state is lost exactly as a
    /// process death loses it (actors, the membership view, Raft's role and
    /// commit index); durable state survives through the mode's storage seam —
    /// the per-node-cached [`RaftStorage`](actor_cluster::RaftStorage), the
    /// external registry. Network blocks involving the node are cleared: the
    /// new process comes up with working connectivity.
    ///
    /// The old instance is shut down *before* its successor exists, and a
    /// shut-down node processes nothing further, so the old incarnation can
    /// never write to the shared durable state after the new one has loaded
    /// it — the property the production restart relies on, modeled exactly.
    pub fn restart(&self, node: NodeId) -> SimCluster {
        let old = {
            let mut inner = self.inner.lock().expect("network mutex poisoned");
            let index = inner
                .joined
                .iter()
                .position(|system| system.node() == node)
                .expect("restart of a node that never joined");
            inner.joined.remove(index)
        };
        old.shutdown();
        {
            let mut inner = self.inner.lock().expect("network mutex poisoned");
            // Drop the old inbound sender: queued frames die with the old
            // receive loop, and new frames route to the successor only.
            inner.nodes.remove(&node);
            inner.blocked.retain(|(a, b)| *a != node && *b != node);
        }
        let system = self.bring_up(node, false);
        let mut inner = self.inner.lock().expect("network mutex poisoned");
        for existing in &inner.joined {
            // Re-introduce the roster both ways; `add_member` is idempotent and
            // never resurrects a terminal member.
            existing.add_member(node);
            system.add_member(existing.node());
        }
        inner.joined.push(system.clone());
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

        // Fast synchronous path only when there is neither a fault nor a base
        // latency to apply — the cheapest case (spec §18.2).
        if !self.faults.active() && self.base_latency.is_zero() {
            return sender
                .try_send((from, frame))
                .map_err(|_| TransportError::Unreachable);
        }

        // Drop/duplicate are applied **only when faults are configured**, so the
        // base-latency-only default draws no entropy here — the seeded random
        // stream stays byte-identical to a zero-latency run, only delivery timing
        // shifts. (`buggify` always consumes entropy, so it must not run otherwise.)
        let copies = if self.faults.active() {
            // Seeded loss (also models corruption / association loss): the node
            // never sees the frame, so it cannot be wedged by it (spec §7.3).
            if self.entropy.buggify(self.faults.drop_num, self.faults.drop_den) {
                self.stats.record_dropped();
                return Ok(());
            }
            // Seeded duplication (spec §18.3): the framework tolerates it; the
            // caller still sees a single outcome (§7.2).
            if self.entropy.buggify(self.faults.duplicate_num, self.faults.duplicate_den) {
                self.stats.record_duplicated();
                2
            } else {
                1
            }
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
        // Seeded jitter only when `max_latency` is set (drawing entropy); floored by
        // the fixed `base_latency` so every delivery is at least `now + base` — the
        // floor draws no entropy.
        let jitter = if self.faults.max_latency.is_zero() {
            Duration::ZERO
        } else {
            let span = self.faults.max_latency.as_nanos() as u64 + 1;
            Duration::from_nanos(self.entropy.next_u64() % span)
        };
        let earliest = self.clock.now() + jitter.max(self.base_latency);
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
