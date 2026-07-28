//! The in-memory simulated network (spec §7, §18.2, §18.3).
//!
//! [`SimNetwork`] routes [`Frame`]s between [`ClusterSystem`] nodes running on
//! one simulation, implementing the real [`Transport`] trait, so a simulated
//! cluster runs the real routing, dispatch, codec, and failure detection with
//! only the wire in-memory. It also injects faults under seed control (spec
//! §18.3): a blocked directed pair drops frames, which the SWIM detector then
//! observes as unreachability.
//!
//! This module is the **wire**: routing, seeded loss/duplication/latency, and
//! per-pair FIFO. Bringing nodes up and down, and the vocabulary that blocks
//! those pairs, is the sibling [`cluster`](crate::cluster) module.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::Authorizer;
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

use crate::SimClock;
use crate::SimEntropy;
use crate::SimSpawner;
use crate::Simulation;
use crate::coverage::FaultCounters;
use crate::coverage::FaultStats;
use crate::faults::FaultPolicy;

/// **One** cluster node running under the simulator, the multi-node counterpart
/// of [`SimSystem`](crate::SimSystem). A whole simulated cluster is a
/// [`SimNetwork`] and the nodes joined to it.
pub type SimNode = ClusterSystem<SimClock, SimEntropy, SimSpawner, SimTransport>;

pub(crate) struct NetInner {
    /// Each node's inbound frame sender (its receive loop holds the receiver).
    pub(crate) nodes: BTreeMap<NodeId, async_channel::Sender<(NodeId, Frame)>>,
    /// Directed pairs whose frames are dropped (partitions and crashes).
    pub(crate) blocked: BTreeSet<(NodeId, NodeId)>,
    /// Last scheduled delivery time per directed pair, kept strictly increasing
    /// so per-pair FIFO survives latency jitter (spec §7.2, invariant #3).
    pair_clock: BTreeMap<(NodeId, NodeId), Instant>,
    /// Joined systems, so a new node can be wired into every roster.
    pub(crate) joined: Vec<SimNode>,
    /// Each node's **live** process incarnation (its scheduler domain), so
    /// `pause`/`resume` freeze the running process and `restart` retires exactly
    /// the outgoing one. Keyed by node because a restart replaces the value.
    pub(crate) domains: BTreeMap<NodeId, u64>,
}

/// An in-memory network shared by the nodes of one simulation (spec §18.2).
///
/// Fields are `pub(crate)` because the sibling `cluster` module implements the
/// lifecycle and nemesis half of this same type; nothing outside
/// `actor-simulation` can reach them.
#[derive(Clone)]
pub struct SimNetwork {
    pub(crate) inner: Arc<Mutex<NetInner>>,
    pub(crate) clock: SimClock,
    pub(crate) entropy: SimEntropy,
    pub(crate) spawner: SimSpawner,
    pub(crate) mailbox_capacity: usize,
    pub(crate) mode: MembershipMode,
    pub(crate) events: Arc<dyn EventSink>,
    pub(crate) authorizer: Option<Arc<dyn Authorizer>>,
    /// Seeded loss/duplication/latency, shared with every clone of this handle
    /// so [`quiesce`](SimNetwork::quiesce) reaches the routing path a node is
    /// already sending on.
    pub(crate) faults: Arc<Mutex<FaultPolicy>>,
    /// A fixed minimum delivery latency applied to every frame (spec §18.2).
    /// **Not** a fault, and it draws no entropy: it exists so virtual time
    /// always advances on delivery. Without it, zero-latency delivery completes
    /// synchronously at the current instant (`SimClock::sleep(0)` is immediately
    /// ready), and a burst of same-instant traffic can pin the clock and starve
    /// future timers.
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
                domains: BTreeMap::new(),
            })),
            clock: sim.clock(),
            entropy: sim.entropy(),
            spawner: sim.spawner(),
            mailbox_capacity: 64,
            mode: MembershipMode::Static { detector: None },
            events: Arc::new(()),
            authorizer: None,
            faults: Arc::new(Mutex::new(FaultPolicy::default())),
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
    pub fn with_faults(self, faults: FaultPolicy) -> SimNetwork {
        *self.faults.lock().expect("fault policy mutex poisoned") = faults;
        self
    }

    /// The fault policy in force right now (a snapshot; the policy is shared and
    /// [`quiesce`](SimNetwork::quiesce) can retire it mid-run).
    fn faults(&self) -> FaultPolicy {
        *self.faults.lock().expect("fault policy mutex poisoned")
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

    /// Run every node in the given membership [`mode`](MembershipMode) (spec
    /// §9.4): the general form of the per-mode builders, so a swarm can sweep
    /// one workload across all four control planes.
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

    /// Route a frame from `from` to `to`. A blocked pair drops the frame
    /// silently (a partition is loss, not an error); an unknown node is
    /// unreachable. With no faults the push is synchronous and in-order; under a
    /// [`FaultPolicy`] the frame may be dropped, duplicated, or delayed, with
    /// per-pair delivery kept strictly ordered so per-pair FIFO survives (#3).
    fn route(&self, from: NodeId, to: NodeId, frame: Frame) -> Result<(), TransportError> {
        let faults = self.faults();
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
        // latency to apply (spec §18.2). It draws no entropy; see
        // `reserve_pair_slot` for why that matters.
        if !faults.active() && self.base_latency.is_zero() {
            return sender
                .try_send((from, frame))
                .map_err(|_| TransportError::Unreachable);
        }

        // Drop/duplicate are applied **only when faults are configured**, so the
        // base-latency-only default draws no entropy here and the seeded stream
        // stays byte-identical to a zero-latency run. (`buggify` always consumes
        // a draw, so it must not run otherwise.)
        let copies = if faults.active() {
            // Seeded loss (also models corruption / association loss): the node
            // never sees the frame, so it cannot be wedged by it (spec §7.3).
            if self.entropy.buggify(faults.drop_num, faults.drop_den) {
                self.stats.record_dropped();
                return Ok(());
            }
            // Seeded duplication (spec §18.3): the framework tolerates it; the
            // caller still sees a single outcome (§7.2).
            if self
                .entropy
                .buggify(faults.duplicate_num, faults.duplicate_den)
            {
                self.stats.record_duplicated();
                2
            } else {
                1
            }
        } else {
            1
        };
        for _ in 0..copies {
            let deliver_at = self.reserve_pair_slot(from, to, faults.max_latency);
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
    ///
    /// This draws entropy **only** when `max_latency` is set, which keeps the
    /// seeded stream byte-identical across latency configs: `route`'s fast path
    /// returns before calling this, and a base-latency-only run reaches here
    /// with `max_latency` zero, so neither path draws. Drawing unconditionally
    /// would silently break reproducibility with no compile error to catch it,
    /// so keep this gate in lockstep with `route`'s.
    fn reserve_pair_slot(&self, from: NodeId, to: NodeId, max_latency: Duration) -> Instant {
        // Seeded jitter only when `max_latency` is set (drawing entropy); floored by
        // the fixed `base_latency` so every delivery is at least `now + base` — the
        // floor draws no entropy.
        let jitter = if max_latency.is_zero() {
            Duration::ZERO
        } else {
            let span = max_latency.as_nanos() as u64 + 1;
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
    pub(crate) net: SimNetwork,
    pub(crate) from: NodeId,
}

impl Transport for SimTransport {
    async fn send(&self, peer: NodeId, frame: Frame) -> Result<(), TransportError> {
        self.net.route(self.from, peer, frame)
    }
}
