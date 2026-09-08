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
use actor_cluster::WIRE;
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

/// One node's **live** process on the network: the inbound frame sender its
/// receive loop reads from, the process incarnation (scheduler domain) that
/// `pause`/`resume` freeze and `restart` retires, and the running system a new
/// joiner is wired into. One entry travels as one unit: a restart replaces the
/// whole entry, so the three can never disagree about which incarnation is live.
pub(crate) struct Process {
    pub(crate) node: NodeId,
    pub(crate) sender: async_channel::Sender<(NodeId, Frame)>,
    pub(crate) domain: u64,
    pub(crate) system: SimNode,
}

pub(crate) struct NetInner {
    /// The live processes, one per node, in join order.
    pub(crate) processes: Vec<Process>,
    /// Directed pairs whose frames are dropped (partitions and crashes).
    pub(crate) blocked: BTreeSet<(NodeId, NodeId)>,
    /// Last scheduled delivery time per directed pair, kept strictly increasing
    /// so per-pair FIFO survives latency jitter (spec §7.2, invariant #3).
    pair_clock: BTreeMap<(NodeId, NodeId), Instant>,
    /// Nodes running a wire window other than [`WIRE`]: what a simulated node's
    /// *build* accepts and writes (spec §7.1, compatibility spec §3).
    ///
    /// Holds only the overrides, so a node absent here is one running the window
    /// this build ships, and a run that never sets one behaves exactly as before.
    /// Keyed by node and **not** cleared by `restart`, because a restart is how a
    /// rolling upgrade is modelled: set the window, then replace the process.
    windows: BTreeMap<NodeId, compat::Window>,
}

impl NetInner {
    /// The live process running `node`, if any. A handful of nodes at most, so
    /// the scan costs what a map lookup would.
    pub(crate) fn process(&self, node: NodeId) -> Option<&Process> {
        self.processes.iter().find(|p| p.node == node)
    }
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
    stats: Arc<FaultCounters>,
}

/// The fixed minimum delivery latency applied to every frame (spec §18.2).
/// **Not** a fault, and it draws no entropy: it exists so virtual time always
/// advances on delivery. Without it, zero-latency delivery would complete
/// synchronously at the current instant (`SimClock::sleep(0)` is immediately
/// ready), and a burst of same-instant traffic could pin the clock and starve
/// future timers. Small and realistic; a constant, so every run has exactly
/// one delivery mode to reason about.
const BASE_LATENCY: Duration = Duration::from_millis(1);

impl SimNetwork {
    /// Create a network backed by a simulation's runtime seam (SWIM off,
    /// no faults, no-op observability).
    pub fn new(sim: &Simulation) -> SimNetwork {
        SimNetwork {
            inner: Arc::new(Mutex::new(NetInner {
                processes: Vec::new(),
                blocked: BTreeSet::new(),
                pair_clock: BTreeMap::new(),
                windows: BTreeMap::new(),
            })),
            clock: sim.clock(),
            entropy: sim.entropy(),
            spawner: sim.spawner(),
            mailbox_capacity: 64,
            mode: MembershipMode::Static { detector: None },
            events: Arc::new(()),
            authorizer: None,
            faults: Arc::new(Mutex::new(FaultPolicy::default())),
            stats: Arc::new(FaultCounters::default()),
        }
    }

    /// A snapshot of the faults this network has exercised so far (spec §18.3).
    pub fn fault_stats(&self) -> FaultStats {
        self.stats.snapshot()
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
    /// `client` is the authority; every node syncs against it each
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
    /// small capacity makes backpressure: `MailboxFull` on the inbound remote
    /// path (invariant #5); observable in a test without flooding the default.
    pub fn with_mailbox_capacity(mut self, capacity: usize) -> SimNetwork {
        self.mailbox_capacity = capacity;
        self
    }

    /// The [`Transport`] handle `node`'s system sends through: the same one
    /// [`bring_up`](SimNetwork::join) hands it, so asking this what it would settle
    /// with a peer asks the running node's own send path.
    pub fn transport(&self, node: NodeId) -> SimTransport {
        SimTransport {
            net: self.clone(),
            from: node,
        }
    }

    /// Move `node` to another release's wire window and record it as an injected
    /// fault (spec §18.3): the nemesis's rolling upgrade, as distinct from
    /// [`set_wire_window`](SimNetwork::set_wire_window), which a scenario uses to
    /// set a run's starting shape and which nothing counts.
    pub(crate) fn upgrade(&self, node: NodeId, window: compat::Window) {
        self.set_wire_window(node, window);
        self.stats.record_upgrade();
    }

    /// Run `node` on a wire window other than the one this build ships (spec
    /// §7.1, compatibility spec §3, §4).
    ///
    /// This is what makes a **mixed-version** run possible: with every node on
    /// [`WIRE`] there is exactly one revision in the cluster and negotiation has
    /// nothing to settle, so a rolling upgrade cannot be simulated at all. Give one
    /// node `Window::new("actor.wire", 1, 2, 1)` and it reads the next revision
    /// without writing it, which is the first half of **V4**; move `writes` up and
    /// it is the second.
    ///
    /// Takes effect on the next [`peer_version`](Transport::peer_version): the
    /// simulator holds no association, so there is no handshake to redo. To model
    /// the upgrade as a process replacement instead, set the window and then
    /// [`restart`](SimNetwork::restart) the node.
    pub fn set_wire_window(&self, node: NodeId, window: compat::Window) {
        self.inner
            .lock()
            .expect("network mutex poisoned")
            .windows
            .insert(node, window);
    }

    /// The wire window `node` is running: its override, or [`WIRE`] for a node
    /// that has never been given one.
    pub fn wire_window(&self, node: NodeId) -> compat::Window {
        self.inner
            .lock()
            .expect("network mutex poisoned")
            .windows
            .get(&node)
            .copied()
            .unwrap_or(WIRE)
    }

    /// Route a frame from `from` to `to`. A blocked pair drops the frame
    /// silently (a partition is loss, not an error); an unknown node is
    /// unreachable. Every delivery advances virtual time by at least
    /// [`BASE_LATENCY`]; under a [`FaultPolicy`] the frame may additionally be
    /// dropped, duplicated, or delayed, with per-pair delivery kept strictly
    /// ordered so per-pair FIFO survives (#3).
    fn route(&self, from: NodeId, to: NodeId, frame: Frame) -> Result<(), TransportError> {
        let faults = self.faults();
        let sender = {
            let inner = self.inner.lock().expect("network mutex poisoned");
            if inner.blocked.contains(&(from, to)) {
                self.stats.record_blocked();
                return Ok(());
            }
            match inner.process(to) {
                Some(process) => process.sender.clone(),
                None => return Err(TransportError::Unreachable),
            }
        };

        // Drop/duplicate are applied **only when faults are configured**, so the
        // no-fault default draws no entropy here and the seeded stream is not
        // perturbed by routing. (`buggify` always consumes a draw, so it must
        // not run otherwise.)
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
    /// seeded stream byte-identical across latency configs: a no-fault run
    /// reaches here with `max_latency` zero and draws nothing. Drawing
    /// unconditionally would silently break reproducibility with no compile
    /// error to catch it.
    fn reserve_pair_slot(&self, from: NodeId, to: NodeId, max_latency: Duration) -> Instant {
        // Seeded jitter only when `max_latency` is set (drawing entropy); floored by
        // the fixed [`BASE_LATENCY`] so every delivery is at least `now + base`: the
        // floor draws no entropy.
        let jitter = if max_latency.is_zero() {
            Duration::ZERO
        } else {
            let span = max_latency.as_nanos() as u64 + 1;
            Duration::from_nanos(self.entropy.next_u64() % span)
        };
        // Only a jitter above the fixed floor moves this delivery relative to
        // un-jittered traffic; that, not the base every frame pays, is the
        // "delayed" fault the coverage tally counts (spec §18.3).
        if jitter > BASE_LATENCY {
            self.stats.record_delayed();
        }
        let earliest = self.clock.now() + jitter.max(BASE_LATENCY);
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

    /// The revision this node and `peer` would settle on (spec §7.1,
    /// compatibility spec §3.1): the same `negotiate` over the same two ranges
    /// the TCP handshake runs, against whatever windows the two nodes are running
    /// ([`SimNetwork::set_wire_window`]).
    ///
    /// The simulator keeps no association, so what stands in for one is the
    /// condition under which a real one would exist: the peer is a node on this
    /// network, and the directed pair is not blocked. That makes a partition
    /// answer `None`: a gate must not keep writing a revision agreed with a peer
    /// it can no longer reach, because whatever comes back may not be the process
    /// that agreed to it.
    fn peer_version(&self, peer: NodeId) -> Option<compat::Version> {
        let inner = self.net.inner.lock().expect("network mutex poisoned");
        if inner.process(peer).is_none() || inner.blocked.contains(&(self.from, peer)) {
            return None;
        }
        let ours = inner.windows.get(&self.from).copied().unwrap_or(WIRE);
        let theirs = inner.windows.get(&peer).copied().unwrap_or(WIRE);
        // Disjoint ranges are a refused association (**V2**), which reaches a
        // caller as no association at all: the same `None` an unreachable peer
        // gives, and the same conservative send it must produce.
        ours.negotiate(theirs.accepted()).ok()
    }
}
