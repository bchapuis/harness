//! Cluster lifecycle and the nemesis vocabulary (spec §9.3, §18.3).
//!
//! [`SimNetwork`](crate::SimNetwork) does two jobs that pull apart cleanly. The
//! wire — routing, seeded loss/duplication/latency — lives in
//! [`transport`](crate::transport). This is the other one: bringing nodes up and
//! down, and the vocabulary a nemesis or a scenario uses to break them.
//!
//! - **Lifecycle** (§9.3): `join` pre-wires a founding roster, `join_seeded`
//!   exercises the real gossip join, and `restart` models process death —
//!   volatile state lost, durable state surviving through the mode's storage
//!   seam.
//! - **Faults** (§18.3): `partition` and `crash` block directed pairs;
//!   `partition_one_way` expresses the asymmetric case symmetric partition
//!   cannot; `pause`/`resume` freeze a node's tasks without touching the wire;
//!   `heal` clears every block. `inject` bypasses routing entirely, for hostile
//!   frames a well-behaved peer would never send.
//!
//! These are inherent methods on `SimNetwork` rather than a separate type: the
//! nemesis needs the same `inner` the router does, and splitting the *state*
//! would buy nothing. Splitting the *file* keeps `transport.rs` about the wire.

use std::sync::Arc;

use actor_cluster::ClusterConfig;
use actor_cluster::Frame;
use actor_cluster::ClusterSystem;
use actor_core::NodeId;
use actor_serialization::Codec;
use actor_serialization::JsonCodec;

use crate::SimNetwork;
use crate::transport::SimNode;
use crate::transport::SimTransport;

impl SimNetwork {
    /// Bring up a node's system on the network, registering it for routing.
    /// `joining` selects founding (`Up`) vs joiner (`Joining`) startup (spec §9.3).
    fn bring_up(&self, node: NodeId, joining: bool) -> SimNode {
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
            // Tag the node's spawner with its id so all its tasks can be frozen as a
            // unit by `pause` (spec §18.3); child tasks inherit the tag.
            self.spawner.clone().with_domain(node.uid()),
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
    pub fn join(&self, node: NodeId) -> SimNode {
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
    pub fn join_seeded(&self, node: NodeId, seeds: &[NodeId]) -> SimNode {
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
    /// the per-node-cached [`RaftWAL`](actor_cluster::RaftWAL), the
    /// external registry. Network blocks involving the node are cleared: the
    /// new process comes up with working connectivity.
    ///
    /// The old instance is shut down *before* its successor exists, and a
    /// shut-down node processes nothing further, so the old incarnation can
    /// never write to the shared durable state after the new one has loaded
    /// it — the property the production restart relies on, modeled exactly.
    pub fn restart(&self, node: NodeId) -> SimNode {
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

    /// Sever communication in **one direction only** (spec §18.3): frames from any
    /// node in `from` to any node in `to` are dropped, but the reverse direction
    /// keeps flowing. This is the asymmetric ("one-way" / half-open) partition that
    /// symmetric `partition` cannot express — the source of zombie leaders, where a
    /// deposed leader still *receives* traffic (so it can be told it is stale) but
    /// cannot *reach* a quorum, or the reverse, where it keeps heartbeating outward
    /// but never hears the votes that would let it commit. `heal` clears it.
    pub fn partition_one_way(&self, from: &[NodeId], to: &[NodeId]) {
        let mut inner = self.inner.lock().expect("network mutex poisoned");
        for &f in from {
            for &t in to {
                inner.blocked.insert((f, t));
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

    /// **Freeze a node** (spec §18.3): its tasks stop being polled, so it makes no
    /// progress at all — a process pause / VM freeze / long GC stall. Unlike `crash`,
    /// the node keeps its state and its inbound frames queue up, to be processed when
    /// it resumes. Virtual time is global and keeps advancing, but while frozen no
    /// task code runs: a paused leader does not climb its term, so when it wakes it
    /// is cleanly behind and discovers it is deposed (§8.1, "a paused leader that
    /// wakes is already deposed"). This is the case symmetric partition cannot make.
    /// `resume` thaws it; the backlog then drains and any timers that came due in the
    /// meantime fire at once.
    pub fn pause(&self, node: NodeId) {
        self.spawner.set_paused(node.uid(), true);
    }

    /// Thaw a node frozen by [`pause`](SimNetwork::pause): its queued inbound frames
    /// drain and its overdue timers fire, so it rejoins and reconciles (spec §18.3).
    pub fn resume(&self, node: NodeId) {
        self.spawner.set_paused(node.uid(), false);
    }

    /// Clear all partitions/crashes — the network heals (spec §9.2).
    pub fn heal(&self) {
        self.inner
            .lock()
            .expect("network mutex poisoned")
            .blocked
            .clear();
    }
}
