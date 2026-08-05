//! Cluster lifecycle and the nemesis vocabulary (spec §9.3, §18.3).
//!
//! The wire half of [`SimNetwork`](crate::SimNetwork) (routing, seeded
//! loss/duplication/latency) lives in [`transport`](crate::transport). This is
//! the other half: bringing nodes up and down, and the vocabulary a nemesis or a
//! scenario uses to break them.
//!
//! These are inherent methods on `SimNetwork` rather than a separate type: the
//! nemesis needs the same `inner` the router does.

use std::sync::Arc;

use actor_cluster::ClusterConfig;
use actor_cluster::ClusterSystem;
use actor_cluster::Frame;
use actor_core::NodeId;
use actor_serialization::Codec;
use actor_serialization::JsonCodec;

use crate::SimNetwork;
use crate::transport::SimNode;

/// A node's process was replaced by [`SimNetwork::restart`] — emitted on the
/// event stream (actor §16) just before the old system is shut down.
///
/// A successor process assigns actor *paths* from zero, so `node-2//user/0`
/// names one actor before the restart and a different one after; without the
/// boundary a checker that accumulates per-actor state (the lifecycle invariant,
/// #6) reads the successor's first actor as a second assignment of the
/// predecessor's. Routing needs no such marker: a host stamps its **process
/// incarnation** onto every id it assigns (`LocalHost::with_incarnation`), so a
/// stale ref cannot resolve to whatever now holds that path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeRestarted {
    pub node: NodeId,
}

impl SimNetwork {
    /// Bring up a node's system on the network, registering it for routing.
    /// `joining` selects founding (`Up`) vs joiner (`Joining`) startup (spec §9.3).
    fn bring_up(&self, node: NodeId, joining: bool) -> SimNode {
        // A fresh scheduler domain per incarnation, not per node: `restart` needs
        // to retire the outgoing process's tasks while leaving its successor's —
        // which carry the same `NodeId` — untouched.
        let (spawner, domain) = self.spawner.with_fresh_domain();
        let (tx, rx) = async_channel::unbounded();
        let transport = self.transport(node);
        let codec: Arc<dyn Codec> = Arc::new(JsonCodec);
        let config = ClusterConfig {
            codec,
            mailbox_capacity: self.mailbox_capacity,
            events: Arc::clone(&self.events),
            membership: self.mode.clone(),
            joining,
            authorizer: self.authorizer.clone(),
            // The scheduler domain is already one-per-incarnation and monotonic,
            // so it is exactly the stamp actor ids need to stop colliding across
            // a restart (`LocalHost::with_incarnation`).
            incarnation: domain,
        };
        let system = ClusterSystem::start(
            node,
            self.clock.clone(),
            self.entropy.clone(),
            // Every task this system spawns carries its incarnation's domain, so
            // the process can be frozen by `pause` or ended by `restart` as a
            // unit (spec §18.3); child tasks inherit the tag.
            spawner,
            transport,
            rx,
            config,
        );
        let mut inner = self.inner.lock().expect("network mutex poisoned");
        inner.nodes.insert(node, tx);
        inner.domains.insert(node, domain);
        system
    }

    /// Bring up a founding node, wiring it into every existing node's roster as
    /// a full `Up` member (spec §9.3 join, pre-wired: for when the whole roster
    /// is known up front).
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
    /// only its `seeds`, which it contacts to gossip itself into the cluster.
    /// The leader then admits it to `Up`. No pre-wiring, so this exercises the
    /// real join protocol.
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

    /// Restart `node` (spec §18.3, §9.4.3 item 2): stop its current system (an
    /// **abrupt** stop, not a graceful leave) and bring up a fresh one under the
    /// same identity and mode. Volatile state is lost exactly as a process death
    /// loses it (actors, the membership view, Raft's role and commit index);
    /// durable state survives through the mode's storage seam, the
    /// per-node-cached [`RaftWAL`](actor_cluster::RaftWAL) or the external
    /// registry. Network blocks involving the node are cleared: the new process
    /// comes up with working connectivity.
    ///
    /// The old process is **ended**, not merely disconnected, in three steps that
    /// must run in order. [`ClusterSystem::shutdown`] sets the shutdown flag and
    /// drops the transport; the inbound sender is dropped, so queued frames die
    /// with the old receive loop; and the incarnation's scheduler domain is
    /// retired, so every task it owns leaves the run. Only then does the successor
    /// exist, or the predecessor's actors would keep being polled alongside their
    /// replacement.
    ///
    /// Ending a process mid-flight leaves brackets open, and [`NodeRestarted`] is
    /// how a checker learns to stop expecting the other half (see
    /// [`Invariant::forget_node`](crate::Invariant::forget_node)).
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
        let retiring = {
            let mut inner = self.inner.lock().expect("network mutex poisoned");
            // Drop the old inbound sender: queued frames die with the old
            // receive loop, and new frames route to the successor only.
            inner.nodes.remove(&node);
            inner.blocked.retain(|(a, b)| *a != node && *b != node);
            inner.domains.remove(&node)
        };
        // Ordered after the shutdown and before the announcement, so the
        // predecessor has emitted everything it will ever emit by the time a
        // checker is told the boundary is here.
        if let Some(domain) = retiring {
            self.spawner.retire_domain(domain);
        }
        self.events
            .emit(actor_core::Event::app(NodeRestarted { node }));
        let system = self.bring_up(node, false);
        let mut inner = self.inner.lock().expect("network mutex poisoned");
        for existing in &inner.joined {
            // `add_member` is idempotent and never resurrects a terminal member.
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
    /// keeps flowing. The asymmetric ("one-way" / half-open) partition, and the
    /// source of zombie leaders. `heal` clears it.
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
    /// progress at all (a process pause, VM freeze, or long GC stall). Unlike
    /// `crash`, the node keeps its state and its inbound frames queue up, to be
    /// processed when it resumes. Virtual time is global and keeps advancing, but
    /// while frozen no task code runs: a paused leader does not climb its term, so
    /// when it wakes it is cleanly behind and discovers it is deposed (§8.1).
    pub fn pause(&self, node: NodeId) {
        if let Some(domain) = self.domain_of(node) {
            self.spawner.set_paused(domain, true);
        }
    }

    /// The scheduler domain of `node`'s live process. Read through the registry
    /// rather than derived from the id, because a restart gives the successor a
    /// different domain (see [`restart`]).
    ///
    /// [`restart`]: SimNetwork::restart
    fn domain_of(&self, node: NodeId) -> Option<u64> {
        self.inner
            .lock()
            .expect("network mutex poisoned")
            .domains
            .get(&node)
            .copied()
    }

    /// Thaw a node frozen by [`pause`](SimNetwork::pause): its queued inbound frames
    /// drain and its overdue timers fire, so it rejoins and reconciles (spec §18.3).
    pub fn resume(&self, node: NodeId) {
        if let Some(domain) = self.domain_of(node) {
            self.spawner.set_paused(domain, false);
        }
    }

    /// Clear all partitions and crashes (spec §9.2).
    ///
    /// This clears *blocks* only: seeded loss, duplication, and latency keep
    /// running, because the nemesis heals between rounds and a heal that retired
    /// fault injection would leave the rest of the run unfaulted.
    /// [`quiesce`](SimNetwork::quiesce) is the other half.
    pub fn heal(&self) {
        self.inner
            .lock()
            .expect("network mutex poisoned")
            .blocked
            .clear();
    }

    /// Stop injecting transport faults: from here on frames are neither dropped,
    /// duplicated, nor jittered (the fixed base latency stays — it is not a
    /// fault, spec §18.2).
    ///
    /// Reaching a **converged** cluster needs this, not just
    /// [`heal`](SimNetwork::heal). A healed network that still loses a seeded
    /// share of frames keeps the SWIM detector firing: probes go missing, peers
    /// flip to `suspect` and on to `unreachable`, and every node's serving set
    /// (utilities spec §2.1) keeps changing under it, so an at-quiescence
    /// assertion that presumes convergence (the singleton's exactly-one,
    /// utilities spec §4 item 3) has no ground to stand on.
    ///
    /// Faults already tallied stay counted, so a coverage assertion (spec §18.3)
    /// still sees what the run exercised.
    pub fn quiesce(&self) {
        *self.faults.lock().expect("fault policy mutex poisoned") = crate::FaultPolicy::default();
    }
}
