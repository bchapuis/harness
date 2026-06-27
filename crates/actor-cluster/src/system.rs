//! The networked `ClusterSystem` (spec §4, §7, §10).
//!
//! `ClusterSystem` is the reference [`ActorSystem`] for multiple nodes. It
//! reuses the local actor machinery ([`LocalHost`]) and adds the network
//! boundary: outbound `remote_ask`/`remote_tell` over a [`Transport`], an
//! inbound receive loop that decodes envelopes and routes replies, and a SWIM
//! failure detector that maintains [`Membership`] reachability and drives the
//! node-down cascade (spec §8.1): a node declared `down` completes its in-flight
//! callers with `Unreachable` rather than letting them hang.
//!
//! It also disseminates membership by gossip with direct and **indirect** SWIM
//! probing (spec §10), runs the configured membership **control plane** (spec
//! §9.4) — the registry sync loop in registry-based mode, the Raft driver in
//! leader-based mode, the coordinator lifecycle in gossip-based mode — prunes
//! via death watch, and runs the receptionist with broadcast-on-change plus
//! periodic anti-entropy (spec §12, §13).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorId;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::CallError;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::EventSink;
use actor_core::Mailbox;
use actor_core::NodeId;
use actor_core::ReplyHandle;
use actor_core::ReplyResult;
use actor_core::Spawner;
use actor_core::Terminated;
use actor_core::TerminationReason;
use actor_core::host::LocalHost;
use actor_core::host::WatchDelivery;
use actor_core::receptionist::ReceptionistState;
use actor_serialization::Codec;
use async_channel::Receiver;
use async_channel::Sender;
use futures::channel::oneshot;

use crate::consensus::RaftConsensus;
use crate::correlator::Correlator;
use crate::membership::LeaderMode;
use crate::membership::MemberStatus;
use crate::membership::Membership;
use crate::membership::MembershipCommand;
use crate::membership::MembershipMode;
use crate::membership::SwimConfig;
use crate::protocol::CallId;
use crate::protocol::Frame;
use crate::protocol::ReceptionistEntry;
use crate::raft::Committed;
use crate::raft::EntryPayload;
use crate::raft::GroupId;
use crate::raft::MultiRaft;
use crate::raft::RaftGroup;
use crate::raft::RaftOutput;
use crate::registry::RegistryClient;
use crate::registry::RegistryState;
use crate::transport::Transport;

/// Authorizes inbound messages per association (spec §15). Consulted before an
/// envelope is delivered; a denied message is rejected as a system failure and
/// never reaches the actor (so deserialization side effects are also avoided).
/// A system without one permits every message that clears the transport
/// handshake.
pub trait Authorizer: Send + Sync + 'static {
    /// Whether `peer` may deliver `manifest` to `recipient` (spec §15).
    fn authorize(&self, peer: NodeId, recipient: &ActorId, manifest: &str) -> bool;
}

/// Configuration for a [`ClusterSystem`] node.
pub struct ClusterConfig {
    /// The wire codec (spec §5).
    pub codec: Arc<dyn Codec>,
    /// Per-actor bounded mailbox capacity (spec §6).
    pub mailbox_capacity: usize,
    /// Observability sink (spec §16).
    pub events: Arc<dyn EventSink>,
    /// Which control plane governs membership (spec §9.4): a fixed
    /// [`Static`](MembershipMode::Static) roster, an external
    /// [`Registry`](MembershipMode::Registry), a self-hosted Raft log behind an
    /// elected [`Leader`](MembershipMode::Leader), or peer-to-peer
    /// [`Gossip`](MembershipMode::Gossip). `Static` without the observe-only
    /// detector runs no SWIM loop.
    pub membership: MembershipMode,
    /// Start this node as a joiner (spec §9.3): it enters the cluster `Joining`
    /// and is admitted to `Up` by the mode's authority (the gossip coordinator,
    /// or a committed log entry). Meaningful in gossip- and leader-based mode;
    /// static members always start `Up`, and in registry-based mode admission
    /// *is* the registry entry (spec §9.4.2 item 2). `false` (the default)
    /// starts the node as a founding `Up` member.
    pub joining: bool,
    /// Per-message authorization (spec §15); `None` permits every message that
    /// clears the transport handshake.
    pub authorizer: Option<Arc<dyn Authorizer>>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            codec: Arc::new(actor_serialization::JsonCodec),
            mailbox_capacity: 64,
            events: Arc::new(()),
            membership: MembershipMode::Static { detector: None },
            joining: false,
            authorizer: None,
        }
    }
}

/// A subscriber to one Raft group's committed `(index, app-bytes)` stream
/// (the [`RaftConsensus`](crate::RaftConsensus) seam).
type CommitSink = Sender<Committed>;

struct Inner<C, E, S, T> {
    clock: C,
    entropy: E,
    spawner: S,
    transport: T,
    codec: Arc<dyn Codec>,
    host: LocalHost,
    membership: Membership,
    /// The configured control plane (spec §9.4), kept so the operator API can
    /// dispatch to the mode's authority.
    mode: MembershipMode,
    /// The multi-group consensus engine, in leader-based mode (spec §9.4.3);
    /// `None` elsewhere. It hosts the membership control group ([`GroupId::CONTROL`])
    /// and (for granary, later) a group per replicated shard.
    raft: Option<Arc<MultiRaft>>,
    /// Resolved SWIM parameters, so loops spawned without the config (e.g. a
    /// relayed indirect probe) can still read `rtt`/`k` (spec §10).
    swim: SwimConfig,
    events: Arc<dyn EventSink>,
    /// In-flight `ask`s awaiting a reply, keyed by correlation id. Each waiter
    /// carries its target node so the cascade can complete it on a node-down
    /// (spec §8.1 step 3).
    calls: Correlator<CallId, (NodeId, oneshot::Sender<ReplyResult>)>,
    /// In-flight SWIM probes awaiting an `Ack` (direct, indirect, or a helper's
    /// relayed probe), keyed by seq. The completion carries the target's
    /// incarnation, so a relay can report it back to the requester (spec §10).
    pings: Correlator<u64, oneshot::Sender<u64>>,
    receptionist: Arc<ReceptionistState>,
    /// Per-group subscribers to committed application entries (the [`RaftConsensus`]
    /// seam, granary's sharded journal). A non-`CONTROL` group's committed
    /// `(index, bytes)` are broadcast here in `apply_raft_output`; the control
    /// group's entries drive membership instead and are never published.
    ///
    /// [`RaftConsensus`]: crate::RaftConsensus
    commit_sinks: Mutex<BTreeMap<GroupId, Vec<CommitSink>>>,
    /// Set on a graceful stop (spec §9.3): the detector and receptionist gossip
    /// loops return once they see it. Default `false`, so a system that never
    /// stops (every simulation run) behaves exactly as before.
    shutdown: std::sync::atomic::AtomicBool,
    /// Optional per-message authorization (spec §15).
    authorizer: Option<Arc<dyn Authorizer>>,
}

/// A networked, multi-node actor system (spec §4, §7). Cloning shares the same
/// underlying node.
pub struct ClusterSystem<C, E, S, T> {
    inner: Arc<Inner<C, E, S, T>>,
}

impl<C, E, S, T> Clone for ClusterSystem<C, E, S, T> {
    fn clone(&self) -> Self {
        ClusterSystem {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<C, E, S, T> ClusterSystem<C, E, S, T>
where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    /// Start a node: build the local host and membership, bind the transport,
    /// launch the receive loop, and — if SWIM is configured — the detector.
    pub fn start(
        node: NodeId,
        clock: C,
        entropy: E,
        spawner: S,
        transport: T,
        inbound: Receiver<(NodeId, Frame)>,
        config: ClusterConfig,
    ) -> ClusterSystem<C, E, S, T> {
        let mode = config.membership.clone();
        let swim = mode.detector().unwrap_or_default();
        let host = LocalHost::new(node, Arc::clone(&config.events), config.mailbox_capacity);
        let membership = Membership::new(node, &mode, Arc::clone(&config.events), config.joining);
        let raft = match &mode {
            MembershipMode::Leader(leader) => {
                Some(Arc::new(MultiRaft::new(node, &leader.raft, clock.now())))
            }
            _ => None,
        };
        let system = ClusterSystem {
            inner: Arc::new(Inner {
                clock,
                entropy,
                spawner,
                transport,
                codec: config.codec,
                host,
                membership,
                mode: mode.clone(),
                raft,
                swim,
                events: config.events,
                calls: Correlator::new(),
                pings: Correlator::new(),
                receptionist: Arc::new(ReceptionistState::new()),
                commit_sinks: Mutex::new(BTreeMap::new()),
                shutdown: std::sync::atomic::AtomicBool::new(false),
                authorizer: config.authorizer,
            }),
        };
        system
            .inner
            .spawner
            .launch(Box::pin(receive_loop(system.clone(), inbound)));
        if mode.detector().is_some() {
            system
                .inner
                .spawner
                .launch(Box::pin(detector(system.clone(), swim)));
            // Anti-entropy rides alongside the detector on the same cadence: it
            // only makes sense for a live cluster, and gating it on SWIM keeps
            // SWIM-off systems quiescent (spec §13, §18.1).
            system.inner.spawner.launch(Box::pin(receptionist_gossip(
                system.clone(),
                swim.probe_interval,
            )));
        }
        // Registry-based mode (spec §9.4.2): the sync loop watches the external
        // registry and applies its revision-stamped state to the local view.
        if let MembershipMode::Registry(registry) = &mode {
            system.inner.spawner.launch(Box::pin(registry_sync(
                system.clone(),
                Arc::clone(&registry.client),
                registry.sync_interval,
            )));
        }
        // Leader-based mode (spec §9.4.3): the Raft driver runs elections,
        // replication, and the leader's control-plane duties.
        if let MembershipMode::Leader(leader) = &mode {
            system
                .inner
                .spawner
                .launch(Box::pin(raft_driver(system.clone(), leader.clone())));
        }
        system
    }

    /// This node's identity.
    pub fn node(&self) -> NodeId {
        self.inner.host.node()
    }

    /// The system clock.
    pub fn clock(&self) -> &C {
        &self.inner.clock
    }

    /// The system entropy source.
    pub fn entropy(&self) -> &E {
        &self.inner.entropy
    }

    /// Read access to this node's membership view (for tests/inspection).
    pub fn membership(&self) -> &Membership {
        &self.inner.membership
    }

    /// Spawn an actor from a `factory` so it can be restarted on fault (spec
    /// §11.2).
    pub fn spawn_with<A, F>(&self, mut factory: F) -> ActorRef<A>
    where
        A: Actor<System = Self>,
        F: FnMut() -> A + Send + 'static,
    {
        self.inner.host.spawn_actor(
            self.clone(),
            self.inner.clock.clone(),
            &self.inner.spawner,
            Box::new(move || Some(factory())),
            None,
        )
    }

    /// Introduce a peer into this node's roster as a full `Up` member (spec §9.3).
    pub fn add_member(&self, node: NodeId) {
        self.inner
            .membership
            .add_member(node, self.inner.clock.now());
    }

    /// Begin a graceful leave (spec §9.3): announce `Leaving` in gossip. The
    /// mode's authority finalizes this node to `Down`, and watchers of its
    /// actors are notified through the node-down cascade (spec §8.1, §12). The
    /// caller drains and shuts the node down after announcing.
    pub fn leave(&self) {
        self.inner.membership.begin_leaving();
    }

    /// **Admit** `node` into the member set as a full `Up` member, through the
    /// mode's authority (spec §9.3): a registry registration (registry-based,
    /// spec §9.4.2) — admission is the entry itself — or a committed `Admit`
    /// entry (leader-based, spec §9.4.3). In static and gossip-based mode there
    /// is no authority to command (joiners are admitted by the coordinator,
    /// spec §9.4.4), so this returns `false`. Returns whether the change took:
    /// the registry acknowledged it, or this node observed the committed
    /// transition; the *cluster* converges asynchronously through the mode's
    /// dissemination.
    pub async fn admit(&self, node: NodeId) -> bool {
        match &self.inner.mode {
            MembershipMode::Registry(registry) => registry.client.register(node).await.is_ok(),
            MembershipMode::Leader(leader) => {
                self.propose_and_wait(leader, MembershipCommand::Admit(node), move |m| {
                    m.status(node) == Some(MemberStatus::Up)
                })
                .await
            }
            _ => false,
        }
    }

    /// **Drain** `node` for maintenance — the reversible cordon (spec §9.1,
    /// §9.4), through the mode's authority: a registry state change
    /// (registry-based) or a committed `Drain` entry (leader-based). The node
    /// stays a member (no `down`, no death watch, in-flight calls unaffected);
    /// [`resume`](Self::resume) returns it to `Up`. `false` in modes without an
    /// authoritative control plane.
    pub async fn drain(&self, node: NodeId) -> bool {
        match &self.inner.mode {
            MembershipMode::Registry(registry) => registry
                .client
                .set_state(node, RegistryState::Draining)
                .await
                .is_ok(),
            MembershipMode::Leader(leader) => {
                self.propose_and_wait(leader, MembershipCommand::Drain(node), move |m| {
                    m.status(node) == Some(MemberStatus::Draining)
                })
                .await
            }
            _ => false,
        }
    }

    /// **Resume** a drained `node` after maintenance (spec §9.1, §9.4) — the
    /// reverse of [`drain`](Self::drain). `false` in modes without an
    /// authoritative control plane.
    pub async fn resume(&self, node: NodeId) -> bool {
        match &self.inner.mode {
            MembershipMode::Registry(registry) => registry
                .client
                .set_state(node, RegistryState::Up)
                .await
                .is_ok(),
            MembershipMode::Leader(leader) => {
                self.propose_and_wait(leader, MembershipCommand::Resume(node), move |m| {
                    m.status(node) == Some(MemberStatus::Up)
                })
                .await
            }
            _ => false,
        }
    }

    /// **Decommission** `node`: terminally remove it from the cluster through
    /// the mode's authority (spec §9.4) — a registry deregistration
    /// (registry-based), whose revision finalizes `down`, or a committed `Down`
    /// entry (leader-based). Irrevocable (invariant #15); each node runs the
    /// node-down cascade (spec §8.1) as the decision reaches it — every
    /// in-flight `ask` to `node` completes `Unreachable`, and watchers of its
    /// actors receive `Terminated { NodeDown }`. `false` in modes without an
    /// authoritative control plane.
    pub async fn decommission(&self, node: NodeId) -> bool {
        match &self.inner.mode {
            MembershipMode::Registry(registry) => registry.client.deregister(node).await.is_ok(),
            MembershipMode::Leader(leader) => {
                self.propose_and_wait(leader, MembershipCommand::Down(node), move |m| {
                    m.is_down(node)
                })
                .await
            }
            _ => false,
        }
    }

    /// **Add a voter** to the Raft quorum (leader-based mode, spec §9.4.3
    /// item 2) — a committed single-server configuration change. Leader-only:
    /// `false` anywhere else (call it on [`leader`](Self::leader)).
    pub async fn add_voter(&self, node: NodeId) -> bool {
        self.voter_change(EntryPayload::AddVoter(node), node, true)
            .await
    }

    /// **Remove a voter** from the Raft quorum (spec §9.4.3 item 2) — e.g. a
    /// voter that was declared `down` and must leave the quorum. Leader-only.
    pub async fn remove_voter(&self, node: NodeId) -> bool {
        self.voter_change(EntryPayload::RemoveVoter(node), node, false)
            .await
    }

    /// Propose a single-server voter-set change to the **control group** and wait
    /// for it to take effect. Voter changes are engine-level entries (not app
    /// commands), so this proposes the `EntryPayload` directly.
    async fn voter_change(&self, change: EntryPayload, node: NodeId, desired: bool) -> bool {
        let MembershipMode::Leader(leader) = &self.inner.mode else {
            return false;
        };
        let raft = self.inner.raft.as_ref().expect("leader mode has raft");
        let Some(control) = raft.group(GroupId::CONTROL) else {
            return false;
        };
        if !control.is_leader() {
            return false;
        }
        control.propose(change);
        let deadline = self.inner.clock.now() + 10 * leader.raft.election_timeout;
        while self.inner.clock.now() < deadline {
            if control.has_voter(node) == desired {
                return true;
            }
            self.inner.clock.sleep(leader.raft.heartbeat_interval).await;
        }
        false
    }

    /// Offer `command` to the Raft leader and wait — bounded by ten election
    /// timeouts — for `done` to observe the committed effect in the local view.
    /// The proposal is re-submitted each poll, so a dropped frame or a leader
    /// change does not strand it (the leader dedups pending commands, and a
    /// re-application is idempotent under `apply_stamped`).
    async fn propose_and_wait(
        &self,
        leader: &LeaderMode,
        command: MembershipCommand,
        done: impl Fn(&Membership) -> bool,
    ) -> bool {
        let deadline = self.inner.clock.now() + 10 * leader.raft.election_timeout;
        loop {
            if done(&self.inner.membership) {
                return true;
            }
            if self.inner.clock.now() >= deadline {
                return false;
            }
            self.submit_proposal(leader, command).await;
            self.inner.clock.sleep(leader.raft.heartbeat_interval).await;
        }
    }

    /// One proposal submission to the **control group** (spec §9.4.3 item 1):
    /// append locally when leading; otherwise send `RaftPropose` to the known
    /// leader, or — knowing none yet — to every configured voter, which forward
    /// it to theirs. The membership command is encoded to the control group's
    /// opaque app payload.
    async fn submit_proposal(&self, leader: &LeaderMode, command: MembershipCommand) {
        let raft = self.inner.raft.as_ref().expect("leader mode has raft");
        let Some(control) = raft.group(GroupId::CONTROL) else {
            return;
        };
        if control.is_leader() {
            control.propose(EntryPayload::App(command.encode()));
            return;
        }
        if let Some(target) = control.leader_hint() {
            let frame = Frame::RaftPropose {
                group: GroupId::CONTROL,
                command: command.encode(),
                forwarded: true,
            };
            let _ = self.inner.transport.send(target, frame).await;
            return;
        }
        for &voter in leader.raft.voters.iter().filter(|&&v| v != self.node()) {
            let frame = Frame::RaftPropose {
                group: GroupId::CONTROL,
                command: command.encode(),
                forwarded: false,
            };
            let _ = self.inner.transport.send(voter, frame).await;
        }
    }

    /// The Raft group `group` if this node runs the consensus engine and hosts
    /// that group (spec §9.4.3). The receive loop routes each group's frames
    /// through it; an unknown group is dropped. Also the basis of the
    /// [`RaftConsensus`](crate::RaftConsensus) seam (see `consensus.rs`).
    pub(crate) fn group(&self, group: GroupId) -> Option<Arc<RaftGroup>> {
        self.inner.raft.as_ref().and_then(|raft| raft.group(group))
    }

    /// The control-plane leader as this node sees it (spec §9.4): the Raft
    /// leader in leader-based mode, the coordinator in gossip-based mode.
    /// `None` in static and registry-based mode, which have no in-cluster
    /// authority.
    pub fn leader(&self) -> Option<NodeId> {
        match &self.inner.mode {
            MembershipMode::Gossip(_) => self.inner.membership.coordinator(),
            MembershipMode::Leader(_) => self
                .inner
                .raft
                .as_ref()
                .and_then(|raft| raft.group(GroupId::CONTROL))
                .and_then(|control| control.leader_hint()),
            _ => None,
        }
    }

    /// The owner of `key` by rendezvous placement over this node's serving set
    /// (utilities spec §2): a pure function of the local view, so converged
    /// nodes agree on every owner. `None` while the serving set is empty.
    pub fn place(&self, key: &[u8]) -> Option<NodeId> {
        crate::placement::owner(&self.inner.membership.serving_members(), key)
    }

    /// Stop this node (spec §9.3): halt the detector and gossip loops and release
    /// the transport's resources (its listener, connections, and background
    /// tasks; closing the inbound path also ends the receive loop). Typically
    /// preceded by [`leave`](Self::leave) so peers learn of the departure before
    /// the node goes away. Idempotent.
    pub fn shutdown(&self) {
        self.inner
            .shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.inner.transport.shutdown();
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.inner
            .shutdown
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Launch a background task on the system's spawner — the seam the
    /// singleton manager (utilities spec §4) starts its tick loop through, and
    /// the one layered runtimes (the agentic harness) launch their I/O on so a
    /// simulated run stays on the seeded scheduler (spec §18.1).
    pub fn launch_task(&self, task: impl std::future::Future<Output = ()> + Send + 'static) {
        self.inner.spawner.launch(Box::pin(task));
    }

    /// Emit onto the observability stream (spec §16). Public so layered
    /// runtimes extending the [`Event`] enum (utilities spec §5, harness spec
    /// §10.4) emit into the same stream the checkers read.
    pub fn emit(&self, event: Event) {
        self.inner.events.emit(event);
    }

    /// The resolved SWIM probe interval — the cadence background utility loops
    /// reuse so they add no second tunable (utilities spec §4).
    pub(crate) fn probe_interval(&self) -> Duration {
        self.inner.swim.probe_interval
    }

    /// Complete every in-flight `ask` to `node` with `err` — the node-down
    /// cascade for in-flight callers (spec §8.1 step 3).
    fn fail_pending_to(&self, node: NodeId, err: CallError) {
        for (_, waiter) in self
            .inner
            .calls
            .take_matching(|(target, _)| *target == node)
        {
            let _ = waiter.send(Err(err.clone()));
        }
    }

    fn pending_remove(&self, call: CallId) {
        self.inner.calls.take(call);
    }

    /// Run the node-down cascade for `node` (spec §8.1): complete its in-flight
    /// callers with `Unreachable` (step 3) and synthesize `Terminated { NodeDown }`
    /// for local watchers of its actors (step 4). Reached from both the gossip
    /// merge and the detector, which down a node by independent paths.
    async fn node_down_cascade(&self, node: NodeId) {
        self.fail_pending_to(node, CallError::Unreachable);
        self.inner.host.synthesize_node_down(node).await;
    }

    async fn remote_ask_inner(
        &self,
        recipient: &ActorId,
        manifest: &'static str,
        payload: Vec<u8>,
        within: Duration,
    ) -> Result<Vec<u8>, CallError> {
        // Routing afterward: a known-down node fails fast (spec §8.1 step 6).
        if self.inner.membership.is_down(recipient.node()) {
            return Err(CallError::Unreachable);
        }

        let call = self.inner.calls.next_id();
        let (tx, rx) = oneshot::channel::<ReplyResult>();
        self.inner.calls.register(call, (recipient.node(), tx));

        let frame = Frame::Envelope {
            recipient: recipient.clone(),
            manifest: manifest.to_string(),
            correlation: Some(call),
            payload,
        };
        if self
            .inner
            .transport
            .send(recipient.node(), frame)
            .await
            .is_err()
        {
            self.pending_remove(call);
            return Err(CallError::Unreachable);
        }

        // Mandatory deadline (spec §14.2); the cascade resolves the waiter with
        // `Unreachable` if the node is downed first (spec §7.2, invariant #2).
        match self.inner.clock.timeout(within, rx).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_canceled)) => {
                self.pending_remove(call);
                Err(CallError::Unreachable)
            }
            Err(_elapsed) => {
                self.pending_remove(call);
                Err(CallError::Timeout)
            }
        }
    }
}

impl<C, E, S, T> ActorSystem for ClusterSystem<C, E, S, T>
where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    fn spawn<A: Actor<System = Self>>(&self, actor: A) -> ActorRef<A> {
        let mut once = Some(actor);
        self.inner.host.spawn_actor(
            self.clone(),
            self.inner.clock.clone(),
            &self.inner.spawner,
            Box::new(move || once.take()),
            None,
        )
    }

    fn spawn_child<A: Actor<System = Self>>(&self, child: A, parent: ActorId) -> ActorRef<A> {
        let mut once = Some(child);
        self.inner.host.spawn_actor(
            self.clone(),
            self.inner.clock.clone(),
            &self.inner.spawner,
            Box::new(move || once.take()),
            Some(parent),
        )
    }

    fn spawn_child_with<A, F>(&self, mut factory: F, parent: ActorId) -> ActorRef<A>
    where
        A: Actor<System = Self>,
        F: FnMut() -> A + Send + 'static,
    {
        self.inner.host.spawn_actor(
            self.clone(),
            self.inner.clock.clone(),
            &self.inner.spawner,
            Box::new(move || Some(factory())),
            Some(parent),
        )
    }

    fn resolve_local<A: Actor<System = Self>>(&self, id: &ActorId) -> Option<Mailbox<A>> {
        self.inner.host.resolve_local(id)
    }

    fn is_local(&self, id: &ActorId) -> bool {
        self.inner.host.is_local(id)
    }

    fn is_serving(&self, node: NodeId) -> bool {
        // Route service discovery away from a node taken out of rotation: one the
        // operator drained for maintenance, or one that is down (spec §9.4, §13).
        // A drained node keeps its registrations and still answers
        // direct calls — it is just not handed out to new callers.
        !self.inner.membership.is_down(node)
            && self.inner.membership.status(node) != Some(MemberStatus::Draining)
    }

    fn codec(&self) -> Arc<dyn Codec> {
        Arc::clone(&self.inner.codec)
    }

    async fn remote_ask(
        &self,
        recipient: &ActorId,
        manifest: &'static str,
        payload: Vec<u8>,
        within: Duration,
    ) -> Result<Vec<u8>, CallError> {
        // Bracket the remote call with events so the no-silent-loss invariant
        // (#1) covers remote asks, not just local ones.
        self.inner.events.emit(Event::AskIssued {
            actor: recipient.clone(),
            manifest,
        });
        let result = self
            .remote_ask_inner(recipient, manifest, payload, within)
            .await;
        self.inner.events.emit(Event::AskOutcome {
            actor: recipient.clone(),
            manifest,
            failed: result.is_err(),
        });
        result
    }

    async fn remote_tell(
        &self,
        recipient: &ActorId,
        manifest: &'static str,
        payload: Vec<u8>,
    ) -> Result<(), CallError> {
        if self.inner.membership.is_down(recipient.node()) {
            return Err(CallError::Unreachable);
        }
        let frame = Frame::Envelope {
            recipient: recipient.clone(),
            manifest: manifest.to_string(),
            correlation: None,
            payload,
        };
        self.inner
            .transport
            .send(recipient.node(), frame)
            .await
            .map_err(|_| CallError::Unreachable)
    }

    fn watch(&self, target: ActorId, watcher: ActorId, deliver: WatchDelivery) {
        // Watch-after-death (invariant #12): a local target that is gone, or a
        // peer node already declared `down`, is reported immediately.
        // Launch the immediate delivery rather than running it inline: delivery
        // now applies mailbox backpressure, and a watcher calling `watch` from
        // inside its own handler must not block on its own mailbox.
        if self.is_local(&target) {
            if !self.inner.host.contains(&target) {
                // Report the actual reason it died (Failed vs Stopped) when still
                // remembered; otherwise default to a graceful stop (spec §12).
                let reason = self
                    .inner
                    .host
                    .termination_reason(&target)
                    .unwrap_or(TerminationReason::Stopped);
                self.inner
                    .spawner
                    .launch(deliver(Terminated { id: target, reason }));
                return;
            }
        } else if self.inner.membership.is_down(target.node()) {
            self.inner.spawner.launch(deliver(Terminated {
                id: target,
                reason: TerminationReason::NodeDown,
            }));
            return;
        }
        // Track locally so a delivered `Terminated` (synthesized on node-down, or
        // arriving as a frame from the target's node) reaches the watcher.
        self.inner
            .host
            .add_watch(target.clone(), watcher.clone(), deliver);

        // For a remote target, register interest with its node so a graceful
        // per-actor stop — not just a node-down — notifies us (spec §12).
        if !self.is_local(&target) {
            let frame = Frame::Watch {
                target: target.clone(),
                watcher,
            };
            let transport = self.inner.transport.clone();
            let node = target.node();
            self.inner.spawner.launch(Box::pin(async move {
                let _ = transport.send(node, frame).await;
            }));
        }
    }

    fn unwatch(&self, target: &ActorId, watcher: &ActorId) {
        self.inner.host.remove_watch(target, watcher);
        if !self.is_local(target) {
            let frame = Frame::Unwatch {
                target: target.clone(),
                watcher: watcher.clone(),
            };
            let transport = self.inner.transport.clone();
            let node = target.node();
            self.inner.spawner.launch(Box::pin(async move {
                let _ = transport.send(node, frame).await;
            }));
        }
    }

    fn node(&self) -> NodeId {
        self.inner.host.node()
    }

    fn next_random(&self) -> u64 {
        self.inner.entropy.next_u64()
    }

    fn receptionist_state(&self) -> Arc<ReceptionistState> {
        Arc::clone(&self.inner.receptionist)
    }

    fn replicate_registration(&self, key: &str, origin: NodeId, id: ActorId) {
        // Broadcast-on-change to every peer (spec §13). Periodic anti-entropy
        // for nodes that join later or miss a frame is a follow-up.
        for peer in self.inner.membership.members() {
            let frame = Frame::Receptionist {
                key: key.to_string(),
                origin,
                actor: id.clone(),
            };
            let transport = self.inner.transport.clone();
            self.inner.spawner.launch(Box::pin(async move {
                let _ = transport.send(peer, frame).await;
            }));
        }
    }
}

/// The inbound receive loop (spec §4.4): dispatch envelopes to local actors,
/// resolve pending `ask`s on replies, and answer SWIM probes.
async fn receive_loop<C, E, S, T>(
    system: ClusterSystem<C, E, S, T>,
    inbound: Receiver<(NodeId, Frame)>,
) where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    while let Ok((from, frame)) = inbound.recv().await {
        // A stopped node processes nothing further (spec §9.3): frames already
        // queued when `shutdown` was called are dropped, not handled. This is
        // what lets a restart hand the node's durable state to a successor —
        // the old incarnation can no longer write to it.
        if system.is_shutting_down() {
            return;
        }
        match frame {
            Frame::Envelope {
                recipient,
                manifest,
                correlation,
                payload,
            } => {
                // Authorization gate (spec §15): an unauthorized message is
                // rejected as a system failure and never reaches the actor. An
                // `ask` gets the failure as its reply; a `tell` is dropped.
                if let Some(authorizer) = &system.inner.authorizer
                    && !authorizer.authorize(from, &recipient, &manifest)
                {
                    if let Some(call) = correlation {
                        let _ = system
                            .inner
                            .transport
                            .send(
                                from,
                                Frame::Reply {
                                    correlation: call,
                                    outcome: Err(CallError::System("unauthorized".into())),
                                },
                            )
                            .await;
                    }
                    continue;
                }
                let codec = system.codec();
                let (reply, reply_rx) = ReplyHandle::channel(Arc::clone(&codec));
                // Decode under this node's system so any `ActorRef` embedded in
                // the message rebinds to a usable handle here (spec §4.4).
                actor_core::with_decoding_system(&system, || {
                    system
                        .inner
                        .host
                        .deliver(&*codec, &recipient, &manifest, &payload, reply);
                });

                match correlation {
                    Some(call) => {
                        let transport = system.inner.transport.clone();
                        system.inner.spawner.launch(Box::pin(async move {
                            let outcome = reply_rx.await.unwrap_or(Err(CallError::DeadLetter));
                            let _ = transport
                                .send(
                                    from,
                                    Frame::Reply {
                                        correlation: call,
                                        outcome,
                                    },
                                )
                                .await;
                        }));
                    }
                    None => drop(reply_rx),
                }
            }
            Frame::Reply {
                correlation,
                outcome,
            } => {
                if let Some((_, tx)) = system.inner.calls.take(correlation) {
                    let _ = tx.send(outcome);
                }
            }
            // SWIM probe: hearing from `from` is direct liveness evidence; merge
            // its gossip, then answer with our own incarnation + digest.
            Frame::Ping {
                seq,
                incarnation,
                digest,
            } => {
                let now = system.inner.clock.now();
                system
                    .inner
                    .membership
                    .mark_alive_direct(from, incarnation, now);
                gossip_merge(&system, digest, now).await;
                let ack = Frame::Ack {
                    seq,
                    incarnation: system.inner.membership.self_incarnation(),
                    digest: system.inner.membership.digest(),
                };
                let _ = system.inner.transport.send(from, ack).await;
            }
            Frame::Ack {
                seq,
                incarnation,
                digest,
            } => {
                let now = system.inner.clock.now();
                system
                    .inner
                    .membership
                    .mark_alive_direct(from, incarnation, now);
                gossip_merge(&system, digest, now).await;
                if let Some(tx) = system.inner.pings.take(seq) {
                    // Hand the prober the target's incarnation, so a relayed
                    // probe can report it in its `IndirectAck` (spec §10).
                    let _ = tx.send(incarnation);
                }
            }
            // A peer asks us to probe `target` on its behalf (spec §10 #2): merge
            // its gossip, then relay a probe and forward the result.
            Frame::PingReq {
                seq,
                target,
                incarnation,
                digest,
            } => {
                let now = system.inner.clock.now();
                system
                    .inner
                    .membership
                    .mark_alive_direct(from, incarnation, now);
                gossip_merge(&system, digest, now).await;
                system.inner.spawner.launch(Box::pin(relay_probe(
                    system.clone(),
                    from,
                    seq,
                    target,
                )));
            }
            // A helper relayed that `target` answered (spec §10 #2): clear our
            // suspicion of `target` and complete the indirect wait.
            Frame::IndirectAck {
                seq,
                target,
                incarnation,
                digest,
            } => {
                let now = system.inner.clock.now();
                system
                    .inner
                    .membership
                    .mark_alive_direct(target, incarnation, now);
                gossip_merge(&system, digest, now).await;
                if let Some(tx) = system.inner.pings.take(seq) {
                    let _ = tx.send(incarnation);
                }
            }
            // Cross-node death watch (spec §12): a remote `watcher` registers
            // interest in our local `target`.
            Frame::Watch { target, watcher } => {
                if system.inner.host.contains(&target) {
                    // Deliver a `Terminated` frame back to the watcher's node when
                    // the target stops. A `Weak` handle avoids a reference cycle
                    // (system → host → watch closure → system).
                    let weak: Weak<Inner<C, E, S, T>> = Arc::downgrade(&system.inner);
                    let watcher_node = watcher.node();
                    let watcher_id = watcher.clone();
                    let deliver: WatchDelivery = Arc::new(move |signal: Terminated| {
                        if let Some(inner) = weak.upgrade() {
                            let frame = Frame::Terminated {
                                target: signal.id,
                                watcher: watcher_id.clone(),
                                reason: signal.reason,
                            };
                            let transport = inner.transport.clone();
                            inner.spawner.launch(Box::pin(async move {
                                let _ = transport.send(watcher_node, frame).await;
                            }));
                        }
                        // Forwarding the signal over the transport is fire-and-forget
                        // (the launched task carries it), so this resolves immediately.
                        Box::pin(async {}) as BoxFuture<'static, ()>
                    });
                    system.inner.host.add_watch(target, watcher, deliver);
                } else {
                    // Already gone: report immediately (§12), with the true reason
                    // (Failed vs Stopped) when still remembered, else a graceful
                    // stop by default.
                    let reason = system
                        .inner
                        .host
                        .termination_reason(&target)
                        .unwrap_or(TerminationReason::Stopped);
                    let frame = Frame::Terminated {
                        target: target.clone(),
                        watcher: watcher.clone(),
                        reason,
                    };
                    let _ = system.inner.transport.send(watcher.node(), frame).await;
                }
            }
            Frame::Unwatch { target, watcher } => {
                system.inner.host.remove_watch(&target, &watcher);
            }
            // The target's node tells us a watched actor terminated. The frame is
            // addressed to one specific `watcher` (the target's node sends one per
            // remote watcher), so deliver it only there — fanning to every local
            // watcher of `target` would hand the others a spurious extra signal
            // when one of them re-watches the dead actor (invariant #11).
            Frame::Terminated {
                target,
                watcher,
                reason,
            } => {
                system
                    .inner
                    .host
                    .deliver_terminated_to(&target, &watcher, reason)
                    .await;
            }
            Frame::Receptionist { key, origin, actor } => {
                system
                    .receptionist()
                    .apply_remote_registration(&key, origin, actor);
            }
            Frame::ReceptionistSync { entries } => {
                // Merge a peer's full registry, skipping entries from a node we
                // already consider `down` so anti-entropy never resurrects a
                // pruned registration (spec §8.1 step 5, §13).
                let receptionist = system.receptionist();
                for entry in entries {
                    if !system.inner.membership.is_down(entry.origin) {
                        receptionist.apply_remote_registration(
                            &entry.key,
                            entry.origin,
                            entry.actor,
                        );
                    }
                }
            }
            // Raft consensus traffic (leader-based mode, spec §9.4.3) rides the
            // ordinary transport as system messages; a node not in leader mode
            // ignores it.
            Frame::RaftVote {
                group,
                term,
                candidate,
                last_index,
                last_term,
            } => {
                if let Some(raft) = system.group(group) {
                    let out = raft.handle_vote(
                        candidate,
                        term,
                        last_index,
                        last_term,
                        system.inner.clock.now(),
                        &system.inner.entropy,
                    );
                    apply_raft_output(&system, group, out).await;
                }
            }
            Frame::RaftVoteReply {
                group,
                term,
                granted,
            } => {
                if let Some(raft) = system.group(group) {
                    let out = raft.handle_vote_reply(
                        from,
                        term,
                        granted,
                        system.inner.clock.now(),
                        &system.inner.entropy,
                    );
                    apply_raft_output(&system, group, out).await;
                }
            }
            Frame::RaftAppend {
                group,
                term,
                leader,
                prev_index,
                prev_term,
                entries,
                commit,
            } => {
                if let Some(raft) = system.group(group) {
                    let out = raft.handle_append(
                        leader,
                        term,
                        prev_index,
                        prev_term,
                        entries,
                        commit,
                        system.inner.clock.now(),
                        &system.inner.entropy,
                    );
                    apply_raft_output(&system, group, out).await;
                }
            }
            Frame::RaftAppendReply {
                group,
                term,
                ok,
                match_index,
            } => {
                if let Some(raft) = system.group(group) {
                    let out = raft.handle_append_reply(
                        from,
                        term,
                        ok,
                        match_index,
                        system.inner.clock.now(),
                        &system.inner.entropy,
                    );
                    apply_raft_output(&system, group, out).await;
                }
            }
            Frame::RaftInstallSnapshot {
                group,
                term,
                leader,
                snapshot_index,
                snapshot_term,
                voters,
                learners,
                data,
            } => {
                if let Some(raft) = system.group(group) {
                    let out = raft.handle_install_snapshot(
                        leader,
                        term,
                        snapshot_index,
                        snapshot_term,
                        voters,
                        learners,
                        data,
                        system.inner.clock.now(),
                        &system.inner.entropy,
                    );
                    apply_raft_output(&system, group, out).await;
                }
            }
            // An application command offered to a group's leader (spec §9.4.3
            // item 1): append it when leading, forward it once when not. A
            // forwarded proposal landing on a non-leader is dropped — the
            // proposer's bounded re-submission handles the stale-leader case.
            Frame::RaftPropose {
                group,
                command,
                forwarded,
            } => {
                if let Some(raft) = system.group(group) {
                    if raft.is_leader() {
                        raft.propose(EntryPayload::App(command));
                    } else if !forwarded && let Some(target) = raft.leader_hint() {
                        let frame = Frame::RaftPropose {
                            group,
                            command,
                            forwarded: true,
                        };
                        let _ = system.inner.transport.send(target, frame).await;
                    }
                }
            }
        }
    }
}

impl<C, E, S, T> RaftConsensus for ClusterSystem<C, E, S, T>
where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    fn node(&self) -> NodeId {
        self.inner.host.node()
    }

    fn cluster_voters(&self) -> Vec<NodeId> {
        // The control group's current voter set — the consensus-agreed cluster
        // configuration, identical on every admitted node (spec §9.4.3).
        self.inner
            .raft
            .as_ref()
            .and_then(|raft| raft.group(GroupId::CONTROL))
            .map(|control| control.voters())
            .unwrap_or_default()
    }

    fn configured_voters(&self) -> Vec<NodeId> {
        // The statically configured founding voter set — identical and unchanging
        // on every node (spec §9.4.3), unlike the live `cluster_voters`.
        match &self.inner.mode {
            MembershipMode::Leader(leader) => leader.raft.voters.clone(),
            _ => Vec::new(),
        }
    }

    fn reconfigure_group(&self, group: GroupId, voters: Vec<NodeId>) {
        let Some(raft) = self.group(group) else {
            return;
        };
        if !raft.is_leader() {
            return; // only the leader proposes config changes (spec §9.4.3)
        }
        let desired: std::collections::BTreeSet<NodeId> = voters.into_iter().collect();
        let current: std::collections::BTreeSet<NodeId> = raft.voters().into_iter().collect();
        for &node in desired.difference(&current) {
            raft.propose(EntryPayload::AddVoter(node));
        }
        for &node in current.difference(&desired) {
            raft.propose(EntryPayload::RemoveVoter(node));
        }
    }

    fn create_group(&self, group: GroupId, voters: Vec<NodeId>, learners: Vec<NodeId>) {
        if let Some(raft) = self.inner.raft.as_ref() {
            raft.create_group(group, voters, learners, self.inner.clock.now());
        }
    }

    fn subscribe_commits(&self, group: GroupId) -> Receiver<Committed> {
        let (tx, rx) = async_channel::unbounded();
        // Seed a subscriber that came up over a reloaded, compacted log with the
        // snapshot it reloaded, as the first thing on its stream — so a node that
        // restarts from a snapshot (not a full log) rebuilds its projection from
        // it, the leaderless counterpart of a leader InstallSnapshot (spec §9).
        // Sent before the sender is registered, so it strictly precedes any commit
        // a later tick fans out to this sink. `None` for an uncompacted group.
        if let Some(snapshot) = self.group(group).and_then(|g| g.snapshot_observation()) {
            let _ = tx.try_send(snapshot);
        }
        self.inner
            .commit_sinks
            .lock()
            .expect("commit sinks mutex poisoned")
            .entry(group)
            .or_default()
            .push(tx);
        rx
    }

    fn compact(&self, group: GroupId, index: u64, snapshot: Vec<u8>) {
        if let Some(raft) = self.group(group) {
            raft.compact(index, snapshot);
        }
    }

    async fn propose_to(&self, group: GroupId, command: Vec<u8>) {
        let Some(raft) = self.group(group) else {
            return;
        };
        if raft.is_leader() {
            raft.propose(EntryPayload::App(command));
            return;
        }
        if let Some(target) = raft.leader_hint() {
            let frame = Frame::RaftPropose {
                group,
                command,
                forwarded: true,
            };
            let _ = self.inner.transport.send(target, frame).await;
            return;
        }
        let self_node = self.inner.host.node();
        for voter in raft.voters().into_iter().filter(|&v| v != self_node) {
            let frame = Frame::RaftPropose {
                group,
                command: command.clone(),
                forwarded: false,
            };
            let _ = self.inner.transport.send(voter, frame).await;
        }
    }

    fn group_is_leader(&self, group: GroupId) -> bool {
        self.group(group).map(|g| g.is_leader()).unwrap_or(false)
    }

    fn group_term(&self, group: GroupId) -> Option<u64> {
        self.group(group).map(|g| g.term())
    }

    fn group_leader(&self, group: GroupId) -> Option<NodeId> {
        self.group(group).and_then(|g| g.leader_hint())
    }

    fn next_u64(&self) -> u64 {
        self.inner.entropy.next_u64()
    }

    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        let clock = self.inner.clock.clone();
        Box::pin(async move { clock.sleep(dur).await })
    }

    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.inner.spawner.launch(task);
    }
}

/// Act on one group's Raft step (spec §9.4.3): emit the election event (tagged
/// with `group`), apply the group's newly committed application commands, and
/// send the produced consensus frames. For the control group, committed commands
/// are membership transitions — decoded and merged into the local view, each
/// stamped with its commit index (spec §9.2), running the node-down cascade
/// (spec §8.1) for a committed `Down`/`Leave`. Other groups' committed entries
/// belong to their owner (granary shards, later) and are not membership.
async fn apply_raft_output<C, E, S, T>(
    system: &ClusterSystem<C, E, S, T>,
    group: GroupId,
    out: RaftOutput,
) where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    if let Some(term) = out.elected {
        system.inner.events.emit(Event::LeaderElected {
            node: system.node(),
            term,
            group: group.value(),
        });
    }
    if group == GroupId::CONTROL {
        for observation in out.committed {
            // The control group never compacts, so it only ever applies commands.
            let Committed::Apply { index, command, .. } = observation else {
                continue;
            };
            // The control group's app payload is a `MembershipCommand`; a
            // malformed payload is defensively ignored, never panicked on.
            let Some(command) = MembershipCommand::decode(&command) else {
                continue;
            };
            let (node, status) = match command {
                MembershipCommand::Admit(node) | MembershipCommand::Resume(node) => {
                    (node, MemberStatus::Up)
                }
                MembershipCommand::Drain(node) => (node, MemberStatus::Draining),
                MembershipCommand::Leave(node) | MembershipCommand::Down(node) => {
                    (node, MemberStatus::Down)
                }
            };
            let now = system.inner.clock.now();
            if system
                .inner
                .membership
                .apply_stamped(node, status, index, now)
            {
                system.node_down_cascade(node).await;
            }
        }
    } else {
        // An application group (granary's sharded journal): publish each
        // committed entry to its subscribers, in commit order. The send is a
        // synchronous `try_send` on an unbounded channel — no `.await` between
        // draining `out.committed` and delivering — so commit order is preserved
        // and a slow consumer cannot interleave another group's batch.
        if !out.committed.is_empty() {
            let mut sinks = system
                .inner
                .commit_sinks
                .lock()
                .expect("commit sinks mutex poisoned");
            if let Some(senders) = sinks.get_mut(&group) {
                senders.retain(|sender| !sender.is_closed());
                for observation in &out.committed {
                    for sender in senders.iter() {
                        let _ = sender.try_send(observation.clone());
                    }
                }
            }
        }
    }
    for (to, frame) in out.frames {
        let _ = system.inner.transport.send(to, frame).await;
    }
}

/// The Raft driver (leader-based mode, spec §9.4.3): each heartbeat interval,
/// tick the consensus state machine — elections on a follower/candidate whose
/// timer fired, replication and quorum commit on the leader — and, when this
/// node leads, perform the leader's control-plane duties: propose `Admit` for
/// reachable joiners, `Leave` for members announcing departure (committed at
/// the departing node's request, spec §9.3), and `Down` for members the
/// configured downing policy condemns (spec §9.4.3 item 4) — each a log entry,
/// so every transition stays quorum-gated (invariant #22).
async fn raft_driver<C, E, S, T>(system: ClusterSystem<C, E, S, T>, mode: LeaderMode)
where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    let raft = Arc::clone(system.inner.raft.as_ref().expect("leader mode has raft"));
    loop {
        system.inner.clock.sleep(mode.raft.heartbeat_interval).await;
        if system.is_shutting_down() {
            return;
        }
        let now = system.inner.clock.now();
        // Tick every group; apply each group's output under its own id.
        for (group, out) in raft.tick_all(now, &system.inner.entropy) {
            apply_raft_output(&system, group, out).await;
        }
        // The control group's leader performs the membership control-plane
        // duties, encoding each transition as the group's opaque app payload.
        if let Some(control) = raft.group(GroupId::CONTROL)
            && control.is_leader()
        {
            let propose = |command: MembershipCommand| {
                control.propose(EntryPayload::App(command.encode()));
            };
            for node in system.inner.membership.admission_candidates() {
                propose(MembershipCommand::Admit(node));
            }
            for node in system.inner.membership.leaving_members() {
                propose(MembershipCommand::Leave(node));
            }
            for node in system
                .inner
                .membership
                .downing_candidates(mode.downing, now)
            {
                propose(MembershipCommand::Down(node));
            }
        }
    }
}

/// Merge a gossiped digest into membership and run the node-down cascade for any
/// node the merge newly declares `down` (spec §8.1, §9.2).
async fn gossip_merge<C, E, S, T>(
    system: &ClusterSystem<C, E, S, T>,
    digest: Vec<crate::membership::MemberDigest>,
    now: actor_core::Instant,
) where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    for node in system.inner.membership.merge(digest, now) {
        system.node_down_cascade(node).await;
    }
}

/// The SWIM failure detector (spec §10): probe a random member each interval,
/// suspect it on a missed `Ack`, and apply suspicion/downing timeouts —
/// completing the in-flight calls of any node it downs (spec §8.1).
async fn detector<C, E, S, T>(system: ClusterSystem<C, E, S, T>, config: SwimConfig)
where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    loop {
        system.inner.clock.sleep(config.probe_interval).await;
        if system.is_shutting_down() {
            return;
        }
        let now = system.inner.clock.now();

        // Apply suspicion/downing timeouts and run the cascade for newly-downed:
        // complete their in-flight callers (step 3) and notify watchers (step 4).
        for node in system.inner.membership.tick(now) {
            system.node_down_cascade(node).await;
        }

        let Some(target) = system
            .inner
            .membership
            .pick_probe_target(&system.inner.entropy)
        else {
            continue;
        };

        let seq = system.inner.pings.next_id();
        let (tx, rx) = oneshot::channel::<u64>();
        system.inner.pings.register(seq, tx);
        let frame = Frame::Ping {
            seq,
            incarnation: system.inner.membership.self_incarnation(),
            digest: system.inner.membership.digest(),
        };
        let _ = system.inner.transport.send(target, frame).await;

        // A received `Ack` is handled by the receive loop (mark alive + merge).
        let direct_ok = matches!(system.inner.clock.timeout(config.rtt, rx).await, Ok(Ok(_)));
        system.inner.pings.take(seq);

        // On a missed direct probe, enlist `k` helpers to probe indirectly before
        // suspecting — a single bad link should not cause a false suspicion
        // (spec §10 #2). Suspect only if both the direct and indirect probes fail.
        if !direct_ok && !indirect_probe(&system, target, &config).await {
            system
                .inner
                .membership
                .mark_suspect(target, system.inner.clock.now());
        }
    }
}

/// Indirect probing (spec §10 #2): ask up to `k` random helpers to probe
/// `target`, returning `true` if any relays back an `IndirectAck`. The relayed
/// ack also clears our suspicion via the receive loop. `false` if there are no
/// helpers or none answer within `rtt`.
async fn indirect_probe<C, E, S, T>(
    system: &ClusterSystem<C, E, S, T>,
    target: NodeId,
    config: &SwimConfig,
) -> bool
where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    let helpers =
        system
            .inner
            .membership
            .pick_helpers(config.indirect_count, target, &system.inner.entropy);
    if helpers.is_empty() {
        return false;
    }

    let seq = system.inner.pings.next_id();
    let (tx, rx) = oneshot::channel::<u64>();
    system.inner.pings.register(seq, tx);

    let incarnation = system.inner.membership.self_incarnation();
    let digest = system.inner.membership.digest();
    for helper in helpers {
        let frame = Frame::PingReq {
            seq,
            target,
            incarnation,
            digest: digest.clone(),
        };
        let _ = system.inner.transport.send(helper, frame).await;
    }

    let alive = matches!(system.inner.clock.timeout(config.rtt, rx).await, Ok(Ok(_)));
    system.inner.pings.take(seq);
    alive
}

/// Helper side of indirect probing (spec §10 #2): probe `target` ourselves and,
/// if it answers within `rtt`, relay an `IndirectAck` (carrying the target's
/// incarnation) back to `requester`.
async fn relay_probe<C, E, S, T>(
    system: ClusterSystem<C, E, S, T>,
    requester: NodeId,
    requester_seq: u64,
    target: NodeId,
) where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    let seq = system.inner.pings.next_id();
    let (tx, rx) = oneshot::channel::<u64>();
    system.inner.pings.register(seq, tx);
    let ping = Frame::Ping {
        seq,
        incarnation: system.inner.membership.self_incarnation(),
        digest: system.inner.membership.digest(),
    };
    let _ = system.inner.transport.send(target, ping).await;

    // Bound the relayed probe by the same RTT as a direct one.
    let rtt = system.inner.swim.rtt;
    match system.inner.clock.timeout(rtt, rx).await {
        Ok(Ok(target_incarnation)) => {
            let ack = Frame::IndirectAck {
                seq: requester_seq,
                target,
                incarnation: target_incarnation,
                digest: system.inner.membership.digest(),
            };
            let _ = system.inner.transport.send(requester, ack).await;
        }
        _ => {
            system.inner.pings.take(seq);
        }
    }
}

/// The registry sync loop (spec §9.4.2): every `interval`, fetch the external
/// registry's state and apply it to the local membership view, each entry
/// stamped with its revision so the merge converges on the registry's latest
/// state regardless of sync order (spec §9.2).
///
/// - A snapshot whose global revision is **lower** than the last applied one is
///   a stale read and is skipped — application is monotonic, so a laggy or
///   replayed read can never revert a drain or resurrect a pruned tombstone.
/// - A member **absent** from the snapshot that a previous sync knew is
///   deregistered: it moves `down` at the snapshot's revision and the node-down
///   cascade runs (spec §8.1, §9.4.2 items 3–4).
/// - Registry **unavailability pauses membership changes only** (spec §9.4.2
///   item 6): a failed fetch changes nothing, and the data plane keeps running
///   on the last-synced view.
async fn registry_sync<C, E, S, T>(
    system: ClusterSystem<C, E, S, T>,
    client: Arc<dyn RegistryClient>,
    interval: Duration,
) where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    let mut last_revision: Option<u64> = None;
    // The members the registry was last seen to contain, so a later absence is
    // recognized as a deregistration.
    let mut registered: std::collections::BTreeSet<NodeId> = std::collections::BTreeSet::new();
    loop {
        if let Ok(snapshot) = client.fetch().await {
            // Skip stale reads: application must be monotonic in the registry's
            // global revision (spec §9.4.2 item 1).
            if last_revision.is_none_or(|last| snapshot.revision >= last) {
                let advanced = last_revision != Some(snapshot.revision);
                last_revision = Some(snapshot.revision);
                let now = system.inner.clock.now();
                let mut downed = Vec::new();
                let mut present = std::collections::BTreeSet::new();
                for entry in &snapshot.entries {
                    present.insert(entry.node);
                    let status = match entry.state {
                        RegistryState::Up => MemberStatus::Up,
                        RegistryState::Draining => MemberStatus::Draining,
                    };
                    if system.inner.membership.apply_stamped(
                        entry.node,
                        status,
                        entry.revision,
                        now,
                    ) {
                        downed.push(entry.node);
                    }
                }
                for &node in registered.iter() {
                    if !present.contains(&node)
                        && system.inner.membership.apply_stamped(
                            node,
                            MemberStatus::Down,
                            snapshot.revision,
                            now,
                        )
                    {
                        downed.push(node);
                    }
                }
                registered = present;
                for node in downed {
                    system.node_down_cascade(node).await;
                }
                if advanced {
                    system.inner.events.emit(Event::RegistrySynced {
                        observer: system.node(),
                        revision: snapshot.revision,
                    });
                }
            }
        }
        system.inner.clock.sleep(interval).await;
        if system.is_shutting_down() {
            return;
        }
    }
}

/// Receptionist anti-entropy (spec §13): every `interval`, push this node's full
/// registry to a random member. Registrations replicate broadcast-on-change, so
/// a node that joined late or missed a broadcast would otherwise never learn
/// them; periodic push gossip converges the cluster without the registrant
/// having to re-register. Down origins are skipped so a pruned registration is
/// never resurrected (spec §8.1 step 5).
async fn receptionist_gossip<C, E, S, T>(system: ClusterSystem<C, E, S, T>, interval: Duration)
where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    loop {
        system.inner.clock.sleep(interval).await;
        if system.is_shutting_down() {
            return;
        }

        let entries: Vec<ReceptionistEntry> = system
            .inner
            .receptionist
            .digest()
            .into_iter()
            .filter(|(_, origin, _)| !system.inner.membership.is_down(*origin))
            .map(|(key, origin, actor)| ReceptionistEntry { key, origin, actor })
            .collect();
        if entries.is_empty() {
            continue;
        }

        let Some(target) = system
            .inner
            .membership
            .pick_probe_target(&system.inner.entropy)
        else {
            continue;
        };
        let _ = system
            .inner
            .transport
            .send(target, Frame::ReceptionistSync { entries })
            .await;
    }
}
