//! The networked `ClusterSystem` (spec §2.8, §2.13, §7).
//!
//! `ClusterSystem` is the reference [`ActorSystem`] for multiple nodes. It
//! reuses the local actor machinery ([`LocalHost`]) and adds the network
//! boundary: outbound `remote_ask`/`remote_tell` over a [`Transport`], an
//! inbound receive loop that decodes envelopes and routes replies, and a SWIM
//! failure detector that maintains [`Membership`] reachability and drives the
//! node-down cascade (spec §5.1): a node declared `down` completes its in-flight
//! callers with `Unreachable` rather than letting them hang.
//!
//! It runs the configured membership **control plane** (spec §6.4): the registry
//! sync loop in registry-based mode, the Raft driver in leader-based mode, the
//! coordinator lifecycle in gossip-based mode. It also prunes via death watch and
//! runs the receptionist with broadcast-on-change plus periodic anti-entropy
//! (spec §2.5, §2.6).

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
use actor_core::Codec;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::EventSink;
use actor_core::Mailbox;
use actor_core::Manifest;
use actor_core::NodeId;
use actor_core::ReplyHandle;
use actor_core::ReplyResult;
use actor_core::Spawner;
use actor_core::Terminated;
use actor_core::TerminationReason;
use actor_core::host::ActorFactory;
use actor_core::host::LocalHost;
use actor_core::host::ReceptionistState;
use actor_core::host::WatchDelivery;
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
use crate::protocol::RaftBeat;
use crate::protocol::RaftBeatReply;
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

/// Authorizes inbound messages per association (spec §9). Consulted before an
/// envelope is delivered; a denied message is rejected as a system failure and
/// never reaches the actor (so deserialization side effects are also avoided).
/// A system without one permits every message that clears the transport
/// handshake.
pub trait Authorizer: Send + Sync + 'static {
    /// Whether `peer` may deliver `manifest` to `recipient` (spec §9).
    fn authorize(&self, peer: NodeId, recipient: &ActorId, manifest: &str) -> bool;
}

/// Configuration for a [`ClusterSystem`] node.
pub struct ClusterConfig {
    /// The wire codec (spec §2.11).
    pub codec: Arc<dyn Codec>,
    /// Per-actor bounded mailbox capacity (spec §3.3).
    pub mailbox_capacity: usize,
    /// Observability sink (spec §2.7).
    pub events: Arc<dyn EventSink>,
    /// Which control plane governs membership (spec §6.4): a fixed
    /// [`Static`](MembershipMode::Static) roster, an external
    /// [`Registry`](MembershipMode::Registry), a self-hosted Raft log behind an
    /// elected [`Leader`](MembershipMode::Leader), or peer-to-peer
    /// [`Gossip`](MembershipMode::Gossip). `Static` without the observe-only
    /// detector runs no SWIM loop.
    pub membership: MembershipMode,
    /// Start this node as a joiner (spec §6.3): it enters the cluster `Joining`
    /// and is admitted to `Up` by the mode's authority (the gossip coordinator,
    /// or a committed log entry). Meaningful in gossip- and leader-based mode;
    /// static members always start `Up`, and in registry-based mode admission
    /// *is* the registry entry (spec §6.4.2 item 2). `false` (the default)
    /// starts the node as a founding `Up` member.
    pub joining: bool,
    /// Per-message authorization (spec §9); `None` permits every message that
    /// clears the transport handshake.
    pub authorizer: Option<Arc<dyn Authorizer>>,
    /// This **process** incarnation, stamped onto every actor id the node assigns
    /// (spec §2.2). It must differ from the value any predecessor process used
    /// under this `NodeId`, or a peer's ref from before a restart resolves against
    /// whatever actor now holds that path; see
    /// [`LocalHost::with_incarnation`](actor_core::host::LocalHost::with_incarnation).
    /// A deployment that never restarts a node in place can leave it `0`.
    pub incarnation: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            codec: Arc::new(actor_core::JsonCodec),
            mailbox_capacity: 64,
            events: Arc::new(()),
            membership: MembershipMode::Static { detector: None },
            joining: false,
            authorizer: None,
            incarnation: 0,
        }
    }
}

/// A subscriber to one Raft group's committed `(index, app-bytes)` stream
/// (the [`RaftConsensus`] seam).
type CommitSink = Sender<Committed>;

struct Inner<C, E, S, T> {
    clock: C,
    entropy: E,
    spawner: S,
    transport: T,
    codec: Arc<dyn Codec>,
    host: LocalHost,
    membership: Membership,
    /// The configured control plane (spec §6.4), kept so the operator API can
    /// dispatch to the mode's authority.
    mode: MembershipMode,
    /// The multi-group consensus engine, in leader-based mode (spec §6.4.3);
    /// `None` elsewhere. It hosts the membership control group
    /// ([`GroupId::CONTROL`]) plus a group per replicated shard.
    raft: Option<Arc<MultiRaft>>,
    /// Resolved SWIM parameters, so loops spawned without the config (e.g. a
    /// relayed indirect probe) can still read `rtt`/`k` (spec §7).
    swim: SwimConfig,
    events: Arc<dyn EventSink>,
    /// In-flight `ask`s awaiting a reply, keyed by correlation id. Each waiter
    /// carries its target node so the cascade can complete it on a node-down
    /// (spec §5.1 step 3).
    calls: Correlator<CallId, (NodeId, oneshot::Sender<ReplyResult>)>,
    /// In-flight SWIM probes awaiting an `Ack` (direct, indirect, or a helper's
    /// relayed probe), keyed by seq. The completion carries the target's
    /// incarnation, so a relay can report it back to the requester (spec §7).
    pings: Correlator<u64, oneshot::Sender<u64>>,
    receptionist: Arc<ReceptionistState>,
    /// Per-group subscribers to committed application entries (the
    /// [`RaftConsensus`] seam). A non-`CONTROL` group's
    /// committed entries are broadcast here in `apply_raft_output`; the control
    /// group's drive membership instead and are never published.
    commit_sinks: Mutex<BTreeMap<GroupId, Vec<CommitSink>>>,
    /// Set on a graceful stop (spec §6.3): the detector and receptionist gossip
    /// loops return once they see it.
    shutdown: std::sync::atomic::AtomicBool,
    /// Optional per-message authorization (spec §9).
    authorizer: Option<Arc<dyn Authorizer>>,
}

/// Which way a single-server voter-set change moves a node (spec §6.4.3 item 2).
///
/// One value rather than an entry plus a flag saying what to wait for: the entry
/// proposed and the local view that confirms it are two readings of the same
/// intent, and separate parameters let a call site state them differently.
#[derive(Clone, Copy, Debug)]
enum VoterChange {
    /// Into the Raft quorum.
    Add,
    /// Out of it, e.g. a voter declared `down` that must leave the quorum.
    Remove,
}

impl VoterChange {
    /// The engine-level entry that carries this change for `node`.
    fn payload(self, node: NodeId) -> EntryPayload {
        match self {
            VoterChange::Add => EntryPayload::AddVoter(node),
            VoterChange::Remove => EntryPayload::RemoveVoter(node),
        }
    }

    /// Whether `node` is a voter once this change has taken: the local view the
    /// proposer polls for.
    fn leaves_node_voting(self) -> bool {
        matches!(self, VoterChange::Add)
    }
}

/// A networked, multi-node actor system (spec §2.8, §2.13). Cloning shares the same
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
    /// launch the receive loop, and (if SWIM is configured) the detector.
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
        let host = LocalHost::with_incarnation(
            node,
            Arc::clone(&config.events),
            config.mailbox_capacity,
            config.incarnation,
        );
        let membership = Membership::new(node, &mode, Arc::clone(&config.events), config.joining);
        let raft = match &mode {
            MembershipMode::Leader(leader) => {
                // The cluster layer owns the decision that a control group exists
                // (spec §6.4.3): build the generic engine, then create the
                // well-known `GroupId::CONTROL` from the configured voter set.
                let engine = Arc::new(MultiRaft::new(node, &leader.raft));
                engine.create_group(
                    GroupId::CONTROL,
                    leader.raft.voters.clone(),
                    Vec::new(),
                    clock.now(),
                );
                Some(engine)
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
            .launch(Box::pin(system.clone().receive_loop(inbound)));
        if mode.detector().is_some() {
            system
                .inner
                .spawner
                .launch(Box::pin(system.clone().detector()));
            // Anti-entropy rides alongside the detector on the same cadence: it
            // only makes sense for a live cluster, and gating it on SWIM keeps
            // SWIM-off systems quiescent (spec §2.5, §2.6, §2.12.2).
            system
                .inner
                .spawner
                .launch(Box::pin(system.clone().anti_entropy(swim.probe_interval)));
        }
        // Registry-based mode (spec §6.4.2).
        if let MembershipMode::Registry(registry) = &mode {
            system.inner.spawner.launch(Box::pin(
                system
                    .clone()
                    .registry_sync(Arc::clone(&registry.client), registry.sync_interval),
            ));
        }
        // Leader-based mode (spec §6.4.3).
        if let MembershipMode::Leader(leader) = &mode {
            system
                .inner
                .spawner
                .launch(Box::pin(system.clone().raft_driver(leader.clone())));
        }
        system
    }

    /// This node's identity.
    pub fn node(&self) -> NodeId {
        self.inner.host.node()
    }

    /// The system entropy source.
    pub fn entropy(&self) -> &E {
        &self.inner.entropy
    }

    /// Read access to this node's membership view. The mutators are
    /// crate-private (the detector loops are the only writers), so this view
    /// cannot be used to bypass the control plane's authority rules.
    pub fn membership(&self) -> &Membership {
        &self.inner.membership
    }

    /// Introduce a peer into this node's roster as a full `Up` member (spec §6.3).
    pub fn add_member(&self, node: NodeId) {
        self.inner
            .membership
            .add_member(node, self.inner.clock.now());
    }

    /// Begin a graceful leave (spec §6.3): announce `Leaving` in gossip. The
    /// mode's authority finalizes this node to `Down`, and watchers of its
    /// actors are notified through the node-down cascade (spec §5.1, §2.5). The
    /// caller drains and shuts the node down after announcing.
    pub fn leave(&self) {
        self.inner.membership.begin_leaving();
    }

    /// **Admit** `node` into the member set as a full `Up` member, through the
    /// mode's authority (spec §6.3): a registry registration (registry-based,
    /// spec §6.4.2) (admission is the entry itself), or a committed `Admit`
    /// entry (leader-based, spec §6.4.3). In static and gossip-based mode there
    /// is no authority to command (joiners are admitted by the coordinator,
    /// spec §6.4.4), so this returns `false`. Returns whether the change took:
    /// the registry acknowledged it, or this node observed the committed
    /// transition; the *cluster* converges asynchronously through the mode's
    /// dissemination.
    pub async fn admit(&self, node: NodeId) -> bool {
        self.apply_membership_op(MembershipCommand::Admit(node))
            .await
    }

    /// **Drain** `node` for maintenance, the reversible cordon (spec §6.1,
    /// §6.4), through the mode's authority: a registry state change
    /// (registry-based) or a committed `Drain` entry (leader-based). The node
    /// stays a member (no `down`, no death watch, in-flight calls unaffected);
    /// [`resume`](Self::resume) returns it to `Up`. `false` in modes without an
    /// authoritative control plane.
    pub async fn drain(&self, node: NodeId) -> bool {
        self.apply_membership_op(MembershipCommand::Drain(node))
            .await
    }

    /// **Resume** a drained `node` after maintenance (spec §6.1, §6.4): the
    /// reverse of [`drain`](Self::drain). `false` in modes without an
    /// authoritative control plane.
    pub async fn resume(&self, node: NodeId) -> bool {
        self.apply_membership_op(MembershipCommand::Resume(node))
            .await
    }

    /// **Decommission** `node`: terminally remove it from the cluster through
    /// the mode's authority (spec §6.4); a registry deregistration
    /// (registry-based), whose revision finalizes `down`, or a committed `Down`
    /// entry (leader-based). Irrevocable (invariant #15); each node runs the
    /// node-down cascade (spec §5.1) as the decision reaches it: every
    /// in-flight `ask` to `node` completes `Unreachable`, and watchers of its
    /// actors receive `Terminated { NodeDown }`. `false` in modes without an
    /// authoritative control plane.
    pub async fn decommission(&self, node: NodeId) -> bool {
        self.apply_membership_op(MembershipCommand::Down(node))
            .await
    }

    /// Execute a membership operator command through the mode's authority (spec
    /// §6.4): a registry mutation (registry-based), or a committed control-group
    /// entry (leader-based) awaited until the local view reflects
    /// [`effect()`](MembershipCommand::effect). Both readings of the command are
    /// derived here rather than passed in, so the operator methods cannot state
    /// them differently. Modes without an authoritative control plane return
    /// `false`.
    async fn apply_membership_op(&self, command: MembershipCommand) -> bool {
        match &self.inner.mode {
            MembershipMode::Registry(registry) => {
                let client = &registry.client;
                match command {
                    MembershipCommand::Admit(node) => client.register(node).await.is_ok(),
                    MembershipCommand::Drain(node) => client
                        .set_state(node, RegistryState::Draining)
                        .await
                        .is_ok(),
                    MembershipCommand::Resume(node) => {
                        client.set_state(node, RegistryState::Up).await.is_ok()
                    }
                    // Removal is what finalizes `down` in a registry (spec §6.4.2
                    // items 3–4), for a decommission and a leave alike.
                    MembershipCommand::Leave(node) | MembershipCommand::Down(node) => {
                        client.deregister(node).await.is_ok()
                    }
                }
            }
            MembershipMode::Leader(leader) => {
                let (node, status) = command.effect();
                let done = move |m: &Membership| match status {
                    // `is_down` also covers the `removed` tombstone the effect
                    // may have advanced to by the time we look.
                    MemberStatus::Down => m.is_down(node),
                    status => m.status(node) == Some(status),
                };
                self.propose_and_wait(leader, command, done).await
            }
            _ => false,
        }
    }

    /// **Add a voter** to the Raft quorum (leader-based mode, spec §6.4.3
    /// item 2), a committed single-server configuration change. Leader-only:
    /// `false` anywhere else (call it on [`leader`](Self::leader)).
    pub async fn add_voter(&self, node: NodeId) -> bool {
        self.voter_change(node, VoterChange::Add).await
    }

    /// **Remove a voter** from the Raft quorum (spec §6.4.3 item 2), e.g. a
    /// voter that was declared `down` and must leave the quorum. Leader-only.
    pub async fn remove_voter(&self, node: NodeId) -> bool {
        self.voter_change(node, VoterChange::Remove).await
    }

    /// Propose a single-server voter-set change to the **control group** and wait
    /// for it to take effect. Voter changes are engine-level entries (not app
    /// commands), so this proposes the `EntryPayload` directly.
    async fn voter_change(&self, node: NodeId, change: VoterChange) -> bool {
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
        control.propose(change.payload(node));
        let deadline = self.inner.clock.now() + 10 * leader.raft.election_timeout;
        while self.inner.clock.now() < deadline {
            if control.has_voter(node) == change.leaves_node_voting() {
                return true;
            }
            self.inner.clock.sleep(leader.raft.heartbeat_interval).await;
        }
        false
    }

    /// Offer `command` to the Raft leader and wait (bounded by ten election
    /// timeouts) for `done` to observe the committed effect in the local view.
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

    /// One proposal submission to the **control group** (spec §6.4.3 item 1):
    /// append locally when leading; otherwise send `RaftPropose` to the known
    /// leader, or (knowing none yet) to every configured voter, which forward
    /// it to theirs. The membership command is encoded to the control group's
    /// opaque app payload.
    async fn submit_proposal(&self, leader: &LeaderMode, command: MembershipCommand) {
        self.propose_or_forward(GroupId::CONTROL, command.encode(), &leader.raft.voters)
            .await;
    }

    /// Offer an encoded app command to `group`'s leader (spec §6.4.3 item 1):
    /// append locally when leading; otherwise forward one `RaftPropose` to the
    /// known leader; otherwise (no leader known yet) fan the proposal out to
    /// `voters`, which forward it to theirs. The inbound counterpart is
    /// [`handle_raft_propose`](Self::handle_raft_propose), which never fans out.
    /// `voters` is the caller's choice of quorum: the configured founding set for
    /// the control group, or a group's live voter set.
    async fn propose_or_forward(&self, group: GroupId, command: Vec<u8>, voters: &[NodeId]) {
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
        let self_node = self.node();
        for &voter in voters.iter().filter(|&&v| v != self_node) {
            let frame = Frame::RaftPropose {
                group,
                command: command.clone(),
                forwarded: false,
            };
            let _ = self.inner.transport.send(voter, frame).await;
        }
    }

    /// The Raft group `group` if this node runs the consensus engine and hosts
    /// that group (spec §6.4.3). The receive loop routes each group's frames
    /// through it; an unknown group is dropped.
    pub(crate) fn group(&self, group: GroupId) -> Option<Arc<RaftGroup>> {
        self.inner.raft.as_ref().and_then(|raft| raft.group(group))
    }

    /// The control-plane leader as this node sees it (spec §6.4): the Raft
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
    /// (utilities spec §2.1): a pure function of the local view, so converged
    /// nodes agree on every owner. `None` while the serving set is empty.
    pub fn place(&self, key: &[u8]) -> Option<NodeId> {
        crate::placement::owner(&self.inner.membership.serving_members(), key)
    }

    /// Stop this node (spec §6.3): halt the detector and gossip loops and release
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

    /// The resolved SWIM probe interval (spec §7), the cadence layered loops reuse.
    pub fn probe_interval(&self) -> Duration {
        self.inner.swim.probe_interval
    }

    /// Send one SWIM probe and await its ack (spec §7): register a waiter under
    /// a fresh seq, send the frames `build` derives from it, and wait out the
    /// configured RTT. The shared plumbing of the direct probe, the indirect
    /// fan-out, and a helper's relayed probe: each call site states only who
    /// gets which frame. Returns the acked incarnation (delivered by the receive
    /// loop's `Ack`/`IndirectAck` handler), or `None` when nothing answered in
    /// time; the waiter is cleaned up either way.
    async fn probe(&self, build: impl FnOnce(u64) -> Vec<(NodeId, Frame)>) -> Option<u64> {
        let seq = self.inner.pings.next_id();
        let (tx, rx) = oneshot::channel::<u64>();
        self.inner.pings.register(seq, tx);
        for (to, frame) in build(seq) {
            let _ = self.inner.transport.send(to, frame).await;
        }
        let acked = match self.inner.clock.timeout(self.inner.swim.rtt, rx).await {
            Ok(Ok(incarnation)) => Some(incarnation),
            _ => None,
        };
        self.inner.pings.take(seq);
        acked
    }

    /// Complete every in-flight `ask` to `node` with `err`: the node-down
    /// cascade for in-flight callers (spec §5.1 step 3).
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

    /// Run the node-down cascade for `node` (spec §5.1): complete its in-flight
    /// callers with `Unreachable` (step 3) and synthesize `Terminated { NodeDown }`
    /// for local watchers of its actors (step 4). Reached from both the gossip
    /// merge and the detector, which down a node by independent paths.
    async fn node_down_cascade(&self, node: NodeId) {
        self.fail_pending_to(node, CallError::Unreachable);
        self.inner.host.synthesize_node_down(node).await;
    }

    /// Apply the control group's committed entries as membership transitions (spec
    /// §6.2): decode each as a [`MembershipCommand`], stamp its `effect()` with the
    /// commit index, and run the node-down cascade (spec §5.1) for one that lands a
    /// `Down`/`Leave`.
    async fn apply_membership_commits(&self, committed: Vec<Committed>) {
        // A node that stopped applying stays stopped: this runs every tick, and
        // the flag is what makes the refusal below permanent rather than one
        // batch's worth (`Membership::stop_applying`).
        if self.inner.membership.stopped_applying() {
            return;
        }
        for observation in committed {
            let (index, command) = match observation {
                Committed::Apply { index, command } => (index, command),
                // The control group never compacts, so a snapshot is an entry this
                // build cannot apply: it subsumes transitions it never saw. Stop
                // rather than skip it, by the same rule as an unreadable entry
                // below (spec §6.4.3 item 1).
                Committed::Snapshot { index, .. } => {
                    self.inner.membership.stop_applying(format!(
                        "control group: a snapshot through index {index} was \
                         committed, but this build cannot install one, so this \
                         node stopped applying membership transitions rather than \
                         skip the transitions it subsumes",
                    ));
                    return;
                }
            };
            // **Fail closed on a committed entry** (compatibility **V2**). The
            // control group's log is agreed, so an entry this node skips is one its
            // peers apply: it would go on applying the transitions *after* an
            // admission or a downing it never saw, holding a member set that is not
            // a prefix of the committed log but a gap in it, while remaining a full
            // voting member of the cluster whose membership it has stopped tracking.
            //
            // The blast radius is bounded: every transition this build *knows* also
            // travels in gossip, stamped with its commit index, so a peer that
            // applied the entry can still deliver the outcome here
            // (`Membership::merge`). That repair is what makes stopping cheap. It
            // does not cover the case that gets here: a tag from a later build is a
            // transition this one cannot name, in gossip no more than in the log.
            //
            // So the node stops applying, keeps gossiping (the repair path is
            // strictly better left running), keeps acknowledging the group's appends
            // (quorum there is about the log being *durable*, not about this build
            // having applied it), and hands the control-plane duties to a peer that
            // can still perform them (`raft_driver`).
            //
            // `admit` rather than `decode`, so the reason an operator reads names a
            // *version* where there is one (`actor.membership`, compatibility §4).
            //
            // Two `admit`s meet here, one word from each spec's vocabulary: this
            // one admits a payload's version (compatibility §2.1, bytes at rest),
            // while `ClusterSystem::admit`, whose committed
            // `MembershipCommand::Admit` this decodes, admits a *member* (§6.3).
            let command = match MembershipCommand::admit(&command) {
                Ok(command) => command,
                Err(why) => {
                    self.inner.membership.stop_applying(format!(
                        "control group: committed entry {index} is not a \
                         MembershipCommand this build can read ({why}), so this \
                         node stopped applying membership transitions rather than \
                         skip one its peers will apply",
                    ));
                    return;
                }
            };
            let (node, status) = command.effect();
            let now = self.inner.clock.now();
            if self
                .inner
                .membership
                .apply_stamped(node, status, index, now)
            {
                self.node_down_cascade(node).await;
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
    fn spawn_factory<A: Actor<System = Self>>(
        &self,
        factory: ActorFactory<A>,
        parent: Option<ActorId>,
    ) -> ActorRef<A> {
        self.inner.host.spawn_actor(self.clone(), factory, parent)
    }

    fn resolve_local<A: Actor<System = Self>>(&self, id: &ActorId) -> Option<Mailbox<A>> {
        self.inner.host.resolve_local(id)
    }

    fn is_local(&self, id: &ActorId) -> bool {
        self.inner.host.is_local(id)
    }

    fn in_rotation(&self, node: NodeId) -> bool {
        self.inner.membership.in_rotation(node)
    }

    fn codec(&self) -> Arc<dyn Codec> {
        Arc::clone(&self.inner.codec)
    }

    async fn remote_ask(
        &self,
        recipient: &ActorId,
        manifest: Manifest,
        payload: Vec<u8>,
        within: Duration,
    ) -> Result<Vec<u8>, CallError> {
        // Routing afterward: a known-down node fails fast (spec §5.1 step 6).
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

        // Mandatory deadline (spec §8.2); the cascade resolves the waiter with
        // `Unreachable` if the node is downed first (spec §4, invariant #2).
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

    async fn remote_tell(
        &self,
        recipient: &ActorId,
        manifest: Manifest,
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
        // peer node already declared `down`, is reported immediately. The
        // delivery is launched, not run inline: it applies mailbox backpressure,
        // and a watcher calling `watch` from inside its own handler must not
        // block on its own mailbox.
        if self.is_local(&target) {
            // The host judges the already-dead case and its recorded reason.
            if let Some(immediate) = self.inner.host.watch(target, watcher, deliver) {
                self.inner.spawner.launch(immediate);
            }
            return;
        }
        if self.inner.membership.is_down(target.node()) {
            self.inner.spawner.launch(deliver(Terminated {
                id: target,
                reason: TerminationReason::NodeDown,
            }));
            return;
        }
        // A remote target the host cannot judge: track interest raw, so a
        // delivered `Terminated` (synthesized on node-down, or arriving as a
        // frame from the target's node) reaches the watcher.
        self.inner
            .host
            .add_watch(target.clone(), watcher.clone(), deliver);

        // And register interest with the target's node so a graceful per-actor
        // stop (not just a node-down) notifies us (spec §2.5).
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

    type Clock = C;

    fn clock(&self) -> &C {
        &self.inner.clock
    }

    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.inner.spawner.launch(task);
    }

    fn emit(&self, event: Event) {
        self.inner.events.emit(event);
    }

    fn receptionist_state(&self) -> Arc<ReceptionistState> {
        Arc::clone(&self.inner.receptionist)
    }

    fn replicate_registration(&self, key: &str, origin: NodeId, id: ActorId) {
        // Broadcast-on-change to every peer (spec §2.6); nodes that join later or
        // miss a frame converge through `anti_entropy`.
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
        self.inner
            .raft
            .as_ref()
            .and_then(|raft| raft.group(GroupId::CONTROL))
            .map(|control| control.voters())
            .unwrap_or_default()
    }

    fn configured_voters(&self) -> Vec<NodeId> {
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
            return; // only the leader proposes config changes (spec §6.4.3)
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

    fn remove_group(&self, group: GroupId) {
        if let Some(raft) = self.inner.raft.as_ref() {
            raft.remove_group(group);
        }
    }

    fn subscribe_commits(&self, group: GroupId) -> Receiver<Committed> {
        let (tx, rx) = async_channel::unbounded();
        // A subscriber over a reloaded, compacted log is seeded with the snapshot
        // it reloaded (storage §8, `RaftGroup::snapshot_observation`). Sent before
        // the sender is registered, so it strictly precedes any commit a later
        // tick fans out to this sink.
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

    async fn propose_to(&self, group: GroupId, command: Vec<u8>) {
        let voters = self
            .group(group)
            .map(|raft| raft.voters())
            .unwrap_or_default();
        self.propose_or_forward(group, command, &voters).await;
    }

    async fn transfer_group_leadership(&self, group: GroupId, target: NodeId) -> bool {
        let Some(raft) = self.group(group) else {
            return false;
        };
        let (started, out) = raft.transfer_leadership(target);
        self.apply_raft_output(group, out).await;
        started
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

    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        let clock = self.inner.clock.clone();
        Box::pin(async move { clock.sleep(dur).await })
    }

    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.inner.spawner.launch(task);
    }
}

impl<C: Clock, E: Entropy, S: Spawner, T: Transport> ClusterSystem<C, E, S, T> {
    /// The inbound receive loop (spec §2.10): dispatch envelopes to local actors,
    /// resolve pending `ask`s on replies, and answer SWIM probes.
    async fn receive_loop(self, inbound: Receiver<(NodeId, Frame)>) {
        while let Ok((from, frame)) = inbound.recv().await {
            // A stopped node processes nothing further (spec §6.3): frames already
            // queued when `shutdown` was called are dropped, not handled. This is
            // what lets a restart hand the node's durable state to a successor:
            // the old incarnation can no longer write to it.
            if self.is_shutting_down() {
                return;
            }
            self.dispatch_frame(from, frame).await;
        }
    }

    /// Route one inbound frame to its handler (spec §2.10).
    async fn dispatch_frame(&self, from: NodeId, frame: Frame) {
        match frame {
            Frame::Envelope {
                recipient,
                manifest,
                correlation,
                payload,
            } => {
                self.handle_envelope(from, recipient, manifest, correlation, payload)
                    .await
            }
            Frame::Reply {
                correlation,
                outcome,
            } => self.handle_reply(correlation, outcome),
            Frame::Ping {
                seq,
                incarnation,
                digest,
            } => self.handle_ping(from, seq, incarnation, digest).await,
            // An `Ack` answers for its sender; an `IndirectAck` for the target a
            // helper reached on our behalf (spec §7 #2).
            Frame::Ack {
                seq,
                incarnation,
                digest,
            } => self.complete_probe(from, seq, incarnation, digest).await,
            Frame::PingReq {
                seq,
                target,
                incarnation,
                digest,
            } => {
                self.handle_ping_req(from, seq, target, incarnation, digest)
                    .await
            }
            Frame::IndirectAck {
                seq,
                target,
                incarnation,
                digest,
            } => self.complete_probe(target, seq, incarnation, digest).await,
            Frame::Watch { target, watcher } => self.handle_watch(target, watcher).await,
            Frame::Unwatch { target, watcher } => self.handle_unwatch(target, watcher),
            Frame::Terminated {
                target,
                watcher,
                reason,
            } => self.handle_terminated(target, watcher, reason).await,
            Frame::Receptionist { key, origin, actor } => {
                self.handle_receptionist(key, origin, actor)
            }
            Frame::ReceptionistSync { entries } => self.handle_receptionist_sync(entries),
            // Raft consensus traffic (leader-based mode, spec §6.4.3) rides the ordinary
            // transport as system messages; a node not in leader mode ignores it. The
            // frame carries the engine's own message, so there is nothing to open.
            Frame::Raft { group, message } => {
                self.drive_group(group, |raft, now, entropy| {
                    raft.handle(from, message, now, entropy)
                })
                .await
            }
            // Each beat is dispatched to its own group exactly as a lone `RaftAppend`
            // would be, so no group's state machine can tell the difference; only the
            // replies are gathered, so one batch in yields one batch out.
            Frame::RaftHeartbeats { beats } => {
                let mut replies = Vec::with_capacity(beats.len());
                for beat in beats {
                    let group = beat.group;
                    if let Some(raft) = self.group(group) {
                        let mut out = raft.handle(
                            from,
                            beat.into_append(),
                            self.inner.clock.now(),
                            &self.inner.entropy,
                        );
                        // Lift this group's reply out of the output and into the batch;
                        // anything else it produced still goes out on its own.
                        out.messages.retain(|(_, message)| {
                            match RaftBeatReply::from_reply(group, message) {
                                Some(reply) => {
                                    replies.push(reply);
                                    false
                                }
                                None => true,
                            }
                        });
                        self.apply_raft_output(group, out).await;
                    }
                }
                if !replies.is_empty() {
                    let _ = self
                        .inner
                        .transport
                        .send(from, Frame::RaftHeartbeatReplies { replies })
                        .await;
                }
            }
            Frame::RaftHeartbeatReplies { replies } => {
                for reply in replies {
                    self.drive_group(reply.group, |raft, now, entropy| {
                        raft.handle(from, reply.into_reply(), now, entropy)
                    })
                    .await;
                }
            }
            Frame::RaftPropose {
                group,
                command,
                forwarded,
            } => self.handle_raft_propose(group, command, forwarded).await,
        }
    }

    /// Run one group's Raft handler and apply its output (spec §6.4.3): resolve the
    /// group, draw `clock.now()` + `entropy` once, invoke the handler, apply the
    /// output. Drawing `now` inside keeps the entropy/clock ordering identical across
    /// every receive-side Raft arm (the determinism seam, spec §2.12.2).
    async fn drive_group<F>(&self, group: GroupId, handle: F)
    where
        F: FnOnce(&RaftGroup, actor_core::Instant, &E) -> RaftOutput,
    {
        if let Some(raft) = self.group(group) {
            let out = handle(&raft, self.inner.clock.now(), &self.inner.entropy);
            self.apply_raft_output(group, out).await;
        }
    }

    /// Absorb a peer's SWIM gossip (spec §7): hearing from `alive` is direct liveness
    /// evidence, so mark it alive and merge its digest under one `clock.now()`. The
    /// preamble every probe/ack frame shares; the caller then does its distinct tail
    /// (answer, complete a wait, or relay).
    async fn absorb_gossip(
        &self,
        alive: NodeId,
        incarnation: u64,
        digest: Vec<crate::membership::MemberDigest>,
    ) {
        let now = self.inner.clock.now();
        self.inner
            .membership
            .mark_alive_direct(alive, incarnation, now);
        self.gossip_merge(digest, now).await;
    }

    /// Deliver an envelope to a local actor (spec §2.10), gating on authorization and
    /// spawning the reply relay for an `ask`.
    async fn handle_envelope(
        &self,
        from: NodeId,
        recipient: ActorId,
        manifest: String,
        correlation: Option<CallId>,
        payload: Vec<u8>,
    ) {
        // Authorization gate (spec §9). An `ask` gets the rejection as its reply; a
        // one-way `tell` has no reply, so the refusal is recorded on the event stream,
        // its only trace (spec §3.3 rule 4, §2.7), exactly as the host records the
        // refusals it decides itself (`LocalHost::deliver`).
        if let Some(authorizer) = &self.inner.authorizer
            && !authorizer.authorize(from, &recipient, &manifest)
        {
            let refusal = CallError::System("unauthorized".into());
            self.inner.events.emit(actor_core::Event::Undelivered {
                actor: recipient.clone(),
                manifest: manifest.clone(),
                error: refusal.clone(),
            });
            if let Some(call) = correlation {
                let _ = self
                    .inner
                    .transport
                    .send(
                        from,
                        Frame::Reply {
                            correlation: call,
                            outcome: Err(refusal),
                        },
                    )
                    .await;
            }
            return;
        }
        let (reply, reply_rx) = ReplyHandle::channel(self.codec());
        self.inner
            .host
            .deliver(self, &recipient, &manifest, &payload, reply);

        match correlation {
            Some(call) => {
                let transport = self.inner.transport.clone();
                self.inner.spawner.launch(Box::pin(async move {
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

    /// Resolve a pending `ask` on its reply frame.
    fn handle_reply(&self, correlation: CallId, outcome: ReplyResult) {
        if let Some((_, tx)) = self.inner.calls.take(correlation) {
            let _ = tx.send(outcome);
        }
    }

    /// SWIM probe (spec §7): merge the prober's gossip, then answer with our own
    /// incarnation + digest.
    async fn handle_ping(
        &self,
        from: NodeId,
        seq: u64,
        incarnation: u64,
        digest: Vec<crate::membership::MemberDigest>,
    ) {
        self.absorb_gossip(from, incarnation, digest).await;
        let ack = Frame::Ack {
            seq,
            incarnation: self.inner.membership.self_incarnation(),
            digest: self.inner.membership.digest(),
        };
        let _ = self.inner.transport.send(from, ack).await;
    }

    /// `alive` answered our probe `seq` (spec §7), directly or through a helper
    /// (§7 #2): absorb its gossip, which also clears our suspicion of it, and
    /// complete the wait, handing the prober `alive`'s incarnation so a relayed
    /// probe can report it in its `IndirectAck`.
    async fn complete_probe(
        &self,
        alive: NodeId,
        seq: u64,
        incarnation: u64,
        digest: Vec<crate::membership::MemberDigest>,
    ) {
        self.absorb_gossip(alive, incarnation, digest).await;
        if let Some(tx) = self.inner.pings.take(seq) {
            let _ = tx.send(incarnation);
        }
    }

    /// A peer asks us to probe `target` on its behalf (spec §7 #2): merge its
    /// gossip, then relay a probe and forward the result.
    async fn handle_ping_req(
        &self,
        from: NodeId,
        seq: u64,
        target: NodeId,
        incarnation: u64,
        digest: Vec<crate::membership::MemberDigest>,
    ) {
        self.absorb_gossip(from, incarnation, digest).await;
        self.inner
            .spawner
            .launch(Box::pin(self.clone().relay_probe(from, seq, target)));
    }

    /// Cross-node death watch (spec §2.5): a remote `watcher` registers interest in our
    /// local `target`, or is answered immediately when the target is already gone.
    async fn handle_watch(&self, target: ActorId, watcher: ActorId) {
        // Deliver a `Terminated` frame back to the watcher's node when the target
        // stops. A `Weak` handle avoids a reference cycle
        // (system → host → watch closure → system).
        let weak: Weak<Inner<C, E, S, T>> = Arc::downgrade(&self.inner);
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
        // The host judges the already-gone case (invariant #12) and hands back the
        // immediate delivery, carrying the true recorded reason; awaiting it here
        // sends the `Terminated` frame through the same closure the live path uses.
        if let Some(immediate) = self.inner.host.watch(target, watcher, deliver) {
            immediate.await;
        }
    }

    /// Drop a remote watcher's registration (spec §2.5).
    fn handle_unwatch(&self, target: ActorId, watcher: ActorId) {
        self.inner.host.remove_watch(&target, &watcher);
    }

    /// The target's node tells us a watched actor terminated. The frame is addressed
    /// to one specific `watcher` (the target's node sends one per remote watcher), so
    /// deliver it only there, fanning to every local watcher of `target` would hand
    /// the others a spurious extra signal when one of them re-watches the dead actor
    /// (invariant #11).
    async fn handle_terminated(
        &self,
        target: ActorId,
        watcher: ActorId,
        reason: TerminationReason,
    ) {
        self.inner
            .host
            .deliver_terminated_to(&target, &watcher, reason)
            .await;
    }

    /// Apply a single remote receptionist registration, unless the actor is hosted
    /// on a node this node already considers `down`: pruning follows the actor's
    /// host, not the node whose `register` call recorded it (spec §2.6 requirement
    /// 3), so a broadcast that races the down decision never resurrects a pruned
    /// registration (spec §5.1 step 5).
    fn handle_receptionist(&self, key: String, origin: NodeId, actor: ActorId) {
        if !self.inner.membership.is_down(actor.node()) {
            self.receptionist()
                .apply_remote_registration(&key, origin, actor);
        }
    }

    /// Merge a peer's full receptionist registry, skipping entries whose actor is
    /// hosted on a node we already consider `down` so anti-entropy never resurrects a
    /// pruned registration (spec §5.1 step 5, §2.6 requirement 3).
    fn handle_receptionist_sync(&self, entries: Vec<ReceptionistEntry>) {
        let receptionist = self.receptionist();
        for entry in entries {
            if !self.inner.membership.is_down(entry.actor.node()) {
                receptionist.apply_remote_registration(&entry.key, entry.origin, entry.actor);
            }
        }
    }

    /// An application command offered to a group's leader (spec §6.4.3 item 1): append
    /// it when leading, forward it once when not. This is the deliberately partial
    /// form of [`ClusterSystem::propose_or_forward`]: an already-inbound proposal is
    /// never fanned out to voters, and a `forwarded` one landing on a non-leader is
    /// dropped (the proposer's bounded re-submission handles the stale-leader case).
    async fn handle_raft_propose(&self, group: GroupId, command: Vec<u8>, forwarded: bool) {
        if let Some(raft) = self.group(group) {
            if raft.is_leader() {
                raft.propose(EntryPayload::App(command));
            } else if !forwarded && let Some(target) = raft.leader_hint() {
                let frame = Frame::RaftPropose {
                    group,
                    command,
                    forwarded: true,
                };
                let _ = self.inner.transport.send(target, frame).await;
            }
        }
    }

    /// Act on one group's Raft step (spec §6.4.3): emit the election event (tagged
    /// with `group`), apply the group's newly committed application commands, and
    /// send the produced consensus frames.
    async fn apply_raft_output(&self, group: GroupId, out: RaftOutput) {
        if let Some(term) = out.elected {
            self.inner.events.emit(Event::LeaderElected {
                node: self.node(),
                term,
                group: group.value(),
            });
        }
        // Route the group's committed entries to whoever owns them. The control
        // group's are membership transitions the cluster layer applies in-process;
        // every other group's belong to its subscribers.
        //
        // Membership is applied synchronously in the tick rather than through a
        // `subscribe_commits` subscription: it runs `node_down_cascade` inline and
        // must observe each commit in the same tick it lands, and the async
        // `commit_sinks` channel would shift that interleaving.
        if group == GroupId::CONTROL {
            self.apply_membership_commits(out.committed).await;
        } else {
            self.publish_commits(group, &out.committed);
        }
        for (to, message) in out.messages {
            let _ = self
                .inner
                .transport
                .send(to, Frame::Raft { group, message })
                .await;
        }
    }

    /// Publish an application group's committed entries (entity's sharded journal) to
    /// its subscribers, in commit order. The send is a synchronous `try_send` on an
    /// unbounded channel, no `.await` between draining `committed` and delivering, so
    /// commit order is preserved and a slow consumer cannot interleave another group's
    /// batch.
    fn publish_commits(&self, group: GroupId, committed: &[Committed]) {
        if committed.is_empty() {
            return;
        }
        let mut sinks = self
            .inner
            .commit_sinks
            .lock()
            .expect("commit sinks mutex poisoned");
        if let Some(senders) = sinks.get_mut(&group) {
            senders.retain(|sender| !sender.is_closed());
            for observation in committed {
                for sender in senders.iter() {
                    let _ = sender.try_send(observation.clone());
                }
            }
        }
    }

    /// The Raft driver (leader-based mode, spec §6.4.3): each heartbeat interval,
    /// tick the consensus state machine (elections on a follower/candidate whose
    /// timer fired, replication and quorum commit on the leader), and, when this
    /// node leads, perform the leader's control-plane duties: propose `Admit` for
    /// reachable joiners, `Leave` for members announcing departure (committed at
    /// the departing node's request, spec §6.3), and `Down` for members the
    /// configured downing policy condemns (spec §6.4.3 item 4); each a log entry,
    /// so every transition stays quorum-gated (invariant #22).
    async fn raft_driver(self, mode: LeaderMode) {
        let raft = Arc::clone(self.inner.raft.as_ref().expect("leader mode has raft"));
        loop {
            self.inner.clock.sleep(mode.raft.heartbeat_interval).await;
            if self.is_shutting_down() {
                return;
            }
            let now = self.inner.clock.now();
            // Tick every group, then send. The two are separated so the tick's *pure
            // heartbeats* (empty appends, which is what almost every group emits on
            // almost every tick) can be batched per destination into one frame instead
            // of one frame per group (spec §6.4.3; see `Frame::RaftHeartbeats`).
            let mut beats: BTreeMap<NodeId, Vec<RaftBeat>> = BTreeMap::new();
            let mut outputs = raft.tick_all(now, &self.inner.entropy);
            for (group, out) in &mut outputs {
                out.messages.retain(|(to, message)| {
                    match RaftBeat::from_empty_append(*group, message) {
                        Some(beat) => {
                            beats.entry(*to).or_default().push(beat);
                            false // carried in the batch below
                        }
                        // An append with entries is real replication and goes as itself:
                        // batching it would bound one group's throughput by another's.
                        None => true,
                    }
                });
            }
            // Heartbeats go out BEFORE the outputs are applied. Applying is a per-group
            // await, so with many groups it is the slowest part of the tick, and a
            // heartbeat that waited behind all of it would arrive later the more groups
            // this node hosts: precisely the direction that must not scale with `G`,
            // since a late heartbeat is what starts a spurious election.
            for (to, beats) in beats {
                let _ = self
                    .inner
                    .transport
                    .send(to, Frame::RaftHeartbeats { beats })
                    .await;
            }
            for (group, out) in outputs {
                self.apply_raft_output(group, out).await;
            }
            // The control group's leader performs the membership control-plane
            // duties, encoding each transition as the group's opaque app payload.
            if let Some(control) = raft.group(GroupId::CONTROL)
                && control.is_leader()
            {
                if self.inner.membership.stopped_applying() {
                    // A leader that cannot apply the log it commits to is the one node
                    // that must not hold this job: it would propose a `Down`, commit
                    // it, never observe it, and propose it again on the next tick,
                    // forever. So it invites a peer to take over instead. Best-effort
                    // and retried each tick: a target that has not replicated this
                    // leader's last entry refuses, and replication fixes that within a
                    // heartbeat or two.
                    self.hand_off_control(&mode).await;
                } else {
                    let propose = |command: MembershipCommand| {
                        control.propose(EntryPayload::App(command.encode()));
                    };
                    for node in self.inner.membership.admission_candidates() {
                        propose(MembershipCommand::Admit(node));
                    }
                    for node in self.inner.membership.leaving_members() {
                        propose(MembershipCommand::Leave(node));
                    }
                    for node in self.inner.membership.downing_candidates(mode.downing, now) {
                        propose(MembershipCommand::Down(node));
                    }
                }
            }
        }
    }

    /// Hand the control group's leadership to any other configured voter, for a node
    /// that has stopped applying membership transitions (spec §6.4.3 item 1). A
    /// leader that cannot observe its own committed decisions would re-propose them
    /// on every pass, forever.
    ///
    /// The configured voters, not the live ones: the live set is read out of the
    /// member view this node has stopped advancing, which is the state being escaped.
    /// The founding set is a constant every node agrees on, and a target that is no
    /// longer a voter simply refuses, which costs one call and is retried next tick.
    async fn hand_off_control(&self, mode: &LeaderMode) {
        let self_node = self.node();
        for target in mode.raft.voters.iter().copied().filter(|&n| n != self_node) {
            if self
                .transfer_group_leadership(GroupId::CONTROL, target)
                .await
            {
                return;
            }
        }
    }

    /// Merge a gossiped digest into membership and run the node-down cascade for any
    /// node the merge newly declares `down` (spec §5.1, §6.2).
    async fn gossip_merge(
        &self,
        digest: Vec<crate::membership::MemberDigest>,
        now: actor_core::Instant,
    ) {
        for node in self.inner.membership.merge(digest, now) {
            self.node_down_cascade(node).await;
        }
    }

    /// The SWIM failure detector (spec §7). Each `probe_interval` it first
    /// applies the suspicion and downing timeouts (running the cascade for any
    /// node they down, spec §5.1), then probes one member drawn from `Entropy`:
    /// a direct `Ping` awaited for `rtt`; on a miss, a `PingReq` to up to
    /// `indirect_count` helpers, awaited for `rtt` again; and only if neither
    /// path answers, a suspicion. Acks land through the receive loop, which
    /// clears suspicion and merges the digest they carry.
    async fn detector(self) {
        loop {
            self.inner.clock.sleep(self.inner.swim.probe_interval).await;
            if self.is_shutting_down() {
                return;
            }
            let now = self.inner.clock.now();

            // Apply suspicion/downing timeouts and run the cascade for newly-downed:
            // complete their in-flight callers (step 3) and notify watchers (step 4).
            for node in self.inner.membership.tick(now) {
                self.node_down_cascade(node).await;
            }

            let Some(target) = self.inner.membership.pick_probe_target(&self.inner.entropy) else {
                continue;
            };

            // A received `Ack` is handled by the receive loop (mark alive + merge).
            let direct_ok = self.direct_probe(target).await.is_some();

            // On a missed direct probe, enlist `k` helpers to probe indirectly before
            // suspecting: a single bad link should not cause a false suspicion
            // (spec §7 #2). Suspect only if both the direct and indirect probes fail.
            if !direct_ok && !self.indirect_probe(target).await {
                self.inner
                    .membership
                    .mark_suspect(target, self.inner.clock.now());
            }
        }
    }

    /// Probe `target` with one `Ping` carrying this node's incarnation and digest
    /// (spec §7): the detector's own probe and a helper's relayed one alike.
    /// Returns the incarnation `target` acked with, or `None` past `rtt`.
    async fn direct_probe(&self, target: NodeId) -> Option<u64> {
        self.probe(|seq| {
            vec![(
                target,
                Frame::Ping {
                    seq,
                    incarnation: self.inner.membership.self_incarnation(),
                    digest: self.inner.membership.digest(),
                },
            )]
        })
        .await
    }

    /// Indirect probing (spec §7 #2): ask up to `k` random helpers to probe
    /// `target`, returning `true` if any relays back an `IndirectAck`. The relayed
    /// ack also clears our suspicion via the receive loop. `false` if there are no
    /// helpers or none answer within `rtt`.
    async fn indirect_probe(&self, target: NodeId) -> bool {
        let helpers = self.inner.membership.pick_helpers(
            self.inner.swim.indirect_count,
            target,
            &self.inner.entropy,
        );
        if helpers.is_empty() {
            return false;
        }
        let incarnation = self.inner.membership.self_incarnation();
        let digest = self.inner.membership.digest();
        self.probe(|seq| {
            helpers
                .into_iter()
                .map(|helper| {
                    (
                        helper,
                        Frame::PingReq {
                            seq,
                            target,
                            incarnation,
                            digest: digest.clone(),
                        },
                    )
                })
                .collect()
        })
        .await
        .is_some()
    }

    /// Helper side of indirect probing (spec §7 #2): probe `target` ourselves and,
    /// if it answers within `rtt`, relay an `IndirectAck` (carrying the target's
    /// incarnation) back to `requester`.
    async fn relay_probe(self, requester: NodeId, requester_seq: u64, target: NodeId) {
        if let Some(target_incarnation) = self.direct_probe(target).await {
            let ack = Frame::IndirectAck {
                seq: requester_seq,
                target,
                incarnation: target_incarnation,
                digest: self.inner.membership.digest(),
            };
            let _ = self.inner.transport.send(requester, ack).await;
        }
    }

    /// The registry sync loop (spec §6.4.2): every `interval`, fetch the external
    /// registry's state and apply it to the local membership view, each entry
    /// stamped with its revision so the merge converges on the registry's latest
    /// state regardless of sync order (spec §6.2).
    ///
    /// - A snapshot whose global revision is **lower** than the last applied one is
    ///   a stale read and is skipped: application is monotonic, so a laggy or
    ///   replayed read can never revert a drain or resurrect a pruned tombstone.
    /// - A member **absent** from the snapshot that a previous sync knew is
    ///   deregistered: it moves `down` at the snapshot's revision and the node-down
    ///   cascade runs (spec §5.1, §6.4.2 items 3–4).
    /// - Registry **unavailability pauses membership changes only** (spec §6.4.2
    ///   item 6): a failed fetch changes nothing, and the data plane keeps running
    ///   on the last-synced view.
    async fn registry_sync(self, client: Arc<dyn RegistryClient>, interval: Duration) {
        let mut last_revision: Option<u64> = None;
        // The members the registry was last seen to contain, so a later absence is
        // recognized as a deregistration.
        let mut registered: std::collections::BTreeSet<NodeId> = std::collections::BTreeSet::new();
        loop {
            if let Ok(snapshot) = client.fetch().await {
                // Skip stale reads (spec §6.4.2 item 1).
                if last_revision.is_none_or(|last| snapshot.revision >= last) {
                    let advanced = last_revision != Some(snapshot.revision);
                    last_revision = Some(snapshot.revision);
                    let now = self.inner.clock.now();
                    let mut downed = Vec::new();
                    let mut present = std::collections::BTreeSet::new();
                    for entry in &snapshot.entries {
                        present.insert(entry.node);
                        let status = match entry.state {
                            RegistryState::Up => MemberStatus::Up,
                            RegistryState::Draining => MemberStatus::Draining,
                        };
                        if self.inner.membership.apply_stamped(
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
                            && self.inner.membership.apply_stamped(
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
                        self.node_down_cascade(node).await;
                    }
                    if advanced {
                        self.inner.events.emit(Event::RegistrySynced {
                            observer: self.node(),
                            revision: snapshot.revision,
                        });
                    }
                }
            }
            self.inner.clock.sleep(interval).await;
            if self.is_shutting_down() {
                return;
            }
        }
    }

    /// Anti-entropy (spec §2.5, §2.6): every `interval`, repair what a lost frame
    /// left behind. Two repairs share the tick:
    ///
    /// - **Receptionist:** push this node's full registry to a random member,
    ///   converging nodes that joined late or missed a broadcast. Actors hosted on
    ///   a down node are skipped so a pruned registration is never resurrected
    ///   (spec §5.1 step 5, §2.6 requirement 3).
    /// - **Death watch:** re-send `Watch` for every remote watch this node holds on
    ///   one peer, the peers taken in turn in `NodeId` order, so each is revisited
    ///   within one tick per peer. A target's node answers a repeated `Watch`
    ///   idempotently, and with `Terminated` once the target is gone, so a lost
    ///   `Watch` registers late and a lost `Terminated` is answered again; the
    ///   watcher's node drops a duplicate, its subscription already consumed. The
    ///   receptionist prunes a terminated actor's registration through the same
    ///   watch, so a lost `Terminated` cannot leave a stale listing either.
    async fn anti_entropy(self, interval: Duration) {
        let mut turn = 0usize;
        loop {
            self.inner.clock.sleep(interval).await;
            if self.is_shutting_down() {
                return;
            }
            self.repair_watches(turn).await;
            turn = turn.wrapping_add(1);

            let entries: Vec<ReceptionistEntry> = self
                .inner
                .receptionist
                .digest()
                .into_iter()
                .filter(|(_, _, actor)| !self.inner.membership.is_down(actor.node()))
                .map(|(key, origin, actor)| ReceptionistEntry { key, origin, actor })
                .collect();
            if entries.is_empty() {
                continue;
            }

            let Some(target) = self.inner.membership.pick_probe_target(&self.inner.entropy) else {
                continue;
            };
            let _ = self
                .inner
                .transport
                .send(target, Frame::ReceptionistSync { entries })
                .await;
        }
    }

    /// Re-assert this node's remote watches on the `turn`-th live peer (spec
    /// §2.5): one `Watch` frame per subscription. A peer that is `down` already
    /// had its watches answered by the node-down cascade (§5.1 step 4).
    async fn repair_watches(&self, turn: usize) {
        let me = self.node();
        let peers: Vec<NodeId> = self
            .inner
            .membership
            .members()
            .into_iter()
            .filter(|n| *n != me)
            .collect();
        let Some(&peer) = peers.get(turn % peers.len().max(1)) else {
            return;
        };
        if self.inner.membership.is_down(peer) {
            return;
        }
        for (target, watcher) in self.inner.host.watches_on(peer) {
            let _ = self
                .inner
                .transport
                .send(peer, Frame::Watch { target, watcher })
                .await;
        }
    }
}
