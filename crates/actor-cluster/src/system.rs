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
//! probing (spec §10), drives the **leader-based** join/leave lifecycle (spec
//! §9.2, §9.3), prunes via death watch, and runs the receptionist with
//! broadcast-on-change plus periodic anti-entropy (spec §12, §13). Full seen-by
//! gossip-convergence detection remains a follow-up.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
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
use actor_core::LocalHost;
use actor_core::Mailbox;
use actor_core::NodeId;
use actor_core::ReceptionistState;
use actor_core::ReplyHandle;
use actor_core::ReplyResult;
use actor_core::Spawner;
use actor_core::Terminated;
use actor_core::TerminationReason;
use actor_core::WatchDelivery;
use actor_serialization::Codec;
use async_channel::Receiver;
use futures::channel::oneshot;

use crate::membership::Membership;
use crate::membership::SwimConfig;
use crate::transport::CallId;
use crate::transport::Frame;
use crate::transport::ReceptionistEntry;
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
    /// SWIM failure detection; `None` disables it (no detector loop).
    pub swim: Option<SwimConfig>,
    /// Start this node as a joiner (spec §9.3): it enters the cluster `Joining`
    /// and is admitted to `Up` by the leader once membership converges. `false`
    /// (the default) starts it as a founding `Up` member.
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
            swim: None,
            joining: false,
            authorizer: None,
        }
    }
}

struct Inner<C, E, S, T> {
    clock: C,
    entropy: E,
    spawner: S,
    transport: T,
    codec: Arc<dyn Codec>,
    host: LocalHost,
    membership: Membership,
    /// Resolved SWIM parameters, so loops spawned without the config (e.g. a
    /// relayed indirect probe) can still read `rtt`/`k` (spec §10).
    swim: SwimConfig,
    events: Arc<dyn EventSink>,
    /// In-flight `ask`s awaiting a reply: correlation id → (target node, waiter).
    /// The target node lets the cascade complete them on a node-down (spec §8.1).
    pending: Mutex<BTreeMap<CallId, (NodeId, oneshot::Sender<ReplyResult>)>>,
    /// In-flight SWIM probes awaiting an `Ack` (direct, indirect, or a helper's
    /// relayed probe), keyed by seq. The completion carries the target's
    /// incarnation, so a relay can report it back to the requester (spec §10).
    pending_pings: Mutex<BTreeMap<u64, oneshot::Sender<u64>>>,
    receptionist: Arc<ReceptionistState>,
    next_call: AtomicU64,
    next_ping: AtomicU64,
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
        let swim = config.swim.unwrap_or_default();
        let host = LocalHost::new(node, Arc::clone(&config.events), config.mailbox_capacity);
        let membership = Membership::new(node, &swim, Arc::clone(&config.events), config.joining);
        let system = ClusterSystem {
            inner: Arc::new(Inner {
                clock,
                entropy,
                spawner,
                transport,
                codec: config.codec,
                host,
                membership,
                swim,
                events: config.events,
                pending: Mutex::new(BTreeMap::new()),
                pending_pings: Mutex::new(BTreeMap::new()),
                receptionist: Arc::new(ReceptionistState::new()),
                next_call: AtomicU64::new(0),
                next_ping: AtomicU64::new(0),
                shutdown: std::sync::atomic::AtomicBool::new(false),
                authorizer: config.authorizer,
            }),
        };
        system
            .inner
            .spawner
            .launch(Box::pin(receive_loop(system.clone(), inbound)));
        if config.swim.is_some() {
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
    /// leader finalizes this node to `Down`, and watchers of its actors are
    /// notified through the node-down cascade (spec §8.1, §12). The caller drains
    /// and shuts the node down after announcing.
    pub fn leave(&self) {
        self.inner.membership.begin_leaving();
    }

    /// The cluster leader as this node sees it (spec §9.2), or `None` if this
    /// node has left.
    pub fn leader(&self) -> Option<NodeId> {
        self.inner.membership.leader()
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

    fn is_shutting_down(&self) -> bool {
        self.inner
            .shutdown
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Complete every in-flight `ask` to `node` with `err` — the node-down
    /// cascade for in-flight callers (spec §8.1 step 3).
    fn fail_pending_to(&self, node: NodeId, err: CallError) {
        let mut pending = self.inner.pending.lock().expect("pending mutex poisoned");
        let ids: Vec<CallId> = pending
            .iter()
            .filter(|(_, (target, _))| *target == node)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            if let Some((_, waiter)) = pending.remove(&id) {
                let _ = waiter.send(Err(err.clone()));
            }
        }
    }

    fn pending_remove(&self, call: CallId) {
        self.inner
            .pending
            .lock()
            .expect("pending mutex poisoned")
            .remove(&call);
    }

    async fn remote_ask_inner(
        &self,
        recipient: &ActorId,
        manifest: &'static str,
        payload: Vec<u8>,
        within: Duration,
    ) -> Result<Vec<u8>, CallError> {
        // Routing afterward: a known-down node fails fast (spec §8.1 step 6).
        if self.inner.membership.is_down(recipient.node) {
            return Err(CallError::Unreachable);
        }

        let call = CallId(self.inner.next_call.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = oneshot::channel::<ReplyResult>();
        self.inner
            .pending
            .lock()
            .expect("pending mutex poisoned")
            .insert(call, (recipient.node, tx));

        let frame = Frame::Envelope {
            recipient: recipient.clone(),
            manifest: manifest.to_string(),
            correlation: Some(call),
            payload,
        };
        if self
            .inner
            .transport
            .send(recipient.node, frame)
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
        if self.inner.membership.is_down(recipient.node) {
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
            .send(recipient.node, frame)
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
        } else if self.inner.membership.is_down(target.node) {
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
            let node = target.node;
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
            let node = target.node;
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
                if let Some(authorizer) = &system.inner.authorizer {
                    if !authorizer.authorize(from, &recipient, &manifest) {
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
                let waiter = system
                    .inner
                    .pending
                    .lock()
                    .expect("pending mutex poisoned")
                    .remove(&correlation)
                    .map(|(_, tx)| tx);
                if let Some(tx) = waiter {
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
                let waiter = system
                    .inner
                    .pending_pings
                    .lock()
                    .expect("pending pings mutex poisoned")
                    .remove(&seq);
                if let Some(tx) = waiter {
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
                let waiter = system
                    .inner
                    .pending_pings
                    .lock()
                    .expect("pending pings mutex poisoned")
                    .remove(&seq);
                if let Some(tx) = waiter {
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
                    let watcher_node = watcher.node;
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
                    let _ = system.inner.transport.send(watcher.node, frame).await;
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
        system.fail_pending_to(node, CallError::Unreachable);
        system.inner.host.synthesize_node_down(node).await;
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
            system.fail_pending_to(node, CallError::Unreachable);
            system.inner.host.synthesize_node_down(node).await;
        }

        let Some(target) = system
            .inner
            .membership
            .pick_probe_target(&system.inner.entropy)
        else {
            continue;
        };

        let seq = system.inner.next_ping.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel::<u64>();
        system
            .inner
            .pending_pings
            .lock()
            .expect("pending pings mutex poisoned")
            .insert(seq, tx);
        let frame = Frame::Ping {
            seq,
            incarnation: system.inner.membership.self_incarnation(),
            digest: system.inner.membership.digest(),
        };
        let _ = system.inner.transport.send(target, frame).await;

        // A received `Ack` is handled by the receive loop (mark alive + merge).
        let direct_ok = matches!(system.inner.clock.timeout(config.rtt, rx).await, Ok(Ok(_)));
        system
            .inner
            .pending_pings
            .lock()
            .expect("pending pings mutex poisoned")
            .remove(&seq);

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

    let seq = system.inner.next_ping.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = oneshot::channel::<u64>();
    system
        .inner
        .pending_pings
        .lock()
        .expect("pending pings mutex poisoned")
        .insert(seq, tx);

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
    system
        .inner
        .pending_pings
        .lock()
        .expect("pending pings mutex poisoned")
        .remove(&seq);
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
    let seq = system.inner.next_ping.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = oneshot::channel::<u64>();
    system
        .inner
        .pending_pings
        .lock()
        .expect("pending pings mutex poisoned")
        .insert(seq, tx);
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
            system
                .inner
                .pending_pings
                .lock()
                .expect("pending pings mutex poisoned")
                .remove(&seq);
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
