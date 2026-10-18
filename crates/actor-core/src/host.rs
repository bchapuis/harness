//! The node-local actor runtime: the implementor-facing seam (spec §3.1, §3.3, §2.5).
//!
//! **This module is not application API.** Programs use [`Actor`], [`ActorRef`],
//! [`Ctx`], and the [`ActorSystem`] trait; only code implementing a *new*
//! `ActorSystem` reaches for [`LocalHost`], [`WatchDelivery`], [`ActorFactory`],
//! [`ReceptionistState`], or [`DispatchFn`].
//!
//! Death-watch is split across the boundary. The host owns *local* delivery:
//! when a watched actor on this node stops, its local watchers receive a
//! [`Terminated`] through their mailbox, and when a peer node is declared `down`,
//! [`synthesize_node_down`](LocalHost::synthesize_node_down) manufactures a
//! `Terminated { NodeDown }` for each local watcher of an actor on that node
//! (spec §5.1 step 4), since no stop message can arrive from a dead node.
//! Delivery to a *remote* watcher is the implementor's job: it supplies a
//! [`WatchDelivery`] closure that forwards the signal over its transport.

use std::any::Any;
use std::any::TypeId;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use actor_serialization::Codec;
use futures::FutureExt;

pub use crate::receptionist::ReceptionistState;
pub use crate::registry::DispatchFn;

use crate::actor::Actor;
use crate::actor::StopReason;
use crate::actor::Terminated;
use crate::actor::TerminationReason;
use crate::context::Ctx;
use crate::error::CallError;
use crate::event::Event;
use crate::event::EventSink;
use crate::event::SupervisionDecision;
use crate::id::ActorId;
use crate::id::NodeId;
use crate::id::Path;
use crate::mailbox::Envelope;
use crate::mailbox::Inbox;
use crate::mailbox::Mailbox;
use crate::rebind::with_decoding_system;
use crate::refs::ActorRef;
use crate::registry::HandlerRegistry;
use crate::reply::ReplyHandle;
use crate::runtime::BoxFuture;
use crate::runtime::Clock;
use crate::runtime::Instant;
use crate::supervision::Fault;
use crate::supervision::Supervision;
use crate::supervision::SupervisionDirective;
use crate::system::ActorSystem;

/// A source of fresh actor instances for (re)starts. Yields `Some` while the
/// actor can be (re-)created and `None` once exhausted: a value-spawned actor
/// yields once, so a restart directive degrades to `Stop`; a factory-spawned one
/// yields indefinitely (spec §2.4).
pub type ActorFactory<A> = Box<dyn FnMut() -> Option<A> + Send>;

/// The factory behind a by-value spawn: yields `actor` once, so a `Restart`
/// directive finds it exhausted and degrades to `Stop` (spec §2.4).
pub(crate) fn once<A: Actor>(actor: A) -> ActorFactory<A> {
    let mut slot = Some(actor);
    Box::new(move || slot.take())
}

/// The factory behind a restartable spawn: re-creates the actor from `factory`
/// on every call (spec §2.4).
pub(crate) fn always<A, F>(mut factory: F) -> ActorFactory<A>
where
    A: Actor,
    F: FnMut() -> A + Send + 'static,
{
    Box::new(move || Some(factory()))
}

/// Delivers a [`Terminated`] to one watcher by enqueuing it on the watcher's
/// mailbox. Built in [`Ctx::watch`](crate::Ctx::watch), where the watcher type
/// is still known.
///
/// Returns a future because local delivery applies mailbox backpressure rather
/// than dropping the signal (spec §3.3, §2.5 invariant #11): callers `.await` it so
/// a busy watcher is never skipped.
pub type WatchDelivery = Arc<dyn Fn(Terminated) -> BoxFuture<'static, ()> + Send + Sync>;

/// One live local actor: its mailbox plus the dispatch registry that turns
/// inbound `(manifest, payload)` pairs into typed handler calls.
struct ActorEntry<A: Actor> {
    mailbox: Mailbox<A>,
    /// Shared with the per-type cache: every actor of type `A` dispatches
    /// through the one registry `Actor::register` built.
    registry: Arc<HandlerRegistry<A>>,
}

/// Type-erased view of a live actor, so one map holds every actor type while
/// sends stay typed: the local fast path downcasts back to `Mailbox<A>`.
trait ErasedActor: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    /// Dispatch an inbound remote message; an unregistered manifest fails the
    /// reply with [`CallError::Unhandled`] (spec §2.10, the allowlist). The
    /// returned error mirrors what `reply` was failed with, for the event stream.
    fn deliver(
        &self,
        codec: &dyn Codec,
        manifest: &str,
        payload: &[u8],
        reply: ReplyHandle,
    ) -> Result<(), CallError>;
}

impl<A: Actor> ErasedActor for ActorEntry<A> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn deliver(
        &self,
        codec: &dyn Codec,
        manifest: &str,
        payload: &[u8],
        reply: ReplyHandle,
    ) -> Result<(), CallError> {
        match self.registry.dispatch(manifest) {
            Some(entry) => entry(codec, payload, reply, &self.mailbox),
            None => {
                reply.fail(CallError::Unhandled);
                Err(CallError::Unhandled)
            }
        }
    }
}

/// A bounded record of how recently-resigned local actors terminated, so a
/// watch placed *after* an actor is gone reports the true reason (`Failed`, not
/// a blanket `Stopped`) to the watcher (spec §2.5, invariant #12).
///
/// Only the last [`CAP`](Terminations::CAP) terminations are kept; a death
/// evicted from the FIFO falls back to the `Stopped` default. Keyed lookups and
/// insertion-ordered eviction only, never iterated, so it adds no observable
/// nondeterminism (§2.12.1).
#[derive(Default)]
struct Terminations {
    reason: BTreeMap<ActorId, TerminationReason>,
    order: VecDeque<ActorId>,
}

impl Terminations {
    const CAP: usize = 1024;

    fn record(&mut self, id: &ActorId, reason: TerminationReason) {
        if self.reason.insert(id.clone(), reason).is_none() {
            self.order.push_back(id.clone());
            if self.order.len() > Self::CAP
                && let Some(evicted) = self.order.pop_front()
            {
                self.reason.remove(&evicted);
            }
        }
    }

    fn reason(&self, id: &ActorId) -> Option<TerminationReason> {
        self.reason.get(id).copied()
    }
}

/// Shared mutable state of one node's host.
struct HostState {
    node: NodeId,
    events: Arc<dyn EventSink>,
    mailbox_capacity: usize,
    actors: Mutex<BTreeMap<ActorId, Arc<dyn ErasedActor>>>,
    /// Death-watch subscriptions: watched target → its local watchers.
    watchers: Mutex<BTreeMap<ActorId, Vec<(ActorId, WatchDelivery)>>>,
    terminations: Mutex<Terminations>,
    /// Escalation inboxes: actor id → a sender its children escalate failures to
    /// (spec §2.4). The value is the failed child's id, for context.
    escalators: Mutex<BTreeMap<ActorId, async_channel::Sender<ActorId>>>,
    /// Per-actor-type dispatch registries, built once on first spawn of each
    /// type so `Actor::register` runs at most once per type (spec §2.10), not per
    /// spawn. Keyed lookups only (never iterated), so it adds no observable
    /// nondeterminism.
    registry_cache: Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
    next_path: AtomicU64,
    /// This process incarnation's stamp on every actor id it assigns; see
    /// [`LocalHost::with_incarnation`].
    incarnation: u64,
}

/// Owns the actors on one node (spec §3.1). Cheap to clone: a shared handle to
/// the node's state.
#[derive(Clone)]
pub struct LocalHost {
    state: Arc<HostState>,
}

impl LocalHost {
    /// Create a host for `node`, with `mailbox_capacity` slots per actor.
    pub fn new(node: NodeId, events: Arc<dyn EventSink>, mailbox_capacity: usize) -> LocalHost {
        LocalHost::with_incarnation(node, events, mailbox_capacity, 0)
    }

    /// A host whose actor ids carry `incarnation`: the **process** incarnation,
    /// not the per-actor one (spec §2.2).
    ///
    /// Paths are handed out by a counter that starts at zero, so a process that
    /// restarts under the same [`NodeId`] re-issues the very ids its predecessor
    /// used. Without the stamp, a peer holding a ref from before the restart
    /// resolves it against whatever actor now occupies that path: a *different*
    /// actor, since spawn order does not survive a restart. Stamping the process
    /// incarnation makes such a ref fail to resolve instead, which is a
    /// `DeadLetter`, the one outcome that proves the command never ran (§5), so
    /// the caller re-resolves and re-issues safely.
    ///
    /// A single-node system passes `0`: every ref to its actors lives in its own
    /// process and dies with it, so no stale ref can outlive a restart.
    pub fn with_incarnation(
        node: NodeId,
        events: Arc<dyn EventSink>,
        mailbox_capacity: usize,
        incarnation: u64,
    ) -> LocalHost {
        LocalHost {
            state: Arc::new(HostState {
                node,
                events,
                mailbox_capacity,
                actors: Mutex::new(BTreeMap::new()),
                watchers: Mutex::new(BTreeMap::new()),
                terminations: Mutex::new(Terminations::default()),
                escalators: Mutex::new(BTreeMap::new()),
                registry_cache: Mutex::new(HashMap::new()),
                next_path: AtomicU64::new(0),
                incarnation,
            }),
        }
    }

    /// This host's node identity.
    pub fn node(&self) -> NodeId {
        self.state.node
    }

    /// Whether `id` is owned by this node (spec §3.2).
    pub fn is_local(&self, id: &ActorId) -> bool {
        id.node() == self.state.node
    }

    /// Whether a live local actor with `id` currently exists.
    fn contains(&self, id: &ActorId) -> bool {
        self.state
            .actors
            .lock()
            .expect("actors mutex poisoned")
            .contains_key(id)
    }

    /// The reason a recently-resigned local actor terminated with, if still
    /// remembered (spec §2.5, invariant #12). `None` once evicted, or never owned
    /// here, in which case [`watch`](LocalHost::watch) falls back to `Stopped`.
    fn termination_reason(&self, id: &ActorId) -> Option<TerminationReason> {
        self.state
            .terminations
            .lock()
            .expect("terminations mutex poisoned")
            .reason(id)
    }

    fn assign_id(&self) -> ActorId {
        let n = self.state.next_path.fetch_add(1, Ordering::Relaxed);
        let id = ActorId::new(
            self.state.node,
            Path::new(format!("/user/{n}")),
            self.state.incarnation,
        );
        self.state.events.emit(Event::AssignId { id: id.clone() });
        id
    }

    /// The dispatch registry for actor type `A`, built on first use and cached
    /// by type, so `Actor::register` runs at most once per type (spec §2.10).
    fn registry_for<A: Actor>(&self) -> Arc<HandlerRegistry<A>> {
        let key = TypeId::of::<A>();
        let mut cache = self
            .state
            .registry_cache
            .lock()
            .expect("registry cache mutex poisoned");
        if let Some(cached) = cache.get(&key) {
            return Arc::clone(cached)
                .downcast::<HandlerRegistry<A>>()
                .expect("registry cache type mismatch");
        }
        let mut registry = HandlerRegistry::<A>::default();
        A::register(&mut registry);
        let registry = Arc::new(registry);
        let erased: Arc<dyn Any + Send + Sync> = registry.clone();
        cache.insert(key, erased);
        registry
    }

    /// Spawn an actor from a `factory`, returning a handle (spec §2.8, §3.1). The
    /// factory supplies the initial instance and any restart re-creations
    /// (spec §2.4). The executor runs on `system`'s spawner and takes
    /// supervision backoff's time and jitter from its clock and entropy (§2.12.1).
    /// `parent` is the spawning actor (the supervisor), or `None` for a root
    /// (spec §2.4).
    pub fn spawn_actor<A: Actor>(
        &self,
        system: A::System,
        factory: ActorFactory<A>,
        parent: Option<ActorId>,
    ) -> ActorRef<A> {
        let id = self.assign_id();
        let (mailbox, inbox) = Mailbox::<A>::channel(
            id.clone(),
            self.state.mailbox_capacity,
            self.state.events.clone(),
        );

        let entry = ActorEntry {
            mailbox: mailbox.clone(),
            registry: self.registry_for::<A>(),
        };
        self.state
            .actors
            .lock()
            .expect("actors mutex poisoned")
            .insert(id.clone(), Arc::new(entry));

        // Children escalate their failures here (spec §2.4).
        let (escalation_tx, escalation_rx) = async_channel::unbounded::<ActorId>();
        self.state
            .escalators
            .lock()
            .expect("escalators mutex poisoned")
            .insert(id.clone(), escalation_tx);

        let handle = ActorRef::from_parts(id.clone(), system.clone());
        let executor = Executor {
            host: self.clone(),
            ctx: Ctx::<A>::new(id, system.clone()),
            factory,
            supervision: A::supervision(),
            restarts: Vec::new(),
            parent,
        };
        system.launch(Box::pin(executor.run(inbox, escalation_rx)));

        handle
    }

    /// Resolve `id` to its local mailbox, or `None` if no live local actor of
    /// type `A` owns it (spec §3.2).
    pub fn resolve_local<A: Actor>(&self, id: &ActorId) -> Option<Mailbox<A>> {
        let actors = self.state.actors.lock().expect("actors mutex poisoned");
        actors
            .get(id)?
            .as_any()
            .downcast_ref::<ActorEntry<A>>()
            .map(|entry| entry.mailbox.clone())
    }

    /// Dispatch an inbound remote message to a local actor (spec §2.10), decoding
    /// it with `system`'s codec and under `system`, so an `ActorRef` the message
    /// carries rebinds to a usable handle on this node (invariant #10). A
    /// refusal always resolves `reply` with its error and is recorded as
    /// [`Event::Undelivered`]: an `ask` learns of it through the reply, but a
    /// one-way `tell` has no reply, and a message MUST NOT vanish without a
    /// trace (spec §3.3 rule 4).
    ///
    /// The checks run in a fixed order: no live local actor under `recipient`
    /// is `DeadLetter`; a manifest the actor type's registry does not list is
    /// `Unhandled` (the allowlist, before any byte is decoded); a payload that
    /// does not decode is `Serialization`; a full mailbox is `MailboxFull`, since
    /// the enqueue never waits and so never stalls the caller's receive loop.
    /// Otherwise the handler runs on the actor's executor and its outcome goes
    /// back through `reply`. A clustered system gates the envelope through its
    /// authorizer before calling this (spec §9).
    pub fn deliver<S: ActorSystem>(
        &self,
        system: &S,
        recipient: &ActorId,
        manifest: &str,
        payload: &[u8],
        reply: ReplyHandle,
    ) {
        let entry = {
            let actors = self.state.actors.lock().expect("actors mutex poisoned");
            actors.get(recipient).cloned()
        };
        let outcome = match entry {
            Some(entry) => {
                let codec = system.codec();
                with_decoding_system(system, || entry.deliver(&*codec, manifest, payload, reply))
            }
            None => {
                reply.fail(CallError::DeadLetter);
                Err(CallError::DeadLetter)
            }
        };
        if let Err(error) = outcome {
            self.state.events.emit(Event::Undelivered {
                actor: recipient.clone(),
                manifest: manifest.to_string(),
                error,
            });
        }
    }

    /// Register a death-watch subscription (spec §2.5), or, when `target` has
    /// already terminated, hand back the immediate `Terminated` delivery
    /// (invariant #12) carrying the true recorded reason (`Failed` vs
    /// `Stopped`; the graceful default once evicted). The host judges that case
    /// because it owns the termination record, so every system reports the same
    /// reason. The caller launches the returned future rather than awaiting it
    /// inline: delivery applies mailbox backpressure, and a watcher calling
    /// `watch` from inside its own handler must not block on its own mailbox.
    #[must_use = "launch the immediate delivery, or the watcher never learns the target is gone"]
    pub fn watch(
        &self,
        target: ActorId,
        watcher: ActorId,
        deliver: WatchDelivery,
    ) -> Option<BoxFuture<'static, ()>> {
        if self.contains(&target) {
            self.add_watch(target, watcher, deliver);
            return None;
        }
        let reason = self
            .termination_reason(&target)
            .unwrap_or(TerminationReason::Stopped);
        Some(deliver(Terminated { id: target, reason }))
    }

    /// Register a death-watch subscription **without** the already-dead check
    /// (spec §2.5), for a *remote* target this host cannot judge, tracked
    /// locally so a synthesized node-down or a forwarded `Terminated` frame
    /// reaches the watcher. For a local target use
    /// [`watch`](LocalHost::watch), which owns the watch-after-death case.
    /// Deduplicated by watcher id so a target yields at most one `Terminated`
    /// per watcher (invariant #11).
    pub fn add_watch(&self, target: ActorId, watcher: ActorId, deliver: WatchDelivery) {
        let mut watchers = self.state.watchers.lock().expect("watchers mutex poisoned");
        let entry = watchers.entry(target).or_default();
        if entry.iter().any(|(w, _)| *w == watcher) {
            return;
        }
        entry.push((watcher, deliver));
    }

    /// Every `(target, watcher)` subscription this host holds on an actor of
    /// `node`, in target order: the remote watches a cluster re-asserts so a lost
    /// `Watch` or `Terminated` frame is repaired (spec §2.5).
    pub fn watches_on(&self, node: NodeId) -> Vec<(ActorId, ActorId)> {
        self.state
            .watchers
            .lock()
            .expect("watchers mutex poisoned")
            .iter()
            .filter(|(target, _)| target.node() == node)
            .flat_map(|(target, entry)| entry.iter().map(|(w, _)| (target.clone(), w.clone())))
            .collect()
    }

    /// Cancel a death-watch subscription (spec §2.5).
    pub fn remove_watch(&self, target: &ActorId, watcher: &ActorId) {
        if let Some(entry) = self
            .state
            .watchers
            .lock()
            .expect("watchers mutex poisoned")
            .get_mut(target)
        {
            entry.retain(|(w, _)| w != watcher);
        }
    }

    /// Synthesize `Terminated { NodeDown }` for watchers of actors on `node`
    /// (spec §5.1 step 4): a dead node sends no stop message, so the watcher's
    /// own node must manufacture the signal. Called by the failure detector on
    /// a down decision.
    pub async fn synthesize_node_down(&self, node: NodeId) {
        let targets: Vec<ActorId> = self
            .state
            .watchers
            .lock()
            .expect("watchers mutex poisoned")
            .keys()
            .filter(|id| id.node() == node)
            .cloned()
            .collect();
        for target in targets {
            self.notify_terminated(&target, TerminationReason::NodeDown)
                .await;
        }
    }

    /// Deliver an inbound `Terminated { reason }` frame to the single `watcher`
    /// it is addressed to, removing only that subscription (spec §2.5). An
    /// inbound frame is addressed: fanning it to *every* local watcher would
    /// give the others a spurious extra signal (a second watcher's
    /// watch-after-death re-watch must not re-notify the first; invariant #11).
    pub async fn deliver_terminated_to(
        &self,
        target: &ActorId,
        watcher: &ActorId,
        reason: TerminationReason,
    ) {
        // Take just this watcher's delivery closure out under the lock, dropping
        // the empty entry behind it, then await delivery with no lock held.
        let deliver = {
            let mut watchers = self.state.watchers.lock().expect("watchers mutex poisoned");
            let removed = watchers.get_mut(target).and_then(|entry| {
                entry
                    .iter()
                    .position(|(w, _)| w == watcher)
                    .map(|pos| entry.remove(pos).1)
            });
            if watchers.get(target).is_some_and(|entry| entry.is_empty()) {
                watchers.remove(target);
            }
            removed
        };
        if let Some(deliver) = deliver {
            deliver(Terminated {
                id: target.clone(),
                reason,
            })
            .await;
        }
    }

    /// Deliver `Terminated` to every watcher of `target`, then forget them, so
    /// each watcher is notified exactly once (invariant #11).
    async fn notify_terminated(&self, target: &ActorId, reason: TerminationReason) {
        // Release the lock before awaiting delivery: the guard ends with this
        // statement, so none is held across the `.await` below.
        let entries = self
            .state
            .watchers
            .lock()
            .expect("watchers mutex poisoned")
            .remove(target);
        if let Some(entries) = entries {
            for (_watcher, deliver) in entries {
                deliver(Terminated {
                    id: target.clone(),
                    reason,
                })
                .await;
            }
        }
    }

    /// Route a child's failure to its parent's escalation inbox (spec §2.4). A
    /// no-op if the parent is already gone.
    fn escalate(&self, parent: &ActorId, child: ActorId) {
        if let Some(sender) = self
            .state
            .escalators
            .lock()
            .expect("escalators mutex poisoned")
            .get(parent)
        {
            let _ = sender.try_send(child);
        }
    }

    /// Release an identity and notify watchers (spec §3.1 step 5, §2.5).
    async fn release(&self, id: &ActorId, reason: TerminationReason) {
        // Record the reason before the actor leaves the live set, so a watch that
        // arrives after this point reads it rather than racing an empty slot (§2.5).
        self.state
            .terminations
            .lock()
            .expect("terminations mutex poisoned")
            .record(id, reason);
        self.state
            .actors
            .lock()
            .expect("actors mutex poisoned")
            .remove(id);
        self.state
            .escalators
            .lock()
            .expect("escalators mutex poisoned")
            .remove(id);
        self.state.events.emit(Event::ResignId { id: id.clone() });
        self.notify_terminated(id, reason).await;
    }
}

/// How one run of the message loop ended.
enum Outcome {
    /// `Ctx::stop`, or all senders gone.
    Stopped,
    /// A handler panicked.
    Faulted,
    /// A child escalated its failure to this actor (spec §2.4).
    Escalated,
}

/// How the actor's run finally ended.
enum End<A> {
    /// We still hold a live instance to stop with this reason.
    Stop(A, StopReason),
    /// The instance was already stopped (a restart whose factory was exhausted);
    /// just release the id with this reason.
    Released(TerminationReason),
}

/// The executor's next step in an actor's life (spec §3.3, §2.4): run `started` on
/// the instance (first start or a restart), process messages until the next stop
/// or fault, or terminate.
enum Step<A> {
    Start(A),
    Run(A),
    End(End<A>),
}

/// The serial executor + supervisor for one actor (spec §3.3, §2.4): the
/// loop-invariant state of one actor's whole life (host, context, factory,
/// supervision) bundled once, so each step below reads as a method. The
/// context's system supplies the clock and entropy supervision reads.
struct Executor<A: Actor> {
    host: LocalHost,
    ctx: Ctx<A>,
    factory: ActorFactory<A>,
    supervision: Supervision,
    restarts: Vec<Instant>,
    /// The spawning actor (the supervisor), or `None` for a root (spec §2.4).
    parent: Option<ActorId>,
}

impl<A: Actor> Executor<A> {
    /// Run the actor's life to completion. Processes one message at a time (so
    /// `&mut self` is never aliased), and on a fault applies the actor's
    /// [`Supervision`] strategy: `Resume` keeps the actor, `Restart` re-creates
    /// it from the factory with backoff, and `Stop`/`Escalate` (or exceeding
    /// the restart window) terminate it. On stop the mailbox is drained (so any
    /// queued `ask` is dead-lettered, never left hanging), the id is released,
    /// and watchers are notified (spec §2.5).
    async fn run(mut self, inbox: Inbox<A>, escalation: async_channel::Receiver<ActorId>) {
        let Some(actor) = (self.factory)() else {
            self.host
                .release(self.ctx.id(), TerminationReason::Stopped)
                .await;
            return;
        };
        let mut ever_started = false;

        let mut step = Step::Start(actor);
        let end = loop {
            step = match step {
                // A panic in `started` is a start failure, contained by the same
                // executor boundary as a handler panic (spec §2.4).
                Step::Start(mut actor) => match AssertUnwindSafe(actor.started(&self.ctx))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_panic| Err("started panicked".into()))
                {
                    Ok(()) => {
                        // `ActorReady` fires once (spec §3.1); a restart re-runs
                        // `started` but the id stays resolvable, so it emits
                        // `Restarted` instead.
                        let id = self.ctx.id().clone();
                        if ever_started {
                            self.host.state.events.emit(Event::Restarted { actor: id });
                        } else {
                            self.host.state.events.emit(Event::ActorReady { id });
                            ever_started = true;
                        }
                        // `Ctx::stop` from `started` stops before the first
                        // message (spec §2.3): there is no current message to
                        // finish, so the loop must not admit one.
                        if self.ctx.is_stopping() {
                            Step::End(End::Stop(actor, StopReason::Stopped))
                        } else {
                            Step::Run(actor)
                        }
                    }
                    Err(_err) => self.handle_fault(Fault::Started, actor).await,
                },
                Step::Run(mut actor) => {
                    let looped =
                        Self::message_loop(&self.host, &self.ctx, &mut actor, &inbox, &escalation)
                            .await;
                    let fault = match looped {
                        Outcome::Stopped => break End::Stop(actor, StopReason::Stopped),
                        Outcome::Faulted => Fault::Message,
                        Outcome::Escalated => Fault::Escalation,
                    };
                    self.handle_fault(fault, actor).await
                }
                Step::End(end) => break end,
            };
        };

        // Dead-letter anything still queued so callers waiting on a reply do not
        // hang once the actor is gone (an `ask`'s reply channel cancels when its
        // envelope drops).
        inbox.close();
        while inbox.try_recv().is_ok() {}

        match end {
            // Stop the instance, then release its identity (spec §3.1 step 5).
            End::Stop(actor, reason) => {
                stop_contained(actor, reason).await;
                self.host.release(self.ctx.id(), reason.into()).await;
            }
            // No instance to stop: it was already stopped during a failed restart.
            End::Released(reason) => self.host.release(self.ctx.id(), reason).await,
        }
    }

    /// Apply supervision to one fault and report the executor's next step
    /// (spec §2.4). Shared by the `started`-fault and message-loop-fault paths.
    ///
    /// A restart runs the faulted instance's `stopped` hook before rebuilding,
    /// and defers its event to [`resolve_restart`](Executor::resolve_restart),
    /// which emits the *effective* decision once it knows whether a fresh
    /// instance exists.
    async fn handle_fault(&mut self, fault: Fault, actor: A) -> Step<A> {
        let decision = supervise(
            &self.supervision,
            fault,
            self.ctx.system(),
            &mut self.restarts,
        )
        .await;
        match decision {
            SupervisionDecision::Resume => {
                self.emit_supervision(fault, SupervisionDecision::Resume);
                Step::Run(actor)
            }
            SupervisionDecision::Stop => {
                self.emit_supervision(fault, SupervisionDecision::Stop);
                Step::End(End::Stop(actor, StopReason::Failed))
            }
            SupervisionDecision::Escalate => {
                self.emit_supervision(fault, SupervisionDecision::Escalate);
                if let Some(parent) = &self.parent {
                    self.host.escalate(parent, self.ctx.id().clone());
                }
                Step::End(End::Stop(actor, StopReason::Failed))
            }
            SupervisionDecision::Restart => {
                // The faulted instance stops with `Failed` first (spec §2.4).
                stop_contained(actor, StopReason::Failed).await;
                match self.resolve_restart(fault) {
                    Some(fresh) => Step::Start(fresh),
                    None => Step::End(End::Released(TerminationReason::Failed)),
                }
            }
        }
    }

    /// Carry out a `Restart` decision against the factory, emitting the
    /// *effective* supervision decision (spec §2.4, §2.7).
    ///
    /// An actor spawned **by value** has an exhausted factory and cannot be
    /// reconstructed, so its `Restart` degrades to `Stop` (spec §2.4). The
    /// event stream MUST report that effective `Stop`, not the candidate
    /// `Restart`: an observer that saw `Restart` here would expect a following
    /// `Restarted`, never the `ResignId` a degraded stop produces.
    ///
    /// `None` means it degraded; the caller then terminates the actor with
    /// `Failed`.
    fn resolve_restart(&mut self, fault: Fault) -> Option<A> {
        match (self.factory)() {
            Some(fresh) => {
                self.emit_supervision(fault, SupervisionDecision::Restart);
                Some(fresh)
            }
            None => {
                self.emit_supervision(fault, SupervisionDecision::Stop);
                None
            }
        }
    }

    /// Emit the *effective* supervision decision for a faulted actor (spec
    /// §2.4, §2.7): the restart window and backoff have already been applied by
    /// the time this is called.
    fn emit_supervision(&self, fault: Fault, decision: SupervisionDecision) {
        self.host.state.events.emit(Event::Supervised {
            actor: self.ctx.id().clone(),
            fault,
            decision,
        });
    }

    /// Process messages until the actor stops, panics, or a child escalates.
    /// The escalation channel makes the executor interruptible (spec §2.4): a
    /// parked actor reacts to a child failure promptly, not only on its next
    /// message.
    ///
    /// Over the executor's two `Sync` halves rather than `&self`: the executor
    /// also owns the actor factory, whose `FnMut` box is not `Sync`, and a
    /// `&self` held across this loop's awaits would demand it be.
    async fn message_loop(
        host: &LocalHost,
        ctx: &Ctx<A>,
        actor: &mut A,
        inbox: &Inbox<A>,
        escalation: &async_channel::Receiver<ActorId>,
    ) -> Outcome {
        // Built once and polled across every iteration rather than rebuilt per message.
        // Awaiting a channel registers a listener on it, so a fresh future each time
        // round would register and deregister on the escalation channel for every
        // message handled, to watch for something that arrives at most once.
        let escalated = escalation.recv();
        futures::pin_mut!(escalated);
        loop {
            let next = inbox.recv();
            futures::pin_mut!(next);
            match futures::future::select(next, escalated.as_mut()).await {
                futures::future::Either::Left((Err(_), _)) => return Outcome::Stopped,
                futures::future::Either::Left((Ok(envelope), _)) => {
                    let Envelope {
                        manifest,
                        run,
                        reply,
                    } = envelope;
                    host.state.events.emit(Event::DispatchStart {
                        actor: ctx.id().clone(),
                        manifest,
                    });
                    let running = run(actor, ctx, reply);
                    let panicked = AssertUnwindSafe(running).catch_unwind().await.is_err();
                    host.state.events.emit(Event::DispatchEnd {
                        actor: ctx.id().clone(),
                        manifest,
                    });
                    if panicked {
                        return Outcome::Faulted;
                    }
                    if ctx.is_stopping() {
                        return Outcome::Stopped;
                    }
                }
                futures::future::Either::Right((Ok(_child), _)) => return Outcome::Escalated,
                futures::future::Either::Right((Err(_), _)) => return Outcome::Stopped,
            }
        }
    }
}

/// Run an actor's `stopped` hook with a panic contained (spec §2.4): the actor
/// is already on its way out, so a panic there changes nothing about the stop
/// (the id is still released and watchers still notified), and MUST NOT take the
/// executor, and with it the node, down.
async fn stop_contained<A: Actor>(actor: A, reason: StopReason) {
    let _ = AssertUnwindSafe(actor.stopped(reason)).catch_unwind().await;
}

/// Apply the supervision directive for `fault`, including the restart window
/// (`max` within `within` → escalate to stop) and backoff sleep (spec §2.4),
/// reporting the effective decision (`Stop` means "stop with `Failed`").
/// `system` supplies the clock and, for backoff jitter, a draw from its seeded
/// `Entropy` (§2.12.1). The draw happens only when a jittered sleep actually does,
/// so a non-restart fault consumes no randomness and never perturbs the run.
async fn supervise<S: ActorSystem>(
    supervision: &Supervision,
    fault: Fault,
    system: &S,
    restarts: &mut Vec<Instant>,
) -> SupervisionDecision {
    match supervision.decide(fault) {
        SupervisionDirective::Stop => SupervisionDecision::Stop,
        SupervisionDirective::Escalate => SupervisionDecision::Escalate,
        // `Resume` drops the failed message and carries on; a failed `started`
        // has no message to drop, and carrying on would run an actor that never
        // became ready (spec §2.1, §3.1 step 3), so it degrades to `Stop`.
        SupervisionDirective::Resume => match fault {
            Fault::Started => SupervisionDecision::Stop,
            Fault::Message | Fault::Escalation => SupervisionDecision::Resume,
        },
        SupervisionDirective::Restart {
            max,
            within,
            backoff,
        } => {
            let now = system.now();
            restarts.retain(|t| now.duration_since(*t) < within);
            restarts.push(now);
            if restarts.len() as u32 > max {
                SupervisionDecision::Stop
            } else {
                let base = backoff.delay(restarts.len() as u32);
                let delay = if base.is_zero() {
                    base
                } else {
                    jittered(base, system.next_random())
                };
                system.clock().sleep(delay).await;
                SupervisionDecision::Restart
            }
        }
    }
}

/// Apply equal-jitter to a backoff delay (spec §2.4): hold half the delay fixed
/// and randomize the other half. `rand` comes from the seeded `Entropy` (§2.12.1),
/// keeping a simulated run reproducible.
fn jittered(base: std::time::Duration, rand: u64) -> std::time::Duration {
    let nanos = base.as_nanos() as u64;
    let half = nanos / 2;
    std::time::Duration::from_nanos(half + rand % (half + 1))
}
