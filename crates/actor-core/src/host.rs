//! The local actor host (spec §4.2, §6, §12).
//!
//! `LocalHost` owns the actors running on one node: it assigns identities,
//! stores each actor's mailbox plus its dispatch table, runs the serial
//! executor, resolves ids to local mailboxes, and tracks death-watch
//! subscriptions. It is the machinery both [`LocalSystem`](crate::LocalSystem)
//! and the cluster runtime build on — the cluster adds the network boundary.
//!
//! Watch delivery is node-local: when a watched actor on this node stops, its
//! local watchers receive a [`Terminated`]; when a peer node is declared `down`,
//! [`synthesize_node_down`](LocalHost::synthesize_node_down) sends each local
//! watcher of an actor on that node a `Terminated { NodeDown }` (spec §8.1 step
//! 4), since no stop message can arrive from a dead node.

use std::any::Any;
use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use actor_serialization::Codec;
use futures::FutureExt;

use crate::actor::Actor;
use crate::actor::StopReason;
use crate::actor::Terminated;
use crate::actor::TerminationReason;
use crate::context::Ctx;
use crate::error::CallError;
use crate::event::Event;
use crate::event::EventSink;
use crate::id::ActorId;
use crate::id::NodeId;
use crate::id::Path;
use crate::mailbox::Inbox;
use crate::mailbox::Mailbox;
use crate::refs::ActorRef;
use crate::registry::DispatchFn;
use crate::registry::HandlerRegistry;
use crate::reply::ReplyHandle;
use crate::runtime::Clock;
use crate::runtime::Instant;
use crate::runtime::Spawner;
use crate::supervision::Fault;
use crate::supervision::Supervision;
use crate::supervision::SupervisionDirective;

/// A source of fresh actor instances for (re)starts. Yields `Some` while the
/// actor can be (re-)created and `None` once exhausted — a value-spawned actor
/// yields once, so a restart directive degrades to `Stop`; a factory-spawned one
/// yields indefinitely (spec §11.2).
pub type ActorFactory<A> = Box<dyn FnMut() -> Option<A> + Send>;

/// Delivers a [`Terminated`] to one watcher by enqueuing it on the watcher's
/// mailbox. Built in [`Ctx::watch`](crate::Ctx::watch), where the watcher type
/// is known; stored type-erased in the watch registry.
pub type WatchDelivery = Arc<dyn Fn(Terminated) + Send + Sync>;

/// One live local actor: its mailbox plus the dispatch table that turns inbound
/// `(manifest, payload)` pairs into typed handler calls.
struct ActorEntry<A: Actor> {
    mailbox: Mailbox<A>,
    dispatch: BTreeMap<&'static str, DispatchFn<A>>,
}

/// Type-erased view of a live actor, stored keyed by [`ActorId`]. Supports the
/// local fast path (downcast to `Mailbox<A>`) and remote delivery (dispatch a
/// payload), keeping storage heterogeneous while sends stay typed.
trait ErasedActor: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    /// Dispatch an inbound remote message; an unregistered manifest fails the
    /// reply with [`CallError::Unhandled`] (spec §4.4, the allowlist).
    fn deliver(&self, codec: &dyn Codec, manifest: &str, payload: &[u8], reply: ReplyHandle);
}

impl<A: Actor> ErasedActor for ActorEntry<A> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn deliver(&self, codec: &dyn Codec, manifest: &str, payload: &[u8], reply: ReplyHandle) {
        match self.dispatch.get(manifest) {
            Some(entry) => {
                let _ = entry(codec, payload, reply, &self.mailbox);
            }
            None => reply.fail(CallError::Unhandled),
        }
    }
}

/// Shared mutable state of one node's host. Held behind an `Arc` so the executor
/// task can reach it on termination.
struct HostState {
    node: NodeId,
    events: Arc<dyn EventSink>,
    mailbox_capacity: usize,
    actors: Mutex<BTreeMap<ActorId, Arc<dyn ErasedActor>>>,
    /// Death-watch subscriptions: watched target → its local watchers.
    watchers: Mutex<BTreeMap<ActorId, Vec<(ActorId, WatchDelivery)>>>,
    /// Escalation inboxes: actor id → a sender its children escalate failures to
    /// (spec §11.1). The value is the failed child's id, for context.
    escalators: Mutex<BTreeMap<ActorId, async_channel::Sender<ActorId>>>,
    next_path: AtomicU64,
}

impl HostState {
    /// Register `watcher`'s interest in `target`. Deduplicated by watcher id so
    /// a target yields at most one `Terminated` per watcher (invariant #11).
    fn add_watch(&self, target: ActorId, watcher: ActorId, deliver: WatchDelivery) {
        let mut watchers = self.watchers.lock().expect("watchers mutex poisoned");
        let entry = watchers.entry(target).or_default();
        if entry.iter().any(|(w, _)| *w == watcher) {
            return;
        }
        entry.push((watcher, deliver));
    }

    fn remove_watch(&self, target: &ActorId, watcher: &ActorId) {
        if let Some(entry) = self
            .watchers
            .lock()
            .expect("watchers mutex poisoned")
            .get_mut(target)
        {
            entry.retain(|(w, _)| w != watcher);
        }
    }

    /// Deliver `Terminated` to every watcher of `target`, then forget them — so
    /// each watcher is notified exactly once (invariant #11).
    fn notify_terminated(&self, target: &ActorId, reason: TerminationReason) {
        let entries = self
            .watchers
            .lock()
            .expect("watchers mutex poisoned")
            .remove(target);
        if let Some(entries) = entries {
            for (_watcher, deliver) in entries {
                deliver(Terminated {
                    id: target.clone(),
                    reason,
                });
            }
        }
    }

    /// Route a child's failure to its parent's escalation inbox (spec §11.1). A
    /// no-op if the parent is already gone.
    fn escalate(&self, parent: &ActorId, child: ActorId) {
        if let Some(sender) = self
            .escalators
            .lock()
            .expect("escalators mutex poisoned")
            .get(parent)
        {
            let _ = sender.try_send(child);
        }
    }

    /// Synthesize `Terminated { NodeDown }` for every watched actor on `node`
    /// (spec §8.1 step 4): a dead node sends no stop message, so the watcher's
    /// own node must manufacture the signal.
    fn synthesize_node_down(&self, node: NodeId) {
        let targets: Vec<ActorId> = self
            .watchers
            .lock()
            .expect("watchers mutex poisoned")
            .keys()
            .filter(|id| id.node == node)
            .cloned()
            .collect();
        for target in targets {
            self.notify_terminated(&target, TerminationReason::NodeDown);
        }
    }
}

/// Owns the actors on one node (spec §4.2). Cheap to clone — a shared handle to
/// the node's state.
#[derive(Clone)]
pub struct LocalHost {
    state: Arc<HostState>,
}

impl LocalHost {
    /// Create a host for `node`, emitting events to `events`, with the given
    /// per-actor mailbox capacity.
    pub fn new(node: NodeId, events: Arc<dyn EventSink>, mailbox_capacity: usize) -> LocalHost {
        LocalHost {
            state: Arc::new(HostState {
                node,
                events,
                mailbox_capacity,
                actors: Mutex::new(BTreeMap::new()),
                watchers: Mutex::new(BTreeMap::new()),
                escalators: Mutex::new(BTreeMap::new()),
                next_path: AtomicU64::new(0),
            }),
        }
    }

    /// This host's node identity.
    pub fn node(&self) -> NodeId {
        self.state.node
    }

    /// Whether `id` is owned by this node (spec §4.3).
    pub fn is_local(&self, id: &ActorId) -> bool {
        id.node == self.state.node
    }

    /// Whether a live local actor with `id` currently exists.
    pub fn contains(&self, id: &ActorId) -> bool {
        self.state
            .actors
            .lock()
            .expect("actors mutex poisoned")
            .contains_key(id)
    }

    fn assign_id(&self) -> ActorId {
        let n = self.state.next_path.fetch_add(1, Ordering::Relaxed);
        let id = ActorId::new(self.state.node, Path::new(format!("/user/{n}")), 0);
        self.state.events.emit(Event::AssignId { id: id.clone() });
        id
    }

    /// Spawn an actor from a `factory`, returning a handle (spec §4.1, §4.2). The
    /// factory supplies the initial instance and any restart re-creations
    /// (spec §11.2); `clock` drives supervision backoff. `parent` is the spawning
    /// actor (the supervisor), or `None` for a root (spec §11.1).
    pub fn spawn_actor<A, C, Sp>(
        &self,
        system: A::System,
        clock: C,
        spawner: &Sp,
        factory: ActorFactory<A>,
        parent: Option<ActorId>,
    ) -> ActorRef<A>
    where
        A: Actor,
        C: Clock,
        Sp: Spawner,
    {
        let id = self.assign_id();
        let (mailbox, inbox) = Mailbox::<A>::channel(
            id.clone(),
            self.state.mailbox_capacity,
            self.state.events.clone(),
        );

        let mut registry = HandlerRegistry::<A>::default();
        A::register(&mut registry);
        let entry = ActorEntry {
            mailbox: mailbox.clone(),
            dispatch: registry.into_entries(),
        };
        self.state
            .actors
            .lock()
            .expect("actors mutex poisoned")
            .insert(id.clone(), Arc::new(entry));

        // Escalation inbox: children escalate failures here (spec §11.1).
        let (escalation_tx, escalation_rx) = async_channel::unbounded::<ActorId>();
        self.state
            .escalators
            .lock()
            .expect("escalators mutex poisoned")
            .insert(id.clone(), escalation_tx);

        let handle = ActorRef::from_parts(id.clone(), system.clone());
        let ctx = Ctx::<A>::new(id.clone(), system);

        spawner.launch(Box::pin(run_actor(
            Arc::clone(&self.state),
            factory,
            ctx,
            inbox,
            id,
            clock,
            parent,
            escalation_rx,
        )));

        handle
    }

    /// Resolve `id` to its local mailbox, or `None` if no live local actor of
    /// type `A` owns it (spec §4.3).
    pub fn resolve_local<A: Actor>(&self, id: &ActorId) -> Option<Mailbox<A>> {
        let actors = self.state.actors.lock().expect("actors mutex poisoned");
        actors
            .get(id)?
            .as_any()
            .downcast_ref::<ActorEntry<A>>()
            .map(|entry| entry.mailbox.clone())
    }

    /// Dispatch an inbound remote message to a local actor (spec §4.4).
    pub fn deliver(
        &self,
        codec: &dyn Codec,
        recipient: &ActorId,
        manifest: &str,
        payload: &[u8],
        reply: ReplyHandle,
    ) {
        let entry = {
            let actors = self.state.actors.lock().expect("actors mutex poisoned");
            actors.get(recipient).cloned()
        };
        match entry {
            Some(entry) => entry.deliver(codec, manifest, payload, reply),
            None => reply.fail(CallError::DeadLetter),
        }
    }

    /// Register a death-watch subscription (spec §12).
    pub fn add_watch(&self, target: ActorId, watcher: ActorId, deliver: WatchDelivery) {
        self.state.add_watch(target, watcher, deliver);
    }

    /// Cancel a death-watch subscription (spec §12).
    pub fn remove_watch(&self, target: &ActorId, watcher: &ActorId) {
        self.state.remove_watch(target, watcher);
    }

    /// Synthesize `Terminated { NodeDown }` for watchers of actors on `node`
    /// (spec §8.1 step 4). Called by the failure detector on a down decision.
    pub fn synthesize_node_down(&self, node: NodeId) {
        self.state.synthesize_node_down(node);
    }

    /// Deliver `Terminated { reason }` to every local watcher of `target`, then
    /// forget them (spec §12). Used by the cluster to fan a `Terminated` frame
    /// received from `target`'s node out to local watchers; the forget keeps it
    /// at most once per watcher (invariant #11).
    pub fn deliver_terminated(&self, target: &ActorId, reason: TerminationReason) {
        self.state.notify_terminated(target, reason);
    }
}

/// How one run of the message loop ended.
enum Outcome {
    /// Stopped gracefully (`Ctx::stop` or all senders gone).
    Stopped,
    /// A handler panicked.
    Faulted,
    /// A child escalated its failure to this actor (spec §11.1).
    Escalated,
}

/// What supervision decided for a fault (spec §11.2), after applying the restart
/// window and backoff.
enum Decision {
    /// Keep the actor, drop the failed message, carry on.
    Resume,
    /// Re-create the actor (after backoff).
    Restart,
    /// Stop with `Failed`.
    StopFailed,
    /// Stop, and fail this actor's parent (spec §11.1).
    Escalate,
}

/// How the actor's run finally ended.
enum End<A> {
    /// We still hold a live instance to stop with this reason.
    Stop(A, StopReason),
    /// The instance was already stopped (a restart whose factory was exhausted);
    /// just release the id with this reason.
    Released(TerminationReason),
}

/// The serial executor + supervisor for one actor (spec §6, §11). Processes one
/// message to completion before the next (so `&mut self` is never aliased), and
/// on a fault applies the actor's [`Supervision`] strategy: `Resume` keeps the
/// actor, `Restart` re-creates it from the factory with backoff, and
/// `Stop`/`Escalate` (or exceeding the restart window) terminate it. On stop the
/// mailbox is drained (so any queued `ask` is dead-lettered, never left hanging),
/// the id is released, and watchers are notified (spec §12).
#[allow(clippy::too_many_arguments)]
async fn run_actor<A, C>(
    state: Arc<HostState>,
    mut factory: ActorFactory<A>,
    ctx: Ctx<A>,
    inbox: Inbox<A>,
    id: ActorId,
    clock: C,
    parent: Option<ActorId>,
    escalation: async_channel::Receiver<ActorId>,
) where
    A: Actor,
    C: Clock,
{
    let supervision = A::supervision();
    let mut restarts: Vec<Instant> = Vec::new();

    let Some(mut actor) = factory() else {
        release(&state, &id, TerminationReason::Stopped);
        return;
    };
    let mut need_start = true;
    let mut ever_started = false;

    let end = loop {
        if need_start {
            if let Err(_err) = actor.started(&ctx).await {
                match supervise(&supervision, Fault::Started, &clock, &mut restarts).await {
                    Decision::Resume => need_start = false,
                    Decision::StopFailed => break End::Stop(actor, StopReason::Failed),
                    Decision::Escalate => {
                        escalate(&state, &parent, &id);
                        break End::Stop(actor, StopReason::Failed);
                    }
                    Decision::Restart => {
                        actor.stopped(StopReason::Failed).await;
                        match factory() {
                            Some(fresh) => {
                                actor = fresh;
                                continue;
                            }
                            None => break End::Released(TerminationReason::Failed),
                        }
                    }
                }
            } else {
                // `ActorReady` fires once (spec §4.2); a restart re-runs `started`
                // but the id stays resolvable, so it emits `Restarted` instead.
                if ever_started {
                    state.events.emit(Event::Restarted { actor: id.clone() });
                } else {
                    state.events.emit(Event::ActorReady { id: id.clone() });
                    ever_started = true;
                }
                need_start = false;
            }
        }

        let fault = match message_loop(&mut actor, &ctx, &inbox, &escalation, &state, &id).await {
            Outcome::Stopped => break End::Stop(actor, StopReason::Stopped),
            Outcome::Faulted => Fault::Message,
            Outcome::Escalated => Fault::Escalation,
        };
        match supervise(&supervision, fault, &clock, &mut restarts).await {
            Decision::Resume => continue,
            Decision::StopFailed => break End::Stop(actor, StopReason::Failed),
            Decision::Escalate => {
                escalate(&state, &parent, &id);
                break End::Stop(actor, StopReason::Failed);
            }
            Decision::Restart => {
                // The faulted instance stops with `Failed` (spec §11.2).
                actor.stopped(StopReason::Failed).await;
                match factory() {
                    Some(fresh) => {
                        actor = fresh;
                        need_start = true;
                        continue;
                    }
                    None => break End::Released(TerminationReason::Failed),
                }
            }
        }
    };

    // Dead-letter anything still queued so callers waiting on a reply do not
    // hang once the actor is gone (an `ask`'s reply channel cancels when its
    // envelope drops).
    inbox.close();
    while inbox.try_recv().is_ok() {}

    match end {
        End::Stop(actor, reason) => finish(actor, reason, &state, &id).await,
        End::Released(reason) => release(&state, &id, reason),
    }
}

/// Route this actor's failure to its parent's escalation inbox (spec §11.1).
fn escalate(state: &HostState, parent: &Option<ActorId>, id: &ActorId) {
    if let Some(parent) = parent {
        state.escalate(parent, id.clone());
    }
}

/// Apply the supervision directive for `fault`, including the restart window
/// (`max` within `within` → escalate to stop) and backoff sleep (spec §11.2).
async fn supervise<C: Clock>(
    supervision: &Supervision,
    fault: Fault,
    clock: &C,
    restarts: &mut Vec<Instant>,
) -> Decision {
    match supervision.decide(fault) {
        SupervisionDirective::Stop => Decision::StopFailed,
        SupervisionDirective::Escalate => Decision::Escalate,
        SupervisionDirective::Resume => Decision::Resume,
        SupervisionDirective::Restart {
            max,
            within,
            backoff,
        } => {
            let now = clock.now();
            restarts.retain(|t| now.duration_since(*t) < within);
            restarts.push(now);
            if restarts.len() as u32 > max {
                Decision::StopFailed
            } else {
                clock.sleep(backoff.delay(restarts.len() as u32)).await;
                Decision::Restart
            }
        }
    }
}

/// Process messages until the actor stops, panics, or a child escalates. The
/// escalation channel makes the executor interruptible (spec §11.1): a parked
/// actor reacts to a child failure promptly, not only on its next message.
async fn message_loop<A: Actor>(
    actor: &mut A,
    ctx: &Ctx<A>,
    inbox: &Inbox<A>,
    escalation: &async_channel::Receiver<ActorId>,
    state: &HostState,
    id: &ActorId,
) -> Outcome {
    loop {
        let next = inbox.recv();
        let escalated = escalation.recv();
        futures::pin_mut!(next, escalated);
        match futures::future::select(next, escalated).await {
            futures::future::Either::Left((Err(_), _)) => return Outcome::Stopped,
            futures::future::Either::Left((Ok(envelope), _)) => {
                let manifest = envelope.manifest;
                state.events.emit(Event::DispatchStart {
                    actor: id.clone(),
                    manifest,
                });
                let running = (envelope.run)(actor, ctx);
                let panicked = AssertUnwindSafe(running).catch_unwind().await.is_err();
                state.events.emit(Event::DispatchEnd {
                    actor: id.clone(),
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

/// Stop `actor` and release its identity (spec §4.2 step 5, §12).
async fn finish<A: Actor>(actor: A, reason: StopReason, state: &HostState, id: &ActorId) {
    actor.stopped(reason).await;
    release(state, id, reason.into());
}

/// Release an identity and notify watchers, without an actor instance to stop
/// (the instance was already stopped during a failed restart).
fn release(state: &HostState, id: &ActorId, reason: TerminationReason) {
    state
        .actors
        .lock()
        .expect("actors mutex poisoned")
        .remove(id);
    state
        .escalators
        .lock()
        .expect("escalators mutex poisoned")
        .remove(id);
    state.events.emit(Event::ResignId { id: id.clone() });
    state.notify_terminated(id, reason);
}
