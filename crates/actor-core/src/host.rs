//! The node-local actor runtime — the implementor-facing seam (spec §4.2, §6, §12).
//!
//! [`LocalHost`] owns the actors running on one node: it assigns identities,
//! stores each actor's mailbox plus its dispatch table, runs the serial
//! executor, resolves ids to local mailboxes, dispatches inbound messages, and
//! tracks death-watch subscriptions. It is the machinery an [`ActorSystem`]
//! *implementor* builds on: the single-node [`LocalSystem`](crate::LocalSystem)
//! is a thin wrapper over it, and the cluster runtime composes it with a
//! transport to add the network boundary.
//!
//! **This module is not application API.** Programs use
//! [`Actor`], [`ActorRef`], [`Ctx`], and the [`ActorSystem`] trait; only code
//! implementing a
//! *new* `ActorSystem` reaches for [`LocalHost`], [`WatchDelivery`], or
//! [`ActorFactory`]. They are `pub` for exactly that reason but deliberately
//! kept out of the crate's top-level prelude, so the user-facing surface stays
//! the model, not its runtime. This is the one boundary the cluster crate
//! crosses into core; the ~dozen methods below are its whole contract.
//!
//! **Death-watch is split across the boundary by design, not by accident.** The
//! host owns *local* delivery: when a watched actor on this node stops, its local
//! watchers receive a [`Terminated`] through their mailbox, and when a peer node
//! is declared `down`,
//! [`synthesize_node_down`](LocalHost::synthesize_node_down) manufactures a
//! `Terminated { NodeDown }` for each local watcher of an actor on that node
//! (spec §8.1 step 4) — no stop message can arrive from a dead node. Delivery to
//! a *remote* watcher is the implementor's job: it supplies a [`WatchDelivery`]
//! closure that forwards the signal over its transport, because the host is
//! transport-agnostic and must not know how a frame reaches another node. The
//! host stores and fires that closure ([`add_watch`](LocalHost::add_watch)); the
//! implementor decides what crossing the network means.

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
use crate::mailbox::Inbox;
use crate::mailbox::Mailbox;
use crate::refs::ActorRef;
use crate::registry::DispatchFn;
use crate::registry::HandlerRegistry;
use crate::reply::ReplyHandle;
use crate::runtime::BoxFuture;
use crate::runtime::Clock;
use crate::runtime::Instant;
use crate::runtime::Spawner;
use crate::supervision::Fault;
use crate::supervision::Supervision;
use crate::supervision::SupervisionDirective;
use crate::system::ActorSystem;

/// A source of fresh actor instances for (re)starts. Yields `Some` while the
/// actor can be (re-)created and `None` once exhausted — a value-spawned actor
/// yields once, so a restart directive degrades to `Stop`; a factory-spawned one
/// yields indefinitely (spec §11.2).
pub type ActorFactory<A> = Box<dyn FnMut() -> Option<A> + Send>;

/// Delivers a [`Terminated`] to one watcher by enqueuing it on the watcher's
/// mailbox. Built in [`Ctx::watch`](crate::Ctx::watch), where the watcher type
/// is known; stored type-erased in the watch registry.
///
/// Returns a future because local delivery applies mailbox backpressure rather
/// than dropping the signal (spec §6, §12 invariant #11): callers `.await` it so
/// a busy watcher is never skipped.
pub type WatchDelivery = Arc<dyn Fn(Terminated) -> BoxFuture<'static, ()> + Send + Sync>;

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

/// A bounded record of how recently-resigned local actors terminated, so a
/// watch placed *after* an actor is gone reports the true reason — `Failed`, not
/// a blanket `Stopped` — to the watcher (spec §12, invariant #12).
///
/// Watch-after-death has no live actor to ask, so without this the system can
/// only guess. It is bounded (a FIFO of the last [`CAP`](Terminations::CAP)
/// terminations): the watch-after-death race resolves within a few scheduler
/// turns of the stop, so only *recent* deaths need fidelity, and ancient ones
/// fall back to the `Stopped` default. Keyed lookups and insertion-ordered
/// eviction only — no iteration — so it adds no observable nondeterminism (§4.6).
#[derive(Default)]
struct Terminations {
    reason: BTreeMap<ActorId, TerminationReason>,
    order: VecDeque<ActorId>,
}

impl Terminations {
    /// How many recent terminations to remember. Far above the handful of
    /// in-flight watch-after-death races a run realistically has.
    const CAP: usize = 1024;

    fn record(&mut self, id: &ActorId, reason: TerminationReason) {
        if self.reason.insert(id.clone(), reason).is_none() {
            self.order.push_back(id.clone());
            if self.order.len() > Self::CAP {
                if let Some(evicted) = self.order.pop_front() {
                    self.reason.remove(&evicted);
                }
            }
        }
    }

    fn reason(&self, id: &ActorId) -> Option<TerminationReason> {
        self.reason.get(id).copied()
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
    /// Reasons recently-resigned local actors terminated with, for accurate
    /// watch-after-death reporting (spec §12, invariant #12).
    terminations: Mutex<Terminations>,
    /// Escalation inboxes: actor id → a sender its children escalate failures to
    /// (spec §11.1). The value is the failed child's id, for context.
    escalators: Mutex<BTreeMap<ActorId, async_channel::Sender<ActorId>>>,
    /// Per-actor-type dispatch tables, built once on first spawn of each type so
    /// `Actor::register` runs at most once per type (spec §4.4), not per spawn.
    /// The value is a type-erased `BTreeMap<&'static str, DispatchFn<A>>`. Keyed
    /// lookups only (never iterated), so it adds no observable nondeterminism.
    dispatch_cache: Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
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
    ///
    /// Delivery itself emits the `TerminatedDelivered` event (at the watcher's
    /// mailbox, see [`Mailbox::enqueue_signal`](crate::Mailbox)), so this only
    /// fans out: a local watcher's closure enqueues onto its mailbox; a remote
    /// watcher's closure forwards a frame, which is *not* a delivery and so is
    /// re-emitted only when it lands on the remote node.
    async fn notify_terminated(&self, target: &ActorId, reason: TerminationReason) {
        // Drop the lock before awaiting delivery: the guard is released at the
        // end of this statement, so no lock is held across the `.await` below.
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
                })
                .await;
            }
        }
    }

    /// Deliver `Terminated` to **one** named watcher of `target`, removing only
    /// that subscription (spec §12). Used for an inbound `Terminated` frame,
    /// which is addressed to a specific watcher: fanning it to *every* local
    /// watcher would give the others a spurious extra signal (a second watcher's
    /// watch-after-death re-watch must not re-notify the first — invariant #11).
    async fn notify_terminated_one(
        &self,
        target: &ActorId,
        watcher: &ActorId,
        reason: TerminationReason,
    ) {
        // Take just this watcher's delivery closure out under the lock, dropping
        // the empty entry behind it, then await delivery with no lock held.
        let deliver = {
            let mut watchers = self.watchers.lock().expect("watchers mutex poisoned");
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
            // Delivery emits `TerminatedDelivered` at the watcher's mailbox.
            deliver(Terminated {
                id: target.clone(),
                reason,
            })
            .await;
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
    async fn synthesize_node_down(&self, node: NodeId) {
        let targets: Vec<ActorId> = self
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
                terminations: Mutex::new(Terminations::default()),
                escalators: Mutex::new(BTreeMap::new()),
                dispatch_cache: Mutex::new(HashMap::new()),
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
        id.node() == self.state.node
    }

    /// Whether a live local actor with `id` currently exists.
    pub fn contains(&self, id: &ActorId) -> bool {
        self.state
            .actors
            .lock()
            .expect("actors mutex poisoned")
            .contains_key(id)
    }

    /// The reason a recently-resigned local actor terminated with, if still
    /// remembered. Used for accurate watch-after-death reporting: a watch placed
    /// once the actor is gone reports `Failed` rather than a blanket `Stopped`
    /// (spec §12, invariant #12). `None` once evicted (or never owned here), in
    /// which case callers fall back to `Stopped`.
    pub fn termination_reason(&self, id: &ActorId) -> Option<TerminationReason> {
        self.state
            .terminations
            .lock()
            .expect("terminations mutex poisoned")
            .reason(id)
    }

    fn assign_id(&self) -> ActorId {
        let n = self.state.next_path.fetch_add(1, Ordering::Relaxed);
        let id = ActorId::new(self.state.node, Path::new(format!("/user/{n}")), 0);
        self.state.events.emit(Event::AssignId { id: id.clone() });
        id
    }

    /// The dispatch table for actor type `A`, building it once and caching it by
    /// type (spec §4.4): `Actor::register` is invoked at most once per type — on
    /// the first spawn — rather than on every spawn. The cached map is a `fn`
    /// pointer table, so cloning it into each actor's entry is cheap.
    fn dispatch_table<A: Actor>(&self) -> BTreeMap<&'static str, DispatchFn<A>> {
        let key = TypeId::of::<A>();
        let mut cache = self
            .state
            .dispatch_cache
            .lock()
            .expect("dispatch cache mutex poisoned");
        if let Some(cached) = cache.get(&key) {
            return cached
                .downcast_ref::<BTreeMap<&'static str, DispatchFn<A>>>()
                .expect("dispatch cache type mismatch")
                .clone();
        }
        let mut registry = HandlerRegistry::<A>::default();
        A::register(&mut registry);
        let table = registry.into_entries();
        cache.insert(key, Arc::new(table.clone()));
        table
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

        let entry = ActorEntry {
            mailbox: mailbox.clone(),
            dispatch: self.dispatch_table::<A>(),
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
    pub async fn synthesize_node_down(&self, node: NodeId) {
        self.state.synthesize_node_down(node).await;
    }

    /// Deliver an inbound `Terminated { reason }` frame to the single `watcher`
    /// it is addressed to, removing only that subscription (spec §12). A frame
    /// arrives once per remote watcher, so delivering to just that watcher keeps
    /// each notified exactly once per `watch` and never lets one watcher's signal
    /// spill onto another's subscription (invariant #11).
    pub async fn deliver_terminated_to(
        &self,
        target: &ActorId,
        watcher: &ActorId,
        reason: TerminationReason,
    ) {
        self.state
            .notify_terminated_one(target, watcher, reason)
            .await;
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

/// The executor's next step in an actor's life (spec §6, §11): (re)start the
/// instance, run its message loop, or end it. The loop and [`handle_fault`] both
/// speak this, so the start/run/terminate transitions live in one vocabulary.
enum Step<A> {
    /// Run `started` (first start or a restart), then process messages.
    Start(A),
    /// Process messages until the next stop or fault.
    Run(A),
    /// Terminate, via the shared end-of-life path.
    End(End<A>),
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

    let Some(actor) = factory() else {
        release(&state, &id, TerminationReason::Stopped).await;
        return;
    };
    let mut ever_started = false;

    // The actor's life is a small state machine over `Step`: `Start` runs
    // `started`, `Run` processes messages, and a fault from either phase flows
    // through the one `handle_fault` procedure, which returns the next `Step` —
    // re-start, keep running, or terminate. The supervision policy thus lives in
    // exactly one place instead of once per phase.
    let mut step = Step::Start(actor);
    let end = loop {
        step = match step {
            Step::Start(mut actor) => match actor.started(&ctx).await {
                Ok(()) => {
                    // `ActorReady` fires once (spec §4.2); a restart re-runs
                    // `started` but the id stays resolvable, so it emits
                    // `Restarted` instead.
                    if ever_started {
                        state.events.emit(Event::Restarted { actor: id.clone() });
                    } else {
                        state.events.emit(Event::ActorReady { id: id.clone() });
                        ever_started = true;
                    }
                    Step::Run(actor)
                }
                Err(_err) => {
                    handle_fault(
                        Fault::Started,
                        actor,
                        &supervision,
                        &clock,
                        &mut restarts,
                        &ctx,
                        &state,
                        &id,
                        &parent,
                        &mut factory,
                    )
                    .await
                }
            },
            Step::Run(mut actor) => {
                let fault =
                    match message_loop(&mut actor, &ctx, &inbox, &escalation, &state, &id).await {
                        Outcome::Stopped => break End::Stop(actor, StopReason::Stopped),
                        Outcome::Faulted => Fault::Message,
                        Outcome::Escalated => Fault::Escalation,
                    };
                handle_fault(
                    fault,
                    actor,
                    &supervision,
                    &clock,
                    &mut restarts,
                    &ctx,
                    &state,
                    &id,
                    &parent,
                    &mut factory,
                )
                .await
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
        End::Stop(actor, reason) => finish(actor, reason, &state, &id).await,
        End::Released(reason) => release(&state, &id, reason).await,
    }
}

/// Apply supervision to one fault and report what the executor should do next
/// (spec §11.2). Shared by the `started`-fault and message-loop-fault paths, so
/// both resume, restart, escalate, and degrade through exactly one procedure.
///
/// Takes the live `actor` by value: `Restart`/`Stop`/`Escalate` consume it
/// (a restart runs its `stopped` hook first), while `Resume` hands it back
/// unchanged. The `Restart` arm defers its event to [`resolve_restart`], which
/// emits the *effective* decision once it knows whether a fresh instance exists.
#[allow(clippy::too_many_arguments)]
async fn handle_fault<A, C>(
    fault: Fault,
    actor: A,
    supervision: &Supervision,
    clock: &C,
    restarts: &mut Vec<Instant>,
    ctx: &Ctx<A>,
    state: &HostState,
    id: &ActorId,
    parent: &Option<ActorId>,
    factory: &mut ActorFactory<A>,
) -> Step<A>
where
    A: Actor,
    C: Clock,
{
    let decision = supervise(supervision, fault, clock, restarts, &|| {
        ctx.system().next_random()
    })
    .await;
    match decision {
        Decision::Resume => {
            emit_supervision(state, id, fault, &Decision::Resume);
            Step::Run(actor)
        }
        Decision::StopFailed => {
            emit_supervision(state, id, fault, &Decision::StopFailed);
            Step::End(End::Stop(actor, StopReason::Failed))
        }
        Decision::Escalate => {
            emit_supervision(state, id, fault, &Decision::Escalate);
            escalate(state, parent, id);
            Step::End(End::Stop(actor, StopReason::Failed))
        }
        Decision::Restart => {
            // The faulted instance stops with `Failed` (spec §11.2); then
            // `resolve_restart` emits the effective decision — a real restart, or
            // a degrade to stop when the factory is spent.
            actor.stopped(StopReason::Failed).await;
            match resolve_restart(state, id, fault, factory) {
                Some(fresh) => Step::Start(fresh),
                None => Step::End(End::Released(TerminationReason::Failed)),
            }
        }
    }
}

/// Carry out a `Restart` decision against the factory, emitting the *effective*
/// supervision decision (spec §11.2, §16).
///
/// An actor spawned **by value** has an exhausted factory and cannot be
/// reconstructed, so its `Restart` degrades to `Stop` (spec §11.2). The event
/// stream MUST report that effective `Stop`, not the candidate `Restart`:
/// `Supervised` always carries the effective decision (see [`emit_supervision`]),
/// and an observer (or a continuous checker) that saw `Restart` here would expect
/// a following `Restarted`, never the `ResignId` a degraded stop produces.
///
/// Returns the fresh instance to swap in on a real restart, or `None` when it
/// degraded to a stop (the caller then terminates the actor with `Failed`).
fn resolve_restart<A: Actor>(
    state: &HostState,
    id: &ActorId,
    fault: Fault,
    factory: &mut ActorFactory<A>,
) -> Option<A> {
    match factory() {
        Some(fresh) => {
            emit_supervision(state, id, fault, &Decision::Restart);
            Some(fresh)
        }
        None => {
            emit_supervision(state, id, fault, &Decision::StopFailed);
            None
        }
    }
}

/// Emit the supervision decision for a faulted actor (spec §11.2, §16). The
/// decision is the effective one — the restart window and backoff have already
/// been applied, so an exhausted window surfaces here as `Stop`, and a `Restart`
/// with no factory to rebuild from degrades to `Stop` (see [`resolve_restart`]).
fn emit_supervision(state: &HostState, id: &ActorId, fault: Fault, decision: &Decision) {
    let decision = match decision {
        Decision::Resume => SupervisionDecision::Resume,
        Decision::Restart => SupervisionDecision::Restart,
        Decision::StopFailed => SupervisionDecision::Stop,
        Decision::Escalate => SupervisionDecision::Escalate,
    };
    state.events.emit(Event::Supervised {
        actor: id.clone(),
        fault,
        decision,
    });
}

/// Route this actor's failure to its parent's escalation inbox (spec §11.1).
fn escalate(state: &HostState, parent: &Option<ActorId>, id: &ActorId) {
    if let Some(parent) = parent {
        state.escalate(parent, id.clone());
    }
}

/// Apply the supervision directive for `fault`, including the restart window
/// (`max` within `within` → escalate to stop) and backoff sleep (spec §11.2).
/// `draw_random` yields a value from the system's seeded `Entropy` (§4.6) for
/// backoff jitter; it is called only when a jittered sleep actually happens, so
/// a non-restart fault consumes no randomness and never perturbs the run.
async fn supervise<C: Clock>(
    supervision: &Supervision,
    fault: Fault,
    clock: &C,
    restarts: &mut Vec<Instant>,
    draw_random: &(dyn Fn() -> u64 + Send + Sync),
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
                let base = backoff.delay(restarts.len() as u32);
                let delay = if base.is_zero() {
                    base
                } else {
                    jittered(base, draw_random())
                };
                clock.sleep(delay).await;
                Decision::Restart
            }
        }
    }
}

/// Apply equal-jitter to a backoff delay (spec §11.2): hold half the delay fixed
/// and randomize the other half, so a wave of simultaneous restarts desynchronizes
/// instead of retrying in lockstep. `rand` comes from the seeded `Entropy` (§4.6),
/// keeping a simulated run reproducible.
fn jittered(base: std::time::Duration, rand: u64) -> std::time::Duration {
    let nanos = base.as_nanos() as u64;
    let half = nanos / 2;
    std::time::Duration::from_nanos(half + rand % (half + 1))
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
    release(state, id, reason.into()).await;
}

/// Release an identity and notify watchers, without an actor instance to stop
/// (the instance was already stopped during a failed restart).
async fn release(state: &HostState, id: &ActorId, reason: TerminationReason) {
    // Record the reason before the actor leaves the live set, so a watch that
    // arrives after this point reads it rather than racing an empty slot (§12).
    state
        .terminations
        .lock()
        .expect("terminations mutex poisoned")
        .record(id, reason);
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
    state.notify_terminated(id, reason).await;
}
