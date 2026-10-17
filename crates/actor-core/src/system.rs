//! The `ActorSystem` contract and the single-node `LocalSystem` (spec §2.8).
//!
//! [`ActorSystem`] is the runtime an actor runs on. Beyond spawning and local
//! resolution it carries the **transport boundary** (spec §2.8): locality
//! classification, the system codec, and the byte-level `remote_ask`/
//! `remote_tell` the typed [`ActorRef`] layer encodes onto. [`LocalSystem`] is
//! the single-node implementation: its remote methods are unreachable because
//! it has no peers; the cluster runtime (`actor-cluster`) provides a networked
//! implementation over the same trait.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use actor_serialization::Codec;
use actor_serialization::JsonCodec;

use crate::actor::Actor;
use crate::error::CallError;
use crate::event::Event;
use crate::event::EventSink;
use crate::host::ActorFactory;
use crate::host::LocalHost;
use crate::host::WatchDelivery;
use crate::host::always;
use crate::host::once;
use crate::id::ActorId;
use crate::id::NodeId;
use crate::mailbox::Mailbox;
use crate::message::Manifest;
use crate::receptionist::Receptionist;
use crate::receptionist::ReceptionistState;
use crate::refs::ActorRef;
use crate::runtime::BoxFuture;
use crate::runtime::Clock;
use crate::runtime::Entropy;
use crate::runtime::Instant;
use crate::runtime::Spawner;

/// The runtime an actor runs on (spec §2.8). A cloneable handle: cloning shares
/// the same underlying system.
///
/// One trait with two faces (spec §2.8). A layer above calls the consumer face:
/// `spawn`/`spawn_with`/`resolve`, `node`, `receptionist`, `clock`/`now`/`sleep`,
/// `launch`, `emit`, and `next_random`, taking its identity, time, randomness,
/// scheduling, and observability from the system so one seed reproduces it under
/// simulation. An implementation supplies the rest: `spawn_factory`,
/// `resolve_local`, `is_local`, `in_rotation`, `codec`, `remote_ask`/`remote_tell`,
/// `watch`/`unwatch`, `receptionist_state`, and `replicate_registration`. The
/// provided methods define the first face in terms of the second.
pub trait ActorSystem: Clone + Send + Sync + 'static {
    /// Spawn an actor from `factory`, parented to `parent` (`None` for a root):
    /// the one spawn seam an implementation supplies (spec §2.8, §3.1). The
    /// factory yields the initial instance and any restart re-creations, and
    /// `None` once exhausted (spec §2.4). Callers use the four provided forms
    /// ([`spawn`], [`spawn_with`], [`Ctx::spawn`], [`Ctx::spawn_with`]), which
    /// are defined in terms of this.
    ///
    /// [`spawn`]: ActorSystem::spawn
    /// [`spawn_with`]: ActorSystem::spawn_with
    /// [`Ctx::spawn`]: crate::Ctx::spawn
    /// [`Ctx::spawn_with`]: crate::Ctx::spawn_with
    fn spawn_factory<A: Actor<System = Self>>(
        &self,
        factory: ActorFactory<A>,
        parent: Option<ActorId>,
    ) -> ActorRef<A>;

    /// Spawn a root actor and return a handle to it (spec §2.8, §3.1). The
    /// actor is value-spawned: it cannot be re-created, so a `Restart`
    /// directive degrades to `Stop` (spec §2.4); use
    /// [`spawn_with`](ActorSystem::spawn_with) for a restartable actor.
    fn spawn<A: Actor<System = Self>>(&self, actor: A) -> ActorRef<A> {
        self.spawn_factory(once(actor), None)
    }

    /// Spawn a root actor from a `factory` so a `Restart` directive can
    /// re-create it (spec §2.4), unlike the value-based
    /// [`spawn`](ActorSystem::spawn).
    fn spawn_with<A, F>(&self, factory: F) -> ActorRef<A>
    where
        A: Actor<System = Self>,
        F: FnMut() -> A + Send + 'static,
    {
        self.spawn_factory(always(factory), None)
    }

    /// Resolve `id` to its local mailbox if a live local actor of type `A` owns
    /// it (spec §3.2). `None` for a remote or resigned id; performs no network
    /// round-trip and never blocks.
    fn resolve_local<A: Actor<System = Self>>(&self, id: &ActorId) -> Option<Mailbox<A>>;

    /// Build a typed handle to `id` on this system (spec §3.2). The handle works
    /// whether the target is local or remote; locality is decided on each send.
    fn resolve<A: Actor<System = Self>>(&self, id: ActorId) -> ActorRef<A> {
        ActorRef::from_parts(id, self.clone())
    }

    /// Classify `id` as owned by this node, from the id alone (spec §3.2).
    fn is_local(&self, id: &ActorId) -> bool;

    /// Whether `node` is in rotation for service discovery: the receptionist
    /// listing filter (spec §6.4, §2.6 requirement 4). The default is `true` (a
    /// system without membership treats every node as available); a clustered
    /// system returns `false` for a node an operator has drained for maintenance,
    /// or one that is down. It gates which actors a receptionist
    /// [`lookup`](crate::Receptionist::lookup) hands back, not whether a node is
    /// reachable: a drained node still answers direct calls. Looser than a
    /// cluster's placement serving set, which also leaves out joining and
    /// confirmed-unreachable nodes.
    fn in_rotation(&self, _node: NodeId) -> bool {
        true
    }

    /// The system codec, fixed per system (spec §2.11). The `ActorRef` layer uses
    /// it to (de)serialize messages and replies that cross the wire.
    fn codec(&self) -> Arc<dyn Codec>;

    /// Send an already-encoded request to a remote actor and await its encoded
    /// reply (spec §2.8, transport boundary). The typed caller,
    /// [`ActorRef::ask`], brackets the call with [`Event::AskIssued`] and
    /// [`Event::AskOutcome`], so an implementation emits neither.
    fn remote_ask(
        &self,
        recipient: &ActorId,
        manifest: Manifest,
        payload: Vec<u8>,
        within: Duration,
    ) -> impl Future<Output = Result<Vec<u8>, CallError>> + Send;

    /// Send an already-encoded one-way message to a remote actor (spec §2.8).
    fn remote_tell(
        &self,
        recipient: &ActorId,
        manifest: Manifest,
        payload: Vec<u8>,
    ) -> impl Future<Output = Result<(), CallError>> + Send;

    /// Register `watcher`'s death watch of `target`, delivering via `deliver`
    /// (spec §2.5). An already-terminated target is reported immediately
    /// (invariant #12).
    fn watch(&self, target: ActorId, watcher: ActorId, deliver: WatchDelivery);

    /// Cancel `watcher`'s death watch of `target` (spec §2.5).
    fn unwatch(&self, target: &ActorId, watcher: &ActorId);

    /// This node's identity.
    fn node(&self) -> NodeId;

    /// A draw from the system's seeded [`Entropy`] (§2.12.1), the
    /// single source of randomness in the run: it never reaches for a host RNG, so
    /// a simulated run stays reproducible. Used for supervision backoff jitter
    /// (§2.4) and by actors needing deterministic randomness.
    fn next_random(&self) -> u64;

    /// The system's [`Clock`] (§2.12.1): the single source of time. Layered
    /// runtimes read and schedule time through this (never a host clock), so
    /// one seed reproduces their timing under simulation.
    type Clock: Clock;

    /// The system clock.
    fn clock(&self) -> &Self::Clock;

    /// The current logical time: [`clock`](ActorSystem::clock)`().now()`.
    fn now(&self) -> Instant {
        self.clock().now()
    }

    /// A future that completes after `dur` of logical time, detachable: unlike
    /// [`Clock::sleep`], whose future borrows the clock, this one clones the
    /// clock handle so the future is `'static` and can be
    /// [`launch`](ActorSystem::launch)ed as a background task.
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        let clock = self.clock().clone();
        Box::pin(async move { clock.sleep(dur).await })
    }

    /// Launch a detached background task on the system's executor (§2.12.1): the
    /// same [`Spawner`] that runs the mailboxes, so layered
    /// runtimes' loops are scheduled (and, under simulation, interleaved
    /// deterministically) like everything else.
    fn launch(&self, task: BoxFuture<'static, ()>);

    /// Emit onto the observability stream (spec §2.7). Layered runtimes
    /// extending the [`Event`] enum through [`Event::App`] emit into the same
    /// ordered stream the checkers and the reproducibility recorder read.
    fn emit(&self, event: Event);

    /// The shared receptionist registry backing [`receptionist`](Self::receptionist).
    fn receptionist_state(&self) -> Arc<ReceptionistState>;

    /// Replicate a registration to the rest of the cluster (spec §2.6). The
    /// default does nothing: a single-node system has no peers.
    fn replicate_registration(&self, _key: &str, _origin: NodeId, _id: ActorId) {}

    /// The receptionist for service discovery (spec §2.6).
    fn receptionist(&self) -> Receptionist<Self>
    where
        Self: Sized,
    {
        Receptionist::new(self.clone(), self.receptionist_state())
    }
}

struct Inner<C, E, S> {
    clock: C,
    entropy: E,
    spawner: S,
    codec: Arc<dyn Codec>,
    host: LocalHost,
    receptionist: Arc<ReceptionistState>,
    events: Arc<dyn EventSink>,
}

/// A single-node actor system (spec §2.8). Generic over the runtime seam so the
/// same code runs in production and under deterministic simulation.
pub struct LocalSystem<C, E, S> {
    inner: Arc<Inner<C, E, S>>,
}

impl<C, E, S> Clone for LocalSystem<C, E, S> {
    fn clone(&self) -> Self {
        LocalSystem {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Builder for a [`LocalSystem`] (node id, mailbox capacity, codec, events).
pub struct LocalSystemBuilder<C, E, S> {
    clock: C,
    entropy: E,
    spawner: S,
    node: NodeId,
    mailbox_capacity: usize,
    codec: Arc<dyn Codec>,
    events: Arc<dyn EventSink>,
}

impl<C: Clock, E: Entropy, S: Spawner> LocalSystemBuilder<C, E, S> {
    /// Start a builder with the given runtime seam and defaults (node 0, mailbox
    /// capacity 64, JSON codec, no-op observability).
    pub fn new(clock: C, entropy: E, spawner: S) -> LocalSystemBuilder<C, E, S> {
        LocalSystemBuilder {
            clock,
            entropy,
            spawner,
            node: NodeId::new(0),
            mailbox_capacity: 64,
            codec: Arc::new(JsonCodec),
            events: Arc::new(()),
        }
    }

    /// Set this node's identity.
    pub fn node(mut self, node: NodeId) -> Self {
        self.node = node;
        self
    }

    /// Set the bounded mailbox capacity for every actor (spec §3.3).
    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        assert!(capacity > 0, "mailbox capacity must be positive");
        self.mailbox_capacity = capacity;
        self
    }

    /// Set the wire codec (spec §2.11).
    pub fn codec(mut self, codec: Arc<dyn Codec>) -> Self {
        self.codec = codec;
        self
    }

    /// Route observability events to `events` (spec §2.7).
    pub fn events(mut self, events: Arc<dyn EventSink>) -> Self {
        self.events = events;
        self
    }

    /// Build the system.
    pub fn build(self) -> LocalSystem<C, E, S> {
        let host = LocalHost::new(self.node, Arc::clone(&self.events), self.mailbox_capacity);
        LocalSystem {
            inner: Arc::new(Inner {
                clock: self.clock,
                entropy: self.entropy,
                spawner: self.spawner,
                codec: self.codec,
                host,
                receptionist: Arc::new(ReceptionistState::new()),
                events: self.events,
            }),
        }
    }
}

impl<C: Clock, E: Entropy, S: Spawner> LocalSystem<C, E, S> {
    /// Build a system with default configuration.
    pub fn new(clock: C, entropy: E, spawner: S) -> LocalSystem<C, E, S> {
        LocalSystemBuilder::new(clock, entropy, spawner).build()
    }

    /// The system entropy source.
    pub fn entropy(&self) -> &E {
        &self.inner.entropy
    }
}

impl<C: Clock, E: Entropy, S: Spawner> ActorSystem for LocalSystem<C, E, S> {
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

    fn codec(&self) -> Arc<dyn Codec> {
        Arc::clone(&self.inner.codec)
    }

    // A single-node system has no peers: any non-local target is unreachable.
    async fn remote_ask(
        &self,
        _recipient: &ActorId,
        _manifest: Manifest,
        _payload: Vec<u8>,
        _within: Duration,
    ) -> Result<Vec<u8>, CallError> {
        Err(CallError::Unreachable)
    }

    async fn remote_tell(
        &self,
        _recipient: &ActorId,
        _manifest: Manifest,
        _payload: Vec<u8>,
    ) -> Result<(), CallError> {
        Err(CallError::Unreachable)
    }

    fn watch(&self, target: ActorId, watcher: ActorId, deliver: WatchDelivery) {
        // The host judges an already-terminated target (invariant #12). Launch
        // the immediate delivery it hands back rather than running it inline: a
        // watcher calling `watch` from its own handler must not deadlock
        // against its own mailbox.
        if let Some(immediate) = self.inner.host.watch(target, watcher, deliver) {
            self.inner.spawner.launch(immediate);
        }
    }

    fn unwatch(&self, target: &ActorId, watcher: &ActorId) {
        self.inner.host.remove_watch(target, watcher);
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
}
