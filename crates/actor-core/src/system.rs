//! The `ActorSystem` contract and the single-node `LocalSystem` (spec §4).
//!
//! [`ActorSystem`] is the runtime an actor runs on. Beyond spawning and local
//! resolution it carries the **transport boundary** (spec §4.1): locality
//! classification, the system codec, and the byte-level `remote_ask`/
//! `remote_tell` the typed [`ActorRef`] layer encodes onto. [`LocalSystem`] is
//! the single-node implementation — its remote methods are unreachable because
//! it has no peers; the cluster runtime (`actor-cluster`) provides a networked
//! implementation over the same trait.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use actor_serialization::Codec;
use actor_serialization::JsonCodec;

use crate::actor::Actor;
use crate::actor::Terminated;
use crate::actor::TerminationReason;
use crate::error::CallError;
use crate::event::EventSink;
use crate::host::LocalHost;
use crate::host::WatchDelivery;
use crate::id::ActorId;
use crate::id::NodeId;
use crate::mailbox::Mailbox;
use crate::receptionist::Receptionist;
use crate::receptionist::ReceptionistState;
use crate::refs::ActorRef;
use crate::runtime::Clock;
use crate::runtime::Entropy;
use crate::runtime::Spawner;

/// The runtime an actor runs on (spec §4). A cloneable handle: cloning shares
/// the same underlying system.
pub trait ActorSystem: Clone + Send + Sync + 'static {
    /// Spawn a root actor and return a handle to it (spec §4.1, §4.2).
    fn spawn<A: Actor<System = Self>>(&self, actor: A) -> ActorRef<A>;

    /// Spawn an actor parented to `parent` (the supervisor), so a fault it
    /// escalates fails the parent (spec §11.1). Used by [`Ctx::spawn`].
    ///
    /// [`Ctx::spawn`]: crate::Ctx::spawn
    fn spawn_child<A: Actor<System = Self>>(&self, child: A, parent: ActorId) -> ActorRef<A>;

    /// Resolve `id` to its local mailbox if a live local actor of type `A` owns
    /// it (spec §4.3). `None` for a remote or resigned id; performs no network
    /// round-trip and never blocks.
    fn resolve_local<A: Actor<System = Self>>(&self, id: &ActorId) -> Option<Mailbox<A>>;

    /// Build a typed handle to `id` on this system (spec §4.3). The handle works
    /// whether the target is local or remote; locality is decided on each send.
    fn resolve<A: Actor<System = Self>>(&self, id: ActorId) -> ActorRef<A> {
        ActorRef::from_parts(id, self.clone())
    }

    /// Classify `id` as owned by this node, from the id alone (spec §4.3).
    fn is_local(&self, id: &ActorId) -> bool;

    /// The system codec, fixed per system (spec §5). The `ActorRef` layer uses
    /// it to (de)serialize messages and replies that cross the wire.
    fn codec(&self) -> Arc<dyn Codec>;

    /// Send an already-encoded request to a remote actor and await its encoded
    /// reply (spec §4.1, transport boundary).
    fn remote_ask(
        &self,
        recipient: &ActorId,
        manifest: &'static str,
        payload: Vec<u8>,
        within: Duration,
    ) -> impl Future<Output = Result<Vec<u8>, CallError>> + Send;

    /// Send an already-encoded one-way message to a remote actor (spec §4.1).
    fn remote_tell(
        &self,
        recipient: &ActorId,
        manifest: &'static str,
        payload: Vec<u8>,
    ) -> impl Future<Output = Result<(), CallError>> + Send;

    /// Register `watcher`'s death watch of `target`, delivering via `deliver`
    /// (spec §12). An already-terminated target is reported immediately
    /// (invariant #12).
    fn watch(&self, target: ActorId, watcher: ActorId, deliver: WatchDelivery);

    /// Cancel `watcher`'s death watch of `target` (spec §12).
    fn unwatch(&self, target: &ActorId, watcher: &ActorId);

    /// This node's identity.
    fn node(&self) -> NodeId;

    /// The shared receptionist registry backing [`receptionist`](Self::receptionist).
    fn receptionist_state(&self) -> Arc<ReceptionistState>;

    /// Replicate a registration to the rest of the cluster (spec §13). A
    /// single-node system has no peers and does nothing.
    fn replicate_registration(&self, key: &str, origin: NodeId, id: ActorId);

    /// The receptionist for service discovery (spec §13).
    fn receptionist(&self) -> Receptionist<Self>
    where
        Self: Sized,
    {
        Receptionist::new(self.clone(), self.receptionist_state(), self.node())
    }
}

struct Inner<C, E, S> {
    clock: C,
    entropy: E,
    spawner: S,
    codec: Arc<dyn Codec>,
    host: LocalHost,
    receptionist: Arc<ReceptionistState>,
}

/// A single-node actor system (spec §4). Generic over the runtime seam so the
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

    /// Set the bounded mailbox capacity for every actor (spec §6).
    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        assert!(capacity > 0, "mailbox capacity must be positive");
        self.mailbox_capacity = capacity;
        self
    }

    /// Set the wire codec (spec §5).
    pub fn codec(mut self, codec: Arc<dyn Codec>) -> Self {
        self.codec = codec;
        self
    }

    /// Route observability events to `events` (spec §16).
    pub fn events(mut self, events: Arc<dyn EventSink>) -> Self {
        self.events = events;
        self
    }

    /// Build the system.
    pub fn build(self) -> LocalSystem<C, E, S> {
        let host = LocalHost::new(self.node, self.events, self.mailbox_capacity);
        LocalSystem {
            inner: Arc::new(Inner {
                clock: self.clock,
                entropy: self.entropy,
                spawner: self.spawner,
                codec: self.codec,
                host,
                receptionist: Arc::new(ReceptionistState::new()),
            }),
        }
    }
}

impl<C: Clock, E: Entropy, S: Spawner> LocalSystem<C, E, S> {
    /// Build a system with default configuration.
    pub fn new(clock: C, entropy: E, spawner: S) -> LocalSystem<C, E, S> {
        LocalSystemBuilder::new(clock, entropy, spawner).build()
    }

    /// The system clock.
    pub fn clock(&self) -> &C {
        &self.inner.clock
    }

    /// The system entropy source.
    pub fn entropy(&self) -> &E {
        &self.inner.entropy
    }

    /// The system task spawner.
    pub fn spawner(&self) -> &S {
        &self.inner.spawner
    }

    /// Spawn an actor from a `factory` so it can be restarted on fault (spec
    /// §11.2). Unlike [`spawn`](ActorSystem::spawn), a `Restart` directive
    /// re-creates the actor by calling `factory` again.
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
}

impl<C: Clock, E: Entropy, S: Spawner> ActorSystem for LocalSystem<C, E, S> {
    fn spawn<A: Actor<System = Self>>(&self, actor: A) -> ActorRef<A> {
        // A value-spawned actor yields once; a restart directive degrades to Stop.
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
        _manifest: &'static str,
        _payload: Vec<u8>,
        _within: Duration,
    ) -> Result<Vec<u8>, CallError> {
        Err(CallError::Unreachable)
    }

    async fn remote_tell(
        &self,
        _recipient: &ActorId,
        _manifest: &'static str,
        _payload: Vec<u8>,
    ) -> Result<(), CallError> {
        Err(CallError::Unreachable)
    }

    fn watch(&self, target: ActorId, watcher: ActorId, deliver: WatchDelivery) {
        // Single node: a target not currently live has already terminated.
        if !self.inner.host.contains(&target) {
            deliver(Terminated {
                id: target,
                reason: TerminationReason::Stopped,
            });
            return;
        }
        self.inner.host.add_watch(target, watcher, deliver);
    }

    fn unwatch(&self, target: &ActorId, watcher: &ActorId) {
        self.inner.host.remove_watch(target, watcher);
    }

    fn node(&self) -> NodeId {
        self.inner.host.node()
    }

    fn receptionist_state(&self) -> Arc<ReceptionistState> {
        Arc::clone(&self.inner.receptionist)
    }

    // Single node: no peers to replicate to.
    fn replicate_registration(&self, _key: &str, _origin: NodeId, _id: ActorId) {}
}
