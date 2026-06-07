//! The receptionist: service discovery (spec §13).
//!
//! Actors are addressed by [`ActorRef`], but a node needs a way to obtain the
//! initial handle for a remote service without hardcoding its [`ActorId`]. The
//! receptionist is a cluster-replicated registry: register an actor under a
//! typed [`Key`], and any node can [`lookup`](Receptionist::lookup) or
//! [`subscribe`](Receptionist::subscribe) to the current [`Listing`].
//!
//! Registrations are an OR-set keyed by the registering node, so concurrent
//! registrations merge. **Pruning rides on death watch** (spec §12, §13): the
//! receptionist watches each registered actor, so when one stops — or its node
//! is declared `down` (spec §8.1 step 5) — the corresponding registration is
//! removed and subscribers see a fresh listing.
//!
//! Only [`ActorId`]s travel and are stored; a typed [`ActorRef`] is
//! reconstructed at the lookup site (where the actor type is known) via
//! [`ActorSystem::resolve`], so this needs no `ActorRef` wire format.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;

use futures::Stream;
use futures::StreamExt;

use crate::actor::Actor;
use crate::actor::Terminated;
use crate::host::WatchDelivery;
use crate::id::ActorId;
use crate::id::NodeId;
use crate::id::Path;
use crate::refs::ActorRef;
use crate::system::ActorSystem;

/// A typed registry key (spec §13). The actor type makes `lookup`/`subscribe`
/// return correctly typed `ActorRef`s.
pub struct Key<A> {
    id: &'static str,
    _marker: PhantomData<fn() -> A>,
}

impl<A> Key<A> {
    /// Define a key from a stable string identity.
    pub const fn new(id: &'static str) -> Key<A> {
        Key {
            id,
            _marker: PhantomData,
        }
    }

    /// The key's string identity.
    pub const fn id(&self) -> &'static str {
        self.id
    }
}

impl<A> Clone for Key<A> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<A> Copy for Key<A> {}

/// A snapshot of the actors registered under a key (spec §13).
pub struct Listing<A: Actor> {
    refs: Vec<ActorRef<A>>,
}

impl<A: Actor> Listing<A> {
    /// The first registered actor, if any.
    pub fn first(&self) -> Option<&ActorRef<A>> {
        self.refs.first()
    }

    /// Iterate the registered actors.
    pub fn iter(&self) -> impl Iterator<Item = &ActorRef<A>> {
        self.refs.iter()
    }

    /// Whether the listing is empty.
    pub fn is_empty(&self) -> bool {
        self.refs.is_empty()
    }

    /// How many actors are registered.
    pub fn len(&self) -> usize {
        self.refs.len()
    }

    /// Consume the listing into its handles.
    pub fn into_vec(self) -> Vec<ActorRef<A>> {
        self.refs
    }
}

/// The node-local registry data shared between the receptionist handle, the
/// receive loop (remote registrations), and the death-watch pruning closures.
pub struct ReceptionistState {
    /// key → set of (registering node, actor id). The node component is the
    /// OR-set tag; storing the actor id type-agnostically lets one registry
    /// hold every key.
    registry: Mutex<BTreeMap<String, BTreeSet<(NodeId, ActorId)>>>,
    subscribers: Mutex<BTreeMap<String, Vec<async_channel::Sender<Vec<ActorId>>>>>,
}

impl Default for ReceptionistState {
    fn default() -> Self {
        ReceptionistState {
            registry: Mutex::new(BTreeMap::new()),
            subscribers: Mutex::new(BTreeMap::new()),
        }
    }
}

impl ReceptionistState {
    /// A fresh, empty registry.
    pub fn new() -> ReceptionistState {
        ReceptionistState::default()
    }

    /// Add `(origin, id)` under `key`; returns whether the set changed.
    fn add(&self, key: &str, origin: NodeId, id: ActorId) -> bool {
        self.registry
            .lock()
            .expect("registry mutex poisoned")
            .entry(key.to_string())
            .or_default()
            .insert((origin, id))
    }

    /// Remove `id` from every key (an actor terminated), notifying the
    /// subscribers of each affected key.
    fn remove_actor(&self, id: &ActorId) {
        let affected: Vec<String> = {
            let mut registry = self.registry.lock().expect("registry mutex poisoned");
            let mut affected = Vec::new();
            for (key, set) in registry.iter_mut() {
                let before = set.len();
                set.retain(|(_, actor)| actor != id);
                if set.len() != before {
                    affected.push(key.clone());
                }
            }
            affected
        };
        for key in affected {
            self.notify(&key);
        }
    }

    /// Every `(key, origin, actor)` entry in the registry, in deterministic
    /// order — the full local view, advertised to a peer for anti-entropy
    /// reconciliation (spec §13).
    pub fn digest(&self) -> Vec<(String, NodeId, ActorId)> {
        let registry = self.registry.lock().expect("registry mutex poisoned");
        let mut entries = Vec::new();
        for (key, set) in registry.iter() {
            for (origin, id) in set {
                entries.push((key.clone(), *origin, id.clone()));
            }
        }
        entries
    }

    /// The actor ids registered under `key`, in deterministic order.
    fn snapshot(&self, key: &str) -> Vec<ActorId> {
        self.registry
            .lock()
            .expect("registry mutex poisoned")
            .get(key)
            .map(|set| set.iter().map(|(_, id)| id.clone()).collect())
            .unwrap_or_default()
    }

    /// Push the current snapshot of `key` to all its subscribers, dropping any
    /// whose receiver is gone.
    fn notify(&self, key: &str) {
        let snapshot = self.snapshot(key);
        let mut subscribers = self.subscribers.lock().expect("subscribers mutex poisoned");
        if let Some(senders) = subscribers.get_mut(key) {
            senders.retain(|tx| tx.try_send(snapshot.clone()).is_ok());
        }
    }

    /// Subscribe to `key`, delivering the current snapshot immediately (spec §13
    /// rule 4) and then on every change.
    fn subscribe(&self, key: &str) -> async_channel::Receiver<Vec<ActorId>> {
        let (tx, rx) = async_channel::unbounded();
        let _ = tx.try_send(self.snapshot(key));
        self.subscribers
            .lock()
            .expect("subscribers mutex poisoned")
            .entry(key.to_string())
            .or_default()
            .push(tx);
        rx
    }
}

/// The synthetic identity the receptionist uses when watching registered actors.
fn receptionist_id(node: NodeId) -> ActorId {
    ActorId::new(node, Path::new("/system/receptionist"), 0)
}

/// A handle to a node's receptionist (spec §13). Obtained from the system.
pub struct Receptionist<S: ActorSystem> {
    system: S,
    state: Arc<ReceptionistState>,
    node: NodeId,
}

impl<S: ActorSystem> Receptionist<S> {
    pub(crate) fn new(system: S, state: Arc<ReceptionistState>, node: NodeId) -> Receptionist<S> {
        Receptionist {
            system,
            state,
            node,
        }
    }

    /// Register `who` under `key` (spec §13), replicating to the cluster and
    /// arranging for pruning when `who` terminates.
    pub fn register<A: Actor<System = S>>(&self, key: Key<A>, who: &ActorRef<A>) {
        self.record(key.id(), self.node, who.id().clone());
        self.system
            .replicate_registration(key.id(), self.node, who.id().clone());
    }

    /// The current listing for `key` (spec §13).
    pub fn lookup<A: Actor<System = S>>(&self, key: Key<A>) -> Listing<A> {
        let refs = self
            .state
            .snapshot(key.id())
            .into_iter()
            .map(|id| self.system.resolve::<A>(id))
            .collect();
        Listing { refs }
    }

    /// Subscribe to `key` (spec §13): the stream yields the current listing
    /// first, then a fresh listing on every change.
    pub fn subscribe<A: Actor<System = S>>(
        &self,
        key: Key<A>,
    ) -> impl Stream<Item = Listing<A>> + Send + use<A, S> {
        let system = self.system.clone();
        self.state.subscribe(key.id()).map(move |ids| Listing {
            refs: ids.into_iter().map(|id| system.resolve::<A>(id)).collect(),
        })
    }

    /// Apply a registration received from a peer (spec §13): record it without
    /// re-broadcasting.
    pub fn apply_remote_registration(&self, key: &str, origin: NodeId, id: ActorId) {
        self.record(key, origin, id);
    }

    /// Record a registration locally: add it, watch the actor so it is pruned on
    /// termination (spec §12, §13), and notify subscribers.
    fn record(&self, key: &str, origin: NodeId, id: ActorId) {
        if self.state.add(key, origin, id.clone()) {
            let state = Arc::clone(&self.state);
            let deliver: WatchDelivery = Arc::new(move |signal: Terminated| {
                state.remove_actor(&signal.id);
            });
            self.system.watch(id, receptionist_id(self.node), deliver);
            self.state.notify(key);
        }
    }
}
