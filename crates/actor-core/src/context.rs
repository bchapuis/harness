//! The handler/lifecycle context (spec §3.4).
//!
//! A `Ctx<A>` grants an actor controlled capabilities — its identity, a
//! self-reference, the system handle, child spawning, and self-stop — without
//! ever exposing actor state, preserving isolation (spec §3.5).

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::actor::Actor;
use crate::actor::Handler;
use crate::actor::Terminated;
use crate::host::WatchDelivery;
use crate::id::ActorId;
use crate::refs::ActorRef;
use crate::system::ActorSystem;

/// Context passed to handlers and lifecycle hooks (spec §3.4).
pub struct Ctx<A: Actor> {
    id: ActorId,
    system: A::System,
    stopping: Arc<AtomicBool>,
}

impl<A: Actor> Ctx<A> {
    pub(crate) fn new(id: ActorId, system: A::System) -> Ctx<A> {
        Ctx {
            id,
            system,
            stopping: Arc::new(AtomicBool::new(false)),
        }
    }

    /// This actor's identity.
    pub fn id(&self) -> &ActorId {
        &self.id
    }

    /// A shareable self-reference (spec §3.4).
    pub fn this(&self) -> ActorRef<A> {
        ActorRef::from_parts(self.id.clone(), self.system.clone())
    }

    /// The system this actor runs on.
    pub fn system(&self) -> &A::System {
        &self.system
    }

    /// Spawn a child actor on the same system (spec §3.4, §11.1). The child is
    /// parented to this actor, so a fault the child escalates fails this actor,
    /// applying its supervision strategy.
    ///
    /// The child is value-spawned, so a `Restart` directive degrades to `Stop`
    /// (it cannot be reconstructed). For a restartable child, use
    /// [`spawn_with`](Self::spawn_with).
    pub fn spawn<C: Actor<System = A::System>>(&self, child: C) -> ActorRef<C> {
        self.system.spawn_child(child, self.id.clone())
    }

    /// Spawn a child actor from a `factory` (spec §3.4, §11.1), parented to this
    /// actor. Like [`spawn`](Self::spawn) but restartable: a `Restart`
    /// supervision directive re-creates the child by calling `factory` again,
    /// keeping its `ActorId` and mailbox (spec §11.2).
    pub fn spawn_with<C, F>(&self, factory: F) -> ActorRef<C>
    where
        C: Actor<System = A::System>,
        F: FnMut() -> C + Send + 'static,
    {
        self.system.spawn_child_with(factory, self.id.clone())
    }

    /// Request that this actor stop after the current message completes (spec
    /// §3.4).
    pub fn stop(&self) {
        self.stopping.store(true, Ordering::Relaxed);
    }

    /// Begin watching `target` (spec §12): when it terminates for any reason —
    /// including its node going `down` — this actor receives exactly one
    /// [`Terminated`] in its mailbox. Requires `Self: Handler<Terminated>`, the
    /// way a watcher observes the signal. Watching an already-terminated target
    /// yields `Terminated` immediately (invariant #12).
    pub fn watch<B: Actor>(&self, target: &ActorRef<B>)
    where
        A: Handler<Terminated>,
    {
        self.watch_id(target.id().clone());
    }

    /// Begin watching the actor identified by `target` (spec §12), when only
    /// its id is known — an identity carried in a message or folded from
    /// durable state rather than held as a typed ref. Identical to
    /// [`watch`](Ctx::watch) otherwise.
    pub fn watch_id(&self, target: ActorId)
    where
        A: Handler<Terminated>,
    {
        // Deliver onto this actor's own mailbox; it is local and live (we are
        // running inside one of its handlers or `started`).
        let Some(mailbox) = self.system.resolve_local::<A>(&self.id) else {
            return;
        };
        let deliver: WatchDelivery = Arc::new(move |signal| {
            let mailbox = mailbox.clone();
            Box::pin(async move { mailbox.enqueue_signal(signal).await })
        });
        self.system.watch(target, self.id.clone(), deliver);
    }

    /// Stop watching `target` (spec §12).
    pub fn unwatch<B: Actor>(&self, target: &ActorRef<B>) {
        self.system.unwatch(target.id(), &self.id);
    }

    /// Whether [`Ctx::stop`] has been requested. Read by the executor between
    /// messages.
    pub(crate) fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Relaxed)
    }
}
