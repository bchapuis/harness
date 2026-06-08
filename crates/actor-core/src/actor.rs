//! The actor and handler traits, and termination reasons (spec §3.1, §3.2, §12).

use std::future::Future;

use serde::Deserialize;
use serde::Serialize;

use crate::context::Ctx;
use crate::id::ActorId;
use crate::message::Manifest;
use crate::message::Message;
use crate::registry::HandlerRegistry;
use crate::supervision::Supervision;
use crate::system::ActorSystem;

/// A boxed error returned by [`Actor::started`].
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Why an actor stopped, as observed by its own [`Actor::stopped`] hook (spec
/// §12). An actor runs `stopped` only on its own node, so it never sees
/// `NodeDown`; that case is observed by *watchers* as
/// [`TerminationReason::NodeDown`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    /// Graceful stop (e.g. via [`Ctx::stop`]).
    Stopped,
    /// A fault: a handler panicked, or `started` failed.
    Failed,
}

/// Why a *watched* actor terminated, as delivered to watchers (spec §12).
/// Extends [`StopReason`] with the `NodeDown` case, where the actor's node died
/// and no local `stopped` could run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TerminationReason {
    /// Graceful stop.
    Stopped,
    /// Fault or panic.
    Failed,
    /// The actor's node was declared down.
    NodeDown,
}

impl From<StopReason> for TerminationReason {
    fn from(reason: StopReason) -> Self {
        match reason {
            StopReason::Stopped => TerminationReason::Stopped,
            StopReason::Failed => TerminationReason::Failed,
        }
    }
}

/// The death-watch signal (spec §12): delivered into a watcher's mailbox when a
/// watched actor terminates. A watcher observes it by implementing
/// `Handler<Terminated>`; it arrives in the watcher's serial order like any
/// other message (invariant #13).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Terminated {
    pub id: ActorId,
    pub reason: TerminationReason,
}

impl Message for Terminated {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("actor.Terminated");
}

/// A unit of state plus behavior that processes messages one at a time (spec
/// §3.1). An ordinary Rust struct; its fields are private state, reachable only
/// from its own handlers.
///
/// The lifecycle hooks use `-> impl Future + Send` rather than `async fn` so
/// that generic runtime code can rely on their futures being `Send`.
pub trait Actor: Sized + Send + 'static {
    /// The system this actor runs on.
    type System: ActorSystem;

    /// Called once after spawn, before the first message. Returning `Err` aborts
    /// startup, and the actor is stopped with [`StopReason::Failed`].
    fn started(&mut self, _ctx: &Ctx<Self>) -> impl Future<Output = Result<(), BoxError>> + Send {
        async { Ok(()) }
    }

    /// Called once when the actor stops, for any reason, taking ownership of the
    /// actor value.
    fn stopped(self, _reason: StopReason) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// List the messages this actor accepts over the network (spec §4.4).
    ///
    /// Each `r.accept::<M>()` captures the dispatch entry for `(Self, M)`. The
    /// default registers nothing — a purely local actor. An actor addressable
    /// remotely overrides this (or an optional derive generates it); the
    /// registry it fills is the deserialization allowlist (spec §5, §15).
    fn register(_registry: &mut HandlerRegistry<Self>) {}

    /// This actor's supervision strategy (spec §11.2). The default is `Stop`.
    /// `Restart` is only honored for actors spawned with a factory
    /// (`ActorSystem::spawn_with` for a root, `Ctx::spawn_with` for a child); a
    /// value-spawned actor (`spawn`/`Ctx::spawn`) cannot be re-created, so a
    /// restart directive degrades to `Stop`.
    fn supervision() -> Supervision {
        Supervision::stop()
    }
}

/// An actor's implementation for one message type (spec §3.2).
///
/// `handle` takes `&mut self`; exclusive mutation is sound because the executor
/// is serial (spec §6). An actor accepts exactly the set of `M` for which it
/// implements `Handler<M>` — anything else is a compile error at the call site.
pub trait Handler<M: Message>: Actor {
    /// Process one message and produce its reply.
    fn handle(&mut self, msg: M, ctx: &Ctx<Self>) -> impl Future<Output = M::Reply> + Send;
}
