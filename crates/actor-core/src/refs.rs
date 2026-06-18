//! `ActorRef`: the only handle to an actor (spec §3.3).
//!
//! It holds exactly an [`ActorId`] plus a handle to the system, and carries
//! **no** actor state. The `A: Handler<M>` bound on [`ask`](ActorRef::ask) /
//! [`tell`](ActorRef::tell) is the dispatch mechanism: it proves at compile time
//! that the target accepts `M`, so invalid sends do not compile (spec §3.3,
//! invariant #20) and no runtime type check is needed.
//!
//! `ask`/`tell` are identical for local and remote targets (spec §3.3): the
//! system classifies locality on each call. A **local** target enqueues by value
//! with no serialization (spec §4.3); a **remote** target is encoded with the
//! system codec and routed through `remote_ask`/`remote_tell` (spec §4.4).

use std::time::Duration;

use crate::actor::Actor;
use crate::actor::Handler;
use crate::error::CallError;
use crate::id::ActorId;
use crate::mailbox::Mailbox;
use crate::message::Message;
use crate::system::ActorSystem;

/// The default deadline applied to `ask` when the caller gives none — every
/// request carries an effective deadline (spec §14.2).
const DEFAULT_ASK_TIMEOUT: Duration = Duration::from_secs(5);

/// A serializable, cloneable, typed handle to an actor (spec §3.3).
pub struct ActorRef<A: Actor> {
    id: ActorId,
    system: A::System,
}

// Manual `Clone`: `A` itself need not be `Clone`; only the id and system handle
// (a cheap `Arc` clone) are cloned.
impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        ActorRef {
            id: self.id.clone(),
            system: self.system.clone(),
        }
    }
}

// Equality and hashing derive purely from the `ActorId` (spec §3.1).
impl<A: Actor> PartialEq for ActorRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl<A: Actor> Eq for ActorRef<A> {}
impl<A: Actor> std::hash::Hash for ActorRef<A> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<A: Actor> std::fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ActorRef({})", self.id)
    }
}

// An `ActorRef` travels as just its `ActorId` (spec §4.4): the system handle is
// not serializable and is rebound on the receiving node.
impl<A: Actor> serde::Serialize for ActorRef<A> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&self.id, serializer)
    }
}

// Deserializing rebinds the id to the *current decoding system* (set by the
// runtime around a message decode, spec §4.4, invariant #10), so a ref embedded
// in a message is usable on the node that receives it.
impl<'de, A: Actor> serde::Deserialize<'de> for ActorRef<A> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let id = <ActorId as serde::Deserialize>::deserialize(deserializer)?;
        match crate::rebind::current_decoding_system::<A::System>() {
            Some(system) => Ok(ActorRef::from_parts(id, system)),
            None => Err(serde::de::Error::custom(
                "an ActorRef can only be deserialized while a decoding system is in scope (spec §4.4)",
            )),
        }
    }
}

/// Where a send to an [`ActorRef`] is delivered, classified once per call from
/// the id alone (spec §4.3). Resolving locality and the live local mailbox in
/// one place keeps the local-vs-remote branch out of every send method — the
/// one secret of how a ref reaches its actor.
enum Target<A: Actor> {
    /// A live local actor: enqueue by value, no serialization (spec §4.3).
    Local(Mailbox<A>),
    /// The id is local but no live actor owns it — it has resigned (spec §4.3).
    DeadLetter,
    /// The id is owned by another node: encode and route over the transport.
    Remote,
}

impl<A: Actor> ActorRef<A> {
    pub(crate) fn from_parts(id: ActorId, system: A::System) -> ActorRef<A> {
        ActorRef { id, system }
    }

    /// Classify where a send goes and resolve the local mailbox if any, in one
    /// place (spec §4.3). Every send method dispatches on this rather than
    /// re-deriving locality.
    fn locate(&self) -> Target<A> {
        if !self.system.is_local(&self.id) {
            return Target::Remote;
        }
        match self.system.resolve_local::<A>(&self.id) {
            Some(mailbox) => Target::Local(mailbox),
            None => Target::DeadLetter,
        }
    }

    /// This actor's identity.
    pub fn id(&self) -> &ActorId {
        &self.id
    }

    /// The system this ref is bound to (the local system after decode, spec
    /// §4.4). Lets a handle carrying an `ActorRef` recover the local system
    /// without threading one separately — e.g. a deserialized `GrainRef`
    /// recovering its routing system from its gateway ref.
    pub fn system(&self) -> &A::System {
        &self.system
    }

    /// Request/response (spec §3.3), with the system default deadline. The
    /// `A: Handler<M>` bound proves at compile time that this actor accepts `M`.
    pub async fn ask<M>(&self, msg: M) -> Result<M::Reply, CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        self.ask_within(msg, DEFAULT_ASK_TIMEOUT).await
    }

    /// Request/response with an explicit deadline overriding the default (spec
    /// §3.3).
    pub async fn ask_timeout<M>(&self, msg: M, within: Duration) -> Result<M::Reply, CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        self.ask_within(msg, within).await
    }

    /// Request/response that collapses the two error levels into one (spec
    /// §14.3). When the reply is itself a `Result<T, E>`, an outer transport or
    /// system [`CallError`] is folded into the application error via
    /// `E: From<CallError>`, so the caller handles a single `Result<T, E>`
    /// instead of `Result<Result<T, E>, CallError>`. Use plain [`ask`](Self::ask)
    /// when the two levels must stay distinct.
    pub async fn ask_flat<M, T, E>(&self, msg: M) -> Result<T, E>
    where
        A: Handler<M>,
        M: Message<Reply = Result<T, E>>,
        E: From<CallError>,
    {
        match self.ask_within(msg, DEFAULT_ASK_TIMEOUT).await {
            Ok(inner) => inner,
            Err(call_err) => Err(E::from(call_err)),
        }
    }

    /// Run `f` directly against the actor's state, **only if it is local** (spec
    /// §3.5.1). Returns `Some(result)` when the target lives on this node and is
    /// alive — `f` runs on its serial executor, so isolation is preserved — and
    /// `None` when the target is remote or already gone. This is the one
    /// sanctioned exception to location transparency; it SHOULD be used only in
    /// tests and local optimizations.
    pub async fn when_local<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut A) -> R + Send + 'static,
        R: Send + 'static,
    {
        match self.locate() {
            Target::Local(mailbox) => mailbox.run_local(f).await.ok(),
            Target::DeadLetter | Target::Remote => None,
        }
    }

    async fn ask_within<M>(&self, msg: M, within: Duration) -> Result<M::Reply, CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        match self.locate() {
            // Local: enqueue by value and await the typed reply, no serialization.
            Target::Local(mailbox) => mailbox.ask(msg).await,
            Target::DeadLetter => Err(CallError::DeadLetter),
            // Remote: encode, route over the transport, decode the reply.
            Target::Remote => {
                let codec = self.system.codec();
                let payload = actor_serialization::encode(&*codec, &msg)
                    .map_err(|e| CallError::Serialization(e.to_string()))?;
                let bytes = self
                    .system
                    .remote_ask(&self.id, M::MANIFEST.as_str(), payload, within)
                    .await?;
                // Decode under this system so an `ActorRef` in the reply rebinds
                // here (spec §4.4).
                crate::rebind::with_decoding_system(&self.system, || {
                    actor_serialization::decode::<M::Reply>(&*codec, &bytes)
                        .map_err(|e| CallError::Serialization(e.to_string()))
                })
            }
        }
    }

    /// Fire-and-forget (spec §3.3). Errors only for enqueue/transport failure,
    /// never the handler outcome. Applies backpressure on a local target.
    pub async fn tell<M>(&self, msg: M) -> Result<(), CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        match self.locate() {
            Target::Local(mailbox) => mailbox.tell(msg).await,
            Target::DeadLetter => Err(CallError::DeadLetter),
            Target::Remote => {
                let codec = self.system.codec();
                let payload = actor_serialization::encode(&*codec, &msg)
                    .map_err(|e| CallError::Serialization(e.to_string()))?;
                self.system
                    .remote_tell(&self.id, M::MANIFEST.as_str(), payload)
                    .await
            }
        }
    }

    /// Non-blocking local fire-and-forget: returns [`CallError::MailboxFull`]
    /// rather than awaiting when the mailbox is full (spec §6). Local-only — a
    /// remote target yields [`CallError::Unreachable`]; use [`tell`] for remote.
    ///
    /// [`tell`]: ActorRef::tell
    pub fn try_tell<M>(&self, msg: M) -> Result<(), CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        match self.locate() {
            Target::Local(mailbox) => mailbox.try_tell(msg),
            Target::DeadLetter => Err(CallError::DeadLetter),
            Target::Remote => Err(CallError::Unreachable),
        }
    }
}
