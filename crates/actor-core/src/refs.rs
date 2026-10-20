//! `ActorRef`: the only handle to an actor (spec §3.2).
//!
//! The `A: Handler<M>` bound on [`ask`](ActorRef::ask) / [`tell`](ActorRef::tell)
//! is the dispatch mechanism: it proves at compile time that the target accepts
//! `M`, so invalid sends do not compile (spec §3.2, invariant #20) and no
//! runtime type check is needed.
//!
//! `ask`/`tell` are identical for local and remote targets (spec §3.2): the
//! system classifies locality on each call. A **local** target enqueues by value
//! with no serialization (spec §5); a **remote** target is encoded with the
//! system codec and routed through `remote_ask`/`remote_tell` (spec §7).

use std::time::Duration;

use crate::actor::Actor;
use crate::actor::Handler;
use crate::error::CallError;
use crate::event::Event;
use crate::id::ActorId;
use crate::mailbox::Mailbox;
use crate::message::Message;
use crate::system::ActorSystem;

/// The default deadline applied to `ask` when the caller gives none: every
/// request carries an effective deadline (spec §8.2).
const DEFAULT_ASK_TIMEOUT: Duration = Duration::from_secs(5);

/// A serializable, cloneable, typed handle to an actor (spec §3.2).
pub struct ActorRef<A: Actor> {
    id: ActorId,
    system: A::System,
}

// Manual `Clone`: `A` itself need not be `Clone`.
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

// An `ActorRef` travels as just its `ActorId` (spec §7): the system handle is
// not serializable and is rebound on the receiving node.
impl<A: Actor> serde::Serialize for ActorRef<A> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&self.id, serializer)
    }
}

// Deserializing rebinds the id to the *current decoding system* (set by the
// runtime around a message decode, spec §7, invariant #10).
impl<'de, A: Actor> serde::Deserialize<'de> for ActorRef<A> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let id = <ActorId as serde::Deserialize>::deserialize(deserializer)?;
        match crate::rebind::current_decoding_system::<A::System>() {
            Some(system) => Ok(ActorRef::from_parts(id, system)),
            None => Err(serde::de::Error::custom(
                "an ActorRef can only be deserialized while a decoding system is in scope (spec §7)",
            )),
        }
    }
}

/// Where a send to an [`ActorRef`] is delivered, classified once per call from
/// the id alone (spec §5).
enum Target<A: Actor> {
    Local(Mailbox<A>),
    /// The id is local but no live actor owns it: it has resigned.
    DeadLetter,
    Remote,
}

impl<A: Actor> ActorRef<A> {
    pub(crate) fn from_parts(id: ActorId, system: A::System) -> ActorRef<A> {
        ActorRef { id, system }
    }

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
    /// §7).
    pub fn system(&self) -> &A::System {
        &self.system
    }

    /// Request/response (spec §3.2), with the framework's fixed default deadline
    /// (no system configures it; use [`ask_timeout`](Self::ask_timeout) for
    /// another bound).
    pub async fn ask<M>(&self, msg: M) -> Result<M::Reply, CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        self.ask_timeout(msg, DEFAULT_ASK_TIMEOUT).await
    }

    /// Request/response with an explicit deadline overriding the default (spec
    /// §3.2).
    pub async fn ask_timeout<M>(&self, msg: M, within: Duration) -> Result<M::Reply, CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        // Every ask, local, remote, or dead-lettered, passes here, so this is the
        // one place that brackets it: `AskIssued` before routing and exactly one
        // `AskOutcome` once it resolves, which is what lets a checker prove no
        // ask is silently lost or left pending at quiescence (invariant #1).
        let caller = self.system.node();
        self.system.emit(Event::AskIssued {
            actor: self.id.clone(),
            caller,
            manifest: M::MANIFEST,
        });
        let result = self.route_ask(msg, within).await;
        self.system.emit(Event::AskOutcome {
            actor: self.id.clone(),
            caller,
            manifest: M::MANIFEST,
            failed: result.is_err(),
        });
        result
    }

    /// Route one ask to its target and await the typed reply, inside the
    /// bracket [`ask_timeout`](Self::ask_timeout) emits.
    async fn route_ask<M>(&self, msg: M, within: Duration) -> Result<M::Reply, CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        // Both paths carry the deadline (spec §8.2): the local one on the
        // system clock, the remote one inside `remote_ask`.
        match self.locate() {
            Target::Local(mailbox) => mailbox.ask(msg, self.system.clock(), within).await,
            Target::DeadLetter => Err(CallError::DeadLetter),
            Target::Remote => {
                let codec = self.system.codec();
                let payload = actor_serialization::encode(&*codec, &msg)?;
                let bytes = self
                    .system
                    .remote_ask(&self.id, M::MANIFEST, payload, within)
                    .await?;
                // Decode under this system so an `ActorRef` in the reply rebinds
                // here (spec §7).
                crate::rebind::with_decoding_system(&self.system, || {
                    actor_serialization::decode::<M::Reply>(&*codec, &bytes)
                        .map_err(CallError::from)
                })
            }
        }
    }

    /// Fire-and-forget (spec §3.2). Errors only for enqueue/transport failure,
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
                let payload = actor_serialization::encode(&*codec, &msg)?;
                self.system
                    .remote_tell(&self.id, M::MANIFEST, payload)
                    .await
            }
        }
    }

    /// Non-blocking local fire-and-forget: returns [`CallError::MailboxFull`]
    /// rather than awaiting when the mailbox is full (spec §6). Local-only: a
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
