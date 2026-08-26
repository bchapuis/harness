//! The dispatch registry (spec §4.4).
//!
//! An actor lists the messages it accepts over the network in
//! [`Actor::register`](crate::Actor::register). Each `r.accept::<M>()` captures
//! the monomorphized **dispatch entry** for `(Self, M)`: a plain `fn` pointer
//! that, given the system codec and a payload, decodes `M` and enqueues
//! `Handler::<M>::handle` on the resolved actor's mailbox, routing the reply
//! through a [`ReplyHandle`].
//!
//! The registry maps `manifest → dispatch entry` and is the **deserialization
//! allowlist** (spec §5, §15): only listed message types are ever built from
//! network bytes. A purely local actor overrides nothing — its default
//! registration is empty and its messages flow by value (spec §4.3).

use std::collections::BTreeMap;

use actor_serialization::Codec;

use crate::actor::Actor;
use crate::actor::Handler;
use crate::error::CallError;
use crate::mailbox::Mailbox;
use crate::message::Manifest;
use crate::message::Message;
use crate::reply::ReplyHandle;

/// A monomorphized dispatch entry for one `(actor type, message type)`: decode a
/// payload and enqueue the handler on `mailbox`, routing the reply through
/// `reply`. A plain `fn` pointer — no codegen, no captured state (spec §4.4).
pub type DispatchFn<A> = fn(
    codec: &dyn Codec,
    payload: &[u8],
    reply: ReplyHandle,
    mailbox: &Mailbox<A>,
) -> Result<(), CallError>;

/// The dispatch entry for `(A, M)`. Decodes `M` from `payload` with `codec`
/// (rejecting bad input as `CallError::Serialization`), then enqueues it.
fn dispatch_entry<A, M>(
    codec: &dyn Codec,
    payload: &[u8],
    reply: ReplyHandle,
    mailbox: &Mailbox<A>,
) -> Result<(), CallError>
where
    A: Handler<M>,
    M: Message,
{
    let msg: M = actor_serialization::decode(codec, payload)?;
    mailbox.enqueue_remote::<M>(msg, reply)
}

/// Maps a manifest to its dispatch entry for one actor type (spec §4.4). The
/// typed builder behind [`Actor::register`](crate::Actor::register); also the
/// deserialization allowlist.
pub struct HandlerRegistry<A: Actor> {
    entries: BTreeMap<Manifest, DispatchFn<A>>,
}

impl<A: Actor> Default for HandlerRegistry<A> {
    fn default() -> Self {
        HandlerRegistry {
            entries: BTreeMap::new(),
        }
    }
}

// Manual `Clone`: `A` itself need not be `Clone` (the entries are `fn` pointers).
impl<A: Actor> Clone for HandlerRegistry<A> {
    fn clone(&self) -> Self {
        HandlerRegistry {
            entries: self.entries.clone(),
        }
    }
}

impl<A: Actor> HandlerRegistry<A> {
    /// Accept message type `M` over the network (spec §4.4), capturing the
    /// monomorphized dispatch entry for `(A, M)`.
    pub fn accept<M>(&mut self)
    where
        A: Handler<M>,
        M: Message,
    {
        self.entries.insert(M::MANIFEST, dispatch_entry::<A, M>);
    }

    /// The dispatch entry for a *claimed* manifest, or `None` if `(A, manifest)`
    /// is not registered — the receive loop turns `None` into
    /// `CallError::Unhandled`. Takes `&str`, not [`Manifest`]: the claim is
    /// untrusted wire text, and this lookup is where it either matches a
    /// declared identity or is refused (spec §5, §15).
    pub fn dispatch(&self, manifest: &str) -> Option<DispatchFn<A>> {
        self.entries.get(manifest).copied()
    }
}
