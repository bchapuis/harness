//! Reply handling (spec §2.10).
//!
//! A [`ReplyHandle`] carries the correlation and return channel for one inbound
//! `ask`. `deliver` MUST resolve exactly one of [`send`](ReplyHandle::send) or
//! [`fail`](ReplyHandle::fail). Because application errors live inside
//! `M::Reply` (spec §2.1), `send` carries both successful and
//! application-failed outcomes; `fail` is reserved for transport or system
//! failures the handler never produced.

use std::sync::Arc;

use actor_serialization::Codec;
use actor_serialization::SerializationRequirement;
use futures::channel::oneshot;

use crate::error::CallError;

/// The encoded outcome routed back to a remote caller: serialized reply bytes,
/// or a transport/system [`CallError`].
pub type ReplyResult = Result<Vec<u8>, CallError>;

/// The reply side of one inbound request (spec §2.10). Serializes the handler's
/// reply with the system codec and routes it back to the caller.
pub struct ReplyHandle {
    codec: Arc<dyn Codec>,
    tx: oneshot::Sender<ReplyResult>,
}

impl ReplyHandle {
    /// Create a handle and the receiver that observes its single outcome. The
    /// cluster receive loop forwards the received bytes over the transport; a
    /// loopback test can await the receiver directly.
    pub fn channel(codec: Arc<dyn Codec>) -> (ReplyHandle, oneshot::Receiver<ReplyResult>) {
        let (tx, rx) = oneshot::channel();
        (ReplyHandle { codec, tx }, rx)
    }

    /// Serialize `reply` and return it to the caller (spec §2.10). Carries both
    /// successful and application-failed outcomes (the latter as values inside
    /// `R`). Non-blocking: the return channel never applies backpressure. A
    /// receiver already gone (a one-way `tell`, or an `ask` that stopped
    /// waiting) reads nothing, so the reply is not encoded at all.
    pub fn send<R: SerializationRequirement>(self, reply: R) {
        if self.tx.is_canceled() {
            return;
        }
        let encoded = actor_serialization::encode(&*self.codec, &reply).map_err(CallError::from);
        let _ = self.tx.send(encoded);
    }

    /// Fail the call with a transport/system error (spec §2.10). Distinct from an
    /// application error, which travels inside the reply via [`send`].
    ///
    /// [`send`]: ReplyHandle::send
    pub fn fail(self, failure: CallError) {
        let _ = self.tx.send(Err(failure));
    }
}
