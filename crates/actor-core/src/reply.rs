//! Reply handling (spec §4.5).
//!
//! A [`ReplyHandle`] carries the correlation and return channel for one inbound
//! `ask`. `deliver` MUST resolve exactly one of [`send`](ReplyHandle::send),
//! [`fail`](ReplyHandle::fail), or [`none`](ReplyHandle::none). Because
//! application errors live inside `M::Reply` (spec §3.2), `send` carries both
//! successful and application-failed outcomes; `fail` is reserved for transport
//! or system failures the handler never produced.

use std::sync::Arc;

use actor_serialization::Codec;
use actor_serialization::SerializationRequirement;
use futures::channel::oneshot;

use crate::error::CallError;

/// The encoded outcome routed back to a remote caller: serialized reply bytes,
/// or a transport/system [`CallError`].
pub type ReplyResult = Result<Vec<u8>, CallError>;

/// The reply side of one inbound request (spec §4.5). Serializes the handler's
/// reply with the system codec and routes it back to the caller.
pub struct ReplyHandle {
    codec: Arc<dyn Codec>,
    tx: Option<oneshot::Sender<ReplyResult>>,
}

impl ReplyHandle {
    /// Create a handle and the receiver that observes its single outcome. The
    /// cluster receive loop forwards the received bytes over the transport; a
    /// loopback test can await the receiver directly.
    pub fn channel(codec: Arc<dyn Codec>) -> (ReplyHandle, oneshot::Receiver<ReplyResult>) {
        let (tx, rx) = oneshot::channel();
        (
            ReplyHandle {
                codec,
                tx: Some(tx),
            },
            rx,
        )
    }

    /// Serialize `reply` and return it to the caller (spec §4.5). Carries both
    /// successful and application-failed outcomes (the latter as values inside
    /// `R`). Non-blocking: the return channel never applies backpressure.
    pub fn send<R: SerializationRequirement>(mut self, reply: R) {
        if let Some(tx) = self.tx.take() {
            let encoded = actor_serialization::encode(&*self.codec, &reply)
                .map_err(|e| CallError::Serialization(e.to_string()));
            let _ = tx.send(encoded);
        }
    }

    /// Fail the call with a transport/system error (spec §4.5). Distinct from an
    /// application error, which travels inside the reply via [`send`].
    ///
    /// [`send`]: ReplyHandle::send
    pub fn fail(mut self, failure: CallError) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Err(failure));
        }
    }

    /// Resolve with no reply, for a one-way `tell` (spec §4.5).
    pub fn none(mut self) {
        let _ = self.tx.take();
    }
}
