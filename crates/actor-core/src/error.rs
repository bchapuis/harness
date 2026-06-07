//! The error model (spec §14).
//!
//! [`CallError`] covers **transport and system** failures only — the failure to
//! *complete* a call. Application failures a handler deliberately produces live
//! inside `M::Reply` (spec §3.2), never here. The variants are exhaustive (no
//! `#[non_exhaustive]`) by design: callers must handle every kind of partial
//! failure explicitly, so the type system forces failure handling at each
//! cross-actor boundary (spec §14.1).

use serde::Deserialize;
use serde::Serialize;

/// A transport- or system-level failure to complete a call (spec §14.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallError {
    /// The call's deadline was exceeded.
    Timeout,
    /// The recipient node is down or the association was lost.
    Unreachable,
    /// No live actor exists for the id.
    DeadLetter,
    /// The recipient actor has no handler / no registration for this message.
    Unhandled,
    /// The mailbox was full and the send was rejected (backpressure, spec §6).
    MailboxFull,
    /// Encoding or decoding failed.
    Serialization(String),
    /// Any other system-level failure.
    System(String),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Timeout => f.write_str("call timed out"),
            CallError::Unreachable => f.write_str("recipient unreachable"),
            CallError::DeadLetter => f.write_str("no live actor for recipient"),
            CallError::Unhandled => f.write_str("recipient has no handler for this message"),
            CallError::MailboxFull => f.write_str("recipient mailbox full"),
            CallError::Serialization(e) => write!(f, "serialization failure: {e}"),
            CallError::System(e) => write!(f, "system failure: {e}"),
        }
    }
}

impl std::error::Error for CallError {}
