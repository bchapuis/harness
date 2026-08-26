//! The error model (spec §14).
//!
//! [`CallError`] covers **transport and system** failures only — the failure to
//! *complete* a call. Application failures a handler deliberately produces live
//! inside `M::Reply` (spec §3.2), never here. The variants are exhaustive (no
//! `#[non_exhaustive]`): callers MUST handle every kind of partial failure
//! explicitly at each cross-actor boundary (spec §14.1).

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

/// How a codec failure surfaces on a call (spec §14.1): its message, under
/// `Serialization`. The one definition every encode/decode site leans on
/// through `?`, so the mapping cannot drift between them.
impl From<actor_serialization::CodecError> for CallError {
    fn from(e: actor_serialization::CodecError) -> Self {
        CallError::Serialization(e.to_string())
    }
}

/// Await **every** future, then report the first error — the non-abandoning
/// counterpart to `futures::future::try_join_all`.
///
/// `try_join_all` returns the moment one future fails and **drops** the rest.
/// That is fine for pure computation and wrong for anything that has issued an
/// [`ask`](crate::ActorRef::ask): dropping the future abandons the call, which then
/// never reaches a reply or a definite [`CallError`] — exactly what no-silent-
/// loss forbids (spec §7.2, §18.5 #1).
///
/// ```ignore
/// // not `try_join_all`: a failing chunk would drop its siblings' asks
/// let ids = join_all_results(chunks.into_iter().map(|c| store.put(c))).await?;
/// ```
pub async fn join_all_results<F, T, E>(futures: impl IntoIterator<Item = F>) -> Result<Vec<T>, E>
where
    F: std::future::Future<Output = Result<T, E>>,
{
    futures::future::join_all(futures)
        .await
        .into_iter()
        .collect()
}
