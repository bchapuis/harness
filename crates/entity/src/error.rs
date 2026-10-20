//! The entity error model (spec §3.2).
//!
//! Entity calls surface two failure layers, kept distinct as the actor framework
//! keeps [`CallError`] and `M::Reply` apart (actor §8): an *application*
//! failure the handler deliberately produced lives **inside** `M::Reply` (e.g.
//! `Result<T, E>`, §4.2), never here; [`EntityError`] carries only the transport
//! and durability failures of *reaching and committing* a command.
//!
//! The variants are exhaustive (no `#[non_exhaustive]`) by design (invariant
//! **E5** support): a caller must handle every real partial failure explicitly.

use actor_core::CallError;
use actor_core::NodeId;
use serde::Deserialize;
use serde::Serialize;

/// A failure to reach an entity's activation or to commit its effects (spec §3.2).
///
/// `EntityError` is serializable because it rides inside a command's reply on the
/// wire (the host's `Reply` is `Result<M::Reply, EntityError>`, §6).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityError {
    /// A transport or system failure reaching the activation (actor §8.2).
    Call(CallError),
    /// Leadership moved off the contacted node; the runtime retries against the
    /// hint and surfaces this only once the bounded redirect is exhausted (§5.4,
    /// storage §6). Unreachable in the `Local` single-node journal.
    NotLeader(NodeId),
    /// The entity's shard could not reach a quorum, **or** the commit timed out
    /// (storage §11). CP, not AP: the caller retries or fails over rather than forking.
    ///
    /// The outcome is **ambiguous**: a replica minority may already hold the
    /// record, and a later activation's quorum recovery can find it on a
    /// majority and adopt it, so a command that reported `Unavailable` MAY
    /// still become visible, at its own `Seq` slot, some time afterwards
    /// (storage §5, §3.1, §11). That is why the runtime never auto-retries it (§5.4)
    /// and why writes that must not double-apply carry the caller's own
    /// idempotence (§1.1). Contrast [`NotLeader`], which the spec does pin as
    /// never committing.
    ///
    /// On the `Local` single-node journal it arises only from a failed store (storage §11).
    ///
    /// [`NotLeader`]: EntityError::NotLeader
    Unavailable(String),
}

impl From<CallError> for EntityError {
    fn from(err: CallError) -> Self {
        EntityError::Call(err)
    }
}

impl std::fmt::Display for EntityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntityError::Call(e) => write!(f, "entity call failed: {e}"),
            EntityError::NotLeader(node) => write!(f, "entity shard leadership moved to {node}"),
            EntityError::Unavailable(why) => write!(f, "entity shard unavailable: {why}"),
        }
    }
}

impl std::error::Error for EntityError {}
