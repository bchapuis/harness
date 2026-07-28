//! The grain error model (spec §12).
//!
//! Grain calls surface two failure layers, kept distinct as the actor framework
//! keeps [`CallError`] and `M::Reply` apart (actor §14): an *application*
//! failure the handler deliberately produced lives **inside** `M::Reply` (e.g.
//! `Result<T, E>`, §4.2), never here; [`GrainError`] carries only the transport
//! and durability failures of *reaching and committing* a command.
//!
//! The variants are exhaustive (no `#[non_exhaustive]`) by design (invariant
//! **G5** support): a caller must handle every real partial failure explicitly.

use actor_core::CallError;
use actor_core::NodeId;
use serde::Deserialize;
use serde::Serialize;

/// A failure to reach a grain's activation or to commit its effects (spec §12).
///
/// `GrainError` is serializable because it rides inside a command's reply on the
/// wire (the host's `Reply` is `Result<M::Reply, GrainError>`, §6).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrainError {
    /// A transport or system failure reaching the activation (actor §14.1).
    Call(CallError),
    /// Leadership moved off the contacted node; the runtime retries against the
    /// hint and surfaces this only once the bounded redirect is exhausted (§5.4,
    /// §8). Unreachable in the `Local` single-node journal, kept for API stability.
    NotLeader(NodeId),
    /// The grain's shard could not reach a quorum, **or** the commit timed out
    /// (§11). CP, not AP: the caller retries or fails over rather than forking.
    ///
    /// The outcome is **ambiguous**, and callers must treat it that way: a
    /// replica minority may already hold the record, and a later activation's
    /// quorum recovery can find it on a majority and adopt it — so a command
    /// that reported `Unavailable` MAY still become visible, at its own `Seq`
    /// slot, some time afterwards (§7.2, §7.3, §11). That is why the runtime
    /// never auto-retries it (§5.4) and why writes that must not double-apply
    /// carry the caller's own idempotence (§2.2). Contrast [`NotLeader`], which
    /// the spec does pin as never committing.
    ///
    /// Unreachable in the `Local` single-node journal, kept for API stability.
    ///
    /// [`NotLeader`]: GrainError::NotLeader
    Unavailable(String),
}

impl From<CallError> for GrainError {
    fn from(err: CallError) -> Self {
        GrainError::Call(err)
    }
}

impl std::fmt::Display for GrainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GrainError::Call(e) => write!(f, "grain call failed: {e}"),
            GrainError::NotLeader(node) => write!(f, "grain shard leadership moved to {node}"),
            GrainError::Unavailable(why) => write!(f, "grain shard unavailable: {why}"),
        }
    }
}

impl std::error::Error for GrainError {}
