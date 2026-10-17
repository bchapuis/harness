//! The `Settled` command (workflow spec §4.1): an effect's outcome, back on the
//! entity's serial command path.
//!
//! An effect's outcome returns to the activation as an ordinary command,
//! delivered through the entity's mailbox like any other and therefore ordered
//! against every other command by the input gate (entity §5). The engine holds
//! the manifest and the handler body ([`Engine::settled`](crate::Engine::settled));
//! the consumer sees only [`Workflow::settle`].
//!
//! There is deliberately **no reply**. `Settled` is delivered as a `tell` (core
//! §2.2), because there is no caller to answer: the effect's result is durable
//! when its records commit, and anything waiting on it is waiting on the record,
//! not on the message (§2.7).
//!
//! Like the engine's [`Tick`](crate::Tick), this is a **local-only** self-tell.
//! A consumer MUST NOT `accept` it in its `EntityRegistry`: the registry is the
//! deserialization allowlist for *network* commands (entity §4.5), and nothing
//! outside the entity has any business injecting an effect outcome.

use actor_core::Manifest;
use actor_core::Message;
use serde::Deserialize;
use serde::Serialize;

use crate::EffectError;
use crate::Workflow;

/// One effect's outcome, self-`tell`ed back onto the command path (spec §4.1).
///
/// Generic over the workflow entity so `key` and `outcome` are the consumer's own
/// types; the `serde` bound is empty because both associated types already carry
/// [`SerializationRequirement`](actor_serialization::SerializationRequirement)
/// and `D` itself is never serialized.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Settled<D: Workflow> {
    pub key: D::Key,
    pub outcome: Result<D::Outcome, EffectError>,
}

// Hand-written rather than `#[derive(Clone)]`: deriving would impose a spurious
// `D: Clone` bound on the entity behavior, which a message never needs. `Clone` is
// required because `EntityRef::tell` takes it (entity §2.2), not by `Message`.
impl<D: Workflow> Clone for Settled<D> {
    fn clone(&self) -> Self {
        Settled {
            key: self.key.clone(),
            outcome: self.outcome.clone(),
        }
    }
}

impl<D: Workflow> std::fmt::Debug for Settled<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settled")
            .field("key", &self.key)
            .field("ok", &self.outcome.is_ok())
            .finish()
    }
}

impl<D: Workflow> Message for Settled<D> {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("workflow.Settled");
}
