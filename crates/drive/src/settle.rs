//! The `Settled` command (drive spec §4.2): an effect's outcome, back on the
//! grain's serial command path.
//!
//! An effect's outcome returns to the activation as an ordinary command,
//! delivered through the grain's mailbox like any other and therefore ordered
//! against every other command by the input gate (granary §6). The drive holds
//! the manifest and the handler body ([`Drive::settled`](crate::Drive::settled));
//! the consumer sees only [`Driven::settle`].
//!
//! There is deliberately **no reply**. `Settled` is delivered as a `tell` (core
//! §3.3), because there is no caller to answer: the effect's result is durable
//! when its records commit, and anything waiting on it is waiting on the record,
//! not on the message (§7.1).
//!
//! Like the drive's [`Nudge`](crate::Nudge), this is a **local-only** self-tell.
//! A consumer MUST NOT `accept` it in its `GrainRegistry`: the registry is the
//! deserialization allowlist for *network* commands (granary §5.5), and nothing
//! outside the grain has any business injecting an effect outcome.

use actor_core::Manifest;
use actor_core::Message;
use serde::Deserialize;
use serde::Serialize;

use crate::Driven;
use crate::EffectError;

/// One effect's outcome, self-`tell`ed back onto the command path (spec §4.2).
///
/// Generic over the driven grain so `key` and `outcome` are the consumer's own
/// types; the `serde` bound is empty because both associated types already carry
/// [`SerializationRequirement`](actor_serialization::SerializationRequirement)
/// and `D` itself is never serialized.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Settled<D: Driven> {
    pub key: D::Key,
    pub outcome: Result<D::Outcome, EffectError>,
}

// Hand-written rather than `#[derive(Clone)]`: deriving would impose a spurious
// `D: Clone` bound on the grain behavior, which a message never needs. `Clone` is
// required because `GrainRef::tell` takes it (granary §4.3), not by `Message`.
impl<D: Driven> Clone for Settled<D> {
    fn clone(&self) -> Self {
        Settled {
            key: self.key.clone(),
            outcome: self.outcome.clone(),
        }
    }
}

impl<D: Driven> std::fmt::Debug for Settled<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settled")
            .field("key", &self.key)
            .field("ok", &self.outcome.is_ok())
            .finish()
    }
}

impl<D: Driven> Message for Settled<D> {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("drive.Settled");
}
