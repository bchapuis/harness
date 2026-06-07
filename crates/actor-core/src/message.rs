//! Messages and their wire identity (spec §3.2, §4.4).

use actor_serialization::SerializationRequirement;

/// The stable wire identity of a message type, and its dispatch key (spec §4.4).
///
/// Author-controlled and stable across recompiles and renames. An explicit
/// string such as `"myapp.Greet"` is RECOMMENDED; a breaking shape change should
/// become a new message type with a new manifest, not a silent redefinition.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Manifest(&'static str);

impl Manifest {
    /// Construct a manifest from its stable string identity.
    pub const fn new(id: &'static str) -> Manifest {
        Manifest(id)
    }

    /// The manifest's string form, used as the dispatch and event key.
    pub const fn as_str(&self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for Manifest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// A serializable value sent to an actor (spec §3.2).
///
/// Each message declares its [`Reply`](Message::Reply) type and its stable
/// [`MANIFEST`](Message::MANIFEST). Both the message and its reply must satisfy
/// [`SerializationRequirement`] — an ordinary trait bound checked at compile
/// time, so an unserializable message is a compile error, not a runtime fault.
///
/// A handler that can fail uses `type Reply = Result<T, E>`: an application
/// failure is a value distinct from a transport failure ([`CallError`], spec
/// §3.2 rule 4).
///
/// [`CallError`]: crate::CallError
pub trait Message: SerializationRequirement {
    /// The reply this message elicits.
    type Reply: SerializationRequirement;

    /// Stable, author-controlled wire identity and dispatch key (spec §4.4).
    const MANIFEST: Manifest;
}
