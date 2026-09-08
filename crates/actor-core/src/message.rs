//! Messages and their wire identity (spec §3.2, §4.4).

use actor_serialization::SerializationRequirement;

/// The stable wire identity of a message type, and its dispatch key (spec §4.4).
///
/// Author-controlled and stable across recompiles and renames. An explicit
/// string such as `"myapp.Greet"` is RECOMMENDED; a breaking shape change should
/// become a new message type with a new manifest, not a silent redefinition.
///
/// A `Manifest` value exists only for statically **declared** identities; text
/// arriving off the wire stays `&str` until
/// [`HandlerRegistry::dispatch`](crate::HandlerRegistry::dispatch) matches it
/// against a declared one: the allowlist is the point where a claimed manifest
/// becomes a recognized identity (spec §5, §15).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Manifest(&'static str);

impl Manifest {
    /// Construct a manifest from its stable string identity.
    pub const fn new(id: &'static str) -> Manifest {
        Manifest(id)
    }

    /// The manifest's string form: what a transport frame carries on the wire.
    pub const fn as_str(&self) -> &'static str {
        self.0
    }
}

/// Lets a registry keyed by `Manifest` answer a lookup for an untrusted wire
/// `&str` without allocating. Sound because `Eq`/`Ord`/`Hash` all delegate to
/// the string, as [`Borrow`](std::borrow::Borrow) requires.
impl std::borrow::Borrow<str> for Manifest {
    fn borrow(&self) -> &str {
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
/// [`SerializationRequirement`]: an ordinary trait bound checked at compile
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
