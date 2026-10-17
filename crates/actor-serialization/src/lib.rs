//! Serialization layer for the distributed actor framework: the bound every
//! wire-crossing value satisfies and the pluggable wire codec.
//!
//! # Seams
//!
//! - [`SerializationRequirement`]: the compile-time bound on every message and
//!   reply type. Blanket-implemented for any `Serialize + DeserializeOwned +
//!   Send + 'static` type, so user code never implements it by hand.
//! - [`Codec`]: the object-safe serializer a system holds as `Arc<dyn Codec>`,
//!   a plug point a deployment chooses once for the whole cluster. Its
//!   [`Codec::name`] is what the transport handshake compares, so both ends of
//!   an association agree. [`JsonCodec`] (human-readable, for tests and
//!   debugging) and [`PostcardCodec`] (compact, for bulk bytes) are the two
//!   implementations here.
//!
//! # Mechanism
//!
//! Object safety over serde's generic traits comes from `erased_serde`: a codec
//! serializes a type-erased value and lends a type-erased deserializer over a
//! byte slice, and the typed [`encode`] and [`decode`] functions bridge back to
//! concrete types. Because the codec is object-safe, the dispatch registry in
//! `actor-core` stores plain `fn` pointers that decode a concrete message
//! given `&dyn Codec`, and the codec type never appears in `ActorRef`, `Ctx`,
//! or the registry's type. That registry, not this crate, is the
//! deserialization allowlist: it decides which types are built from network
//! bytes (spec §2.10, §9).
//!
//! Specified by `docs/distributed-actor-spec.md`.

mod codec;

pub use codec::Codec;
pub use codec::CodecError;
pub use codec::JsonCodec;
pub use codec::PostcardCodec;
pub use codec::decode;
pub use codec::encode;

use serde::Serialize;
use serde::de::DeserializeOwned;

/// The bound every message and reply type must satisfy to cross the wire
/// (spec §2.11).
///
/// It is a parameter of the system, enforced at compile time by the
/// [`Message`](../actor_core/trait.Message.html) and `Handler` bounds. The
/// blanket impl means any type that is `Serialize + DeserializeOwned + Send +
/// 'static` qualifies automatically; user code never implements this by hand.
pub trait SerializationRequirement: Serialize + DeserializeOwned + Send + 'static {}

impl<T: Serialize + DeserializeOwned + Send + 'static> SerializationRequirement for T {}
