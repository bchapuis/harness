//! Serialization layer for the distributed actor framework (spec §5).
//!
//! Defines the bound every wire-crossing value must satisfy
//! ([`SerializationRequirement`]) and the pluggable, object-safe wire
//! [`Codec`]. The dispatch registry that maps a manifest to a
//! decode-and-dispatch entry lives in `actor-core` (it must reference the actor
//! and mailbox types), and is the deserialization allowlist (spec §4.4, §15).

mod codec;

pub use codec::Codec;
pub use codec::CodecError;
pub use codec::JsonCodec;
pub use codec::decode;
pub use codec::encode;

use serde::Serialize;
use serde::de::DeserializeOwned;

/// The bound every message and reply type must satisfy to cross the wire
/// (spec §5).
///
/// It is a parameter of the system, enforced at compile time by the
/// [`Message`](../actor_core/trait.Message.html) and `Handler` bounds. The
/// blanket impl means any type that is `Serialize + DeserializeOwned + Send +
/// 'static` qualifies automatically; user code never implements this by hand.
pub trait SerializationRequirement: Serialize + DeserializeOwned + Send + 'static {}

impl<T: Serialize + DeserializeOwned + Send + 'static> SerializationRequirement for T {}
