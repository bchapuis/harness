//! The wire codec (spec §5).
//!
//! [`Codec`] is the pluggable, object-safe serializer fixed per system. Both ends
//! of an association MUST agree on it (spec §5 rule 2). Object safety matters:
//! the system holds a single `Arc<dyn Codec>` and the dispatch registry stores
//! plain `fn` pointers that decode a concrete message given `&dyn Codec`, so the
//! codec type never leaks into `HandlerRegistry<A>`, `ActorRef`, or `Ctx`.
//!
//! Object safety with serde's generic `Serialize`/`Deserialize` is provided by
//! [`erased_serde`]. The free functions [`encode`] and [`decode`] are the typed
//! entry points used by the `ActorRef` layer and the dispatch entries.

use serde::Serialize;
use serde::de::DeserializeOwned;

/// A (de)serialization failure (spec §14, surfaced as `CallError::Serialization`).
#[derive(Clone, Debug)]
pub struct CodecError(pub String);

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CodecError {}

/// An object-safe, pluggable wire codec (spec §5).
///
/// Implementors serialize a type-erased value and expose a type-erased
/// deserializer over a byte slice; the generic [`encode`]/[`decode`] helpers
/// build the typed bridge on top.
pub trait Codec: Send + Sync + 'static {
    /// A short identifier for this codec, exchanged in the handshake so both
    /// ends can confirm they agree (spec §5, §7.1).
    fn name(&self) -> &'static str;

    /// Serialize a type-erased value to bytes.
    fn encode_erased(&self, value: &dyn erased_serde::Serialize) -> Result<Vec<u8>, CodecError>;

    /// Build a type-erased deserializer over `bytes` and hand it to `f`. The
    /// callback form keeps the borrowed deserializer's lifetime local while
    /// staying object-safe.
    fn with_deserializer(
        &self,
        bytes: &[u8],
        f: &mut dyn FnMut(&mut dyn erased_serde::Deserializer),
    );
}

/// Encode a typed value with `codec`.
pub fn encode<T: Serialize>(codec: &dyn Codec, value: &T) -> Result<Vec<u8>, CodecError> {
    codec.encode_erased(value)
}

/// Decode a typed value with `codec`.
pub fn decode<T: DeserializeOwned>(codec: &dyn Codec, bytes: &[u8]) -> Result<T, CodecError> {
    let mut out: Option<Result<T, CodecError>> = None;
    codec.with_deserializer(bytes, &mut |de| {
        out = Some(erased_serde::deserialize::<T>(de).map_err(|e| CodecError(e.to_string())));
    });
    out.expect("with_deserializer must invoke the callback")
}

/// A JSON codec (human-readable, convenient for tests and debugging). The wire
/// format is real serde, so every cross-node hop exercises true encoding
/// (spec §18.2).
#[derive(Clone, Copy, Default)]
pub struct JsonCodec;

impl Codec for JsonCodec {
    fn name(&self) -> &'static str {
        "json"
    }

    fn encode_erased(&self, value: &dyn erased_serde::Serialize) -> Result<Vec<u8>, CodecError> {
        let mut buf = Vec::new();
        let mut serializer = serde_json::Serializer::new(&mut buf);
        let mut erased = <dyn erased_serde::Serializer>::erase(&mut serializer);
        value
            .erased_serialize(&mut erased)
            .map_err(|e| CodecError(e.to_string()))?;
        Ok(buf)
    }

    fn with_deserializer(
        &self,
        bytes: &[u8],
        f: &mut dyn FnMut(&mut dyn erased_serde::Deserializer),
    ) {
        let mut de = serde_json::Deserializer::from_slice(bytes);
        let mut erased = <dyn erased_serde::Deserializer>::erase(&mut de);
        f(&mut erased);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Sample {
        name: String,
        count: u32,
    }

    #[test]
    fn json_round_trips_a_value() {
        let codec = JsonCodec;
        let value = Sample {
            name: "greeter".into(),
            count: 3,
        };
        let bytes = encode(&codec, &value).unwrap();
        let back: Sample = decode(&codec, &bytes).unwrap();
        assert_eq!(value, back);
    }

    #[test]
    fn decode_reports_malformed_input() {
        let codec = JsonCodec;
        let err = decode::<Sample>(&codec, b"not json");
        assert!(err.is_err());
    }
}
