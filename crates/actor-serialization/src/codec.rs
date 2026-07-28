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
///
/// It is the wrong default for a deployment that moves **bulk bytes**. JSON has
/// no byte-string form, so a `Vec<u8>` is written as an array of decimal
/// numbers — about 3.6 chars per byte — and an envelope's payload pays that
/// twice, once as the message payload and again as the frame's `payload:
/// Vec<u8>` field. A 1 MiB value costs ~10.7 MiB on the wire and over a second
/// of CPU per copy, which is more than a quorum write's budget. Deployments
/// carrying values of that size want [`PostcardCodec`].
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

/// A `postcard` codec: compact, binary, and the right choice for a deployment
/// whose messages carry bulk bytes (spec §5 rule 2 lists it among the formats a
/// system may fix). A `Vec<u8>` costs its own length plus a varint, so a value
/// crosses the wire once at size rather than growing with every encoding layer.
///
/// Not self-describing: `postcard` writes no field names or type tags, so it
/// decodes only against the same type definition that encoded it. A message
/// type that needs `deserialize_any` — `serde_json::Value`, `#[serde(untagged)]`
/// or `#[serde(flatten)]` — cannot travel over it, and a system carrying one
/// wants [`JsonCodec`]. Both ends of an association must agree either way; the
/// handshake compares [`Codec::name`] and refuses a mismatch (spec §7.1).
#[derive(Clone, Copy, Default)]
pub struct PostcardCodec;

impl Codec for PostcardCodec {
    fn name(&self) -> &'static str {
        "postcard"
    }

    fn encode_erased(&self, value: &dyn erased_serde::Serialize) -> Result<Vec<u8>, CodecError> {
        postcard::to_allocvec(value).map_err(|e| CodecError(e.to_string()))
    }

    fn with_deserializer(
        &self,
        bytes: &[u8],
        f: &mut dyn FnMut(&mut dyn erased_serde::Deserializer),
    ) {
        let mut de = postcard::Deserializer::from_bytes(bytes);
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

    #[test]
    fn postcard_round_trips_a_value() {
        let codec = PostcardCodec;
        let value = Sample {
            name: "greeter".into(),
            count: 3,
        };
        let bytes = encode(&codec, &value).unwrap();
        let back: Sample = decode(&codec, &bytes).unwrap();
        assert_eq!(value, back);
    }

    /// The property the blob path depends on: bulk bytes cross at their own
    /// size, so an envelope that encodes a payload and then frames it does not
    /// multiply it. The same value under [`JsonCodec`] is an array of decimal
    /// numbers, and framing it multiplies that again.
    #[test]
    fn postcard_keeps_bulk_bytes_at_their_own_size() {
        let block: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
        let payload = encode(&PostcardCodec, &block).unwrap();
        let framed = encode(&PostcardCodec, &payload).unwrap();
        assert!(
            framed.len() < block.len() + 16,
            "postcard framed {} bytes for a {} byte block",
            framed.len(),
            block.len()
        );
        let back: Vec<u8> = decode(
            &PostcardCodec,
            &decode::<Vec<u8>>(&PostcardCodec, &framed).unwrap(),
        )
        .unwrap();
        assert_eq!(back, block);
    }

    #[test]
    fn postcard_decode_reports_malformed_input() {
        // A length prefix that runs past the end of the input: postcard reads
        // no field names, so a short buffer is the shape of a corrupt frame.
        let err = decode::<Sample>(&PostcardCodec, &[0xff, 0xff, 0xff]);
        assert!(err.is_err());
    }
}
