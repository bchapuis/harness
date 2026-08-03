//! Telling the codec a field is bytes changes nothing on the wire.
//!
//! Several message types in this workspace carry their payload as
//! `#[serde(with = "serde_bytes")] Vec<u8>` (or `serde_bytes::ByteBuf`) rather than a
//! plain `Vec<u8>` — granary's `StoreBlob`, `StoreSnapshot` and `StoreRecord`,
//! blob-store's `StoreBlob`. That is a **performance** change: it reaches
//! `serialize_bytes`/`deserialize_bytes` instead of serde's default sequence path,
//! which is worth ~160x on encode and ~670x on decode for a mebibyte under postcard
//! (`benches/codec.rs`). It was made on the claim that it is *only* a performance
//! change, and this file is that claim.
//!
//! Why it needs stating rather than assuming. The `actor.wire` boundary is at
//! revision 1 and cannot currently be bumped at all: the negotiated revision is
//! discarded after the handshake, so there is no send-side gate and no way to run two
//! revisions at once (compatibility spec §3.1). A change that altered the encoding
//! would therefore not be a version bump but a flag day. The reason this one does not:
//!
//! - **postcard** has a byte-string form, and it is encoded exactly as its sequence
//!   form — a varint length then the payload. Same bytes, different code path.
//! - **`serde_json`** has *no* byte form, so `serialize_bytes` falls back to
//!   `collect_seq` and emits the same array of decimal numbers. (It is still ~2.5x
//!   faster, which is a surprise the bench records; the bytes are what matter here.)
//!
//! **This is a property of these two codecs, not of serde.** A codec with a distinct
//! byte representation — CBOR, MessagePack, bincode's variable encodings — would make
//! these fields encode differently, and adding one to the tree is a wire-format change
//! whatever else it looks like. That is exactly the regression these tests exist to
//! turn into a failure, so a new `Codec` impl belongs in `CODECS` below.

use actor_serialization::Codec;
use actor_serialization::JsonCodec;
use actor_serialization::PostcardCodec;
use actor_serialization::decode;
use actor_serialization::encode;
use serde::Deserialize;
use serde::Serialize;

/// Every codec the workspace can be deployed with. A new one goes here, and if it
/// has a byte form distinct from its sequence form these tests will say so.
const CODECS: &[(&str, &dyn Codec)] = &[("postcard", &PostcardCodec), ("json", &JsonCodec)];

/// A payload field as it was written before: serde infers a sequence of `u8`.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Plain {
    shard: u32,
    payload: Vec<u8>,
    trailing: String,
}

/// The same field, told to the codec as bytes. Field names, order, and types are
/// otherwise identical, so any difference in output is attributable to the attribute
/// alone.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Tagged {
    shard: u32,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
    trailing: String,
}

/// The `Vec<Vec<u8>>` shape, which `#[serde(with)]` cannot reach — granary's
/// `StoreRecord::records` uses `Vec<ByteBuf>` instead, and it has to be neutral too.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct PlainMany {
    records: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct TaggedMany {
    records: Vec<serde_bytes::ByteBuf>,
}

/// Payloads chosen to catch an encoder that special-cases the easy shapes: empty,
/// one byte, every byte value, and something past a single varint length byte.
fn payloads() -> Vec<Vec<u8>> {
    vec![
        Vec::new(),
        vec![0],
        vec![0xff],
        (0..=255u8).collect(),
        vec![0x5a; 5000],
    ]
}

fn plain(payload: &[u8]) -> Plain {
    Plain {
        shard: 7,
        payload: payload.to_vec(),
        trailing: "after".into(),
    }
}

fn tagged(payload: &[u8]) -> Tagged {
    Tagged {
        shard: 7,
        payload: payload.to_vec(),
        trailing: "after".into(),
    }
}

#[test]
fn tagging_a_byte_field_does_not_change_what_goes_on_the_wire() {
    for (name, codec) in CODECS {
        for payload in payloads() {
            let untagged = encode(*codec, &plain(&payload)).expect("encodes");
            let bytes = encode(*codec, &tagged(&payload)).expect("encodes");
            assert_eq!(
                untagged,
                bytes,
                "{name} encodes a {}-byte payload differently once the field is \
                 tagged as bytes — this is a wire-format change, and `actor.wire` \
                 has no way to run two revisions at once (compatibility spec §3.1)",
                payload.len(),
            );
        }
    }
}

#[test]
fn a_vector_of_byte_vectors_is_neutral_too() {
    for (name, codec) in CODECS {
        let untagged = encode(
            *codec,
            &PlainMany {
                records: payloads(),
            },
        )
        .expect("encodes");
        let bytes = encode(
            *codec,
            &TaggedMany {
                records: payloads()
                    .into_iter()
                    .map(serde_bytes::ByteBuf::from)
                    .collect(),
            },
        )
        .expect("encodes");
        assert_eq!(
            untagged, bytes,
            "{name} encodes `Vec<Vec<u8>>` and `Vec<ByteBuf>` differently — \
             granary's `StoreRecord::records` relies on them agreeing",
        );
    }
}

#[test]
fn each_form_decodes_what_the_other_wrote() {
    // The rolling-upgrade question, and the one that actually bites: a node running
    // the tagged build must read a peer running the untagged one, in both directions,
    // or the change is a flag day however identical the bytes look.
    for (name, codec) in CODECS {
        for payload in payloads() {
            let from_untagged = encode(*codec, &plain(&payload)).expect("encodes");
            let from_tagged = encode(*codec, &tagged(&payload)).expect("encodes");

            let new_reads_old: Tagged = decode(*codec, &from_untagged).expect("decodes");
            let old_reads_new: Plain = decode(*codec, &from_tagged).expect("decodes");

            assert_eq!(
                new_reads_old,
                tagged(&payload),
                "{name}: a tagged build misread an untagged peer's message",
            );
            assert_eq!(
                old_reads_new,
                plain(&payload),
                "{name}: an untagged build misread a tagged peer's message",
            );
        }
    }
}
