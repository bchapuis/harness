//! What a frame carrying bulk bytes costs to put on the wire, in both directions.
//!
//! Every remote actor message crosses this boundary twice over: the sender encodes its
//! message with the codec, and the result is carried as bytes inside a [`Frame`] that
//! is *itself* codec-encoded onto the socket. The inner encode is
//! `actor-serialization/benches/codec.rs`'s subject. This file is the outer one, which
//! is the layer that was missed — granary's blob messages were told they held bytes
//! long before the frame wrapping them was, so a replicated mebibyte still went
//! through serde's element-at-a-time sequence path once in each direction and it cost
//! ~11 ms a block on real nodes (TODO.md).
//!
//! Two variants carry bulk bytes and they are measured separately because they are
//! fixed differently and were fixed at different times:
//!
//! - [`Frame::Envelope`] is the **request** direction — a blob being replicated to a
//!   peer, a grain record being appended. Its `payload` takes
//!   `#[serde(with = "serde_bytes")]` directly.
//! - [`Frame::Reply`] is the **answer** — a `fetch_blob` returning a block to a leader
//!   that lacks it, a grain read returning its records. Its outcome is `ReplyResult`,
//!   a `Result<Vec<u8>, CallError>` *type alias* with no field to hang an attribute
//!   on, so `protocol::reply_bytes` mirrors serde's `Result` representation instead.
//!   That mirror is the thing this bench exists to keep honest about cost, and
//!   `protocol`'s own tests keep it honest about bytes.
//!
//! The two should read the same. If `Reply` is slower than `Envelope` at the same
//! payload, the mirror has stopped reaching `serialize_bytes` — which is a performance
//! regression the byte-identity tests cannot see, because the bytes would still be
//! right.
//!
//! Sizes span three orders of magnitude because the defect this measures is invisible
//! at small ones: a per-element loop over 1 KiB is nothing, and over a mebibyte it is
//! the whole cost of the operation.

use actor_cluster::CallId;
use actor_cluster::Frame;
use actor_core::ActorId;
use actor_core::CallError;
use actor_core::NodeId;
use actor_core::Path;
use actor_serialization::Codec;
use actor_serialization::PostcardCodec;
use actor_serialization::decode;
use actor_serialization::encode;
use divan::Bencher;
use divan::black_box;
use divan::counter::BytesCount;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// Postcard, because that is what a deployment carrying blobs runs
/// (`machine-standalone`). JSON has no byte form at all, so it cannot show this
/// difference in bytes and shows a much smaller one in time; `codec.rs` covers it.
const CODEC: &dyn Codec = &PostcardCodec;

fn envelope(payload: Vec<u8>) -> Frame {
    Frame::Envelope {
        recipient: ActorId::new(NodeId::new(1), Path::new("bench/blob"), 7),
        manifest: "bench.Blob".to_string(),
        correlation: Some(CallId(1)),
        payload,
    }
}

fn reply(payload: Vec<u8>) -> Frame {
    Frame::Reply {
        correlation: CallId(1),
        outcome: Ok(payload),
    }
}

fn encode_frame(bencher: Bencher, frame: Frame, bytes: usize) {
    bencher
        .counter(BytesCount::new(bytes))
        .bench_local(|| black_box(encode(CODEC, black_box(&frame)).expect("encodes")));
}

fn decode_frame(bencher: Bencher, frame: Frame, bytes: usize) {
    let wire = encode(CODEC, &frame).expect("encodes");
    bencher
        .counter(BytesCount::new(bytes))
        .bench_local(|| black_box(decode::<Frame>(CODEC, black_box(&wire)).expect("decodes")));
}

/// The request direction, on the leader.
#[divan::bench(consts = [1024, 65_536, 1_048_576])]
fn envelope_encode<const N: usize>(bencher: Bencher) {
    encode_frame(bencher, envelope(vec![0x5a; N]), N);
}

/// The request direction, on the peer. The half that was worth the most in
/// `codec.rs`: the default path grows the vector one `u8` at a time where a byte
/// string reads a length and copies once.
#[divan::bench(consts = [1024, 65_536, 1_048_576])]
fn envelope_decode<const N: usize>(bencher: Bencher) {
    decode_frame(bencher, envelope(vec![0x5a; N]), N);
}

/// The answer direction, on the node that holds the bytes — through the mirror.
#[divan::bench(consts = [1024, 65_536, 1_048_576])]
fn reply_encode<const N: usize>(bencher: Bencher) {
    encode_frame(bencher, reply(vec![0x5a; N]), N);
}

/// The answer direction, on the node that asked.
#[divan::bench(consts = [1024, 65_536, 1_048_576])]
fn reply_decode<const N: usize>(bencher: Bencher) {
    decode_frame(bencher, reply(vec![0x5a; N]), N);
}

/// A failed reply, which carries no bytes at all.
///
/// Here to keep the mirror's *other* arm in the measurement: `Err` is the common case
/// under partition, it goes through the same hand-written module, and a mirror that
/// somehow made the error path expensive would be paid on exactly the frames a
/// struggling cluster sends most.
#[divan::bench]
fn reply_err_encode(bencher: Bencher) {
    let frame = Frame::Reply {
        correlation: CallId(1),
        outcome: Err(CallError::Unreachable),
    };
    bencher.bench_local(|| black_box(encode(CODEC, black_box(&frame)).expect("encodes")));
}
