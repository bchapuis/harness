//! What the wire codec costs, and what the default costs against the alternative.
//!
//! [`JsonCodec`] is what a system gets unless it chooses otherwise, and its doc comment
//! carries a specific warning: JSON has no byte-string form, so a `Vec<u8>` goes out as an
//! array of decimal numbers, and a 1 MiB value is said to cost ~10.7 MiB on the wire and
//! over a second of CPU per copy. That claim has never been measured. These benchmarks
//! measure it, on the two shapes that behave differently:
//!
//! - `bytes` is a `Vec<u8>` — the shape the warning is about, and the shape a grain's
//!   journaled event or a blob actually is.
//! - `record` is an ordinary struct of strings and numbers, where the two formats are
//!   much closer and JSON's readability is worth something.
//!
//! Both codecs are driven through `encode`/`decode`, i.e. through the `&dyn Codec` seam a
//! real send uses, so the dynamic dispatch is in the measurement rather than optimized
//! away by monomorphizing to a concrete codec.

use actor_serialization::Codec;
use actor_serialization::JsonCodec;
use actor_serialization::PostcardCodec;
use actor_serialization::decode;
use actor_serialization::encode;
use divan::Bencher;
use divan::black_box;
use divan::counter::BytesCount;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// A message of ordinary scalar fields — the shape where JSON is competitive and its
/// readability is a real benefit.
#[derive(Serialize, Deserialize, Clone)]
struct Record {
    session: String,
    turn: u64,
    tokens: u64,
    tool: String,
    ok: bool,
}

fn record() -> Record {
    Record {
        session: "session-01HQ8V3XK2".to_string(),
        turn: 42,
        tokens: 1_536,
        tool: "shell".to_string(),
        ok: true,
    }
}

/// Encode `value` through `codec`, counting `bytes` of *input* so two codecs' figures
/// are directly comparable. `bytes` of 0 leaves the throughput counter off, for values
/// too small for a rate to mean anything.
fn encode_bench<T: Serialize + Sync>(bencher: Bencher, codec: &dyn Codec, value: &T, bytes: usize) {
    let bencher = match bytes {
        0 => bencher,
        n => bencher.counter(BytesCount::new(n)),
    };
    bencher.bench(|| encode(black_box(codec), black_box(value)));
}

/// Decode what `codec` made of `value`, measuring the reverse direction.
fn decode_bench<T: Serialize + DeserializeOwned + Sync>(
    bencher: Bencher,
    codec: &dyn Codec,
    value: &T,
) {
    let encoded = encode(codec, value).expect("encodes");
    bencher.bench(|| decode::<T>(black_box(codec), black_box(&encoded)));
}

/// A payload that tells the codec it is bytes, rather than letting serde infer a
/// sequence that happens to hold them.
///
/// The distinction is one `#[serde(with)]` and it changes which serde call the codec
/// sees: `serialize_bytes` and `deserialize_bytes` instead of the default sequence
/// path's element-at-a-time loop. It changes nothing on the wire — under postcard a
/// byte string and a `Vec<u8>` are the same varint length and the same payload, and
/// under JSON `serde_json` has no byte form so it emits the identical number array —
/// which is what makes the `*_fast` figures below a free comparison rather than a
/// format trade.
#[derive(Serialize, Deserialize)]
struct Bytes(#[serde(with = "serde_bytes")] Vec<u8>);

/// Encoding a `Vec<u8>` through postcard, which has a byte-string form.
#[divan::bench(consts = [1024, 65_536, 1_048_576])]
fn bytes_postcard<const N: usize>(bencher: Bencher) {
    encode_bench(bencher, &PostcardCodec, &vec![0x5a_u8; N], N);
}

/// The same encode with the codec told these are bytes — the paired figure.
#[divan::bench(consts = [1024, 65_536, 1_048_576])]
fn bytes_postcard_fast<const N: usize>(bencher: Bencher) {
    encode_bench(bencher, &PostcardCodec, &Bytes(vec![0x5a_u8; N]), N);
}

/// The same payload through JSON: the case the `JsonCodec` warning is about, where every
/// byte becomes a decimal number in an array.
#[divan::bench(consts = [1024, 65_536, 1_048_576])]
fn bytes_json<const N: usize>(bencher: Bencher) {
    encode_bench(bencher, &JsonCodec, &vec![0x5a_u8; N], N);
}

/// Decoding a `Vec<u8>` through postcard — the other half of a round trip.
///
/// Here because a replicated blob pays both directions and neither is free: the leader
/// encodes the payload into each peer's message and the peer decodes it before its
/// store ever sees it, so a quorum put costs one encode and one decode per replica
/// before any device is touched. A blob is a mebibyte, which makes this the largest
/// single thing the codec is asked to do, and a peer round trip carrying one measured
/// ~33 ms on loopback with the peer's disk work deduplicated away entirely
/// (`machine-cost.sh`). Encode plus decode is how much of that the codec can account
/// for — and it turned out to be nearly all of it, at each of the two layers a blob is
/// encoded by.
#[divan::bench(consts = [1024, 65_536, 1_048_576])]
fn bytes_postcard_decode<const N: usize>(bencher: Bencher) {
    decode_bench(bencher, &PostcardCodec, &vec![0x5a_u8; N]);
}

/// The same decode with the codec told these are bytes. This is the half that was
/// worth the most: the default path grows the vector as it goes, one `u8` at a time,
/// where a byte string reads a length and copies once.
#[divan::bench(consts = [1024, 65_536, 1_048_576])]
fn bytes_postcard_decode_fast<const N: usize>(bencher: Bencher) {
    decode_bench(bencher, &PostcardCodec, &Bytes(vec![0x5a_u8; N]));
}

/// JSON's encode with the codec told these are bytes.
///
/// Measured because the obvious prediction is that it cannot matter — `serde_json`
/// has no byte form, so `serialize_bytes` emits the same array of decimal numbers,
/// and the bytes on the wire are indeed identical. It is ~2.5x faster anyway (139
/// MB/s to 343 MB/s at a mebibyte): what changes is not the output but the path to
/// it, `collect_seq` over a `&[u8]` against the generic sequence loop serde uses for
/// `Vec<u8>`. Kept as a bench rather than folded into the postcard story because a
/// figure nobody expected is the one most worth being able to re-run.
#[divan::bench(consts = [1024, 65_536, 1_048_576])]
fn bytes_json_fast<const N: usize>(bencher: Bencher) {
    encode_bench(bencher, &JsonCodec, &Bytes(vec![0x5a_u8; N]), N);
}

// The wire-size half of the `JsonCodec` warning is asserted in the crate's own tests
// (`json_inflates_a_byte_vector_by_roughly_the_documented_multiple`), not here: `cargo
// test` does not run bench targets, so a test living in this file would compile in CI and
// never execute. Benchmarks measure; tests assert.

/// A scalar-field message through postcard — the common small-message case.
#[divan::bench]
fn record_postcard(bencher: Bencher) {
    encode_bench(bencher, &PostcardCodec, &record(), 0);
}

/// The same message through JSON.
#[divan::bench]
fn record_json(bencher: Bencher) {
    encode_bench(bencher, &JsonCodec, &record(), 0);
}

/// Decoding a scalar-field message, postcard.
#[divan::bench]
fn record_postcard_decode(bencher: Bencher) {
    decode_bench(bencher, &PostcardCodec, &record());
}

/// Decoding a scalar-field message, JSON.
#[divan::bench]
fn record_json_decode(bencher: Bencher) {
    decode_bench(bencher, &JsonCodec, &record());
}
