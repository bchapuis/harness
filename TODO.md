# Open work

Items that survived a review with a reason to exist and no fix yet. Each says what
would close it. The reviews that produced them — a production-readiness audit and a
review against [docs/hardware-envelope.md](docs/hardware-envelope.md) — were removed
once their conclusions reached the code; `git log` holds the full argument, and the
commit that added this file names them.

Nothing here is a bug. Bugs get fixed, not listed.

## Build

**Compression on the replication path.** Nothing in the tree compresses. Against a
125 MB/s uplink, LZ4 buys ~40 bytes of link per byte of CPU (hw §3.3), collected R−1
times (hw §3.9). It must sit *below* the blob id — a `BlobId` is BLAKE3 of the
plaintext, and hashing compressed bytes would break dedup and every durable manifest.
Now unblocked: the snapshot state payload it was to be sequenced behind is chunked
(`cdc.rs`, §7.12). Measure the ratio on real transcripts first.

**~~Skip the bytes for a blob the peer already holds.~~** *Withdrawn.* An offer round
(`has` the ids, put the absent ones) in `QuorumReplicator::put_blob` was to centralize
the don't-put-what-they-have discipline the facets each implement, and to catch a
chunk two *different* grains produced. It cannot catch that one: a grain's blob area
is per-grain all the way down — `FileGrainStore::segment_id` keys on `(shard, grain)`
and `has_blob` with it — so the round can only ask about *this* grain, and
centralizing would not create cross-grain dedup. What remains is a re-put the facets
above already filter, bought by making every *cold* put pay a `has` round first. On a
create every chunk is cold, and pipelining sharpened rather than softened that: puts
now cost one round per wave, so an offer round is two, doubling what was just halved.
Reopen only alongside a shared (not per-grain) blob namespace, where the question
would have a useful answer. Kept as a note so it is not re-proposed.

**A bare-metal deployment path.** `docs/standalone-deployment.md` now carries sizing,
`LimitNOFILE`, timeouts by topology, and a failure-domain mapping, but the only
production-shaped artifact in the tree is still a Kubernetes StatefulSet. Bare metal
wants a systemd unit and a disk layout.

## Measure before changing

**What a machine create costs, and where the four minutes went.** Measured, on all
three layers, and the premise this item started from was wrong: the time was never
in the capture path, and it was never one number.

`benches/disk_capture.rs` takes the single-node path apart. Per 1 MiB block, medians:
read 33 µs, write 94 µs, BLAKE3 568 µs, a cold put into the memory store 0.3 µs, a
cold put into the file store 11.4 ms, and a put the store already holds — the dedup
hit, which is most of a fresh image — 17 µs. `tests/disk_rounds.rs` counts the
`Quorum` tier in virtual time: one round trip per block, the same for a four-byte
blob as for a mebibyte, so the cost is rounds and not payload.

Then the cluster itself, on real nodes over loopback. `./machine-cost.sh` is the
harness — it boots a cluster the way `machine-demo.sh` does and times two creates
against it, because the first and the second answer different questions. Slopes
between two image sizes, release build, `--machine fake`:

| replicas | ms per block |
|---------:|-------------:|
| 1        | 11.0 |
| 2        | ~40  |
| 3        | 63–71 |

Two things fall out. **A create is a large fixed cost plus a small per-block one.**
The first create after a cluster starts takes ~33 s on one node and ~40–57 s on
three *regardless of image size* — 1 block and 128 blocks cost the same — while the
second create on the same live cluster costs 0.06 s and 8.8 s respectively. Nothing
in that ~33 s is the disk facet; it is cluster and control-plane warm-up, and with
`machine-standalone`'s deliberately patient timings (SWIM probe 2 s, Raft heartbeat
4 s, election timeout 20 s) that is where to look for it first. **And the single-node
per-block cost is 11.0 ms, which is the file store's 11.4 ms cold put and nothing
else** — the local path is fully explained by the bench, with no gap left in it.

The fan-out is not the problem, and an earlier draft of this entry said it was. That
draft read a per-replica cost off the *random* image, where each added replica also
adds a replica's worth of cold `atomic_replace` on a laptop where all three nodes
share one device — disk work, not coordination. Re-run against an all-zero image,
where every block after the first is a dedup hit at every replica and no node writes
anything, the shape is different: **1 node 0.9 ms per block, 2 nodes 33.8, 3 nodes
40.5.** The first peer costs ~33 ms; the second costs ~7 ms more. Peers overlap.

So the question is a single one: **a peer round trip carrying a 1 MiB blob costs
~33 ms on loopback, with the peer's disk work deduplicated away entirely.** Half of
that was the codec, and that half is now gone.

The bench had priced postcard on a 1 MiB `Vec<u8>` at 3.8 ms to encode and 12.0 ms to
decode — 272 MB/s and 86 MB/s against a memcpy's several GB/s — because nothing told
the codec these were bytes rather than a sequence that happened to hold them, so the
decode grew its vector one `u8` at a time. The payload fields now say so
(`serde_bytes`), and the same bench reads **23.5 µs to encode and 17.9 µs to decode**:
163x and 669x, taking encode-on-the-leader plus decode-on-the-peer from ~15.8 ms to
~0.04 ms.

*The compatibility objection this item carried was wrong, which is why it moved.* It
said `serde_bytes` was a format change under JSON — an array of decimal numbers
becoming a base64 string — and so a compatibility-window question. `serde_json` has
no byte form at all: `serialize_bytes` falls back to `collect_seq` and emits the same
number array, ~2.5x faster for the same output. Under postcard a byte string and a
`Vec<u8>` were always the same varint length and payload. So the encoding is
unchanged for both codecs in the tree and `actor.wire` stays at revision 1 —
`actor-serialization/tests/wire_bytes.rs` asserts exactly that, including
cross-decoding both ways, and names the condition it depends on: a codec with a
*distinct* byte form (CBOR, MessagePack) would make these fields a wire change.

That leaves ~17 ms in the transport and the peer's actor hop, which is where to
instrument next, and it is now the whole of what remains rather than half of it. The
~33 ms figure itself is pre-pipelining and pre-codec and should be re-taken on real
nodes (`./machine-cost.sh`) before anything is built on it.

Scale it before prioritizing it. At ~65 ms per block a 512 MB create is ~33 s of
blocks on top of a one-time warm-up — so the headline "four minutes" was mostly the
warm-up plus a debug build, which costs ~2.4x release. Both of those are worth more
than the block path is.

*Since measured:* the block path's rounds no longer add up. The disk facet's puts
are pipelined `IN_FLIGHT_CHUNKS` at a time, so `disk_rounds.rs` now counts one
quorum round per **wave of 16 blocks** where it counted one per block — 2 ms of
virtual time either way, which is the full 16-fold. The ~33 s of blocks above is
the serialized figure and should be re-taken on real nodes; what it does *not*
change is the one-time warm-up, which was always the larger half.

*A correction to what this file said before:* the earlier entry divided 240 s by 512
blocks to get "~470 ms per round" and went looking for a round trip that expensive.
There is no such round trip. The division was wrong because it spread a large fixed
cost across the blocks — the same intercept error `disk_rounds.rs` was restructured
to avoid, made here in prose.

**Flush distribution on the actual drives.** `blocking.rs` justifies the I/O pool by
tail isolation, which is right, but the median matters for sizing and nothing records
whether the deployment's NVMe has power-loss protection: 30 µs with, ~500 µs without,
and `ThreadPoolIo::sized_for_host`'s `clamp(2, 8)` should follow the measurement.

**`HostCache` lock contention.** One `Mutex<Generations>` per grain type, taken on
every call including a hit, in the gateway — one long-lived process fronting every
tenant on a many-core box (hw §3.4). If it shows, shard the map by name hash into N
independent mutexes, preserving the generation policy. Deliberately not done before a
number exists. Same treatment for `FileGrainStore`'s manifest and loaded-segment locks.

## Decide

**Whether `Reserved` should exist.** It is now a newtype over an already-durable value.
The eagerness it once guaranteed comes from the trait method being synchronous, not
from the wrapper; what survives is `#[must_use]` on a value that can be `Fenced`,
`Stale`, `Sealed` or `Failed`, and a name at each call site. A `#[must_use]` on the
trait methods would give the same guarantee without ~139 call sites of ceremony.
Deleting it is a public-API change, hence a decision rather than a cleanup.

**The Raft WAL's panic policy.** `actor-runtime/src/storage.rs` still panics when it
cannot persist. The grain store was changed to poison and refuse instead, but a voter
that cannot persist its term must stop voting, which is a different and larger question
than a replica dropping out of a quorum.

**The local fsync is serialized ahead of the peer fan-out.** In
`QuorumReplicator::append` and `save_snapshot` the peer asks are lazy futures, so
`offload(...).await` means no `StoreRecord` reaches a replica until this node's own
flush completes: commit latency is `local + RTT` rather than `max(local, RTT)`. Noise
at the median, the whole cost at the tail. Overlapping them changes ordering under
`InlineIo` and so changes what the deterministic simulator sees — safe, but it needs
the seed sweeps run deliberately rather than incidentally.

## Quality, when the code is next opened

- `blocking_io` and `metrics` are node-scoped but live in the per-grain-type
  `GranaryConfig`, so every deployment threads them by hand into every type's config and
  the accessors allocate a fresh `Arc` per call. `GranarySystem` is where the node's
  other capabilities already hang.
- `file_store.rs` clones the whole record batch per append — once to frame, once to
  apply. Building the `SegOp` first and destructuring it after the append reclaims the
  `Vec`.
