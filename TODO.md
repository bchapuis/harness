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

**Skip the bytes for a blob the peer already holds.** `QuorumReplicator::put_blob`
sends every chunk to every peer; the peer recognizes the id and writes nothing. Dedup
is therefore a *disk* saving in the substrate, and a bandwidth saving only because
every writer above it declines to put at all — the ws facet caches per-file chunk ids,
the disk facet captures dirty blocks, facet 0's state checks its root set. That works,
but it puts the same discipline in four places and gets nothing for a chunk two
*different* grains or a re-import produced. An offer round (`has` the ids, put the
absent ones) would move it under the seam once. It costs an extra RTT on a cold put,
which is the whole objection — and the objection is much weaker once the puts it rides
on are pipelined rather than serialized, so sequence it after the item below.

**Pipeline the disk facet's block puts.** `DiskHandle::import` and `DiskHandle::capture`
`await` one `blobs.put` per block in a plain `for` loop, so a 512-block image is 512
serialized trips through the journal seam. Every other checkpointing facet hands its
chunks to `facet_blobs::put_chunked`, which runs `IN_FLIGHT_CHUNKS` of them at once —
the SQL facet (§7.14), the workspace facet (§7.11), and facet 0's own state (§7.12).
The disk facet is the only one that hand-rolls the loop, and it is the one with by far
the largest artifacts. On the `Local` tier the serialization costs nothing and
`benches/disk_capture.rs` shows why: a trip through the seam is 0.58 µs against 1.4 ms
of bytes per block, so there is nothing to overlap. On the `Quorum` tier each of those
trips is a quorum round, and `tests/disk_rounds.rs` now measures it: 2 ms of virtual
time per block — one frame out, one acknowledgement back — and the same 2 ms for a
four-byte blob, so the cost is the round trip and not the payload. A 512-block image
is 512 serialized quorum rounds. The change is small: `put_chunked` already exists and
already handles the ordering and the don't-abandon-in-flight-work discipline. It is
worth up to `IN_FLIGHT_CHUNKS`-fold on the create path, and `disk_rounds.rs` is the
before it gets measured against — that test is written to fail low when this lands.

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

So the remaining question is small and precise, which is the point of having measured:
each additional replica adds ~25 ms per block, linearly. Linearly is the surprise —
`put_blob` fans peers out through `FuturesUnordered` and should overlap them, so a
second replica ought to cost roughly what one does. It does not, which points at the
fan-out not actually overlapping; the "local fsync is serialized ahead of the peer
fan-out" item under "Decide" is very likely the same root cause seen from the other
side. Nagle is already ruled out — the transport sets `TCP_NODELAY` on both ends.
Instrument `QuorumReplicator::put_blob` and split it: framing and encode, time on the
wire, the peer's own store call, the quorum wait.

Scale it before prioritizing it. At ~65 ms per block a 512 MB create is ~33 s of
blocks on top of a one-time warm-up — so the headline "four minutes" was mostly the
warm-up plus a debug build, which costs ~2.4x release. Both of those are worth more
than the block path is.

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
