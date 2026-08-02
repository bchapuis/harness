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
trips is a quorum round, and that is where serializing five hundred of them is paid.
The change is small — `put_chunked` already exists and already handles the ordering and
the don't-abandon-in-flight-work discipline. What it wants first is the cluster-side
number under "Measure", so the before and after are compared against a measurement
rather than an argument.

**A bare-metal deployment path.** `docs/standalone-deployment.md` now carries sizing,
`LimitNOFILE`, timeouts by topology, and a failure-domain mapping, but the only
production-shaped artifact in the tree is still a Kubernetes StatefulSet. Bare metal
wants a systemd unit and a disk layout.

## Measure before changing

**Where a machine create's four minutes actually go.** The local half is now measured
and it is not the answer. `crates/granary/benches/disk_capture.rs` takes the capture
path apart in three layers on one node. Per 1 MiB block, medians: read 33 µs, write
94 µs, BLAKE3 568 µs, a cold put into the memory store 0.3 µs, a cold put into the file
store 11.4 ms, and a put the file store already holds — the dedup hit, which is most of
a fresh image — 17 µs. Whole path through a real grain: a clean capture scans at
746 MB/s, an import runs at 454 MB/s into memory and 299 MB/s onto disk, and all three
are flat across 4, 16 and 64 MiB, so nothing in the path is super-linear. Extrapolated
to 512 MB that is ~0.7 s to scan and under 2 s to import, against the ~240 s a create
takes. **Over 99% of a create is above the store**, and neither `IN_FLIGHT_CHUNKS` nor
the block size is the lever: `puts` shows a trip through the seam costs 0.58 µs, four
thousand times less than the bytes it carries, so the per-block work is per-byte and
already near its floor.

What is left to measure is therefore the cluster side, and it needs a cluster rather
than a bench: where a single `put_blob` spends its time on the `Quorum` tier — transport
framing, the peer's own `atomic_replace`, the quorum wait — and how much of the 240 s is
the *count* of those rounds rather than any one of them. 240 s over 512 blocks is
~470 ms per block, which is far too large to be the 1 MiB of link (~8 ms to two peers at
125 MB/s) and far too large to be a peer's fsync (11 ms measured above), so the leading
hypothesis is the serialization named under "Build" — but it is a hypothesis, and the
demo is where it gets settled. **Still larger than everything else here combined.**

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

- The `offload` prelude (`Arc::clone` the store, clone the name and shard, call, unwrap)
  is written out at eleven sites across `replicator.rs` and `replica_store.rs`. A
  sibling of `blocking::offload` would collapse each to one expression — and would make
  it readable which store calls reach the pool, which today is neither uniform nor
  stated.
- `blocking_io` and `metrics` are node-scoped but live in the per-grain-type
  `GranaryConfig`, so every deployment threads them by hand into every type's config and
  the accessors allocate a fresh `Arc` per call. `GranarySystem` is where the node's
  other capabilities already hang.
- `AtomicGrainMetrics::entry` takes a node-wide `Mutex<BTreeMap>` on every commit,
  rehydrate and activation, though its own doc says the type set is fixed at startup.
- `file_store.rs` clones the whole record batch per append — once to frame, once to
  apply. Building the `SegOp` first and destructuring it after the append reclaims the
  `Vec`.
