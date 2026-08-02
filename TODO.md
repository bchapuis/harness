# Open work

Items that survived a review with a reason to exist and no fix yet. Each says what
would close it. The reviews that produced them — a production-readiness audit and a
review against [docs/hardware-envelope.md](docs/hardware-envelope.md) — were removed
once their conclusions reached the code; `git log` holds the full argument, and the
commit that added this file names them.

Nothing here is a bug. Bugs get fixed, not listed.

## Build

**Chunk the composite snapshot's state payload.** `host.rs::snapshot_now` encodes facet
0's `State` whole and, on the `Quorum` tier, ships it to every replica. The bulk facets
(ws, sql, disk) already put their bytes through `facet_blobs::put_chunked`, so an
unchanged region costs nothing on the wire; the grain's own state — an agent's whole
folded transcript — goes through none of it. Applying the existing mechanism to the one
payload that lacks it makes a snapshot proportional to what changed rather than to what
accumulated. Touches a durable format and the `granary.store.segment` compatibility
boundary, so it needs a design step. Consider content-defined chunking over fixed
offsets, since re-encoded state shifts every fixed boundary. **The largest single win
available on the target hardware.**

**Compression on the replication path.** Nothing in the tree compresses. Against a
125 MB/s uplink, LZ4 buys ~40 bytes of link per byte of CPU (hw §3.3), collected R−1
times (hw §3.9). It must sit *below* the blob id — a `BlobId` is BLAKE3 of the
plaintext, and hashing compressed bytes would break dedup and every durable manifest.
Do it after the chunking above, and measure the ratio on real transcripts first.

**A bare-metal deployment path.** `docs/standalone-deployment.md` now carries sizing,
`LimitNOFILE`, timeouts by topology, and a failure-domain mapping, but the only
production-shaped artifact in the tree is still a Kubernetes StatefulSet. Bare metal
wants a systemd unit and a disk layout.

## Measure before changing

**The disk-facet capture path.** Creating a 512 MB machine takes roughly four minutes.
The uplink floor for that work is ~8 seconds, and that assumes nothing deduplicates —
a fresh image is mostly zeros, which all hash to one blob. Local BLAKE3 over 512 MB is
~170 ms. So ~97% of the time is unexplained by any constant in the path. Benchmark it
before touching `IN_FLIGHT_CHUNKS` or the block size. **Larger than everything else
here combined.**

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
