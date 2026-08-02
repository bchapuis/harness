# Review against the hardware envelope

**Date:** 2026-08-02
**Branch:** `machine-fence-per-grain-type` (at `9717fd3`, plus the working tree)
**Against:** [docs/hardware-envelope.md](docs/hardware-envelope.md) — a rented dedicated
server (AMD or Ampere ARM, 128–256 GB, local NVMe, **1 Gbps uplink**), one datacenter.
**Method:** every tuning constant, bound, cache, replication path, and batching decision
read against the envelope's arithmetic, and each one classified. Nothing below was found by
a failing test or a benchmark; the arithmetic is stated so it can be checked or refuted.
**Companion:** [audit.md](audit.md) — the production-readiness audit, whose finding 9 was
the first place this assumption was used and the reason it is now written down.

The envelope's binding constraint is not the disk. It is the **1 Gbps uplink** (hw I1, I2):
a byte is ~50× cheaper to persist than to move, and under `replication_factor: 3` every
durable byte crosses that port twice. A node's sustainable ingest of new data is ~60 MB/s
against a device that would take 3–7 GB/s. Six of the twelve findings below are consequences
of that one fact, and the largest of them is a default nobody has looked at since it was
copied from another platform.

| Verdict | Meaning |
|---|---|
| **Simplify** | The envelope removes the reason for this code or this planned work. Delete it. |
| **Retune** | The mechanism is right; the number came from a different machine. |
| **Measure** | The envelope says this is where the cost now is, and there is no number yet. |
| **Keep** | Already correct under the envelope; the reasoning belongs in the code so it is not re-litigated. |

| # | Finding | Verdict |
|---|---|---|
| 1 | Snapshots ship whole state to every peer, and `idle_after: 10s` triggers one per turn | **Retune** |
| 2 | The composite snapshot's own state payload is the one large thing with no dedup | **Simplify** |
| 3 | Compression on the replication path is earned here, and is absent | **Retune** |
| 4 | `Reserved`'s pending arm: a delayed-durability seam nothing constructs | **Simplify** |
| 5 | Manifest compaction and the shard-bytes counter: planned work the envelope cancels | **Simplify** |
| 6 | `DEFAULT_SEGMENT_CAPACITY = 8192` against a `RLIMIT_NOFILE` nobody sets any more | **Retune** |
| 7 | The disk-facet capture path: the uplink explains 3% of the observed time | **Measure** |
| 8 | The blocking-I/O pool's justification depends on a drive spec nobody has checked | **Measure** |
| 9 | `HostCache`: one mutex on the hot path of the tier meant to scale out | **Measure** |
| 10 | Timeouts, Raft timings, and failure domains have no derivation for this deployment | **Measure** |
| 11 | `k8s/harness.yaml` sizes a node at 1 CPU / 512 MiB, and is not the target anyway | **Contradiction** |
| 12 | Seven decisions the envelope confirms, whose reasoning lives outside the code | **Keep** |

A finding from the first pass of this review has been **withdrawn**; see *Corrections* at the end.

## Status

Applied, with the two structural findings deferred and stated as such.

| # | Verdict | Status |
|---|---|---|
| 1 | Retune | **Applied** — `idle_after` 10 s → 300 s; `snapshot_every` 256 → 4096; the idle path now snapshots through §9's trigger instead of unconditionally (`host.rs::passivate`). Spec §9/§10/Appendix A and the harness spec updated. |
| 2 | Simplify | **Deferred** — chunking the composite snapshot's state payload changes a durable format and the `granary.store.segment` compatibility boundary. Needs a design step, not an edit. |
| 3 | Retune | **Deferred** — compression on the transport adds a dependency and an encoding below the blob id. Measure after finding 2, which removes the bytes dedup can remove. |
| 4 | Simplify | **Applied** — `Reserved`'s pending arm and its boxed future deleted; `durable()` is now synchronous, which collapsed five `refusal()` short-circuits at the call sites. |
| 5 | Simplify | **Applied** — thresholds recorded at `file_store.rs`'s manifest docs and `shard_bytes`; neither piece of work built. |
| 6 | Retune | **Applied** — `DEFAULT_SEGMENT_CAPACITY` 8192 → 65536 with the `LimitNOFILE` requirement stated in the code and in the deployment guide. `host_cache_capacity` 8192 → 65536 on the same reasoning. |
| 7 | Measure | **Open** — needs a benchmark, not an edit. |
| 8 | Measure | **Partly applied** — `blocking.rs`'s rationale rewritten around tail isolation, and `sized_for_host` re-derived from it. The flush-distribution measurement is still open. |
| 9 | Measure | **Open** — deliberately not sharded before a number exists. |
| 10 | Measure | **Applied** — `docs/standalone-deployment.md` gains a timeout-by-topology table and a worked `failure_domains` mapping; `config.rs` points at them. |
| 11 | Contradiction | **Applied** — `k8s/harness.yaml` marks its limits demo-scale, carries a production sizing block and the `LimitNOFILE` requirement, and notes the storage-class dependency. |
| 12 | Keep | **Applied** — the reasoning moved into the code it governs (`wal`, `file_store`, `blobs`, `replicator::load`). |

One test changed behaviour and is worth recording, because it did its job: `grain_swarm`'s
hibernating sweep asserts that some activation returns from a snapshot, and with finding 1's
change its short faulted bursts stopped crossing a cadence of 3. The sweep now sets `1`, so
the restore path stays exercised — the guard caught exactly the coverage the change would
otherwise have silently removed.

**Verification.** `cargo check --workspace --all-targets` and `cargo clippy --workspace
--all-targets` clean. `cargo test -p granary --features sql,testing`: 30 suites, 236 tests,
0 failures. `cargo test --workspace --exclude granary`: 60 suites, 428 tests, 0 failures.
That covers the suites these changes put most at risk — the hibernation sweeps across all
four facets, `partition_safety` and `raft_journal` (the fence and lossless failover), the
store oracle, and the deterministic seed-reproducibility sweeps.

---

## The uplink findings

### 1. A snapshot is a full-state broadcast, and the idle window fires one per turn

`crates/granary/src/replicator.rs`, `QuorumReplicator::save_snapshot`: the snapshot state is
`state.clone()`d to **every peer replica** and quorum-counted, exactly like a record append.
`crates/granary/src/host.rs:670-680` triggers one every `snapshot_every` events, and
`:640-652` triggers one on **every voluntary hibernation** — `snapshot_now` skips only when
nothing has been written since the last snapshot. `crates/granary/src/config.rs:132-133`
defaults `idle_after` to 10 s and `snapshot_every` to 256.

Compose those three and put an agent session through them.

A session grain's state is the folded transcript; call it 2 MB for a substantial run. The
harness vetoes hibernation while a run is live (`agent.rs:1682-1688`), so the grain stays
resident through the model call. Then the turn ends. The user reads the answer. Ten seconds
later the session hibernates, and hibernating writes a snapshot: **2 MB fsynced locally, a
segment rewrite, and 4 MB across the uplink** to two peers — 32 ms of port time — to persist
a transcript that grew by a few kilobytes. Then the next turn re-activates it and does it
again.

That is roughly 4 MB of replication per turn per session, almost all of it bytes the peers
already hold. At fifty concurrent sessions turning every thirty seconds it is ~7 MB/s, 5% of
the uplink. At five hundred it is ~67 MB/s — **over half the cluster's entire network
capacity**, spent re-sending unchanged transcripts. That is the concurrency ceiling of the
product, and it is set by a default inherited from Durable Objects, which evicts at ten
seconds because it prices multi-tenant memory on shared hosts (hw §3.6). Neither clause of
that reason is true on a 128 GB dedicated box.

The same fan-out is on the write path independently: `snapshot_every: 256` re-broadcasts the
whole state every 256 events. For a facet like `Kv`, whose map is re-encoded whole at every
snapshot (`kv.rs:47-52`), that is O(state) on the wire for O(1) change.

**Retune, in three parts.**

- Raise `idle_after` by one to two orders of magnitude (300 s is a defensible start) and
  change the DO citation: it explains why the *mechanism* exists, not why the number is ten.
  Under hw §3.2 the memory this reclaims is the abundant resource; under hw §3.9 the bytes it
  spends are the scarce one.
- Express `snapshot_every` in **bytes appended since the last snapshot** rather than in
  events, so the trigger scales with the thing it trades against instead of with a count that
  means something different for every facet, and raise it.
- Take finding 2, which is the structural half of the same problem.

What does *not* change: hibernation itself, `can_passivate`, the alarm-index handshake, the
failover re-activation path. Those are correctness machinery (hw §5); a longer window makes
them run less often, not less.

### 2. Facet payloads are chunked and deduplicated; the grain's own state is not

`crates/granary/src/host.rs`, `snapshot_now`: the composite snapshot is facet 0's `State`
plus one contribution per declared facet (§7.12). The facets that carry bulk — workspace
(`ws.rs`, 1 MiB chunks), SQL (`sql.rs`, 256 KiB chunks), disk (`disk.rs`, 1 MiB blocks) —
each put their bytes into the grain's content-addressed blob area, so an unchanged region
hashes to a blob every replica already holds and costs nothing on the wire. That machinery
exists, is correct, and is the single largest saver of uplink bytes in the tree.

Facet 0's `State` — the grain's own state, which for the agentic harness is the whole folded
transcript and for a machine is its metadata — goes through none of it. It is encoded whole
and shipped whole, every snapshot, to every peer.

So the tree already contains the right answer to finding 1's cost, applied to every payload
except the one that grows monotonically on its most important workload.

**Simplify** in the sense that matters: apply the mechanism that already exists to the one
payload that lacks it. Chunk the encoded state through `facet_blobs::put_chunked` and let the
composite snapshot carry a chunk manifest instead of bytes. An append-only transcript then
re-ships only its final partial chunk, and the cost of a snapshot becomes proportional to
what changed rather than to what accumulated. The root-keeping discipline (**F3**,
`facet_blobs.rs:16-42`) and the sweep already cover blob liveness, so this adds no new
reclamation question — it puts one more producer behind the existing one.

Two caveats to check before building it. Content-defined chunking is worth considering over
fixed offsets if the state is re-encoded rather than appended, since one inserted byte shifts
every fixed boundary. And the snapshot path is `Local`-tier too, where there are no peers and
chunking buys only disk — so it should stay a policy of the payload, not of the tier.

### 3. Compression on the replication path is earned here, and there is none

`grep -rn "zstd\|lz4\|flate\|compress" Cargo.toml crates/*/Cargo.toml` returns nothing. Every
record, snapshot, and blob crosses the uplink as postcard bytes.

This is the one place the envelope **adds** a technique rather than removing one. Against a
125 MB/s port, LZ4 at ~5 GB/s buys forty bytes of link per byte of CPU and zstd -1 at
~1.5 GB/s buys twelve (hw §3.3), on a machine with tens of idle cores. Agent transcripts,
SQL pages, and workspace text compress 3–5×; a 3× ratio on the replication path is a 3×
increase in the cluster's *entire* sustainable write throughput, and under hw §3.9 it is
collected R−1 times.

**Retune** — but deliberately, because compression has two places it can go and they are not
equivalent.

- **On the transport, under the blob id.** A `BlobId` is BLAKE3 of the *plaintext*
  (`blobs.rs:40`), which is what makes dedup and the read-path verification work (**B1/G17**).
  Compression must therefore be an encoding *below* the id — compress after hashing, decompress
  and then verify — never a change to what is hashed, or identical content stops deduplicating
  and every id in every durable manifest changes meaning.
- **At rest.** Independent, smaller, and a compatibility-boundary change
  (`granary.store.segment`). Not worth coupling to the transport change.

Start with the transport, measure the ratio on real transcripts before choosing the codec,
and note that this interacts with finding 2: chunk-level dedup removes bytes that compression
would otherwise have to work on, so do 2 first and measure 3 against what remains.

---

## Simplify

### 4. `Reserved`'s pending arm is a seam for a design that was withdrawn

`crates/granary/src/store.rs:204-256`. `Reserved<T>` carries an outcome plus
`Option<BoxFuture<'static, ()>>` — durability that has not happened yet. The whole `Some` half
is unreachable in production: `Reserved::pending` is `#[cfg_attr(not(test), allow(dead_code))]`
and its only caller is a test at `:1322`. The module says so plainly: *"Both stores in this
crate settle synchronously, so outside a test build nothing constructs a pending outcome."*

The design that would have constructed one is group commit — a writer that returns "your slot
is reserved" and syncs a batch later. That was withdrawn on this envelope's arithmetic (audit
finding 9 revised; hw I3, §3.1). What remains is the interface of a rejected implementation,
on the hottest path in the storage layer: every store call constructs a `Reserved`, and
`durable()` on the settled path boxes a `std::future::ready` for a future that was never going
to yield.

The type still earns its `#[must_use]` and its name. **G14** — a `Stored` reported before its
bytes are stable shrinks a write's durability below the quorum counted for it — is real, and
enforcing it with a type rather than a comment is right.

**Simplify.** Delete `pending` and the `Option<BoxFuture>` field. `Reserved<T>` becomes a
`#[must_use]` newtype with `map`, `refusal()`, and an infallible synchronous `durable()`. The
G14 guard survives — a caller still cannot read the outcome without naming durability — and
the append path loses a boxed future per call and a state from its contract. The cost of being
wrong is small and known: if the deployment moves to network-attached storage (hw §6), the
pending arm is roughly forty lines, and the test that documents its contract survives the
deletion.

### 5. Two pieces of planned work the envelope cancels

Both are recorded in `audit.md` as open or recommended, and neither should be built.

**The manifest's unbounded growth** (`crates/granary/src/file_store.rs:29-34`, audit finding
9's second bullet). A `ManifestEntry` is a shard, a name, and a `u64`; call it 64 bytes
resident. Ten million grains is 640 MB against 128–256 GB, and the file is replayed at
`fs::read` plus XXH3 — 5–14 GB/s and 15–25 GB/s — so even a multi-gigabyte manifest opens in
under a second. The leak is real, and the spec sentence it contradicts (granary §7.8, *"total
grain count is limited only by the shards' storage"*) is still not literally true. But the
crossover is past a million grains per node, and compaction now adds a second format and a
rewrite path for nothing.

**A shard-bytes counter** (audit finding 8's second fix, `file_store.rs:1143-1170`).
`shard_bytes` walks the manifest, `stat`s each segment, and `read_dir`s each blob directory
every `SPLIT_TRIGGER_INTERVAL` — now 30 s. A `stat` from the dentry cache is ~1 µs, so 100k
grains costs ~100 ms of one core every 30 s: a fraction of a percent of one core out of
dozens, on a machine where cores are abundant (hw §3.3). A maintained counter is a number that
can drift from the filesystem, on a path whose job is to describe the filesystem.

**Simplify.** Do neither. Record the thresholds where the code is — around a million grains
for the manifest, and a `shard_bytes` walk that exceeds a second — so revisiting is triggered
by a measurement rather than by memory. hw §3.6 requires the reasoning to live at the
constant, not only in a review.

---

## Retune

### 6. `DEFAULT_SEGMENT_CAPACITY = 8192` budgets against a limit nobody sets

`crates/granary/src/file_store.rs:138-148`. The rationale is explicit and was correct when
written: *"well under a typical tuned `RLIMIT_NOFILE` (65536)"*.

65536 is what a 2010-era distribution shipped. On a current Linux host `fs.nr_open` defaults
to 1,048,576, systemd units routinely set `LimitNOFILE` to a million, and an open descriptor
costs a few hundred bytes of kernel memory — 65536 of them is tens of megabytes against
128 GB. The cap is a real bound and must stay (hw §5: a bound is not an optimization); it is
simply set an order of magnitude below what the machine allows, and each eviction costs a
reopen and replay of a grain's segment.

**Retune.** Raise the default to 65536 and state the `LimitNOFILE` a deployment must set for
it. Nothing in `k8s/`, the systemd guidance, or `docs/standalone-deployment.md` sets one today,
so a deployment would hit the *kernel's* limit before the store's — the bound would not be the
one the code thinks it is. The two-generation eviction machinery is unchanged.

---

## Measure

These are the places the envelope says the cost now lives. None should be changed
speculatively; each needs a number first. The metrics seam from audit finding 3
(`crates/granary/src/metrics.rs`) is the instrument, and `crates/granary/benches/store.rs`
is where to extend.

### 7. The disk-facet capture path: the uplink explains 3% of it

`crates/granary/src/disk.rs` (1 MiB blocks), `crates/granary/src/facet_blobs.rs:56`
(`IN_FLIGHT_CHUNKS = 16`). The constants are fine, and now the arithmetic can be done rather
than guessed.

Creating a 512 MB machine in the demo takes roughly four minutes. The uplink floor for that
work: 512 chunks × 1 MiB × 2 peers = 1 GiB over a 125 MB/s port ≈ **8.4 seconds**, and that is
the pessimistic case in which nothing deduplicates — a fresh image is mostly zeros, whose
blocks all hash to one blob, so the real transfer should be a small fraction of it. Local
BLAKE3 over 512 MB is ~170 ms. Against an observed ~240 seconds, the network accounts for at
most 3% and the device for far less.

So the envelope's answer here is precise and negative: **the uplink is not the explanation**,
and neither is any constant in this document. Something else is costing 97% of that time.

**Measure** this before touching anything in the path. It is larger than every other finding
here combined, and it is the one place where the arithmetic and the observation disagree by
more than an order of magnitude.

### 8. The blocking-I/O pool's justification depends on a drive spec nobody has checked

`crates/granary/src/blocking.rs`. The module argues that an inline fsync *"stalls heartbeats
past the election timeout"*. Whether that is still true is a **hardware question the tree has
not answered**: with power-loss protection a flush is ~30 µs and the argument is dead (against
a 250 ms heartbeat, on one of dozens of cores, it is noise); without PLP it is ~500 µs and the
argument is alive. Drive models vary by tier, and nothing in the repo records which one the
deployment has.

Either way the pool is worth keeping, but possibly for a different reason: **tail isolation**
(hw §3.7). The same device that flushes in 30 µs at the median stalls for 200 ms during
garbage collection or a RAID rebuild, and *that* is what must not park a worker driving Raft.
That is a different argument with a different sizing rule — pool width should follow the
concurrency you want to survive a stall, not the core count that
`ThreadPoolIo::sized_for_host`'s `clamp(2, 8)` currently follows.

**Measure** the flush distribution on the actual drives, then rewrite the module's rationale
around whichever argument survives. Note the coupling to `blocking.rs`'s single
`Mutex<Receiver>`: every worker takes one global lock to dequeue, uncontended at 8 workers and
not obviously so much beyond it.

### 9. `HostCache` puts one mutex on the hot path of the tier meant to scale out

`crates/granary/src/grainref.rs:84-200`. One `Mutex<Generations>` per grain type, taken on
**every** call including a hit. The gateway — one long-lived process fronting every tenant, on
a many-core box, explicitly *"scaled independently"* — is exactly the workload that turns this
into the serial resource (hw §3.4). The two-generation design promotes on read, so it cannot
relax to an `RwLock` read.

**Measure.** If it shows, the fix is small and local: shard the map by name hash into N
independent `Mutex<Generations>`, preserving the generation policy exactly. Do not do it
before the number exists — many cores is a reason to look here first, not a reason to shard
every map in the tree. The same treatment applies to `FileGrainStore`'s manifest and
loaded-segment locks. Note that hw §3.8 raises the bar for acting here: a lock on the session
path costs microseconds against a model call measured in seconds, so this only matters as a
*throughput* ceiling under concurrency, never as latency.

### 10. Timeouts, Raft timings, and failure domains have no derivation for this deployment

`quorum_timeout` and `recover_timeout` default to 2 s (`config.rs:135-137`); Raft is 1 s
election / 250 ms heartbeat (`raft.rs:223`). The envelope now supplies the topology numbers
these should be derived from: same-datacenter round trip ~200 µs, two nearby datacenters
~3 ms, across a continent ~25 ms, across continents ~90 ms.

Two distinct gaps.

**The timeouts are not derived.** A single-datacenter cluster, one spanning two nearby
datacenters, and one spanning a continent have p99.9 commit latencies two orders of magnitude
apart, and one default cannot be right for all three. The existing comment is correct that the
value belongs to the deployment and that too low is worse than slow (every timeout is
ambiguous, §7.2). What is missing is a per-topology table in
`docs/standalone-deployment.md`. Measure first: with store I/O off the executor (audit finding
1), the disk-stall feedback loop that justified conservative Raft timing is weaker, and
single-datacenter failover could plausibly be cut several-fold — but only against a measured p99.9,
never against a median (hw §3.7).

**Failure domains have no mapping.** `GranaryConfig::failure_domains` exists and defaults to
`None`, meaning every node is its own domain. On dedicated hardware there are no availability
zones: the domains are racks and datacenter sites, and nothing in the tree or the deployment guide says
how to derive one from a server's location. Without that mapping, audit finding 7's fix is
present but unused — all three replicas of a shard can still land in one rack. This is the
cheapest availability improvement available and needs no code, only a documented convention
and a worked example.

---

## The contradiction

### 11. The reference deployment is neither the reference machine nor the target platform

`k8s/harness.yaml:134-140` — a node requests 250m CPU / 256Mi and is limited to **1 CPU and
512 MiB**, with a **1 Gi** PVC; the gateway gets 500m / 256Mi (`:251-257`). Against an
envelope of 128–256 GB and dozens of cores, that is three orders of magnitude low on memory.
There is no `LimitNOFILE` anywhere, while finding 6's descriptor budget assumes a tuned one.

There is now a second mismatch on top of the first: the stated deployment target is **rented
dedicated hardware**, and the only production-shaped deployment artifact in the tree is a
Kubernetes StatefulSet. Bare metal wants a systemd unit, a documented `LimitNOFILE`, a disk
layout, and a private-network note — none of which exist.
`docs/standalone-deployment.md` covers running the binary, not running it on the target.

**Resolve both explicitly.** Either the manifest is demo-scale and says so in a comment beside
the limits, with a production sizing block next to it — or the envelope is wrong about the
deployment. And add the bare-metal deployment path the envelope actually describes, since that
is what will be run.

---

## Keep

### 12. Seven decisions the envelope confirms

Correct as they stand. What is missing is the *reason* being in the code, so the next reader
does not re-propose the disk-era alternative. hw §3.6 cuts both ways: an inherited number
needs re-deriving, and a re-derived one needs writing down.

- **`wal::open` reads the whole file and verifies every frame** (`crates/wal/src/lib.rs:549`).
  `fs::read` at 5–14 GB/s, XXH3 at 15–25 GB/s: a 1 GiB log opens in a fraction of a second, and
  the alternative — a trusted checkpoint offset — is state that can be wrong. hw §3.3.
- **`sync_all`, not `sync_data`** (`:660-668`). Already documented and measured; the envelope
  explains *why* it measured equal on a PLP device, and finding 8 is the caveat.
- **One segment file per grain** (`file_store.rs:24-27`). The module's honest statement of the
  cost — *"no two grains' appends can share an fsync"* — is the disk-era objection, and under
  hw I3 it is not the binding one. Metadata is (hw §3.5). The comment should say so, or group
  commit gets proposed again.
- **Content-addressed chunked blobs with dedup** (§7.10, `ws.rs`, `sql.rs`, `disk.rs`). Written
  as an incremental-capture mechanism; under hw I2 and §3.9 it is also the largest saver of
  uplink bytes in the tree, which is now the stronger justification — and finding 2 is the
  place it has not been applied.
- **Replay reads come from the local store** (`replicator.rs:827-838`, `file_store.rs:958-974`).
  A rehydration costs one quorum head-recovery round trip plus in-memory reads, not a round
  trip per batch. See *Corrections*.
- **The manifest replayed and held whole** (`file_store.rs:29-34`). See finding 5.
- **`InlineIo` as the default `BlockingIo`.** Not a concession — the third clause of hw §5. The
  simulator runs the production store on one logical thread, and a real pool there would trade
  seed reproducibility for a median that may not be a problem at all (finding 8).

---

## Corrections

**`REPLAY_BATCH = 256` — withdrawn.** The first pass of this review called it the clearest
instance of hw §3.1, on the reading that a 10k-record rehydration meant 40 sequential network
round trips. It does not. `QuorumReplicator::load` (`replicator.rs:827-838`) reads from
`self.local`, and `FileGrainStore::read_from` (`file_store.rs:958-974`) returns
`Reserved::ready` over the loaded segment's in-memory records. Replay is 40 in-memory reads;
raising the batch saves lock acquisitions and nothing else. The quorum round trip on the
rehydration path is `head()`, once per activation, and it is already once.

---

## Suggested order

1. **Findings 1 and 2** — the snapshot fan-out and the un-chunked state payload. Together they
   are the concurrency ceiling of the product on the target hardware, and the second one is
   applying machinery the tree already has to the one payload that lacks it.
2. **Finding 11** — the deployment artifacts. The tree currently documents one machine and
   ships another, and there is no bare-metal path at all.
3. **Finding 7** — measure the capture path. Out of order by size, because the arithmetic and
   the observation disagree by 30× and nothing here explains it.
4. **Finding 4** — delete the pending arm. Small, contained, and it removes a seam that will
   otherwise keep suggesting the design it was built for.
5. **Finding 3** — compression on the transport, measured *after* finding 2 has removed the
   bytes dedup can remove.
6. **Findings 6 and 10** — the inherited descriptor budget, and the timeout and failure-domain
   derivations the deployment needs.
7. **Findings 8 and 9** — the remaining measurements, once metrics are being collected
   somewhere.
8. **Finding 5** — nothing to do. Record the thresholds and close it.
