# Production Readiness Audit

**Date:** 2026-08-02
**Branch:** `machine-fence-per-grain-type` (at `9717fd3`)
**Scope:** the whole tree, read as a distributed system headed for production.
**Method:** specifications read first, then the critical paths verified against the code.
Every finding below was identified by reading, not by a failing test — at audit time
`cargo check` and `cargo clippy` were run and clean, and the suites were not. The fixes
recorded under **Status** were subsequently verified against the full suites.

## Status

All thirteen findings are resolved — twelve fixed, and one (9) revised after its premise
was corrected. Each landed with tests and, where it changed a normative rule, a
specification update.

| # | Finding | Status |
|---|---|---|
| 1 | Blocking I/O on the async executor | **Fixed** — `granary::blocking` seam; `ThreadPoolIo` wired into both node binaries |
| 2 | Store panics on I/O error | **Fixed** — store poisons and refuses (`StoreAck::Failed`) instead of killing the node |
| 3 | No production telemetry | **Fixed** — `GrainMetrics` seam + `AtomicGrainMetrics` (Prometheus text), collected on both nodes |
| 4 | No Raft leadership transfer | **Fixed** — Raft `TimeoutNow`; `Granary::hand_off_leadership()` for graceful drain |
| 5 | Unbounded blob fan-out | **Fixed** — `IN_FLIGHT_CHUNKS` bound, order preserved |
| 6 | Unbounded host cache | **Fixed** — two-generation bounded cache |
| 7 | No failure-domain awareness | **Fixed** — `FailureDomains` seam; voters spread per domain |
| 8 | `O(shards)` control-plane polling | **Fixed** — allocator is edge-triggered; size trigger 500 ms → 30 s |
| 9 | No group commit; file per grain | **Revised** — descriptor leak fixed; group commit withdrawn on NVMe (see below) |
| 10 | No heartbeat coalescing | **Fixed** — per-peer batched `RaftHeartbeats`, sent before per-group applies |
| 11 | Insecure-by-default transport | **Fixed** — `Encryption` enum; plaintext is now a named choice |
| 12 | Hardcoded timeouts | **Fixed** — `quorum_timeout` / `recover_timeout` in `GranaryConfig` |
| 13 | Session-id collision | **Fixed** — client-supplied ids rejected if they contain `/` |

Verification after the changes: `cargo check --workspace --all-targets` and
`cargo clippy --workspace --all-targets` clean; all 20 workspace lib suites, all 29
`granary` integration suites, all 28 `actor-simulation` conformance suites, and the
`actor-cluster` and `actor-runtime` integration suites pass. That includes the tests the
changes put most at risk: `partition_safety` and `raft_journal` (the fence and lossless
failover), `shard_split`, the deterministic seed-reproducibility and leader-storm sweeps,
the mutual-TLS handshake tests, and the hibernating facet swarms.

Two notes on what the fixes did **not** change. The blocking-I/O seam defaults to
inline, because `raft_journal.rs` runs the production `FileGrainStore` inside the
deterministic simulator and a real thread pool there would break seed-reproducibility
(actor §18.1); production opts in. And finding 2's fix covers the grain store, not
`actor-runtime`'s Raft WAL (`storage.rs:321,335,346`), which keeps the panic policy —
a voter that cannot persist its term must stop voting, which is a different and larger
design question than a replica dropping out of a quorum.

### Finding 9, revised

As written, finding 9 bundled three separate problems under one fix. Re-examined against
the deployment's actual assumption — datacenter NVMe, not network-attached or spinning
storage — they separate cleanly, and only two of them were worth acting on.

- **Loaded segments were never closed.** The strongest of the three, and the one the
  original write-up under-called. Every loaded segment holds an open file descriptor, and
  nothing ever released one: a segment entered the map on first access and left only when
  its grain was explicitly deleted. Hibernation does not help — it stops the grain's host,
  which never touches the store — so a node accumulated one descriptor per grain it had
  *ever* served. That is a hard ceiling at `RLIMIT_NOFILE`, reached at a grain count well
  below anything else here is sized for. **Fixed**: the loaded set is capped, evicting only
  entries no caller holds (`Arc::strong_count == 1` under the map lock — dropping a live
  one would let a second `Wal` open the same file and interleave appends into corruption).
- **The manifest grows without bound.** Held whole in memory, append-only, and an id
  assignment outlives the grain it names. A slow leak rather than a cliff, and the reason
  granary §7.8's *"total grain count is limited only by the shards' storage"* is not yet
  true. Still open, and cheap whenever it matters.
- **No group commit — withdrawn.** This was the part that required the per-shard log
  redesign, and the argument for it does not survive the hardware assumption. It rested on
  fsync being expensive enough that amortizing it dominates: true at ~1 ms on
  network-attached storage, and the reason every disk-era database does group commit. On
  datacenter NVMe an fsync is tens of microseconds, and with power-loss protection the
  cache flush is close to free; the device also absorbs concurrency natively. With store
  I/O already off the executor and on a pool (finding 1), the ceiling is the pool width and
  syscall cost, not the flush — and a layout redesign does not move either. The honest
  conclusion is that the remaining cost of file-per-grain on NVMe is filesystem *metadata*
  (millions of inodes, large directories), which is a real but different problem and not
  one group commit addresses.

The recommendation is therefore to **not** do the per-shard segmented log. If write
throughput later becomes a measured constraint, extend `crates/granary/benches/store.rs`
to measure concurrent-grain append throughput first, so any redesign has a number to beat
rather than an argument.

Two of the fixes were caught being wrong by the tests before they landed, which is worth
recording. The blocking-I/O poison check was initially placed in `segment()` and missed
the blob area, which a poisoned-store test caught. And the first cut of heartbeat
coalescing sent the batches *after* each group's output was applied, which made heartbeat
latency scale with the number of groups — the exact property the finding is about; the
`disk_swarm` hibernation sweep failed on it, and the fix (batch first, apply after) is
both correct and what the spec now requires.

## Verdict

*Everything from here down is the audit as originally written, before the fixes. It is
kept as the record of what was found and why; see **Status** above for what changed.*

The core distributed-systems idea is right, and better than most attempts at it. The
operational substrate around it is not yet production-grade. Nothing here breaks the
safety argument — these are the things that turn a correct design into a 3am page:
blocking I/O on the reactor, panic-on-`ENOSPC`, no telemetry, no graceful leadership
handoff, and unbounded buffers.

## What is strong

- **Consensus is off the data path.** A per-grain quorum append fenced by the shard's
  leadership term, recovered by per-slot read-repair instead of leader-completeness over
  a shared log. `O(shards) + O(types)` groups, never `O(grains)`, none carrying data.
- **The fence is a term, not a clock** (granary §8.1) — the correct call, with the
  reasoning written down rather than assumed.
- **The term-bounded rollback** (`crates/granary/src/replicator.rs:488-499`) is the
  subtlety most implementations get wrong: a term-blind rollback of a failed append would
  silently delete a *newer* leader's committed records from this replica and shrink a
  committed write's durability below a quorum. It was seen and bounded.
- **Tenant isolation by edge-side key prefixing**
  (`crates/harness-gateway/src/auth.rs:31-57`): the principal charset excludes `/`, so the
  prefix splits back unambiguously and a client can never name outside its namespace.
  Small, load-bearing, correct.
- **Verification.** 523 test functions plus seeded deterministic simulation with fault
  injection the specs *require* to fire, and an invariant catalogue bound to real test
  files by a drift test.

---

## Findings, by production risk

### 1. All filesystem I/O runs inline on the async executor

`grep -rn "spawn_blocking\|block_in_place" crates/*/src` returns **zero hits**.
`crates/wal/src/lib.rs:686` does `write_all` + `sync_all` synchronously inside
`append_batch`; `crates/granary/src/file_store.rs` has 34 direct `fs::`/`File::` sites.
All four binaries are `#[tokio::main]` multi-thread.

Every fsync parks a tokio worker. Raft heartbeats are 250 ms against a 1 s election
timeout (`crates/actor-cluster/src/raft.rs:222`); the quorum append deadline is 2 s
(`crates/granary/src/replicator.rs:72`). One slow disk stalls workers, which misses
heartbeats, which starts spurious elections across every shard group that node leads,
which churns terms, which steps grains down (granary §6 steps 5–6), which forces mass
rehydration — itself more I/O. That is a positive feedback loop, and the most likely
origin of a real outage in this design.

**Fix.** Route store and WAL calls through `spawn_blocking`, or better a dedicated
per-store I/O thread fed by a channel. The `Reserved`/`durable()` split in
`crates/granary/src/store.rs` is already the seam to hang this on.

### 2. The store panics on any post-open I/O error

Policy stated at `crates/granary/src/file_store.rs:62-67`; panics at `:267`, `:358`,
`:399`, `:485`, `:588`, `:615`.

`ENOSPC`, one `EIO`, or fd exhaustion kills the process — and the node was leading N
shards, so all N fail over at once. A local, containable fault becomes a cluster event.
Worse: `file_store.rs:20-27` notes a node's file count tracks its grain count, so `EMFILE`
and `ENOSPC` are *expected* operating conditions at scale, not exotic ones.

**Fix.** Make `GrainStore` writes fallible; map a write error to `Unavailable` for that
grain, mark the store degraded, and shed leadership (finding 4). Reserve panic for
violated internal invariants, not for the environment.

### 3. No production telemetry

No `tracing`, no metrics, no counters anywhere in the tree. The only sink is
`StderrEvents` (`crates/harness-standalone/src/node.rs:527`), which `eprintln!`s
membership transitions. Granary §13 says metrics *should* include per-shard commit
latency, log size, active-grain count, shard count, and leadership changes. None exist.

The system cannot be operated. Every other finding here is invisible until it is an
outage. The `Event` stream is the wrong instrument for this job: it is built for the
simulator's checkers and emits per-message `Enqueue`/`DispatchStart`/`DispatchEnd` — the
wrong granularity and cardinality for production.

**Fix.** A `MetricsSink` beside `EventSink` carrying a small fixed set — append latency by
outcome, recovery latency, leadership changes, active grains, store bytes, mailbox depth,
fsync latency — plus `tracing` spans on the append and recovery paths.

### 4. No Raft leadership transfer, so a rolling restart is a failover storm

`crates/actor-cluster/src/raft.rs` implements Vote, AppendEntries, InstallSnapshot,
AddVoter, and RemoveVoter. There is no `TimeoutNow` (Raft §3.10) and no transfer path.

Draining a node that leads 1,000 shards means 1,000 independent election timeouts (≥1 s
each, with dueling candidates) plus, per grain, a fresh quorum head-recovery round-trip.
Rolling upgrades are the most frequent production operation, and this design makes them
the worst case. The actor membership layer has `draining`/`leaving` states; they never
reach the shard groups.

**Fix.** Implement `TimeoutNow` and drive it from the drain path, so a departing leader
hands each shard to a caught-up replica before exiting.

### 5. Unbounded fan-out and buffering in the facet blob paths

`crates/granary/src/facet_blobs.rs:48` — `put_chunked` calls `join_all_results` over
*every* chunk with no `buffer_unordered` or semaphore. `get_concat` collects all parts
into a `Vec` and then `.concat()`s them.

A 16 GiB disk image (`crates/granary/src/disk.rs:67`) at 1 MiB blocks is 16,384 concurrent
quorum puts, each fanned to R replicas. `get_concat` materializes an artifact twice in
RAM. The disk facet's own restore (`disk.rs:245`, `apply_manifest`) is correctly
sequential and bounded; it is SQL and workspace that go through `get_concat`.

**Fix.** `buffer_unordered(k)` on the puts; stream `get_concat` into the target file the
way `apply_manifest` already does.

### 6. Unbounded client-side host cache

`crates/granary/src/grainref.rs:85` — `hosts: Mutex<HashMap<GrainName, ActorRef<Host<G>>>>`.
Entries are removed only when leadership moves (`:121`) or a call fails (`:135`). No cap,
no TTL.

The gateway is one long-lived process fronting every tenant. Touch N distinct session
names and the map grows to N and never shrinks — and grains hibernate after 10 s, so most
entries are stale handles held alive purely by the cache. Unbounded memory on the tier
meant to scale horizontally.

**Fix.** An LRU with a size cap. A miss costs one gateway round-trip, which is the path
granary §5.4 already specifies.

### 7. Placement has no failure-domain awareness

`crates/actor-cluster/src/placement.rs:86` — `top()` ranks purely by rendezvous weight,
and `select_replicas` takes the top R.

Nothing prevents all R replicas of a shard landing in one rack or availability zone. For a
design whose entire safety story is quorum intersection, this is the gap that turns a
survivable zone event into data unavailability on a cluster that still looks healthy.

**Fix.** Rendezvous-rank *within* domains: walk domains in weight order and take one node
per domain until R. Purity and minimal movement are preserved; only the candidate
iteration changes.

### 8. The control plane is `O(shards)` polling at 10 Hz, per grain type

Eight loops are launched at `crates/granary/src/shardmap.rs:413-470`, and
`ALLOCATE_INTERVAL = 100ms` (`:54`) drives the allocator, reconcile, migrate, split, merge,
and leader-watch loops. `allocator_loop` (`:1020`) recomputes `select_replicas` — a
rendezvous sort over all voters — for **every** shard on **every** tick, changed or not.
`split_trigger_loop` (`:1571`) calls `shard_bytes`
(`crates/granary/src/file_store.rs:935`) per led shard every 500 ms, and that `stat`s
every grain's segment *and* does a `read_dir` plus `metadata` over every blob in every
grain's blob directory — inline, compounding finding 1.

At the default 4 shards this is invisible. At 10k shards the allocator alone is roughly
10⁵ sorts per second on the map leader. The size trigger defaults off
(`shard_target_bytes: 0`), which spares the worst of it — but split/merge is the stated
elasticity mechanism, so enabling it is the intended path.

**Fix.** Make these edge-triggered. Cache a voters epoch and skip the allocator sweep when
`cluster_voters()` is unchanged; track shard bytes as a counter in the store rather than
restating the filesystem every tick.

### 9. No group commit; one file and one fsync per grain

`crates/granary/src/file_store.rs:24-27` states it plainly: *"a grain owns a file, so a
node's file count tracks its grain count, and no two grains' appends can share an fsync."*
The honesty is good; the consequence is a node capped near one fsync-latency per
grain-append (~1k/s on network-attached storage), with no batching lever available.

This also contradicts granary §7.8's *"Total grain count is limited only by the shards'
storage"*. The real limits are inodes, open fds, and the manifest, which
`file_store.rs:29-34` says is *"replayed and held whole"* in memory.

**Fix.** A per-shard segmented log with a per-grain index, plus a batching writer that
coalesces concurrent appends into one fsync. This is also what makes finding 1's offload
cheap.

### 10. No heartbeat coalescing across Raft groups

`crates/actor-cluster/src/raft.rs:222` — a 250 ms heartbeat, one group per shard. A node
leading G shards to R−1 followers sends `4·G·(R−1)` messages per second of pure heartbeat.
At G=5,000 and R=3 that is 40k msg/s before any work happens. This is the multi-raft
ceiling that CockroachDB and TiKV both had to solve with store-level coalescing, and it is
the hard cap on how far split/merge can actually take the design.

### 11. Insecure-by-default transport

`crates/actor-runtime/src/transport.rs:126-128` — `tls: Option<TlsConfig>`, and `None`
runs plaintext. The `cluster_secret` is sent in the `Hello` frame (`:147-156`), and
`allowlist: None` admits any peer that clears the secret check.

With the default configuration, the shared secret that authorizes cluster membership
crosses the wire in clear. Anyone who observes one handshake can join and become a shard
replica. The gateway already gets this pattern right for auth — insecure mode is gated to
loopback (`crates/harness-gateway/src/auth.rs:71`). The transport should fail closed the
same way.

**Fix.** Require `TlsConfig` unless an explicit `--insecure-transport` flag is passed.

### 12. Hardcoded, non-configurable, ambiguous timeouts

`QUORUM_TIMEOUT` and `RECOVER_TIMEOUT`, both 2 s
(`crates/granary/src/replicator.rs:72,79`), are compile-time constants. Every timeout is
*ambiguous* by design: it steps the activation down and forces a full rehydrate. Under
cross-zone latency or a large batch, transient slowness becomes activation churn. These
belong in configuration, set well above p99.9 commit latency.

### 13. Same-tenant session-id collision (minor)

`crates/harness-gateway/src/auth.rs:55` — the principal charset correctly excludes `/`,
but the client-supplied `session` segment is unconstrained, and delegated children are
named `P/session/t-1/c-2`. A client can name a session `demo/t-1/c-2` and land on its own
sub-agent's grain. Cross-tenant isolation holds; this is same-tenant integrity only.

**Fix.** Apply the same charset rule to the client-supplied session segment.

---

## The architectural judgment

What S3 and Dynamo actually taught is not "build a rich substrate". It is: pick one small
set of guarantees, make them unbreakable, make them boring to operate, and only then grow
the surface.

This tree is roughly 95k lines of Rust across 22 crates and 9 normative specifications. On
top of a substrate whose own failure modes (findings 1–4) are not yet operable, it already
carries seven storage facets (kv, sql, disk, workspace, alarm, workflow, blobs), a
content-addressed blob store with no in-tree consumer, two model providers, a TUI, a VS
Code client, and a Firecracker/SSH machine layer.

[design-principles.md](design-principles.md) argues against exactly this — "complexity
accumulates one small compromise at a time" — and the compromise being accumulated here is
breadth, not local hacks.

The recommendation is to freeze the facet surface, make the substrate operable, and then
prove it at a scale that actually exercises split and merge. Until finding 4 lands, a
rolling upgrade is not possible; until finding 3 lands, there is no way to tell whether any
of this holds under load. Neither is an interesting engineering problem, which is precisely
why they get deferred and precisely why they must not be.

## Suggested order

1. **Findings 1, 2** — I/O off the executor, stop panicking on the environment. These are
   the outage generators.
2. **Finding 3** — telemetry. Nothing else can be fixed while it is invisible.
3. **Findings 6, 5** — memory bounds. Cheap, and they prevent the OOMs.
4. **Findings 4, 7** — leadership transfer and failure domains. These two are what make
   the system deployable.
5. **Findings 9, 8, 10** — throughput and the multi-raft ceiling, before shard counts grow.
6. **Findings 11, 12, 13** — security posture and timeout configuration.

## Build-hygiene note

The workspace uses `members = ["crates/*"]`, so any stray directory under `crates/` — a
tool's scratch directory, an editor artifact — is treated as a member crate and fails the
build with a confusing "failed to read Cargo.toml". Consider an explicit member list, or
`crates/*/`.
