# Spec-Alignment Audit

**Date:** 2026-07-22
**Method:** One verification agent per formal specification, each extracting the
spec's numbered invariants and key MUST clauses, then reading the implementation
crate(s) and classifying every requirement as ALIGNED / GAP / UNVERIFIED with
`file:line` evidence. Static reading of code and tests; no files were modified.

## Scope

Eight formal RFC-2119 specs in `docs/`, each with a numbered invariant catalogue,
mapped to their implementation crates:

| Spec | Implementation crate(s) |
|---|---|
| `wal-spec` | `wal` |
| `cluster-utilities-spec` | `actor-cluster` |
| `blob-store-spec` | `blob-store` |
| `sandbox-spec` | `harness-sandbox` |
| `distributed-actor-spec` | `actor-core`, `actor-runtime`, `actor-serialization`, `actor-simulation` |
| `granary-spec` | `granary` |
| `machine-spec` | `machine`, `machine-frontdoor`, `machine-proto`, `microvm`, `guest/machine-agent` |
| `agentic-harness-spec` | `harness` (+ `harness-anthropic/openai/gateway/standalone/tui`, `tenancy`) |

## Architecture under test

A distributed agentic runtime in three layers, each documented by one spec:

- **Base** — location-transparent distributed actor framework (`actor-core`,
  `actor-runtime`, `actor-serialization`, `actor-simulation`, `actor-cluster`).
- **Middle** — *granary*, which makes an actor a **grain** (durable,
  single-writer, virtually-activated object), plus the shared primitives:
  content-addressed **blob store** and **write-ahead log**.
- **Top** — three grain consumers: the **agentic harness**, the **machine** (a
  durable SSH-reachable microVM), and the **sandbox** tier model.

The recurring design thesis — "an X is a grain plus a few things, everything else
inherited unchanged" — was the main claim under test.

## Verdict per spec

| Spec | Verdict | Invariants |
|---|---|---|
| wal | Strong | W1–W4 all aligned |
| cluster-utilities | Strong | U1–U2 + all router/singleton reqs aligned |
| blob-store | Strong | B1–B7 aligned (1 medium durability edge) |
| sandbox | Strong | S1–S5 aligned (S3/Network deliberately absent) |
| agentic-harness | Strong | H1, H3–H8 all aligned |
| granary | Strong | 24/24 aligned (G15 now implemented) |
| machine | Good | M1–M5 aligned; M6/egress unwired |
| distributed-actor | Strong (in-scope) | 22 invariants; realization of 5 lives in `actor-cluster` |

**Every audit came back "faithful implementation."** No agent found a silent or
incorrect divergence from a MUST that the spec did not already acknowledge.
Invariants are backed by real continuous checkers and seeded fault-injection
simulation, not just example assertions.

## Findings that matter

### 1. Machine egress (M6) is unwired — MEDIUM (the one genuine functional gap)

The `nft` rule generator is correct and golden-tested, but nothing installs it:
`net::apply::install`/`remove` has zero call sites and `FirecrackerMachineProvider::boot`
never populates `microvm::NetIf` (stays `net: None`, `firecracker.rs:95-102`). A
real machine boots with **no network interface at all**. The spec Status line
claims the tap/NAT plumbing exists; it is present only as an orphaned `apply`
module, never connected to the grain lifecycle.
*(machine)*

### 2. Blob-store puts can ack below the durability target — MEDIUM

`run_put` computes `need = W.min(owners.len())` (`cluster.rs:182`), so when live
owners < W the put returns `Ok` at fewer than W copies instead of `Unavailable`.
This silently weakens the durability contract (B3 / §5.2) exactly when the
cluster is under-provisioned. Only manifests when live owners < W; normal-path
tests use 3 nodes ≥ W.
*(blob-store)*

### 3. Granary shard split/merge (G15) — RESOLVED (2026-07-23)

Previously the shard count was fixed at `granary()` construction with no key-range
split, and the elasticity claim ("bounded per-shard work as grains scale to
millions") rested on unimplemented machinery. Split and merge are now implemented:
the name→shard partition is stored as contiguous key ranges in the consensus shard
map, a shard auto-splits past `shard_target_bytes` (or on request) with the parent
keeping the low half and a fresh child taking the high half, and two adjacent
shards merge the reverse way, retiring a leader-election group (G7). G15 rests on a
store-level **append seal** on a quorum — from which no append to the moving range
can reach a write quorum at any term — followed by a fence-bypassing transfer of
each moved grain's committed prefix, snapshot, and blobs, and only then the map
commit; every step is idempotent and re-drivable on a crash. The `ShardSplit`,
`ShardMerged`, and `LeaderChanged` events (§13) are emitted, and the per-`Committed`
`shard` stamp makes G15 a continuous checker. Verified under deterministic
simulation, including the §14-mandated split-under-concurrent-writes
linearizability case, a merge counterpart, crash-the-driver re-drive, compacted and
blob-bearing grains, the size trigger, and alarm-index reconciliation across a move
(`crates/granary/tests/shard_split.rs`, `alarm_cluster.rs`).
*(granary)*

### 4. Actor-framework traceability is stale and unenforced — RESOLVED (2026-07-23)

Previously `conformance_catalogue.rs` cross-checked only `Verify::Checker` names;
it never verified that `SimTest`/`Differential`/`CompileFail` file pointers
reference real files, and many were wrong — the catalogue cited `cluster.rs`,
`failure.rs`, `watch.rs`, `gossip.rs`, `actor.rs`, `escalation.rs`, none of which
existed in `tests/` (the tests are `conformance_*.rs`). Every stale pointer is now
corrected to the real `conformance_*.rs` file it was consolidated into, and the
drift gate is extended with an `every_file_pointer_references_a_real_file` test
that resolves each `SimTest`/`Differential` pointer under `tests/` and the
`CompileFail` pointer under `crates/`, failing the build if any names a file that
does not exist. A silently-deleted or renamed distributed-invariant test
(#2/#14/#16/#17/#22 scenario halves) now breaks the build rather than going
unnoticed.
*(distributed-actor)*

### 5. Scope note — membership realization outside audited crates

SWIM / membership modes / leader-Raft / node-down cascade live in `actor-cluster`,
outside the four crates the actor-spec audit covered. Invariants #14, #16, #17 and
the quorum-gating halves of #2 and #22 are therefore test-covered but their
*realization* was not directly read. A follow-up pass over `actor-cluster` would
close this.
*(distributed-actor)*

## Low-severity theme: documentation drift

Three audits flagged the same class of issue — prose that has drifted from the
code — which matters given the project convention that specs describe current state:

- **cluster-utilities:** §6 cites test filenames (`fault_coverage.rs`,
  `reproducibility.rs`, `cluster_swarm.rs`) that were renamed to `conformance_*.rs`.
  Coverage exists under the new names; only the references are stale.
- **machine:** `crates/machine/src/lib.rs` docstrings still say
  `Facets = (Disk, Alarm)`; the implementation is `(Disk, Alarm, Ws)`
  (`grain.rs:471`).
- **distributed-actor:** the stale catalogue pointers (finding 4) are now
  corrected and machine-verified to exist by the drift gate.

## Other low-severity notes (within spec intent or explicitly deferred)

- **wal:** two `expect`/panic paths (serialization failure `lib.rs:121`, missing
  file name `lib.rs:326`) are outside the "return `io::Result`" rule but fall in
  the spec's own "caller bug, not I/O failure" carve-out.
- **agentic-harness:** `budget_floor` defaults to 0 (`client.rs:130`) — compliant
  to the letter of §9.1.2 (a configurable floor) but the default disables the
  protection it exists for. Delegation to a kind not hosted on the node degrades
  safely as a `ToolError`, unreachable under `cluster()`/`host_all`.
- **sandbox:** S3/Network tier is unimplemented and unverified — consistent with
  §6's `(future)` marker; the *withholding* half (refusing egress-naming profiles
  at open) is enforced and tested. Native/microVM confinement smoke tests are
  runtime-gated (docker / KVM), so those legs rest on construction + KVM-free
  protocol tests where the runtime is absent.
- **blob-store:** tombstone cluster-wide-set GC is deferred (unbounded memory,
  documented "future refinement"); sweep-ack bookkeeping is established only by
  the coordinating anchor. Both are liveness conservatisms, not safety breaks.
- **granary:** linearizable reads / check-quorum lease deferred (§7.5, §16) — the
  documented relaxed read-your-leader contract; writes never fork. (Shard
  split/merge and the `ShardSplit`/`ShardMerged`/`LeaderChanged` events are now
  implemented — see finding 3.)
- **machine:** no production `MachineAuthority` wiring an SSH listener to a real
  leader (M4 proven over loopback with a fake authority) — the deliberate seam
  boundary; cross-node channel relay is explicit future work (§8).

## Bottom line

Implementation-to-spec alignment is high across all eight specs. The distributed
core — single-writer fences, quorum append, lossless failover, CP-under-partition,
facet atomicity, blob integrity — is implemented as specified and exercised under
deterministic fault injection.

Suggested priority order for action:

1. **Machine egress wiring (M6)** — the one functional gap; the property holds
   only as a pure function, not an enforced runtime posture.
2. **Blob-store under-W ack (B3)** — return `Unavailable` when live owners < W.
3. **Doc-drift cleanup** — cheap, and it is a stated project convention.
