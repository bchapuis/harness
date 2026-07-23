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
| blob-store | Strong | B1–B7 aligned (under-W ack now returns Unavailable) |
| sandbox | Strong | S1–S5 aligned (S3/Network deliberately absent) |
| agentic-harness | Strong | H1, H3–H8 all aligned |
| granary | Strong | 24/24 aligned (G15 now implemented) |
| machine | Strong | M1–M6 aligned (M6 egress now wired into boot/kill) |
| distributed-actor | Strong | 22 invariants; realization of 5 in `actor-cluster` now read (finding 5) |

**Every audit came back "faithful implementation."** No agent found a silent or
incorrect divergence from a MUST that the spec did not already acknowledge.
Invariants are backed by real continuous checkers and seeded fault-injection
simulation, not just example assertions.

## Findings that matter

### 1. Machine egress (M6) is unwired — RESOLVED (2026-07-23)

Previously the `nft` rule generator was correct and golden-tested but nothing
installed it: `net::apply::install`/`remove` had zero call sites and
`FirecrackerMachineProvider::boot` never populated `microvm::NetIf` (stayed
`net: None`), so a real machine booted with no network interface at all — the
tap/NAT plumbing existed only as an orphaned `apply` module. Egress is now wired
into the Firecracker binding's boot/kill lifecycle. `VmSpec` carries the
machine's journaled `EgressPolicy`; `FirecrackerMachineConfig` gains an optional
`EgressConfig` (uplink, cluster CIDRs, guest-address pool base and size). On boot,
`wire_egress` allocates the machine a guest `/30` from a node-local `GuestPool`,
generates its policy ruleset via `nft_ruleset`, calls `net::apply::install` to
create and address the tap and load the rules, populates `vm_config.net` with the
tap and a stable per-machine MAC, and appends the guest's `ip=` kernel arg so
`eth0` comes up before init. The returned VM holds an `EgressHandle` that calls
`net::apply::remove` and returns the pool slot on kill (and again on drop, once,
via a latch), so a dropped activation leaks neither tap nor slot. The real
plumbing stays behind `feature = "net"` on Linux; a node without that config or
without `CAP_NET_ADMIN` degrades to no NIC rather than failing the boot (net.rs's
documented posture). The pure pieces — guest addressing, the `/30` carve, the
boot arg, the pool allocator — are unit-tested (`net.rs`); the `ip`/`nft`
shell-out remains runtime-gated like the rest of the Firecracker suite.
*(machine)*

### 2. Blob-store puts can ack below the durability target — RESOLVED (2026-07-23)

Previously `run_put` computed `need = W.min(owners.len())`, so when live owners < W
the put returned `Ok` at fewer than W copies instead of `Unavailable`, silently
weakening the durability contract (B3 / §5.2) exactly when the cluster was
under-provisioned. `run_put` now sets `need = W` and refuses early with
`Unavailable` when the serving set holds fewer than W possible owners, so a put
never acks below the target — the blob it confirms always survives losing any
R − W owners. Covered by `a_put_below_write_quorum_is_refused`
(`crates/blob-store/tests/clustered.rs`), which partitions a node down to a
sub-W serving set and asserts the put is refused.
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

### 5. Membership realization in `actor-cluster` — RESOLVED (2026-07-23)

Previously the distributed-actor audit read only `actor-core`, `actor-runtime`,
`actor-serialization`, and `actor-simulation`. SWIM / membership modes / leader-Raft
/ node-down cascade live in `actor-cluster`, so invariants #14, #16, #17 and the
quorum-gating halves of #2 and #22 were test-covered but their *realization* was
never directly read. That read is now done — one verification pass per invariant over
`membership.rs`, `raft.rs`, `consensus.rs`, and `system.rs`, each classified against
the code and then adversarially re-checked. All five are **ALIGNED**:

- **#14 (convergence):** both axes of the per-member lattice are order-independent
  joins — `status_supersedes` / `reachability_supersedes` take the lexicographic max
  on `(stamp, rank)` (`membership.rs:68-78`), terminal `down` is absorbing
  (`membership.rs:704-710, 1056-1062`), first-sight is a roster union
  (`1037-1050`); leader mode stamps by the monotonic Raft log index applied in order
  (`raft.rs:929`). A post-heal reachability-poison loop self-terminates because every
  digest carries the recipient's own entry and triggers a `merge_self` incarnation
  bump (`membership.rs:957-977, 1111-1115`).
- **#16 (partition tolerance):** the only detector→`down` path is gated on
  `DowningPolicy::Timeout` (`membership.rs:858-870`); every non-gossip mode hard-codes
  `Conservative` (`373-378`) and `RegistryMode` has no `downing` field at all
  (`219-226`). A minority leader may propose `Down`, but `advance_commit` never reaches
  quorum (`raft.rs:884-901`), so a partition alone downs nobody.
- **#17 (SWIM refutation):** `merge_self` increments incarnation and re-broadcasts
  `alive@inc+1`, which supersedes any equal-or-lower suspicion cluster-wide
  (`membership.rs:1082-1094, 1111-1115`); bound by the seeded sim
  `a_suspected_node_refutes_a_false_suspicion`
  (`actor-simulation/tests/conformance_membership.rs:417`).
- **#2 (quorum half):** every write to `state.voters` is a committed, log-ordered
  entry (`raft.rs:551, 906-937, 1156`); no ad-hoc mutation path exists; `down` is
  terminal. Removal of a downed voter is operator-driven, matching the spec sentence's
  own "replacement joins with a fresh NodeId" administrative framing and §9.4.3's
  discretionary "MAY down".
- **#22 (quorum-gated control plane):** election safety rests on persisted
  one-vote-per-term (`raft.rs:972-976`) plus the current-term commit rule (`886`);
  membership transitions flow only through the committed log.

Two latent smells surfaced, neither reachable as a violating execution today:

- `advance_commit` counts `self` unconditionally with no `state.voters` membership
  check, and `drain_committed` applies `RemoveVoter(self)` without a leader step-down
  (`raft.rs:889, 922-926`). In principle a self-removed leader could commit on a
  phantom self-vote; in practice the `tick()` non-voter early-return (`raft.rs:704`)
  makes it dormant — it never replicates, so it never receives the append-replies that
  are `advance_commit`'s only remaining call site — so the path is dead. A defensive
  guard (skip the self-count when `self` is not a voter, or step down on
  `RemoveVoter(self)`) would make the safety local rather than emergent.
- A downed CONTROL voter lingers in `state.voters` until an operator calls
  `remove_voter`; this is spec-conformant (removal is the administrative half of a
  fresh-NodeId replace) and carries no fault-tolerance regression, but the phantom
  voter inflates the quorum denominator until reconciled.
*(distributed-actor)*

## Low-severity theme: documentation drift

Three audits flagged the same class of issue — prose that has drifted from the
code — which matters given the project convention that specs describe current state:

- **cluster-utilities:** RESOLVED (2026-07-23). §6 cited test filenames
  (`fault_coverage.rs`, `reproducibility.rs`, `cluster_swarm.rs`) that were
  renamed to `conformance_*.rs`. The references now point at the real files
  (`conformance_faults.rs`, `conformance_determinism.rs`, `conformance_swarm.rs`).
- **machine:** RESOLVED (2026-07-23). `crates/machine/src/lib.rs` docstrings
  said `Facets = (Disk, Alarm)`; they now read `(Disk, Alarm, Ws)`, matching the
  implementation (`grain.rs:471`) and the spec's disk-plus-workspace framing.
- **distributed-actor:** the stale catalogue pointers (finding 4) are now
  corrected and machine-verified to exist by the drift gate.

## Other low-severity notes (within spec intent or explicitly deferred)

- **wal:** two `expect`/panic paths (serialization failure `lib.rs:121`, missing
  file name `lib.rs:326`) are outside the "return `io::Result`" rule but fall in
  the spec's own "caller bug, not I/O failure" carve-out.
- **agentic-harness:** `budget_floor` now defaults to the default per-call
  `max_tokens` (`client.rs`, RESOLVED 2026-07-23) — previously 0, which was
  compliant to the letter of §9.1.2 (a configurable floor) but disabled the
  protection it exists for; the default now stops a run rather than issue a call
  that cannot fit a full-size response (set to 0 to opt out). Delegation to a kind
  not hosted on the node degrades safely as a `ToolError`, unreachable under
  `cluster()`/`host_all`.
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

1. **Machine egress wiring (M6)** — RESOLVED (2026-07-23): the tap, node NAT, and
   guest addressing are installed at boot and removed at kill behind
   `feature = "net"` on Linux, degrading to no NIC where the capability is absent.
2. **Blob-store under-W ack (B3)** — RESOLVED (2026-07-23): `run_put` returns
   `Unavailable` when fewer than W owners could hold a copy.
3. **Doc-drift cleanup** — cheap, and it is a stated project convention.
