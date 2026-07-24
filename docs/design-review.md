# Design Review — Distributed Actor Framework / Harness

*Reviewed against [Software Design Principles](software-design-principles.md), distilled from
John Ousterhout, "A Philosophy of Software Design." Graded as CS190 studio work: by the
management of complexity above all else.*

## 1. Overall assessment

This is strategic programming, and unusually disciplined about it. The core — `actor-core`
and the runtime seam — is exemplary studio work: deep modules, real information hiding,
errors defined out of existence, and comments that carry rationale the code cannot.
Complexity is trending *down* in the sense that matters — the design is actively maintained,
specs are kept as current-state descriptions, and audit findings get resolved rather than
papered over — but it is trending *up* locally in the three hardest crates (`granary`,
`actor-cluster`, `harness`), where the distributed protocols are accreting repetition and a
few genuine god-functions faster than they're being consolidated. The gap between the
pristine center and the eroding edge is the whole story of this codebase.

## 2. Red flags, ranked by impact

### ① Duplicated durability protocol — repetition + information leakage *(resolved)*
`store.rs:625` (`MemoryGrainStore`) vs `file_store.rs:477` (`FileGrainStore`). The fence/seal
write-guard — "check the append bound before the fence can bump; `Transfer` skips the fence;
hold the bump under the segment lock" — was re-implemented in both stores, comments
near-verbatim. This was the most dangerous red flag in the codebase, because the two copies
encode a *correctness* invariant and the compiler cannot tell you when they diverge. Change
which write kinds bypass the fence in one store and the other is now a silent safety bug. One
decision, two modules.

**Resolved.** The composing *policy* now lives once, in a `WriteGuard` trait (`store.rs`): its
default methods `guard_record` (append-bound-before-fence, `Append`-only seal, `Transfer`
skips the fence) and `guard_snapshot` (fence-only — a snapshot is not append-bounded) own the
which-kinds-bypass decision, and each store supplies only the two primitives that legitimately
differ — `sealed` and `bump_fence` (the file store fsyncs each bump; the memory store keeps it
in a map). The six call sites now delegate to the shared guard; the rationale comments moved to
the trait, one copy. Note the guard did *not* fold into `GrainRecords` as first suggested: the
fence and seal are per-*shard* state that lives in the store, while `GrainRecords` is the
per-*grain* segment, so a trait over the two stores — not the per-grain algebra — is where the
shared secret belongs.

### ② God-functions that must be held whole in the head
`recover_with` (`replicator.rs:503`, 435 lines) does the fence-read phase, per-slot
highest-term merge, quorum accounting, write-back, snapshot adoption, and blob migration in
one body — no sub-step is independently readable or testable. `receive_loop` (`system.rs:850`,
~370 lines) is a 17-arm frame match with six copy-paste Raft arms, each re-threading
`clock.now()` + `entropy` into `apply_raft_output`. Cost is cognitive load and
unknown-unknowns: adding a frame or a recovery phase means surgery inside a monolith. A
`Frame::dispatch(&system)` and a handful of named recovery phases would cut both sharply.

### ③ Repeated protocol skeletons — three families
The same non-trivial shape is written many times:
- The background driver-loop preamble (weak-upgrade-or-exit + leadership gate) across *seven*
  loops in `shardmap.rs` (`allocator_loop:981` … `leader_watch_loop:1556`).
- The "propose to leader / forward / fan-out to voters" logic *three* times (`system.rs:473`,
  `1200`, `1309`).
- The four operator methods `admit`/`drain`/`resume`/`decommission` (`system.rs:327`).
- The resolve-retry skeleton in both `grainref.rs:289` and `466`.
- The begin-turn trio and sandbox-teardown in `harness/agent.rs` (`601`/`949`, `1563`/`1656`).

None is catastrophic alone; together they mean the lifecycle-teardown contract lives in seven
places and the forwarding rule in three. Each is a change-amplification tax.

### ④ Semantics split across the module boundary — information leakage
`membership.rs:234` vs `system.rs:1398`. `MembershipCommand` and its codec live in
`membership.rs`, but the load-bearing "committed command → `MemberStatus`" mapping
(`Drain → Draining`, `Leave|Down → Down`) lives in `system.rs`. Adding a command touches the
enum, encode, decode *and* a match in a different file. What a command *means* is in neither
module alone. A `MembershipCommand::effect() -> (NodeId, MemberStatus)` consolidates the
secret.

### ⑤ Special-general mixtures
Three real ones:
- The `GrainStore` trait (`store.rs:136`) welds a fenced term-ordered record log to an
  orthogonal unfenced content-addressed blob area — 7 of ~24 methods are blob concerns every
  implementor must supply, coupling two unrelated secrets behind one wide interface.
- `MultiRaft` sells itself as a generic engine but `apply_raft_output` hard-branches on
  `GroupId::CONTROL` to decode membership (`system.rs:1370`), and `create_group` suppresses
  jitter specifically to keep the control-only entropy draw order stable — the general
  mechanism carries knowledge of its one special caller.
- `buggify` (see ⑥).

### ⑥ `buggify` in the `Entropy` trait — special-general mixture, minor but notable
`runtime.rs:135`. A fault-injection hook — purely a simulation concern — sits in the
production randomness interface, forcing `entropy.rs` and `lib.rs` to both document "buggify
stays off." The `default → false` guard makes it cheap and defensible, and it is probably
worth keeping, but it is the one blemish on an otherwise pure seam: three modules share the
fault secret.

### Nonobvious code worth a comment-audit
- The snapshot-index arithmetic `log[index - snapshot_index - 1]` recurs at five sites in
  `raft.rs` (`485`, `783`, `824`, `1110`, `1123`), each independently responsible for the
  off-by-one — the "log is offset by the compacted prefix" invariant wants one indexing type.
- `SimNetwork::route`/`reserve_pair_slot` (`transport.rs:415`) are conjoined by entropy-draw
  ordering: the fast path *must* draw nothing or byte-reproducibility silently breaks, with no
  compile error to catch a wrong edit.
- One stale comment: `supervision.rs:27` says "jitter is a follow-up," but `host.rs:814`
  already implements `jittered`.

## 3. What's done well

This is where the grade comes from.

- **`ActorRef` (`refs.rs`) is a genuinely deep module** — `locate()` consolidates the
  local/dead/remote secret in one place, and a single `ask` hides codec, transport,
  correlation, timeout, and ref-rebinding; the caller writes `actor.ask(msg).await?` and never
  learns locality exists.
- **The runtime seam is real and clean** (verified): `Clock`/`Entropy`/`Spawner`/`Transport`/
  `RaftWAL` are defined once and implemented twice, and both `TcpCluster` and `SimCluster`
  instantiate the *identical* generic with only four type parameters differing. Zero
  `cfg(simulation)`, zero if-simulation branches in production; the determinism boundary is
  enforced mechanically by `clippy.toml` banning host time/RNG/thread APIs and crossed in
  exactly one greppable `#![allow]`. Textbook "generalize the interface, specialize the
  implementation."
- **Errors are defined out of existence** — `resolve` is infallible because every `ActorId`
  is well-formed; `CallError` is a deliberately exhaustive (non-`#[non_exhaustive]`)
  transport-error set the caller matches once, kept distinct from application failures that
  live inside `M::Reply`.
- **The executor** (`host.rs run_actor`) models an actor's life as a small
  `Step`/`Decision`/`End` state machine so supervision policy lives in exactly one procedure
  instead of once per phase.
- Other standouts: `placement.rs` (pure version-pinned rendezvous hashing), `correlator.rs`
  (one small generic unifying two formerly-parallel waiter maps), `FileRaftWAL` (4-method
  infallible interface hiding torn-write recovery), and `GrainRecords` (the lock-free record
  algebra both stores *should* fully share).

## 4. Highest-leverage changes

Not a laundry list — the three redesigns that most reduce overall complexity:

1. ~~**Collapse the fence/seal write-guard into `GrainRecords`.**~~ *Done* — converted the
   silent-divergence correctness hazard into a single deep function, though behind a shared
   `WriteGuard` trait over the two stores rather than inside `GrainRecords` (the fence and seal
   are per-shard store state, not per-grain segment state). See red flag ①.
2. **Table-drive the two dispatch/recovery monoliths.** A `Frame::dispatch(&system)` for
   `receive_loop` and named phases for `recover_with` turn "hold 400 lines in your head" into
   "read one phase." The single biggest cognitive-load reduction available.
3. **Extract the three repeated protocol skeletons behind one helper each** — a driver-loop
   combinator (`run_leader_loop(interval, |shard| …)`), one `apply_membership_op`, one
   `propose_or_forward`. Kills the seven-place teardown contract and the three-place
   forwarding rule in one sweep.

Do these three and the edge crates start looking like the core.

## 5. Grade: **A−**

The center of this project is A+ work — `actor-core`, the runtime seam, and the error model
are lecture-quality examples of deep modules, information hiding, and defining errors out of
existence, and the simulation seam is a genuinely hard idea executed cleanly. What holds it
back from an unqualified A is that the strategic discipline visibly thins out in the newest,
hardest layers: a durability invariant that *was* copy-pasted across two stores where
divergence is a silent bug (now collapsed behind a shared `WriteGuard` trait — red flag ①),
two 400-line functions that resist being read in parts, and a fistful of protocol
skeletons repeated three-to-seven times. These are exactly the tactical compromises the
philosophy warns accumulate one at a time — and the remarkable thing is how *few* of them
there are across 90K lines, which is why this is an A− and not a B+. Tighten the three
redesigns above and the edge will match the center. Excellent work; finish the job.
