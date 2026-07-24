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
papered over. The three hardest crates (`granary`, `actor-cluster`, `harness`) *were* the
story of this codebase — the distributed protocols there had accreted repetition, a few
genuine god-functions, and semantics leaking across module boundaries faster than they were
being consolidated. That gap has now been closed: every ranked red flag (①–⑥) is resolved — the
special-general mixtures of ⑤ and ⑥ split apart on top of the ①–④ fixes — and the edge crates read
much closer to the pristine center. Even the lowest-severity tail — the comment-audit list — is now
closed: the raft snapshot-offset off-by-one lives in one `slot()` method, and the two nonobvious
entropy/jitter couplings carry the comments that were missing. What dominated the original review —
change-amplification and hold-it-all-in-your-head hazards — is gone.

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

### ② God-functions that must be held whole in the head *(resolved)*
`recover_with` (`replicator.rs:503`, 435 lines) does the fence-read phase, per-slot
highest-term merge, quorum accounting, write-back, snapshot adoption, and blob migration in
one body — no sub-step is independently readable or testable. `receive_loop` (`system.rs:850`,
~370 lines) is a 17-arm frame match with six copy-paste Raft arms, each re-threading
`clock.now()` + `entropy` into `apply_raft_output`. Cost is cognitive load and
unknown-unknowns: adding a frame or a recovery phase means surgery inside a monolith. A
`Frame::dispatch(&system)` and a handful of named recovery phases would cut both sharply.

**Resolved.** Both are now readable in parts. `receive_loop` is a thin shutdown-gated
loop delegating to a flat `dispatch_frame` table where every arm is a single call; the five
copy-paste Raft arms collapse behind one `drive_group` helper that owns the shared shape
(resolve group, draw `clock.now()` + `entropy` once, apply the output — so a wrong edit to
the threading can no longer diverge across arms), the four SWIM arms share an `absorb_gossip`
preamble, and each remaining frame family is its own `handle_*` function. `RaftPropose` stays
distinct (it never calls `apply_raft_output`). Dispatch lives in `system.rs`, not on `Frame`
in the wire crate — the handlers reach into `host`/`membership`/`receptionist`, so moving
them would invert the layering. `recover_with` is now a readable spine — leadership gate →
`fence_read` → quorum policy → merge → snapshot adopt → `write_back` — with the read and
write-back phases extracted as named methods carrying their own rationale (the "435 lines"
figure was an overcount: the function is ~146 lines, 503–648; blob migration lives in sibling
methods, not here). Both refactors are pure structure moves — the entropy/clock draw order is
unchanged, verified by the full simulation conformance sweep.

### ③ Repeated protocol skeletons — three families *(resolved)*
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

**Resolved.** Each skeleton now has one home. `driver_loop` (`shardmap.rs`) owns the seven
loops' teardown contract — the tick sleep and the weak-upgrade-or-exit — while each loop keeps
its own leadership gate in its body: the gate was never the shared secret (it genuinely varies —
whole-tick, per-shard-group, per-term, and two loops run their proposer phase *ungated*), the
exit contract was, and it now lives once. `propose_or_forward` (`system.rs`) holds the
append/forward/fan-out rule the two full copies (`submit_proposal`, `propose_to`) duplicated;
the inbound `handle_raft_propose` stays its own function, now documented as the deliberately
*partial* form (no fan-out, honors the `forwarded` loop-breaker). `apply_membership_op`
(`system.rs`) holds the mode-dispatch skeleton `admit`/`drain`/`resume`/`decommission` shared,
each now a three-line call. `resolve_twice` (`grainref.rs`) holds the two-attempt resolve-retry
`tell` and `dispatch` layer over `resolve` — a `guard_budget` flag keeps `tell`'s "never
`Unavailable`" contract (§6) intact while `dispatch` guards its budget, so neither behavior
shifted. `begin_turn` (`agent.rs`) holds the reset → enqueue-`RunStarted` → start-records trio;
`take_sandbox` (`agent.rs`) holds the slot-swap/release/announce teardown both `release_sandbox`
and `on_passivate` ran. All are pure structure moves but one deliberate normalization:
passivation now announces `SandboxReleased` whenever a bind is left unpaired, independent of the
slot — the stronger H8 pairing `release_sandbox` already had. Verified by the full test suite.

### ④ Semantics split across the module boundary — information leakage *(resolved)*
`membership.rs:234` vs `system.rs:1398`. `MembershipCommand` and its codec live in
`membership.rs`, but the load-bearing "committed command → `MemberStatus`" mapping
(`Drain → Draining`, `Leave|Down → Down`) lived in `system.rs`. Adding a command touched the
enum, encode, decode *and* a match in a different file. What a command *meant* was in neither
module alone.

**Resolved.** `MembershipCommand::effect() -> (NodeId, MemberStatus)` (`membership.rs`) now
owns the mapping, sitting alongside `node`/`encode`/`decode` — the same module that already
holds `MemberStatus`, so it needs no new coupling. The control-group apply loop in `system.rs`
collapses to a single `command.effect()` call, and the four-arm status match no longer lives in
the applier. Adding a command is now one edit at the enum and its methods, not a second match a
file away. Pure structure move — the mapping is arm-for-arm identical; the `actor-cluster`
suite passes unchanged.

### ⑤ Special-general mixtures *(resolved; `buggify` split out under ⑥)*
Three were listed. Two welded unrelated secrets or tangled a generic mechanism with its one caller:
- The `GrainStore` trait (`store.rs:136`) welded a fenced term-ordered record log to an
  orthogonal unfenced content-addressed blob area — 7 of its 18 methods were blob concerns every
  implementor had to supply, coupling two unrelated secrets behind one wide interface.
- `MultiRaft` sells itself as a generic engine but `apply_raft_output` hard-branched on
  `GroupId::CONTROL` to decode membership (`system.rs`), `MultiRaft::new` hard-created the control
  group, and `create_group` justified its no-jitter first arm "on the control-only path" — the
  general mechanism carrying knowledge of its one special caller.
- `buggify` (see ⑥) — the fault-injection gate sat on the production `Entropy` trait.

**Resolved (both record/engine mixtures).** The blob area is now its own trait: the seven
content-addressed methods live on `GrainBlobStore` (`store.rs`) — unfenced, unordered, its secret
documented once — and `GrainStore: GrainBlobStore` keeps the fenced record log plus the three
enumeration/reclamation methods (`grains`/`remove_grain`/`shard_bytes`) that genuinely span both
areas, so they belong on the combining trait, not either half. Each store splits into an
`impl GrainBlobStore` + an `impl GrainStore`; the per-node handle stays one `Arc<dyn GrainStore>`
(the supertrait coerces), so no consumer changed. The split stopped at the blob area — the one clean
orthogonal sub-task — rather than a symmetric `RecordStore`/`GrainBlobStore`/combinator three-way,
since no consumer is record-only (`shardmap` already needs the spanning methods).

For `MultiRaft`, three coordinated moves de-specialize the engine. The generic `apply_raft_output`
no longer decodes membership — it *routes* the control group's committed entries to
`ClusterSystem::apply_membership_commits` (which owns decode → `effect()` → stamp → cascade) and
every other group's to `publish_commits`; the `if group == CONTROL` that remains is just the
specialization boundary, carrying no membership knowledge. `MultiRaft::new` now builds an empty
registry, and the cluster layer creates `GroupId::CONTROL` at startup — the engine names no group in
code. The no-first-jitter arm is re-documented as the general per-group invariant (every group draws
no entropy on its first arm, only on later resets), not "the control-only path." Membership stays
applied *synchronously in the tick* (not folded into a `subscribe_commits` subscriber) on purpose —
it runs `node_down_cascade` inline and must observe a commit in the tick it lands; routing it through
the async commit channel would shift that interleaving. Pure structure moves — the `granary` and
`actor-cluster` suites pass unchanged.

### ⑥ `buggify` in the `Entropy` trait — special-general mixture, minor but notable *(resolved)*
`runtime.rs:135`. A fault-injection hook — purely a simulation concern — sat in the
production randomness interface, forcing `entropy.rs` and `lib.rs` to both document "buggify
stays off." The `default → false` guard made it cheap and defensible, but it was the one
blemish on an otherwise pure seam: three modules shared the fault secret.

**Resolved.** `buggify` is now an inherent method on `SimEntropy` (`actor-simulation/entropy.rs`),
not a method on the `Entropy` trait. This was possible because every one of the five call sites
already holds `SimEntropy` concretely — the four simulation modules (`transport`, `registry`, the
harness model stub) store it by value, and the swarm test reaches it through
`SimSystem = LocalSystem<…, SimEntropy, …>`, so none needed the method on the shared trait. The
production `Entropy` trait (`runtime.rs`) loses the `buggify` default outright, and the two "buggify
stays off" comments in `OsEntropy` (`actor-runtime/entropy.rs`, `lib.rs`) are gone — the production
seam now carries no trace of the fault secret, which lives in exactly one place. The de-specialization
also let the doc sharpen: the "always consumes one draw, so gate it behind are-faults-configured"
contract (the subtle invariant `SimNetwork::route` depends on) now lives on the method itself. Pure
structure move — the gate body is byte-identical, verified by the full simulation conformance sweep
(the seed-controlled `flaky_service` fault test replays unchanged).

### Nonobvious code worth a comment-audit *(resolved)*
- The snapshot-index arithmetic `log[index - snapshot_index - 1]` was spelled inline at several
  mutation sites in `raft.rs` (`append_entry`, `compact`, the log-matching truncate), each
  independently owning the off-by-one even though the three read paths already funneled through
  `term_at`/`entry_at`/`suffix_from`. **Resolved.** A single `RaftState::slot(index)` now owns the
  absolute→local mapping — it is the one place the `- snapshot_index - 1` is written, and every
  read, `truncate`, and `drain` routes through it (a `drain(..=slot(index))` sheds exactly the
  entries at or below `index`). The "log is offset by the compacted prefix" invariant lives once;
  a wrong edit can no longer diverge across sites. Pure structure move — the arithmetic is
  identical, verified by the compaction/install-snapshot raft tests.
- `SimNetwork::route`/`reserve_pair_slot` (`transport.rs`) are conjoined by entropy-draw ordering:
  the fast path *must* draw nothing or byte-reproducibility silently breaks, with no compile error
  to catch a wrong edit. **Resolved (comment).** The coupling is now documented on both ends —
  `reserve_pair_slot`'s doc states it draws entropy only under the same `max_latency`/faults gate
  that makes `route` skip its fast path, and the fast-path comment points back at it — so the two
  gates are visibly a lockstep pair rather than two independent decisions.
- One stale comment: `supervision.rs` said "jitter is a follow-up," but `host.rs:814` already
  implements `jittered`. **Resolved.** The `Backoff` doc now states the executor adds equal-jitter
  (`host::jittered`) to the returned delay so simultaneous restarts desynchronize.

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
2. ~~**Table-drive the two dispatch/recovery monoliths.**~~ *Done* — `receive_loop` is now a
   thin loop over a flat `dispatch_frame` table (Raft arms collapsed behind `drive_group`, SWIM
   arms behind `absorb_gossip`, one `handle_*` per family) and `recover_with` a readable spine
   over extracted `fence_read` / `write_back` phases. "Hold 400 lines in your head" is now "read
   one arm / one phase." See red flag ②.
3. ~~**Extract the three repeated protocol skeletons behind one helper each.**~~ *Done* —
   `driver_loop` (the teardown combinator; the leadership gate stays per-loop, since it varies
   too much to fold in), one `apply_membership_op`, one `propose_or_forward`, plus `resolve_twice`,
   `begin_turn`, and `take_sandbox` for the two further skeletons issue ③ listed. The seven-place
   teardown contract and the three-place forwarding rule now live once each. See red flag ③.

With all three done, the edge crates now read much closer to the core.

## 5. Grade: **A−**

The center of this project is A+ work — `actor-core`, the runtime seam, and the error model
are lecture-quality examples of deep modules, information hiding, and defining errors out of
existence, and the simulation seam is a genuinely hard idea executed cleanly. What holds it
back from an unqualified A is that the strategic discipline visibly thinned out in the newest,
hardest layers: a durability invariant that *was* copy-pasted across two stores where
divergence is a silent bug (now collapsed behind a shared `WriteGuard` trait — red flag ①),
two long functions that resisted being read in parts (now table-driven / phase-split — red
flag ②), a fistful of protocol skeletons repeated three-to-seven times (now behind one
helper each — red flag ③), and a command's meaning split across two modules (now consolidated
in `MembershipCommand::effect` — red flag ④). These are exactly the tactical compromises the
philosophy warns accumulate one at a time — and the remarkable thing is how *few* of them
there are across 90K lines, which is why this is an A− and not a B+. With all four now
resolved — and the special-general mixtures of ⑤ and ⑥ split apart on top (the blob area behind
its own `GrainBlobStore` trait, the Raft engine de-specialized so it names no control group, and
`buggify` moved off the production `Entropy` trait onto `SimEntropy`) — and the comment-audit tail
now closed on top (one `slot()` owns the raft snapshot offset; the entropy/jitter couplings are
documented), the edge reads much closer to the center. Excellent work.
