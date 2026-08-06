# Open Work

**Status:** an index, not a specification. Nothing here is normative. Every entry points at the section that owns it, and that section is the source of truth — this page is a view over them, ordered by how much a reader has to know before the item makes sense. Nothing on this page blocks anything else on it, so the order is a reading order and not a dependency graph. A change of scope belongs in the owning spec first and here second; an item that lands should be deleted from both in the same commit.

**Each open item carries a next step**, so a session can pick one up cold. A next step is the first concrete move and what "done" looks like — file and symbol anchors, the ordering that matters, and the alternative if the first move turns out wrong. It is a starting point, not a commitment: the owning spec still decides scope, and a next step that proves misconceived should be rewritten here rather than followed. Items in §3 and §4 carry none on purpose, for the reason each section gives.

The tree carries no TODO backlog. A sweep of `crates/` finds zero `todo!()`, zero `unimplemented!()`, zero `#[ignore]`, and zero `TODO`/`FIXME`/`HACK` comments; the only two marker hits in the repository are a vendored `clang-sys` artifact under `guest/qjs-runner/target/` and an `mktemp` template in `scripts/bench-machine-cost.sh`. That is a property worth keeping: unfinished work here is *declared in a spec*, where a reader can see its cost and its blast radius, rather than dropped as a comment beside the code that would have to change. The consequence is that "what is unfinished" cannot be answered by grepping, which is why this page exists.

## 1. Compatibility policy, half enforced

Of the two checks that keep **V4**/**V5** from decaying into prose (compatibility §4), the golden corpus now exists: every registered boundary has checked-in bytes at its current revision, a test beside the format decodes them, and a completeness gate fails the build when a new boundary ships without them (`crates/compat/tests/golden_corpus.rs`, which reads the §3 registry out of the spec and holds the tree to it in both directions).

The sidecars are done. The Raft term and snapshot pointers, granary's per-shard fences and seals, and the blob-store tombstones are stamped boundaries with checked-in bytes; each reader still adopts its unstamped predecessor, and each writer now emits the stamped form. Two of them were not in the spec's list: the fence and the seal, which were the most dangerous of the set, and which the list did not mention. Note what this does *not* close — unstamped-to-stamped is not revision 1 to revision 2, so the tree still has no boundary keeping a prior definition behind a revision, which is §1.1. What compatibility §5 still lists as unstamped is §1.2 through §1.4, in its priority order; each is a place where a format change today is a migration rather than an edit.

### 1.1 No boundary has a second revision

**Mixed-version simulation** is most of the way there. The negotiation is pinned node by node, and a swarm sweeps a `Rollout` — the nemesis walking nodes one release at a time, forward and back, under the usual partitions and crashes — asserting that no node is ever sent a form of a message its build cannot read (compatibility §4, `crates/actor-simulation/tests/conformance_mixed_version_swarm.rs`).

What is left is that the sweep's revision-varying behavior is the workload's own. Every production window in `crates/` is `compat::Window::at(…, 1)`; the only multi-revision windows in the tree are inside tests. So the sweep varies a synthetic `Form` on its own message rather than a tree format: the gate is shown usable and a build ignoring it is caught, but no real format is exercised across a rollout, and no granary invariant or linearizability check is asserted across the transition.

**Next step —** this is not a project of its own; it lands with the first real wire bump, and starting it early would mean inventing a format change nobody needs. When a change to `Frame` (`crates/actor-cluster/src/protocol.rs`) actually calls for one, do **V4**'s two releases against the `WIRE` window: widen to `Window::new("actor.wire", 1, 2, 1)` in the first, flip to `Window::new("actor.wire", 1, 2, 2)` in the second, check in `crates/actor-runtime/corpus/actor.wire/v2.bin` beside `v1.bin`, and gate the send side on `Transport::peer_version` — remembering that `None` means *not yet known*, never *anything goes* (compatibility §3.1). The mechanism is already built; §3.1 says as much. Done when a granary or linearizability swarm asserts its own invariants while the nemesis walks a real `Rollout`, with the synthetic `Form` no longer carrying the property alone.

### 1.2 The shard map's commands are skipped, not refused

`ShardMapCommand` (granary §7.6) is `serde_json` inside `EntryPayload::App`, riding the stamped `actor.raft.log` — whose revision covers the envelope, not the payload. The consequence is worse than an unstamped format. In `crates/granary/src/shardmap.rs`, `decode` ends in `.ok()`, and the apply loop answers `None` with `continue` and the comment *"a command this map cannot parse is defensively ignored"*. So an unrecognized command is silently skipped at apply time on one node while its peer applies it: one node applies a split commit, its peer does not, and the two disagree about which node owns a range. That is state divergence on a consensus apply path — the thing running the shard map through consensus was supposed to make impossible — and skipping rather than refusing violates **V2** directly.

**Next step —** two separable changes, in this order, and the first is the one that matters.

1. **Fail closed.** Make `decode` return its error and make the apply loop stop the node rather than `continue`. The precedent is one boundary over: a node that cannot read its own log refuses to open rather than proceeding on a prefix, because the consensus history is the strictest boundary in the tree (compatibility §3.2.1). A committed entry a build cannot apply is not skippable for the same reason.
2. **Give the payload a boundary.** A `granary.shardmap` row in the §3 registry, a stamp or a revision ahead of the JSON, and a corpus fixture — so the refusal names a revision instead of a parse failure.

Believing (1) needs mixed-version simulation rather than a fixture: a workload running one node on a build that knows a command variant and its peer on one that does not, asserting the cluster refuses rather than diverges. Done when a node that cannot apply a committed shard-map command halts, and a test proves it halts rather than drifts.

### 1.3 A log's record-schema stamp understates after a bump

`wal` writes the caller's record-schema revision into the fixed 16-byte header once, at create (`header()`, `crates/wal/src/lib.rs`). `append` then writes frames at whatever revision its build writes without touching that header, so once a caller's window spans two revisions the stamp understates what the file holds until a compaction restamps it (wal §2.1 rule 5). This is fail-closed — the header claims less than the file contains, so a reader refuses rather than misparses — and compaction is the workaround, which is why it sits below §1.2.

**Next step —** on the first append whose `records.writes()` exceeds the revision recorded in the header, rewrite those two header bytes in place through a second, non-append handle (compatibility §5 names this shape). The ordering is the whole of the work: the header must claim the higher revision, durably, *before* a frame at that revision lands, or a crash between the two leaves exactly the understating file this fixes. Done when a `Wal` created at revision 1, reopened under a window that writes revision 2, and appended to, reports revision 2 in its header with no compaction — and a crash injected between the two writes leaves a file that still opens.

### 1.4 A cluster-wide minimum revision

Each build announces the range it accepts at a boundary (`Window::accepted()`), and `actor.wire` negotiates that per association (compatibility §3.1). Nothing aggregates it across the cluster, so **V4** — do not write a form until every peer can read it — is a policy an operator follows rather than a mechanism the code enforces.

**Next step —** carry each member's announced range in the SWIM gossip digest (`MemberDigest` in `crates/actor-cluster/src/membership.rs`, which already rides `Ping`/`Ack`/`PingReq` in `protocol.rs`), and expose the cluster-wide minimum as something a caller can gate on. A behavior then enables itself once the whole cluster accepts it, rather than on the release an operator believes is everywhere. The hazard is the one §3.1 already states for `peer_version`: a node that has not yet heard from a member must read the minimum as *not yet known* and write conservatively, never as *no constraint*. Done when a behavior can ask the cluster, rather than its own build, whether it may write a form.

## 2. Where the sweeps do not reach

Known gaps between what the simulation sweeps exercise and what the specs mandate (simulation-testing, *Where the sweeps do not yet reach*, which owns all three). Each is a place a bug could live undetected today.

### 2.1 Granary alarms have no continuous sweep

At-most-once firing is now catalogued as **G21** and verified by scenarios (`alarm.rs`, `alarm_cluster.rs`), but a `ClusterWorkload` asserting it under the continuous nemesis trips `no-silent-loss` on about one seed in ten — *"1 ask(s) still pending at quiescence"*. The alarm driver polls its shard's index every 500 ms for as long as its node lives (`ALARM_DRIVE_INTERVAL`, `crates/granary/src/grainref.rs`), so an alarm-wired granary never reaches ask-quiescence. Nothing is lost — those asks carry the 5 s `DEFAULT_ASK_TIMEOUT` and do resolve — so this is a check meeting a subsystem it was not written for.

**Settle this first.** A commit on this branch, `932ebca`, recorded a stronger claim than the paragraph above: that an alarm-wired granary does not commit *at all* under plain frame loss — no partition, no crash — the grain activating, never committing, being passivated, while the caller hangs to its own deadline, with a controlled differential against plain `granary` on the same seed. It was reverted by `557ad1a` with no reason recorded, and the milder quiescence story replaced it. Its named suspects were demonstrably wrong: both cited asks are bounded by `DEFAULT_ASK_TIMEOUT` (`GrainRef::ask`, `ActorRef::ask`). Whether the *symptom* was explained away or merely dropped is written down nowhere, and the workload that produced it was never committed. Reproduce it before building the sweep — if it is real, the sweep is not the work.

**Next step —** build the `ClusterWorkload` on `granary_with_alarms::<Timer>` over the hoisted fixture in `crates/granary/tests/support/timer.rs`, which `alarm_cluster.rs` and `alarm_index.rs` already drive, and take the fourth way out of the quiescence problem: drop `NoSilentLoss` from `default_invariants()` (`crates/actor-simulation/src/invariant.rs`) exactly as `crates/blob-store/tests/swarm.rs` does in its `invariants()`, writing down the same reasoning. That move is honest only while the workload awaits every op it issues to an outcome, which is what leaves the data path's no-loss covered with the checker gone; if that reads as too weak, the alternatives all change the runner or the driver — exempt background-loop asks from the tally, require in-flight-zero over a sustained window, or give the driver a way to stand down. Done when a seeded sweep asserts **G21** under the continuous nemesis and passes every seed, not nine in ten.

### 2.2 Granary workflows have no sweep at all

What is assertable is not "the effect ran once" — `LaunchGuard` is per-activation and never journaled, so a re-activation legitimately re-launches an unresolved step (granary §7.17) — but that the **memo is write-once**: `complete_step` records only a step that is not already done, so the first committed result wins and every later drive resolves from it. Observing that needs a chain: commit a step, be interrupted, re-launch, then be readable. The seeds that get far enough to observe anything are the calm ones that never re-launch, and about two in twenty-four observe a memo at all. More seeds and a longer settle were both tried and moved nothing.

**Next step —** shorten the chain until one commit suffices. Today's fixture (`crates/granary/tests/workflow.rs`) opens with an ask — `Pipeline` accepts a `Start` message — so a round trip has to succeed before the workflow commits anything; dropping it in favour of activating on first touch is the obvious first cut. Alongside it, give the effect a value that differs on each run (a counter, not a constant), because a constant-valued effect cannot tell an overwritten memo from a preserved one. Then assert write-once against the memo rather than against the effect count. Done when a seeded sweep observes a memo on most seeds and re-launches on some of them — the two conditions that currently never co-occur.

### 2.3 Three crates have no sweep

`harness-sandbox`, `harness-gateway`, and `machine-frontdoor` are I/O-boundary crates rather than distributed ones, so the simulator reaches them only indirectly. Each has tests; none has a `ClusterWorkload`.

**Next step —** the decision comes before the code. These crates fail at an edge the simulator does not model — a process boundary, an HTTP boundary, a TCP one — so the first question is what a *fault* even is there and which invariant survives it. Pick one crate and answer that in its own spec's terms before writing a workload; a sweep whose faults the simulator cannot inject asserts nothing while looking like coverage, which is worse than the gap it closes. Done when one of the three has a stated fault model, whether or not a sweep follows.

## 3. Deferred by design

Scope calls, not oversights. **These carry no next step**: each is recorded so a reader does not mistake it for something forgotten, and the move for any of them is to change the owning spec's mind, not to build what the spec declined. Read the cited section first — it holds the reasoning and the cost that the summary here compresses away.

**Consistency and reads.** Reads are **read-your-leader** (relaxed), so a deposed-but-unfenced minority leader can serve a stale read; writes never fork (granary §7.5, §8). The upgrade is a check-quorum lease on the shard's leader-election group, with follower reads as a separate extension (granary §16). The harness inherits this: a `Tail` is exactly this read, and its linearizable form rides the grain's extension rather than a harness mechanism (harness §10.1, §13).

**Storage scaling.** Lazy hydration is deferred at all three facets and for the same reason each time — an activation materializes the whole artifact rather than the part the next session touches: SQL pages behind a capture VFS, workspace files with ranged sub-records above the 64 MiB tree cap, and disk blocks from checkpoint blobs (granary §16, machine §8). Alongside them: disk overlay dirty tracking to replace the reference content-hash scan, proactive blob re-replication on membership change (deferred in lockstep with the same work for records and snapshots), range-verified blob streaming against the BLAKE3 tree the `BlobId` already roots, and a linear `async` DSL over the workflow facet.

**The machine** (machine §8). Memory-snapshot warm resume, so a migrated machine keeps its running processes instead of rebooting the guest; within-connection SSH migration, which needs that snapshot plus connection-state handoff at the front door; point-in-time restore and forking, which need policy before they need mechanism; richer ingress (port forwarding, HTTP preview, TCP); and per-owner quotas. Credit-based flow control across the actor transport is likewise open. Separately, the cross-node front-door relay is deferred, so each node's door serves only the machines it hosts (`crates/machine-standalone/src/node.rs`).

**The agent** (harness §13). Context compaction — an over-long transcript fails the run explicitly today (`crates/harness/src/model.rs`); durable-alarm integration, where the grain half exists and the harness half does not, so resumption is strictly caller-driven; a scheduler singleton above it; token streaming and push-based run observation; per-call permission gating, the dynamic half of an authorization story whose static half is the tier cap; loop-executing tools beyond the built-in `delegate`; code mode; external trace interop; and cross-session sagas.

**The sandbox** (sandbox §7). No tier accepts inbound connections, so a sandboxed server is reachable only from inside its own environment. Warm pools and per-tier snapshots are the shared upgrade with the machine's memory snapshot above.

**Cluster utilities** (cluster-utilities §7). Cluster sharding by `(entity type, entity id)` over §2's placement; distributed pub/sub over per-node topic mediators; and a leader-anchored singleton that trades the §4 convergence caveat for a quorum-gated activation. The singleton is not a mutual-exclusion primitive until that lands.

**`wal`** (wal §8). Group commit across handles, and only if a caller needs it. The rest of that section is non-goals, not deferrals.

## 4. Finished but unused

**`blob-store` has no in-tree consumer.** The crate is complete against its spec (B1–B7), carries its own cluster sweep, and nothing in the workspace depends on it — the docs index says so, the workspace manifest says so beside the dependency itself, and no crate names it. It is the **cold** half of the hot/cold split: granary's grain-colocated blobs, not this, serve the live path (granary §7.10). Nothing to do here; it is recorded so a reader does not take a change to it as load-bearing.
