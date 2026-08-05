# Open Work

**Status:** an index, not a specification. Nothing here is normative. Every entry points at the section that owns it, and that section is the source of truth — this page is a view over them, ordered by what blocks what. A change of scope belongs in the owning spec first and here second; an item that lands should be deleted from both in the same commit.

The tree carries no TODO backlog. A sweep of `crates/` finds zero `todo!()`, zero `unimplemented!()`, zero `#[ignore]`, and zero `TODO`/`FIXME`/`HACK` comments; the only two marker hits in the repository are a vendored `clang-sys` artifact under `guest/qjs-runner/target/` and an `mktemp` template in `scripts/bench-machine-cost.sh`. That is a property worth keeping: unfinished work here is *declared in a spec*, where a reader can see its cost and its blast radius, rather than dropped as a comment beside the code that would have to change. The consequence is that "what is unfinished" cannot be answered by grepping, which is why this page exists.

## 1. The critical path

**`Transport::peer_version` — surfacing the negotiated wire revision** (compatibility §3.1, §5). The association handshake negotiates a revision and then **discards** it, so no caller can gate what it sends. Two things follow. The `actor.wire` boundary **cannot be bumped at all** today: running two revisions at once needs a send-side gate, and there is nothing to gate on, so a wire-format change is a build-this-first project rather than an edit. And the mixed-version simulation of §2 has nothing to vary, because a simulated node cannot be told what its peer speaks. It is first in the compatibility spec's own priority order for exactly this reason, and it is the one item on this page whose absence blocks another item.

## 2. Compatibility policy, half enforced

Of the two checks that keep **V4**/**V5** from decaying into prose (compatibility §4), the golden corpus now exists: every registered boundary has checked-in bytes at its current revision, a test beside the format decodes them, and a completeness gate fails the build when a new boundary ships without them.

What remains is **mixed-version simulation** — simulated nodes given different windows, prior record and message definitions kept behind their revision, and the granary invariants asserted across a rolling upgrade and a rollback. It is blocked on §1: a simulated node cannot be told what its peer speaks until the negotiated revision reaches a caller.

Boundaries that exist as formats but are **not yet stamped**, in the spec's priority order (compatibility §5). Each is a place where a format change today is a migration rather than an edit.

| Boundary | What an unstamped change costs |
|---|---|
| `granary.store` | A grain's event payloads record no codec. Swapping the deployment codec turns every record past the last snapshot into a storm of corrupt-grain activation aborts rather than one diagnosable configuration error. Worth having *before* a codec swap, not after. |
| Sidecars | The Raft term and snapshot pointers, the shardmap, and the blob-store tombstones are durable formats written through `wal::atomic_replace` with no stamp. `compat::Stamp` wraps them without disturbing the primitive's opaque-bytes interface. |
| A log's record-schema stamp | A `Wal` appends at the revision its build writes without updating the header, so once a caller's window spans two revisions the stamp understates until a compaction restamps it (wal §2.1 rule 5). Fail-closed, and compaction is the workaround. |
| A cluster-wide minimum revision | Carrying each member's announced range in the membership digest, so a behavior enables itself only once the whole cluster accepts it. Turns **V4** from a policy into a mechanism. |

## 3. Where the sweeps do not reach

Known gaps between what the simulation sweeps exercise and what the specs mandate (simulation-testing, *Where the sweeps do not yet reach*). Each is a place a bug could live undetected today.

- **Granary alarms have no continuous sweep.** `alarm-cluster/leader-crash` sweeps 24 seeds, but no `ClusterWorkload` runs alarms under the continuous nemesis with at-most-once firing as a checker. Needs the alarm-index wiring (`granary_with_alarms`) threaded through a workload.
- **Granary workflows have no sweep at all.** "A step's effect runs at most once across passivation" is a natural continuous invariant and nothing asserts it.
- **`harness-sandbox`, `harness-gateway`, and `machine-frontdoor` have no sweep.** These are I/O-boundary crates rather than distributed ones, so the simulator reaches them only indirectly, and what a sweep would even look like there is itself unsettled.

## 4. Deferred by design

Scope calls, not oversights. Grouped by the layer that owns them; read the cited section for the reasoning and the cost.

**Consistency and reads.** Reads are **read-your-leader** (relaxed), so a deposed-but-unfenced minority leader can serve a stale read; writes never fork (granary §7.5, §8). The upgrade is a check-quorum lease on the shard's leader-election group, with follower reads as a separate extension (granary §16). The harness inherits this: a `Tail` is exactly this read, and its linearizable form rides the grain's extension rather than a harness mechanism (harness §10.1, §13).

**Storage scaling.** Lazy hydration is deferred at all three facets and for the same reason each time — an activation materializes the whole artifact rather than the part the next session touches: SQL pages behind a capture VFS, workspace files with ranged sub-records above the 64 MiB tree cap, and disk blocks from checkpoint blobs (granary §16, machine §8). Alongside them: disk overlay dirty tracking to replace the reference content-hash scan, proactive blob re-replication on membership change (deferred in lockstep with the same work for records and snapshots), range-verified blob streaming against the BLAKE3 tree the `BlobId` already roots, and a linear `async` DSL over the workflow facet.

**The machine** (machine §8). Memory-snapshot warm resume, so a migrated machine keeps its running processes instead of rebooting the guest; within-connection SSH migration, which needs that snapshot plus connection-state handoff at the front door; point-in-time restore and forking, which need policy before they need mechanism; richer ingress (port forwarding, HTTP preview, TCP); and per-owner quotas. Credit-based flow control across the actor transport is likewise open. Separately, the cross-node front-door relay is deferred, so each node's door serves only the machines it hosts (`crates/machine-standalone/src/node.rs`).

**The agent** (harness §13). Context compaction — an over-long transcript fails the run explicitly today (`crates/harness/src/model.rs`); durable-alarm integration, where the grain half exists and the harness half does not, so resumption is strictly caller-driven; a scheduler singleton above it; token streaming and push-based run observation; per-call permission gating, the dynamic half of an authorization story whose static half is the tier cap; loop-executing tools beyond the built-in `delegate`; code mode; external trace interop; and cross-session sagas.

**The sandbox** (sandbox §7). No tier accepts inbound connections, so a sandboxed server is reachable only from inside its own environment. Warm pools and per-tier snapshots are the shared upgrade with the machine's memory snapshot above.

**Cluster utilities** (cluster-utilities §7). Cluster sharding by `(entity type, entity id)` over §2's placement; distributed pub/sub over per-node topic mediators; and a leader-anchored singleton that trades the §4 convergence caveat for a quorum-gated activation. The singleton is not a mutual-exclusion primitive until that lands.

**`wal`** (wal §8). Group commit across handles, and only if a caller needs it. The rest of that section is non-goals, not deferrals.

## 5. Finished but unused

**`blob-store` has no in-tree consumer.** The crate is complete against its spec (B1–B7) and nothing in the workspace depends on it — the docs index says so, and no `Cargo.toml` names it. It is the **cold** half of the hot/cold split: granary's grain-colocated blobs, not this, serve the live path (granary §7.10). Worth knowing before reading a change to it as load-bearing.
