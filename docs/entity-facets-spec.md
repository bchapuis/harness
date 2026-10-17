# Entity Facets: Specification

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

A `§` with no document name refers to this spec. `entity §N` is [`entity-spec.md`](entity-spec.md), `storage §N` is [`entity-storage-spec.md`](entity-storage-spec.md), and `actor §N` is [`distributed-actor-spec.md`](distributed-actor-spec.md). Durable Objects lessons are cited as **DO §N** ([`../research/durable-objects.md`](../research/durable-objects.md)).

---

## 1. Scope

A **facet** is a durable storage feature of one entity, defined entirely as an interpretation over the two storage primitives, records and blobs (storage §4, §9), as Durable Objects exposes several storage APIs over one substrate (DO §4.2). Every facet shares the entity's one journal, one atomic commit per command, and one composite snapshot; none has a replication path, fence, or recovery of its own, and nothing at or below the journal seam changes for one. The event fold is facet 0 (entity §3.1). Six facets are built in: workspace (§4.1), KV (§4.2), SQL (§4.3, behind the `sql` cargo feature), disk (§4.4), alarm (§4.5), and steps (§4.6). They build on the per-command protocol (entity §5) and the storage substrate; the agent runtime, the persistent machine, and the workflow layer consume them.

### 1.1 Non-goals

- **A facet's own durability machinery.** A new feature MUST NOT add a replication mechanism.
- **Third-party facets.** The seam is sealed (§6).
- **Applications as facets.** A git-style artifact store is refs in KV plus objects in blobs. A feature earns a facet only when it needs durable records the application's `Event` type should not carry, or host participation no library can obtain (discard on a non-committed outcome, callerless delivery from the folded form).

---

## 2. Seams

### 2.1 The facet seam: `Facet`, `FacetSet`, `HasFacet`

The boundary between the host and the facets, in `entity::facet`: public and sealed, so the built-in facets are its only implementations.

```rust
pub trait Facet: Send + Sync + 'static {
    const TAG: u8;
    const PHYSICAL: bool = false;
    type Form: Default + Clone + Send + Sync + 'static;
    type Stage: Default + Send + 'static;
    fn begin(form: &mut Self::Form, stage: &mut Self::Stage) -> Result<(), FacetError>;
    fn seal(form: &mut Self::Form, stage: &mut Self::Stage) -> Result<(), FacetError>;
    fn drain(stage: Self::Stage) -> Vec<Vec<u8>>;
    fn fold(form: &mut Self::Form, payload: &[u8]) -> Result<(), FacetError>;
    fn rehydrate(form: &Self::Form, env: &FacetEnv) -> impl Future<Output = Result<(), FacetError>> + Send;
    fn discard(form: &mut Self::Form);
    fn snapshot(form: &Self::Form, env: &FacetEnv) -> impl Future<Output = Result<Vec<u8>, FacetError>> + Send;
    fn restore(part: Option<&[u8]>, env: &FacetEnv) -> impl Future<Output = Result<Self::Form, FacetError>> + Send;
    fn roots(form: &Self::Form) -> BTreeSet<BlobId>;
}
```

`begin`, `seal`, `rehydrate`, and `discard` default to no-ops; two further defaulted methods, `alarm_due` and `stage_clear_alarm`, are the alarm facet's runtime hook (§4.5).

The facet's side:

- `TAG` is nonzero, stable, and never reused (storage §8.3); it also keys the facet's snapshot contribution.
- `drain` turns a command's stage into record payloads; empty means the facet adds nothing to the batch.
- `fold` MUST be deterministic (**F1**); `restore` then replay MUST equal the full fold (**F2**); `roots` MUST cover every blob the form can reference (**F3**); a physical facet MUST survive `discard` plus rehydration identically (**F4**).
- `snapshot` puts bulk bytes in blobs and returns a manifest. A facet MUST NOT sweep the blob area (§3.6).
- The alarm hook MUST NOT be extended per facet; a second facet needing one generalizes it.

The host's side: it arms a fresh stage and calls `begin` before each decision; calls `seal` and `drain` after it, before the append; folds a logical facet's records only after the commit and skips a physical facet's (its form already changed); calls `discard` on every other outcome; calls `rehydrate` once after restore and replay, before the first command; unions every facet's roots into every sweep; and asserts that a set's tags are distinct.

### 2.2 Composition and accessors

An entity declares `type Facets`, a `FacetSet`: `()` for none, or a tuple of up to four distinct facets such as `(Kv, Ws)`. The declaration fixes the type's tag registry, which makes the unknown-tag rule (§3.2) checkable. Each facet surfaces one `EntityCtx` accessor (`kv`, `ws`, `sql`, `disk`, `alarm`, `steps`) gated by a `HasFacet` bound, so an accessor for an undeclared facet does not compile (**E10** applied to storage).

### 2.3 The facet environment: `FacetEnv`

What a facet's async work receives: a blob handle on the entity's area (storage §2.6) and a node-local scratch location under `EntityConfig::data_dir`, keyed by node and entity so a failover in one simulated process never lands two nodes' forms on one path. It holds rebuildable caches only.

---

## 3. The facet contract

### 3.1 Three answers

A facet is defined by its records (the ordered mutations it journals), its blobs (bulk bytes referenced by id), and its snapshot contribution (its checkpoint at a `Seq`). Replication, fencing, recovery, and compaction are the substrate's (storage §4, §5, §8).

### 3.2 One batch, one order

**Every facet MUST turn a command's writes into records in that command's single batch, and MUST leave committed state unchanged on any outcome but `Committed`.** A command's events and all its facet records append as one batch, committing everywhere or nowhere (**E19**). Records interleave in the entity's one `Seq` order, each wearing its facet's tag (storage §8.3). Replay dispatches by tag, and an unrecognized tag MUST abort the activation rather than be skipped (**E19**). Event reads and subscriptions deliver only facet 0's records (entity §6.2).

### 3.3 Logical and physical facets

- A **logical** facet folds: its records are semantic operations applied by a pure function to an in-memory form (**F1**, generalizing **E2**), folded only after durability (**E1**). KV, alarm, and steps are logical.
- A **physical** facet materializes: its records are captured deltas applied byte-deterministically on replay (**F1**), and its local form (a database file, a directory, a block image) changes during the decision.

**The physical discipline.** A physical facet's local form MAY run ahead of committed state only until the command's outcome (or, for a declared departure, the next capture); on any other outcome the runtime MUST discard it before the entity serves again, and the next activation rebuilds it from the composite snapshot plus committed records (**E20**). This is sound because the form is a rebuildable cache and the gates hide un-durable state: no other command runs during the append, the reply is held, and a failed outcome destroys the only tainted copy. The disk facet is the one declared departure (§4.4).

### 3.4 Staging

- **Command-scoped.** A facet write outside a decision (from `on_activate` or `on_deactivate`) has no batch to join and MUST fail loudly, never be dropped or deferred: the SQL and disk accessors return an error, the others panic.
- **Plan, then stage.** Staged operations commit even when the reply is an application error, so an accessor MUST do its fallible work (blob puts, scans) first and stage only on success; a failed operation leaves no trace.
- A logical accessor reads committed state plus what this command staged.

### 3.5 The composite snapshot and capture

The snapshot at *S* is facet 0's state and every declared facet's contribution at *S* in one `save_snapshot` payload (storage §8). **E4** applies to the whole. Bulk bytes go to blobs and the composite holds manifests, since the record is broadcast whole; a facet declared after the last snapshot restores empty. A snapshot MUST be the committed image: a physical form's uncaptured writes MUST NOT enter it. **A capture MUST yield a delta that, replayed onto the last committed form, reproduces exactly the form it read at a point no writer was mutating.**

### 3.6 Blob roots and sweep safety

Every facet reports its root set (**F3**). A facet MUST NOT sweep the shared area: the one sweep is `EntityBlobs::gc`, called by the application with its own roots over a handle that unions every facet's roots in, so no facet's sweep drops another's bytes. A writer puts first and roots later (a checkpoint once its chunks are durable, a KV spill at its commit), so a sweep MUST also keep every blob the activation has put since its current or latest command began, and no put may start while a sweep that has read its retain set runs. Blobs orphaned by a failed commit are reclaimed by a later sweep.

### 3.7 The facet obligations

No gated catalogue holds these; each facet's own suites exercise them (§6).

| | Obligation |
|---|---|
| **F1** | **Deterministic interpretation.** A logical fold is pure and deterministic; a physical delta's application is byte-deterministic. |
| **F2** | **Snapshot equivalence.** Restoring the contribution at *S* and replaying the records in (*S*, head] yields the form the full history yields. |
| **F3** | **Complete roots.** The root set covers every blob the restored form can reference. |
| **F4** | **Discard safety** (physical only). Discarding the form at any moment and rehydrating reproduces it identically. |

---

## 4. The facets

### 4.1 Workspace (`Ws`, physical)

A real node-local directory (`WsHandle::dir_path`) that a shell, container, or microVM works in directly. A decision calls `WsHandle::capture` after its side effects land, staging one record per changed file with its bytes inline plus one naming deleted paths, so a tool's result and its files commit together; replay applies the bytes, never re-executes (**F1**), and an unchanged tree stages nothing. The discipline is §3.3. The contribution maps each path to content-addressed chunk ids, so a checkpoint uploads only new content, and those ids are the roots (**F3**); a snapshot taken while an in-flight tool has dirtied the tree MUST fail. Only regular files are durable (directories are implied; empty directories, symlinks, and special files are not kept), regenerable cache trees are never captured, and the durable tree is capped at `MAX_TREE_BYTES`: an over-cap capture stages nothing, and the next diffs against the last committed index. Consumers: the agent's sandbox (agent §4.4), the machine's guest (machine §3).

### 4.2 KV (`Kv`, logical)

An ordered map of small values through `KvHandle` (`get`, `put`, `delete`, prefix `keys` and `list`), staged with read-your-staged-writes. `Put` and `Delete` records fold into the map (**F1**); the contribution is the encoded map (**F2**). A value above `INLINE_MAX` spills to the blob area, durable before the map names it, and `get` verifies it (**E17**); spilled ids are the roots (**F3**). Native, with no C dependency, and unchanged under simulation; predicate queries belong to SQL.

### 4.3 SQL (`Sql`, physical)

A private SQLite database per entity through `SqlHandle` (DO §2.3). Replication ships physical frames and never re-executes, so nondeterministic SQL is fine.

- **One command, one transaction, one record.** A command's statements run in one implicit transaction; none spans commands. At local commit the host MUST obtain exactly that transaction's log frames, in order, as one record (**E19**); a read-only transaction yields none.
- **Discipline.** The file changes at local commit (§3.3, **E20**); durability is the quorum acknowledgement, never the local disk.
- **Checkpoint.** With each composite snapshot the facet checkpoints its log into the database and contributes a manifest of page-aligned content-addressed chunks, the roots (**F3**). Rehydration fetches them verified (**E17**), materializes the file, and applies later frames (**F1**).
- **No out-of-command SQL.** A lifecycle hook's statement would produce uncaptured frames, so it MUST be refused (`SqlError`); schema setup is idempotent DDL in a writing command.
- **Bounds.** A query returns at most `MAX_QUERY_ROWS` rows, as an error rather than a truncation. No bound is enforced on a transaction's frame bytes (§6), so a bulk load SHOULD split across commands.

### 4.4 Disk (`Disk`, physical)

A fixed-size block image a microVM mounts as its root filesystem (`DiskHandle::path`). Block bytes never ride a record: `DiskHandle::capture` puts each changed block as a blob, then stages exactly one record, a manifest of the image size and `(block index, BlobId)` pairs (**E19**), so a crash never commits part of a capture. `DiskHandle::import` provisions a full-coverage manifest, putting every block into this entity's own blob area.

- **Capture rides a command.** The guest writes between commands, so the consumer MUST drive every capture through an explicit command, MUST capture only while the guest is not writing (§3.5), and its `can_hibernate` MUST refuse eviction while the image holds uncaptured writes. A capture MUST obtain exactly the blocks changed since the last one.
- **Off the consensus worker.** A capture or import reads and hashes every block, so it MUST NOT run on the worker driving the node's consensus; it runs on the node's blocking pool (storage §2.5).
- **The named departure.** The guest's un-durable writes are observable to the guest, and to whoever is attached, before the next capture. Entity-level observables (records, replies, event reads) still honour the gates; the wider exposure is the consumer's to bound (machine §4).
- **Checkpoint uploads nothing.** The contribution is the committed block index as a full-coverage manifest of blobs already put; uncaptured writes MUST NOT enter it, and an uncovered region restores as zeros. The roots are the checkpoint's blocks plus those later manifests name (**F3**). Replay folds later manifests into a pending set that `Facet::rehydrate` fetches and applies (**F1**, **E17**).
- **Bounded.** `MAX_IMAGE_BYTES` caps an import and every folded manifest, and a manifest holds ids, never bytes.

### 4.5 Alarm (`Alarm`, logical)

At most one pending deadline (DO §5), set or cleared through `AlarmHandle`, firing a callerless `on_alarm` (entity §3.3). Set and clear records fold into one optional deadline (last write wins), the source of truth; staging is as for KV; the root set is empty.

- **Firing goes through the barrier.** After each commit and at the end of rehydration the host arms a timer from the folded deadline, and when it is due runs `on_alarm` through the per-command protocol. A superseded timer's late delivery MUST be a no-op.
- **Consume on fire** (**E21**). The fired deadline's clear joins `on_alarm`'s own batch, so it clears atomically with the effects unless the handler re-arms; a failed commit leaves it pending to fire again.
- **Liveness.** On a node that hosts the type with an alarm index (`EntityNode::alarm_index`), a committed deadline MUST invoke `on_alarm` once due on the shard's current leader, whether or not the entity is resident. Without an index an alarm fires only while resident, so a failover delays it to the next access.
- **A pending alarm vetoes hibernation.** An entity with a pending deadline MUST stay resident unless the per-shard `AlarmIndex` entity has acknowledged that deadline; registration is an `ask` answered after the index commits, retried off the command path. The index is a hint and the facet the truth, so a stale entry costs at worst a spurious wake. The index reaches exactly the hosted types that declare `Alarm`.

### 4.6 Steps (`Steps`, logical)

Durable step memoization for the `step`, `sleep`, and `retry` pattern. Each step runs an external effect; its result is journaled and folded into a map from `StepId` to bytes (`StepsHandle`), so a later drive, re-activation, or failover resolves a completed step from the memo. Step ids are the consumer's stable call-site ordinals.

- **The memo is write-once.** The launch guard (`LaunchGuard`) is per activation and never journaled, so a re-activation MAY re-launch a step whose result has not committed; the first committed result MUST win, and every later `StepDone` for that step commits nothing (`complete_step`).
- `sleep` is a step whose effect is an alarm; `retry` re-launches after an alarm-backed backoff.
- An entity whose step results are already its events MUST NOT memoize them again (one decision, two folds); it adopts only the launch guard. Launching, guarding, and recovering orphaned effects are generalized above entity by the workflow (workflow §3.3).

---

## 5. Invariants defined here

This document defines the catalogue rows **E19** (§3.2), **E20** (§3.3, §4.3), and **E21** (§4.5); the catalogue is entity §12. The facet obligations **F1** to **F4** are §3.7.

---

## 6. Deferred

- **Third-party facets:** opening the sealed seam once the tag registry and cross-version rules settle.
- **A generic facet harness:** **F1** to **F4** are exercised per facet, not by one harness over every facet, and **F3** has no dedicated test.
- **SQL lazy page hydration:** capturing frames below SQLite and faulting pages in from blobs, so a large database activates without materializing whole.
- **SQL bounds:** an enforced per-transaction frame cap and a maximum database size.
- **Workspace scaling:** ranged sub-records for a file larger than a record bound, faulting files in on first access, and preserving file times across a guest round trip so capture's stat pruning survives it.
- **Disk lazy block hydration:** faulting blocks in on first guest access.
- **Disk overlay dirty tracking:** a copy-on-write overlay per capture epoch whose extents are the dirty set, replacing the full scan; privileged and Linux-only.
- **Gating `on_alarm`** behind the facet set, so an entity without the alarm facet cannot implement it.
- **A linear step DSL:** an `async` `step`/`sleep`/`retry` surface over the memo.
