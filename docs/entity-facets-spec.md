# Entity Facets: Specification

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** carry the meanings defined in RFC 2119.

A `§` with no document name refers to this spec. `entity §N` is [`entity-spec.md`](entity-spec.md), `storage §N` is [`entity-storage-spec.md`](entity-storage-spec.md), `workflow §N` is [`workflow-spec.md`](workflow-spec.md), `agent §N` is [`agent-spec.md`](agent-spec.md), and `machine §N` is [`machine-spec.md`](machine-spec.md).

---

## 1. Scope

A **facet** is a durable storage feature of one entity, defined entirely as an interpretation over the two storage primitives, records and blobs (storage §5, §10). Every facet shares the entity's one journal, its one atomic commit per decision, and its one composite snapshot, and nothing at or below the journal seam (storage §3.1) changes for one. The entity's event fold is facet 0 (entity §4.1). Six further facets exist: workspace (§5.1), KV (§5.2), SQL (§5.3), disk (§5.4), alarm (§5.5), and steps (§5.6).

Facets build on the per-command protocol (entity §6) and on entity storage (storage §5, §9, §10). The agent runtime (agent §5.3), the persistent machine (machine §4, §6), and the workflow layer (workflow §4.3) consume them.

### 1.1 Non-goals

- **Durability machinery of a facet's own.** A facet MUST NOT add a replication path, fence, recovery protocol, or compaction; those are the substrate's (storage §5, §6, §9).
- **Application features as facets.** A feature expressible over records, blobs, and the existing facets is an application, not a facet: a git-style artifact store is refs in KV plus objects in blobs.

### 1.2 Conformance

- A **host** (the entity host of entity §6) conforms by meeting the host's side of §3, §4, the subsection of §5 for each facet it offers, and §6. The SQL facet is optional: a host MAY omit it.
- A **facet** conforms by meeting the facet's side of §3.1, §4, its own subsection of §5, and F1 to F4 (§7).
- A **consumer**, an entity type that declares facets, conforms by meeting the consumer's obligations in §3.2, §4.3, and the subsections of §5 for the facets it declares.

---

## 2. Model

- **Records, blobs, contribution:** a facet is defined by three answers: its **records**, the ordered mutations it journals; its **blobs**, bulk bytes it references by content id (storage §10); and its **contribution**, its part of the composite snapshot.
- **Facet 0:** the entity's event fold (entity §4.1). Its records are the entity's events and its form is the entity's state.
- **Tag:** the byte that identifies a facet. Every record of the facet carries it in the record envelope (storage §12), and it keys the facet's contribution (§6.1).
- **Form:** a facet's committed state as the host holds it: a logical facet's folded value, or a physical facet's local materialization (a database file, a directory, a block image).
- **Stage:** a facet's per-decision buffer: the operations a logical facet's accessor staged, or the delta a physical facet captured.
- **Logical facet:** a facet whose records are semantic operations, folded by a pure function into an in-memory form. KV, alarm, and steps are logical.
- **Physical facet:** a facet whose records are captured deltas, applied byte-deterministically on replay, and whose local form changes during the decision. Workspace, SQL, and disk are physical.
- **Capture:** turning a physical facet's local changes into a delta record in the current decision's batch.
- **Composite snapshot:** facet 0's state and every declared facet's contribution at one position *S* of the entity's record order, written as one snapshot (storage §9).
- **Root set:** the ids of every blob a facet's form can reference.
- **Facet set:** the facets an entity type declares. It fixes the tags the type's records can carry.
- **Accessor:** the operations one facet offers a consumer's decision.
- **Facet environment:** what the host gives a facet's restore, rehydrate, and snapshot operations: the entity's blob area (storage §10) and a node-local scratch location unique to the node and the entity, which holds rebuildable caches only.
- **Decision:** a command's handler or a callerless decision (entity §4.2, §4.3).
- **Durable tree:** the workspace files the workspace facet keeps (§5.1).
- **Block image:** the disk facet's fixed-size image, a file a guest mounts as a block device.
- **Capture manifest:** the disk facet's record: an image size and, for each block it covers, the block's index and blob id.
- **Deadline:** the alarm facet's one optional pending instant. The **alarm decision** is the entity's callerless decision that runs when the deadline is due (entity §4.3).
- **Alarm index:** an entity holding the pending deadlines of one type's entities in one shard.
- **Step, step id, memo:** a **step** is one external effect whose result a consumer journals; its **step id** names it; the **memo** is the steps facet's form, a map from step id to result bytes.
- **Launch guard:** a per-activation record of the steps the activation has launched, never journaled.

---

## 3. Seams

### 3.1 The facet seam

The boundary between the host, which invokes, and a facet, which answers. The six facets of §5 are its only implementations.

| Operation | The host invokes it | The facet |
|---|---|---|
| begin | before each decision, on a fresh stage | opens its per-decision work; a physical facet fails if its local form is lost |
| seal | after the decision, before the append | closes its per-decision work; a physical facet captures its delta into the stage |
| drain | after seal | yields the stage's record payloads in order; none means it adds nothing to the batch |
| fold | per record with its tag: on replay, and after a commit for a logical facet only | applies the record to its form |
| restore | at rehydration, with its contribution or none | yields its form, empty when the snapshot carries no contribution |
| rehydrate | once, after restore and replay, before the first command | resolves replayed records that name blobs into its form |
| discard | on every outcome but a commit | a physical facet drops its local form; a logical facet does nothing |
| snapshot | when the host writes a composite snapshot | puts its bulk bytes in the blob area and yields its contribution |
| roots | at every sweep | yields its root set |
| deadline | after every commit and at the end of rehydration | the alarm facet yields its deadline; every other facet yields none |
| consume | before the alarm decision runs | the alarm facet stages the deadline's clear; every other facet does nothing |

Begin, seal, fold, restore, rehydrate, and snapshot can fail with a facet error.

The facet's side:

- Its tag is nonzero, stable, and never reused (storage §12).
- Fold MUST be deterministic (**F1**); restore then replay MUST equal the full fold (**F2**); the root set MUST cover every blob the form can reference (**F3**); a physical facet MUST survive discard plus rehydration identically (**F4**).

The host's side:

- A failed begin, seal, or live fold is an outcome other than a commit: the host invokes discard and the activation ends (entity §6).
- A failed restore, replayed fold, or rehydrate aborts the activation.
- It refuses to host a facet set whose tags are not distinct.

### 3.2 Declaration and accessors

The boundary between a consumer and the host.

- A consumer declares its facet set: none, or up to four distinct facets. The declaration fixes the tags replay recognizes for the type (§4.1).
- Every decision's context (entity §3.1) offers one accessor per declared facet. Using the accessor of an undeclared facet is refused at build time (**E10** applied to storage).
- Each accessor's operations are in its facet's subsection of §5; the rules every accessor follows are §4.3.

---

## 4. The facet contract

### 4.1 One batch, one order

**Every facet MUST turn a decision's writes into records in that decision's single batch, and MUST leave committed state unchanged on any outcome but a commit.** A decision's events and all its facet records append as one batch, committing everywhere or nowhere (**E19**). Records interleave in the entity's one record order, each carrying its facet's tag (storage §12). Replay dispatches each record by tag, and a tag no declared facet claims MUST abort the activation, never be skipped (**E19**). Event reads and subscriptions deliver only facet 0's records (entity §7.2).

### 4.2 Logical and physical facets

A logical facet's fold is a pure function (**F1**, generalizing **E2**), applied only after the batch commits (**E1**). A physical facet's deltas apply byte-deterministically (**F1**).

**The physical discipline:** a physical facet's local form MAY run ahead of committed state only until the decision's outcome (for the disk facet, the one declared departure, until the next capture, §5.4). On any other outcome the host MUST discard it before the entity serves again, and the next activation rebuilds it from the composite snapshot plus committed records (**E20**).

### 4.3 Staging

- **Decision-scoped.** A facet write outside a decision (from the activation or deactivation hook, entity §3.1) has no batch to join and MUST fail loudly, never be dropped or deferred: the SQL and disk accessors return an error, and the others raise a fault that ends the hook.
- **Plan, then stage.** Staged operations commit even when the reply is an application error (entity §3.2), so an accessor MUST do its fallible work (blob puts, scans) first and stage only on success: a failed operation leaves no trace.
- **Read your staged writes.** A logical accessor reads committed state plus what the current decision staged; outside a decision it reads committed state.

### 4.4 The composite snapshot and capture

The snapshot at *S* is facet 0's state and every declared facet's contribution at *S*, in one snapshot written through the journal (storage §3.1, §9.1) in the `entity.snapshot` format (storage §12). **E4** applies to the whole. A contribution holds manifests and puts its bulk bytes in the blob area; a facet declared after the last snapshot restores empty. A snapshot MUST be the committed image: a physical form's uncaptured writes MUST NOT enter it. **A capture MUST yield a delta that, replayed onto the last committed form, reproduces exactly the form it read at a point no writer was mutating.**

### 4.5 Blob roots and sweep safety

Every facet reports its root set (**F3**). A facet MUST NOT sweep the blob area. The entity's one sweep is the blob area's (storage §10, §10), invoked by the consumer with its own roots, and the host MUST add the union of every facet's root set, facet 0's included (storage §12), so no sweep drops bytes a facet references. A writer puts a blob before the form naming it commits (a checkpoint names its chunks once they are durable, and a KV value spilled to the blob area is named at its commit), so a sweep MUST also keep every blob the activation has put since its current or latest decision began, and a put MUST NOT start while a sweep that has read its retained set runs. Blobs orphaned by a failed commit are reclaimed by a later sweep.

---

## 5. The facets

### 5.1 Workspace (physical)

A real node-local directory that a shell, a container, or a microVM works in directly. Its durable tree is the directory's regular files whose path is valid UTF-8 and has no component named `node_modules`, `target`, `.venv`, `.git`, `__pycache__`, `dist`, or `build`. Directories are implied by their files; empty directories, symbolic links, and special files are not kept. The durable tree is bounded at 64 MiB (2^26 bytes).

| Operation | Takes | Yields |
|---|---|---|
| directory | | the directory's path, valid once rehydration has materialized it |
| capture | | the count of files written and removed and the durable tree's size, or an error: not materialized, directory lost, tree over the bound, or a local I/O failure |

- **Capture.** A decision captures after its side effects land. A capture stages one record per changed file, carrying the file's whole content, and one record naming the deleted paths, so a tool's result and its files commit together (**E19**); an unchanged tree stages nothing. Replay writes the recorded bytes and removes the recorded paths, never re-executing what produced them (**F1**). The discipline is §4.2.
- **Over the bound.** A capture whose durable tree exceeds the bound stages nothing and reports it; the next capture compares against the last committed tree, so no change is lost.
- **A lost directory.** A capture that finds the directory gone reports it and stages nothing, never the deletion of every file. A decision that begins without the directory fails, and the next activation rebuilds it (§4.2).
- **Snapshot.** The contribution names, for each durable file, the blobs holding its content; those blobs are the root set (**F3**). A snapshot that finds a durable file's content differing from its committed content (an in-flight tool's uncaptured writes) MUST fail rather than include it.
- **Replay stays inside the directory.** A record path that is empty or has an empty, `.`, or `..` component MUST abort the activation.

Consumers: the agent's sandbox (agent §5.3) and the machine's guest (machine §4).

### 5.2 KV (logical)

An ordered map from string keys to byte values.

| Operation | Takes | Yields |
|---|---|---|
| get | key | the value, or none |
| put | key, value | stages the write, or an error if a large value cannot be put in the blob area |
| delete | key | stages the removal |
| keys | prefix | the keys with that prefix, in key order |
| list | prefix | the key and value pairs with that prefix, in key order |

- Keys order bytewise.
- Its records are puts and deletes, folded into the map, last write wins (**F1**); its contribution is the encoded map (**F2**).
- A value above the host's inline size is put in the blob area first, durable before the map names it (§4.3), and every read of it verifies the bytes (**E17**). The ids of such values are the root set (**F3**).

### 5.3 SQL (physical)

A private database per entity, speaking the SQLite dialect. A value is null, a 64-bit integer, a 64-bit float, text, or a blob.

| Operation | Takes | Yields |
|---|---|---|
| execute | statement, parameters | the number of rows changed |
| query | statement, parameters | the rows |
| query one | statement, parameters | the one row, or an error if the query yields any other number |
| select | statement, parameters | the column names and the rows |

Every operation returns failure to the decision as an error value.

- **One decision, one transaction, one record.** A decision's statements run in one implicit transaction; none spans decisions. At that transaction's local commit the host MUST capture exactly the database pages it wrote, in order, as one record (**E19**); a transaction that writes nothing yields none, and a decision that runs no statement opens no transaction.
- **Replay never re-executes.** Replay applies the recorded pages (**F1**), so a statement MAY be nondeterministic.
- **Discipline.** The database changes at the transaction's local commit, before the append (§4.2, **E20**). A write is durable only when its batch commits, never because the local file holds it.
- **Snapshot.** The contribution names the blobs holding the database image at *S*; they are the root set (**F3**). Restore fetches them, verified (**E17**), and materializes the database, and replay applies later records (**F1**).
- **Statements stay inside the decision and the database.** The host MUST refuse, with an error, a statement outside a decision, and a statement that controls a transaction or savepoint, attaches or detaches a database, or sets a pragma. A consumer sets up its schema with idempotent statements inside a writing decision.
- **Bounds.** A query yielding more than 10,000 rows MUST fail with an error, never return a truncated result. A transaction's pages ride one record, so a consumer SHOULD split a bulk load across decisions.

### 5.4 Disk (physical)

A block image a guest mounts as its root filesystem. Block bytes never ride a record: a capture puts each changed block in the blob area, then stages exactly one capture manifest (**E19**), so a capture commits whole or not at all.

| Operation | Takes | Yields |
|---|---|---|
| path | | the image file's path; the file exists once an import has committed |
| size | | the committed image size in bytes, zero before an import commits |
| content digest | | an identifier of the committed content, none before an import commits, unchanged by a rehydration that reproduces the same blocks |
| import | a source image | the count of blocks and bytes staged, or an error |
| capture | | the count of blocks and bytes staged, or an error |

- **Import.** An import replaces the image wholesale and stages one manifest covering every block, each block put into this entity's own blob area. An import larger than 16 GiB (2^34 bytes) MUST be refused.
- **Capture rides a decision.** A decision stages at most one capture or import, and the host captures only when a decision asks. The guest writes between decisions, so the consumer MUST drive every capture through an explicit command, MUST capture only while the guest is not writing (§4.4), and its hibernation veto (entity §3.1) MUST refuse eviction while the image holds uncaptured writes. A capture MUST obtain exactly the blocks changed since the last committed capture or import; an unchanged image stages nothing.
- **Off the consensus worker.** A capture or import reads every block of the image, so it MUST NOT run on the worker that drives the node's consensus; it runs through the node's blocking-I/O seam (storage §3.4).
- **The declared departure.** The guest's uncaptured writes are observable to the guest, and to whoever is attached, before the next capture. Entity-level observables (records, replies, event reads) still expose only committed state (**E20**); bounding the wider exposure is the consumer's (machine §6).
- **Snapshot.** The contribution is a manifest covering the committed image and naming only blobs already put, so a checkpoint puts nothing. Uncaptured writes MUST NOT enter it, and a region no manifest covers restores as zeros. The root set is the blocks the restored manifest names plus those every later manifest names (**F3**). Replay collects the later manifests, and rehydrate fetches their blocks, verified (**E17**), and applies them in record order (**F1**).
- **Bounded.** A replayed or restored manifest that names an image above 16 GiB, or a block outside its image, MUST abort the activation. A manifest holds ids, never bytes.

### 5.5 Alarm (logical)

At most one deadline per entity, firing the alarm decision (entity §4.3).

| Operation | Takes | Yields |
|---|---|---|
| pending | | the deadline, or none |
| set at | an instant | stages it as the deadline, replacing any other |
| set after | a duration | stages a deadline that long after the system's current time |
| clear | | stages the deadline's removal |

- Set and clear records fold into one optional deadline, last write wins, and the folded deadline is the source of truth. The root set is empty.
- **Firing goes through the barrier.** After each commit and at the end of rehydration the host arms a timer from the folded deadline, and when it is due runs the alarm decision through the per-command protocol (entity §6); a deadline already past is due at once. A superseded timer's late delivery MUST be a no-op.
- **Consume on fire** (**E21**). The fired deadline's clear joins the alarm decision's own batch, so it clears atomically with the decision's effects unless the decision sets a new deadline; a failed commit leaves it pending to fire again.
- **Liveness.** On a node configured with an alarm index, a committed deadline MUST run the alarm decision once due, on the shard's current leader, whether or not the entity is resident. Without an index an alarm fires only while its entity is resident, so a failover delays it to the next access.
- **A pending alarm vetoes hibernation.** An entity with a pending deadline MUST stay resident until its shard's alarm index has acknowledged that deadline. The host registers each change of the deadline by a request the index answers only after committing it (entity §6); registration never delays a decision, and a failed one is retried. The folded deadline, not the index, decides whether and when the alarm decision runs: a stale entry costs at most a wake that fires nothing. An index serves exactly the hosted types that declare the alarm facet.

### 5.6 Steps (logical)

Durable step memoization for the step, sleep, and retry pattern. Each step runs an external effect; its result is journaled and folded into the memo, so a later drive, re-activation, or failover resolves a completed step from the memo. A step's effect returns its result to the entity as a step-completion command (`entity.StepDone`).

| Operation | Takes | Yields |
|---|---|---|
| result | step id | the step's decoded result, none if the step has not completed, or an error if the result does not decode |
| is done | step id | whether the step has completed |
| record | step id, result | stages the result |
| complete | a step-completion command | stages its result unless the step has completed, and nothing otherwise |

The root set is empty. The launch guard claims a step (true only the first time in this activation), tests a claim, tests that no claim is held, and forgets every claim.

- **Step ids.** A step id is the consumer's stable call-site ordinal: a step MUST get the same id on every re-drive and replay.
- **The memo is write-once.** The launch guard is per activation and never journaled, so a re-activation MAY re-launch a step whose result has not committed. The first committed result MUST win, and every later completion of that step commits nothing.
- **Sleep and retry.** A sleep is a step whose effect is an alarm (§5.5); a retry re-launches after an alarm-backed backoff.
- **No double memo.** An entity whose step results are already its events MUST NOT memoize them again; it uses only the launch guard. Launching, guarding, and recovering orphaned effects are generalized above the entity by the workflow layer (workflow §4.3).

---

## 6. Registries

### 6.1 Facet tags

| Tag | Facet | Kind | Section |
|---|---|---|---|
| 0 | facet 0, the event fold | logical | entity §4.1 |
| 1 | KV | logical | §5.2 |
| 2 | workspace | physical | §5.1 |
| 3 | SQL | physical | §5.3 |
| 4 | alarm | logical | §5.5 |
| 5 | steps | logical | §5.6 |
| 6 | disk | physical | §5.4 |

Every tag lies in the range of the record envelope that storage §12 gives the host's own facets; the range it reserves for facets defined outside the host is unallocated.

### 6.2 Manifests and entity types

| Name | Kind | Meaning |
|---|---|---|
| `entity.StepDone` | command | a step's result returning to its entity: the step id and the encoded result (§5.6) |
| `entity.AlarmIndex` | entity type | the alarm index (§5.5): one entity per hosted type and shard, keyed by the type name, `/`, and the shard index in decimal |
| `entity.AlarmIndex.Sync` | command | a target entity's deadline, or its absence, as of a head of the target's journal |
| `entity.AlarmIndex.DueBefore` | command | the registered entities whose deadline is at or before an instant; a read |
| `entity.AlarmIndex.AllPending` | command | every registered entity and its deadline; a read |

---

## 7. Invariants

This document defines the catalogue rows **E19** (§4.1, §4.4), **E20** (§4.2, §5.3), and **E21** (§5.5); the catalogue is entity §12. The facet obligations are:

| | Obligation |
|---|---|
| **F1** | **Deterministic interpretation.** A logical fold is pure and deterministic; a physical delta's application is byte-deterministic. |
| **F2** | **Snapshot equivalence.** Restoring the contribution at *S* and replaying the records in (*S*, head] yields the form the full history yields. |
| **F3** | **Complete roots.** The root set covers every blob the restored form can reference. |
| **F4** | **Discard safety** (physical only). Discarding the form at any moment and rehydrating reproduces it identically. |

---

## 8. Security considerations

- **Trust.** The host, its facets, and the consumer's code run in one trusted process. A facet reads only records and contributions the host wrote under the entity's fence (storage §6), and every blob read is verified against its id (**E17**).
- **Local materializations.** A physical facet's local form lives in a scratch location unique to the node and the entity (§2).
- **Hostile or corrupt records.** Replay never writes outside the workspace directory (§5.1) and never materializes a disk image past its bound (§5.4).
- **Statements from less trusted sources.** A SQL statement cannot reach outside the entity's database or control its transaction, and a query's result is bounded (§5.3).
- **The disk departure.** Whoever is attached to a guest sees its uncaptured writes (§5.4); bounding that exposure is the consumer's (machine §6).
- Transport security, the deserialization allowlist, and authorization are entity §11's.
