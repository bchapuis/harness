# Entity Facets: Specification

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** carry the meanings defined in RFC 2119.

A `§` with no document name refers to this spec. `entity §N` is [`entity-spec.md`](entity-spec.md), `storage §N` is [`entity-storage-spec.md`](entity-storage-spec.md), `workflow §N` is [`workflow-spec.md`](workflow-spec.md), `agent §N` is [`agent-spec.md`](agent-spec.md), and `machine §N` is [`machine-spec.md`](machine-spec.md).

---

## 1. Scope

A **facet** is a durable storage feature of one entity, defined entirely over the storage primitives, records and blobs (storage §5, §10). Facets share the entity's one journal, one atomic commit per decision, and one composite snapshot, and change nothing at or below the journal seam (storage §3.1). Besides facet 0, the event fold (entity §4.1), six exist: workspace (§5.1), KV (§5.2), SQL (§5.3), disk (§5.4), alarm (§5.5), and steps (§5.6).

Facets build on the per-command protocol (entity §6) and entity storage (storage §5, §9, §10); the agent runtime (agent §5.3), the persistent machine (machine §4, §6), and the workflow layer (workflow §4.3) consume them.

### 1.1 Non-goals

- **Durability machinery of a facet's own.** A facet MUST NOT add a replication path, fence, recovery protocol, or compaction; those are the substrate's (storage §5, §6, §9).
- **Application features as facets.** A feature expressible over records, blobs, and the existing facets is an application, not a facet.

### 1.2 Conformance

- **Host** (the entity host of entity §6): the host's side of §3, §4, and §6, and the §5 subsection of each facet it offers. A host MAY omit the SQL facet.
- **Facet:** the facet's side of §3.1 and §4, its own §5 subsection, and F1 to F4 (§7).
- **Consumer** (an entity type that declares facets): §3.2, §4.3, and the §5 subsection of each facet it declares.

---

## 2. Model

- **Records, blobs, contribution:** a facet is defined by its **records**, the ordered mutations it journals; its **blobs**, bulk bytes it references by content id (storage §10); and its **contribution**, its part of the composite snapshot.
- **Facet 0:** the entity's event fold; its records are the entity's events and its form is the entity's state.
- **Tag:** the byte that identifies a facet in the record envelope (storage §12) and keys its contribution (§6.1).
- **Form:** a facet's committed state as the host holds it: a logical facet's folded value, or a physical facet's local materialization (a database file, a directory, a block image).
- **Stage:** a facet's per-decision buffer: the operations a logical accessor staged, or the delta a physical facet captured.
- **Logical and physical facets:** a logical facet's records are semantic operations folded into an in-memory form (KV, alarm, steps); a physical facet's records are captured deltas applied on replay, and its local form changes during the decision (workspace, SQL, disk).
- **Capture:** turning a physical facet's local changes into a delta record in the current decision's batch.
- **Composite snapshot:** facet 0's state and every declared facet's contribution at one position *S* of the entity's record order, written as one snapshot (storage §9).
- **Root set:** the ids of every blob a facet's form can reference.
- **Accessor:** the operations one facet offers a consumer's decision.
- **Decision:** a command's handler or a callerless decision (entity §4.2, §4.3).

---

## 3. Seams

### 3.1 The facet seam

Between the host, which invokes, and a facet, which answers; the six facets of §5 are its only implementations.

| Operation | The host invokes it | The facet |
|---|---|---|
| begin | before each decision, on a fresh stage | opens its per-decision work; a physical facet fails if its local form is lost |
| seal | after the decision, before the append | closes that work; a physical facet captures its delta into the stage |
| drain | after seal | yields the stage's record payloads in order, possibly none |
| fold | per record with its tag: on replay, and after a commit for a logical facet only | applies the record to its form |
| restore | at rehydration, with its contribution or none | yields its form, empty without a contribution |
| rehydrate | once, after restore and replay, before the first command | resolves replayed records that name blobs into its form |
| discard | on every outcome but a commit | a physical facet drops its local form; a logical facet does nothing |
| snapshot | when the host writes a composite snapshot | puts its bulk bytes in the blob area and yields its contribution |
| roots | at every sweep | yields its root set |
| deadline | after every commit and at the end of rehydration | the alarm facet yields its deadline (§5.5), others none |
| consume | before the alarm decision (§5.5) runs | the alarm facet stages the deadline's clear, others nothing |

Begin, seal, fold, restore, rehydrate, and snapshot can fail with a facet error.

The facet's side:

- Its tag is nonzero, stable, and never reused (storage §12).
- It MUST meet **F1** to **F4** (§7).

The host's side:

- A failed begin, seal, or live fold is an outcome other than a commit, and the activation ends (entity §6).
- A failed restore, replayed fold, or rehydrate aborts the activation.
- It refuses a facet set whose tags are not distinct.
- It gives restore, rehydrate, and snapshot the entity's blob area (storage §10) and a scratch location unique to the node and the entity, for rebuildable caches only.

### 3.2 Declaration and accessors

Between a consumer and the host.

- A consumer declares its **facet set**: none, or up to four distinct facets, which fixes the tags replay recognizes for the type (§4.1).
- Every decision's context (entity §3.1) offers one accessor per declared facet, with its §5 operations and the rules of §4.3. Using an undeclared facet's accessor is refused at build time (**E10** applied to storage).

---

## 4. The facet contract

### 4.1 One batch, one order

**Every facet MUST turn a decision's writes into records in that decision's single batch, and MUST leave committed state unchanged on any outcome but a commit.** That batch holds the decision's events and facet records, interleaved in the entity's one record order, and commits everywhere or nowhere (**E19**). Replay dispatches each record by tag; a tag no declared facet claims MUST abort the activation, never be skipped (**E19**). Event reads and subscriptions deliver only facet 0's records (entity §7.2).

### 4.2 Logical and physical facets

A logical facet's fold is pure (**F1**, generalizing **E2**) and runs only after the batch commits (**E1**); a physical facet's deltas apply byte-deterministically (**F1**).

**The physical discipline:** a physical facet's local form MAY run ahead of committed state only until the decision's outcome (for the disk facet, the one declared departure, until the next capture, §5.4). On any outcome but a commit the host MUST discard it before the entity serves again, and the next activation rebuilds it from the composite snapshot plus committed records (**E20**).

### 4.3 Staging

- **Decision-scoped.** A facet write outside a decision (in the activation or deactivation hook, entity §3.1) MUST fail loudly, never be dropped or deferred: the SQL and disk accessors return an error, and the others raise a fault that ends the hook.
- **Plan, then stage.** Staged operations commit even when the reply is an application error (entity §3.2), so an accessor MUST do its fallible work (blob puts, scans) first and stage only on success.
- **Read your staged writes.** A logical accessor reads committed state plus what the current decision staged; outside a decision it reads committed state.

### 4.4 The composite snapshot and capture

The composite snapshot is written through the journal (storage §3.1, §9.1) in the `entity.snapshot` format (storage §12), and **E4** applies to the whole; a facet declared after the last snapshot restores empty. A snapshot MUST be the committed image: a physical form's uncaptured writes MUST NOT enter it. **A capture MUST yield a delta that, replayed onto the last committed form, reproduces exactly the form it read at a point no writer was mutating.**

### 4.5 Blob roots and sweep safety

Every facet reports its root set (**F3**) and MUST NOT sweep the blob area. The entity's one sweep is the blob area's (storage §10), invoked by the consumer with its own roots; the host MUST add every facet's root set, facet 0's included (storage §12). A sweep MUST also keep every blob the activation has put since its current or latest decision began, and a put MUST NOT start while a sweep that has read its retained set runs. A later sweep reclaims blobs orphaned by a failed commit.

---

## 5. The facets

### 5.1 Workspace (physical)

A node-local directory that a shell, a container, or a microVM works in directly. Its **durable tree** is its regular files whose path is valid UTF-8 and has no component named `node_modules`, `target`, `.venv`, `.git`, `__pycache__`, `dist`, or `build`; directories are implied by their files, and empty directories, symbolic links, and special files are not kept. The durable tree is bounded at 64 MiB (2^26 bytes).

| Operation | Takes | Yields |
|---|---|---|
| directory | | the directory's path, valid once rehydration has materialized it |
| capture | | the count of files written and removed and the durable tree's size, or an error: not materialized, directory lost, tree over the bound, or a local I/O failure |

- **Capture.** A decision captures after its side effects land, staging one record per changed file, with its whole content, and one record naming the deleted paths (**E19**); an unchanged tree stages nothing. Replay writes the recorded bytes and removes the recorded paths, never re-executing what produced them (**F1**).
- **Over the bound.** A capture over the bound stages nothing and reports it; the next capture compares against the last committed tree, so no change is lost.
- **A lost directory.** A capture that finds the directory gone reports it and stages nothing, never the deletion of every file. A decision that begins without the directory fails, and the next activation rebuilds it (§4.2).
- **Snapshot.** The contribution names the blobs holding each durable file's content; they are the root set (**F3**). A snapshot that finds a durable file differing from its committed content MUST fail rather than include it.
- **Replay stays inside the directory.** A record path that is empty or has an empty, `.`, or `..` component MUST abort the activation.

Consumers: the agent's sandbox (agent §5.3) and the machine's guest (machine §4).

### 5.2 KV (logical)

An ordered map from string keys to byte values; keys order bytewise.

| Operation | Takes | Yields |
|---|---|---|
| get | key | the value, or none |
| put | key, value | stages the write, or an error if a large value's blob put fails |
| delete | key | stages the removal |
| keys | prefix | the keys with that prefix, in key order |
| list | prefix | the key and value pairs with that prefix, in key order |

- Records are puts and deletes, folded into the map, last write wins (**F1**); the contribution is the encoded map (**F2**).
- A value above the host's inline size is put in the blob area, durable, before the map names it (§4.3), and every read verifies it (**E17**); such values' ids are the root set (**F3**).

### 5.3 SQL (physical)

A private database per entity, speaking the SQLite dialect. A value is null, a 64-bit integer, a 64-bit float, text, or a blob.

| Operation | Takes | Yields |
|---|---|---|
| execute | statement, parameters | the number of rows changed |
| query | statement, parameters | the rows |
| query one | statement, parameters | the one row, or an error if the query yields any other number |
| select | statement, parameters | the column names and the rows |

Every operation returns failure to the decision as an error value.

- **One decision, one transaction, one record.** A decision's statements run in one implicit transaction that never spans decisions. At its local commit the host MUST capture exactly the database pages it wrote, in order, as one record (**E19**); a transaction that writes nothing yields none, and a decision that runs no statement opens no transaction.
- **Replay never re-executes.** Replay applies the recorded pages (**F1**), so a statement MAY be nondeterministic.
- **Discipline.** The database changes at the transaction's local commit, before the append (§4.2, **E20**).
- **Snapshot.** The contribution names the blobs holding the database image at *S*; they are the root set (**F3**). Restore fetches them, verified (**E17**), and materializes the database.
- **Statements stay inside the decision and the database.** The host MUST refuse with an error a statement outside a decision, and one that controls a transaction or savepoint, attaches or detaches a database, or sets a pragma. A consumer sets up its schema with idempotent statements inside a writing decision.
- **Bounds.** A query yielding more than 10,000 rows MUST fail with an error, never return a truncated result. A transaction's pages ride one record, so a consumer SHOULD split a bulk load across decisions.

### 5.4 Disk (physical)

A fixed-size **block image**, a file a guest mounts as its root filesystem. Block bytes never ride a record: a capture puts each changed block in the blob area, then stages exactly one **manifest**, holding an image size and, for each block it covers, the block's index and blob id (**E19**), so a capture commits whole or not at all.

| Operation | Takes | Yields |
|---|---|---|
| path | | the image file's path; the file exists once an import has committed |
| size | | the committed image size in bytes, zero before an import commits |
| content digest | | an identifier of the committed content, none before an import commits, unchanged by a rehydration that reproduces the same blocks |
| import | a source image | the count of blocks and bytes staged, or an error |
| capture | | as import |

- **Import.** An import replaces the image wholesale and stages one manifest covering every block, each put into this entity's own blob area. An import larger than 16 GiB (2^34 bytes) MUST be refused.
- **Capture rides a decision.** A decision stages at most one capture or import, and the host captures only when a decision asks. The consumer MUST drive every capture through an explicit command, MUST capture only while the guest is not writing (§4.4), and its hibernation veto (entity §3.1) MUST refuse eviction while the image holds uncaptured writes. A capture MUST obtain exactly the blocks changed since the last committed capture or import; an unchanged image stages nothing.
- **Off the consensus worker.** A capture or import MUST NOT run on the worker that drives the node's consensus; it runs through the node's blocking-I/O seam (storage §3.4).
- **The declared departure.** Before the next capture, the guest's uncaptured writes are observable to the guest and to whoever is attached; entity-level observables (records, replies, event reads) expose only committed state (**E20**). Bounding the wider exposure is the consumer's (machine §6).
- **Snapshot.** The contribution is a manifest covering the committed image (§4.4), naming only blobs already put, so a checkpoint puts nothing; a region no manifest covers restores as zeros. The root set is the blocks the restored manifest and every later one name (**F3**). Replay collects the later manifests; rehydrate fetches their blocks, verified (**E17**), and applies them in record order (**F1**).
- **Bounded.** A replayed or restored manifest that names an image above 16 GiB, or a block outside its image, MUST abort the activation.

### 5.5 Alarm (logical)

At most one pending instant per entity, its **deadline**, which when due runs the **alarm decision**, the entity's callerless decision (entity §4.3).

| Operation | Takes | Yields |
|---|---|---|
| pending | | the deadline, or none |
| set at | an instant | stages it as the deadline, replacing any other |
| set after | a duration | stages a deadline that long after the system's current time |
| clear | | stages the deadline's removal |

- Set and clear records fold into one optional deadline, last write wins. The root set is empty.
- **Firing goes through the barrier.** The host arms a timer from each deadline the facet yields (§3.1) and, when due, runs the alarm decision through the per-command protocol (entity §6); a past deadline is due at once. A superseded timer's late delivery MUST be a no-op.
- **Consume on fire** (**E21**). The fired deadline's clear joins the alarm decision's batch, clearing atomically with its effects unless the decision sets a new deadline; a failed commit leaves it pending to fire again.
- **Liveness.** An **alarm index** (`entity.AlarmIndex`, §6.2) holds the pending deadlines of one hosted type's entities in one shard; an index serves exactly the hosted types that declare the alarm facet. On a node configured with an alarm index, a committed deadline MUST run the alarm decision once due, on the shard's current leader, whether or not the entity is resident. Without an index an alarm fires only while its entity is resident, so a failover delays it to the next access.
- **A pending alarm vetoes hibernation.** An entity with a pending deadline MUST stay resident until its shard's alarm index has acknowledged it. The host registers each deadline change with a request the index answers only after committing it (entity §6); registration never delays a decision, and a failed one is retried. The folded deadline, not the index, decides whether and when the alarm decision runs, so a stale entry costs at most a wake that fires nothing.

### 5.6 Steps (logical)

Durable memoization for the step, sleep, and retry pattern. A **step** is one external effect whose result the consumer journals, named by its **step id**. The facet's form, the **memo**, maps step ids to result bytes, so a later drive, re-activation, or failover resolves a completed step from it. A step's effect returns its result to the entity as a step-completion command (`entity.StepDone`).

| Operation | Takes | Yields |
|---|---|---|
| result | step id | the decoded result, none if not completed, or an error if it does not decode |
| is done | step id | whether the step has completed |
| record | step id, result | stages the result |
| complete | a step-completion command | stages its result unless the step has completed |

The root set is empty. The **launch guard**, never journaled, records the steps this activation has launched: it claims a step (true only the first time), tests a claim, tests that no claim is held, and forgets every claim.

- **Step ids.** A step id is the consumer's stable call-site ordinal: a step MUST get the same id on every re-drive and replay.
- **The memo is write-once.** Because the launch guard is per activation, a re-activation MAY re-launch a step whose result has not committed. The first committed result MUST win; every later completion of that step commits nothing.
- **Sleep and retry.** A sleep is a step whose effect is an alarm (§5.5); a retry re-launches after an alarm-backed backoff.
- **No double memo.** An entity whose step results are already its events MUST NOT memoize them again; it uses only the launch guard. The workflow layer generalizes launching, guarding, and recovering orphaned effects above the entity (workflow §4.3).

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

Every tag lies in the envelope range storage §12 gives the host's own facets; the range for facets defined elsewhere is unallocated.

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

- **Trust.** The host, its facets, and the consumer's code run in one trusted process. A facet reads only records and contributions the host wrote under the entity's fence (storage §6), and every blob read is verified (**E17**).
- **Local materializations.** A physical facet's local form lives in a scratch location unique to the node and the entity (§3.1).
- **Hostile or corrupt records.** Replay stays inside the workspace directory (§5.1) and the disk image bound (§5.4).
- **Statements from less trusted sources.** A SQL statement cannot leave its database or control its transaction, and its result is bounded (§5.3).
- **The disk departure.** Whoever is attached to a guest sees its uncaptured writes (§5.4).
- Transport security, the deserialization allowlist, and authorization are entity §13's.
