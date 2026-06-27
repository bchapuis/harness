# Durable SQLite databases and durable workspaces on the granary substrate

**Status:** Research note (no implementation)
**Purpose:** Decide whether granary's per-grain replication substrate can host a **durable SQLite database** and a **durable filesystem / workspace** that survive hibernation, migration, and node loss — the deferred "alternative record interpretations (SQLite, File)" of [`granary-spec.md`](../docs/granary-spec.md) §16. This note researches the mechanism and the model changes it forces; it builds nothing.

References: `granary §N` → [`granary-spec.md`](../docs/granary-spec.md); `harness §N` → [`agentic-harness-spec.md`](../docs/agentic-harness-spec.md); `DO §N` → [`durable-objects.md`](durable-objects.md). Claims inferred rather than sourced are marked **(inferred)**.

---

## 1. The question, and where we start from

granary already gives a grain **durable, quorum-replicated, term-fenced, single-writer storage** with rehydrate-on-activation (granary §6–§9). What it stores today is an **event log folded by a pure `apply` into a small in-memory `State`** (granary §4.1). The two artifacts the user wants — a SQLite database and a working-directory filesystem that survive hibernation — are explicitly *not* that:

- They are **large on-disk artifacts** (megabytes to gigabytes), not a small foldable value.
- Their natural "record" is not a user `Event` but a **physical byte mutation** (a SQLite WAL frame, a file byte-range write).
- For the harness workspace specifically, durability is today **deliberately given up**: the sandbox is one-per-activation, released on every hibernation and migration, and the loss is put on the record as a `WorkspaceReset` so the model re-derives rather than trusting vanished state (harness §5.1, §5.5, §7.2, invariant H8). Making the workspace survive hibernation is a reversal of that stance, and the note must say what that costs.

The good news, established in §2: **the hard part — durability, ordering, fencing, quorum recovery — is already built and carries over unchanged.** What is missing is a different *materialization* layer above the same Replicator, plus a snapshot seam that can move bytes at scale. The rest of this note is about that delta.

---

## 2. One substrate, three record interpretations

granary's journal seam (`GrainJournal`, granary §7.3) already operates on **opaque, codec-agnostic record bytes**:

```rust
fn append(&self, grain: &GrainName, after: Seq, events: Vec<Vec<u8>>) -> AppendOutcome; // quorum, term-fenced
fn load (&self, grain: &GrainName, from: Seq, limit: usize) -> Vec<(Seq, Vec<u8>)>;
fn head (&self, grain: &GrainName) -> Seq;                                              // quorum read-repair recovery
```

Nothing in `append`/`load`/`head` or in the Replicator below it (granary §7.2, `replicator.rs`, `store.rs`) interprets the bytes. The whole distributed-systems core is **content-blind**:

- **single writer per grain** → the records are already totally ordered, no consensus on the data path (granary §7.2, G1);
- **term fence** → a deposed leader's append reaches no quorum, so the byte stream never forks (granary §8, `StoreAck::Fenced`/`Stale` in `store.rs`);
- **quorum read-repair recovery** → a fresh leader reconstructs the grain's head slot-by-slot from a write quorum, losing no acknowledged record (granary §8, §9, G14);
- **input/output gates** → no second command observes half-applied state; the reply waits until the record is durable (granary §6).

So the only variable across grain flavors is **"what is the record, and how is it materialized into servable state?"**

| Flavor | Record (`Vec<u8>`) | Materialized state | Snapshot | Status |
|---|---|---|---|---|
| **Event** (today) | user `Event`, codec-encoded | in-memory `State` via pure `apply` fold | serialized `State` blob | shipped |
| **SQLite** | a **WAL frame** = `(page#, page bytes, commit marker)` | an **on-disk SQLite DB file** | a **checkpointed DB file** | deferred (§16) |
| **File** | a **byte-range / slice write + inode op** | an **on-disk directory tree** | a **metadata image + live-block manifest** | deferred (§16) |

This is exactly the Cloudflare Durable Objects framing (DO §4.2): a WAL frame is "write these bytes at this offset," an opaque position-addressed idempotent record, shipped to a quorum on commit. granary's Replicator is the same shape as Cloudflare's Storage Relay Service, minus the interpretation. The §16 extension is therefore **not a new storage engine — it is two new record interpreters over the existing one.**

---

## 3. The SQLite grain

### 3.1 Two sourcings: statement (free today) vs frame (the real thing)

There are two ways to make a SQLite-backed grain, and they are the database analogue of **logical vs physical replication**.

- **Statement-sourced** — the `Event` is a SQL statement; `apply` runs it against an in-memory SQLite connection. **This needs nothing new**: it is an ordinary event-sourced grain whose `State` happens to be a `rusqlite` in-memory database serialized via the backup API. Its limits are the limits of the event model: the database must fit in memory and replay cost grows with history (bounded only by snapshots), and **every statement must be deterministic** — `random()`, `CURRENT_TIMESTAMP`, and implicit `ROWID` allocation must be avoided or pre-resolved, because replay re-executes them and any divergence violates the deterministic-fold invariant (granary G2). Good for small, append-mostly metadata; not a "durable object database."

- **Frame-sourced** — the record is the **physical WAL frame** SQLite produces, shipped *after* the engine has executed the statement. This is the deferred §16 work and the thing worth building, for three reasons: (1) the database lives **on disk**, so it can be gigabytes and reads are zero-latency local (DO §4.2, granary §7.5); (2) replay is **byte-identical and deterministic regardless of SQL nondeterminism**, because you ship the resulting pages, not the statement that produced them — `random()` is resolved once on the writer and the same bytes land everywhere; (3) it matches the proven Cloudflare/dqlite/libSQL design.

The rest of §3 is about frame-sourcing.

### 3.2 The record: a WAL frame

SQLite in WAL mode appends **frames** to a `-wal` file (source: SQLite *WAL-mode File Format*, `walformat.html`). Each frame is a **24-byte header + one page of data**:

| Field | Meaning |
|---|---|
| page number | which DB page these bytes replace |
| DB size after commit | **non-zero ⇒ this frame ends a transaction (commit marker)**; zero mid-transaction |
| salt-1, salt-2 | WAL "epoch" tags; salt-1 increments and salt-2 randomizes on each checkpoint/reset, invalidating stale frames |
| checksum-1, checksum-2 | running Fibonacci-weighted checksum chained frame-to-frame; a reader stops at the first bad checksum, which is how SQLite finds the valid tail |

A transaction is a run of frames ending in a commit frame. To apply received frames on a replica and obtain a byte-identical database, write each frame's page bytes to `(page# − 1) × page_size` and, on the commit frame, set the DB size to the stated page count. **This is precisely granary's `apply`, with the fold target moved from an in-memory value to a file** — position-addressed, idempotent, order-sensitive within the log. granary's per-`Seq`-slot idempotency (granary §7.2) and quorum read-repair (granary §8) work on these bytes unchanged; a late-committing timed-out frame simply reappears at its slot on recovery, exactly as an event would.

### 3.3 Interception in Rust — where to capture the frame

Four interception points, from best-fit to worst for this runtime:

1. **libSQL virtual WAL (`xFrames`) — recommended.** libSQL (Turso's SQLite fork) exposes `libsql_wal_methods` modeled on the `sqlite3_vfs` API; the `xFrames` hook is called **with the actual frames at the commit boundary**. You receive already-parsed semantic frames `(page_no, page_data, commit_flag, count)` — exactly the record to ship — and libSQL's own WAL-streaming replication proves the pattern. Cost: build against libSQL instead of stock SQLite (libSQL has Rust bindings). This is the cleanest match to "ship opaque frames, replay them."

2. **Custom VFS over stock SQLite (the dqlite pattern).** SQLite's VFS (`xOpen/xRead/xWrite/xSync/...`) is interceptable from Rust via the `sqlite-vfs` crate (rkusa) or the newer `sqlite-plugin` (orbitinghail/Graft), used through `rusqlite`'s `open_with_flags_and_vfs`. dqlite keeps the DB and WAL **entirely in memory** in a custom VFS, and on commit encodes the modified pages into a replicated log entry, applying to the in-memory WAL image only after the quorum commits — a 1:1 template for granary's output gate. Cost: the VFS sees **raw byte writes to the `-wal` file**, so you parse the frame structure yourself (§3.2), but you get them synchronously with no checkpoint race, on stock SQLite + `rusqlite`.

3. **`sqlite3_wal_hook` — notification only, not a feed.** The hook fires after each WAL commit with the WAL page count (`rusqlite::Connection::wal_hook`). It tells you a commit happened but hands you no frames; you must then read `-wal` yourself and race the next checkpoint. Usable as a *trigger* for an "drain new frames now" step, not as the primary data path. (This is what classic Litestream did, and why it had to hold read locks to stop the WAL resetting under it.)

4. **FUSE (the LiteFS approach) — only if you cannot link the engine.** LiteFS interposes a FUSE filesystem under the DB file, watches `-wal` writes and lock transitions to infer transaction boundaries, and packages changed pages into LTX files. Language-agnostic and fully transparent, but adds a syscall tax, a mount, and privileges, and forces you to *reconstruct* commit boundaries from lock state. Since granary hosts the engine in-process, options 1–2 dominate.

**Avoid `cr-sqlite`.** It is CRDT row-level merge for *multi-writer* convergence; granary grains are single-writer (the term fence), so physical frame shipping gives byte-identity for free and CRDT merge would only throw that away.

### 3.4 The gates map exactly

The SQLite commit path slots onto granary's §6 protocol with no new concept:

```
1. command executes the SQL against the local engine        (the writer mutates -wal locally)
2. xFrames / VFS captures the transaction's frames          (the record batch)
3. journal.append(grain, head, frames)  ── quorum, term-fenced ──  OUTPUT GATE held here
4. on Committed: the local commit is now durable on a quorum; release the reply
   on NotLeader/Unavailable: discard, step down — the writer's local -wal is a stale cache (G3)
```

The **input gate** (no second command while an append is in flight, granary §6) gives single-threaded SQLite exactly the serialization it wants; there is never a concurrent reader to observe a frame that has not yet quorum-committed. The subtlety to get right: SQLite considers a transaction committed **locally** once its commit frame hits the local `-wal`, *before* the quorum acks. The host must therefore hold the application-visible success (the output gate already does this) and, on `Unavailable`/`NotLeader`, **abandon the local commit** — step down and let rehydration rebuild the DB from the quorum-durable head — rather than letting a subsequent read observe a locally-committed-but-not-replicated transaction. This is the byte-stream form of granary's existing "fold only after durability, fold only a contiguous head" rule (G1).

### 3.5 Checkpoint = snapshot; the 2×-size rule

A SQLite **checkpoint** folds `-wal` pages back into the main DB file and restarts the WAL. That is the natural snapshot boundary: after a checkpoint the main DB file **is** a consistent snapshot of everything up to that point. This maps onto granary's `save_snapshot`/`load_snapshot` (granary §9), with the snapshot payload being the **checkpointed DB file** instead of a serialized `State`.

Cloudflare's snapshot cadence is the rule to copy (DO §4.2, source: Cloudflare SRS blog): **snapshot whenever the accumulated log since the last snapshot exceeds the live database size.** This bounds cold-start reconstruction to ≤ 2× the database size (one snapshot + at most one DB's worth of frames) and bounds total stored bytes to ~2× as well. It is also why Cloudflare caps a SQLite-backed object at 10 GB — beyond that, rehydration streams too much. granary's per-grain compaction (granary §9 — a replica drops the record prefix a snapshot subsumes, advancing the per-grain base) already implements the "drop frames below the checkpoint" half; the missing half is the size-driven *trigger* and a snapshot payload that is a file, not a blob (§6.1).

---

## 4. The filesystem / workspace grain

A workspace is a working directory: files the agent creates and edits, which today vanish on hibernation (harness §5.5). Making it durable is a *general* filesystem-over-a-log problem, and the cleanest published template is **JuiceFS's split** (source: JuiceFS architecture/internals docs).

### 4.1 The record: metadata op + immutable content block

JuiceFS separates **metadata** (the authoritative state) from **data** (immutable, content-addressed):

- **Metadata** — the inode table, directory tree, permissions, timestamps, and the `chunk → slice → block` map. This is the source of truth and the part that must be totally ordered.
- **Data** — files split into chunks (≤ 64 MiB), chunks into **slices** (one per contiguous write), slices into fixed **blocks** (≈ 4 MiB) stored by content hash. **Slices are immutable and append-only**: an overwrite or a byte-range write creates a *new* slice that shadows older bytes, and the metadata records which slice wins each range.

So a filesystem mutation decomposes into a **data record** — `(inode, offset, len, slice_id)`, where the block bytes are content-addressed — and a **metadata op** — create / unlink / rename / chmod / truncate / setxattr. Both are idempotent and replayable. This is the filesystem analogue of a WAL frame, and it rides granary's Replicator the same way.

The decisive property for granary: **the metadata is small and is the only thing that must flow through the per-grain quorum log.** Bulk data blocks are immutable and content-addressed, so they can live in a shared object store (or, single-node, a local content-addressed directory) and be referenced by hash from the metadata records. The grain's `Seq`-ordered record stream is the metadata op log plus block references; it stays small even when the workspace is large.

### 4.2 Small workspace vs large workspace

- **Small workspace (whole-directory grain).** Records = byte-range/inode mutations; the snapshot is a tar/manifest of the live tree; rehydration replays the log and rebuilds the directory in full. Simple, and fine for an agent workspace of a few thousand small files. Replay materializes everything up front.

- **Large workspace (lazy hydration).** Materializing gigabytes on every activation defeats cheap hibernation. The fix is the **Litestream v0.5 / VFS read-replica pattern**: rehydrate only the **metadata** eagerly; **fault in data blocks on first access** and cache them locally. A grain that re-activates and then touches ten files pulls ten files' blocks, not the whole tree. This is what makes a large durable object hibernate cheaply, and it is the same move that lets Cloudflare evict aggressively.

### 4.3 Selective durability

A workspace is not uniformly worth replicating. `node_modules`, `target/`, `.venv`, and build caches are large, regenerable, and churn-heavy; replicating their byte streams through a quorum is waste. A durable-workspace design should support **include/exclude rules** (a `.granaryignore`, or an explicit "durable paths" set), replicating source and agent-authored artifacts while leaving regenerable trees to a non-durable local overlay that is rebuilt by re-running the build on rehydration. This keeps the record stream proportional to *meaningful* change, not disk churn — the filesystem analogue of not journaling scratch state. (This directly tempers the cost of reversing harness §5.5: durable does not have to mean *all* of the workspace.)

---

## 5. Hibernation and rehydration: the durable / cache line

The whole design rests on one line drawn cleanly:

**Durable (must survive eviction and machine moves — lives in the quorum):**
- the ordered **record stream** back to the last snapshot (WAL frames, or metadata ops + block refs), term-fenced and quorum-acked;
- periodic **snapshots** (checkpointed DB file; metadata image + live-block manifest);
- the **commit watermark** (highest quorum-acked `Seq`) — already granary's recovered `head` (granary §9);
- a **whole-state rolling checksum** **(inferred** as a desirable add; LiteFS carries one per transaction**)** so a rehydrated replica can *prove* byte-identity and detect split-brain before serving.

**Rebuildable local cache (drop freely):**
- the on-disk DB file, `-wal`, `-shm`, page cache; the materialized workspace directory and block cache. After eviction these are reconstructed from snapshot + bounded replay (+ lazy fetch).

Two cases that granary treats alike today but which differ sharply in cost here, and this distinction is the practical heart of the user's question:

- **Same-node idle hibernation (the easy win).** The grain leaves memory but the **leader does not move** (granary §10 idle eviction). The on-disk DB file / workspace directory can stay on local disk as a **warm cache**; reactivation needs only to confirm the head from the quorum and re-open the file — no large transfer. *This is the case where "survive hibernation" is cheap and clearly worth doing.* It requires granary to let a hibernated grain leave its materialized artifact on disk rather than deleting it, keyed by grain name, and to validate it against the recovered head on re-open (discarding it if a higher term wrote past it while it slept).

- **Cross-node migration (the expensive case).** Leadership moves (granary §8.3), so the new leader has **none** of the grain's bytes locally and must rebuild from the quorum: download the latest snapshot, replay frames to the head. For a multi-GB grain this is real work, and it is why §4.2's lazy hydration matters and why §6.4's hibernation policy must differ from the 10s event-grain default.

Cloudflare accepts exactly this asymmetry (DO §5): cheap idle hibernation, more costly relocation, mitigated by lazy reconstruction and sticky placement. granary should too.

---

## 6. What this demands of the granary model (the delta)

Everything in granary §6–§8 (gates, term fence, quorum append, head recovery, ordering, single-writer) carries over **unchanged** — that is the headline. The gaps are all in the *materialization and snapshot* seam, plus lifecycle tuning.

### 6.1 The snapshot seam must stream, not pass a blob
Today `save_snapshot(grain, at, state: Vec<u8>)` and `load_snapshot → Option<(Seq, Vec<u8>)>` move the entire snapshot as one in-memory buffer (`journal.rs`), and `host.rs` `rehydrate` folds events into one in-memory `self.state`. A checkpointed multi-GB DB file cannot be a `Vec<u8>`. The seam needs a **chunked/streamed snapshot** (write/read by ranges, or a content-addressed block list) and a snapshot store that can hold large objects (local file dir single-node; object store clustered). This is the single largest seam change.

### 6.2 The host needs a pluggable materializer
`host.rs` hard-codes "fold record via `G::apply` into in-memory `State`." SQLite/File grains need the fold target to be **an on-disk artifact**: `apply_frame(file, frame)` / `apply_fs_op(tree, op)`. The clean shape is a **`Materializer` trait** the host drives instead of `G::apply` directly, with three implementations (in-memory fold = today's behavior; SQLite-file; FS-tree). The durability protocol (granary §6 steps 1–6) does not change; only the "fold AFTER durability" line dispatches through the materializer. The decide/apply split (granary §4.2) also loosens: a SQLite grain's "decision" is *execute the statement and capture the resulting frames*, which is an effectful local execution, not a pure `(state) → (events, reply)`. Worth spelling out as a distinct `GrainHandler` variant rather than bending the pure-fold one.

### 6.3 Determinism restated (G2 still holds, differently)
Frame-sourcing makes G2 *easier*, not harder: replay applies bytes, so it is deterministic even when the originating SQL was not. The obligation moves to "the captured frame stream is the canonical effect" — which the writer guarantees by capturing post-execution. Statement-sourcing (§3.1) keeps the old, stricter G2 (deterministic SQL only). The invariant catalogue should distinguish the two.

### 6.4 Hibernation policy must be size-aware
granary's 10s `idle_after` default (granary §10) assumes reactivation is a cheap snapshot+replay. For a large SQLite/workspace grain that is false on migration. These grain types want: a **longer `idle_after`**, **on-disk artifact retention across same-node hibernation** (§5), **sticky placement** so leadership moves reluctantly, and **lazy hydration** (§4.2) so even a cold reactivation does not materialize the whole artifact up front. This is per-grain-type config, not a global change.

### 6.5 What does *not* change
No new transport, no new consensus, no change to the shard/leader-election/shard-map machinery, no change to the term fence or quorum recovery, no change to the gateway/routing. The Replicator (`replicator.rs`, `store.rs`, `replica_store.rs`) ships and recovers opaque bytes already; SQLite frames and FS records *are* opaque bytes. This is the payoff of granary having kept the journal a content-blind seam.

---

## 7. A phased path (if this is pursued)

1. **Statement-sourced SQLite grain** — zero new substrate; validates the ergonomics of a SQLite-shaped grain and the deterministic-SQL constraint. A day's work over the existing model.
2. **Streamed snapshot seam (§6.1)** — the prerequisite for any large-artifact grain; useful on its own (large event-grain snapshots).
3. **`Materializer` trait + on-disk SQLite via custom VFS (dqlite pattern) on the `Local` tier** — frame capture, file replay, checkpoint-as-snapshot, all single-node, fully in the deterministic simulator (granary §14). Proves byte-identical replay with no clustering.
4. **`Quorum` tier** — frames ride the existing per-grain quorum append and read-repair recovery; this is where it becomes a true durable object. Most risk is in §6.4 lifecycle tuning, not the data path.
5. **Filesystem grain (JuiceFS split) + lazy hydration + selective durability** — the workspace use case; reuses 2–4 with a different record interpreter and a content-addressed block store.
6. **libSQL virtual WAL** as an alternative to the custom VFS if the cleaner `xFrames` feed proves worth the libSQL dependency.

---

## 8. Open questions

- **Snapshot of a live SQLite file under the single-writer gate.** A `VACUUM`/full checkpoint can be large and slow; does it block the input gate, or run against a frozen WAL generation while writes continue? Cloudflare snapshots asynchronously off the write path — granary needs the same, which interacts with §6.1's streaming seam.
- **Block store ownership for the FS grain.** Are immutable data blocks per-grain (replicated through its quorum) or in a cluster-shared content-addressed store referenced by hash? Shared is far cheaper for large workspaces but adds a second durability domain off the grain's quorum — its GC and its own replication need a design.
- **Rolling whole-state checksum.** Worth adding to granary's recovery (G14) generally, or only for byte-stream grains? It is cheap insurance against a silently divergent replica that quorum-intersection alone would not catch if a record were misapplied.
- **Reversing harness §5.5 selectively.** With a durable workspace available, does the harness keep `WorkspaceReset` for the non-durable (excluded) subtree and drop it for the durable subtree? The two coexist; the record stream should say which paths survived.
- **Is dqlite's in-memory-VFS model or an on-disk-VFS model right?** dqlite keeps DB+WAL in memory (fast, size-capped); a true durable object wants on-disk (large, zero-latency local reads). The VFS work differs; §3.3 leans on-disk but this needs prototyping.

---

## Sources

- granary substrate: [`granary-spec.md`](../docs/granary-spec.md) §6–§9, §16; harness workspace stance: [`agentic-harness-spec.md`](../docs/agentic-harness-spec.md) §5.1, §5.5, §7.2; Durable Objects lessons: [`durable-objects.md`](durable-objects.md).
- Cloudflare, *Zero-latency SQLite storage in every Durable Object* — https://blog.cloudflare.com/sqlite-in-durable-objects/ (SRS, WAL-frame shipping, 3-of-5 quorum, snapshot-when-log≥db-size, 16 MB/10 s batching, 30-day PITR, 10 GB cap).
- SQLite, *WAL-mode File Format* — https://sqlite.org/walformat.html ; *Database File Format* — https://sqlite.org/fileformat.html ; *WAL hook* — https://sqlite.org/c3ref/wal_hook.html
- libSQL virtual WAL — https://github.com/tursodatabase/libsql/blob/main/libsql-sqlite3/doc/libsql_extensions.md and `wal.h`; Turso sync (physical pages, byte-identical) — https://docs.turso.tech/libsql
- dqlite replication (custom VFS, pages → Raft entry → apply on commit) — https://canonical.com/dqlite/docs/explanation/replication and `src/vfs.c`
- Rust VFS crates: `sqlite-vfs` — https://github.com/rkusa/sqlite-vfs ; `sqlite-plugin` (Graft) — https://github.com/orbitinghail/sqlite-plugin
- LiteFS / LTX (FUSE, transaction-file shipping, rolling whole-DB checksum) — https://github.com/superfly/litefs/blob/main/docs/ARCHITECTURE.md ; https://fly.io/blog/introducing-litefs/
- Litestream (checkpoint-as-snapshot, shadow WAL, generations; v0.5 LTX + VFS read replicas / lazy hydration) — https://litestream.io/how-it-works/ ; https://fly.io/blog/litestream-v050-is-here/
- JuiceFS (metadata engine + content-addressed immutable slices/blocks; the FS-over-log template) — https://github.com/juicedata/juicefs/blob/main/docs/en/introduction/architecture.md ; https://juicefs.com/docs/community/internals/
- cr-sqlite (CRDT row-level merge — why *not* to use it for single-writer physical replication) — https://github.com/vlcn-io/cr-sqlite
