# Blob Store: a namespaced, content-addressed object store on the actor framework

**Status:** Draft v1
**Scope:** A store for **immutable** binary blobs, addressed by the BLAKE3 hash of their content **within a consumer-chosen namespace**, the **unit of deletion** (§2, §5.3), with a single-node tier and a clustered replicate-by-hash tier behind one seam. Chunking, naming, directory structure, and fine-grained liveness belong to the consumer.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

`blob-store` is the crate and namespace name. Sections of this document are cited as plain **§N**, and invariants defined here are numbered **B1, B2, …**. Cross-references: `entity §N` → [`entity-spec.md`](entity-spec.md); `actor §N` → [`distributed-actor-spec.md`](distributed-actor-spec.md); `utilities §N` → [`cluster-utilities-spec.md`](cluster-utilities-spec.md); `wal §N` → [`wal-spec.md`](wal-spec.md); `DO §N` → [`../research/durable-objects.md`](../research/durable-objects.md).

> **Design stance.** Immutable, content-addressed blobs need durability and deletion **without consensus**. A content hash names exactly one byte sequence, so there is nothing to *order* and nothing to *agree on*: two writers of the same bytes cannot conflict, and a reader proves it received the right bytes by re-hashing them. That removes the entity journal's three hard pieces (the per-record order `Seq`, the single-writer term fence, and quorum-intersection recovery, entity §8) and leaves the entity Quorum replicator (entity §7.2) without its hard half: store the bytes on enough machines, read them back from any one, and verify (the Durable Objects split of *ordering apart from durability*, DO §4.3, taken to the limit). Scoping identity to a **namespace** makes reclamation a monotonic, never-reclaimed "this namespace is gone" tombstone (§5.3), not a reference-tracking garbage collector: no root set, no mark-and-sweep, no coordination.

---

## 1. Scope and layering

The crate sits **beside** entity, not above it: it is built from plain cluster actors (actor §3) because it needs no virtual identity, journal, single-activation lease, or gateway. It depends on `actor-cluster` (transport, membership, placement) and `wal`, and on nothing in entity. Consumers layer **above** it. It provides the **`BlobStore`** seam (§3), `put`, `get`, `has`, and `delete_namespace` over opaque bytes addressed by content within a namespace (§2), and two implementations of it, the `Local` and `Clustered` tiers (§5).

It is **not** a filesystem, a database, or an index: it imposes no structure on a blob's bytes, names blobs only by content hash within the consumer's namespace, and tracks no relationships between them. Two concerns belong to the **consumer**:

- **Chunking and metadata.** How a large object is split into blobs and assembled, and what directory or inode structure references them (the motivating consumer is the durable filesystem entity, `research/durable-sqlite-and-filesystem.md` §4).
- **Fine-grained liveness.** The store cannot know which blobs in a live namespace are referenced, so it never reclaims an *individual* blob. A consumer groups blobs that share a lifecycle (a tenant, a workspace, a snapshot set) under one namespace and reclaims them together with `delete_namespace` (§5.3).

---

## 2. The blob model

A **blob** is an immutable, finite byte string. Its content names it, and a **namespace** scopes that name:

- A **`BlobId`** is the 32-byte BLAKE3 digest of the blob's bytes. It is `Copy + Eq + Hash + Ord + Serialize + DeserializeOwned`, rendered in lowercase hex for display. BLAKE3 because every `get` re-hashes its bytes (§4, **B1**), putting hash throughput on the read path, and because the digest is a Merkle root, which range-verified streaming (§10) would build on.
- A **`Namespace`** is an opaque, consumer-chosen identifier (a short byte string; a UUID in the filesystem entity), `Clone + Eq + Hash + Ord + Serialize + DeserializeOwned`. To the store it means only *the unit of deletion*: every blob is stored under exactly one namespace, and `delete_namespace` removes them all (§5.3). A `Namespace` is **single-use**: a consumer MUST NOT reuse an id after deleting it, so a tombstone can never be confused with a later recreation.
- A blob's full address is **`(Namespace, BlobId)`**. The `BlobId` depends only on the bytes; the namespace selects *which copy* a `get` reads and *which owners* hold it (§5.2).
- Blobs are **write-once within a namespace**: `(ns, id)` resolves to the same bytes until `ns` is deleted, then stops resolving. The only removal is whole-namespace (§5.3).
- **Dedup is intrinsic within a namespace**: equal content yields one stored copy per namespace. Cross-namespace dedup is given up so that a delete is unambiguous: nothing outside a namespace can reference its bytes. Two lifecycles that should share content belong in one namespace.
- **The verifiable unit is the whole blob.** A reader hashes the bytes it received and compares them to the requested `BlobId` (§4, **B1**); the namespace is not hashed into the id. v1 ignores the BLAKE3 tree structure, so a consumer that wants independently verifiable pieces splits its data into bounded blobs (the filesystem entity chunks files into fixed-size blocks); range-verified streaming is deferred (§10).

Because the unit is the whole blob, an implementation SHOULD bound a blob's size and a consumer SHOULD chunk beyond that bound.

---

## 3. The `BlobStore` seam

The store is a trait over opaque blob bytes, a simulation and deployment seam like `EntityJournal`, `Transport`, and `Clock` (entity §7.3, actor §4.6):

```rust
pub trait BlobStore: Clone + Send + Sync + 'static {
    /// Store `bytes` under `ns` and return its content id. Idempotent and dedup'd
    /// within the namespace (B2): content already present in `ns` re-acknowledges
    /// and writes nothing new. A deleted namespace refuses with `Deleted` (§2).
    /// Returns `Unavailable` if the durability target (§5.2) was not met; the blob
    /// MAY be partially stored, and a retry is safe because the id is a pure
    /// function of the bytes.
    fn put(&self, ns: &Namespace, bytes: Vec<u8>)
        -> impl Future<Output = Result<BlobId, BlobError>> + Send;

    /// Fetch `(ns, id)`, or a byte range of it (`None` = the whole blob). The bytes
    /// are verified against `id` before return (§4, B1): an absent or corrupt blob
    /// is an error, never wrong bytes. A node that knows `ns` is deleted returns
    /// `Deleted`; a node not yet aware of the tombstone may still serve the real
    /// bytes (B7 liveness, §5.3). A range is served by verifying the whole blob,
    /// then slicing (§2).
    fn get(&self, ns: &Namespace, id: &BlobId, range: Option<Range<u64>>)
        -> impl Future<Output = Result<Vec<u8>, BlobError>> + Send;

    /// Whether `(ns, id)` is durably present: at least W copies on the `Clustered`
    /// tier (§5.2), one durable copy on `Local`. A namespace known to be deleted
    /// reports `false`.
    fn has(&self, ns: &Namespace, id: &BlobId)
        -> impl Future<Output = Result<bool, BlobError>> + Send;

    /// Make every blob stored under `ns` permanently unresolvable (§5.3).
    /// Idempotent and monotonic: re-deleting is a no-op. On `Clustered` it returns
    /// once every serving member has acked, failed, or timed out, and succeeds iff
    /// W of the namespace's R tombstone owners acked (§5.3), after which no
    /// surviving or rejoining copy can resurrect a blob of `ns`.
    fn delete_namespace(&self, ns: &Namespace)
        -> impl Future<Output = Result<(), BlobError>> + Send;
}
```

`impl Future` returns are not object-safe, so the seam has a boxed mirror, `DynBlobStore`, held as `Arc<dyn DynBlobStore>` to select the tier at construction (as `DynEntityJournal` is, entity §7.3); the `Clone` bound lets the mirror clone the store into each `'static` boxed future. The trait moves raw bytes, so no serialization format leaks across the seam.

`put` takes ownership of `bytes` to avoid a copy. The id it returns equals `BLAKE3(bytes)` on every tier and in every namespace, so a consumer MAY migrate tiers, or copy a blob between namespaces during compaction, without re-addressing its data.

---

## 4. Verification and the absence of consensus

Content addressing gives the data path three properties:

1. **Read verification (B1).** Every `get` MUST hash the bytes it is about to return and compare to the requested `BlobId`. On mismatch it MUST NOT return those bytes: on the clustered tier it tries the next owner (§5.2); if no source yields verifying bytes it returns `Corrupt` or `Unavailable` as §5.2 decides. This is `wal`'s torn-tail rejection (wal §3.1) with a cryptographic digest in place of a checksum, because the bytes may have crossed the network.

2. **No coordination on the data path (B4).** The store runs no election, no term, no agreement round, and no read-repair to write: a `put` is a fan-out of immutable bytes, a `get` a verified read. Concurrent `put`s of the same content under one namespace converge on the same `BlobId`, bytes, and owners; different content has different ids. The entity journal's single-writer fence (entity §8) keeps one mutable slot from forking, and a blob has no mutable slot.

3. **Deletion is monotonic, not ordered (B7).** Because namespaces are single-use (§2), `delete_namespace` only moves a namespace from *live* to *gone*, so a delete commutes with itself and needs no ordering against the `put`s it supersedes: a later `put` is rejected (§3), and bytes the delete missed are swept by the tombstone (§5.3). The one hazard, a partitioned owner re-replicating a deleted namespace's blob (resurrection), is closed without agreement by a tombstone that is **never reclaimed** (§5.3, §7), so retention needs no membership reasoning, grace timer, or consensus round.

---

## 5. Tiers and deletion

The durability mechanism is the **tier**, chosen at deployment as a membership mode is (actor §9.4). Both tiers satisfy §3 and §4 identically and differ in *where the bytes live*, plus one bound: `Clustered` refuses a blob larger than `max_blob_bytes` (§5.2), while `Local` enforces none. Deletion (§5.3) is one contract across both.

### 5.1 `Local` (single-node)

One on-disk content-addressed store: the embedded, test, and simulator tier. A blob is written to `blobs/<ns>/<hh>/<hash>` by `wal`'s atomic replace (wal §5), so a reader sees the whole blob or none, and a `put` is acknowledged on its directory fsync. `<hh>`, the hash's first hex byte, fans each namespace so no directory grows unbounded. Because the path *is* the address, a `put` of a present blob is a no-op (B2). `get` verifies what it reads (§4); bit-rot is `Corrupt`. `delete_namespace` records a tombstone for `<ns>` (refusing later `put`s, §3) and removes the `blobs/<ns>` subtree, fsyncing the parent; a removal cut short by a crash is harmless and re-driven, because the tombstone, not the files, makes the namespace gone. With no replication, `Local` cannot survive losing its disk.

### 5.2 `Clustered` (replicate-by-hash)

The fault-tolerant tier. Each blob is replicated to **R** owner nodes, and a `put` is durable once **W ≤ R** of them have stored it. There is no leader and no quorum-intersection requirement: with immutable content, W and R are independent durability and availability knobs, not a correctness constraint (§4; contrast entity §8's write-quorum ∩ read-quorum ≠ ∅).

- **Owner selection.** The R owners of a blob are `placement::top(serving_members, key(ns, id), R)`: the R highest-ranked members of `Membership::serving_members()` (utilities §2.1) under the cluster-utilities rendezvous function (utilities §2.2, not entity's, utilities §7), keyed on namespace and content hash together. Hashing **`(ns, id)`** rather than `id` alone spreads a namespace's blobs across the cluster instead of concentrating it on R nodes, at the cost of making `delete_namespace` a cluster-wide fan-out (§5.3), acceptable for infrequent reclamation. Every node computes the same owners for a given `(ns, id)` and view (**B5**), so writer and reader agree with no directory lookup.
- **Write.** `put` hashes the bytes and sends each owner a `StoreBlob { ns, id, bytes }` through its replica actor (§6); a local owner is reached in process, skipping serialization but not the per-owner payload clone. A blob larger than `max_blob_bytes` (Appendix A) is refused before any owner is contacted, as `Unavailable` naming the bound (no dedicated variant; a retry fails the same way). `put` returns `Ok(id)` once **W** stores acknowledge and drains the rest in the background (as entity §7.2 drains slow replicas). If fewer than W acknowledge before a timeout, it returns `Unavailable`; the blob MAY be partially stored, and a retry is safe (§3, B2). An owner holding a tombstone for `ns` refuses the store, surfacing `Deleted`.
- **Read.** If the local node knows `ns` is tombstoned (§5.3), `get` returns `Deleted` without asking anyone. Otherwise it asks the owners **in rank order** and returns the first response that verifies (§4). It MAY widen past the top R, since during a membership transition a blob may still sit on an owner under the previous view (utilities §2.3). A queried owner holding the tombstone answers `FetchBlob` with `None`, as for a blob it never held, so on a node unaware of the tombstone a `get` of a deleted namespace ends in `Unavailable`, or in the real bytes from another unaware node (B7 liveness). If no reachable node yields verifying bytes, `get` returns `Corrupt` when some owner answered with non-verifying bytes, else `Unavailable`.
- **Durability.** A blob survives the loss of any **R − W** of its owners; losing more MAY lose it irrecoverably. The reconcile loop (§7) restores the margin after a node departs, before further loss compounds.

### 5.3 Namespace deletion

`delete_namespace(ns)` reclaims a whole namespace with a **tombstone**, not reference tracking:

- **Tombstone.** A tiny record, `(ns, deleted_at)`: no blob of `ns` may exist or be (re-)created. A namespace's blobs are scattered across the cluster (§5.2), so **every serving node**, not only a blob's owners, must know the tombstone to refuse a `StoreBlob` or `get` for `ns`. It has two homes:
  - a **durable anchor**: the namespace's *tombstone owners*, `placement::top(serving_members, key_ns(ns), R)`, keyed on the namespace alone. `delete_namespace` fans the tombstone out to every serving member and succeeds iff **W** of the R anchors durably recorded it (all of them, when fewer than W are serving), so it cannot be lost. Every recipient records it durably before acking; the anchors are the floor the call guarantees, not the only durable copies.
  - **cluster-wide awareness**: the set (one entry per deleted namespace) reaches every serving node by the fan-out, then by anti-entropy: each reconcile pass (§7) pulls every serving peer's set (`SyncTombstones`, §6) and records each missing entry as a `DeleteNamespace` would, durably and with a sweep. A node that was down or partitioned thus learns missed tombstones on its first pass after rejoin, from any peer that answers. Nothing waits on that pull: the node accepts `StoreBlob`s and reconciles from the moment it serves, which the resurrection guard makes safe.
- **Sweep.** Each recipient drops its local blobs under `ns` after recording the tombstone and before acking (on `Local`, the `blobs/<ns>` subtree, §5.1). The sweep is on the delete's latency path and not rate-limited: the coordinator sends every member's `DeleteNamespace` concurrently, each bounded by `DELETE_TIMEOUT` (2 s), and returns once all have acked, failed, or timed out, so a delete costs the slowest reachable sweep, capped at that timeout; the result is decided at W anchor acks but reported after the drain. An unreached node need not have swept: once a node knows the tombstone, the blobs are unresolvable there (§5.2) and un-re-creatable, whether or not the bytes are gone.
- **Resurrection guard.** A node partitioned during the delete still holds blobs of `ns`, and on rejoin its reconcile loop (§7) would re-push them to owners that swept them. Three things stop this. The tombstone is **never reclaimed**, neither the awareness entry nor the on-disk record (§10), so it outlives every node that could carry a stale copy. The rejoining node's first pass pushes before it pulls tombstones (§7), so it MAY re-offer a stale copy, but reconcile skips and the receiving `StoreBlob` **rejects** a tombstoned namespace's blob, answering `StoreAck::Deleted`: a stale copy can be pushed, never accepted. And the same pass's pull teaches the holder the tombstone, so it sweeps its own copy.
- **No consensus.** The tombstone is monotonic (set-once), so its dissemination needs no ordering and no term (**B7**).

Reclaiming *individual* blobs while a namespace lives on (a deleted file in a still-mounted workspace) is consumer-driven **compaction**, outside the store (§10). The store only ever deletes whole namespaces.

---

## 6. The clustered replica actor

The `Clustered` tier reuses the actor transport with no new wire protocol (actor §7.1), as the entity Quorum replicator does (entity §7.2), mirroring `replica_store.rs` minus fencing and ordering:

- A per-node **`BlobReplica`** actor owns this node's local on-disk store (§5.1 mechanics) and accepts five messages: `StoreBlob { ns, id, bytes } -> StoreAck`, `FetchBlob { ns, id, range } -> Option<Vec<u8>>`, `HasBlob { ns, id } -> bool`, `DeleteNamespace { ns, deleted_at } -> DeleteAck`, and `SyncTombstones -> Vec<Tombstone>` (the replica's whole awareness set, for the anti-entropy pull of §5.3). It is registered in the receptionist (actor §13) under a well-known key. Unlike entity's `StoreRecord`, `StoreBlob` carries **no shard, no `after`, no term, and no `repair` flag**: nothing needs fencing or ordering.
- The tier resolves a peer's `BlobReplica` through the receptionist and `ask`s it, one request per message, and runs its detached tasks (the reconcile loop, a `put`'s straggler drain) on the host's spawner. It has no transport of its own, so the simulator substitutes the framework's seams (§8).

`StoreAck` is `Stored` or `Deleted` (namespace tombstoned, §5.3), plus transport errors surfaced to the caller; with no term and no mutable head there is no `Fenced` or `Stale` variant. A replica whose local store fails a `StoreBlob` other than with `Deleted` has lost its durable storage and **panics**, the designed fallback for a broken disk: the panic stops the actor and prunes its receptionist registration, so the `put` counts the remaining owners and reconcile (§7) restores the copy count (**B6**). `DeleteNamespace` (fanned out, recorded, swept, acked: §5.3) is idempotent and monotonic, so a redelivered or reconcile-driven `DeleteNamespace` is harmless.

---

## 7. Placement and rebalancing

Owner selection is a pure function of the membership view (§5.2), so as nodes join and leave, the *intended* placement of every blob changes minimally (utilities §2.2, invariant U1), but no bytes move. Restoring the durability target takes an active **reconcile loop**, modeled on entity's `shardmap.rs` reconcile (entity §7.6) but copying blob bytes rather than reconfiguring a Raft group.

The loop is **push-based and decentralized**, with no coordinator. Every 250 ms (`RECONCILE_INTERVAL`), with no membership trigger, each node computes `top(serving_members, key(ns, id), R)` for each local blob and `StoreBlob`s it to any current owner that lacks it (`HasBlob` gates the copy); a membership change is acted on at the next tick. A tombstoned namespace's blob is **never copied**: reconcile skips it and the receiver would reject it (§5.3). The same pass then pulls every serving peer's tombstone set (§5.3); the push runs first so a slow or dead peer never starves re-replication behind the pull.

Rebalancing **only restores copies; it never drops them**. Bytes are removed only by `delete_namespace` (§5.3), since reconcile cannot know whether a misplaced copy is still wanted:

- **Node removed → mandatory re-replication.** Nothing read-repairs a cold under-replicated blob, so the surviving holders re-push it to the new R-th owner, restoring the R − W margin (**B6**). This is the correctness-critical direction.
- **Node added → optional migration.** The prior owners still hold W..R copies, so moving blobs onto the new node is load balancing, not durability, and MAY be lazy or low-priority.
- **No drop on rebalance.** A node no longer an owner of a blob keeps its copy until the namespace is deleted (§5.3); reconcile tolerates over-replication rather than risk under-replication.

The loop reads membership through the cluster's seam and runs on the framework's `Spawner`/`Clock`, so it is seed-reproducible (§8). A pass walks local blobs in sorted order and copies sequentially, one blob and one owner per await, with no priority for under-replicated blobs and no rate limit beyond the 250 ms period.

---

## 8. Testability and deterministic simulation

Both tiers run under the actor framework's deterministic simulation (actor §18, entity §14): a cluster of blob stores in one process, on one logical thread, over virtual time, network, and randomness, so one `(seed, configuration)` reproduces a run exactly. Because the store sits on the framework's own seams, simulation drives the real placement, replication, and reconcile code, not a model of it.

Fault injection MUST be able to produce:

- **node loss mid-`put`**: fewer than W acknowledge, surfacing `Unavailable`, and a later retry succeeds;
- **under-replication repair**: a node holding owner copies leaves, and the reconcile loop (§7) restores R copies on the surviving owners (**B6**);
- **`put`/`get` under partition**: a minority-side reader widens past unreachable owners (§5.2) or fails cleanly;
- **duplicate `put`**: the same content stored concurrently from two nodes (same namespace) converges to one blob (**B2, B4**);
- **a corrupted stored blob**: a tampered on-disk or in-flight blob is detected on read and never returned as valid (**B1**), and on the clustered tier the reader falls through to a good owner;
- **delete during partition (resurrection)**: a namespace is deleted while one owner is partitioned; on rejoin the reconcile loop MUST NOT resurrect any blob of that namespace, however long the partition lasted, because the never-reclaimed tombstone rejects the copy and then sweeps it (**B7**, §5.3). Two injectable cases: a partition outlasting any fixed window, and a merely `unreachable` node returning with its disk, whose tombstone MUST NOT have been forgotten;
- **`put` racing `delete_namespace`**: a `put` into a namespace being deleted either succeeds before the tombstone (and is swept) or is refused with `Deleted`, but never leaves a resolvable blob in a deleted namespace (**B7**).

Beyond these: codec round-trips for the replica messages (including `DeleteNamespace`), duplicate-tolerance tests (B2), node-crash cascade tests (B6), delete-monotonicity and no-resurrection tests (B7), and seed-reproducibility of the event stream.

---

## 9. Invariants

Invariants hold under the faults of §8 and are verified as the framework prescribes (actor §18.5/§18.6): continuous checkers over the event stream for safety properties, targeted simulation tests for the rest.

The catalogue is machine-readable as `b_catalogue()` beside this crate's conformance suite, guarded by the drift test in `conformance_catalogue.rs`, which also checks **B4** from the crate's manifest: an `entity` dependency, and with it a consensus engine to run, fails the test rather than quietly invalidating the claim.

| # | Invariant | Defined in | Verified by |
|---|---|---|---|
| **B1** | **Address integrity.** `get(ns, id)` returns bytes whose BLAKE3 hash equals `id`, or an error; it never returns wrong or corrupt bytes. Verification is on the read path, after any network transfer. | §2, §4 | `a_corrupt_blob_is_detected_and_never_returned`, `a_tampered_copy_falls_through_to_a_good_owner` |
| **B2** | **Idempotent, dedup'd put.** Equal content under the same namespace yields one stored copy; a `put` of already-present content writes nothing new and re-acknowledges. | §2, §5 | `putting_the_same_bytes_twice_stores_once`, `equal_content_converges_to_one_id_across_nodes` |
| **B3** | **Durability target.** A `put` is acknowledged only once at least W copies are stored (one on `Local`); the blob then survives the loss of any R − W owners. | §5.2 | `a_blob_put_on_one_node_is_gettable_from_every_node`, `a_put_below_write_quorum_is_refused` |
| **B4** | **No consensus on the data path.** The store runs no election, term, agreement round, or write-time read-repair; concurrent writers of the same content do not coordinate and do not fork. | §4 | structural: the crate depends on `actor-cluster`, not `entity`, so no consensus group exists to run; plus the B2 convergence tests |
| **B5** | **Deterministic placement.** A blob's owners are a pure, version-stable function of the serving set and the `(namespace, content hash)` key; every node agrees on them for a given view, and a single membership change reassigns only the blobs whose owners changed. | §5.2 | the placement function's known-answer vectors are pinned in `actor-cluster` (utilities U1); `one_membership_change_moves_minimal_blobs` asserts minimal movement over the `(namespace, content hash)` key |
| **B6** | **Repair restores the target.** After a node leaves, the reconcile loop restores ≥ R copies of every live blob the cluster still holds anywhere; rebalancing is additive and never drops the last or only verifying copy of a non-deleted blob. | §7 | `a_departed_owner_is_re_replicated`, `rebalancing_never_deletes`, `acknowledged_blobs_are_re_replicated_after_a_heal` (under the fault swarm) |
| **B7** | **Monotonic deletion, no resurrection.** `delete_namespace` is set-once and commutes with itself. *Safety:* no node aware of the tombstone resolves a blob of the namespace, no `put` into a deleted namespace ever leaves a resolvable blob, and reconcile never resurrects one, even across a partition of unbounded duration, because the tombstone is never reclaimed (§10) and so outlives every member that could carry a stale copy. *Liveness:* the tombstone reaches every serving node within the propagation bound, after which the namespace resolves nowhere. | §4, §5.3 | `a_deleted_namespace_stays_deleted`, `a_deleted_namespace_resolves_nowhere_and_refuses_puts`, `a_rejoining_unaware_holder_does_not_resurrect_a_deleted_namespace` |

A `blob-store` implementation conforms iff every invariant holds, verified under deterministic simulation (§8) for both tiers.

---

## 10. Non-goals and future work

Non-goals:

- **No mutation.** Blobs are immutable (§2). Names, directories, chunking, and assembly are the consumer's (§1).
- **No fine-grained liveness.** The store knows liveness only at namespace granularity (§1, §5.3); per-blob reclamation is consumer-driven compaction (below).

Future work:

- **Tombstone-set GC.** Every node that learned a tombstone keeps its awareness entry and on-disk `tombstones/<ns>` record forever: one tiny record per deleted namespace. A reclaim would have to honour a membership fact, not a clock: forget a tombstone only once every member serving at fan-out time has swept or reached the terminal `down`/`removed` state (actor §9.1). `down` is **irrevocable and absorbing**, and a downed node rejoins only under a fresh `NodeId`, so it cannot return an un-swept blob under its old identity; a merely `unreachable` node (actor §10) may return with its disk and so keeps the tombstone alive. Retention would thus be bounded by the downing decision (an operator decommission or the mode's downing policy, actor §9.4), not a grace timer. A safe GC needs every anchor to track that rule and the awareness set to shrink in step with the durable copies; until then B7 holds by retention.
- **Compaction helper.** A reusable routine for reclaiming individual blobs within a live namespace: copy the still-referenced blobs from `ns` into a fresh `ns'`, redirect the consumer's references, then `delete_namespace(ns)`.
- **Reference-aware per-blob delete.** A `delete(ns, id)` with refcount or consumer-declared roots, for a consumer that must drop a single blob without compacting. Not in v1; compaction covers the motivating cases.
- **Cross-namespace dedup.** An optional shared namespace (never deleted, or compacted on its own schedule) into which consumers place content two lifecycles share, recovering the dedup §2 gives up at the cost of that namespace needing its own reclamation story.
- **Range-verified streaming.** Verifying a byte range of one large blob against the BLAKE3 tree the `BlobId` already roots (the Bao encoding), so a large blob need not be fetched whole to be verified (§2).
- **External object-store tier.** An S3-compatible backend behind the same seam, with the blob a single content-keyed object, `put` a conditional upload, and `get` a verified fetch, for cloud and cold or archival storage. The deterministic simulator (§8) cannot drive it, so it would have only integration tests against an emulated endpoint.
- **Tiering.** A wrapper composing `Clustered` (hot) over the external object-store tier (cold) with fall-through reads and background demotion, the hot/cold split Cloudflare draws between a Durable Object's local SQLite and its object-store archive (DO §4.2).
- **Locality caching.** A read-through local cache of fetched blobs on a non-owner node; immutable blobs never invalidate, so only the first read is remote.
- **Encryption at rest.** Per-blob encryption beneath the content hash (the hash over ciphertext or a convergent scheme), for untrusted backends.

---

## Appendix A: End-to-end example

```rust
// --- Clustered tier on an existing cluster system (actor §9.4.3), over this
// --- node's own on-disk store (one per node, §5.1)
let clustered = blob_store::clustered(system.clone(), BlobConfig {
    replication_factor: 3,      // R: owners per blob (§5.2)
    write_quorum: 2,            // W ≤ R: acks before a put returns durable (§5.2)
    max_blob_bytes: 4 << 20,    // bound a blob; consumers chunk beyond it (§2)
}, blob_store::local("/var/lib/app/blobs")?);
// Hold it behind the object-safe mirror to pick the tier at construction (§3).
let blobs: Arc<dyn DynBlobStore> = Arc::new(clustered);

// --- A namespace groups blobs with a shared lifecycle (§2): here, one workspace ---
let ws = Namespace::new(workspace_uuid.as_bytes().to_vec());   // single-use id; never reused after deletion

// --- Store some bytes under the namespace; the id is their BLAKE3 hash (§2) ---
let id = blobs.put(&ws, block.to_vec()).await?;   // Ok(BlobId) once W copies are durable

// --- Fetch and verify (§4); identical call site on any tier ---
match blobs.get(&ws, &id, None).await {
    Ok(bytes)                      => assert_eq!(BlobId::of(&bytes), id), // B1 holds by construction
    Err(BlobError::Unavailable(_)) => { /* fewer than the needed copies reachable; retry/failover */ }
    Err(BlobError::Corrupt(_))     => { /* every reachable copy failed verification: data loss */ }
    Err(BlobError::Deleted(_))     => { /* the namespace has been deleted (§5.3) */ }
}

// --- Membership: presence is a durable-copy question (§5.2) ---
assert!(blobs.has(&ws, &id).await?);

// --- Reclaim the whole workspace in one call; no per-blob bookkeeping (§5.3) ---
blobs.delete_namespace(&ws).await?;               // (ws, *) becomes permanently unresolvable
assert!(!blobs.has(&ws, &id).await?);

// --- Single-node / test deployments use the same seam ---
let local: Arc<dyn DynBlobStore> = Arc::new(blob_store::local("/var/lib/app/blobs")?);   // §5.1
```

```rust
pub enum BlobError {
    Unavailable(String),   // could not reach W copies on put, or any owner on get (§5.2)
    Corrupt(BlobId),       // a copy was found but none verified against the id (§4)
    Deleted(Namespace),    // the target namespace has been deleted (§5.3)
}
```

## Appendix B: Suggested crate layout

```
blob-store/                # namespaced, content-addressed object store on actor-core + actor-cluster
  blob.rs                  # BlobId (BLAKE3), Namespace, the BlobStore seam, BlobError, BlobConfig (§2, §3)
  local.rs                 # Local tier: on-disk CAS over wal::atomic_replace; namespace subtree + tombstone (§5.1, §5.3)
  cluster.rs               # Clustered tier: owner selection, W-of-R put, verified rank-order read, delete (§5.2, §5.3)
  replica.rs               # per-node BlobReplica actor (StoreBlob/FetchBlob/HasBlob/DeleteNamespace/SyncTombstones) (§6)
  placement.rs             # thin reuse of actor-cluster placement::top, keyed on (namespace, id) (§5.2)
  tombstone.rs             # namespace tombstone record and the awareness set (§5.3)
  reconcile.rs             # additive, push-based rebalancing that respects tombstones, then the tombstone anti-entropy pull (§7)
  system.rs                # BlobSystem: the serving set and event emission the tier needs from its host (§5.2, §8)
  event.rs                 # BlobEvent, the store's entries on the framework's event stream (§8)
```

`blob-store` depends on **actor-core** (the model and the `Clock`/`Entropy`/`Spawner` seams), **actor-cluster** (the `Transport`, `Membership::serving_members`, `placement`, and the receptionist), **wal** (the `Local` tier's atomic replace, and `wal::Sidecar`, which encodes the tombstone body, wal §5), **compat** (the stamp on the tombstone record, compatibility §3), **blake3** (the content hash), and **serde**, **serde_bytes**, and **futures** (the message derives, the byte-tagged payloads, and the fan-out's unordered joins). The replica messages ride whichever codec the host's transport carries, so the crate needs no serialization crate of the tree's own. It depends on **neither entity nor the runtime or simulation crates**: the host system injects the time, randomness, spawning, and transport seams, as for `ClusterSystem<C, E, S, T>` (actor §18), so the same store code runs in production and in the deterministic simulator. The durable filesystem entity (`research/durable-sqlite-and-filesystem.md` §4) depends on `blob-store`, never the reverse.
