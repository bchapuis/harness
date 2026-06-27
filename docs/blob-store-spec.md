# Blob Store: a namespaced, content-addressed object store on the actor framework

**Status:** Draft v1
**Scope:** A pluggable, content-addressed store for **immutable** binary blobs, addressed by the BLAKE3 hash of their content **within a consumer-chosen namespace**. Two deployment tiers sit behind one seam: a single-node on-disk store and a clustered replicate-by-hash store. This document owns the *content-addressed durability contract*: how a blob is named, what `put`/`get`/`has`/`delete_namespace` guarantee, and how the clustered tier keeps a blob durable as the cluster changes shape. It does **not** own what blobs *mean*: chunking, naming, and directory structure belong to a consumer above it. Liveness belongs to the consumer too, but the store gives it one lever. The namespace is the **unit of deletion** (§2, §5.3), so a consumer reclaims storage by deleting a namespace, not by teaching the store which blobs are referenced.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Throughout, `blob-store` is the crate and namespace name. Sections of this document are cited as plain **§N**, and invariants defined here are numbered **B1, B2, …**. Cross-references: `granary §N` → [`granary-spec.md`](granary-spec.md); `actor §N` → [`distributed-actor-spec.md`](distributed-actor-spec.md); `utilities §N` → [`cluster-utilities-spec.md`](cluster-utilities-spec.md); `wal §N` → [`wal-spec.md`](wal-spec.md); `DO §N` → [`../research/durable-objects.md`](../research/durable-objects.md).

> **Design stance.** Immutable, content-addressed blobs need durability and deletion **without consensus**. A content hash names exactly one byte sequence, for all time, so there is nothing to *order* and nothing to *agree on*: two writers that store the same bytes cannot conflict, and a reader proves it received the right bytes by re-hashing them. This removes the three hard pieces of the grain journal (the per-record order `Seq`, the single-writer term fence, and quorum-intersection recovery, granary §8) and leaves only "store the bytes on enough machines, read them back from any one, and verify." The blob store is the grain Quorum replicator (granary §7.2) with its hard half removed. It takes the Durable Objects split of *ordering apart from durability* (DO §4.3) to the limit: for immutable content there is no ordering step at all. Deletion folds in along the same grain. Scoping a blob's identity to a **namespace** makes the namespace the unit of liveness, so reclamation is a *monotonic* "this namespace is gone" tombstone, not a reference-tracking garbage collector: no root set, no mark-and-sweep, no coordination beyond a membership-bounded guard against resurrection (§5.3). Everything below follows from these two ideas, and the design's job is to avoid reintroducing the coordination that content addressing made unnecessary.

---

## 1. Scope and layering

The crate sits **beside** granary, not above it. A blob store is built from plain cluster actors (actor §3), not grains, because it needs none of the grain machinery: no virtual identity, no journal, no single-activation lease, no gateway. It depends on the actor framework's transport, membership, and placement (`actor-cluster`), on the local durable-file primitive (`wal`), and on nothing in granary. Consumers layer **above** it.

It provides:

- **`BlobStore`.** The seam (§3): `put`, `get`, `has`, and `delete_namespace` over opaque bytes addressed by content within a namespace (§2).
- **Two tiers.** `Local` and `Clustered`, each an implementation of the seam (§5).

It is **not** a filesystem, a database, or an index. It imposes no structure on a blob's bytes, assigns no names of its own beyond the content hash, scopes blobs only by the consumer-supplied namespace, and tracks no relationships between blobs. Two concerns belong by design to the **consumer**, not the store:

- **Chunking and metadata.** How a large object is split into blobs, how blobs are named or assembled, and what directory or inode structure references them is the consumer's concern (the durable filesystem grain is the motivating one, `research/durable-sqlite-and-filesystem.md` §4). The store sees only individual blobs.
- **Fine-grained liveness.** Within a live namespace the store cannot know which blobs are referenced, so it never reclaims an *individual* blob on its own. It exposes the **namespace** as the unit of liveness instead: a consumer groups blobs that share a lifecycle (a tenant, a workspace, a snapshot-set) under one namespace and reclaims them together with `delete_namespace` (§5.3). Reclaiming individual blobs *within* a still-live namespace, such as a file deleted while its workspace lives on, is consumer-driven **compaction** (copy the live blobs into a fresh namespace, delete the old one, §10), not a store-side reference collector. The store thus needs no root set and no garbage collection.

The store is the byte-and-durability contract for content-addressed blobs, exactly as `wal` is the byte-and-durability contract for a local log (wal §1); it takes no position on what the bytes mean, only on when a whole namespace of them is discarded.

---

## 2. The blob model

A **blob** is an immutable, finite byte string. Its content names it, and a **namespace** scopes that name:

- A **`BlobId`** is the 32-byte BLAKE3 digest of the blob's bytes. It is `Copy + Eq + Hash + Ord + Serialize + DeserializeOwned`, rendered in lowercase hex for display. BLAKE3 is chosen over SHA-256 for two reasons that bear on this design. First, it hashes several times faster (SIMD, optionally multithreaded), which matters because **every** `get` re-hashes its bytes to verify them (§4, **B1**), so hashing throughput sits on the read path, not only the write path. Second, it is internally a Merkle tree whose root *is* the id, so the range-verified streaming deferred to §10 exposes structure the id already commits to, instead of layering a second hash over a flat digest. The trade, losing SHA-256's FIPS status and wide tooling, costs nothing here, because the store is a self-contained CAS that takes no position on what the bytes mean.
- A **`Namespace`** is an opaque, consumer-chosen identifier (a short byte string, a UUID in the motivating filesystem grain). It is `Clone + Eq + Hash + Ord + Serialize + DeserializeOwned`. It carries no meaning to the store beyond *the unit of deletion*: every blob is stored under exactly one namespace, and `delete_namespace` removes all of them (§5.3). A `Namespace` is **single-use**: a consumer MUST NOT reuse an id after deleting it (the motivating consumer mints a fresh UUID), so a delete tombstone (§5.3) can never be confused with a later recreation.
- A blob's full address is the pair **`(Namespace, BlobId)`**. The `BlobId` is a pure function of the bytes and is identical across namespaces; the namespace selects *which copy* a `get` reads and *which owners* hold it (§5.2).
- Blobs are **write-once within a namespace**: `(ns, id)` resolves to the same bytes as long as `ns` exists, then stops resolving once `ns` is deleted. There is no in-place mutation; the only removal is whole-namespace (§5.3).
- **Dedup is intrinsic within a namespace.** Equal content under the same namespace yields one stored copy, so storing the same bytes twice in `ns` stores them once. Across *different* namespaces the same content is stored once per namespace. Giving up cross-namespace dedup is deliberate, and it is what buys the clean per-namespace delete: a blob belongs to one namespace, so deleting that namespace is unambiguous, and nothing else can still reference its bytes. A consumer that wants two lifecycles to share content keeps them in one namespace.
- **The verifiable unit is the whole blob.** A reader proves correctness by hashing the bytes it received and comparing to the requested `BlobId` (§4, **B1**); the namespace is not hashed into the id, so namespacing leaves verification unchanged. BLAKE3 is internally a Merkle tree, so the id in principle commits to every subtree, but v1 ignores that structure and verifies only whole blobs, because the simplest contract ("hash what you got, compare to the id") suffices when blobs are bounded. A consumer that wants cheap independently-verifiable pieces splits its data into several bounded blobs (the common case: the filesystem grain chunks files into fixed-size blocks). Range-verified streaming of a single large blob, verifying a byte range against the id's own tree with no second hash, is a deferred extension (§10).

Because the unit is the whole blob, an implementation SHOULD bound a blob's size and a consumer SHOULD chunk beyond that bound. A blob is expected to be "a block," not "a database."

---

## 3. The `BlobStore` seam

The store is a trait over opaque blob bytes, a simulation and deployment seam like `GrainJournal`, `Transport`, and `Clock` (granary §7.3, actor §4.6):

```rust
pub trait BlobStore: Send + Sync + 'static {
    /// Store `bytes` under `ns` and return its content id. Idempotent and dedup'd
    /// within the namespace (B2): storing content already present in `ns`
    /// re-acknowledges and writes nothing new. Storing into a deleted namespace is
    /// an error (`Deleted`); namespaces are single-use (§2). Returns `Unavailable`
    /// if the durability target (§5.2) could not be met, in which case the blob
    /// MAY or MAY NOT be partially stored, and the caller retries (the id is a pure
    /// function of the bytes, so a retry carries no double-write risk).
    fn put(&self, ns: &Namespace, bytes: Vec<u8>)
        -> impl Future<Output = Result<BlobId, BlobError>> + Send;

    /// Fetch `(ns, id)`, or a byte range of it (`None` = the whole blob). The returned
    /// bytes are verified against `id` before return (§4, B1): an absent or corrupt
    /// blob is an error, never wrong bytes. A node that knows `ns` is deleted returns
    /// `Deleted` (§5.3); a node not yet aware of the tombstone may still serve the
    /// real bytes until it learns of it (B7 liveness). A ranged request is served by
    /// obtaining and verifying the whole blob, then slicing (§2); efficient range
    /// streaming is deferred (§10).
    fn get(&self, ns: &Namespace, id: &BlobId, range: Option<Range<u64>>)
        -> impl Future<Output = Result<Vec<u8>, BlobError>> + Send;

    /// Whether `(ns, id)` is durably present: at least W copies on the `Clustered`
    /// tier (§5.2), one durable copy on `Local`. A namespace known to be deleted
    /// reports `false`.
    fn has(&self, ns: &Namespace, id: &BlobId)
        -> impl Future<Output = Result<bool, BlobError>> + Send;

    /// Reclaim an entire namespace: every blob stored under `ns` becomes
    /// permanently unresolvable (§5.3). Idempotent and monotonic: a namespace, once
    /// deleted, stays deleted, and re-deleting is a no-op. Returns once the tombstone
    /// is durably anchored (W of the namespace's R tombstone owners, §5.3), after
    /// which it cannot be lost and is disseminated to the rest of the cluster; from
    /// that point no surviving or rejoining copy can resurrect a blob of `ns`. The
    /// bytes are swept in the background.
    fn delete_namespace(&self, ns: &Namespace)
        -> impl Future<Output = Result<(), BlobError>> + Send;
}
```

Object-safety (`impl Future` vs. a boxed `DynBlobStore`) is an implementation choice consistent with the actor framework's seams (granary §7.3); a `DynBlobStore` mirrors `DynGrainJournal`. The trait is codec-agnostic: it moves raw bytes, and the `BlobId` is computed from those bytes, so no serialization format leaks across the seam.

`put` takes ownership of `bytes` so the implementation hashes and stores without copying. The id it returns equals `BLAKE3(bytes)` regardless of tier or namespace, so the same bytes are addressable by the same id wherever they are stored (a consumer MAY migrate tiers, or copy a blob between namespaces during compaction, without re-addressing its data).

---

## 4. Verification and the absence of consensus

Three properties define the data path, all consequences of content addressing:

1. **Read verification (B1).** Every `get` MUST hash the bytes it is about to return and compare to the requested `BlobId`. On mismatch it MUST NOT return those bytes: on the clustered tier it tries the next owner (§5.2); if no source yields verifying bytes it returns `Corrupt`. This makes corruption and misdelivery *detectable at the point of use*, not a silent fault. It is the blob store's analogue of `wal`'s torn-tail rejection (wal §3.1), strengthened from a checksum to a cryptographic digest because the bytes may have crossed the network.

2. **No coordination on the data path (B4).** The store runs no election, no term, no agreement round, and no read-repair to write. A `put` is a fan-out of immutable bytes; a `get` is a verified read. Two concurrent `put`s of the same content under the same namespace converge because they produce the same `BlobId` and the same bytes at the same place; two `put`s of *different* content never collide because different content has different ids. The single-writer fence the grain journal needs (granary §8) exists only to keep one mutable slot from forking, and a blob has no mutable slot. So the store needs neither the leader-election group nor the per-record term that the grain journal carries.

3. **Deletion is monotonic, not ordered (B7).** Because namespaces are single-use (§2), `delete_namespace` only moves a namespace from *live* to *gone*, never back. A delete therefore commutes with itself (re-delete is a no-op) and needs no ordering against the `put`s it supersedes: any `put` into a deleted namespace is rejected (§3), and any byte the delete missed is swept by the tombstone (§5.3). A replicated delete introduces one hazard, a partitioned owner re-replicating a blob of a deleted namespace after the delete (resurrection). A *membership-bounded* tombstone closes it without agreement (§5.3, §7): the tombstone is retained until every node that could carry a stale copy has swept it or reached the terminal `down`/`removed` state (actor §9.1), after which that node can rejoin only under a fresh, empty identity. So deletion adds a monotonic flag whose lifetime is gated on membership, not a grace timer and not a consensus round.

What remains on the write path is a single durability question, "is the blob stored on enough machines yet?", answered per §5.2 with no consensus protocol; and on the delete path, a single monotonic flag, answered per §5.3.

---

## 5. Tiers and deletion

The durability mechanism is the **tier**, chosen at deployment as a membership mode is (actor §9.4). The two tiers (§5.1, §5.2) satisfy §3 and §4 identically and differ only in *where the bytes live*. Deletion (§5.3) is cross-tier behavior, the same `delete_namespace` contract over whichever tier holds the bytes, so it is described once here rather than per tier.

### 5.1 `Local` (single-node)

One on-disk content-addressed store: the embedded, test, and simulator tier. A blob is written to `blobs/<ns>/<hh>/<hash>` via the `wal` atomic-replace discipline (wal §5): write a temp file, fsync, rename onto the final path, fsync the directory, so a reader sees either the whole blob or no blob, never a torn one. The namespace `<ns>` is the top directory, and the first hex byte `<hh>` of the content hash fans each namespace so no directory grows unbounded. A `put` is acknowledged on that fsync. Because the path *is* the namespace plus the content hash, a `put` of an already-present blob is a no-op (the file exists), giving B2 for free. `get` reads the file and verifies it (§4); a hash mismatch (bit-rot on disk) is `Corrupt`. `delete_namespace` records a tombstone for `<ns>` (so a later `put` into it is refused, §3) and removes the `blobs/<ns>` subtree, fsyncing the parent; a partial removal interrupted by a crash is harmless and re-driven, because the tombstone, not the presence of files, makes the namespace gone. There is no replication: `Local` is CP trivially (one store, one writer) and cannot survive losing that node's disk. Best for single-node deployments, tests, and the deterministic simulator (§8).

### 5.2 `Clustered` (replicate-by-hash)

The fault-tolerant tier. Each blob is replicated to **R** owner nodes, and a `put` is durable once **W ≤ R** of them have stored it. It has no leader and no quorum-intersection requirement: with immutable content, W and R are independent durability and availability knobs, not a correctness constraint (§4, contrast granary §8's write-quorum ∩ read-quorum ≠ ∅).

- **Owner selection.** The R owners of a blob are `placement::top(serving_members, key(ns, id), R)`: the R highest-ranked nodes under rendezvous hashing (utilities §2, §7), where `key(ns, id)` is the rendezvous key formed from the namespace and the content hash together. This is the same version-stable function granary uses to place shard replicas. The candidate set is `Membership::serving_members()` (utilities §2.1). Hashing **`(ns, id)` together** rather than `id` alone spreads a namespace's blobs evenly across the cluster, so no single namespace concentrates load on R nodes; the cost is that `delete_namespace` becomes a cluster-wide fan-out rather than an R-node operation (§5.3), acceptable for an infrequent background reclamation. Every node computes the identical owner list for a given `(ns, id)` and membership view (**B5**), so a writer and a reader agree on where a blob lives with no directory lookup.
- **Write.** `put` hashes the bytes, computes the R owners of `(ns, id)`, and sends each a `StoreBlob { ns, id, bytes }` over the per-node replica actor (§6). If this node is itself an owner it stores locally, moving the bytes in rather than copying. The `put` returns `Ok(id)` once **W** stores have acknowledged, and drains the remaining sends in the background (off the latency path, as granary §7.2 drains slow replicas). If fewer than W acknowledge before a timeout, it returns `Unavailable`; the blob MAY be partially stored, and a retry is safe and idempotent (§3, B2). An owner that holds a tombstone for `ns` refuses the store, surfacing `Deleted`.
- **Read.** `get` computes the owners of `(ns, id)` and asks them **in rank order**, returning the first response that verifies (§4). If the local node knows `ns` is tombstoned (§5.3), `get` short-circuits to `Deleted` without asking anyone. Otherwise it MAY widen past the top-R to lower-ranked nodes, because during a membership transition a blob may still sit on a node that was an owner under the previous view but is not under the current one (placement is a routing function, not a lease, utilities §2.3); widening lets a read find it instead of failing. If a queried owner reports the namespace tombstoned, `get` returns `Deleted`; if no reachable node yields verifying bytes, `get` returns `Corrupt` when some owner answered with non-verifying bytes, else `Unavailable`.
- **Durability.** A blob survives the loss of any **R − W** of its owners. Losing more than that MAY lose the blob (it is irrecoverable, like any data whose every copy is gone); the reconcile loop (§7) restores the R − W margin after a node departs, before further loss can compound.

### 5.3 Namespace deletion

`delete_namespace(ns)` reclaims a whole namespace without reference tracking. The mechanism is a **tombstone**, not a sweep of known-live roots:

- **Tombstone.** A namespace tombstone is a tiny record, `(ns, deleted_at)`, meaning "no blob of `ns` may exist or be (re-)created." A namespace's blobs are scattered across the whole cluster (owner selection hashes `(ns, id)`, §5.2), so the tombstone must be known to **every serving node**, not only to a blob's owners. Otherwise a node receiving a `StoreBlob` or serving a `get` could not tell that `ns` is gone. A tombstone therefore has two homes:
  - a **durable anchor**: the namespace's *tombstone owners*, `placement::top(serving_members, key_ns(ns), R)` (the R owners of the namespace id alone, a stable home independent of any blob). `delete_namespace` fans the tombstone out cluster-wide and returns once **W** of the R anchor owners have durably recorded it, so it cannot be lost. The anchor owners are also where each node reports its sweep completion (the resurrection guard below), so they hold both the durable tombstone and the record of who has finished acting on it.
  - **cluster-wide awareness**: the tombstone set is small (one entry per deleted namespace) and reaches every serving node by the same fan-out and thereafter by gossip. A node that was down or partitioned re-syncs the set from the anchor owners on rejoin, before it resumes accepting `StoreBlob`s or reconciling `ns`.
- **Sweep.** The fan-out also triggers a background, cluster-wide pass: every node drops its local blobs under `ns` (on `Local`, `rm` the `blobs/<ns>` subtree, §5.1). The sweep is rate-limited and off any latency path. A node need not finish sweeping for the delete to be correct: once a node knows the tombstone, the blobs are already unresolvable (`get` short-circuits, §5.2) and un-re-creatable, whether or not the bytes are gone yet.
- **Resurrection guard.** The hazard is a node partitioned during the delete: it still holds blobs of `ns`, and on rejoin the reconcile loop (§7) would re-push them to owners that had swept them. The tombstone closes this. A rejoining node re-syncs the tombstone set *before* reconciling, and both reconcile and the receiving `StoreBlob` **reject** a blob whose namespace is tombstoned, so a stale copy can neither be pushed nor accepted; its holder sweeps it once it learns the tombstone. The tombstone must therefore outlive every node that could still carry a stale copy, and *which* nodes those are is a membership fact, not a clock reading. Each node acks its sweep to the anchor owners. The tombstone is retained until every node that was a serving member when it was anchored has either acked its sweep or reached the terminal `down`/`removed` membership state (actor §9.1). That terminal state is the load-bearing one: `down` is **irrevocable and absorbing**, and a downed node may rejoin only by wiping its identity and joining under a fresh `NodeId` (actor §9.1), so it can never return carrying an un-swept blob under its old identity. A merely `unreachable` node (actor §10) is **not** enough: that state is reversible, the node may return with its disk intact, so its tombstone is held until it returns and sweeps or is downed. Once every member-at-anchor is acked-or-`down`, no node can still hold an un-swept blob of `ns`, and the tombstone is reclaimed.

  The blob store thus owns **no grace timer of its own**: it inherits its safety boundary from the membership lattice rather than re-deriving a grace period. What bounds a tombstone's life is the same downing decision that reclaims a dead node's capacity, an operator decommission or the mode's downing policy (actor §9.4), so tombstone-GC liveness is exactly membership-downing liveness. A node left `unreachable` forever keeps its tombstones alive, but that is one more reason to down it, not a license to forget behind its back: forgetting a tombstone for a still-reversible node is the one move that could resurrect a blob, and gating on terminal `down` forecloses it. The safety path carries **no timing assumption**.
- **No consensus.** The tombstone is monotonic (set-once), so its dissemination needs no ordering and no term. It is the §4 and **B7** thesis applied to deletion: a flag fanned out and gossiped, anchored on W owners, retained until membership says every holder has swept or reached terminal `down` (actor §9.1). Not an agreement round.

A consumer that needs to reclaim *individual* blobs while a namespace lives on (a deleted file in a still-mounted workspace) does so by **compaction**, outside the store: copy the still-live blobs into a fresh namespace and `delete_namespace` the old one (§10). The store never learns which individual blobs are live; it only ever deletes whole namespaces.

---

## 6. The clustered replica actor

The `Clustered` tier reuses the actor framework's transport, with no new wire protocol (actor §2.2), exactly as the grain Quorum replicator does (granary §7.2). The pieces mirror `replica_store.rs`, minus everything fencing- and order-related:

- A per-node **`BlobReplica`** actor owns this node's local on-disk store (§5.1 mechanics) and accepts four messages: `StoreBlob { ns, id, bytes } -> StoreAck`, `FetchBlob { ns, id, range } -> Option<Vec<u8>>`, `HasBlob { ns, id } -> bool`, and `DeleteNamespace { ns, deleted_at } -> DeleteAck`. It is registered in the receptionist (actor §13) under a well-known key so peers discover it. Unlike `StoreRecord` (granary `replica_store.rs`), `StoreBlob` carries **no shard, no `after`, no term, and no `repair` flag**: nothing needs fencing and nothing needs ordering, and the only field beyond the bytes is the namespace it lives under.
- A **`BlobTransport`** seam (the analogue of `ReplicaTransport`, granary `replica_store.rs`) sends those messages to a named peer's `BlobReplica`, resolving it through the receptionist; its reference implementation rides the actor system's `Transport`. Keeping it a seam preserves deterministic simulation (§8).

`StoreAck` is `Stored` or `Deleted` (the target namespace is tombstoned, §5.3), plus the transport errors surfaced by the caller. It has no `Fenced` or `Stale` variant, because there is no term and no mutable head to be stale against (contrast granary `store.rs` `StoreAck`). `DeleteNamespace` is fanned out to every serving node, not only a blob's owners (§5.3); each recipient durably records the tombstone, acks, and sweeps its local bytes in the background. It is idempotent and monotonic, so a redelivered, gossiped, or reconcile-driven `DeleteNamespace` is harmless, and a node re-syncs missed tombstones from the anchor owners on rejoin. This message set makes the §4 thesis concrete in the wire contract: a verified write, a verified read, and a monotonic delete flag, with no order and no term.

---

## 7. Placement and rebalancing

Owner selection is a pure function of the membership view (§5.2), so as nodes join and leave, the *intended* placement of every blob changes automatically and minimally: rendezvous hashing reassigns only the blobs whose owner set changed (utilities §2.2, invariant U1). Recomputing owners does not move bytes, though. Restoring the durability target after a change requires an active **reconcile loop**, modeled on granary's `shardmap.rs` reconcile (granary §7.6) but copying blob bytes rather than reconfiguring a Raft group.

The loop is **push-based and decentralized**. Periodically, and on a membership change, each node ensures every blob it holds is present on that blob's *current* top-R owners: it computes `top(serving_members, key(ns, id), R)` for each local blob and `StoreBlob`s to any current owner that lacks it (`HasBlob` gates the copy). A blob whose namespace is tombstoned (§5.3) is **never copied**: reconcile skips it, and the receiving owner would reject it anyway, so rebalancing cannot resurrect a deleted namespace. No coordinator is needed; each surviving owner drives its blobs toward the target on its own.

Rebalancing **only restores copies; it never drops them**. Bytes are removed on exactly one path, `delete_namespace` (§5.3), never by an inference of the reconcile loop, which cannot know whether a misplaced copy is still wanted:

- **Node removed → mandatory re-replication.** A blob that lost an owner is now under-replicated, and demand never read-repairs a cold blob, so the surviving owners actively re-push it to the new R-th owner, restoring the R − W margin (**B6**). This is the correctness-critical direction.
- **Node added → optional migration.** Nothing is lost when a new node holds nothing: the prior owners still have W..R copies. Migrating some blobs onto the new node is load balancing, not durability, so it MAY be lazy or low-priority.
- **No drop on rebalance.** A node that is no longer an owner of a blob it holds keeps the extra copy; reconcile tolerates over-replication and never risks under-replication. Such a now-misplaced copy is reclaimed only when its whole namespace is deleted (then the tombstone-driven sweep removes it, §5.3), not by reconcile guessing it is unwanted.

The loop reads membership through the same seam the rest of the cluster does and runs on the framework's `Spawner`/`Clock`, so it is seed-reproducible under simulation (§8). It SHOULD rate-limit copying and prioritize restoring under-replicated blobs over balancing well-replicated ones.

---

## 8. Testability and deterministic simulation

The `Local` and `Clustered` tiers are testable by the same deterministic simulation as the actor framework (actor §18, granary §14): a cluster of blob stores runs in one process, on one logical thread, over virtual time, network, and randomness, so a single `(seed, configuration)` reproduces a run exactly (V&V principle 1). The `BlobTransport` and the store are seams (§3, §6), so simulation drives the real placement, replication, and reconcile code, not a model of it.

Fault injection MUST be able to produce:

- **node loss mid-`put`**: fewer than W acknowledge, surfacing `Unavailable`, with a later retry succeeding;
- **under-replication repair**: a node holding owner copies leaves, and the reconcile loop (§7) restores R copies on the surviving owners (**B6**);
- **`put`/`get` under partition**: a minority-side reader widens past unreachable owners (§5.2) or fails cleanly;
- **duplicate `put`**: the same content stored concurrently from two nodes (same namespace) converges to one blob (**B2, B4**);
- **a corrupted stored blob**: a tampered on-disk or in-flight blob is detected on read and never returned as valid (**B1**), and on the clustered tier the reader falls through to a good owner;
- **delete during partition (resurrection)**: a namespace is deleted while one owner is partitioned; on rejoin the reconcile loop MUST NOT resurrect any blob of that namespace, because the tombstone rejects the copy and then sweeps it (**B7**). This must hold however long the partition lasts, since retention is gated on the partitioned node rejoining-and-sweeping or reaching terminal `down`/`removed` (actor §9.1), not on a grace timer. Two injectable cases: a partition outlasting any fixed window, and a merely `unreachable` node returning with its disk, whose tombstone MUST NOT have been forgotten;
- **`put` racing `delete_namespace`**: a `put` into a namespace being deleted either succeeds before the tombstone (and is swept) or is refused with `Deleted`, but never leaves a resolvable blob in a deleted namespace (**B7**).

Per the V&V checklist: codec round-trips for the replica messages (including `DeleteNamespace`), idempotency/duplicate-tolerance tests (B2), node-crash cascade tests (B6), delete-monotonicity and no-resurrection tests (B7), and seed-reproducibility of the event stream.

---

## 9. Invariants

Invariants hold under the faults of §8 and are verified as the framework prescribes (actor §18.5/§18.6): continuous checkers over the event stream for safety properties, targeted simulation tests for the rest.

| # | Invariant | Defined in | Verified by |
|---|---|---|---|
| **B1** | **Address integrity.** `get(ns, id)` returns bytes whose BLAKE3 hash equals `id`, or an error; it never returns wrong or corrupt bytes. Verification is on the read path, after any network transfer. | §2, §4 | `a_corrupt_blob_is_detected_and_never_returned`, `a_tampered_in_flight_blob_falls_through_to_a_good_owner` |
| **B2** | **Idempotent, dedup'd put.** Equal content under the same namespace yields one stored copy; a `put` of already-present content writes nothing new and re-acknowledges. | §2, §5 | `putting_the_same_bytes_twice_stores_once`, `concurrent_puts_of_equal_content_converge` |
| **B3** | **Durability target.** A `put` is acknowledged only once at least W copies are stored (one on `Local`); the blob then survives the loss of any R − W owners. | §5.2 | `a_put_acks_at_w_copies`, `a_blob_survives_losing_r_minus_w_owners` |
| **B4** | **No consensus on the data path.** The store runs no election, term, agreement round, or write-time read-repair; concurrent writers of the same content do not coordinate and do not fork. | §4 | `the_data_path_runs_no_consensus_group` (structural) + the B2 convergence tests |
| **B5** | **Deterministic placement.** A blob's owners are a pure, version-stable function of the serving set and the `(namespace, content hash)` key; every node agrees on them for a given view, and a single membership change reassigns only the blobs whose owners changed. | §5.2 | reuses `placement` known-answer vectors (utilities U1); `one_membership_change_moves_minimal_blobs` |
| **B6** | **Repair restores the target.** After a node leaves, the reconcile loop restores ≥ R copies of every live blob the cluster still holds anywhere; rebalancing is additive and never drops the last or only verifying copy of a non-deleted blob. | §7 | `a_departed_owner_is_re_replicated`, `rebalancing_never_deletes` |
| **B7** | **Monotonic deletion, no resurrection.** `delete_namespace` is set-once and commutes with itself. *Safety:* no node aware of the tombstone resolves a blob of the namespace, no `put` into a deleted namespace ever leaves a resolvable blob, and reconcile never resurrects one, even across a partition of unbounded duration, because the tombstone outlives every member that could carry a stale copy (retention is gated on each holder acking its sweep or reaching terminal `down`/`removed`, after which it rejoins only as a fresh empty identity, actor §9.1, not on a timer). *Liveness:* the tombstone reaches every serving node within the propagation bound, after which the namespace resolves nowhere. | §4, §5.3 | `a_deleted_namespace_stays_deleted`, `a_partitioned_owner_does_not_resurrect_a_deleted_namespace`, `a_put_racing_delete_never_resolves` |

A `blob-store` implementation conforms iff every invariant holds, verified under deterministic simulation (§8) for both tiers.

---

## 10. Non-goals and future work

Non-goals (today and by design):

- **No mutation.** Blobs are immutable; a namespace's value for `(ns, id)` never changes in place. Names, directories, chunking, and assembly are a consumer's (§1).
- **No fine-grained liveness.** The store knows liveness only at namespace granularity (§5.3). It does not know which *individual* blobs in a live namespace are referenced, so it never reclaims one on its own; per-blob reclamation is consumer-driven compaction (below).

Future work, named so the core stays small:

- **Compaction helper.** A reusable routine for the consumer pattern of reclaiming individual blobs within a live namespace: copy the still-referenced blobs from `ns` into a fresh `ns'`, redirect the consumer's references, then `delete_namespace(ns)`. The store stays free of reference knowledge; the helper packages the copy-and-swap (and could stream it tier-to-tier). This is the namespaced analogue of a copying garbage collector, driven from the consumer's root set, never from inside the store.
- **Reference-aware per-blob delete.** If a consumer ever needs to drop a single blob without compacting, a `delete(ns, id)` with refcount or consumer-declared roots could be added. It is *not* in v1, because it reintroduces the liveness problem that namespacing was chosen to avoid, and compaction covers the motivating cases without it.
- **Cross-namespace dedup.** An optional shared namespace (never deleted, or compacted on its own schedule) into which consumers place content they want two lifecycles to share, recovering the cross-namespace dedup §2 gives up, at the cost of that namespace needing its own reclamation story. Out of v1 by choice.
- **Range-verified streaming.** Independently verifying a byte range of one large blob against the BLAKE3 tree the `BlobId` already roots (the Bao encoding), so very large blobs need not be fetched whole to be verified (§2). Because the id *is* the tree root, this exposes existing structure rather than layering a second hash over a flat digest.
- **External object-store tier.** An S3-compatible backend behind the same seam, with the blob a single content-keyed object, `put` a conditional upload, and `get` a verified fetch, suited to cloud and cold or archival storage. It cannot be driven by the deterministic simulator (§8), so it would carry only integration-test coverage against an emulated endpoint.
- **Tiering.** A wrapper composing `Clustered` (hot) over the external object-store tier (cold) with fall-through reads and background demotion, the hot/cold split Cloudflare draws between a Durable Object's local SQLite and its object-store archive (DO §4.2).
- **Locality caching.** A read-through local cache of fetched blobs on a non-owner node; because blobs are immutable, cached copies never invalidate, so first read is remote and every later read is local.
- **Encryption at rest.** Per-blob encryption beneath the content hash (the hash over ciphertext or a convergent scheme), for untrusted backends.

---

## Appendix A: End-to-end example

```rust
// --- Clustered tier on an existing cluster system (actor §9.4.3) ---
let blobs: Arc<dyn BlobStore> = BlobStore::clustered(system.clone(), BlobConfig {
    replication_factor: 3,      // R: owners per blob (§5.2)
    write_quorum: 2,            // W ≤ R: acks before a put returns durable (§5.2)
    max_blob_bytes: 4 << 20,    // bound a blob; consumers chunk beyond it (§2)
});

// --- A namespace groups blobs with a shared lifecycle (§2): here, one workspace ---
let ws = Namespace::fresh();   // single-use id (a UUID); never reused after deletion

// --- Store some bytes under the namespace; the id is their BLAKE3 hash (§2) ---
let id = blobs.put(&ws, block.to_vec()).await?;   // Ok(BlobId) once W copies are durable

// --- Fetch and verify (§4); identical call site on any tier ---
match blobs.get(&ws, &id, None).await {
    Ok(bytes)                      => assert_eq!(blake3(&bytes), id),   // B1 holds by construction
    Err(BlobError::Unavailable(_)) => { /* fewer than the needed copies reachable; retry/failover */ }
    Err(BlobError::Corrupt(_))     => { /* every reachable copy failed verification: data loss */ }
    Err(BlobError::Deleted(_))     => { /* the namespace has been deleted (§5.3) */ }
    Err(BlobError::Transport(e))   => eprintln!("transport: {e:?}"),
}

// --- Membership: presence is a durable-copy question (§5.2) ---
assert!(blobs.has(&ws, &id).await?);

// --- Reclaim the whole workspace in one call; no per-blob bookkeeping (§5.3) ---
blobs.delete_namespace(&ws).await?;               // (ws, *) becomes permanently unresolvable
assert!(!blobs.has(&ws, &id).await?);

// --- Single-node / test deployments use the same seam ---
let local: Arc<dyn BlobStore> = BlobStore::local("/var/lib/app/blobs")?;   // §5.1
```

```rust
pub enum BlobError {
    Unavailable(String),   // could not reach W copies on put, or any owner on get (§5.2)
    Corrupt(BlobId),       // a copy was found but none verified against the id (§4)
    Deleted(Namespace),    // the target namespace has been deleted (§5.3)
    Transport(CallError),  // underlying actor transport/system failure (actor §14.1)
}
```

## Appendix B: Suggested crate layout

```
blob-store/                # namespaced, content-addressed object store on actor-core + actor-cluster
  blob.rs                  # BlobId (BLAKE3), Namespace, the BlobStore seam, BlobError, BlobConfig (§2, §3)
  local.rs                 # Local tier: on-disk CAS over wal::atomic_replace; namespace subtree + tombstone (§5.1, §5.3)
  cluster.rs               # Clustered tier: owner selection, W-of-R put, verified rank-order read, delete (§5.2, §5.3)
  replica.rs               # per-node BlobReplica actor (StoreBlob/FetchBlob/HasBlob/DeleteNamespace) + BlobTransport seam (§6)
  placement.rs             # thin reuse of actor-cluster placement::top, keyed on (namespace, id) (§5.2)
  tombstone.rs             # namespace tombstone record, replication, sweep-ack tracking, membership-gated reclamation (§5.3)
  reconcile.rs             # additive, push-based rebalancing that respects tombstones (§7)
```

`blob-store` depends on **actor-core** (the model and the `Clock`/`Entropy`/`Spawner` seams), **actor-cluster** (the `Transport`, `Membership::serving_members`, `placement`, and the receptionist), **actor-serialization** (the replica messages' codec), and **wal** (the `Local` tier's atomic-replace and checksum, wal §5). It depends on **neither granary nor the runtime or simulation crates**: the host system injects the time, randomness, spawning, and transport seams, exactly as for `ClusterSystem<C, E, S, T>` (actor §18), so the same store code runs in production and in the deterministic simulator. The durable filesystem grain (`research/durable-sqlite-and-filesystem.md` §4) depends on `blob-store`; `blob-store` depends on none of its consumers.
