# Entity Storage: Specification

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

A `§` with no document name refers to this spec. `entity §N` is [`entity-spec.md`](entity-spec.md), `facets §N` is [`entity-facets-spec.md`](entity-facets-spec.md), and `actor §N` is [`distributed-actor-spec.md`](distributed-actor-spec.md). Durable Objects lessons are cited as **DO §N** ([`../research/durable-objects.md`](../research/durable-objects.md)).

---

## 1. Scope

Entity storage is the durable substrate an entity's host writes through, and the only layer with a fence, a quorum, and a recovery protocol. It has two replication primitives: per-entity ordered, term-fenced **records** (§4) and immutable content-addressed **blobs** (§9). Every further durable feature is a facet interpreting those two (facets §3). Records are placed by **shards** (§3) whose leaders are elected by consensus, while each entity's records replicate by a per-entity quorum append with no consensus on the data path. Snapshots bound replay (§8).

It builds on the actor framework's consensus groups (actor §6.4.3), messaging (no new transport), and deterministic seams (actor §2.12.1); the file-backed store rests on the `wal` crate (`wal::Wal`) and the compatibility spec's stamps (compatibility §4). The entity runtime (entity §2.3, §5) and the facets build on it. This half of the `entity` crate knows the programming model only through `EntityName`.

### 1.1 Non-goals

- **A shared log.** No cluster-wide or shard-wide log orders records (§4).
- **Consensus per entity, or on the data path.** Consensus places shards; a record needs only a quorum append (§3.2).
- **Write availability under quorum loss** (§10).

---

## 2. Seams

### 2.1 The journal: `EntityJournal`, `DynEntityJournal`

The host's only view of storage, over opaque codec-encoded bytes. `DynEntityJournal` is the object-safe form the runtime holds per shard, blanket-implemented for every `EntityJournal`. It is an internal boundary: its implementations, `LocalEntityJournal` and the per-shard quorum journal, are chosen by the system type through `ShardMapSource::journal` (entity §2.3), not injected by a deployment.

```rust
pub trait EntityJournal: Clone + Send + Sync + 'static {
    fn append(&self, entity: &EntityName, after: Seq, events: Vec<Vec<u8>>) -> impl Future<Output = AppendOutcome> + Send;
    fn load(&self, entity: &EntityName, from: Seq, limit: usize) -> impl Future<Output = Result<RecordPage, EntityJournalError>> + Send;
    fn head(&self, entity: &EntityName) -> impl Future<Output = Result<Seq, EntityJournalError>> + Send;
    fn term(&self) -> Option<Term>;
    fn save_snapshot(&self, entity: &EntityName, at: Seq, state: Vec<u8>) -> impl Future<Output = AppendOutcome> + Send;
    fn load_snapshot(&self, entity: &EntityName) -> impl Future<Output = Result<Option<(Seq, Vec<u8>)>, EntityJournalError>> + Send;
    fn put_blob(&self, entity: &EntityName, id: BlobId, bytes: Vec<u8>) -> impl Future<Output = Result<(), EntityJournalError>> + Send;
    fn get_blob(&self, entity: &EntityName, id: BlobId) -> impl Future<Output = Result<Option<Vec<u8>>, EntityJournalError>> + Send;
    fn has_blob(&self, entity: &EntityName, id: BlobId) -> impl Future<Output = Result<bool, EntityJournalError>> + Send;
    fn retain_blobs(&self, entity: &EntityName, retain: Vec<BlobId>) -> impl Future<Output = ()> + Send;
    fn delete_blobs(&self, entity: &EntityName) -> impl Future<Output = ()> + Send;
}
```

The host calls `append` only on the shard leader and behind the input gate, so `after` is always the entity's known head (entity §5). The journal's side:

- `append` commits `events` as one atomic entry after `after`. `AppendOutcome::Committed` carries the new head and means durable. `NotLeader` MUST be returned only when no replica can hold the batch, so the command provably did not commit. `Unavailable` is ambiguous: the batch MAY commit later, at its own slots (§4).
- `load` returns up to `limit` records after `from` (exclusive), ascending, in a `RecordPage` whose base is read consistently with them (§8.2): a local, fence-free read.
- `head` is the rehydration barrier: it returns the committed head per §5.2 and leaves the records and best snapshot in the local store, so later `load` and `load_snapshot` read locally.
- `term` is the term this node writes the shard under now, `None` off the leader: a local read. Regained leadership is always a higher term, so an unchanged term proves no other node appended in between.
- `save_snapshot` persists a snapshot at a committed `Seq` (§8.1); `NotLeader` off the leader.
- The blob half is unordered and unfenced (§9): `put_blob` returns once durable, `get_blob` returns verified bytes or `None`, `has_blob` reports presence on any reachable replica, and the reclamation calls are best-effort.
- A local read that cannot complete is `EntityJournalError::Unavailable`.

**Durability tiers.** On `Local`, one linearizable store: an append commits once that store has made it durable, the term is `Term::ZERO`, `NotLeader` never arises, and `Unavailable` arises only from a failed store. On `Quorum`, a per-entity quorum append over the shard's replicas, fenced by the term (§5), surviving the loss of any minority. Blobs ride the record seam because they share an entity's replicas, recovery, migration and split passes, and simulation seed.

### 2.2 The replica store: `EntityStore`

A node's durable store of entity records, snapshots, and blobs, and a plug point: a deployment supplies an `EntityStoreFactory` on `EntityConfig::entity_store`, called once per `(entity type, node)`. `MemoryEntityStore` (the default, lost on restart) and `FileEntityStore` (durable, built by `EntityNode::durable`) are the stock implementations. The `Local` journal writes one store directly; on `Quorum` the leader writes each replica's store through the replica protocol (§2.3).

The store's side:

- **Durable on return.** Every call is synchronous; when it returns, its effect has happened. A store MUST acknowledge a write (`StoreAck::Stored`, `BlobAck::Stored`) only once it is durable.
- **The fence.** Records are keyed by `(shard, entity)` and stamped with their term. A store MUST refuse any record or snapshot write stamped below the highest term it has promised for the shard (`StoreAck::Fenced`, carrying that term).
- **Recovery reads fence.** `prepare` MUST record its promise not to accept a lower term before returning the replica's slots and snapshot (`ReadOutcome::Fenced` if it promised higher). A plain `read` does not fence and MAY include records not yet stable, which a caller MUST NOT count toward a quorum.
- **Write kinds.** A `WriteKind::Append` MUST be refused as `Stale` when `after` is below the compaction base or a target slot holds anything but a re-delivery of the same append (same term and bytes). A `Repair` fills slots by highest term and never reports `Stale`. A `Transfer` skips the fence and is safe only from the split and merge driver (§7). Every kind keeps a slot already holding an equal-or-higher term.
- **Term-bounded rollback.** `truncate(after, term)` MUST drop only records past `after` whose term is at most `term` (§4).
- **The append bound.** `seal_range` MUST durably and monotonically refuse every `Append` whose name hash is at or above the bound, at any term (`Sealed`), admitting repairs and transfers; `unseal`, `remove_range`, and `drop_shard` apply a committed split or merge.
- **Failure is a refusal.** A store that cannot make a write durable MUST answer `Failed`, a refusal that counts toward no quorum, and MUST then report `failed` as true, so its unfenced reads are not trusted (§10).
- **One store per type.** A store MUST NOT be shared across entity types (§5.1).
- **A stamp at rest.** A store whose records outlive their writer MUST carry an `entity.store` stamp naming the deployment codec, admitted at open before any fence, manifest, or record is read (compatibility §4), so a codec change is one refusal naming both codecs. A directory with no stamp is fresh and records the running codec.

The caller's side: every refusal is `#[must_use]`, and a caller MUST read it, since dropping one is how a deposed leader convinces itself it committed.

### 2.3 The replica protocol: `ReplicaTransport`

How a shard leader reaches its replicas' stores: one message per `EntityStore` call, in `entity::cluster::replica` (`StoreRecord`, `Prepare`, `StoreSnapshot`, `SealRange`, `PutBlob`, `GetBlob`, `HasBlob`, `SweepBlobs`, `ListEntities`, `ListBlobs`), each a `Message` whose manifest (`entity.StoreRecord` and so on) both ends must agree on. An internal boundary with one implementation over actor messaging: each node runs one replica-store actor per hosted type, owning its store.

- A replica MUST answer each message by running the named call on its store and replying with its outcome, so it acknowledges exactly what its store made durable. A `SealRange` reply means the bound is durable.
- A transport failure counts as no acknowledgement and as possibly applied: a timed-out message MAY still land.
- The leader's own replica is reached locally and counts toward the quorum like any other.
- Every ask the leader issues MUST run to completion: after a quorum commits, the stragglers drain off the hot path (`ReplicaTransport::launch`), so none is abandoned unobserved (actor §2.12.2).

### 2.4 Placement: `ShardMapSource`, `FailureDomains`

`ShardMapSource` is a node's object-safe access to its type's shard map (§6): a shard's committed replicas, this node's journal for a shard it replicates, the shard owning a name hash (`shard_of`), the committed shards, split and merge requests, the leadership handoff (§5.3), and why the map stopped (`poisoned`). It is an internal boundary with three implementations (single-node, consensus-backed, and a routing-only client's empty map). The consensus engine is actor-cluster's `RaftConsensus` (actor §2.19).

- `shard_of` is authoritative once ranges commit; the founding partition (`shard_for`) agrees with it until the first split or merge, and serves only while the map bootstraps.
- `FailureDomains` MUST map a node to the same domain on every node: static deployment metadata qualifies, anything node-local does not.
- `Deadlines` carries `EntityConfig::quorum_timeout` and `EntityConfig::recover_timeout`. Every expiry is ambiguous, so a deadline set too low turns slowness into step-downs.

### 2.5 Blocking I/O: `BlockingIo`, `NodeStore`

Where a store's blocking device work runs: a node-scoped plug point (`EntityNode::blocking_io`), with `InlineIo` (the default) and `ThreadPoolIo`. The runtime reaches a store only through `NodeStore`, which pairs it with this seam.

- A file-backed store's writes MUST NOT run on the async worker that drives the node's heartbeats and quorum waits, where one slow device triggers re-elections and a rehydration storm; a deployment on real storage SHOULD run them on a pool.
- Every write (records, snapshots, `prepare`, rollbacks, seals, range removals, blobs and their reclamation) MUST be offloaded and awaited before the caller proceeds; reads MAY run inline, since no durability barrier is open across them.
- The seam changes which thread blocks, never the order of guard, fence, and apply; `BlockingIo::submit` refuses only a job that will never run.
- The default is inline because the deterministic simulation runs the production store (entity §11).

### 2.6 The blob area: `EntityBlobs`, `BlobId`

An entity's handle on its blob area (`EntityCtx::blobs`), over the blob halves of the journal and store: `put`, `get` (whole or a byte range), `has`, `gc`, `repair_facets`, and `destroy`. Semantics are §9; the facets' share of the live set is facets §3.6.

---

## 3. Shards

### 3.1 The partition and the leader-election group

Each entity type's names are partitioned into shards, each owning a contiguous range of a 64-bit name-hash space. A stable hash, which MUST agree on every node and across runs, places each name in one shard. The founding partition is `EntityConfig::shards` near-equal ranges; only a split or merge changes it (§7).

Each shard has a consensus group of *R* replicas (`EntityConfig::replication_factor`), its **leader-election group**, which elects one leader per term and records the replica set. It holds no entity data. The leader is the single writer and host for every entity in the range (entity §4.2). The cluster runs one such group per shard plus one map group per type (§6), bounded by split and merge (§7), never one per entity, and none on the data path (§4, **E7**). Shard-mates share leadership and replicas, not a serialization point: each entity's records are quorum-appended independently.

**Replica placement SHOULD be failure-domain aware.** Durability rests on quorum intersection over a shard's replicas, so a shard whose replicas share one zone is lost with it. A deployment that knows its domains SHOULD supply `FailureDomains`; the allocator then takes at most `ceil(R / domains)` replicas per domain, filling from the remaining candidates when a lopsided layout cannot meet the cap, since an imperfectly spread shard beats an under-replicated one.

### 3.2 Consensus layers and cost

Three consensus layers run at different rates, and none touches entity data: the actor control plane (one group, actor §6.4.3) owns membership and seeds the map groups' voters; the shard map (one group per type, §6) owns each shard's replicas and range; the leader-election groups (one per shard) own each shard's leader and term. Group identifiers MUST NOT collide across the layers. The total is O(shards) plus O(entity types) plus one, bounded by split and merge (**E7**).

Cost therefore tracks the active working set and the cluster size, not the entity count. Control-plane writes happen only on cluster events (**E9**). Activation touches no consensus group and costs at most one quorum head recovery (**E8**), which makes aggressive hibernation affordable. The residual cost is placement: a hot shard's entities share one leader, which splitting caps.

---

## 4. Records: ordering and quorum durability

The shard leader is an entity's single writer (§5.1), so it assigns each record the next `Seq` with no agreement round. The quorum journal fans the record to the shard's replicas, writing the leader's own replica in the same round, and reports it durable once a write quorum has stored it (DO §4.3); during a replica-set migration the quorum is joint (§6). An entity's records are totally ordered among themselves and unordered against other entities'; other entities' appends proceed concurrently on their own quorums.

**Idempotent by slot.** An append not acknowledged within `quorum_timeout` is `Unavailable` (§10) and MAY still reach a quorum later. That never double-applies: the host neither retries an append nor folds anything but a contiguous commit (entity §5), and the next activation recovers the head (§5.2) and folds each record once, at its one slot. The `Seq` slot is the record's identity; no de-duplication token is needed.

**The rollback is term-bounded.** On any outcome but a commit, the leader MUST roll back its own tentative local write, so a later recovery falling back to the local view never folds an uncommitted record, and MUST drop only slots written at or below the appending term: a newer leader may meanwhile have fenced this store and landed committed records above the same head (**E14**).

---

## 5. The fence and recovery

### 5.1 The term

The fence is the shard's leadership term: the leader-election group elects at most one leader per term, every append carries that term, and each replica persists the highest term it has acknowledged and refuses lower writes (§2.2). A write therefore commits only while a quorum still recognizes its term: a deposed leader's in-flight appends reach no quorum, two nodes that briefly both believe they lead cannot both commit, and a record never forks (**E1**). A host whose append is refused for leadership MUST step down and MUST NOT keep serving an entity whose shard it no longer leads.

**The persisted fence MUST be keyed by the shard's whole identity,** `(entity_type, index)`. The same index under two types is two groups whose terms advance independently; keyed by index alone, the type that elects more often raises the fence past the other's term and every append and recovery of the quieter type is refused, unrecoverably. Hence one store per type (§2.2).

Leadership is a quorum fact, not a clock: a leader that pauses and wakes finds its appends refused under a higher term, whatever the skew. A clock lease appears only as the deferred route to linearizable reads (entity §13).

### 5.2 Recovery

A fresh leader does not inherit an entity's records as a Raft leader inherits its log. Before it serves an entity it MUST recover the entity's head through `head` (§2.1):

- **A head recovered under a confirmed quorum MUST contain every record any earlier term reported committed, at its original `Seq` and bytes** (**E14**), and MUST be durable on a write quorum under the recovering term before the first append, so no later recovery regresses it. Where replicas disagree at a slot, the higher term wins: a partitioned replica MAY hold a stale-term record at a slot a higher term later took. One conforming recovery fences a read quorum with `prepare`, keeps the highest-term record per slot above the best snapshot, and writes the merged tail back under its own term.
- **Short of a quorum** within `recover_timeout`, the leader MAY adopt the merged local view without write-back. That view MAY be stale and MUST NOT be able to commit a write; it cannot, because every append still needs a quorum, and some up-to-date replica in it refuses an append onto a slot it holds differently (`Stale`) or below its base.
- Recovery MUST fail as `Unavailable` while the shard is electing, when a store has fenced the node behind a higher term, when the local store has failed (§10), and when a confirmed read's write-back reaches no quorum.
- A split or merge transfer (§7) and a migration catch-up (§6) MUST use a confirmed quorum, never the fallback.

### 5.3 Failover

**A planned departure SHOULD hand leadership off rather than drop it.** A node shut down deliberately SHOULD hand each shard it leads to a caught-up replica (actor §6.4.3) through `EntityNode::hand_off_leadership`, which covers every type hosted through the node. Skipping it costs availability, not correctness: each led shard otherwise waits out an election and rehydrates its active entities cold. The handoff is best-effort, and a departing node MUST NOT block its shutdown waiting for one.

On an unplanned loss the replicas elect a new leader under a higher term, which recovers each entity lazily, on first access (§5.2). During the failover window calls fail fast (`NotLeader` then a retry, or `Unavailable`) rather than block. No acknowledged write is lost (**E14**).

---

## 6. The shard map and replica-set migration

The shard map records each shard's key range and replica set; leadership is the leader-election group's, surfaced by `NotLeader` redirects. The map MUST be consensus-agreed, so every node resolves a name to the same replica set regardless of join order. Each type's map is a consensus group whose committed log is the allocation, seeded from the leader-based control plane's voters (actor §6.4.3); the gossip mode (actor §6.4.4) cannot agree one, so entity REQUIRES the leader-based mode.

A leader-only allocator keeps each shard's committed replica set equal to the rendezvous choice over the current cluster voters (utilities §2.1), spread across failure domains (§3.1). Because the leader-election group holds no entity data, catching up its log is not enough: a new replica MUST NOT stand in for a member of the current majority before it holds each entity's committed records and latest snapshot (**E14**). A changed choice runs a **joint-quorum migration**:

1. The new set commits as the shard's **target**, beside the current set. From then on every append, snapshot, and recovery MUST use the joint quorum (a majority of each set, fanned to their union), so every earlier and concurrent commit stays intersected by every recovery. Nothing further is proposed for the shard until the migration ends.
2. The shard leader catches every entity up on the target: it enumerates the shard's entities from a read quorum of the current set, recovers each under the joint confirmed quorum (whose write-back lands the records on the target), re-persists the best snapshot on the joint quorum, and copies the blob area (§9). Every step is idempotent; a leader change re-drives it.
3. Only after a complete pass may the leader commit the flip to the target. Departing nodes drop their journals, and a routing hint is trusted only while the committed map still lists the hinted node.

**A committed command a node cannot decode MUST stop that node's map, never be skipped** (compatibility **V2**). The node freezes at the last command it applied, hands off the shards it leads, stops proposing, keeps acknowledging map-group appends, and reports why through `ShardMapSource::poisoned`, which is one-way. Commands carry the `entity.shardmap` stamp (compatibility §4), so another version is refused by version.

The map changes only on splits, merges, reconfiguration, and membership changes, never on an entity write or activation (**E9**).

---

## 7. Split and merge

A shard that grows too large or hot **splits**: the parent keeps the low half of its range and a fresh child shard (a new index, so a new group) takes the high half on the parent's replicas. Two small adjacent shards **merge** the reverse way, the left absorbing the right, whose group retires. A split MUST be atomic with respect to writes, in this order:

1. **Seal.** A majority of the parent's replicas durably record an append bound at the boundary (`EntityStore::seal_range`), so no append to the moving range can assemble a quorum at any term, even from a leader that has not learned of the split. A leader that knows of it refuses the append before any store attempt (`NotLeader`); a store's refusal is ambiguous and surfaces as `Unavailable`. Either way the caller re-resolves against the committed map.
2. **Transfer.** For each moved entity the parent leader recovers the head under a confirmed quorum (§5.2) and copies the records and latest snapshot to the child's keys on a majority of its replicas, unfenced (`WriteKind::Transfer`), since no child leader exists yet; the blob area is copied best-effort (§9).
3. **Commit.** Only after a full pass does the new mapping commit; no node routes the moved range to the child before it.

No write is lost or duplicated, and no entity is writable in two shards at once (**E15**). Every step is idempotent and derived from committed map state, so a crash or leader change re-drives it. A merge mirrors the split, driven by the right shard's leader, whose term fences the right's deposed leaders: seal the whole right range, transfer its entities into the left's keys, commit (**E7**). Splits, merges, and migrations run one at a time per shard, and every proposal is validated deterministically against committed state, so a raced one is a no-op everywhere. A split or merge is requested explicitly (`Entities::split_shard`, `Entities::merge_shards`), and a split also by the opt-in size trigger `EntityConfig::shard_target_bytes`.

---

## 8. Snapshots and compaction

### 8.1 Snapshots and rehydration

After a commit the host MAY persist a snapshot of the composite state (facets §3.5) at the head through `save_snapshot`, on the trigger `EntityConfig::snapshot_every`. Only the shard leader writes snapshots, and one trigger governs every snapshot, the idle one included (entity §7). On `Quorum` a snapshot is broadcast to every replica while the replay it saves is read locally ([hardware-envelope](hardware-envelope.md) §3.9), so a type whose events are large relative to its state SHOULD lower the trigger.

**Rehydration.** Before reading an entity's head a leader MUST pass the recovery barrier (`head`, §5.2). It then reads the snapshot the barrier selected and replays the later records, up to the recovered head, through `apply` and the facets' folds (facets §3.2). The head comes only from journal and snapshot returns (**E3**), so an entity evicted on idle re-activates with identical state (**E12**).

**A snapshot MUST NOT shorten the effective log** (**E4**). A head is defined as the best snapshot's `Seq` plus the contiguous run of records above it (`ReadReply::head`), so no snapshot can outrun the head it is measured against. The host MUST still refuse a snapshot beyond the recovered head and replay from the journal; and since a compacted prefix exists only in the snapshot, a replay whose cursor is below the store's base MUST fail the activation rather than fold over the gap.

### 8.2 Compaction

Storing a snapshot MAY drop the records it subsumes and advance the entity's **base**. Every `load` page MUST report the base, and every event read carries it to the caller (entity §6.2), because a truncated log served as complete is invisible data loss. A snapshot is taken only at a committed head, and a replica MUST drop a prefix only if it holds a snapshot covering it, so any recovery quorum finds every committed record or a snapshot subsuming it (**E14**); compaction therefore need not wait for the snapshot to be quorum-durable. Each entity compacts independently.

### 8.3 Encoding at rest

- **The record envelope** is `[tag][payload]` (`entity.record`, compatibility §3.3), worn by every record. A tag is `0..=0x7F`; tags `0..=0x3F` belong to facets defined in the `entity` crate and `0x40..=0x7F` to facets defined outside it, and a tag is permanent. A leading byte at or above `0x80` marks another envelope version, and a reader MUST refuse it by version, not as an unknown tag (facets §3.2). A payload change is a new version of the format that carries it.
- **The composite snapshot** is stamped `entity.snapshot`, its magic and version read before the body (compatibility §4). The body records the codec of facet 0's state, its one codec-dependent part; a mismatch MUST name both codecs and abort the activation.
- **Facet 0's state** rides inline below a threshold, and above it as content-defined chunks in the blob area named by the composite, so the chunks after an edit match those before it and a chunk already rooted is not put again. The chunk puts MUST complete before the record naming them is written.

---

## 9. Blobs

The journal holds small foldable state; bulk immutable bytes go in an entity's blob area, on the same shard replicas (DO §2.3). The blob path has no `Seq`, no term, and no read-repair: a content hash names one byte sequence forever, so writers cannot conflict and a reader verifies by re-hashing.

- **The model.** A `BlobId` is the 32-byte BLAKE3 digest of the bytes; a blob's address is `(EntityName, BlobId)`, write-once and deduplicated within an entity. A blob carries no format stamp: its name is its hash, checked on every read (compatibility §4).
- **Verification on read.** Every `get` MUST re-hash the bytes before returning them and MUST NOT return them on a mismatch; on `Quorum` it falls through to the next replica and errors only if none verifies (**E17**).
- **Durability.** A `put` writes the local replica and the peers and is acknowledged once a write quorum that includes the leader's own copy has stored the bytes (on `Local`, the one store), with no election, term, or agreement round (**E18**); short of a quorum it is `Unavailable`, and a retry is idempotent. `has` reports presence on any reachable replica, not durability.
- **Reclamation is entity-scoped and root-driven:** a mark-from-roots sweep (`EntityBlobs::gc`, over the union of facets §3.6) and a whole-area `destroy`. A delete a partitioned replica misses leaves orphan bytes for a later sweep, never a resurrected referenced blob, because only referenced blobs are fetched or re-replicated; no tombstone is needed.
- **Lazy hydration and self-heal.** A `get` that misses locally fetches a verifying copy from a peer and backfills, so a re-activation pulls only what it touches; a corrupt local copy is evicted, then replaced. `repair_facets` re-puts every facet-root blob to the current replicas, best-effort.
- **Replicas joining a shard receive the blobs.** Before a migration flips (§6) the leader MUST copy each entity's blob area to every target replica lacking any of it, verified (**E17**); an unreachable target fails the pass. A split or merge copies blobs best-effort, and a replica still lacking one heals on access.

---

## 10. Failure model

A failure's blast radius is the shard.

**A replica whose storage fails withdraws; it does not crash.** A replica that cannot make a write durable MUST NOT acknowledge it and MUST report the refusal rather than terminate: after an I/O failure (full volume, lost mount, failing device) the store answers `Failed` to every record store, recovery read, and blob store, and a `Failed` answer MUST NOT count toward any quorum. The node drops out of its quorums, which the majority rules absorb: writes commit on the other replicas, and the shard elects around a failed leader. The refusal MUST be distinguishable from `Fenced` or `Stale`, which a healthy replica decided. It SHOULD be one-way and store-wide, since the fault belongs to the volume. Terminating would turn a local fault into a cluster event.

**Quorum loss is unavailability, not a fork** (**E11**). A shard that cannot reach a quorum commits nothing: `append` is `Unavailable` and the activation steps down (entity §5), since a timed-out commit MAY land later. A minority cannot commit (no leader is elected there, and a stale-term append is refused), so the shard never forks, and only its entities are affected. Reads stay read-your-leader (entity §6.1).

---

## 11. Invariants defined here

This document defines, wholly or in part, the catalogue rows **E1** (§5), **E3** (§8), **E4** (§8), **E7** (§3.1, §4, §6, §7), **E8** (§3.2), **E9** (§6), **E11** (§10), **E12** (§8), **E14** (§5, §5.3), **E15** (§7), **E17** (§9), and **E18** (§9). The catalogue is entity §12.

---

## 12. Deferred

- **History that outlives compaction:** compacting facet records only would keep facet 0's events whole and drop the base from the read API, but recovery, transfer, migration, and the store's compaction rewrite would all have to carry the history.
- **Range-verified blob streaming:** verifying a byte range against the blob's BLAKE3 tree (Bao); a ranged `get` fetches and verifies the whole blob.
- **A cold blob tier:** an external content-addressed object store as the durable home of an entity's blobs, the replicas reduced to read-through caches (DO §6), with the journal's blob half as the plug point and the records and fence still quorum-replicated (machine §7).
