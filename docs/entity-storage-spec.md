# Entity Storage: Specification

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, and **MAY** carry the meanings defined in RFC 2119.

A `§` with no document name refers to this spec. `entity §N` is [`entity-spec.md`](entity-spec.md), `facets §N` is [`entity-facets-spec.md`](entity-facets-spec.md), `actor §N` is [`distributed-actor-spec.md`](distributed-actor-spec.md), `cluster §N` is [`cluster-spec.md`](cluster-spec.md), `utilities §N` is [`cluster-utilities-spec.md`](cluster-utilities-spec.md), and `compatibility §N` is [`compatibility-spec.md`](compatibility-spec.md).

---

## 1. Scope

Entity storage is the durable substrate an entity's host writes through, the only layer with a fence, a quorum, and a recovery protocol. Its two primitives are per-entity ordered, term-fenced records (§5), placed by shard (§4) with replay bounded by snapshots (§9), and immutable, content-addressed blobs (§10). It addresses an entity only by name (entity §2), never interpreting its state or events.

It builds on the cluster's consensus groups (cluster §3.5, §5.4.3), actor messaging (actor §7), and the compatibility convention (compatibility §3, §4), and follows the deterministic profile (actor §11). The entity runtime (entity §3.3, §6) and the facets (facets §4) build on it; every durable feature beyond records and blobs is a facet that interprets them.

### 1.1 Non-goals

- **A shared log.** No cluster-wide or shard-wide log orders records (§5).
- **Consensus per entity, or on the data path.** Consensus places shards and elects their leaders; a record needs only a quorum append (§4.2).
- **A transport of its own.** The replica protocol rides actor messaging (§3.3).
- **Write availability under quorum loss** (§11).

### 1.2 Conformance classes

- A *journal* meets §3.1 and §5 to §11 for its durability tier.
- A *store* meets §3.2, and §12 for each format it writes.
- A *replica* (a node answering the replica protocol) meets §3.3.
- A *blocking I/O provider* meets §3.4.
- A *host* (entity §2), the journal's caller, meets the caller obligations of §3.1, §6.1, and §9.1.
- A *deployment* meets the deployment obligations of §3.2, §3.4, §4.1, §6.3, §7, and §11.

---

## 2. Model

- **Record:** an opaque byte string at one position in an entity's total order, in the record envelope (§12).
- **Sequence number:** a record's position in its entity's total order, from 1; 0 is the empty head.
- **Batch:** the records of one append, committed atomically at consecutive sequence numbers.
- **Snapshot:** an entity's composite state (facets §4.4) at a committed sequence number, subsuming every record at or below it.
- **Head:** the best snapshot's sequence number (0 without one) plus the length of the contiguous run of records above it.
- **Base:** the sequence number at or below which compaction has dropped an entity's records, leaving them only in its snapshot (0 if none).
- **Blob:** an immutable byte string named by its **blob id**, the 32-byte BLAKE3 digest of its bytes. An entity's **blob area** holds its blobs, each addressed by the entity's name and blob id.
- **Name hash:** a stable hash of an entity name onto a 64-bit space.
- **Shard:** a contiguous range of one entity type's name-hash space, identified by the type and an index.
- **Replica set:** the R nodes holding a shard's records, snapshots, and blobs, for the deployment's replication factor R.
- **Leader-election group:** the consensus group of a shard's replicas; it elects one leader per term and holds no entity data.
- **Leader:** the node elected for the current term, the shard's only writer and host of all its entities.
- **Term:** the leader-election group's term, which only grows and is carried by every write a leader issues.
- **Durability tier:** the **local tier** (one store) or the **quorum tier** (the shard's replica set), fixed by the hosting system (entity §3.3; §3.1).
- **Write quorum** and **read quorum:** a majority of the replica set; during a migration, a **joint quorum** is a majority of each of the current and target sets.
- **Confirmed quorum:** a read quorum whose every member has promised the recovering term (§3.2).
- **Journal:** a node's seam onto one shard it replicates, serving every entity of the shard; each operation names one entity and reaches only that entity's records, snapshots, and blob area (§3.1).
- **Store:** one node's durable store of one entity type's records, snapshots, and blobs, keyed by shard and entity (§3.2).
- **Fence:** the highest term a store has accepted for a shard, by a fenced write or a recovery promise.
- **Append bound:** a per-shard name-hash bound that seals appends at or above it (§3.2, §8).
- **Shard map:** the per-type, consensus-agreed record of each shard's range, replica set, and, during a **migration** to new replicas, **target** set (§7).
- **Recovery:** establishing an entity's head under a leader's term before serving it (§6.2).

---

## 3. Seams

### 3.1 The journal

A boundary between the entity host (the caller, entity §2) and storage, one journal per shard a node replicates, serving all of the shard's entities. Every operation names one entity and reaches only that entity's records, snapshots, and blob area.

| Operation | Takes | Yields |
|---|---|---|
| append | the head it follows, a batch | committed with the new head, not leader, or unavailable |
| load | a sequence number, a limit | up to the limit records above it, ascending, with the base; or a read error |
| head | | the committed head, or a read error |
| term | | the term this node writes the shard under now, or none |
| save snapshot | a committed sequence number, the state | committed, not leader, or unavailable |
| load snapshot | | the latest snapshot and its sequence number, or none; or a read error |
| put blob, get blob, has blob | a blob id, and the bytes for a put | done; the verified bytes or none; presence on any reachable replica; or unavailable |
| retain blobs, delete blobs | the blob ids to keep, for retain | nothing |

**Committed** means durable on the tier, **not leader** carries a hint naming the node believed to lead, **unavailable** carries a reason, and a **read error** is a local read that could not complete.

- **Append:** commits the batch right after the given head. Not leader MUST be returned only when no replica can hold the batch, so it provably did not commit. Unavailable is ambiguous (§5).
- **Load:** a local, unfenced read on the leader; the base is consistent with the records (§9.2).
- **Head:** the rehydration barrier (§6.2); it leaves the records and best snapshot in the local store for later loads.
- **Term:** a local read. Regained leadership always has a higher term, so no other node appended to the shard between two reads returning the same term.
- **Save snapshot:** not leader off the leader (§9.1).
- **Blobs:** unordered and unfenced (§10); a put returns once durable, and retain and delete are best-effort.
- **Caller:** the journal MAY assume its caller appends only on the shard's leader, one append at a time per entity, after the entity's known head (entity §6).
- **Local tier:** one linearizable store. An append commits once that store has made it durable, the term is 0, not leader never arises, and unavailable arises only from a failed store.
- **Quorum tier:** a per-entity quorum append over the replica set, fenced by the term (§6), surviving the loss of any minority of the set.

### 3.2 The store

A plug point below the journal (the caller), supplied by the deployment for each pair of entity type and node. On the quorum tier the leader reaches every replica's store through the replica protocol (§3.3).

```rust
pub trait EntityStore: Send + Sync + 'static {
    fn put_blob(&self, shard: u32, entity: &EntityName, id: BlobId, bytes: Vec<u8>) -> BlobAck;
    fn get_blob(&self, shard: u32, entity: &EntityName, id: BlobId) -> Option<Vec<u8>>;
    fn has_blob(&self, shard: u32, entity: &EntityName, id: BlobId) -> bool;
    fn delete_blob(&self, shard: u32, entity: &EntityName, id: BlobId);
    fn delete_blobs(&self, shard: u32, entity: &EntityName);
    fn retain_blobs(&self, shard: u32, entity: &EntityName, retain: &BTreeSet<BlobId>);
    fn blob_ids(&self, shard: u32, entity: &EntityName) -> Vec<BlobId>;
    fn store_record(&self, shard: u32, entity: &EntityName, after: Seq, term: Term, records: Vec<Vec<u8>>, kind: WriteKind) -> StoreAck;
    fn read(&self, shard: u32, entity: &EntityName) -> ReadReply;
    fn head(&self, shard: u32, entity: &EntityName) -> Seq;
    fn snapshot(&self, shard: u32, entity: &EntityName) -> Option<(Seq, Vec<u8>)>;
    fn read_from(&self, shard: u32, entity: &EntityName, from: Seq, limit: usize) -> RecordPage;
    fn failed(&self) -> bool;
    fn prepare(&self, shard: u32, entity: &EntityName, term: Term) -> ReadOutcome;
    fn store_snapshot(&self, shard: u32, entity: &EntityName, at: Seq, term: Term, state: Vec<u8>, kind: WriteKind) -> StoreAck;
    fn truncate(&self, shard: u32, entity: &EntityName, after: Seq, term: Term);
    fn entities(&self, shard: u32) -> Vec<EntityName>;
    fn seal_range(&self, shard: u32, from: u64);
    fn unseal(&self, shard: u32);
    fn remove_range(&self, shard: u32, from: u64);
    fn drop_shard(&self, shard: u32);
    fn shard_bytes(&self, shard: u32) -> u64;
}
```

The signature's vocabulary:

- `WriteKind`: `Append` (a leader's append), `Repair` (a recovery write-back), or `Transfer` (a split or merge copy, §8).
- `StoreAck`: `Stored` or `Stale` (each with the replica's head), `Fenced` (with the store's fence), `Sealed`, or `Failed`.
- `BlobAck`: `Stored` or `Failed`. `ReadOutcome`: `Prepared` with a `ReadReply`, `Fenced`, or `Failed`.
- `ReadReply`: every occupied slot (sequence number, term, bytes), ascending, and the latest snapshot (sequence number, term, state). `RecordPage`: the base and the records above both `from` and the base, ascending.
- `head` and `snapshot` answer what `read` would, and default to computing it; `failed` defaults to false. `entities` lists every entity a shard holds anything for, `blob_ids` one entity's blobs, and `shard_bytes` estimates a shard's stored bytes (§8). `delete_blob` evicts one blob, `retain_blobs` keeps only the listed ones, and `delete_blobs` drops the whole area.

The store's side:

- **Durable on return:** a call's effect has happened when it returns, and a store MUST acknowledge a write (`Stored`) only once it is durable.
- **The fence:** a store stamps each record with its write's term and MUST refuse any record or snapshot write, except a `Transfer`, stamped below its fence for the shard (`Fenced`, carrying the fence).
- **Recovery reads fence:** `prepare` MUST record its promise not to accept a lower term before returning the replica's slots and snapshot, and answers `Fenced` if its fence is higher. `read` does not fence and MAY include records not yet stable, which a caller MUST NOT count toward a quorum.
- **Write kinds:** an `Append` MUST be refused as `Stale` when `after` is below the base or a target slot holds anything but a re-delivery of the same append (same term and bytes). A `Repair` fills slots by highest term and never reports `Stale`. A caller MUST NOT use a `Transfer` outside a split or merge transfer (§8). No kind overwrites a slot holding an equal or higher term.
- **Term-bounded rollback:** `truncate` MUST drop only records past `after` whose term is at most `term` (§5).
- **The append bound:** `seal_range` MUST durably and monotonically refuse every `Append` whose name hash is at or above the bound, at any term (`Sealed`), while admitting repairs and transfers. `unseal`, `remove_range`, and `drop_shard` apply a committed split or merge.
- **Failure is a refusal:** a store that cannot make a write durable MUST answer `Failed` (to a record or snapshot write, a `prepare`, or a blob put) and MUST then report `failed` as true, after which its unfenced reads are not trusted (§6.2, §11). The condition SHOULD be one-way and store-wide.
- **One store per type:** a store MUST NOT be shared across entity types (§6.1).
- **A stamp at rest:** a store whose records outlive their writer MUST carry an `entity.store` stamp naming the deployment codec (§12), admitted on open before any other state is read (compatibility §3.2), so a codec change is one refusal naming both codecs. A store with no stamp is fresh and records the running codec.

The caller's side: a caller MUST read every refusal and MUST NOT treat a write whose outcome it has not read as stored.

### 3.3 The replica protocol

A boundary between a shard's leader (the sender) and each replica's store (the receiver), over actor messaging (actor §7). Each message carries one store call's arguments (§3.2) and runs it on the receiver's store for the message's entity type:

| Manifest | Runs | Reply |
|---|---|---|
| `entity.StoreRecord` | `store_record` | its `StoreAck` |
| `entity.Prepare` | `prepare` | its `ReadOutcome` |
| `entity.StoreSnapshot` | `store_snapshot` | its `StoreAck` |
| `entity.SealRange` | `seal_range` | empty, once the bound is durable |
| `entity.PutBlob` | `put_blob` | its `BlobAck` |
| `entity.GetBlob` | `get_blob` | the bytes, or none |
| `entity.HasBlob` | `has_blob` | presence |
| `entity.SweepBlobs` | `retain_blobs`, or `delete_blobs` when no ids are given | empty |
| `entity.ListEntities` | `entities` | the entity names |
| `entity.ListBlobs` | `blob_ids` | the blob ids |

- Both ends MUST use these manifests. A replica MUST answer each message by running the named call and replying with its outcome, so it acknowledges exactly what its store made durable.
- A message with no reply counts as no acknowledgement and as possibly applied: a timed-out message MAY still land.
- The leader's own replica, reached locally, counts toward a quorum like any other.
- A leader MAY act on a quorum before every reply arrives, but MUST still await each message it sent to its reply or failure (actor §8.1).

### 3.4 Blocking I/O

A plug point deciding where a node's blocking store writes run, one provider for every type the node hosts (entity §3.2). Its one operation, `submit`, takes a job (one store call) and returns `true` if the job will run, `false` only if it never will.

- **Off the consensus path:** a store write that blocks on a device MUST NOT run on a task that drives the node's consensus heartbeats or quorum waits. A deployment whose store writes to a device SHOULD supply a provider that runs jobs on threads of its own.
- **Writes are submitted:** every store write (record and snapshot writes, `prepare`, rollbacks, seals, range and shard removals, blob puts and reclamation) MUST be submitted to the provider, and its caller MUST await the result before proceeding. Reads MAY run on the calling task.
- **Where, not when:** the provider decides only where a job runs: it orders no jobs and leaves the order of fence check, apply, and acknowledgement inside a store call unchanged.
- Under the deterministic profile (actor §11) the provider runs each job on the calling task.

---

## 4. Shards

### 4.1 The partition, the leader-election group, and placement

The name hash MUST agree on every node and across runs, placing each name in exactly one shard of its type. The founding partition divides the space into a deployment-chosen number of near-equal ranges and serves only while the shard map bootstraps; then the map's committed ranges, which start equal to it and change only by split or merge (§8), place names.

Each shard's leader-election group takes its voters from the replica set the shard map records (§7), joined by the target set during a migration, and its leader hosts the range's entities (entity §5.2). A cluster runs one such group per shard plus one map group per type (§7), bounded by split and merge (§8): never one per entity, and none on the data path (§5, **E7**).

**Replica placement SHOULD be failure-domain aware.** A deployment that knows its failure domains SHOULD declare them, and the shard map then places at most ceil(R / D) of a shard's replicas in any of the D domains, filling from the other nodes when the layout cannot meet that cap. The node-to-domain mapping MUST be the same on every node: static deployment metadata qualifies, anything node-local does not.

### 4.2 Consensus layers and cost

Three consensus layers run, none touching entity data: the cluster's control plane (one group, cluster §5.4.3) owns membership and seeds the map groups' voters; the shard map (one group per type, §7) owns each shard's range and replica set; the leader-election groups (one per shard) own each shard's leader and term. Group identifiers MUST NOT collide across the layers. The total, O(shards) plus O(entity types) plus one, is bounded by split and merge (**E7**).

No entity write or activation writes to any consensus group (**E9**), and activating an entity takes no consensus round and costs at most one quorum head recovery (**E8**).

---

## 5. Records: ordering and quorum durability

The leader, its shard's single writer (§6.1), assigns each record the next sequence number with no agreement round. On the quorum tier the journal sends the batch to the replica set and reports it committed once a write quorum has stored it (joint during a migration, §7). An entity's records are totally ordered among themselves and unordered against other entities': entities sharing a shard share its leader and replicas, not a serialization point, and append concurrently, each on its own quorum.

**Idempotent by position:** an unavailable append (§3.1), such as one not acknowledged within the quorum deadline (§11), MAY still commit later, at its own sequence numbers. It never applies twice: the host neither retries an append nor folds anything but a contiguous commit (entity §6), and recovery (§6.2) finds each record at its one sequence number. A record's sequence number is its identity; no de-duplication token exists.

**The rollback is term-bounded:** on any append outcome but committed, the leader MUST roll back its own replica's copy of the batch, and MUST drop only records written at or below the appending term: records a newer term wrote above the same head survive (**E14**).

---

## 6. The fence and recovery

### 6.1 The term

The fence is the shard's term: at most one leader is elected per term (§2), and each store refuses fenced writes below its fence (§3.2). A write therefore commits only while a quorum still accepts its term: a deposed leader's in-flight appends reach no quorum, two nodes that both believe they lead cannot both commit, and a record never forks (**E1**). A host whose append is refused for leadership MUST step down and MUST NOT keep serving an entity whose shard it no longer leads.

**The persisted fence MUST be keyed by the shard's whole identity,** its entity type and index, since shards of two types with one index have independent terms.

Leadership is a quorum fact, never a clock reading: a leader that pauses and wakes finds its appends refused under a higher term.

### 6.2 Recovery

Before a leader serves an entity under its term, it MUST recover the entity's head through the journal's head operation (§3.1):

- **A head recovered under a confirmed quorum MUST contain every record any earlier term reported committed, at its original sequence number and bytes** (**E14**), and MUST be durable on a write quorum under the recovering term before the first append. Where replicas disagree at a position, the higher term wins: a partitioned replica MAY hold a stale-term record at a position a higher term later took.
- **Short of a confirmed quorum** within the recovery deadline (§11), the leader MAY adopt the merged view of the replies it has, without write-back. That view MAY be stale and MUST NOT be able to commit a write: every append still needs a write quorum, and an up-to-date replica refuses an append onto a position it holds differently or below its base (§3.2).
- Recovery MUST fail as unavailable while the shard is electing, when a store has fenced the node behind a higher term, when the local store has failed (§11), and when a confirmed recovery's write-back reaches no quorum.
- A split or merge transfer (§8) and a migration catch-up (§7) MUST use a confirmed quorum, never the fallback.

### 6.3 Failover

**Planned departure:** a node shut down deliberately SHOULD first hand each shard it leads, for every type it hosts, to a caught-up replica rather than drop leadership (cluster §5.4.3). The handoff is best-effort: the node MUST NOT block its shutdown waiting for one.

On an unplanned loss the replicas elect a new leader under a higher term, which recovers each entity on first access (§6.2). During the failover window calls fail fast with not leader (which the caller's redirect absorbs, entity §5.4) or unavailable, rather than block. No acknowledged write is lost (**E14**).

---

## 7. The shard map and replica-set migration

The shard map records no leader. It MUST be consensus-agreed: every node resolves a name to the same replica set regardless of join order. Each type's map is one consensus group (cluster §3.5) whose voters are seeded from the leader-based control plane's (cluster §5.4.3), which is REQUIRED for the quorum tier.

Each shard's committed replica set follows the rendezvous choice over the current cluster voters (utilities §4.1), spread across failure domains (§4.1). A new replica MUST NOT stand in for a member of the current majority before it holds each entity's committed records and latest snapshot (**E14**). A changed choice runs a migration:

1. The new set commits as the shard's target, beside the current set. From then on every append, snapshot, and recovery MUST use the joint quorum, sent to the union of both sets, and nothing further is proposed for the shard until the migration ends.
2. The leader catches every entity up on the target: it enumerates the shard's entities from a read quorum of the current set, recovers each under the joint confirmed quorum (whose write-back lands its records on the target), saves the best snapshot again on the joint quorum, and copies the blob area (§10). Every step is idempotent, and a new leader re-drives an unfinished migration.
3. The leader MUST NOT commit the flip to the target before a complete pass. Departing replicas then stop hosting the shard, and a not-leader hint is followed only while the committed map still lists the hinted node.

**A committed map command a node cannot decode MUST stop that node's map, never be skipped** (compatibility **V2**). The node freezes at the last command it applied, hands off the shards it leads, stops proposing, keeps acknowledging map-group appends, and reports the reason to its operator, permanently. Commands are stamped (§12), so another version is refused by version.

The map changes only on splits, merges, reconfiguration, and membership changes, never on an entity write or activation (**E9**).

---

## 8. Split and merge

A shard that grows too large or hot splits: the parent keeps the low half of its range, and a new child shard (a new index, so a new leader-election group) takes the high half on the parent's replicas. A merge is the reverse: the left of two adjacent shards absorbs the right, whose group retires. A split MUST be atomic with respect to writes, in this order:

1. **Seal:** a majority of the parent's replicas durably record an append bound at the boundary (§3.2), so no append to the moving range can assemble a write quorum at any term, even from a leader unaware of the split. A leader aware of it refuses the append before any store attempt (not leader); a store's refusal is ambiguous and surfaces as unavailable. Either way the caller re-resolves against the committed map.
2. **Transfer:** for each moved entity the parent's leader recovers the head under a confirmed quorum (§6.2) and copies the records and latest snapshot to the child's keys on a majority of the child's replicas, as unfenced transfer writes (§3.2). The blob area is copied best-effort (§10).
3. **Commit:** the new mapping commits only after a complete transfer pass; no node routes the moved range to the child before it.

No write is lost or duplicated, and no entity is writable in two shards at once (**E15**). Every step is idempotent and derived from committed map state, so a crash or a leader change re-drives it. A merge mirrors the split, driven by the right shard's leader, whose term fences the right shard's deposed leaders: seal the whole right range, transfer its entities into the left's keys, commit (**E7**). Splits, merges, and migrations run one at a time per shard, and each proposal is validated against committed state alone, so a raced one is a no-op on every node.

A split or merge is requested through the entity API (entity §3.2); a deployment MAY also enable a size trigger, under which a shard's leader requests a split once the shard's stored bytes pass a deployment-chosen threshold.

---

## 9. Snapshots and compaction

### 9.1 Snapshots and rehydration

After a commit the host MAY save a snapshot at the head, on the deployment's snapshot trigger. Only the shard's leader saves snapshots, one trigger governs every snapshot, the idle one included (entity §8), and on the quorum tier a snapshot goes to every replica.

**Rehydration:** before reading an entity's head a leader MUST pass the recovery barrier (§6.2), then reads the snapshot the barrier selected and replays the later records, up to the recovered head, through the entity's fold (entity §4.1) and the facets' folds (facets §4.1). The head comes only from journal and snapshot returns (**E3**), so an entity evicted on idle re-activates with identical state (**E12**).

**A snapshot MUST NOT shorten the effective log** (**E4**). No snapshot can outrun the head (§2). The host MUST still refuse a snapshot beyond the recovered head and replay from the journal, and a replay whose cursor is below the store's base MUST fail the activation rather than fold over the gap.

### 9.2 Compaction

Storing a snapshot MAY drop the records it subsumes and advance the entity's base. Every load page MUST report the base, and every event read carries it to its caller (entity §7.2). A snapshot is taken only at a committed head, and a replica MUST drop a prefix only if it holds a snapshot covering it, so any recovery quorum finds every committed record or a snapshot subsuming it (**E14**). A replica MAY therefore compact before the snapshot is durable on a quorum. Each entity compacts independently.

---

## 10. Blobs

A blob operation carries no sequence number and no term, and recovery does not repair blobs: a blob id names one byte string forever, so writers cannot conflict and a reader verifies by re-hashing. An entity reaches its blob area through its context (entity §3.1) with six operations: put (returning the blob id), get (the whole blob or a byte range), has, sweep, repair facet roots, and destroy.

- **Addressing:** a blob is write-once and stored once per entity, however often it is put. It carries no format stamp, its name being its hash (compatibility §3.5).
- **Verification on read:** every get MUST re-hash the bytes before returning them and MUST NOT return them on a mismatch; on the quorum tier it falls through to the next replica and is unavailable only if none verifies (**E17**).
- **Durability:** a put writes the local replica and the peers and is acknowledged once a write quorum including the leader's own copy has stored the bytes (on the local tier, the one store), with no election, term, or agreement round (**E18**). Short of a quorum it is unavailable, and a retry is idempotent. Has reports presence (§3.1), not durability.
- **Reclamation is entity-scoped and root-driven:** a sweep drops every blob outside the union of the given ids and the facets' roots (facets §4.5), and destroy drops the whole area. Only referenced blobs are fetched or re-replicated, so a delete a partitioned replica misses leaves orphan bytes for a later sweep, never a resurrected referenced blob, and no tombstone exists.
- **Lazy hydration and self-heal:** a get that misses locally fetches a verifying copy from a peer and keeps it, so a re-activation pulls only what it touches; a corrupt local copy is evicted, then replaced. Repair facet roots puts each facet-root blob again to the current replica set, best-effort.
- **Replicas joining a shard receive the blobs:** before a migration flips (§7) the leader MUST copy each entity's blob area, verified (**E17**), to every target replica lacking any of it; an unreachable target fails the pass. A split or merge copies blobs best-effort, and a replica still lacking one fetches it on access.

---

## 11. Failure model

A failure's blast radius is the shard.

**A replica whose storage fails withdraws; it does not crash.** A replica that cannot make a write durable MUST NOT acknowledge it and MUST report the refusal (§3.2) rather than terminate, and a failed answer MUST NOT count toward any quorum. The node drops out of its quorums: writes commit on the other replicas, and the shard elects around a failed leader. The refusal MUST be distinguishable from a fenced or stale refusal, which a healthy replica decided.

**Quorum loss is unavailability, not a fork** (**E11**). A shard that cannot reach a quorum commits nothing: an append is unavailable and the activation steps down (entity §6). A minority cannot commit (§6.1), so the shard never forks, and only its entities are affected. Reads stay read-your-leader (entity §7.1).

**Deadlines:** a deployment chooses a **quorum deadline**, bounding an append, a snapshot, a blob put, and every other replica-protocol message outside recovery, and a **recovery deadline**, bounding a recovery's fenced reads and its write-back. A quorum-deadline expiry on a write is ambiguous: the write MAY still commit (§5). A recovery-deadline expiry leaves no caller's write in doubt: a replica that has not replied counts toward no quorum, so the recovery adopts the fallback view or fails as unavailable as §6.2 prescribes, and a write-back that lands late re-writes only records the recovered view already held.

---

## 12. Registries

Each format follows the compatibility convention (compatibility §3) and is listed, with its version, in its registry (compatibility §4). The manifests this spec owns are the replica protocol's (§3.3).

| Format | Stamp | Holds |
|---|---|---|
| `entity.record` | the high bit of the envelope's first byte | one record |
| `entity.snapshot` | `GRSNAP`, ahead of the composite | one composite snapshot |
| `entity.store` | `GRSTOR`, once per store | the codec of the store's records |
| `entity.store.manifest` | the log header's record schema | a file-backed store's entity index |
| `entity.store.segment` | the log header's record schema | one entity's records and snapshot, file-backed |
| `entity.store.fence` | `GRFNCE`, per shard | a shard's fence |
| `entity.store.seal` | `GRSEAL`, per shard | a shard's append bound |
| `entity.shardmap` | `GRSMAP`, per committed command | one shard-map command |

- **The record envelope** is `[tag][payload]`. A tag is in `0x00..=0x7F` and permanent: `0x00..=0x3F` belong to the built-in facets (facets §5) and `0x40..=0x7F` to facets defined elsewhere. A first byte at or above `0x80` marks another envelope version, and a reader MUST refuse it by version, not as an unknown tag (facets §4.1). A payload is versioned by the format that carries it (compatibility §3.3).
- **The composite snapshot**'s magic and version are read before its body (compatibility §3.2). The body records the codec of facet 0's state, its one codec-dependent part; a mismatch MUST name both codecs and abort the activation. Facet 0's state is inline or stored as chunks in the entity's blob area, named in order by the composite; the chunk puts MUST complete before the snapshot naming them is saved.
- **A shard-map command** carries its `entity.shardmap` stamp inside the consensus log entry that holds it (compatibility §3.3).

---

## 13. Invariants defined here

This spec defines, wholly or in part, these rows of the entity catalogue (entity §12), which holds their statements: **E1** (§6.1), **E3** (§9.1), **E4** (§9.1), **E7** (§4.1, §5, §7, §8), **E8** (§4.2), **E9** (§7), **E11** (§11), **E12** (§9.1), **E14** (§6, §6.3), **E15** (§8), **E17** (§10), and **E18** (§10).

---

## 14. Security considerations

- **Trust:** storage trusts a shard's replicas. The fence and the append bound stop a deposed or out-of-date leader, not a hostile peer: a node that can deliver replica-protocol messages can write or reclaim entity state on the receiver. The protocol inherits the cluster's transport security, peer authentication, and per-manifest authorization (cluster §11), and the actor model's deserialization allowlist (actor §14).
- **Integrity at rest:** every blob read is verified against its id (**E17**); every other durable format is stamped and refused by version (§12, compatibility §3.1). A node's store is trusted local state, not authenticated.
- **Availability:** a failed store withdraws its node from its quorums without crashing it (§11), and quorum loss pauses only the shard's own entities (**E11**).
