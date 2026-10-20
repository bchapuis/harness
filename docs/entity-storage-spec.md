# Entity Storage: Specification

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, and **MAY** carry the meanings defined in RFC 2119.

A `§` with no document name refers to this spec. `entity §N` is [`entity-spec.md`](entity-spec.md), `facets §N` is [`entity-facets-spec.md`](entity-facets-spec.md), `actor §N` is [`distributed-actor-spec.md`](distributed-actor-spec.md), `utilities §N` is [`cluster-utilities-spec.md`](cluster-utilities-spec.md), and `compatibility §N` is [`compatibility-spec.md`](compatibility-spec.md).

---

## 1. Scope

Entity storage is the durable substrate an entity's host writes through, and the only layer with a fence, a quorum, and a recovery protocol. It has two replication primitives: per-entity ordered, term-fenced records (§5) and immutable, content-addressed blobs (§10). Shards place records (§4); consensus elects each shard's leader, while each entity's records replicate by a per-entity quorum append with no consensus on the data path. Snapshots bound replay (§9). Storage addresses an entity only by its name (entity §2) and never interprets its state or events.

It builds on the actor framework's consensus groups (cluster §3.5, §5.4.3) and messaging (actor §7) and on the compatibility convention (compatibility §3, §4), and its components follow the deterministic profile (actor §11). The entity runtime (entity §3.3, §6) builds on it, and so do the facets (facets §4), every durable feature beyond records and blobs being a facet that interprets them.

### 1.1 Non-goals

- **A shared log.** No cluster-wide or shard-wide log orders records (§5).
- **Consensus per entity, or on the data path.** Consensus places shards and elects their leaders; a record needs only a quorum append (§4.2).
- **A transport of its own.** The replica protocol rides actor messaging (§3.3).
- **Write availability under quorum loss** (§11).

### 1.2 Conformance classes

- A *journal* conforms by meeting §3.1 and §5 to §11 for its durability tier.
- A *store* conforms by meeting §3.2, and §12 for each format it writes.
- A *replica* (a node answering the replica protocol) conforms by meeting §3.3.
- A *blocking I/O provider* conforms by meeting §3.4.
- A *host* (entity §2), as the journal's caller, conforms by meeting the caller obligations of §3.1, §6.1, and §9.1.
- A *deployment* conforms by meeting the deployment obligations of §3.2, §3.4, §4.1, §6.3, §7, and §11.

---

## 2. Model

- **Record:** an opaque byte string at one position of an entity's journal, wearing the record envelope (§12).
- **Sequence number:** a record's position in its entity's total order. The first record commits at 1; 0 is the empty head.
- **Batch:** the records of one append, committed as one atomic entry at consecutive sequence numbers.
- **Snapshot:** an entity's composite state (facets §4.4) at a committed sequence number, subsuming every record at or below it.
- **Head:** the best snapshot's sequence number (0 without one) plus the length of the contiguous run of records above it.
- **Base:** the sequence number at or below which compaction has dropped an entity's records, which then live only in its snapshot; 0 when nothing is compacted.
- **Blob:** an immutable byte string, named by its **blob id**, the 32-byte BLAKE3 digest of its bytes. An entity's **blob area** holds its blobs, each addressed by the entity's name and the blob id.
- **Name hash:** a stable hash of an entity name onto a 64-bit space.
- **Shard:** a contiguous range of one entity type's name-hash space, identified by the entity type and an index.
- **Replica set:** the R nodes holding a shard's records, snapshots, and blobs, R being the deployment's replication factor.
- **Leader-election group:** the consensus group of a shard's replicas that elects one leader per term. It holds no entity data.
- **Leader:** the node elected for the current term: the shard's only writer and the host of all its entities.
- **Term:** the leader-election group's term. It only grows, and every write a leader issues carries it.
- **Durability tier:** the **local tier** writes one store and never elects; the **quorum tier** writes the shard's replica set under the term. The hosting system fixes the tier (entity §3.3).
- **Write quorum** and **read quorum:** a majority of the replica set; during a migration, a **joint quorum** is a majority of each of the current and target sets.
- **Confirmed quorum:** a read quorum each of whose members has promised the recovering term (§3.2).
- **Journal:** an entity's records, snapshots, and blob area as its host sees them (§3.1).
- **Store:** one node's durable store of one entity type's records, snapshots, and blobs, keyed by shard and entity (§3.2).
- **Fence:** the highest term a store has accepted for a shard, by a fenced write or a recovery promise.
- **Append bound:** a per-shard name-hash bound at or above which a store refuses appends at any term (§8).
- **Shard map:** the per-type, consensus-agreed record of each shard's range, replica set, and **target** set during a **migration** to new replicas (§7).
- **Split** and **merge:** dividing a shard's range with a new shard, and fusing two adjacent shards (§8).
- **Recovery:** establishing an entity's head under a leader's term before serving it (§6.2).
- **Quorum deadline** and **recovery deadline:** deployment-chosen bounds on waiting for a write quorum and for a recovery read quorum (§11).

---

## 3. Seams

### 3.1 The journal

A boundary between the entity host (the caller, entity §2) and storage, one journal per shard a node replicates. Every operation names one entity.

| Operation | Takes | Yields |
|---|---|---|
| append | the head it follows, a batch | committed with the new head, not leader with a hint, or unavailable |
| load | a sequence number, a limit | up to the limit records above it, ascending, with the base; or a read error |
| head | | the committed head, or a read error |
| term | | the term this node writes the shard under now, or none |
| save snapshot | a committed sequence number, the state | committed, not leader, or unavailable |
| load snapshot | | the latest snapshot and its sequence number, or none; or a read error |
| put blob, get blob, has blob | a blob id, and the bytes for a put | done; the verified bytes or none; presence on any reachable replica; or unavailable |
| retain blobs, delete blobs | the blob ids to keep, for retain | nothing |

**Committed** means durable on the tier, **not leader** carries a hint naming the node believed to lead, **unavailable** carries a reason, and a **read error** is a local read that could not complete.

- **Append:** commits the batch as one atomic entry immediately after the given head. Not leader MUST be returned only when no replica can hold the batch, so the batch provably did not commit. Unavailable is ambiguous: the batch MAY commit later, at its own sequence numbers (§5).
- **Load:** a local, unfenced read on the leader; the base is read consistently with the records (§9.2).
- **Head:** the rehydration barrier. It returns the committed head per §6.2 and leaves the records and best snapshot in the local store, so later loads read locally.
- **Term:** a local read. A node regains leadership only under a higher term, so between two reads returning the same term no other node appended to the shard.
- **Save snapshot:** persists a snapshot at a committed sequence number (§9.1); not leader off the leader.
- **Blobs:** unordered and unfenced (§10); a put returns once durable, and retain and delete are best-effort.
- **Caller:** the journal MAY assume its caller appends only on the shard's leader, one append at a time per entity, after the entity's known head (entity §6).
- **Local tier:** one linearizable store. An append commits once that store has made it durable, the term is 0, not leader never arises, and unavailable arises only from a failed store.
- **Quorum tier:** a per-entity quorum append over the shard's replica set, fenced by the term (§6), surviving the loss of any minority of the set.

### 3.2 The store

A plug point between the journal (the caller) and one node's durable storage. A deployment supplies the store for each pair of entity type and node. On the quorum tier the leader reaches every replica's store, its own included, through the replica protocol (§3.3).

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
- `StoreAck`: `Stored` (durable, with the replica's head), `Fenced` (with the store's higher fence), `Stale` (an append onto a stale head, with the replica's head), `Sealed` (refused by the append bound), or `Failed`.
- `BlobAck`: `Stored` or `Failed`. `ReadOutcome`: `Prepared` with a `ReadReply`, `Fenced`, or `Failed`.
- `ReadReply`: every occupied slot (sequence number, term, bytes), ascending, and the latest snapshot (sequence number, term, state). `RecordPage`: the base and the records above both `from` and the base, ascending.
- `head` and `snapshot` answer what `read` would, and default to computing it; `failed` defaults to false. `entities` lists every entity a shard holds anything for, `blob_ids` one entity's blobs, and `shard_bytes` estimates a shard's stored bytes (§8). `delete_blob` evicts one blob, `retain_blobs` keeps only the listed ones, and `delete_blobs` drops the whole area.

The store's side:

- **Durable on return:** a call's effect has happened when it returns. A store MUST acknowledge a write (`Stored`) only once it is durable.
- **The fence:** a store stamps each record with the term it was written under. It MUST refuse any record or snapshot write stamped below its fence for the shard (`Fenced`, carrying the fence), except a `Transfer`.
- **Recovery reads fence:** `prepare` MUST record its promise not to accept a lower term before returning the replica's slots and snapshot, and answers `Fenced` if its fence is higher. `read` does not fence and MAY include records not yet stable, which a caller MUST NOT count toward a quorum.
- **Write kinds:** an `Append` MUST be refused as `Stale` when `after` is below the base or a target slot holds anything but a re-delivery of the same append (same term and bytes). A `Repair` fills slots by highest term and never reports `Stale`. A `Transfer` skips the fence, and a caller MUST NOT use it except to land a split or merge transfer (§8). Every kind keeps a slot that already holds an equal or higher term.
- **Term-bounded rollback:** `truncate` MUST drop only records past `after` whose term is at most `term` (§5).
- **The append bound:** `seal_range` MUST durably and monotonically refuse every `Append` whose name hash is at or above the bound, at any term (`Sealed`), while admitting repairs and transfers. `unseal`, `remove_range`, and `drop_shard` apply a committed split or merge.
- **Failure is a refusal:** a store that cannot make a write durable MUST answer `Failed` (to a record or snapshot write, a `prepare`, or a blob put) and MUST then report `failed` as true, after which its unfenced reads are not trusted (§6.2, §11). The condition SHOULD be one-way and store-wide.
- **One store per type:** a store MUST NOT be shared across entity types (§6.1).
- **A stamp at rest:** a store whose records outlive their writer MUST carry an `entity.store` stamp naming the deployment codec (§12), admitted on open before any other state is read (compatibility §3.2), so a codec change is one refusal naming both codecs. A store with no stamp is fresh and records the running codec.

The caller's side: a caller MUST read every refusal, and MUST NOT treat a write whose outcome it has not read as stored.

### 3.3 The replica protocol

A boundary between a shard's leader (the sender) and each replica's store (the receiver), over actor messaging (actor §7). Each message carries the arguments of one store call (§3.2) and runs it on the receiver's store for the message's entity type:

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
- The leader reaches its own replica locally, and that replica counts toward a quorum like any other.
- A leader MAY act on a quorum before every reply arrives, but MUST still await each message it sent to its reply or failure (actor §8.1).

### 3.4 Blocking I/O

A plug point deciding where a node's blocking store writes run; a node uses one provider for every type it hosts (entity §3.2). Its one operation, `submit`, takes a job (one store call, as a closure) and returns `true` if the job will run, or `false` only if it never will.

- **Off the consensus path:** a store write that blocks on a device MUST NOT run on a task that drives the node's consensus heartbeats or quorum waits. A deployment whose store writes to a device SHOULD supply a provider that runs jobs on threads of its own.
- **Writes are submitted:** every store write (record and snapshot writes, `prepare`, rollbacks, seals, range and shard removals, blob puts and reclamation) MUST be submitted to the provider, and its caller MUST await the result before proceeding. Reads MAY run on the calling task.
- **Where, not when:** the provider decides only where a job runs. It imposes no order between jobs and leaves the order of fence check, apply, and acknowledgement inside a store call unchanged.
- Under the deterministic profile (actor §11) the provider runs each job on the calling task.

---

## 4. Shards

### 4.1 The partition, the leader-election group, and placement

Each entity type's names are partitioned into shards, each owning a contiguous range of the name-hash space. The name hash MUST agree on every node and across runs, and places each name in exactly one shard. The founding partition divides the space into a deployment-chosen number of near-equal ranges; only a split or merge changes it (§8). Once the shard map has committed a type's ranges, they place its names; the founding partition, which agrees with them until the first split or merge, serves only while the map bootstraps.

Each shard has a leader-election group of R replicas, which elects one leader per term and records the replica set. The leader is the single writer and host for every entity in the range (entity §5.2). A cluster runs one such group per shard plus one map group per type (§7), bounded by split and merge (§8): never one per entity, and none on the data path (§5, **E7**).

**Replica placement SHOULD be failure-domain aware.** A deployment that knows its failure domains SHOULD declare them. The shard map then places at most ceil(R / D) of a shard's replicas in any one of the D declared domains, filling from the remaining nodes when the layout cannot meet that cap. A deployment's mapping from node to domain MUST be the same on every node: static deployment metadata qualifies, anything node-local does not.

### 4.2 Consensus layers and cost

Three consensus layers run, none touching entity data: the actor control plane (one group, cluster §5.4.3) owns membership and seeds the map groups' voters; the shard map (one group per type, §7) owns each shard's range and replica set; the leader-election groups (one per shard) own each shard's leader and term. Group identifiers MUST NOT collide across the layers. The total is O(shards) plus O(entity types) plus one, bounded by split and merge (**E7**).

No entity write or activation writes to any consensus group (**E9**). Activating an entity takes no consensus round and costs at most one quorum head recovery (**E8**).

---

## 5. Records: ordering and quorum durability

The leader is the single writer of every entity in its shard (§6.1), so it assigns each record the next sequence number with no agreement round. On the quorum tier the journal sends the batch to the shard's replica set, the leader's own replica included, and reports it committed once a write quorum has stored it; during a replica-set migration the quorum is joint (§7). An entity's records are totally ordered among themselves and unordered against other entities': entities sharing a shard share its leader and replicas, not a serialization point, and their appends proceed concurrently, each on its own quorum.

**Idempotent by position:** an append not acknowledged within the quorum deadline is unavailable (§11) and MAY still reach a quorum later. It never applies twice: the host neither retries an append nor folds anything but a contiguous commit (entity §6), and recovery (§6.2) finds each record at its one sequence number. A record's sequence number is its identity; no de-duplication token exists.

**The rollback is term-bounded:** on any append outcome but committed, the leader MUST roll back its own replica's copy of the batch, and MUST drop only records written at or below the appending term: records a newer term wrote above the same head survive (**E14**).

---

## 6. The fence and recovery

### 6.1 The term

The fence is the shard's term: the leader-election group elects at most one leader per term, every write the leader issues carries that term, and each store refuses fenced writes below its fence (§3.2). A write therefore commits only while a quorum still accepts its term: a deposed leader's in-flight appends reach no quorum, two nodes that briefly both believe they lead cannot both commit, and a record never forks (**E1**). A host whose append is refused for leadership MUST step down and MUST NOT keep serving an entity whose shard it no longer leads.

**The persisted fence MUST be keyed by the shard's whole identity,** its entity type and index: shards of two types with one index belong to two groups whose terms advance independently.

Leadership is a quorum fact, never a clock reading: a leader that pauses and wakes finds its appends refused under a higher term, whatever the clock skew.

### 6.2 Recovery

Before a leader serves an entity under its term, it MUST recover the entity's head through the journal's head operation (§3.1):

- **A head recovered under a confirmed quorum MUST contain every record any earlier term reported committed, at its original sequence number and bytes** (**E14**), and MUST be durable on a write quorum under the recovering term before the first append. Where replicas disagree at a position, the higher term wins: a partitioned replica MAY hold a stale-term record at a position a higher term later took.
- **Short of a confirmed quorum** within the recovery deadline, the leader MAY adopt the merged view of the replies it has, without write-back. That view MAY be stale and MUST NOT be able to commit a write: every append still needs a write quorum, and an up-to-date replica refuses an append onto a position it holds differently or below its base (§3.2).
- Recovery MUST fail as unavailable while the shard is electing, when a store has fenced the node behind a higher term, when the local store has failed (§11), and when a confirmed recovery's write-back reaches no quorum.
- A split or merge transfer (§8) and a migration catch-up (§7) MUST use a confirmed quorum, never the fallback.

### 6.3 Failover

**A planned departure SHOULD hand leadership off rather than drop it.** A node shut down deliberately SHOULD first hand each shard it leads, for every type it hosts, to a caught-up replica (cluster §5.4.3). The handoff is best-effort, and a departing node MUST NOT block its shutdown waiting for one.

On an unplanned loss the replicas elect a new leader under a higher term, which recovers each entity on first access (§6.2). During the failover window calls fail fast, with not leader (which the caller's redirect absorbs, entity §5.4) or unavailable, rather than block. No acknowledged write is lost (**E14**).

---

## 7. The shard map and replica-set migration

The shard map records each shard's range and replica set, not its leader. The map MUST be consensus-agreed: every node resolves a name to the same replica set regardless of join order. Each type's map is one consensus group (cluster §3.5) whose voters are seeded from the leader-based control plane's (cluster §5.4.3). The leader-based control plane is REQUIRED for the quorum tier.

Each shard's committed replica set follows the rendezvous choice over the current cluster voters (utilities §4.1), spread across failure domains (§4.1). A new replica MUST NOT stand in for a member of the current majority before it holds each entity's committed records and latest snapshot (**E14**). A changed choice runs a migration:

1. The new set commits as the shard's target, beside the current set. From then on every append, snapshot, and recovery MUST use the joint quorum, sent to the union of both sets. Nothing further is proposed for the shard until the migration ends.
2. The shard's leader catches every entity up on the target: it enumerates the shard's entities from a read quorum of the current set, recovers each under the joint confirmed quorum (whose write-back lands its records on the target), saves the best snapshot again on the joint quorum, and copies the blob area (§10). Every step is idempotent, and a new leader re-drives an unfinished migration.
3. The leader MUST NOT commit the flip to the target before a complete pass. Departing replicas then stop hosting the shard, and a not-leader hint is followed only while the committed map still lists the hinted node.

**A committed map command a node cannot decode MUST stop that node's map, never be skipped** (compatibility **V2**). The node freezes at the last command it applied, hands off the shards it leads, stops proposing, keeps acknowledging map-group appends, and reports the reason to its operator, permanently. Commands carry the `entity.shardmap` stamp (§12), so another version is refused by version.

The map changes only on splits, merges, reconfiguration, and membership changes, never on an entity write or activation (**E9**).

---

## 8. Split and merge

A shard that grows too large or hot splits: the parent keeps the low half of its range, and a new child shard (a new index, so a new leader-election group) takes the high half on the parent's replicas. Two adjacent shards merge the reverse way, the left absorbing the right, whose group retires. A split MUST be atomic with respect to writes, in this order:

1. **Seal:** a majority of the parent's replicas durably record an append bound at the boundary (§3.2), so no append to the moving range can assemble a write quorum at any term, even from a leader that has not learned of the split. A leader that knows of it refuses the append before any store attempt (not leader); a store's refusal is ambiguous and surfaces as unavailable. Either way the caller re-resolves against the committed map.
2. **Transfer:** for each moved entity the parent's leader recovers the head under a confirmed quorum (§6.2) and copies the records and latest snapshot to the child's keys on a majority of the child's replicas, as unfenced transfer writes (§3.2). The blob area is copied best-effort (§10).
3. **Commit:** the new mapping commits only after a complete transfer pass; no node routes the moved range to the child before it.

No write is lost or duplicated, and no entity is writable in two shards at once (**E15**). Every step is idempotent and derived from committed map state, so a crash or a leader change re-drives it. A merge mirrors the split, driven by the right shard's leader, whose term fences the right shard's deposed leaders: seal the whole right range, transfer its entities into the left's keys, commit (**E7**). Splits, merges, and migrations run one at a time per shard, and every proposal is validated against committed state alone, so a raced one is a no-op on every node.

A split or merge is requested explicitly through the entity API (entity §3.2). A deployment MAY also enable a size trigger, under which a shard's leader requests a split once the shard's stored bytes pass a deployment-chosen threshold.

---

## 9. Snapshots and compaction

### 9.1 Snapshots and rehydration

After a commit the host MAY save a snapshot of the composite state (facets §4.4) at the head, on the deployment's snapshot trigger. Only the shard's leader saves snapshots, and one trigger governs every snapshot, the idle one included (entity §8). On the quorum tier a snapshot is sent to every replica.

**Rehydration:** before reading an entity's head a leader MUST pass the recovery barrier (§6.2). It then reads the snapshot the barrier selected and replays the later records, up to the recovered head, through the entity's fold (entity §4.1) and the facets' folds (facets §4.1). The head comes only from journal and snapshot returns (**E3**), so an entity evicted on idle re-activates with identical state (**E12**).

**A snapshot MUST NOT shorten the effective log** (**E4**). A head is measured from the best snapshot (§2), so no snapshot can outrun it. The host MUST still refuse a snapshot beyond the recovered head and replay from the journal, and a replay whose cursor is below the store's base MUST fail the activation rather than fold over the gap.

### 9.2 Compaction

Storing a snapshot MAY drop the records it subsumes and advance the entity's base. Every load page MUST report the base, and every event read carries it to its caller (entity §7.2). A snapshot is taken only at a committed head, and a replica MUST drop a prefix only if it holds a snapshot covering it, so any recovery quorum finds every committed record or a snapshot subsuming it (**E14**). A replica MAY therefore compact before the snapshot is durable on a quorum. Each entity compacts independently.

---

## 10. Blobs

An entity's bulk immutable bytes live in its blob area, on its shard's replica set. A blob operation carries no sequence number and no term, and recovery does not repair blobs: a blob id names one byte string forever, so writers cannot conflict and a reader verifies by re-hashing. An entity reaches its area through its context (entity §3.1) with six operations: put (returning the blob id), get (the whole blob or a byte range), has, sweep, repair facet roots, and destroy.

- **Addressing:** a blob is write-once and stored once per entity, however often it is put. It carries no format stamp: its name is its hash, checked on every read (compatibility §3.5).
- **Verification on read:** every get MUST re-hash the bytes before returning them and MUST NOT return them on a mismatch; on the quorum tier it falls through to the next replica and is unavailable only if none verifies (**E17**).
- **Durability:** a put writes the local replica and the peers and is acknowledged once a write quorum that includes the leader's own copy has stored the bytes (on the local tier, the one store), with no election, term, or agreement round (**E18**). Short of a quorum it is unavailable, and a retry is idempotent. Has reports presence on any reachable replica, not durability.
- **Reclamation is entity-scoped and root-driven:** a sweep drops every blob outside the union of the given ids and the facets' roots (facets §4.5), and destroy drops the whole area. Only referenced blobs are fetched or re-replicated, so a delete a partitioned replica misses leaves orphan bytes for a later sweep, never a resurrected referenced blob, and no tombstone exists.
- **Lazy hydration and self-heal:** a get that misses locally fetches a verifying copy from a peer and keeps it, so a re-activation pulls only what it touches; a corrupt local copy is evicted, then replaced. Repair facet roots puts each facet-root blob again, to the current replica set, best-effort.
- **Replicas joining a shard receive the blobs:** before a migration flips (§7) the leader MUST copy each entity's blob area to every target replica lacking any of it, verified (**E17**); an unreachable target fails the pass. A split or merge copies blobs best-effort, and a replica still lacking one fetches it on access.

---

## 11. Failure model

A failure's blast radius is the shard.

**A replica whose storage fails withdraws; it does not crash.** A replica that cannot make a write durable MUST NOT acknowledge it and MUST report the refusal rather than terminate: after an I/O failure its store answers failed to every record write, recovery read, and blob put (§3.2), and a failed answer MUST NOT count toward any quorum. The node drops out of its quorums, which the majority rules absorb: writes commit on the other replicas, and the shard elects around a failed leader. The refusal MUST be distinguishable from a fenced or stale refusal, which a healthy replica decided.

**Quorum loss is unavailability, not a fork** (**E11**). A shard that cannot reach a quorum commits nothing: an append is unavailable and the activation steps down (entity §6). A minority cannot commit (no leader is elected there, and a stale-term append is refused), so the shard never forks, and only its entities are affected. Reads stay read-your-leader (entity §7.1).

**Deadlines:** the quorum deadline bounds an append, a snapshot, and a blob put; the recovery deadline bounds a recovery read. A deployment chooses both, and every expiry is ambiguous: the write MAY still commit (§5).

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

- **The record envelope** is `[tag][payload]`. A tag is in `0x00..=0x7F` and permanent: tags `0x00..=0x3F` belong to the built-in facets (facets §5) and `0x40..=0x7F` to facets defined elsewhere. A first byte at or above `0x80` marks another envelope version, and a reader MUST refuse it by version, not as an unknown tag (facets §4.1). A payload is versioned by the format that carries it (compatibility §3.3).
- **The composite snapshot**'s magic and version are read before its body (compatibility §3.2). The body records the codec of facet 0's state, its one codec-dependent part; a mismatch MUST name both codecs and abort the activation. Facet 0's state is either inline or stored as chunks in the entity's blob area, named in order by the composite; the chunk puts MUST complete before the snapshot naming them is saved.
- **A shard-map command** carries its own stamp inside the consensus log entry that holds it (compatibility §3.3).

---

## 13. Invariants defined here

This spec defines, wholly or in part, these rows of the entity catalogue (entity §12), which holds their statements: **E1** (§6.1), **E3** (§9.1), **E4** (§9.1), **E7** (§4.1, §5, §7, §8), **E8** (§4.2), **E9** (§7), **E11** (§11), **E12** (§9.1), **E14** (§6, §6.3), **E15** (§8), **E17** (§10), and **E18** (§10).

---

## 14. Security considerations

- **Trust:** storage trusts a shard's replicas. The fence and the append bound stop a deposed or out-of-date leader, not a hostile peer: a node that can deliver replica-protocol messages can write or reclaim entity state on the receiver. The protocol inherits actor messaging's transport security, peer authentication, deserialization allowlist, and per-manifest authorization (actor §14).
- **Integrity at rest:** every blob read is verified against its id (**E17**); every other durable format is stamped and refused by version (§12, compatibility §3.1). A node's store is trusted local state, not authenticated.
- **Availability:** a failed store withdraws its node from its quorums without crashing it (§11), and quorum loss pauses only the shard's own entities (**E11**).
