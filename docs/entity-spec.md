# Entity: Specification

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

A `§` with no document name refers to this spec. `actor §N` is [`distributed-actor-spec.md`](distributed-actor-spec.md), `storage §N` is [`entity-storage-spec.md`](entity-storage-spec.md), and `facets §N` is [`entity-facets-spec.md`](entity-facets-spec.md). Durable Objects lessons are cited as **DO §N** ([`../research/durable-objects.md`](../research/durable-objects.md)).

---

## 1. Scope

An **entity** is a virtual, durable, single-activation object addressed by a global name. It is an actor (actor §2.1) plus three things: a name-based virtual identity, a durable event-sourced journal, and a durability barrier on the reply. Everything else (mailboxes, serial execution, location-transparent `ask` and `tell`, membership, supervision, death watch, the receptionist) is the actor framework's, unchanged (actor §2.1–§2.6); the layer adds no transport and no required macro.

| | Actor | Entity |
|---|---|---|
| Identity | `ActorId`, minted at spawn | a stable, caller-chosen `EntityName` (§4.1) |
| Lifecycle | explicit spawn and stop | activated on first message, hibernated on idle (§7) |
| State | in memory, lost on stop | a journal plus snapshots, the source of truth |
| Reply | released at once | held until its records are durable (§5) |

At most one live activation of a name exists in the cluster, and every message for the name routes to it. The activation is a cache of state folded from the journal (**E3**). Each entity is its own single writer, so its records replicate independently, with no shared log and no consensus on the data path: that substrate is storage §1, and durable features beyond the event fold are facets over it (facets §1). Handler code is ordinary sequential Rust; the gates of §5 make it race-free and durable, so a crash loses no acknowledged write. Workflows, agents, machines, and the tenancy directory are entities.

### 1.1 Non-goals

- **One cluster-wide log, or one consensus group per entity.** Entity MUST NOT serialize all entities through one group nor run a group per entity (storage §3.2).
- **Cross-entity transactions.** Each entity is its own consistency boundary; multi-entity work is built above it with sagas and idempotency.
- **Exactly-once side effects.** Delivery is at-most-once and effectful messages are never auto-retried (actor §4); idempotency is the caller's.
- **Write availability under quorum loss.** A shard short of a quorum pauses writes rather than forking (storage §10).
- **Required codegen.** A derive MAY default `register` (actor §1); the hand-written form is normative.

### 1.2 Terms

| Term | Definition |
|---|---|
| **`EntityName`** | An entity's stable, cluster-wide identity: an `(entity_type, key)` pair. |
| **Activation** | The live, in-memory instance of an entity on its shard leader; disposable. |
| **`Seq`** | A record's position in one entity's total order; the first commits at 1, `Seq::ZERO` is the empty head. |
| **Shard, leader, term** | A key range of one type's names; the node elected to write it; the term that fences that node (storage §3.1, §5.1). |
| **Durability tier** | `Local` (one store, no term) or `Quorum` (per-entity quorum append), chosen by the system type (§2.3). |
| **Base** | An entity's compaction floor: slots at or below it live only in the snapshot (storage §8.2). |
| **Gateway, host** | The per-type actor that routes calls and owns the activation table (§4.3); the per-entity actor that runs §5. |

---

## 2. Seams

### 2.1 Authoring: `Entity`, `EntityHandler`, `EntityRegistry`, `EntityCtx`

The plug point every entity type implements. The type is the behavior (immutable configuration); it declares its state, event, and facet types, a pure fold, and one decision per accepted command. Every method but `apply` has a default.

```rust
pub trait Entity: Sized + Send + 'static {
    type System: EntitySystem;
    type State: SerializationRequirement + Default;
    type Event: SerializationRequirement;
    type Facets: FacetSet;
    const ENTITY_TYPE: &'static str;
    fn apply(state: &mut Self::State, event: &Self::Event);
    fn register(registry: &mut EntityRegistry<Self>);
    fn on_activate(&mut self, ctx: &EntityCtx<Self>) -> impl Future<Output = Result<(), BoxError>> + Send;
    fn on_deactivate(&mut self, ctx: &EntityCtx<Self>) -> impl Future<Output = ()> + Send;
    fn can_hibernate(&self, state: &Self::State) -> bool;
    fn on_alarm(&self, state: &Self::State, ctx: &EntityCtx<Self>) -> impl Future<Output = Vec<Self::Event>> + Send;
    fn on_peer_terminated(&self, state: &Self::State, ctx: &EntityCtx<Self>, peer: &ActorId, reason: TerminationReason)
        -> impl Future<Output = Vec<Self::Event>> + Send;
}

pub trait EntityHandler<M: Message>: Entity {
    fn handle(&self, state: &Self::State, msg: M, ctx: &EntityCtx<Self>)
        -> impl Future<Output = (Vec<Self::Event>, M::Reply)> + Send;
}
```

The author's side:

- `apply` MUST be pure and deterministic, with no I/O, clock, or entropy, so live commit and replay agree (**E2**).
- `ENTITY_TYPE` is REQUIRED, an explicit constant stable across renames and runs: it tags every name of the type and keys its gateway.
- `handle` MUST NOT mutate state and MUST NOT perform durable I/O (§3.2); a read-only command returns no events.
- `register` lists the commands accepted from the network (§4.5).
- `on_activate` runs once after rehydration, before the first command; an error aborts the activation. `on_deactivate` runs once on every deactivation path, even when the journal is unwritable, so it releases per-activation resources but cannot persist. `can_hibernate` is consulted only on idle eviction (§7).
- `on_alarm` and `on_peer_terminated` are callerless decisions (§3.3).

The runtime's side: it folds only committed records (§5) and hands each decision an `EntityCtx`, which carries the name, the blob area (`EntityCtx::blobs`, storage §9), a self-reference, the system, death watch (§3.3), and one compile-time-gated accessor per declared facet (facets §2.2). It exposes no `persist` and no state mutation. `SerializationRequirement` is the actor framework's bound (actor §2.11); the codec is the system's.

### 2.2 Client and hosting: `EntityRef`, `Entities`, `EntityExt`, `EntityNode`

`EntityRef<G>` is the only handle to an entity: cloneable, serializable, `Send` and `Sync`, carrying the name and a system handle, never granting access to state.

```rust
impl<G: Entity> EntityRef<G> {
    pub fn name(&self) -> &EntityName;
    pub async fn ask<M>(&self, msg: M) -> Result<M::Reply, EntityError> where G: EntityHandler<M>, M: Clone;
    pub async fn ask_timeout<M>(&self, msg: M, within: Duration) -> Result<M::Reply, EntityError> where G: EntityHandler<M>, M: Clone;
    pub async fn tell<M>(&self, msg: M) -> Result<(), EntityError> where G: EntityHandler<M>, M: Clone;
    pub async fn subscribe(&self, from: Seq) -> Result<Subscription<G>, EntityError>;
    pub fn follow(&self, from: Seq) -> Follower<G>;
    pub async fn events(&self, from: Seq, limit: usize) -> Result<EventHistory<G::Event>, EntityError>;
}
```

- A call the entity does not accept fails to compile, by the `G: EntityHandler<M>` bound (**E10**). `subscribe`, `follow`, and `events` are built-ins with no such bound.
- The call site is the same for a local or a remote activation (**E13**).
- The runtime MAY re-issue a command once, under the caller's original deadline, only when the first attempt provably did not run: a `CallError::DeadLetter` from a stopped host, or `NotLeader` (§9). It MUST NOT auto-retry an ambiguous outcome (`Unreachable`, `Timeout`, `Unavailable`). The `M: Clone` bound serves that one re-issue.

`Entities<G>` addresses a hosted type by key. `EntityExt`, implemented for every `EntitySystem`, hosts a type (`entities`), returns the node's hosting handle (`entity_node`), or builds a routing-only client (`entities_client`). `EntityNode` hosts types and carries three node-scoped capabilities: the blocking-I/O pool (storage §2.5), the metrics sink (§2.4), and the alarm index (facets §4.5).

- A node MUST share one of each capability across every type it hosts. Hosting straight off the system defaults all three (inline I/O, discarded metrics, no index), the `Local` shape that deterministic simulation requires.
- `EntityNode::entities_named` hosts one Rust entity under an explicit type name with a per-activation factory. The name MUST be stable cluster-wide and across runs; two names are two independent types, each with its own gateway, shard map, and groups.
- `EntityNode::durable` builds a production node over real storage; `EntityNode::hand_off_leadership` hands off every shard led by any type hosted through the node (storage §5.3).

### 2.3 The host system: `EntitySystem`

An `ActorSystem` that can host entities, adding the entity event channel (`emit_entity_event`), the type's shard map (`shard_map`), and leadership queries (`shard_leader`, `leads_shard`). It is implemented here for `LocalSystem` (the `Local` tier) and `ClusterSystem` (the `Quorum` tier), so the system type chooses the tier. The runtime's boundary with storage is this trait together with what it returns: `shard_map` builds a `ShardMapSource` (storage §2.4), whose `ShardMapSource::journal` yields a `DynEntityJournal` per replicated shard (storage §2.1).

- `leads_shard` MUST be true only on the node that may write the shard under the current term (storage §5.1); `Local` leads every shard. `shard_leader` is `None` when this node cannot know.
- The `Quorum` tier uses actor-cluster's consensus (`RaftConsensus`, actor §2.19) concretely and REQUIRES the leader-based control plane (storage §6).

### 2.4 Observation: `EntityEvent`, `EntityMetrics`

Two sinks, kept apart (§10). `EntityEvent` rides the actor event stream, one record per transition, for the simulator's checkers. `EntityMetrics` is a plug point a deployment supplies on `EntityNode::metrics`: aggregates for operators.

---

## 3. The entity model

### 3.1 State, events, and the fold

State is the fold of an entity's events by `apply` from `State::default()` at `Seq::ZERO`, and a cache of the journal. Rehydration rebuilds it from the latest snapshot plus the later records (storage §8.1), so `apply` is the one definition of what an event means, live and on replay (**E2**). An entity whose durable state is entirely in facets declares `type Event = NoEvent`.

### 3.2 Commands: decide, then apply

A command is a `Message` (actor §2.1) with a declared reply and a stable manifest. `handle` returns the events to persist and the reply; it does not persist. This split is normative: the host alone mutates state, on its serial executor; a command's events commit as one batch, so no observer sees part of a command; and the durability barrier exists once, in the host (§5). Through facet accessors a decision also stages records, or mutates a rebuildable local form captured before the append (facets §3.3); either way committed state changes only at the commit point. A reply reflecting post-command state is computed from the given state plus the emitted events. Application errors are values: a fallible command declares `type Reply = Result<T, E>` (§9).

### 3.3 Callerless decisions

Two hooks decide with no caller, through the same barrier as a command (§5); an empty decision with nothing staged commits nothing.

- `on_alarm` runs when the entity's durable alarm is due (facets §4.5). An entity without the alarm facet may implement it and is never called.
- `on_peer_terminated` runs when an actor the entity watches terminates, its node's loss included (actor §2.5). `EntityCtx::watch` queues a watch that the host installs after the current decision, before its reply releases. Watches are activation-local: a rehydrated entity re-watches from its folded state in `on_activate`, and a watch placed after the target died fires at once.

---

## 4. Names, activation, and routing

### 4.1 Names and shards

An `EntityName` names a logical object, not a node, so it is not locality-classifiable (actor §2.2). It is `Clone`, `Eq`, `Hash`, `Send`, `Sync`, and serializable. A stable hash places it in exactly one shard of its type (storage §3.1). Name to shard changes only on a split or merge (storage §7); shard to leader changes on elections.

### 4.2 Where an entity lives

An entity activates on its shard's leader, which is one of the shard's replicas, so the leader's own copy of each record and blob is local and counts toward its quorum (storage §4, §9). A new leader serves the shard's entities, rehydrating each lazily on first access (§7). Load is balanced by moving shard leadership and splitting shards, never by moving entities. Activating or hibernating takes no consensus round (**E8**).

### 4.3 Exactly-once activation

Each node runs one gateway per entity type, registered in the receptionist (actor §2.6) under one key per type. For the shards it leads, the gateway owns the table from name to host, needed because an activation's `ActorId` is minted at spawn. The gateway is serial, so a node activates a name at most once, and concurrent requests for an inactive name find the same host (**E6**). Cluster-wide single activation follows from one leader per term (storage §5.1).

### 4.4 Routing a call

1. **Name to shard,** by the committed partition (storage §6).
2. **Shard to host.** A ref caches the hosts it has reached; a miss asks a gateway, this node's first, for the name, and the gateway gets or activates the host and returns its handle.
3. **Command to host,** never through the gateway: unserialized into a local host's mailbox (actor §3.2), or as one envelope with the command's manifest and payload to a remote one (§4.5).
4. **Stale leader.** A node that does not lead the shard answers `NotLeader(hint)`. The caller follows the hint, backs off while the shard elects, and re-discovers a gateway when its target is unreachable, all within its own deadline.

A call to a local or a remote activation yields observably identical replies and ordering (**E13**): the redirect waits out a transient miss rather than surfacing it. The host cache MUST be bounded (`EntityConfig::host_cache_capacity`), since it is keyed by name. A node that replicates none of the shard reads no shard map: a stale entry shows as `NotLeader`, `DeadLetter`, or `Unreachable`, so steady-state routing costs no control-plane traffic and passes no serial gateway.

### 4.5 The dispatch allowlist

`EntityRegistry` holds the command manifests a type accepts, filled by `register` through `accept` and replayed onto the host actor's handler registry, so decoding, dispatch, and reply encoding are the actor framework's (actor §2.10). It is the deserialization allowlist for entity commands (actor §2.11, §9): an unregistered manifest yields `EntityError::Call(CallError::Unhandled)`, and no type outside it is built from network bytes. `accepted_manifests` reads it. Subscription is a built-in every host accepts outside it.

---

## 5. The gates and the per-command protocol

The gate model of Durable Objects (DO §3) makes sequential handler code race-free and durable.

- **Input gate.** While an entity's append is in flight, the host admits no new command; messages wait in the mailbox.
- **Output gate.** The host holds the reply (actor §2.10) until the command's records are durable; a failed commit turns it into an `EntityError`.

The host's per-command protocol, in order:

```
1. (events, reply) = G::handle(&state, msg, ctx)            // decide: no mutation, no I/O
   records = tagged(events) ++ drained facet stages          // facets §3.2
2. if records is empty { return reply }                     // a read commits nothing (§6.1)
3. outcome = journal.append(name, after = head, records)    // storage §2.1
4. on Committed(new_head):
       if new_head != head + records.len() { step_down(); return Unavailable }
       fold records into state and facet forms; head = new_head
       deliver to subscribers; maybe snapshot                // §6.3, storage §8.1
       return reply                                          // the output gate releases
5. on NotLeader(hint):  step_down(); return NotLeader(hint)  // did not commit
6. on Unavailable:      step_down(); return Unavailable      // ambiguous: MAY commit later
```

Three ordering rules are normative (**E1**, **E5**):

- **Fold only after durability.** The host MUST NOT fold state before the batch commits.
- **Reply only after durability.** The host MUST NOT release the reply before the batch commits, nor as success if it does not.
- **Fold only a contiguous head.** A commit MUST advance the head by exactly the batch appended; recovery establishes a quorum-durable head first (storage §5.2), so this guard catches residue such as a late timed-out append (storage §4). On any other outcome the host MUST NOT fold and MUST end the activation, whose head is no longer trustworthy; the next access rehydrates (**E3**). A forced step-down discards every physical facet's local form (facets §3.3) and emits `Deactivated` (§10).

Facets add no barrier: their records join step 3's single batch (**E19**).

**`tell` is fire-and-forget.** It returns once the host has accepted the command (actor §4), so it reports only enqueue-time failures (`Call`, an unregistered manifest included, and `NotLeader`), never `Unavailable`. The protocol is the same; a failed commit notifies no one.

**A self-tell waits for its sender's commit.** A message a decision sends to its own entity (through `EntityCtx::this`) queues behind the running command, so it is handled only after that decision's records commit; if they do not, the activation stops first and never handles it. Layers above rely on this to act only on committed state (workflow §5).

---

## 6. Reads and observation

### 6.1 Reads from the activation

A command that emits nothing commits nothing (§5 step 2): the leader answers it locally, with no per-read consensus (DO §4.3) and no wait on replication. The contract is **read-your-leader**: the read path does not reconfirm leadership, so a deposed leader on the minority side MAY serve stale reads until its activation stops, while writes never fork (storage §5.1). A caller needing a linearizable read issues a trivial writing command, which rides the output gate.

### 6.2 The event read

`EntityRef::events(from, limit)` returns up to `limit` committed events after `from` as an `EventHistory`, without activating the entity, so polling leaves a hibernated entity hibernated. It returns the **event projection**:

- **The `Seq`s are the journal's own.** Facet records leave gaps, so a caller pages from the last `Seq` it saw, never by arithmetic.
- **A short page means the head.** Fewer than `limit` events come back only at the committed head.
- **Every reply carries the base** (storage §8.2). An absent slot at or below it is truncated history; `base <= from` means nothing asked for was compacted. Compaction waits for no reader, so the base MUST ride every page.
- **Corruption is loud.** A record that will not decode surfaces as `Unavailable`, never as a dropped slot.

The shard leader's gateway serves it from the journal's `load` (storage §2.1), touching no host; a non-leader answers `NotLeader(hint)`, so it is read-your-leader like §6.1. Each page is bounded by the server and carries a cursor, an at-head flag, and the base, so a page of facet records still makes progress. The store cannot tell committed records from tentative ones, so the read MUST serve only records at or below a head known committed under the term this node leads with now (`EntityJournal::term`): a resident activation's head, or a recovery run under that term. Until one exists the read is transient, the ref retries under the caller's deadline, and only its lapse reaches the caller, as `Unavailable`. No observer sees an unacknowledged write (**E5**).

### 6.3 Subscriptions and the follower

A subscription is best-effort, live delivery of committed records to a sink, layered over the durable `load`, never a source of truth.

- `EntityRef::subscribe(from)` registers a sink on the leader's host at the committed head and returns that head with a stream of later batches; the subscriber backfills from `from` by `events`.
- The host delivers each batch after the fold and the output gate, where it emits `Committed`; delivery MUST NOT gate a commit or a reply (**E5**).
- **Reconcile by `Seq`.** Each batch carries the `Seq` it begins after. A sink that has seen up to `last` MUST close any gap by reading the journal and MUST ignore records at or below `last`, which absorbs the drops, duplicates, and reordering at-most-once delivery may produce: the reconciled sequence is exactly what `load` returns (**E16**).
- **Bounded, never blocking.** On overflow of a sink's bounded buffer the host drops the sink; a slow subscriber MUST NOT stall writes or the input gate.
- **Ephemeral.** Sinks are activation state: every deactivation drops them, and subscribers re-subscribe and backfill. A subscription does not veto idle eviction.

`EntityRef::follow(from)` packages that reconciliation as a `Follower` whose `next` yields `Followed::Batch`, re-subscribing, backfilling, and periodically re-reading the journal (a silent leader move leaves an old stream idle). When a read reports a base above its cursor, it MUST yield `Followed::Truncated` before resuming, never a short history presented as complete.

---

## 7. Lifecycle

```
activate → rehydrate (storage §8.1) → on_activate → serve → idle → on_deactivate → (snapshot) → stop
```

- **Activation** starts when the first message for the name reaches its shard leader's gateway. It takes no consensus round; on `Quorum` it costs one quorum head recovery (**E8**).
- **Migration** follows shard leadership (storage §5.3); in-memory state is never carried across.
- **Hibernation.** After `EntityConfig::idle_after` without a command, the host MAY, if `can_hibernate` permits, run `on_deactivate`, snapshot if the snapshot trigger has been crossed, and stop. The idle snapshot MUST use the write path's trigger and MUST NOT be unconditional, since a snapshot reaches every replica (storage §8.1). A veto, or a pending alarm the index does not yet hold (facets §4.5), reschedules the check. The next command or subscription re-activates the entity with identical state (**E12**); an event read does not. An implementation SHOULD apply a small minimum residency or jitter, so an entity touched just slower than the timer is not evicted and reloaded each time.
- **Forced step-down.** When leadership moves or the shard goes unavailable, the activation runs `on_deactivate`, takes no snapshot, emits `Deactivated`, and stops.
- **Eviction races.** A command reaching a stopped cached host gets `CallError::DeadLetter`, proof that it never ran: the ref re-resolves and re-issues once (§2.2), and a second `DeadLetter` surfaces.

---

## 8. Failure model

The actor failure model applies (actor §5), and a failure's blast radius is the shard. When a node fails, each shard it led elects a new leader (storage §5.3); every pending `ask` to an entity it hosted completes with `EntityError::Call` wrapping `CallError::Unreachable`, never a hang; watchers receive `Terminated` with `NodeDown` (actor §2.5); and the next message re-activates each entity on the new leader, losing no acknowledged write (**E14**). A shard short of a quorum answers `Unavailable` and never forks (storage §10).

---

## 9. Errors

```rust
pub enum EntityError {
    Call(CallError),
    NotLeader(NodeId),
    Unavailable(String),
}
```

- **Application errors** live in the command's reply (§3.2), never here.
- **`Call`** is a transport or system failure reaching the activation (actor §8.1), an unregistered manifest included.
- **`NotLeader`** means leadership moved and the command did not commit; the redirect of §4.4 absorbs it unless exhausted.
- **`Unavailable`** means no quorum or a timed-out commit, and is ambiguous: the write MAY become visible later, at its own `Seq`. On the event read it is a failed local read, safe to re-ask.
- `EntityError` MUST be exhaustive at the public API (actor §8).

---

## 10. Security and observability

**Security.** The layer inherits mutual TLS, the handshake allowlist, and the deserialization allowlist (§4.5) from actor §9. A cluster's `Authorizer` sees each remote command as an envelope to the host actor under the command's own manifest, so it can gate by peer and manifest; the entity layer consults no authorizer and cannot gate by `EntityName`.

**Events.** `EntityEvent` rides the actor event stream (actor §2.7) through `emit_entity_event`, the checkers' interface: `Activated`, `Rehydrated` (from a snapshot or not, records replayed), `Committed` (shard and `Seq`), `Snapshotted`, and `Deactivated`, each with the node and the name. Every `Activated` MUST be matched by one `Deactivated` or the node's loss, so **E6** is checkable over the stream, and `Committed` carries the shard, so a split or merge is checkable as a move that keeps `Seq` increasing (**E15**). The shard events are `LeaderChanged` (once per term won), `ShardSplit`, and `ShardMerged` (by each node as it applies the change). Subscriptions add no content-bearing event.

**Metrics.** A node SHOULD expose an `EntityMetrics` sink (`AtomicEntityMetrics` is the stock registry), node-scoped and costing O(1) per operation, reporting three measurements: each commit's latency by `CommitOutcome`, each rehydration's cost, and the resident activation count. Its vocabulary MUST be closed, keyed by nothing wider than an entity type, since shard count is unbounded by elasticity. Leadership changes, splits, and merges are left to the event stream.

---

## 11. Determinism

The runtime MUST take time, randomness, task launching, and messaging only through the actor framework's seams (`Clock`, `Entropy`, `Spawner`, `Transport`; actor §2.12.1), so a cluster of entities runs in one process under virtual time and one seed reproduces a run (actor §2.12.2). Every hash that places a name or a shard MUST agree on every node and across runs. Store I/O runs inline by default (storage §2.5), and a subscription's drop decision depends only on buffer occupancy (§6.3), for the same reason.

---

## 12. Invariants

A conforming implementation upholds every row under the faults of the deterministic simulation, and **E10** at compile time. The Rust copy of this table, with the suites that verify each row, is the `entity` crate's conformance catalogue.

| # | Invariant | Defined in |
|---|---|---|
| **E1** | **Single writer per entity.** Only the shard leader appends, under its term, and a deposed leader reaches no quorum, so a record never forks. A commit advances the head by exactly the batch appended, and the host folds only such a commit. | §5, storage §5 |
| **E2** | **Deterministic fold.** `apply` yields identical state on live commit and on replay from any snapshot and journal prefix. | §3.1 |
| **E3** | **The journal is the source of truth.** An activation's head and state derive only from journal and snapshot returns, never from a prior activation's memory. | §1, storage §8 |
| **E4** | **A snapshot never shortens the log.** A head is the best snapshot's `Seq` plus the contiguous records above it, and the host refuses a snapshot the head does not cover. | storage §8 |
| **E5** | **Reply iff durable.** A reply is released, and its events folded, only after its batch commits; `NotLeader` or `Unavailable` yields an error, no fold, and no success reply. | §5 |
| **E6** | **Exactly-once activation per node.** A node activates a name at most once; concurrent requests find the same host. | §4.3 |
| **E7** | **Bounded consensus groups.** O(shards) plus O(entity types) groups, bounded by split and merge, never O(entities) and never one, and none on the data path. | storage §3.1, §4, §6, §7 |
| **E8** | **Activation without consensus.** Activating or hibernating takes no consensus round; on `Quorum` it pays at most one quorum head recovery. | §4.2, storage §3.2, entity §7 |
| **E9** | **Control plane off the data path.** The shard map changes only on cluster events; no write or activation contacts it. | storage §6 |
| **E10** | **Type-safe calls.** A command an entity has no `EntityHandler` for does not compile. | §2.2 |
| **E11** | **CP under partition.** A shard short of a quorum pauses its writes (`Unavailable`) and never forks; other shards serve. | storage §10 |
| **E12** | **Hibernation round-trip.** An entity evicted on idle re-activates with identical state through snapshot and replay. | storage §8, entity §7 |
| **E13** | **Location transparency.** A call to a local or a remote entity yields observably identical replies and ordering. | §4.4 |
| **E14** | **Lossless failover.** A head recovered under a confirmed quorum contains every record any earlier term reported committed, at its original `Seq` and bytes, so no acknowledged write is lost across a leadership change. | storage §5, §5.3 |
| **E15** | **Split and merge safety.** An entity is writable in exactly one shard at any time; a split or merge loses or duplicates no write. | storage §7 |
| **E16** | **Subscriptions are observational and lossless by `Seq`.** Delivery never gates a commit, forks state, or advances a head, and a sink reconciling by `Seq` reconstructs exactly the committed sequence. | §6.3 |
| **E17** | **Blob address integrity.** A blob read returns bytes whose BLAKE3 hash is the requested id, or an error, verified after any transfer. | storage §9 |
| **E18** | **Blob durability and entity-scoped reclamation.** A blob put is acknowledged only once on a write quorum of the entity's replicas that includes the leader (the one store on `Local`), with no consensus on the path; equal content stores once; reclamation is entity-scoped, idempotent, and never removes a referenced blob. | storage §9 |
| **E19** | **Facet atomicity and interpretation safety.** A command's records across all facets commit as one batch or not at all; replay dispatches by tag and aborts on an unknown one; the composite snapshot restores every facet to one `Seq`. | facets §3 |
| **E20** | **Physical facets expose only durable state.** A physical facet's uncommitted local mutations are unobservable to entity-level observers and discarded on any outcome but a commit; rehydration reproduces the form exactly. | facets §3, §4.3 |
| **E21** | **An alarm fires at most once per arm.** One armed deadline commits at most one `on_alarm` effect; a fire whose commit fails leaves it pending. | facets §4.5 |

---

## 13. Deferred

Storage deferrals are storage §12, facet deferrals facets §6.

- **Linearizable reads:** a check-quorum lease on the leader-election group, the activation self-fencing when it lapses (§6.1).
- **Follower reads:** reads from followers under a freshness bound, for read scale.
- **Hibernatable connections:** parking a subscription across hibernation, so a follower need not re-subscribe after each idle eviction.
- **One read path for followers:** a blocking event read that answers once the head passes `from`, serving `Follower` with no sink set, at one ask per batch.
- **The full actor context:** `EntityCtx` does not surface child spawning and the rest of the actor `Ctx`.
- **Per-entity authorization:** an authorizer consulted per `(peer, EntityName, manifest)` (§10).
- **Idle-eviction jitter:** no minimum residency or jitter is applied (§7), so an entity touched just slower than `idle_after` churns.
- **Cross-entity sagas,** above the single-entity boundary (§1.1).
- **A derive for `Entity`,** defaulting `register` (actor §2.10).
