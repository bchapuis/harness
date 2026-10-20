# Entity: Specification

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

A `§` with no document name refers to this spec. `actor §N` is [`distributed-actor-spec.md`](distributed-actor-spec.md), `cluster §N` is [`cluster-spec.md`](cluster-spec.md), `storage §N` is [`entity-storage-spec.md`](entity-storage-spec.md), and `facets §N` is [`entity-facets-spec.md`](entity-facets-spec.md); `workflow`, `agent`, `machine`, and `edge` name the specs of the layers above.

---

## 1. Scope

An **entity** is a virtual, durable, single-activation object addressed by a global name: an actor (actor §3.1–§3.7) plus a name-based identity, an event-sourced journal, and a durability barrier on the reply.

At most one live activation of a name exists in the cluster, receiving every message for the name; it is a cache of state folded from the journal (**E3**). Each entity is its own single writer, replicating its records over entity storage (storage §1) with no shared log and no consensus on the data path; facets add durable features beyond the event fold (facets §1). Workflows (workflow §1), agent sessions (agent §4.1), machines (machine §4), and the tenancy directory (edge §3.3) are entities.

### 1.1 Non-goals

- **One cluster-wide log, or one consensus group per entity.** An implementation MUST NOT serialize all entities through one consensus group nor run one per entity (storage §4.2).
- **Cross-entity transactions.** Each entity is its own consistency boundary.
- **Exactly-once side effects.** Delivery is at-most-once, with no re-issue after an ambiguous outcome (§3.2, actor §8.1); idempotency is the caller's.
- **Write availability under quorum loss.** A shard short of a quorum pauses writes rather than forking (storage §11).
- **Required code generation.** The hand-written entity type (§3.1) is normative; a derive MAY supply a default registration (actor §7).

### 1.2 Conformance classes

An **entity runtime** (hosts, gateways, references) meets the runtime's side of §3.1, §3.2, and §3.4, and §4–§11; an **entity type** the author's side of §3.1, and §4; an **entity system** §3.3; a **metrics sink** the sink's side of §3.4; a **caller** the caller's side of §3.2, and §7.3; a **deployment** §5.6.

---

## 2. Model

- **Entity type:** a **behavior** (the configuration, fold, and decisions supplied to each activation) and a **type name** that tags every name of the type; two type names are independent types, even over one behavior.
- **Entity name:** a type name and a caller-chosen key: an entity's stable, cluster-wide identity.
- **Event, fold, state:** an event is a unit of durable change; the fold applies one event to a state; an entity's state is the fold of its committed events from the type's default state.
- **Command:** an actor message (actor §3.1) addressed to an entity, with a declared reply and a stable manifest.
- **Decision:** the type's function from the current state and one input (a command, a due alarm, or a watched actor's termination) to the events to persist and, for a command, the reply.
- **Journal, sequence number, head, base:** an entity's totally ordered committed records (storage §3.1); a record's position, the first being 1 and 0 the empty head; the latest committed position; the compaction floor, below which records live only in the snapshot (storage §9.2).
- **Facet:** a durable storage feature of one entity over its journal and blob area (facets §1); the event fold is facet 0.
- **Shard, leader, term:** a key range of one type's names; the node elected to write it; the term that fences that node (storage §4.1, §6.1).
- **Durability tier:** the **local tier** (one store, no term) or the **quorum tier** (a per-entity quorum append) of storage §3.1, fixed by the entity system (§3.3).
- **Activation, host, gateway:** the live, disposable instance of an entity on its shard's leader; the actor that holds one activation and runs the per-command protocol (§6); the actor, one per node and type, that routes calls to hosts (§5.3).
- **Reference:** the only handle to an entity (§3.2).
- **Entity error:** a call error, a not-leader error, or an unavailable error (§3.2).
- **Entity system:** an actor system that can host entities (§3.3).
- **Subscription:** best-effort live delivery of an entity's committed records to a sink (§7.3).

---

## 3. Seams

### 3.1 Authoring: `Entity`, `EntityHandler`

A plug point: an entity type implements it and the entity runtime calls it. The implementing value is the behavior; it declares its state, event, and facet types, its fold, and one decision per accepted command.

```rust
pub trait Entity: Sized + Send + 'static {
    type System: EntitySystem;
    type State: SerializationRequirement + Default;
    type Event: SerializationRequirement;
    type Facets: FacetSet;
    const ENTITY_TYPE: &'static str;
    fn apply(state: &mut Self::State, event: &Self::Event);
    fn register(registry: &mut EntityRegistry<Self>);
    async fn on_activate(&mut self, ctx: &EntityCtx<Self>) -> Result<(), BoxError>;
    async fn on_deactivate(&mut self, ctx: &EntityCtx<Self>);
    fn can_hibernate(&self, state: &Self::State) -> bool;
    async fn on_alarm(&self, state: &Self::State, ctx: &EntityCtx<Self>) -> Vec<Self::Event>;
    async fn on_peer_terminated(&self, state: &Self::State, ctx: &EntityCtx<Self>, peer: &ActorId, reason: TerminationReason) -> Vec<Self::Event>;
}

pub trait EntityHandler<M: Message>: Entity {
    async fn handle(&self, state: &Self::State, msg: M, ctx: &EntityCtx<Self>) -> (Vec<Self::Event>, M::Reply);
}
```

Every method but `apply` has a default: `register` accepts nothing, `on_activate` succeeds, `on_deactivate` does nothing, `can_hibernate` permits, and `on_alarm` and `on_peer_terminated` decide nothing.

The author's side:

- `apply` is the fold. It MUST be pure and deterministic, with no I/O, clock, or entropy (**E2**).
- `ENTITY_TYPE` is the type name: REQUIRED, explicit, and stable across runs and renames of the implementation. It keys the type's gateway (§5.3).
- `handle` is a command's decision. It MUST NOT mutate state and MUST NOT perform durable I/O; a read-only command returns no events.
- `register` accepts each command the type takes from the network, through `EntityRegistry::accept` (§5.5).
- `State` and `Event` meet the actor wire bound (`SerializationRequirement`, actor §3.4); the default `State` is the state at the empty head; `Facets` declares the facet set (facets §3.2).
- `on_activate`, the activation hook, runs once after rehydration and before the first command; an error aborts the activation. `on_deactivate`, the deactivation hook, runs once on every deactivation path, even with the journal unwritable, and cannot persist. `can_hibernate`, the hibernation veto, is consulted only on idle eviction (§8).
- `on_alarm` is the alarm decision and `on_peer_terminated` the peer-termination decision (§4.3).

The runtime's side: it hands each decision an `EntityCtx` carrying the entity's name, its blob area (`EntityCtx::blobs`, storage §10), a self-reference (`EntityCtx::this`), the system, death watch (`EntityCtx::watch`, §4.3), and one accessor per declared facet (facets §3.2). The context offers no way to persist or mutate state.

### 3.2 References: `EntityRef`

A boundary between a caller and the entity runtime. A reference carries the entity's name, never access to state; it can be copied and sent in messages, and routes from whichever node decodes it. A caller obtains one by key for a hosted type, from a message, or from a decision's context; obtaining one activates nothing.

| Operation | Takes | Yields |
|---|---|---|
| Name | nothing | the entity's name |
| Ask | a command the type accepts, optionally a deadline | the reply, released once the command's records are durable (§6), or an entity error |
| Tell | a command the type accepts | the host's acceptance, or an enqueue-time entity error (§6) |
| Subscribe | a sequence number | the committed head and a stream of later committed batches (§7.3) |
| Follow | a sequence number | a follower over the committed events after it (§7.3) |
| Read events | a sequence number and a limit | up to that many committed events after it, and the base (§7.2) |

The runtime's side:

- An ask or tell of a command the type does not accept does not compile (**E10**). The three reads serve every type.
- The runtime MAY re-issue a command once, under the caller's original deadline, only when the first attempt provably did not run: a dead letter from a stopped host (actor §8.2), or a host's not-leader error (§6). The re-issue re-resolves the host through the redirect of §5.4, and its outcome is final. It MUST NOT re-issue after an ambiguous outcome: an unreachable or timed-out call (actor §8.2), or an unavailable error.

The caller's side: an application failure arrives in the command's reply (§4.2), never as an entity error.

**Errors:** the set of kinds MUST be closed, so a caller handles every kind (actor §8).

| Kind | Meaning |
|---|---|
| Call error | A transport or system failure reaching the activation (actor §8.2), an unregistered manifest included. |
| Not leader | Leadership moved; the command did not commit. Its hint names the node believed to lead. A reference absorbs a gateway's not-leader by the redirect of §5.4 and a host's by its one re-issue, so a caller sees one only when the redirect outlasts the call's deadline or the re-issued command meets another. |
| Unavailable | No quorum, or a timed-out commit: ambiguous, the write MAY become visible later at its own sequence number. On an event read, a failed local read, safe to re-ask. |

### 3.3 The entity system: `EntitySystem`

A boundary between the entity runtime and the actor system beneath it: an entity system is an actor system (actor §3.3) offering the operations below. A single-node actor system hosts the local tier, a clustered one the quorum tier.

| Operation | Takes | Yields |
|---|---|---|
| Emit entity event | an entity event (§11) | the event, on the node's actor event stream as an application event (actor §3.6) |
| Shard map | a type name and this node's storage for it | the type's placement view (storage §4.1), which yields this node's journal for each shard it replicates (storage §3.1) |
| Shard leader | a shard | the node leading it, or none when this node cannot know (it replicates none of the shard, or an election runs) |
| Leads shard | a shard | whether this node leads it |

- Leads shard MUST be true only on the node permitted to write the shard under the current term (storage §6.1). On the local tier the node leads every shard.
- The quorum tier runs its shard maps and leader elections on the actor framework's application consensus (cluster §3.5) and REQUIRES the leader-based control plane (cluster §5.4.3, storage §7).

### 3.4 Metrics: `EntityMetrics`

A plug point: the entity runtime reports aggregates to the metrics sink a deployment supplies. A node SHOULD supply a sink, node-scoped (§5.6) and costing O(1) per report.

```rust
pub trait EntityMetrics: Send + Sync + 'static {
    fn commit(&self, entity_type: &'static str, outcome: CommitOutcome, took: Duration);
    fn rehydrated(&self, entity_type: &'static str, took: Duration, replayed: u64);
    fn activation_delta(&self, entity_type: &'static str, delta: i64);
}
```

The runtime calls `commit` once per append (§6) with its outcome (`CommitOutcome::Committed`, `CommitOutcome::NotLeader`, or `CommitOutcome::Unavailable`) and the time from issue to outcome; `rehydrated` once per activation with its duration, head recovery included, and the records replayed after the snapshot; and `activation_delta` with +1 per activation and −1 per deactivation, so the sum is the resident count. The vocabulary MUST be closed: these three measurements, labelled by nothing wider than the entity type and a commit's outcome, never by shard or entity name. Leadership changes, splits, and merges appear only on the event stream (§11).

---

## 4. The entity model

### 4.1 State, events, and the fold

State is a cache of the journal. Rehydration rebuilds it from the latest snapshot plus the later records (storage §9.1). The fold is the one definition of what an event means, live and on replay (**E2**).

### 4.2 Commands: decide, then apply

A decision returns the events to persist and the reply, and persists nothing. The split is normative: only the host changes state, one decision at a time; a command's events commit as one batch, so no observer sees part of a command; and the durability barrier exists once, in the host (§6). A decision also stages records through facet accessors, or changes a rebuildable local form captured before the append (facets §4.2); committed state still changes only at the commit point. A decision sees the state before the command, so a reply reporting the state after it applies the decision's events to that state. Application errors are values: a fallible command's reply is itself a result.

### 4.3 Callerless decisions

Two decisions run with no caller, through the same barrier as a command (§6); an empty decision with nothing staged commits nothing.

- The alarm decision runs when the entity's durable alarm is due (facets §5.5), never for an entity without the alarm facet.
- The peer-termination decision runs when an actor the entity watches terminates, its node's loss included (actor §10). A decision requests a watch through its context (§3.1); the host installs it after the decision, before its reply is released. Watches belong to the activation: a rehydrated entity re-watches from its folded state in its activation hook, and a watch placed after the target died fires at once.

---

## 5. Names, activation, and routing

### 5.1 Names and shards

A name denotes a logical object, not a node, so it is not locality-classifiable (actor §3.2). Names compare by value and travel in messages. A stable hash places a name in exactly one shard of its type (storage §4.1), a placement that changes only on a split or merge (storage §8); a shard's leader changes on elections.

### 5.2 Where an entity lives

An entity activates only on its shard's leader, one of the shard's replicas (storage §4.1, §5). A new leader rehydrates each of the shard's entities lazily, on first access (§8). An entity moves only with its shard's leadership or range, never alone: load is balanced by moving leadership and splitting shards. Activating or hibernating takes no consensus round (**E8**).

### 5.3 Single activation

Each node hosting a type runs one gateway for it, registered in the receptionist (actor §3.7) under one key per type name. For the shards its node leads, the gateway owns the table from name to host. A node activates a name at most once, and concurrent requests for an inactive name find the same host (**E6**). Cluster-wide single activation follows from one leader per term (storage §6.1).

### 5.4 Routing a call

1. **Name to shard:** by the committed partition (storage §7).
2. **Shard to host:** a reference MAY cache the hosts it has reached. On a miss it asks a gateway, this node's first, which gets or activates the host and returns its handle.
3. **Command to host:** never through the gateway; by value into a local host's mailbox (actor §5), or as one envelope with the command's manifest and payload to a remote one (§5.5).
4. **Stale leader:** a gateway whose node does not lead the shard answers an activation or event-read request not-leader with a hint. Neither request commits anything, so the reference re-asks until the call's deadline lapses (an implementation MAY also cap the attempts): it follows the hint, backs off while the shard elects, re-discovers a gateway when its target is unreachable, and on lapse surfaces not-leader with the last hint. A host that answers a command not-leader (§6) gets it re-issued at most once, re-resolved by this redirect (§3.2).

Local and remote calls yield observably identical replies and ordering (**E13**): the redirect waits out a transient miss. A node's routing state MUST stay bounded however many names it addresses. Steady-state routing contacts no control plane and no gateway: a node replicating none of the shard holds no copy of its map, and a stale cached host shows as a not-leader error, a dead letter, or an unreachable call.

### 5.5 The command allowlist

A type's command allowlist is what its registration accepts (§3.1). A host decodes, dispatches, and answers remote commands as an actor does (actor §7), under each command's own manifest. The allowlist is the deserialization allowlist for entity commands (actor §3.4, §14): a manifest not on it fails as a call error of kind unhandled (actor §8.2), and no type outside it is built from network bytes. Outside the allowlist, every host also accepts the subscription request, and every gateway the activation and event-read requests.

### 5.6 Hosting a type

A node hosting a type runs its gateway (§5.3) and replicates the shards allocated to it (storage §7). A node MAY host one behavior under several type names, each an independent type with its own names, gateway, shard map, and leader-election groups; each type name MUST be stable cluster-wide and across runs. A node MAY instead be a routing-only client of a type: it hosts nothing, holds no shard map, and routes through a hosting node's gateway found in the receptionist.

The blocking-I/O pool (storage §3.4), the metrics sink (§3.4), and the alarm index (facets §5.5) are node-scoped: a node MUST share one of each across every type it hosts.

---

## 6. The gates and the per-command protocol

- **Input gate:** while an entity's append is in flight, its host admits no new command; messages wait in the mailbox.
- **Output gate:** the host holds the reply (actor §7) until the command's records are durable.

For each command and each callerless decision (§4.3), the host runs these steps in order:

1. **Decide:** run the decision on the current state, yielding events and a reply; it MAY also stage facet records (facets §4.3).
2. **Batch:** form one batch of the events, each tagged as facet 0, then the records drained from every facet's stage (facets §4.1).
3. **Read path:** if the batch is empty, release the reply; nothing commits (§7.1).
4. **Append:** append the batch to the journal after the known head (storage §3.1).
5. **Committed:** if the head did not advance by exactly the batch's length, end the activation and answer unavailable. Otherwise fold the batch into the state and facet forms, advance the head, emit `Committed`, deliver to subscribers (§7.3), snapshot if the trigger is crossed (storage §9.1), and release the reply.
6. **Not leader:** end the activation and answer not-leader with the hint; the batch did not commit.
7. **Unavailable:** end the activation and answer unavailable; the batch MAY commit later.

Three ordering rules are normative (**E1**, **E5**):

- **Fold only after durability:** the host MUST NOT fold state before the batch commits.
- **Reply only after durability:** the host MUST NOT release the reply before the batch commits, nor as success if it does not.
- **Fold only a contiguous head:** the host MUST fold a commit only if it advances the head by exactly the batch appended. On a commit that skips ahead (an earlier timed-out append that landed late, storage §5) and on every other outcome, the host MUST NOT fold and MUST end the activation; the next access rehydrates (**E3**). This forced end discards every physical facet's local form (facets §4.2) and emits `Deactivated` (§8).

Facets add no barrier: their records join the one batch (**E19**).

**Tell:** a tell returns once the host accepts the command (actor §8.1), so it reports only enqueue-time failures (a call error, an unregistered manifest included, or not-leader), never unavailable. The protocol is the same; a failed commit notifies no one.

**Self-tell:** a message a decision sends its own entity through the self-reference queues behind the running command, so it is handled only after that decision's records commit; if they do not, the activation stops first and never handles it.

---

## 7. Reads and observation

### 7.1 Reads from the activation

A command that emits nothing commits nothing (§6): the node that believes it leads the shard answers it from the activation, with no consensus, no wait on replication, and no reconfirmation of its leadership. Such reads are read-your-leader: a deposed leader on the minority side MAY serve stale reads until its activation stops, while writes never fork (storage §6.1). A caller needing a linearizable read issues a trivial writing command, which rides the output gate.

### 7.2 The event read

An event read (§3.2) activates nothing, so polling leaves a hibernated entity hibernated. It returns the entity's committed events, without its facet records:

- **Sequence numbers:** the journal's own. Facet records leave gaps, so a caller pages from the last sequence number it saw, never by arithmetic.
- **Short reads:** a read returns fewer events than its limit only at the committed head, and carries no at-head flag of its own: a short result, an empty one included, is how a caller detects the head, while a full one may end exactly at it.
- **The base:** compaction waits for no reader, so every page MUST carry the base (storage §9.2). An absent sequence number at or below it is truncated history; a base at or below the requested one means nothing asked for was compacted.
- **Corruption:** a record that will not decode surfaces as unavailable, never as a dropped sequence number.

The shard leader's gateway serves the read from the journal's load (storage §3.1), touching no host; a non-leader answers not-leader with a hint. A read is one or more gateway requests. Each scans a bounded number of records and yields one page carrying the events among them, a cursor (the last record scanned, event or not), an at-head flag, and the base. Facet records count toward that bound, so a page away from the head can hold fewer events than the read wants, or none; only its at-head flag, set when the scan reached the served head below, marks the head. The reference requests pages from each cursor until it holds the limit or a page is at head, so a page of facet records still advances and a read is short only at the head. A load can return uncommitted records (storage §3.2), so the gateway MUST serve only records at or below a head known committed under the term this node currently leads with: a resident activation's head, or one recovered under that term. Until one exists the read is transient: the reference retries within the read's deadline and reports only its lapse, as unavailable. No observer sees an unacknowledged write (**E5**).

### 7.3 Subscriptions and the follower

A subscription is layered over the journal's load and is never a source of truth.

- Subscribing registers a sink on the leader's host at the committed head and returns that head with a stream of later batches; the subscriber backfills from its own starting point by reading events.
- Delivery follows the fold (§6) and MUST NOT gate a commit or a reply (**E5**).
- **Reconcile by sequence number:** each batch carries the sequence number it begins after. A sink that has seen up to some sequence number MUST close any gap by reading the journal and MUST ignore records at or below it, absorbing the drops, duplicates, and reordering of at-most-once delivery: the reconciled sequence is exactly what the journal's load returns (**E16**).
- **Bounded, never blocking:** the host drops a sink exactly when its bounded buffer overflows; a slow subscriber MUST NOT stall writes or the input gate.
- **Ephemeral:** every deactivation drops the sinks; subscribers re-subscribe and backfill. A subscription does not veto idle eviction.

A follower reconciles a subscription with event reads: it yields the committed event batches in sequence order, re-subscribing and backfilling as needed, and keeps progressing across a leader move that leaves its old stream open but silent. When a read reports a base above its cursor, the follower MUST yield a truncation notice before resuming, never a short history presented as complete.

---

## 8. Lifecycle

An activation runs through rehydration (storage §9.1), the activation hook, serving, idleness, the deactivation hook, an optional snapshot, and stop.

- **Activation:** starts when the first message for the name reaches its shard leader's gateway, and takes no consensus round, only one quorum head recovery on the quorum tier (**E8**).
- **Migration:** follows shard leadership (storage §6.3); in-memory state is never carried across.
- **Hibernation:** once the type's idle window (a per-type duration) passes with no command, the host MAY, if the hibernation veto permits, run the deactivation hook, snapshot, and stop. The idle snapshot MUST use the write path's trigger and MUST NOT be unconditional (storage §9.1). A veto, or a pending alarm the alarm index does not yet hold (facets §5.5), reschedules the check. The next command or subscription re-activates the entity with identical state (**E12**). A host SHOULD NOT evict and reload an entity on every touch when its touches arrive just slower than the idle window.
- **Forced step-down:** when leadership moves or the shard goes unavailable, the activation runs the deactivation hook, takes no snapshot, emits `Deactivated`, and stops.
- **Eviction races:** a command reaching a stopped cached host gets a dead letter, proof it never ran: the reference re-resolves and re-issues once (§3.2), and a second dead letter surfaces.
- **Pairing:** every `Activated` MUST be matched by exactly one `Deactivated` or by its node's loss.

---

## 9. Failure model

The actor failure model applies (actor §8.2), and a failure's blast radius is the shard. When a node fails, each shard it led elects a new leader (storage §6.3); every pending ask to an entity it hosted completes with a call error of kind unreachable, never a hang; watchers receive a termination for the node's loss (actor §10); and the next message re-activates each entity on the new leader, losing no acknowledged write (**E14**). A shard short of a quorum answers unavailable and never forks (storage §11).

---

## 10. Determinism

The entity runtime MUST meet the deterministic profile (actor §11). Every hash that places a name or a shard MUST agree on every node and across runs (storage §4.1).

---

## 11. Registries

**Entity event kinds:** emitted through the entity system (§3.3), each carrying the emitting node; the first five also carry the entity name. Subscriptions add no event that carries content.

| Kind | Emitted | Also carries |
|---|---|---|
| `Activated` | when an activation is ready for its first command (§8) | |
| `Rehydrated` | when an activation has rebuilt its state (storage §9.1) | whether a snapshot seeded it, the records replayed |
| `Committed` | at each commit, before the reply (§6) | the shard served under, the new head |
| `Snapshotted` | when a snapshot is persisted (storage §9.1) | its sequence number |
| `Deactivated` | when an activation ends (§8) | |
| `LeaderChanged` | by a shard's newly elected leader, once per term (storage §6) | the type name, the shard, the term |
| `ShardSplit` | by each node as it applies a committed split (storage §8) | the type name, the parent and child shards, the boundary |
| `ShardMerged` | by each node as it applies a committed merge (storage §8) | the type name, the left and right shards |

---

## 12. Invariants

A conforming implementation upholds every row; **E10** holds at compile time.

| # | Invariant | Defined in |
|---|---|---|
| **E1** | **Single writer per entity.** Only the shard leader appends, under its term, and a deposed leader reaches no quorum, so a record never forks. A commit advances the head by exactly the batch appended, and the host folds only such a commit. | §6, storage §6 |
| **E2** | **Deterministic fold.** The fold yields identical state on live commit and on replay from any snapshot and journal prefix. | §4.1 |
| **E3** | **The journal is the source of truth.** An activation's head and state derive only from journal and snapshot returns, never from a prior activation's memory. | §1, storage §9 |
| **E4** | **A snapshot never shortens the log.** A head is the best snapshot's sequence number plus the contiguous records above it, and the host refuses a snapshot the head does not cover. | storage §9 |
| **E5** | **Reply iff durable.** A reply is released, and its events folded, only after its batch commits; a not-leader or unavailable outcome yields an error, no fold, and no success reply. | §6 |
| **E6** | **Exactly-once activation per node.** A node activates a name at most once; concurrent requests find the same host. | §5.3 |
| **E7** | **Bounded consensus groups.** O(shards) plus O(entity types) groups, bounded by split and merge, never O(entities) and never one, and none on the data path. | storage §4.1, §5, §7, §8 |
| **E8** | **Activation without consensus.** Activating or hibernating takes no consensus round; on the quorum tier it pays at most one quorum head recovery. | §5.2, storage §4.2, entity §8 |
| **E9** | **Control plane off the data path.** The shard map changes only on cluster events; no write or activation contacts it. | storage §7 |
| **E10** | **Type-safe calls.** A command an entity has no `EntityHandler` for does not compile. | §3.2 |
| **E11** | **CP under partition.** A shard short of a quorum pauses its writes (unavailable) and never forks; other shards serve. | storage §11 |
| **E12** | **Hibernation round-trip.** An entity evicted on idle re-activates with identical state through snapshot and replay. | storage §9, entity §8 |
| **E13** | **Location transparency.** A call to a local or a remote entity yields observably identical replies and ordering. | §5.4 |
| **E14** | **Lossless failover.** A head recovered under a confirmed quorum contains every record any earlier term reported committed, at its original sequence number and bytes, so no acknowledged write is lost across a leadership change. | storage §6, §6.3 |
| **E15** | **Split and merge safety.** An entity is writable in exactly one shard at any time; a split or merge loses or duplicates no write. | storage §8 |
| **E16** | **Subscriptions are observational and lossless by sequence number.** Delivery never gates a commit, forks state, or advances a head, and a sink reconciling by sequence number reconstructs exactly the committed sequence. | §7.3 |
| **E17** | **Blob address integrity.** A blob read returns bytes whose BLAKE3 hash is the requested id, or an error, verified after any transfer. | storage §10 |
| **E18** | **Blob durability and entity-scoped reclamation.** A blob put is acknowledged only once on a write quorum of the entity's replicas that includes the leader (the one store on the local tier), with no consensus on the path; equal content stores once; reclamation is entity-scoped, idempotent, and never removes a referenced blob. | storage §10 |
| **E19** | **Facet atomicity and interpretation safety.** A command's records across all facets commit as one batch or not at all; replay dispatches by tag and aborts on an unknown one; the composite snapshot restores every facet to one sequence number. | facets §4 |
| **E20** | **Physical facets expose only durable state.** A physical facet's uncommitted local mutations are unobservable to entity-level observers and discarded on any outcome but a commit; rehydration reproduces the form exactly. | facets §4, §5.3 |
| **E21** | **An alarm fires at most once per arm.** One armed deadline commits at most one alarm decision; a fire whose commit fails leaves it pending. | facets §5.5 |

---

## 13. Security considerations

Entities add no transport and no authentication: transport security, peer authentication, and per-envelope authorization are the cluster's (cluster §11).

- **Deserialization:** only the command allowlist builds an entity command type from network bytes (§5.5).
- **Authorization:** a remote command is an actor envelope under its own manifest (§5.5), so an actor authorizer (cluster §3.6) can gate entity commands by peer and manifest; the entity runtime consults no authorizer of its own and cannot gate by entity name.
- **Observation and activation:** the subscription, activation, and event-read requests bypass the allowlist (§5.5), so any peer the actor layer admits can activate a name and read its committed events unless the authorizer refuses them.
