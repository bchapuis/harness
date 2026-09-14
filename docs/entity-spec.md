# Entity: Durable Objects for the Distributed Actor Framework

**Status:** Draft v3
**Scope:** An **entity**: a virtual, durable, single-activation object addressed by a global name, with colocated event-sourced storage and a durability barrier on the reply path, built on the actor framework of [`distributed-actor-spec.md`](distributed-actor-spec.md).

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Throughout, `entity` is the crate and namespace name and an entity is the durable object. A `§` with no document name refers to a section of *this* spec, and `actor §N` refers to [`distributed-actor-spec.md`](distributed-actor-spec.md). The design lessons come from [`../research/durable-objects.md`](../research/durable-objects.md) (cited as **DO §N**).

> **Design stance.** An entity is an **actor plus three things**: a name-based virtual identity, a durable event-sourced journal, and a durability barrier on the reply. It inherits everything else unchanged from the actor framework (mailboxes, serial execution, location-transparent `ask`/`tell`, membership, failure detection, supervision, death watch, the receptionist; actor §3–§13). The layer adds no new transport and no required macros (serde derives in user code are the only ones, as in actor §1.1).
>
> The architecture rests on one idea: **each entity is its own single writer, so its journal replicates independently, with no shared log and no consensus on the data path.** An entity's records are already totally ordered, because one writer assigns each the next `Seq`; durability is then a quorum append to the entity's replicas, with no agreement round per write (§7.2). Only *placement* needs consensus, meaning which node may write a shard's entities: a small per-shard Raft group elects one leader per term and holds nothing but leadership, term, and the replica set (§7.1, §8). The cluster runs O(shards) of these leader-election groups plus O(entity types) shard-map groups (§7.6), kept bounded by splitting a shard that grows hot or large (§7.7), never one group per entity and never one global log (DO §4.3–§4.4).
>
> **Storage** takes the same shape. The substrate is two replication primitives (per-entity ordered, term-fenced records, §7.2, and immutable content-addressed blobs, §7.10), and every further durable feature is a **facet**: an interpretation layered over them (§7.12), such as the KV map (§7.13), the workspace directory (§7.11), and the SQLite database (§7.14). Many storage APIs above one substrate, never a new replication mechanism per feature (DO §2, §6).

---

## 1. What an entity is

An **entity** is a unit of single-threaded compute fused with strongly-consistent, durable, colocated storage, addressed by a globally-unique **name**. At most one live **activation** of an entity exists in the cluster at a time; every message for the name routes to that activation. The runtime creates the activation on first access, migrates it, and evicts it when idle; its durable state survives all of these. The identity is permanent; the activation is disposable.

This is the actor model (actor §3) specialized in four ways:

| | Actor (actor spec) | Entity (this spec) |
|---|---|---|
| Identity | `ActorId` = node + path + incarnation, minted at `spawn` | a stable user-chosen **`EntityName`**; the activation's `ActorId` is incidental |
| Lifecycle | explicit `spawn`/stop | **virtual**: activate on first message, hibernate on idle (§10) |
| State | in-memory only, lost on stop | **event-sourced journal + snapshots**, source of truth (§7, §9) |
| Reply | synchronous, no durability barrier | held until the producing events are **durable** (§6) |

An entity's in-memory activation is a **cache** of state folded from the journal. The journal, never memory, is the source of truth (§7, invariant **E3**).

---

## 2. Goals and non-goals

### 2.1 Goals
- **Global name, single activation.** Address an entity by a stable name from any node; the runtime guarantees at most one live activation and routes to it (§5).
- **Durable by construction.** A handler's effects become visible only after the events it produced are durable (§6). A crash loses no acknowledged write.
- **Strong consistency that scales with the cluster, not the object count.** Consensus is sharded (§7). The cluster runs O(shards) Raft groups and splits them as it grows; no single cluster-wide log sits on the write path, and no entity has a consensus group of its own. Activating an entity takes no consensus at all (§10).
- **Single-writer safety under partition.** An entity never forks. The shard leader is its only writer, and an append fenced by a stale term cannot reach a quorum, so two leaders never both commit (§8).
- **Pluggability.** The journal is a seam with two reference *durability tiers*, single-node `Local` and clustered `Quorum` (§7.4), over a per-shard leader-election group for placement (§7.1) and per-entity quorum replication for durability (§7.2), as transport and the control plane are pluggable in the actor framework.
- **One substrate, many facets.** Durable features beyond the event fold are **facets**: the KV map (§7.13), the workspace directory (§7.11), the SQLite database (§7.14), the disk image (§7.15), the alarm deadline (§7.16), the steps memo (§7.17). Each is an interpretation over the two storage primitives (ordered records and immutable blobs), sharing one journal, one atomic commit per command, and one composite snapshot (§7.12). A new feature MUST NOT add a third replication mechanism.
- **One programming model.** The handler is ordinary sequential Rust; the input and output gates (§6) supply atomicity-on-the-outside and durability-before-effects with no explicit locking.

### 2.2 Non-goals
- **A single cluster-wide log.** Entity MUST NOT serialize all entities through one Raft/Paxos group; sharding (§7) exists to avoid that bottleneck.
- **A consensus group per entity.** Entity MUST NOT run one Raft group per entity; the unit of consensus is the shard's leader-election group, which covers many entities, and their data needs no consensus at all, only a per-entity quorum append (§7.1, §7.2).
- **Cross-entity transactions.** Each entity is its own consistency boundary. A workflow spanning entities is built above this layer with sagas/idempotency, not by the runtime (§16).
- **Exactly-once side effects.** Inherited from actor §7.2: delivery is at-most-once and the framework never auto-retries effectful messages. Idempotency is the caller's responsibility.
- **Availability of the write path during quorum loss.** An entity whose shard cannot reach a quorum pauses writes (returns `Unavailable`) rather than forking (§11). It is CP, not AP.
- **Required codegen.** As in actor §1.1, an optional `#[derive(Entity)]` MAY default the manifest/`register` boilerplate, but the hand-written form is normative.

---

## 3. Terminology

| Term | Definition |
|---|---|
| **Entity** | A virtual, durable, single-activation object: state + behavior addressed by an `EntityName`. |
| **`EntityName`** | The stable, cluster-wide, serializable identity of an entity. A `(entity_type, key)` pair; `key` is an arbitrary application string. |
| **Shard** | A partition of one entity type's namespace and the unit of *placement*: a small per-shard Raft group (the **leader-election group**) owning leadership and term, and holding as its Raft membership the *realized* replica set for that type's entities whose names fall in its range, holding *no* entity data (§7.1, §8). The entities' data is durable by per-entity quorum append, not a shared log. |
| **Shard leader** | The single node elected by a shard's leader-election group to write and host every entity in the shard's range, for one term (§5.2, §8). |
| **Replicator** | The per-entity durability mechanism: it quorum-appends an entity's records to the shard's replicas, fenced by the shard term, and recovers an entity's head from a quorum on activation. Two **durability tiers**: single-node `Local`, clustered `Quorum` (§7.2, §7.4). |
| **Shard map** | The cluster's consensus-agreed record of the *intended* allocation: which nodes should replicate each shard and which key range it covers. A per-entity-type map group seeded from the leader-based control plane, reconciled into each shard's realized membership (§7.6). |
| **Activation** | The live, in-memory instance of an entity on the leader of its shard. Disposable; rebuilt from the journal. |
| **Event** | A serializable value appended to an entity's journal; the unit of durable change. |
| **`apply`** | The pure fold that applies an event to state. Runs identically on live commit and on replay. |
| **State** | The value obtained by folding an entity's events; the snapshot payload. A cache of the journal. |
| **EntityJournal** | An entity's durable, totally-ordered, append-only log of events, quorum-appended by its **Replicator** to the shard's replicas (§7.3). The source of truth. |
| **Snapshot** | A persisted `(Seq, state)` checkpoint that bounds replay cost (§9); under the facet model, the composite of every facet's contribution at one seq (§7.12). |
| **Facet** | A durable storage feature of one entity (the event fold, the KV map §7.13, the workspace §7.11, the SQL database §7.14, the disk image §7.15, the alarm §7.16, the steps memo §7.17), defined entirely as an interpretation over the two storage primitives: what its records mean, what it keeps in blobs, what it contributes to the snapshot (§7.12). |
| **Facet tag** | The stable identifier a multi-facet entity's records carry, dispatching each record to its facet on replay; an unrecognized tag aborts activation (§7.12). Tags are `0..=0x7F`, the high bit being reserved as the record envelope's version mark (§9). |
| **`Seq`** | The position of an event in one entity's total order; first event at 1. |
| **Base** | An entity's compaction floor: the highest `Seq` its store has truncated behind a snapshot. Slots at or below it are gone from the log and live only in the snapshot, so every event read reports it (§7.5, §9). |
| **Event projection** | The view of the journal an event read returns: `EntityRef::events` in full, the subscription sink's record filter in part (§7.5). |
| **Durability tier** | Which Replicator a deployment runs, `Local` or `Quorum` (§7.4). Unrelated to the agent runtime's *capability* tiers (agent §5.6), which share only the word. |
| **Quorum** | The majority of a shard's replicas whose acknowledgment makes a per-entity append durable (§7.2), or commits a leader-election entry. |
| **Term** | The shard's leadership term; it increases on every leadership change and is the single-writer fencing token every append carries (§8). |
| **Gateway** | The node-local actor that routes a call to the right shard leader and owns the activation table for shards this node leads (§5.3). |
| **Host** | The per-entity actor on the shard leader: holds state and head, and drives the durability protocol (§6, §10). |

---

## 4. The entity model

### 4.1 The `Entity` trait

An entity is event-sourced. The author implements the **behavior** (immutable configuration) as a type, declares the **state** and **event** types, and writes the pure fold:

```rust
pub trait Entity: Sized + Send + 'static {
    /// The system this entity's activation runs on: an `EntitySystem`, a system
    /// that can host entities (an `ActorSystem`, actor §4, plus the entity host's
    /// own seams). `LocalSystem` implements it for the `Local` tier, `ClusterSystem`
    /// for the shard-hosting clustered one.
    type System: EntitySystem;

    /// The folded state and snapshot payload. Rebuilt from the journal on activation.
    type State: SerializationRequirement + Default;

    /// The journal record type: the unit of durable change.
    type Event: SerializationRequirement;

    /// The entity's declared facet set (§7.12): `()` for none, or a tuple of
    /// built-in facets, e.g. `(Kv,)`, `(Kv, Ws)`. Each declared facet surfaces a
    /// compile-time-gated `EntityCtx` accessor, contributes tagged records to the
    /// same atomic per-command batch (E19), joins the composite snapshot, and adds
    /// its blob roots to the entity's unioned live set.
    type Facets: FacetSet;

    /// The entity type's stable, serializable identity: the namespace tag in every
    /// `EntityName` of this type (§5.1) and the receptionist key the type's gateway
    /// is discovered under (§5.3). An explicit constant (e.g. `"bank.Account"`) is
    /// REQUIRED: the runtime needs a rename-stable tag, which `type_name` is not.
    /// It is the *default* type name; one Rust entity MAY be hosted under several
    /// runtime type names via `entities_named` (Appendix A), in which case this
    /// const is the fallback a wire-decoded `EntityRef` recovers.
    const ENTITY_TYPE: &'static str;

    /// Apply one event to state. MUST be pure and deterministic: it runs on the
    /// live commit path AND on replay/rehydration, and the two MUST agree
    /// (invariant E2). It MUST NOT perform I/O, read the clock, or use entropy.
    fn apply(state: &mut Self::State, event: &Self::Event);

    /// List the command messages this entity accepts over the network (§5.5).
    /// Mirrors `Actor::register` (actor §4.4); the default registers nothing.
    fn register(_r: &mut EntityRegistry<Self>) {}

    /// Called once after the activation has rehydrated, before the first command.
    /// Returning Err aborts activation (the message that triggered it fails).
    fn on_activate(&mut self, _ctx: &EntityCtx<Self>)
        -> impl Future<Output = Result<(), BoxError>> + Send { async { Ok(()) } }

    /// Called once before the activation deactivates: idle eviction, handoff, OR
    /// a forced step-down (leadership move, quorum loss). Runs even when the
    /// journal is unwritable, since it cannot persist (the ctx exposes no
    /// `persist`); a layered runtime uses it to release non-durable per-activation
    /// resources (e.g. the agent runtime's sandbox, agent §5.3).
    fn on_deactivate(&mut self, _ctx: &EntityCtx<Self>)
        -> impl Future<Output = ()> + Send { async {} }

    /// Whether the activation MAY idle-hibernate now (§10). Consulted only on idle
    /// eviction; a forced step-down ignores it. The default always permits it. An
    /// entity with autonomous, not-yet-journaled work (the agent runtime's live
    /// run) overrides this to veto eviction until the work settles; the host
    /// reschedules the idle check rather than evicting mid-flight.
    fn can_hibernate(&self, _state: &Self::State) -> bool { true }

    /// The two callerless hooks, both defaulted to a no-op and both decisions
    /// through the §6 barrier (return the events to journal; an empty result
    /// commits nothing, §7.5). Specified where their machinery is: `on_alarm`
    /// with the alarm facet (§7.16), `on_peer_terminated` with death watch (§4.3).
    /// `on_alarm` is not gated by the declared facet set: an entity that never
    /// declares the alarm facet may implement it and is never called (§7.16).
    fn on_alarm(&self, state: &Self::State, ctx: &EntityCtx<Self>)
        -> impl Future<Output = Vec<Self::Event>> + Send { async { Vec::new() } }
    fn on_peer_terminated(&self, state: &Self::State, ctx: &EntityCtx<Self>,
                          peer: &ActorId, reason: TerminationReason)
        -> impl Future<Output = Vec<Self::Event>> + Send { async { Vec::new() } }
}
```

`SerializationRequirement` is the actor framework's bound (actor §5); the codec is the system's, not the entity's.

### 4.2 Commands and the decide/apply split

A **command** is a `Message` (actor §3.2): a serializable request with a declared `Reply` and a stable `MANIFEST`. An entity accepts a command by implementing `EntityHandler<M>`:

```rust
pub trait EntityHandler<M: Message>: Entity {
    /// Decide the outcome of a command: inspect the current state, and return
    /// the events to persist together with the reply.
    ///
    /// This is a *decision*, not a mutation: it MUST NOT mutate state directly
    /// (state changes only through `apply`, §4.1) and MUST NOT perform durable
    /// I/O (the host owns persistence, §6). A read-only command returns no
    /// events: `(vec![], reply)`, which commits nothing (§7.5).
    fn handle(&self, state: &Self::State, msg: M, ctx: &EntityCtx<Self>)
        -> impl Future<Output = (Vec<Self::Event>, M::Reply)> + Send;
}
```

This **decide/apply split** is normative, the alternative to an imperative `ctx.persist(event).await` API. The reasons:

1. **No interior mutability.** The host actor owns the activation's state and journal head and mutates them only through its `&mut self` on the serial executor (actor §6). The split keeps that the only writer; an imperative `persist` under the shared `&EntityCtx` would force an `Arc<Mutex<…>>` over state, re-introducing the sharing the actor model removes (actor §3.5).
2. **`Send` falls out for free.** Handler futures must be `Send` (actor §3.2); with the split, nothing crosses an `.await` but the user's own values.
3. **Atomic batch per command.** All of a command's events commit in one atomic append (§7.3), so no observer ever sees a partial command.
4. **The durability barrier lives in exactly one place**, the host (§6), not scattered across handlers.

**Facets stage; the commit applies (§7.12).** On an entity with facets beyond the event fold, the handler also works imperatively through the facet accessors: `ctx.kv()`, `ctx.ws()`, `ctx.sql()`. A *logical* facet stages operations in a per-command overlay: reads see committed-plus-staged, and the staged operations become the command's tagged records, folded only on commit. A *physical* facet mutates a local materialization that is a rebuildable cache (§1), captured into records before the append and discarded outright on any non-committed outcome (§7.11, §7.14). Either way, committed, observable state changes only at the commit point.

A reply that must reflect post-command state computes it from `&State` plus the events the handler is emitting. Where this is awkward, the host MAY expose a non-normative convenience that folds the emitted events into a scratch copy of state and passes it to a reply closure.

**Application errors are values.** As in actor §3.2/§14, a fallible command uses `type Reply = Result<T, E>`. An application failure is a value inside the reply, distinct from a transport failure (`CallError`) or a durability failure (`EntityError`, §12).

Example:

```rust
pub struct Account;                                   // behavior: stateless config here
#[derive(Default, Serialize, Deserialize)] pub struct Balance { cents: i64 }
#[derive(Serialize, Deserialize)] pub enum Ledger { Deposited(u64), Withdrew(u64) }
pub struct Overdraft;                                 // an application error (lives inside M::Reply)

impl Entity for Account {
    type System = ClusterSystem;
    type State  = Balance;
    type Event  = Ledger;
    type Facets = ();                                 // no facets beyond the event fold (§7.12)
    const ENTITY_TYPE: &'static str = "bank.Account"; // the rename-stable type tag (§5.1)
    fn apply(state: &mut Balance, e: &Ledger) {
        match e { Ledger::Deposited(n) => state.cents += *n as i64,
                  Ledger::Withdrew(n)  => state.cents -= *n as i64 }
    }
    fn register(r: &mut EntityRegistry<Self>) { r.accept::<Withdraw>(); r.accept::<Deposit>(); }
}

#[derive(Serialize, Deserialize)] pub struct Withdraw { cents: u64 }
impl Message for Withdraw { type Reply = Result<i64, Overdraft>; const MANIFEST: Manifest = Manifest::new("bank.Withdraw"); }

impl EntityHandler<Withdraw> for Account {
    async fn handle(&self, state: &Balance, msg: Withdraw, _: &EntityCtx<Self>)
        -> (Vec<Ledger>, Result<i64, Overdraft>) {
        if (state.cents as u64) < msg.cents { return (vec![], Err(Overdraft)); }   // no event, nothing to commit
        (vec![Ledger::Withdrew(msg.cents)], Ok(state.cents - msg.cents as i64))    // reply reflects post-state
    }
}

#[derive(Serialize, Deserialize)] pub struct Deposit { cents: u64 }
impl Message for Deposit { type Reply = i64; const MANIFEST: Manifest = Manifest::new("bank.Deposit"); }

impl EntityHandler<Deposit> for Account {
    async fn handle(&self, state: &Balance, msg: Deposit, _: &EntityCtx<Self>) -> (Vec<Ledger>, i64) {
        (vec![Ledger::Deposited(msg.cents)], state.cents + msg.cents as i64)       // infallible: always one event
    }
}
```

### 4.3 `EntityRef` and `EntityCtx`

`EntityRef<G>` is the only handle to an entity. It is `Clone + Serialize + DeserializeOwned + Send + Sync`, carries the `EntityName` plus a system handle, and never grants access to state (the actor `ActorRef` discipline, actor §3.3):

```rust
impl<G: Entity> EntityRef<G> {
    pub fn name(&self) -> &EntityName;
    pub async fn ask<M>(&self, msg: M)  -> Result<M::Reply, EntityError> where G: EntityHandler<M>, M: Clone;
    pub async fn tell<M>(&self, msg: M) -> Result<(), EntityError>       where G: EntityHandler<M>, M: Clone;
    pub async fn ask_timeout<M>(&self, msg: M, within: Duration) -> Result<M::Reply, EntityError> where G: EntityHandler<M>, M: Clone;

    /// Subscribe to the entity's committed records, returning the committed head
    /// at registration and a stream of the record batches committed after it
    /// (§7.9). `from` is the subscriber's exclusive lower bound; the host does
    /// not consult it, so the subscriber backfills `from`..`head` by `events`.
    /// A framework built-in, available for every entity type without the author
    /// registering it: the analogue of `load`/`head`, surfaced as a push.
    pub async fn subscribe(&self, from: Seq) -> Result<Subscription<G>, EntityError>;

    /// Up to `limit` of the entity's committed events after `from` (exclusive),
    /// decoded, in ascending `Seq` order, served by the shard leader's gateway
    /// straight from the journal
    /// **without get-or-activating the entity** (§7.5): polling a hibernated
    /// entity leaves it hibernated (§10). A framework built-in like `subscribe`.
    /// Returns the event projection (§7.5): real `Seq`s with facet gaps, a
    /// short page only at the head, and the compaction `base` on every reply.
    pub async fn events(&self, from: Seq, limit: usize)
        -> Result<EventHistory<G::Event>, EntityError>;
}

/// A completed event read: the decoded events, plus the highest compaction
/// `base` the read observed (§9). An absent slot at or below `base` is
/// truncated history (subsumed by the entity's snapshot), never one of the
/// interleaved records the projection skips (§7.12); `base <= from` means
/// nothing the caller asked after was compacted.
pub struct EventHistory<E> {
    pub base: Seq,
    pub events: Vec<(Seq, E)>,
}
```

The `G: EntityHandler<M>` bound proves at compile time that the entity accepts `M`, so an invalid call does not compile (invariant **E10**, the entity analogue of actor §3.3). The call site is identical whether the activation is local or on another node (§5). `subscribe` and `events` are entity-type-agnostic and so carry no such bound (§7.9, §7.5).

The `M: Clone` bound lets the runtime re-issue the command when the first attempt provably did not run: a stale cached host that hibernated (`DeadLetter`) or whose leadership moved (`NotLeader`), neither of which commits (§6, §8). An *ambiguous* transport failure (`Unreachable`/`Timeout`) is never auto-retried, because the command may have committed before the reply was lost (at-most-once, §2.2). The clone is of the caller's own small command value.

`EntityCtx<G>` is the handler/lifecycle context. It exposes the entity's name, its colocated blob area (the content-addressed blob primitive, §7.10), an `EntityRef` self-reference, the system handle, and death watch. (Surfacing the full actor `Ctx` underneath for the remaining inherited capabilities, such as child spawning, is a deferred addition, §16; the context exposes the five accessors below, plus one accessor per declared facet, §7.12.)

```rust
impl<G: Entity> EntityCtx<G> {
    fn name(&self) -> &EntityName;
    fn blobs(&self) -> EntityBlobs;       // the entity's content-addressed blob area (§7.10)
    fn this(&self) -> EntityRef<G>;
    fn system(&self) -> &G::System;
    /// Death-watch an actor by id (actor §12): when it terminates, including
    /// its node going down, the entity's `on_peer_terminated` runs as a
    /// callerless decision through the §6 barrier, the alarm protocol's
    /// sibling (§7.16). Queued here; the host installs it after the current
    /// handler completes. Watches are activation-local: a rehydrated entity
    /// re-watches from its folded state, and watch-after-death fires
    /// immediately, reconciling what died while it slept.
    fn watch(&self, target: ActorId);
}
```

`EntityCtx` exposes **no** `persist` method and no state mutation (§4.2); state changes only through events folded by `apply`. Under the facet model it additionally exposes one accessor per facet the entity declares (`kv()`, §7.13; `ws()`, §7.11; `sql()`, §7.14; `disk()`, §7.15; `alarm()`, §7.16; `steps()`, §7.17), each gated at compile time by the entity's facet set (§7.12); a facet accessor stages, it never mutates committed state (§4.2).

Beside `on_alarm` (§7.16), the `Entity` trait carries one more callerless hook, defaulted to a no-op: `on_peer_terminated(&self, state, ctx, peer: &ActorId, reason) -> Vec<Event>`, invoked when an actor the entity watched terminates. It runs through the same §6 barrier as any command (one atomic batch, an empty decision commits nothing, §7.5), and is the basis for liveness-coupled durable state: the machine folds `Detached { FrontDoorLost }` when the front-door member holding an attachment dies (machine §5.1).

---

## 5. Virtual activation and routing

### 5.1 Names and shards

An entity is addressed by `EntityName`, a `(entity_type, key)` pair where `key` is an arbitrary application-chosen string (`"account/42"`, a UUID, a tenant id). Unlike an `ActorId` (actor §3.6), an `EntityName` is **not** locality-classifiable on its own: it names a logical object, not a node.

Every name maps to exactly one **shard** of its entity type, by a stable hash of the name onto a key-range partition (§7.1). The mapping changes only when a shard splits or merges (§7.7), not when nodes come and go, so it is steadier than a direct name-to-node mapping. A second lookup, the shard map (§7.6), gives the shard's current leader. Resolution is two levels: name to shard (stable), shard to leader (changes on Raft elections).

`EntityName` is `Clone + Eq + Hash + Send + Sync + Serialize + DeserializeOwned`.

### 5.2 Where an entity lives

An entity activates on the **leader of its shard**. The leader is one of the shard's replicas, so the Replicator fans a record to the shard's peers and writes the leader's own replica concurrently; the local write counts as one more replica's acknowledgement toward the quorum and is always awaited before the append returns (§7.2). A blob put lands locally first and then fans out (§7.10). Rehydration confirms the entity's head from a quorum and serves subsequent reads locally (§9). The leader hosts the compute for every active entity in its shard.

Leadership, and therefore activation placement, follows the shard's Raft election. When leadership moves, the new leader serves the shard's entities; activations rehydrate there lazily on first access (§10). A node that leads many shards hosts many entities; the cluster balances load by moving shard leadership and by splitting shards (§7.7), not by moving individual entities.

### 5.3 The gateway and exactly-once activation

Each node runs one **gateway** actor per entity *type*, registered in the receptionist (actor §13) under a single well-known key for that type, one gateway entry per node. The gateway routes inbound calls (§5.4) and, for shards this node leads, owns the **activation table** mapping `EntityName → Host`.

A gateway is mandatory: the runtime mints an activation's `ActorId` at `spawn` (actor §4.2), and that id is **not** derivable from the `EntityName`, so the name-to-activation mapping must live in an explicit table, and the gateway is that table plus the actor that guards it.

Activation is **exactly-once per node by construction.** The gateway is a serial actor (actor §6), so getting-or-activating a name is a critical section: the gateway processes two concurrent requests for a not-yet-active name in sequence, the first activating the host, the second finding it. No lock is needed (invariant **E6**).

Beside routing, the gateway serves the **non-activating event read** (§7.5): one bounded page of an entity's committed events straight from the shard's journal, touching no host. Because it is a page bounded server-side and the leader's `load` is a local read (§7.3), serving it inline costs the serial gateway one bounded local copy, never I/O it must wait out; the one slow step the read can need, a hibernated entity's committed-head recovery (§7.5), runs on a spawned task while the caller retries.

### 5.4 Routing a call

`EntityRef::ask<M>(msg)` resolves in two levels:

1. **Name to shard** by the stable hash (§5.1).
2. **Shard to host.** A handle minted by an `Entities` on this node keeps a bounded cache of the hosts it has reached. On a node that replicates the shard the current leader is a local read, so a cached host that no longer sits on it is dropped before use; elsewhere a cached host is used until an outcome shows it stale. A miss goes to a gateway, this node's first.
3. **Send the command to the host.** The gateway is asked for the *name* only: it gets-or-activates the host and answers with that host's `ActorRef`. The command itself never passes through the gateway; it goes to the host actor.
   - **Local leader:** the typed command goes straight into the local host's mailbox. No serialization (the local fast path, actor §4.3).
   - **Remote leader:** the command crosses as one envelope carrying its `MANIFEST` and codec-encoded payload, addressed to the host actor on the leader's node, where the host's own handler registry (§5.5) decodes it, invokes the typed handler, and encodes the reply.
4. **Stale leader:** if the target node no longer leads the shard, it replies `NotLeader(hint)`. The caller follows the hint to that node's gateway, discovered in the receptionist (§5.3), backs off while the shard elects, and re-discovers another gateway when its target is unreachable, all within the caller's own deadline. This is ordinary Raft client redirection.

Consequences of caching (§7.8):
- The gateway serializes only the activation-table mutation. Once a caller has cached the leader and the host is active, repeated calls go straight to the host actor (a normal mailbox), so the serial gateway is off the steady-state hot path.
- Routing reads no shard map on a node that does not replicate the shard: a stale entry is found by the `NotLeader`, `DeadLetter` or `Unreachable` outcome it produces, so routing costs no control-plane traffic in steady state.
- The host cache MUST be **bounded** (`host_cache_capacity`, Appendix A). It is keyed per entity *name*, and a long-lived client (the gateway fronts every tenant) would otherwise hold one entry for every name it has ever addressed. A miss costs one gateway round trip and nothing else, so the bound is set by what an entry costs rather than by how many are expected live: an unlucky workload must not be able to consume the node.

### 5.5 The entity dispatch registry

`EntityRegistry<G>` is the set of command manifests an entity accepts, plus one thunk per accepted type that accepts the command on the **host actor's** own `HandlerRegistry` (actor §4.4). `Entity::register` fills it via `r.accept::<M>()`, exactly as `Actor::register` does, and hosting the type replays the thunks onto the host. Decoding the payload, invoking the typed handler, and encoding the reply are therefore the actor framework's dispatch, unchanged: the entity registry says which commands enter it, and adds no second dispatch path. It is the deserialization allowlist for entity commands (actor §5, §15): a name-addressed command whose manifest is unregistered yields `EntityError::Call(CallError::Unhandled)`, and no type outside the registry is ever built from network bytes.

A command reaches the host wrapped in one generic carrier whose manifest is the command's own, so the wire key is the command manifest the registry is written in.

---

## 6. The consistency model: input and output gates

The entity layer lifts the Durable Objects gate model (DO §3) onto the framework, so that ordinary sequential handler code is race-free and durable.

- **Input gate.** While an entity's append is in flight, the host admits **no new command** to that entity. New messages queue in the mailbox until the host has no outstanding append. This closes the window a plain serial executor leaves open *at `.await` points* (actor §6): a second command cannot observe half-committed state mid-handler.
- **Output gate.** When a command produces events, the host holds the **reply** until those events are durable on a quorum (§7). The reply travels through the actor `ReplyHandle::send` (actor §4.5) only after the commit. If the commit fails, the reply becomes an `EntityError` (§12); no observer is ever told an effect happened that did not durably happen.

The host's per-command protocol runs in order:

```
1. (events, reply) = G::handle(&state, msg, ctx)        // decide; no mutation, no I/O
2. if events.is_empty() { return reply }                // read path: nothing to commit (§7.5)
3. outcome = journal.append(entity = name, after = head, events)  // §7.3, per-entity quorum append, fenced by the shard term
4. on Committed(new_head):                              // durable on a quorum
       if new_head != head + events.len() {             // CONTIGUITY GUARD (E1/E3)
           step_down(); return EntityError::Unavailable  //   head was stale: re-read, do not fold
       }
       for e in &events { G::apply(&mut state, e) }     // fold AFTER durability
       head = new_head
       maybe_snapshot()                                 // §9
       return reply                                     // OUTPUT GATE releases here
5. on NotLeader(hint):                                  // leadership moved off this node (§8)
       step_down(); return EntityError::NotLeader(hint)  // caller retries against the new leader
6. on Unavailable:                                      // shard quorum lost OR commit timed out (§11)
       step_down(); return EntityError::Unavailable      // ambiguous fate: re-read; caller retries
```

Three ordering rules are normative (invariant **E1**):
- **Fold only after durability.** The host MUST NOT fold state before the entry commits. A handler observes its own prior writes because each completed command folded only on commit.
- **Reply only after durability.** The host MUST NOT release the reply before the entry commits, and MUST NOT release it as success if the entry does not commit.
- **Fold only a contiguous head.** An entity is its shard's single writer (§8), so a commit MUST advance its head by exactly the batch just appended. Recovery establishes a quorum-durable head before the first append (§9), so a non-contiguous return is unreachable in normal operation and this guard is a backstop against residue such as a previously timed-out append surfacing late (§7.2). If the journal ever returns a head that jumps further, the host MUST NOT fold (the intervening committed events were never applied to its state); it steps down and the next access rehydrates from the journal authority (**E3**). The host treats `NotLeader` and `Unavailable` the same way: any outcome other than a contiguous commit ends the activation, because after it the in-memory head can no longer be trusted. A forced step-down emits `Deactivated` (§13) so the lifecycle stream stays balanced.

**With facets, the protocol generalizes without adding a barrier (§7.12).** Step 1 also drains every declared facet's stage (a logical facet's overlay operations, a physical facet's captured delta) into tagged records that join the events in step 3's single atomic batch (**E19**); step 2's read path is a command whose batch is empty after the drain. Step 4 folds the logical stages and keeps the physical materializations; steps 5–6 additionally discard every physical facet's materialization, a rebuildable cache the next activation reconstructs (**E20**, §7.14).

**`tell` is fire-and-forget.** A `tell` returns once the host accepts the command, not after the commit (actor §3.3, §7.2), so it reports only the enqueue-time failures (`Call`, an unregistered manifest included, and `NotLeader`), never `Unavailable`. The host runs the same protocol for the command, but the caller does not await the outcome: if the commit cannot complete, the command has no effect and the caller is not notified. This is the at-most-once contract (actor §7.2), which callers make idempotent where it matters.

---

## 7. Durability, replication, and facets

Two layers:

- **§7.1–§7.10, the substrate.** How an entity's bytes become durable: shards and placement (§7.1), the per-entity quorum append (§7.2), the journal seam and its durability tiers (§7.3, §7.4), reads (§7.5), the shard map and its elasticity (§7.6–§7.8), record subscriptions (§7.9), and the second storage primitive, the content-addressed blob area (§7.10). This is the layer that has a fence, a quorum, and a recovery protocol.
- **§7.11–§7.17, the facets.** What is built on it: the facet model (§7.12) and the built-in facets under it: the workspace directory (§7.11), the KV map (§7.13), the SQLite database (§7.14), the disk image (§7.15), the alarm deadline (§7.16), the steps memo (§7.17). None of them adds a replication path, a fence, or a recovery protocol; §7.12 states the contract they share.

### 7.1 Shards: the unit of placement

The entity layer partitions each entity type's namespace into **shards**. A shard owns a contiguous range of its type's name hash space; a small per-shard Raft group of *R* replicas (`R=3` or `R=5`), the shard's **leader-election group**, elects a single leader per term and records the replica set (§8). This group holds *no entity data*, only leadership, term, and membership, so its log is tiny and changes only on failover and reconfiguration. The leader is the single writer for every entity in the range and hosts their activations (§5.2); each entity's records are made durable by a per-entity quorum append (§7.2), not by a shared log. Per-type shards keep the gateway, dispatch registry, and configuration (Appendix A) all keyed by one entity type, and let an entity replay through a single known `apply` (§4.1).

**Replica placement SHOULD be failure-domain aware.** A shard's *R* replicas chosen by rendezvous alone spread across the cluster but are blind to how the cluster is arranged, so nothing prevents all *R* landing in one rack or availability zone. Every durability claim here rests on quorum intersection over those *R* replicas (§7.2, §8, **E14**), so a concentrated shard turns the loss of one domain into an unavailable shard, or an unrecoverable one if only a minority survives, on a cluster whose other shards look healthy. A deployment that knows its domains SHOULD therefore supply the mapping, and the allocator then takes at most `ceil(R / domains)` replicas per domain, filling from the remaining rendezvous order when a lopsided layout cannot meet the cap (an imperfectly spread shard beats an under-replicated one). The mapping MUST be a function every node evaluates identically, because the allocation is agreed by every node computing the same rendezvous choice (§7.6); static deployment metadata satisfies this, anything node-local does not. Absent a mapping the choice stays domain-blind, which is the correct default only where the cluster has no distinct domains to spread over.

A shard's leader-election group covers many entities, so the cluster runs O(shards) of them, plus one map group per type (§7.6), bounded by splitting (§7.7), never O(entities), and none is on the data path.

A shard shares leadership and a replica set among its entities, not a serialization point: each entity's records are quorum-appended on their own, so a write-heavy entity never queues behind its shard-mates. Splitting a hot shard (§7.7) rebalances *placement and replication* load across nodes; it is not a knob for write bandwidth.

### 7.2 Ordering is free; durability is a quorum append

Each entity has a single writer, the shard's leader (§8), so ordering is free *per entity*: the host assigns each of an entity's records the next `Seq` with no agreement round, because no other writer proposes records for that entity. The entity's **Replicator** then fans the record out to the shard's replicas and reports it durable once a quorum has stored it (DO §4.3). The only agreement round, electing the shard leader, happens on failover (§8.3), never per write, and never across entities.

An entity's records are totally ordered among themselves but share no order with any other entity's: there is no shard-wide log to serialize them into. The host appends an entity's records in `Seq` order behind its input gate (§6), and replay reads them back in that order (§9); a second entity's appends proceed concurrently on their own quorum.

**Idempotent by sequence slot.** The leader awaits each append's quorum acknowledgment, bounded by a timeout that reports `Unavailable` (§11). A timed-out append MAY still reach a quorum later, but this never double-applies and needs no de-duplication token, because of the discipline §6 already mandates. The host does not retry an append and does not fold on any outcome but a contiguous commit: on a timeout it steps down without folding, and the next activation recovers the entity's head from a quorum (§9) and folds each record once, in `Seq` order. A record occupies exactly one `Seq` slot for its entity, so a late commit appears at its slot on recovery and is applied there once; a stale-term append racing a new leader is refused outright (the slot is filled and the term is lower, §8). The single-writer `Seq` is the record's identity; no separate proposal id or epoch is required.

**The rollback is term-bounded.** On any non-committed outcome the leader rolls back its own *tentative local* write (so a later recovery that falls back to the local view short of a quorum, `head` in §7.3, never folds an uncommitted record), and that rollback MUST drop only slots written **at or below the appending term**. While the failed append's quorum wait was in flight, a newer leader may already have fenced this store and landed committed records for the same entity above the same head (this node is still one of the shard's replicas), and those records carry the higher term. A term-blind rollback would silently delete them from this replica, shrinking a committed write's durability below a quorum (violating **E14**); the term bound makes it drop exactly the failed append's own slots and nothing newer.

### 7.3 The journal seam

The journal is a trait, a simulation and deployment seam like `Transport` and `Clock` (actor §4.6, §7), operating on opaque, codec-encoded event bytes so it stays codec-agnostic:

```rust
pub trait EntityJournal: Clone + Send + Sync + 'static {
    /// One atomic entry after `after`, committed on a quorum and fenced by the shard term (§7.2, §8).
    fn append(&self, entity: &EntityName, after: Seq, events: Vec<Vec<u8>>)
        -> impl Future<Output = AppendOutcome> + Send;
    /// Up to `limit` records after `from`, with the compaction `base` that explains a missing prefix (§9).
    fn load(&self, entity: &EntityName, from: Seq, limit: usize)
        -> impl Future<Output = Result<RecordPage, EntityJournalError>> + Send;
    /// The committed head, recovered from the shard's replicas: the rehydration barrier (§8, §9).
    fn head(&self, entity: &EntityName)
        -> impl Future<Output = Result<Seq, EntityJournalError>> + Send;
    /// The term this node writes under now, `None` off the leader: the non-activating read's validity token (§7.5).
    fn term(&self) -> Option<Term>;
    /// A snapshot at a committed seq (§9); `NotLeader` off the leader.
    fn save_snapshot(&self, entity: &EntityName, at: Seq, state: Vec<u8>)
        -> impl Future<Output = AppendOutcome> + Send;
    /// The latest snapshot, a local read once `head` has selected the quorum-best one (§8, §9).
    fn load_snapshot(&self, entity: &EntityName)
        -> impl Future<Output = Result<Option<(Seq, Vec<u8>)>, EntityJournalError>> + Send;

    // The blob half (§7.10): unordered and unfenced, on the same replicas.
    fn put_blob(&self, entity: &EntityName, id: BlobId, bytes: Vec<u8>)
        -> impl Future<Output = Result<(), EntityJournalError>> + Send;
    fn get_blob(&self, entity: &EntityName, id: BlobId)
        -> impl Future<Output = Result<Option<Vec<u8>>, EntityJournalError>> + Send;
    fn has_blob(&self, entity: &EntityName, id: BlobId)
        -> impl Future<Output = Result<bool, EntityJournalError>> + Send;
    fn retain_blobs(&self, entity: &EntityName, retain: Vec<BlobId>)
        -> impl Future<Output = ()> + Send;
    fn delete_blobs(&self, entity: &EntityName) -> impl Future<Output = ()> + Send;
}

pub enum AppendOutcome {
    Committed(Seq),          // durable on a quorum; the new head (for a snapshot, the snapshot seq)
    NotLeader(NodeId),       // this node no longer leads the shard; redirect (§8)
    Unavailable(String),     // no quorum reached; the entity pauses writes (§11)
}

pub struct RecordPage {
    pub base: Seq,                     // the compaction base (§9): slots at or below it are gone
    pub records: Vec<(Seq, Vec<u8>)>,  // the records after max(from, base), ascending
}

pub enum EntityJournalError {
    Unavailable(String),     // a local read could not complete (I/O or corruption), or, from `head`, the §8 refusals
}
```

`Seq::ZERO` is the empty head; an entity's first event commits at `1`. The host calls `append` only from the shard leader and behind the entity's input gate, so `after` always equals the entity's known head; a non-leader call returns `NotLeader` rather than a stale-sequence error. Object-safety (`impl Future` vs. boxed) is an implementation choice consistent with the actor framework's seams. The full contracts are the trait's rustdoc; the recovery `head` runs is specified in §8 and §9, the blob half in §7.10.

**Below the seam: placement and durability.** `EntityJournal` is the *only* seam the host sees, and it does not change. Its clustered implementation rests on two parts:

- the shard's **leader-election group** (§8) supplies placement: who may write, under which term. The journal consults it to stamp every append with the current term and to answer `NotLeader`.
- a per-entity **Replicator** supplies durability: it quorum-appends an entity's opaque record bytes to the shard's replicas, fenced by the shard term, and recovers an entity's committed head from a quorum on activation (§8, §9). The two reference tiers (§7.4) are Replicator implementations.

The records are the entity's `Event`s and state is the `apply` fold (§4.1); a snapshot is the serialized `State`. (Interpreting an entity's records as something other than an event log (the SQL facet's WAL frames) is the facet model's physical class, §7.12/§7.14, over this same Replicator substrate.) The host neither names nor depends on the parts below the seam, so the substrate is replaceable with no change above it.

**Why one trait, and why it is named for the records.** The substrate is two primitives (§7.10), yet the host sees a single trait carrying both, plus a `term()` that is neither. Three compressions, each with a cost:

- **The blobs ride the record seam** because they are the *same* bytes' second half: an entity's blobs are replicated to the same replicas, recovered on the same activation, transferred by the same migration and split passes, and simulated under the same seed. The cost is a seam the blob half nearly doubles, where the deep-module preference argues for fewer methods. Splitting them apart would be cheap and the option stays open (a blob supertrait the journal extends, leaving one implementing type and one construction site), and it would narrow the one consumer above the seam, `EntityBlobs`, to the half it actually calls; it is declined because the two are never deployed apart, not because it is hard.
- **The name follows the ordered half** because that half is what the `Seq`, the term fence, and the recovery protocol are about, and the blob half is defined by having *none* of them (§7.10). A neutral name would hide exactly the asymmetry an implementer must respect.
- **`term()` is a read of placement, not a control of it.** Placement is decided below the seam by the leader-election group (§8); the seam exposes only the current answer, because the gateway's non-activating read needs a validity token for its cached head floor (§7.5) and the alternative, routing that question around the seam, would give the host a second dependency on the layer this trait exists to hide.

### 7.4 Durability tiers: the two Replicators

The durability concern is the **Replicator** (§7.3), and it admits multiple implementations; two are reference **durability tiers**, chosen at deployment like a membership mode (actor §9.4). "Tier" carries other senses in this specification set, so it is never left bare here.

1. **`Local` (single-node).** One linearizable local store (in-memory, or the file-backed `FileEntityStore`, whose single-writer guard is an advisory lock on the store directory plus a stamped fence file per shard). An append commits on local fsync; there is no leader-election group and no term, because a single node is always its own writer. Best for: embedded, single-node, tests, and the deterministic simulator (§14). CP trivially (one writer, one store), not fault-tolerant to node loss.
2. **`Quorum` (clustered).** The fault-tolerant, CP production target: a per-entity quorum append fenced by the shard term (§8), over the shard's *R* replicas. An entity's record is durable once a quorum has stored it, and survives the loss of any minority of the shard's replicas; a new leader recovers each entity's head from a quorum on activation (§8, **E14**). Placement and replication stay bounded by split/merge (§7.7).

**Where a store's blocking I/O runs is a node-scoped seam, `BlockingIo`.** A file-backed store blocks on fsync, and that MUST NOT happen on the async worker that also drives the node's election heartbeats and quorum waits, where one slow device would trigger re-elections and a rehydration storm. A deployment on real storage SHOULD run store calls on a pool (`ThreadPoolIo`). The default, `InlineIo`, runs them inline, because the simulation (§14) must not depend on OS scheduling. The pool and the metrics sink (§13) belong to the node, not to an entity type, so a node MUST share one of each across every type it hosts (Appendix A). The seam changes only which thread blocks, never the order of the guard, the fence and the apply.

**A durable store records which codec wrote it.** A store whose records outlive the process that wrote them MUST carry an `entity.store` stamp ([compatibility](compatibility-spec.md) §3.4) naming the deployment codec, admitted at open before any fence, manifest, or record is read. An entity's *event payloads* are user types under that codec (§4.1, §5), while facet payloads are `postcard` by construction and a composite snapshot carries its own copy for facet 0 (§7.12), so an entity with records past its last snapshot, or with no snapshot at all, has nothing else that would notice the codec changing. Unstamped, every such record fails to decode and each failure surfaces on its own as a corrupt-entity activation abort; stamped, the mismatch is one refusal at open naming both codecs. The stamp is per **store directory**, not per record, because every record under one store is written by one deployment's codec and asking per record would spend bytes on the hottest durable path to answer the same question repeatedly. A directory with no stamp is a fresh store, and the codec running at that moment is written down.

### 7.5 Reads

A command that emits no events (a query) commits nothing (§6 step 2). The leader serves it from the entity's in-memory activation, a local, replication-free read with **no per-read consensus** (DO §4.3, §4.4). Reads scale with the leaders' capacity and never wait on replication.

**The event projection: what an event read returns.** An event read, `EntityRef::events` (§4.3), returns this view of the journal:

- **The `Seq`s are the journal's own.** Facet records (§7.12) are skipped, so the returned slots have gaps where other facets' records sit. A caller pages by re-asking from the last `Seq` it saw, never by arithmetic on `Seq`s.
- **A short page means the head.** Fewer than `limit` events come back only when the read has reached the entity's committed head, so page shape is never a way to learn that history was truncated. A truncated prefix and a facet gap look identical in the `Seq`s alone, which is why the base below is announced rather than deduced.
- **Every reply carries the compaction `base`** (§9). Slots at or below it are subsumed by the entity's snapshot and no re-read recovers them: `base <= from` means the page is complete history from `from`. The base MUST ride **every** page, not just the first, because compaction is bounded by no reader's cursor: a snapshot at the head can outrun a slow pager mid-read.
- **Corruption is loud.** A record that will not split or decode surfaces as `Unavailable`, never a silently dropped slot. `Unavailable` on a read reports a failed local read; a read commits nothing, so re-asking is always safe.

The subscription sink (§7.9) shares the first rule only (it decodes facet-0 records and skips the rest), and neither of the paging rules applies to a push: a batch carries no base and delivery MAY drop, duplicate, or reorder, which is what the sink's seq reconciliation against `load` is for (**E16**).

**The non-activating event read (`EntityRef::events`, §4.3).** This read form skips the activation entirely: the shard leader's **gateway** (§5.3) serves the projection straight from the journal's `load` (§7.3), without get-or-activating the entity, so observation never wakes a hibernated entity (§10) and continuous polling stays compatible with hibernation. Each wire page is bounded server-side (the gateway scans at most a fixed number of records per ask, SHOULD default to about **1024**) and carries an explicit cursor, an at-head flag, and the base, which lets a page diluted by facet records still make progress while the caller-side loop preserves the projection's contract; the paging loop and the event decode run on the **caller**, keeping the serial gateway to one bounded local read per ask (§5.3). Like `Activate`, the read is answered only on the shard's leader (a non-leader replies `NotLeader(hint)` for the caller's bounded redirect, §5.4), so its consistency is exactly the read-your-leader contract below, stale only where every read is.

**The read serves at or below a committed-head floor, never the raw store.** The local store alone cannot tell a committed record from a tentative one: a leader writes its own replica before its quorum resolves (§7.2), and a fresh leader's store may hold undecided records, or miss committed ones, until a `head` recovery read-repairs it (§8). The activation path is shielded by construction (rehydration runs the recovery, and the input gate orders reads behind in-flight appends, §6); the gateway read, which bypasses both, MUST therefore bound every page by the entity's committed head. A **resident** activation publishes its committed head to the gateway (updated at recovery and on each commit), so reads of an active entity serve up to the last commit and never an in-flight slot. For a **hibernated** name the gateway recovers a floor once per leadership: a spawned `head` recovery (§8), which also backfills this leader's store so the subsequent local reads are complete. The floor is cached under the shard term it ran under, and the term is the validity token, since regained leadership is always a higher term (§8.1) and an activation starting locally invalidates the entry. While the floor is being established the gateway answers a transient *warming* reply, which `EntityRef` absorbs: it backs off and re-asks under the caller's own deadline, exactly as it waits out an election (§5.4). The state never reaches the caller, which is why §12 carries no variant for it; only the deadline lapsing does, as `Unavailable`. Records above the floor are not served yet (the next poll sees them once committed), so no observer is ever shown an unacknowledged write (**E5**), and the anomaly budget stays what read-your-leader already grants: staleness, never invention.

**The contract is read-your-leader (relaxed), not linearizable under partition.** Because the activation is colocated with the shard leader (§5.2) and the read path does not reconfirm leadership per call, a deposed but not-yet-fenced leader, isolated on the minority side and unaware it lost the election, MAY serve a stale read until its activation stops. Writes never fork (the shard term fences the commit, §8); only reads can be stale, and only on the minority side of a partition. A caller that needs a linearizable read in the meantime issues a trivial *writing* command (one that emits an event): it rides the §6 output gate, so it commits through the shard leader and reflects committed state, or fails (`NotLeader`/`Unavailable`) on a deposed leader.

**Linearizable reads are a deferred upgrade (§16), via a check-quorum property on the shard's leader-election group (§8), not a per-read consensus round.** The leader serves reads locally while it holds a **check-quorum lease** (it has heard from a quorum within an election timeout, so no other leader can have been elected). When the lease lapses the activation **self-fences**, returning `Unavailable`/`NotLeader`, trading availability for consistency on the minority side (CP, §11). Reads from shard *followers* are a separate deferred extension (§16).

### 7.6 The shard map is a consensus-agreed allocation

The **shard map** records, for each shard, its key range and its replica set; leadership within a shard is the leader-election group's (§8), surfaced to routers by `NotLeader` redirect (§5.4), not tracked in the map. It MUST be **consensus-agreed**, so every node resolves a name to the same replica set regardless of join order: a rendezvous choice each node snapshots from its own *live* membership view would diverge across nodes that join at different membership epochs. The reference implementation realizes this as a **per-entity-type Raft group whose committed log *is* the allocation** (one `Assign { shard, replicas, range }` record per shard, the range fixed by the founding assignment and ignored on every later one); every node applies the identical committed entries and so agrees on where each shard lives. This map group is keyed by entity type and derives its group id from a reserved shard index, so it never collides with a shard's leader-election group (§8.2).

A leader-only **allocator** keeps each shard's committed replica set equal to its rendezvous choice over the current cluster voters, and a leader-only **reconcile** loop drives each group it leads (the map group and the shard leader-election groups it holds) toward its intended membership, so shards rebalance onto and off of changed members as the cluster grows or shrinks. Because the leader-election group holds no entity data (§7.1), catching up its prefix is *not* enough: the new replica MUST NOT stand in for a member of the *current* majority before it holds each entity's committed records and latest snapshot; a voter that lacked the entity data would break the quorum intersection E14 relies on (it could join a recovery quorum holding none of an entity's records). Under the joint quorum below a target replica counts toward the **target** majority from the `Assign` commit, before its catch-up, which is safe only because a majority of `current` is still required alongside it until `Migrated` flips the set.

**Replica-set migration.** A changed rendezvous choice therefore never replaces a committed set outright; it runs a **joint-quorum migration**:

1. The allocator proposes `Assign { shard, replicas, range }`; on an already-allocated shard this commits the new set as the shard's **target**, leaving `current` in place. From that commit, every per-entity append, snapshot, and recovery on the shard uses the **joint quorum**, a majority of `current` AND a majority of `target`, fanned to the union, so every pre-migration commit (on a majority of `current`) and every in-migration commit (on both majorities) stays intersected by every recovery read. The allocator proposes nothing further for a shard while its migration is in flight.
2. Every node in the union builds its journal over the joint sets, and the reconcile loop drives the shard's leader-election group toward the union. The replica sets are read per operation, so a continuing replica switches in place.
3. The shard leader's **migration driver** catches every entity up on the target: it enumerates the shard's entities from a read quorum of `current` (the union of their local lists; any read majority sees every committed entity), then per entity runs the §8 recovery (whose joint write-back lands the records on the target replicas), re-persists the best snapshot on the joint quorum (a compacted entity's prefix exists only in its snapshot), and copies the blob area verified-by-content. Every step is idempotent; a leader change mid-migration re-drives on the new leader, and a failed step just retries.
4. Only after a complete pass does the driver propose `Migrated { shard }`: applying it flips `current = target`, departing nodes drop their journals, and the reconcile loop shrinks the group's voters to the new set. A node that departed keeps a stale local view of the group it left, so a routing hint from a believed leader is trusted only when the committed map still lists that node as a replica (§5.4).

**A committed command a node cannot decode MUST stop that node's map, never be skipped.** Skipping would leave its allocation a gap in the agreed log rather than a prefix of it, the divergence consensus exists to prevent ([compatibility](compatibility-spec.md) **V2**). A stopped node freezes at the last command it applied, hands off every shard it leads (§8.3), and stops its proposers, since an allocator sweeping a frozen view would propose founding assignments over real ones. It stays in the map group acknowledging appends, and reports why it stopped through `ShardMapSource::poisoned`. Each command carries the `entity.shardmap` stamp ([compatibility](compatibility-spec.md) §3), so another version is refused by version rather than by parse failure.

The map's write rate is low: it changes on shard splits and merges (§7.7), on replica-set reconfiguration, and on node membership changes, never on an entity activation or an entity write. Routing follows `NotLeader` hints (§5.4) rather than reading the map, so steady-state routing adds no map-group traffic (invariant **E9**). The map group seeds its membership from the actor framework's **leader-based control plane** (actor §9.4.3): entity therefore requires the leader-based control-plane mode; the pure gossip-based AP mode (actor §9.4.4) cannot agree a shard map.

> **Note.** Actor §9.4.3 item 6 stores the allocation *inside* the control-plane group's own log; entity instead runs a dedicated per-type map group seeded from the control plane's voters. The guarantee (a single consensus-agreed allocation, off the entity data path) is identical, but the consensus-group count is `O(shards) + O(entity types)` rather than `O(shards)` alone (§8.2, **E7**).

### 7.7 Splitting and merging shards

A shard that grows too large or too hot **splits**: its key range divides at a boundary, the parent keeping the low half `[start, boundary)` in place and a fresh **child** shard (a new index, and so a new leader-election group, §8.2) taking the high half `[boundary, end]` on the parent's replica set. Two small, cold adjacent shards **merge** the reverse way: the right shard folds into the left, extending the left's range over it and retiring the right's leader-election group. Split and merge are the elasticity mechanism: the number of shards tracks load and cluster size, so per-shard work stays bounded as the entity count climbs into the millions.

A split MUST be atomic with respect to writes, in this order:

1. **Seal.** The parent's replicas durably record an **append bound** at `boundary`, refusing every append whose name hashes at or above it, on a majority. From that quorum on, no append to the moving range can assemble a write quorum at *any* term, so even a parent leader that has not yet learned of the split cannot land one: a majority of acks would have to include a sealed store. The bound is a store-level quorum barrier, not a leader-local flag, because a leader elected at a higher term but lagging the map log would bypass a flag. An append refused for the moved range surfaces as `NotLeader`/`Unavailable`, never a silent loss (**E5**); the caller re-resolves against the committed map.

2. **Transfer.** For each moved entity, the parent leader recovers its committed head from a read quorum (§8) and copies its committed records, latest snapshot, and blob area to the child's keys on the child's replicas: a majority for the records and snapshot, best-effort for blobs (a down replica heals on access, §7.10). The copy is fence-bypassing under a null term: the child's keys are fresh (disjoint range) and no child leader exists yet, so there is nothing to contest.

3. **Commit.** Only after a full transfer pass does the new mapping commit to the shard map (§7.6): the parent's range shrinks, the child's allocation commits, and the child's replicas create its leader-election group. Only from this commit does any node route the moved range to the child; the map commits *before* either side serves it.

No write is lost or duplicated across the boundary, and no entity is writable in two shards at once (invariant **E15**). Every step is idempotent and derived from committed map state, so a crash or leader change mid-split re-drives on the new parent leader. A merge is the mirror image, driven by the **right** shard's leader (which holds the term to fence the right's deposed leaders): seal the whole right range, transfer every right entity into the left's keys, then commit, extending the left's range, dropping the right, and retiring its group (**E7**). Split and merge are one-at-a-time per shard, mutually exclusive with a replica-set migration (which never moves a range).

The size trigger is `shard_target_bytes` (Appendix A): the shard leader auto-splits a shard whose durable footprint grows past it. It is **opt-in**: `0` (the default) leaves the partition to explicit split/merge requests (the admin seam), because a type whose entities are individually large (a VM image, a big database) wants a higher threshold than a type of many small entities. Splitting on request-rate signals from the §13 event stream is a conforming addition. A split (or merge) commits its progress through the same shard-map log: `SplitStarted`/`SplitCommitted` (`MergeStarted`/`MergeCommitted`) records validated deterministically against committed state, so an invalid or raced proposal is a no-op on every node.

### 7.8 Scaling: where the cost goes

Cost tracks the *active working set* and the *cluster size*, not the total entity count.

- **Consensus groups:** O(shards) leader-election groups plus O(entity types) map groups, bounded by split/merge (§7.7), never O(entities); none carries entity data.
- **Control-plane writes:** O(cluster events), namely splits, merges, reconfigurations, and membership, never per activation or per write (§7.6).
- **Activation:** no consensus, no election, no agreement round (§5.2, §10). On the `Quorum` tier it costs one quorum-recovery round-trip to confirm the entity's head (§9); on the `Local` tier it is fully local. Either way it touches no Raft group, which makes aggressive hibernation affordable (§10).
- **Memory and streams:** bounded by the entities *active* at once, which hibernation keeps to the working set (§10). Total entity count is limited only by the shards' storage.
- **Routing:** two cached lookups, off the control plane and off the serial gateway in steady state (§5.4).

The residual bounded cost is per-shard placement: a hot shard's entities share one leader node, capped by splitting the shard (§7.7).

---

### 7.9 Record subscriptions (the journal follower)

A **subscription** is a live, best-effort delivery of an entity's committed records to a sink, layered over the durable `load` (§7.3). It exists so a follower learns of each record as it commits rather than by polling, without making delivery a source of truth.

`EntityRef::subscribe(from)` spawns the **sink** on the *caller's* node, a `RecordSink` actor that decodes each wire `RecordBatch` to the entity's events and forwards them on a bounded channel, and carries its `ActorRef` in `Subscribe` to the shard leader (§5.4), whose host registers a bounded **forwarder** task in the activation's **sink set**. The forwarder takes each committed batch off the host's executor and pushes it to the sink over the transport, so wire backpressure never reaches the write path. The call returns a `Subscription` carrying the committed `head` and a receiver of live record batches; the caller does not supply the sink but drains the receiver. The `from` argument rides in `Subscribe` but is not consulted: the host registers the sink at the entity's current head and pushes only the batches committed after registration, so the subscriber itself backfills `from`..`head` by `load` (`EntityRef::events`, §7.5) before reconciling the live stream. After each commit (§6 step 4), the host delivers a batch of the seqs just appended to every sink. Delivery occurs **after** the fold and the output-gate release and MUST NOT gate the commit or any caller's reply; it is observational, emitted at the same point as `Committed` (§13), and cannot affect a write's outcome (preserves **E5**). `subscribe` is a framework built-in, dispatched to the host for *any* entity type without appearing in the entity's `register` allowlist (§5.5), the read-path analogue of `head`/`load`, not a user command.

**Reconcile by `Seq`.** A sink treats `Seq` as authoritative. Each batch carries the `from` it begins after; a sink that has seen up to `last` MUST close any gap (`from > last`) by `load` (§7.3), and MUST ignore records at or below `last`: a re-subscribe replay, or a timed-out append that committed late (§7.2). At-most-once delivery (actor §7.2) MAY drop, duplicate, or, across a re-subscribe, reorder batches; seq reconciliation absorbs all three. Push is a latency optimization over `load`; correctness rests on the journal (**E3**), never on delivery reliability. The reconstructed sequence a sink obtains by applying this rule is exactly what `load(from, …)` to the head would return (**E16**).

**Bounded, never blocking.** Each sink has a bounded delivery buffer. On overflow the host drops the sink rather than awaiting it; a slow or dead subscriber MUST NOT stall an entity's writes or its input gate (§6); the subscription is off the write path entirely. A dropped sink observes its stream close and re-subscribes, backfilling from its last seq. Delivery runs through the framework's `Spawner`/`Transport` seams (actor §4.6), so it stays seed-reproducible under simulation (§14); the drop decision is a pure function of buffer occupancy, not the wall clock.

**Ephemeral.** The sink set is per-activation, never journaled (§1): a cache rebuilt by subscribers re-contacting. On hibernation, migration, or forced step-down it is dropped and `Deactivated` emitted (§13); subscribers re-subscribe against the current leader (§5.4) and backfill from their last seq. A subscription does **not** veto idle eviction (`can_hibernate`, §10): a hibernated entity produces no records, and the next write re-activates it, at which point subscribers re-establish. (Parking a subscription across hibernation is the deferred *hibernatable connections* extension, §16.)

### 7.10 The entity-native content-addressed blob store (the second primitive)

Records (§7.2–§7.3) and the blobs of this section are entity's **two storage primitives**: the ordered, term-fenced half and the immutable, bulk half of an entity's durable state. They are the only two replication mechanisms in the system: every further storage feature is a **facet**, an interpretation layered over them (§7.12), never a third mechanism.

An entity's journal holds its **small foldable state**, the value `apply` rebuilds on activation (§4.1, §9). Bulk, immutable bytes do not belong there: a workspace's file blocks, a database's pages, any large content an entity references but does not fold. So beside the journal, an entity node owns a second store: an **immutable content-addressed blob area**, replicated to the *same* shard replicas as the journal, reached from a handler or lifecycle hook through `ctx.blobs()` (DO §2.3, §6). An entity stores bulk bytes by content and keeps only their ids in its folded state.

The blob path carries **no `Seq`, no term, and no read-repair**. A content hash names exactly one byte sequence for all time, so there is nothing to order and nothing to agree on: two writers of the same bytes cannot conflict, and a reader proves it got the right bytes by re-hashing them. The single-writer term fence (§8) exists only to keep one *mutable* head from forking, and a blob has no mutable slot. What remains is a single durability question, "is the blob stored on enough replicas yet?", answered exactly as a record's is.

- **The model.** A **`BlobId`** is the 32-byte BLAKE3 digest of a blob's bytes; entity defines it natively (it does not depend on a separate content store). A blob's full address is `(EntityName, BlobId)`: the id is identical wherever the bytes are stored; the entity scopes *which* area holds it. Blobs are write-once and dedup'd by content **within an entity**: equal bytes under one entity store once. `ctx.blobs()` offers `put` (→ `BlobId`), `get` (whole or a byte range), `has` (present on **any reachable replica**, so a `get` can source the bytes, not a quorum-durability measure, since durability is established at `put`), `gc` (mark-from-roots: keep only a live id set, unioned with the facets' roots, §7.12), `repair_facets` (re-`put` every facet-root blob, re-fanning it to the shard's current replica set: additive, idempotent, and best-effort, run off the activation latency path), and `destroy` (drop the whole area).
- **Verification on read.** Every `get` re-hashes the bytes it is about to return and compares to the requested `BlobId`; on mismatch it MUST NOT return them. On the `Quorum` tier it then falls through to the next replica, and returns an error only if no replica yields verifying bytes. Corruption and misdelivery are thus detectable at the point of use, never silent wrong bytes (**E17**).
- **Durability is a quorum append, colocated.** A `put` writes the **local** replica (the leader is one of the shard's replicas, §5.2, so a later `get` reads locally) and fans the bytes to the peers, acknowledging once a write quorum has stored them; the blob then survives the loss of a minority of the entity's replicas. There is no leader-election, term, or agreement round on this path (**E18**). A `put` that cannot reach a quorum returns `Unavailable`, and the caller retries idempotently, since the id is a pure function of the bytes.
- **Reclamation is entity-scoped and root-driven.** The entity knows its own live id set (the ids its state still references), so it reclaims storage with a **mark-from-roots sweep** (`gc(live)` drops every blob not in `live`) and a whole-area `destroy`. The blob store therefore needs **no namespace tombstone, no membership-gated resurrection guard, and no cluster-wide delete fan-out**: deletion targets the entity's own known replicas, and a delete a partitioned replica misses leaves only *orphan bytes* (reclaimed by a later sweep), never a *resurrection of referenced data*, because nothing in the entity's state points at them and only referenced blobs are ever fetched or re-replicated.
- **Lazy hydration and corruption self-heal.** A blob is faulted in on first access: a `get` that misses locally fetches a verifying copy from a peer and backfills the local replica, so an entity that re-activates on a fresh leader and then touches ten files pulls ten files' blocks, not its whole area (DO §5). The same path heals a *corrupt* local copy as well as a *missing* one: a local copy that fails verification is evicted and replaced in place by the verifying peer's bytes. (Eviction-before-replace is required because a content-addressed write of an id already on disk stores nothing.)

**Placement is colocation, not rendezvous.** An entity's blobs ride its *own shard replica set*, not a content-hash-rendezvous spread across the whole cluster: reads are leader-local and recovery comes from the entity's known replicas, at the cost of concentrating an entity's blob bytes on its R replicas. A *cluster-shared* content store, rendezvous-placed, namespace-scoped, for cross-entity dedup, archival, or an external object-store tier, is the complementary **cold** tier of the DO hot/cold split (DO §6), out of scope here (§16). That store is specified separately as the [blob store](blob-store-spec.md), a standalone product beside entity with no in-tree consumer today: this section's blobs, not it, serve the live path. The two are easy to confuse: the blob store scopes a blob by a consumer-chosen namespace and places it by content hash across the whole cluster, while an entity's blobs are scoped by `EntityName` and colocated on the entity's own shard replicas.

**A replica joining the shard receives the blobs with the records.** A replica-set migration (§7.6) copies each entity's blob area to every target replica that lacks any of it before `Migrated` is proposed: the driver sources each blob from its own store or a reachable *current* replica, fetches it verified by content (**E17**), and stores it on the lacking targets; an unreachable target fails the pass, which retries, so the flip never leaves a target without its blobs. A split or merge transfer (§7.7) copies blobs best-effort, and a replica that still lacks one heals it by **recovery-on-access** (a verified peer-fetch on `get_blob`). Range-verified streaming of one large blob against the BLAKE3 tree the id already roots (the Bao encoding) is a further deferred refinement (§16); v1 verifies whole blobs and a consumer chunks beyond a bound.

---

### 7.11 The workspace facet (physical)

The workspace facet gives an entity a **real, node-local directory**: the working set a shell, container, or microVM runs against, with no interposition and no typed-API translation. The directory is the live truth for the activation; durability comes from **capture**: after a command's side effects have landed on disk, the handler calls `ctx.ws().capture()`, which diffs the durable subtree against the committed index (stat-pruned, content-hash-verified) and stages one record per changed file, the file's bytes **inline**, plus one record naming the deleted paths. The staged records join the command's atomic batch (§7.12, **E19**): the command's reply, its events, and the workspace delta commit together or not at all. Replay applies the recorded bytes byte-deterministically, never by re-execution (**F1**). An unchanged tree stages nothing and rides the §7.5 read path.

- **The physical discipline (E20/F4).** The directory mutates before durability: tools write it directly, between and during commands. On `Committed` the host keeps the materialization; on **any** other outcome it deletes the directory and steps down; the next activation rematerializes from the composite snapshot plus committed delta records. The directory is a rebuildable cache (§1), keyed under `EntityConfig::data_dir` by **node and entity**: a materialization is activation-local, and under the simulator many nodes share one process, so a failover must never land two nodes' materializations of one entity on one path.
- **Checkpoints are the snapshot contribution.** At snapshot time every durable file is chunked into **content-addressed blobs** (§7.10) and the contribution is a small `path → chunk ids` manifest; unchanged files hash to blobs already stored, so a checkpoint uploads only new content, incremental by dedup. The manifests' chunk ids are the facet's root set (**F3**); §9 compaction then drops the delta records the checkpoint subsumes, which keeps inline delta bytes from accumulating in the journal. A snapshot taken while an in-flight tool has dirtied the directory fails (the snapshot must be the committed image, **E4**); a snapshot is only an optimization, and the idle-deactivation snapshot runs quiesced.
- **Scope: files only, excluded trees, a cap.** Directories are implied by the files within them; empty directories, symlinks, and special files are not durable (matching what the sandbox tar paths preserve). Trees named in the exclusion list (`target/`, `node_modules/`, and kin) live in the directory but are never captured, journaled, or restored: regenerable caches, rebuilt rather than replicated. The durable subtree is capped (64 MiB, the sandbox tar bound); an over-cap capture fails as an application error with nothing staged, and the next successful capture diffs against the last *committed* index, so nothing is skipped. Ranged sub-records for files larger than a record bound, and lazy hydration for large workspaces, are the deferred refinements (§16).
- **Consumption.** The agent runtime declares `Facets = (Ws,)`, hands `ctx.ws().dir_path()` to the sandbox provider at open, and captures in the command that journals each tool outcome, so a tool's result and the files it wrote are one quorum append. The persistent machine (machine §3) declares `Facets = (Disk, Alarm, Ws)`, projects the directory into its guest as a tmpfs `/workspace` (pushed at boot, pulled at each capture quiescent point over vsock) and captures in the same command as its disk manifest, so workspace and rootfs commit in one atomic batch (E19); its file commands write the facet directly while no VM runs, so a hibernated machine's workspace is editable without booting it.

---

### 7.12 The facet model: one substrate, many storage features

Durable Objects exposes several storage APIs to one object but implements **one** replicated substrate (DO §4.2). The entity layer applies the same discipline over its two-part substrate (§7.10): ordered records and immutable blobs. A **facet** is a durable storage feature of one entity defined entirely by three answers, and nothing else:

1. what its **records** mean: the ordered mutations it contributes to the entity's journal;
2. what it keeps in **blobs**: bulk immutable bytes, referenced by id;
3. what it contributes to the **snapshot**: its checkpoint at a `Seq`.

A facet gets no replication path, no fence, and no recovery protocol of its own; those exist once, in the substrate (§7.2, §8, §9, **E14**). The facet model changes **nothing at or below the journal seam** (§7.3): `append`/`load`/`head`/`save_snapshot`/`load_snapshot` and the blob operations carry the same opaque bytes as today. Everything in this section happens in the host.

**Tagged records, one order, one barrier.** A multi-facet entity's records interleave in the entity's single `Seq` order, each carrying its facet's stable **tag**. All records a command produces, its events plus every facet's operations, append as **one atomic batch** (§6, §7.3), so a command that touches state, the KV map, and the database commits everywhere or nowhere (**E19**). Replay dispatches each record to its facet by tag; a record whose tag the runtime does not recognize MUST abort activation rather than be skipped: an entity's history is never silently misread by a runtime missing one of its facets (**E19**). **Every** record wears the tag, including facet 0's on an entity with no declared facets: one uniform envelope, no second encoding path.

Records and the composite snapshot are both stamped for version, and past a threshold facet 0's state travels as content-defined chunks in the blob area; §9 specifies both encodings. Subscriptions (§7.9) deliver the **event projection** (§7.5): the typed sink decodes facet-0 records and skips other facets', whose seqs appear as the gaps the subscriber's seq-reconciliation already absorbs (E16 holds per projection).

**Two facet classes.** The classes differ in where a mutation lives before the commit:

- A **logical facet** folds. Its records are semantic operations; a pure, deterministic function applies them to an in-memory form (**F1**, the generalization of **E2**), and the host folds only after durability, exactly **E1**. The event fold itself is *facet 0*, the primordial logical facet; the KV map (§7.13) is logical.
- A **physical facet** materializes. Its record is a captured physical delta, applied byte-deterministically on replay (**F1**): the SQL facet's WAL frames (§7.14), the workspace facet's changed-file bytes (§7.11), the disk facet's capture manifest with its block bytes in blobs (§7.15). Its live form (the database file, the directory, the block image) mutates locally *during* the handler; the disk image also mutates *between* handlers, the one departure, owned and bounded by §7.15. The E1 discipline inverts to **expose only after durability; discard on anything else** (**E20**). The inversion is sound because the materialization is a rebuildable cache (§1) and the §6 gates make un-durable state unobservable: the input gate admits no other command while the append is in flight, the output gate holds the reply, and a non-committed outcome destroys the only copy of the tainted state.

**Staging: the handler writes through the facet; the commit makes it real.** Each declared facet surfaces an `EntityCtx` accessor (§4.3). A logical facet's accessor stages operations in a **per-command overlay**: reads through it see committed-plus-staged, and at append time the host drains the overlay into the command's tagged records, folded into the facet's form only on commit and dropped on any other outcome. A physical facet's accessor operates on the materialization directly, inside one implicit transaction per command (§7.14). Both keep §4.2's split intact: committed, observable state changes only at the commit point. Two disciplines follow. *Staging is command-scoped*: a facet write outside a command handler (from `on_activate`/`on_deactivate`) has no batch to join, and is never silently dropped or deferred; the SQL and disk accessors refuse an out-of-command write as an error (`SqlError` for a statement, `DiskError` for a capture or import), and the KV, alarm, steps and workspace accessors panic, since a staged write that vanished would be exactly the silent drop this rule forbids. *Plan, then stage*: staged operations commit with the batch even when the handler's reply is an application error, so an accessor operation performs its fallible work (blob puts, scans) first and stages only on success, so an operation that returns `Err` leaves no trace (the KV facet's spill and the workspace facet's capture are built this way, §7.13, §7.11).

**One composite snapshot.** The snapshot at seq *S* is the tuple of every facet's contribution at *S*, encoded as one `save_snapshot` payload: facet 0's encoded `State`, the KV map, the workspace checkpoint manifest, the SQL checkpoint manifest, the disk checkpoint manifest, the alarm deadline, the steps memo. **E4** applies to the composite as a whole: a composite whose seq exceeds the committed head is ignored entire, and replay runs from `ZERO`. Bulk bytes go to blobs and the composite holds manifests of ids, keeping the snapshot record small, which matters because the record, unlike a blob, is broadcast whole to every replica on each snapshot.

**Blob liveness is unioned by the host.** Every facet reports its blob **root set** (**F3**): the workspace facet its checkpoint chunks; the SQL facet the pages of its live checkpoint manifests; the disk facet the blocks of its live checkpoint manifest plus the blocks its post-checkpoint capture records reference; facet 0 the chunks its state was carried in. A facet MUST NOT sweep the shared area itself: the one sweep above the seam is `ctx.blobs().gc(live)`, which the application calls with its own roots, and the handle the host built for it unions the facets' root sets into that live set before dropping anything. One facet's GC can therefore never drop another facet's bytes.

**Compile-time composition.** An entity declares its facets in its type (an associated facet set on `Entity`, a tuple whose unit form `()` declares none), and each accessor is gated by a containment bound, so `ctx.sql()` on an entity without the SQL facet does not compile (the **E10** discipline applied to storage). The declaration statically fixes the entity type's record-tag registry, which makes the unknown-tag rule (E19) checkable.

**The facet contract.** Every facet, built-in or future, owes the same obligations, verified generically under §14 simulation:

| | Obligation |
|---|---|
| **F1** | **Deterministic interpretation.** A logical facet's fold is pure and deterministic (E2 generalized); a physical facet's delta application is byte-deterministic. |
| **F2** | **Snapshot equivalence.** Restoring the facet's snapshot contribution at *S* and replaying its records in (*S*, head] yields the same form as interpreting the full record history. |
| **F3** | **Complete roots.** The facet's reported root set covers every blob its restored form can reference; nothing it can later `get` is outside it. |
| **F4** | **Discard safety** (physical only). Discarding the materialization at any moment and rehydrating from the composite snapshot plus records reproduces the identical form. |

**Apps are libraries over facets, never facets.** A git-style artifact store is refs in the KV facet plus objects in blobs (§7.10), and a work queue is events or KV. A feature earns facet-hood on one of two grounds. Either it needs **durable records the application's own `Event` type must not have to carry**, hence its own tag and snapshot contribution, which is what the KV map, the workspace, the database, the image, and the step memo each need; or it needs **host participation no library can obtain**: discard of a materialization on a non-committed outcome (every physical facet, **E20**), or callerless delivery driven from the folded form (the alarm, §7.16). "It needs a per-command overlay" is not a ground, since every logical facet has one by construction; the overlay is what facet-hood *gives* a feature, not what earns it. The two examples above are libraries because their vocabulary is one the application is content to own: a git store's refs are KV keys it is happy to name.

The steps facet is the boundary case, and §7.17 works it: the memo earns a tag, while the launch and the guard's keying stay with the entity, and the layer that generalizes those is the workflow, above entity rather than another facet.

**The seam is public and sealed.** The facet trait is published, but sealed, so the built-in facets are its only implementations; opening it to out-of-crate facets is deferred (§16).

---

### 7.13 The KV facet (logical)

The KV facet gives an entity an ordered map of small values (configuration, refs, cursors, indices) without designing an event vocabulary for it: `ctx.kv()` offers `get`, `put`, `delete`, and ordered prefix listing (`keys`/`list`), staged per command (§7.12) and read-your-staged-writes within the handler.

- **Records.** `Put(key, value)` and `Delete(key)` under the kv tag, folded into an ordered map; the fold is trivially pure (**F1**).
- **Snapshot.** The encoded map is the facet's composite-snapshot contribution (**F2**). Keys are unbounded; an inline value is bounded by the constant `INLINE_MAX` (64 KiB, the Durable Objects KV bound is the reference point), and the facet transparently **spills** a value above it into the entity's blob area, storing the `BlobId` in the map and fetching it back on `get`, verified by content (**E17**). Spilled ids are the facet's root set (**F3**), so spilled bytes are reclaimed when their keys are deleted, by the host's unioned sweep (§7.12).
- **Native, not sugar over SQL.** KV is its own logical facet: no C dependency, its form in memory, unchanged under deterministic simulation (§14), usable by an entity that wants a map without a database file. An entity that needs range queries with predicates wants the SQL facet, not a richer KV.

---

### 7.14 The SQL facet (physical)

The SQL facet gives an entity a **private, on-disk SQLite database** (DO §2.3, §4.2). The handler runs ordinary SQL through `ctx.sql()` against a local file: reads are leader-local and replication-free (§7.5), writes are transactional, and nondeterministic SQL (`random()`, `datetime('now')`, autoincrement) is fine, because replication is physical, never re-execution. (A *statement-sourced* SQLite entity (SQL text as events, folded by re-execution) remains expressible with plain event sourcing and no facet; it is an application pattern, not this facet.)

- **One command, one transaction, one record.** `ctx.sql()` is valid for the duration of a command and wraps it in one implicit transaction; interactive transactions spanning commands do not exist (the DO restriction). At local commit the host captures the transaction's **WAL frames** (page number, page bytes, database size after commit) and encodes them as one tagged record in the command's atomic batch (§7.12, **E19**). A read-only transaction produces no frames and no record, riding the §7.5 read path. A transaction's frame bytes are bounded by configuration (one record must fit the replicator's message bounds); a bulk load splits into multiple commands. Idempotency needs no new mechanism: a frame record occupies one `Seq` slot, so a timed-out append that lands late is applied once, at its slot, on recovery (§7.2).
- **The physical discipline (E20/F4).** The database file mutates at local commit, before durability, the class-defining inversion (§7.12). On `Committed` the host keeps the materialization and releases the reply; on **any** other outcome it deletes the database file, folds nothing, and steps down; the next activation rematerializes from the composite snapshot plus committed frame records. The local file is a cache, so `PRAGMA synchronous=OFF` is sound: durability is the quorum ack, never the local disk (DO §4.2: the local disk can be lost entirely).
- **The capture seam.** After a local commit the host MUST obtain exactly that transaction's frames, in order. The reference mechanism is **WAL tailing** (autocheckpoint disabled, the host owning checkpoint timing, the Litestream-proven approach); a VFS shim intercepting `-wal` writes is the deferred upgrade (§16), which also enables lazy page hydration on activation.
- **Checkpoints are the snapshot contribution.** Periodically (a frames/bytes trigger, configuration like `snapshot_every`) the host checkpoints the WAL into the database file (`TRUNCATE`), chunks the file into **page-aligned, content-addressed blocks**, `put`s them as blobs (§7.10), and contributes a **manifest** (page size, page count, ordered `BlobId` list) to the composite snapshot (§7.12). Unchanged pages hash to blobs already stored, so a checkpoint uploads only dirty pages: incremental by dedup, entity's analogue of SRS's WAL archiving (DO §4.2). The live manifests' pages are the facet's root set (**F3**); §9 compaction then drops the frame records the checkpoint subsumes.
- **Rehydration.** Composite snapshot → manifest → fetch pages, verified by content (**E17**) → materialize the file → apply frame records after the snapshot seq (byte-deterministic, **F1**) → serve. The quorum barrier (`head`, §9) is unchanged. There is **no out-of-band schema hook**: DDL is a journaled write like any other, so schema setup is idempotent `CREATE … IF NOT EXISTS` at the top of a writing command; replay reproduces it physically, and a lifecycle hook that mutated the database outside a command would produce frames no record captures (which is why out-of-command SQL is refused outright).
- **Dependencies and simulation.** SQLite (bundled `rusqlite`) sits behind the `sql` cargo feature, keeping the base crate dependency-light. The facet introduces real file I/O into the entity hot path, since the database file lives under `EntityConfig::data_dir` (a rebuildable cache, §1, keyed by the entity name's content hash); under deterministic simulation (§14) tests point `data_dir` at a temp directory: frame *production* is real SQLite, while frame *application* and everything around it stays seed-driven. A single query returns at most `MAX_QUERY_ROWS` (10 000) rows, enforced as an error rather than a truncation, so a predicate that matches far more than the caller expects cannot exhaust the leader's memory; beyond it a caller paginates. Enforced per-transaction frame caps and a max database size remain **deferred** configuration (the other bounds are the replicator's message limits and the node's disk).

---

### 7.15 The disk facet (physical)

The disk facet gives an entity a **raw block image**: a fixed-size disk a microVM mounts as its rootfs, the whole-machine analogue of the workspace facet's directory (§7.11). Where the workspace facet captures changed *files* and the SQL facet captures WAL *frames*, the disk facet captures changed **blocks**. A session's dirty set can run to gigabytes, far past the replicator's message bounds, so the block *bytes* never ride the record: a **capture** `put`s each dirty block as a content-addressed blob (§7.10) and stages exactly **one** record, a **capture manifest** carrying the image size and the ordered `(block index, BlobId)` list, which joins the command's atomic batch (§7.12, **E19**). One capture, one record, one commit: a crash can never leave a committed *prefix* of one. The blob `put`s precede staging (*plan, then stage*, §7.12), and blobs orphaned by a failed commit are unreferenced by any root and swept by the host's unioned GC. Replay decodes each manifest into a pending set (the fold, pure: **F1** holds on the recorded bytes); the post-replay `rehydrate` hook then fetches each pending manifest's blocks, verified by content (**E17**), writes each at its offset, and truncates to the recorded size, byte-deterministically, before the first command runs. An unchanged image stages nothing and rides the §7.5 read path.

The physical seam the disk facet rides is the SQL facet's (§7.14): capture at a quiescent point, checkpoint into content-addressed blobs, discard on non-commit, rehydrate then replay. The disk facet is that machinery with **dirty blocks** for WAL frames and a **raw image** for the database file; capture bytes travel as blobs and capture rides an explicit command. Blob-referencing records use the seam's one post-replay hook, **`rehydrate`**: called once after restore + replay, before the first command, it is where a facet whose records carry blob ids rather than bytes fetches and applies them, so the fold stays synchronous and pure. Every other facet keeps the no-op default.

**Capture rides a command.** §7.12's staging rule is unchanged: nothing is staged from `on_activate`/`on_deactivate`. Because the guest writes the image *between* commands, the consumer drives every capture through an explicit **capture command**, a host-delivered command like the alarm's `AlarmDue` (§7.16), passing through the ordinary §6 barrier: input gate, one atomic batch, output gate. A consumer that must capture before a lifecycle boundary (idle hibernation, a planned migration) runs its capture command to completion and only then permits the boundary; the machine spec's quiescent points are exactly this (machine §4). The consumer's `can_hibernate` refuses eviction while the live image holds uncaptured writes, so the §10 idle path cannot silently strand a dirty image.

An entity declaring `Facets = (Disk,)` persists the guest's **entire rootfs** over the same two primitives, with no new replication path, where the agent runtime's `Native` capability tier persists only the workspace tar (sandbox §3.5). Its worked consumer is the persistent machine ([`machine-spec.md`](machine-spec.md)), which owns *when* the image is captured: a machine's command unit is a whole session, not a single message (machine §4).

- **The scan runs off the async executor.** A capture reads and hashes every block of the image, and an import reads and writes every block of the source; at 1 MiB blocks that is seconds of device and CPU work for an image of any size. It MUST NOT run on the worker driving the node's consensus: a node that stops serving heartbeats for the length of a scan is a node its peers elect around, and the shard they elect around is precisely the one whose machine is being captured, so the checkpoint deposes the activation that owns the guest. This is the §7.4 blocking-I/O seam applied to a facet rather than to the store, and it lets a deployment size its election timeout for the network rather than for the longest scan. The scan proceeds a **wave** at a time, so moving it off the executor does not raise the facet's peak memory past the bound the blob-put pipeline already holds, and blocks whose hash still matches the committed index are discarded without leaving the pool.

- **The physical discipline (E20/F4), with one named departure.** The image mutates before durability: the guest writes it live, across a session, not one command. On `Committed` the host keeps the materialization; on any other outcome it deletes the local image, folds nothing, and steps down, and the next activation rematerializes from the composite snapshot plus committed capture manifests. The image is a rebuildable cache (§1), keyed under `EntityConfig::data_dir` by node and entity, as the SQL database file is. One departure from the class: for the other physical facets the §6 gates make un-durable state *unobservable*; the disk's un-durable writes **are** observable, to the guest and to whoever is attached to it, before the next capture. The entity-level observables (records, replies, §7.5 reads) still honor the gates; the wider exposure is the consumer's to bound and name (the machine's session-grained barrier, machine §4, its invariant M3).
- **The capture seam.** The host MUST obtain exactly the blocks that changed since the last capture, read at a **quiescent point** (the guest paused or stopped, machine §4), so the read sees a stable image, as the SQL capture reads a settled database after the handler returns. The host never captures a *running* guest's concurrently-written image; the consumer quiesces the guest first. Dirty tracking is the **host's own**, not the VMM's: Firecracker's diff-snapshot tracking covers guest *memory* pages and exposes no block-device write bitmap, so the host, which owns the backing file, tracks writes itself. The reference mechanism is **content-hash pruning**: the host chunks the image and diffs block hashes against the last manifest, so the mismatches *are* the dirty set. It is unprivileged and deterministic under simulation; its cost is a full image read per capture. A **copy-on-write overlay per capture epoch**, whose allocated extents are the dirty set, is a conforming alternative and the deferred upgrade (§16). The obligation is the property, not the mechanism.
- **Checkpoints are the snapshot contribution, and they scan and upload nothing.** The committed form keeps a **per-block `BlobId` index** (the last committed capture of each block, which is also the dirty-tracking baseline above), and the contribution is that index serialized as a **full-coverage manifest**: image size, block size, ordered `(block index, BlobId)` list. Every referenced blob was already `put` by the capture (or import) that committed it, so a checkpoint after a capture re-uploads nothing. The live image is deliberately **not** consulted, because its uncaptured guest writes are not committed state and MUST NOT enter the snapshot (the SQL checkpoint may read its settled database file; the disk's file is never settled while a guest runs). A region no manifest ever covered is absent from the index and restores as zeros: the image file is created sparse and only manifests write it. The live checkpoint manifest's blocks, plus the blocks referenced by capture records after it, are the facet's root set (**F3**); §9 compaction then drops the capture records the checkpoint subsumes.
- **Rehydration.** Composite snapshot → checkpoint manifest → fetch blocks, verified by content (**E17**) → materialize the image → replay decodes capture manifests after the snapshot seq into the pending set → `rehydrate` fetches and applies them the same way (byte-deterministic, **F1**) → boot the microVM against it. The quorum barrier (`head`, §9) is unchanged. Lazy block hydration (faulting blocks in from blobs on first guest access, so a large image activates without materializing whole) is the deferred refinement (§16), the disk analogue of the SQL capture VFS.
- **Bounded, like every physical facet.** The image is fixed-size, bounded by the constant `MAX_IMAGE_BYTES` (16 GiB), which caps an import and every folded manifest; a capture manifest is a small record (indices and ids, never bytes), each block blob is bounded by the fixed block size, and the checkpoint obeys the same blob bounds the SQL and workspace facets obey. A guest cannot make the host materialize an unmetered allocation (sandbox §3.2).

---

### 7.16 The alarm facet (logical)

The alarm facet gives an entity a **durable timer**: at most one pending deadline (the Durable Objects model, DO §5), armed or cancelled through `ctx.alarm()` and fired as a callerless invocation of the entity's `on_alarm` handler. It is the basis for retries, timeouts, batch flushes, and the steps facet's `sleep` (§7.17).

- **Storage is a trivially logical facet.** `Set(due)` and `Clear` records under the alarm tag fold into a single `Option` deadline (last write wins); the folded form is the source of truth for whether and when the entity fires. Arming and cancelling stage into the command's atomic batch like a KV write (§7.12); the encoded form is the snapshot contribution; the root set is empty.
- **Firing is the runtime half, through the same barrier.** After each commit and on activation the host reads the folded deadline and arms an **epoch-guarded in-activation timer**; when it elapses, the host delivers itself a callerless `AlarmDue` command that runs `on_alarm` through the ordinary §6 barrier (input gate, one atomic batch, output gate), so an alarm's effects are as durable and as fenced as any command's. The epoch guard makes a superseded timer's late `AlarmDue` a no-op, a cancellation without a race.
- **Two surfaces here are ungated, and both are dead code rather than a hole.** `on_alarm` (§4.1) is declared on `Entity` itself, so an entity that implements it without declaring `Facets = (Alarm, …)` compiles and is never called: nothing arms a deadline for it, since only the facet's folded form yields one. Hosting through `entities_with_alarms` is ungated in the same way: a type without the facet may be wired to the index and gains nothing. Both are places the compile-time gating of §7.12 stops short, and both are reachable: the hosting bound is a `where` clause on two signatures, and the hook could move behind `EntityRegistry` (which already runs with the facet set in scope) the way a command handler is registered. Until they do, the inertness is stated rather than enforced.
- **Consume-on-fire (E21).** The fired deadline's `Clear` joins the same batch as `on_alarm`'s own records, so the alarm clears atomically with its effects unless the handler re-arms (last write wins in the shared stage): a re-arming handler chains, a consumed alarm stops, and a failed commit leaves the deadline pending to fire again. One arm therefore commits **at most one** effect. The bound is on effects that commit rather than on handler invocations: a fire whose commit fails runs again, which is this invariant holding, not breaking.
- **A pending alarm vetoes hibernation until the index acknowledges it.** An entity owing a wake MUST stay resident, where its in-activation timer fires on time, unless something else will wake it. The per-shard **alarm index** is that something: one small index entity per (entity type, shard) holds an `EntityName → due` map, registered on every alarm change; the alarm **driver** sweeps the index for the shards its node leads and re-activates each entity owing a wake, whose activation re-arms its own timer (**E3**). This covers both deactivation paths, **failover** and **voluntary hibernation**, but the host may hibernate an alarmed entity only when the *current* deadline is **acked-registered**: registration is an `ask` whose reply releases only after the index entity's commit (the output gate, §6), retried on the idle tick until acked, never awaited on the command path. A registration that has not (yet) acked keeps the entity resident. The index is a *hint*, the entity's own facet the source of truth, so a stale entry reconciles on the next activation: a spurious wake at worst, never a lost or double fire.

---

### 7.17 The steps facet (logical)

The steps facet gives an entity **durable step memoization**: the Cloudflare-Workflows-style `step`/`sleep`/`retry` pattern, layered on the event fold, the alarm facet (§7.16), and an entity's self-driving loop (the workflow is the layer that supplies one; the agent runtime is the reference consumer, agent §3).

- **The durable core is a memo.** A steps consumer is a re-entrant decision the entity re-enters after every commit. Each **step** runs an external effect once; the facet journals its result under the steps tag and folds it into a `step id → bytes` map. On any later tick (a re-activation, a failover) a completed step resolves from the map instead of re-running, so effects are at-most-once across crashes. Step ids are the consumer's stable call-site ordinals, deterministic across ticks and replay.

  **The memo is write-once**, and that, not "the effect ran once", is what a test can assert. The launch guard below is per-activation and never journaled, so a re-activation MAY legitimately re-launch a step whose result has not yet committed, and the same effect MAY therefore run several times; what MUST hold is that the **first committed result wins** and every later `StepDone` for that step commits nothing. Verified by `steps.rs` (scripted, single-node) and `steps_swarm.rs` (the same entity under the cluster nemesis), which read a step's memo repeatedly and require every answer to agree.
- **`sleep` is a step whose effect is an alarm** (§7.16); `retry` is a step that re-launches after an alarm-backed backoff.
- **What the entity still wires.** Launching a step's effect off the command path and self-`tell`ing its result back is entity-specific (the effect's type is), so the facet supplies the durable half (the memo's `result`/`record`) plus a generic `StepDone` command that journals a returned result; the "already launched this activation" guard (`LaunchGuard`, generic over the claim key) is ephemeral activation state. A linear `async` DSL over these is deferred (§16).
- **Which half an entity adopts depends on where its results already live.** The memo serves entities whose step results are *not* part of their event vocabulary. An entity whose results are already rich journaled events with their own fold and consumers (the agent's `ModelResponse`/`ToolOutcome` records) MUST NOT memoize them a second time under the steps tag (one decision, two folds); such an entity adopts only the launch-once guard, keyed by its own step identity. The agent is the reference for that posture, and takes its guard from the workflow's per-activation claims rather than from this facet; a plain effect-runner adopts both halves. The half this facet leaves to the entity (the launch, the guard, the self-`tell`, and the recovery of whatever a crash left orphaned) is generalized one layer up, by the workflow ([`workflow-spec.md`](workflow-spec.md)), which re-derives what the guard is keyed by from committed state instead of tracking it. Nothing here rests on it: the memo and the guard stand on their own, and an entity wanting both declares the steps facet beside the workflow.

---

## 8. The single-writer fence

An entity must have one writer. The fence is the shard's **leadership term**: one leader per term, and a per-entity append that carries that term as its fencing token.

- **One leader per term.** A shard's leader-election group is a small Raft group (§7.1) that elects at most one leader per term and records the shard's replica set. The leader is the single writer for every entity in the shard's range and hosts their activations (§5.2). Electing a leader is the *only* agreement round, and it happens on failover (§8.3), never per write.
- **Every append carries the term.** An entity write is a per-entity quorum append (§7.2) stamped with the leader's term. Each replica persists the highest term it has acknowledged for the shard and refuses any append stamped lower. A write commits only while a quorum still recognizes its term, the fencing-token discipline with the shard term as the token.
- **The persisted fence MUST be keyed by the shard's whole identity, `(entity_type, index)`.** A shard index alone does not name a shard: the same index under two entity types is two leader-election groups (§8.2), whose terms advance independently and at unrelated rates. Keying a replica's persisted term by index alone therefore lets the type that elects more often raise the fence past the quieter type's term, on a shard nothing has gone wrong with, after which every append of that type is refused (`NotLeader`, so a live activation steps down) and every recovery read is refused too (`Unavailable`, so the next activation cannot rehydrate and the entity never returns). Neither half can recover on its own, because no leader of that group can reach a term above a fence its neighbour raised. The corollary binds the deployment, not just the store: a node hosting several types MUST NOT share one store across them, since sharing the store is how the fence gets shared. Where a store is obtained from a factory, taking the entity type as part of the key makes that structural.
- **A deposed leader cannot commit.** A new leader raises the term; the old leader's in-flight appends are then refused by every replica that has seen it, reach no quorum, and return `NotLeader` or `Unavailable` (§7.3). Two nodes that briefly both believe they lead the shard cannot both commit, because a quorum acknowledges only one term, so the minority's writes never land and the entity's record never forks (invariant **E1**).
- **A new leader recovers each entity's head from a quorum.** Because durability is a per-entity quorum append and *not* a shared replicated log, a fresh leader does not inherit every entity's latest records the way a Raft leader inherits its log. Before serving an entity it recovers that entity's head and tail from the shard's replicas (the rehydration barrier, §9): it fence-reads every replica, waits at most `recover_timeout` (Appendix A) for each, and merges the replies that arrive. The recovery is a read-repair, not a bare read. For each `Seq` slot the leader takes the record carried under the **highest term** any replica in the quorum holds; a partitioned replica MAY hold a stale-term record at a slot a higher term later won, so slot occupancy alone does not decide it. When the replies form a quorum, the leader **writes the recovered tail back** to a quorum under its own term before serving, so the head it adopts is itself quorum-durable and no later recovery can regress it. Short of a quorum within the wait the leader falls back to the merged local view without write-back and serves from it (the relaxed read-your-leader contract of §7.5, staleness never invention). That is write-safe because the fallback head commits nothing on its own: every append still needs a quorum, and a replica refuses an append whose slots it already holds under another term or whose head is below its base (**E14**). Four things make the recovery `Unavailable`: an election in progress, a store that has fenced the node behind a higher term, a local store that has failed (§11), and a confirmed read whose write-back reaches no quorum. A decision that moves data, a split or merge transfer (§7.7), MUST NOT use the fallback; it requires the confirmed quorum. By quorum intersection, any record acknowledged on a quorum is present, under its committing term, on at least one replica the new leader reads, so no acknowledged write is lost (invariant **E14**). E14 rests on this **quorum read-repair of a per-entity head**, in place of leader-completeness over a shared log.
- **The host steps down cleanly.** On `NotLeader` the host deactivates the entity and the caller retries against the new leader (§5.4, §6 step 5). The host MUST NOT keep serving an entity whose shard it no longer leads.

### 8.1 Why a term, not a clock

The fence is a logical term, not a wall-clock lease. Leadership is a quorum fact: a leader that pauses and wakes finds a quorum has moved to a higher term and refuses its appends, whatever the clock skew, so no time-based expiry is needed for *write* safety. A clock lease returns only as the deferred optimization for linearizable *reads* (§7.5, §16), where the goal is to skip a quorum round, not to fence writes. The per-entity `Seq` orders an entity's own records and aligns its snapshots (§9); it never arbitrates writers; the term does.

### 8.2 The actor control plane, the shard maps, and the shard leadership groups, side by side

Three consensus layers operate, at different rates, and none touches entity data:
- the **actor control plane** (one leader-based group, actor §9.4.3) owns cluster membership and seeds the map groups' voters, changing on cluster events;
- the **shard map** per entity type (one group per type, §7.6) owns the allocation (which nodes form each shard and which key range it covers), changing on splits, merges, reconfiguration, and membership;
- the **shard leader-election groups** (O(shards) groups) own placement (which replica leads each shard and under which term), changing on failover and reconfiguration, never on an entity write.

The entities' **data** sits under none of them: it is off consensus entirely, a per-entity quorum append (§7.2) carrying the shard term. Every group derives a distinct, non-colliding group id (the membership control group is id 0; a shard map uses a reserved shard index; shard leader-election groups hash `(entity_type, index)`), so none is on another's hot path. The total group count is `O(shards) + O(entity types) + 1`, bounded by split/merge (§7.7) and the number of hosted types, never O(entities), and never one per write (**E7**).

### 8.3 Failover

**A planned departure SHOULD hand leadership off rather than drop it.** Everything below describes an *unplanned* loss, where waiting out an election is the only option. A node that is being shut down deliberately (a rolling upgrade, a scale-in, a drain) knows it is leaving, and SHOULD therefore hand each shard it leads to one of that shard's other replicas before it stops, using the leadership transfer of actor §9.4.3 item 8. Not doing so costs availability rather than correctness, and the cost scales with how well the cluster is packed: a node leading *S* shards imposes *S* failover windows at once, each ending in the rehydration of every active entity on that shard (§9), all while the replacement node is starting cold. The handoff is best-effort by nature, since it can only target a replica already caught up to the leader's last entry, so a shard whose peers are lagging or unreachable falls back to the unplanned path below, and a departing node MUST NOT block its own shutdown waiting for one.

When a shard's leader fails or is partitioned away, the shard's replicas elect a new leader by ordinary Raft over the leader-election group, raising the term. The new leader holds the shard at once, but, unlike a shared-log leader, it does not yet hold the entities' records: it recovers each entity's committed head and tail from the shard's replicas as that entity is activated (§8, §9, §10), lazily on first access: a quorum read-repair, or the bounded-wait local fallback of §8 when a quorum does not answer in time. The window between the old leader's loss and the new election is the shard's **failover window**, during which calls to that shard's entities fail fast (`NotLeader` then a retry, or `Unavailable`, §11) rather than block. No acknowledged write is lost: acknowledgment required a quorum, and quorum intersection guarantees the new leader's recovery read sees it (invariant **E14**).

---

## 9. Snapshots and compaction

Replaying a long event history on every activation is wasteful, so an entity periodically snapshots its folded state.

- **Snapshot policy.** After a commit, the host MAY persist a snapshot `(head, State)` for the entity via `save_snapshot` (§7.3). The trigger (`snapshot_every`, a record count) is configuration, not part of the model. Only the shard leader writes snapshots, so a deposed leader cannot (its `save_snapshot` returns `NotLeader`). **One trigger governs every snapshot the host takes**, including the one on idle hibernation (§10): a snapshot is worth taking or it is not, and the answer cannot depend on which path arrived at it.
- **What a snapshot costs, and therefore how often.** On the `Quorum` tier the composite record goes to every replica and is quorum-counted like a record append (§7.3), so its cost is multiplied by *R−1*. What that record *contains* is the lever: bulk facet bytes and, past 64 KiB, facet 0's state itself are content-addressed chunks in the blob area, and only the chunks that changed are put (below), so a snapshot is roughly O(delta) plus a manifest rather than O(state). What it buys is a shorter replay, and replay reads are served from the **local** store (§9 rehydration, §7.5), costing no round trips at all. A deployment SHOULD still set the trigger high rather than low: chunking lowers the per-snapshot bytes, not the round trips, and a snapshot is a round of puts plus a record however little changed. The counter-pressure is failover rather than steady state: a new leader recovers the records above the last snapshot from a quorum (§8), so a larger gap means a larger one-off recovery read per entity, once per leadership change. See [hardware-envelope](hardware-envelope.md) §3.9 for the arithmetic these defaults are derived from. An entity type whose events are large relative to its state inverts the trade and SHOULD lower it.
- **The rehydration barrier (§10).** Before reading an entity's head, the leader MUST run the recovery read over the shard's replicas; it does not hold the entity's records merely by winning the shard (§8). This recovery is `head` (§7.3): with a quorum answering within `recover_timeout` it returns the quorum-confirmed head and backfills any records the leader is missing, so a freshly-activated entity never rebuilds from a stale or partial local view and then folds onto a short head or serves stale reads; short of a quorum it adopts the merged local view, which MAY be stale but cannot commit a write (§8). It is a no-op on the `Local` tier, whose single store *is* the committed state.
- **Rehydration (§10).** On activation, after the barrier, the leader loads the entity's latest snapshot `(s_seq, s_state)` by `load_snapshot` (§7.3), a local read that on the `Quorum` tier returns the quorum-best snapshot the barrier's recovery selected and cached. It then loads the records after `s_seq` up to the recovered head, replaying them and folding via `apply` (§4.1) to reach the head. The head is set **only** from journal/snapshot returns, never trusted from a prior activation's memory (invariant **E3**).
- **A snapshot MUST NOT shorten the effective log** (invariant **E4**). This holds *structurally*: a head is defined as the best snapshot's `Seq` plus the contiguous run of records above it (`ReadReply::head` on the `Local` tier, the recovery merge's base on the `Quorum` tier, §8), so a snapshot can never outrun the head it is measured against, and the ordering needs no runtime check. The host nonetheless refuses a snapshot whose `s_seq` exceeds the recovered head: defence in depth against a future store whose two answers could disagree, and the safe direction to fail in, since the journal is the authority on where an entity ends (**E3**). Refusing is not a *recovery*, though, and the snapshot is not merely an optimization: once compaction has run, the subsumed prefix exists **only** in the snapshot (below), so a replay that discarded one would fold a truncated state rather than the true head. The replay therefore checks the store's base against its cursor: an untruncated journal replays whole from `ZERO`, and a compacted one **fails the activation**: a base above the replay cursor is history the store can no longer serve, surfaced as unavailability rather than folded over, the same loud-failure rule as an unrecognized facet tag (**E19**). E4 rests on the head always covering the snapshot, not on the guard being able to rebuild what the snapshot alone still holds.

**Encoding: the record envelope, the composite stamp, and chunked state.** These are the bytes at rest behind the facet model of §7.12; a facet author never writes them.

**The tag space reserves its high bit, and is split in two.** A facet tag is `0..=0x7F`; a leading byte at or above `0x80` is not a tag but the mark of another **envelope version** ([compatibility](compatibility-spec.md) §3), and a build MUST refuse such a record by version rather than report it as an unrecognized tag; the two have different causes and different fixes.

Within the tag range, `0..=0x3F` is reserved for facets defined in this crate and `0x40..=0x7F` for facets defined outside it, so the two can be allocated independently. A tag is permanent, the dispatch key for every record ever written under it, so a collision between a built-in facet and a third-party one is not a build error but two readings of the same durable byte. Version 1 is `[tag][payload]`, so the reservation costs nothing on disk and every record ever written is already a stamped version-1 record. It does **not** version a facet's *payload* schema, which evolves separately: `postcard` appends enum variants safely, so a facet's operation enum grows at its end.

**Facet 0's state is chunked by content.** Past a threshold (64 KiB) the encoded `State` is cut into content-addressed chunks and the composite carries only their ids, exactly as the checkpointing facets (§7.12) carry theirs. The cut is **content-defined** rather than at fixed offsets. A SQLite page and a disk block have fixed addresses, so an unchanged one occupies the same offsets in the next image; a re-encoded `State` has no such addressing, since inserting a turn into a folded transcript moves every byte after it and the length prefixes ahead of it. A rolling hash chooses each boundary from the surrounding bytes, so the chunks after an edit are byte-identical to the ones before it. A chunk already in the entity's root set is **not put again**; that skip makes the snapshot cost proportional to what changed rather than to what accumulated, and without it the chunked form would be worse than inline. The chunk puts complete before the record naming them is written, so a durable manifest never references a chunk that is not durable. Under the threshold the state stays inline: the chunked form spends a quorum round trip to save bytes, and at that size the bytes are not worth it.

The payload is **stamped** with a magic and a version ([compatibility](compatibility-spec.md) §3), read before the body so a composite from another version is refused rather than misparsed. A snapshot gets a full stamp where a record makes do with a reserved bit, because `postcard` leaves its leading bytes no structure a reader could recognize the format by. The stamped body also records **which codec encoded facet 0's contribution**: that contribution is the one part of a snapshot that is not codec-independent (a user type under the deployment's codec, §4.1, §5, while facet payloads are `postcard`), so without it a changed codec would turn every stored snapshot into a corrupt-entity report rather than the misconfiguration it is. A codec mismatch MUST name both codecs and abort the activation. The body carries facet 0's state as one of two forms, inline bytes or the manifest of its chunks, so a reader can never mistake a chunked state for an empty one.

Compaction, truncating an entity's record prefix once a snapshot subsumes it, is a per-replica operation of the Replicator, below the seam: a replica drops the records up to a snapshot's `Seq` when it stores that snapshot, advancing a per-entity **base** (the file-backed store then rewrites its on-disk log to a single checkpoint, reclaiming the space). The base is not internal bookkeeping: every `load` page reports it, read together with the records under one segment lock (§7.3), and every event read carries it to the caller (§7.5, §4.3); compaction changes what a reader of *history* can see (folded state is untouched; the snapshot subsumes the prefix), and a contract that silently served a truncated log as if complete would turn every long-lived entity's first snapshot into invisible data loss for its observers. Its safety rests on two facts, so it need not wait for the snapshot to be durable on a quorum. First, a snapshot is only ever taken at the **committed head** (§9), so every record it subsumes was already quorum-committed. Second, a fresh leader recovers an entity's head by taking the **highest snapshot `Seq` any replica in its read quorum holds** as the head base and merging the records above it (§8). A replica has dropped a record prefix only if it holds a snapshot covering that prefix, so any recovery quorum finds, for every committed record, *either* the record itself *or* a snapshot that subsumes it, never neither. Quorum intersection thus still loses no acknowledged write (invariant **E14**). Per-entity snapshots let an entity's history compact independently of its shard-mates.

---

## 10. Lifecycle: activation, migration, hibernation

An entity's lifecycle, from the host's view, is a loop of:

```
activate → rehydrate (snapshot + replay, §9) → on_activate → serve commands → idle → on_deactivate → (snapshot) → stop
```

- **Activation** is triggered by the first message for the name reaching its shard leader's gateway (§5.3). The leader rehydrates the entity (§9), runs `on_activate`, then serves. Activation takes no consensus: no election, no agreement round; on the `Quorum` tier it costs one quorum-recovery round-trip to confirm the entity's head (§9), and on the `Local` tier it is fully local.
- **Migration** follows shard leadership. When the shard's leader changes (§8.3), the entity's activation moves to the new leader and rehydrates there. In-memory state is never preserved across the move; the journal rebuilds it.
- **Hibernation (deactivate-on-idle).** After an idle interval the leader MAY, *if the entity's `can_hibernate` permits it*, run `on_deactivate`, snapshot **if §9's trigger has been crossed**, and `ctx.stop()`, dropping the entity's in-memory state. (`on_deactivate` cannot mutate state, §4.3, so it runs before or after the snapshot indistinguishably; the implementation runs it first.) The snapshot MUST go through the same trigger the write path uses and MUST NOT be unconditional: on the `Quorum` tier it reaches every replica (§9), so an unconditional one would spend a replication per idle window on every entity that is written once and then left alone; chunking facet 0's state (§9) makes that smaller, never free. An entity that vetoes eviction (`can_hibernate` returns false, e.g. an autonomous entity with a live run) is left running and the idle check rescheduled. The gateway prunes the name from its activation table (it watches its hosts via death watch, actor §12). The next command (or subscribe) re-activates and rehydrates; the gateway-level event read deliberately does **not** (§7.5), so a hibernated entity can be observed without paying a rehydration. Hibernation reclaims memory; persisted storage survives; in-memory state was only ever a cache (§1).
- **Forced step-down.** When leadership moves (§8) or the shard goes unavailable (§11), the activation deactivates involuntarily: it runs `on_deactivate` (to release non-durable per-activation resources) but takes **no snapshot**, since the journal may be unwritable, then emits `Deactivated` and stops. The journal is the authority; the next access rehydrates.
  - **Default and tuning.** The *mechanism* is the Durable Objects eviction window (DO §5); the *number* is not, because DO's ten seconds prices resident memory on shared multi-tenant hosts and this deployment's does not ([hardware-envelope](hardware-envelope.md) §1, §3.6). Reactivation is cheap but not free, costing a quorum head-recovery round trip plus a snapshot-bounded replay (§9), while what eviction reclaims is an entity's state on a node with memory to spare, so `idle_after` SHOULD default to **minutes** rather than seconds, long enough that an entity in intermittent use stays resident between touches. A deployment whose entities are many, rarely touched, and individually large SHOULD shorten it. To avoid thrashing when an entity is accessed just slower than the timer, an implementation SHOULD apply a small minimum residency or jitter so a barely-idle entity is not evicted and reloaded repeatedly; the reference implementation applies neither.
- **Eviction races.** If a command reaches a cached host that has stopped, the host `ask` returns `CallError::DeadLetter`. `EntityRef` treats that as proof the command never ran (only `DeadLetter` qualifies, never an ambiguous `Unreachable`/`Timeout`): it drops its cached host entry, re-resolves through the gateway, and re-issues the command exactly once under the caller's original deadline. On that re-resolve the gateway finds the table entry's host no longer resolvable locally, prunes it without waiting for `Terminated`, and activates afresh. Two attempts bound the loop: a second `DeadLetter` surfaces to the caller.
- **Subscriptions are ephemeral activation state.** An entity's record sinks live only in the activation and are dropped on every deactivation path; subscribers re-subscribe against the current leader and backfill from their last seq (§7.9).

Durable **alarms** (§7.16) intersect this lifecycle in two places: a pending alarm vetoes idle hibernation until the per-shard alarm index has acknowledged the deadline, and after a failover that same index re-activates each entity owing a callerless wake, which no message would otherwise touch.

---

## 11. Failure model and the node-down cascade

The entity layer inherits the actor failure model (actor §8) and adds the durability outcomes. The blast radius of a failure is the **shard**, not the cluster and not the single entity.

When a node fails (actor §8.1):
1. **Shard leadership.** Every shard the node led re-elects a leader among its remaining replicas (§8.3). The shards it merely followed continue under their existing leaders.
2. **In-flight callers.** Every pending `ask` to an entity on the failed leader completes with `EntityError` wrapping `CallError::Unreachable` (actor §7.2, §8.1), never a hang; the caller retries against the new leader.
3. **Watchers.** An entity MAY be watched as any actor; watchers receive `Terminated { NodeDown }` (actor §12) for the activation.
4. **Re-activation.** The next message re-activates each affected entity on its shard's new leader, which rehydrates from the log (§9). No acknowledged write is lost (invariant **E14**).

**A replica whose storage fails withdraws; it does not crash.** A replica that cannot make a write durable MUST NOT acknowledge it, but it MUST report the refusal rather than terminate: an I/O failure (a full volume, a lost mount, a failing device) makes the store answer a distinguished *failed* outcome to every subsequent record store, recovery read, and blob store, and that outcome MUST NOT count toward any write or read quorum. The node then drops out of the quorums of every shard it replicates, which the majority rules of §7.2 and §8 already handle: writes commit on the remaining replicas, and a failed leader's entities cannot recover on it, so the shard elects around it (§8.3). The refusal MUST be distinguishable from a fence (`Fenced`) and a stale head (`Stale`), which are decisions a healthy replica *made* and which carry a term or head the caller acts on; a failed store has decided nothing and its answer carries no such information. The failure SHOULD be one-way and store-wide, since the fault belongs to the volume rather than to the entity whose write first observed it, and nothing below the seam can establish that the volume recovered. Terminating the process instead would convert a local fault into a cluster event (every shard the node led re-elects and rehydrates elsewhere), which is worse than the degradation the quorum rules were designed to absorb.

**Quorum loss is unavailability, not a fork (CP).** If a shard cannot reach a quorum, whether its leader-election group cannot elect a leader or an entity's Replicator cannot reach a write quorum, it commits nothing. `append` returns `Unavailable`, and every entity in that shard pauses writes (§6 step 6): effects are not applied, the reply is `EntityError::Unavailable`, and the affected activation steps down (the outcome is ambiguous, since a timed-out commit MAY land later, §7.2/§7.3, so the in-memory head is no longer trusted; the next access rehydrates from the journal). Callers retry or fail over. The shard does not fork, because a minority cannot commit: the leader-election group elects no leader on the minority side, and any stale-term append is refused by the quorum (§8). Only that shard's entities are affected; the rest of the cluster serves normally. Reads remain read-your-leader (§7.5): until the check-quorum read lease of §7.5/§16 exists, an isolated minority leader cannot fence itself and MAY serve a stale read, though it can commit nothing; with that lease, a leader holding a valid one MAY keep serving linearizable reads.

---

## 12. Error model

The entity layer calls surface two failure layers, kept distinct exactly as the actor framework keeps `CallError` and `M::Reply` apart (actor §14):

```rust
pub enum EntityError {
    Call(CallError),       // transport/system failure reaching the activation (actor §14.1),
                           //   including CallError::Unhandled for an unregistered manifest (§5.5)
    NotLeader(NodeId),     // leadership moved; the runtime retries against the hint, surfacing this only if retries are exhausted
    Unavailable(String),   // the entity's shard cannot reach a quorum; the outcome is ambiguous,
                           //   and the write MAY still become visible later (§11).
                           //   On the non-activating read (§7.5): the leader's local journal read
                           //   failed: transient, and a read commits nothing, so re-asking is safe
}
```

- **Application errors** the handler deliberately produced live **inside `M::Reply`** (e.g. `Result<T, E>`, §4.2), never in `EntityError`.
- `NotLeader` is normally absorbed by the runtime's bounded redirect (§5.4); it surfaces to the caller only when retries are exhausted. `Unavailable` is a real durability outcome the caller must handle.
- `EntityError` MUST be exhaustive at the public API, so callers handle partial failure explicitly (actor §14).

---

## 13. Security and observability

- **Security.** The entity layer adds no transport; it inherits mutual-TLS associations, the handshake allowlist, and the deserialization allowlist (the entity dispatch registry, §5.5) from actor §15. The gateway MAY consult an `Authorizer` per `(peer, EntityName, manifest)` before activating or dispatching, as actor §15 item 5 gates `deliver`.
- **Observability.** The entity layer emits, on the actor framework's single extensible `Event` stream (actor §16), the entity-lifecycle events `Activated`, `Rehydrated { from_snapshot, replayed }`, `Committed { shard, seq }`, `Snapshotted { at }`, and `Deactivated`. Every variant also carries the `node` that hosts the activation (its shard's leader, §5.2) and the entity `name`, which makes the per-node activation guarantee (**E6**) expressible as a continuous checker over the stream; `Committed` also carries the `shard` the entity committed under, so a split or merge is checkable as a move that keeps `seq` strictly increasing across the boundary (**E15**). The shard events `LeaderChanged { shard, term }` (emitted by a new shard leader, once per term it wins), `ShardSplit { parent, child, boundary }`, and `ShardMerged { left, right }` (emitted by each node as it applies the committed partition change, §7.7) drive both operator tooling and the simulator's invariant checks (§14). Record subscriptions (§7.9) add **no** content-bearing event: they reuse `Committed { seq }` as their commit signal and deliver the records out of band, keeping the journal the one place an entity's content lives.
- **Metrics are a second, separate sink.** The event stream is the *checker's* interface: one structured record per lifecycle transition, so the simulator can assert an invariant over an exact history (§14). That is the wrong shape for an operator: it emits per message, its volume scales with traffic, and answering "is commit latency rising?" from it means rebuilding a distribution from a firehose. A node therefore SHOULD also expose a **metrics sink**: a small set of aggregates costing O(1) per operation. An implementation SHOULD report three measurements: the **commit round**, labelled by its outcome (`Committed`/`NotLeader`/`Unavailable`, §6 steps 4–6), because a fast refusal and a fast commit mean opposite things and averaging them hides both; **rehydration** (§9), separately, because it is the cost of a *cold* access and the thing hibernation trades against memory (§10); and the **resident activation count**, which is what hibernation exists to bound. The sink is **node-scoped**, like the blocking-I/O seam and for the same reason (§7.4): it describes the node an operator is judging, not one type it happens to host. Its vocabulary MUST be closed rather than an open `incr(name, labels)` surface, and its keys no wider than an entity type: unbounded label cardinality is the ordinary way a metrics pipeline falls over, and a per-*shard* key is unbounded by §7.7's own elasticity. What is missing from that list is missing on purpose: leadership changes, splits, and merges are *transitions*, already carried exactly once each by `LeaderChanged`, `ShardSplit`, and `ShardMerged` above, and a counter would restate them less precisely.

---

## 14. Testability and deterministic simulation

The entity layer is testable by the same deterministic simulation as the actor framework (actor §18): a whole cluster of entities runs in one process, on one logical thread, over virtual time, network, and randomness, so a single seed reproduces a run exactly.

- The journal (§7.3) is a **simulation seam**, like `Transport`, and so are the parts below it. The shard **leader-election group** reuses the actor framework's Raft (the leader-based control plane's implementation, actor §9.4.3), so simulation drives the real consensus code, not a model of it; the **Replicator** (`Local`/`Quorum`) is exercised directly on the per-entity quorum-append path.
- Reusing the framework's seams (`Clock`/`Entropy`/`Spawner`/`Transport`, actor §4.6, §7) means simulation runs the real host, gateway, shard, and rehydration code.
- Fault injection MUST be able to produce: a shard **leader** crash and election mid-write (to exercise the term fence and quorum recovery, §8); **leader-election loss** (no leader elected) and **per-entity write-quorum loss** as distinct routes to `Unavailable` (§11); a stale-term append from a deposed leader racing a new election (to exercise the fencing token, §8); a timed-out append that commits late, re-read on the next activation (to exercise idempotency by `Seq` slot without a dedup token, §7.2); eviction mid-command (to exercise rehydration and the output gate, §6, §10); and a shard split under concurrent writes (to exercise §7.7). A no-fault run is the simplest case and MUST pass.
- For **record subscriptions** (§7.9), fault injection MUST be able to produce: a **leader move mid-stream** (the subscriber re-subscribes against the new leader and backfills, reconstructing the sequence with no gap or duplicate); a **slow sink** whose buffer overflows (dropped, then re-subscribed and backfilled); **hibernation and reactivation** under a live subscription; and a **timed-out append that commits late** (delivered or backfilled once, at its slot). In every case the sink's seq-reconciled sequence MUST equal what `load` to the head returns (**E16**).

---

## 15. Invariant catalogue and conformance

These invariants appear as MUSTs above; collected here, they are the contract a conforming implementation verifies, each holding even under the faults of §14. They are checked the way actor §18.5/§18.6 prescribe: continuous checkers over the §13 event stream for safety properties, targeted tests for the rest, compile-fail for type-safety.

The catalogue is machine-readable as `e_catalogue()` beside this crate's conformance suite, guarded by the same drift-test pattern as its siblings (`conformance_catalogue.rs`): a renamed or deleted suite fails the build rather than leaving the column below pointing at nothing. Test files named here are under `crates/entity/tests/`.

| | Invariant | Defined in | Verified by |
|---|---|---|---|
| **E1** | **Single writer per entity.** Only the shard leader appends; the leader-election group elects one leader per term and every append carries the term as a fencing token, so a deposed leader reaches no quorum and an entity's record never forks. A commit advances the entity's head by exactly the batch appended; the host folds only such a contiguous commit and steps down on any other outcome. | §6, §8 | `clustered_entities.rs`, `entities.rs`, `partition_safety.rs` |
| **E2** | **Deterministic fold.** `apply` produces identical state on live commit and on replay from any snapshot/journal prefix. | §4.1 | `clustered_entities.rs`, `entities.rs`, `entity_swarm.rs` |
| **E3** | **EntityJournal is the source of truth.** `head` and state derive only from journal/snapshot returns; in-memory state is never trusted across activation. | §1, §9 | `entities.rs`, `entity_swarm.rs`, `disk_swarm.rs`, `sql_swarm.rs`, `subscription_swarm.rs` |
| **E4** | **Snapshot never shortens the log.** A head is *defined* as the best snapshot's seq plus the contiguous run of records above it (`ReadReply::head` on `Local`, `merge`'s base on `Quorum`), so a snapshot can never outrun the head it is measured against. The host additionally refuses a snapshot the head does not cover, replaying from the journal instead (**E3**). | §9 | `entities.rs`: `a_snapshot_never_outruns_the_head_it_is_measured_against` (the structural identity), `a_snapshot_beyond_the_committed_head_is_refused_in_favour_of_the_journal` (the guard's direction, via a store double) |
| **E5** | **Reply iff durable.** A command's reply is released, and its events folded, only after the entry commits; a `NotLeader`/`Unavailable` outcome yields an error, no fold, no success reply. | §6 | `clustered_entities.rs`, `entities.rs`, `entity_swarm.rs`, `disk_swarm.rs`, `sql_swarm.rs`, `subscription_swarm.rs` |
| **E6** | **Exactly-once activation per node.** The serial gateway activates a name at most once; concurrent requests find the same host. | §5.3 | `entities.rs` (a count of `Activated` events), `entity_swarm.rs`, `disk_swarm.rs`, `sql_swarm.rs` (the `ActivationSingletonPerNode` checker) |
| **E7** | **Bounded consensus groups.** The cluster runs `O(shards) + O(entity types)` Raft groups (one **leader-election group** per shard plus one shard-map group per type, §7.6), kept bounded by split/merge, never O(entities) and never one, with **no consensus on the data path**, which is a per-entity quorum append (§7.2). | §7.1, §7.2, §7.6, §7.7 | `clustered_entities.rs`, `shard_split.rs` |
| **E8** | **Activation without consensus.** Activating or hibernating an entity touches no consensus group: no election, no agreement round (on the `Quorum` tier it pays at most one quorum head-recovery round-trip, §9, never a consensus round); only shard membership/leadership/split touches a consensus group. | §5.2, §7.8, §10 | `clustered_entities.rs` |
| **E9** | **Control plane off the data path.** The shard map changes only on cluster events; no entity write or activation contacts it. | §7.6 | `clustered_entities.rs` |
| **E10** | **Type-safe calls.** A command an entity has no `EntityHandler` for does not compile. | §4.3 | `compile_fail.rs` (trybuild) |
| **E11** | **CP under partition.** A shard that cannot reach a quorum pauses its entities' writes (`Unavailable`) and never forks; other shards serve. | §11 | `clustered_entities.rs`, `partition_safety.rs` |
| **E12** | **Hibernation round-trip.** An entity evicted when idle re-activates with identical state via snapshot + replay; no acknowledged write is lost. | §9, §10 | `clustered_entities.rs`, `entities.rs`, `entity_swarm.rs`, `facets.rs`, `disk_local.rs`, `sql.rs`, `ws_local.rs` |
| **E13** | **Location transparency.** A call to a local versus remote entity produces observably identical replies and ordering. | §5.4 | `clustered_entities.rs`, `entity_swarm.rs`, `blobs.rs`, `blob_swarm.rs`, `disk_swarm.rs`, `sql_swarm.rs`, `subscription_swarm.rs`, `ws_clustered.rs`, `ws_swarm.rs` |
| **E14** | **Lossless failover.** A new shard leader recovers each entity's committed head on activation: it fence-reads a read quorum, takes the highest-term record per slot, and writes the merged tail back to a quorum under its own term before serving; by quorum intersection no acknowledged write is lost across a leadership change. | §8, §8.3 | `clustered_entities.rs`, `entity_swarm.rs`, `partition_safety.rs`, `raft_journal.rs`, `ws_clustered.rs`, `ws_swarm.rs` |
| **E15** | **Split/merge safety.** An entity is writable in exactly one shard at any time; a split or merge transfers the committed prefix atomically and loses or duplicates no write. | §7.7 | `shard_split.rs` |
| **E16** | **Subscriptions are observational and lossless-by-seq.** Every committed record is delivered to a live sink or left as a gap the sink closes by `load`; subscription delivery never gates a commit, never forks state, and never advances an entity's head. By seq reconciliation a sink reconstructs the exact committed sequence regardless of drops, duplicates, reordering, moves, or hibernation (push ⊆ `load`). | §7.9 | `subscription_faults.rs`, `subscription_swarm.rs`, `entity_swarm.rs` |
| **E17** | **Blob address integrity.** `ctx.blobs().get(id)` returns bytes whose BLAKE3 hash equals `id`, or an error; it never returns wrong or corrupt bytes. Verification is on the read path, after any network transfer; on the `Quorum` tier a corrupt copy falls through to a verifying replica. | §7.10 | `blobs.rs`, `blob_swarm.rs`, `disk_local.rs`, `disk_swarm.rs`, `sql_swarm.rs` |
| **E18** | **Blob durability, idempotence, and entity-scoped reclamation.** A blob `put` is acknowledged only once stored on a write quorum of the entity's replicas (one local copy on `Local`), always including the leader, with no consensus on the path; equal content under an entity stores once; the blob survives the loss of a minority of replicas. Reclamation (`gc`/`destroy`) is entity-scoped, monotonic, and idempotent, and never resurrects a referenced blob. | §7.10 | `blobs.rs`, `blob_swarm.rs` |
| **E19** | **Facet atomicity and interpretation safety.** A command's records across all facets commit as one atomic batch or not at all; a multi-facet entity's records carry facet tags; replay dispatches by tag and aborts activation on an unrecognized tag; the composite snapshot restores every facet to one seq. | §7.12 | `facets.rs`, `disk_swarm.rs`, `sql_swarm.rs`, `ws_local.rs` |
| **E20** | **Physical facets expose only durable state.** A physical facet's materialization is a rebuildable cache: its mutations are unobservable before the commit (the §6 gates) and it is discarded on any non-committed outcome; rehydration from the composite snapshot plus records reproduces it exactly. | §7.12, §7.14 | `sql.rs`, `ws_local.rs` |
| **E21** | **An alarm fires at most once per arm.** One armed deadline commits at most one callerless `on_alarm` effect: the fired deadline's `Clear` joins the same atomic batch as the handler's own records, and the epoch guard makes a superseded timer's late delivery a no-op. The bound is on effects that **commit**, not on handler invocations: a fire whose commit fails leaves the deadline pending and runs again, which is the guarantee holding rather than breaking. | §7.16 | `alarm.rs`, `alarm_cluster.rs`, `alarm_swarm.rs` |

An `entity` implementation conforms iff every invariant holds, verified under deterministic simulation (§14) for the distributed ones and by compile-fail tests for **E10**. Facets add the per-facet contract **F1–F4** (§7.12), verified generically per facet under the same simulation.

---

## 16. Extensions (deferred)

Named here, specified elsewhere or later, so the core stays small:

- **A linear step DSL.** An `async` `step`/`sleep`/`retry` surface over the steps facet's memo and commands (§7.17), so a long-running procedure reads as straight-line code rather than a re-entrant loop. It would be written over the workflow's re-entrant cycle rather than beside it.
- **Hibernatable connections.** Parking a record subscription (§7.9), or a WebSocket/stream connection, *across* hibernation, holding the registration while the entity sleeps and re-delivering on wake (DO §5), so a follower need not re-subscribe after every idle eviction.
- **Third-party facets.** Publishing the facet seam (§7.12) for out-of-crate facets, once the record-tag registry and cross-version compatibility rules have settled.
- **SQL lazy page hydration (a capture VFS).** A VFS shim for the SQL facet (§7.14) that both captures WAL frames below SQLite and faults database pages in from blobs on first access, so a large database activates without materializing whole, the upgrade over the reference WAL-tailing capture.
- **Workspace scaling: ranged sub-records and lazy hydration.** For the workspace facet (§7.11): splitting a changed file larger than a record bound into ranged sub-records (today one `Write` record carries a whole file, bounded by the 64 MiB tree cap); faulting files in from checkpoint chunks on first access so a large workspace activates without materializing whole; and preserving mtimes across the microVM tar pull so capture's stat-prune survives an fc command (today each fc command re-hashes the tree).
- **Disk lazy block hydration.** For the disk facet (§7.15): faulting image blocks in from checkpoint blobs on first guest access, so a large rootfs activates without materializing whole. The disk analogue of the SQL capture VFS above.
- **Disk overlay dirty tracking.** For the disk facet (§7.15): the copy-on-write overlay per capture epoch (a device-mapper snapshot or a userspace block device presenting the image), whose allocated extents are the dirty set, replacing the reference content-hash scan for images large enough that a per-capture full read is the dominant cost. Privileged, Linux-only, and isolated behind the facet's private dirty-set seam, so it swaps in without touching the facet contract.
- **Range-verified blob streaming.** Verifying a byte range of one large blob against the BLAKE3 tree the `BlobId` already roots (the Bao encoding, §7.10), so large blobs need not be fetched whole; a ranged `get` obtains and verifies the whole blob and then slices it. The other half of the DO hot/cold split (DO §6) is not deferred: the rendezvous-placed, namespace-scoped content store for cross-entity dedup, archival, and an external object-store tier is the [blob store](blob-store-spec.md), specified beside entity rather than under it (§7.10).
- **Linearizable reads (check-quorum lease).** Extending the shard's leader-election group (§8) with a check-quorum property, so the leader serves reads locally while it can still confirm a quorum and the activation self-fences when it can no longer confirm shard ownership, rather than returning stale state (§7.5). Requires a check-quorum primitive on the leader-election group's Raft engine (a leader that has not heard from a quorum within an election timeout is no longer read-valid).
- **Follower reads.** Serving reads from a shard's followers with a freshness bound, trading linearizability for read scale (§7.5).
- **Cross-entity sagas.** Idempotent multi-entity workflows built above the single-entity consistency boundary (non-goal §2.2).
- **Optional `#[derive(Entity)]`.** Defaulting the manifest and `register` list, as actor §4.4 permits for actors: a convenience above the model, never required.

---

## Appendix A: End-to-end example

```rust
// --- A clustered system in the leader-based mode (actor §9.4.3) that holds the shard map ---
let system = ClusterSystem::start("node-a", config_leader_based).await?;

// --- Host entities of type Account: registers the gateway and joins/leads shards ---
let accounts: Entities<Account> = system.entities(EntityConfig {
    shards: 16,                             // initial partitions of this type's namespace (§7.1)
    replication_factor: 3,                  // replicas per shard (§7.1)
    shard_target_bytes: 256 << 20,          // opt-in auto-split past this size (§7.7); 0 (default) off
    idle_after: Duration::from_secs(300),   // hibernation window (§10); memory is not
                                            //   the scarce resource, round trips are
    snapshot_every: 4096,                   // events (§9); a snapshot reaches every
                                            //   replica, a replay is local reads
    entity_store: None,                      // None ⇒ fresh in-memory store per node; a
                                            //   factory persists records across a cold
                                            //   restart (§7.4, the Raft-WAL analogue, E14);
                                            //   keyed by (entity_type, node), never shared
                                            //   across types (§8, the fence)
    failure_domains: None,                  // None ⇒ domain-blind placement; a mapping
                                            //   caps replicas per rack/zone (§7.1)
    quorum_timeout: Duration::from_secs(2), // append/snapshot/blob-put deadline (§11)
    recover_timeout: Duration::from_secs(2),// head-recovery deadline (§8, §9); separate
                                            //   because a slow recovery is an entity that
                                            //   cannot activate at all
    host_cache_capacity: 65536,             // bound on resolved host handles (§5.4)
    data_dir: None,                         // node-local scratch for physical facets
                                            //   (§7.12/§7.14); None ⇒ the system temp dir
});

// --- Address an entity by name; activate-on-first-use, identical call site local or remote ---
let acct = accounts.entity("account/42");       // EntityRef<Account>, no activation yet
match acct.ask(Withdraw { cents: 500 }).await {
    Ok(Ok(balance))                  => println!("balance now {balance}"),  // committed + durable
    Ok(Err(Overdraft))               => { /* application outcome, inside the reply */ }
    Err(EntityError::Unavailable(_))  => { /* shard quorum lost; retry/failover (CP) */ }
    Err(EntityError::NotLeader(_))    => { /* redirects exhausted within the deadline; try again */ }
    Err(EntityError::Call(e))         => eprintln!("transport: {e:?}"),  // incl. CallError::Unhandled
}
```

**What is *not* in this config, and why.** `EntityConfig` is per entity type, so the two
capabilities that belong to the **node** are not fields of it (§7.4, §13): the blocking-I/O
pool, which exists to bound *this node's* concurrent device work, and the metrics sink,
which describes *this node's* health. Both hang off a node hosting handle instead, set once
and used to host every type:

```rust
let node = system
    .entity_node()                                          // the node's hosting handle
    .blocking_io(Arc::new(ThreadPoolIo::sized_for_host()))   // §7.4; default: inline
    .metrics(Arc::clone(&metrics));                          // §13; default: discarded

let accounts: Entities<Account> = node.entities(config_accounts);
let orders:   Entities<Order>   = node.entities(config_orders); // one pool, one sink
```

A production node over real storage makes all three node-scoped choices in one call,
`EntityNode::durable(system, data)`, which returns the node with a thread pool and a
metrics registry, and the durable store factory every type it hosts shares.

Hosting straight off the system (`system.entities(config)`, above) is the same call with both
defaulted (inline I/O and discarded measurements), which is the `Local` tier's shape and
what the deterministic simulation requires (§14). Making them per-type fields instead would
be expressible but wrong in a way nothing catches: a node hosting three types would write the
same two handles into three configs, and a config that omitted one would silently get a
*second*, inline pool rather than the node's, invisible in a signature and invisible at
runtime, until a stalled device blocks the executor the pool was added to protect.

**Hosting one entity under many type names (`entities_named`).** `entities(config)` hosts a
type under its own `ENTITY_TYPE`, built by `G::default` per activation, the common case.
A caller that must host **one Rust entity as several distinct entity types at runtime** uses
the node hosting handle's `entities_named`, which adds two extension points:

```rust
fn entities_named<G: Entity<System = S>>(                // on EntityNode<S>
    &self,
    entity_type: &'static str,                       // overrides G::ENTITY_TYPE (§5.1)
    config: EntityConfig,
    factory: Arc<dyn Fn() -> G + Send + Sync>,      // overrides G::default (per-activation seam injection)
) -> Entities<G>;
// node.entities(config) == node.entities_named(G::ENTITY_TYPE, config, Arc::new(G::default))
```

Each `entity_type` is a fully distinct entity type: its own gateway (one `gateway_key` per
name), its own shard map, and, in the `Quorum` tier, its own leader-election groups (the group id hashes
`(entity_type, index)`, §8.2). The same key under two names addresses two independent entities.
`entity_type` MUST be stable cluster-wide and across runs, exactly as `ENTITY_TYPE` must be
(§5.1); the `&'static str` makes that lifetime explicit (a deployment leaks its bounded set
of names if they are not literals). The `factory` lets the runtime inject per-node seam
handles into each fresh activation, so the entity needs no `Default`. This is the seam the
agent runtime rides: each *kind* is its own entity type (`KindId` IS the `entity_type`),
hosted under one shared agent run loop.

## Appendix B: Suggested crate layout

```
entity/                 # the entity runtime, built on actor-core + actor-cluster
  entity.rs               # Entity, EntityHandler, EntityCtx, EntityName,
                         #   EntityRegistry (the per-entity dispatch builder, §4, §5.5)
  host.rs                # the host actor: durability protocol, rehydrate, hibernate (§6, §9, §10)
  gateway.rs             # per-node gateway: routing, activation table, NotLeader redirect, non-activating read (§5.3, §5.4, §7.5)
  entity_ref.rs          # EntityRef, the Entities type handle (whose `entity(key)` addresses
                         #   one), and the system extension that hosts a type (§4.3, §5.4)
  journal.rs             # the EntityJournal seam + AppendOutcome + Seq + DynEntityJournal (§7.3)
  election.rs            # the read-only view of a shard's leader-election group (is_leader, term,
                         #   leader_hint) over the Raft group shardmap.rs creates and reconfigures (§8)
  replicator.rs          # the Replicator: per-entity quorum append fenced by the shard term; its
                         #   shard handle is the clustered EntityJournal (Quorum tier, §7, §7.2, §7.4)
  store.rs               # the per-node EntityStore seam: term-fenced records keyed by (shard, entity),
                         #   plus the in-memory MemoryEntityStore reference impl (§7.2, §7.4)
  replica_store.rs       # the per-node replica-store actor + ReplicaTransport: the Quorum replicator
                         #   quorum-appends to and recovers from peers over actor messaging (§7.2, §8)
  shardmap.rs            # the consensus-agreed shard map (per-type map group); allocator + reconcile (§7.6, §7.7)
  memory.rs              # the single-node journal: Local replicator + in-memory event log (tier 1, §7.4)
  file_store.rs          # the file-backed EntityStore, a per-entity `wal::Wal` op log per segment:
                         #   a node's records durable across a restart, so a cold-restarted cluster
                         #   recovers each entity from a quorum (§7.4, E14)
  blobs.rs               # the entity-native content-addressed blob store: BlobId + EntityBlobs
                         #   (put/get/has/gc/destroy/repair_facets), colocated with the journal (§7.10)
  subscription.rs        # record subscriptions (the journal follower): the push analogue of
                         #   load: live delivery of committed records to a Subscription's sink (§7.9)
  system.rs              # the EntitySystem capability seam + name→shard/group hashing (§5.1, §7)
  event.rs               # EntityEvent observability stream (§13)
  config.rs              # EntityConfig (Appendix A)
  node.rs                # the node's hosting handle: the capabilities every type it hosts
                         #   shares: the BlockingIo pool (§7.4) and the metrics sink (§13)
  blocking.rs            # the BlockingIo seam (§7.4): where a store's fsync blocks:
                         #   inline by default, a thread pool on real storage
  metrics.rs             # the operator-facing metrics sink (§13): a closed vocabulary of
                         #   counters and fixed-bucket histograms, keyed by entity type
  cdc.rs                 # content-defined chunking (§9): the rolling-hash split that
                         #   makes facet 0's snapshot cost what changed, not what accumulated
  error.rs               # EntityError (§12)
  ws.rs                  # the workspace facet (§7.11): a real per-entity directory,
                         #   captured file deltas as records, checkpoint chunks as blobs
  facet.rs               # the facet seam (§7.12): tags, staging, the composite
                         #   snapshot, and the host's unioned blob roots
  kv.rs                  # the KV facet (§7.13): an ordered map folded from
                         #   Put/Delete records, transparent blob spill for large values
  sql.rs                 # the SQL facet (§7.14, `sql` feature): a private on-disk
                         #   SQLite per entity: WAL-frame records, checkpoint chunks as blobs
  disk.rs                # the disk facet (§7.15): a fixed-size raw block image per entity:
                         #   dirty blocks as blobs, one capture-manifest record per capture
  alarm.rs               # the alarm facet (§7.16): one durable deadline per entity, folded
                         #   from Set/Clear records and fired as a callerless AlarmDue
  alarm_index.rs         # the per-shard alarm index (§7.16): the durable EntityName → due
                         #   directory a new leader sweeps to re-activate entities owing a wake
  steps.rs               # the steps facet (§7.17): durable step memoization
                         #   (step/sleep/retry) plus the launch-once guard
  facet_blobs.rs         # blob machinery shared by the checkpointing facets (§7.12): the F3
                         #   root-keeping discipline and the chunked blob-area transfers
  testing.rs             # shared simulation invariants over entity events (§14), and the store
                         #   doubles StaticEntityStore and SnapshotAheadOfHeadStore behind E4's test
  cluster.rs             # the sharding and replication namespace: a re-export surface over
                         #   system.rs, shardmap.rs, replica_store.rs, replicator.rs, node.rs
                         #   and alarm_index.rs, defining nothing of its own
```

(`entity_ref.rs` rather than `ref.rs`, since `ref` is a reserved word.)

**The public surface is a deep root and three deep modules.** The root carries the
authoring vocabulary and nothing else: `Entity`, `EntityCtx`, `EntityHandler`,
`EntityName`, `EntityRegistry`, `EntityRef`, `Entities`, `EntityExt`, `EntityError`,
`EntityEvent`, `EntityConfig`, `EntitySystem`, `NoEvent`, `EventHistory`,
`accepted_manifests`, and `Seq`. Everything else is reached through
`entity::facet` (the facet family of §7.11 to §7.17 and its seam), `entity::store`
(the durability substrate a deployment wires: the `EntityStore` and `EntityJournal`
seams, the stock implementations, `BlockingIo`, the record subscriptions, and the
commit metrics), or `entity::cluster` (§7.1, §7.2, §7.6, §7.7 above).

The root is not a fourth namespace, and deliberately so: authoring an entity is
written in the names above, so putting them behind a module every consumer would
open in full would add a layer that relays and says nothing. The split is drawn
where the crate's own dependencies are already drawn: `entity::store` and
`entity::cluster` name the entity programming model only through `EntityName`.

`entity` depends on `actor-core` (the model and seams), `actor-cluster` (the
clustered `ActorSystem`, the leader-based control plane, and the Raft
implementation the shards reuse), `actor-serialization` (the codec and
dispatch building blocks), `wal` (the crash-safe log primitive under the file
store), and `compat` (the stamps records and snapshots are read by version
through). It is orthogonal to and independent of the agentic
`agent` crate. As in the actor framework, no *required* macro crate exists; any
`#[derive(Entity)]` is an optional convenience layered above the model.
