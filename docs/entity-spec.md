# Entity: Durable Objects for the Distributed Actor Framework

**Status:** Draft v3
**Scope:** An **entity**: a virtual, durable, single-activation object addressed by a global name, with colocated event-sourced storage and a durability barrier on the reply path, built on the actor framework of [`distributed-actor-spec.md`](distributed-actor-spec.md).

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Throughout, `entity` is the crate and namespace name and an entity is the durable object. A `§` with no document name refers to a section of *this* spec, and `actor §N` refers to [`distributed-actor-spec.md`](distributed-actor-spec.md). The design lessons come from [`../research/durable-objects.md`](../research/durable-objects.md) (cited as **DO §N**).

> **Design stance.** An entity is an **actor plus three things**: a name-based virtual identity, a durable event-sourced journal, and a durability barrier on the reply. Everything else (mailboxes, serial execution, location-transparent `ask`/`tell`, membership, failure detection, supervision, death watch, the receptionist; actor §3–§13) is inherited unchanged. The layer adds no transport and no required macros (actor §1.1).
>
> The architecture rests on one idea: **each entity is its own single writer, so its journal replicates independently, with no shared log and no consensus on the data path.** One writer assigns each record the next `Seq`, so durability is a quorum append with no agreement round per write (§7.2). Only *placement* (which node may write a shard's entities) needs consensus: a small per-shard Raft group elects one leader per term and holds only leadership, term, and the replica set (§7.1, §8). The cluster runs O(shards) such groups plus O(entity types) shard-map groups (§7.6), bounded by splitting (§7.7), never one group per entity nor one global log (DO §4.3–§4.4).
>
> **Storage** takes the same shape: two replication primitives (per-entity ordered, term-fenced records, §7.2; immutable content-addressed blobs, §7.10), and every further durable feature is a **facet** interpreting them (§7.12), never a new replication mechanism (DO §2, §6).

---

## 1. What an entity is

An **entity** is single-threaded compute fused with strongly-consistent, durable, colocated storage, addressed by a globally-unique **name**. At most one live **activation** exists in the cluster at a time, and every message for the name routes to it. The runtime creates the activation on first access, migrates it, and evicts it when idle; durable state survives all three. The identity is permanent; the activation is disposable.

It is the actor model (actor §3) specialized in four ways:

| | Actor (actor spec) | Entity (this spec) |
|---|---|---|
| Identity | `ActorId` = node + path + incarnation, minted at `spawn` | a stable user-chosen **`EntityName`**; the activation's `ActorId` is incidental |
| Lifecycle | explicit `spawn`/stop | **virtual**: activate on first message, hibernate on idle (§10) |
| State | in-memory only, lost on stop | **event-sourced journal + snapshots**, source of truth (§7, §9) |
| Reply | synchronous, no durability barrier | held until the producing events are **durable** (§6) |

The activation is a **cache** of state folded from the journal; the journal is the source of truth (§7, invariant **E3**).

---

## 2. Goals and non-goals

### 2.1 Goals
- **Global name, single activation.** Address an entity by a stable name from any node; the runtime guarantees at most one live activation and routes to it (§5).
- **Durable by construction.** A handler's effects become visible only after its events are durable (§6); a crash loses no acknowledged write.
- **Strong consistency that scales with the cluster, not the object count.** Consensus is sharded (§7): O(shards) Raft groups, split as the cluster grows, with no cluster-wide log on the write path and no per-entity group. Activation takes no consensus (§10).
- **Single-writer safety under partition.** The shard leader is an entity's only writer, and a stale-term append cannot reach a quorum, so two leaders never both commit and an entity never forks (§8).
- **Pluggability.** The journal is a seam with two reference *durability tiers*, single-node `Local` and clustered `Quorum` (§7.4), over a per-shard leader-election group for placement (§7.1) and per-entity quorum replication for durability (§7.2).
- **One substrate, many facets.** Durable features beyond the event fold are **facets** (§7.11, §7.13–§7.17), each an interpretation over the two storage primitives, sharing one journal, one atomic commit per command, and one composite snapshot (§7.12). A new feature MUST NOT add a third replication mechanism.
- **One programming model.** The handler is ordinary sequential Rust; the input and output gates (§6) supply atomicity and durability-before-effects with no explicit locking.

### 2.2 Non-goals
- **A single cluster-wide log.** Entity MUST NOT serialize all entities through one Raft/Paxos group (§7).
- **A consensus group per entity.** Entity MUST NOT run one Raft group per entity; the unit of consensus is the shard's leader-election group, and entity data needs only a per-entity quorum append (§7.1, §7.2).
- **Cross-entity transactions.** Each entity is its own consistency boundary; multi-entity work is built above this layer with sagas and idempotency (§16).
- **Exactly-once side effects.** Delivery is at-most-once and effectful messages are never auto-retried (actor §7.2); idempotency is the caller's responsibility.
- **Write availability during quorum loss.** A shard without a quorum pauses writes (`Unavailable`) rather than forking (§11): CP, not AP.
- **Required codegen.** An optional `#[derive(Entity)]` MAY default the manifest/`register` boilerplate (actor §1.1), but the hand-written form is normative.

---

## 3. Terminology

| Term | Definition |
|---|---|
| **Entity** | A virtual, durable, single-activation object: state + behavior addressed by an `EntityName`. |
| **`EntityName`** | The stable, cluster-wide, serializable identity of an entity. A `(entity_type, key)` pair; `key` is an arbitrary application string. |
| **Shard** | A partition of one entity type's namespace and the unit of *placement*: a small per-shard Raft group (the **leader-election group**) owning leadership and term, whose Raft membership is the *realized* replica set for the entities in its range. It holds *no* entity data (§7.1, §8). |
| **Shard leader** | The node elected by a shard's leader-election group to write and host every entity in the shard's range, for one term (§5.2, §8). |
| **Replicator** | The per-entity durability mechanism: quorum-appends an entity's records to the shard's replicas, fenced by the shard term, and recovers the entity's head from a quorum on activation. Two **durability tiers**: `Local`, `Quorum` (§7.2, §7.4). |
| **Shard map** | The consensus-agreed *intended* allocation: which nodes replicate each shard and which key range it covers. A per-entity-type map group seeded from the leader-based control plane, reconciled into each shard's realized membership (§7.6). |
| **Activation** | The live, in-memory instance of an entity on its shard leader. Disposable; rebuilt from the journal. |
| **Event** | A serializable value appended to an entity's journal; the unit of durable change. |
| **`apply`** | The pure fold of an event into state; identical on live commit and replay. |
| **State** | The fold of an entity's events; the snapshot payload; a cache of the journal. |
| **EntityJournal** | An entity's durable, totally-ordered, append-only event log, quorum-appended by its **Replicator** (§7.3). The source of truth. |
| **Snapshot** | A persisted `(Seq, state)` checkpoint that bounds replay (§9); with facets, the composite of every facet's contribution at one seq (§7.12). |
| **Facet** | A durable storage feature of one entity (the event fold, §7.11, §7.13–§7.17), defined entirely by what its records mean, what it keeps in blobs, and what it contributes to the snapshot (§7.12). |
| **Facet tag** | The stable identifier on each record that dispatches it to its facet on replay; an unrecognized tag aborts activation (§7.12). Tags are `0..=0x7F`; the high bit marks the record envelope's version (§9). |
| **`Seq`** | The position of an event in one entity's total order; first event at 1. |
| **Base** | An entity's compaction floor: the highest `Seq` its store has truncated behind a snapshot. Slots at or below it live only in the snapshot, so every event read reports it (§7.5, §9). |
| **Event projection** | The view of the journal an event read returns: `EntityRef::events` in full, the subscription sink's record filter in part (§7.5). |
| **Durability tier** | Which Replicator a deployment runs, `Local` or `Quorum` (§7.4). Unrelated to the agent runtime's *capability* tiers (agent §5.6). |
| **Quorum** | The majority of a shard's replicas whose acknowledgment makes a per-entity append durable (§7.2) or commits a leader-election entry. |
| **Term** | The shard's leadership term; it increases on every leadership change and is the fencing token every append carries (§8). |
| **Gateway** | The node-local actor that routes a call to the shard leader and owns the activation table for shards this node leads (§5.3). |
| **Host** | The per-entity actor on the shard leader: holds state and head and drives the durability protocol (§6, §10). |

---

## 4. The entity model

### 4.1 The `Entity` trait

An entity is event-sourced. The author implements the **behavior** (immutable configuration) as a type, declares the **state** and **event** types, and writes the pure fold:

```rust
pub trait Entity: Sized + Send + 'static {
    /// The system the activation runs on: an `ActorSystem` (actor §4) plus the
    /// entity host's seams. `LocalSystem` implements it for the `Local` tier,
    /// `ClusterSystem` for the clustered one.
    type System: EntitySystem;

    /// The folded state and snapshot payload, rebuilt from the journal on activation.
    type State: SerializationRequirement + Default;

    /// The journal record type: the unit of durable change.
    type Event: SerializationRequirement;

    /// The declared facet set (§7.12): `()` for none, or a tuple such as `(Kv,)`,
    /// `(Kv, Ws)`. Each facet surfaces a compile-time-gated `EntityCtx` accessor,
    /// contributes tagged records to the per-command batch (E19), joins the
    /// composite snapshot, and adds its blob roots to the unioned live set.
    type Facets: FacetSet;

    /// The type's stable identity: the namespace tag in every `EntityName` of this
    /// type (§5.1) and the receptionist key of its gateway (§5.3). An explicit
    /// constant (e.g. `"bank.Account"`) is REQUIRED because `type_name` is not
    /// rename-stable. It is the *default* type name; one Rust entity MAY be hosted
    /// under several via `entities_named` (Appendix A), and this const is then the
    /// fallback a wire-decoded `EntityRef` recovers.
    const ENTITY_TYPE: &'static str;

    /// Apply one event to state. MUST be pure and deterministic: live commit and
    /// replay MUST agree (invariant E2). It MUST NOT perform I/O, read the clock,
    /// or use entropy.
    fn apply(state: &mut Self::State, event: &Self::Event);

    /// The commands this entity accepts over the network (§5.5), as
    /// `Actor::register` (actor §4.4); the default registers nothing.
    fn register(_r: &mut EntityRegistry<Self>) {}

    /// Called once after rehydration, before the first command. Err aborts
    /// activation (the triggering message fails).
    fn on_activate(&mut self, _ctx: &EntityCtx<Self>)
        -> impl Future<Output = Result<(), BoxError>> + Send { async { Ok(()) } }

    /// Called once before deactivation: idle eviction, handoff, or forced
    /// step-down. Runs even when the journal is unwritable (it cannot persist);
    /// releases non-durable per-activation resources (e.g. the agent runtime's
    /// sandbox, agent §5.3).
    fn on_deactivate(&mut self, _ctx: &EntityCtx<Self>)
        -> impl Future<Output = ()> + Send { async {} }

    /// Whether the activation MAY idle-hibernate now (§10); consulted only on idle
    /// eviction, never on forced step-down. An entity with not-yet-journaled
    /// autonomous work (the agent runtime's live run) vetoes eviction, and the host
    /// reschedules the idle check.
    fn can_hibernate(&self, _state: &Self::State) -> bool { true }

    /// The two callerless hooks, no-op by default, both decisions through the §6
    /// barrier (an empty result commits nothing): `on_alarm` (§7.16) and
    /// `on_peer_terminated` (§4.3). `on_alarm` is not gated by the facet set: an
    /// entity without the alarm facet may implement it and is never called.
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
    /// Decide the outcome of a command: return the events to persist and the reply.
    /// It MUST NOT mutate state (only `apply` does, §4.1) and MUST NOT perform
    /// durable I/O (the host owns persistence, §6). A read-only command returns
    /// `(vec![], reply)`, which commits nothing (§7.5).
    fn handle(&self, state: &Self::State, msg: M, ctx: &EntityCtx<Self>)
        -> impl Future<Output = (Vec<Self::Event>, M::Reply)> + Send;
}
```

This **decide/apply split** is normative, in place of an imperative `ctx.persist(event).await`, because:

1. **No interior mutability.** The host mutates state and head only through its own `&mut self` on the serial executor (actor §6); a `persist` under the shared `&EntityCtx` would force an `Arc<Mutex<…>>` over state (actor §3.5).
2. **`Send` for free.** Nothing crosses an `.await` but the user's own values (actor §3.2).
3. **Atomic batch per command.** A command's events commit in one append (§7.3); no observer sees a partial command.
4. **One durability barrier**, in the host (§6), not scattered across handlers.

**Facets stage; the commit applies (§7.12).** With facets, the handler also works imperatively through accessors (`ctx.kv()`, `ctx.ws()`, `ctx.sql()`). A *logical* facet stages operations in a per-command overlay (reads see committed-plus-staged) that become tagged records folded only on commit. A *physical* facet mutates a rebuildable local materialization (§1), captured into records before the append and discarded on any non-committed outcome (§7.11, §7.14). Either way, committed state changes only at the commit point.

A reply reflecting post-command state computes it from `&State` plus the emitted events. The host MAY expose a non-normative convenience that folds the emitted events into a scratch copy of state for a reply closure.

**Application errors are values.** A fallible command uses `type Reply = Result<T, E>` (actor §3.2, §14), distinct from a transport failure (`CallError`) or a durability failure (`EntityError`, §12).

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

`EntityRef<G>` is the only handle to an entity. It is `Clone + Serialize + DeserializeOwned + Send + Sync`, carries the `EntityName` plus a system handle, and never grants access to state (actor §3.3):

```rust
impl<G: Entity> EntityRef<G> {
    pub fn name(&self) -> &EntityName;
    pub async fn ask<M>(&self, msg: M)  -> Result<M::Reply, EntityError> where G: EntityHandler<M>, M: Clone;
    pub async fn tell<M>(&self, msg: M) -> Result<(), EntityError>       where G: EntityHandler<M>, M: Clone;
    pub async fn ask_timeout<M>(&self, msg: M, within: Duration) -> Result<M::Reply, EntityError> where G: EntityHandler<M>, M: Clone;

    /// Subscribe to committed records: the committed head at registration plus a
    /// stream of later batches (§7.9). The host does not consult `from`; the
    /// subscriber backfills `from`..`head` by `events`. A framework built-in for
    /// every entity type, needing no registration.
    pub async fn subscribe(&self, from: Seq) -> Result<Subscription<G>, EntityError>;

    /// Up to `limit` committed events after `from` (exclusive), decoded, ascending,
    /// served by the shard leader's gateway from the journal **without
    /// get-or-activating the entity** (§7.5), so polling leaves a hibernated entity
    /// hibernated (§10). A built-in like `subscribe`; returns the event projection
    /// (§7.5).
    pub async fn events(&self, from: Seq, limit: usize)
        -> Result<EventHistory<G::Event>, EntityError>;
}

/// A completed event read plus the highest compaction `base` it observed (§9).
/// An absent slot at or below `base` is truncated history, never a skipped facet
/// record (§7.12); `base <= from` means nothing asked for was compacted.
pub struct EventHistory<E> {
    pub base: Seq,
    pub events: Vec<(Seq, E)>,
}
```

The `G: EntityHandler<M>` bound makes a call the entity does not accept fail to compile (invariant **E10**, actor §3.3). The call site is identical whether the activation is local or remote (§5). `subscribe` and `events` are type-agnostic and carry no such bound.

The `M: Clone` bound lets the runtime re-issue a command whose first attempt provably did not run: a stale cached host that hibernated (`DeadLetter`) or lost leadership (`NotLeader`), neither of which commits (§6, §8). An *ambiguous* failure (`Unreachable`/`Timeout`) is never auto-retried, since the command may have committed (at-most-once, §2.2).

`EntityCtx<G>` is the handler and lifecycle context: the entity's name, its blob area (§7.10), a self-reference, the system handle, and death watch, plus one compile-time-gated accessor per declared facet (§7.12). Surfacing the full actor `Ctx` (child spawning and the rest) is deferred (§16).

```rust
impl<G: Entity> EntityCtx<G> {
    fn name(&self) -> &EntityName;
    fn blobs(&self) -> EntityBlobs;       // the entity's content-addressed blob area (§7.10)
    fn this(&self) -> EntityRef<G>;
    fn system(&self) -> &G::System;
    /// Death-watch an actor (actor §12): when it terminates, node loss included,
    /// `on_peer_terminated` runs as a callerless decision through the §6 barrier.
    /// Installed after the current handler completes. Watches are
    /// activation-local: a rehydrated entity re-watches from its folded state, and
    /// watch-after-death fires immediately.
    fn watch(&self, target: ActorId);
}
```

`EntityCtx` exposes **no** `persist` and no state mutation (§4.2). Its facet accessors (`kv()`, §7.13; `ws()`, §7.11; `sql()`, §7.14; `disk()`, §7.15; `alarm()`, §7.16; `steps()`, §7.17) stage; they never mutate committed state.

`on_peer_terminated(&self, state, ctx, peer: &ActorId, reason) -> Vec<Event>` runs when a watched actor terminates, through the same §6 barrier as any command. It is the basis for liveness-coupled durable state: the machine folds `Detached { FrontDoorLost }` when the front-door member holding an attachment dies (machine §5.1).

---

## 5. Virtual activation and routing

### 5.1 Names and shards

An entity is addressed by `EntityName`, a `(entity_type, key)` pair where `key` is an arbitrary application string (`"account/42"`, a UUID, a tenant id). Unlike an `ActorId` (actor §3.6), it names a logical object, not a node, so it is **not** locality-classifiable.

A stable hash maps every name to exactly one **shard** of its type, a key-range partition (§7.1). That mapping changes only on split or merge (§7.7), not on node churn. The shard's current leader is a second lookup (§5.4): name to shard is stable, shard to leader changes on Raft elections.

`EntityName` is `Clone + Eq + Hash + Send + Sync + Serialize + DeserializeOwned`.

### 5.2 Where an entity lives

An entity activates on the **leader of its shard**, which is one of the shard's replicas: the Replicator fans a record to the peers and writes the leader's own replica concurrently, counting it toward the quorum and always awaiting it before the append returns (§7.2). A blob put likewise lands locally first (§7.10). Rehydration confirms the head from a quorum, then reads locally (§9).

Placement follows the shard's Raft election: a new leader serves the shard's entities, rehydrating them lazily on first access (§10). The cluster balances load by moving shard leadership and splitting shards (§7.7), not by moving individual entities.

### 5.3 The gateway and exactly-once activation

Each node runs one **gateway** actor per entity *type*, registered in the receptionist (actor §13) under one well-known key per type. It routes inbound calls (§5.4) and, for shards this node leads, owns the **activation table** `EntityName → Host`. The table is mandatory because an activation's `ActorId` is minted at `spawn` (actor §4.2) and is not derivable from the name.

Activation is **exactly-once per node by construction**: the gateway is a serial actor (actor §6), so two concurrent requests for an inactive name run in sequence, the first activating the host and the second finding it, with no lock (invariant **E6**).

The gateway also serves the **non-activating event read** (§7.5): one server-bounded page from the leader's local `load` (§7.3), touching no host, so it costs the serial gateway one bounded local copy. Its one slow step, a hibernated entity's committed-head recovery, runs on a spawned task while the caller retries.

### 5.4 Routing a call

`EntityRef::ask<M>(msg)` resolves as follows:

1. **Name to shard** by the stable hash (§5.1).
2. **Shard to host.** A handle minted by an `Entities` on this node keeps a bounded cache of the hosts it has reached. On a node that replicates the shard the current leader is a local read, so a cached host no longer on it is dropped before use; elsewhere a cached host is used until an outcome shows it stale. A miss asks a gateway, this node's first.
3. **Send the command to the host.** The gateway is asked for the *name* only: it gets-or-activates the host and returns its `ActorRef`. The command never passes through the gateway.
   - **Local leader:** the typed command goes straight into the host's mailbox, unserialized (actor §4.3).
   - **Remote leader:** one envelope carrying the `MANIFEST` and codec-encoded payload, which the host's handler registry (§5.5) decodes, dispatches, and answers.
4. **Stale leader:** a node that no longer leads the shard replies `NotLeader(hint)`. The caller follows the hint to that node's gateway (§5.3), backs off while the shard elects, and re-discovers another gateway when its target is unreachable, all within its own deadline: ordinary Raft client redirection.

Consequences of caching (§7.8):
- The gateway serializes only the activation-table mutation; steady-state calls go straight to the active host's mailbox.
- A node that does not replicate the shard reads no shard map: a stale entry shows up as `NotLeader`, `DeadLetter`, or `Unreachable`, so steady-state routing costs no control-plane traffic.
- The host cache MUST be **bounded** (`host_cache_capacity`, Appendix A). It is keyed by entity *name*, so a long-lived client (the gateway fronts every tenant) would otherwise keep an entry for every name it ever addressed. A miss costs only one gateway round trip, so the bound is set by entry cost: no workload may consume the node.

### 5.5 The entity dispatch registry

`EntityRegistry<G>` is the set of command manifests an entity accepts, plus one thunk per type that accepts the command on the **host actor's** `HandlerRegistry` (actor §4.4). `Entity::register` fills it via `r.accept::<M>()`, as `Actor::register` does, and hosting the type replays the thunks onto the host, so decoding, dispatch, and reply encoding are the actor framework's, with no second dispatch path. It is the deserialization allowlist for entity commands (actor §5, §15): an unregistered manifest yields `EntityError::Call(CallError::Unhandled)`, and no type outside the registry is built from network bytes. A command reaches the host in one generic carrier whose manifest is the command's own.

---

## 6. The consistency model: input and output gates

The entity layer lifts the Durable Objects gate model (DO §3) onto the framework, so sequential handler code is race-free and durable.

- **Input gate.** While an entity's append is in flight, the host admits **no new command**; messages queue in the mailbox. This closes the window a serial executor leaves open at `.await` points (actor §6): no command observes half-committed state.
- **Output gate.** The host holds the **reply** (`ReplyHandle::send`, actor §4.5) until the command's events are durable on a quorum (§7). A failed commit turns the reply into an `EntityError` (§12); no observer is told of an effect that did not durably happen.

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
- **Fold only after durability.** The host MUST NOT fold state before the entry commits.
- **Reply only after durability.** The host MUST NOT release the reply before the entry commits, nor as success if it does not commit.
- **Fold only a contiguous head.** A commit MUST advance the head by exactly the batch just appended. Recovery establishes a quorum-durable head before the first append (§9), so this guard is a backstop against residue such as a timed-out append surfacing late (§7.2). On a head that jumps further the host MUST NOT fold (the intervening events were never applied); it steps down and the next access rehydrates (**E3**). Any outcome other than a contiguous commit, `NotLeader` and `Unavailable` included, ends the activation, because the in-memory head is no longer trustworthy. A forced step-down emits `Deactivated` (§13) to keep the lifecycle stream balanced.

**With facets, the protocol generalizes without adding a barrier (§7.12).** Step 1 also drains every facet's stage (logical overlay operations, physical captured deltas) into tagged records that join step 3's single atomic batch (**E19**); step 2 applies when the drained batch is empty. Step 4 folds the logical stages and keeps the physical materializations; steps 5–6 also discard every physical materialization, which the next activation rebuilds (**E20**, §7.14).

**`tell` is fire-and-forget.** It returns once the host accepts the command (actor §3.3, §7.2), so it reports only enqueue-time failures (`Call`, including an unregistered manifest, and `NotLeader`), never `Unavailable`. The host runs the same protocol, but a commit that fails has no effect and notifies no one: the at-most-once contract (actor §7.2).

---

## 7. Durability, replication, and facets

§7.1–§7.10 are the **substrate**, the only layer with a fence, a quorum, and a recovery protocol; §7.11–§7.17 are the **facets** built on it, whose shared contract is §7.12.

### 7.1 Shards: the unit of placement

Each entity type's namespace is partitioned into **shards**, each owning a contiguous range of the type's name hash space. A small per-shard Raft group of *R* replicas (`R=3` or `R=5`), the **leader-election group**, elects one leader per term and records the replica set (§8). It holds *no entity data*, so its log is tiny and changes only on failover and reconfiguration. The leader is the single writer for every entity in the range and hosts their activations (§5.2); each entity's records are durable by a per-entity quorum append (§7.2), not a shared log. Per-type shards key the gateway, dispatch registry, and configuration (Appendix A) by one entity type, so an entity replays through one known `apply` (§4.1).

**Replica placement SHOULD be failure-domain aware.** Rendezvous alone can land all *R* replicas in one rack or zone, and since every durability claim rests on quorum intersection over those replicas (§7.2, §8, **E14**), losing that domain makes the shard unavailable, or unrecoverable if only a minority survives. A deployment that knows its domains SHOULD supply the mapping; the allocator then takes at most `ceil(R / domains)` replicas per domain, filling from the remaining rendezvous order when a lopsided layout cannot meet the cap (an imperfectly spread shard beats an under-replicated one). The mapping MUST evaluate identically on every node, because every node computes the same rendezvous choice (§7.6): static deployment metadata qualifies, anything node-local does not. Without a mapping placement is domain-blind, correct only where there are no distinct domains.

The cluster runs O(shards) leader-election groups plus one map group per type (§7.6), bounded by splitting (§7.7), never O(entities), and none on the data path.

A shard shares leadership and a replica set among its entities, not a serialization point: each entity's records are quorum-appended independently, so a write-heavy entity never queues behind its shard-mates. Splitting a hot shard (§7.7) rebalances placement and replication load, not write bandwidth.

### 7.2 Ordering is free; durability is a quorum append

An entity's single writer is its shard leader (§8), so the host assigns each record the next `Seq` with no agreement round. The **Replicator** fans the record to the shard's replicas and reports it durable once a quorum has stored it (DO §4.3). The only agreement round, leader election, happens on failover (§8.3), never per write.

An entity's records are totally ordered among themselves and unordered against any other entity's; there is no shard-wide log. The host appends in `Seq` order behind its input gate (§6), replay reads in that order (§9), and other entities' appends proceed concurrently on their own quorums.

**Idempotent by sequence slot.** The leader awaits each append's quorum acknowledgment under a timeout that reports `Unavailable` (§11). A timed-out append MAY still reach a quorum later; this never double-applies and needs no de-duplication token, because the host neither retries an append nor folds on anything but a contiguous commit (§6). It steps down, and the next activation recovers the head from a quorum (§9) and folds each record once, at its one `Seq` slot. A stale-term append racing a new leader is refused (the slot is filled at a higher term, §8). The single-writer `Seq` is the record's identity; no proposal id or epoch is needed.

**The rollback is term-bounded.** On any non-committed outcome the leader rolls back its *tentative local* write, so a later recovery that falls back to the local view (`head`, §7.3) never folds an uncommitted record. That rollback MUST drop only slots written **at or below the appending term**: during the failed quorum wait a newer leader may have fenced this store and landed higher-term committed records for the same entity above the same head, and a term-blind rollback would delete them from this replica, dropping a committed write below a quorum (violating **E14**).

### 7.3 The journal seam

The journal is a simulation and deployment seam like `Transport` and `Clock` (actor §4.6, §7), over opaque codec-encoded bytes so it stays codec-agnostic:

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

`Seq::ZERO` is the empty head; the first event commits at `1`. The host calls `append` only on the shard leader and behind the input gate, so `after` always equals the known head; a non-leader call returns `NotLeader`, not a stale-sequence error. Object safety (`impl Future` vs. boxed) is an implementation choice. The full contracts are the trait's rustdoc; `head`'s recovery is specified in §8 and §9, the blob half in §7.10.

**Below the seam: placement and durability.** `EntityJournal` is the *only* seam the host sees. Its clustered implementation rests on two parts:

- the shard's **leader-election group** (§8) supplies placement: who may write, under which term. The journal stamps every append with the current term and answers `NotLeader` from it.
- a per-entity **Replicator** supplies durability: it quorum-appends opaque record bytes to the shard's replicas, fenced by the term, and recovers the committed head from a quorum on activation (§8, §9). The two tiers (§7.4) are Replicator implementations.

The records are the entity's `Event`s, state is the `apply` fold (§4.1), and a snapshot is the serialized `State`; physical facets read records as something else (WAL frames, §7.12, §7.14) over the same substrate. The host depends on nothing below the seam, so the substrate is replaceable.

**Why one trait, named for the records.** The host sees one trait carrying both primitives (§7.10) plus a `term()` that is neither:

- **The blobs ride the record seam** because an entity's blobs share its replicas, activation recovery, migration and split passes, and simulation seed. The cost is a seam the blob half nearly doubles. Splitting it out (a blob supertrait the journal extends) would be cheap and would narrow `EntityBlobs` to the half it calls; it is declined because the two are never deployed apart.
- **The name follows the ordered half**, which owns the `Seq`, the term fence, and recovery; the blob half is defined by having none of them (§7.10), and a neutral name would hide that asymmetry.
- **`term()` reads placement; it does not control it.** The gateway's non-activating read needs a validity token for its cached head floor (§7.5), and routing that question around the seam would give the host a second dependency on the layer the trait hides.

### 7.4 Durability tiers: the two Replicators

The **Replicator** (§7.3) admits multiple implementations; two are reference **durability tiers**, chosen at deployment like a membership mode (actor §9.4). "Tier" has other senses in this specification set, so it is never left bare here.

1. **`Local` (single-node).** One linearizable local store (in-memory, or the file-backed `FileEntityStore`, guarded by an advisory lock on the store directory plus a stamped fence file per shard). An append commits on local fsync; there is no leader-election group and no term. For embedded use, tests, and the deterministic simulator (§14). Trivially CP; not tolerant of node loss.
2. **`Quorum` (clustered).** The fault-tolerant CP production target: a per-entity quorum append over the shard's *R* replicas, fenced by the term (§8). A record survives the loss of any minority of replicas, and a new leader recovers each entity's head from a quorum (§8, **E14**).

**Where a store's blocking I/O runs is a node-scoped seam, `BlockingIo`.** A file-backed store's fsync MUST NOT block the async worker that drives the node's election heartbeats and quorum waits, where one slow device would trigger re-elections and a rehydration storm. A deployment on real storage SHOULD run store calls on a pool (`ThreadPoolIo`); the default `InlineIo` runs them inline, because the simulation (§14) must not depend on OS scheduling. The pool and the metrics sink (§13) belong to the node, so a node MUST share one of each across every type it hosts (Appendix A). The seam changes only which thread blocks, never the order of guard, fence, and apply.

**A durable store records which codec wrote it.** A store whose records outlive their writer MUST carry an `entity.store` stamp ([compatibility](compatibility-spec.md) §3.4) naming the deployment codec, admitted at open before any fence, manifest, or record is read. Event payloads are user types under that codec (§4.1, §5), while facet payloads are `postcard` and a composite snapshot records facet 0's codec itself (§9), so records past the last snapshot have nothing else to notice a codec change: unstamped, each fails alone as a corrupt-entity activation abort; stamped, the mismatch is one refusal at open naming both codecs. The stamp is per **store directory**, since one deployment's codec writes every record under it and a per-record stamp would spend bytes on the hottest durable path. A directory with no stamp is a fresh store and records the running codec.

### 7.5 Reads

A command that emits no events (a query) commits nothing (§6 step 2): the leader serves it from the activation, a local read with **no per-read consensus** (DO §4.3, §4.4) that never waits on replication.

**The event projection: what an event read returns.** `EntityRef::events` (§4.3) returns this view of the journal:

- **The `Seq`s are the journal's own.** Facet records (§7.12) are skipped, leaving gaps. A caller pages by re-asking from the last `Seq` it saw, never by `Seq` arithmetic.
- **A short page means the head.** Fewer than `limit` events come back only at the committed head. A truncated prefix and a facet gap look identical in the `Seq`s, which is why the base is announced rather than deduced.
- **Every reply carries the compaction `base`** (§9). Slots at or below it live only in the snapshot: `base <= from` means the page is complete history from `from`. The base MUST ride **every** page, because compaction waits for no reader's cursor and can outrun a slow pager mid-read.
- **Corruption is loud.** A record that will not split or decode surfaces as `Unavailable` (a failed local read), never a dropped slot; re-asking is always safe.

The subscription sink (§7.9) shares only the first rule: a pushed batch carries no base, and delivery MAY drop, duplicate, or reorder, which the sink's seq reconciliation against `load` absorbs (**E16**).

**The non-activating event read.** The shard leader's **gateway** (§5.3) serves `EntityRef::events` straight from the journal's `load` (§7.3) without get-or-activating the entity, so polling never wakes a hibernated entity (§10). Each wire page is bounded server-side (at most a fixed number of records scanned per ask, SHOULD default to about **1024**) and carries an explicit cursor, an at-head flag, and the base, so a page diluted by facet records still makes progress. The paging loop and event decode run on the **caller**. Like `Activate`, the read is answered only on the leader (a non-leader replies `NotLeader(hint)`, §5.4), so it has exactly the read-your-leader consistency below.

**The read serves at or below a committed-head floor, never the raw store.** The local store cannot tell committed from tentative: a leader writes its own replica before its quorum resolves (§7.2), and a fresh leader's store may hold undecided records or miss committed ones until a `head` recovery read-repairs it (§8). The activation path is shielded by rehydration and the input gate (§6); the gateway read bypasses both, so it MUST bound every page by the committed head. A **resident** activation publishes its committed head to the gateway at recovery and on each commit. For a **hibernated** name the gateway runs one spawned `head` recovery per leadership (§8), which also backfills the leader's store, and caches the floor under the shard term it ran under: the term is the validity token, since regained leadership is always a higher term (§8.1), and a local activation invalidates the entry. Meanwhile the gateway answers a transient *warming* reply, which `EntityRef` absorbs by backing off and re-asking under the caller's deadline, as for an election (§5.4); only the deadline lapsing reaches the caller, as `Unavailable`, which is why §12 has no variant for it. Records above the floor wait for the next poll, so no observer sees an unacknowledged write (**E5**): staleness, never invention.

**The contract is read-your-leader (relaxed), not linearizable under partition.** The read path does not reconfirm leadership per call, so a deposed, not-yet-fenced leader on the minority side MAY serve stale reads until its activation stops. Writes never fork (§8). A caller needing a linearizable read issues a trivial *writing* command: it rides the §6 output gate, so it reflects committed state or fails (`NotLeader`/`Unavailable`) on a deposed leader.

**Linearizable reads are a deferred upgrade (§16)**: a check-quorum property on the leader-election group (§8), not a per-read consensus round. The leader serves reads locally while it holds a **check-quorum lease** (it has heard from a quorum within an election timeout, so no other leader can exist); when the lease lapses the activation **self-fences** with `Unavailable`/`NotLeader` (CP, §11). Follower reads are a separate deferred extension (§16).

### 7.6 The shard map is a consensus-agreed allocation

The **shard map** records each shard's key range and replica set; leadership is the leader-election group's (§8), surfaced by `NotLeader` redirect (§5.4), not tracked in the map. It MUST be **consensus-agreed**, so every node resolves a name to the same replica set regardless of join order; a rendezvous choice over each node's own live membership view would diverge across membership epochs. The reference implementation is a **per-entity-type Raft group whose committed log *is* the allocation**: one `Assign { shard, replicas, range }` record per shard, the range fixed by the founding assignment and ignored on every later one. The map group derives its group id from a reserved shard index, so it never collides with a leader-election group (§8.2).

A leader-only **allocator** keeps each shard's committed replica set equal to its rendezvous choice over the current cluster voters (utilities §2.2's `top`, keyed on the shard's group), and a leader-only **reconcile** loop drives each group it leads (the map group and its shards' leader-election groups) toward its intended membership, so shards rebalance as the cluster grows or shrinks. Since the leader-election group holds no entity data (§7.1), catching up its log is *not* enough: a new replica MUST NOT stand in for a member of the *current* majority before it holds each entity's committed records and latest snapshot, or a recovery quorum could include a voter holding none of an entity's records (breaking **E14**). A target replica counts toward the **target** majority from the `Assign` commit, before its catch-up, which is safe only because a majority of `current` stays required until `Migrated`.

**Replica-set migration.** A changed rendezvous choice never replaces a committed set outright; it runs a **joint-quorum migration**:

1. The allocator proposes `Assign { shard, replicas, range }`; on an allocated shard this commits the new set as the **target**, leaving `current` in place. From then on every append, snapshot, and recovery on the shard uses the **joint quorum** (a majority of `current` AND of `target`, fanned to the union), so every pre-migration and in-migration commit stays intersected by every recovery read. The allocator proposes nothing further for the shard until the migration ends.
2. Every node in the union builds its journal over the joint sets, and the reconcile loop drives the leader-election group toward the union. Replica sets are read per operation, so a continuing replica switches in place.
3. The shard leader's **migration driver** catches every entity up on the target: it enumerates the shard's entities from a read quorum of `current` (any read majority sees every committed entity), then per entity runs the §8 recovery (whose joint write-back lands the records on the target), re-persists the best snapshot on the joint quorum (a compacted prefix exists only there), and copies the blob area verified by content. Every step is idempotent; a leader change re-drives on the new leader, and a failed step retries.
4. Only after a complete pass does the driver propose `Migrated { shard }`, which flips `current = target`; departing nodes drop their journals and the reconcile loop shrinks the group's voters. A departed node keeps a stale view of the group it left, so a routing hint is trusted only while the committed map still lists the hinted node as a replica (§5.4).

**A committed command a node cannot decode MUST stop that node's map, never be skipped.** Skipping would leave a gap in the agreed log, the divergence consensus exists to prevent ([compatibility](compatibility-spec.md) **V2**). A stopped node freezes at the last applied command, hands off every shard it leads (§8.3), and stops its proposers (an allocator on a frozen view would propose founding assignments over real ones). It keeps acknowledging map-group appends and reports why it stopped through `ShardMapSource::poisoned`. Each command carries the `entity.shardmap` stamp ([compatibility](compatibility-spec.md) §3), so another version is refused by version, not by parse failure.

The map changes only on splits and merges (§7.7), replica-set reconfiguration, and membership changes, never on an entity activation or write; routing follows `NotLeader` hints (§5.4) instead of reading it (invariant **E9**). The map group seeds its membership from the **leader-based control plane** (actor §9.4.3), so entity requires that mode; the gossip-based AP mode (actor §9.4.4) cannot agree a shard map.

> **Note.** Actor §9.4.3 item 6 stores the allocation *inside* the control-plane group's log; entity instead runs a dedicated per-type map group seeded from the control plane's voters. The guarantee is identical, but the group count is `O(shards) + O(entity types)` (§8.2, **E7**).

### 7.7 Splitting and merging shards

A shard that grows too large or hot **splits**: the parent keeps the low half `[start, boundary)` and a fresh **child** shard (a new index, hence a new leader-election group, §8.2) takes `[boundary, end]` on the parent's replica set. Two small, cold adjacent shards **merge** the reverse way: the right folds into the left, whose range extends over it, and the right's group retires. Shard count thus tracks load and cluster size, keeping per-shard work bounded as entities climb into the millions.

A split MUST be atomic with respect to writes, in this order:

1. **Seal.** A majority of the parent's replicas durably record an **append bound** at `boundary`, refusing every append whose name hashes at or above it. From then on no append to the moving range can assemble a write quorum at *any* term, even from a parent leader that has not learned of the split. The bound is a store-level quorum barrier, not a leader-local flag, which a higher-term leader lagging the map log would bypass. A refused append surfaces as `NotLeader`/`Unavailable`, never a silent loss (**E5**), and the caller re-resolves against the committed map.
2. **Transfer.** For each moved entity, the parent leader recovers the committed head from a read quorum (§8) and copies the records, latest snapshot, and blob area to the child's keys on the child's replicas: a majority for records and snapshot, best-effort for blobs (a down replica heals on access, §7.10). The copy bypasses the fence under a null term, since the child's keys are fresh and no child leader exists yet.
3. **Commit.** Only after a full transfer pass does the new mapping commit to the shard map (§7.6): the parent's range shrinks, the child's allocation commits, and the child's replicas create its leader-election group. No node routes the moved range to the child before this commit.

No write is lost or duplicated across the boundary, and no entity is writable in two shards at once (invariant **E15**). Every step is idempotent and derived from committed map state, so a crash or leader change mid-split re-drives on the new parent leader. A merge mirrors it, driven by the **right** shard's leader (which holds the term that fences the right's deposed leaders): seal the whole right range, transfer every right entity into the left's keys, then commit, extending the left, dropping the right, and retiring its group (**E7**). Split and merge run one at a time per shard, exclusive with a replica-set migration.

The size trigger is `shard_target_bytes` (Appendix A): the shard leader auto-splits a shard whose durable footprint exceeds it. It is **opt-in**, `0` (the default) leaving the partition to explicit split/merge requests, because a type of individually large entities (a VM image, a big database) wants a higher threshold than a type of many small ones. Splitting on request-rate signals from the §13 event stream is a conforming addition. Progress commits through the shard-map log as `SplitStarted`/`SplitCommitted` (`MergeStarted`/`MergeCommitted`) records validated deterministically against committed state, so an invalid or raced proposal is a no-op on every node.

### 7.8 Scaling: where the cost goes

Cost tracks the *active working set* and the *cluster size*, not the total entity count.

- **Consensus groups:** O(shards) + O(entity types), bounded by split/merge (§7.7), never O(entities); none carries entity data.
- **Control-plane writes:** O(cluster events) (splits, merges, reconfigurations, membership), never per activation or write (§7.6).
- **Activation:** touches no Raft group (§10); on `Quorum` it costs one quorum-recovery round trip (§9), on `Local` nothing. This makes aggressive hibernation affordable.
- **Memory and streams:** bounded by the entities active at once, which hibernation keeps to the working set (§10). Total entity count is limited only by storage.
- **Routing:** two cached lookups, off the control plane and the serial gateway in steady state (§5.4).

The residual cost is placement: a hot shard's entities share one leader node, capped by splitting (§7.7).

---

### 7.9 Record subscriptions (the journal follower)

A **subscription** is live, best-effort delivery of an entity's committed records to a sink, layered over the durable `load` (§7.3), so a follower learns of each commit without polling and without delivery becoming a source of truth.

`EntityRef::subscribe(from)` spawns the **sink** on the *caller's* node (a `RecordSink` actor that decodes each wire `RecordBatch` to events and forwards them on a bounded channel) and sends its `ActorRef` in `Subscribe` to the shard leader (§5.4), whose host registers a bounded **forwarder** task in the activation's **sink set**. The forwarder pushes each committed batch over the transport off the host's executor, so wire backpressure never reaches the write path. The call returns a `Subscription` carrying the committed `head` and a receiver of live batches. The host ignores `from`: it registers the sink at the current head, so the subscriber backfills `from`..`head` by `EntityRef::events` (§7.5) before reconciling the live stream. After each commit (§6 step 4) the host delivers the batch just appended to every sink, **after** the fold and output-gate release; delivery MUST NOT gate the commit or any reply, and is emitted at the same point as `Committed` (§13) (preserves **E5**). `subscribe` is a framework built-in, dispatched for *any* entity type outside the `register` allowlist (§5.5), not a user command.

**Reconcile by `Seq`.** A sink treats `Seq` as authoritative. Each batch carries the `from` it begins after; a sink that has seen up to `last` MUST close any gap (`from > last`) by `load` (§7.3) and MUST ignore records at or below `last` (a re-subscribe replay, or a timed-out append that committed late, §7.2). This absorbs the drops, duplicates, and cross-re-subscribe reordering that at-most-once delivery (actor §7.2) MAY produce. Push is a latency optimization; correctness rests on the journal (**E3**), and the reconciled sequence is exactly what `load(from, …)` to the head returns (**E16**).

**Bounded, never blocking.** Each sink has a bounded delivery buffer; on overflow the host drops the sink rather than awaiting it, since a slow or dead subscriber MUST NOT stall the entity's writes or input gate (§6). A dropped sink sees its stream close, re-subscribes, and backfills from its last seq. Delivery runs through the `Spawner`/`Transport` seams (actor §4.6) and the drop decision depends only on buffer occupancy, not the wall clock, so it stays seed-reproducible (§14).

**Ephemeral.** The sink set is per-activation and never journaled (§1). Hibernation, migration, or forced step-down drops it and emits `Deactivated` (§13); subscribers re-subscribe against the current leader (§5.4) and backfill. A subscription does **not** veto idle eviction (`can_hibernate`, §10): a hibernated entity produces no records, and the next write re-activates it. Parking a subscription across hibernation is deferred (*hibernatable connections*, §16).

### 7.10 The entity-native content-addressed blob store (the second primitive)

Records (§7.2–§7.3) and blobs are entity's **two storage primitives**, the ordered, term-fenced half and the immutable bulk half of durable state, and the only two replication mechanisms; every further storage feature is a facet over them (§7.12).

The journal holds **small foldable state** (§4.1, §9). Bulk immutable bytes (a workspace's file blocks, a database's pages) go in a second store: an **immutable content-addressed blob area**, replicated to the *same* shard replicas, reached through `ctx.blobs()` (DO §2.3, §6). An entity stores bulk bytes by content and keeps only their ids in its state.

The blob path carries **no `Seq`, no term, and no read-repair**. A content hash names one byte sequence forever, so two writers of the same bytes cannot conflict and a reader verifies by re-hashing; the term fence (§8) guards a *mutable* head, which a blob lacks. The one remaining question, "stored on enough replicas yet?", is answered as for a record.

- **The model.** A **`BlobId`** is the 32-byte BLAKE3 digest of the bytes, defined natively by entity. A blob's full address is `(EntityName, BlobId)`: the id is the same everywhere; the entity scopes which area holds it. Blobs are write-once and deduplicated by content **within an entity**. `ctx.blobs()` offers `put` (→ `BlobId`), `get` (whole or a byte range), `has` (present on **any reachable replica**, so a `get` can source it; not a durability measure, since durability is established at `put`), `gc` (keep only a live id set, unioned with the facets' roots, §7.12), `repair_facets` (re-`put` every facet-root blob to the shard's current replica set: additive, idempotent, best-effort, off the activation latency path), and `destroy` (drop the whole area).
- **Verification on read.** Every `get` re-hashes the bytes before returning them and MUST NOT return them on mismatch. On `Quorum` it falls through to the next replica and errors only if no replica yields verifying bytes, so corruption and misdelivery are never silent (**E17**).
- **Durability is a colocated quorum append.** A `put` writes the **local** replica (the leader is a replica, §5.2, so later `get`s are local) and fans out to the peers, acknowledging once a write quorum has stored the bytes; the blob then survives the loss of a minority of replicas. No election, term, or agreement round is on this path (**E18**). A `put` short of a quorum returns `Unavailable`, and retrying is idempotent since the id is a function of the bytes.
- **Reclamation is entity-scoped and root-driven.** The entity knows its live id set, so it reclaims by a **mark-from-roots sweep** (`gc(live)` drops every blob not in `live`) and a whole-area `destroy`. This needs **no namespace tombstone, no resurrection guard, and no cluster-wide delete fan-out**: a delete a partitioned replica misses leaves only *orphan bytes* for a later sweep, never resurrected referenced data, because only referenced blobs are ever fetched or re-replicated.
- **Lazy hydration and corruption self-heal.** A `get` that misses locally fetches a verifying copy from a peer and backfills the local replica, so an entity re-activating on a fresh leader pulls only what it touches (DO §5). The same path heals a *corrupt* local copy: it is evicted, then replaced by the peer's bytes (eviction first, because a content-addressed write of an id already on disk stores nothing).

**Placement is colocation, not rendezvous.** An entity's blobs ride its *own shard replica set*, so reads are leader-local and recovery uses known replicas, at the cost of concentrating its bytes on R replicas. An external object store as the durable home of these blobs, for archival or whole-cluster loss, is the **cold** tier of the DO hot/cold split (DO §6), deferred (§16).

**A replica joining the shard receives the blobs with the records.** Before `Migrated` is proposed (§7.6), the driver copies each entity's blob area to every target replica lacking any of it, sourcing each blob from its own store or a reachable *current* replica, verified by content (**E17**); an unreachable target fails the pass, which retries, so the flip never leaves a target without its blobs. A split or merge transfer (§7.7) copies blobs best-effort, and a replica still lacking one heals by **recovery-on-access** (a verified peer fetch on `get_blob`). Range-verified streaming of one large blob (Bao) is deferred (§16); v1 verifies whole blobs and a consumer chunks beyond a bound.

---

### 7.11 The workspace facet (physical)

The workspace facet gives an entity a **real, node-local directory**: the working set a shell, container, or microVM runs against, with no interposition. The directory is the activation's live truth; durability comes from **capture**: after a command's side effects land on disk, the handler calls `ctx.ws().capture()`, which diffs the durable subtree against the committed index (stat-pruned, content-hash-verified) and stages one record per changed file with its bytes **inline**, plus one record naming deleted paths. These join the command's atomic batch (§7.12, **E19**), so reply, events, and workspace delta commit together or not at all. Replay applies the recorded bytes, never re-executes (**F1**). An unchanged tree stages nothing (§7.5).

- **The physical discipline (E20/F4).** Tools write the directory directly, between and during commands. On `Committed` the host keeps it; on **any** other outcome it deletes it and steps down, and the next activation rematerializes from the composite snapshot plus committed delta records. The directory is keyed under `EntityConfig::data_dir` by **node and entity**, because under the simulator many nodes share one process and a failover must never land two nodes' materializations of one entity on one path.
- **Checkpoints are the snapshot contribution.** At snapshot time every durable file is chunked into **content-addressed blobs** (§7.10) and the contribution is a small `path → chunk ids` manifest; unchanged files hash to stored blobs, so a checkpoint uploads only new content. The chunk ids are the facet's root set (**F3**); §9 compaction then drops the subsumed delta records, so inline bytes do not accumulate. A snapshot taken while an in-flight tool has dirtied the directory fails (it must be the committed image, **E4**); the idle-deactivation snapshot runs quiesced.
- **Scope: files only, excluded trees, a cap.** Directories are implied by their files; empty directories, symlinks, and special files are not durable (matching the sandbox tar paths). Trees in the exclusion list (`target/`, `node_modules/`, and kin) are regenerable caches, never captured, journaled, or restored. The durable subtree is capped at 64 MiB (the sandbox tar bound); an over-cap capture fails as an application error with nothing staged, and the next capture diffs against the last *committed* index, so nothing is skipped. Ranged sub-records and lazy hydration are deferred (§16).
- **Consumption.** The agent runtime declares `Facets = (Ws,)`, hands `ctx.ws().dir_path()` to the sandbox provider at open, and captures in the command that journals each tool outcome, so a tool's result and its files are one quorum append. The persistent machine (machine §3) declares `Facets = (Disk, Alarm, Ws)`, projects the directory into its guest as a tmpfs `/workspace` (pushed at boot, pulled over vsock at each capture quiescent point), and captures in the same command as its disk manifest, so workspace and rootfs commit in one batch (E19); its file commands write the facet directly while no VM runs, so a hibernated machine's workspace is editable without booting.

---

### 7.12 The facet model: one substrate, many storage features

Durable Objects exposes several storage APIs over **one** replicated substrate (DO §4.2); entity does the same over records and blobs (§7.10). A **facet** is a durable storage feature of one entity defined entirely by three answers:

1. what its **records** mean: the ordered mutations it contributes to the journal;
2. what it keeps in **blobs**: bulk immutable bytes, referenced by id;
3. what it contributes to the **snapshot**: its checkpoint at a `Seq`.

A facet gets no replication path, fence, or recovery protocol of its own; those exist once, in the substrate (§7.2, §8, §9, **E14**). Nothing at or below the journal seam (§7.3) changes: everything in this section happens in the host.

**Tagged records, one order, one barrier.** A multi-facet entity's records interleave in its single `Seq` order, each carrying its facet's stable **tag**. A command's events plus every facet's operations append as **one atomic batch** (§6, §7.3), committing everywhere or nowhere (**E19**). Replay dispatches each record by tag; an unrecognized tag MUST abort activation rather than be skipped, so a runtime missing a facet never silently misreads history (**E19**). **Every** record wears the tag, facet 0's included on an entity with no declared facets: one envelope, no second encoding path.

Records and the composite snapshot are both version-stamped, and past a threshold facet 0's state travels as content-defined chunks in the blob area (§9). Subscriptions (§7.9) deliver the **event projection** (§7.5): other facets' seqs appear as gaps the sink's seq reconciliation absorbs (E16 holds per projection).

**Two facet classes**, differing in where a mutation lives before the commit:

- A **logical facet** folds. Its records are semantic operations applied by a pure, deterministic function to an in-memory form (**F1**, generalizing **E2**), folded only after durability (**E1**). The event fold is *facet 0*; the KV map (§7.13) is logical.
- A **physical facet** materializes. Its record is a captured physical delta, applied byte-deterministically on replay (**F1**): WAL frames (§7.14), changed-file bytes (§7.11), a block capture manifest with bytes in blobs (§7.15). Its live form (database file, directory, block image) mutates locally *during* the handler; the disk image also mutates *between* handlers, the one departure, bounded by §7.15. E1 inverts to **expose only after durability; discard on anything else** (**E20**). This is sound because the materialization is a rebuildable cache (§1) and the §6 gates make un-durable state unobservable: the input gate admits no other command during the append, the output gate holds the reply, and a non-committed outcome destroys the only tainted copy.

**Staging: the handler writes through the facet; the commit makes it real.** Each declared facet surfaces an `EntityCtx` accessor (§4.3). A logical accessor stages operations in a **per-command overlay** (reads see committed-plus-staged) that the host drains into tagged records at append time, folds on commit, and drops otherwise. A physical accessor operates on the materialization inside one implicit transaction per command (§7.14). Either way committed state changes only at the commit point (§4.2). Two disciplines follow:

- *Staging is command-scoped.* A facet write outside a command handler (from `on_activate`/`on_deactivate`) has no batch to join and is never silently dropped or deferred: the SQL and disk accessors return an error (`SqlError` for a statement, `DiskError` for a capture or import), and the KV, alarm, steps, and workspace accessors panic.
- *Plan, then stage.* Staged operations commit even when the reply is an application error, so an accessor performs its fallible work (blob puts, scans) first and stages only on success; an operation that returns `Err` leaves no trace (as the KV spill and the workspace capture do, §7.13, §7.11).

**One composite snapshot.** The snapshot at seq *S* is every facet's contribution at *S* in one `save_snapshot` payload: facet 0's `State`, the KV map, the workspace, SQL, and disk checkpoint manifests, the alarm deadline, the steps memo. **E4** applies to the whole: a composite beyond the committed head is ignored entire, and replay runs from `ZERO`. Bulk bytes go to blobs and the composite holds id manifests, because the record, unlike a blob, is broadcast whole to every replica on each snapshot.

**Blob liveness is unioned by the host.** Every facet reports its blob **root set** (**F3**): workspace checkpoint chunks; the SQL facet's live checkpoint pages; the disk facet's live checkpoint blocks plus those its later capture records reference; facet 0's state chunks. A facet MUST NOT sweep the shared area itself: the one sweep is `ctx.blobs().gc(live)`, called by the application with its own roots, and the host-built handle unions in every facet's roots first, so no facet's GC can drop another's bytes.

**Compile-time composition.** An entity declares its facets as an associated tuple on `Entity` (`()` for none), and each accessor is gated by a containment bound, so `ctx.sql()` without the SQL facet does not compile (**E10** applied to storage). The declaration statically fixes the type's record-tag registry, which makes the unknown-tag rule (E19) checkable.

**The facet contract.** Every facet, built-in or future, owes these obligations, verified generically under §14 simulation:

| | Obligation |
|---|---|
| **F1** | **Deterministic interpretation.** A logical facet's fold is pure and deterministic (E2 generalized); a physical facet's delta application is byte-deterministic. |
| **F2** | **Snapshot equivalence.** Restoring the facet's snapshot contribution at *S* and replaying its records in (*S*, head] yields the same form as interpreting the full record history. |
| **F3** | **Complete roots.** The facet's reported root set covers every blob its restored form can reference; nothing it can later `get` is outside it. |
| **F4** | **Discard safety** (physical only). Discarding the materialization at any moment and rehydrating from the composite snapshot plus records reproduces the identical form. |

**Apps are libraries over facets, never facets.** A git-style artifact store is refs in the KV facet plus objects in blobs (§7.10); a work queue is events or KV. A feature earns facet-hood on one of two grounds: it needs **durable records the application's own `Event` type must not have to carry** (its own tag and snapshot contribution: the KV map, workspace, database, image, step memo), or it needs **host participation no library can obtain** (discard on a non-committed outcome, every physical facet, **E20**; callerless delivery driven from the folded form, the alarm, §7.16). Needing a per-command overlay is not a ground: the overlay is what facet-hood *gives*, not what earns it. The examples above are libraries because the application is content to own their vocabulary.

The steps facet is the boundary case (§7.17): the memo earns a tag, while the launch and the guard's keying stay with the entity and are generalized by the workflow, above entity.

**The seam is public and sealed.** The facet trait is published but sealed, so the built-in facets are its only implementations; opening it is deferred (§16).

---

### 7.13 The KV facet (logical)

The KV facet gives an entity an ordered map of small values (configuration, refs, cursors, indices) without an event vocabulary: `ctx.kv()` offers `get`, `put`, `delete`, and ordered prefix listing (`keys`/`list`), staged per command (§7.12), with read-your-staged-writes in the handler.

- **Records.** `Put(key, value)` and `Delete(key)` under the kv tag, folded into an ordered map (**F1**).
- **Snapshot.** The encoded map (**F2**). Keys are unbounded; an inline value is bounded by `INLINE_MAX` (64 KiB, after the Durable Objects KV bound), and a larger value transparently **spills** to the blob area, the map storing its `BlobId` and `get` fetching it verified by content (**E17**). Spilled ids are the root set (**F3**), reclaimed by the host's unioned sweep once their keys are deleted (§7.12).
- **Native, not sugar over SQL.** No C dependency, in-memory form, unchanged under simulation (§14). An entity needing predicate range queries wants the SQL facet, not a richer KV.

---

### 7.14 The SQL facet (physical)

The SQL facet gives an entity a **private, on-disk SQLite database** (DO §2.3, §4.2). The handler runs ordinary SQL through `ctx.sql()` against a local file: reads are leader-local (§7.5), writes are transactional, and nondeterministic SQL (`random()`, `datetime('now')`, autoincrement) is fine, because replication is physical, never re-execution. (A *statement-sourced* entity, SQL text as events, is plain event sourcing with no facet.)

- **One command, one transaction, one record.** `ctx.sql()` wraps a command in one implicit transaction; no transaction spans commands (the DO restriction). At local commit the host captures the transaction's **WAL frames** (page number, page bytes, database size after commit) as one tagged record in the command's batch (§7.12, **E19**). A read-only transaction produces no record (§7.5). A transaction's frame bytes are bounded by configuration, since one record must fit the replicator's message bounds; a bulk load splits across commands. A frame record occupies one `Seq` slot, so a late-landing timed-out append is applied once on recovery (§7.2).
- **The physical discipline (E20/F4).** The database file mutates at local commit, before durability (§7.12). On `Committed` the host keeps it and releases the reply; on **any** other outcome it deletes the file, folds nothing, and steps down, and the next activation rematerializes from the composite snapshot plus committed frame records. The local file is a cache, so `PRAGMA synchronous=OFF` is sound: durability is the quorum ack, never the local disk (DO §4.2).
- **The capture seam.** After a local commit the host MUST obtain exactly that transaction's frames, in order. The reference mechanism is **WAL tailing** (autocheckpoint disabled, the host owning checkpoint timing, as in Litestream); a VFS shim intercepting `-wal` writes is the deferred upgrade (§16), which also enables lazy page hydration.
- **Checkpoints are the snapshot contribution.** On a frames/bytes trigger (configuration like `snapshot_every`) the host checkpoints the WAL into the file (`TRUNCATE`), chunks it into **page-aligned, content-addressed blocks** `put` as blobs (§7.10), and contributes a **manifest** (page size, page count, ordered `BlobId` list). Unchanged pages hash to stored blobs, so a checkpoint uploads only dirty pages (the analogue of SRS's WAL archiving, DO §4.2). The live manifests' pages are the root set (**F3**); §9 compaction then drops the subsumed frame records.
- **Rehydration.** Composite snapshot → manifest → fetch pages, verified (**E17**) → materialize the file → apply later frame records (**F1**) → serve. The quorum barrier (`head`, §9) is unchanged. There is **no out-of-band schema hook**: DDL is a journaled write, so schema setup is idempotent `CREATE … IF NOT EXISTS` at the top of a writing command. A lifecycle hook mutating the database would produce frames no record captures, which is why out-of-command SQL is refused.
- **Dependencies and simulation.** SQLite (bundled `rusqlite`) sits behind the `sql` cargo feature. The database file lives under `EntityConfig::data_dir` (a rebuildable cache, §1, keyed by the entity name's content hash), bringing real file I/O into the hot path; under simulation (§14) tests point `data_dir` at a temp directory, so frame *production* is real SQLite while frame *application* stays seed-driven. A query returns at most `MAX_QUERY_ROWS` (10 000) rows, enforced as an error rather than a truncation so an unexpectedly broad predicate cannot exhaust the leader's memory; beyond it a caller paginates. Enforced per-transaction frame caps and a maximum database size are **deferred** (the other bounds are the replicator's message limits and the node's disk).

---

### 7.15 The disk facet (physical)

The disk facet gives an entity a **raw block image**: a fixed-size disk a microVM mounts as its rootfs, capturing changed **blocks** as the workspace facet captures files (§7.11) and the SQL facet WAL frames (§7.14). A session's dirty set can run to gigabytes, far past the replicator's message bounds, so block bytes never ride the record: a **capture** `put`s each dirty block as a content-addressed blob (§7.10) and stages exactly **one** record, a **capture manifest** (image size and ordered `(block index, BlobId)` list), in the command's atomic batch (§7.12, **E19**). One capture, one record, one commit, so a crash never leaves a committed *prefix* of a capture. The puts precede staging (*plan, then stage*, §7.12); blobs orphaned by a failed commit are unreferenced and swept by the host's unioned GC. An unchanged image stages nothing (§7.5).

The disk facet rides the SQL facet's physical seam (capture at a quiescent point, checkpoint into blobs, discard on non-commit, rehydrate then replay) with dirty blocks for frames and a raw image for the database file. Because its records carry blob ids rather than bytes, it uses the seam's one post-replay hook, **`rehydrate`**: called once after restore and replay, before the first command, it fetches and applies what replay decoded, so the fold stays synchronous and pure. Every other facet keeps the no-op default.

**Capture rides a command.** Nothing is staged from `on_activate`/`on_deactivate` (§7.12). Because the guest writes the image *between* commands, the consumer drives every capture through an explicit, host-delivered **capture command** (like the alarm's `AlarmDue`, §7.16) through the ordinary §6 barrier. A consumer that must capture before a lifecycle boundary (idle hibernation, planned migration) completes its capture command first (the machine's quiescent points, machine §4), and its `can_hibernate` refuses eviction while the image holds uncaptured writes, so the §10 idle path cannot strand a dirty image.

`Facets = (Disk,)` persists the guest's **entire rootfs** over the same two primitives, where the agent runtime's `Native` capability tier persists only the workspace tar (sandbox §3.5). The worked consumer is the persistent machine ([`machine-spec.md`](machine-spec.md)), which owns *when* to capture: its command unit is a whole session (machine §4).

- **The scan runs off the async executor.** A capture reads and hashes every block, and an import reads and writes every block of the source: seconds of device and CPU work at 1 MiB blocks. It MUST NOT run on the worker driving the node's consensus: peers would elect around a node silent for a scan, deposing exactly the activation that owns the guest being captured. This is the §7.4 blocking-I/O seam applied to a facet, and lets a deployment size its election timeout for the network rather than the longest scan. The scan proceeds a **wave** at a time, so peak memory stays within the blob-put pipeline's bound, and blocks whose hash matches the committed index are discarded without leaving the pool.
- **The physical discipline (E20/F4), with one named departure.** The guest writes the image live across a session. On `Committed` the host keeps it; on any other outcome it deletes the local image, folds nothing, and steps down, and the next activation rematerializes from the composite snapshot plus committed capture manifests. The image is keyed under `EntityConfig::data_dir` by node and entity. The departure: other physical facets' un-durable state is *unobservable* behind the §6 gates, but the disk's un-durable writes **are** observable to the guest and whoever is attached to it before the next capture. Entity-level observables (records, replies, §7.5 reads) still honor the gates; the wider exposure is the consumer's to bound (the machine's session-grained barrier, machine §4, invariant M3).
- **The capture seam.** The host MUST obtain exactly the blocks changed since the last capture, read at a **quiescent point** (guest paused or stopped, machine §4); it never captures a running guest. Dirty tracking is the **host's own**: Firecracker's diff snapshots track guest *memory* and expose no block-device write bitmap. The reference mechanism is **content-hash pruning**: chunk the image and diff block hashes against the last manifest, the mismatches being the dirty set. It is unprivileged and deterministic under simulation, at the cost of a full image read per capture. A **copy-on-write overlay per capture epoch**, whose allocated extents are the dirty set, is a conforming alternative and the deferred upgrade (§16). The obligation is the property, not the mechanism.
- **Checkpoints are the snapshot contribution, and they scan and upload nothing.** The committed form keeps a **per-block `BlobId` index** (each block's last committed capture, also the dirty-tracking baseline), serialized as a **full-coverage manifest**: image size, block size, ordered `(block index, BlobId)` list. Every referenced blob was already `put` by the capture or import that committed it. The live image is **not** consulted: its uncaptured writes are not committed state and MUST NOT enter the snapshot. A region no manifest covered restores as zeros (the image file is created sparse). The live checkpoint's blocks plus those referenced by later capture records are the root set (**F3**); §9 compaction then drops the subsumed capture records.
- **Rehydration.** Composite snapshot → checkpoint manifest → fetch blocks, verified (**E17**) → materialize the image → replay decodes later capture manifests into a pending set → `rehydrate` fetches and writes each block at its offset and truncates to the recorded size (**F1**) → boot the microVM. The quorum barrier (`head`, §9) is unchanged. Lazy block hydration on first guest access is deferred (§16).
- **Bounded, like every physical facet.** The image is bounded by `MAX_IMAGE_BYTES` (16 GiB), which caps an import and every folded manifest; a capture manifest holds indices and ids, never bytes; each block blob is bounded by the fixed block size; and checkpoints obey the same blob bounds as the other physical facets. A guest cannot make the host materialize an unmetered allocation (sandbox §3.2).

---

### 7.16 The alarm facet (logical)

The alarm facet gives an entity a **durable timer**: at most one pending deadline (DO §5), armed or cancelled through `ctx.alarm()` and fired as a callerless `on_alarm` invocation. It underlies retries, timeouts, batch flushes, and the steps facet's `sleep` (§7.17).

- **Storage is a trivially logical facet.** `Set(due)` and `Clear` records fold into a single `Option` deadline (last write wins), the source of truth for whether and when the entity fires. Arming and cancelling stage like a KV write (§7.12); the encoded form is the snapshot contribution; the root set is empty.
- **Firing goes through the same barrier.** After each commit and on activation the host arms an **epoch-guarded in-activation timer** from the folded deadline; when it elapses the host delivers itself a callerless `AlarmDue` command that runs `on_alarm` through the §6 barrier, so an alarm's effects are as durable and fenced as any command's. The epoch guard makes a superseded timer's late `AlarmDue` a no-op.
- **Two surfaces are ungated, and both are inert rather than a hole.** `on_alarm` (§4.1) is declared on `Entity` itself, so an entity implementing it without `Facets = (Alarm, …)` compiles and is never called, since only the facet's folded form arms a deadline. Hosting through `entities_with_alarms` is likewise ungated: a type without the facet gains nothing from the index. Gating both is reachable (a `where` clause on two hosting signatures; moving the hook behind `EntityRegistry`, which has the facet set in scope); until then the inertness is stated, not enforced.
- **Consume-on-fire (E21).** The fired deadline's `Clear` joins `on_alarm`'s own batch, so the alarm clears atomically with its effects unless the handler re-arms (last write wins): a re-arming handler chains, a consumed alarm stops, and a failed commit leaves the deadline pending to fire again. One arm commits **at most one** effect; the bound is on committed effects, not handler invocations.
- **A pending alarm vetoes hibernation until the index acknowledges it.** An entity owing a wake MUST stay resident, where its timer fires on time, unless something else will wake it: the per-shard **alarm index**, one small index entity per (entity type, shard) holding an `EntityName → due` map updated on every alarm change. The alarm **driver** sweeps the index for the shards its node leads and re-activates each entity owing a wake, which re-arms its own timer (**E3**). This covers **failover** and **voluntary hibernation**, but the host may hibernate an alarmed entity only once the *current* deadline is **acked-registered**: registration is an `ask` answered after the index entity's commit (§6), retried on the idle tick, never awaited on the command path; until it acks, the entity stays resident. The index is a *hint* and the entity's facet the truth, so a stale entry costs at worst a spurious wake, never a lost or double fire.

---

### 7.17 The steps facet (logical)

The steps facet gives an entity **durable step memoization**: the Cloudflare-Workflows-style `step`/`sleep`/`retry` pattern over the event fold, the alarm facet (§7.16), and a self-driving loop (supplied by the workflow; the agent runtime is the reference consumer, agent §3).

- **The durable core is a memo.** A steps consumer is a re-entrant decision the entity re-enters after every commit. Each **step** runs an external effect; the facet journals its result under the steps tag and folds it into a `step id → bytes` map, so on any later tick (re-activation, failover) a completed step resolves from the map instead of re-running. Step ids are the consumer's stable call-site ordinals, deterministic across ticks and replay.

  **The memo is write-once**, and that, not "the effect ran once", is what a test can assert. The launch guard is per-activation and never journaled, so a re-activation MAY re-launch a step whose result has not committed, running the effect again; what MUST hold is that the **first committed result wins** and every later `StepDone` for that step commits nothing. Verified by `steps.rs` (scripted, single-node) and `steps_swarm.rs` (under the cluster nemesis), which read a step's memo repeatedly and require every answer to agree.
- **`sleep` is a step whose effect is an alarm** (§7.16); `retry` is a step that re-launches after an alarm-backed backoff.
- **What the entity still wires.** Launching a step's effect off the command path and self-`tell`ing its result back depends on the effect's type, so the facet supplies the durable half (the memo's `result`/`record`) plus a generic `StepDone` command that journals a returned result; the "already launched this activation" guard (`LaunchGuard`, generic over the claim key) is ephemeral activation state. A linear `async` DSL is deferred (§16).
- **Which half an entity adopts depends on where its results already live.** The memo serves entities whose step results are *not* in their event vocabulary. An entity whose results are already journaled events with their own fold and consumers (the agent's `ModelResponse`/`ToolOutcome` records) MUST NOT memoize them again under the steps tag (one decision, two folds); it adopts only the launch-once guard, keyed by its own step identity. The agent takes that guard from the workflow's per-activation claims; a plain effect-runner adopts both halves. What this facet leaves to the entity (launch, guard, self-`tell`, and recovery of what a crash orphaned) is generalized by the workflow ([`workflow-spec.md`](workflow-spec.md)), which re-derives the guard's keys from committed state. The memo and guard do not depend on it; an entity wanting both declares the steps facet beside the workflow.

---

## 8. The single-writer fence

An entity must have one writer. The fence is the shard's **leadership term**: one leader per term, and every per-entity append carries that term as its fencing token.

- **One leader per term.** The shard's leader-election group (§7.1) elects at most one leader per term and records the replica set. The leader is the single writer and host for every entity in the range (§5.2). Election is the *only* agreement round, on failover (§8.3), never per write.
- **Every append carries the term.** Each replica persists the highest term it has acknowledged for the shard and refuses any append stamped lower, so a write commits only while a quorum still recognizes its term.
- **The persisted fence MUST be keyed by the shard's whole identity, `(entity_type, index)`.** The same index under two types is two leader-election groups (§8.2) whose terms advance independently. Keyed by index alone, the type that elects more often raises the fence past the quieter type's term, after which the quieter type's appends are refused (`NotLeader`, so a live activation steps down) and its recovery reads too (`Unavailable`, so the entity never rehydrates), unrecoverably, since no leader of that group can exceed a fence its neighbour raised. Hence a node hosting several types MUST NOT share one store across them; a store factory keyed by entity type makes that structural.
- **A deposed leader cannot commit.** Once a new leader raises the term, the old leader's in-flight appends are refused by every replica that has seen it, reach no quorum, and return `NotLeader` or `Unavailable` (§7.3). A quorum acknowledges only one term, so two nodes that briefly both believe they lead cannot both commit, and the record never forks (invariant **E1**).
- **A new leader recovers each entity's head from a quorum.** With no shared log, a fresh leader does not inherit entity records the way a Raft leader inherits its log. Before serving an entity it recovers the head and tail from the replicas (the rehydration barrier, §9): it fence-reads every replica, waits at most `recover_timeout` (Appendix A) for each, and merges the replies. The merge is a read-repair: per `Seq` slot it takes the record under the **highest term** any replica holds, since a partitioned replica MAY hold a stale-term record at a slot a higher term later won. When the replies form a quorum, the leader **writes the recovered tail back** to a quorum under its own term before serving, so the adopted head is quorum-durable and no later recovery can regress it. Short of a quorum within the wait, the leader serves the merged local view without write-back (read-your-leader, §7.5: staleness, never invention). That is write-safe because every append still needs a quorum, and a replica refuses an append whose slots it holds under another term or whose head is below its base (**E14**). The recovery is `Unavailable` on an election in progress, a store that has fenced the node behind a higher term, a failed local store (§11), or a confirmed read whose write-back reaches no quorum. A split or merge transfer (§7.7) MUST NOT use the fallback; it requires the confirmed quorum. By quorum intersection every acknowledged record is present, under its committing term, on some replica the new leader reads (invariant **E14**): E14 rests on this **quorum read-repair of a per-entity head**, in place of leader-completeness over a shared log.
- **The host steps down cleanly.** On `NotLeader` the host deactivates the entity and the caller retries against the new leader (§5.4, §6 step 5). The host MUST NOT keep serving an entity whose shard it no longer leads.

### 8.1 Why a term, not a clock

Leadership is a quorum fact: a leader that pauses and wakes finds its appends refused under a higher term, whatever the clock skew, so write safety needs no time-based expiry. A clock lease appears only as the deferred optimization for linearizable *reads* (§7.5, §16). The per-entity `Seq` orders records and aligns snapshots (§9); only the term arbitrates writers.

### 8.2 The actor control plane, the shard maps, and the shard leadership groups, side by side

Three consensus layers run at different rates, and none touches entity data:
- the **actor control plane** (one leader-based group, actor §9.4.3) owns cluster membership and seeds the map groups' voters; it changes on cluster events;
- the **shard map** (one group per entity type, §7.6) owns the allocation (each shard's nodes and key range); it changes on splits, merges, reconfiguration, and membership;
- the **shard leader-election groups** (O(shards)) own placement (each shard's leader and term); they change on failover and reconfiguration, never on an entity write.

Entity **data** is off consensus entirely: a per-entity quorum append carrying the shard term (§7.2). Group ids never collide (the membership control group is id 0, a shard map uses a reserved shard index, leader-election groups hash `(entity_type, index)`). The total is `O(shards) + O(entity types) + 1`, bounded by split/merge (§7.7) and the hosted types, never O(entities) (**E7**).

### 8.3 Failover

**A planned departure SHOULD hand leadership off rather than drop it.** A node shut down deliberately (rolling upgrade, scale-in, drain) SHOULD hand each shard it leads to another replica before stopping, using the leadership transfer of actor §9.4.3 item 8. Skipping this costs availability, not correctness: a node leading *S* shards otherwise imposes *S* simultaneous failover windows, each ending in the rehydration of every active entity on the shard (§9) while the replacement starts cold. The handoff is best-effort, since it can target only a replica caught up to the leader's last entry; a shard with lagging or unreachable peers falls back to the unplanned path below, and a departing node MUST NOT block its shutdown waiting for one.

On an unplanned loss, the shard's replicas elect a new leader by ordinary Raft, raising the term. The new leader holds the shard at once but not yet the entities' records: it recovers each entity's head and tail lazily on first access (§8, §9, §10). Between the old leader's loss and the election lies the shard's **failover window**, during which calls fail fast (`NotLeader` then a retry, or `Unavailable`, §11) rather than block. No acknowledged write is lost (invariant **E14**).

---

## 9. Snapshots and compaction

An entity periodically snapshots its folded state so activation need not replay its whole history.

- **Snapshot policy.** After a commit the host MAY persist a snapshot `(head, State)` via `save_snapshot` (§7.3), on a configured trigger (`snapshot_every`, a record count). Only the shard leader writes snapshots; a deposed leader's `save_snapshot` returns `NotLeader`. **One trigger governs every snapshot the host takes**, including on idle hibernation (§10): whether a snapshot is worth taking cannot depend on the path that reached it.
- **What a snapshot costs, and therefore how often.** On `Quorum` the composite record reaches every replica and is quorum-counted like an append (§7.3), costing *R−1* times over. Bulk facet bytes and, past 64 KiB, facet 0's state are content-addressed chunks of which only changed ones are put (below), so a snapshot is roughly O(delta) plus a manifest. What it buys is a shorter replay, whose reads are **local** and cost no round trips. A deployment SHOULD still set the trigger high: chunking lowers bytes, not round trips, and every snapshot is a round of puts plus a record. The counter-pressure is failover: a new leader recovers the records above the last snapshot from a quorum (§8), once per leadership change. The arithmetic behind the defaults is [hardware-envelope](hardware-envelope.md) §3.9. An entity type whose events are large relative to its state inverts the trade and SHOULD lower the trigger.
- **The rehydration barrier (§10).** Winning the shard does not give the leader an entity's records (§8), so before reading an entity's head it MUST run the recovery read, `head` (§7.3). With a quorum answering within `recover_timeout` it returns the quorum-confirmed head and backfills missing records, so a fresh activation never folds onto a short head or serves stale reads from a partial local view; short of a quorum it adopts the merged local view, which MAY be stale but cannot commit a write (§8). On `Local` it is a no-op: the single store *is* the committed state.
- **Rehydration (§10).** After the barrier the leader reads the latest snapshot `(s_seq, s_state)` by `load_snapshot` (§7.3), a local read that on `Quorum` returns the quorum-best snapshot the barrier selected, then replays the records after `s_seq` up to the recovered head through `apply` (§4.1). The head is set **only** from journal and snapshot returns, never from a prior activation's memory (invariant **E3**).
- **A snapshot MUST NOT shorten the effective log** (invariant **E4**). This holds *structurally*: a head is defined as the best snapshot's `Seq` plus the contiguous run of records above it (`ReadReply::head` on `Local`, the recovery merge's base on `Quorum`, §8), so no snapshot can outrun the head it is measured against. The host still refuses a snapshot whose `s_seq` exceeds the recovered head, as defence in depth against a store whose two answers disagree, failing toward the journal's authority (**E3**). Refusing is not recovery: once compacted, the subsumed prefix exists **only** in the snapshot, so replay checks the store's base against its cursor. An untruncated journal replays whole from `ZERO`; a compacted one **fails the activation**, since a base above the replay cursor is history the store can no longer serve, surfaced as unavailability rather than folded over (the loud-failure rule of **E19**).

**Encoding: the record envelope, the composite stamp, and chunked state.** These are the bytes at rest behind §7.12; a facet author never writes them.

**The tag space reserves its high bit, and is split in two.** A facet tag is `0..=0x7F`; a leading byte at or above `0x80` marks another **envelope version** ([compatibility](compatibility-spec.md) §3), and a build MUST refuse such a record by version rather than as an unrecognized tag, since the two have different causes and fixes.

Within the tag range, `0..=0x3F` is reserved for facets defined in this crate and `0x40..=0x7F` for facets defined outside it, so each side allocates independently; a tag is permanent, and a collision would give two readings of the same durable byte. Version 1 is `[tag][payload]`, so the reservation costs nothing on disk. It does **not** version a facet's *payload* schema: a payload change is a new version of the format that carries the payload ([compatibility](compatibility-spec.md) §3.2).

**Facet 0's state is chunked by content.** Past 64 KiB the encoded `State` is cut into content-addressed chunks and the composite carries only their ids, as the checkpointing facets do (§7.12). The cut is **content-defined**, not at fixed offsets: a SQLite page or disk block has a fixed address, but inserting a turn into a folded transcript shifts every later byte and the length prefixes ahead of it. A rolling hash picks each boundary from the surrounding bytes, so chunks after an edit match those before it. A chunk already in the root set is **not put again**, which makes snapshot cost proportional to change rather than accumulation (without the skip, chunking would be worse than inline). The chunk puts complete before the record naming them is written, so a durable manifest never references a non-durable chunk. Under the threshold state stays inline, since chunking spends a quorum round trip to save bytes not worth saving.

The payload is **stamped** with a magic and a version ([compatibility](compatibility-spec.md) §3), read before the body so a composite from another version is refused rather than misparsed; a snapshot needs a full stamp where a record uses a reserved bit, because `postcard` output has no recognizable leading structure. The body also records **which codec encoded facet 0's contribution**, the one codec-dependent part of a snapshot (a user type under the deployment codec, §4.1, §5), so a changed codec is reported as the misconfiguration it is rather than as corrupt entities. A codec mismatch MUST name both codecs and abort the activation. The body carries facet 0's state as inline bytes or a chunk manifest, so a chunked state is never mistaken for an empty one.

**Compaction**, truncating a record prefix a snapshot subsumes, is a per-replica Replicator operation below the seam: storing a snapshot drops the records up to its `Seq` and advances the entity's **base** (the file-backed store then rewrites its log to a single checkpoint). Every `load` page reports the base, read with the records under one segment lock (§7.3), and every event read carries it to the caller (§7.5, §4.3), because compaction changes what a reader of *history* sees and serving a truncated log as complete would be invisible data loss for observers. Compaction need not wait for the snapshot to be quorum-durable, for two reasons. A snapshot is only taken at the **committed head**, so every record it subsumes was already quorum-committed. And a fresh leader takes the **highest snapshot `Seq` in its read quorum** as the head base and merges the records above it (§8); a replica drops a prefix only if it holds a snapshot covering it, so any recovery quorum finds, for every committed record, the record or a snapshot subsuming it (invariant **E14**). Each entity compacts independently of its shard-mates.

---

## 10. Lifecycle: activation, migration, hibernation

From the host's view an entity's lifecycle is:

```
activate → rehydrate (snapshot + replay, §9) → on_activate → serve commands → idle → on_deactivate → (snapshot) → stop
```

- **Activation** starts when the first message for the name reaches its shard leader's gateway (§5.3): the leader rehydrates (§9), runs `on_activate`, then serves. It takes no consensus; on `Quorum` it costs one quorum-recovery round trip (§9), on `Local` nothing.
- **Migration** follows shard leadership (§8.3): the activation rehydrates on the new leader, and in-memory state is never carried across.
- **Hibernation (deactivate-on-idle).** After an idle interval the leader MAY, *if `can_hibernate` permits*, run `on_deactivate`, snapshot **if §9's trigger has been crossed**, and `ctx.stop()`. (`on_deactivate` cannot mutate state, §4.3, so its order relative to the snapshot is unobservable; the implementation runs it first.) The snapshot MUST use the write path's trigger and MUST NOT be unconditional: on `Quorum` it reaches every replica (§9), so an unconditional one would spend a replication per idle window on every write-once entity, and chunking makes that smaller, never free. An entity that vetoes eviction (e.g. one with a live autonomous run) keeps running and the idle check is rescheduled. The gateway prunes the name from its table (it death-watches its hosts, actor §12). The next command or subscribe re-activates; the gateway event read does **not** (§7.5), so a hibernated entity can be observed without rehydration. Persisted storage survives; in-memory state was only a cache (§1).
  - **Default and tuning.** The *mechanism* is the Durable Objects eviction window (DO §5); the *number* is not, because DO's ten seconds prices resident memory on shared multi-tenant hosts and this deployment does not ([hardware-envelope](hardware-envelope.md) §1, §3.6). Reactivation costs a quorum head recovery plus a snapshot-bounded replay (§9), while eviction reclaims memory a node has to spare, so `idle_after` SHOULD default to **minutes**, keeping intermittently used entities resident. A deployment of many rarely touched, individually large entities SHOULD shorten it. An implementation SHOULD apply a small minimum residency or jitter so an entity accessed just slower than the timer is not repeatedly evicted and reloaded; the reference implementation applies neither.
- **Forced step-down.** When leadership moves (§8) or the shard goes unavailable (§11), the activation runs `on_deactivate` but takes **no snapshot** (the journal may be unwritable), emits `Deactivated`, and stops; the next access rehydrates.
- **Eviction races.** A command reaching a stopped cached host gets `CallError::DeadLetter`, which `EntityRef` treats as proof the command never ran (never an ambiguous `Unreachable`/`Timeout`): it drops the cache entry, re-resolves through the gateway, and re-issues once under the original deadline. The gateway, finding the entry's host no longer resolvable locally, prunes it without waiting for `Terminated` and activates afresh. A second `DeadLetter` surfaces to the caller.
- **Subscriptions are ephemeral activation state**, dropped on every deactivation path; subscribers re-subscribe and backfill (§7.9).

Durable **alarms** (§7.16) intersect this lifecycle twice: a pending alarm vetoes hibernation until the alarm index acknowledges it, and after failover the index re-activates each entity owing a callerless wake.

---

## 11. Failure model and the node-down cascade

The entity layer inherits the actor failure model (actor §8) and adds durability outcomes. A failure's blast radius is the **shard**, not the cluster or the single entity.

When a node fails (actor §8.1):
1. **Shard leadership.** Every shard it led elects a new leader among the remaining replicas (§8.3); shards it followed continue unchanged.
2. **In-flight callers.** Every pending `ask` to an entity on the failed leader completes with `EntityError` wrapping `CallError::Unreachable` (actor §7.2, §8.1), never a hang; the caller retries.
3. **Watchers.** Watchers of an activation receive `Terminated { NodeDown }` (actor §12), as for any actor.
4. **Re-activation.** The next message re-activates each affected entity on the new leader, which rehydrates (§9). No acknowledged write is lost (invariant **E14**).

**A replica whose storage fails withdraws; it does not crash.** A replica that cannot make a write durable MUST NOT acknowledge it, and MUST report the refusal rather than terminate: an I/O failure (full volume, lost mount, failing device) makes the store answer a distinguished *failed* outcome to every later record store, recovery read, and blob store, which MUST NOT count toward any quorum. The node thereby drops out of every quorum it belongs to, which the majority rules of §7.2 and §8 absorb: writes commit on the other replicas, and a failed leader's entities cannot recover on it, so the shard elects around it (§8.3). The refusal MUST be distinguishable from a fence (`Fenced`) or stale head (`Stale`), which are decisions a healthy replica made and carry a term or head to act on; a failed store has decided nothing. The failure SHOULD be one-way and store-wide, since the fault belongs to the volume and nothing below the seam can establish its recovery. Terminating instead would turn a local fault into a cluster event (every shard the node led re-elects and rehydrates elsewhere).

**Quorum loss is unavailability, not a fork (CP).** A shard that cannot reach a quorum, whether for leader election or for an entity's write, commits nothing: `append` returns `Unavailable`, the reply is `EntityError::Unavailable`, and the activation steps down (§6 step 6), because a timed-out commit MAY land later (§7.2, §7.3) and the in-memory head is no longer trusted. Callers retry or fail over. A minority cannot commit (no leader is elected there, and a stale-term append is refused, §8), so the shard never forks, and only its entities are affected. Reads stay read-your-leader (§7.5): until the check-quorum lease (§16) exists, an isolated minority leader MAY serve stale reads though it commits nothing; with it, a leader holding a valid lease MAY keep serving linearizable reads.

---

## 12. Error model

Entity calls surface two failure layers, kept distinct as the actor framework keeps `CallError` and `M::Reply` apart (actor §14):

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

- **Application errors** live **inside `M::Reply`** (e.g. `Result<T, E>`, §4.2), never in `EntityError`.
- `NotLeader` is normally absorbed by the bounded redirect (§5.4); `Unavailable` is a real durability outcome the caller must handle.
- `EntityError` MUST be exhaustive at the public API, so callers handle partial failure explicitly (actor §14).

---

## 13. Security and observability

- **Security.** The entity layer adds no transport; it inherits mutual-TLS associations, the handshake allowlist, and the deserialization allowlist (the dispatch registry, §5.5) from actor §15. The gateway MAY consult an `Authorizer` per `(peer, EntityName, manifest)` before activating or dispatching, as actor §15 item 5 gates `deliver`.
- **Observability.** On the actor framework's extensible `Event` stream (actor §16) the entity layer emits `Activated`, `Rehydrated { from_snapshot, replayed }`, `Committed { shard, seq }`, `Snapshotted { at }`, and `Deactivated`. Every variant carries the hosting `node` (the shard leader, §5.2) and the entity `name`, making **E6** a continuous checker over the stream; `Committed` carries the `shard`, so a split or merge is checkable as a move that keeps `seq` strictly increasing across the boundary (**E15**). The shard events `LeaderChanged { shard, term }` (emitted by a new leader once per term won), `ShardSplit { parent, child, boundary }`, and `ShardMerged { left, right }` (emitted by each node as it applies the committed change, §7.7) serve operator tooling and the simulator's checks (§14). Subscriptions (§7.9) add **no** content-bearing event: they reuse `Committed { seq }` and deliver records out of band, keeping the journal the one home of entity content.
- **Metrics are a second, separate sink.** The event stream is the *checker's* interface, one record per transition, and the wrong shape for an operator: its volume scales with traffic, and "is commit latency rising?" would mean rebuilding a distribution from a firehose. A node therefore SHOULD also expose a **metrics sink** of aggregates costing O(1) per operation, reporting three measurements: the **commit round** labelled by outcome (`Committed`/`NotLeader`/`Unavailable`, §6 steps 4–6), since averaging a fast refusal with a fast commit hides both; **rehydration** (§9), the cost of a cold access that hibernation trades against memory (§10); and the **resident activation count**, which hibernation exists to bound. The sink is **node-scoped**, like the blocking-I/O seam (§7.4). Its vocabulary MUST be closed rather than an open `incr(name, labels)` surface, with keys no wider than an entity type: unbounded label cardinality is how metrics pipelines fall over, and per-*shard* keys are unbounded by §7.7's elasticity. Leadership changes, splits, and merges are deliberately absent: they are transitions already carried exactly once by the shard events above.

---

## 14. Testability and deterministic simulation

The entity layer uses the actor framework's deterministic simulation (actor §18): a whole cluster runs in one process, on one logical thread, over virtual time, network, and randomness, so one seed reproduces a run exactly.

- The journal (§7.3) and the parts below it are **simulation seams**. The **leader-election group** reuses the actor framework's Raft (actor §9.4.3), so simulation drives the real consensus code; the **Replicator** (`Local`/`Quorum`) is exercised directly. Through the framework's `Clock`/`Entropy`/`Spawner`/`Transport` seams (actor §4.6, §7), simulation runs the real host, gateway, shard, and rehydration code.
- Fault injection MUST be able to produce: a shard **leader** crash and election mid-write (term fence and quorum recovery, §8); **leader-election loss** and **per-entity write-quorum loss** as distinct routes to `Unavailable` (§11); a stale-term append from a deposed leader racing a new election (§8); a timed-out append that commits late, re-read on the next activation (idempotency by `Seq` slot, §7.2); eviction mid-command (rehydration and the output gate, §6, §10); and a shard split under concurrent writes (§7.7). A no-fault run MUST pass.
- For **record subscriptions** (§7.9), fault injection MUST be able to produce: a **leader move mid-stream**; a **slow sink** whose buffer overflows; **hibernation and reactivation** under a live subscription; and a **timed-out append that commits late**. In every case the sink's seq-reconciled sequence MUST equal what `load` to the head returns, with no gap or duplicate (**E16**).

---

## 15. Invariant catalogue and conformance

These invariants collect the MUSTs above into the contract a conforming implementation verifies, each holding under the faults of §14. They are checked as actor §18.5/§18.6 prescribe: continuous checkers over the §13 event stream for safety, targeted tests for the rest, compile-fail for type safety.

The catalogue is machine-readable as `e_catalogue()` beside the conformance suite, guarded by the drift test (`conformance_catalogue.rs`), so a renamed or deleted suite fails the build. Test files named here are under `crates/entity/tests/`.

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

An `entity` implementation conforms iff every invariant holds, verified under deterministic simulation (§14) and, for **E10**, by compile-fail tests. Facets add the per-facet contract **F1–F4** (§7.12), verified generically under the same simulation.

---

## 16. Extensions (deferred)

Named here, specified elsewhere or later, so the core stays small:

- **A linear step DSL.** An `async` `step`/`sleep`/`retry` surface over the steps facet (§7.17), written over the workflow's re-entrant cycle, so a long-running procedure reads as straight-line code.
- **Hibernatable connections.** Parking a record subscription (§7.9) or a WebSocket/stream connection *across* hibernation and re-delivering on wake (DO §5), so a follower need not re-subscribe after every idle eviction.
- **Third-party facets.** Opening the sealed facet seam (§7.12) once the record-tag registry and cross-version rules have settled.
- **SQL lazy page hydration (a capture VFS).** A VFS shim (§7.14) that captures WAL frames below SQLite and faults pages in from blobs on first access, so a large database activates without materializing whole.
- **Workspace scaling.** For §7.11: ranged sub-records for a changed file larger than a record bound (one `Write` record carries a whole file, bounded by the 64 MiB tree cap); faulting files in from checkpoint chunks on first access; and preserving mtimes across the microVM tar pull so capture's stat-prune survives an fc command (each fc command re-hashes the tree).
- **Disk lazy block hydration.** For §7.15: faulting image blocks in from checkpoint blobs on first guest access, so a large rootfs activates without materializing whole.
- **Disk overlay dirty tracking.** For §7.15: a copy-on-write overlay per capture epoch (a device-mapper snapshot or a userspace block device), whose allocated extents are the dirty set, replacing the content-hash scan where a full read per capture dominates. Privileged and Linux-only, behind the facet's private dirty-set seam, so it swaps in without touching the facet contract.
- **Range-verified blob streaming.** Verifying a byte range of a large blob against the BLAKE3 tree its `BlobId` roots (Bao, §7.10); a ranged `get` fetches and verifies the whole blob, then slices.
- **A cold blob tier.** An external content-addressed object store (S3-compatible or homegrown) as the durable home of an entity's blobs, the shard replicas reduced to read-through caches: the cold half of the DO hot/cold split (§7.10, DO §6). The blob half of the journal seam (§7.3) is the plug point, and the contract stays immutable, verified, content-addressed put/get, never a conditional write; the records, and with them the fence, stay quorum-replicated. The persistent machine is the motivating consumer (machine §8).
- **Linearizable reads (check-quorum lease).** A check-quorum property on the leader-election group's Raft engine (§8): the leader serves reads locally while it has heard from a quorum within an election timeout, and the activation self-fences otherwise, rather than returning stale state (§7.5).
- **Follower reads.** Serving reads from followers with a freshness bound, trading linearizability for read scale (§7.5).
- **Cross-entity sagas.** Idempotent multi-entity workflows above the single-entity consistency boundary (§2.2).
- **Optional `#[derive(Entity)]`.** Defaulting the manifest and `register` list, as actor §4.4 permits: a convenience, never required.

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
    idle_after: Duration::from_secs(300),   // hibernation window (§10)
    snapshot_every: 4096,                   // events (§9)
    entity_store: None,                      // None ⇒ fresh in-memory store per node; a
                                            //   factory persists records across a cold
                                            //   restart (§7.4, E14); keyed by
                                            //   (entity_type, node), never shared across
                                            //   types (§8, the fence)
    failure_domains: None,                  // None ⇒ domain-blind placement; a mapping
                                            //   caps replicas per rack/zone (§7.1)
    quorum_timeout: Duration::from_secs(2), // append/snapshot/blob-put deadline (§11)
    recover_timeout: Duration::from_secs(2),// head-recovery deadline (§8, §9); separate
                                            //   because a slow recovery blocks activation
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

**What is *not* in this config, and why.** `EntityConfig` is per entity type, so the two **node** capabilities (§7.4, §13), the blocking-I/O pool bounding the node's device work and the metrics sink describing its health, hang off a node hosting handle instead, set once for every type:

```rust
let node = system
    .entity_node()                                          // the node's hosting handle
    .blocking_io(Arc::new(ThreadPoolIo::sized_for_host()))   // §7.4; default: inline
    .metrics(Arc::clone(&metrics));                          // §13; default: discarded

let accounts: Entities<Account> = node.entities(config_accounts);
let orders:   Entities<Order>   = node.entities(config_orders); // one pool, one sink
```

A production node over real storage makes all three node-scoped choices in one call, `EntityNode::durable(system, data)`, which returns the node with a thread pool, a metrics registry, and the durable store factory every hosted type shares.

Hosting straight off the system (`system.entities(config)`, above) defaults both (inline I/O, discarded measurements), the `Local` tier's shape and what simulation requires (§14). Per-type fields would be wrong in a way nothing catches: a config that omitted the pool would silently get a *second*, inline one, invisible until a stalled device blocks the executor the pool exists to protect.

**Hosting one entity under many type names (`entities_named`).** `entities(config)` hosts a type under its own `ENTITY_TYPE`, built by `G::default` per activation. To host **one Rust entity as several distinct entity types at runtime**, the node hosting handle's `entities_named` adds two extension points:

```rust
fn entities_named<G: Entity<System = S>>(                // on EntityNode<S>
    &self,
    entity_type: &'static str,                       // overrides G::ENTITY_TYPE (§5.1)
    config: EntityConfig,
    factory: Arc<dyn Fn() -> G + Send + Sync>,      // overrides G::default (per-activation seam injection)
) -> Entities<G>;
// node.entities(config) == node.entities_named(G::ENTITY_TYPE, config, Arc::new(G::default))
```

Each `entity_type` is a fully distinct entity type, with its own gateway (one `gateway_key` per name), shard map, and, on `Quorum`, leader-election groups (the group id hashes `(entity_type, index)`, §8.2); the same key under two names addresses two independent entities. `entity_type` MUST be stable cluster-wide and across runs, as `ENTITY_TYPE` must (§5.1); the `&'static str` makes that explicit (a deployment leaks its bounded set of names if they are not literals). The `factory` injects per-node seam handles into each fresh activation, so the entity needs no `Default`. The agent runtime rides this seam: each *kind* is its own entity type (`KindId` IS the `entity_type`), hosted under one shared agent run loop.

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
                         #   records survive a restart, so a cold-restarted cluster recovers (§7.4, E14)
  blobs.rs               # the entity-native content-addressed blob store: BlobId + EntityBlobs
                         #   (put/get/has/gc/destroy/repair_facets), colocated with the journal (§7.10)
  subscription.rs        # record subscriptions: live delivery of committed records to a sink (§7.9)
  system.rs              # the EntitySystem capability seam + name→shard/group hashing (§5.1, §7)
  event.rs               # EntityEvent observability stream (§13)
  config.rs              # EntityConfig (Appendix A)
  node.rs                # the node's hosting handle: the shared BlockingIo pool (§7.4) and metrics sink (§13)
  blocking.rs            # the BlockingIo seam (§7.4): inline by default, a thread pool on real storage
  metrics.rs             # the operator-facing metrics sink (§13): a closed vocabulary of
                         #   counters and fixed-bucket histograms, keyed by entity type
  cdc.rs                 # content-defined chunking of facet 0's snapshot state (§9)
  error.rs               # EntityError (§12)
  ws.rs                  # the workspace facet (§7.11)
  facet.rs               # the facet seam (§7.12): tags, staging, composite snapshot, unioned blob roots
  kv.rs                  # the KV facet (§7.13)
  sql.rs                 # the SQL facet (§7.14, `sql` feature)
  disk.rs                # the disk facet (§7.15)
  alarm.rs               # the alarm facet (§7.16)
  alarm_index.rs         # the per-shard alarm index (§7.16)
  steps.rs               # the steps facet (§7.17): the memo plus the launch-once guard
  facet_blobs.rs         # blob machinery shared by the checkpointing facets (§7.12): the F3
                         #   root-keeping discipline and the chunked blob-area transfers
  testing.rs             # shared simulation invariants over entity events (§14), and the store
                         #   doubles StaticEntityStore and SnapshotAheadOfHeadStore behind E4's test
  cluster.rs             # the sharding and replication namespace: a re-export surface over
                         #   system.rs, shardmap.rs, replica_store.rs, replicator.rs, node.rs
                         #   and alarm_index.rs, defining nothing of its own
```

(`entity_ref.rs` rather than `ref.rs`, since `ref` is a reserved word.)

**The public surface is a deep root and three deep modules.** The root carries the authoring vocabulary and nothing else: `Entity`, `EntityCtx`, `EntityHandler`, `EntityName`, `EntityRegistry`, `EntityRef`, `Entities`, `EntityExt`, `EntityError`, `EntityEvent`, `EntityConfig`, `EntitySystem`, `NoEvent`, `EventHistory`, `accepted_manifests`, and `Seq`. Everything else is reached through `entity::facet` (§7.11–§7.17 and the seam), `entity::store` (the durability substrate a deployment wires: the `EntityStore` and `EntityJournal` seams, stock implementations, `BlockingIo`, record subscriptions, commit metrics), or `entity::cluster` (§7.1, §7.2, §7.6, §7.7).

The root is deliberately not a fourth namespace: every consumer would open such a module in full, so it would relay and say nothing. The split follows the crate's own dependencies: `entity::store` and `entity::cluster` name the programming model only through `EntityName`.

`entity` depends on `actor-core` (the model and seams), `actor-cluster` (the clustered `ActorSystem`, the leader-based control plane, and the Raft the shards reuse), `actor-serialization` (codec and dispatch building blocks), `wal` (the crash-safe log under the file store), and `compat` (the version stamps on records and snapshots). It is independent of the `agent` crate. No *required* macro crate exists; `#[derive(Entity)]` is an optional convenience.
