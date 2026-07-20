# Granary: Durable Objects ("Grains") for the Distributed Actor Framework

**Status:** Draft v3
**Scope:** A virtual, durable, single-activation object, a **grain**, addressable by a global name, with colocated event-sourced storage and a durability barrier on the reply path, built on the actor framework of [`distributed-actor-spec.md`](distributed-actor-spec.md).

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Throughout, `granary` is the crate and namespace name and `grain` is the durable object. This document is the sibling of the actor specification and cross-references it freely; a `§` with no document name refers to a section of *this* spec, and `actor §N` refers to [`distributed-actor-spec.md`](distributed-actor-spec.md). The design lessons come from [`../research/durable-objects.md`](../research/durable-objects.md) (cited as **DO §N**).

> **Design stance.** A grain is an **actor plus three things**: a name-based virtual identity, a durable event-sourced journal, and a durability barrier on the reply. The grain inherits everything else unchanged from the actor framework (mailboxes, serial execution, location-transparent `ask`/`tell`, membership, failure detection, supervision, death watch, the receptionist; actor §3–§13). Granary adds no new transport and no required macros (serde derives in user code are the only ones, as in actor §1.1).
>
> The architecture rests on one idea: **each grain is its own single writer, so its journal replicates independently, with no shared log and no consensus on the data path.** A grain's records are already totally ordered, because one writer assigns each the next `Seq`; making them durable is then just a quorum append to the grain's replicas, with no agreement round per write (§7.2). The only thing that needs consensus is *placement* (which node may write a shard's grains): a small per-shard Raft group that elects one leader per term and holds nothing but leadership, term, and the replica set (§7.1, §8). The cluster runs O(shards) of these leader-election groups plus O(grain types) shard-map groups (§7.6), kept bounded by splitting a shard that grows hot or large (§7.7), never one group per grain and never one global log. This is the Cloudflare Durable Objects decomposition (DO §4.3–§4.4): single-writer placement off the hot path, durability as a per-object quorum append, ordering free from the single owner, without DO's operationally infeasible one-Raft-group-per-object. It sits between two failures a single shared log invites: one cluster-wide log is a global bottleneck, and one Raft group per grain is millions of groups and an election storm.
>
> A parallel stance governs **storage**. The substrate is two replication primitives — per-grain ordered, term-fenced records (§7.2) and immutable content-addressed blobs (§7.10) — and every further durable feature is a **facet**: an interpretation layered over them (§7.12), such as the KV map (§7.13), the workspace directory (§7.11), and the SQLite database (§7.14). This is the other half of the Durable Objects lesson (DO §2, §6): one substrate beneath, many storage APIs above, never a new replication mechanism per feature.

---

## 1. What a grain is

A **grain** is a unit of single-threaded compute fused with strongly-consistent, durable, colocated storage, addressed by a globally-unique **name**. At most one live **activation** of a grain exists in the cluster at a time; every message for the name routes to that activation. The runtime creates the activation on first access, migrates it, and evicts it when idle; its durable state survives all of these. The grain's identity is permanent and conceptual; the activation is disposable.

This is the actor model (actor §3) specialized in four ways:

| | Actor (actor spec) | Grain (this spec) |
|---|---|---|
| Identity | `ActorId` = node + path + incarnation, minted at `spawn` | a stable user-chosen **`GrainName`**; the activation's `ActorId` is incidental |
| Lifecycle | explicit `spawn`/stop | **virtual**: activate on first message, deactivate on idle (§10) |
| State | in-memory only, lost on stop | **event-sourced journal + snapshots**, source of truth (§7, §9) |
| Reply | synchronous, no durability barrier | held until the producing events are **durable** (§6) |

A grain's in-memory activation is a **cache** of state folded from the journal. The journal, never memory, is the source of truth (§7, invariant **G3**). This is the rule every other guarantee rests on.

---

## 2. Goals and non-goals

### 2.1 Goals
- **Global name, single activation.** Address a grain by a stable name from any node; the runtime guarantees at most one live activation and routes to it (§5).
- **Durable by construction.** A handler's effects become visible only after the events it produced are durable (§6). A crash loses no acknowledged write.
- **Strong consistency that scales with the cluster, not the object count.** Consensus is sharded (§7). The cluster runs O(shards) Raft groups and splits them as it grows; there is no single cluster-wide log on the write path, and there is no per-grain consensus group. Activating a grain takes no consensus at all (§10).
- **Single-writer safety under partition.** A grain never forks. The shard leader is its only writer, and an append fenced by a stale term cannot reach a quorum, so two leaders never both commit (§8).
- **Pluggability.** The journal is a seam with two reference tiers, single-node `Local` and clustered `Quorum` (§7.4), over a per-shard leader-election group for placement (§7.1) and per-grain quorum replication for durability (§7.2), as transport and the control plane are pluggable in the actor framework.
- **One substrate, many facets.** Durable features beyond the event fold are **facets**: the KV map (§7.13), the workspace directory (§7.11), the SQLite database (§7.14), the disk image (§7.15), the alarm deadline (§7.16), the workflow step memo (§7.17). Each is an interpretation over the two storage primitives (ordered records and immutable blobs), sharing one journal, one atomic commit per command, and one composite snapshot (§7.12). A new feature MUST NOT add a third replication mechanism.
- **One programming model.** The handler is ordinary sequential Rust; the input and output gates (§6) supply atomicity-on-the-outside and durability-before-effects with no explicit locking.

### 2.2 Non-goals
- **A single cluster-wide log.** Granary MUST NOT serialize all grains through one Raft/Paxos group; sharding (§7) exists to avoid that bottleneck.
- **A consensus group per grain.** Granary MUST NOT run one Raft group per grain; the unit of consensus is the shard's leader-election group, which covers many grains, and their data needs no consensus at all, only a per-grain quorum append (§7.1, §7.2).
- **Cross-grain transactions.** Each grain is its own consistency boundary. A workflow spanning grains is built above this layer with sagas/idempotency, not by the runtime (§16).
- **Exactly-once side effects.** Inherited from actor §7.2: delivery is at-most-once and the framework never auto-retries effectful messages. Idempotency is the caller's responsibility.
- **Availability of the write path during quorum loss.** A grain whose shard cannot reach a quorum pauses writes (returns `Unavailable`) rather than forking (§11). It is CP, not AP.
- **Required codegen.** As in actor §1.1, an optional `#[derive(Grain)]` MAY default the manifest/`register` boilerplate, but the hand-written form is normative.

---

## 3. Terminology

| Term | Definition |
|---|---|
| **Grain** | A virtual, durable, single-activation object: state + behavior addressed by a `GrainName`. |
| **`GrainName`** | The stable, cluster-wide, serializable identity of a grain. A `(GrainType, key)` pair; `key` is an arbitrary application string. |
| **Shard** | A partition of one grain type's namespace and the unit of *placement*: a small per-shard Raft group (the **leader-election group**) owning leadership and term, and holding as its Raft membership the *realized* replica set for that type's grains whose names fall in its range, holding *no* grain data (§7.1, §8). The grains' data is durable by per-grain quorum append, not a shared log. |
| **Shard leader** | The single node elected by a shard's leader-election group to write and host every grain in the shard's range, for one term (§5.2, §8). |
| **Replicator** | The per-grain durability mechanism: it quorum-appends a grain's records to the shard's replicas, fenced by the shard term, and recovers a grain's head from a quorum on activation. Two tiers: single-node `Local`, clustered `Quorum` (§7.2, §7.4). |
| **Shard map** | The cluster's consensus-agreed record of the *intended* allocation: which nodes should replicate each shard and which key range it covers. A per-grain-type map group seeded from the leader-based control plane, reconciled into each shard's realized membership (§7.6). |
| **Activation** | The live, in-memory instance of a grain on the leader of its shard. Disposable; rebuilt from the journal. |
| **Event** | A serializable value appended to a grain's journal; the unit of durable change. |
| **`apply`** | The pure fold that applies an event to state. Runs identically on live commit and on replay. |
| **State** | The value obtained by folding a grain's events; the snapshot payload. A cache of the journal. |
| **GrainJournal** | A grain's durable, totally-ordered, append-only log of events, quorum-appended by its **Replicator** to the shard's replicas (§7.3). The source of truth. |
| **Snapshot** | A persisted `(Seq, state)` checkpoint that bounds replay cost (§9); under the facet model, the composite of every facet's contribution at one seq (§7.12). |
| **Facet** | A durable storage feature of one grain (the event fold, the KV map §7.13, the workspace §7.11, the SQL database §7.14, the disk image §7.15, the alarm §7.16, the workflow memo §7.17), defined entirely as an interpretation over the two storage primitives: what its records mean, what it keeps in blobs, what it contributes to the snapshot (§7.12). |
| **Facet tag** | The stable identifier a multi-facet grain's records carry, dispatching each record to its facet on replay; an unrecognized tag aborts activation (§7.12). |
| **`Seq`** | The position of an event in one grain's total order; first event at 1. |
| **Quorum** | The majority of a shard's replicas whose acknowledgment makes a per-grain append durable (§7.2), or commits a leader-election entry. |
| **Term** | The shard's leadership term; it increases on every leadership change and is the single-writer fencing token every append carries (§8). |
| **Gateway** | The node-local actor that routes a call to the right shard leader and owns the activation table for shards this node leads (§5.3). |
| **Host** | The per-grain actor on the shard leader: holds state and head, and drives the durability protocol (§6, §10). |

---

## 4. The grain model

### 4.1 The `Grain` trait

A grain is event-sourced. The author implements the **behavior** (immutable configuration) as a type, declares the **state** and **event** types, and writes the pure fold:

```rust
pub trait Grain: Sized + Send + 'static {
    /// The system this grain's activation runs on (a clustered ActorSystem, actor §4).
    type System: ActorSystem;

    /// The folded state and snapshot payload. Rebuilt from the journal on activation.
    type State: SerializationRequirement + Default;

    /// The journal record type: the unit of durable change.
    type Event: SerializationRequirement;

    /// The grain type's stable, serializable identity: the namespace tag in every
    /// `GrainName` of this type (§5.1) and the receptionist key the type's gateway
    /// is discovered under (§5.3). An explicit constant (e.g. `"bank.Account"`) is
    /// REQUIRED — the runtime needs a rename-stable tag, which `type_name` is not.
    /// It is the *default* type name; one Rust grain MAY be hosted under several
    /// runtime type names via `granary_named` (Appendix A), in which case this
    /// const is the fallback a wire-decoded `GrainRef` recovers.
    const GRAIN_TYPE: &'static str;

    /// Apply one event to state. MUST be pure and deterministic: it runs on the
    /// live commit path AND on replay/rehydration, and the two MUST agree
    /// (invariant G2). It MUST NOT perform I/O, read the clock, or use entropy.
    fn apply(state: &mut Self::State, event: &Self::Event);

    /// List the command messages this grain accepts over the network (§5.4).
    /// Mirrors `Actor::register` (actor §4.4); the default registers nothing.
    fn register(_r: &mut GrainRegistry<Self>) {}

    /// Called once after the activation has rehydrated, before the first command.
    /// Returning Err aborts activation (the message that triggered it fails).
    fn on_activate(&mut self, _ctx: &GrainCtx<Self>)
        -> impl Future<Output = Result<(), BoxError>> + Send { async { Ok(()) } }

    /// Called once before the activation deactivates — idle eviction, handoff, OR
    /// a forced step-down (leadership move, quorum loss). Runs even when the
    /// journal is unwritable, since it cannot persist (the ctx exposes no
    /// `persist`); a layered runtime uses it to release non-durable per-activation
    /// resources (e.g. the agentic harness's sandbox, harness §5.3).
    fn on_passivate(&mut self, _ctx: &GrainCtx<Self>)
        -> impl Future<Output = ()> + Send { async {} }

    /// Whether the activation MAY idle-hibernate now (§10). Consulted only on idle
    /// eviction; a forced step-down ignores it. The default always permits it. A
    /// grain with autonomous, not-yet-journaled work (the agentic harness's live
    /// run) overrides this to veto eviction until the work settles; the host
    /// reschedules the idle check rather than evicting mid-flight.
    fn can_passivate(&self, _state: &Self::State) -> bool { true }
}
```

`SerializationRequirement` is the actor framework's bound (actor §5); the codec is the system's, not the grain's.

### 4.2 Commands and the decide/apply split

A **command** is a `Message` (actor §3.2): a serializable request with a declared `Reply` and a stable `MANIFEST`. A grain accepts a command by implementing `GrainHandler<M>`:

```rust
pub trait GrainHandler<M: Message>: Grain {
    /// Decide the outcome of a command: inspect the current state, and return
    /// the events to persist together with the reply.
    ///
    /// This is a *decision*, not a mutation: it MUST NOT mutate state directly
    /// (state changes only through `apply`, §4.1) and MUST NOT perform durable
    /// I/O (the host owns persistence, §6). A read-only command returns no
    /// events: `(vec![], reply)`, which commits nothing (§7.5).
    fn handle(&self, state: &Self::State, msg: M, ctx: &GrainCtx<Self>)
        -> impl Future<Output = (Vec<Self::Event>, M::Reply)> + Send;
}
```

This **decide/apply split** is normative, the deliberate alternative to an imperative `ctx.persist(event).await` API. The reasons:

1. **No interior mutability.** The host actor owns the activation's state and journal head and mutates them only through its `&mut self` on the serial executor (actor §6). The split keeps that the only writer; an imperative `persist` under the shared `&GrainCtx` would force an `Arc<Mutex<…>>` over state, re-introducing the very sharing the actor model removes (actor §3.5) and inviting a "don't hold the lock across `.await`" footgun.
2. **`Send` falls out for free.** Handler futures must be `Send` (actor §3.2); with the split, nothing crosses an `.await` but the user's own values.
3. **Atomic batch per command.** All of a command's events commit in one atomic append (§7.3), so no observer ever sees a partial command.
4. **The durability barrier lives in exactly one place**, the host (§6), where it is auditable, not scattered across handlers.

**Facets stage; the commit applies (§7.12).** On a grain with facets beyond the event fold, the handler also works imperatively through the facet accessors — `ctx.kv()`, `ctx.ws()`, `ctx.sql()` — without weakening the split. A *logical* facet stages operations in a per-command overlay: reads see committed-plus-staged, and the staged operations become the command's tagged records, folded only on commit. A *physical* facet mutates a local materialization that is a rebuildable cache (§1), captured into records before the append and discarded outright on any non-committed outcome (§7.11, §7.14). The split's guarantee was never "the handler touches nothing"; it is "committed, observable state changes only at the commit point," and that survives intact.

A reply that must reflect post-command state computes it from `&State` plus the events the handler is emitting (the handler knows both). Where this is awkward, the host MAY expose a convenience that folds the emitted events into a scratch copy of state and passes it to a reply closure; that convenience is non-normative.

**Application errors are values.** As in actor §3.2/§14, a fallible command uses `type Reply = Result<T, E>`. An application failure is a value inside the reply, distinct from a transport failure (`CallError`) or a durability failure (`GrainError`, §12).

Example:

```rust
pub struct Account;                                   // behavior: stateless config here
#[derive(Default, Serialize, Deserialize)] pub struct Balance { cents: i64 }
#[derive(Serialize, Deserialize)] pub enum Ledger { Deposited(u64), Withdrew(u64) }
pub struct Overdraft;                                 // an application error (lives inside M::Reply)

impl Grain for Account {
    type System = ClusterSystem;
    type State  = Balance;
    type Event  = Ledger;
    fn apply(state: &mut Balance, e: &Ledger) {
        match e { Ledger::Deposited(n) => state.cents += *n as i64,
                  Ledger::Withdrew(n)  => state.cents -= *n as i64 }
    }
    fn register(r: &mut GrainRegistry<Self>) { r.accept::<Withdraw>(); r.accept::<Deposit>(); }
}

#[derive(Serialize, Deserialize)] pub struct Withdraw { cents: u64 }
impl Message for Withdraw { type Reply = Result<i64, Overdraft>; const MANIFEST: Manifest = Manifest::new("bank.Withdraw"); }

impl GrainHandler<Withdraw> for Account {
    async fn handle(&self, state: &Balance, msg: Withdraw, _: &GrainCtx<Self>)
        -> (Vec<Ledger>, Result<i64, Overdraft>) {
        if (state.cents as u64) < msg.cents { return (vec![], Err(Overdraft)); }   // no event, nothing to commit
        (vec![Ledger::Withdrew(msg.cents)], Ok(state.cents - msg.cents as i64))    // reply reflects post-state
    }
}

#[derive(Serialize, Deserialize)] pub struct Deposit { cents: u64 }
impl Message for Deposit { type Reply = i64; const MANIFEST: Manifest = Manifest::new("bank.Deposit"); }

impl GrainHandler<Deposit> for Account {
    async fn handle(&self, state: &Balance, msg: Deposit, _: &GrainCtx<Self>) -> (Vec<Ledger>, i64) {
        (vec![Ledger::Deposited(msg.cents)], state.cents + msg.cents as i64)       // infallible: always one event
    }
}
```

### 4.3 `GrainRef` and `GrainCtx`

`GrainRef<G>` is the only handle to a grain. It is `Clone + Serialize + DeserializeOwned + Send + Sync`, carries the `GrainName` plus a system handle, and never grants access to state (the actor `ActorRef` discipline, actor §3.3):

```rust
impl<G: Grain> GrainRef<G> {
    pub fn name(&self) -> &GrainName;
    pub async fn ask<M>(&self, msg: M)  -> Result<M::Reply, GrainError> where G: GrainHandler<M>, M: Clone;
    pub async fn tell<M>(&self, msg: M) -> Result<(), GrainError>       where G: GrainHandler<M>, M: Clone;
    pub async fn ask_timeout<M>(&self, msg: M, within: Duration) -> Result<M::Reply, GrainError> where G: GrainHandler<M>, M: Clone;

    /// Subscribe to the grain's committed records from `from` (exclusive),
    /// returning the current head and a stream of live record batches (§7.9). A
    /// framework built-in, available for every grain type without the author
    /// registering it — the analogue of `load`/`head`, surfaced as a push.
    pub async fn subscribe(&self, from: Seq) -> Result<Subscription<G>, GrainError>;
}
```

The `G: GrainHandler<M>` bound proves at compile time that the grain accepts `M`, so an invalid call does not compile (invariant **G10**, the grain analogue of actor §3.3). The call site is identical whether the activation is local or on another node (§5). `subscribe` is grain-type-agnostic and so carries no such bound (§7.9).

The `M: Clone` bound lets the runtime re-issue the command when the first attempt provably did not run: a stale cached host that hibernated (`DeadLetter`) or whose leadership moved (`NotLeader`), neither of which commits (§6, §8). An *ambiguous* transport failure (`Unreachable`/`Timeout`) is never auto-retried, because the command may have committed before the reply was lost (at-most-once, §2.2). The clone is of the caller's own small command value.

`GrainCtx<G>` is the handler/lifecycle context. It exposes the grain's name, its colocated blob area (the content-addressed blob primitive, §7.10), a `GrainRef` self-reference, and the system handle. (Surfacing the actor `Ctx` underneath for inherited capabilities such as death watch and child spawning is a deferred addition, §16; the context exposes the four accessors below, plus one accessor per declared facet, §7.12.)

```rust
impl<G: Grain> GrainCtx<G> {
    fn name(&self) -> &GrainName;
    fn blobs(&self) -> GrainBlobs;       // the grain's content-addressed blob area (§7.10)
    fn this(&self) -> GrainRef<G>;
    fn system(&self) -> &G::System;
}
```

`GrainCtx` deliberately exposes **no** `persist` method (§4.2). It does not expose state mutation; state changes only through events folded by `apply`. Under the facet model it additionally exposes one accessor per facet the grain declares — `kv()` (§7.13), `ws()` (§7.11), `sql()` (§7.14) — each gated at compile time by the grain's facet set (§7.12); a facet accessor stages, it never mutates committed state (§4.2).

---

## 5. Virtual activation and routing

### 5.1 Names and shards

A grain is addressed by `GrainName`, a `(GrainType, key)` pair where `key` is an arbitrary application-chosen string (`"account/42"`, a UUID, a tenant id). Unlike an `ActorId` (actor §3.6), a `GrainName` is **not** locality-classifiable on its own: it names a logical object, not a node.

Every name maps to exactly one **shard** of its grain type, by a stable hash of the name onto a key-range partition (§7.1). The mapping changes only when a shard splits or merges (§7.7), not when nodes come and go, so it is far steadier than a direct name-to-node mapping. A second lookup, the shard map (§7.6), gives the shard's current leader. Resolution is therefore two levels: name to shard (stable), shard to leader (changes on Raft elections).

`GrainName` is `Clone + Eq + Hash + Send + Sync + Serialize + DeserializeOwned`.

### 5.2 Where a grain lives

A grain activates on the **leader of its shard**. Co-locating the activation with the shard leader is deliberate: the leader is one of the shard's replicas, so a write appends to its local replica before the Replicator fans it to a quorum (§7.2), and rehydration confirms the grain's head from a quorum and serves subsequent reads locally (§9). The leader hosts the compute for every active grain in its shard.

Leadership, and therefore activation placement, follows the shard's Raft election. When leadership moves, the new leader serves the shard's grains; activations rehydrate there lazily on first access (§10). A node that leads many shards hosts many grains; the cluster balances load by moving shard leadership and by splitting shards (§7.7), not by moving individual grains.

### 5.3 The gateway and exactly-once activation

Each node runs one **gateway** actor per grain *type*, registered in the receptionist (actor §13) under a single well-known key for that type, one gateway entry per node. The gateway routes inbound calls (§5.4) and, for shards this node leads, owns the **activation table** mapping `GrainName → Host`.

A gateway is mandatory, not incidental: the runtime mints an activation's `ActorId` at `spawn` (actor §4.2), and that id is **not** derivable from the `GrainName`. No `ActorId` a remote caller could reconstruct addresses a grain directly, so the name-to-activation mapping must live in an explicit table, and the gateway is that table plus the actor that guards it.

Activation is **exactly-once per node by construction.** The gateway is a serial actor (actor §6), so getting-or-activating a name is a critical section: the gateway processes two concurrent requests for a not-yet-active name in sequence, the first activating the host, the second finding it. No lock is needed (invariant **G6**).

### 5.4 Routing a call

`GrainRef::ask<M>(msg)` resolves in two levels and caches both:

1. **Name to shard** by the stable hash (§5.1).
2. **Shard to leader** from the cached shard map (§7.6).
3. **Send to the leader's node:**
   - **Local leader:** hand the typed command to this node's gateway, which gets-or-activates the host and `ask`s it. No serialization (the local fast path, actor §4.3).
   - **Remote leader:** send one envelope carrying the name, the command's `MANIFEST`, and the codec-encoded payload to the leader node's gateway, which gets-or-activates the local host, looks the manifest up in the grain's dispatch registry (§5.5), decodes, invokes the typed handler, and encodes the reply.
4. **Stale leader:** if the target node no longer leads the shard, it replies `NotLeader(hint)`. The caller refreshes its shard-map cache and retries against the hint, bounded to avoid a loop. This is ordinary Raft client redirection.

Two performance notes follow from caching, both addressing throughput at scale (§7.8):
- The gateway serializes only the activation-table mutation. Once a caller has cached the leader and the host is active, repeated calls go straight to the host actor (a normal mailbox), so the serial gateway is off the steady-state hot path.
- The shard-map cache is refreshed on `NotLeader`, not consulted per call, so routing costs no control-plane traffic in steady state.

### 5.5 The grain dispatch registry

`GrainRegistry<G>` maps a command's `MANIFEST` to a dispatch entry that *decodes the payload, invokes the typed `GrainHandler`, and encodes the reply*, the grain analogue of the actor dispatch registry (actor §4.4), differing only in that it returns reply bytes rather than enqueuing-and-dropping. `Grain::register` fills it via `r.accept::<M>()`, exactly as `Actor::register` does. The registry is the deserialization allowlist for grain commands (actor §5, §15): a name-addressed command whose manifest is unregistered yields `GrainError::Call(CallError::Unhandled)`, and no type outside the registry is ever built from network bytes.

As in the actor framework, a single concrete envelope type carries the manifest and payload across the wire, so there is no need to derive a per-command manifest for a generic wrapper; the inner manifest is the dispatch key.

---

## 6. The consistency model: input and output gates

Granary lifts the Durable Objects gate model (DO §3) onto the framework, so that ordinary sequential handler code is race-free and durable at almost no cost over the serial executor.

- **Input gate.** While a grain's append is in flight, the host admits **no new command** to that grain. New messages queue in the mailbox until the host has no outstanding append. This closes the window a plain serial executor leaves open *at `.await` points* (actor §6): a second command cannot observe half-committed state mid-handler.
- **Output gate.** When a command produces events, the host holds the **reply** until those events are durable on a quorum (§7). The reply travels through the actor `ReplyHandle::send` (actor §4.5) only after the commit. If the commit fails, the reply becomes a `GrainError` (§12); no observer is ever told an effect happened that did not durably happen.

The host's per-command protocol runs in order, and it is the one place the barrier lives:

```
1. (events, reply) = G::handle(&state, msg, ctx)        // decide; no mutation, no I/O
2. if events.is_empty() { return reply }                // read path: nothing to commit (§7.5)
3. outcome = journal.append(grain = name, after = head, events)  // §7.3, per-grain quorum append, fenced by the shard term
4. on Committed(new_head):                              // durable on a quorum
       if new_head != head + events.len() {             // CONTIGUITY GUARD (G1/G3)
           step_down(); return GrainError::Unavailable  //   head was stale — re-read, do not fold
       }
       for e in &events { G::apply(&mut state, e) }     // fold AFTER durability
       head = new_head
       maybe_snapshot()                                 // §9
       return reply                                     // OUTPUT GATE releases here
5. on NotLeader(hint):                                  // leadership moved off this node (§8)
       step_down(); return GrainError::NotLeader(hint)  // caller retries against the new leader
6. on Unavailable:                                      // shard quorum lost OR commit timed out (§11)
       step_down(); return GrainError::Unavailable      // ambiguous fate — re-read; caller retries
```

Three ordering rules are normative and load-bearing (invariant **G1**):
- **Fold only after durability.** The host MUST NOT fold state before the entry commits. A handler observes its own prior writes because each completed command folded only on commit.
- **Reply only after durability.** The host MUST NOT release the reply before the entry commits, and MUST NOT release it as success if the entry does not commit.
- **Fold only a contiguous head.** A grain is its shard's single writer (§8), so a commit MUST advance its head by exactly the batch just appended. The quorum-recovery barrier (§9) makes a non-contiguous return unreachable in normal operation, since recovery establishes a quorum-durable head before the first append, so this guard is a defensive backstop against any residue (for example a previously timed-out append surfacing late, §7.2). If the journal ever returns a head that jumps further, the host MUST NOT fold (the intervening committed events were never applied to its state); it steps down and the next access rehydrates from the journal authority (**G3**). The host treats `NotLeader` and `Unavailable` the same way: any outcome other than a contiguous commit ends the activation, because after it the in-memory head can no longer be trusted. A forced step-down emits `Passivated` (§13) so the lifecycle stream stays balanced.

**With facets, the protocol generalizes without adding a barrier (§7.12).** Step 1 also drains every declared facet's stage — a logical facet's overlay operations, a physical facet's captured delta — into tagged records that join the events in step 3's single atomic batch (**G19**); step 2's read path is a command whose batch is empty after the drain. Step 4 folds the logical stages and keeps the physical materializations; steps 5–6 additionally discard every physical facet's materialization, a rebuildable cache the next activation reconstructs (**G20**, §7.14). One command, one batch, one barrier, whatever the facet mix.

**`tell` is fire-and-forget.** A `tell` returns once the host accepts the command, not after the commit (actor §3.3, §7.2), so it reports only the enqueue-time failures (`Call` — including an unregistered manifest — and `NotLeader`), never `Unavailable`. The host runs the same protocol for the command, but the caller does not await the outcome: if the commit cannot complete, the command has no effect and the caller is not notified. This is the at-most-once contract (actor §7.2), which callers make idempotent where it matters.

---

## 7. Durability and replication

### 7.1 Shards: the unit of placement

Granary partitions each grain type's namespace into **shards**. A shard owns a contiguous range of its type's name hash space; a small per-shard Raft group of *R* replicas (`R=3` or `R=5`), the shard's **leader-election group**, elects a single leader per term and records the replica set (§8). This group holds *no grain data*, only leadership, term, and membership, so its log is tiny and changes only on failover and reconfiguration. The leader is the single writer for every grain in the range and hosts their activations (§5.2); each grain's records are made durable by a per-grain quorum append (§7.2), not by a shared log. Per-type shards keep the gateway, dispatch registry, and configuration (Appendix A) all keyed by one grain type, and let a grain replay through a single known `apply` (§4.1).

This is the deliberate middle ground the design stance draws, between one cluster-wide log (a global bottleneck) and one Raft group per grain (millions of groups, each with its own timers and persistent term/vote, and an election storm on any node failure). A shard's leader-election group covers many grains, so the cluster runs O(shards) of them, plus one map group per type (§7.6), bounded by splitting (§7.7), never O(grains), and none is on the data path.

Keeping grain data off the leader-election group is what avoids the shared-log bottleneck: because each grain's records are quorum-appended on their own, a write-heavy grain never queues behind its shard-mates, and no head-of-line block couples grains that merely share a range. Per-shard write throughput then scales with the number of active grains, not one leader's log. Splitting a hot shard (§7.7) rebalances *placement and replication* load across nodes; it is not a knob for write bandwidth. What a shard shares among its grains is leadership and a replica set, not a serialization point.

### 7.2 Ordering is free; durability is a quorum append

Each grain has a single writer, the shard's leader (§8), so ordering is free *per grain*: the host assigns each of a grain's records the next `Seq` with no agreement round, because no other writer proposes records for that grain. The grain's **Replicator** then fans the record out to the shard's replicas and reports it durable once a quorum has stored it. This is exactly the split the DO research note draws (DO §4.3): ordering is free from the single owner, durability is a quorum append. The only agreement round, electing the shard leader, happens on failover (§8.3), never per write, and never across grains.

A grain's records are totally ordered among themselves but share no order with any other grain's: there is no shard-wide log to serialize them into. The host appends a grain's records in `Seq` order behind its input gate (§6), and replay reads them back in that order (§9); a second grain's appends proceed concurrently on their own quorum.

**Idempotent by sequence slot.** The leader awaits each append's quorum acknowledgment, bounded by a timeout that reports `Unavailable` (§11). A timed-out append MAY still reach a quorum later, but this never double-applies and needs no de-duplication token, because of the discipline §6 already mandates. The host does not retry an append and does not fold on any outcome but a contiguous commit: on a timeout it steps down without folding, and the next activation recovers the grain's head from a quorum (§9) and folds each record once, in `Seq` order. A record occupies exactly one `Seq` slot for its grain, so a late commit simply appears at its slot on recovery and is applied there once; a stale-term append racing a new leader is refused outright (the slot is filled and the term is lower, §8). The single-writer `Seq` is the record's identity; no separate proposal id or epoch is required.

**The rollback is term-bounded.** On any non-committed outcome the leader rolls back its own *tentative local* write (so a later quorum-less recovery, §7.5, never folds an uncommitted record), and that rollback MUST drop only slots written **at or below the appending term**. While the failed append's quorum wait was in flight, a newer leader may already have fenced this store and landed committed records for the same grain above the same head — this node is still one of the shard's replicas — and those records carry the higher term. A term-blind rollback would silently delete them from this replica, shrinking a committed write's durability below a quorum (violating **G14**); the term bound makes it drop exactly the failed append's own slots and nothing newer.

### 7.3 The journal seam

The journal is a trait, a simulation and deployment seam like `Transport` and `Clock` (actor §4.6, §7), operating on opaque, codec-encoded event bytes so it stays codec-agnostic:

```rust
pub trait GrainJournal: Clone + Send + Sync + 'static {
    /// Append `events` for one grain immediately after `after`, as one atomic
    /// entry. Commits on a quorum, fenced by the shard term (§7.2).
    fn append(&self, grain: &GrainName, after: Seq, events: Vec<Vec<u8>>)
        -> impl Future<Output = AppendOutcome> + Send;

    /// Up to `limit` committed events for one grain from `from` (exclusive)
    /// toward its head. Local on the leader once `head` has recovered the grain
    /// (records up to the recovered head are present locally).
    fn load(&self, grain: &GrainName, from: Seq, limit: usize)
        -> impl Future<Output = Result<Vec<(Seq, Vec<u8>)>, GrainJournalError>> + Send;

    /// The grain's committed head — the authoritative source of `head` on
    /// rehydration (§9, invariant G3/G4), and the rehydration barrier itself.
    /// `Seq::ZERO` for a grain with no committed events. On the `Quorum` tier this
    /// recovers the head from a write quorum of the shard's replicas by read-repair
    /// (highest-term record per slot, written back to a quorum, §8) and backfills
    /// any records the leader is missing, so a fresh leader never folds onto a
    /// stale head and subsequent `load`s read locally (§8, §9). On the `Local`
    /// tier it reads locally. Rehydration derives `head` from this, never from
    /// memory.
    fn head(&self, grain: &GrainName)
        -> impl Future<Output = Result<Seq, GrainJournalError>> + Send;

    /// Persist a snapshot for one grain at a committed seq (§9). Returns
    /// `Committed(at)` on success, or `NotLeader` if this node no longer leads.
    fn save_snapshot(&self, grain: &GrainName, at: Seq, state: Vec<u8>)
        -> impl Future<Output = AppendOutcome> + Send;

    /// The latest snapshot for one grain, if any (§9). On the `Quorum` tier this
    /// recovers the latest durable snapshot from a write quorum of the shard's
    /// replicas, so a fresh leader that lacks it locally still finds it (the
    /// snapshot analogue of `head`'s record recovery, §8). On the `Local` tier it
    /// reads locally.
    fn load_snapshot(&self, grain: &GrainName)
        -> impl Future<Output = Result<Option<(Seq, Vec<u8>)>, GrainJournalError>> + Send;

    // --- The grain-native content-addressed blob primitive (§7.10) ---
    // A grain's immutable blobs, replicated to the *same* shard replicas as its
    // records but off the ordered/fenced path (no `Seq`, no term). They ride the
    // journal seam so the host needs no extra dependency to hand the grain a
    // `GrainBlobs` handle; the runtime surfaces them through `ctx.blobs()`.

    /// Store an immutable blob for one grain, durable on a write quorum of its
    /// replicas (one local copy on `Local`). Idempotent and dedup'd by content.
    fn put_blob(&self, grain: &GrainName, id: BlobId, bytes: Vec<u8>)
        -> impl Future<Output = Result<(), GrainJournalError>> + Send;

    /// Fetch a verified blob for one grain, or `None` if no replica holds it.
    fn get_blob(&self, grain: &GrainName, id: BlobId)
        -> impl Future<Output = Result<Option<Vec<u8>>, GrainJournalError>> + Send;

    /// Whether one grain's blob is present on **any reachable replica** (not a
    /// quorum count): a `true` means a `get_blob` can source the bytes, not that
    /// they are quorum-durable (durability is established at `put_blob`).
    fn has_blob(&self, grain: &GrainName, id: BlobId)
        -> impl Future<Output = Result<bool, GrainJournalError>> + Send;

    /// Keep only the listed blobs of one grain, dropping the rest — the grain's
    /// mark-from-roots GC (`ctx.blobs().gc`, §7.10). Best-effort.
    fn retain_blobs(&self, grain: &GrainName, retain: Vec<BlobId>)
        -> impl Future<Output = ()> + Send;

    /// Drop **all** of one grain's blobs — grain-scoped reclamation on destroy
    /// (`ctx.blobs().destroy`, §7.10). Best-effort.
    fn delete_blobs(&self, grain: &GrainName) -> impl Future<Output = ()> + Send;
}

pub enum AppendOutcome {
    Committed(Seq),          // durable on a quorum; the new head (for a snapshot, the snapshot seq)
    NotLeader(NodeId),       // this node no longer leads the shard; redirect (§8)
    Unavailable(String),     // quorum unreachable, OR the commit timed out (ambiguous); pause (§11)
}

pub enum GrainJournalError {
    Unavailable(String),     // a local read could not complete (I/O or corruption)
}
```

`Seq::ZERO` is the empty head; a grain's first event commits at `1`. The host calls `append` only from the shard leader and behind the grain's input gate, so `after` always equals the grain's known head; a non-leader call returns `NotLeader` rather than a stale-sequence error. Object-safety (`impl Future` vs. boxed) is an implementation choice consistent with the actor framework's seams.

**Below the seam: placement and durability.** `GrainJournal` is the *only* seam the host sees, and it does not change. Its clustered implementation rests on two parts:

- the shard's **leader-election group** (§8) supplies placement: who may write, under which term. The journal consults it to stamp every append with the current term and to answer `NotLeader`.
- a per-grain **Replicator** supplies durability: it quorum-appends a grain's opaque record bytes to the shard's replicas, fenced by the shard term, and recovers a grain's committed head from a quorum on activation (§8, §9). The two reference tiers (§7.4) are Replicator implementations.

The records are the grain's `Event`s and state is the `apply` fold (§4.1); a snapshot is the serialized `State`. (Interpreting a grain's records as something other than an event log — the SQL facet's WAL frames — is the facet model's physical class, §7.12/§7.14, over this same Replicator substrate.) The host neither names nor depends on the parts below the seam: it sees `append`/`load`/`head`/`save_snapshot`/`load_snapshot` for the journal and `put_blob`/`get_blob`/`has_blob`/`retain_blobs`/`delete_blobs` for the colocated blob primitive (§7.10), and nothing else. This is what keeps the substrate replaceable with no change above the seam.

### 7.4 Durability tiers: the two Replicators

The durability concern is the **Replicator** (§7.3), and it admits multiple implementations; two are reference tiers, chosen at deployment like a membership mode (actor §9.4):

1. **`Local` (single-node).** One linearizable local store (in-memory, or a local file log with the hard-link/optimistic-concurrency fence of the harness prior art). An append commits on local fsync; there is no leader-election group and no term, because a single node is always its own writer. Best for: embedded, single-node, tests, and the deterministic simulator (§14). CP trivially (one writer, one store), not fault-tolerant to node loss.
2. **`Quorum` (clustered).** The fault-tolerant, CP production target: a per-grain quorum append fenced by the shard term (§8), over the shard's *R* replicas. A grain's record is durable once a quorum has stored it, and survives the loss of any minority of the shard's replicas; a new leader recovers each grain's head from a quorum on activation (§8, **G14**). Placement and replication stay bounded by split/merge (§7.7).

### 7.5 Reads

A command that emits no events (a query) commits nothing (§6 step 2). The leader serves it from the grain's in-memory activation, a local, replication-free read. This is the property Durable Objects prize: a read is served by the single owner with **no per-read consensus**, so it is effectively zero-latency (DO §4.3, §4.4). Reads scale with the leaders' capacity and never wait on replication.

**The contract is read-your-leader (relaxed), not linearizable under partition.** Because the activation is colocated with the shard leader (§5.2) and the read path does not reconfirm leadership per call, a leader that has been deposed but not yet fenced, an isolated minority leader that has not yet learned it lost the election, MAY serve a stale read until its activation stops. Writes never fork (the shard term fences the commit, §8); only reads can be stale, and only on the minority side of a partition. A caller that needs a linearizable read in the meantime issues a trivial *writing* command (one that emits an event): it rides the §6 output gate, so it commits through the shard leader and reflects committed state, or fails (`NotLeader`/`Unavailable`) on a deposed leader.

**Linearizable reads are a deferred upgrade (§16), via a check-quorum property on the shard's leader-election group (§8), not a per-read consensus round.** The DO-faithful mechanism is single-instance fencing, not a Raft read-index: the leader serves reads locally while it holds a **check-quorum lease** (it has heard from a quorum within an election timeout, so no other leader can have been elected), and the activation **self-fences**, returning `Unavailable`/`NotLeader`, when the lease lapses, trading availability for consistency on the minority side (CP, §11). This keeps steady-state reads local and zero-latency, unlike a read-index, which would pay a quorum round-trip per read and defeat read scaling (§7.8). (Reads from shard *followers* are a separate deferred extension, §16.)

### 7.6 The shard map is a consensus-agreed allocation

The **shard map** records, for each shard, its key range and its replica set; leadership within a shard is the leader-election group's (§8), surfaced to routers by `NotLeader` redirect (§5.4), not tracked in the map. It MUST be **consensus-agreed**, so every node resolves a name to the same replica set regardless of join order: a rendezvous choice each node snapshots from its own *live* membership view would diverge across nodes that join at different membership epochs. The reference implementation realizes this as a **per-grain-type Raft group whose committed log *is* the allocation** (one `Assign { shard, replicas }` record per shard); every node applies the identical committed entries and so agrees on where each shard lives. This map group is keyed by grain type and derives its group id from a reserved shard index, so it never collides with a shard's leader-election group (§8.2).

A leader-only **allocator** keeps each shard's committed replica set equal to its rendezvous choice over the current cluster voters, and a leader-only **reconcile** loop drives each group it leads (the map group and the shard leader-election groups it holds) toward its intended membership, so shards rebalance onto and off of changed members as the cluster grows or shrinks. Because the leader-election group holds no grain data (§7.1), catching up its prefix is *not* enough: the new replica MUST also receive each grain's committed records and latest snapshot **before** it counts toward any per-grain write or recovery quorum — a voter that lacked the grain data would break the quorum intersection G14 relies on (it could join a recovery quorum holding none of a grain's records).

**Replica-set migration (implemented).** A changed rendezvous choice therefore never replaces a committed set outright; it runs a **joint-quorum migration**:

1. The allocator proposes `Assign { shard, replicas }`; on an already-allocated shard this commits the new set as the shard's **target**, leaving `current` in place. From that commit, every per-grain append, snapshot, and recovery on the shard uses the **joint quorum** — a majority of `current` AND a majority of `target`, fanned to the union — so every pre-migration commit (on a majority of `current`) and every in-migration commit (on both majorities) stays intersected by every recovery read. The allocator proposes nothing further for a shard while its migration is in flight.
2. A node newly in the union creates the shard's leader-election group as a non-member over the *current* voters (no election disruption; the reconcile loop, which drives the group toward the union during migration, promotes it) and builds its journal over the joint sets. A continuing replica's journal switches to the joint sets in place — the replica sets are **live**, read per operation, never a construction-time snapshot.
3. The shard leader's **migration driver** catches every grain up on the target: it enumerates the shard's grains from a read quorum of `current` (the union of their local lists — any read majority sees every committed grain), then per grain runs the §8 recovery (whose joint write-back lands the records on the target replicas), re-persists the best snapshot on the joint quorum (a compacted grain's prefix exists only in its snapshot), and copies the blob area verified-by-content. Every step is idempotent; a leader change mid-migration re-drives on the new leader, and a failed step just retries.
4. Only after a complete pass does the driver propose `Migrated { shard }`: applying it flips `current = target`, departing nodes drop their journals, and the reconcile loop shrinks the group's voters to the new set. A node that departed keeps a stale local view of the group it left, so a routing hint from a believed leader is trusted only when the committed map still lists that node as a replica (§5.4).

The map's write rate is low: it changes on shard splits and merges (§7.7), on replica-set reconfiguration, and on node membership changes, never on a grain activation or a grain write. Nodes cache the map and refresh on `NotLeader` (§5.4), so steady-state routing adds no map-group traffic (invariant **G9**). The map group seeds its membership from the actor framework's **leader-based control plane** (actor §9.4.3): granary therefore requires the leader-based control-plane mode; the pure gossip-based AP mode (actor §9.4.4) cannot agree a shard map (this is the consistency-versus-availability tradeoff of actor §12, resolved toward CP).

> **Note.** Actor §9.4.3 item 6 stores the allocation *inside* the control-plane group's own log; granary instead runs a dedicated per-type map group seeded from the control plane's voters. The guarantee (a single consensus-agreed allocation, off the grain data path) is identical, but the consensus-group count is `O(shards) + O(grain types)` rather than `O(shards)` alone (§8.2, **G7**).

### 7.7 Splitting and merging shards

A shard that grows too large or too hot **splits**: its key range divides in two, and the grains on each side become two shards with their own leader-election groups. A pair of small, cold adjacent shards MAY **merge**. Split and merge are the elasticity mechanism: the number of shards tracks load and cluster size, so per-shard work stays bounded as the grain count climbs into the millions.

A split MUST be atomic with respect to writes: the parent leader stops accepting new appends for the moving range, each moved grain's committed records and latest snapshot transfer to the child shard's replicas, and the new mapping commits to the shard map (§7.6) before either child serves the moved range. No write is lost or duplicated across the boundary, and no grain is writable in two shards at once (invariant **G15**). Implementations SHOULD trigger split/merge from shard size and request-rate signals on the §13 event stream.

### 7.8 Scaling: where the cost goes

The architecture's scaling claim is that cost tracks the *active working set* and the *cluster size*, not the total grain count.

- **Consensus groups:** O(shards) leader-election groups plus O(grain types) map groups, bounded by split/merge (§7.7), never O(grains). A node runs a fixed handful of Raft groups regardless of how many grains it stores, and none carries grain data.
- **Control-plane writes:** O(cluster events), namely splits, merges, reconfigurations, and membership, never per activation or per write (§7.6).
- **Activation:** no consensus, hence no election, no agreement round, no shared component (§5.2, §10). On the `Quorum` tier it costs one quorum-recovery round-trip to confirm the grain's head (§9); on the `Local` tier it is fully local. Either way it touches no Raft group, which is what lets hibernation be aggressive (§10).
- **Memory and streams:** bounded by the grains *active* at once, which hibernation keeps to the working set (§10). Total grain count is limited only by the shards' storage.
- **Routing:** two cached lookups, off the control plane and off the serial gateway in steady state (§5.4).

The residual bounded cost is per-shard placement: a hot shard's grains share one leader node, capped by splitting the shard (§7.7). The data path itself shares nothing across grains.

---

### 7.9 Record subscriptions (the journal follower)

A **subscription** is a live, best-effort delivery of a grain's committed records to a sink, layered over the durable `load` (§7.3). It exists so a follower learns of each record as it commits rather than by polling, without making delivery a source of truth. It is the push analogue of §7.5's local read: cheap, off the write path, and never authoritative.

`GrainRef::subscribe(from)` routes to the shard leader (§5.4), where the framework spawns a local **sink** — an `ActorRef` to a mailbox accepting the grain's record batches — and registers it in the activation's **sink set**. The call returns a `Subscription` carrying the committed `head` and a receiver of live record batches (the caller does not supply the sink; the framework owns it, so the subscriber just drains the receiver). After each commit (§6 step 4), the host delivers a batch of the seqs just appended to every sink. Delivery occurs **after** the fold and the output-gate release and MUST NOT gate the commit or any caller's reply; it is observational, emitted at the same point as `Committed` (§13), and so cannot affect a write's outcome (preserves **G5**). `subscribe` is a framework built-in, dispatched to the host for *any* grain type without appearing in the grain's `register` allowlist (§5.5) — the read-path analogue of `head`/`load`, not a user command.

**Reconcile by `Seq`.** A sink treats `Seq` as authoritative. Each batch carries the `from` it begins after; a sink that has seen up to `last` MUST close any gap (`from > last`) by `load` (§7.3), and MUST ignore records at or below `last` — a re-subscribe replay, or a timed-out append that committed late (§7.2). At-most-once delivery (actor §7.2) MAY drop, duplicate, or — across a re-subscribe — reorder batches; seq reconciliation absorbs all three. Push is a latency optimization over `load`; correctness rests on the journal (**G3**), never on delivery reliability. The reconstructed sequence a sink obtains by applying this rule is exactly what `load(from, …)` to the head would return (**G16**).

**Bounded, never blocking.** Each sink has a bounded delivery buffer. On overflow the host drops the sink rather than awaiting it; a slow or dead subscriber MUST NOT stall a grain's writes or its input gate (§6) — the subscription is off the write path entirely. A dropped sink observes its stream close and re-subscribes, backfilling from its last seq. Delivery runs through the framework's `Spawner`/`Transport` seams (actor §4.6), so it stays seed-reproducible under simulation (§14); the drop decision is a pure function of buffer occupancy, not the wall clock.

**Ephemeral.** The sink set is per-activation, never journaled (§1) — like the in-memory head, it is a cache rebuilt by subscribers re-contacting, exactly as a layered runtime rebuilds its outcome subscribers after a resume (the agentic harness, harness §7.3). On hibernation, migration, or forced step-down it is dropped and `Passivated` emitted (§13); subscribers re-subscribe against the current leader (§5.4) and backfill from their last seq. A subscription does **not** veto idle eviction (`can_passivate`, §10): a hibernated grain produces no records, and the next write re-activates it, at which point subscribers re-establish. (Parking a subscription across hibernation — holding the registration while the grain sleeps, so no re-subscribe is needed — is the deferred *hibernatable connections* extension, §16.)

### 7.10 The grain-native content-addressed blob store (the second primitive)

Records (§7.2–§7.3) and the blobs of this section are granary's **two storage primitives**: the ordered, term-fenced half and the immutable, bulk half of a grain's durable state. They are deliberately the only two replication mechanisms in the system — every further storage feature is a **facet**, an interpretation layered over them (§7.12), never a third mechanism.

A grain's journal holds its **small foldable state** — the value `apply` rebuilds on activation (§4.1, §9). Bulk, immutable bytes do not belong there: a workspace's file blocks, a database's pages, any large content a grain references but does not fold. So beside the journal, a grain node owns a second store: an **immutable content-addressed blob area**, replicated to the *same* shard replicas as the journal, reached from a handler or lifecycle hook through `ctx.blobs()`. A grain stores bulk bytes by content and keeps only their ids in its folded state. This is the colocated, zero-latency storage a Durable Object keeps on the machine where it runs (DO §2.3, §6 "the object is the repo"), and it is what lets a grain be a *durable object with a working set* rather than only a small event-sourced value.

The blob store is the journal's durability half with its **hard half removed**. A content hash names exactly one byte sequence for all time, so there is nothing to *order* and nothing to *agree on*: two writers of the same bytes cannot conflict, and a reader proves it got the right bytes by re-hashing them. The single-writer term fence (§8) exists only to keep one *mutable* head from forking; a blob has no mutable slot, so the blob path carries **no `Seq`, no term, and no read-repair** — it is `StoreRecord` minus everything fencing- and order-related. What remains is a single durability question, "is the blob stored on enough replicas yet?", answered exactly as a record's is.

- **The model.** A **`BlobId`** is the 32-byte BLAKE3 digest of a blob's bytes; granary defines it natively (it does not depend on a separate content store). A blob's full address is `(GrainName, BlobId)` — the id is identical wherever the bytes are stored; the grain scopes *which* area holds it. Blobs are write-once and dedup'd by content **within a grain**: equal bytes under one grain store once. `ctx.blobs()` offers `put` (→ `BlobId`), `get` (whole or a byte range), `has` (present on **any reachable replica**, so a `get` can source the bytes — not a quorum-durability measure, since durability is established at `put`), `gc` (mark-from-roots: keep only a live id set), and `destroy` (drop the whole area).
- **Verification on read (the blob analogue of `wal`'s torn-tail rejection, strengthened to a cryptographic digest).** Every `get` re-hashes the bytes it is about to return and compares to the requested `BlobId`; on mismatch it MUST NOT return them. On the `Quorum` tier it then falls through to the next replica, and returns an error only if no replica yields verifying bytes. Corruption and misdelivery are thus detectable at the point of use, never silent wrong bytes (**G17**).
- **Durability is a quorum append, colocated.** A `put` writes the **local** replica (the leader is one of the shard's replicas, §5.2, so a later `get` reads locally) and fans the bytes to the peers, acknowledging once a write quorum has stored them; the blob then survives the loss of a minority of the grain's replicas. There is no leader-election, term, or agreement round on this path (**G18**). A `put` that cannot reach a quorum returns `Unavailable`, and the caller retries — idempotently, since the id is a pure function of the bytes.
- **Reclamation is grain-scoped and root-driven.** The grain knows its own live id set (the ids its state still references), so it reclaims storage with a **mark-from-roots sweep** — `gc(live)` drops every blob not in `live` — and a whole-area `destroy`. This is what a liveness-blind shared store cannot do, and it is why the blob store needs **no namespace tombstone, no membership-gated resurrection guard, and no cluster-wide delete fan-out**: deletion targets the grain's own known replicas, and a delete a partitioned replica misses leaves only *orphan bytes* (reclaimed by a later sweep), never a *resurrection of referenced data*, because nothing in the grain's state points at them and only referenced blobs are ever fetched or re-replicated.
- **Lazy hydration and corruption self-heal.** A blob is faulted in on first access: a `get` that misses locally fetches a verifying copy from a peer and backfills the local replica, so a grain that re-activates on a fresh leader and then touches ten files pulls ten files' blocks, not its whole area — the move that lets a large durable object hibernate and migrate cheaply (DO §5). The same path heals a *corrupt* local copy as well as a *missing* one: a local copy that fails verification is evicted and replaced in place by the verifying peer's bytes, so a bit-rotted replica does not re-fetch on every read or sit one short of its durability margin until a peer is lost. (Eviction-before-replace is required because a content-addressed write of an id already on disk stores nothing.)

**Placement is colocation, not rendezvous.** A grain's blobs ride its *own shard replica set*, deliberately **not** a content-hash-rendezvous spread across the whole cluster. The trade is intentional: colocation keeps a grain's bulk data with its compute, so reads are leader-local and recovery comes from the grain's known replicas, at the cost of concentrating a grain's blob bytes on its R replicas (bounded the same way the grain's journal already is). A *cluster-shared* content store — rendezvous-placed, namespace-scoped, for cross-grain dedup, archival, or an external object-store tier — is the complementary **cold** tier of the DO hot/cold split (DO §6), off the grain's hot path and out of scope here (§16).

**Deferred, in lockstep with §7.6.** Today a grain's blobs reach a newly-added shard replica the same way its records do — by **recovery-on-access** (a verified peer-fetch), not by proactive streaming. When §7.6's "stream each grain's committed records and latest snapshot to a new replica before it counts toward quorum" is realized, it MUST stream the grain's blobs too, so the durability margin is actively restored after a membership change rather than only on demand. Range-verified streaming of one large blob against the BLAKE3 tree the id already roots (the Bao encoding) is a further deferred refinement (§16); v1 verifies whole blobs and a consumer chunks beyond a bound.

---

### 7.11 The workspace facet (physical)

**Status: implemented** (`ws.rs`).

The workspace facet gives a grain a **real, node-local directory** — the working set a shell, container, or microVM runs against, with no interposition and no typed-API translation. The directory is the live truth for the activation; durability comes from **capture**: after a command's side effects have landed on disk, the handler calls `ctx.ws().capture()`, which diffs the durable subtree against the committed index (stat-pruned, content-hash-verified) and stages one record per changed file — the file's bytes **inline**, plus one record naming the deleted paths. The staged records join the command's atomic batch (§7.12, **G19**): the command's reply, its events, and the workspace delta commit together or not at all. Replay applies the bytes byte-deterministically (**F1** holds on the bytes, exactly as the SQL facet's F1 holds on WAL frames — never on re-execution). An unchanged tree stages nothing and rides the §7.5 read path.

- **The physical discipline (G20/F4).** The directory mutates before durability — tools write it directly, between and during commands. On `Committed` the host keeps the materialization; on **any** other outcome it deletes the directory and steps down; the next activation rematerializes from the composite snapshot plus committed delta records. The directory is a rebuildable cache (§1), keyed under `GranaryConfig::data_dir` by **node and grain** — a materialization is activation-local, and under the simulator many nodes share one process, so a failover must never land two nodes' materializations of one grain on one path.
- **Checkpoints are the snapshot contribution.** At snapshot time every durable file is chunked into **content-addressed blobs** (§7.10) and the contribution is a small `path → chunk ids` manifest; unchanged files hash to blobs already stored, so a checkpoint uploads only new content — incremental by dedup. The manifests' chunk ids are the facet's root set (**F3**); §9 compaction then drops the delta records the checkpoint subsumes, which is what keeps inline delta bytes from accumulating in the journal. A snapshot taken while an in-flight tool has dirtied the directory fails (the snapshot must be the committed image, **G4**); a snapshot is only an optimization, and the idle-passivation snapshot runs quiesced.
- **Scope: files only, excluded trees, a cap.** Directories are implied by the files within them; empty directories, symlinks, and special files are not durable (matching what the sandbox tar paths preserve). Trees named in the exclusion list (`target/`, `node_modules/`, and kin) live in the directory but are never captured, journaled, or restored — regenerable caches, rebuilt rather than replicated. The durable subtree is capped (64 MiB, the sandbox tar bound); an over-cap capture fails as an application error with nothing staged, and the next successful capture diffs against the last *committed* index, so nothing is skipped. Ranged sub-records for files larger than a record bound, and lazy hydration for large workspaces, are the deferred refinements (§16).
- **Consumption.** The agentic harness is the worked consumer: its agent grain declares `Facets = (Ws,)`, hands `ctx.ws().dir_path()` to the sandbox provider at open, and captures in the command that journals each tool outcome — so a tool's result and the files it wrote are one quorum append. Root-driven blob repair (§7.10, lazy hydration and self-heal) is the consumer's `on_activate` conduct, re-fanning the live checkpoint chunks after membership changes.

---

### 7.12 The facet model: one substrate, many storage features

**Status: implemented.** The facet seam (`facet.rs`), the KV facet (§7.13), the workspace facet (§7.11), and the SQL facet (§7.14) are all in the crate, landed in the staging order this section closes with.

Durable Objects exposes several storage APIs to one object — a KV store, a SQL database, alarms — but implements **one** replicated substrate: every API layers over the same SQLite-WAL replication (DO §4.2). Granary adopts the same discipline over its own, deliberately two-part substrate (§7.10): ordered records and immutable blobs. A **facet** is a durable storage feature of one grain defined entirely by three answers, and nothing else:

1. what its **records** mean — the ordered mutations it contributes to the grain's journal;
2. what it keeps in **blobs** — bulk immutable bytes, referenced by id;
3. what it contributes to the **snapshot** — its checkpoint at a `Seq`.

A facet gets no replication path, no fence, and no recovery protocol of its own; those exist once, in the substrate (§7.2, §8, §9, **G14**). The facet model changes **nothing at or below the journal seam** (§7.3): `append`/`load`/`head`/`save_snapshot`/`load_snapshot` and the blob operations carry the same opaque bytes as today. Everything in this section happens in the host.

**Tagged records, one order, one barrier.** A multi-facet grain's records interleave in the grain's single `Seq` order, each carrying its facet's stable **tag**. All records a command produces — its events plus every facet's operations — append as **one atomic batch** (§6, §7.3), so a command that touches state, the KV map, and the database commits everywhere or nowhere (**G19**). This cross-facet atomicity is exactly what composing separate grains cannot give (§7.11). Replay dispatches each record to its facet by tag; a record whose tag the runtime does not recognize MUST abort activation rather than be skipped — a grain's history is never silently misread by a runtime missing one of its facets (**G19**). **Every** record wears the tag, including facet 0's on a grain with no declared facets: one uniform envelope, no second encoding path. Subscriptions (§7.9) are the grain's **event projection**: the typed sink decodes facet-0 records and skips other facets' — their seqs appear as gaps the subscriber's seq-reconciliation already absorbs (G16 holds per projection).

**Two facet classes.** The classes differ in where a mutation lives before the commit:

- A **logical facet** folds. Its records are semantic operations; a pure, deterministic function applies them to an in-memory form (**F1**, the generalization of **G2**), and the host folds only after durability — exactly **G1**. The event fold itself is *facet 0*, the primordial logical facet; the KV map (§7.13) is logical.
- A **physical facet** materializes. Its record is a captured physical delta, applied byte-deterministically on replay (**F1**): the SQL facet's WAL frames (§7.14), the workspace facet's changed-file bytes (§7.11), the disk facet's capture manifest with its block bytes in blobs (§7.15). Its live form (the database file, the directory, the block image) mutates locally *during* the handler; the disk image also mutates *between* handlers, the one departure, owned and bounded by §7.15. The G1 discipline inverts to **expose only after durability; discard on anything else** (**G20**). The inversion is sound because the materialization is a rebuildable cache (§1) and the §6 gates make un-durable state unobservable: the input gate admits no other command while the append is in flight, the output gate holds the reply, and a non-committed outcome destroys the only copy of the tainted state.

**Staging: the handler writes through the facet; the commit makes it real.** Each declared facet surfaces a `GrainCtx` accessor (§4.3). A logical facet's accessor stages operations in a **per-command overlay**: reads through it see committed-plus-staged, and at append time the host drains the overlay into the command's tagged records — folded into the facet's form only on commit, dropped for free on any other outcome. A physical facet's accessor operates on the materialization directly, inside one implicit transaction per command (§7.14). Both keep §4.2's split intact: committed, observable state changes only at the commit point. Two disciplines follow. *Staging is command-scoped*: a facet write outside a command handler (from `on_activate`/`on_passivate`) has no batch to join and is refused, never silently dropped or deferred. *Plan, then stage*: staged operations commit with the batch even when the handler's reply is an application error, so an accessor operation performs its fallible work (blob puts, scans) first and stages only on success — an operation that returns `Err` leaves no trace (the KV facet's spill and the workspace facet's capture are built this way, §7.13, §7.11).

**One composite snapshot.** The snapshot at seq *S* is the tuple of every facet's contribution at *S*, encoded as one `save_snapshot` payload: facet 0's encoded `State`, the KV map, the workspace checkpoint manifest, the SQL checkpoint manifest, the disk checkpoint manifest, the alarm deadline, the workflow memo. **G4** applies to the composite as a whole: a composite whose seq exceeds the committed head is ignored entire, and replay runs from `ZERO`. Bulk checkpoint bytes (SQL pages) go to blobs; the composite holds manifests of ids, keeping the snapshot record small.

**Blob liveness is unioned by the host.** Every facet reports its blob **root set** (**F3**): the workspace facet its checkpoint chunks; the SQL facet the pages of its live checkpoint manifests; the disk facet the blocks of its live checkpoint manifest plus the blocks its post-checkpoint capture records reference. The host computes the grain's live set as the union across facets plus the application's own roots, and only the host issues `retain_blobs`; a facet MUST NOT sweep the shared area itself, so one facet's GC can never drop another facet's bytes.

**Compile-time composition.** A grain declares its facets in its type — an associated facet set on `Grain`, a tuple whose unit form `()` declares none — and each accessor is gated by a containment bound, so `ctx.sql()` on a grain without the SQL facet does not compile (the **G10** discipline applied to storage). The declaration statically fixes the grain type's record-tag registry, which is what makes the unknown-tag rule (G19) checkable.

**The facet contract.** Every facet, built-in or future, owes the same obligations, verified generically under §14 simulation:

| | Obligation |
|---|---|
| **F1** | **Deterministic interpretation.** A logical facet's fold is pure and deterministic (G2 generalized); a physical facet's delta application is byte-deterministic. |
| **F2** | **Snapshot equivalence.** Restoring the facet's snapshot contribution at *S* and replaying its records in (*S*, head] yields the same form as interpreting the full record history. |
| **F3** | **Complete roots.** The facet's reported root set covers every blob its restored form can reference; nothing it can later `get` is outside it. |
| **F4** | **Discard safety** (physical only). Discarding the materialization at any moment and rehydrating from the composite snapshot plus records reproduces the identical form. |

**Apps are libraries over facets, never facets.** A git-style artifact store is refs in the KV facet plus objects in blobs (git objects are content-addressed and immutable, which the blob primitive was built for, §7.10), and a work queue is events or KV. A feature earns facet-hood only when it needs **host participation**: a transaction lifecycle and discard (SQL), a per-command overlay (KV), a timer that re-activates the grain (alarms, §7.16, whose *durable* half is trivially a logical facet's records). Everything else layers above, in user code, where it needs no spec.

**The seam stays internal.** The facet trait is not public API: the built-in set (facet 0, KV §7.13, workspace §7.11, SQL §7.14, disk §7.15, alarm §7.16, workflow §7.17) proves the seam. Publishing it for out-of-crate facets is deferred (§16) until the tag registry and cross-version compatibility rules have settled under real use.

**Staging plan (all landed).** Implementation arrived in dependency order, each stage shippable alone: (1) the facet seam (tagged records, the staging drain, the composite snapshot, unioned roots), re-expressing facet 0 through it; (2) the KV facet, the smallest logical facet, proving overlay staging and snapshot composition end to end; (3) the SQL facet, the first physical facet, adding the capture seam and checkpoint machinery (§7.14); (4) the workspace facet (§7.11), first landed as a logical filesystem-metadata facet and re-realized once the physical seam (3) was proved, deleting the metadata fold and its directory-reconciliation bridges; (5) the alarm facet and its per-shard index (§7.16), the first facet paired with a runtime driver; (6) the workflow memo facet (§7.17), layered on the fold and the alarm.

---

### 7.13 The KV facet (logical)

**Status: implemented** (`kv.rs`).

The KV facet gives a grain an ordered map of small values — configuration, refs, cursors, indices — without designing an event vocabulary for it: `ctx.kv()` offers `get`, `put`, `delete`, and ordered prefix listing (`keys`/`list`), staged per command (§7.12) and read-your-staged-writes within the handler.

- **Records.** `Put(key, value)` and `Delete(key)` under the kv tag, folded into an ordered map; the fold is trivially pure (**F1**).
- **Snapshot.** The encoded map is the facet's composite-snapshot contribution (**F2**). Keys and inline values are bounded by configuration (the Durable Objects KV bounds are the reference point); the facet transparently **spills** a value above the inline bound into the grain's blob area, storing the `BlobId` in the map and fetching it back on `get`, verified by content (**G17**). Spilled ids are the facet's root set (**F3**), so spilled bytes are reclaimed when their keys are deleted, by the host's unioned sweep (§7.12).
- **Why native, not sugar over SQL.** Durable Objects backs its KV API with the object's SQLite database; the facet model permits that convergence later without API change. Granary keeps KV a native logical facet first: it adds no C dependency, holds its form in memory, runs unchanged under deterministic simulation (§14), and serves grains that want a map without a database file. A grain that needs range queries with predicates wants the SQL facet, not a richer KV.

---

### 7.14 The SQL facet (physical)

**Status: implemented** (`sql.rs`, behind the `sql` cargo feature). This is the store §16 once deferred as an "alternative record interpretation," realized as a facet.

The SQL facet gives a grain a **private, on-disk SQLite database** — the Durable Objects storage model itself (DO §2.3, §4.2). The handler runs ordinary SQL through `ctx.sql()` against a local file: reads are leader-local and replication-free (§7.5), writes are transactional, and nondeterministic SQL (`random()`, `datetime('now')`, autoincrement) is fine, because replication is physical, never re-execution. (A *statement-sourced* SQLite grain — SQL text as events, folded by re-execution — remains expressible with plain event sourcing and no facet, at the cost of determinism discipline and replay-by-re-execution; it is an application pattern, not this facet.)

- **One command, one transaction, one record.** `ctx.sql()` is valid for the duration of a command and wraps it in one implicit transaction; interactive transactions spanning commands do not exist (the DO restriction). At local commit the host captures the transaction's **WAL frames** — page number, page bytes, database size after commit — and encodes them as one tagged record in the command's atomic batch (§7.12, **G19**). A read-only transaction produces no frames and no record, riding the §7.5 read path. A transaction's frame bytes are bounded by configuration (one record must fit the replicator's message bounds); a bulk load splits into multiple commands. Idempotency needs no new mechanism: a frame record occupies one `Seq` slot, so a timed-out append that lands late is applied once, at its slot, on recovery (§7.2).
- **The physical discipline (G20/F4).** The database file mutates at local commit, before durability — the class-defining inversion (§7.12). On `Committed` the host keeps the materialization and releases the reply; on **any** other outcome it deletes the database file, folds nothing, and steps down; the next activation rematerializes from the composite snapshot plus committed frame records. The local file is a cache, so `PRAGMA synchronous=OFF` is sound: durability is the quorum ack, never the local disk (DO §4.2 — the local disk can be lost entirely).
- **The capture seam.** After a local commit the host MUST obtain exactly that transaction's frames, in order. The reference mechanism is **WAL tailing** (autocheckpoint disabled, the host owning checkpoint timing — the Litestream-proven approach); a VFS shim intercepting `-wal` writes is the deferred upgrade (§16), which also enables lazy page hydration on activation.
- **Checkpoints are the snapshot contribution.** Periodically (a frames/bytes trigger, configuration like `snapshot_every`) the host checkpoints the WAL into the database file (`TRUNCATE`), chunks the file into **page-aligned, content-addressed blocks**, `put`s them as blobs (§7.10), and contributes a **manifest** — page size, page count, ordered `BlobId` list — to the composite snapshot (§7.12). Unchanged pages hash to blobs already stored, so a checkpoint uploads only dirty pages: incremental by dedup, granary's analogue of SRS's WAL archiving (DO §4.2). The live manifests' pages are the facet's root set (**F3**); §9 compaction then drops the frame records the checkpoint subsumes.
- **Rehydration.** Composite snapshot → manifest → fetch pages, verified by content (**G17**) → materialize the file → apply frame records after the snapshot seq (byte-deterministic, **F1**) → serve. The quorum barrier (`head`, §9) is unchanged. There is **no out-of-band schema hook**: DDL is a journaled write like any other, so schema setup is idempotent `CREATE … IF NOT EXISTS` at the top of a writing command — replay reproduces it physically, and a lifecycle hook that mutated the database outside a command would produce frames no record captures (which is why out-of-command SQL is refused outright).
- **Dependencies and simulation.** SQLite (bundled `rusqlite`) sits behind the `sql` cargo feature, keeping the base crate dependency-light. The facet introduces real file I/O into the grain hot path — the database file lives under `GranaryConfig::data_dir` (a rebuildable cache, §1, keyed by the grain name's content hash); under deterministic simulation (§14) tests point `data_dir` at a temp directory — frame *production* is real SQLite, while frame *application* and everything around it stays seed-driven. Enforced per-transaction frame caps and a max database size remain **deferred** configuration (today the bounds are the replicator's message limits and the node's disk).

---

### 7.15 The disk facet (physical)

**Status: specified, not yet in the crate.** The physical seam it rides is the SQL facet's (§7.14), unchanged: capture at a quiescent point, checkpoint into content-addressed blobs, discard on non-commit, rehydrate then replay. The disk facet is that machinery with **dirty blocks** for WAL frames and a **raw image** for the database file, so this section is short by design: it states the substitutions and the two disciplines that differ. Capture bytes travel as blobs, and capture rides an explicit command.

The disk facet gives a grain a **raw block image**: a fixed-size disk a microVM mounts as its rootfs, the whole-machine analogue of the workspace facet's directory (§7.11). Where the workspace facet captures changed *files* and the SQL facet captures WAL *frames*, the disk facet captures changed **blocks**. A session's dirty set can run to gigabytes, far past the replicator's message bounds, so the block *bytes* never ride the record: a **capture** `put`s each dirty block as a content-addressed blob (§7.10) and stages exactly **one** record, a **capture manifest** carrying the image size and the ordered `(block index, BlobId)` list, which joins the command's atomic batch (§7.12, **G19**). One capture, one record, one commit: a capture is atomic by construction, and a crash can never leave a committed *prefix* of one. The blob `put`s precede staging (*plan, then stage*, §7.12), and blobs orphaned by a failed commit are unreferenced by any root and swept by the host's unioned GC. Replay fetches each manifest's blocks, verified by content (**G17**), writes each at its offset, and truncates to the recorded size, byte-deterministically (**F1**). An unchanged image stages nothing and rides the §7.5 read path.

**Capture rides a command.** §7.12's staging rule is unchanged: nothing is staged from `on_activate`/`on_passivate`. Because the guest writes the image *between* commands, the consumer drives every capture through an explicit **capture command**, a host-delivered command like the alarm's `AlarmDue` (§7.16), passing through the ordinary §6 barrier: input gate, one atomic batch, output gate. A consumer that must capture before a lifecycle boundary (idle hibernation, a planned migration) runs its capture command to completion and only then permits the boundary; the machine spec's quiescent points are exactly this (machine §4). `can_passivate` refuses eviction while the live image holds uncaptured writes, so the §10 idle path cannot silently strand a dirty image.

The facet is the substrate's answer to a durable *virtual machine*. The harness's Native tier (sandbox §3.4) confines a microVM but persists only the workspace tar (sandbox §3.5), so a guest's installed packages, home directory, and `/tmp` die with the activation. A grain declaring `Facets = (Disk,)` persists the guest's **entire rootfs** as a facet over the same two primitives, with no new replication path. Its worked consumer is the persistent machine ([`machine-spec.md`](machine-spec.md)), which owns *when* the image is captured: a machine's command unit is a whole session, not a single message (machine §4).

- **The physical discipline (G20/F4), with one named departure.** The image mutates before durability: the guest writes it live, across a session, not one command. On `Committed` the host keeps the materialization; on any other outcome it deletes the local image, folds nothing, and steps down, and the next activation rematerializes from the composite snapshot plus committed capture manifests. The image is a rebuildable cache (§1), keyed under `GranaryConfig::data_dir` by node and grain, as the SQL database file is. One departure from the class: for the other physical facets the §6 gates make un-durable state *unobservable*; the disk's un-durable writes **are** observable, to the guest and to whoever is attached to it, before the next capture. The grain-level observables (records, replies, §7.5 reads) still honor the gates; the wider exposure is the consumer's to bound and name (the machine's session-grained barrier, machine §4, its invariant M3).
- **The capture seam.** The host MUST obtain exactly the blocks that changed since the last capture, read at a **quiescent point** (the guest paused or stopped, machine §4), so the read sees a stable image, as the SQL capture reads a settled database after the handler returns. The host never captures a *running* guest's concurrently-written image; the consumer quiesces the guest first. Dirty tracking is the **host's own**, not the VMM's: Firecracker's diff-snapshot tracking covers guest *memory* pages and exposes no block-device write bitmap, so the host, which owns the backing file, tracks writes itself. The reference mechanism is a **copy-on-write overlay per capture epoch**: the guest's block writes land in a sparse overlay whose allocated extents *are* the dirty set; a capture drains the overlay and resets it. A conforming alternative is **content-hash pruning**, chunking the image and diffing block hashes against the last manifest (the workspace facet's stat-prune at block granularity), at the cost of a full scan per capture. The obligation is the property, not the mechanism.
- **Checkpoints are the snapshot contribution.** At snapshot time the host chunks the image into **block-aligned, content-addressed blocks** (the SQL facet's page-aligned blocks at a coarser granularity, e.g. 1 MiB), `put`s them as blobs (§7.10), and contributes a **manifest**: block size, block count, ordered `BlobId` list. Unchanged regions hash to blobs already stored, so a checkpoint uploads only dirty regions, incremental by dedup as the SQL and workspace checkpoints are; and because capture records already left their block bytes in blobs, a checkpoint after a capture re-uploads nothing. The live checkpoint manifest's blocks, plus the blocks referenced by capture records after it, are the facet's root set (**F3**); §9 compaction then drops the capture records the checkpoint subsumes.
- **Rehydration.** Composite snapshot → checkpoint manifest → fetch blocks, verified by content (**G17**) → materialize the image → apply capture manifests after the snapshot seq, fetching their blocks the same way (byte-deterministic, **F1**) → boot the microVM against it. The quorum barrier (`head`, §9) is unchanged. Lazy block hydration (faulting blocks in from blobs on first guest access, so a large image activates without materializing whole) is the deferred refinement (§16), the disk analogue of the SQL capture VFS.
- **Bounded, like every physical facet.** The image is fixed-size (a configured maximum); a capture manifest is a small record (indices and ids, never bytes), each block blob is bounded by the fixed block size, and the checkpoint obeys the same blob bounds the SQL and workspace facets obey. A guest cannot make the host materialize an unmetered allocation (the sandbox §3.2 stance, applied to the image), because block indices are bounded by the fixed image size and block bytes by the fixed block size.

---

### 7.16 The alarm facet (logical)

**Status: implemented** (`alarm.rs`, `alarm_index.rs`; the crate's doc comments predate this section and still cite §16).

The alarm facet gives a grain a **durable timer**: at most one pending deadline (the Durable Objects model, DO §5), armed or cancelled through `ctx.alarm()` and fired as a callerless invocation of the grain's `on_alarm` handler. It is the basis for retries, timeouts, batch flushes, and the workflow facet's `sleep` (§7.17).

- **Storage is a trivially logical facet.** `Set(due)` and `Clear` records under the alarm tag fold into a single `Option` deadline (last write wins); the folded form is the source of truth for whether and when the grain fires. Arming and cancelling stage into the command's atomic batch like a KV write (§7.12); the encoded form is the snapshot contribution; the root set is empty.
- **Firing is the runtime half, through the same barrier.** After each commit and on activation the host reads the folded deadline and arms an **epoch-guarded in-activation timer**; when it elapses, the host delivers itself a callerless `AlarmDue` command that runs `on_alarm` through the ordinary §6 barrier (input gate, one atomic batch, output gate), so an alarm's effects are as durable and as fenced as any command's. The epoch guard makes a superseded timer's late `AlarmDue` a no-op, a cancellation without a race.
- **Consume-on-fire.** The fired deadline's `Clear` joins the same batch as `on_alarm`'s own records, so the alarm clears atomically with its effects unless the handler re-arms (last write wins in the shared stage): a re-arming handler chains, a consumed alarm stops, and a failed commit leaves the deadline pending to fire again.
- **A pending alarm vetoes hibernation.** A grain owing a wake stays resident (`can_passivate` refuses), so a resident grain always fires on time. The gap this leaves is **failover**: a forced step-down passivates the grain, and a callerless deadline would otherwise sleep until the next access. The per-shard **alarm index** closes it: one small index grain per (grain type, shard) holds a `GrainName → due` map, registered on every alarm change; after a leadership change the alarm **driver** reads the index for its new shards and re-activates each grain owing a wake, whose activation re-arms its own timer (**G3**). The index is a *hint*, the grain's own facet the source of truth, so a stale or missing entry reconciles on the next activation, never a lost or double fire. (Letting an alarmed grain hibernate, woken by the driver at its deadline, is the deferred refinement, §16.)

---

### 7.17 The workflow facet (logical)

**Status: implemented** (`workflow.rs`).

The workflow facet gives a grain **durable step memoization**: the Cloudflare-Workflows-style `step`/`sleep`/`retry` pattern, layered on the event fold, the alarm facet (§7.16), and a grain's self-driving loop (the agentic harness's `advance` loop is the reference consumer, harness §3).

- **The durable core is a memo.** A workflow is a re-entrant decision the grain re-drives after every commit. Each **step** runs an external effect once; the facet journals its result under the workflow tag and folds it into a `step id → bytes` map. On any later drive (a re-activation, a failover) a completed step resolves from the map instead of re-running, so effects are at-most-once across crashes. Step ids are the workflow's stable call-site ordinals, deterministic across re-drives and replay.
- **`sleep` is a step whose effect is an alarm** (§7.16); `retry` is a step that re-launches after an alarm-backed backoff.
- **What the grain still wires.** Launching a step's effect off the command path and self-`tell`ing its result back is grain-specific (the effect's type is), so the facet supplies the durable half (the memo's `result`/`record`) plus a generic `StepDone` command that journals a returned result; the "already launched this activation" guard is ephemeral activation state. A linear `async` DSL over these is deferred (§16).

---

## 8. The single-writer fence

A grain must have one writer. The fence is the shard's **leadership term**: one leader per term, and a per-grain append that carries that term as its fencing token.

- **One leader per term.** A shard's leader-election group is a small Raft group (§7.1) that elects at most one leader per term and records the shard's replica set. The leader is the single writer for every grain in the shard's range and hosts their activations (§5.2). Electing a leader is the *only* agreement round, and it happens on failover (§8.3), never per write.
- **Every append carries the term.** A grain write is a per-grain quorum append (§7.2) stamped with the leader's term. Each replica persists the highest term it has acknowledged for the shard and refuses any append stamped lower. A write commits only while a quorum still recognizes its term, the fencing-token discipline with the shard term as the token.
- **A deposed leader cannot commit.** A new leader raises the term; the old leader's in-flight appends are then refused by every replica that has seen it, reach no quorum, and return `NotLeader` or `Unavailable` (§7.3). Two nodes that briefly both believe they lead the shard cannot both commit, because a quorum acknowledges only one term, so the minority's writes never land and the grain's record never forks (invariant **G1**).
- **A new leader recovers each grain's head from a quorum.** Because durability is a per-grain quorum append and *not* a shared replicated log, a fresh leader does not inherit every grain's latest records the way a Raft leader inherits its log. Before serving a grain it recovers that grain's head and tail from a write quorum of the shard's replicas (the rehydration barrier, §9). The recovery is a read-repair, not a bare read: the leader takes, for each `Seq` slot, the record carried under the **highest term** any replica in the quorum holds (a partitioned replica MAY hold a stale-term record at a slot a higher term later won, so slot occupancy alone does not decide it), and **writes the recovered tail back** to a quorum under its own term before serving, so the head it adopts is itself quorum-durable and no later recovery can regress it. By quorum intersection, any record acknowledged on a quorum is present, under its committing term, on at least one replica the new leader reads, so no acknowledged write is lost (invariant **G14**). This is G14's basis: **quorum read-repair of a per-grain head**, in place of leader-completeness over a shared log.
- **The host steps down cleanly.** On `NotLeader` the host passivates the grain and the caller retries against the new leader (§5.4, §6 step 5). The host MUST NOT keep serving a grain whose shard it no longer leads.

Because leadership is per shard (§7.1), the cost of "one leader per object" is paid once per shard, not once per grain, and a stale activation is fenced by the term every append must carry.

### 8.1 Why a term, not a clock

The fence is a logical term, not a wall-clock lease. Leadership is a quorum fact: a leader that pauses and wakes finds a quorum has moved to a higher term and refuses its appends, whatever the clock skew, so no time-based expiry is needed for *write* safety. A clock lease returns only as the deferred optimization for linearizable *reads* (§7.5, §16), where the goal is to skip a quorum round, not to fence writes. The per-grain `Seq` orders a grain's own records and aligns its snapshots (§9); it never arbitrates writers; the term does.

### 8.2 The actor control plane, the shard maps, and the shard leadership groups, side by side

Three consensus layers operate, at very different rates, and none touches grain data:
- the **actor control plane** (one leader-based group, actor §9.4.3) owns cluster membership and seeds the map groups' voters, changing on cluster events;
- the **shard map** per grain type (one group per type, §7.6) owns the allocation (which nodes form each shard and which key range it covers), changing on splits, merges, reconfiguration, and membership;
- the **shard leader-election groups** (O(shards) groups) own placement (which replica leads each shard and under which term), changing on failover and reconfiguration, never on a grain write.

The grains' **data** sits under none of them: it is off consensus entirely, a per-grain quorum append (§7.2) carrying the shard term. Every group derives a distinct, non-colliding group id (the membership control group is id 0; a shard map uses a reserved shard index; shard leader-election groups hash `(grain_type, index)`), so none is on another's hot path. The total group count is `O(shards) + O(grain types) + 1`, bounded by split/merge (§7.7) and the number of hosted types, never O(grains), and never one per write (**G7**).

### 8.3 Failover

When a shard's leader fails or is partitioned away, the shard's replicas elect a new leader by ordinary Raft over the leader-election group, raising the term. The new leader holds the shard at once, but, unlike a shared-log leader, it does not yet hold the grains' records: it recovers each grain's committed head and tail from a write quorum of the shard's replicas as that grain is activated (§8, §9, §10), lazily on first access. The window between the old leader's loss and the new election is the shard's **failover window**, during which calls to that shard's grains fail fast (`NotLeader` then a retry, or `Unavailable`, §11) rather than block. No acknowledged write is lost: acknowledgment required a quorum, and quorum intersection guarantees the new leader's recovery read sees it (invariant **G14**).

---

## 9. Snapshots and compaction

Replaying a long event history on every activation is wasteful, so a grain periodically snapshots its folded state.

- **Snapshot policy.** After a commit, the host MAY persist a snapshot `(head, State)` for the grain via `save_snapshot` (§7.3). The trigger (every *N* events, every *T* of growth) is configuration, not part of the model. Only the shard leader writes snapshots, so a deposed leader cannot (its `save_snapshot` returns `NotLeader`).
- **The rehydration barrier (§10).** Before reading a grain's head, the leader MUST recover it from a write quorum of the shard's replicas; it does not hold the grain's records merely by winning the shard (§8). This recovery is `head` (§7.3): it returns the quorum-confirmed head and backfills any records the leader is missing, so a freshly-activated grain never rebuilds from a stale or partial local view and then folds onto a short head or serves stale reads. It is a no-op on the `Local` tier, whose single store *is* the committed state.
- **Rehydration (§10).** On activation, after the barrier, the leader loads the grain's latest snapshot `(s_seq, s_state)`, itself recovered from a quorum on the `Quorum` tier (`load_snapshot`, §7.3), and the records after `s_seq` up to the recovered head, replaying them and folding via `apply` (§4.1) to reach the head. The head is set **only** from journal/snapshot returns, never trusted from a prior activation's memory (invariant **G3**).
- **A snapshot MUST NOT shorten the effective log.** If a snapshot's `s_seq` exceeds the grain's committed head, the host MUST ignore it and replay from `ZERO`. The journal is always the authority; the snapshot is only an optimization (invariant **G4**).

Compaction — truncating a grain's record prefix once a snapshot subsumes it — is a per-replica operation of the Replicator, below the seam: a replica drops the records up to a snapshot's `Seq` when it stores that snapshot, advancing a per-grain **base** (the file-backed store then rewrites its on-disk log to a single checkpoint, reclaiming the space). Its safety rests on two facts, so it need not wait for the snapshot to be durable on a quorum. First, a snapshot is only ever taken at the **committed head** (§9), so every record it subsumes was already quorum-committed. Second, a fresh leader recovers a grain's head by taking the **highest snapshot `Seq` any replica in its read quorum holds** as the head base and merging the records above it (§8). A replica has dropped a record prefix only if it holds a snapshot covering that prefix, so any recovery quorum finds, for every committed record, *either* the record itself *or* a snapshot that subsumes it — never neither. Quorum intersection thus still loses no acknowledged write (invariant **G14**). Per-grain snapshots let a grain's history compact independently of its shard-mates.

---

## 10. Lifecycle: activation, migration, hibernation

A grain's lifecycle, from the host's view, is a loop of:

```
activate → rehydrate (snapshot + replay, §9) → on_activate → serve commands → idle → on_passivate → (snapshot) → stop
```

- **Activation** is triggered by the first message for the name reaching its shard leader's gateway (§5.3). The leader rehydrates the grain (§9), runs `on_activate`, then serves. Activation takes no consensus: no election, no agreement round; on the `Quorum` tier it costs one quorum-recovery round-trip to confirm the grain's head (§9), and on the `Local` tier it is fully local.
- **Migration** follows shard leadership. When the shard's leader changes (§8.3), the grain's activation moves to the new leader and rehydrates there. In-memory state is never preserved across the move; the journal rebuilds it.
- **Hibernation (deactivate-on-idle).** After an idle interval the leader MAY, *if the grain's `can_passivate` permits it*, run `on_passivate`, snapshot to bound the next replay, and `ctx.stop()`, dropping the grain's in-memory state. (`on_passivate` cannot mutate state, §4.3, so it runs before or after the snapshot indistinguishably; the implementation runs it first.) A grain that vetoes eviction (`can_passivate` returns false, e.g. an autonomous grain with a live run) is left running and the idle check rescheduled. The gateway prunes the name from its activation table (it watches its hosts via death watch, actor §12). The next message re-activates and rehydrates. Hibernation reclaims memory; persisted storage survives; in-memory state was only ever a cache (§1).
- **Forced step-down.** When leadership moves (§8) or the shard goes unavailable (§11), the activation deactivates involuntarily: it runs `on_passivate` (to release non-durable per-activation resources) but takes **no snapshot**, since the journal may be unwritable, then emits `Passivated` and stops. The journal is the authority; the next access rehydrates.
  - **Default and tuning.** `idle_after` SHOULD default to about **10 seconds**, matching the Durable Objects eviction window (DO §5), because reactivation is cheap: at most a quorum head-recovery plus a snapshot-bounded replay (§9). To avoid thrashing when a grain is accessed just slower than the timer, an implementation SHOULD apply a small minimum residency or jitter so a barely-idle grain is not evicted and reloaded repeatedly.
- **Eviction races.** If a command reaches the gateway for a name whose host has stopped but whose `Terminated` has not yet pruned the table, the host `ask` returns `CallError::DeadLetter`; the gateway MUST treat that as "reactivate" (drop the stale entry and activate afresh), bounded to avoid a loop.
- **Subscriptions are ephemeral activation state.** A grain's record sinks (§7.9) live only in the activation, never in the journal, and are dropped on every deactivation path — hibernation, migration, and forced step-down alike. Subscribers re-establish them by re-subscribing against the current leader and backfilling from their last seq (§7.9); they do not preserve in-memory state across the move any more than the grain itself does (**G3**).

Durable **alarms** (§7.16) intersect this lifecycle in two places: a pending alarm vetoes idle hibernation (the grain stays resident to fire on time), and after a failover the per-shard alarm index re-activates each grain owing a callerless wake, which no message would otherwise touch.

---

## 11. Failure model and the node-down cascade

Granary inherits the actor failure model (actor §8) and adds the durability outcomes. The blast radius of a failure is the **shard**, not the cluster and not the single grain.

When a node fails (actor §8.1):
1. **Shard leadership.** Every shard the node led re-elects a leader among its remaining replicas (§8.3). The shards it merely followed continue under their existing leaders.
2. **In-flight callers.** Every pending `ask` to a grain on the failed leader completes with `GrainError` wrapping `CallError::Unreachable` (actor §7.2, §8.1), never a hang; the caller retries against the new leader.
3. **Watchers.** A grain MAY be watched as any actor; watchers receive `Terminated { NodeDown }` (actor §12) for the activation.
4. **Re-activation.** The next message re-activates each affected grain on its shard's new leader, which rehydrates from the log (§9). No acknowledged write is lost (invariant **G14**).

**Quorum loss is unavailability, not a fork (CP).** If a shard cannot reach a quorum, whether its leader-election group cannot elect a leader or a grain's Replicator cannot reach a write quorum, it commits nothing. `append` returns `Unavailable`, and every grain in that shard pauses writes (§6 step 6): effects are not applied, the reply is `GrainError::Unavailable`, and the affected activation steps down (the outcome is ambiguous, since a timed-out commit MAY land later, §7.2/§7.3, so the in-memory head is no longer trusted; the next access rehydrates from the journal). Callers retry or fail over. The shard does not fork, because a minority cannot commit: the leader-election group elects no leader on the minority side, and any stale-term append is refused by the quorum (§8). Only that shard's grains are affected; the rest of the cluster serves normally. Reads remain read-your-leader (§7.5): until the check-quorum read lease of §7.5/§16 exists, an isolated minority leader cannot fence itself and MAY serve a stale read, though it can commit nothing; with that lease, a leader holding a valid one MAY keep serving linearizable reads.

---

## 12. Error model

Grain calls surface two failure layers, kept distinct exactly as the actor framework keeps `CallError` and `M::Reply` apart (actor §14):

```rust
pub enum GrainError {
    Call(CallError),       // transport/system failure reaching the activation (actor §14.1),
                           //   including CallError::Unhandled for an unregistered manifest (§5.5)
    NotLeader(NodeId),     // leadership moved; the runtime retries against the hint, surfacing this only if retries are exhausted
    Unavailable(String),   // the grain's shard cannot reach a quorum; the write did not commit (§11)
}
```

- **Application errors** the handler deliberately produced live **inside `M::Reply`** (e.g. `Result<T, E>`, §4.2), never in `GrainError`.
- `NotLeader` is normally absorbed by the runtime's bounded redirect (§5.4); it surfaces to the caller only when retries are exhausted. `Unavailable` is a real durability outcome the caller must handle.
- `GrainError` MUST be exhaustive at the public API, so callers handle partial failure explicitly (actor §14, "define errors out of existence": keep only the real ones).

---

## 13. Security and observability

- **Security.** Granary adds no transport; it inherits mutual-TLS associations, the handshake allowlist, and the deserialization allowlist (the grain dispatch registry, §5.5) from actor §15. The gateway MAY consult an `Authorizer` per `(peer, GrainName, manifest)` before activating or dispatching, as actor §15.4 gates `deliver`.
- **Observability.** Granary emits, on the actor framework's single extensible `Event` stream (actor §16), the grain-lifecycle events `Activated`, `Rehydrated { from_snapshot, replayed }`, `Committed { seq }`, `Snapshotted { at }`, and `Passivated`. Every variant also carries the `node` that hosts the activation (its shard's leader, §5.2) and the grain `name`, which is what makes the per-node activation guarantee (**G6**) expressible as a continuous checker over the stream. The shard events `LeaderChanged { shard }`, `ShardSplit`, and `ShardMerged` are **deferred**: leadership is observed through the system seam rather than emitted, and split/merge (§7.7) is not yet implemented. These drive both operator tooling and the simulator's invariant checks (§14). Metrics SHOULD include per-shard commit latency and log size, per-node active-grain count, shard count, and leadership changes. Record subscriptions (§7.9) add **no** content-bearing event: they reuse `Committed { seq }` as their commit signal and deliver the records out of band, keeping the journal the one place a grain's content lives.

---

## 14. Testability and deterministic simulation

Granary is testable by the same deterministic simulation as the actor framework (actor §18): a whole cluster of grains runs in one process, on one logical thread, over virtual time, network, and randomness, so a single seed reproduces a run exactly.

- The journal (§7.3) is a **simulation seam**, like `Transport`, and so are the parts below it. The shard **leader-election group** reuses the actor framework's Raft (the leader-based control plane's implementation, actor §9.4.3), so simulation drives the real consensus code, not a model of it; the **Replicator** (`Local`/`Quorum`) is exercised directly on the per-grain quorum-append path.
- Reusing the framework's seams (`Clock`/`Entropy`/`Spawner`/`Transport`, actor §4.6, §7) means simulation runs the real host, gateway, shard, and rehydration code.
- Fault injection MUST be able to produce: a shard **leader** crash and election mid-write (to exercise the term fence and quorum recovery, §8); **leader-election loss** (no leader elected) and **per-grain write-quorum loss** as distinct routes to `Unavailable` (§11); a stale-term append from a deposed leader racing a new election (to exercise the fencing token, §8); a timed-out append that commits late, re-read on the next activation (to exercise idempotency by `Seq` slot without a dedup token, §7.2); eviction mid-command (to exercise rehydration and the output gate, §6, §10); and a shard split under concurrent writes (to exercise §7.7). A no-fault run is the simplest case and MUST pass.
- For **record subscriptions** (§7.9), fault injection MUST be able to produce: a **leader move mid-stream** (the subscriber re-subscribes against the new leader and backfills, reconstructing the sequence with no gap or duplicate); a **slow sink** whose buffer overflows (dropped, then re-subscribed and backfilled); **hibernation and reactivation** under a live subscription; and a **timed-out append that commits late** (delivered or backfilled once, at its slot). In every case the sink's seq-reconciled sequence MUST equal what `load` to the head returns (**G16**).

---

## 15. Invariant catalogue and conformance

These invariants appear as MUSTs above; collected here, they are the contract a conforming implementation verifies, each holding even under the faults of §14. They are checked the way actor §18.5/§18.6 prescribe: continuous checkers over the §13 event stream for safety properties, targeted tests for the rest, compile-fail for type-safety.

| | Invariant | Defined in |
|---|---|---|
| **G1** | **Single writer per grain.** Only the shard leader appends; the leader-election group elects one leader per term and every append carries the term as a fencing token, so a deposed leader reaches no quorum and a grain's record never forks. A commit advances the grain's head by exactly the batch appended; the host folds only such a contiguous commit and steps down on any other outcome. | §6, §8 |
| **G2** | **Deterministic fold.** `apply` produces identical state on live commit and on replay from any snapshot/journal prefix. | §4.1 |
| **G3** | **GrainJournal is the source of truth.** `head` and state derive only from journal/snapshot returns; in-memory state is never trusted across activation. | §1, §9 |
| **G4** | **Snapshot never shortens the log.** A snapshot seq beyond a grain's committed head is ignored; replay always reaches the true head. | §9 |
| **G5** | **Reply iff durable.** A command's reply is released, and its events folded, only after the entry commits; a `NotLeader`/`Unavailable` outcome yields an error, no fold, no success reply. | §6 |
| **G6** | **Exactly-once activation per node.** The serial gateway activates a name at most once; concurrent requests find the same host. | §5.3 |
| **G7** | **Bounded consensus groups.** The cluster runs `O(shards) + O(grain types)` Raft groups (one **leader-election group** per shard plus one shard-map group per type, §7.6), kept bounded by split/merge, never O(grains) and never one, with **no consensus on the data path**, which is a per-grain quorum append (§7.2). | §7.1, §7.2, §7.6, §7.7 |
| **G8** | **Activation without consensus.** Activating or hibernating a grain touches no consensus group: no election, no agreement round (on the `Quorum` tier it pays at most one quorum head-recovery round-trip, §9, never a consensus round); only shard membership/leadership/split touches a consensus group. | §5.2, §7.8, §10 |
| **G9** | **Control plane off the data path.** The shard map changes only on cluster events; no grain write or activation contacts it. | §7.6 |
| **G10** | **Type-safe calls.** A command a grain has no `GrainHandler` for does not compile. | §4.3 |
| **G11** | **CP under partition.** A shard that cannot reach a quorum pauses its grains' writes (`Unavailable`) and never forks; other shards serve. | §11 |
| **G12** | **Hibernation round-trip.** A grain evicted when idle re-activates with identical state via snapshot + replay; no acknowledged write is lost. | §9, §10 |
| **G13** | **Location transparency.** A call to a local versus remote grain produces observably identical replies and ordering. | §5.4 |
| **G14** | **Lossless failover.** A new shard leader recovers each grain's committed head from a write quorum on activation; by quorum intersection no acknowledged write is lost across a leadership change. | §8, §8.3 |
| **G15** | **Split/merge safety.** A grain is writable in exactly one shard at any time; a split or merge transfers the committed prefix atomically and loses or duplicates no write. | §7.7 |
| **G16** | **Subscriptions are observational and lossless-by-seq.** Every committed record is delivered to a live sink or left as a gap the sink closes by `load`; subscription delivery never gates a commit, never forks state, and never advances a grain's head. By seq reconciliation a sink reconstructs the exact committed sequence regardless of drops, duplicates, reordering, moves, or hibernation (push ⊆ `load`). | §7.9 |
| **G17** | **Blob address integrity.** `ctx.blobs().get(id)` returns bytes whose BLAKE3 hash equals `id`, or an error; it never returns wrong or corrupt bytes. Verification is on the read path, after any network transfer; on the `Quorum` tier a corrupt copy falls through to a verifying replica. | §7.10 |
| **G18** | **Blob durability, idempotence, and grain-scoped reclamation.** A blob `put` is acknowledged only once stored on a write quorum of the grain's replicas (one local copy on `Local`), always including the leader, with no consensus on the path; equal content under a grain stores once; the blob survives the loss of a minority of replicas. Reclamation (`gc`/`destroy`) is grain-scoped, monotonic, and idempotent, and never resurrects a referenced blob. | §7.10 |
| **G19** | **Facet atomicity and interpretation safety.** A command's records across all facets commit as one atomic batch or not at all; a multi-facet grain's records carry facet tags; replay dispatches by tag and aborts activation on an unrecognized tag; the composite snapshot restores every facet to one seq. | §7.12 |
| **G20** | **Physical facets expose only durable state.** A physical facet's materialization is a rebuildable cache: its mutations are unobservable before the commit (the §6 gates) and it is discarded on any non-committed outcome; rehydration from the composite snapshot plus records reproduces it exactly. | §7.12, §7.14 |

A `granary` implementation conforms iff every invariant holds, verified under deterministic simulation (§14) for the distributed ones and by compile-fail tests for **G10**. Facets add the per-facet contract **F1–F4** (§7.12), verified generically per facet under the same simulation.

---

## 16. Extensions (deferred)

Named here, specified elsewhere or later, so the core stays small:

- **Alarm hibernation.** Letting a grain with a pending alarm (§7.16) hibernate, the alarm driver re-activating it at its deadline from the per-shard index. Today a pending alarm vetoes eviction and the grain stays resident, the index covering only the failover gap.
- **A linear workflow DSL.** An `async` `step`/`sleep`/`retry` surface over the workflow facet's memo and commands (§7.17), so a workflow reads as straight-line code rather than a hand-driven re-entrant loop.
- **Hibernatable connections.** Parking a record subscription (§7.9) — or a WebSocket/stream connection — *across* hibernation, holding the registration while the grain sleeps and re-delivering on wake (DO §5), so a follower need not re-subscribe after every idle eviction. An extension of the §7.9 primitive, not a separate mechanism: subscriptions today are dropped on deactivation and re-established by the subscriber; this would persist the registration instead.
- **Third-party facets.** The facet seam (§7.12) stays internal until the built-in set (facet 0, KV §7.13, workspace §7.11, SQL §7.14, disk §7.15, alarm §7.16, workflow §7.17) has settled the record-tag registry and cross-version compatibility rules; publishing it for out-of-crate facets is a later policy decision.
- **SQL lazy page hydration (a capture VFS).** A VFS shim for the SQL facet (§7.14) that both captures WAL frames below SQLite and faults database pages in from blobs on first access, so a large database activates without materializing whole — the upgrade over the reference WAL-tailing capture.
- **Workspace scaling: ranged sub-records and lazy hydration.** For the workspace facet (§7.11): splitting a changed file larger than a record bound into ranged sub-records (today one `Write` record carries a whole file, bounded by the 64 MiB tree cap); faulting files in from checkpoint chunks on first access so a large workspace activates without materializing whole; and preserving mtimes across the microVM tar pull so capture's stat-prune survives an fc command (today each fc command re-hashes the tree).
- **Disk lazy block hydration.** For the disk facet (§7.15): faulting image blocks in from checkpoint blobs on first guest access, so a large rootfs activates without materializing whole. The disk analogue of the SQL capture VFS above, and the move that lets a many-gigabyte machine migrate by touching only the blocks its next session reads.
- **Proactive blob re-replication on membership change.** Streaming a grain's blobs (§7.10) to a newly-added shard replica before it counts toward a quorum, restoring the durability margin actively rather than only on read-access. Deferred *in lockstep* with the same §7.6 work for records and snapshots, since both reach a new replica by recovery-on-access today.
- **Range-verified blob streaming and a cluster-shared cold tier.** Verifying a byte range of one large blob against the BLAKE3 tree the `BlobId` already roots (the Bao encoding, §7.10), so very large blobs need not be fetched whole; and a separate rendezvous-placed, namespace-scoped content store for cross-grain dedup, archival, and an external object-store tier — the **cold** half of the DO hot/cold split (DO §6), complementary to the grain-colocated **hot** blob store of §7.10.
- **Linearizable reads (check-quorum lease).** Extending the shard's leader-election group (§8) with a check-quorum property, so the leader serves reads locally while it can still confirm a quorum and the activation self-fences when it can no longer confirm shard ownership, rather than returning stale state (§7.5). The DO-faithful single-instance fence; cheaper than a per-read Raft read-index. Requires a check-quorum primitive on the leader-election group's Raft engine (a leader that has not heard from a quorum within an election timeout is no longer read-valid).
- **Follower reads.** Serving reads from a shard's followers with a freshness bound, trading linearizability for read scale (§7.5).
- **Cross-grain sagas.** Idempotent multi-grain workflows built above the single-grain consistency boundary (non-goal §2.2).
- **Optional `#[derive(Grain)]`.** Defaulting the manifest and `register` list, as actor §4.4 permits for actors: a convenience above the model, never required.

---

## Appendix A: End-to-end example

```rust
// --- A clustered system in the leader-based mode (actor §9.4.3) that holds the shard map ---
let system = ClusterSystem::start("node-a", config_leader_based).await?;

// --- Host grains of type Account: registers the gateway and joins/leads shards ---
let accounts: Granary<Account> = system.granary(GranaryConfig {
    shards: 16,                             // partitions of this type's namespace (§7.1)
    replication_factor: 3,                  // replicas per shard (§7.1)
    shard_target_bytes: 256 << 20,          // split a shard past this size (§7.7, deferred)
    idle_after: Duration::from_secs(10),    // hibernation window, matches DO (§10)
    snapshot_every: 256,                    // events (§9)
    grain_store: None,                      // None ⇒ fresh in-memory store per node; a
                                            //   factory persists records across a cold
                                            //   restart (§7.4, the Raft-WAL analogue, G14)
});

// --- Address a grain by name; activate-on-first-use, identical call site local or remote ---
let acct = accounts.grain("account/42");       // GrainRef<Account>, no activation yet
match acct.ask(Withdraw { cents: 500 }).await {
    Ok(Ok(balance))                  => println!("balance now {balance}"),  // committed + durable
    Ok(Err(Overdraft))               => { /* application outcome, inside the reply */ }
    Err(GrainError::Unavailable(_))  => { /* shard quorum lost; retry/failover (CP) */ }
    Err(GrainError::NotLeader(_))    => { /* retries exhausted; refresh and try again */ }
    Err(GrainError::Call(e))         => eprintln!("transport: {e:?}"),  // incl. CallError::Unhandled
}
```

**Hosting one grain under many type names (`granary_named`).** `granary(config)` hosts a
type under its own `GRAIN_TYPE`, built by `G::default` per activation, the common case.
A caller that must host **one Rust grain as several distinct grain types at runtime** uses
`granary_named`, which adds two extension points:

```rust
fn granary_named<G: Grain<System = Self>>(
    &self,
    grain_type: &'static str,                       // overrides G::GRAIN_TYPE (§5.1)
    config: GranaryConfig,
    factory: Arc<dyn Fn() -> G + Send + Sync>,      // overrides G::default (per-activation seam injection)
) -> Granary<G>;
// granary(config) == granary_named(G::GRAIN_TYPE, config, Arc::new(G::default))
```

Each `grain_type` is a fully distinct grain type: its own gateway (one `gateway_key` per
name), its own shard map, and, in the `Quorum` tier, its own leader-election groups (the group id hashes
`(grain_type, index)`, §8.2). The same key under two names addresses two independent grains.
`grain_type` MUST be stable cluster-wide and across runs, exactly as `GRAIN_TYPE` must be
(§5.1); the `&'static str` makes that lifetime explicit (a deployment leaks its bounded set
of names if they are not literals). The `factory` lets the runtime inject per-node seam
handles into each fresh activation, so the grain needs no `Default`. This is the seam the
agentic harness rides: each *kind* is its own grain type (`KindId` IS the `GrainType`),
hosted under one shared agent run loop.

## Appendix B: Suggested crate layout

```
granary/                 # the grain runtime, built on actor-core + actor-cluster
  grain.rs               # Grain, GrainHandler, GrainCtx, GrainName,
                         #   GrainRegistry (the per-grain dispatch builder, §4, §5.5)
  host.rs                # the host actor: durability protocol, rehydrate, hibernate (§6, §9, §10)
  gateway.rs             # per-node gateway: routing, activation table, NotLeader redirect (§5.3, §5.4)
  grainref.rs            # GrainRef + Granary handle + the system extension (`granary`/`grain`) (§4.3, §5.4)
  journal.rs             # the GrainJournal seam + AppendOutcome + Seq + DynGrainJournal (§7.3)
  election.rs            # the per-shard leader-election group: a small Raft group owning leadership/term/replica-set (§8)
  replicator.rs          # the Replicator: per-grain quorum append fenced by the shard term;
                         #   Local (single-node) and Quorum (clustered) tiers (§7.2, §7.4)
  store.rs               # the per-node GrainStore seam: term-fenced records keyed by (shard, grain),
                         #   plus the in-memory MemoryGrainStore reference impl (§7.2, §7.4)
  replica_store.rs       # the per-node replica-store actor + ReplicaTransport: the Quorum replicator
                         #   quorum-appends to and recovers from peers over actor messaging (§7.2, §8)
  shard.rs               # the clustered GrainJournal: composes the leader-election group + Replicator over a key range (§7)
  shardmap.rs            # the consensus-agreed shard map (per-type map group); allocator + reconcile (§7.6, §7.7)
  memory.rs              # the single-node journal: Local replicator + in-memory event log (tier 1, §7.4)
  file_store.rs          # the file-backed GrainStore: a node's records durable across a restart,
                         #   so a cold-restarted cluster recovers each grain from a quorum (§7.4, G14)
  blobs.rs               # the grain-native content-addressed blob store: BlobId + GrainBlobs
                         #   (put/get/has/gc/destroy), colocated with the journal (§7.10)
  subscription.rs        # record subscriptions (the journal follower): the push analogue of
                         #   load — live delivery of committed records to a Subscription's sink (§7.9)
  system.rs              # the GranarySystem capability seam + name→shard/group hashing (§5.1, §7)
  event.rs               # GrainEvent observability stream (§13)
  config.rs              # GranaryConfig (Appendix A)
  error.rs               # GrainError (§12)
  ws.rs                  # the workspace facet (§7.11): a real per-grain directory,
                         #   captured file deltas as records, checkpoint chunks as blobs
  facet.rs               # the facet seam (§7.12): tags, staging, the composite
                         #   snapshot, and the host's unioned blob roots
  kv.rs                  # the KV facet (§7.13): an ordered map folded from
                         #   Put/Delete records, transparent blob spill for large values
  sql.rs                 # the SQL facet (§7.14, `sql` feature): a private on-disk
                         #   SQLite per grain — WAL-frame records, checkpoint chunks as blobs
```

(`grainref.rs` rather than `ref.rs`, since `ref` is a reserved word; `system.rs`,
`event.rs`, and `config.rs` carry the `GranarySystem` seam, the event enum, and
the deployment config respectively.)

`granary` depends on `actor-core` (the model and seams), `actor-cluster` (the
clustered `ActorSystem`, the leader-based control plane, and the Raft
implementation the shards reuse), and `actor-serialization` (the codec and
dispatch building blocks). It is orthogonal to and independent of the agentic
`harness` crate. As in the actor framework, no *required* macro crate exists; any
`#[derive(Grain)]` is an optional convenience layered above the model.
