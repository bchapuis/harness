# Granary: Durable Objects ("Grains") for the Distributed Actor Framework

**Status:** Draft v2
**Scope:** A virtual, durable, single-activation object, a **grain**, addressable by a global name, with colocated event-sourced storage and a durability barrier on the reply path, built on the actor framework of [`distributed-actor-spec.md`](distributed-actor-spec.md).

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Throughout, `granary` is the crate and namespace name and `grain` is the durable object. This document is the sibling of the actor specification and cross-references it freely; a `§` with no document name refers to a section of *this* spec, and `actor §N` refers to [`distributed-actor-spec.md`](distributed-actor-spec.md). The design lessons come from [`../research/durable-objects.md`](../research/durable-objects.md) (cited as **DO §N**).

> **Design stance.** A grain is an **actor plus three things**: a name-based virtual identity, a durable event-sourced journal, and a durability barrier on the reply. The grain inherits everything else unchanged from the actor framework (mailboxes, serial execution, location-transparent `ask`/`tell`, membership, failure detection, supervision, death watch, the receptionist; actor §3–§13). Granary adds no new transport and no required macros (serde derives in user code are the only ones, as in actor §1.1).
>
> The architecture rests on one idea: **partition the grain namespace into shards, and make each shard a Raft group** that owns its grains' log, durability, leadership, and the compute for their activations (§7). The cluster runs O(shards) such groups, splitting a shard that grows hot or large so per-shard load stays bounded (§7.7). This is the single consistency mechanism. It sits between two failures: one cluster-wide log would be a global bottleneck, and one Raft group per grain would be millions of groups and an election storm. A bounded number of shards is neither. Within a shard, Raft gives free ordering from a single leader and durability from a quorum, and Raft's one-leader-per-term rule *is* the single-writer fence (§8).

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
- **Single-writer safety under partition.** A grain never forks. The shard leader is its only writer, and Raft forbids two leaders from committing in the same term (§8).
- **Pluggability.** The journal is a trait with two reference tiers, single-node and sharded-Raft (§7.4), as transport and the control plane are pluggable in the actor framework.
- **One programming model.** The handler is ordinary sequential Rust; the input and output gates (§6) supply atomicity-on-the-outside and durability-before-effects with no explicit locking.

### 2.2 Non-goals
- **A single cluster-wide log.** Granary MUST NOT serialize all grains through one Raft/Paxos group; sharding (§7) exists to avoid that bottleneck.
- **A consensus group per grain.** Granary MUST NOT run one Raft group per grain; the unit of consensus is the shard, which holds many grains (§7.1).
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
| **Shard** | A partition of one grain type's namespace and the unit of consensus: one Raft group owning the journal, leadership, and activations of that type's grains whose names fall in its range (§7.1). |
| **Shard map** | The cluster's consensus-agreed record of which nodes replicate each shard and which key range it covers; a per-grain-type map group seeded from the leader-based control plane (§7.6). |
| **Activation** | The live, in-memory instance of a grain on the leader of its shard. Disposable; rebuilt from the journal. |
| **Event** | A serializable value appended to a grain's journal; the unit of durable change. |
| **`apply`** | The pure fold that applies an event to state. Runs identically on live commit and on replay. |
| **State** | The value obtained by folding a grain's events; the snapshot payload. A cache of the journal. |
| **GrainJournal** | A grain's durable, totally-ordered, append-only log of events, stored in its shard's Raft log. The source of truth. |
| **Snapshot** | A persisted `(Seq, State)` checkpoint that bounds replay cost (§9). |
| **`Seq`** | The position of an event in one grain's total order; first event at 1. |
| **Quorum** | The majority of a shard's replicas whose acknowledgment commits a Raft entry. |
| **Term** | A shard's Raft term; it increases on every leader change and is the single-writer fence (§8). |
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

This **decide/apply split** is normative, and it is the deliberate alternative to an imperative `ctx.persist(event).await` API. The reasons:

1. **No interior mutability.** The host actor owns the activation's state and journal head and mutates them only through its `&mut self` on the serial executor (actor §6). The split keeps that the only writer; an imperative `persist` under the shared `&GrainCtx` would force an `Arc<Mutex<…>>` over state, re-introducing the very sharing the actor model removes (actor §3.5) and inviting a "don't hold the lock across `.await`" footgun.
2. **`Send` falls out for free.** Handler futures must be `Send` (actor §3.2); with the split, nothing crosses an `.await` but the user's own values.
3. **Atomic batch per command.** All of a command's events commit in one Raft entry (§7.3), so no observer ever sees a partial command.
4. **The durability barrier lives in exactly one place**, the host (§6), where it is auditable, not scattered across handlers.

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
}
```

The `G: GrainHandler<M>` bound proves at compile time that the grain accepts `M`, so an invalid call does not compile (invariant **G10**, the grain analogue of actor §3.3). The call site is identical whether the activation is local or on another node (§5).

The `M: Clone` bound lets the runtime re-issue the command when the first attempt provably did not run — a stale cached host that hibernated (`DeadLetter`) or whose leadership moved (`NotLeader`), neither of which commits (§6, §8). An *ambiguous* transport failure (`Unreachable`/`Timeout`) is never auto-retried, because the command may have committed before the reply was lost (at-most-once, §2.2). The clone is of the caller's own small command value.

`GrainCtx<G>` is the handler/lifecycle context. It exposes the grain's name, a `GrainRef` self-reference, and the system handle. (Surfacing the actor `Ctx` underneath for inherited capabilities such as death watch and child spawning is a deferred addition, §16; the current context exposes only the three accessors below.)

```rust
impl<G: Grain> GrainCtx<G> {
    fn name(&self) -> &GrainName;
    fn this(&self) -> GrainRef<G>;
    fn system(&self) -> &G::System;
}
```

`GrainCtx` deliberately exposes **no** `persist` method (§4.2). It does not expose state mutation; state changes only through events folded by `apply`.

---

## 5. Virtual activation and routing

### 5.1 Names and shards

A grain is addressed by `GrainName`, a `(GrainType, key)` pair where `key` is an arbitrary application-chosen string (`"account/42"`, a UUID, a tenant id). Unlike an `ActorId` (actor §3.6), a `GrainName` is **not** locality-classifiable on its own: it names a logical object, not a node.

Every name maps to exactly one **shard** of its grain type, by a stable hash of the name onto a key-range partition (§7.1). The mapping changes only when a shard splits or merges (§7.7), not when nodes come and go, so it is far steadier than a direct name-to-node mapping. A second lookup, the shard map (§7.6), gives the shard's current leader. Resolution is therefore two levels: name to shard (stable), shard to leader (changes on Raft elections).

`GrainName` is `Clone + Eq + Hash + Send + Sync + Serialize + DeserializeOwned`.

### 5.2 Where a grain lives

A grain activates on the **leader of its shard**. Co-locating the activation with the shard leader is deliberate: the leader already holds the shard's Raft log on local disk, so a write appends locally before replicating (§7.2) and a rehydration reads locally without a network round trip (§9). The leader hosts the compute for every active grain in its shard.

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

`GrainRegistry<G>` maps a command's `MANIFEST` to a dispatch entry that *decodes the payload, invokes the typed `GrainHandler`, and encodes the reply*, the grain analogue of the actor dispatch registry (actor §4.4), differing only in that it returns reply bytes rather than enqueuing-and-dropping. `Grain::register` fills it via `r.accept::<M>()`, exactly as `Actor::register` does. The registry is the deserialization allowlist for grain commands (actor §5, §15): a name-addressed command whose manifest is unregistered yields `GrainError::Unhandled`, and no type outside the registry is ever built from network bytes.

As in the actor framework, a single concrete envelope type carries the manifest and payload across the wire, so there is no need to derive a per-command manifest for a generic wrapper; the inner manifest is the dispatch key.

---

## 6. The consistency model: input and output gates

Granary lifts the Durable Objects gate model (DO §3) onto the framework, so that ordinary sequential handler code is race-free and durable at almost no cost over the serial executor.

- **Input gate.** While a grain's append is in flight, the host admits **no new command** to that grain. New messages queue in the mailbox until the host has no outstanding append. This closes the window a plain serial executor leaves open *at `.await` points* (actor §6): a second command cannot observe half-committed state mid-handler.
- **Output gate.** When a command produces events, the host holds the **reply** until those events are committed in the shard's Raft log (§7). The reply travels through the actor `ReplyHandle::send` (actor §4.5) only after the commit. If the commit fails, the reply becomes a `GrainError` (§12); no observer is ever told an effect happened that did not durably happen.

The host's per-command protocol runs in order, and it is the one place the barrier lives:

```
1. (events, reply) = G::handle(&state, msg, ctx)        // decide; no mutation, no I/O
2. if events.is_empty() { return reply }                // read path: nothing to commit (§7.5)
3. outcome = journal.append(grain = name, after = head, events)  // §7.3, Raft-replicate on the shard
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
- **Fold only a contiguous head.** A grain is its shard's single writer (§8), so a commit MUST advance its head by exactly the batch just appended. If the journal returns a head that jumps further — its starting head was stale, because the leader's view lagged at activation (§9) or a previously timed-out append committed late (§7.2) — the host MUST NOT fold (the intervening committed events were never applied to its state); it steps down and the next access rehydrates from the journal authority (**G3**). The host treats `NotLeader` and `Unavailable` the same way: any outcome other than a contiguous commit ends the activation, because after it the in-memory head can no longer be trusted. A forced step-down emits `Passivated` (§13) so the lifecycle stream stays balanced.

**`tell` is fire-and-forget.** A `tell` returns once the host accepts the command, not after the commit (actor §3.3, §7.2), so it reports only the enqueue-time failures (`Call`, `NotLeader`, `Unhandled`), never `Unavailable`. The host runs the same protocol for the command, but the caller does not await the outcome: if the commit cannot complete, the command has no effect and the caller is not notified. This is the at-most-once contract (actor §7.2), which callers make idempotent where it matters.

---

## 7. Durability and replication

### 7.1 Shards: the unit of consensus

Granary partitions each grain type's namespace into **shards**. A shard owns a contiguous range of its type's name hash space and is one Raft group of *R* replicas (`R=3` or `R=5`). The shard's Raft log holds the journals of all that type's grains in its range, each entry tagged with the grain it belongs to; a grain's events are the subsequence of the shard log carrying its tag. The shard leader is the single writer and hosts the grains' activations (§5.2). Per-type shards keep the gateway, dispatch registry, and configuration (Appendix A) all keyed by one grain type, and let a shard replay with a single known `apply` (§4.1).

This is the deliberate middle ground (design stance). One cluster-wide log would serialize every grain through one leader; one Raft group per grain would mean millions of groups, each with its own timers, persistent term/vote, and a synchronized election storm on any node failure. A shard holds many grains and there are O(shards) shards, a number the cluster keeps bounded by splitting (§7.7). The cluster runs as many Raft groups as it has shards, not as it has grains.

The cost of a shared shard log is honest and bounded: the grains in one shard commit through one leader, so a write-heavy grain can queue ahead of its shard-mates. Splitting a hot shard (§7.7) caps this, the way a range-partitioned store splits a hot range. The benefit bought is the thing that reaches millions of grains: a bounded number of consensus groups and no per-grain coordination.

### 7.2 Ordering is free; durability is a quorum append

A shard has one leader per term (§8). The leader assigns each entry its position in the shard log with no agreement round, because no other writer proposes entries. Raft then replicates the entry and commits it once a quorum has it. So the split the DO research note draws (DO §4.3) holds at the shard level: ordering is free from the single leader, and durability is a quorum append. Per-write consensus is just log replication; the only agreement round, leader election, happens on failover (§8.3), not per write.

Each grain's own events stay totally ordered within the shared log: the host appends them in `Seq` order behind the input gate (§6), and replay reads them back in that order (§9).

**Commit-once under late commits.** A leader awaits its append's commit, bounded by a timeout that reports `Unavailable` (§11). A timed-out entry MAY still commit later, so the apply path MUST be idempotent: each proposal carries a **proposal id** and is applied to the projection at most once. The id MUST be unique across the lifetime of a proposer, *including process restarts* — a node that crashes and re-starts reuses its stable node identity, so a bare `(node, sequence)` id would re-mint a prior incarnation's ids and the dedup would silently swallow the re-started node's writes. The id therefore carries a per-incarnation **epoch** (drawn from the deterministic entropy seam at journal construction), making `(node, epoch, sequence)` unique across restarts while staying reproducible under simulation. This is the durability analogue of the membership layer's incarnation-stamped merge.

### 7.3 The journal seam

The journal is a trait, a simulation and deployment seam like `Transport` and `Clock` (actor §4.6, §7), operating on opaque, codec-encoded event bytes so it stays codec-agnostic:

```rust
pub trait GrainJournal: Send + Sync + 'static {
    /// Append `events` for one grain immediately after `after`, as one atomic
    /// entry in the shard log. Commits on a Raft quorum.
    fn append(&self, grain: &GrainName, after: Seq, events: Vec<Vec<u8>>)
        -> impl Future<Output = AppendOutcome> + Send;

    /// Up to `limit` committed events for one grain from `from` (exclusive)
    /// toward its head. A local, fence-free read on the leader.
    fn load(&self, grain: &GrainName, from: Seq, limit: usize)
        -> impl Future<Output = Result<Vec<(Seq, Vec<u8>)>, GrainJournalError>> + Send;

    /// The grain's committed head — the authoritative source of `head` on
    /// rehydration (§9, invariant G3/G4). `Seq::ZERO` for a grain with no
    /// committed events. Rehydration derives `head` from this rather than trusting
    /// memory: the log always knows its head, and the sharded tier knows the
    /// per-grain commit index.
    fn head(&self, grain: &GrainName)
        -> impl Future<Output = Result<Seq, GrainJournalError>> + Send;

    /// Persist a snapshot for one grain at a committed seq (§9). Returns
    /// `Committed(at)` on success, or `NotLeader` if this node no longer leads.
    fn save_snapshot(&self, grain: &GrainName, at: Seq, state: Vec<u8>)
        -> impl Future<Output = AppendOutcome> + Send;

    /// The latest snapshot for one grain, if any (§9).
    fn load_snapshot(&self, grain: &GrainName)
        -> impl Future<Output = Result<Option<(Seq, Vec<u8>)>, GrainJournalError>> + Send;

    /// Block until this node's local view reflects every write committed as of
    /// now — the rehydration barrier (§9). A no-op on a synchronous single-node
    /// store; on the sharded tier it waits for the committed stream to drain. The
    /// wait MAY be bounded (so a pathological backlog cannot wedge an activation
    /// indefinitely); the host's contiguity guard (§6) is the backstop for any
    /// residue, so a bounded barrier never folds onto a stale head.
    fn catch_up(&self) -> impl Future<Output = ()> + Send { async {} }
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

### 7.4 Durability tiers

The seam admits multiple implementations; two are reference tiers, chosen at deployment like a membership mode (actor §9.4):

1. **Single-node.** One linearizable local store (in-memory, or a local file log with the hard-link/optimistic-concurrency fence of the harness prior art). `append` commits on local fsync. Sweet spot: embedded, single-node, tests, and the deterministic simulator (§14). CP trivially (one writer, one store), not fault-tolerant to node loss.
2. **Sharded Raft.** The clustered, fault-tolerant, CP production target described in this section: O(shards) Raft groups of *R* replicas, split to stay bounded. A committed write survives the loss of any minority of a shard's replicas.

### 7.5 Reads

A command that emits no events (a query) commits nothing (§6 step 2). The leader serves it from the grain's in-memory activation — a local, replication-free read. This is the property Durable Objects prize: a read is served by the single owner with **no per-read consensus**, so it is effectively zero-latency (DO §4.3, §4.4). Reads scale with the leaders' capacity and never wait on replication.

**The contract is read-your-leader (relaxed), not linearizable under partition.** Because the activation is colocated with the shard leader (§5.2) and the read path does not reconfirm leadership per call, a leader that has been deposed but not yet fenced — an isolated minority leader that has not yet learned it lost the election — MAY serve a stale read until its activation stops. Writes never fork (Raft fences the commit, §8); only reads can be stale, and only on the minority side of a partition. A caller that needs a linearizable read in the meantime issues a trivial *writing* command (one that emits an event): it rides the §6 output gate, so it commits through the shard leader and reflects committed state, or fails (`NotLeader`/`Unavailable`) on a deposed leader.

**Linearizable reads are a deferred upgrade (§16), via a leader lease — not a per-read consensus round.** The DO-faithful mechanism is single-instance fencing, not a Raft read-index: the leader serves reads locally while it holds a **check-quorum lease** (it has heard from a quorum within an election timeout, so no other leader can have been elected), and the activation **self-fences** — returning `Unavailable`/`NotLeader` — when the lease lapses, trading availability for consistency on the minority side (CP, §11). This keeps steady-state reads local and zero-latency, unlike a read-index, which would pay a quorum round-trip per read and defeat read scaling (§7.8). (Reads from shard *followers* are a separate deferred extension, §16.)

### 7.6 The shard map is a consensus-agreed allocation

The **shard map** records, for each shard, its key range and its replica set, and tracks current leadership. It MUST be **consensus-agreed**, so every node resolves a name to the same replica set regardless of join order: a rendezvous choice each node snapshots from its own *live* membership view would diverge across nodes that join at different membership epochs. The reference implementation realizes this as a **per-grain-type Raft group whose committed log *is* the allocation** (one `Assign { shard, replicas }` record per shard); every node applies the identical committed entries and so agrees on where each shard lives. This map group is keyed by grain type and derives its group id from a reserved shard index, so it never collides with a data shard's group (§8.2).

A leader-only **allocator** keeps each shard's committed replica set equal to its rendezvous choice over the current cluster voters, and a leader-only **reconcile** loop drives each group it leads (the map group and the shards it leads) toward its intended membership — so shards rebalance onto and off of changed members as the cluster grows or shrinks. A node that is newly a replica creates the shard's group as a non-member over the *old* replica set (no election disruption) and catches up the committed prefix by replication before the reconcile loop adds it as a voter.

The map's write rate is low: it changes on shard splits and merges (§7.7), on replica-set reconfiguration, and on node membership changes, never on a grain activation or a grain write. Nodes cache the map and refresh on `NotLeader` (§5.4), so steady-state routing adds no map-group traffic (invariant **G9**). The map group seeds its membership from the actor framework's **leader-based control plane** (actor §9.4.3): granary therefore requires the leader-based control-plane mode; the pure gossip-based AP mode (actor §9.4.4) cannot agree a shard map (this is the consistency-versus-availability tradeoff of actor §12, resolved toward CP).

> **Note.** An earlier draft stored the allocation *inside* the control-plane group's own log (actor §9.4.3 item 6). The implementation instead runs a dedicated per-type map group seeded from the control plane's voters; the guarantee (a single consensus-agreed allocation, off the grain data path) is identical, but the consensus-group count is `O(shards) + O(grain types)` rather than `O(shards)` alone (§8.2, **G7**).

### 7.7 Splitting and merging shards

A shard that grows too large or too hot **splits**: its key range divides in two, and the grains on each side become two shards with their own Raft groups. A pair of small, cold adjacent shards MAY **merge**. Split and merge are the elasticity mechanism: the number of shards tracks load and cluster size, so per-shard work stays bounded as the grain count climbs into the millions.

A split MUST be atomic with respect to writes: the parent stops accepting new appends for the moving range, the committed prefix transfers to the child group, and the new mapping commits to the shard map (§7.6) before either child serves the moved range. No write is lost or duplicated across the boundary, and no grain is writable in two shards at once (invariant **G15**). Implementations SHOULD trigger split/merge from shard size and request-rate signals on the §13 event stream.

### 7.8 Scaling: where the cost goes

The architecture's scaling claim is that cost tracks the *active working set* and the *cluster size*, not the total grain count.

- **Consensus groups:** O(shards), bounded by split/merge (§7.7), not O(grains). A node runs a fixed handful of Raft groups regardless of how many grains it stores.
- **Control-plane writes:** O(cluster events), namely splits, merges, reconfigurations, and membership, never per activation or per write (§7.6).
- **Activation:** a local operation on the shard leader, no consensus and no network (§5.2, §10). This is what lets hibernation be aggressive (§10) without loading any shared component.
- **Memory and streams:** bounded by the grains *active* at once, which hibernation keeps to the working set (§10). Total grain count is limited only by the shards' storage.
- **Routing:** two cached lookups, off the control plane and off the serial gateway in steady state (§5.4).

The residual bounded cost is the per-shard shared log (§7.1), capped by splitting a hot shard.

---

## 8. The single-writer fence

A grain must have one writer. In the sharded-Raft tier that fence is Raft itself, not a separate token.

- **One leader per term.** Raft elects at most one leader per term per shard, and only the leader appends. A grain's writer is its shard's leader.
- **A deposed leader cannot commit.** A leader that has lost quorum (a partition, a new election) cannot commit a new entry; its `append` returns `NotLeader` or `Unavailable` (§7.3). Raft's leader-completeness property guarantees the new leader holds every committed entry, so the log never forks (invariant **G1**).
- **The host steps down cleanly.** On `NotLeader` the host passivates the grain and the caller retries against the new leader (§5.4, §6 step 5). The host MUST NOT keep serving a grain whose shard it no longer leads.

Because consensus is sharded (§7.1), the cost of "one leader per object" is paid once per shard, not once per grain, and a stale activation is fenced by the same Raft that orders writes. A placement disagreement (two nodes briefly believing they lead a shard) cannot fork the log: the minority leader reaches no quorum, so its writes never commit.

### 8.1 Why this is enough

A tempting alternative pins each grain's writer with a clock-based lease and a monotonic fencing token. Raft subsumes both: leadership is a quorum fact, not a clock fact, so a paused leader that wakes is already deposed and cannot commit, regardless of clock skew, and no separate token is needed. The grain keeps a per-grain `Seq` only to order its own events and to align snapshots with the log (§9), never to arbitrate writers.

### 8.2 The actor control plane, the shard maps, and the shards, side by side

Three consensus layers operate, at very different rates, and they do not conflict:
- the **actor control plane** (one leader-based group, actor §9.4.3) owns cluster membership and seeds the map groups' voters, changing on cluster events;
- the **shard map** per grain type (one group per type, §7.6) owns the allocation — which nodes form each shard and which key range it covers — changing on splits, merges, reconfiguration, and membership;
- the **shards** (O(shards) groups) own the data, changing on every write.

A shard map decides *which nodes form a shard and which key range it covers*; the shard's own Raft decides *which replica leads it and in what order entries commit*. Every group derives a distinct, non-colliding group id (the membership control group is id 0; a shard map uses a reserved shard index; data shards hash `(grain_type, index)`), so none is on another's hot path. The total group count is `O(shards) + O(grain types) + 1`, still bounded by split/merge (§7.7) and the number of hosted types — never O(grains) (**G7**).

### 8.3 Failover

When a shard's leader fails or is partitioned away, the shard's replicas elect a new leader by ordinary Raft. The new leader already holds every committed entry (leader completeness), so it can serve the shard's grains at once; their activations rehydrate lazily on first access (§9, §10). The window between the old leader's loss and the new leader's election is the shard's **failover window**, during which calls to that shard's grains fail fast (`NotLeader` then a retry, or `Unavailable`, §11) rather than block. No acknowledged write is lost, because acknowledgment required a quorum commit and the new leader inherits it (invariant **G14**).

---

## 9. Snapshots and compaction

Replaying a long event history on every activation is wasteful, so a grain periodically snapshots its folded state.

- **Snapshot policy.** After a commit, the host MAY persist a snapshot `(head, State)` for the grain via `save_snapshot` (§7.3). The trigger (every *N* events, every *T* of growth) is configuration, not part of the model. Only the shard leader writes snapshots, so a deposed leader cannot (its `save_snapshot` returns `NotLeader`).
- **The rehydration barrier (§10).** Before reading the head, the leader MUST wait until its local view reflects every write committed as of activation. A freshly-elected leader has every committed entry in its Raft log, but the colocated read model (§7.5) serves from an in-memory projection of the committed stream, which may still be draining the backlog at the instant of activation. Reading the head from a still-draining projection would rebuild a short state and then serve stale reads or fold onto a stale head. The barrier (`catch_up`, the journal seam) closes that window; it is a no-op on the single-node tier, whose store *is* the committed state.
- **Rehydration (§10).** On activation, after the barrier, the leader loads the grain's latest snapshot `(s_seq, s_state)`, then replays its events after `s_seq`, folding via `apply`, to reach the head. Both reads are local to the leader (§5.2). The head is set **only** from journal/snapshot returns, never trusted from a prior activation's memory (invariant **G3**).
- **A snapshot MUST NOT shorten the effective log.** If a snapshot's `s_seq` exceeds the grain's committed head, the host MUST ignore it and replay from `ZERO`. The journal is always the authority; the snapshot is only an optimization (invariant **G4**).

Compaction (truncating event prefixes for a grain once a durable snapshot covers them) is a background operation of the shard, below the seam; it MUST NOT remove any event not yet covered by a durable snapshot. Per-grain snapshots let the shard compact a grain's history independently of its shard-mates.

---

## 10. Lifecycle: activation, migration, hibernation

A grain's lifecycle, from the host's view, is a loop of:

```
activate → rehydrate (snapshot + replay, §9) → on_activate → serve commands → idle → on_passivate → (snapshot) → stop
```

- **Activation** is triggered by the first message for the name reaching its shard leader's gateway (§5.3). The leader rehydrates the grain (§9), runs `on_activate`, then serves. Activation takes no consensus and no network: the leader holds the log locally (§5.2).
- **Migration** follows shard leadership. When the shard's leader changes (§8.3), the grain's activation moves to the new leader and rehydrates there. In-memory state is never preserved across the move; the journal rebuilds it.
- **Hibernation (deactivate-on-idle).** After an idle interval the leader MAY — *if the grain's `can_passivate` permits it* — run `on_passivate`, snapshot to bound the next replay, and `ctx.stop()`, dropping the grain's in-memory state. (`on_passivate` cannot mutate state — §4.3 — so it runs before or after the snapshot indistinguishably; the implementation runs it first.) A grain that vetoes eviction (`can_passivate` returns false, e.g. an autonomous grain with a live run) is left running and the idle check rescheduled. The gateway prunes the name from its activation table (it watches its hosts via death watch, actor §12). The next message re-activates and rehydrates. Hibernation reclaims memory; persisted storage survives; in-memory state was only ever a cache (§1).
- **Forced step-down.** When leadership moves (§8) or the shard goes unavailable (§11), the activation deactivates involuntarily: it runs `on_passivate` (to release non-durable per-activation resources) but takes **no snapshot** — the journal may be unwritable — then emits `Passivated` and stops. The journal is the authority; the next access rehydrates.
  - **Default and tuning.** `idle_after` SHOULD default to about **10 seconds**, matching the Durable Objects eviction window (DO §5), because reactivation is a cheap local replay (§5.2). To avoid thrashing when a grain is accessed just slower than the timer, an implementation SHOULD apply a small minimum residency or jitter so a barely-idle grain is not evicted and reloaded repeatedly.
- **Eviction races.** If a command reaches the gateway for a name whose host has stopped but whose `Terminated` has not yet pruned the table, the host `ask` returns `CallError::DeadLetter`; the gateway MUST treat that as "reactivate" (drop the stale entry and activate afresh), bounded to avoid a loop.

Durable **alarms**, a stored timer that re-activates a grain to run an `alarm()` handler with no caller present (DO §5), are a named extension, deferred (§16).

---

## 11. Failure model and the node-down cascade

Granary inherits the actor failure model (actor §8) and adds the durability outcomes. The blast radius of a failure is the **shard**, not the cluster and not the single grain.

When a node fails (actor §8.1):
1. **Shard leadership.** Every shard the node led re-elects a leader among its remaining replicas (§8.3). The shards it merely followed continue under their existing leaders.
2. **In-flight callers.** Every pending `ask` to a grain on the failed leader completes with `GrainError` wrapping `CallError::Unreachable` (actor §7.2, §8.1), never a hang; the caller retries against the new leader.
3. **Watchers.** A grain MAY be watched as any actor; watchers receive `Terminated { NodeDown }` (actor §12) for the activation.
4. **Re-activation.** The next message re-activates each affected grain on its shard's new leader, which rehydrates from the log (§9). No acknowledged write is lost (invariant **G14**).

**Quorum loss is unavailability, not a fork (CP).** If a shard cannot reach a quorum (enough of its replicas are down or partitioned away), it elects no leader and commits nothing. `append` returns `Unavailable`, and every grain in that shard pauses writes (§6 step 6): effects are not applied, the reply is `GrainError::Unavailable`, and the affected activation steps down (the outcome is ambiguous — a timed-out commit MAY land later, §7.2/§7.3 — so the in-memory head is no longer trusted; the next access rehydrates from the journal). Callers retry or fail over. The shard does not fork, because a minority cannot commit (Raft). Only that shard's grains are affected; the rest of the cluster serves normally. Reads remain read-your-leader (§7.5): until the leader lease of §7.5/§16 exists, an isolated minority leader cannot fence itself and MAY serve a stale read, though it can commit nothing; with the lease, a leader holding a valid one MAY keep serving linearizable reads.

---

## 12. Error model

Grain calls surface two failure layers, kept distinct exactly as the actor framework keeps `CallError` and `M::Reply` apart (actor §14):

```rust
pub enum GrainError {
    Call(CallError),       // transport/system failure reaching the activation (actor §14.1)
    NotLeader(NodeId),     // leadership moved; the runtime retries against the hint, surfacing this only if retries are exhausted
    Unavailable(String),   // the grain's shard cannot reach a quorum; the write did not commit (§11)
    Unhandled,             // no registered handler for the command's manifest (§5.5)
}
```

- **Application errors** the handler deliberately produced live **inside `M::Reply`** (e.g. `Result<T, E>`, §4.2), never in `GrainError`.
- `NotLeader` is normally absorbed by the runtime's bounded redirect (§5.4); it surfaces to the caller only when retries are exhausted. `Unavailable` is a real durability outcome the caller must handle.
- `GrainError` MUST be exhaustive at the public API, so callers handle partial failure explicitly (actor §14, "define errors out of existence": keep only the real ones).

---

## 13. Security and observability

- **Security.** Granary adds no transport; it inherits mutual-TLS associations, the handshake allowlist, and the deserialization allowlist (the grain dispatch registry, §5.5) from actor §15. The gateway MAY consult an `Authorizer` per `(peer, GrainName, manifest)` before activating or dispatching, as actor §15.4 gates `deliver`.
- **Observability.** Granary emits, on the actor framework's single extensible `Event` stream (actor §16), grain and shard events: `Activated`, `Rehydrated { from_snapshot, replayed }`, `Committed { seq }`, `Snapshotted { at }`, `Passivated`, `LeaderChanged { shard }`, `ShardSplit`, `ShardMerged`. These drive both operator tooling and the simulator's invariant checks (§14). Metrics SHOULD include per-shard commit latency and log size, per-node active-grain count, shard count, and leadership changes.

---

## 14. Testability and deterministic simulation

Granary is testable by the same deterministic simulation as the actor framework (actor §18): a whole cluster of grains runs in one process, on one logical thread, over virtual time, network, and randomness, so a single seed reproduces a run exactly.

- The journal (§7.3) is a **simulation seam**, like `Transport`. The per-shard Raft reuses the actor framework's Raft (the leader-based control plane's implementation, actor §9.4.3), so simulation drives the real consensus code, not a model of it.
- Reusing the framework's seams (`Clock`/`Entropy`/`Spawner`/`Transport`, actor §4.6, §7) means simulation runs the real host, gateway, shard, and rehydration code.
- Fault injection MUST be able to produce: a shard leader crash and election mid-write (to exercise the fence, §8), shard quorum loss (to exercise `Unavailable`, §11), eviction mid-command (to exercise rehydration and the output gate, §6, §10), and a shard split under concurrent writes (to exercise §7.7). A no-fault run is the simplest case and MUST pass.

---

## 15. Invariant catalogue and conformance

These invariants appear as MUSTs above; collected here, they are the contract a conforming implementation verifies, each holding even under the faults of §14. They are checked the way actor §18.5/§18.6 prescribe: continuous checkers over the §13 event stream for safety properties, targeted tests for the rest, compile-fail for type-safety.

| | Invariant | Defined in |
|---|---|---|
| **G1** | **Single writer per grain.** Only the shard leader appends; Raft elects one leader per term and a deposed leader cannot commit, so a grain's log never forks. A commit advances the grain's head by exactly the batch appended; the host folds only such a contiguous commit and steps down on any other outcome. | §6, §8 |
| **G2** | **Deterministic fold.** `apply` produces identical state on live commit and on replay from any snapshot/journal prefix. | §4.1 |
| **G3** | **GrainJournal is the source of truth.** `head` and state derive only from journal/snapshot returns; in-memory state is never trusted across activation. | §1, §9 |
| **G4** | **Snapshot never shortens the log.** A snapshot seq beyond a grain's committed head is ignored; replay always reaches the true head. | §9 |
| **G5** | **Reply iff durable.** A command's reply is released, and its events folded, only after the entry commits; a `NotLeader`/`Unavailable` outcome yields an error, no fold, no success reply. | §6 |
| **G6** | **Exactly-once activation per node.** The serial gateway activates a name at most once; concurrent requests find the same host. | §5.3 |
| **G7** | **Bounded consensus groups.** The cluster runs `O(shards) + O(grain types)` Raft groups (data shards plus one shard-map group per type, §7.6), kept bounded by split/merge, never O(grains) and never one. | §7.1, §7.6, §7.7 |
| **G8** | **Activation without consensus.** Activating or hibernating a grain touches no consensus group and no network; only shard membership/leadership/split does. | §5.2, §7.8, §10 |
| **G9** | **Control plane off the data path.** The shard map changes only on cluster events; no grain write or activation contacts it. | §7.6 |
| **G10** | **Type-safe calls.** A command a grain has no `GrainHandler` for does not compile. | §4.3 |
| **G11** | **CP under partition.** A shard that cannot reach a quorum pauses its grains' writes (`Unavailable`) and never forks; other shards serve. | §11 |
| **G12** | **Hibernation round-trip.** A grain evicted when idle re-activates with identical state via snapshot + replay; no acknowledged write is lost. | §9, §10 |
| **G13** | **Location transparency.** A call to a local versus remote grain produces observably identical replies and ordering. | §5.4 |
| **G14** | **Lossless failover.** A new shard leader inherits every committed entry (Raft leader completeness); no acknowledged write is lost across a leadership change. | §8.3 |
| **G15** | **Split/merge safety.** A grain is writable in exactly one shard at any time; a split or merge transfers the committed prefix atomically and loses or duplicates no write. | §7.7 |

A `granary` implementation conforms iff every invariant holds, verified under deterministic simulation (§14) for the distributed ones and by compile-fail tests for **G10**.

---

## 16. Extensions (deferred)

Named here, specified elsewhere or later, so the core stays small:

- **Durable alarms.** A stored timer (`set_alarm` → `alarm()` handler) that re-activates a grain with no caller present (DO §5). The basis for retries, timeouts, and batch flushes.
- **Hibernatable connections.** Parking WebSocket/stream connections across hibernation, re-delivering via callbacks (DO §5), so a grain sleeps without dropping clients.
- **Linearizable reads (leader lease).** A check-quorum leader lease that lets the activation self-fence when it can no longer confirm shard ownership, so a deposed/isolated leader stops serving reads rather than returning stale state (§7.5). The DO-faithful single-instance fence; cheaper than a per-read Raft read-index. Requires a check-quorum/lease primitive on the shards' Raft engine (a leader that has not heard from a quorum within an election timeout is no longer lease-valid).
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
});

// --- Address a grain by name; activate-on-first-use, identical call site local or remote ---
let acct = accounts.grain("account/42");       // GrainRef<Account>, no activation yet
match acct.ask(Withdraw { cents: 500 }).await {
    Ok(Ok(balance))                  => println!("balance now {balance}"),  // committed + durable
    Ok(Err(Overdraft))               => { /* application outcome, inside the reply */ }
    Err(GrainError::Unavailable(_))  => { /* shard quorum lost; retry/failover (CP) */ }
    Err(GrainError::NotLeader(_))    => { /* retries exhausted; refresh and try again */ }
    Err(GrainError::Call(e))         => eprintln!("transport: {e:?}"),
    Err(GrainError::Unhandled)       => unreachable!("Withdraw is registered"),
}
```

**Hosting one grain under many type names (`granary_named`).** `granary(config)` hosts a
type under its own `GRAIN_TYPE`, built by `G::default` per activation — the common case.
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
name), its own shard map, and — at Tier 2 — its own consensus groups (the group id hashes
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
  shard.rs               # the shard: a Raft group over a key range; per-grain append/load/snapshot (§7)
  shardmap.rs            # the consensus-agreed shard map (per-type map group); allocator + reconcile (§7.6, §7.7)
  journal.rs             # the GrainJournal seam + AppendOutcome + Seq + DynGrainJournal (§7.3)
  memory.rs              # single-node local journal (tier 1, §7.4)
  system.rs              # the GranarySystem capability seam + name→shard/group hashing (§5.1, §7)
  event.rs               # GrainEvent observability stream (§13)
  config.rs              # GranaryConfig (Appendix A)
  error.rs               # GrainError (§12)
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
