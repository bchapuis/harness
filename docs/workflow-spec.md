# Workflow: Specification

**Status:** Implemented by the `workflow` crate. The agent runtime implements `Workflow` (agent §3.2) and keeps beside the cycle only what no fold expresses: an environment pre-pass ahead of every tick (agent §5.4, §5.5, §5.6) and the events it owes once a record commits (agent §10.4, §11).
**Scope:** Autonomy under an entity, specified as its own layer: re-derive the work committed state calls for, launch what is not yet in flight, fold each outcome back, and stop when nothing is left. The entity supplies identity, the journal, the single-writer fence, placement, activation, and hibernation; the workflow adds the reconciling cycle and the properties that keep it safe under crash and failover.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Sections of the entity specification ([`entity-spec.md`](entity-spec.md)) are cited as **entity §N** (its invariants as **E1–E21**); the actor framework ([`distributed-actor-spec.md`](distributed-actor-spec.md)) as **core §N**; the agent runtime ([`agent-spec.md`](agent-spec.md)) as **agent §N**; the machine ([`machine-spec.md`](machine-spec.md)) as **machine §N**. Sections of this document are cited as plain **§N**, and its invariants are numbered **W1, W2, …**. Citations of the agent runtime run against the crate dependency (`agent` depends on `workflow`): the agent is the layer this document generalizes, so it is the motivation and the worked example.

> **Design stance.** An entity waits: it admits a command, decides records, commits them, and replies (entity §4.2, §6). An autonomous entity drives itself forward until its work is finished, yet never awaits I/O inside a handler, because a handler is a pure decision the entity replays (entity §4.1, E2). The agent runtime calls this **autonomy** (agent §3).
>
> Autonomy is not agentic: "re-derive the work, launch what is not in flight, fold each outcome back, stop when nothing is left" names no model, tool, or prompt. This document specifies that mechanism as the **workflow**, one layer below the agent runtime, so an application can write its own loop with durability, resume, and effect discipline supplied, and so the properties that make autonomy safe under crash and failover (W1–W7, §9) are stated and checked once.
>
> The design rests on one decision: **the work outstanding is a pure function of committed state**, never a journaled queue. Recovery computes the same function over the same records, so resume is not a special case, and one set is the dispatch list (§3.2), the recovery set (§4.4), and the straggler filter (§4.5).

---

## 1. Scope and layering

Everything here is built **on top of** the entity and changes nothing beneath it:

- the **entity** (entity §1–§16) supplies identity, the journal, the fold, the single-writer fence, placement, virtual activation, hibernation, and lossless failover; the workflow adds no journal, fence, placement function, or resume protocol;
- the entity's **output gate** (entity §6) is the workflow's write-ahead barrier: "intent before effect" (§5) is the gate, used, not a mechanism built here;
- **actors** (core §3–§12) host the activation. Every effect launches through `Spawner` (core §4.6), and every outcome returns as an ordinary typed message with a hand-written manifest (core §4.4);
- the **event stream** (core §16, entity §13) carries the workflow's checker-facing events through the core's application-event extension point (§8).

The steps facet (entity §7.17) supplies durable step **memoization** and leaves to each consumer the entity-specific half: launching an effect off the command path and self-`tell`ing its result back. The workflow is that half. Entity rules that an entity whose results are already journaled events with their own fold, such as the agent's `ModelResponse` and `ToolOutcome` records, **MUST NOT** memoize them a second time under the steps tag, and adopts only the launch-once guard. The workflow therefore never memoizes: the consumer's own records are the durable fact, and the desired set (§3) is re-derived from them. An entity that also wants the memo declares the steps facet beside the workflow; the two compose without knowing each other.

### 1.1 Non-goals

- **A model, a tool, or a transcript.** `agent` owns all three; a second consumer may own none of them.
- **Idempotent request submission** (agent §7.4): a fold over the consumer's records with its own notion of equality (§7.2).
- **Cancellation.** A consumer cancels by journaling a terminal record, from which its `pending` re-derives the propagating sends as ordinary effects (§7.2).
- **A step DSL.** The linear `async` `step`/`sleep`/`retry` surface stays deferred (entity §16); the workflow is the re-entrant form such a DSL would be written over.
- **Exactly-once effects.** Impossible above an at-most-once transport (core §7.2) and a fence that guards records rather than the world (agent §5.5). The workflow guarantees W3 and W6, and a re-activation MAY re-launch an effect whose outcome has not committed.
- **Scheduling.** Deadlines that re-activate a hibernated entity belong to the alarm facet (entity §7.16). The workflow's only timer is the per-key backoff inside an activation (§3.4); a key at rest waits for a caller, and alarm-driven ticks are future work (§11).

---

## 2. The autonomous entity

### 2.1 Anatomy

A workflow entity is an entity plus one function and one seam:

- **`pending`** (§3.1): the effects its committed state calls for. Pure, deterministic, and folded from the journal, so identical on every activation that folds the same prefix.
- **`perform`** (§4.1): the one place a workflow entity performs I/O, launched off the command path so no handler awaits it.

The third method, `settle` (§4.2), is an ordinary entity decision (entity §4.2), differing from a handler only in that the workflow holds the command it answers.

`Workflow` extends `Entity`, so `Self::State`, `Self::Event`, and `EntityCtx<Self>` below mean what they mean for any entity. Its two handler supertraits make the wiring a compile-time obligation, the posture entity takes for its own calls (E10): an entity that has not routed `Tick` to `Engine::advance` and `Settled` to `Engine::settled` does not compile. No blanket impl supplies those handlers (the orphan rule forbids one), so a consumer writes two small forwarders. The consumer holds an `Engine` in its behavior and forwards four hooks to it: `on_activate`, `can_hibernate`, and the two handlers. One further obligation is not a hook: every handler of the consumer's own whose records call for work MUST call `Engine::tick` with the state it was given (Appendix A). That call is the cycle's entry after a commit (§3.2 item 1), and on an activation's first handler it classifies the orphan set (§4.4). The whole surface:

```rust
pub trait Workflow: Entity + EntityHandler<Tick> + EntityHandler<Settled<Self>> {
    /// An effect's identity within this entity (§3.1). Re-derived, never journaled,
    /// but it rides the `Settled` command (§4.2) and keys the claim and backoff
    /// tables (§3.3, §3.4), so the bounds are the ordinary command bounds, plus
    /// `Debug`: the checker-facing events render it (§8).
    type Key: SerializationRequirement + Clone + Ord + Debug;
    /// One unit of work performed outside the entity. Opaque to the workflow; it is
    /// carried from `pending` to `perform` and never crosses a wire.
    type Effect: Send + 'static;
    /// What an effect returns. Rides `Settled` back onto the command path (§4.2);
    /// `Clone` because `EntityRef::tell` takes it (entity §4.3).
    type Outcome: SerializationRequirement + Clone;

    fn pending(&self, state: &Self::State) -> Vec<Pending<Self::Key, Self::Effect>>;  // §3.1
    fn perform(&self, effect: Self::Effect, cx: Perform<Self::System>)                // §4.1
        -> impl Future<Output = Result<Self::Outcome, EffectError>> + Send + 'static;
    fn settle(&self, state: &Self::State, key: &Self::Key,                            // §4.2
              outcome: Result<Self::Outcome, EffectError>, ctx: &EntityCtx<Self>)
        -> impl Future<Output = Vec<Self::Event>> + Send;
}
```

```text
        command ──► handler (entity §4.2)  ──► records ──► output gate (entity §6)
                    a pure decision                          durable on a quorum
                                                                    │
                                                                    ▼
                                                          ┌─── the workflow ───┐
                                                          │ desired =       │
        ┌───────────────────────────────────────────────► │  pending(state) │
        │                                                 │ launch desired  │
        │                                                 │  minus claimed  │
        │                                                 └────────┬────────┘
        │                                                          │ Spawner
        │                                                          ▼
        │                                             ══ the only I/O: perform (§4.1) ══
        │                                                          │
        │  records ◄── settle (a pure decision, §4.2) ◄── Settled (an ordinary command)
        └──────────────────────────────────────────────────────────┘
```

A commit, of a command's records or of a settled outcome's, enters the cycle (§3.2 lists every entry point). It exits at **quiescence** (§6): nothing in flight and every member of `pending` at rest, in particular `pending` empty. The entity is then an ordinary waiting entity and MAY hibernate.

### 2.2 Vocabulary

| Term | Definition |
|---|---|
| **Effect** | One unit of work performed outside the entity: a model call, a sandboxed tool call, an outbound message, a capture. The consumer's own type; opaque to the workflow. |
| **Key** | An effect's identity within its entity (`Workflow::Key`). Consumer-chosen, **re-derived and never journaled**, and **unique over the entity's whole journal**, not merely within one desired set (§3.1). |
| **Desired set** | `pending(state)`: the keyed effects the committed state calls for, at one instant (§3). |
| **Engine** | One reconciliation of the desired set against what this activation has launched, run at each of §3.2's entry points. |
| **Claim** | An activation's record that it has launched a key. Ephemeral, per-activation, never journaled (§3.3). |
| **In flight** | Claimed and not yet settled. |
| **Settle** | To fold one effect's outcome into records (§4.2). The outcome is durable once those records commit. |
| **Orphaned** | In the desired set an activation first sees: launched by a previous activation, if at all, and of unknown fate (§4.4). |
| **Straggler** | An outcome whose key is no longer in the desired set (§4.5). Discarded, never settled. |
| **At rest** | A member of the desired set neither in flight nor due to launch, because its backoff has reached the cap (§3.4). A consumer-driven tick wakes it (§3.2); unattended, only the next activation moves it (§6). |
| **Quiescent** | Nothing in flight and every member of the desired set at rest, in particular an empty set. The workflow permits hibernation exactly then (§6). |

---

## 3. The desired set

### 3.1 `pending` is a pure function of committed state

```rust
fn pending(&self, state: &Self::State) -> Vec<Pending<Self::Key, Self::Effect>>;
```

`pending` MUST be pure and deterministic. It runs on the live path after every commit and on the first tick of every activation, and the two MUST agree, the obligation `Entity::apply` carries (entity §4.1, E2): a resumed activation that disagreed with its predecessor about outstanding work would abandon or duplicate it. It MUST NOT perform I/O, read the clock, or use entropy. It MUST NOT read the activation's ephemeral state (the claims §3.3, the backoff table §3.4, any binding the consumer holds), or it would no longer be a function of the journal. The consumer upholds this last rule; the layer cannot, since `pending` takes `&self` and a behavior that reached into its own binding or asked `Engine::quiescent` would compile. `Perform`, by contrast, carries no handle to the entity's state, journal, or facets, so that rule is enforced (§4.1).

It is a *set*, not a queue of dispatches. A queue records what the loop decided, so it must be journaled, kept consistent with the work it describes, and reconciled after a crash; a set re-derived from committed state has one copy and recovers by recomputing.

A `Pending` carries the key, the effect, and the effect's recovery policy:

```rust
pub struct Pending<K, E> {
    pub key: K,
    pub effect: E,
    pub on_orphan: OnOrphan,   // Reexecute | Interrupt (§4.4)
}
```

`OnOrphan` is **defined here and consumed above**: the agent runtime's `ToolDecl::on_orphan` (agent §5.2) holds this type, re-exported. The agent keeps of its own only the *outcome* an interrupted call synthesizes, a `ToolError` its model reads, where a workflow mints an `EffectError` (§4.3).

The workflow **never journals the key**: re-derived with the desired set, it needs no stability across deployments and no registry, only determinism within one fold. It needs `Key: SerializationRequirement + Clone + Ord` because it rides the `Settled` command through the mailbox (§4.2), and an entity command is a `Message` carrying the serialization bound whether or not it leaves the node (entity §4.2); and because it orders the claim and backoff tables (§3.3, §3.4). A key taken from a `CallId` the consumer already journals meets all of that; an ordinal key must be derived from state, not from a counter in memory.

**A key MUST be unique over the entity's whole journal**, not merely within one desired set. The straggler filter (§4.5) tests only presence in the set, so a key that leaves and later re-enters (a retried call at the same call site, a second run reusing a step ordinal) would let an outcome launched under the first occurrence be settled as the second's. Only the consumer knows what distinguishes two visits to a call site, so uniqueness is its obligation; the agent runtime's call ids and step ordinals repeat across runs, so its `StepKey` qualifies each with the turn. The workflow deliberately offers no per-launch generation: it would be ephemeral state the desired set cannot see (W1), and re-deriving it after a crash is the problem the desired set exists to avoid.

### 3.2 The workflow cycle

A tick MUST run:

1. after **every** commit of records, a command's or a settled outcome's (§4.2);
2. once after **rehydration**, as the activation's first tick (entity §10), ticked from `on_activate`: the tick that resolves the orphan set (§4.4);
3. when a key's **backoff elapses** (§3.4): the `Engine` sleeps the delay on `Clock` through `Spawner` and re-enters through a tick. It re-enters once more, without delay, when a key **reaches the cap** and comes to rest, because the settle that put it there committed nothing and only a tick can conclude quiescence (§6);
4. on an explicit **tick**, delivered as an ordinary self-addressed command: how a consumer re-enters the cycle after an event that commits nothing.

Entry point 3 is required: a failed settle and a settle that deliberately returns no records (§4.2) both leave their key in the desired set and commit nothing, so neither re-enters through entry point 1, and without the timer a backed-off effect is stranded and **W2 does not hold**.

Entry point 2 launches directly but **cannot commit**: entity journals only through the per-command protocol (entity §6), and `on_activate` returns no events and sees no state (entity §4.1). An orphan the first tick resolves rather than relaunches (§4.4) therefore enters the mailbox as a synthesized `Settled` (§4.2) and commits as an ordinary command. The orphan set is *classified* at the activation's first sight of committed state, which is not always the first tick: a command already in the mailbox is handled ahead of the rehydration tick, and its handler sees the committed state before its own records (on a fresh activation, exactly the rehydrated state). So the first handler that re-enters the cycle (entry point 1 or 4, through `Engine::tick`, which takes the state for this reason) or delivers a `Settled` classifies from the state it was given. Only the resolution rides the mailbox, because a read of committed state can produce a message but not a record.

A tick a consumer's handler drives (`Engine::tick`, entry point 1 or 4) also **wakes every at-rest key** (§3.4): it drops the key's delay and count, so the tick launches it and its schedule starts over. The engine's own re-entries (the rehydration tick, the tick after its own settle, the backoff timer) leave at-rest keys alone. That is the line between the engine driving itself and someone asking, and it lets §6 hibernate on a capped-out key without abandoning it.

Each tick:

1. computes `desired = pending(state)` over the **committed** state;
2. releases every claim and backoff count whose key is absent from `desired`: the journal considers the effect finished (§3.3);
3. for each member of `desired` that this activation has not claimed and whose backoff (§3.4) has elapsed, takes the claim and launches `perform` through `Spawner` (core §4.6), including every at-rest key when the tick is a consumer's;
4. if nothing is in flight and every member of `desired` is at rest, marks the activation **quiescent** (§6).

The workflow MUST NOT await a launch: step 3 hands the future to the spawner and returns, and the outcome arrives later as a `Settled` command (§4.2). An entity with a hundred outstanding effects holds no thread and, once quiescent, no memory (entity §10).

Because a tick launches the whole unclaimed desired set, a consumer holding a gate the set cannot express (an environment it must open or reacquire, which is activation state and no function of any fold) MUST settle that gate before it ticks, never inside a launch. The agent runtime's environment pre-pass is this rule (agent §5.4, §5.5, §5.6).

Step 2 bounds the claim table by the work outstanding rather than the work ever done, and lets a consumer re-request the same *call site* later (a retried tool call under a fresh journal record) without the workflow mistaking it for the one already done. It does **not** license reusing a *key*: §3.1's uniqueness rule keeps §4.5 exact once a claim is released.

### 3.3 Launch-once is per-activation, and the record is the durable fact

A claim is **ephemeral activation state**: rebuilt from nothing on every activation and never journaled, entity's own posture for the launch guard (entity §7.17). Journaling a claim would make the *launch* the durable fact, and the launch is what a crash loses track of; the workflow makes the **settled record** the durable fact and treats every launch as speculative. Hence:

- **Within one activation, an effect is launched at most once per claim** (W3). A tick triggered by an unrelated commit does not relaunch an effect in flight.
- **Across activations, an effect MAY be launched more than once.** A crash between a launch and its settled record leaves the desired set unchanged, so the next activation launches it again, and a deposed leader not yet aware of it MAY overlap its launch with its successor's. The fence guards the record, not the world (agent §5.5). A consumer whose effect is unsafe to re-run declares `Interrupt` (§4.4).

### 3.4 Backoff

A settle that commits nothing (§4.2) leaves its key in the desired set, where the next tick would relaunch it at the rate the workflow cycles. The workflow MUST therefore keep, in activation state, a per-key count of consecutive settles that committed nothing, and delay that key's next launch by a capped exponential backoff timed by `Clock` and jittered from `Entropy` (core §4.6), so the delay is virtual and seed-reproducible under simulation (core §18.1). The elapsing delay is a tick entry point (§3.2 item 3). A settle whose records commit clears the key's count.

The trigger is the empty commit, not the failure. An `Err(EffectError)` (§4.3) backs its key off only when the consumer folds it into no records; a settle that journals what the failure means commits, brings the next tick, and clears the count. An `Ok` outcome the consumer journals nothing for backs off exactly like a failure.

A re-activation starts the count at zero. The workflow imposes **no attempt bound**: only the consumer knows what the work is worth, and the agent runtime bounds it with a budget (agent §9.1). The workflow bounds the *delay*, and a key backed off to the cap is **at rest** (§6) and no longer holds the activation resident; otherwise one permanently failing effect would pin the activation forever.

---

## 4. Effects

### 4.1 The seam

```rust
fn perform(&self, effect: Self::Effect, cx: Perform<Self::System>)
    -> impl Future<Output = Result<Self::Outcome, EffectError>> + Send + 'static;
```

`perform` is the **only** place a workflow entity performs I/O, and it runs outside the activation, in a task the workflow launched, holding no lock on the entity and no claim on its mailbox. It MUST NOT touch the entity's state, journal, or facets, and cannot: `Perform` carries only the entity's name and the system's seams (`Clock`, `Entropy`, `Spawner`, core §4.6). The consumer's own seams (model, sandbox, network) are injected into the behavior at activation (entity Appendix A, the `entities_named` factory) and cloned out of `&self` before the future is returned. `Perform` is owned and the future `'static` because the launched task outlives the call that returned it.

A consumer's `Effect` type is where its seams appear: the agent runtime's is, in outline, `Model | Tool | Delegate | Cancel | Fail`, one arm per seam it dispatches onto.

### 4.2 Settling

An effect's outcome returns as an ordinary command, `Settled { key, outcome }`, through the entity's mailbox, ordered against every other command by the input gate (entity §6). The workflow holds the manifest and the handler; the consumer sees only:

```rust
fn settle(
    &self,
    state: &Self::State,
    key: &Self::Key,
    outcome: Result<Self::Outcome, EffectError>,
    ctx: &EntityCtx<Self>,
) -> impl Future<Output = Vec<Self::Event>> + Send;
```

`settle` is a **decision** under the rules of any `EntityHandler` decision (entity §4.2): it MUST NOT mutate state, perform durable I/O, or await anything. Its records make the outcome durable once the output gate commits them, and the workflow itself ticks after that commit (§3.2), so a consumer wires nothing beyond forwarding the two commands. Returning no records is legal and commits nothing (entity §7.5): it says "this outcome changed nothing", and leaves the key in the desired set for the backoff timer's tick (§3.2 item 3).

There is no reply: `Settled` is delivered as a `tell` (core §3.3), with no caller to answer. Anything waiting on the result waits on the record, not on the message (§7.1).

### 4.3 Effect failure is a value

An effect that fails does not fail the entity. The workflow settles it as `Err(EffectError)` and the consumer decides what it means:

| `EffectError` | Meaning |
|---|---|
| `Timeout` | A bound the consumer's `perform` set on itself elapsed. Its effects MAY still land; only the outcome is bounded. |
| `Interrupted` | An orphaned effect declaring `OnOrphan::Interrupt` was resolved on resume (§4.4). Nothing is known about whether it ran. |
| `Lost(String)` | The environment the effect needed is gone. |
| `Failed(String)` | The effect ran and failed. |

**The workflow mints two variants**: `Interrupted` for an orphan resolved on resume (§4.4) and `Failed` for a panic caught at the task boundary. The other two are what a consumer's `perform` returns: the workflow neither bounds an effect nor interprets its failure, and a consumer that wants a per-effect timeout takes it from the `Clock` on `Perform` (§4.1), as the agent runtime's per-tool bound does (agent §5.3 item 3). `EffectError` and the agent runtime's `ToolError` (agent §5.4) stay distinct because a tool failure carries a payload the *model* reads; the four variants map one to one in the agent runtime's `settle`.

As in agent §5.4 one layer up, a failing effect is information for the consumer's fold, not a fault that tears down an activation holding durable state; the workflow has **no** failure mode that ends an entity. A panic inside the future `perform` returns is caught at the task boundary and settled as `Failed`; the synchronous call to `perform` runs on the command path, where a panic is the entity's to answer for like any handler's.

### 4.4 Orphaned effects and resume

An effect's intent is journaled before it launches and its outcome after (§5). A crash or migration between the two leaves it **orphaned**: the journal says it was wanted, and nothing says whether it ran.

**The desired set over the committed state an activation first sees is its orphan set**: every member was put there by a record committed before this activation existed, and none can yet be claimed by it. That first sight is the first tick or the first command handled ahead of it (§3.2); every member appearing afterwards is fresh. A consumer handler that commits work without re-entering the cycle leaves classification to the first tick, which reads that work as orphaned: conservative for `Interrupt`, exact for `Reexecute`. The workflow MUST resolve each member of the orphan set by its declared policy:

- `OnOrphan::Reexecute`: launch it, as an ordinary launch. The consumer has declared the effect idempotent, self-deduplicating, or cheap enough that a second run beats an unknown one. A failure the interruption caused surfaces per §4.3.
- `OnOrphan::Interrupt`: settle it as `Err(EffectError::Interrupted)` without a launch. The settle still commits through the ordinary barrier, riding the mailbox as a synthesized `Settled`, because the first tick cannot journal (§3.2). The consumer decides what a side effect of unknown fate means; the agent runtime hands it to the model (agent §5.5).

Neither policy is a default: an unstated policy would silently decide whether to repeat a side effect, and there is no safe guess (`Reexecute` risks duplication, `Interrupt` abandonment).

No member of the orphan set may be dropped in silence (W6): one neither launched nor settled would leave the entity believing work is outstanding that nothing is doing, so quiescence never arrives and the entity never hibernates.

### 4.5 Stragglers

An outcome MAY arrive after its key has left the desired set: a timeout racing the real result, a cancelled effect completing anyway, a previous activation's effect reaching a leader that has since re-derived the world. The workflow MUST **discard** such a `Settled` without calling `settle` (W5).

The test is the desired set itself, so it needs no bookkeeping: a key is in the set exactly while its outcome is not yet journaled, the settle's records remove it, and a second outcome then finds it absent. A consumer needs no resolved-call set of its own, and the input gate (entity §6) admits no second outcome while the first's append is in flight.

Exactness rests on **§3.1's key-uniqueness rule**: the filter decides presence, not identity, so a key that re-entered the set would let a straggler of its first occurrence be settled as the second's outcome. The agent runtime's `StepKey` names the turn for this reason.

The workflow MUST also discard an outcome that arrives after the activation has stepped down on a non-contiguous commit (entity §6, the contiguity guard); the entity host enforces this by refusing the command.

---

## 5. Durability is the entity's

This document specifies **no journal seam**: the workflow's write-ahead barrier *is* the entity's output gate (entity §6, E5).

**An effect is launched only after the records that placed it in the desired set have committed** (W4). No check enforces this; it follows from the cycle's shape. Every entry point of §3.2 reads *committed* state, the only state `pending` is given, so an effect whose intent has not committed is not in the set.

Two inherited consequences:

- **Durability failure pauses; it does not fail.** A shard that cannot reach a quorum returns `Unavailable` (entity §7.3, §11): the record does not commit, the activation steps down, and the work stays in the desired set for the next activation to re-derive and re-drive (entity §10). The workflow invents no outcome it could not record.
- **The fence guards the record, not the world.** Two nodes transiently leading a shard produce one journal (entity §8, E1) but MAY launch the same effect twice (§3.3), so a consumer with externally visible effects must choose its §4.4 policy deliberately.

---

## 6. Quiescence, hibernation, and activation-scoped bindings

An entity with outstanding autonomous work must not be evicted mid-flight, and one with none must not be pinned resident. The workflow derives the answer rather than leaving each consumer a hand-written `can_hibernate` veto (agent §7.2):

> An activation is **quiescent** when nothing is in flight and every member of its desired set is **at rest**: the set is empty, or its remaining members are backed off to §3.4's cap. The workflow MUST report quiescence to `Entity::can_hibernate` (entity §10), permitting hibernation exactly when quiescent and vetoing it otherwise.

The at-rest clause matters because §3.4 imposes no attempt bound: under "the set is empty" alone, one permanently failing effect, such as an owed cancel to a node that is not coming back (§7.2), would veto eviction forever. At the cap the key comes to rest, the debt stays in the fold, and the entity hibernates until a message next ticks it (agent §9.2 item 3, agent §7.5). This is safe because hibernation drops only claims and counts, and the next activation re-derives the identical desired set and drives it (W1). Only *unattended* progress is given up, which the workflow never had: a caller or an alarm (§11) resumes a hibernated entity, never the entity itself.

A consumer MAY narrow this, keeping its own `can_hibernate` conjoined with the workflow's, but never needs to widen it, and the common case needs no code. The agent runtime narrows twice: a turn accepted into its queue but not yet started (agent §3.1, §7.2) is work owed now that no effect represents, and a sandbox mid-open is a `Binding` in its *opening* state (below), which the engine does not see. A launched call still outstanding needs no clause of the consumer's: that is quiescence.

A forced step-down (a leadership move, quorum loss) is involuntary and ignores `can_hibernate` (entity §10). The workflow MUST therefore treat deactivation as unremarkable: claims and backoff counts are dropped, in-flight launches are abandoned, and their outcomes become stragglers at whichever activation next holds the entity (§4.5). Nothing is owed at deactivation, which is what makes a forced step-down safe.

**Activation-scoped bindings.** A consumer often holds an external resource for the life of an activation, such as the agent runtime's sandbox (agent §5.3) or the machine's runtime binding (machine §2.1): opened once, released on every deactivation, and vetoing hibernation while the open is in flight. The workflow supplies this as a component rather than a trait method, because the resource's type and open path are the consumer's: a `Binding<T>` with the states *closed, opening, open, failed*, released on `on_deactivate`. Its failed state settles the effects that needed it rather than retrying invisibly, and its *opening* state joins the hibernation veto, an open in flight being the one piece of in-flight work that is not an effect. A consumer that needs no binding declares nothing.

---

## 7. Callers

### 7.1 Awaiting a durable outcome

Autonomous work outlives the request that started it, so a caller cannot receive it as a reply: a reply handle resolves exactly once within the delivery of one message (core §4.5), and the work spans many. The agent runtime answers with an ack, a registered reply-to, and a later notification (agent §7.3). The workflow supplies that pattern as a **component**, `Subscribers<K, V>`, held in the consumer's activation state, with a normative discipline:

1. The subscriber set MUST NOT be journaled. It is ephemeral activation state, rebuilt by callers re-contacting after a resume: a reply-to is a routing address, not a fact about the work.
2. A notification MUST be sent only **after** the record carrying the outcome commits (entity §6), so no caller holds a completion the journal could lose.
3. A notification is a `tell` (core §3.3): at-most-once, best-effort, **delivery and not the source of truth**. A lost notification is recovered by the caller re-contacting the entity, never by a transparent retry (core §1.2).
4. A caller arriving after the outcome has committed MUST be answered at once from the fold, not registered, or it would wait for a notification already sent.

It is a component and not a trait method because the key and payload are the consumer's, and a consumer with no external callers (one driven entirely by alarms) should carry none of it.

### 7.2 What the workflow does not own, and why that is the point

Two expected features are absent by design: owning either would require the workflow to know what the consumer's work *means*.

**Idempotent submission** (agent §7.4) needs request identity and content equality, both the consumer's (the agent runtime compares a turn's content and literal budget, and rejects a kind mismatch), and both folds over the consumer's own records. A generic version would need a serialization bound, a digest scheme, and a conflict type, and would serve one consumer.

**Cancellation** needs nothing. The agent runtime's propagation (agent §9.2) is: journal the terminal record; the fold moves each unresolved child into an owed-cancels projection; `pending` returns one `Cancel` effect per owed child, under a key of its own; the child's ack journals a delivery receipt that clears the debt. *The owed cancels are a desired set*, so with no cancellation feature the consumer receives:

- **crash-safety**, because the debt is a fold and the next activation re-derives it;
- **launch-once**, because claims already cover it;
- **retry with backoff**, from §3.4;
- **straggler rejection**, from §4.5;
- **completion**, because the receipt record removes the effect from the desired set.

One property is not free: the desired set cannot give up, so a naive quiescence rule would hold the parent resident while the child stayed unreachable. §6's at-rest clause lets the parent hibernate at the backoff cap, and the debt, being a fold, survives.

---

## 8. Observability

The workflow's events are checker-facing and ride the core stream through its application-event extension point (core §16), beside the entity's own (entity §13). They describe the machinery around effects, never their content, which belongs in the consumer's records (agent §10.1).

| Event | Fields | Meaning |
|---|---|---|
| `Launched` | `entity`, `key`, `node` | An effect was claimed and handed to the spawner (§3.2). |
| `Interrupted` | `entity`, `key`, `node` | An orphaned `Interrupt` member was resolved without a launch (§4.4); its synthesized outcome is in the mailbox. Opens the claim its `Settled` closes, which makes W6's "none dropped, none resolved twice" readable off the stream. |
| `Settled` | `entity`, `key`, `node`, `ok`, `committed` | An outcome reached `settle`. `ok` distinguishes `Ok` from `Err`; `committed` says whether the settle's records committed. |
| `Discarded` | `entity`, `key`, `node` | A straggling outcome was dropped (§4.5). |
| `Quiescent` | `entity`, `node` | The desired set came to rest and nothing is in flight (§6). |

Every event carries the `node` hosting the activation, as the entity's own events do (entity §13): under failover a deposed leader running down its launches and its successor are two activations of one entity that the stream can tell apart only by node.

`Settled` fires on **every** settle that runs, whether or not it writes. A settle that returns no records (§4.2) is exactly the one whose key is relaunched under backoff, so an event fired only on commit would leave a `Launched` with no terminator and W3's and W7's windows unclosable; `committed: false` is a checker's only view of the backoff path. When there are records, the workflow stages the event and releases it from the tick after the commit, so a settle whose append fails (the activation stepping down) leaves no `Settled` claiming records that never became durable. A settle that decided no records emits at once.

`Settled` names both the command that carries an outcome onto the command path (§4.2, `settle.rs`) and the event that marks its records committed (`event.rs`): one fact at its two ends, the pairing the agent runtime makes for `RunEnded` (agent §10.4). A checker reads the event; a consumer never sees the command.

The vocabulary holds only what a stream checker cannot reconstruct from the entity's events: activation, deactivation, leadership, and commit are read from `Activated`, `Deactivated`, `LeaderChanged`, and `Committed` (entity §13). `Discarded` exists because a discarded outcome leaves no record anywhere, and W5 would be uncheckable without it.

A consumer's own events (agent §10.4) ride the same stream and are emitted at the same post-commit point.

---

## 9. Conformance

The catalogue is machine-readable as `w_catalogue()` beside the crate's conformance suite, guarded by the same drift test as its siblings (`conformance_catalogue.rs`): the `workflow` site in `spec::catalogue::CATALOGUES` holds the table below equal to the Rust copy in numbering, defining sections, and the suites each row names, and holds the `workflow-cycle` checker's declared coverage equal to the rows that cite it, per `(checker, invariant)` pair. Test files named here are under `crates/workflow/tests/`.

| # | Invariant | Defined in | Verified by | Entity basis |
|---|---|---|---|---|
| W1 | **Desired-set purity.** `pending` is a pure, deterministic function of committed state. Two activations that fold the same journal prefix compute the same desired set, and no ephemeral state (claims, backoff, bindings) is reachable from it. | §3.1 | differential test: the live activation before a crash, its successor, and an offline fold of the journal held equal (`conformance_resume.rs`); re-derivation across an eviction, the outstanding set before and after held equal (`conformance_cycle.rs`); seed-reproducibility sweep (`workflow_swarm.rs`) | E2, E3 |
| W2 | **Launch coverage.** Once faults cease and an activation is live, every member of the desired set is in flight, settled, or waiting out its backoff; none stays unlaunched indefinitely while the activation holds it. The liveness is conditional on the activation, as the agent runtime's A2 and A4 are conditional on contact: a member backed off to the cap is at rest (§6), and an entity hibernated in that state re-derives and re-drives it when a caller or an alarm next reaches it. | §3.2, §3.4, §6 | continuous checker (`workflow-cycle`, coverage); scenario sweeps (`conformance_cycle.rs`), including a settle that commits nothing and a relaunch driven only by the backoff timer; the fault swarm (`workflow_swarm.rs`) | (none) |
| W3 | **Launch-once per claim.** An activation launches each key at most once per claim; a later tick never relaunches an effect in flight. Across activations, re-launch is permitted, and the settled record, not the launch, is the durable fact. | §3.3 | continuous checker (`workflow-cycle`, one `Launched` per claim, never a second before the claim's `Settled` or `Discarded`); crash-window and migration scenarios (`conformance_orphan.rs`); the fault swarm (`workflow_swarm.rs`) | E6 |
| W4 | **Intent before effect.** An effect is launched only after the records that placed it in the desired set have committed. | §5 | journal audit (`conformance_cycle.rs`: every `Launched` follows the `Committed` that admitted its key); the fault swarm (`workflow_swarm.rs`). Not a checker: which commit admitted a key is the consumer's fold to know, and a resumed activation's first launches follow a commit made before it existed, so the stream alone cannot pair them. | E5 (output gate) |
| W5 | **Straggler rejection.** An outcome whose key is absent from the current desired set is discarded, never settled, and writes no record. Exact because keys are unique over the journal (§3.1): the filter never mistakes a later occurrence for the one it launched. | §4.5, §3.1 | continuous checker (`workflow-cycle`, `Discarded` pairing); race scenarios (`conformance_straggler.rs`), including key reuse across runs; the fault swarm (`workflow_swarm.rs`) | E1 |
| W6 | **Orphan resolution.** After rehydration, every member of the first tick's desired set is re-launched or settled `Interrupted` per its declared policy; none is dropped, and none is resolved twice. | §4.4 | crash and migration scenarios (`conformance_orphan.rs`; hibernation cannot leave an effect orphaned, W7); differential resume test (`conformance_resume.rs`); the fault swarm (`workflow_swarm.rs`) | E14 |
| W7 | **Quiescence.** An activation permits hibernation exactly when nothing is in flight and every member of its desired set is at rest; an entity holding launchable work vetoes eviction until that work settles or backs off to the cap, and one hibernated while quiescent re-derives the same set on activation and drives it. | §6, §3.4 | continuous checker (`workflow-cycle`: no `Quiescent` with a claim open, and on the single-node tier no `Deactivated` either; under failover a forced step-down abandons its launches legitimately, §6); idle-eviction scenarios (`conformance_cycle.rs`), including eviction under a capped-out backoff and its re-derivation on the next activation; the fault swarm (`workflow_swarm.rs`) | E6, entity §10 |

The workflow is also held to the core and entity testing contracts (core §18.1, §18.3; entity §14): seed-reproducibility of the full event stream, workflow events included, and fault-coverage accounting proving that effect faults and the entity's own faults fired while workflow traffic flowed.

---

## 10. Testability and deterministic simulation

The workflow adds **one** row to the seams the entity already virtualizes (entity §14, core §18.2): the consumer's seam, which the workflow launches.

| Seam | Production | Simulation |
|---|---|---|
| `Workflow::perform` | the consumer's own seams (the agent runtime's model and sandbox, agent §12.1) | a **scripted effect**: a deterministic function of the key and the seed; seeded latency, failure, a hang, a panic, and crash-mid-effect |

The `workflow` crate itself MUST satisfy core §18.1: no wall clock, no OS threads, no unseeded randomness, every launch through `Spawner` and every delay through `Clock`, with backoff jitter from `Entropy` (§3.4).

Simulating a workflow entity runs the real entity host, gateway, shard, and rehydration code (entity §14) under one seed. The workflow cycle is the only new code on the simulated path, and the agent's loop, which sits on it, adds none.

Under seed control a simulated workflow MUST be able to inject at least: effect latency and failure; a run of failures reaching the backoff cap, and a settle that commits nothing (both re-enter the cycle only through the timer, §3.2 item 3, and both bear on quiescence); a crash between a launch and its settled record (§4.4); an outcome delivered after its key has left the desired set (§4.5); eviction while quiescent, attempted eviction while not, and eviction under a capped-out backoff whose work the next activation must re-derive (§6); and, inherited, shard-leader crash and election mid-write, quorum loss, and eviction mid-command (entity §14).

---

## 11. Future work

- **Alarm-driven ticks.** A tick fired by the alarm facet (entity §7.16), so an entity can hold work it will do *later* without holding an activation. It lifts the agent runtime's caller-driven resumption (agent §7.5, §13) and is one more entry point to §3.2.
- **A step DSL over the workflow.** The `async` `step`/`sleep`/`retry` surface deferred at entity §16, written over the workflow: `step` a memoized effect (entity §7.17), `sleep` an alarm, and the re-entrant cycle what the DSL compiles to.
- **The programmable agent.** The layer above (transcript, model seam, tool dispatch, compaction) with the loop written by the application, specified separately.
- **Effect priority and concurrency bounds.** The desired set is launched in full. A consumer with a hundred outstanding effects and one sandbox needs a bound, which belongs here rather than in each consumer, but its shape (a per-key class, a token count, a fairness rule) should be chosen against a second consumer's needs, not the agent runtime's alone.
- **Cross-entity effects.** An effect whose target is another entity is an ordinary effect (§7.2's cancel is one). Whether the workflow should know that, deduplicating by entity name or riding the sagas entity defers (entity §16), is open.
- **Post-commit emission for consumers.** A consumer's own events owed once a record commits (agent §10.4) are staged by the handler that decides the record and emitted by the tick after the commit, as the engine stages its own (§8). One staging could serve both.

---

## Appendix A: End-to-end example

An entity that fans a job out to N workers, collects their results, and finishes, with no memo and no recovery code. The `workflow` crate's `Fanout` fixture is this example made real.

```rust
// --- The entity: ordinary entity. Records, a fold, a handler (entity §4.1, §4.2).
impl Entity for Fanout {
    type State = FanoutState;                 // { requested: Vec<Task>, done: BTreeMap<TaskId, Answer> }
    type Event = FanoutRecord;                // Requested { tasks } | Answered { task, answer } | Finished
    type Facets = ();
    const ENTITY_TYPE: &'static str = "example.Fanout";
    fn apply(state: &mut FanoutState, r: &FanoutRecord) { /* … */ }

    // The workflow's hooks, forwarded: a fresh activation ticks itself, and
    // hibernation is permitted exactly when the workflow is quiescent (§6).
    async fn on_activate(&mut self, ctx: &EntityCtx<Self>) -> Result<(), BoxError> {
        self.engine.on_activate(ctx);
        Ok(())
    }
    fn can_hibernate(&self, _state: &FanoutState) -> bool { self.engine.quiescent() }
}

// --- The workflow: what the committed state calls for, how to do one, what an outcome means.
impl Workflow for Fanout {
    type Key = TaskId;
    type Effect = Task;
    type Outcome = Answer;

    // Pure (§3.1). Every task requested and not yet answered, which is at once
    // the dispatch list, the recovery set after a crash, and the straggler filter.
    fn pending(&self, state: &FanoutState) -> Vec<Pending<TaskId, Task>> {
        state.requested.iter()
            .filter(|t| !state.done.contains_key(&t.id))
            .map(|t| Pending { key: t.id, effect: t.clone(),
                               on_orphan: OnOrphan::Reexecute })   // a read: safe to repeat
            .collect()
    }

    // The only I/O (§4.1). Off the command path, holding nothing: the seam is
    // cloned out of `&self` before the future is returned.
    fn perform(&self, task: Task, cx: Perform<Self::System>)
        -> impl Future<Output = Result<Answer, EffectError>> + Send + 'static {
        let worker = self.worker.clone();
        async move {
            worker.run(task, cx.system()).await
                .map_err(|e| EffectError::Failed(e.to_string()))
        }
    }

    // A pure decision (§4.2), like any handler: records out, no I/O, no mutation.
    async fn settle(&self, state: &FanoutState, key: &TaskId,
                    outcome: Result<Answer, EffectError>, _ctx: &EntityCtx<Self>)
        -> Vec<FanoutRecord> {
        let answer = match outcome {
            Ok(a)  => a,
            Err(e) => Answer::failed(e),      // a failure is a value (§4.3), not a fault
        };
        let mut out = vec![FanoutRecord::Answered { task: *key, answer }];
        if state.done.len() + 1 == state.requested.len() {
            out.push(FanoutRecord::Finished); // the last answer ends the work
        }
        out
    }
}

// --- The two commands the workflow requires, forwarded (§2.1). A consumer's own
// --- handler that creates work ends with `self.engine.tick(self, state, ctx)`.
impl EntityHandler<Tick> for Fanout {
    async fn handle(&self, state: &FanoutState, _: Tick, ctx: &EntityCtx<Self>)
        -> (Vec<FanoutRecord>, ()) { (self.engine.advance(self, state, ctx), ()) }
}
impl EntityHandler<Settled<Self>> for Fanout {
    async fn handle(&self, state: &FanoutState, msg: Settled<Self>, ctx: &EntityCtx<Self>)
        -> (Vec<FanoutRecord>, ()) { (self.engine.settled(self, state, msg, ctx).await, ()) }
}
```

What is *not* written is the point: no dispatch loop, no in-flight table, no "did I already launch this", no recovery pass, no straggler check, and no `can_hibernate` body of the author's own. A crash after ten of a hundred tasks resumes by folding the journal and calling `pending`, which returns ninety. The entity hibernates once `Finished` commits, because the desired set is then empty (§6).

---

## Appendix B: Crate and module layout

```
workflow/                  # the autonomous entity (this spec): an entity that drives itself
  lib.rs                #   the Workflow trait, Pending, OnOrphan, EffectError, Perform (§3, §4)
  cycle.rs              #   the workflow cycle: its entry points, desired-set
                        #     reconciliation, claims, launch, quiescence (§3.2, §3.3, §6)
  settle.rs             #   the Settled command, its manifest and handler (§4.2)
  orphan.rs             #   first-sight classification and policy resolution (§4.4)
  backoff.rs            #   per-key backoff off Clock/Entropy, the timer that re-enters
                        #     the cycle, and the cap that puts a key at rest (§3.4, §6)
  binding.rs            #   Binding<T>: an activation-scoped external resource (§6)
  subscribers.rs        #   Subscribers<K, V>: the await-a-durable-outcome component (§7.1)
  event.rs              #   the checker-facing event vocabulary (§8)
  testing.rs            #   the continuous W-checkers over that stream, behind the
                        #     `testing` feature, as entity and the agent runtime both ship
  tests/
    support/            #   Fanout (Appendix A, made real), the scripted effect seam
                        #     (§10), the W-catalogue, and the three-node cluster
    conformance_cycle.rs, conformance_straggler.rs,      # the suites §9 names
    conformance_orphan.rs, conformance_resume.rs,
    workflow_swarm.rs, conformance_catalogue.rs
```

Every module is private except `testing`, and the crate root re-exports the public items, so each has one path; `backoff.rs` and `orphan.rs` export nothing at all.

`workflow` depends on `entity`, and directly on `actor-core` for the system seams it launches and delays through and on `actor-serialization` for the bounds a key and an outcome carry onto the command path. It consumes the journal, fence, placement, and resume from entity (§1) and names no durable type of its own: every record a workflow entity writes is the consumer's `Entity::Event`, and the workflow's own state (claims, backoff, subscribers) is ephemeral (§3.3, §3.4, §7.1). It observes the workspace conventions: edition 2024, `unsafe_code = "forbid"`, `clippy::all = "warn"`, serde derives only.
