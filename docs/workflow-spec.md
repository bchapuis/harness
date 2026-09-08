# Workflow: Specification

**Status:** Implemented by the `workflow` crate. The agent runtime depends on it and takes its `OnOrphan` (agent §5.2), and holds its dispatcher against a fold-only desired set (`agent`'s `desired` module, agent §13), but does not yet implement `Workflow`: its environment reconciliation (agent §5.4, §5.5, §5.6) gates a launch from the activation rather than the fold and must move ahead of the cycle first.
**Scope:** The mechanism of autonomy under an entity, specified as its own layer: re-derive the work committed state calls for, launch what is not yet in flight, fold each outcome back, and stop when nothing is left. The entity layer supplies identity, the journal, the single-writer fence, placement, activation, and hibernation. The workflow adds the cycle that reconciles wanted work against launched work, and the properties that make that cycle safe under crash and failover.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Sections of the entity specification ([`entity-spec.md`](entity-spec.md)) are cited as **entity §N** (its invariants as **E1–E21**); the actor framework ([`distributed-actor-spec.md`](distributed-actor-spec.md)) as **core §N**; the agent runtime ([`agent-spec.md`](agent-spec.md)) as **agent §N**; the machine ([`machine-spec.md`](machine-spec.md)) as **machine §N**. Sections of this document are cited as plain **§N**, and its invariants are numbered **W1, W2, …**, apart from every sibling catalogue. The agent runtime citations run against the layering: the agent runtime is the layer this document generalizes, so it is at once the motivation, the worked example, and the measure (Appendix C). The crate dependency runs the other way, `agent` on `workflow`, and the citations turn around with the mechanism rather than with the manifest.

> **Design stance.** An entity waits: it holds durable state, admits a command, decides records, commits them, and replies (entity §4.2, §6). An agent does not wait: its activation drives itself forward until the work it holds is finished, never awaiting I/O inside a handler, because a handler is a pure decision the entity replays (entity §4.1, E2). The agent runtime calls this **autonomy** (agent §3) and builds it once, welded to one agentic policy.
>
> Autonomy is not agentic. "Re-derive the work my committed state calls for, launch what is not yet in flight, fold each outcome back, and stop when nothing is left" names no model, tool, or prompt. This document specifies that mechanism as the **workflow**, one layer below the agent runtime, so that an application can write an agent's loop as its own code with durability, resume, and effect discipline supplied, and so that the properties making autonomy safe under crash and failover (W1–W7, §9) are stated and checked once.
>
> The whole design rests on one decision: **the work outstanding is a pure function of committed state**, never a journaled queue. A set re-derived from the journal needs no reconciliation after a crash, because recovery computes the same function over the same records. Resume stops being a special case, and the same set is the dispatch list (§3.2), the recovery set (§4.4), and the straggler filter (§4.5).

---

## 1. Scope and layering

Everything here is built **on top of** the entity and changes nothing beneath it:

- the **entity** (entity §1–§16) supplies identity, the journal, the fold, the single-writer fence, placement, virtual activation, hibernation, and lossless failover. The workflow adds no journal, no fence, no placement function, and no resume protocol;
- the entity's **output gate** (entity §6) is the workflow's write-ahead barrier. "Intent before effect" (§5) is the gate, used, not a mechanism this document builds;
- **actors** (core §3–§12) host the activation. Every effect launches through `Spawner` (core §4.6), and every outcome returns as an ordinary typed message with a hand-written manifest (core §4.4);
- the **event stream** (core §16, entity §13) carries the workflow's checker-facing events through the core's application-event extension point (§8).

The workflow completes a seam entity opened and left half-built on purpose. The steps facet (entity §7.17) supplies durable step **memoization** and says that "launching a step's effect off the command path and self-`tell`ing its result back is entity-specific (the effect's type is), so the facet supplies the durable half"; the missing half is what every consumer then writes. The entity layer also draws the line this document respects: an entity whose results are already rich journaled events with their own fold, such as the agent's `ModelResponse` and `ToolOutcome` records, **MUST NOT** memoize them a second time under the steps tag, and adopts only the launch-once guard. The workflow therefore never memoizes. The consumer's own records are the durable fact, and the desired set (§3) is re-derived from them. An entity that also wants the memo declares the steps facet beside the workflow; the two compose, and neither knows about the other.

### 1.1 Non-goals

- **A model, a tool, or a transcript.** The workflow knows nothing about inference. `agent` owns all three; a second consumer may own none of them.
- **Idempotent request submission.** Deduplicating a re-submitted request by an application key, comparing request content for equality, and rejecting a mismatch (agent §7.4) are folds over the consumer's own records with the consumer's own notion of equality. To own this the workflow would have to be told what a request is, which is the coupling the layer exists to avoid.
- **Cancellation.** There is no cancel mechanism here, by design. A consumer that cancels journals its terminal record, and its `pending` (§3.1) then returns the propagating sends as ordinary effects, which makes them crash-safe, re-derived, launch-once-guarded, and straggler-filtered by the machinery already specified. §7.2 works the example. A cancellation feature would be a second mechanism doing what the first already does.
- **A step DSL.** A linear `async` `step`/`sleep`/`retry` surface is deferred at entity §16 and stays deferred; the workflow is the re-entrant form such a DSL would be written over, not the DSL.
- **Exactly-once effects.** Impossible above an at-most-once transport (core §7.2) and a fence that guards records rather than the world (agent §5.5). The workflow states what it does guarantee (W3, W6) and states with equal clarity that a re-activation MAY re-launch an effect whose outcome has not committed; entity §7.17 makes the same admission about the memo, for the same reason.
- **Scheduling.** Deadlines that re-activate a hibernated entity with no caller present belong to the alarm facet (entity §7.16). The workflow consumes an alarm where one is declared and mints no timer of its own (§11).

---

## 2. The autonomous entity

### 2.1 Anatomy

A workflow entity is an entity plus one function and one seam:

- **`pending`** (§3.1): the effects its committed state calls for. Pure, deterministic, and folded from the journal like all entity state, so identical on every activation that folds the same prefix.
- **`perform`** (§4.1): the one place a workflow entity performs I/O, launched off the command path so no handler ever awaits it.

The trait carries a third method, `settle` (§4.2), and it is not a third thing to learn: it is an ordinary entity decision under the ordinary rules (entity §4.2), differing from a handler only in that the workflow holds the command it answers.

`Workflow` is a **supertrait of `Entity`**, which is what gives `Self::State`, `Self::Event`, and `EntityCtx<Self>` below their meaning: a workflow entity is an ordinary entity that has also said what work its state calls for. Its other two supertraits make the wiring a compile-time obligation: a workflow entity that has not routed `Tick` to `Engine::advance` and `Settled` to `Engine::settled` does not compile, the posture entity takes for its own calls (E10). No blanket impl supplies the two handlers, because the orphan rule forbids one and entity holds the hand-written form to be normative, so a consumer writes them once, as two small forwarders. The consumer holds an `Engine` in its behavior and forwards four hooks to it: `on_activate`, `can_hibernate`, and the two handlers. The whole surface, elaborated per method in §3 and §4:

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

Everything between them is the workflow's: the cycle that reconciles the two (§3.2), the launch guard that keeps one activation from launching an effect twice (§3.3), the resume discipline that resolves what a previous activation left orphaned (§4.4), the filter that discards a straggling outcome (§4.5), and the quiescence rule that tells the entity host whether it may hibernate (§6).

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

The cycle has one entry and one exit. A commit enters it, of a command's records or of a settled outcome's, and it exits when `pending` is empty and nothing is in flight. That exit is **quiescence** (§6): the entity is an ordinary waiting entity again and MAY hibernate.

### 2.2 Vocabulary

| Term | Definition |
|---|---|
| **Effect** | One unit of work performed outside the entity: a model call, a sandboxed tool call, an outbound message, a capture. The consumer's own type; opaque to the workflow. |
| **Key** | An effect's identity within its entity (`Workflow::Key`). Consumer-chosen, **re-derived and never journaled** (§3.1), and **unique over the entity's whole journal**, not merely within one desired set, which is what makes §4.5 exact. |
| **Desired set** | `pending(state)`: the keyed effects the committed state calls for, at one instant. The workflow's central object: at once the dispatch list (§3.2), the recovery set (§4.4), and the straggler filter (§4.5). |
| **Engine** | One reconciliation of the desired set against what this activation has launched, run at each of §3.2's entry points. |
| **Claim** | An activation's record that it has launched a key. Ephemeral, per-activation, never journaled (§3.3). |
| **In flight** | Claimed and not yet settled. |
| **Settle** | To fold one effect's outcome into records (§4.2). The outcome is durable when those records commit, and not before. |
| **Orphaned** | In the desired set an activation first sees: launched by a previous activation, if at all, and of unknown fate (§4.4). |
| **Straggler** | An outcome whose key is no longer in the desired set (§4.5). Discarded, never settled. |
| **At rest** | A member of the desired set that is neither in flight nor due to launch: its backoff has reached the cap (§3.4). Only the next activation will move it (§6). |
| **Quiescent** | Nothing in flight and every member of the desired set at rest, in particular an empty set. The condition under which the workflow permits hibernation (§6). |

---

## 3. The desired set

### 3.1 `pending` is a pure function of committed state

```rust
fn pending(&self, state: &Self::State) -> Vec<Pending<Self::Key, Self::Effect>>;
```

`pending` MUST be pure and deterministic. It runs on the live path after every commit and on the first tick of every activation, and the two MUST agree, the same obligation `Entity::apply` carries (entity §4.1, E2) for the same reason: a resumed activation that disagreed with the one it replaced about what work is outstanding would abandon work or duplicate it. It MUST NOT perform I/O, read the clock, or use entropy. It MUST NOT read the activation's ephemeral state: the claims (§3.3), the backoff table (§3.4), and any binding the consumer holds are invisible to it by construction, because a function that saw them would no longer be a function of the journal.

This is the document's central decision, and it is a *set* rather than a queue of dispatches for a reason. A queue records what the loop decided to do; it must therefore be journaled, kept consistent with the work it describes, and reconciled after a crash. Those are three obligations, each a place to be wrong. A set re-derived from committed state has none of them: there is one copy, so nothing to keep consistent, and recovery computes the same function over the same journal, so nothing to reconcile. **Resume stops being a special case.** The agent runtime today writes its orphaned recovery separately from its dispatch (`resolve_orphans` beside `dispatch_pending`) and keeps the two agreeing by inspection; under this rule they are the same call.

A `Pending` carries the key, the effect, and the effect's recovery policy:

```rust
pub struct Pending<K, E> {
    pub key: K,
    pub effect: E,
    pub on_orphan: OnOrphan,   // Reexecute | Interrupt (§4.4)
}
```

`OnOrphan` is **defined here and consumed above**. The agent runtime's `ToolDecl::on_orphan` (agent §5.2) holds this type, re-exported, because a `ToolDecl`'s recovery policy is exactly what its `Pending` carries. One decision, one type. What the agent runtime keeps of its own is the *outcome* an interrupted call synthesizes: a `ToolError` its model reads, where a workflow mints an `EffectError` for the consumer's fold (§4.3).

The workflow **never journals the key**. It is re-derived on every activation as part of the desired set, so it needs no stability across deployments and no registry, only determinism within one fold. What it does need is stated rather than left to the reader: `Key: SerializationRequirement + Clone + Ord`. It rides the `Settled` command through the entity's mailbox (§4.2), and an entity command is a `Message` (entity §4.2), which carries the serialization bound whether or not the command leaves the node; and it orders the claim and backoff tables (§3.3, §3.4). A consumer keying by a `CallId` it already journals gets all of that for free; one keying by an ordinal must derive the ordinal from state, not from a counter in memory.

**A key MUST be unique over the entity's whole journal**, not merely within one desired set. Nothing else makes the straggler filter of §4.5 exact: that filter's only test is presence in the set, so a key that leaves the set and later re-enters (a retried call at the same call site, a second run reusing a step ordinal) would let an outcome launched under the first occurrence be settled as the second's. Uniqueness is the consumer's obligation because only the consumer knows what a call site is and what distinguishes two visits to it; the agent runtime's own synthesized call ids and step-keyed claims repeat across runs today, and must be qualified by the turn to sit here. A consumer that cannot make its keys unique needs a per-launch generation, which this document withholds on purpose: a generation would be ephemeral state the desired set cannot see (W1), and re-deriving it after a crash is the problem the desired set exists to avoid.

### 3.2 The workflow cycle

A tick MUST run:

1. after **every** commit of records, a command's or a settled outcome's (§4.2);
2. once after **rehydration**, as the activation's first tick (entity §10), ticked from `on_activate`: the tick that resolves the orphan set (§4.4);
3. when a key's **backoff elapses** (§3.4): the `Engine` sleeps the delay on `Clock` through `Spawner` and re-enters through the tick below. It re-enters once more, without a delay, when a key **reaches the cap** and comes to rest, because the settle that put it there committed nothing and only a tick can conclude quiescence (§6);
4. on an explicit **tick**, delivered as an ordinary self-addressed command: how a consumer re-enters the cycle after an event that commits nothing.

Entry point 3 is not a convenience. A settle that fails and a settle that returns no records on purpose (§4.2) both leave their key in the desired set and both commit nothing, so neither can re-enter through entry point 1; without a timer-driven tick a backed-off effect is stranded and **W2 does not hold**. The sleep is virtual under simulation and its jitter comes from `Entropy`, so the relaunch lands at an instant one seed reproduces (§10).

Entry point 2 launches directly but **cannot itself commit**: entity journals only through the per-command protocol (entity §6), and no rehydration hook can persist (entity §4.1, where `on_activate` returns no events and sees no state). An orphaned effect the first tick resolves rather than relaunches (§4.4) therefore enters the mailbox as a synthesized `Settled` (§4.2) and commits as an ordinary command, ordered like the tick against whatever command woke the activation. The orphan set is *classified* at the activation's first sight of committed state, which is not always the first tick. A command already in the mailbox is handled ahead of the rehydration tick, and its handler sees the committed state before its own records, on a fresh activation the rehydrated state exactly. So the first handler that re-enters the cycle (entry point 1 or 4, through `Engine::tick`, which takes the state for this reason) or delivers an outcome (`Settled`) classifies from the state it was given. Only the resolution rides the mailbox, because a read of committed state can produce a message but not a record.

Each tick:

1. computes `desired = pending(state)` over the **committed** state;
2. releases every claim and backoff count whose key is absent from `desired`: the journal considers the effect finished, and the claim's job is done (§3.3);
3. for each member of `desired` that this activation has not claimed and whose backoff (§3.4) has elapsed, takes the claim and launches `perform` through `Spawner` (core §4.6);
4. if nothing is in flight and every member of `desired` is at rest, marks the activation **quiescent** (§6).

The workflow MUST NOT await a launch. Step 3 hands the future to the spawner and returns; the outcome arrives later as a `Settled` command (§4.2). The activation never blocks, which is what lets an entity with a hundred outstanding effects hold no thread and, once quiescent, no memory (entity §10).

Step 2 is not bookkeeping. Releasing the claim of a key that has left the desired set bounds the claim table by the work outstanding rather than by the work ever done, and lets a consumer re-request the same *call site* later, a retried tool call under a fresh journal record, without the workflow mistaking it for the one already done. It does **not** license reusing the same *key*: §3.1's uniqueness rule stands, and it is what keeps the straggler filter of §4.5 exact once a claim has been let go.

### 3.3 Launch-once is per-activation, and the record is the durable fact

A claim is **ephemeral activation state**: it lives beside the consumer's other per-activation working state, is rebuilt from nothing on every activation, and is never journaled. This is entity's own posture for the launch guard (entity §7.17), and it is deliberate. Journaling a claim would make the *launch* the durable fact, and the launch is exactly what a crash can lose track of. The workflow makes the **settled record** the durable fact instead and treats every launch as speculative.

Two consequences follow, and the second is a limitation stated rather than hidden:

- **Within one activation, an effect is launched at most once per claim** (W3). A tick triggered by an unrelated commit does not relaunch an effect already in flight.
- **Across activations, an effect MAY be launched more than once.** A crash between a launch and its settled record leaves the desired set unchanged, so the next activation launches it again; if the previous leader is deposed but not yet aware of it, the two launches MAY overlap for the duration of the call. The fence guards the record, not the world (agent §5.5). `OnOrphan` (§4.4) is how a consumer chooses which side of that to take per effect, and a consumer whose effect is unsafe to re-run declares `Interrupt` and decides for itself.

### 3.4 Backoff

An effect that settles as `Err(EffectError)` (§4.3), or whose settle commits nothing (§4.2), and that remains in the desired set would otherwise be relaunched by the very next tick, at the rate the workflow cycles. The workflow MUST therefore hold a per-key count of consecutive unproductive settles in activation state and delay the next launch of that key by a capped exponential backoff, timed by `Clock` and jittered from `Entropy` (core §4.6), so the delay is virtual and seed-reproducible under simulation (core §18.1). The elapsing delay is a tick entry point in its own right (§3.2 item 3), because nothing else would re-enter the cycle. A settle whose records commit clears the key's count.

The count is activation state, like the claim: a re-activation starts at zero. The workflow imposes **no attempt bound**, because bounding the work is the consumer's: only the consumer knows what the work is worth, and the agent runtime bounds it with a budget (agent §9.1), the right shape for that consumer and not one this layer should assume. What the workflow does bound is the *delay*, and reaching the cap has a second meaning: a key backed off to the cap is **at rest** (§6) and no longer holds the activation resident. Without that, "no attempt bound" and the quiescence rule would together pin an activation forever on one permanently failing effect.

---

## 4. Effects

### 4.1 The seam

```rust
fn perform(&self, effect: Self::Effect, cx: Perform<Self::System>)
    -> impl Future<Output = Result<Self::Outcome, EffectError>> + Send + 'static;
```

`perform` is the **only** place a workflow entity performs I/O, and it runs outside the activation: in a task the workflow launched, holding no lock on the entity and no claim on its mailbox. It MUST NOT touch the entity's state, journal, or facets. It has no access to any of them, which is how the rule is enforced rather than asked for. `Perform` carries what an effect legitimately needs from outside itself: the entity's name and the system's seams (`Clock`, `Entropy`, `Spawner`, core §4.6). Whatever else an effect needs, such as the consumer's own model, sandbox, or network seams, injected into the behavior at activation (entity Appendix A, the `entities_named` factory that "lets the runtime inject per-node seam handles into each fresh activation"), the consumer clones out of `&self` before returning the future. `Perform` is owned rather than borrowed and the future is `'static`, because the task the workflow launches outlives the call that returned it.

A consumer's `Effect` type is where its seams appear. The agent runtime's would be, in outline, `Model(ModelRequest) | Tool(CallId, ToolCall) | Delegate(…) | Cancel(…)`, and its `perform` a four-arm dispatch onto the model seam, the sandbox seam, and a child `Submit`, which is the whole of what its four current launch paths do once the launch-once and resume logic around them is removed.

### 4.2 Settling

An effect's outcome returns to the activation as an ordinary command, `Settled { key, outcome }`, delivered through the entity's mailbox like any other and therefore ordered against every other command by the input gate (entity §6). The workflow holds the manifest and the handler; the consumer sees only:

```rust
fn settle(
    &self,
    state: &Self::State,
    key: &Self::Key,
    outcome: Result<Self::Outcome, EffectError>,
    ctx: &EntityCtx<Self>,
) -> impl Future<Output = Vec<Self::Event>> + Send;
```

`settle` is a **decision**, under exactly the rules an `EntityHandler` decision follows (entity §4.2): it MUST NOT mutate state, MUST NOT perform durable I/O, and MUST NOT await anything. It returns the records that make the outcome durable; the entity's output gate commits them; the commit re-enters the workflow (§3.2). The workflow itself launches that tick, so a consumer wires nothing beyond forwarding the two commands. Returning no records is legal and commits nothing (entity §7.5). It is how a consumer says "this outcome changed nothing", and it leaves the key in the desired set for the workflow the backoff timer brings (§3.2 item 3), since this settle committed nothing to bring one.

There is no reply, by design. `Settled` is delivered as a `tell` (core §3.3), because there is no caller to answer: the effect's result is durable when its records commit, and anything waiting on it waits on the record, not on the message (§7.1).

### 4.3 Effect failure is a value

An effect that fails does not fail the entity. The workflow settles it as `Err(EffectError)` and hands it to the consumer, which decides what it means:

| `EffectError` | Meaning |
|---|---|
| `Timeout` | A bound the consumer's `perform` set on itself elapsed. Its effects MAY still land; only the outcome is bounded. |
| `Interrupted` | An orphaned effect declaring `OnOrphan::Interrupt` was resolved on resume (§4.4). Nothing is known about whether it ran. |
| `Lost(String)` | The environment the effect needed is gone. |
| `Failed(String)` | The effect ran and failed. |

**Only `Interrupted` is the workflow's to mint** (§4.4). The other three are what a consumer's `perform` returns: the workflow neither bounds an effect nor interprets its failure, and a consumer that wants a per-effect timeout takes it from the `Clock` on `Perform` (§4.1). The agent runtime's per-tool bound (agent §5.3 item 3) is exactly that, one layer up. `EffectError` and the agent runtime's `ToolError` (agent §5.4) stay distinct types, because a tool failure carries a payload the *model* reads and an effect failure carries nothing; the four variants map one to one, and the agent runtime's `perform` does the mapping.

This mirrors agent §5.4 one layer down, for the same reason: a failing effect is information the consumer's fold should record and react to, not a fault that should tear down an activation holding durable state. The workflow itself has **no** failure mode that ends an entity. A panic inside `perform` is caught at the task boundary and settled as `Failed`, so a consumer bug in one effect cannot take the activation with it.

### 4.4 Orphaned effects and resume

An effect's intent is journaled before it launches and its outcome after (§5). A crash or a migration between the two leaves the effect **orphaned**: the journal says it was wanted, and nothing says whether it ran.

The desired set makes the recovery rule short. **The desired set computed over the committed state an activation first sees is its orphan set**, by construction: every member was put there by a record that committed before this activation existed, and no launch of this activation can yet have claimed it. That first sight is the first tick, or the first command handled ahead of it, which sees the state before its own records (§3.2); every member appearing afterwards is fresh. A consumer handler that commits work without re-entering the cycle leaves the classification to the first tick, which then reads that work as orphaned: conservative for `Interrupt`, and exactly right for `Reexecute`. The workflow MUST resolve each member of the orphan set by its declared policy:

- `OnOrphan::Reexecute`: launch it, as an ordinary launch. The consumer has declared the effect idempotent, self-deduplicating, or cheap enough that a second run beats an unknown one. It MAY fail for reasons the interruption created, and surfaces per §4.3.
- `OnOrphan::Interrupt`: settle it as `Err(EffectError::Interrupted)` without launching anything. "Immediately" means without a launch, not without a commit: the settle rides the mailbox as a synthesized `Settled` and commits through the ordinary barrier, because the first tick cannot journal (§3.2). The consumer decides what to do about a side effect of unknown fate. The agent runtime hands exactly this to the model (agent §5.5), the right decision-maker for its case and not one this layer can supply for every case.

Neither policy is a default. A `Pending` names one, because an effect whose recovery policy went unstated would be a silent decision about whether to repeat a side effect, and there is no safe guess: `Reexecute` risks duplication, `Interrupt` risks abandonment, and only the consumer knows which it can afford.

No member of the orphan set may be dropped in silence (W6). A member neither launched nor settled would leave the entity believing work is outstanding that nothing is doing: a quiescence that never arrives, and an entity that never hibernates.

### 4.5 Stragglers

An outcome MAY arrive after its key has left the desired set: a timeout racing the real result, a cancelled effect completing anyway, an effect of a previous activation reaching a leader that has since re-derived the world. The workflow MUST **discard** such a `Settled` without calling `settle` (W5).

The test is the desired set itself, which is why it can be exact and needs no separate bookkeeping. A key is in the set exactly while its outcome is not yet journaled; a settle commits records that remove it; a second outcome for the same key then finds it absent and is dropped. The agent runtime maintains a separate `resolved` set for this today. That set caches a question the desired set already answers, and the input gate (entity §6) closes the window it also covers, since no second outcome is admitted while the first's append is in flight.

Exactness rests on **§3.1's key-uniqueness rule** and fails without it. The filter decides presence, not identity: were a key allowed to leave the set and re-enter under a later occurrence, a straggler of the first would find its key present and be settled as the second's outcome. This is not hypothetical for the reference consumer. The agent runtime's synthesized call ids and step-keyed claims repeat across runs, and its own straggler test is therefore `(turn, call)` rather than the call alone.

The workflow MUST also discard an outcome that arrives after the activation has stepped down on a non-contiguous commit (entity §6, the contiguity guard). The entity host already enforces this by refusing the command.

---

## 5. Durability is the entity's

This document specifies **no journal seam**. The workflow's write-ahead barrier *is* the entity's output gate (entity §6, E5), and the discipline follows from where the workflow sits in the cycle rather than from any mechanism it owns:

**An effect is launched only after the records that placed it in the desired set have committed** (W4). No check enforces this. Every one of §3.2's entry points reads *committed* state, the only state `pending` is given, so an effect whose intent has not committed is not in `pending(state)` and there is nothing to launch. Intent before effect is a consequence of the cycle's shape.

Two inherited consequences, stated because a consumer will meet them:

- **Durability failure pauses; it does not fail.** A shard that cannot reach a quorum returns `Unavailable` (entity §7.3, §11): the record does not commit, the activation steps down, and the workflow does not advance. The outstanding work stays in the desired set, because the journal still calls for it, and the next activation re-derives and re-drives it (entity §10). The workflow invents no outcome it could not record.
- **The fence guards the record, not the world.** Two nodes transiently believing they lead a shard produce one journal (entity §8, E1) but MAY produce two launches of the same effect (§3.3). Consumers whose effects are externally visible must read §4.4 as a choice, not a formality.

---

## 6. Quiescence, hibernation, and activation-scoped bindings

An entity with outstanding autonomous work must not be evicted mid-flight, and an entity with none must not be pinned resident. Today the agent runtime answers this by hand-writing `can_hibernate` to veto eviction while a run is live (agent §7.2). The workflow derives it:

> An activation is **quiescent** when nothing is in flight and every member of its desired set is **at rest**: the set is empty, or its remaining members are backed off to §3.4's cap. The workflow MUST report quiescence to `Entity::can_hibernate` (entity §10), permitting hibernation exactly when quiescent and vetoing it otherwise.

The at-rest clause carries weight. §3.4 imposes no attempt bound, so a rule of "the set is empty" alone would let one permanently failing effect, such as an owed cancel to a node that is not coming back (§7.2), veto eviction forever and pin an activation whose only remaining work is a timer. That is worse than what the agent runtime does today: it abandons a launch after bounded retries, leaves the debt in the fold, and lets the session hibernate until a message next ticks it (agent §9.2 item 3, agent §7.5). At the cap the workflow makes the same trade, and makes it safely, because the debt is a fold and not activation state: hibernation drops the claims and the counts, and the next activation re-derives the identical desired set and drives it (W1). What is given up is *unattended* progress, which the workflow never had: a caller or an alarm (§11) resumes a hibernated entity, never the entity itself.

A consumer MAY narrow this further, keeping its own `can_hibernate` conjoined with the workflow's, but it never has to widen it, and the common case needs no code at all. The agent runtime is the worked case, and it does narrow: a turn accepted into its queue but not yet started (agent §3.1, §7.2) is work owed *now* that no effect represents, so it is invisible to the desired set, and hibernating on it would strand the turn until the next contact. Its `can_hibernate` therefore shrinks to that one clause. The two effect-shaped clauses it carries today, a launched call still outstanding and a sandbox mid-open, are the workflow's: the first as quiescence, the second as `Binding`'s *opening* state below.

The rule composes with the rest of the model because the desired set is journal-derived: an entity evicted while quiescent and re-activated later re-derives the same set, and one that acquires work while hibernated (an alarm, a command) re-derives that work on activation and drives it.

Hibernation is not the only deactivation. A forced step-down, a leadership move or quorum loss, is involuntary and ignores `can_hibernate` (entity §10). The workflow MUST therefore treat deactivation as unremarkable: claims and backoff counts are dropped, in-flight launches are abandoned to whatever they were doing, and their outcomes become stragglers at whichever activation next holds the entity (§4.5). Nothing is owed at deactivation, which is what makes a forced step-down safe.

**Activation-scoped bindings.** A consumer often holds an external resource for the life of an activation, such as the agent runtime's sandbox (agent §5.3) or the machine's runtime binding (machine §2.1), with an open-once lifecycle, a release on every deactivation, and a hibernation veto while the open is in flight. The workflow supplies this as a component rather than a trait method, because the resource's type and its open path are the consumer's: a `Binding<T>` with the states *closed, opening, open, failed*, released on `on_deactivate`, whose failed state settles the effects that needed it rather than retrying invisibly, and whose *opening* state joins the hibernation veto for the duration of the open. An open in flight is an outcome the activation still owes itself, and it is the one piece of in-flight work that is not an effect. A consumer that needs no binding declares nothing.

---

## 7. Callers

### 7.1 Awaiting a durable outcome

Autonomous work outlives the request that started it. A caller that asked for it therefore cannot receive it as a reply: a reply handle is resolved exactly once within the delivery of one message (core §4.5), and the work spans many. The agent runtime meets this with an ack, a registered reply-to, and a later notification (agent §7.3), and every consumer of the workflow meets it the same way.

The workflow supplies the pattern as a **component**, `Subscribers<K, V>`, held in the consumer's activation state, with a normative discipline and no trait method:

1. The subscriber set MUST NOT be journaled. It is ephemeral activation state, rebuilt by callers re-contacting after a resume, like the claims and for the same reason: a reply-to is a routing address, not a fact about the work.
2. A notification MUST be sent only **after** the record carrying the outcome commits (entity §6), so no caller holds a completion the journal could lose.
3. A notification is a `tell` (core §3.3): at-most-once, best-effort, **delivery and not the source of truth**. The committed record is. A lost notification is recovered by the caller re-contacting the entity, never by a transparent retry (core §1.2).
4. A caller arriving after the outcome has committed MUST be answered at once from the fold, not registered; otherwise it waits for a notification that has already been sent.

It is a component and not a trait method because the *key* and the *payload* are the consumer's, and because a consumer with no external callers, one workflow entirely by alarms, should carry none of it.

### 7.2 What the workflow does not own, and why that is the point

Two things a reader will expect here are absent by design, for one reason: the workflow would have to be told what the consumer's work *means* to own them.

**Idempotent submission** (agent §7.4) needs a notion of request identity and content equality. Both are the consumer's (the agent runtime compares a turn's content and its literal budget, and rejects a kind mismatch), and all of it is a fold over the consumer's own records. A generic version would need a serialization bound, a digest scheme, and a conflict type, and would serve one consumer.

**Cancellation** needs nothing, which is the more interesting case. The agent runtime's propagation (agent §9.2) is: journal the terminal record; the fold moves each unresolved child into an owed-cancels projection; a post-commit pass sends `Cancel` to each; the child's ack journals a delivery receipt that clears the debt. Read that against §3: *the owed cancels are a desired set*. A consumer under the workflow expresses them as effects returned by `pending`, one per owed child, keyed by the child's call, and receives, with no cancellation feature anywhere in this document:

- **crash-safety**, because the debt is a fold and the next activation re-derives it (agent §9.2 item 3 hand-builds exactly this);
- **launch-once**, because claims already cover it (the agent runtime's separate `cancels_launched` guard disappears);
- **retry with backoff**, from §3.4 (its bespoke transport-retry loop disappears);
- **straggler rejection**, from §4.5;
- **completion**, because the receipt record removes the effect from the desired set.

One property is not free, and §6 pays for it. The agent runtime's propagation gives up on an unreachable child after bounded retries so the parent can hibernate, and re-launches on the next contact. The desired set has no notion of giving up, so a naive quiescence rule would hold the parent resident for as long as the child stayed unreachable. §6's at-rest clause restores the behavior at the backoff cap rather than at a retry count, the same trade read off a clock instead of a counter, and the debt survives it because it was a fold all along.

A cancellation feature would be a second mechanism doing what the first does. That the agent runtime's most intricate piece of crash-safety reduces to "return it from `pending`" is the strongest evidence available that the desired set is the right primitive.

---

## 8. Observability

The workflow's events are checker-facing and ride the core stream through its application-event extension point (core §16), alongside the entity's own (entity §13). They describe the machinery around effects and carry nothing about their content; the payload is the consumer's and belongs in the consumer's records (agent §10.1).

| Event | Fields | Meaning |
|---|---|---|
| `Launched` | `entity`, `key`, `node` | An effect was claimed and handed to the spawner (§3.2). |
| `Interrupted` | `entity`, `key`, `node` | An orphaned `Interrupt` member was resolved without a launch (§4.4); its synthesized outcome is in the mailbox. Opens the claim its `Settled` closes, which makes W6's "none dropped, none resolved twice" readable off the stream. |
| `Settled` | `entity`, `key`, `node`, `ok`, `committed` | An outcome reached `settle`. `ok` distinguishes `Ok` from `Err`; `committed` says whether the settle's records committed. |
| `Discarded` | `entity`, `key`, `node` | A straggling outcome was dropped (§4.5). |
| `Quiescent` | `entity`, `node` | The desired set came to rest and nothing is in flight (§6). |

Every event carries the `node` hosting the activation, as the entity's own events do (entity §13) and for the same reason: a claim's life is per-activation, and under failover a deposed leader still running down its launches and its successor are two activations of one entity that the stream can tell apart only by the node that emits.

`Settled` fires on **every** settle that runs, whether or not it writes. A settle may return no records (§4.2), and that path is exactly the one whose key stays in the desired set to be relaunched under backoff, so an event that fired only on a commit would leave a `Launched` with no terminator on the stream, and W3's and W7's windows could not be closed. `committed: false` is also how a checker sees the backoff path at all: it is the only signal that an outcome arrived and changed nothing. When there *are* records, the event is emitted after they commit: the workflow stages it and releases it from the workflow that follows the commit, so a settle whose append fails, the activation stepping down, leaves no `Settled` claiming records that never became durable. A settle that decided no records has nothing to wait for and emits at once.

`Settled` names two things on purpose: the command that carries an outcome onto the command path (§4.2, `settle.rs`) and the event that marks its records committed (`event.rs`). They are one fact at its two ends, and the agent runtime makes the same pairing under the same justification for `RunEnded` (agent §10.4); a checker reads the event, and a consumer never sees the command.

The vocabulary is kept to what a stream checker cannot reconstruct from the entity's own events. Activation, deactivation, leadership, and commit are read from `Activated`, `Deactivated`, `LeaderChanged`, and `Committed` (entity §13); the workflow mints no duplicates of them. `Discarded` is present because a discarded outcome leaves no record anywhere. It is the one thing the workflow does that the journal cannot show, and W5 would be uncheckable without it.

A consumer's own events (agent §10.4) ride the same stream and are emitted at the same post-commit point.

---

## 9. Conformance

The catalogue is machine-readable as `w_catalogue()` beside the crate's conformance suite, guarded by the same drift-test pattern as its siblings (`conformance_catalogue.rs`): the `workflow` site in `spec::catalogue::CATALOGUES` holds the table below equal to the Rust copy in numbering, defining sections, and the suites each row names, and holds the `workflow-cycle` checker's declared coverage equal to the rows that cite it, per `(checker, invariant)` pair. Test files named here are under `crates/workflow/tests/`.

| # | Invariant | Defined in | Verified by | Entity basis |
|---|---|---|---|---|
| W1 | **Desired-set purity.** `pending` is a pure, deterministic function of committed state. Two activations that fold the same journal prefix compute the same desired set, and no ephemeral state (claims, backoff, bindings) is reachable from it. | §3.1 | differential tests: the live activation before a crash, its successor, and an offline fold of the journal held equal (`conformance_cycle.rs`, `conformance_resume.rs`); seed-reproducibility sweep (`workflow_swarm.rs`) | E2, E3 |
| W2 | **Launch coverage.** Once faults cease and an activation is live, every member of the desired set is in flight, settled, or waiting out its backoff; none stays unlaunched indefinitely while the activation holds it. The liveness is conditional on the activation, as the agent runtime's A3 and A5 are conditional on contact: a member backed off to the cap is at rest (§6), and an entity hibernated in that state re-derives and re-drives it when a caller or an alarm next reaches it. | §3.2, §3.4, §6 | continuous checker (`workflow-cycle`, coverage); scenario sweeps (`conformance_cycle.rs`), including a settle that commits nothing and a relaunch workflow only by the backoff timer; the fault swarm (`workflow_swarm.rs`) | (none) |
| W3 | **Launch-once per claim.** An activation launches each key at most once per claim; a later tick never relaunches an effect in flight. Across activations, re-launch is permitted, and the settled record, not the launch, is the durable fact. | §3.3 | continuous checker (`workflow-cycle`, one `Launched` per claim, never a second before the claim's `Settled` or `Discarded`); crash-window and migration scenarios (`conformance_orphan.rs`); the fault swarm (`workflow_swarm.rs`) | E6 |
| W4 | **Intent before effect.** An effect is launched only after the records that placed it in the desired set have committed. | §5 | journal audit (`conformance_cycle.rs`: every `Launched` follows the `Committed` that admitted its key); the fault swarm (`workflow_swarm.rs`). Not a checker: which commit admitted a key is the consumer's fold to know, and a resumed activation's first launches follow a commit made before it existed, so the stream alone cannot pair them. | E5 (output gate) |
| W5 | **Straggler rejection.** An outcome whose key is absent from the current desired set is discarded, never settled, and writes no record. Exact because keys are unique over the journal (§3.1): the filter never mistakes a later occurrence for the one it launched. | §4.5, §3.1 | continuous checker (`workflow-cycle`, `Discarded` pairing); race scenarios (`conformance_straggler.rs`), including key reuse across runs; the fault swarm (`workflow_swarm.rs`) | E1 |
| W6 | **Orphan resolution.** After rehydration, every member of the first tick's desired set is re-launched or settled `Interrupted` per its declared policy; none is dropped, and none is resolved twice. | §4.4 | crash and migration scenarios (`conformance_orphan.rs`; hibernation cannot leave an effect orphaned, W7); differential resume test (`conformance_resume.rs`); the fault swarm (`workflow_swarm.rs`) | E14 |
| W7 | **Quiescence.** An activation permits hibernation exactly when nothing is in flight and every member of its desired set is at rest; an entity holding launchable work vetoes eviction until that work settles or backs off to the cap, and one hibernated while quiescent re-derives the same set on activation and drives it. | §6, §3.4 | continuous checker (`workflow-cycle`: no `Quiescent` with a claim open, and on the single-node tier no `Deactivated` either; under failover a forced step-down abandons its launches legitimately, §6); idle-eviction scenarios (`conformance_cycle.rs`), including eviction under a capped-out backoff and its re-derivation on the next activation; the fault swarm (`workflow_swarm.rs`) | E6, entity §10 |

The workflow is also held to the core and entity testing contracts (core §18.1, §18.3; entity §14): seed-reproducibility of the full event stream, workflow events included, and fault-coverage accounting proving that effect faults and the entity's own faults fired while workflow traffic flowed.

---

## 10. Testability and deterministic simulation

The workflow adds **one** row to the table the entity already virtualizes (entity §14, core §18.2). It is the consumer's seam, surfaced here because the workflow is what launches it:

| Seam | Production | Simulation |
|---|---|---|
| `Workflow::perform` | the consumer's own seams (the agent runtime's model and sandbox, agent §12.1) | a **scripted effect**: a deterministic function of the key and the seed; seeded latency, failure, loss, and crash-mid-effect |

The `workflow` crate itself MUST satisfy core §18.1: no wall clock, no OS threads, no unseeded randomness, every launch through `Spawner` and every delay through `Clock`. Backoff jitter comes from `Entropy` (§3.4), so a backed-off relaunch happens at a virtual instant one seed reproduces.

Because the journal, the fence, placement, and resume are the entity's, simulating a workflow entity runs the real entity host, gateway, shard, and rehydration code (entity §14) under one seed. The workflow cycle is the only new code on the simulated path, and once the agent runtime sits on it, the agent's loop is no longer new code on that path at all.

Under seed control a simulated workflow MUST be able to inject at least: effect latency and failure; a run of failures long enough to reach the backoff cap, and a settle that commits nothing, since both re-enter the cycle only through the timer (§3.2 item 3) and both bear on quiescence (§6); a crash between a launch and its settled record (the orphaned case, §4.4); an outcome delivered after its key has left the desired set (the straggler case, §4.5); eviction while quiescent, attempted eviction while not, and eviction under a capped-out backoff whose work the next activation must re-derive (§6); and, inherited, shard-leader crash and election mid-write, quorum loss, and eviction mid-command (entity §14).

---

## 11. Future work

- **Alarm-driven ticks.** A tick triggered by the alarm facet (entity §7.16) rather than by a commit or a caller, so an entity can hold work it will do *later* without holding an activation. This lifts the agent runtime's caller-driven resumption (agent §7.5, §13), and it is small: an alarm's firing is one more entry point to §3.2.
- **A step DSL over the workflow.** The `async` `step`/`sleep`/`retry` surface deferred at entity §16, written over the workflow rather than beside it: `step` becomes a memoized effect (entity §7.17), `sleep` an alarm, and the re-entrant cycle the thing the DSL compiles to.
- **The programmable agent.** The layer above: transcript, model seam, tool dispatch, and compaction, with the loop written by the application. It is the reason this document exists, and it is specified separately.
- **Effect priority and concurrency bounds.** The desired set is launched in full today. A consumer with a hundred outstanding effects and one sandbox needs a bound, and the bound belongs here rather than in each consumer, but its shape (a per-key class, a token count, a fairness rule) should be chosen against a second consumer's needs, not the agent runtime's alone.
- **Cross-entity effects.** An effect whose target is another entity is an ordinary effect today (§7.2's cancel is one). Whether the workflow should know that, deduplicating by entity name or riding the sagas entity defers (entity §16), is open.

---

## Appendix A: End-to-end example

An entity that fans a job out to N workers, collects their results, and finishes. It journals its own records, holds no memo, and writes no recovery code. The `workflow` crate's `Fanout` fixture is this example made real.

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

What is *not* written above is the point. There is no dispatch loop, no in-flight table, no "did I already launch this", no recovery pass, no straggler check, and no `can_hibernate` body of the author's own. A crash after ten of a hundred tasks resumes by folding the journal and calling `pending`, which returns ninety. The entity hibernates when `Finished` commits, because the desired set is then empty (§6), without the author having thought about hibernation at all.

---

## Appendix B: Crate and module layout

```
workflow/                  # the autonomous entity (this spec): an entity that drives itself
  lib.rs                #   the Workflow trait, Pending, OnOrphan, EffectError (§3, §4)
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

`workflow` depends on `entity` (the entity) and through it on `actor-core`. It defines **no journal, no fence, no placement function, and no resume protocol**; each is consumed from entity. It names no durable type of its own: every record a workflow entity writes is the consumer's `Entity::Event`, and the workflow's own state (claims, backoff, subscribers) is ephemeral by specification (§3.3, §3.4, §7.1). It observes the workspace conventions: edition 2024, `unsafe_code = "forbid"`, `clippy::all = "warn"`, serde derives only.

---

## Appendix C: What moves out of the agent runtime

The measure of the seam, still to be taken: the agent runtime does not yet sit on the workflow. Each row is machinery the agent runtime owns today and would consume instead; the last column names what it becomes a case of. One row has landed ahead of the rest, marked *done*: the type was a copy rather than a mechanism, so it moved on its own.

| Today, in `agent/agent.rs` | Under the workflow | Becomes |
|---|---|---|
| `RunScope::launched` (`LaunchGuard<StepKey>`) | claims, in the cycle | W3 |
| `dispatch_pending`, `launch_call`, `launch_open`, `launch_delegation` | `pending` + `perform` | W2, W4 |
| `resolve_orphans`, `RunScope::orphans_resolved` | first-sight classification | W6 |
| `OnOrphan`, `ToolDecl::on_orphan` | the workflow's `OnOrphan`, re-exported (*done*) | §3.1, §4.4 |
| `RunScope::resolved` | the desired set itself, once the keys are unique (§3.1) | W5 |
| `Advance` and its scheduling | the workflow cycle's entry points | §3.2 |
| `ModelDone`, `ToolDone`, `CancelDone` | one `Settled` | §4.2 |
| `cancels_launched`, `propagate_cancels`, `TRANSPORT_RETRIES`, `propagation_backoff` | owed cancels as ordinary effects | §7.2, §3.4 |
| `SandboxSlot`, `release_sandbox`, the `on_deactivate` release | `Binding<Sandbox>` | §6 |
| `can_hibernate`'s effect clauses (a launched call outstanding, `SandboxSlot::Opening`) | derived quiescence and `Binding`'s *opening* state | W7 |
| `Activation::subscribers`, `subscribe`, `notify`, `notify_finished` | `Subscribers` | §7.1 |
| `EventOutbox` | post-commit emission in the cycle | §8 |

What stays is what the agent runtime is about: the transcript and its fold, the model seam and its request assembly, the tool registry and the tier discipline, kinds and their digest, budgets, delegation policy, and idempotent submission (§1.1). Two smaller things stay with it and are worth naming, because a table of departures reads as a claim that nothing else is left: `can_hibernate` keeps its queued-turn clause, work owed that no effect represents (§6), and the agent runtime's `StepKey` and synthesized call ids must be qualified by the turn to satisfy §3.1's uniqueness rule, since they repeat across runs today and the workflow's straggler filter is exact only if they stop. That list describes an agent runtime. The list above describes nothing in particular, which is why it belongs one layer down.
