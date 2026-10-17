# Workflow: Specification

The key words **MUST**, **MUST NOT**, and **MAY** carry the meanings defined in RFC 2119.

## 1. Scope

A **workflow** is an entity that drives itself: its activation re-derives the work its committed state calls for, launches what is not yet in flight, folds each outcome back into records, and stops when nothing is left, never awaiting I/O inside a handler. The `workflow` crate is that cycle with no model, tool, or prompt in it, so the properties that keep autonomy safe under crash and failover (W1 to W7) are stated once.

It builds on the entity runtime (entity §3, §5, §7) for identity, the journal, the fold, the single-writer fence, activation, and hibernation, and adds no journal, fence, or resume protocol: every record a workflow entity writes is the consumer's own `Entity::Event`. Effects launch through the actor runtime's seams (actor §2.12.1). The agent runtime builds on it: `Agent` implements `Workflow` (agent §3).

The design rests on one decision: **the work outstanding is a pure function of committed state**, never a journaled queue. Recovery computes the same function over the same records, so resume is not a special case, and one set is at once the dispatch list (§3.2), the recovery set (§4.3), and the straggler filter (§4.4).

### 1.1 Non-goals

- **A model, a tool, or a transcript**, the agent runtime's.
- **Memoization.** The consumer's records are the durable fact; an entity that also wants step memoization declares the steps facet beside the workflow (facets §4.6).
- **Idempotent submission and cancellation**, folds over the consumer's records. A cancel journals a terminal record from which `pending` derives the propagating sends as ordinary effects.
- **Exactly-once effects.** Impossible above an at-most-once transport (actor §4) and a fence that guards records, not the world: a re-activation MAY relaunch an effect whose outcome has not committed.
- **Scheduling.** The only timer is the per-key backoff inside an activation (§3.4).

## 2. Seams

### 2.1 `Workflow`: what the consumer supplies

A plug point: the consumer implements `Workflow` beside its `Entity`, and the engine (§2.2) calls it. `Agent` is the production implementor; the crate's conformance suites drive a fan-out fixture.

```rust
pub trait Workflow: Entity + EntityHandler<Tick> + EntityHandler<Settled<Self>> {
    type Key: SerializationRequirement + Clone + Ord + Debug;
    type Effect: Send + 'static;
    type Outcome: SerializationRequirement + Clone;

    fn pending(&self, state: &Self::State) -> Vec<Pending<Self::Key, Self::Effect>>;
    fn perform(&self, effect: Self::Effect, cx: Perform<Self::System>)
        -> impl Future<Output = Result<Self::Outcome, EffectError>> + Send + 'static;
    fn settle(&self, state: &Self::State, key: &Self::Key,
              outcome: Result<Self::Outcome, EffectError>, ctx: &EntityCtx<Self>)
        -> impl Future<Output = Vec<Self::Event>> + Send;
}
```

`perform` is the one place a workflow entity performs I/O, and `settle` an ordinary entity decision (entity §3.2). A `Pending` carries a key, the consumer's opaque effect, and an `OnOrphan` policy. The key and the outcome ride the `Settled` command, so they carry the command bounds; the effect never crosses a wire. The consumer:

- MUST make `pending` a pure function of the committed state it is given, reading no activation state (§3.1). The layer cannot enforce this, since `pending` takes `&self`.
- MUST keep every key unique over the entity's whole journal (§3.1), and declare every `Pending`'s `OnOrphan`, which has no default (§4.3).
- MUST NOT touch the entity's state, journal, or facets from `perform`, and cannot: `perform` receives only a `Perform` (§2.4), and clones the consumer's own seams out of `&self` before returning a `'static` future.
- MUST NOT mutate state, perform durable I/O, or await anything in `settle`. Returning no records is legal and commits nothing (§3.4).

The supertraits guarantee only that a `Tick` and a `Settled<Self>` handler exist: an entity lacking either does not compile. That they forward to the engine is the consumer's obligation (§2.2), since the orphan rule forbids a blanket impl.

### 2.2 `Engine`: what the consumer forwards

An internal boundary. The crate implements `Engine<D>`, and the consumer holds one in its behavior and MUST:

- call `Engine::on_activate` from `Entity::on_activate` (§3.2 item 2);
- answer `Entity::can_hibernate` with `Engine::quiescent`, conjoined with any condition of its own: it MAY narrow the answer and MUST NOT widen it (§6);
- call `Engine::advance` from its `Tick` handler, after its pre-tick gate (§2.3), and return what `Engine::settled` returns from its `Settled` handler;
- call `Engine::tick`, with the state it was given, from every handler of its own whose records call for work (§3.2 item 1), which on an activation's first handler also classifies the orphan set (§4.3);
- never admit `Tick` or `Settled` from the network: both are local-only self-tells, absent from its `EntityRegistry` (entity §4.5), so nothing outside the entity can inject an outcome or drive its activation.

The engine's state (claims, backoff counts, the orphan classification, staged events) is ephemeral and never journaled, and it decides no records of its own: every outcome reaches the journal through `Settled`.

### 2.3 The pre-tick gate

A tick launches the whole unclaimed desired set, yet some launches depend on activation state no fold expresses: an environment to open or reacquire, a loss to surface first. The gate has no type. Such a consumer's `Tick` handler MUST resolve that state before calling `Engine::advance`, never inside a launch. While the gate journals records of its own, the handler MUST return them without calling `advance` and re-enter through `Engine::tick`, so nothing launches against a fold about to change; while it waits on an open in flight, it MUST NOT call `advance`, and the open's completion re-enters the cycle. The agent runtime's environment pre-pass is this gate (agent §4.2, §4.4, §4.5).

### 2.4 `Perform`: what an effect may reach

The engine builds a `Perform<S>` for each launch. It carries the entity's `EntityName` and the system `S`, whose `Clock`, `Entropy`, and `Spawner` (actor §2.12.1) are an effect's only route to time, randomness, and task launching, and nothing that reaches the entity's state, journal, or facets. Time and randomness inside an effect MUST come from it, or a simulated run stops reproducing; a per-effect timeout is the consumer's to take from that `Clock`.

### 2.5 The entity beneath

The workflow relies on four properties of the entity runtime:

- `Tick` and `Settled` are ordinary commands, ordered against every other by the input gate (entity §5).
- A self-tell sent from a handler is handled only after that handler's records commit (entity §5), which W4 rests on (§5).
- `Entity::on_activate` sees no state and cannot commit (entity §3.1), so the first tick can launch but never journal (§4.3).
- A forced step-down ignores `can_hibernate` (entity §7), and the host refuses an outcome arriving after a non-contiguous commit has ended the activation (entity §5).

### 2.6 `Binding`: an activation-scoped resource

`Binding<T>` is the passive state machine (`BindingState`: closed, opening, open, failed) for an external resource a consumer holds for the life of an activation, such as the agent runtime's sandbox (agent §2.3). It is a component rather than a trait method because the resource's type and open path are the consumer's, and the engine never sees it, so a consumer holding one:

- MUST veto hibernation while the open is in flight, conjoining `Binding::is_opening` into its `can_hibernate`;
- MUST settle, not relaunch, the effects that needed a resource whose open failed, taking the reason with `Binding::take_failure` in the same round;
- MUST release the resource on every deactivation path, taking it with `Binding::take` from `Entity::on_deactivate`;
- MUST NOT read it from `pending` (W1).

### 2.7 `Subscribers`: awaiting a durable outcome

Autonomous work outlives the request that started it, and a reply resolves within one message's delivery (actor §2.10), so a caller gets an ack, a registered reply-to, and a later notification. `Subscribers<K, V>` holds the registrations in activation state. A consumer that uses it MUST:

1. not journal the set: a reply-to is a routing address, not a fact about the work, and callers re-contact after a resume;
2. notify only after the record carrying the outcome commits, taking the addresses with `Subscribers::take` from its post-commit pass;
3. treat a notification as a `tell` (actor §2.2), at-most-once delivery rather than the source of truth, which a caller recovers by re-contacting the entity, never by a transparent retry;
4. answer a caller arriving after the outcome committed at once from the fold, not register it.

### 2.8 `WorkflowEvent`: the checker-facing stream

The engine emits each `WorkflowEvent` on the actor event stream as an application event (actor §2.7), beside the entity's own (entity §10), for the checker in `workflow::testing`. Events describe the machinery around effects, never their content. Each carries the `node` hosting the activation, because under failover a deposed leader and its successor differ only by node; `key` is the key's `Debug` rendering.

| Event | Fields | Meaning |
|---|---|---|
| `Launched` | `entity`, `key`, `node` | A key was claimed and its effect handed to the spawner (§3.2). |
| `Interrupted` | `entity`, `key`, `node` | An orphaned `Interrupt` member was resolved without a launch (§4.3), opening the claim its `Settled` closes. |
| `Settled` | `entity`, `key`, `node`, `ok`, `committed` | An outcome reached `settle`; `ok` tells `Ok` from `Err`, and `committed` says whether the settle decided records. |
| `Discarded` | `entity`, `key`, `node` | A straggling outcome was dropped (§4.4). |
| `Quiescent` | `entity`, `node` | The activation became quiescent (§6). |

The engine MUST emit `Settled` on every settle that runs, since one that commits nothing is the one whose key backs off, and its `Launched` would otherwise have no terminator. When the settle decided records, the event MUST wait for their commit, so a failed append leaves no event claiming records that never became durable. The vocabulary holds only what the entity's own events do not show; `Discarded` exists because a discarded outcome leaves no record anywhere.

## 3. The desired set

The **desired set** is `pending(state)` over committed state: the keyed effects that state calls for. A **claim** is an activation's ephemeral note that it launched a key, which is **in flight** until settled. A key is **at rest** when backed off to the cap (§3.4), and an activation is **quiescent** when nothing is in flight and every member of the set is at rest (§6).

### 3.1 A pure function of committed state

`pending` MUST be pure and deterministic: no I/O, no clock, no entropy, and no read of activation state (claims, backoff, bindings). It runs after every commit and on every activation's first tick, and the two MUST agree, as `Entity::apply` must (entity §3.1, E2), or a resumed activation would abandon or duplicate work. A set re-derived from committed state recovers by recomputing, where a queue of dispatches would have to be journaled and reconciled.

A key is never journaled, so it needs only determinism within one fold: it is derived from state (a call id the consumer journals), never from a counter in memory. **A key MUST be unique over the entity's whole journal.** The straggler filter tests only presence in the set (§4.4), so a key that left and re-entered (a retried call at the same site, a second run reusing an ordinal) would let the first occurrence's outcome settle as the second's. Only the consumer knows what distinguishes two visits to a call site, so a consumer whose call ids repeat across runs qualifies them with the run.

### 3.2 The cycle

A tick MUST run:

1. after **every** commit of records, a command's or a settled outcome's;
2. once after **rehydration**, armed from `Engine::on_activate`;
3. when a key's **backoff elapses** (§3.4), and once more, without delay, when a key **reaches the cap**, because only a tick can conclude quiescence after a settle that committed nothing;
4. on an explicit `Tick` command, how a consumer re-enters the cycle after an event that commits nothing.

Entry point 3 is required: a settle that commits nothing leaves its key in the set without re-entering through entry point 1, and without the timer W2 does not hold.

Each tick:

1. computes `desired = pending(state)` over the **committed** state;
2. releases every claim and backoff count whose key is absent from `desired`, since the journal considers that effect finished;
3. for each member this activation has not claimed and whose backoff has elapsed, takes the claim and launches `perform` through the `Spawner`;
4. if nothing is in flight and every member is at rest, marks the activation quiescent (§6).

The workflow MUST NOT await a launch: step 3 hands the future to the spawner, and the outcome returns later as a `Settled` command (§4.1). An entity with a hundred outstanding effects holds no thread.

A tick that a consumer's handler drives through `Engine::tick` also **wakes every at-rest key**, dropping its delay and count so the tick launches it afresh. The engine's own re-entries (the rehydration tick, the tick after its own settle, the backoff timer) leave at-rest keys alone: that is the line between the engine driving itself and someone asking, and it lets an entity hibernate on a capped-out key without abandoning it (§6).

### 3.3 Launch-once per activation

A claim is never journaled: journaling it would make the *launch* the durable fact, and the launch is what a crash loses track of. The **settled record** is the durable fact, and every launch is speculative:

- **Within one activation, an effect is launched at most once per claim** (W3): a tick triggered by an unrelated commit does not relaunch an effect in flight.
- **Across activations, an effect MAY be launched more than once.** A crash between a launch and its settled record leaves the set unchanged, so the next activation launches it again, and a deposed leader MAY overlap its successor. An effect unsafe to repeat declares `Interrupt` (§4.3).

### 3.4 Backoff

A settle that commits nothing leaves its key in the desired set, where the next tick would relaunch it at the rate the cycle turns. **A key whose settle committed nothing MUST NOT be relaunched before a positive delay that does not shrink across consecutive empty settles, and after a bounded number of them the key is at rest** until a consumer-driven tick wakes it (§3.2). The delay MUST be timed by `Clock`, with any jitter from `Entropy`, so it is seed-reproducible. A settle whose records commit clears the count, and a re-activation starts every count at zero.

The trigger is the empty commit, not the failure: an `Err` the consumer journals commits and clears the count, and an `Ok` it journals nothing for backs off like a failure. The workflow imposes **no attempt bound**, since only the consumer knows what the work is worth; it bounds the delay, and a key at rest does not hold the activation resident (§6).

## 4. Outcomes

### 4.1 Settling

An effect's outcome returns as an ordinary command, `Settled { key, outcome }`, delivered by `tell` through the mailbox (§2.5). It has no reply: anything waiting on the result waits on the record (§2.7). The engine applies the straggler test (§4.4), then hands the outcome to `Workflow::settle`; the outcome is durable once the settle's records commit, and that commit re-enters the cycle (§3.2 item 1).

### 4.2 Effect failure is a value

An effect that fails does not fail the entity: it settles as `Err(EffectError)`, and the consumer's fold decides what it means. The workflow has no failure mode that ends an entity.

| `EffectError` | Meaning |
|---|---|
| `Timeout` | A bound the consumer's `perform` set on itself elapsed. Its effects MAY still land; only the outcome is bounded. |
| `Interrupted` | An orphaned effect declaring `OnOrphan::Interrupt` was resolved on resume (§4.3). Nothing is known about whether it ran. |
| `Lost` | The environment the effect needed is gone. |
| `Failed` | The effect ran and failed. |

The workflow mints `Interrupted`, and `Failed` for a panic inside the future `perform` returned, caught at the task boundary so a claimed key never goes without an outcome; the other two are the consumer's. A panic in the synchronous call to `perform` is the handler's, like any other on the command path.

### 4.3 Orphans and resume

An effect's intent is journaled before it launches and its outcome after (§5), so a crash or migration between the two leaves it **orphaned**: wanted, with nothing to say whether it ran. Hibernation never orphans an effect, because an open claim vetoes it (W7).

**The orphan set is the desired set over the committed state an activation first sees**: at the first tick, or at a command handled ahead of it that calls `Engine::tick` or delivers a `Settled`. Such a handler classifies from the state it was given, which precedes its own records, so the work it creates is fresh; work committed by a handler that skips `Engine::tick` is read as orphaned, conservative for `Interrupt` and exact for `Reexecute`. The workflow MUST resolve each member by its declared policy:

- `OnOrphan::Reexecute`: launch it as an ordinary launch. The consumer has declared the effect idempotent, self-deduplicating, or cheap enough that a second run beats an unknown one.
- `OnOrphan::Interrupt`: settle it as `Err(EffectError::Interrupted)` without a launch. The first tick cannot journal (§2.5), so the resolution rides the mailbox as a synthesized `Settled`, under a claim.

Neither policy is a default, because an unstated one would silently decide whether to repeat a side effect. No member may be dropped in silence (W6): one neither launched nor settled would leave work outstanding that nothing is doing, and quiescence would never arrive.

### 4.4 Stragglers

An outcome MAY arrive after its key has left the desired set: a timeout racing the real result, a cancelled effect completing anyway, a previous activation's launch reaching its successor. The workflow MUST discard such a `Settled` without calling `settle` and without writing a record (W5). The test is the desired set itself: a key is in the set exactly while its outcome is not journaled, so a second outcome finds it absent, and the input gate admits no second outcome while the first's append is in flight. Exactness rests on key uniqueness (§3.1).

## 5. Durability is the entity's

The workflow's write-ahead barrier is the entity's output gate (entity §5, E5). **An effect is launched only after the records that placed it in the desired set have committed** (W4). No check enforces this; it follows from the cycle: every entry point reads committed state, and the tick that follows a handler's records is handled after they commit (§2.5).

- **Durability failure pauses; it does not fail.** An append that cannot reach a quorum steps the activation down (storage §10), and the work stays in the desired set for the next activation to re-drive. The workflow invents no outcome it could not record.
- **The fence guards the record, not the world.** Two nodes transiently leading a shard produce one journal (storage §5, E1) but MAY launch the same effect twice (§3.3), so a consumer with externally visible effects chooses its orphan policy deliberately.

## 6. Quiescence and hibernation

**An activation is quiescent when nothing is in flight and every member of its desired set is at rest**: the set is empty, or its remaining members are backed off to the cap. The workflow MUST report quiescence to `Entity::can_hibernate` (entity §7), permitting hibernation exactly when quiescent; an activation whose first tick has not run is not quiescent. A consumer MAY narrow this with what the set cannot see, such as a request accepted but not yet started or a `Binding` mid-open (§2.6).

The at-rest clause exists because there is no attempt bound: under "the set is empty" alone, one permanently failing effect (an owed send to a node that is not coming back) would veto eviction forever. At the cap the debt stays in the fold and the entity hibernates until a message next ticks it, which is safe because the next activation re-derives the same set and drives it (W1).

A forced step-down ignores `can_hibernate` (§2.5), so the workflow MUST treat deactivation as unremarkable: claims and counts are dropped, in-flight launches are abandoned, and their outcomes become stragglers at the next activation (§4.4). Nothing is owed at deactivation.

## 7. Determinism

The `workflow` crate MUST satisfy the actor runtime's determinism contract (actor §2.12.2): no wall clock, no OS threads, and no unseeded randomness. Every launch goes through `Spawner`, every delay through `Clock`, and backoff jitter comes from `Entropy`, so a simulated workflow entity, its events included, reproduces from its seed.

## 8. Invariants

The Rust copy of this table, with the suites that verify each row, is the `workflow` crate's conformance catalogue.

| # | Invariant | Defined in | Entity basis |
|---|---|---|---|
| W1 | **Desired-set purity.** Two activations that fold the same journal prefix compute the same desired set, and no ephemeral state (claims, backoff, bindings) is reachable from it. | §3.1 | E2, E3 |
| W2 | **Launch coverage.** Once faults cease and an activation is live, every member of the desired set is in flight, settled, or waiting out its backoff; a member at the cap is at rest, and is re-driven when a caller next reaches the entity. | §3.2, §3.4, §6 | (none) |
| W3 | **Launch-once per claim.** An activation launches each key at most once per claim; a later tick never relaunches an effect in flight. Across activations re-launch is permitted, and the settled record is the durable fact. | §3.3 | E6 |
| W4 | **Intent before effect.** An effect is launched only after the records that placed it in the desired set have committed. | §5 | E5 (output gate) |
| W5 | **Straggler rejection.** An outcome whose key is absent from the current desired set is discarded, never settled, and writes no record; exact because keys are unique over the journal. | §4.4, §3.1 | E1 |
| W6 | **Orphan resolution.** After rehydration, every member of the orphan set is relaunched or settled `Interrupted` per its declared policy; none is dropped, and none is resolved twice. | §4.3 | E14 |
| W7 | **Quiescence.** An activation permits hibernation exactly when nothing is in flight and every member of its desired set is at rest; an entity hibernated while quiescent re-derives the same set on activation and drives it. | §6, §3.4 | E6, entity §7 |

## 9. Deferred

- **Alarm-driven ticks.** A tick fired by the alarm facet (facets §4.5), so an entity can hold work for later without an activation; until then a key at rest waits for a caller.
- **A step DSL.** The linear `async` `step`/`sleep`/`retry` surface (entity §13), written over this cycle; until then a consumer writes the re-entrant form.
- **Effect priority and concurrency bounds.** The desired set launches in full; a consumer with one scarce resource bounds its effects itself until a second consumer shapes the rule.
- **Cross-entity effects.** An effect aimed at another entity is ordinary; whether to deduplicate by entity name or ride entity's deferred sagas (entity §13) is open.
- **Post-commit emission for consumers.** A consumer stages its own post-commit events; one staging could serve it and the engine.
