# Workflow: Specification

The key words **MUST**, **MUST NOT**, and **MAY** carry the meanings defined in RFC 2119.

## 1. Scope

A **workflow** is an entity that drives itself. Its activation derives the work its committed state calls for, launches what is not yet in flight, folds each outcome back into records, and stops when nothing is left, without awaiting I/O inside a handler. This specification defines that cycle and the properties that keep it safe under crash and failover (W1 to W7). The work outstanding is a pure function of committed state, never a journaled queue: one desired set is at once the launch list (§4.2), the orphan set (§5.3), and the straggler test (§5.4).

It builds on the entity runtime (entity §4, §6, §8) for identity, the journal, the fold, the single-writer fence, activation, and hibernation, and adds no journal, fence, or resume protocol of its own. Effects run on the actor system's host environment (actor §11.1). The agent runtime builds on this specification: a session implements the workflow plug point (agent §4.5).

### 1.1 Non-goals

- **No model, tool, or transcript.** Those are the agent runtime's (agent §1).
- **No memoization.** The consumer's records are the durable fact; an entity that also wants step memoization declares the steps facet beside the workflow (facets §5.6).
- **No submission or cancellation protocol.** Idempotent submission and cancellation are folds over the consumer's records: a cancel journals a terminal record, from which the desired set derives the propagating sends as ordinary effects.
- **No exactly-once effects.** A re-activation MAY relaunch an effect whose outcome has not committed (§4.3).
- **No scheduling.** The only timer is the per-key backoff inside an activation (§4.4).

### 1.2 Conformance

- A **workflow engine** conforms by meeting the engine's side of §3.2, §4 to §7, §10, and §11.
- A **workflow entity** conforms by meeting §3.1, the consumer's side of §3.2, §3.3, and §4.1; and §8 if it holds an activation-scoped resource, §9 if callers await its outcomes.

## 2. Model

- **Consumer:** the implementor of the workflow plug point (§3.1) for one entity type. A **workflow entity** is an entity of that type.
- **Engine:** the role that runs the cycle inside each activation of a workflow entity, on the consumer's behalf (§3.2). Everything it holds (claims, backoff counts, the orphan classification, events awaiting a commit) is activation state: never journaled, and discarded at deactivation.
- **Effect:** one unit of work performed outside the entity, described by the consumer. The engine treats it as opaque, and it never crosses a wire.
- **Key:** an effect's identity within its entity, derived from committed state (§4.1).
- **Pending effect:** a key, its effect, and the effect's orphan policy.
- **Desired set:** the pending effects the entity's committed state calls for, as the consumer computes them (§4.1).
- **Outcome:** what an effect returns: a value of the consumer's outcome type, or an effect error (§3.1).
- **Settle:** the consumer's decision that folds one outcome into records. The records it decides are the **settled record**, the durable fact that the effect ran. A settle that decides no records is an **empty settle**.
- **Tick:** one pass of the cycle (§4.2).
- **Claim:** an activation's note that it launched a key, or resolved it as an orphan without a launch. A claimed key is **in flight** until its outcome is settled or discarded.
- **Backoff:** the delay a key earns from consecutive empty settles (§4.4). A key is **at rest** when its backoff has reached the cap.
- **Quiescent:** an activation is quiescent when nothing is in flight and every member of the desired set is at rest (§7).
- **Orphan:** an effect whose intent has committed and whose outcome has not, when an activation begins. The **orphan set** is defined in §5.3.
- **Orphan policy:** what the engine does with an orphan, declared per pending effect (§3.1).
- **Straggler:** an outcome whose key is absent from the desired set over the committed state when the outcome is handled (§5.4).
- **Activation-scoped resource:** an external resource a consumer holds for the life of one activation (§8). It is **closed** (nothing held), **opening** (an open in flight), **open** (held), or **failed** (the last open failed, until the consumer settles the effects that needed it, which closes it again).
- **Subscriber:** a caller's notification address, registered against a unit of work to be told when its outcome commits (§9).

## 3. Seams

### 3.1 `Workflow`: what the consumer supplies

A plug point. The consumer implements `Workflow` for its entity type, beside the entity plug point (entity §3.1), and the engine calls it.

```rust
pub trait Workflow: Entity + EntityHandler<Tick> + EntityHandler<Settled<Self>> {
    type Key: SerializationRequirement + Clone + Ord + Debug;
    type Effect: Send + 'static;
    type Outcome: SerializationRequirement + Clone;

    fn pending(&self, state: &Self::State) -> Vec<Pending<Self::Key, Self::Effect>>;
    fn perform(&self, effect: Self::Effect, cx: Perform<Self::System>)
        -> impl Future<Output = Result<Self::Outcome, EffectError>> + Send + 'static;
    async fn settle(&self, state: &Self::State, key: &Self::Key,
                    outcome: Result<Self::Outcome, EffectError>, ctx: &EntityCtx<Self>)
        -> Vec<Self::Event>;
}
```

- `pending` computes the desired set from committed state. Each `Pending` carries a `key`, the consumer's `effect`, and an `on_orphan` policy.
- `perform` runs one effect, outside the activation, and is the only place a workflow entity performs I/O. Its `Perform` argument carries the entity's name and the actor system, whose clock, entropy, and spawner (actor §11.1) are the effect's only route to time, randomness, and task launching. It reaches nothing of the entity's state, journal, or facets.
- `settle` folds one outcome into records. It is an ordinary entity decision (entity §4.2).
- `Tick` and `Settled` are the commands the engine sends its own entity (§11.1). A `Settled` carries a key and an outcome.

Every pending effect declares one of two orphan policies (`OnOrphan`). Neither is a default.

| Policy | The consumer declares | On resume (§5.3) |
|---|---|---|
| `Reexecute` | that a second run of the effect is acceptable: it is idempotent, deduplicates itself, or is cheap enough that a second run beats an unknown one | launched as an ordinary launch |
| `Interrupt` | that running the effect twice is unsafe | settled as `Interrupted` without a launch |

An effect that produces no value returns an effect error (`EffectError`):

| Error | Minted by | Meaning |
|---|---|---|
| `Timeout` | the consumer | A bound the consumer's `perform` set on itself elapsed. The effect's actions MAY still land; only the outcome is bounded. |
| `Interrupted` | the engine | An orphan declaring `Interrupt` was resolved on resume (§5.3). Nothing is known about whether it ran. |
| `Lost` | the consumer | The environment the effect needed is gone. Carries a reason. |
| `Failed` | the consumer, or the engine for a panic (§5.2) | The effect ran and failed. Carries a reason. |

The consumer:

- MUST make `pending` a pure function of the committed state it is given (§4.1).
- MUST keep every key unique over the entity's whole journal (§4.1), and MUST declare an orphan policy for every pending effect.
- MUST NOT touch the entity's state, journal, or facets from `perform`.
- MUST take time and randomness inside an effect only from the system its `Perform` carries (actor §11). A per-effect timeout is the consumer's, measured on that system's clock.
- MUST NOT mutate state, perform durable I/O, or await anything in `settle`. A settle MAY decide no records; it then commits nothing (§4.4).
- MUST handle `Tick` and `Settled` by forwarding them to the engine (§3.2).

### 3.2 The engine: what the consumer forwards

A boundary inside one workflow entity, with one implementation. The engine offers five operations, which the consumer's entity hooks and handlers invoke.

| Operation | Invoked from | Takes | Does |
|---|---|---|---|
| Activate | the activation hook (entity §3.1) | the entity's context | starts the activation's engine state empty and queues the first tick (§4.2) |
| Quiescence | the hibernation check (entity §8) | nothing | answers whether the activation is quiescent (§7) |
| Advance | the `workflow.Tick` handler, after the pre-tick gate (§3.3) | the committed state | runs one tick (§4.2); decides no records |
| Accept an outcome | the `workflow.Settled` handler | the committed state and the command | discards a straggler (§5.4), or yields the records the consumer's settle decided (§5.1) |
| Request a tick | every handler of the consumer's own whose records call for work | the state the handler was given | queues a tick, handled after the handler's records commit (entity §6), and wakes every at-rest key (§4.2); on the activation's first handler, classifies the orphan set (§5.3) |

The consumer MUST invoke each operation from every place the table names, subject to its pre-tick gate (§3.3), and MUST return what Accept an outcome yields from its `workflow.Settled` handler. Its hibernation check answers Quiescence conjoined with any condition of its own: it MAY narrow the answer and MUST NOT widen it (§7). It MUST NOT admit `workflow.Tick` or `workflow.Settled` from the network: both are absent from its dispatch allowlist (entity §5.5), so only the entity itself sends them.

The engine journals nothing of its own: every record a workflow entity writes is one its consumer decided, and every outcome reaches the journal through the consumer's settle. The engine meets the deterministic profile (actor §11), backoff delays and their jitter included.

### 3.3 The pre-tick gate

Some launches depend on activation state no fold expresses: an environment to open or reacquire, a loss to report first. A consumer with such state MUST resolve it in its `workflow.Tick` handler before invoking Advance, never inside a launch. While the gate journals records of its own, the handler MUST return them without invoking Advance and request a tick. While the gate waits on an open in flight, the handler MUST NOT invoke Advance, and the open's completion re-enters the cycle. The agent runtime's environment pre-pass is this gate (agent §5.2, §5.4, §5.5).

## 4. The desired set

### 4.1 A pure function of committed state

The desired set MUST be a pure, deterministic function of committed state: computing it performs no I/O, reads no clock or entropy, and reads no activation state (claims, backoff, activation-scoped resources). The engine computes it after every commit and at every activation's first tick, and the two MUST agree, like the fold (entity §4.1, E2).

A key is derived from committed state (for example, a call identifier the consumer journals), never from a counter in memory, and the engine never journals it. **A key MUST be unique over the entity's whole journal**, not only within one desired set: a key that leaves the set and later re-enters it (a retried call at the same site, a second run reusing an ordinal) breaks this rule, and with it the exactness of straggler rejection (§5.4).

### 4.2 The cycle

A tick MUST run:

1. after **every** commit of records, a command's or a settled outcome's;
2. once after **rehydration**, queued by Activate;
3. when a key's **backoff elapses** (§4.4), and once more, without delay, when a key **reaches the cap**;
4. when the consumer **requests** one (§3.2), which is how it re-enters the cycle after an event that commits nothing.

Each tick:

1. computes the desired set over the **committed** state;
2. releases every claim and backoff count whose key is absent from the desired set;
3. claims and launches every member this activation has not claimed and whose backoff has elapsed;
4. if nothing is in flight and every member is at rest, marks the activation quiescent (§7).

The engine MUST NOT await a launch: the tick returns while its effects run, and each outcome returns later as a `workflow.Settled` command (§5.1).

A tick the consumer requests also **wakes every at-rest key**, clearing its backoff so the tick launches it afresh. The engine's own ticks (the rehydration tick, the tick after a settle, the backoff timer) leave at-rest keys at rest.

### 4.3 Launch-once per activation

A claim is never journaled. The settled record is the durable fact, and every launch is speculative:

- **Within one activation, an effect is launched at most once per claim** (W3): a tick triggered by an unrelated commit does not relaunch an effect in flight.
- **Across activations, an effect MAY be launched more than once.** A crash between a launch and its settled record leaves the desired set unchanged, so the next activation launches the effect again. Two nodes transiently leading the entity's shard produce one journal (storage §6, E1) but MAY both launch the same effect.

### 4.4 Backoff

An empty settle leaves its key in the desired set. **A key whose settle was empty MUST NOT be relaunched before a positive delay that does not shrink across consecutive empty settles, and after a bounded number of consecutive empty settles the key is at rest** until a tick the consumer requests wakes it (§4.2). A settle whose records commit clears its key's count, and every count starts at zero in a new activation.

The trigger is the empty settle, not the failure: an effect error the consumer journals commits and clears the count, and a value it journals nothing for backs off like a failure. The engine imposes no attempt bound. It bounds the delay, and a key at rest does not hold the activation resident (§7).

## 5. Outcomes

### 5.1 Settling

An effect's outcome returns to the activation as a `workflow.Settled` command carrying the key and the outcome, sent as a tell (actor §3.2) from the entity to itself and ordered like any other command (entity §6). It has no reply: anything waiting on the result waits on the record (§9). The engine applies the straggler test (§5.4), then hands the outcome to the consumer's settle. The outcome is durable once the settle's records commit, and that commit re-enters the cycle (§4.2).

### 5.2 Effect failure is a value

An effect that fails does not fail the entity: it settles as an effect error (§3.1), and the consumer's fold decides what it means. No effect failure ends an entity. The engine mints `Interrupted` for an orphan it resolves (§5.3), and `Failed` for an effect that panics after its launch; a panic in the call to the consumer's perform itself, before the launch, is a panic of the handler that launched it.

### 5.3 Orphans and resume

An effect's intent is journaled before it launches and its outcome after (§6), so a crash or migration between the two leaves it orphaned. Hibernation never orphans an effect: a claim vetoes it (§7).

**The orphan set is the desired set over the committed state an activation first sees**: at its first tick, or at an earlier handler that requests a tick or accepts an outcome (§3.2). Such a handler classifies from the state it was given, which precedes its own records, so work its own records create is not orphaned. Work committed before the first tick by a handler that requests no tick is classified as orphaned. The engine MUST resolve every member of the orphan set exactly once, by its declared policy (§3.1):

- `Reexecute`: launch it as an ordinary launch.
- `Interrupt`: settle it as `Interrupted` without a launch. The resolution reaches the consumer's settle as an ordinary `workflow.Settled` command, and the key is claimed until then.

### 5.4 Stragglers

An outcome MAY arrive after its key has left the desired set: a timeout racing the real result, a cancelled effect completing anyway, a previous activation's launch reaching its successor. The engine MUST discard a straggler without handing it to the consumer's settle and without writing a record (W5). The test is the desired set itself: a key is in the set exactly while its outcome is not journaled, and the input gate admits no second outcome while the first's append is in flight (entity §6), so with unique keys (§4.1) the test is exact.

## 6. Durability is the entity's

The workflow's write-ahead barrier is the entity's output gate (entity §6, E5). **An effect is launched only after the records that placed it in the desired set have committed** (W4).

**Durability failure pauses; it does not fail.** An append that cannot reach a quorum steps the activation down (storage §11), and the work stays in the desired set for the next activation to drive. The engine invents no outcome it could not record.

## 7. Quiescence and hibernation

**An activation is quiescent when nothing is in flight and every member of its desired set is at rest**: the set is empty, or every remaining member is backed off to the cap. The engine's Quiescence operation (§3.2) MUST answer yes exactly when the activation is quiescent, so the hibernation check (entity §8) permits hibernation exactly then; an activation whose first tick has not run is not quiescent. A consumer MAY narrow this with what the desired set cannot see, such as a request accepted but not yet started, or an activation-scoped resource that is opening (§8).

A key at rest stays in the fold: the entity hibernates until a message next requests a tick, and the next activation derives the same set and drives it (W1).

A forced step-down ignores the hibernation check (entity §8). At every deactivation the engine drops its claims and counts and abandons its in-flight launches; nothing is owed at deactivation. An abandoned launch's outcome that reaches a later activation is handled like any other: settled while its key is still in the desired set, and discarded as a straggler once it is not (§5.4).

## 8. Activation-scoped resources

A consumer holding an activation-scoped resource (such as the agent runtime's sandbox, agent §3.3):

- MUST veto hibernation while the resource is opening (§7);
- MUST settle, not relaunch, the effects that needed a resource whose open failed, with the failure's reason, in the same pass that closes the failed resource;
- MUST release the resource on every deactivation path, from its deactivation hook (entity §3.1);
- MUST NOT read it while computing the desired set (§4.1).

The engine never sees the resource.

## 9. Awaiting a durable outcome

A consumer whose callers await work that outlives their request acknowledges the request, registers the caller as a subscriber, and notifies it when the outcome commits; no reply carries the outcome (actor §7). Such a consumer MUST:

1. keep its subscribers in activation state, never journaled; callers re-contact after a resume;
2. notify a subscriber only after the record carrying the outcome commits;
3. send each notification as a tell (actor §3.2): at-most-once delivery, not the source of truth, which a caller recovers by re-contacting the entity, never by a transparent retry;
4. answer a caller that arrives after the outcome committed at once from the fold, without registering it.

## 10. Events

The engine emits the workflow events of §11.2 on the actor event stream as application events (actor §3.6), beside the entity's own (entity §11). They describe the machinery around effects, never an effect's or an outcome's content. Each carries the entity's name and the node hosting the activation.

- The engine MUST emit `Settled` for every outcome it hands to the consumer's settle, whether or not the settle decided records.
- When the settle decided records, the engine MUST NOT emit its `Settled` before those records commit; a settle whose append fails emits none.

## 11. Registries

### 11.1 Manifests

| Manifest | Carries | Sent by |
|---|---|---|
| `workflow.Tick` | nothing: re-enters the cycle (§4.2) | the entity to itself only (§3.2) |
| `workflow.Settled` | an effect's key and outcome (§5.1) | the entity to itself only (§3.2) |

### 11.2 Event kinds

In every kind, `entity` is the entity's name, `node` the node hosting the activation, and `key` the key's debug rendering.

| Event | Fields | Emitted when |
|---|---|---|
| `Launched` | `entity`, `key`, `node` | A key was claimed and its effect launched (§4.2). |
| `Interrupted` | `entity`, `key`, `node` | An orphan declaring `Interrupt` was resolved without a launch (§5.3), opening the claim its `Settled` closes. |
| `Settled` | `entity`, `key`, `node`, `ok`, `committed` | An outcome reached the consumer's settle (§10); `ok` tells a value from an effect error, and `committed` says whether the settle decided records. |
| `Discarded` | `entity`, `key`, `node` | A straggler was dropped (§5.4). |
| `Quiescent` | `entity`, `node` | The activation became quiescent (§7). |

## 12. Invariants

| # | Invariant | Defined in | Entity basis |
|---|---|---|---|
| W1 | **Desired-set purity.** Two activations that fold the same journal prefix compute the same desired set, and no activation state (claims, backoff, activation-scoped resources) is reachable from it. | §4.1 | E2, E3 |
| W2 | **Launch coverage.** Once faults cease and an activation is live, every member of the desired set is in flight, settled, or waiting out its backoff; a member at the cap is at rest, and is re-driven when a caller next reaches the entity. | §4.2, §4.4, §7 | (none) |
| W3 | **Launch-once per claim.** An activation launches each key at most once per claim; a later tick never relaunches an effect in flight. Across activations re-launch is permitted, and the settled record is the durable fact. | §4.3 | E6 |
| W4 | **Intent before effect.** An effect is launched only after the records that placed it in the desired set have committed. | §6 | E5 (output gate) |
| W5 | **Straggler rejection.** An outcome whose key is absent from the current desired set is discarded, never settled, and writes no record; exact because keys are unique over the journal. | §5.4, §4.1 | E1 |
| W6 | **Orphan resolution.** After rehydration, every member of the orphan set is relaunched or settled `Interrupted` per its declared policy; none is dropped, and none is resolved twice. | §5.3 | E14 |
| W7 | **Quiescence.** An activation permits hibernation exactly when nothing is in flight and every member of its desired set is at rest; an entity hibernated while quiescent re-derives the same set on activation and drives it. | §7, §4.4 | E6, entity §8 |

## 13. Security considerations

A workflow entity inherits the entity runtime's trust model (entity §11) and adds three properties:

- No peer can inject an outcome or drive an activation: `workflow.Tick` and `workflow.Settled` are sent only by the entity to itself (§3.2).
- An effect reaches nothing of its entity's state, journal, or facets (§3.1), and workflow events carry no effect or outcome content (§10).
- The fence guards records, not the world: an effect with externally visible actions MAY run more than once across activations (§4.3), and its orphan policy (§3.1, §5.3) is the consumer's only control over that.
