# Actor Model: Specification

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, **RECOMMENDED**, and **MAY** carry the meanings defined in RFC 2119.

## 1. Scope

An actor system hosts **actors**, isolated units of state that exchange typed **messages**. A local and a remote send are the same operation (§5), failures are explicit outcomes and no request is dropped without an error (§8), and the system, its codec, its host environment, and its event stream are seams (§3) that a deployment or a simulator supplies.

It builds on no other specification. The [cluster](cluster-spec.md) builds a multi-node actor system on it; the [cluster utilities](cluster-utilities-spec.md) use its receptionist and event stream; the entity, workflow, and agent layers use the actor system (§3.3), determinism (§11), and the event stream (§3.6).

### 1.1 Non-goals

- **Generic messages over the wire.** Message and reply types MUST be concrete and serializable; a generic actor is allowed.
- **Transparent failure masking.** A system MUST NOT retry a message on its own; retries and idempotency are the caller's.
- **Strong consistency in the data plane.** Messaging, death watch, and discovery are eventually consistent.
- **Shared memory across nodes**, and **exactly-once or durable delivery**, which layers above build with idempotency keys.

### 1.2 Conformance

An **actor system** conforms by meeting §3.2, §3.3, §3.6, §3.7, and §4 to §11; a **codec**, §3.4; a **host environment**, §3.5; an **actor**, what §3.1 asks of it. A system or layer that claims the **deterministic profile** also meets §11.2.

The invariants (§13) hold under message loss, duplication, delay, and reordering, partitions, and crashes. Four properties hold by construction: an actor's state is reachable only through its own handlers (§3.1), every message and reply is serializable (§3.4), the messages an actor accepts from the network are listed explicitly (§7), and nodes associate only through an authenticated, version-checked handshake (cluster §4).

## 2. Model

- **Actor:** an isolated unit of state and behavior that processes its messages one at a time.
- **Message:** a serializable value sent to an actor. Each message type has one **reply** type and one **manifest**, a stable, author-chosen string naming the type on the wire.
- **Handler:** an actor's behavior for one message type, yielding its reply.
- **Node:** one running actor system, named by a **node id**.
- **Actor id:** the cluster-wide identity of one actor: its node id, a **path** on that node, and the **incarnation** of the node process that created it.
- **Reference:** a handle naming one actor id, through which a caller sends. It holds no actor state.
- **Mailbox** and **executor:** each local actor has one bounded mailbox, drained by one executor that runs its handlers serially.
- **Call:** an **ask** (a request awaiting its reply, bounded by a deadline) or a **tell** (one-way).
- **Call error:** a failure to complete a call (§8.2), distinct from an application failure, which a handler returns inside its reply.
- **Envelope:** a message in transit to an actor on another node: the recipient's actor id, the manifest, a correlation for an ask, and the encoded payload.
- **Node-down decision:** a system's terminal judgment that a node and every actor on it are gone; the cluster decides who makes it and when (cluster §5).
- **Rotation:** a system's per-node judgment of whether a node takes new work; a node out of rotation stays reachable.

## 3. Seams

### 3.1 Actors and messages

An application implements these, and the actor system runs them. A plug point.

```rust
pub trait Actor: Sized + Send + 'static {
    type System: ActorSystem;
    async fn started(&mut self, ctx: &Ctx<Self>) -> Result<(), BoxError>;
    async fn stopped(self, reason: StopReason);
    fn register(registry: &mut HandlerRegistry<Self>);
    fn supervision() -> Supervision;
}

pub trait Message: SerializationRequirement {
    type Reply: SerializationRequirement;
    const MANIFEST: Manifest;
}

pub trait Handler<M: Message>: Actor {
    async fn handle(&mut self, msg: M, ctx: &Ctx<Self>) -> M::Reply;
}
```

- `started` runs once, before the first message, and an error stops the actor as failed; `stopped` runs once when the actor stops, for any reason. By default `started` succeeds and `stopped` does nothing.
- `register` lists the message types the actor accepts from the network (§7); by default none, so the actor is local-only.
- `supervision` chooses the strategy of §9; by default the actor stops on any fault.
- `MANIFEST` MUST be stable and unique among the message types one actor accepts (§7), local-only actors included.
- An actor accepts exactly the message types it has a handler for: a typed send of any other type MUST be rejected before run time, and an envelope is held to the registration (§7).
- **Isolation:** an actor's state is owned by its executor. No operation returns access to it; only the actor's reference is held outside it.

### 3.2 References and the handler context

A reference offers four sends, each typed by the messages its target accepts:

| Send | Behavior |
|---|---|
| ask | Awaits the reply under the system's default deadline. |
| ask within | Awaits the reply under an explicit deadline. |
| tell | One-way. Fails only when the message cannot be enqueued or transmitted; awaits space in a full local mailbox. |
| try-tell | One-way and never awaits. Fails a full local mailbox with `MailboxFull` and, being local-only, any remote target with `Unreachable`. |

- Two references are equal exactly when their actor ids are equal.
- A reference MAY travel inside a message or a reply. Only its actor id travels, and the receiving system MUST rebind it so it is usable there.
- An actor id MUST be unique within a cluster for its lifetime and MUST let any node classify its target as local or remote from the id alone (§5). It MUST carry the incarnation of the process that issued it, so that after a restart under the same node id an old id fails as `DeadLetter` instead of reaching the actor now at its path. Within one incarnation a node MUST NOT reuse a path. A deployment that restarts a node in place MUST give the new process an incarnation no predecessor used.

Handlers and lifecycle hooks receive a **handler context**, which grants capabilities without exposing state: the actor's id and reference, its system, spawning a child (from a value, or from a factory, which makes it restartable, §9), watching and unwatching an actor by reference or id (§10), and stopping after the current message.

### 3.3 The actor system

The runtime an actor runs on, consumed by application code and every layer above and supplied by a deployment or a simulator. A plug point.

```rust
pub trait ActorSystem: Clone + Send + Sync + 'static {
    // What a layer above calls
    fn spawn<A: Actor<System = Self>>(&self, actor: A) -> ActorRef<A>;
    fn spawn_with<A, F: FnMut() -> A + Send + 'static>(&self, factory: F) -> ActorRef<A>;
    fn resolve<A: Actor<System = Self>>(&self, id: ActorId) -> ActorRef<A>;
    fn node(&self) -> NodeId;
    fn receptionist(&self) -> Receptionist<Self>;
    type Clock: Clock;
    fn clock(&self) -> &Self::Clock;
    fn now(&self) -> Instant;
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()>;
    fn launch(&self, task: BoxFuture<'static, ()>);
    fn emit(&self, event: Event);
    fn next_random(&self) -> u64;

    // What an implementation supplies
    fn spawn_factory<A: Actor<System = Self>>(&self, factory: ActorFactory<A>, parent: Option<ActorId>) -> ActorRef<A>;
    fn resolve_local<A: Actor<System = Self>>(&self, id: &ActorId) -> Option<Mailbox<A>>;
    fn is_local(&self, id: &ActorId) -> bool;
    fn in_rotation(&self, node: NodeId) -> bool;
    fn codec(&self) -> Arc<dyn Codec>;
    async fn remote_ask(&self, recipient: &ActorId, manifest: Manifest, payload: Vec<u8>, within: Duration) -> Result<Vec<u8>, CallError>;
    async fn remote_tell(&self, recipient: &ActorId, manifest: Manifest, payload: Vec<u8>) -> Result<(), CallError>;
    fn watch(&self, target: ActorId, watcher: ActorId, deliver: WatchDelivery);
    fn unwatch(&self, target: &ActorId, watcher: &ActorId);
    fn receptionist_state(&self) -> Arc<ReceptionistState>;
    fn replicate_registration(&self, key: &str, origin: NodeId, id: ActorId);
}
```

- `spawn_factory` MUST follow the lifecycle of §4.
- `is_local` MUST classify from the id alone.
- `in_rotation` MUST be `false` exactly when this system holds the node out of rotation (§3.7). By default every node is in rotation.
- `remote_ask` and `remote_tell` carry an encoded payload under the message's manifest. `remote_ask` MUST resolve to the encoded reply or a call error within `within` (§8).
- `watch` MUST report a target already terminated, or on a node already down, at once, and MUST NOT block on the watcher's own mailbox.
- `replicate_registration` MUST carry a registration to every other node (§3.7).
- A single-node system conforms by answering every remote send `Unreachable`; it has no node to replicate to.

### 3.4 Codec

The **codec** encodes every value that crosses a node boundary. A system holds exactly one, chosen by its deployment. A plug point.

```rust
pub trait Codec: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn encode_erased(&self, value: &dyn erased_serde::Serialize) -> Result<Vec<u8>, CodecError>;
    fn with_deserializer(&self, bytes: &[u8], f: &mut dyn FnMut(&mut dyn erased_serde::Deserializer));
}
```

1. Every message and reply type MUST be serializable, which the message and handler definitions check before run time.
2. Both ends of an association MUST use the same codec; nodes compare codec names when they associate (cluster §4).
3. Decoding MUST reject an unknown manifest (`Unhandled`) and MUST NOT construct a type outside the recipient's registered messages from untrusted input (§7).
4. A failure to encode or decode is reported as `Serialization` and MUST NOT stop the node's receive path.

### 3.5 Host environment

The **host environment** is a clock, an entropy source, and a task spawner, three plug points: a deployment supplies real ones, a simulator virtual ones.

```rust
pub trait Clock: Clone + Send + Sync + 'static {
    fn now(&self) -> Instant;
    async fn sleep(&self, dur: Duration);
    async fn timeout<F: Future + Send>(&self, within: Duration, f: F) -> Result<F::Output, Elapsed>;
}

pub trait Entropy: Send + Sync + 'static {
    fn next_u64(&self) -> u64;
}

pub trait Spawner: Send + Sync + 'static {
    fn launch(&self, task: BoxFuture<'static, ()>);
}
```

`Instant` is logical, so a virtual clock can produce it; `timeout` yields `Elapsed` when `within` passes first on this clock. What MUST come from these seams is §11.1.

### 3.6 Event stream

Each node emits one ordered **event stream** into a sink its deployment supplies. The sink and the application event are plug points.

```rust
pub trait EventSink: Send + Sync + 'static {
    fn emit(&self, event: Event);
}
```

- A system SHOULD emit the event kinds of §12.2, each at the step it names.
- A system MUST emit `Undelivered`, with the refusing call error, for every inbound envelope it refuses (§6, §7).
- A layer above MUST carry its own events as application events (`Event::App`), never as new kinds of the system's own.
- A system SHOULD expose metrics (mailbox depths, throughput, call latency, restarts, associations, membership size), SHOULD propagate a trace context through envelopes, and SHOULD log lifecycle, membership, downing, and supervision actions at a defined, filterable level.

### 3.7 Receptionist

Every system provides a receptionist, through which a node finds a service's reference without a hard-coded actor id. A boundary.

| Operation | Takes | Yields |
|---|---|---|
| register | a key and a reference | nothing |
| lookup | a key | the current listing, read from local state with no traffic |
| subscribe | a key | a stream of listings |

1. The receptionist is not an actor and has no resolvable path: it is available on every node without prior introduction.
2. Registrations replicate to every node and are eventually consistent. Concurrent registrations MUST merge without coordination; a replicated data type keyed by registering node is RECOMMENDED.
3. When a node is declared down, the receptionist MUST prune every registration whose actor is hosted there, whichever node registered it, and subscribers MUST receive an updated listing. A registration whose actor terminates is pruned likewise, through death watch (§10).
4. A listing MUST omit every actor on a node out of rotation. The node keeps its registrations, which return to the listing without re-registration when it comes back, and it still answers a direct ask.
5. Subscribing MUST deliver the current listing at once and a fresh listing on every change.
6. A key is typed by actor type, so a lookup or a subscription yields references of that type.
7. **Order:** a listing MUST order its entries by a deterministic function of the registered set, so two nodes holding the same registrations and the same rotation view return the same sequence.
8. **Convergence under loss:** once faults cease, every node's listing for a key MUST converge, within bounded logical time, on the registrations of live actors, even when individual replication messages were lost.

## 4. Lifecycle

For each actor, the system MUST observe exactly this order:

1. **Assign:** the id is reserved. It is unique and not yet deliverable.
2. **Start:** the mailbox exists and buffers, the id resolves, and the executor runs `started`.
3. **Ready:** `started` returned success; buffered messages become processable.
4. The actor processes messages.
5. **Resign:** on stop or failure, `stopped` runs and the id and every resource are released.

- Assign and ready each happen exactly once, assign before any message can be delivered. Resign happens exactly once, even when the spawn fails between assign and ready, in which case it follows assign directly.
- Between assign and ready the id is reserved but not deliverable: the system MUST buffer messages to it, or dead-letter them, and never process them.
- Spawning returns the reference at step 2; messages sent before step 3 queue. A supervised re-creation (§9) emits `Restarted`, not a second `ActorReady`.

## 5. Resolution and location transparency

Resolving an actor id yields a reference with no network round trip. Sends through it enqueue by value to a live local actor, dead-letter for a local id with no live actor, and encode and route for a remote id; the system decides per call. Resolution MUST NOT block or contact the remote node and cannot fail: liveness surfaces when a send fails or through death watch. A local send performs no encode or decode, yet its observable result MUST equal the remote path's: one workload yields the same replies and ordering whether its targets are local or remote.

## 6. Execution

1. **One mailbox per actor.** Each local actor has one bounded mailbox feeding exactly one executor.
2. **Serial execution.** The executor processes one message to completion before the next, giving each actor a total order over the messages it observes.
3. **No reentrancy.** A handler MAY suspend; while it is suspended, the executor MUST NOT begin another message for that actor. Handlers SHOULD hand blocking work elsewhere.
4. **Backpressure.** When a mailbox is full, the system MUST apply a defined policy (await space, or fail with `MailboxFull`) and MUST NOT drop the message silently; a tell and a local ask await. A remote tell refused on the receiving node (§7) has no reply to carry the error; its trace is the refusal event (§3.6).
5. **Ordering.** Messages from one sender to one recipient, tells and asks alike, are observed in send order, within one association (cluster §4) for a remote recipient. No order holds across senders.
6. **Fairness.** The system SHOULD schedule executors fairly, so no ready actor starves.

## 7. Dispatch

**Manifests:** a manifest MUST stay stable across recompiles and renames; an explicit string such as `"myapp.Greet"` is RECOMMENDED. A breaking change to a message's shape SHOULD become a new message type with a new manifest.

**Registration:** every actor of a type MUST have its type's complete list of accepted messages (§3.1) before any message can reach it, with no setup step at run time. A local send never consults it. A generated listing MAY exist; a system MUST work with hand-written ones, and a hand-written manifest MUST override a generated one.

**Allowlist:** only registered message types are ever built from network bytes; an envelope's manifest is untrusted until it matches a registration, and one not registered for the recipient's actor type MUST be refused as `Unhandled`. The reserved manifest `actor.Terminated` (§12.1) MUST NOT be registrable, so no peer can inject a termination signal.

**Outbound:** every ask, whatever its path or outcome, MUST be bracketed on the event stream by one `AskIssued` and exactly one `AskOutcome`.

**Inbound:** a system MUST deliver an inbound envelope to its local recipient or refuse it with `DeadLetter` (no live actor holds the id), `Unhandled` (the manifest is not registered for the actor's type), `Serialization` (the payload does not decode), or `MailboxFull` (the receive path never waits on a full mailbox). Each refusal answers the ask, if any, with its error and emits `Undelivered` (§3.6). References inside the message are rebound (§3.2).

**Replies:** each ask MUST be answered exactly once, with the handler's reply or a call error. A reply carries successful and application-failed outcomes alike; a call error only a failure the handler never produced. Answering never applies backpressure to the handler.

## 8. Calls, delivery, and errors

### 8.1 Delivery

- **At most once** per call attempt: a system MUST NOT retransmit a delivered message on its own.
- **No silent loss.** Every ask MUST end in a reply or a call error. A pending ask whose target's node is declared down MUST complete with `Unreachable`; the system MAY do so earlier, once it judges the node unreachable; otherwise its deadline bounds it.
- Every ask MUST carry an effective deadline, explicit or the system's default (§3.2).
- **Ordering** (§6): no order holds between messages sent before an association was lost and messages sent over its replacement; a sender that needs it re-synchronizes with an ask round trip.

### 8.2 Call errors

A call error is one of a closed set, which callers handle exhaustively:

| Error | Meaning |
|---|---|
| `Timeout` | The call exceeded its deadline. |
| `Unreachable` | The target's node is down, or the association to it was lost. |
| `DeadLetter` | No live actor holds the id. |
| `Unhandled` | The target's actor type does not accept the manifest. |
| `MailboxFull` | The target's mailbox is full (§6). |
| `Serialization` | Encoding or decoding failed. |
| `System` | Any other failure of the system, including a refusal by authorization. |

- An application failure is carried inside the reply. When the reply is itself a result, the outcome of an ask MUST keep the two apart: whether the call completed, then what the handler decided.
- A handler's panic never crosses to another actor or over the wire: supervision (§9) contains it, and callers waiting on that actor see `Unreachable` or `DeadLetter`.

## 9. Supervision

An actor chooses a strategy: stop, resume, restart, or a **decider**, a plug point mapping each fault (a failed `started`, a handler panic, a child's escalation) to a directive (stop; restart, with a maximum count, a window, and a backoff; escalate; resume). The executor applies it.

- Every actor but a root has a parent, its spawner; escalating fails the parent, which applies its own strategy.
- The executor catches faults: a panic MUST become a supervision decision and MUST NOT crash the node.
- The default directive MUST be stop.
- Restart MUST construct a fresh actor keeping the actor id and mailbox: the failed actor's `stopped` runs with reason failed, then the fresh one's `started`. Only a factory-spawned actor can be re-created; for one spawned from a value, restart MAY degrade to stop. Exceeding the maximum restarts within the window MUST escalate to stop.
- Resume drops the failed message and keeps the actor; for a failed `started` it MUST degrade to stop.
- Backoff between restarts MUST be supported (exponential with jitter is RECOMMENDED), its delay and jitter drawn from the host environment (§3.5).
- The event stream MUST report the effective decision, after degradation and the restart window (§3.6).
- Supervision is local. A remote failure surfaces to callers as a call error (§8) and to watchers as a termination signal (§10).

## 10. Death watch

A watcher watches through its handler context and accepts the termination signal, `Terminated`, carrying the target's id and a reason: stopped, failed, or node down. An actor's own `stopped` hook sees only stopped or failed: after node down, no hook ran.

1. After a watch, if the target terminates for any reason, the watcher receives exactly one termination signal for it, through its mailbox.
2. **Node down implies termination.** When a node is declared down, every watched actor on it MUST yield a termination signal with reason node down, produced by the watcher's own node.
3. Watching an actor that has already terminated MUST yield its termination signal at once.
4. A termination signal arrives through the mailbox in the watcher's serial order, never out of band.

Guarantee 2 follows node-down decisions, not physical reality: where nothing declares a node down, watchers learn nothing from its crash. A termination signal is never an envelope; between nodes a clustered system carries it in its own frame (cluster §8).

## 11. Determinism

### 11.1 Obligations

1. **Time.** A system MUST take every timer from its clock (§3.5): deadlines, protocol intervals, backoff.
2. **Randomness.** A system MUST take every random choice from its entropy source: peer selection, jitter, generated identities.
3. **Tasks.** A system MUST create every background task through its spawner, bound to no specific async runtime.
4. **Layers above.** A component built on an actor system MUST take its node identity, time, randomness, task scheduling, and observability from the system (§3.3), never from the host.
5. **No observable nondeterminism.** Anything that crosses the wire or appears on the event stream MUST have a deterministic order: unordered iteration MUST NOT affect message order, peer selection, or reply timing.

### 11.2 The deterministic profile

A system SHOULD support the **deterministic profile**: running unchanged over virtual implementations of its host environment and network, so a whole cluster runs in one process over virtual time. A system or layer that claims it MUST satisfy:

1. **Seed reproducibility.** Given the same seed, configuration, and workload, two runs MUST produce identical event streams. All nondeterminism MUST pass through the host environment (§3.5) and the system's network seams (cluster §3).
2. **Quiescence-driven time.** Logical time MUST advance only when no task is ready to run.
3. **No ambient nondeterminism.** A build under the profile MUST NOT read the wall clock, spawn operating-system threads, or use an unseeded random source, and SHOULD enforce this statically.

## 12. Registries

### 12.1 Reserved manifests

| Manifest | Use |
|---|---|
| `actor.Terminated` | The termination signal (§10). Never registrable, never carried as an envelope. |

### 12.2 Event kinds

| Kind | Emitted when |
|---|---|
| `AssignId`, `ActorReady`, `ResignId`, `Restarted` | The lifecycle steps of §4, and a supervised re-creation. |
| `Enqueue`, `DispatchStart`, `DispatchEnd` | A message enters a mailbox; a handler begins and ends. |
| `AskIssued`, `AskOutcome` | The bracket around every ask (§7). |
| `Undelivered` | An inbound envelope is refused (§7). |
| `Supervised` | The effective supervision decision (§9). |
| `TerminatedDelivered` | A termination signal reaches a watcher (§10). |
| `Suspected`, `Unreachable`, `Reachable`, `NodeDown` | Reachability transitions and the node-down decision (cluster §5, §6). |
| `MemberJoining`, `MemberUp`, `MemberDraining`, `MemberResumed`, `RegistrySynced` | Membership transitions (cluster §5). |
| `LeaderElected` | A consensus group elects a leader (cluster §3.5). |
| `SingletonStarted`, `SingletonStopped` | The cluster singleton's hand-over (utilities §5). |
| `App` | An event of a layer above (§3.6). |

## 13. Invariants

| # | Invariant | Defined in |
|---|---|---|
| 1 | **No silent loss.** Every ask ends in exactly one reply or call error; none remains pending once the system is quiescent. | §8 |
| 2 | **A crash completes in-flight calls.** An ask whose target's node is declared down completes with `Unreachable`. | §8.1, cluster §5.4 |
| 3 | **Per-pair FIFO.** Messages from one sender to one recipient are observed in send order within one association. | §6 |
| 4 | **Serial, non-reentrant execution.** An actor never processes two messages concurrently. | §6 |
| 5 | **Bounded, non-dropping mailbox.** A full mailbox waits or answers `MailboxFull`; it never drops a message silently. | §6 |
| 6 | **Lifecycle order, each step once.** Assign, ready, and resign occur in order, each exactly once, even when the spawn fails; the id is undeliverable between assign and ready. | §4 |
| 7 | **Resolution is local.** Classifying an id as local or remote performs no network round trip. | §5 |
| 8 | **Manifest dispatch and allowlist.** An unregistered pair of actor type and manifest yields `Unhandled`; no type outside the registered messages is built from network bytes. | §7, §3.4, §14 |
| 9 | **Local sends skip serialization.** A local send performs no encode or decode, yet observes what the remote path would. | §5, §7 |
| 10 | **References are rebound.** A reference inside a message or reply is rebound to the receiving system and usable there. | §7 |
| 11 | **Death watch delivers exactly once.** After a watch, the watcher receives exactly one termination signal for any cause, node down included. | §10 |
| 12 | **Watch after death.** Watching an actor that has already terminated yields its termination signal at once. | §10 |
| 13 | **Signal ordering.** A termination signal arrives through the mailbox in serial order. | §10 |
| 14 | **Membership convergence.** Once faults cease and partitions heal, all `up` members converge on one membership set within bounded logical time. | cluster §5.2, §5.4 |
| 15 | **`down` is terminal.** A node observed `down` never reappears `up` under the same incarnation. | cluster §5.1 |
| 16 | **Partition tolerance.** Under the default downing policy a partition alone never moves a member to `down`; registry-based mode holds this unconditionally, leader-based mode for any side without a quorum. | cluster §5.4 |
| 17 | **Suspicion is refuted.** A node that sees itself suspected refutes it with a higher incarnation, clearing the suspicion cluster-wide. | cluster §6 |
| 18 | **Supervision containment.** A handler panic never crashes the node; the default directive is stop; restarts back off; exceeding the maximum within the window escalates. | §9 |
| 19 | **Receptionist consistency.** Registrations of actors on a down node are pruned and subscribers notified; subscribing delivers the current listing, then every change; concurrent registrations merge. | §3.7 |
| 20 | **Type safety.** A send of a message type the target does not accept is rejected before run time. | §3.2 |
| 21 | **Location transparency.** One workload, with its targets local or remote, produces identical replies and ordering. | §3.2 |
| 22 | **Quorum-gated control plane.** Every leader-based membership transition is a quorum-committed entry applied in log order; at most one leader per term issues them; a side without a voter quorum commits none; each member's view is a prefix of the log, never a gap. | cluster §5.4.3 |

## 14. Security considerations

No inbound envelope can construct a type outside the recipient's registered messages (§7, §3.4), and no peer can inject a termination signal (§7). Trust between nodes (authentication, encryption, authorization) is the cluster's (cluster §11). A single-node system exposes no network surface.
