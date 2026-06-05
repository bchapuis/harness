# Distributed Actor Framework for Rust — Specification

**Status:** Draft v4
**Scope:** A location-transparent, fault-tolerant distributed actor system for Rust, with a **message-first** API.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** are to be interpreted as in RFC 2119.

`dactor` is used throughout as the placeholder crate/namespace name.

> **Design stance.** The framework uses only ordinary Rust traits and generics; it provides **no macros of its own**. Actors exchange **messages** — serializable value types — and declare one `Handler<M>` for each message type they accept. Each message has a hand-written `const` wire identity (its *manifest*), and each actor lists the messages it accepts over the network in a hand-written `register` function (§4.4). The only macros in user code are serde's `Serialize`/`Deserialize`.
>
> This is a **deliberate trade-off**, not a free win. Hand-written manifests and `register` lists keep the *framework* simple and the wire contract explicit, inspectable, and versionable — at the cost of pushing mechanical boilerplate onto the *user* (one `const` per message, one `accept` line per remote message). Because that boilerplate is derivable, an **optional** derive MAY generate both (§4.4); the goal is *no required codegen* (§1.1), not *no codegen permitted*. The hand-written form is normative and remains the override path for anyone who needs to pin a manifest by hand.

---

## 1. Design goals and non-goals

### 1.1 Goals
- **Location transparency.** Sending a message to a local actor and to a remote actor is the same call. Whether the target is local or on another node is decided at runtime, never in the source.
- **Isolation by construction.** Actor state is never shared. The type system prevents concurrent access to it.
- **Explicit wire contract.** Every cross-node payload is a named, serializable message type. You can version, log, persist, and inspect the protocol; it is not an implicit side effect of a method signature.
- **Robustness.** Node and actor failure are first-class, observable events. The system tolerates partial failure and network partitions, and never drops a request without reporting an error.
- **Pluggability.** Serialization, transport, and the actor system itself are traits. The cluster runtime is one implementation, not the only one.
- **No required codegen.** The whole framework is plain generic code; serde's derives are the only macros it *relies on*. An optional derive that defaults a message's manifest or an actor's `register` list (§4.4) is permitted as a convenience — it lowers user boilerplate without becoming a dependency of the model. What is excluded is *required* codegen: nothing the framework mandates may need a macro.

### 1.2 Non-goals
- **Generic message handlers over the wire.** A message that crosses the wire MUST be a concrete type (Rust monomorphizes; there is no runtime type to send). Generic actors are fine, but the *message* and its *reply* MUST be concrete, serializable types.
- **Transparent failure masking.** The framework MUST NOT auto-retry messages with side effects. Retries and idempotency are the caller's decision.
- **Strong global consistency.** Membership and the receptionist are eventually consistent. The framework provides no built-in consensus.
- **Shared memory across nodes.** All communication is by message; nothing is shared.

---

## 2. Terminology

| Term | Definition |
|---|---|
| **Actor** | A unit of state plus behavior that processes messages one at a time (serially). |
| **Message** | A serializable value sent to an actor. Each message type declares its `Reply` type. |
| **Handler** | An actor's implementation (`Handler<M>`) for one message type `M`. |
| **`ActorRef<A>`** | A serializable, cloneable, typed handle to an actor of type `A`. The only way to address an actor; never grants access to state. |
| **`ActorId`** | A cluster-unique, serializable identity assigned by the system. |
| **Manifest** | The stable wire identifier of a message type; the dispatch key. |
| **System** | An implementation of [`ActorSystem`](#4-the-actorsystem-contract); owns local actors and the runtime. |
| **Node** | One running `System` instance with a network identity. |
| **Cluster** | A set of nodes that have formed associations and share membership. |
| **Association** | An established, authenticated connection between two nodes. |
| **Mailbox** | The bounded queue feeding an actor's serial executor. |
| **Dispatch registry** | Maps `(actor type, manifest) → typed handler invocation`. |

---

## 3. The actor model

### 3.1 Actors

An actor is an ordinary Rust struct that implements the [`Actor`](#311-the-actor-trait) trait. Its fields are private state, reachable only from its own handlers.

```rust
pub struct Greeter {
    greeting: String,
    served: u64,
}

impl Actor for Greeter {
    type System = ClusterSystem;
}
```

#### 3.1.1 The `Actor` trait

```rust
pub trait Actor: Sized + Send + 'static {
    type System: ActorSystem;

    /// Called once after spawn, before the first message. Returning Err aborts startup.
    async fn started(&mut self, _ctx: &Ctx<Self>) -> Result<(), BoxError> { Ok(()) }

    /// Called once when the actor stops, for any reason.
    async fn stopped(self, _reason: StopReason) {}
}
```

An actor's identity (`ActorId`) and its `system`/`self` handle come from its [`Ctx`](#34-context-ctx); the user does not store them. Two actors are equal iff their `ActorId`s are equal; `Hash`/`Eq` on an `ActorRef` derive from the `ActorId`.

### 3.2 Messages and handlers

A **message** is a serializable value type that declares its reply type and its stable wire identity:

```rust
pub trait Message: SerializationRequirement {
    type Reply: SerializationRequirement;
    /// Stable, author-controlled wire identity and dispatch key (§4.4).
    /// Written by hand, or defaulted by an optional derive (§4.4). Stable across recompiles and renames.
    const MANIFEST: Manifest;
}
```

An actor accepts a message by implementing `Handler<M>`:

```rust
pub trait Handler<M: Message>: Actor {
    async fn handle(&mut self, msg: M, ctx: &Ctx<Self>) -> M::Reply;
}
```

Example:

```rust
#[derive(Serialize, Deserialize)]   // serde only; no framework macro
struct Greet { name: String }
impl Message for Greet {
    type Reply = String;
    const MANIFEST: Manifest = Manifest::new("myapp.Greet");
}

impl Handler<Greet> for Greeter {
    async fn handle(&mut self, msg: Greet, _ctx: &Ctx<Self>) -> String {
        self.served += 1;
        format!("{}, {}!", self.greeting, msg.name)
    }
}
```

Rules (MUST hold):
1. `handle` takes `&mut self`. Exclusive mutation is sound because the executor is serial (§6).
2. `M` and `M::Reply` MUST satisfy [`SerializationRequirement`](#5-serialization) (§5). This is an ordinary trait bound, checked at compile time.
3. `M` MUST be concrete at the point of the `Handler` impl (§1.2).
3a. `M::MANIFEST` MUST be a stable, author-chosen identity, unique among the message types a given actor accepts (§4.4). A local-only actor still declares it, but it costs only this one `const` line and no registration: the local fast path (§4.3) never reads it.
4. **Application errors live in the reply.** A handler that can fail uses `type Reply = Result<T, E>` where `T, E: SerializationRequirement`. An application failure is a value, distinct from a transport failure (`CallError`, §14).
5. The set of messages an actor accepts is exactly the set of `M` for which it implements `Handler<M>`. Anything else is a compile error at the call site (§3.3), or `CallError::Unhandled` on the wire (§4.4).

### 3.3 `ActorRef` and location transparency

`ActorRef<A>` is the only handle to an actor. It is `Clone + Serialize + DeserializeOwned + Send + Sync`, and holds exactly the `ActorId` plus a handle to the local system; it carries **no** state.

```rust
pub struct ActorRef<A: Actor> {
    id: <A::System as ActorSystem>::ActorId,
    system: SystemHandle<A::System>,
}

impl<A: Actor> ActorRef<A> {
    /// Request/response. The `A: Handler<M>` bound proves, at compile time,
    /// that this actor accepts M — so only valid messages are sendable.
    pub async fn ask<M>(&self, msg: M) -> Result<M::Reply, CallError>
    where A: Handler<M>, M: Message;

    /// Fire-and-forget. Errors only for enqueue/transport failure, not handler outcome.
    pub async fn tell<M>(&self, msg: M) -> Result<(), CallError>
    where A: Handler<M>, M: Message;

    /// Same as `ask` but with an explicit deadline overriding the system default.
    pub async fn ask_timeout<M>(&self, msg: M, within: Duration) -> Result<M::Reply, CallError>
    where A: Handler<M>, M: Message;
}
```

- The `A: Handler<M>` bound is the dispatch mechanism: it proves at compile time that `A` accepts `M`, so invalid sends do not compile. No runtime check is needed.
- `ask`/`tell` are **identical** for local and remote targets. The system decides at call time whether to enqueue locally or send over a transport (§4.4).
- An `ActorRef` MAY be a field of a message or of an `M::Reply`. On the wire only the `ActorId` travels; the receiving node rebinds it to its own system on decode, giving a working local-or-remote `ActorRef` there (§4.3).
- An `ActorRef` MUST NOT expose actor state or handlers.

### 3.4 Context (`Ctx`)

Handlers and lifecycle hooks receive a `Ctx<A>`, which grants controlled capabilities without breaking isolation:

```rust
impl<A: Actor> Ctx<A> {
    fn id(&self) -> &ActorId;
    fn this(&self) -> ActorRef<A>;                  // self-reference, shareable
    fn system(&self) -> &A::System;

    fn spawn<C: Actor<System = A::System>>(&self, child: C) -> ActorRef<C>; // parented to self (§11)
    fn watch<B: Actor>(&self, target: &ActorRef<B>);                        // death watch (§12)
    fn unwatch<B: Actor>(&self, target: &ActorRef<B>);
    fn stop(&self);                                                          // stop self after current message
}
```

### 3.5 Isolation guarantees

The framework enforces actor isolation through ownership:

1. An actor value is **owned** by its mailbox task and reachable through no other path. No API returns `&A` or `&mut A` to a caller.
2. All state access happens inside the actor's own handlers, run by its serial executor. So `&mut self` in `handle` needs no locking and has no data races.
3. An `ActorRef` is the only object held externally, and it is stateless. Sharing one across threads or nodes is always safe.

#### 3.5.1 Local-only access (`when_local`)

For testing and local optimizations, a system MAY offer:

```rust
impl<A: Actor> ActorRef<A> {
    pub async fn when_local<F, R>(&self, f: F) -> Option<R>
    where F: FnOnce(&mut A) -> R + Send;   // runs on the actor's executor iff local
}
```

`when_local` MUST run `f` on the actor's serial executor (preserving isolation) and MUST return `None` if the actor is remote. It is the only sanctioned exception to location transparency and SHOULD be limited to tests.

### 3.6 Identity (`ActorId`)

An `ActorId` MUST:
- Be unique within a cluster for the cluster's lifetime.
- Let any node identify the target's **owning node** and classify it as local or remote, from the id alone, without a network round-trip (the capability `resolve` depends on, §4.3).
- Carry an **incarnation** that distinguishes a fresh actor from a resigned one that reused the same name.
- Be `Clone + Eq + Hash + Send + Sync + Serialize + DeserializeOwned`.

These are requirements on what an `ActorId` lets a node *do*, not on how it is laid out. The structure below satisfies them and is RECOMMENDED, but a conforming id MAY be represented differently (e.g. an opaque key resolved against the node's local routing state) as long as locality stays classifiable without contacting another node.

Recommended (non-normative) structure:

```
ActorId = {
    node:        NodeId,        // cluster node identity (uid + endpoint)
    path:        Path,          // hierarchical name, e.g. /user/greeter
    incarnation: u64,           // monotonic per path on the owning node
}
```

A few paths are **well-known** (e.g. `/system/receptionist`) and resolvable on every node without prior introduction (§13).

---

## 4. The `ActorSystem` contract

The system is the runtime an actor runs on. The cluster runtime (§9–13) is one implementation. The transport-facing methods work on **already-serialized payloads**; the typed API and the local fast path live in the `ActorRef`/mailbox layer above this trait.

### 4.1 The trait

```rust
pub trait ActorSystem: Send + Sync + 'static {
    type ActorId: Clone + Eq + Hash + Send + Sync + Serialize + DeserializeOwned;

    // ---- Lifecycle (§4.2) ----
    fn assign_id<A: Actor<System = Self>>(&self) -> Self::ActorId;
    fn actor_ready<A: Actor<System = Self>>(&self, actor: A) -> ActorRef<A>;
    fn resign_id(&self, id: &Self::ActorId);

    // ---- Resolution (§4.3) ----
    fn resolve<A: Actor<System = Self>>(
        &self, id: &Self::ActorId,
    ) -> Result<ActorRef<A>, ResolveError>;

    // ---- Outbound, transport boundary (§4.4) ----
    async fn remote_ask(
        &self, recipient: &Self::ActorId, manifest: Manifest, payload: Bytes, within: Duration,
    ) -> Result<Bytes, CallError>;

    async fn remote_tell(
        &self, recipient: &Self::ActorId, manifest: Manifest, payload: Bytes,
    ) -> Result<(), CallError>;

    // ---- Inbound, transport boundary (§4.4) ----
    async fn deliver(
        &self, recipient: &Self::ActorId, manifest: Manifest, payload: Bytes, reply: ReplyHandle,
    );
}
```

> **Object safety.** `async fn` in traits is not dyn-compatible without boxing. Where a system must be used behind `dyn`, the implementation SHOULD expose boxed-future variants (`Pin<Box<dyn Future>>`). Code that is generic over the system SHOULD prefer static dispatch.

The user-facing entry point is `spawn`, which MUST run the lifecycle hooks in order:

```rust
fn spawn<A: Actor<System = Self>>(&self, actor: A) -> ActorRef<A>  // = assign_id → register mailbox → actor_ready
```

### 4.2 Lifecycle ordering

For each actor, the system MUST observe exactly this order:

```
1. id ← assign_id::<A>()          // reserve identity; MUST be unique; MUST NOT be observable yet
2. mailbox created, executor started, actor bound; A::started() runs before first user message
3. actor_ready(actor) → ActorRef  // id becomes resolvable; messages may now be delivered
4. … actor processes messages …
5. resign_id(id)                  // on stop/failure; A::stopped() runs; releases all resources; idempotent
```

Invariants (MUST):
- `assign_id` is called exactly once before any message can be delivered.
- `actor_ready` is called exactly once; after it returns, `resolve(id)` succeeds and the mailbox accepts messages.
- `resign_id` is called exactly once, even if spawn fails between steps 1 and 3 (in which case it follows `assign_id` directly).
- Between `assign_id` and `actor_ready` the id is reserved but **not** deliverable; messages to it MUST be dead-lettered or buffered, never silently accepted.

### 4.3 Resolution: local vs remote

`resolve(id)` returns an `ActorRef` and classifies the target without any network round-trip:

- If the id's owning node is the local node and a live mailbox exists → a **local** `ActorRef` (fast path: messages enqueue directly, by value, without serialization).
- If the id's owning node is the local node but no live mailbox exists → an `ActorRef` that dead-letters (the actor has resigned).
- If the id's owning node is remote → a **remote** `ActorRef` (messages serialize and route through a transport).

`resolve` MUST NOT block or contact the remote node to check existence. Liveness is found when a message is sent, or via failure detection (§10). On a malformed or foreign `id`, it MUST return `ResolveError`.

### 4.4 Manifests, dispatch, and message flow

Every message type carries a stable **manifest** (`Message::MANIFEST`, §3.2): its wire identity and its dispatch key. There is exactly one such identifier per message, and it is author-controlled.

- The manifest MUST be stable across recompiles and renames. An explicit string (e.g. `"myapp.Greet"`) is RECOMMENDED. A breaking change to the message's shape SHOULD become a new message type with a new manifest, rather than a silent redefinition of an existing one.
- The **dispatch registry** maps `(actor type, manifest) → typed dispatch entry`. A dispatch entry knows how to deserialize `M` from a payload, enqueue `Handler::<M>::handle` on the resolved local actor's executor, and serialize `M::Reply`.

**Registration.** An actor that can be addressed remotely lists the messages it accepts over the network by implementing `RemoteActor`. Each `r.accept::<M>()` call is an ordinary generic function that captures the monomorphized dispatch entry for `(Self, M)`.

```rust
pub trait RemoteActor: Actor {
    /// List the messages this actor accepts over the network.
    /// Invoked once per actor type by `spawn` (guarded so it runs at most once).
    fn register(r: &mut HandlerRegistry<Self>);
}

pub struct HandlerRegistry<A: Actor> { /* … */ }
impl<A: Actor> HandlerRegistry<A> {
    pub fn accept<M>(&mut self) where A: Handler<M>, M: Message;  // generic library fn, no codegen
}
```

- `spawn` (§4.1) MUST call `A::register` the first time it spawns an actor of a `RemoteActor` type, filling the registry before any message can arrive. No separate setup step or link-time collection is needed.
- Registration is **inbound-remote only**: a purely local actor (never registered, never sent across nodes) needs no `RemoteActor` impl — its messages flow by value (§4.3).
- A network-delivered message whose `(actor type, manifest)` is not registered MUST yield `CallError::Unhandled`. The registry is also the deserialization allowlist (§5.3, §15.3): only listed message types are ever built from network bytes.

> **Optional derive (non-normative).** Both hand-written pieces above — a message's `MANIFEST` and an actor's `register` body — are mechanical and MAY be generated by an optional derive (e.g. `#[derive(Message)]` defaulting the manifest from the type path; `#[derive(RemoteActor)]` emitting one `accept::<M>()` per `Handler<M>` impl). Such a derive is a convenience layered *above* the model, not part of it: an implementation MUST work with the manifests and `register` lists written by hand, and a hand-written manifest MUST always override a derived default. This keeps the explicit-wire-contract guarantee while removing the per-message boilerplate (see the design stance above §1, and the §1.1 codegen goal).

**Outbound — `ActorRef::ask<M>` (typed layer above the trait):**
1. Resolve locality of `self.id`.
2. **Local:** enqueue `msg` *by value* on the mailbox; await the typed reply. No serialization occurs.
3. **Remote:** `payload = codec.serialize(&msg)`; `bytes = system.remote_ask(&self.id, M::MANIFEST, payload, deadline).await?`; return `codec.deserialize::<M::Reply>(&bytes)?`.

`tell<M>` is identical but expects no reply (`remote_tell`); for a local target it enqueues without a reply channel.

**Inbound — system receive loop (remote path):**
1. Decode the envelope header → `recipient: ActorId`, `manifest: Manifest`, optional `correlation`.
2. `resolve(recipient)`; if no live local actor → reply `CallError::DeadLetter`.
3. `system.deliver(recipient, manifest, payload, reply)`, which:
   a. Looks up `(type of the resolved actor, manifest)` in the dispatch registry; if absent → `CallError::Unhandled`.
   b. Deserializes `M` from `payload`.
   c. Enqueues `Handler::<M>::handle` on the actor's serial executor and awaits its completion.
   d. Routes the reply through the `ReplyHandle`.

`ActorRef` values inside a message or reply carry only their `ActorId` on the wire and are rebound to the receiving system on decode (§4.3).

### 4.5 Reply handling

```rust
pub struct ReplyHandle { /* opaque; holds correlation + reply channel */ }

impl ReplyHandle {
    pub async fn send<R: SerializationRequirement>(self, reply: R);  // serialize + return to caller
    pub async fn fail(self, failure: CallError);                     // transport/system-level failure
    pub fn none(self);                                               // for `tell`: no reply expected
}
```

`deliver` MUST resolve exactly one of these per `ask`. Because application errors live inside `M::Reply` (§3.2), `send` carries both successful and application-failed outcomes; `fail` is reserved for transport or system failures the handler never produced.

### 4.6 Runtime environment (clock, randomness, concurrency)

The runtime needs three capabilities from its environment: time, randomness, and task spawning. A system MUST get each one through a trait, and MUST NOT read it from the host directly. This is what lets a system run under deterministic simulation (§18); each capability is one ordinary trait.

```rust
/// Virtual or real time. No subsystem may read wall-clock time directly.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Instant;
    async fn sleep(&self, dur: Duration);
    async fn timeout<F: Future>(&self, within: Duration, f: F) -> Result<F::Output, Elapsed>;
}

/// The single source of randomness. Seedable; the only randomness in the system.
pub trait Entropy: Send + Sync + 'static {
    fn next_u64(&self) -> u64;
}

/// Task spawning. The mailbox executors, gossip, and the failure detector run through this.
pub trait Spawner: Send + Sync + 'static {
    /// Named `launch`, not `spawn`, so a raw task is never confused with
    /// spawning an actor (`ActorSystem::spawn` / `Ctx::spawn`, §3.4, §4.1).
    fn launch(&self, task: BoxFuture<'static, ()>);
}
```

Rules (MUST):
1. **All timing** — `ask` deadlines (§14.2), SWIM intervals (§10), gossip periods (§9.2), supervision backoff (§11.2) — MUST come from `Clock`. No subsystem reads the wall clock or a host timer directly.
2. **All randomness** — gossip peer selection (§9.2), SWIM's `k` members (§10.2), backoff jitter (§11.2), `NodeId` generation (§3.6, §9.1) — MUST come from `Entropy`.
3. **All background tasks** MUST be created through `Spawner`. The mailbox executor (§6) and the detector loops (§10) MUST NOT bind themselves to a specific async runtime.
4. **No observable nondeterminism.** Anything that crosses the wire or appears in §16 events MUST have a deterministic order. A system MUST NOT let unordered iteration (e.g. `HashMap` order) affect message ordering, peer selection, or reply timing.

The production runtime supplies a wall-clock `Clock`, an OS-seeded `Entropy`, and a multi-threaded `Spawner`. The simulator (§18) supplies virtual versions driven by a single seed; no other code changes.

---

## 5. Serialization

`SerializationRequirement` is the bound every wire-crossing value must satisfy. It is a parameter of the system.

```rust
pub trait SerializationRequirement:
    Serialize + DeserializeOwned + Send + 'static {}
impl<T: Serialize + DeserializeOwned + Send + 'static> SerializationRequirement for T {}
```

Rules:
1. Every message type and reply type MUST satisfy `SerializationRequirement`. This is enforced at compile time by the `Message`/`Handler` bounds.
2. The concrete **codec** (e.g. `postcard`, `bincode`, CBOR, JSON, Protobuf) is pluggable and fixed per system. Both ends of an association MUST agree on the codec.
3. The **manifest** (§4.4) identifies the concrete message type on the wire. The dispatch registry maps a manifest to a deserialize-and-dispatch entry. Decoding MUST reject unknown manifests (`CallError::Unhandled`) and MUST NOT build arbitrary types from untrusted input.
4. Serialization failures are reported as errors (`CallError::Serialization`); they MUST NOT panic the receive loop.

---

## 6. Execution model

1. **One mailbox per actor.** Each local actor has a bounded mailbox (a multi-producer, single-consumer queue) feeding exactly one executor task.
2. **Serial execution.** The executor processes one message to completion before the next. This is what makes `&mut self` sound (§3.5) and gives each actor a total order over the messages it observes.
3. **Cooperative async.** A handler MAY `.await`. While suspended, the executor MUST NOT begin another message for that actor (no reentrancy). Handlers SHOULD avoid blocking the thread; blocking work SHOULD be offloaded.
4. **Backpressure.** Mailboxes are bounded. When full, the system MUST apply a defined policy: `await` until space (default), or fail the send with `CallError::MailboxFull`. It MUST NOT drop messages silently.
5. **Ordering guarantee.** Messages from a single sender to a single recipient are delivered in send order (FIFO per directed pair). No ordering is guaranteed across different senders. `tell` and `ask` from the same sender share this order.
6. **Fairness.** The runtime SHOULD schedule executors fairly so no ready actor is starved.

---

## 7. Transport

A transport is pluggable behind a trait; the default is TCP with length-delimited framing. Requirements:

1. **Associations.** Before exchanging actor traffic, two nodes MUST complete a handshake establishing an association: protocol-version check, node identity exchange, codec agreement, and optional authentication (§15). Actor envelopes are sent only over an established association.
2. **Multiplexing.** Many actor conversations share one association. Each request carries a correlation id; responses reference it.
3. **Framing.** Messages are length-delimited; a malformed frame MUST tear down the association, not the node.
4. **Lifecycle signals.** The transport MUST report association establishment and loss to the membership/failure-detection subsystems (§9, §10).

Like `ActorSystem` (§4.1), the transport is a trait, so the default TCP transport and the simulator's in-memory network (§18) are two implementations of the same trait; nothing above this layer can tell them apart:

```rust
pub trait Transport: Send + Sync + 'static {
    /// Establish (or reuse) an authenticated association to a node (§7.1, §15).
    async fn connect(&self, peer: &NodeId) -> Result<(), TransportError>;

    /// Send one frame to `peer` over its association. At-most-once (§7.2).
    async fn send(&self, peer: &NodeId, frame: Frame) -> Result<(), TransportError>;

    /// Inbound frames from all associations, demultiplexed by the receive loop (§4.4).
    fn inbound(&self) -> impl Stream<Item = (NodeId, Frame)> + Send;

    /// Association lifecycle, feeding membership and failure detection (§9, §10).
    fn events(&self) -> impl Stream<Item = TransportEvent> + Send; // Associated | Lost
}
```

### 7.1 Wire protocol

Two message families travel over an association: **actor envelopes** and **system messages**.

```
Envelope {
    recipient:   ActorId,
    manifest:    Manifest,         // message type id; the dispatch key (§4.4)
    correlation: Option<CallId>,   // Some ⇒ a reply is expected (ask); None ⇒ one-way (tell)
    payload:     Bytes,            // encoded message
}

Reply {
    correlation: CallId,
    outcome:     Ok(Bytes) | Failure(CallError),   // application errors are inside Ok(Bytes) as M::Reply
}

SystemMessage =
    | Handshake(..) | HandshakeAck(..)
    | Membership(GossipDigest)            // §9
    | Swim(Ping | Ack | PingReq | ..)     // §10
    | Watch(ActorId) | Unwatch(ActorId) | Terminated(ActorId, Reason)  // §12
    | Receptionist(..)                    // §13
```

### 7.2 Delivery guarantees

- **At-most-once** per call attempt. The framework MUST NOT transparently retransmit a delivered message.
- **No silent loss.** Every `ask` MUST terminate in a reply, a `CallError::Timeout`, or a `CallError::Unreachable`. A pending ask whose target node is declared `down` (§10) MUST be completed with `Unreachable`.
- **Ordering** per directed sender→recipient pair, as in §6.5, holds end-to-end over a single association.
- Higher guarantees (exactly-once, durable delivery) are out of scope and, if needed, MUST be built atop this layer with explicit idempotency keys.

---

## 8. Failure model overview

Failures the system MUST represent explicitly (never mask):

| Failure | Detected by | Surfaced as |
|---|---|---|
| Target actor does not exist / has resigned | `resolve` / receive loop | `CallError::DeadLetter` |
| No handler registered for the message type | `deliver` / registry | `CallError::Unhandled` |
| Application error from a handler | inside the reply | `M::Reply` (e.g. `Result<T, E>`) |
| Call exceeded its deadline | caller timer | `CallError::Timeout` |
| Recipient node unreachable / down | failure detector (§10) | `CallError::Unreachable` |
| (De)serialization failure | codec | `CallError::Serialization` |
| Association lost | transport | `CallError::Unreachable` |

Actor-level faults (a handler panics or the actor stops) are handled by **supervision** (§11). Node-level faults are handled by **membership + failure detection** (§9–10) and propagated to watchers via **lifecycle monitoring** (§12).

### 8.1 The node-down cascade

Declaring a node `down` is one event whose consequences are owned by five different subsystems. The detail lives with each owner; this is the single trace tying them together. When node `N` is declared `down`:

1. **Detection → decision (§10, §9.2.4).** SWIM suspicion that goes unrefuted for `T_suspect` makes `N` `unreachable`; the downing policy then moves `unreachable → down`. `unreachable` is a suspicion, `down` is the decision.
2. **Membership (§9.1).** The transition is gossiped; `down` is **terminal** — `N` MUST NOT reappear `up` under the same incarnation and MUST restart with a new `NodeId` to rejoin. The entry is later tombstoned (`removed`).
3. **In-flight callers (§7.2, §14).** Every pending `ask` whose target is on `N` completes with `CallError::Unreachable` — it never hangs (invariant §18.5 #2). This is the guarantee plain request/response cannot give.
4. **Watchers (§12.2).** No stop message can arrive from a dead node, so each local failure detector **synthesizes** a `Terminated { reason: NodeDown }` for every watched actor on `N`, delivered into each watcher's mailbox in serial order (§12.4).
5. **Receptionist (§13.3).** Because the receptionist watches the actors it lists, step 4 drives it: every registration originating from `N` is pruned and subscribers receive a fresh `Listing`.
6. **Routing afterward (§4.3).** `resolve` of any `ActorId` on `N` still returns a (remote) `ActorRef`; sends to it fail with `Unreachable` rather than blocking, since liveness is discovered on send, not in `resolve`.

A graceful **leave** (§9.3) reaches the same terminal `down`/`removed` state and the same steps 3–6, differing only in step 1: the node announces `leaving` and drains instead of being suspected.

---

## 9. Cluster membership and node lifecycle

Nodes form a cluster by associating with seed nodes and gossiping membership. Membership is **eventually consistent**.

### 9.1 Member states

```
joining → up → leaving → down → removed
                 ▲          ▲
   (reachability: reachable ⇄ unreachable, orthogonal to the above)
```

- **joining** — handshake complete, not yet admitted to full participation.
- **up** — full member; may host and address actors.
- **leaving** — graceful shutdown initiated; draining.
- **down** — declared dead (gracefully or by failure detection). **Terminal and irrevocable**: a node that was `down` MUST NOT rejoin under the same incarnation; it MUST restart with a new `NodeId`.
- **removed** — tombstone, eventually pruned from gossip.

**Reachability** (`reachable`/`unreachable`) is an orthogonal flag set by the failure detector (§10). `unreachable` is a suspicion; `down` is a decision.

### 9.2 Convergence

1. Membership is disseminated by **gossip**: each node periodically exchanges a digest with a random peer and merges newer information.
2. Each member entry carries an **incarnation** number; higher incarnation wins, letting a node refute a stale suspicion about itself.
3. Transitioning a member to `up` or `down` is a cluster decision. A single elected **leader** (the lowest-address `up`, reachable member) SHOULD perform these transitions, and MUST act only when membership has **converged** (all `up` members agree on the current set), to avoid split decisions.
4. A network partition leaves each side seeing the other as `unreachable`. The framework MUST NOT auto-`down` across a partition by default; downing policy (manual, timeout-based, or quorum-based) is configurable, and the default SHOULD be conservative.

### 9.3 Joining and leaving

- **Join:** new node handshakes a seed → `joining`; leader moves it to `up` on convergence.
- **Graceful leave:** node announces `leaving`, drains, then is moved to `down`/`removed`. Watchers (§12) of its actors are notified.
- **Crash:** detected by §10; failure detector marks `unreachable`, downing policy moves it to `down`.

---

## 10. Failure detection (SWIM)

Each node runs a SWIM-style detector over its associations.

1. **Direct probing.** Periodically (every `T_probe`), pick a member and send `Ping`; expect `Ack` within `T_rtt`.
2. **Indirect probing.** On a missed `Ack`, ask `k` random members to `PingReq` the target on the prober's behalf. If any relays an `Ack`, the target is alive.
3. **Suspicion.** If direct and indirect probes both fail, mark the target `suspect` and gossip the suspicion. A suspicion carries the suspected node's incarnation.
4. **Refutation.** A node that sees itself suspected increments its incarnation and gossips an `alive` override, clearing the suspicion cluster-wide.
5. **Confirmation.** A suspicion unrefuted for `T_suspect` becomes `unreachable`; the downing policy (§9.2.4) then MAY move it to `down`.
6. **Piggybacking.** Membership and suspicion updates SHOULD ride on ping/ack messages to bound overhead and speed dissemination.

Parameters (`T_probe`, `T_rtt`, `k`, `T_suspect`) MUST be configurable. `T_suspect` SHOULD scale with cluster size (e.g. logarithmically) to keep the false-positive rate low.

---

## 11. Supervision

Supervision governs what happens when a **local** actor faults: a handler panics, or `A::started` fails.

### 11.1 Hierarchy
- Every actor except the roots has a **parent**, the actor that spawned it (`Ctx::spawn`, §3.4). Parents supervise children.
- Faults are caught at the executor boundary; a panic MUST be unwound and converted to a supervision decision, never crash the node.

### 11.2 Strategies

```rust
pub enum SupervisionDirective {
    Stop,                                  // terminate the actor; notify watchers (§12)
    Restart { max: u32, within: Duration, backoff: Backoff }, // re-create state and resume
    Escalate,                              // fail the parent, applying the parent's strategy
    Resume,                                // keep state, drop the failed message (use sparingly)
}
```

- The **default** directive MUST be `Stop`. For transient faults, `Restart` is usually the better choice.
- **Restart** MUST construct a fresh actor value (state is not preserved by default); the actor keeps its `ActorId` and mailbox. Exceeding `max` restarts `within` the window MUST escalate to `Stop`.
- **Backoff** between restarts MUST be supported (exponential with jitter RECOMMENDED) to avoid hot-restart loops.
- A `restart` re-runs `A::started`; the prior value's `A::stopped` runs with `StopReason::Failed`.
- The decision is produced by a per-actor **decider**: `fn decide(&Fault) -> SupervisionDirective`, allowing different directives per fault kind.

### 11.3 Scope
Supervision is a **local** mechanism: a node supervises only its own actors. Remote failures are not supervised; they surface to callers as `CallError` (§8) and to watchers as `Terminated` (§12).

---

## 12. Lifecycle monitoring (death watch)

Any actor MAY watch any other actor (local or remote) and be notified when it terminates. This is the primary tool for building robust distributed protocols.

```rust
// via Ctx (§3.4):
fn watch<B: Actor>(&self, target: &ActorRef<B>);
fn unwatch<B: Actor>(&self, target: &ActorRef<B>);

pub struct Terminated {
    pub id: ActorId,
    pub reason: TerminationReason, // Stopped | Failed | NodeDown
}
```

A watcher observes terminations by handling `Terminated` as a system signal delivered into its mailbox (a `Handler<Terminated>` impl, or a dedicated signal hook).

Guarantees (MUST):
1. After `watch(target)`, if the target terminates for **any** reason, the watcher receives exactly one `Terminated` for it, delivered into the watcher's mailbox.
2. **Node down implies termination.** When a node is declared `down` (§10), every watched actor on that node MUST yield a `Terminated { reason: NodeDown }` to its watchers, even though no explicit stop message can arrive. A crashed peer thus still notifies watchers, which plain request/response cannot do.
3. Watching an already-terminated actor MUST immediately yield `Terminated`.
4. Signals respect the per-actor serial order: a `Terminated` is delivered like any other message into the mailbox, never out of band.

Remote watch is implemented by a `Watch(id)` system message to the target's node; that node tracks watchers and emits `Terminated` on stop, and the local failure detector synthesizes `Terminated` if the target node becomes `down`.

---

## 13. Receptionist (service discovery)

Actors are addressed by `ActorRef`, but a node needs a way to obtain the initial `ActorRef` for a remote service without hardcoding its `ActorId`. The **receptionist** is a well-known, cluster-replicated registry.

```rust
pub struct Key<A: Actor> { id: &'static str, _marker: PhantomData<A> }

impl Receptionist {
    fn register<A: Actor>(&self, key: Key<A>, who: ActorRef<A>);
    async fn lookup<A: Actor>(&self, key: Key<A>) -> Listing<A>;          // current snapshot
    fn subscribe<A: Actor>(&self, key: Key<A>) -> impl Stream<Item = Listing<A>>; // live updates
}
```

Requirements:
1. The receptionist is a well-known actor (§3.6) resolvable on every node without prior introduction.
2. Registrations are **replicated** across the cluster and are **eventually consistent**. A CRDT (an OR-Set keyed by registering node) is RECOMMENDED so concurrent registrations merge without coordination.
3. When a node goes `down` (§10), all registrations originating from it MUST be pruned, and subscribers MUST receive an updated `Listing`. (The receptionist watches registered actors to drive this.)
4. `subscribe` MUST deliver the current listing on subscription and a fresh listing on every change.
5. `Key` is typed by actor type so `lookup`/`subscribe` return correctly typed `ActorRef`s.

---

## 14. Error model

### 14.1 `CallError`

`CallError` covers **transport and system** failures only — the failure to *complete* a call. Application failures the handler deliberately produced live inside `M::Reply` (§3.2).

```rust
pub enum CallError {
    Timeout,              // deadline exceeded
    Unreachable,          // recipient node down or association lost
    DeadLetter,           // no live actor for the id
    Unhandled,            // recipient actor has no handler / no registration for this message type
    MailboxFull,          // backpressure rejection
    Serialization(String),// encode/decode failure
    System(String),       // other system-level failure
}
```

- `ActorRef::ask::<M>` returns `Result<M::Reply, CallError>`. When `M::Reply` is itself `Result<T, E>`, the caller sees `Result<Result<T, E>, CallError>`: the outer result distinguishes "the call did not complete" from "the handler ran"; the inner one carries the application outcome.
- `CallError` variants MUST be exhaustive at the public API so callers handle partial failure explicitly; the type system thus forces failure handling at every cross-actor boundary.

### 14.2 Principles
- Errors are **values**, propagated by `Result`; the framework does not use panics for control flow across actors.
- A panic inside a handler is contained by supervision (§11) and never crosses the wire as a panic; it becomes `Terminated`/`Restart` locally and `Unreachable`/`DeadLetter` to in-flight callers.
- Timeouts are mandatory on `ask`: every request MUST carry an effective deadline (explicit or a system default).

### 14.3 Reading a call result

The two nested layers of §14.1 — the **outer** `CallError` (did the call *complete*?) and the **inner** application `E` (what did the handler decide?) — are distinct on purpose and MUST NOT be collapsed in the type: a transport failure the caller may retry is not an application failure it must not.

The type stays two-level; the *handling* need not be re-derived at every call site. A system SHOULD offer one canonical way to consume the common case where a caller treats any failure uniformly:

```rust
// Convenience over `ask` for callers that want a single error channel.
// Available when the application error can absorb a transport failure.
impl<A: Actor> ActorRef<A> {
    pub async fn ask_flat<M, T, E>(&self, msg: M) -> Result<T, E>
    where A: Handler<M>, M: Message<Reply = Result<T, E>>, E: From<CallError>;
}
```

`ask_flat` collapses `Result<Result<T, E>, CallError>` into `Result<T, E>` by mapping a `CallError` through `E: From<CallError>`. Callers that must tell "did not complete" apart from "handler failed" keep using `ask`; callers that react to any failure the same way use `ask_flat`. Either way the two-level match is written once — here — not repeated per call site.

---

## 15. Security (RECOMMENDED)

1. **Transport security.** Associations SHOULD support mutual TLS; node identity SHOULD be bound to a verified certificate.
2. **Authentication.** The handshake SHOULD authenticate the peer and MAY enforce an allowlist of permitted node identities/clusters; a cluster secret prevents accidental cross-cluster association.
3. **Deserialization safety.** As in §5.3, the dispatch registry MUST instantiate only registered, allowlisted message types from network input. There MUST be no path from an incoming envelope to constructing an arbitrary type.
4. **Authorization.** A system MAY gate `deliver` per `(peer, actor, manifest)`; an unauthorized message MUST be rejected as a system failure, not handled.

---

## 16. Observability (RECOMMENDED)

A conforming system SHOULD expose:
- **Metrics:** mailbox depths, message/throughput rates, call latency, restart counts, association count, membership size, suspicion/down events.
- **Tracing:** propagate a trace/correlation context through envelopes so a logical request can be followed across nodes.
- **Lifecycle logging:** spawn/resign, membership transitions, downing decisions, supervision actions — each at a defined, filterable level.

The same events also drive deterministic simulation: a simulator subscribes to this stream to check invariants continuously (§18.5). A conforming system SHOULD emit, as structured events: `assign_id`/`actor_ready`/`resign_id` (§4.2), mailbox enqueue and dispatch (§6), every `ask` outcome (§14), membership and reachability transitions (§9–10), supervision decisions (§11), and `Terminated` deliveries (§12).

---

## 17. Conformance

An implementation conforms to this specification **iff** every property below holds. This section is an index, not a restatement: each property is *defined* normatively by the cited section's MUSTs, and *verified* by the listed method — for runtime properties, the numbered invariant a simulator checks continuously (§18.5). The cited sections and the §18.5 catalogue are the single statements of each requirement; this table only points at them.

| Property | Defined in | Verified by |
|---|---|---|
| Actor state reachable only through the actor's own handlers | §3.5 | construction (ownership); §18.5 #4 |
| An invalid `ask`/`tell` does not compile | §3.3 | §18.5 #20 (compile-fail) |
| The call site is identical for local and remote targets | §3.3 | §18.5 #21 (differential) |
| Lifecycle `assign_id → actor_ready → resign_id`: ordering and exactly-once | §4.2 | §18.5 #6 |
| `resolve` classifies locality with no network round-trip | §4.3 | §18.5 #7 |
| Stable hand-written manifest; unregistered `(actor, manifest)` → `Unhandled` | §4.4 | §18.5 #8; construction |
| Remote dispatch listed by a hand-written `RemoteActor::register`; no framework macro | §1.1, §4.4 | construction |
| Local sends enqueue by value, without serialization | §4.3–4.4 | §18.5 #9 |
| `ActorRef`s in messages/replies are rebound on decode | §4.4 | §18.5 #10 |
| Every message and reply satisfies `SerializationRequirement` | §5 | compile-time (trait bound) |
| Serial execution, per-sender FIFO, bounded non-dropping mailbox | §6 | §18.5 #3, #4, #5 |
| Associations established by an authenticated, version-checked handshake | §7, §15 | construction; transport tests |
| At-most-once delivery; no `ask` completes silently | §7.2 | §18.5 #1, #2 |
| Membership gossips with incarnation refutation and converges | §9 | §18.5 #14, #15, #16, #17 |
| SWIM indirect probing with suspicion/refutation drives reachability | §10 | §18.5 #17 (underlies #14–16) |
| Local faults contained by supervision; default `Stop`; restarts back off | §11 | §18.5 #18 |
| `watch` yields exactly-once `Terminated`, including `NodeDown` | §12 | §18.5 #11, #12, #13 |
| Receptionist: typed register/lookup/subscribe, pruned on node `down` | §13 | §18.5 #19 |
| Transport/system failures are exhaustive `CallError`; app errors in `M::Reply` | §14 | compile-time (exhaustive enum); §18.5 #1 |
| Runs over virtual clock/network/entropy; one seed reproduces a run; every property above holds under fault injection | §18 | §18.1; §18.5 under §18.3 |

---

## 18. Testability and deterministic simulation

A conforming implementation SHOULD be testable by **deterministic simulation**: a whole cluster runs in one process, on one logical thread, over virtual time, network, and randomness, so a single seed reproduces an entire multi-node run — including its failures — exactly. This section is normative for the traits that make such testing possible, and lists the invariants a simulator checks.

### 18.1 Determinism contract

A system that supports simulation MUST satisfy:

1. **Seed-reproducibility.** Given the same seed, configuration, and workload, two runs MUST produce byte-identical event streams (§16). All nondeterminism MUST pass through the `Clock`, `Entropy`, and `Spawner` traits (§4.6) and the `Transport` trait (§7).
2. **Quiescence-driven time.** In simulation, logical time MUST advance only when no task is ready to run. A timeout, SWIM interval, or backoff therefore costs no wall-clock time, so a run can cover hours of cluster time per CPU-second.
3. **No ambient nondeterminism.** A simulation build MUST NOT read the wall clock, spawn OS threads, or use a non-seeded RNG. A single leak breaks reproducibility, so implementations SHOULD enforce this statically (e.g. a lint that forbids the offending APIs).

### 18.2 Virtualized traits

Simulation reuses the traits the production runtime already uses; only the implementations differ:

| Trait | Production | Simulation |
|---|---|---|
| `Clock` (§4.6) | wall clock | logical clock; advances only at quiescence |
| `Entropy` (§4.6) | OS-seeded | one seeded PRNG — the only randomness in the run |
| `Spawner` (§4.6) | multi-thread runtime | single-thread cooperative scheduler |
| `Transport` (§7) | TCP | in-memory network with seeded latency / loss / reorder |
| codec (§5) | production codec | unchanged — runs real (de)serialization |

Because these are the *same* traits production uses, simulation runs the real `ActorSystem`, mailbox, membership, SWIM, supervision, and receptionist code — not a model of it. The codec stays real, so wire encoding is tested on every cross-node hop.

### 18.3 Fault injection

A simulator MUST be able to inject, under seed control, at least:

- **Transport:** frame drop, duplication, delay, reordering; corruption (which MUST tear down the association, not the node — §7.3); association loss.
- **Mailbox:** induced `MailboxFull` (§6.4); maximal cross-sender reordering subject to per-sender FIFO (§6.5).
- **Scheduling:** seed-randomized selection among ready tasks.
- **Membership / SWIM:** dropped or delayed pings, partitions, stale or replayed gossip, stale incarnations (§9–10).
- **Supervision:** induced handler and `started()` faults (§11).
- **Nodes:** abrupt crash (no graceful leave) at an arbitrary step, which MUST surface as `NodeDown`/`Terminated` to watchers (§12.2) and `Unreachable` to in-flight callers (§7.2).

Each run SHOULD enable a random subset of faults at random intensities (sometimes called "swarm" testing); a run with no faults is the simplest case and MUST still pass.

### 18.4 Workloads

Tests are expressed as **workloads** over the cluster: a `setup` that builds actors and registrations, a `start` that drives traffic, and a `check` that asserts the invariants of §18.5. A workload MUST observe the cluster only through public API and the §16 event stream — never through actor state directly (§3.5), except via `when_local` (§3.5.1) where explicitly intended.

### 18.5 Invariant catalogue

These invariants appear as MUSTs throughout this specification; those inline MUSTs are their normative statements. Collected here, they are the contract a simulator checks **continuously** during every run and at final quiescence, and the targets §17 verifies against. Each MUST hold even under the faults of §18.3.

1. **No silent loss (§7.2, §14).** Every `ask` issued terminates in exactly one of `Ok(reply)`, `Timeout`, `Unreachable`, `DeadLetter`, or `Unhandled`; at final quiescence no `ask` remains pending.
2. **Crash completes in-flight calls (§7.2, §10.5).** An `ask` whose target node is declared `down` completes with `Unreachable`; it never hangs.
3. **Per-pair FIFO (§6.5).** Messages from one sender to one recipient are observed in send order, even under maximal reordering injection; no ordering is assumed across senders.
4. **Serial, non-reentrant execution (§6.2–6.3).** An actor never processes two messages concurrently; `&mut self` is never aliased.
5. **Bounded, non-dropping mailbox (§6.4).** A full mailbox blocks or returns `MailboxFull`; it never drops silently.
6. **Lifecycle order and exactly-once (§4.2).** `assign_id` → `actor_ready` → `resign_id` occur in order; `assign_id` and `actor_ready` exactly once; `resign_id` exactly once even when spawn fails between steps 1 and 3; the id is undeliverable between assign and ready.
7. **`resolve` is local (§4.3).** Locality classification performs no network round-trip.
8. **Manifest dispatch and allowlist (§4.4, §5.3, §15.3).** An unregistered `(actor type, manifest)` yields `Unhandled`; no type outside the registry is ever constructed from network bytes.
9. **Local sends skip serialization (§4.3–4.4).** A local `ask`/`tell` performs no encode/decode, yet its observable result is identical to the remote path (cf. 21).
10. **`ActorRef` rebinding (§4.4).** An `ActorRef` carried in a message or reply is rebound to the receiving system on decode and is usable there.
11. **Death-watch exactly-once (§12.1–12.2).** After `watch`, the watcher receives exactly one `Terminated` for any cause, including `NodeDown` when the target's node is declared `down`.
12. **Watch-after-death (§12.3).** Watching an already-terminated actor yields `Terminated` immediately.
13. **Signal ordering (§12.4).** `Terminated` is delivered through the mailbox in serial order, never out of band.
14. **Membership convergence (§9.2).** Once faults cease and partitions heal, all `up` members converge on one membership set within bounded logical time.
15. **`down` is terminal (§9.1).** A node observed `down` never reappears `up` under the same incarnation.
16. **Partition tolerance (§9.2.4).** Under the default downing policy, a partition alone never moves a member to `down`, only to `unreachable`.
17. **SWIM refutation (§10.4).** A node that sees itself suspected refutes via a higher incarnation, clearing the suspicion cluster-wide.
18. **Supervision containment (§11).** A handler panic never crashes the node; the default directive is `Stop`; restarts back off; exceeding `max` within the window escalates.
19. **Receptionist consistency (§13).** Registrations from a `down` node are pruned and subscribers notified; `subscribe` delivers the current snapshot first, then every change; concurrent registrations merge (eventual consistency).
20. **Type-safety (§3.3).** An `ask`/`tell` of a message an actor has no `Handler` for does not compile. (Asserted by compile-fail tests, not at runtime — see §18.6.)
21. **Location transparency (§3.3).** Running the same workload with a target local versus remote produces observably identical replies and ordering. (Differential check.)

### 18.6 Reproduction, layering, and CI

- **Reproduction.** A failing run MUST be replayable from its `(seed, configuration)` alone.
- **Layered checks.** Simulation covers the distributed invariants (1–19, 21). Invariant 20 is covered by **compile-fail tests** — a compiler run that asserts invalid sends are rejected. The low-level mailbox/executor (§6) SHOULD also be model-checked across all interleavings, separately from the simulator.
- **Regression corpus.** Every past failure SHOULD be kept as a `(seed, configuration)` and replayed permanently.
- **Continuous testing.** CI SHOULD run many fresh seeds per change across different fault configurations; the coverage metric is cluster-hours exercised per change, not test count.

---

## Appendix A — End-to-end example

```rust
// --- Define the actor (plain struct + trait impl, no macros) ---
pub struct Greeter { greeting: String }
impl Actor for Greeter { type System = ClusterSystem; }

// --- Define a message (serde derive only; wire identity is a hand-written const) ---
#[derive(Serialize, Deserialize)]
struct Greet { name: String }
impl Message for Greet {
    type Reply = String;
    const MANIFEST: Manifest = Manifest::new("myapp.Greet");
}

impl Handler<Greet> for Greeter {
    async fn handle(&mut self, msg: Greet, _ctx: &Ctx<Self>) -> String {
        format!("{}, {}!", self.greeting, msg.name)
    }
}

// --- List the messages Greeter accepts over the network (macro-free; spawn calls this once) ---
impl RemoteActor for Greeter {
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Greet>();
    }
}

const GREETERS: Key<Greeter> = Key::new("greeters");

// --- Host node ---
let system = ClusterSystem::start("node-a", config).await?;
let greeter = system.spawn(Greeter { greeting: "Hello".into() });
system.receptionist().register(GREETERS, greeter.clone());

// --- Client node (joined to the same cluster) ---
let system = ClusterSystem::start("node-b", config.with_seed("node-a")).await?;
let listing = system.receptionist().lookup(GREETERS).await;
if let Some(greeter) = listing.first() {
    // Identical call site whether `greeter` is local or remote.
    // `greeter.ask(Greet { .. })` compiles only because Greeter: Handler<Greet>.
    match greeter.ask(Greet { name: "world".into() }).await {
        Ok(msg)                      => println!("{msg}"),       // "Hello, world!"
        Err(CallError::Unreachable)  => { /* peer down — react */ }
        Err(CallError::Timeout)      => { /* retry policy decides */ }
        Err(e)                       => eprintln!("call failed: {e:?}"),
    }
}
```

## Appendix B — Suggested crate layout

```
dactor/                  # umbrella re-export
dactor-core/             # Actor, Message, Handler, ActorRef, Ctx, ActorSystem,
                         #   RemoteActor, HandlerRegistry, Manifest, CallError (§3–4, §14)
dactor-serialization/    # SerializationRequirement, dispatch registry, codecs (§5, §4.4)
dactor-cluster/          # ClusterSystem: transport, membership, SWIM, supervision,
                         #   death watch, receptionist (§7–13) — the reference ActorSystem
dactor-simulation/       # TEST-ONLY. Virtual Clock/Entropy/Spawner + in-memory Transport,
                         #   the deterministic simulator, fault injection, invariant checkers (§18)
```

Message identity is a `const`, remote dispatch is a hand-written `RemoteActor::register` list, and the call path is ordinary generic code in `dactor-core`. There is no *required* macro crate; any `#[derive(Message)]`/`#[derive(RemoteActor)]` (§4.4) is an optional convenience layered above the model, not a dependency of it.
