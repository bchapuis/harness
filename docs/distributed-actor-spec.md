# Distributed Actor Framework for Rust: Specification

**Status:** Draft v5
**Scope:** A location-transparent, fault-tolerant distributed actor system for Rust, with a **message-first** API.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Throughout, `actor` stands in as the crate and namespace name. Async trait methods are shown as `async fn` for readability; the framework writes them `fn … -> impl Future<Output = …> + Send` so generic runtime code can require the returned future to be `Send`.

> **Design stance.** Actors exchange **messages** (serializable value types) and implement one `Handler<M>` per message type they accept. Each message carries a hand-written `const` wire identity, its *manifest*, and each actor lists the messages it accepts over the network in a hand-written `register` function (§4.4). The framework is ordinary Rust traits and generics with no macros of its own (§1.1).

---

## 1. Design goals and non-goals

### 1.1 Goals
- **Location transparency.** Sending to a local actor and to a remote one is the same call; the runtime, not the source, decides where the target lives.
- **Isolation by construction.** Actors never share state, and the type system forbids concurrent access to it.
- **Explicit wire contract.** Every cross-node payload is a named, serializable message type, so the protocol can be versioned, logged, persisted, and inspected rather than implied by a method signature.
- **Fault tolerance.** Node and actor failures are first-class, observable events. The system tolerates partial failure and network partitions and never drops a request without reporting an error.
- **Pluggability.** Serialization, transport, and the actor system itself are traits; the cluster runtime is one implementation.
- **No required codegen.** The framework is plain generic code; serde's `Serialize` and `Deserialize` derives are the only macros it relies on. An optional derive for a message's manifest or an actor's `register` list (§4.4) is permitted: only *required* codegen is excluded.

### 1.2 Non-goals
- **Generic message handlers over the wire.** Rust monomorphizes, so there is no runtime type to send. Generic actors are fine, but a *message* and its *reply* MUST be concrete, serializable types.
- **Transparent failure masking.** The framework MUST NOT auto-retry messages with side effects; retries and idempotency are the caller's decision.
- **Strong consistency in the data plane.** Messaging, death watch, and the receptionist are eventually consistent in every mode. The **leader-based** control plane (§9.4.3) runs consensus for membership and control-plane metadata only; no mode puts a quorum on the message path or makes delivery transactional.
- **Shared memory across nodes.** All communication is by message; nothing is shared.

---

## 2. Terminology

| Term | Definition |
|---|---|
| **Actor** | A unit of state plus behavior that processes messages one at a time (serially). |
| **Message** | A serializable value sent to an actor. Each message type declares its `Reply` type. |
| **Handler** | An actor's implementation (`Handler<M>`) for one message type `M`. |
| **`ActorRef<A>`** | A serializable, cloneable, typed handle to an actor of type `A`. The only way to address an actor; it never grants access to state. |
| **`ActorId`** | A cluster-unique, serializable identity the system assigns. |
| **Manifest** | The stable wire identifier of a message type; the dispatch key. |
| **System** | An implementation of [`ActorSystem`](#4-the-actorsystem-contract); it owns local actors and the runtime. |
| **Node** | One running `System` instance with a network identity. |
| **Cluster** | A set of nodes that have formed associations and share membership. |
| **Control plane** | The authority that owns the member set; one of four configurable modes (§9.4). |
| **Voter** | In the leader-based mode (§9.4.3), a member that participates in the Raft quorum. |
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

    /// List the messages this actor accepts over the network (§4.4). The default
    /// registers nothing: a purely local actor; a remote one overrides it.
    fn register(_r: &mut HandlerRegistry<Self>) {}

    /// This actor's supervision strategy (§11.2). The default is `Stop`.
    fn supervision() -> Supervision { Supervision::stop() }
}
```

An actor's identity (`ActorId`) and its `system` and `self` handles come from its [`Ctx`](#34-context-ctx); the user does not store them. Two actors are equal iff their `ActorId`s are equal, and `Hash` and `Eq` on an `ActorRef` derive from the `ActorId`.

### 3.2 Messages and handlers

A **message** is a serializable value type that declares its reply type and its stable wire identity:

```rust
pub trait Message: SerializationRequirement {
    type Reply: SerializationRequirement;
    /// Stable, author-controlled wire identity and dispatch key (§4.4).
    /// Written by hand, or defaulted by an optional derive. Stable across recompiles and renames.
    const MANIFEST: Manifest;
}
```

`Manifest` is a `const`-constructible newtype over `&'static str`, so a `Manifest` value exists only for statically declared identities. The transport boundary passes the typed `Manifest` (§4.1); the envelope carries an owned `String` (§7.1), untrusted text until the allowlist matches it against the registered manifests on receipt (§4.4).

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
2. `M` and `M::Reply` MUST satisfy [`SerializationRequirement`](#5-serialization) (§5), an ordinary trait bound checked at compile time.
3. `M` MUST be concrete at the point of the `Handler` impl (§1.2).
3a. `M::MANIFEST` MUST be a stable, author-chosen identity, unique among the message types a given actor accepts (§4.4). A local-only actor still declares it; the local fast path (§4.3) never looks it up, so there it only labels §16 events.
4. **Application errors live in the reply.** A handler that can fail uses `type Reply = Result<T, E>` where `T, E: SerializationRequirement`: an application failure is a value, distinct from a transport failure (`CallError`, §14).
5. An actor accepts exactly the set of `M` for which it implements `Handler<M>`. Anything else is a compile error at the call site (§3.3), or `CallError::Unhandled` on the wire (§4.4).

### 3.3 `ActorRef` and location transparency

`ActorRef<A>` is the only handle to an actor. It is `Clone + Serialize + DeserializeOwned + Send + Sync`, and it holds exactly the `ActorId` plus a handle to the local system; it carries **no** state.

```rust
pub struct ActorRef<A: Actor> {
    id: ActorId,
    system: A::System,   // a cheap, cloneable handle to the local system
}

impl<A: Actor> ActorRef<A> {
    /// Request/response. The `A: Handler<M>` bound proves, at compile time,
    /// that this actor accepts M, so only valid messages are sendable.
    pub async fn ask<M>(&self, msg: M) -> Result<M::Reply, CallError>
    where A: Handler<M>, M: Message;

    /// Fire-and-forget. Errors only for transport failure, never for handler
    /// outcome; a full local mailbox awaits space (§6).
    pub async fn tell<M>(&self, msg: M) -> Result<(), CallError>
    where A: Handler<M>, M: Message;

    /// Like `tell`, but a full local mailbox fails with `CallError::MailboxFull`
    /// at once instead of awaiting space (§6). Synchronous: it never awaits.
    /// Local-only: a remote target yields `CallError::Unreachable`.
    pub fn try_tell<M>(&self, msg: M) -> Result<(), CallError>
    where A: Handler<M>, M: Message;

    /// Same as `ask` but with an explicit deadline overriding the system default.
    pub async fn ask_timeout<M>(&self, msg: M, within: Duration) -> Result<M::Reply, CallError>
    where A: Handler<M>, M: Message;
}
```

- The `A: Handler<M>` bound is the dispatch mechanism: invalid sends do not compile.
- `ask` and `tell` are **identical** for local and remote targets; the system decides at call time whether to enqueue locally or send over a transport (§4.4).
- An `ActorRef` MAY be a field of a message or of an `M::Reply`; only its `ActorId` travels, and the receiving node rebinds it on decode (§4.4).
- An `ActorRef` MUST NOT expose actor state or handlers.

### 3.4 Context (`Ctx`)

Handlers and lifecycle hooks receive a `Ctx<A>`, which grants controlled capabilities without breaking isolation:

```rust
impl<A: Actor> Ctx<A> {
    fn id(&self) -> &ActorId;
    fn this(&self) -> ActorRef<A>;                  // self-reference, shareable
    fn system(&self) -> &A::System;

    fn spawn<C: Actor<System = A::System>>(&self, child: C) -> ActorRef<C>;          // parented to self (§11)
    fn spawn_with<C, F>(&self, factory: F) -> ActorRef<C>                             // restartable child (§11.2)
        where C: Actor<System = A::System>, F: FnMut() -> C + Send + 'static;
    fn watch<B: Actor>(&self, target: &ActorRef<B>) where A: Handler<Terminated>;     // death watch (§12)
    fn watch_id(&self, target: ActorId) where A: Handler<Terminated>;                  // watch by bare id: an identity carried in a message or folded from durable state
    fn unwatch<B: Actor>(&self, target: &ActorRef<B>);
    fn stop(&self);                                                                   // stop self after current message
}
```

### 3.5 Isolation guarantees

Ownership enforces actor isolation:

1. An actor value is **owned** by its mailbox task and reachable through no other path; no API returns `&A` or `&mut A` to a caller.
2. All state access happens inside the actor's own handlers, run by its serial executor, so `&mut self` in `handle` needs no locking and admits no data races.
3. An `ActorRef` is the only object held externally, and it is stateless, so sharing one across threads or nodes is safe.

#### 3.5.1 Local-only access (`when_local`)

For testing and local optimizations, a system MAY offer:

```rust
impl<A: Actor> ActorRef<A> {
    pub async fn when_local<F, R>(&self, f: F) -> Option<R>   // runs on the actor's executor iff local
    where F: FnOnce(&mut A) -> R + Send + 'static, R: Send + 'static;
}
```

`when_local` MUST run `f` on the actor's serial executor, preserving isolation, and MUST return `None` if the actor is remote. It is the only exception to location transparency, and it SHOULD be limited to tests.

### 3.6 Identity (`ActorId`)

An `ActorId` is a single cluster-wide identity type, shared by every system:

```
ActorId = {
    node:        NodeId,        // cluster node identity: a bare uid; endpoints live in the transport's address book
    path:        Path,          // hierarchical name, e.g. /user/greeter
    incarnation: u64,           // the owning process incarnation, stamped on every id it issues
}
```

It is `Clone + Eq + Hash + Send + Sync + Serialize + DeserializeOwned`, and it MUST:
- Be unique within a cluster for the cluster's lifetime.
- Let any node identify the target's **owning node** (the `node` field) and classify it as local or remote from the id alone, without a network round-trip (§4.3).
- Carry the owning **process incarnation**, so an id issued before a process restart under the same `NodeId` never resolves to an actor the restarted process later places at the same `path`; it fails as `DeadLetter` (§8). Within one incarnation a `path` is never reused (paths come from a counter the node never rewinds), so the path alone distinguishes a fresh actor from a resigned one.

The struct is closed and self-describing, so any id that deserializes is well-formed and locality-classifiable with no routing-table lookup. The receptionist has no resolvable path: it is a handle, not an actor (§13).

---

## 4. The `ActorSystem` contract

The system is the runtime an actor runs on. The cluster runtime (§9 to §13) is one implementation.

### 4.1 The trait

The trait has two faces. A layer above (an entity host, a workflow engine, an agent runtime) calls the first group: it spawns and resolves actors and takes its node identity, time, randomness, task scheduling, and observability from the system rather than the host, so one seed reproduces that layer under simulation (§4.6, §18). An implementation (the reference cluster runtime, the simulator's node) supplies the second group: the byte-level transport boundary over **already-serialized payloads**, and the hooks the typed `ActorRef`/mailbox layer dispatches through. They are one trait so that a system value is one cheap handle (§3.3) and a bound names one thing:

```rust
pub trait ActorSystem: Clone + Send + Sync + 'static {
    // ---- What a layer above calls ----
    fn spawn<A: Actor<System = Self>>(&self, actor: A) -> ActorRef<A>;        // §4.2: assign_id → register mailbox → actor_ready
    fn spawn_with<A, F: FnMut() -> A + Send + 'static>(&self, factory: F) -> ActorRef<A>; // restartable (§11.2); `Ctx` has the child forms
    /// Build a typed handle to `id`. Infallible: every `ActorId` is well-formed
    /// and locality-classifiable (§3.6), so resolution never fails.
    fn resolve<A: Actor<System = Self>>(&self, id: ActorId) -> ActorRef<A>;
    fn node(&self) -> NodeId;
    fn receptionist(&self) -> Receptionist<Self>;                              // §13
    type Clock: Clock;                                                         // §4.6: the one source of time
    fn clock(&self) -> &Self::Clock;
    fn now(&self) -> Instant;
    fn sleep(&self, d: Duration) -> BoxFuture<'static, ()>;                    // detachable: a `'static` future `launch` can run
    fn launch(&self, task: BoxFuture<'static, ()>);                            // §4.6: the one spawner
    fn emit(&self, event: Event);                                              // §16: the one observability stream
    fn next_random(&self) -> u64;                                              // §4.6: the one source of randomness

    // ---- What an implementation supplies ----
    fn spawn_factory<A: Actor<System = Self>>(&self, factory: ActorFactory<A>, parent: Option<ActorId>) -> ActorRef<A>;
    fn resolve_local<A: Actor<System = Self>>(&self, id: &ActorId) -> Option<Mailbox<A>>;
    fn is_local(&self, id: &ActorId) -> bool;                                  // §4.3, from the id alone
    fn is_serving(&self, node: NodeId) -> bool;                                // §9.4, §13; defaults to `true`
    fn codec(&self) -> Arc<dyn Codec>;                                         // §5
    fn remote_ask(&self, recipient: &ActorId, manifest: Manifest, payload: Vec<u8>, within: Duration)
        -> impl Future<Output = Result<Vec<u8>, CallError>> + Send;
    fn remote_tell(&self, recipient: &ActorId, manifest: Manifest, payload: Vec<u8>)
        -> impl Future<Output = Result<(), CallError>> + Send;
    fn watch(&self, target: ActorId, watcher: ActorId, deliver: WatchDelivery); // §12
    fn unwatch(&self, target: &ActorId, watcher: &ActorId);
    fn receptionist_state(&self) -> Arc<ReceptionistState>;                    // §13
    fn replicate_registration(&self, key: &str, origin: NodeId, id: ActorId);  // §13; defaults to a no-op
}
```

The provided forms (`spawn`, `spawn_with`, `resolve`, `now`, `sleep`, `receptionist`) are defined in terms of the supplied ones, and `is_serving` and `replicate_registration` carry single-node defaults (every node serves; nothing to replicate), so an implementation writes the second group and gets the first. On the transport boundary the wire identity is the message's `MANIFEST` (§3.2), passed as the typed `Manifest`, and the payload is its codec-encoded bytes. The system also performs internal operations it does not expose as API: the lifecycle steps `assign_id` / `actor_ready` / `resign_id` (§4.2), and the inbound `deliver`, which the receive loop invokes to dispatch a decoded message to a local actor (§4.4).

### 4.2 Lifecycle ordering

For each actor, the system MUST observe exactly this order:

```
1. id ← assign_id::<A>()          // reserve identity; MUST be unique; MUST NOT be observable yet
2. mailbox created and buffering, id resolvable, executor started; A::started() runs before first user message
3. actor_ready(actor)             // A::started() returned Ok; buffered messages may now be processed
4. … actor processes messages …
5. resign_id(id)                  // on stop/failure; A::stopped() runs; releases all resources
```

Invariants (MUST):
- `assign_id` runs exactly once before any message can be delivered.
- `actor_ready` runs exactly once; after it returns, the mailbox accepts messages.
- `resign_id` runs exactly once, even when spawn fails between steps 1 and 3, in which case it follows `assign_id` directly.
- Between `assign_id` and `actor_ready` the id is reserved but **not** deliverable; the system MUST dead-letter or buffer messages to it, never silently accept them.

`spawn` is synchronous (§4.1): it returns the `ActorRef` at step 2, while `actor_ready` (step 3) follows only after the async `A::started` returns `Ok`, and messages sent in between queue. The normative ordering is *`started` completes before the first user message is processed*; `actor_ready` marks that point.

### 4.3 Resolution: local vs remote

`resolve(id)` returns an `ActorRef` and classifies the target without any network round-trip:

- Owning node local, live mailbox → a **local** `ActorRef` (fast path: messages enqueue by value, without serialization).
- Owning node local, no live mailbox → an `ActorRef` that dead-letters (the actor has resigned).
- Owning node remote → a **remote** `ActorRef` (messages serialize and route through a transport).

`resolve` MUST NOT block or contact the remote node to check existence; liveness surfaces on send or through failure detection (§10). It is **infallible** (§3.6): malformed network input never reaches it, because the codec and the dispatch allowlist reject it first (§5, §15).

### 4.4 Manifests, dispatch, and message flow

Every message type carries a stable **manifest** (`Message::MANIFEST`, §3.2): its author-controlled wire identity and dispatch key.

- The manifest MUST stay stable across recompiles and renames. An explicit string such as `"myapp.Greet"` is RECOMMENDED. A breaking change to the message's shape SHOULD become a new message type with a new manifest, rather than a silent redefinition of an existing one.
- The **dispatch registry** maps `(actor type, manifest) → typed dispatch entry`. A dispatch entry knows how to deserialize `M` from a payload, enqueue `Handler::<M>::handle` on the resolved local actor's executor, and serialize `M::Reply`.

**Registration.** A remotely addressable actor lists the messages it accepts over the network in `Actor::register` (§3.1.1). Each `r.accept::<M>()` is an ordinary generic call that captures the monomorphized dispatch entry for `(Self, M)`:

```rust
pub struct HandlerRegistry<A: Actor> { /* … */ }
impl<A: Actor> HandlerRegistry<A> {
    pub fn accept<M>(&mut self) where A: Handler<M>, M: Message;  // generic library fn, no codegen
}
```

`register` is a defaulted method on `Actor` so that `spawn`, which is generic over `A: Actor`, always has an `A::register` to call.

- `spawn` (§4.1) calls `A::register` the first time it spawns an `A`, filling the registry before any message can arrive: no setup step, no link-time collection.
- Registration is **inbound-remote only**: a purely local actor keeps the empty default, and its messages flow by value (§4.3).
- A network-delivered message whose `(actor type, manifest)` is not registered MUST yield `CallError::Unhandled`. The registry is also the deserialization allowlist (§5, §15): only listed message types are ever built from network bytes.

An optional derive MAY generate a message's `MANIFEST` and an actor's `register` body: `#[derive(Message)]` defaulting the manifest from the type path, `#[derive(RemoteActor)]` emitting one `accept::<M>()` per `Handler<M>` impl. An implementation MUST work with hand-written manifests and `register` lists, and a hand-written manifest MUST override a derived default (§1.1).

**Outbound, `ActorRef::ask<M>` (typed layer above the trait):**
1. Resolve the locality of `self.id`.
2. **Local:** enqueue `msg` *by value* on the mailbox; await the typed reply. No serialization occurs.
3. **Remote:** `payload = encode(codec, &msg)?`; `bytes = system.remote_ask(&self.id, M::MANIFEST, payload, deadline).await?`; return `decode::<M::Reply>(codec, &bytes)?`.

`tell<M>` is identical but expects no reply (`remote_tell`); for a local target it enqueues without a reply channel.

**Inbound, system receive loop (remote path):**
1. Decode the envelope header → `recipient: ActorId`, `manifest: String` (matched against the registered manifests, §3.2), optional `correlation`.
2. `resolve(recipient)`; if no live local actor → reply `CallError::DeadLetter`.
3. `system.deliver(recipient, manifest, payload, reply)`, which:
   a. Looks up `(type of the resolved actor, manifest)` in the dispatch registry; if absent → `CallError::Unhandled`.
   b. Deserializes `M` from `payload`.
   c. Enqueues `Handler::<M>::handle` on the actor's serial executor and awaits its completion.
   d. Routes the reply through the `ReplyHandle`.

An `ActorRef` inside a message or reply travels as its `ActorId`, and the receiving system rebinds it on decode (§4.3).

### 4.5 Reply handling

```rust
pub struct ReplyHandle { /* opaque; holds correlation + reply channel */ }

impl ReplyHandle {
    pub fn send<R: SerializationRequirement>(self, reply: R);  // serialize + return to caller
    pub fn fail(self, failure: CallError);                     // transport/system-level failure
    pub fn none(self);                                         // for `tell`: no reply expected
}
```

These are synchronous: each serializes its outcome and hands it to the correlation channel, which applies no backpressure to the handler. `deliver` MUST resolve exactly one of them per `ask`. `send` carries both successful and application-failed outcomes (§3.2); `fail` is only for transport or system failures the handler never produced.

### 4.6 Runtime environment (clock, randomness, concurrency)

The runtime needs time, randomness, and task spawning from its environment. A system MUST obtain each through a trait and MUST NOT read it from the host directly, so it can run under deterministic simulation (§18).

```rust
/// Virtual or real time. No subsystem may read wall-clock time directly.
/// `Clone`: a clock value is a cheap handle, shared across the subsystems that hold one.
pub trait Clock: Clone + Send + Sync + 'static {
    fn now(&self) -> Instant;
    async fn sleep(&self, dur: Duration);
    async fn timeout<F: Future>(&self, within: Duration, f: F) -> Result<F::Output, Elapsed>;
}

/// The single source of randomness. Seedable; the only randomness in the system.
pub trait Entropy: Send + Sync + 'static {
    fn next_u64(&self) -> u64;
}

/// Task spawning. The mailbox executors, gossip, the failure detector, the
/// registry sync loop, and consensus tasks (§9.4) run through this.
pub trait Spawner: Send + Sync + 'static {
    /// Named `launch`, not `spawn`, so a raw task is never confused with
    /// spawning an actor (`ActorSystem::spawn` / `Ctx::spawn`, §3.4, §4.1).
    fn launch(&self, task: BoxFuture<'static, ()>);
}
```

Rules (MUST):
1. **All timing** MUST come from `Clock`: `ask` deadlines (§14.2), SWIM intervals (§10), gossip periods (§9.4.4), registry sync intervals (§9.4.2), Raft election and heartbeat timers (§9.4.3), supervision backoff (§11.2).
2. **All randomness** MUST come from `Entropy`: gossip peer selection (§9.4.4), SWIM's `k` members (§10), Raft election-timeout jitter (§9.4.3), backoff jitter (§11.2), and minting a `NodeId` where the mode generates identity rather than configuring or persisting it: gossip-based startup, or an identity reset after `down` (§3.6, §9.1, §9.4).
3. **All background tasks** MUST be created through `Spawner`. The mailbox executor (§6) and the detector loops (§10) MUST NOT bind themselves to a specific async runtime.
4. **No observable nondeterminism.** Anything that crosses the wire or appears in §16 events MUST have a deterministic order. A system MUST NOT let unordered iteration (`HashMap` order, for instance) affect message ordering, peer selection, or reply timing.

---

## 5. Serialization

`SerializationRequirement` is the bound every wire-crossing value must satisfy.

```rust
pub trait SerializationRequirement:
    Serialize + DeserializeOwned + Send + 'static {}
impl<T: Serialize + DeserializeOwned + Send + 'static> SerializationRequirement for T {}
```

Rules:
1. Every message type and reply type MUST satisfy `SerializationRequirement`; the `Message` and `Handler` bounds enforce this at compile time.
2. The **codec** (`postcard`, `bincode`, CBOR, JSON, Protobuf, or another) is pluggable and fixed per system; both ends of an association MUST agree on it.
3. The **manifest** (§4.4) identifies the message type on the wire. Decoding MUST reject unknown manifests (`CallError::Unhandled`) and MUST NOT build arbitrary types from untrusted input.
4. Serialization failures are reported as `CallError::Serialization`; they MUST NOT panic the receive loop.

---

## 6. Execution model

1. **One mailbox per actor.** Each local actor has a bounded mailbox (a multi-producer, single-consumer queue) feeding exactly one executor task.
2. **Serial execution.** The executor processes one message to completion before the next, which makes `&mut self` sound (§3.5) and gives each actor a total order over the messages it observes.
3. **Cooperative async.** A handler MAY `.await`; while it is suspended, the executor MUST NOT begin another message for that actor (no reentrancy). Handlers SHOULD avoid blocking the thread and SHOULD offload blocking work.
4. **Backpressure.** Mailboxes are bounded. When one is full, the system MUST apply a defined policy (`await` until space frees, or fail with `CallError::MailboxFull`) and MUST NOT drop messages silently. `tell` awaits; `try_tell` (§3.3) fails with `CallError::MailboxFull` at once and, being local-only, yields `CallError::Unreachable` for a remote target without sending. A remote `tell` refused on the receiving node (a full mailbox, no live actor, no registration, an undecodable payload, or a sender the authorizer refuses, §15) has no reply to carry the error, so the receiving node MUST record the refusal on its event stream (§16).
5. **Ordering guarantee.** Messages from one sender to one recipient, `tell` and `ask` alike, arrive in send order (FIFO per directed pair); for a remote recipient the order is scoped to one association (§7.2). No order holds across senders.
6. **Fairness.** The runtime SHOULD schedule executors fairly so that no ready actor starves.

---

## 7. Transport

A transport is pluggable behind a trait; the default is TCP with length-delimited framing. Requirements:

1. **Associations.** Before exchanging actor traffic, two nodes MUST complete a handshake that establishes an association: a wire-version check, a node-identity exchange, codec agreement, and optional authentication (§15).

   Each end announces the wire version it speaks, and a peer at any other version MUST be refused, naming both ([compatibility](compatibility-spec.md) **V2**), before its codec, secret, or identity is examined, so version skew is never reported as a security failure. A build speaks exactly one version; a wire-format change is a new version every node takes at once.

   A transport MAY carry its own address-discovery table over an association beside the two message families of §7.1 (`Wire::Endpoints` in the reference TCP transport, a `(node, address)` list), consumed below the frame vocabulary and never handed up to the system.
2. **Multiplexing.** Many actor conversations share one association; each request carries a correlation id, and each response references it.
3. **Framing.** Messages are length-delimited; a malformed frame MUST tear down the association, not the node.
4. **Failure detection.** The transport never decides liveness. Wherever a detector runs (§9.4), SWIM (§10) probes members independently of connection state, so an association blip is never mistaken for a failed node; in static mode without its optional detector (§9.4.1), liveness surfaces only as failed sends (§8). A transport need not report association loss. It MAY surface establishment and loss as a hint to speed detection (e.g. an early `suspect`), but such a hint MUST feed only the detector's refutable `suspect` state, never the terminal `down` decision (§9.4).

A transport carries outbound frames and releases its resources on stop; inbound frames arrive out of band, through a channel the transport hands the system's receive loop at construction (so no single-consumer stream is returned from `&self`):

```rust
pub trait Transport: Clone + Send + Sync + 'static {
    /// Send one frame to `peer` over its association, dialing and completing the
    /// §7.1 handshake on first use. At-most-once (§7.2): never retransmits.
    async fn send(&self, peer: NodeId, frame: Frame) -> Result<(), TransportError>;

    /// Release listeners, background tasks, and open associations on a graceful
    /// stop (§9.3); closing the inbound path ends the system's receive loop.
    /// Default no-op (the in-memory simulator holds nothing to release).
    fn shutdown(&self) {}
}
```

### 7.1 Wire protocol

Two message families travel over an association: **actor envelopes** and **system messages**.

```
Envelope {
    recipient:   ActorId,
    manifest:    String,           // message type id; the dispatch key (§4.4). The sender's
                                   //   `Manifest` const becomes an owned String on the wire.
    correlation: Option<CallId>,   // Some ⇒ a reply is expected (ask); None ⇒ one-way (tell)
    payload:     Vec<u8>,          // encoded message
}

Reply {
    correlation: CallId,
    outcome:     Ok(Vec<u8>) | Failure(CallError),   // application errors are inside Ok(..) as M::Reply
}

SystemMessage =
    | Swim(Ping | Ack | PingReq | IndirectAck)        // §10; each carries the sender's
                                                      //   incarnation and a membership
                                                      //   digest (§9.2, §9.4.4 #1)
    | Raft(group, ..) | RaftPropose(group, ..)        // §9.4.3: replication, votes, and
                                                      //   commands offered to the leader
    | Watch { target, watcher } | Unwatch { target, watcher }                     // §12
    | Terminated { target, watcher, reason }          // §12, addressed to one watcher
    | Receptionist(..)                                // §13: one registration, or a whole
                                                      //   registry pushed for anti-entropy
```

Two families are absent by design. The **handshake** is not a frame but a preamble exchanged before the vocabulary applies (requirement 1), encoded independently of the codec the two ends are still agreeing on. **Membership** has no frame: each mode disseminates through a channel it already has, a gossip digest on the detector's probes (§9.4.4 #1) or a transition in the Raft entries (§9.4.3).

**The frame vocabulary is a versioned format** (`actor.wire`, [compatibility](compatibility-spec.md) §3, §3.1) whose version travels in the handshake (requirement 1), so no frame carries its own stamp and an association is admitted once for its lifetime. Under a positional encoding any variant inserted, removed, or edited shifts the layout of everything after it, so any change to the vocabulary is a new wire version.

### 7.2 Delivery guarantees

- **At-most-once** per call attempt. The framework MUST NOT transparently retransmit a delivered message.
- **No silent loss.** Every `ask` MUST terminate in a reply or a definite `CallError` (§14.1; outcomes listed in §18.5 #1). A pending ask whose target node is declared `down` (§9.4) MUST complete with `Unreachable`; one whose target is confirmed `unreachable` (§10) MAY complete with `Unreachable` early, which at-most-once delivery makes safe even if the node recovers; otherwise the ask's deadline (§14.2) bounds it.
- **Ordering** per directed pair (§6) holds end-to-end over one association. No order is defined between frames an association accepted before it was lost and frames sent over its replacement; a sender that needs order across that boundary re-synchronizes at the application level (an `ask` round-trip suffices).
- Higher guarantees (exactly-once, durable delivery) are out of scope; build them atop this layer with explicit idempotency keys.

---

## 8. Failure model overview

Failures the system MUST represent explicitly, never mask:

| Failure | Detected by | Surfaced as |
|---|---|---|
| Target actor does not exist / has resigned | `resolve` / receive loop | `CallError::DeadLetter` |
| No handler registered for the message type | `deliver` / registry | `CallError::Unhandled` |
| Application error from a handler | inside the reply | `M::Reply` (e.g. `Result<T, E>`) |
| Call exceeded its deadline | caller timer | `CallError::Timeout` |
| Mailbox full (backpressure rejection) | mailbox enqueue (§6) | `CallError::MailboxFull` |
| Recipient node unreachable / down | failure detector (§10); down authority (§9.4) | `CallError::Unreachable` |
| (De)serialization failure | codec | `CallError::Serialization` |
| Association lost | transport | `CallError::Unreachable` |

**Supervision** (§11) handles actor-level faults: a handler panics or the actor stops. **Membership and failure detection** (§9 and §10) handle node-level faults and propagate them to watchers through **lifecycle monitoring** (§12).

### 8.1 The node-down cascade

Declaring a node `down` is one event with consequences in six subsystems. When node `N` is declared `down`:

1. **Detection then decision (§10, §9.4).** A `suspect` unrefuted for `T_suspect` confirms `N` `unreachable`; the mode's **down authority** then decides `down`: the coordinator under the downing policy (gossip-based), the leader by quorum commit (leader-based), or the operator/platform removing `N` from the registry (registry-based). Static mode has no down authority, so the cascade never runs there (§9.4.1).
2. **Membership (§9.1).** The transition propagates through the mode's dissemination channel (§9.4). `down` is **terminal**: `N` rejoins only under a new `NodeId` (§9.1). The entry is later tombstoned (`removed`).
3. **In-flight callers (§7.2, §14).** Every pending `ask` whose target is on `N` completes with `CallError::Unreachable`; it never hangs (§18.5 #2).
4. **Watchers (§12).** No stop message can arrive from a dead node, so each node, on observing `down` in its membership view (§9.2), **synthesizes** `Terminated { reason: NodeDown }` for every locally-watched actor on `N`, delivered into each watcher's mailbox in serial order.
5. **Receptionist (§13).** The receptionist watches the actors it lists, so each `Terminated` from step 4 prunes the registration of an actor *hosted* on `N`, whichever node registered it, and subscribers receive a fresh `Listing`.
6. **Routing afterward (§4.3).** `resolve` of an `ActorId` on `N` still returns a remote `ActorRef`; sends fail with `Unreachable` rather than blocking, since liveness is discovered on send.

A graceful **leave** (§9.3) reaches the same terminal state and steps 3 to 6, differing only in step 1: the node announces `leaving` and drains instead of being suspected. An operator **decommission** (a registry removal, or a committed removal entry in leader-based mode) differs only in that the control plane declares `down` directly, with no detector. A **drain** is not this cascade: it is reversible and leaves the node a member (§9.1).

---

## 9. Cluster membership and node lifecycle

The member-state lattice (§9.1), the view-merge rule (§9.2), and the lifecycle steps (§9.3) are common to every cluster; who *decides* the member set, the **control plane**, is one of four configurable **modes** (§9.4).

### 9.1 Member states

```
joining → up ──→ leaving ──→ down ──→ removed     (the monotonic ladder)
           ⇅        ▲           ▲
        draining ───┴───────────┘                 (reversible cordon, off the ladder)

reachability: reachable ⇄ unreachable             (orthogonal to every state above)
```

- **joining**: handshake complete, not yet fully admitted. Used where admission is an in-cluster decision (leader-based and gossip-based, §9.4); static starts every member `up`, and in registry-based mode admission *is* the registry entry (§9.4.2).
- **up**: full member; may host and address actors.
- **draining** *(registry-based and leader-based modes, §9.4)*: a **reversible** maintenance cordon. The node stays a full member, but service discovery routes new work away from it; `resume` returns it to `up`. It sits **off** the monotonic ladder (not terminal, never advancing toward `removed` on its own), so transitions in and out of it are ordered by the control plane's authority stamp rather than by rank (§9.2).
- **leaving**: graceful shutdown initiated; draining its mailboxes before departure.
- **down**: declared dead by the mode's down authority (§9.4), or reached by completing a graceful leave. **Terminal and irrevocable**: a node that was `down` MUST NOT rejoin under the same incarnation; it MUST restart with a new `NodeId`. The stable identity the static, registry-based, and leader-based modes require is *persisted* identity that survives ordinary restarts; rejoining after `down` means wiping it and joining as a new member.
- **removed**: tombstone, eventually pruned.

**Reachability** (`reachable`/`unreachable`) is an orthogonal flag the failure detector (§10) maintains wherever it runs. A node is first `suspect` (refutable); a suspicion unrefuted for `T_suspect` is confirmed `unreachable`. Both are detector observations, reversible by a higher incarnation; `down` is the separate, terminal decision of the mode's authority (§9.4).

### 9.2 Views and the merge rule

Every node holds a local **membership view**: one entry per member, `(NodeId, state, incarnation, stamp)`. Views update through the mode's dissemination channel (§9.4) and merge entry-wise by the same rule in every mode:

1. **Authority stamp wins.** An entry with a higher **authority stamp** (the registry revision, §9.4.2, or the log commit index, §9.4.3) replaces one with a lower or absent stamp. A mode's stamps come from a single logical writer and totally order its decisions; that order, not state rank, lets a *reversible* transition (`up ⇄ draining`) converge.
2. **Otherwise, the lattice.** At equal stamp, the higher-ranked state on the monotonic ladder `joining < up < leaving < down < removed` wins; incarnation plays no part on this axis. `up` is monotonic and `down` is terminal, so independently-reached views merge without conflict.
3. **`down` is absorbing.** No merge result, stamped or not, returns a `down` member to a live state under the same incarnation. An authority that wants the machine back admits it as a *new* member with a fresh `NodeId`.

Reachability (§9.1) is outside this merge: the detector orders suspicion and refutation by incarnation (§10), the higher incarnation winning and, within one, the more severe view. **Static** and **gossip-based** modes issue no stamps, so the merge reduces to the lattice.

### 9.3 Joining and leaving

The mechanics are common to every mode: a joiner handshakes an existing member (a configured seed, or one learned from the registry); a leaver announces `leaving` and drains its mailboxes; a detector, where one runs, confirms a crash `unreachable` (§10). Each transition that *changes the member set* needs an authority, and each mode names a different one (§9.4):

| Who decides | **Static** | **Registry-based** | **Leader-based** | **Gossip-based** |
|---|---|---|---|---|
| Admission to `up` | nobody; the roster is fixed and all start `up` | appearing in the registry, at that revision | the leader commits `joining → up` | the coordinator, on convergence |
| Finalizing a leave (`→ down`) | n/a; members never leave | deregistration: whoever registered the node removes its entry | the leader commits, at the departing node's request | the coordinator finalizes `leaving → down` |
| A crash | nothing; sends just fail (§8) | a registry mutation, or nothing | the leader MAY commit `down` per the downing policy | the coordinator applies the downing policy |

The `leaving` announcement decides nothing: it starts the drain and advises peers. Watchers (§12) of a departed node's actors learn through the node-down cascade (§8.1) in every mode that reaches `down`.

### 9.4 Membership control plane (modes)

A control plane answers three questions: **where the authoritative member set lives**, **how members learn it**, and **who may declare a member dead**. Each mode is one consistent set of answers; the lattice, merge rule, lifecycle, and terminal `down` are shared (§9.1–§9.3). The mode is chosen at node startup, and every node in a cluster MUST run the same mode.

| | **Static** | **Registry-based** | **Leader-based** (consensus) | **Gossip-based** |
|---|---|---|---|---|
| Authoritative member set | local configuration, fixed at deployment | an external registry (platform API, database, coordination service) | a self-hosted replicated log (Raft) | none; the converged peer-to-peer view |
| Runtime membership change | none (redeploy/restart) | operator or platform mutates the registry | command committed through the elected leader | self-organizing (seeds + gossip) |
| Dissemination | none | each node syncs against the registry | log replication, plus gossip of committed state | infection-style gossip |
| View consistency | trivial (fixed) | delegated to the registry; members converge on its latest revision | strong at the log; members converge on the commit order | eventually consistent |
| Failure detection (§10) | none (MAY enable, observe-only) | SWIM, **observe-only** | SWIM as a sensor feeding the leader | full SWIM |
| Declares `down` | nobody | the operator/platform, via the registry | the leader, by quorum-committed entry | the coordinator, under the downing policy |
| `draining` cordon (§9.1) | no | yes (registry state) | yes (committed command) | no |
| External dependency | none | the registry | none | none |
| Sweet spot | embedded, edge, appliances, small fixed clusters | cloud-native, where an orchestrator or managed store already exists | self-contained systems needing authoritative placement and metadata | large or elastic clusters with high churn |

#### 9.4.1 Static

Membership is fixed at deployment: every node reads the full topology from local configuration, all members start `up`, and no lifecycle transition occurs at runtime; changing the topology is a redeploy or restart. By default no detector runs and no membership traffic flows; no configuration has downing.

- Node identity MUST be stable across restarts and derivable from configuration, so a restarted node resumes its place in the roster. `ActorId`s minted by its previous run still resolve to it and dead-letter (§4.3).
- A node that stops answering remains a member: sends to it fail with `Unreachable` or `Timeout` (§8), no `Terminated` is synthesized for its actors (§12), and the node-down cascade (§8.1) never runs.
- A deployment MAY enable the detector (§10) **observe-only**, trading probe traffic for `suspect` and `unreachable` transitions on the event stream (§16). Nothing else changes: with no down authority there is no `down`, no `NodeDown` (§12), and no membership transition. A pending `ask` to an `unreachable` peer runs to its deadline (§7.2), and the receptionist keeps listing that peer's actors (§13 requirement 4), since both key on `down`.

#### 9.4.2 Registry-based

Membership changes at runtime, with the authoritative state held in an **external registry** (a platform API, a database, or a coordination service) that the cluster reads but does not operate. An operator or platform adds and removes nodes by mutating it; every member runs a **sync loop** that watches or polls it and applies its state to the local view.

1. **Revisions.** The registry MUST expose a monotonic **revision** that totally orders its states (a `resourceVersion`, `modRevision`, version column, or equivalent). Each applied entry carries that revision as its authority stamp (§9.2), so members converge on the latest state regardless of sync order, including `up ⇄ draining`. Members MAY also gossip stamped entries; the merge makes that a safe accelerant, never a second authority.
2. **Join:** a node is admitted by appearing in the registry (registered by the platform or by itself, per deployment policy); members observing the entry handshake it and mark it `up` at that revision. `joining` is unused (§9.1).
3. **Leave:** whoever registered the node removes its entry, and the removal's revision finalizes `down`/`removed` and runs the node-down cascade (§8.1). The node's own `leaving` starts its drain but decides nothing (§9.3).
4. **Down:** only a registry mutation declares `down`. The detector (§10) is **observe-only**: a node that stops answering becomes `unreachable`, reversibly, and stays a member until the operator or platform removes its entry, with the effect of item 3.
5. **Drain / resume:** the reversible cordon (§9.1) is a registry state. A `draining` or `unreachable` member keeps its membership across a restart, so node identity MUST be stable across restarts in this mode.
6. **Registry unavailability** pauses membership *changes* only; the data plane keeps running on the last-synced view. A node MUST NOT treat its own inability to reach the registry as evidence about peer liveness.
7. **Simulation seam.** The registry client is a trait, like `Transport` (§7); the simulator supplies an in-memory registry with seeded latency, staleness, and unavailability (§18.2, §18.3).

#### 9.4.3 Leader-based (consensus)

The cluster self-hosts its source of truth as a **replicated log** coordinated through **Raft**. An elected **leader** serializes all membership and control-plane changes, and a **quorum of voters** guarantees strong consistency and safe failover. This specification does not restate Raft; it requires the protocol's observable guarantees (election safety, log matching, leader completeness) of any implementation.

1. **Single writer, total order.** Every membership transition (`joining → up`, `drain`/`resume`, a graceful leave committed at the departing node's request, anything `→ down`) MUST be a log entry committed through the current leader. The commit index is the authority stamp (§9.2): members apply committed entries in log order and converge on one sequence of membership states. A command offered to a non-leader is redirected or rejected, never applied.

   **A committed entry a member cannot decode MUST stop that member's control plane, never be skipped.** An entry one member skips is one its peers apply, so its view becomes a **gap** in the log rather than a *prefix* of it, while it remains a full member of a cluster whose membership it no longer tracks. Such a member MUST stop applying transitions, permanently, and MUST NOT proceed as though it had applied one.

   **The transition payload carries its own version** (`actor.membership`, [compatibility](compatibility-spec.md) §3): it rides `EntryPayload::App` inside `actor.raft.log`, whose version covers the envelope and not the payload, so the stamp distinguishes a transition another build defined from a damaged payload, whose fixes are opposite.

   Such a member SHOULD keep replicating and acknowledging the log: the entry is durable either way and a restart onto a build that can read it applies it, so refusing the append costs availability and buys no safety. If it leads, it SHOULD hand leadership to a peer that can still apply (item 8), since a leader that cannot observe its own committed decisions re-proposes them on every pass, forever. Its condition SHOULD be readable by an operator (the reason item 2 gives). Where stamped status also travels by gossip (item 3), it SHOULD keep gossiping: a peer that applied the entry can still deliver its *outcome*, bounding the damage, though only for transitions the stopped build can name, which excludes the one that stopped it.
2. **Voters.** A configured, modest subset of members (typically 3 or 5) are **voters**; only they elect leaders and form quorums. Voters MUST persist their Raft state (term, vote, log) durably and MUST keep stable node identities across restarts. Voter-set changes are themselves committed configuration entries (single-server change or joint consensus). A voter declared `down` MUST be removed from the voter set by a committed change; its replacement joins with a fresh `NodeId` (§9.1).

   **A voter that cannot persist MUST stop voting.** Every durable write precedes the announcement of the state it records, so a write reported durable that was not lets a voter grant a second vote in a term across a restart, or acknowledge an entry it cannot replay. The durability seam MUST therefore be able to report failure, and a failure MUST be permanent for that voter: it steps down and thereafter grants no vote, starts no election, and acknowledges no append. It MUST NOT resume on a later success: nothing at this layer can establish that the volume recovered, and a log resumed after a gap presents the records either side of it as contiguous.

   A failed voter SHOULD stay reachable and answer with its term, refusing explicitly, rather than fall silent, so peers elect around it at once instead of timing out on every append. Its condition SHOULD be readable by an operator, since a node that has stopped voting otherwise looks merely slow. An implementation SHOULD NOT express this as a panic unless the runtime guarantees a panic halts the whole node: where supervision restarts the faulted unit, or the fault kills one task, the node survives and stays counted in quorums with no durability behind it.
3. **Dissemination.** Voters learn state from log replication. Non-voting members MAY replicate the log as learners or receive committed membership state by gossip stamped with the commit index; by the merge rule (§9.2) both paths converge on the same view.
4. **Downing.** The detector (§10) is a sensor feeding the leader: on a confirmed `unreachable`, the leader MAY commit `unreachable → down` under the configured downing policy. Downing is therefore **quorum-gated**: a leader that has lost quorum cannot commit, so a minority partition can never evict the majority (§18.5 #22). Raft's own heartbeats, which detect a failed leader, are internal to consensus and distinct from member reachability.
5. **Quorum loss** pauses the control plane only (no join, drain, or down commits until a quorum reforms); existing members keep exchanging actor traffic on the last committed view.
6. **Beyond membership.** The log MAY carry other control-plane decisions as opaque entries (singleton placement, shard allocation, cluster-wide configuration), giving them the same total order and failover. This specification normatively defines only the membership entries.
7. **Runtime seams.** Consensus traffic rides the ordinary `Transport` (§7) as system messages, and its timers and jitter come from `Clock` and `Entropy` (§4.6), so a leader-based cluster simulates deterministically (§18).
8. **Leadership transfer (RECOMMENDED).** A group leader SHOULD be able to hand leadership to a named voter without waiting out an election timeout (Raft §3.10), so a planned departure does not cost one failover per led group. It MUST offer the transfer only to a voter that has replicated its last entry, the invitation MUST be accepted only from the leader of the recipient's current term, and the transfer starts an ordinary election, so it can move leadership but never split it.
9. **Heartbeat coalescing (RECOMMENDED).** An implementation SHOULD batch the empty appends bound for one peer into a single frame, and their replies likewise, since a node leading `G` groups otherwise sends `G × (R-1)` empty frames per interval (entity runs one group per shard, entity §8.2); the batching MUST be framing only, invisible to every group's state machine.
10. **The first election is not the detector's business (RECOMMENDED).** A group with no persisted term, log, or snapshot has no stopped leader to detect, so an implementation SHOULD NOT make it wait out the election timeout before its first campaign. That campaign MUST NOT begin before an existing leader could have been heard from (at least one heartbeat interval; the reference implementation waits two, so a single dropped heartbeat does not reopen the window), and its stagger SHOULD be deterministic per `(group, node)` so it draws no entropy (§18.1). A group holding persisted state keeps the full timeout.

#### 9.4.4 Gossip-based

Membership propagates peer-to-peer through **SWIM** and anti-entropy gossip, with no election, quorum, or external authority: every node converges on an eventually consistent view, and per-node load stays constant as the cluster grows.

1. **Dissemination.** There is one channel, the detector's probe: every `Ping`, `Ack`, `PingReq`, and `IndirectAck` carries a membership digest (§10 #6), and the probe target is drawn from `Entropy` (§4.6). Each probe interval thus hands a random peer the sender's view, merged by §9.2, and infection-style spread costs no traffic of its own.
2. **Refutation.** Entries carry the member's incarnation; a node that sees a stale suspicion or state about itself increments its incarnation and gossips the override (§10).
3. **Coordinator.** Transitions that need a single actor (admitting `joining → up`, finalizing `leaving → down`) fall to the **coordinator**: a deterministic *role* each node derives from its own view, not an election, held by the lowest-address member of each node's converged view that is `reachable` and neither terminal (`down`, `removed`) nor departing (`leaving`, `draining`). `joining` members are eligible, the deriving node included, which lets a fresh cluster bootstrap: with no `up` member anywhere, the lowest-address joiner takes the role and admits itself and its peers (#5). The coordinator acts only on a **locally stable, fully-reachable view** (every live member it sees is `reachable`), with one exception: applying the downing policy to an `unreachable` member (#4), which by definition cannot wait for full reachability. Two nodes that transiently both consider themselves coordinator cannot conflict: their decisions merge through the lattice, since `up` is monotonic and `down` terminal (§9.2).
4. **Downing.** The detector drives `suspect → unreachable`; the configured **downing policy** (`DowningPolicy`: conservative, which never auto-downs and leaves `unreachable` to an operator, or timeout-based) decides `unreachable → down`, applied by the coordinator. A partition leaves each side seeing the other `unreachable`; the default policy MUST be conservative and MUST NOT auto-`down` across a partition (§18.5 #16).
5. **Join:** a new node handshakes a configured seed → `joining`; the coordinator admits it to `up` on convergence.

This mode trades an instantaneously consistent view for availability and partition tolerance (§1.2): nodes may briefly disagree, and everything above membership (death watch, §12; the receptionist, §13) tolerates that.

---

## 10. Failure detection (SWIM)

Every mode except **static** runs a SWIM-style detector on each node over its associations; static MAY enable it (§9.4.1). The detector is the cluster's **reachability sensor**, maintaining the `reachable`/`suspect`/`unreachable` axis (§9.1) identically wherever it runs. Only its *authority* over `down` differs by mode (§9.4): in **registry-based** mode, and in **static** mode when enabled, it is **observe-only**, and nothing it reports moves a member to `down`.

1. **Direct probing.** Periodically (every `T_probe`), pick a member and send `Ping`; expect `Ack` within `T_rtt`.
2. **Indirect probing.** On a missed `Ack`, ask `k` random members to `PingReq` the target on the prober's behalf; if any relays an `Ack`, the target is alive.
3. **Suspicion.** If direct and indirect probes both fail, mark the target `suspect` and gossip the suspicion, carrying the suspected node's incarnation.
4. **Refutation.** A node that sees itself suspected increments its incarnation and gossips an `alive` override, clearing the suspicion cluster-wide.
5. **Confirmation.** A suspicion unrefuted for `T_suspect` becomes `unreachable`; the mode's down authority (§9.4) then MAY move it to `down`.
6. **Piggybacking.** Membership and suspicion updates SHOULD ride on ping/ack messages to bound overhead and speed dissemination.

The parameters `T_probe`, `T_rtt`, `k`, and `T_suspect` MUST be configurable. `T_suspect` SHOULD scale with cluster size (logarithmically, for instance) to keep the false-positive rate low.

---

## 11. Supervision

Supervision governs what happens when a **local** actor faults: a handler panics, or `A::started` fails.

### 11.1 Hierarchy
- Every actor except the roots has a **parent**, the actor that spawned it (`Ctx::spawn`, §3.4). Parents supervise children.
- The executor boundary catches faults; a panic MUST unwind into a supervision decision, never crash the node.

### 11.2 Strategies

```rust
pub enum SupervisionDirective {
    Stop,                                  // terminate the actor; notify watchers (§12)
    Restart { max: u32, within: Duration, backoff: Backoff }, // re-create state and resume
    Escalate,                              // fail the parent, applying the parent's strategy
    Resume,                                // keep state, drop the failed message (use sparingly)
}

/// What `Actor::supervision()` returns (§3.1.1): the actor's decider, mapping
/// each fault kind to a directive.
pub struct Supervision { /* decider: Arc<dyn Fn(Fault) -> SupervisionDirective + Send + Sync> */ }

impl Supervision {
    pub fn stop() -> Self;                          // every fault → Stop (the default)
    pub fn resume() -> Self;                        // every fault → Resume
    pub fn restart(max: u32, within: Duration, backoff: Backoff) -> Self; // every fault → Restart{..}
    pub fn with<F>(decider: F) -> Self              // per-fault decision (a closure, not just a fn ptr)
        where F: Fn(Fault) -> SupervisionDirective + Send + Sync + 'static;
    pub fn decide(&self, fault: Fault) -> SupervisionDirective; // evaluate the decider for a fault
}
```

- The **default** directive MUST be `Stop`. For transient faults, `Restart` is usually the better choice.
- **Restart** MUST construct a fresh actor value (state is not preserved by default) while the actor keeps its `ActorId` and mailbox. That needs a *factory*: an actor spawned with one (`spawn_with` for a root, `Ctx::spawn_with` for a child) is restartable. An actor spawned **by value** (`spawn`/`Ctx::spawn`) cannot be reconstructed, so for it `Restart` MAY degrade to `Stop`. Exceeding `max` restarts `within` the window MUST escalate to `Stop`.
- **Resume** drops the failed message and keeps the actor. A failed `started` has no message to drop and the actor is not yet ready (§4.2 step 3), so for `Fault::Started`, `Resume` MUST degrade to `Stop`, reported as the effective decision (§16).
- **Backoff** between restarts MUST be supported (exponential with jitter RECOMMENDED) to avoid hot-restart loops.
- A `restart` re-runs `A::started`; the prior value's `A::stopped` runs with `StopReason::Failed`.
- `Fault` is a small `Copy` enum identifying the fault kind, so the per-actor decider MAY return a different directive per kind.

### 11.3 Scope
Supervision is a **local** mechanism: a node supervises only its own actors. Remote failures are not supervised; they surface to callers as `CallError` (§8) and to watchers as `Terminated` (§12).

---

## 12. Lifecycle monitoring (death watch)

Any actor MAY watch any other actor, local or remote, and learn when it terminates.

```rust
// via Ctx (§3.4):
fn watch<B: Actor>(&self, target: &ActorRef<B>) where A: Handler<Terminated>;
fn unwatch<B: Actor>(&self, target: &ActorRef<B>);

pub struct Terminated {
    pub id: ActorId,
    pub reason: TerminationReason,
}

pub enum TerminationReason {
    Stopped,   // graceful stop
    Failed,    // fault or panic
    NodeDown,  // the actor's node was declared down
}
```

`A::stopped` (§3.1.1) receives a `StopReason`, the local-only subset `{Stopped, Failed}`, since an actor runs its own hook only when it stops on its own node. `TerminationReason`, what a *watcher* observes, adds `NodeDown`: the actor's node died and no local `stopped` could run (§8.1). A watcher observes terminations through the `Handler<Terminated>` impl that `Ctx::watch` requires (§3.4). The framework supplies `Terminated`'s `Message` impl (`type Reply = ()`, reserved manifest `actor.Terminated`) so the bound is well-formed (§3.2), but the type never travels in an actor envelope: only `SystemMessage::Terminated` (§7.1) crosses the wire, and the receiving node synthesizes the local value, so the reserved manifest is never registered or dispatched from network bytes (§4.4).

Guarantees (MUST):
1. After `watch(target)`, if the target terminates for **any** reason, the watcher receives exactly one `Terminated` for it, delivered into the watcher's mailbox.
2. **Node down implies termination.** When a node is declared `down` (§9.4), every watched actor on that node MUST yield a `Terminated { reason: NodeDown }` to its watchers, even though no explicit stop message can arrive.
3. Watching an already-terminated actor MUST immediately yield `Terminated`.
4. Signals respect the per-actor serial order: a `Terminated` arrives through the mailbox like any other message, never out of band.

Guarantee 2 is relative to the cluster's `down` decisions, not physical reality: an actor on a crashed node terminates *observably* only when an authority declares that node `down` (§9.4). Where none does (static always; registry-based until the registry removes the node), watchers learn nothing from the crash, and callers see `Unreachable` or `Timeout`.

Remote watch sends a `Watch` system message to the target's node, which tracks watchers and emits `Terminated` on stop; the watcher's own node synthesizes `Terminated` when its view marks the target's node `down` (§8.1).

---

## 13. Receptionist (service discovery)

A node needs the initial `ActorRef` for a remote service without hardcoding its `ActorId`. The **receptionist** is a cluster-replicated registry, reached through a handle rather than addressed as an actor.

```rust
pub struct Key<A> { id: &'static str, _marker: PhantomData<fn() -> A> }

impl Receptionist {
    fn register<A: Actor>(&self, key: Key<A>, who: &ActorRef<A>);
    fn lookup<A: Actor>(&self, key: Key<A>) -> Listing<A>;                // current snapshot
    fn subscribe<A: Actor>(&self, key: Key<A>) -> impl Stream<Item = Listing<A>>; // live updates
}
```

`lookup` is synchronous: it reads replicated local state (requirement 2), with nothing to await.

Requirements:
1. The receptionist is not an actor and has no resolvable path (§3.6): it is a handle from `ActorSystem::receptionist`, available on every node without prior introduction, and its registrations replicate over the `Receptionist` frame (§7.1).
2. Registrations are **replicated** across the cluster and **eventually consistent**. A CRDT (an OR-Set keyed by registering node) is RECOMMENDED so concurrent registrations merge without coordination.
3. When a node goes `down` (§9.4), the receptionist MUST prune every registration whose actor is *hosted* on it, and subscribers MUST receive an updated `Listing`. Pruning is driven by the `Terminated` the cascade synthesizes for each watched actor (§8.1 step 4), so it follows the actor's host, not the node that registered it.
4. A `lookup` or `subscribe` listing MUST omit actors on a node that is not serving: one an operator has `draining` (§9.4), or one that is `down`. A node confirmed `unreachable` (§10) but not `down` stays listed, since reachability is refutable and only the down authority takes a node out of rotation. A `draining` node's registrations are **retained** and routed around, not pruned; `resume` restores them without re-registration, and a drained node still answers a direct `ask`.
5. `subscribe` MUST deliver the current listing on subscription and a fresh listing on every change.
6. `Key` is typed by actor type, so `lookup` and `subscribe` return correctly typed `ActorRef`s.

---

## 14. Error model

### 14.1 `CallError`

`CallError` covers **transport and system** failures only: the failure to *complete* a call. Application failures live inside `M::Reply` (§3.2).

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
- `CallError` variants MUST be exhaustive at the public API, so callers handle partial failure explicitly.

### 14.2 Principles
- Errors are **values**, propagated by `Result`; the framework does not use panics for control flow across actors.
- Supervision (§11) contains a handler panic, which never crosses the wire as a panic: it becomes `Terminated`/`Restart` locally and `Unreachable`/`DeadLetter` to in-flight callers.
- Timeouts are mandatory on `ask`: every request MUST carry an effective deadline, explicit or a system default.

### 14.3 Reading a call result

The **outer** `CallError` (did the call *complete*?) and the **inner** application `E` (what did the handler decide?) are distinct, and the type MUST NOT collapse them: a transport failure the caller may retry is not an application failure it must not.

A system SHOULD offer one canonical way to consume the common case, where a caller treats any failure uniformly:

```rust
// Convenience over `ask`: one error channel, when `E` can absorb a `CallError`.
impl<A: Actor> ActorRef<A> {
    pub async fn ask_flat<M, T, E>(&self, msg: M) -> Result<T, E>
    where A: Handler<M>, M: Message<Reply = Result<T, E>>, E: From<CallError>;
}
```

`ask_flat` collapses `Result<Result<T, E>, CallError>` into `Result<T, E>` through `E: From<CallError>`. Callers that must tell "did not complete" from "handler failed" keep using `ask`.

---

## 15. Security (RECOMMENDED)

1. **Transport security.** Associations SHOULD support mutual TLS, and node identity SHOULD bind to a verified certificate.
2. **Authentication.** The handshake SHOULD authenticate the peer and MAY enforce an allowlist of permitted node identities or clusters; a cluster secret prevents accidental cross-cluster association. The secret travels **in** the handshake, so on an unencrypted association anyone who observes one handshake can join the cluster as a full member: a secret guards against *accident*, not an on-path adversary, which only transport security (item 1) stops.
3. **Encryption is a stated choice, not a default.** A configuration MUST make a plaintext association something a deployment names explicitly, not what it gets by omitting the TLS material, so the decision and its consequence for item 2's secret are visible where it is taken and at every review. Plaintext remains correct on loopback, in tests, and on a network the operator controls end to end.
4. **Deserialization safety.** As in §5, the dispatch registry MUST instantiate only registered message types from network input; no path may lead from an incoming envelope to constructing an arbitrary type.
5. **Authorization.** A system MAY gate `deliver` per `(peer, actor, manifest)` through an optional `Authorizer` (`fn authorize(&self, peer: NodeId, recipient: &ActorId, manifest: &str) -> bool`), consulted before dispatch. A denied message MUST be rejected as a system failure, never deserialized into the actor's type, so an unauthorized peer cannot trigger handler side effects. With no `Authorizer`, every message that clears the handshake is admitted.

---

## 16. Observability (RECOMMENDED)

A conforming system SHOULD expose:
- **Metrics:** mailbox depths, message and throughput rates, call latency, restart counts, association count, membership size, suspicion and down events.
- **Tracing:** propagate a trace/correlation context through envelopes, so a logical request can be followed across nodes.
- **Lifecycle logging:** spawn and resign, membership transitions, downing decisions, and supervision actions, each at a defined, filterable level.

A simulator subscribes to the same stream to check invariants (§18.5). A conforming system SHOULD emit, as structured events on a single extensible `Event` enum: `assign_id`/`actor_ready`/`resign_id` (§4.2), mailbox enqueue and dispatch (§6), every `ask` outcome (§14), membership and reachability transitions (§9, §10), supervision decisions (§11) and restarts (§11.2), `Terminated` deliveries (§12), and inbound remote messages refused at delivery with the refusing `CallError` (§6, §8), authorization refusals included (§15): the only trace a refused `tell` leaves. Metrics and cross-node trace propagation are RECOMMENDED, not REQUIRED.

---

## 17. Conformance

An implementation conforms **iff** the twenty-two runtime invariants of §18.5 hold, each checked by the method that catalogue assigns it (continuous checker, targeted test, compile-fail, or differential run), and the four properties below hold by construction. The cited section's MUSTs define each property; the runtime catalogue is stated once, in §18.5, and `actor-simulation` holds the Rust copy, which the `spec` gate holds equal to it.

| Property | Defined in | Verified by |
|---|---|---|
| Actor state reachable only through the actor's own handlers | §3.5 | construction (ownership); §18.5 #4 |
| Every message and reply satisfies `SerializationRequirement` | §5 | compile-time (trait bound) |
| Remote dispatch listed by a hand-written `register` (a defaulted `Actor` method); no framework macro | §1.1, §4.4 | construction |
| Associations established by an authenticated, version-checked handshake | §7, §15 | construction; transport tests |

---

## 18. Testability and deterministic simulation

A conforming implementation SHOULD be testable by **deterministic simulation**: a whole cluster runs in one process, on one logical thread, over virtual time, network, and randomness, so a single seed reproduces an entire multi-node run, failures included, exactly.

### 18.1 Determinism contract

A system that supports simulation MUST satisfy:

1. **Seed-reproducibility.** Given the same seed, configuration, and workload, two runs MUST produce byte-identical event streams (§16). All nondeterminism MUST pass through the `Clock`, `Entropy`, and `Spawner` traits (§4.6), the `Transport` trait (§7), and, in registry-based mode, the registry client (§9.4.2).
2. **Quiescence-driven time.** In simulation, logical time MUST advance only when no task is ready to run, so timeouts, SWIM intervals, and backoff cost no wall-clock time and a run can cover hours of cluster time per CPU-second.
3. **No ambient nondeterminism.** A simulation build MUST NOT read the wall clock, spawn OS threads, or use a non-seeded RNG. A single leak breaks reproducibility, so implementations SHOULD enforce this statically (e.g. a lint forbidding the offending APIs).

### 18.2 Virtualized traits

Simulation reuses the production traits with different implementations:

| Trait | Production | Simulation |
|---|---|---|
| `Clock` (§4.6) | wall clock | logical clock; advances only at quiescence |
| `Entropy` (§4.6) | OS-seeded | one seeded PRNG, the only randomness in the run |
| `Spawner` (§4.6) | multi-thread runtime | single-thread cooperative scheduler |
| `Transport` (§7) | TCP | in-memory network with seeded latency / loss / reorder |
| registry client (§9.4.2) | platform API / database | in-memory registry with seeded latency / staleness / outage |
| codec (§5) | production codec | unchanged; runs real (de)serialization |

Because the traits are the same, simulation runs the real `ActorSystem`, mailbox, membership, SWIM, supervision, and receptionist code, not a model of it, and every cross-node hop tests the real wire encoding.

### 18.3 Fault injection

Under seed control, a simulator MUST be able to inject at least:

- **Transport:** frame drop, duplication, delay, reordering; association loss.
- **Mailbox:** induced `MailboxFull`; maximal cross-sender reordering subject to per-sender FIFO (§6).
- **Scheduling:** seed-randomized selection among ready tasks.
- **Membership / control plane:** dropped or delayed pings, partitions, stale or replayed gossip, stale incarnations (§9 and §10); leader crashes and elections mid-transition (leader-based mode); a stalled, lagging, or unavailable registry sync (registry-based mode).
- **Supervision:** induced handler and `started()` faults (§11).
- **Nodes:** abrupt crash (no graceful leave) at an arbitrary step, which MUST surface as `Unreachable` or `Timeout` to in-flight callers (§7.2) and, in a mode whose authority declares `down` (§9.4), as `NodeDown`/`Terminated` to watchers (§12).

A fault is realized by the mechanism that fits its layer, as long as the effect is exercised:
- **Frame corruption** needs real bytes, so §7's malformed-frame requirement is exercised by the production transport's framing tests (`actor-runtime/tests/transport_invariants.rs`, `a_malformed_frame_tears_down_its_association_not_the_node`: a handshaken peer writes an undecodable payload, then a length past the 16 MiB cap, and each association dies alone while the node keeps serving another). The in-memory simulator carries *structured* frames (only the payload is codec-encoded, §18.2), so it injects the consequence, a lost association, directly.
- **Stale / replayed gossip and stale incarnations** arise from drop, duplication, delay, and reordering of gossip-bearing frames on the same faulted transport; no separate injector is needed.
- **Induced handler / `started()` faults** come from workload actors that fault on demand (a handler that panics, a `started` that returns `Err`), not from reaching into an arbitrary actor.

Each run SHOULD enable a random subset of faults at random intensities ("swarm" testing); a run with no faults MUST still pass.

### 18.4 Workloads

Tests are **workloads** over the cluster: a `setup` that builds actors and registrations, a `start` that drives traffic, and a `check` that asserts the invariants of §18.5. A workload MUST observe the cluster only through the public API and the §16 event stream, never through actor state (§3.5), except through `when_local` (§3.5.1) where explicitly intended.

### 18.5 Invariant catalogue

The inline MUSTs each invariant cites are its normative statement. Each MUST hold even under the faults of §18.3.

Verification is **layered** (§18.6). The core *safety* properties, "a bad thing never happens" over the §16 event stream, are checked **continuously**, on every run and at final quiescence, by always-on checkers (at minimum #1, #4, #6, #13, #15). The rest use the method that fits: a targeted conformance test for a *liveness* or scenario property, a compile-fail test for type-safety (#20), a differential local-vs-remote run for location transparency (#21). Some safety MUSTs are not emergent over the stream and stay targeted: #5 (the mailbox bound is structural and backpressure a per-call contract) and #11 (exactly-once is *per `watch`*, but the stream carries no per-`watch` identity, and re-watching a dead actor legitimately re-fires, §12). A machine-checked **catalogue** records each invariant's method; a drift test fails the build if a continuous checker and its catalogue entry disagree, keeping the §17 "Verified by" column true.

1. **No silent loss (§7.2, §14).** Every `ask` issued terminates in exactly one of `Ok(reply)`, `Timeout`, `Unreachable`, `DeadLetter`, `Unhandled`, `MailboxFull`, `Serialization`, or `System`. A local `ask` awaits mailbox space (§6), so `MailboxFull` reaches one only from a remote target, whose receiving node rejects the enqueue into the reply rather than stalling its receive loop. At final quiescence no `ask` remains pending.
2. **Crash completes in-flight calls (§7.2, §9.4).** An `ask` whose target node is declared `down` completes with `Unreachable`; it never hangs. (Where no authority declares `down`, a confirmed `unreachable` MAY complete it early with `Unreachable`, and the deadline bounds it with `Timeout` regardless; #1 still holds.)
3. **Per-pair FIFO (§6).** Messages from one sender to one recipient are observed in send order (within one association, §6, §7.2), even under maximal reordering injection; no ordering is assumed across senders.
4. **Serial, non-reentrant execution (§6).** An actor never processes two messages concurrently; `&mut self` is never aliased.
5. **Bounded, non-dropping mailbox (§6).** A full mailbox blocks or returns `MailboxFull`; it never drops silently.
6. **Lifecycle order and exactly-once (§4.2).** `assign_id` → `actor_ready` → `resign_id` occur in order; `assign_id` and `actor_ready` exactly once; `resign_id` exactly once even when spawn fails between steps 1 and 3; the id is undeliverable between assign and ready.
7. **`resolve` is local (§4.3).** Locality classification performs no network round-trip.
8. **Manifest dispatch and allowlist (§4.4, §5, §15).** An unregistered `(actor type, manifest)` yields `Unhandled`; no type outside the registry is ever constructed from network bytes.
9. **Local sends skip serialization (§4.3, §4.4).** A local `ask`/`tell` performs no encode or decode, yet its observable result is identical to the remote path (cf. 21).
10. **`ActorRef` rebinding (§4.4).** An `ActorRef` carried in a message or reply is rebound to the receiving system on decode and is usable there.
11. **Death-watch exactly-once (§12).** After `watch`, the watcher receives exactly one `Terminated` for any cause, including `NodeDown` when the target's node is declared `down`.
12. **Watch-after-death (§12).** Watching an already-terminated actor yields `Terminated` immediately.
13. **Signal ordering (§12).** `Terminated` is delivered through the mailbox in serial order, never out of band.
14. **Membership convergence (§9.2, §9.4).** Once faults cease and partitions heal, all `up` members converge on one membership set within bounded logical time, by anti-entropy (gossip-based), registry sync (registry-based), or log replication (leader-based).
15. **`down` is terminal (§9.1).** A node observed `down` never reappears `up` under the same incarnation.
16. **Partition tolerance (§9.4).** Under the default downing policy, a partition alone never moves a member to `down`, only to `unreachable`. **Registry-based** mode holds this unconditionally: its detector is observe-only, so only the registry ever declares `down`. **Leader-based** mode holds it for any side without quorum, which can commit no transition at all (#22).
17. **SWIM refutation (§10).** A node that sees itself suspected refutes via a higher incarnation, clearing the suspicion cluster-wide.
18. **Supervision containment (§11).** A handler panic never crashes the node; the default directive is `Stop`; restarts back off; exceeding `max` within the window escalates.
19. **Receptionist consistency (§13).** Registrations from a `down` node are pruned and subscribers notified; `subscribe` delivers the current snapshot first, then every change; concurrent registrations merge (eventual consistency).
20. **Type-safety (§3.3).** An `ask`/`tell` of a message an actor has no `Handler` for does not compile. (Compile-fail tests assert this, not the runtime; see §18.6.)
21. **Location transparency (§3.3).** Running the same workload with a target local versus remote produces observably identical replies and ordering. (Differential check.)
22. **Quorum-gated control plane (§9.4.3).** In leader-based mode, every membership transition is a quorum-committed log entry, applied in log order; at most one leader per term issues them, and a partition side lacking a quorum of voters commits none: a minority can never evict the majority. Applying *in log order* admits no skipping: a member that cannot decode a committed transition stops applying rather than stepping over it, so each member's view is always a prefix of the committed log and never a gap in it.

### 18.6 Reproduction, layering, and CI

- **Reproduction.** A failing run MUST be replayable from its `(seed, configuration)` alone.
- **Layered checks.** Simulation covers the distributed invariants (1 to 19, 21, and 22); compile-fail tests cover invariant 20 by asserting the compiler rejects invalid sends. A `loom`/`kani` model-check of the executor is an optional cross-check: the simulator's seed-randomized ready-task selection (§18.3) already explores executor interleavings deterministically.
- **Regression corpus.** A failure SHOULD be kept as a `(seed, configuration)` and replayed permanently. Fixed-seed swarm sweeps serve as the standing corpus.
- **Continuous testing.** CI SHOULD run many seeds per change across different fault configurations; the coverage metric is cluster-hours exercised per change, not test count.

---

## 19. Cluster utilities (separate document)

Deterministic placement, group routers, and the cluster singleton are specified in [`cluster-utilities-spec.md`](cluster-utilities-spec.md), with their own invariant catalogue (U1, U2, …). They build on membership (§9), the receptionist (§13), and the event stream (§16) without modifying core semantics.

---

## Appendix A: End-to-end example

```rust
// --- Define the actor; `register` lists the messages it accepts over the network ---
pub struct Greeter { greeting: String }
impl Actor for Greeter {
    type System = TcpCluster;   // the production wiring of `ClusterSystem` (Appendix B)
    fn register(r: &mut HandlerRegistry<Self>) { r.accept::<Greet>(); }  // macro-free; spawn calls it once
}

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

const GREETERS: Key<Greeter> = Key::new("greeters");

// --- Bring a node up. One call resolves the address plan, binds, starts the TCP
// --- transport and the system over it, and seeds the membership view (§7, §9.3) ---
async fn node(id: u64) -> Result<TcpCluster, String> {
    actor_runtime::join(JoinOptions {
        node: id,                          // this process, as NodeId::new(id)
        voters: 3,                         // the roster is NodeId::new(1) ..= NodeId::new(3)
        peer_hosts: BTreeMap::new(),       // loopback here; a deployment names hosts
        clients: BTreeMap::new(),          // non-voting edge nodes, if any
        bind_host: "127.0.0.1".into(),
        advertise_host: None,              // what peers dial back, when it differs
        port_base: 7000,                   // node `i` listens on `port_base + i - 1`
        secret: "cluster-secret".into(),   // guards the handshake (§15)
        codec: Arc::new(JsonCodec),        // one codec for the whole cluster (§5)
        events: Arc::new(()),              // the event sink (§16); `()` discards
        encryption: Encryption::PlaintextTrusted,   // or MutualTls on an untrusted link
        // Leader-based membership (§9.4.3) over the roster `join` computes, so the
        // voter set and the admitted member set cannot drift apart.
        membership: Box::new(|roster| MembershipMode::Leader(LeaderMode {
            swim: SwimConfig::default(),
            raft: RaftConfig::new(roster.to_vec()),
            downing: DowningPolicy::Conservative,
        })),
    }).await
}

// --- Host node ---
let system = node(1).await?;
let greeter = system.spawn(Greeter { greeting: "Hello".into() });
system.receptionist().register(GREETERS, &greeter);

// --- Client node: the same call with its own id. It finds the greeter through the
// --- receptionist, with no `ActorId` and no address of its own to configure (§13) ---
let system = node(2).await?;
let listing = system.receptionist().lookup(GREETERS);   // synchronous snapshot (§13)
if let Some(greeter) = listing.first() {
    // Identical call site whether `greeter` is local or remote.
    // `greeter.ask(Greet { .. })` compiles only because Greeter: Handler<Greet>.
    match greeter.ask(Greet { name: "world".into() }).await {
        Ok(msg)                      => println!("{msg}"),       // "Hello, world!"
        Err(CallError::Unreachable)  => { /* peer down, react */ }
        Err(CallError::Timeout)      => { /* retry policy decides */ }
        Err(e)                       => eprintln!("call failed: {e:?}"),
    }
}
```

## Appendix B: Suggested crate layout

```
actor-core/             # Actor, Message, Handler, ActorRef, Ctx, ActorSystem,
                         #   HandlerRegistry, Manifest, CallError, the Clock/Entropy/Spawner
                         #   seam, supervision, death watch, the receptionist, and the
                         #   Event enum (§3, §4, §11 to §14, §16)
actor-serialization/    # SerializationRequirement and the pluggable wire Codec (§5)
actor-cluster/          # ClusterSystem: transport, membership control planes, SWIM; forwards
                         #   the Watch/Terminated/Receptionist frames that carry death watch and
                         #   receptionist replication across nodes (§7 to §10, §12, §13).
                         #   The reference ActorSystem, and the §19 utilities layered on it
                         #   (placement.rs, router.rs, singleton.rs)
actor-runtime/          # Production seam: wall-clock Clock, OS-seeded Entropy, multi-thread
                         #   Spawner, mutual-TLS TCP Transport over length-delimited framing
                         #   (transport.rs, wire.rs, association.rs; §4.6, §7, §15); a voter's
                         #   durable Raft state (storage.rs: FileRaftWAL, formats actor.raft.log,
                         #   actor.raft.term, actor.raft.snapshot; §9.4.3); and node bring-up
                         #   and departure (join.rs, lifecycle.rs; §9.3)
actor-simulation/       # TEST-ONLY. Virtual Clock/Entropy/Spawner + in-memory Transport
                         #   and registry, the deterministic simulator, fault injection,
                         #   invariant checkers (§18)
```

The runtime-agnostic crates (`actor-core`, `actor-cluster`) take their `Clock`/`Entropy`/`Spawner`/`Transport` from a seam (§4.6, §7); `actor-runtime` supplies the production implementations and `actor-simulation` the virtual ones. No macro crate is required; any `#[derive(Message)]` or `#[derive(RemoteActor)]` (§4.4) is optional (§1.1).
