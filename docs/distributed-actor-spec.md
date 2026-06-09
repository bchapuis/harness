# Distributed Actor Framework for Rust: Specification

**Status:** Draft v5
**Scope:** A location-transparent, fault-tolerant distributed actor system for Rust, with a **message-first** API.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Throughout, `actor` stands in as the crate and namespace name. Async trait methods are shown as `async fn` for readability; the framework writes them `fn … -> impl Future<Output = …> + Send` so generic runtime code can require the returned future to be `Send`.

> **Design stance.** The framework uses only ordinary Rust traits and generics, and it ships no macros of its own. Actors exchange **messages**, serializable value types, and implement one `Handler<M>` per message type they accept. Each message carries a hand-written `const` wire identity, its *manifest*; each actor lists the messages it accepts over the network in a hand-written `register` function (§4.4). In user code, serde's `Serialize` and `Deserialize` are the only macros.
>
> This trade-off is deliberate, not a free win. Hand-written manifests and `register` lists keep the *framework* simple and the wire contract explicit, inspectable, and versionable; they cost the *user* mechanical boilerplate: one `const` per message, one `accept` line per remote message. Because that boilerplate is derivable, an **optional** derive MAY generate both (§4.4). The goal is *no required codegen* (§1.1), not *no codegen permitted*. The hand-written form is normative, and it remains the override path for anyone who must pin a manifest by hand.

---

## 1. Design goals and non-goals

### 1.1 Goals
- **Location transparency.** Sending a message to a local actor and sending one to a remote actor are the same call. The runtime, not the source, decides whether the target is local or on another node.
- **Isolation by construction.** Actors never share state, and the type system forbids concurrent access to it.
- **Explicit wire contract.** Every cross-node payload is a named, serializable message type. You can version, log, persist, and inspect the protocol; it is no implicit side effect of a method signature.
- **Robustness.** Node and actor failure are first-class, observable events. The system tolerates partial failure and network partitions, and it never drops a request without reporting an error.
- **Pluggability.** Serialization, transport, and the actor system itself are traits. The cluster runtime is one implementation, not the only one.
- **No required codegen.** The whole framework is plain generic code; serde's derives are the only macros it relies on. An optional derive that defaults a message's manifest or an actor's `register` list (§4.4) is a permitted convenience: it lowers user boilerplate without becoming part of the model. The framework excludes only *required* codegen: nothing it mandates may need a macro.

### 1.2 Non-goals
- **Generic message handlers over the wire.** A message that crosses the wire MUST be a concrete type; Rust monomorphizes, and there is no runtime type to send. Generic actors are fine, but a *message* and its *reply* MUST be concrete, serializable types.
- **Transparent failure masking.** The framework MUST NOT auto-retry messages with side effects. Retries and idempotency are the caller's decision.
- **Strong consistency in the data plane.** Messaging, death watch, and the receptionist are eventually consistent in every mode. The **leader-based** control plane (§9.4.3) self-hosts consensus for membership and control-plane metadata only; no mode places a quorum on the message path or makes delivery transactional.
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
    /// registers nothing — a purely local actor; a remote one overrides it.
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
3a. `M::MANIFEST` MUST be a stable, author-chosen identity, unique among the message types a given actor accepts (§4.4). A local-only actor still declares it, but at a cost of one `const` line and no registration; the local fast path (§4.3) never reads it.
4. **Application errors live in the reply.** A handler that can fail uses `type Reply = Result<T, E>` where `T, E: SerializationRequirement`. An application failure is a value, distinct from a transport failure (`CallError`, §14).
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

    /// Fire-and-forget. Errors only for enqueue/transport failure, not handler outcome.
    pub async fn tell<M>(&self, msg: M) -> Result<(), CallError>
    where A: Handler<M>, M: Message;

    /// Same as `ask` but with an explicit deadline overriding the system default.
    pub async fn ask_timeout<M>(&self, msg: M, within: Duration) -> Result<M::Reply, CallError>
    where A: Handler<M>, M: Message;
}
```

- The `A: Handler<M>` bound is the dispatch mechanism: it proves at compile time that `A` accepts `M`, so invalid sends do not compile and no runtime check is needed.
- `ask` and `tell` are **identical** for local and remote targets. The system decides at call time whether to enqueue locally or send over a transport (§4.4).
- An `ActorRef` MAY be a field of a message or of an `M::Reply`. On the wire only the `ActorId` travels; the receiving node rebinds it to its own system on decode, yielding a working local-or-remote `ActorRef` there (§4.3).
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
    fn unwatch<B: Actor>(&self, target: &ActorRef<B>);
    fn stop(&self);                                                                   // stop self after current message
}
```

### 3.5 Isolation guarantees

Ownership enforces actor isolation:

1. An actor value is **owned** by its mailbox task and reachable through no other path. No API returns `&A` or `&mut A` to a caller.
2. All state access happens inside the actor's own handlers, which its serial executor runs. So `&mut self` in `handle` needs no locking and admits no data races.
3. An `ActorRef` is the only object held externally, and it is stateless. Sharing one across threads or nodes is always safe.

#### 3.5.1 Local-only access (`when_local`)

For testing and local optimizations, a system MAY offer:

```rust
impl<A: Actor> ActorRef<A> {
    pub async fn when_local<F, R>(&self, f: F) -> Option<R>
    where F: FnOnce(&mut A) -> R + Send;   // runs on the actor's executor iff local
}
```

`when_local` MUST run `f` on the actor's serial executor, preserving isolation, and MUST return `None` if the actor is remote. It is the only sanctioned exception to location transparency, and it SHOULD be limited to tests.

### 3.6 Identity (`ActorId`)

An `ActorId` is a single cluster-wide identity type, shared by every system:

```
ActorId = {
    node:        NodeId,        // cluster node identity (uid + endpoint)
    path:        Path,          // hierarchical name, e.g. /user/greeter
    incarnation: u64,           // monotonic per path on the owning node
}
```

It is `Clone + Eq + Hash + Send + Sync + Serialize + DeserializeOwned`, and it MUST:
- Be unique within a cluster for the cluster's lifetime.
- Let any node identify the target's **owning node** (the `node` field) and classify it as local or remote, from the id alone, without a network round-trip (the capability `resolve` depends on, §4.3).
- Carry an **incarnation** that distinguishes a fresh actor from a resigned one that reused the same `path`.

The struct is closed and self-describing, so any id that deserializes is well-formed and locality-classifiable with no routing-table lookup — which keeps `ActorRef<A>`, the wire envelope, and `ActorRef` rebinding (§4.4) simple. A few paths are **well-known** (for example, `/system/receptionist`) and resolvable on every node without prior introduction (§13).

---

## 4. The `ActorSystem` contract

The system is the runtime an actor runs on. The cluster runtime (§9 to §13) is one implementation. The transport-facing methods work on **already-serialized payloads**; the typed API and the local fast path live in the `ActorRef`/mailbox layer above this trait.

### 4.1 The trait

A system resolves ids to refs and bridges the transport boundary, working on already-serialized payloads (the typed API and the local fast path live in the `ActorRef`/mailbox layer above):

```rust
pub trait ActorSystem: Send + Sync + 'static {
    // ---- Resolution (§4.3) ----
    /// Build a typed handle to `id`. Infallible: every `ActorId` is well-formed
    /// and locality-classifiable (§3.6), so resolution never fails.
    fn resolve<A: Actor<System = Self>>(&self, id: ActorId) -> ActorRef<A>;

    // ---- Outbound, transport boundary (§4.4) ----
    async fn remote_ask(
        &self, recipient: &ActorId, manifest: &'static str, payload: Vec<u8>, within: Duration,
    ) -> Result<Vec<u8>, CallError>;

    async fn remote_tell(
        &self, recipient: &ActorId, manifest: &'static str, payload: Vec<u8>,
    ) -> Result<(), CallError>;
}
```

The wire identity is the message's `MANIFEST` (§3.2) as a `&'static str`, and the payload is its codec-encoded bytes. The system also performs internal operations the contract orchestrates but does not expose as separately-called API: the lifecycle steps `assign_id` / `actor_ready` / `resign_id` (§4.2), and the inbound `deliver`, which the node's receive loop invokes to dispatch a decoded message to a local actor (§4.4).

The user-facing entry point is `spawn`. It composes the lifecycle steps in the order §4.2 specifies:

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
- `assign_id` runs exactly once before any message can be delivered.
- `actor_ready` runs exactly once; after it returns, `resolve(id)` succeeds and the mailbox accepts messages.
- `resign_id` runs exactly once, even when spawn fails between steps 1 and 3, in which case it follows `assign_id` directly.
- Between `assign_id` and `actor_ready` the id is reserved but **not** deliverable; the system MUST dead-letter or buffer messages to it, never silently accept them.

### 4.3 Resolution: local vs remote

`resolve(id)` returns an `ActorRef` and classifies the target without any network round-trip:

- The id's owning node is the local node and a live mailbox exists → a **local** `ActorRef` (fast path: messages enqueue directly, by value, without serialization).
- The id's owning node is the local node but no live mailbox exists → an `ActorRef` that dead-letters (the actor has resigned).
- The id's owning node is remote → a **remote** `ActorRef` (messages serialize and route through a transport).

`resolve` MUST NOT block or contact the remote node to check existence. The system finds liveness when a message is sent, or through failure detection (§10). It is **infallible**: every `ActorId` is a well-formed, locality-classifiable struct (§3.6), so there is no failure case. Malformed network input never reaches `resolve` — it is rejected earlier, at the codec and the dispatch allowlist (§5, §15), which never construct an unknown type from untrusted bytes.

### 4.4 Manifests, dispatch, and message flow

Every message type carries a stable **manifest** (`Message::MANIFEST`, §3.2): its wire identity and its dispatch key. Each message has exactly one such identifier, and the author controls it.

- The manifest MUST stay stable across recompiles and renames. An explicit string such as `"myapp.Greet"` is RECOMMENDED. A breaking change to the message's shape SHOULD become a new message type with a new manifest, rather than a silent redefinition of an existing one.
- The **dispatch registry** maps `(actor type, manifest) → typed dispatch entry`. A dispatch entry knows how to deserialize `M` from a payload, enqueue `Handler::<M>::handle` on the resolved local actor's executor, and serialize `M::Reply`.

**Registration.** An actor that can be addressed remotely lists the messages it accepts over the network in `Actor::register` (§3.1.1) — the defaulted method whose default registers nothing. Each `r.accept::<M>()` call is an ordinary generic function that captures the monomorphized dispatch entry for `(Self, M)`:

```rust
pub struct HandlerRegistry<A: Actor> { /* … */ }
impl<A: Actor> HandlerRegistry<A> {
    pub fn accept<M>(&mut self) where A: Handler<M>, M: Message;  // generic library fn, no codegen
}
```

`register` is a defaulted method on `Actor` so that `spawn`, which is generic over `A: Actor`, always has an `A::register` to call: a local actor inherits the empty default, a remote one overrides it. One generic spawn path serves both kinds of actor.

- `spawn` (§4.1) calls `A::register` the first time it spawns an actor of type `A`, filling the registry before any message can arrive — no separate setup step, no link-time collection.
- Registration is **inbound-remote only**: a purely local actor (never registered, never sent across nodes) keeps the empty default, and its messages flow by value (§4.3).
- A network-delivered message whose `(actor type, manifest)` is not registered MUST yield `CallError::Unhandled`. The registry is also the deserialization allowlist (§5, §15): only listed message types are ever built from network bytes.

A message's `MANIFEST` and an actor's `register` body are mechanical, so an optional derive MAY generate them — `#[derive(Message)]` defaulting the manifest from the type path, `#[derive(RemoteActor)]` emitting one `accept::<M>()` per `Handler<M>` impl. Such a derive is a convenience layered *above* the model: an implementation MUST work with hand-written manifests and `register` lists, and a hand-written manifest MUST override a derived default (the no-codegen goal, §1.1).

**Outbound, `ActorRef::ask<M>` (typed layer above the trait):**
1. Resolve the locality of `self.id`.
2. **Local:** enqueue `msg` *by value* on the mailbox; await the typed reply. No serialization occurs.
3. **Remote:** `payload = codec.serialize(&msg)`; `bytes = system.remote_ask(&self.id, M::MANIFEST, payload, deadline).await?`; return `codec.deserialize::<M::Reply>(&bytes)?`.

`tell<M>` is identical but expects no reply (`remote_tell`); for a local target it enqueues without a reply channel.

**Inbound, system receive loop (remote path):**
1. Decode the envelope header → `recipient: ActorId`, `manifest: Manifest`, optional `correlation`.
2. `resolve(recipient)`; if no live local actor → reply `CallError::DeadLetter`.
3. `system.deliver(recipient, manifest, payload, reply)`, which:
   a. Looks up `(type of the resolved actor, manifest)` in the dispatch registry; if absent → `CallError::Unhandled`.
   b. Deserializes `M` from `payload`.
   c. Enqueues `Handler::<M>::handle` on the actor's serial executor and awaits its completion.
   d. Routes the reply through the `ReplyHandle`.

`ActorRef` values inside a message or reply carry only their `ActorId` on the wire, and the receiving system rebinds them on decode (§4.3).

### 4.5 Reply handling

```rust
pub struct ReplyHandle { /* opaque; holds correlation + reply channel */ }

impl ReplyHandle {
    pub fn send<R: SerializationRequirement>(self, reply: R);  // serialize + return to caller
    pub fn fail(self, failure: CallError);                     // transport/system-level failure
    pub fn none(self);                                         // for `tell`: no reply expected
}
```

These are synchronous: each serializes its outcome and hands it to the correlation channel, which applies no backpressure to the handler (the backpressure that does exist — mailbox enqueue (§6) and `Terminated` delivery (§12) — lives on those paths). `deliver` MUST resolve exactly one of them per `ask`. Because application errors live inside `M::Reply` (§3.2), `send` carries both successful and application-failed outcomes; `fail` is reserved for transport or system failures the handler never produced.

### 4.6 Runtime environment (clock, randomness, concurrency)

The runtime needs three capabilities from its environment: time, randomness, and task spawning. A system MUST obtain each one through a trait, and MUST NOT read it from the host directly. This is what lets a system run under deterministic simulation (§18); each capability is one ordinary trait.

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

/// Task spawning. The mailbox executors, gossip, the failure detector, the
/// registry sync loop, and consensus tasks (§9.4) run through this.
pub trait Spawner: Send + Sync + 'static {
    /// Named `launch`, not `spawn`, so a raw task is never confused with
    /// spawning an actor (`ActorSystem::spawn` / `Ctx::spawn`, §3.4, §4.1).
    fn launch(&self, task: BoxFuture<'static, ()>);
}
```

Rules (MUST):
1. **All timing** MUST come from `Clock`: `ask` deadlines (§14.2), SWIM intervals (§10), gossip periods (§9.4.4), registry sync intervals (§9.4.2), Raft election and heartbeat timers (§9.4.3), supervision backoff (§11.2). No subsystem reads the wall clock or a host timer directly.
2. **All randomness** MUST come from `Entropy`: gossip peer selection (§9.4.4), SWIM's `k` members (§10), Raft election-timeout jitter (§9.4.3), backoff jitter (§11.2), and minting a `NodeId` where the mode generates identity rather than configuring or persisting it — gossip-based startup, or an identity reset after `down` (§3.6, §9.1, §9.4).
3. **All background tasks** MUST be created through `Spawner`. The mailbox executor (§6) and the detector loops (§10) MUST NOT bind themselves to a specific async runtime.
4. **No observable nondeterminism.** Anything that crosses the wire or appears in §16 events MUST have a deterministic order. A system MUST NOT let unordered iteration (`HashMap` order, for instance) affect message ordering, peer selection, or reply timing.

The production runtime supplies a wall-clock `Clock`, an OS-seeded `Entropy`, and a multi-threaded `Spawner`. The simulator (§18) supplies virtual versions driven by a single seed, and no other code changes.

---

## 5. Serialization

`SerializationRequirement` is the bound every wire-crossing value must satisfy. It is a parameter of the system.

```rust
pub trait SerializationRequirement:
    Serialize + DeserializeOwned + Send + 'static {}
impl<T: Serialize + DeserializeOwned + Send + 'static> SerializationRequirement for T {}
```

Rules:
1. Every message type and reply type MUST satisfy `SerializationRequirement`. The `Message` and `Handler` bounds enforce this at compile time.
2. The concrete **codec** (`postcard`, `bincode`, CBOR, JSON, Protobuf, or another) is pluggable and fixed per system. Both ends of an association MUST agree on the codec.
3. The **manifest** (§4.4) identifies the concrete message type on the wire. The dispatch registry maps a manifest to a deserialize-and-dispatch entry. Decoding MUST reject unknown manifests (`CallError::Unhandled`) and MUST NOT build arbitrary types from untrusted input.
4. The system reports serialization failures as errors (`CallError::Serialization`); they MUST NOT panic the receive loop.

---

## 6. Execution model

1. **One mailbox per actor.** Each local actor has a bounded mailbox (a multi-producer, single-consumer queue) feeding exactly one executor task.
2. **Serial execution.** The executor processes one message to completion before the next. This is what makes `&mut self` sound (§3.5) and gives each actor a total order over the messages it observes.
3. **Cooperative async.** A handler MAY `.await`. While it is suspended, the executor MUST NOT begin another message for that actor (no reentrancy). Handlers SHOULD avoid blocking the thread, and SHOULD offload blocking work.
4. **Backpressure.** Mailboxes are bounded. When one is full, the system MUST apply a defined policy: `await` until space frees, or fail the send with `CallError::MailboxFull`. It MUST NOT drop messages silently. The two policies are separate calls: `tell` awaits, `try_tell` returns `CallError::MailboxFull` at once (local target).
5. **Ordering guarantee.** Messages from a single sender to a single recipient arrive in send order (FIFO per directed pair). The system guarantees no ordering across different senders. `tell` and `ask` from the same sender share this order.
6. **Fairness.** The runtime SHOULD schedule executors fairly so that no ready actor starves.

---

## 7. Transport

A transport is pluggable behind a trait; the default is TCP with length-delimited framing. Requirements:

1. **Associations.** Before exchanging actor traffic, two nodes MUST complete a handshake that establishes an association: a protocol-version check, a node-identity exchange, codec agreement, and optional authentication (§15). The nodes send actor envelopes only over an established association.
2. **Multiplexing.** Many actor conversations share one association. Each request carries a correlation id, and each response references it.
3. **Framing.** Messages are length-delimited; a malformed frame MUST tear down the association, not the node.
4. **Failure detection.** The transport never decides liveness. Wherever a detector runs (§9.4), SWIM (§10) probes members independently of connection state, so a node whose association merely blipped is never mistaken for a failed one; in static mode without its optional detector (§9.4.1), liveness surfaces only as failed sends (§8). A transport is not required to report association loss. It MAY surface establishment/loss as an *optimization hint* to speed detection (e.g. mark a peer `suspect` early), but such a hint MUST feed only the detector's refutable `suspect` state, never the terminal `down` decision (§9.4).

Like `ActorSystem` (§4.1), the transport is a trait, so the default TCP transport and the simulator's in-memory network (§18) are two implementations of one trait, indistinguishable from above. It carries outbound frames and releases its resources on stop; inbound frames are delivered out of band, through a channel the transport hands to the system's receive loop at construction (this avoids returning a single-consumer stream from `&self`):

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

`connect` is implicit — the first `send` to a peer dials lazily and runs the §7.1 handshake — so there is no separate `connect` method.

### 7.1 Wire protocol

Two message families travel over an association: **actor envelopes** and **system messages**.

```
Envelope {
    recipient:   ActorId,
    manifest:    String,           // message type id; the dispatch key (§4.4). The sender's
                                   //   `&'static str` const becomes an owned String on the wire.
    correlation: Option<CallId>,   // Some ⇒ a reply is expected (ask); None ⇒ one-way (tell)
    payload:     Vec<u8>,          // encoded message
}

Reply {
    correlation: CallId,
    outcome:     Ok(Vec<u8>) | Failure(CallError),   // application errors are inside Ok(..) as M::Reply
}

SystemMessage =
    | Handshake(..) | HandshakeAck(..)
    | Membership(..)                      // §9.4, mode-specific: gossip digests, Raft
                                          //   replication/votes, stamped registry entries
    | Swim(Ping | Ack | PingReq | ..)     // §10
    | Watch(ActorId) | Unwatch(ActorId) | Terminated(ActorId, TerminationReason)  // §12
    | Receptionist(..)                    // §13
```

### 7.2 Delivery guarantees

- **At-most-once** per call attempt. The framework MUST NOT transparently retransmit a delivered message.
- **No silent loss.** Every `ask` MUST terminate in a reply, a `CallError::Timeout`, or a `CallError::Unreachable`. A pending ask whose target node is declared `down` (§9.4) MUST complete with `Unreachable`; one whose target is confirmed `unreachable` (§10) MAY complete with `Unreachable` early — at-most-once delivery (above) makes that safe even if the node recovers; failing both, the ask's deadline (§14.2) bounds it.
- **Ordering** per directed sender→recipient pair, as in §6, holds end-to-end over a single association.
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

Declaring a node `down` is one event whose consequences belong to six different subsystems. The detail lives with each owner; this is the single trace that ties them together. When node `N` is declared `down`:

1. **Detection then decision (§10, §9.4).** A SWIM suspicion (`suspect`) that goes unrefuted for `T_suspect` confirms `N` `unreachable`; the mode's **down authority** then makes the terminal decision — the coordinator applying the downing policy (gossip-based), the leader committing the transition by quorum (leader-based), or the operator/platform removing `N` from the registry (dynamic). `suspect` and `unreachable` are the detector's refutable states; `down` is the cluster's terminal decision. (Static mode has no down authority; this cascade never runs there, §9.4.1.)
2. **Membership (§9.1).** The transition propagates through the mode's dissemination channel (§9.4); `down` is **terminal**. `N` MUST NOT reappear `up` under the same incarnation, and it MUST restart with a new `NodeId` to rejoin. The entry is later tombstoned (`removed`).
3. **In-flight callers (§7.2, §14).** Every pending `ask` whose target is on `N` completes with `CallError::Unreachable`; it never hangs (invariant §18.5 #2). This is the guarantee plain request/response cannot give.
4. **Watchers (§12).** No stop message can arrive from a dead node, so each node, on observing the `down` transition in its membership view (§9.2), **synthesizes** a `Terminated { reason: NodeDown }` for every locally-watched actor on `N` and delivers it into each watcher's mailbox in serial order.
5. **Receptionist (§13).** Because the receptionist watches the actors it lists, step 4 drives it: it prunes every registration originating from `N`, and subscribers receive a fresh `Listing`.
6. **Routing afterward (§4.3).** `resolve` of any `ActorId` on `N` still returns a (remote) `ActorRef`; sends to it fail with `Unreachable` rather than blocking, because liveness is discovered on send, not in `resolve`.

A graceful **leave** (§9.3) reaches the same terminal `down`/`removed` state and the same steps 3 to 6, differing only in step 1: the node announces `leaving` and drains instead of being suspected. An operator **decommission** — removing the node from the registry (dynamic) or committing a removal entry (leader-based) — reaches it the same way, also differing only in step 1: the control plane declares `down` directly, with no detector involved. (A **drain** is *not* this cascade — it is reversible and leaves the node a member; only decommission/removal declares `down`, §9.4.)

---

## 9. Cluster membership and node lifecycle

Nodes share a view of cluster membership. The member-state lattice (§9.1), the view-merge rule (§9.2), and the lifecycle steps (§9.3) are common to every cluster. Who *decides* the member set — the **control plane** — is a configurable **mode** (§9.4): a fixed local configuration (**static**), an external registry (**dynamic**), a self-hosted consensus log (**leader-based**), or peer-to-peer dissemination (**gossip-based**).

### 9.1 Member states

```
joining → up ──→ leaving ──→ down ──→ removed     (the monotonic ladder)
           ⇅        ▲           ▲
        draining ───┴───────────┘                 (reversible cordon, off the ladder)

reachability: reachable ⇄ unreachable             (orthogonal to every state above)
```

- **joining**: handshake complete, not yet admitted to full participation. Used by the modes where admission is an in-cluster decision (leader-based and gossip-based, §9.4); static starts every member `up`, and in dynamic mode admission *is* the registry entry (§9.4.2).
- **up**: full member; may host and address actors.
- **draining** *(modes with an authoritative control plane — dynamic and leader-based, §9.4)*: a **reversible** maintenance cordon. The node stays a full member, but service discovery routes new work away from it; `resume` returns it to `up`. It sits **off** the monotonic ladder — it is not terminal and never advances toward `removed` on its own — so transitions in and out of it are ordered by the control plane's authority stamp rather than by rank (§9.2).
- **leaving**: graceful shutdown initiated; draining its mailboxes before departure.
- **down**: declared dead by the mode's down authority (§9.4), or reached by completing a graceful leave. **Terminal and irrevocable**: a node that was `down` MUST NOT rejoin under the same incarnation; it MUST restart with a new `NodeId`. This coexists with the stable identities the static, dynamic, and leader-based modes require (§9.4): stable identity is *persisted* identity, surviving ordinary restarts; rejoining after `down` is a deliberate identity reset — wipe the persisted identity, join as a new member.
- **removed**: tombstone, eventually pruned.

**Reachability** (`reachable`/`unreachable`) is an orthogonal flag the failure detector (§10) maintains in every mode that runs one. A node is first marked `suspect` (a refutable suspicion); a suspicion unrefuted for `T_suspect` is confirmed `unreachable`. Both are detector observations, reversible by a higher incarnation; `down` is the separate, terminal cluster decision — and *which authority may make it* is precisely what the mode chooses (§9.4).

### 9.2 Views and the merge rule

Every node holds a local **membership view**: one entry per member, `(NodeId, state, incarnation, stamp)`. Views update through the mode's dissemination channel (§9.4) and merge entry-wise by one rule, identical in every mode:

1. **Authority stamp wins.** An entry carrying a higher **authority stamp** — the registry revision (dynamic, §9.4.2) or the log commit index (leader-based, §9.4.3) — replaces an entry with a lower or absent stamp. A mode's stamps come from a single logical writer, so they totally order its decisions; that order, not state rank, is what lets a *reversible* transition (`up ⇄ draining`) converge.
2. **Otherwise, the lattice.** The higher incarnation wins; within one incarnation, the higher-ranked state on the monotonic ladder `joining < up < leaving < down < removed` wins. `up` is monotonic and `down` is terminal, so independently-reached views merge without conflict.
3. **`down` is absorbing.** No merge result — stamped or not — returns a `down` member to a live state under the same incarnation. An authority that wants the machine back admits it as a *new* member with a fresh `NodeId`.

The reachability axis (§9.1) is not part of this merge: suspicion and refutation order by incarnation inside the detector (§10). In **static** and **gossip-based** mode no stamp is ever issued, so the merge reduces exactly to the lattice.

### 9.3 Joining and leaving

The mechanics are common to every mode: a joiner handshakes an existing member (a configured seed, or one learned from the registry); a leaver announces `leaving` and drains its mailboxes; a crash is confirmed `unreachable` by the detector, where one runs (§10). But each transition that *changes the member set* needs an authority, and each mode names a different one (§9.4):

| Who decides | **Static** | **Dynamic** | **Leader-based** | **Gossip-based** |
|---|---|---|---|---|
| Admission to `up` | nobody — roster fixed, all start `up` | appearing in the registry, at that revision | the leader commits `joining → up` | the coordinator, on convergence |
| Finalizing a leave (`→ down`) | n/a — members never leave | deregistration: whoever registered the node removes its entry | the leader commits, at the departing node's request | the coordinator finalizes `leaving → down` |
| A crash | nothing — sends just fail (§8) | a registry mutation, or nothing | the leader MAY commit `down` per the downing policy | the coordinator applies the downing policy |

The `leaving` announcement itself decides nothing: it starts the drain and advises peers, while the mode's authority makes the membership change. Watchers (§12) of a departed node's actors are notified through the node-down cascade (§8.1) in every mode that reaches `down`.

### 9.4 Membership control plane (modes)

A membership control plane must answer three questions: **where does the authoritative member set live**, **how do members learn it**, and **who may declare a member dead**. Each mode is one consistent set of answers; everything else — the lattice, the merge rule, the lifecycle, terminal `down` — is shared (§9.1–§9.3). The mode is one choice at node startup; every node in a cluster MUST run the same mode.

| | **Static** | **Dynamic** (registry) | **Leader-based** (consensus) | **Gossip-based** |
|---|---|---|---|---|
| Authoritative member set | local configuration, fixed at deployment | an external registry (platform API, database, coordination service) | a self-hosted replicated log (Raft) | none — the converged peer-to-peer view |
| Runtime membership change | none (redeploy/restart) | operator or platform mutates the registry | command committed through the elected leader | self-organizing (seeds + gossip) |
| Dissemination | none | each node syncs against the registry | log replication, plus gossip of committed state | infection-style gossip |
| View consistency | trivial (fixed) | delegated to the registry; members converge on its latest revision | strong at the log; members converge on the commit order | eventually consistent |
| Failure detection (§10) | none (MAY enable, observe-only) | SWIM, **observe-only** | SWIM as a sensor feeding the leader | full SWIM |
| Declares `down` | nobody | the operator/platform, via the registry | the leader, by quorum-committed entry | the coordinator, under the downing policy |
| `draining` cordon (§9.1) | — | yes (registry state) | yes (committed command) | — |
| External dependency | none | the registry | none | none |
| Sweet spot | embedded, edge, appliances, small fixed clusters | cloud-native, where an orchestrator or managed store already exists | self-contained systems needing authoritative placement and metadata | large or elastic clusters with high churn |

#### 9.4.1 Static

Membership and configuration are fixed at deployment time. Every node reads the full topology from its local configuration, all members start `up`, and no lifecycle transition ever occurs at runtime; changing the topology is a redeploy or restart. By default no detector runs and no membership traffic flows — zero runtime coordination, fully deterministic behavior — and no downing exists in any configuration.

- Node identity MUST be stable across restarts and derivable from configuration, so a restarted node resumes its place in the roster. `ActorId`s minted by its previous run still resolve to it and dead-letter (§4.3), like any resigned actor's id.
- A node that stops answering remains a member: sends to it fail with `Unreachable` or `Timeout` (§8), no `Terminated` is ever synthesized for its actors (§12), and the node-down cascade (§8.1) never runs.
- A deployment MAY enable the detector (§10) **observe-only**, trading background probe traffic for reachability: §16 events for operators, early `Unreachable` completion of pending `ask`s (§7.2), and discovery that routes around a dead peer (§13). Nothing else changes — there is still no down authority, so no `down`, no `NodeDown` (§12), no membership transition; the member set stays exactly the configured roster.

Best suited to embedded systems, edge deployments, appliances, tests, and small clusters where topology rarely changes.

#### 9.4.2 Dynamic (registry-based)

Membership changes at runtime, with the authoritative cluster state held in an **external registry** — a platform API, a database, or a coordination service — that the cluster reads but does not operate. An operator or platform adds and removes nodes by mutating the registry; every member runs a **sync loop** that watches (or polls) it and applies its state to the local view. The cluster delegates consistency to infrastructure that already provides it.

1. **Revisions.** The registry MUST expose a monotonic **revision** that totally orders its states (a `resourceVersion`, `modRevision`, version column, or equivalent). Each entry a node applies carries that revision as its authority stamp (§9.2), so members converge on the registry's latest state regardless of sync order — including the reversible `up ⇄ draining`. Members MAY additionally gossip stamped entries to one another; the merge makes that a safe accelerant, never a second authority.
2. **Join:** a node is admitted by appearing in the registry (registered by the platform or by itself, per deployment policy); members observing the new entry handshake it and mark it `up` at that revision. The `joining` state is unused: admission is the registry entry itself (§9.1).
3. **Leave:** a graceful leave is the same write in reverse — whoever registered the node removes its entry, and the removal's revision finalizes `down`/`removed`, running the node-down cascade (§8.1). The departing node's `leaving` announcement starts its drain but decides nothing (§9.3).
4. **Down:** only a registry mutation declares `down` — there is no other path. The detector (§10) is **observe-only**: a node that stops answering becomes `unreachable` — reversible — and stays a member until the operator or platform removes it, which moves it `down`/`removed` at that revision and runs the node-down cascade (§8.1). This is the Kubernetes node-lifecycle model: the control plane owns the desired set, workers report status, and a machine out for maintenance is not evicted.
5. **Drain / resume:** the reversible cordon (§9.1) is a registry state. A `draining` or `unreachable` member keeps its membership across a restart, so node identity MUST be stable across restarts in this mode.
6. **Registry unavailability** pauses membership *changes* only; the data plane keeps running on the last-synced view. A node MUST NOT treat its own inability to reach the registry as evidence about peer liveness.
7. **Simulation seam.** The registry client is a trait, like `Transport` (§7): production speaks to the real platform; the simulator supplies an in-memory registry with seeded latency, staleness, and unavailability (§18.2, §18.3).

Best suited to cloud-native deployments where a reliable store or orchestrator (e.g. Kubernetes, a managed database) is already available.

#### 9.4.3 Leader-based (consensus)

The cluster self-hosts its source of truth as a **replicated log** coordinated through the **Raft** protocol. An elected **leader** serializes all membership and control-plane state changes, and a **quorum of voters** guarantees strong consistency and safe failover with no external dependencies. This specification does not restate Raft; it requires the protocol's observable guarantees (election safety, log matching, leader completeness) of any implementation.

1. **Single writer, total order.** Every membership transition — `joining → up`, `drain`/`resume`, a graceful leave (committed at the departing node's request), anything `→ down` — MUST be a log entry committed through the current leader. The commit index is the authority stamp (§9.2): members apply committed entries in log order and converge on one sequence of membership states. A command offered to a non-leader is redirected or rejected, never applied.
2. **Voters.** A configured, modest subset of members (typically 3 or 5) are **voters**; only they elect leaders and form quorums. Voters MUST persist their Raft state (term, vote, log) durably and MUST keep stable node identities across restarts. Changes to the voter set are themselves committed configuration entries (single-server change or joint consensus). A voter declared `down` MUST be removed from the voter set by a committed change; its replacement joins as a new member with a fresh `NodeId` (§9.1).
3. **Dissemination.** Voters learn state from log replication. Non-voting members MAY replicate the log as learners or receive committed membership state by gossip stamped with the commit index; by the merge rule (§9.2) both paths converge on the same view.
4. **Downing.** The detector (§10) is a sensor feeding the leader: on a confirmed `unreachable`, the leader MAY commit `unreachable → down` under the configured downing policy. Downing is therefore **quorum-gated**: a leader that has lost quorum cannot commit, so a minority partition can never evict the majority (§18.5 #22). Raft's own heartbeats, which detect a failed leader for re-election, are internal to the consensus layer and distinct from member reachability.
5. **Quorum loss** pauses the control plane only — no join, drain, or down commits until a quorum reforms — while existing members keep exchanging actor traffic on the last committed view.
6. **Beyond membership.** The log MAY carry other control-plane decisions as opaque entries — singleton placement, shard allocation, cluster-wide configuration — giving them the same total order and failover. This specification normatively defines only the membership entries.
7. **Runtime seams.** Consensus traffic rides the ordinary `Transport` (§7) as system messages, election and heartbeat timers come from `Clock`, and election-timeout jitter from `Entropy` (§4.6), so a leader-based cluster simulates deterministically like everything else (§18).

Best suited to self-contained systems that need authoritative placement and metadata decisions, operating at a modest number of voting nodes.

#### 9.4.4 Gossip-based

Membership and state propagate peer-to-peer through the **SWIM** protocol and anti-entropy gossip. There is no central coordinator, no quorum, and no external authority: every node converges on an eventually consistent view, and per-node load stays constant as the cluster grows.

1. **Dissemination.** Each node periodically exchanges a membership digest with a peer chosen at random (via `Entropy`, §4.6) and merges by §9.2; updates also piggyback on detector traffic (§10) for infection-style spread.
2. **Refutation.** Entries carry the member's incarnation; a node that sees a stale suspicion or state about itself increments its incarnation and gossips the override (§10).
3. **Coordinator.** Transitions that need a single actor — admitting `joining → up`, finalizing `leaving → down` — are performed by the **coordinator**: a deterministic *role*, not an election — it falls to the lowest-address `up`, reachable member of each node's converged view. The coordinator acts only on a **locally stable, fully-reachable view** (every live member it can see is `reachable`), so it never transitions members while its own view is in flux. Two nodes that transiently both consider themselves coordinator cannot conflict: their decisions merge through the lattice, since `up` is monotonic and `down` terminal (§9.2).
4. **Downing.** The detector drives `suspect → unreachable`; the configured **downing policy** (manual, timeout-based, or quorum-heuristic) decides `unreachable → down`, applied by the coordinator. A partition leaves each side seeing the other `unreachable`; the default policy MUST be conservative and MUST NOT auto-`down` across a partition (§18.5 #16). (Timeout-driven downing is coordinator-applied but not gated on full reachability — the node being downed is unreachable by definition.)
5. **Join:** a new node handshakes a configured seed → `joining`; the coordinator admits it to `up` on convergence.

This mode chooses availability and partition tolerance over an instantaneously consistent view (§1.2): nodes may briefly disagree, and everything above membership — death watch (§12), the receptionist (§13) — is designed to tolerate that. Best suited to large or elastic clusters with high churn.

---

## 10. Failure detection (SWIM)

Every mode except **static** runs a SWIM-style detector on each node, over its associations; static MAY enable it too (§9.4.1). The detector is the cluster's **reachability sensor**: it maintains the `reachable`/`suspect`/`unreachable` axis (§9.1) identically wherever it runs. Its *authority* is what differs by mode: in **gossip-based** mode its confirmations feed the downing policy; in **leader-based** mode they feed the leader, which alone may commit `down`; in **dynamic** mode — and in **static** mode when enabled — it is **observe-only**: nothing it reports ever moves a member to `down` (§9.4).

1. **Direct probing.** Periodically (every `T_probe`), pick a member and send `Ping`; expect `Ack` within `T_rtt`.
2. **Indirect probing.** On a missed `Ack`, ask `k` random members to `PingReq` the target on the prober's behalf. If any relays an `Ack`, the target is alive.
3. **Suspicion.** If direct and indirect probes both fail, mark the target `suspect` and gossip the suspicion. A suspicion carries the suspected node's incarnation.
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
```

- The **default** directive MUST be `Stop`. For transient faults, `Restart` is usually the better choice.
- **Restart** MUST construct a fresh actor value (state is not preserved by default) while the actor keeps its `ActorId` and mailbox. Constructing a fresh value requires a *factory*: an actor spawned this way (`spawn_with` for a root, `Ctx::spawn_with` for a child) is restartable. An actor spawned **by value** (`spawn`/`Ctx::spawn`) consumes the only instance, so it cannot be reconstructed; for it a `Restart` directive MAY degrade to `Stop`. The safety property is unaffected either way — a fault is always contained (invariant #18); degradation only means the actor stops instead of restarting. Exceeding `max` restarts `within` the window MUST escalate to `Stop`.
- **Backoff** between restarts MUST be supported (exponential with jitter RECOMMENDED) to avoid hot-restart loops.
- A `restart` re-runs `A::started`; the prior value's `A::stopped` runs with `StopReason::Failed`.
- A per-actor **decider** produces the decision — `fn decide(Fault) -> SupervisionDirective` (`Fault` is a small `Copy` enum) — allowing a different directive per fault kind.

### 11.3 Scope
Supervision is a **local** mechanism: a node supervises only its own actors. Remote failures are not supervised; they surface to callers as `CallError` (§8) and to watchers as `Terminated` (§12).

---

## 12. Lifecycle monitoring (death watch)

Any actor MAY watch any other actor, local or remote, and learn when it terminates. This is the primary tool for building robust distributed protocols.

```rust
// via Ctx (§3.4):
fn watch<B: Actor>(&self, target: &ActorRef<B>);
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

`A::stopped` (§3.1.1) receives a `StopReason`, the local-only subset `{Stopped, Failed}`: an actor runs its own `stopped` hook only when it stops on its own node. `TerminationReason` is what a *watcher* observes and extends `StopReason` with `NodeDown`, the case where the actor's node died and no local `stopped` could run (§8.1). A watcher observes terminations by handling `Terminated` as a system signal delivered into its mailbox, through a `Handler<Terminated>` impl or a dedicated signal hook.

Guarantees (MUST):
1. After `watch(target)`, if the target terminates for **any** reason, the watcher receives exactly one `Terminated` for it, delivered into the watcher's mailbox.
2. **Node down implies termination.** When a node is declared `down` (§9.4), every watched actor on that node MUST yield a `Terminated { reason: NodeDown }` to its watchers, even though no explicit stop message can arrive. A crashed peer thus still notifies watchers, which plain request/response cannot do.
3. Watching an already-terminated actor MUST immediately yield `Terminated`.
4. Signals respect the per-actor serial order: a `Terminated` arrives through the mailbox like any other message, never out of band.

Guarantee 2 is relative to the cluster's `down` decisions, not to physical reality: an actor on a crashed node terminates *observably* only when some authority declares that node `down` (§9.4). In a mode whose authority never does — static always; dynamic until the registry removes the node — watchers learn nothing from the crash, and callers see `Unreachable` or `Timeout` instead. Choosing a control-plane mode therefore also chooses when death watch can fire for node failure.

Remote watch works by sending a `Watch(id)` system message to the target's node; that node tracks watchers and emits `Terminated` on stop, and the watcher's own node synthesizes `Terminated` when its membership view marks the target's node `down` (§8.1).

---

## 13. Receptionist (service discovery)

Actors are addressed by `ActorRef`, but a node needs a way to obtain the initial `ActorRef` for a remote service without hardcoding its `ActorId`. The **receptionist** is a well-known, cluster-replicated registry.

```rust
pub struct Key<A: Actor> { id: &'static str, _marker: PhantomData<A> }

impl Receptionist {
    fn register<A: Actor>(&self, key: Key<A>, who: &ActorRef<A>);
    fn lookup<A: Actor>(&self, key: Key<A>) -> Listing<A>;                // current snapshot
    fn subscribe<A: Actor>(&self, key: Key<A>) -> impl Stream<Item = Listing<A>>; // live updates
}
```

`lookup` is synchronous: the listing is replicated local state (requirement 2), so it is a snapshot read with nothing to await; `subscribe` is a stream of live updates.

Requirements:
1. The receptionist is a well-known actor (§3.6), resolvable on every node without prior introduction.
2. Registrations are **replicated** across the cluster and **eventually consistent**. A CRDT (an OR-Set keyed by registering node) is RECOMMENDED so that concurrent registrations merge without coordination.
3. When a node goes `down` (§9.4), the receptionist MUST prune all registrations originating from it, and subscribers MUST receive an updated `Listing`. (The receptionist watches registered actors to drive this.)
4. A `lookup` or `subscribe` listing MUST omit actors on a node that is not currently serving — one an operator has `draining` for maintenance (§9.4), or one that is `down`. Wherever a detector runs (§9.4), the listing SHOULD likewise omit actors on a node confirmed `unreachable`. Unlike a `down` node's registrations (pruned, requirement 3), a `draining` or `unreachable` node's registrations are **retained** and merely routed around — `resume` or recovered reachability restores them without re-registration. This is load-shedding, not removal: a drained node still answers a direct `ask`.
5. `subscribe` MUST deliver the current listing on subscription and a fresh listing on every change.
6. `Key` is typed by actor type, so `lookup` and `subscribe` return correctly typed `ActorRef`s.

---

## 14. Error model

### 14.1 `CallError`

`CallError` covers **transport and system** failures only: the failure to *complete* a call. Application failures the handler deliberately produced live inside `M::Reply` (§3.2).

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
- `CallError` variants MUST be exhaustive at the public API, so callers handle partial failure explicitly; the type system thus forces failure handling at every cross-actor boundary.

### 14.2 Principles
- Errors are **values**, propagated by `Result`; the framework does not use panics for control flow across actors.
- Supervision (§11) contains a panic inside a handler, and it never crosses the wire as a panic; it becomes `Terminated`/`Restart` locally and `Unreachable`/`DeadLetter` to in-flight callers.
- Timeouts are mandatory on `ask`: every request MUST carry an effective deadline, explicit or a system default.

### 14.3 Reading a call result

The two nested layers of §14.1 are distinct on purpose: the **outer** `CallError` (did the call *complete*?) and the **inner** application `E` (what did the handler decide?). The type MUST NOT collapse them, because a transport failure the caller may retry is not an application failure it must not.

The type stays two-level, but the *handling* need not be re-derived at every call site. A system SHOULD offer one canonical way to consume the common case, where a caller treats any failure uniformly:

```rust
// Convenience over `ask` for callers that want a single error channel.
// Available when the application error can absorb a transport failure.
impl<A: Actor> ActorRef<A> {
    pub async fn ask_flat<M, T, E>(&self, msg: M) -> Result<T, E>
    where A: Handler<M>, M: Message<Reply = Result<T, E>>, E: From<CallError>;
}
```

`ask_flat` collapses `Result<Result<T, E>, CallError>` into `Result<T, E>` by mapping a `CallError` through `E: From<CallError>`. Callers that must tell "did not complete" apart from "handler failed" keep using `ask`; callers that react to any failure the same way use `ask_flat`. Either way the two-level match is written once, here, not repeated per call site.

---

## 15. Security (RECOMMENDED)

1. **Transport security.** Associations SHOULD support mutual TLS, and node identity SHOULD bind to a verified certificate.
2. **Authentication.** The handshake SHOULD authenticate the peer and MAY enforce an allowlist of permitted node identities or clusters; a cluster secret prevents accidental cross-cluster association.
3. **Deserialization safety.** As in §5, the dispatch registry MUST instantiate only registered, allowlisted message types from network input. No path may lead from an incoming envelope to constructing an arbitrary type.
4. **Authorization.** A system MAY gate `deliver` per `(peer, actor, manifest)` through an optional `Authorizer` (`fn authorize(&self, peer: NodeId, recipient: &ActorId, manifest: &str) -> bool`), consulted before an envelope is dispatched. A denied message MUST be rejected as a system failure — never deserialized into the actor's type, so an unauthorized peer cannot trigger handler side effects. With no `Authorizer`, every message that clears the handshake is admitted.

---

## 16. Observability (RECOMMENDED)

A conforming system SHOULD expose:
- **Metrics:** mailbox depths, message and throughput rates, call latency, restart counts, association count, membership size, suspicion and down events.
- **Tracing:** propagate a trace/correlation context through envelopes, so a logical request can be followed across nodes.
- **Lifecycle logging:** spawn and resign, membership transitions, downing decisions, and supervision actions, each at a defined, filterable level.

The same events drive deterministic simulation: a simulator subscribes to this stream to check invariants (§18.5). A conforming system SHOULD emit, as structured events on a single (extensible) `Event` enum: `assign_id`/`actor_ready`/`resign_id` (§4.2), mailbox enqueue and dispatch (§6), every `ask` outcome (§14), membership and reachability transitions (§9 and §10), supervision decisions (§11), and `Terminated` deliveries (§12). Metrics and cross-node trace propagation are not yet provided (both RECOMMENDED); the event stream is the substrate they would build on.

---

## 17. Conformance

An implementation conforms to this specification **iff** every property below holds. This section is an index, not a restatement: the cited section's MUSTs *define* each property normatively, and the listed method *verifies* it: for runtime properties, the numbered invariant a simulator checks continuously (§18.5). The cited sections and the §18.5 catalogue are the single statements of each requirement; this table only points at them.

| Property | Defined in | Verified by |
|---|---|---|
| Actor state reachable only through the actor's own handlers | §3.5 | construction (ownership); §18.5 #4 |
| An invalid `ask`/`tell` does not compile | §3.3 | §18.5 #20 (compile-fail) |
| The call site is identical for local and remote targets | §3.3 | §18.5 #21 (differential) |
| Lifecycle `assign_id → actor_ready → resign_id`: ordering and exactly-once | §4.2 | §18.5 #6 |
| `resolve` classifies locality with no network round-trip | §4.3 | §18.5 #7 |
| Stable hand-written manifest; unregistered `(actor, manifest)` → `Unhandled` | §4.4 | §18.5 #8; construction |
| Remote dispatch listed by a hand-written `register` (a defaulted `Actor` method); no framework macro | §1.1, §4.4 | construction |
| Local sends enqueue by value, without serialization | §4.3, §4.4 | §18.5 #9 |
| `ActorRef`s in messages/replies are rebound on decode | §4.4 | §18.5 #10 |
| Every message and reply satisfies `SerializationRequirement` | §5 | compile-time (trait bound) |
| Serial execution, per-sender FIFO, bounded non-dropping mailbox | §6 | §18.5 #3, #4, #5 |
| Associations established by an authenticated, version-checked handshake | §7, §15 | construction; transport tests |
| At-most-once delivery; no `ask` completes silently | §7.2 | §18.5 #1, #2 |
| Membership converges under the mode's authority; `down` terminal | §9 | §18.5 #14, #15, #16, #17 |
| Leader-based control plane: quorum-gated, log-ordered transitions | §9.4.3 | §18.5 #22 |
| SWIM suspicion/refutation maintains reachability wherever the detector runs | §10 | §18.5 #17 (underlies #16) |
| Local faults contained by supervision; default `Stop`; restarts back off | §11 | §18.5 #18 |
| `watch` yields exactly-once `Terminated`, including `NodeDown` | §12 | §18.5 #11, #12, #13 |
| Receptionist: typed register/lookup/subscribe, pruned on node `down` | §13 | §18.5 #19 |
| Transport/system failures are exhaustive `CallError`; app errors in `M::Reply` | §14 | compile-time (exhaustive enum); §18.5 #1 |
| Runs over virtual clock/network/entropy; one seed reproduces a run; every property above holds under fault injection | §18 | §18.1; §18.5 under §18.3 |

---

## 18. Testability and deterministic simulation

A conforming implementation SHOULD be testable by **deterministic simulation**: a whole cluster runs in one process, on one logical thread, over virtual time, network, and randomness, so that a single seed reproduces an entire multi-node run, including its failures, exactly. This section is normative for the traits that make such testing possible, and it lists the invariants a simulator checks.

### 18.1 Determinism contract

A system that supports simulation MUST satisfy:

1. **Seed-reproducibility.** Given the same seed, configuration, and workload, two runs MUST produce byte-identical event streams (§16). All nondeterminism MUST pass through the `Clock`, `Entropy`, and `Spawner` traits (§4.6), the `Transport` trait (§7), and — in dynamic mode — the registry client (§9.4.2).
2. **Quiescence-driven time.** In simulation, logical time MUST advance only when no task is ready to run. A timeout, SWIM interval, or backoff therefore costs no wall-clock time, so a run can cover hours of cluster time per CPU-second.
3. **No ambient nondeterminism.** A simulation build MUST NOT read the wall clock, spawn OS threads, or use a non-seeded RNG. A single leak breaks reproducibility, so implementations SHOULD enforce this statically, for example with a lint that forbids the offending APIs.

### 18.2 Virtualized traits

Simulation reuses the traits the production runtime already uses; only the implementations differ:

| Trait | Production | Simulation |
|---|---|---|
| `Clock` (§4.6) | wall clock | logical clock; advances only at quiescence |
| `Entropy` (§4.6) | OS-seeded | one seeded PRNG, the only randomness in the run |
| `Spawner` (§4.6) | multi-thread runtime | single-thread cooperative scheduler |
| `Transport` (§7) | TCP | in-memory network with seeded latency / loss / reorder |
| registry client (§9.4.2) | platform API / database | in-memory registry with seeded latency / staleness / outage |
| codec (§5) | production codec | unchanged; runs real (de)serialization |

Because these are the *same* traits production uses, simulation runs the real `ActorSystem`, mailbox, membership, SWIM, supervision, and receptionist code, not a model of it. The codec stays real, so every cross-node hop tests the wire encoding.

### 18.3 Fault injection

Under seed control, a simulator MUST be able to inject at least:

- **Transport:** frame drop, duplication, delay, reordering; association loss.
- **Mailbox:** induced `MailboxFull`; maximal cross-sender reordering subject to per-sender FIFO (§6).
- **Scheduling:** seed-randomized selection among ready tasks.
- **Membership / control plane:** dropped or delayed pings, partitions, stale or replayed gossip, stale incarnations (§9 and §10); leader crashes and elections mid-transition (leader-based mode); a stalled, lagging, or unavailable registry sync (dynamic mode).
- **Supervision:** induced handler and `started()` faults (§11).
- **Nodes:** abrupt crash (no graceful leave) at an arbitrary step, which MUST surface as `Unreachable` or `Timeout` to in-flight callers (§7.2) and — in a mode whose authority declares `down` (§9.4) — as `NodeDown`/`Terminated` to watchers (§12).

A fault is realized by the mechanism that fits its layer, as long as the effect is exercised:
- **Frame corruption** is meaningful only where real bytes exist. The in-memory simulator carries *structured* frames (only the message payload is codec-encoded, §18.2), so it has nothing to bit-flip; the "malformed frame MUST tear down the association, not the node" requirement (§7) is exercised by the production transport's framing tests against real wire bytes. The simulator covers the observable consequence — a lost association — directly.
- **Stale / replayed gossip and stale incarnations** arise from applying drop / duplication / delay / reordering to the gossip-bearing frames; they need no separate injector because gossip rides the same faulted transport.
- **Induced handler / `started()` faults** are produced by workload actors that fault on demand (a handler that panics, a `started` that returns `Err`), which is how supervision (§11) is exercised, rather than by reaching into an arbitrary actor.

Each run SHOULD enable a random subset of faults at random intensities (sometimes called "swarm" testing); a run with no faults is the simplest case and MUST still pass.

### 18.4 Workloads

Tests are expressed as **workloads** over the cluster: a `setup` that builds actors and registrations, a `start` that drives traffic, and a `check` that asserts the invariants of §18.5. A workload MUST observe the cluster only through the public API and the §16 event stream, never through actor state directly (§3.5), except through `when_local` (§3.5.1) where explicitly intended.

### 18.5 Invariant catalogue

These invariants appear as MUSTs throughout this specification, and those inline MUSTs are their normative statements. Collected here, they are the contract a conforming implementation verifies, and the targets §17 checks against. Each MUST hold even under the faults of §18.3.

Verification is **layered**, not uniform (see §18.6). The core *safety* properties — those expressible as "a bad thing never happens" over the §16 event stream — are checked **continuously**, on every run and at final quiescence, by a small set of always-on checkers (today: #1 no-silent-loss, #4 serial execution, #6 lifecycle, #13 signal-in-band, #15 down-is-terminal). The rest are verified by the method that fits them: a *liveness* or scenario property by a targeted conformance test, type-safety (#20) by a compile-fail test, location transparency (#21) by a differential local-vs-remote run. Some safety MUSTs are not emergent over the event stream and so stay targeted by design — e.g. #5 (the mailbox bound is structural and backpressure is a per-call contract) and #11 (death-watch is exactly-once *per `watch`*, but the stream carries no per-`watch` identity, and re-watching a dead actor legitimately re-fires, §12). A machine-checked **catalogue** records, per invariant, which method applies; a drift test (`conformance_catalogue`) fails the build if a continuous checker and its catalogue entry disagree, so the §17 "Verified by" column stays mechanically true. Promoting a property from a targeted test to a continuous checker is always sound where it is a true safety invariant over the existing event stream.

1. **No silent loss (§7.2, §14).** Every `ask` issued terminates in exactly one of `Ok(reply)`, `Timeout`, `Unreachable`, `DeadLetter`, or `Unhandled`; at final quiescence no `ask` remains pending.
2. **Crash completes in-flight calls (§7.2, §9.4).** An `ask` whose target node is declared `down` completes with `Unreachable`; it never hangs. (Where no authority declares `down`, a confirmed `unreachable` MAY complete it early with `Unreachable`, and the deadline bounds it with `Timeout` regardless — #1 still holds.)
3. **Per-pair FIFO (§6).** Messages from one sender to one recipient are observed in send order, even under maximal reordering injection; no ordering is assumed across senders.
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
14. **Membership convergence (§9.2, §9.4).** Once faults cease and partitions heal, all `up` members converge on one membership set within bounded logical time — by anti-entropy (gossip-based), registry sync (dynamic), or log replication (leader-based).
15. **`down` is terminal (§9.1).** A node observed `down` never reappears `up` under the same incarnation.
16. **Partition tolerance (§9.4).** Under the default downing policy, a partition alone never moves a member to `down`, only to `unreachable`. **Dynamic** mode holds this unconditionally: its detector is observe-only, so only the registry ever declares `down`. **Leader-based** mode holds it for any side without quorum, which can commit no transition at all (#22).
17. **SWIM refutation (§10).** A node that sees itself suspected refutes via a higher incarnation, clearing the suspicion cluster-wide.
18. **Supervision containment (§11).** A handler panic never crashes the node; the default directive is `Stop`; restarts back off; exceeding `max` within the window escalates.
19. **Receptionist consistency (§13).** Registrations from a `down` node are pruned and subscribers notified; `subscribe` delivers the current snapshot first, then every change; concurrent registrations merge (eventual consistency).
20. **Type-safety (§3.3).** An `ask`/`tell` of a message an actor has no `Handler` for does not compile. (Compile-fail tests assert this, not the runtime; see §18.6.)
21. **Location transparency (§3.3).** Running the same workload with a target local versus remote produces observably identical replies and ordering. (Differential check.)
22. **Quorum-gated control plane (§9.4.3).** In leader-based mode, every membership transition is a quorum-committed log entry, applied in log order; at most one leader per term issues them, and a partition side lacking a quorum of voters commits none — a minority can never evict the majority.

### 18.6 Reproduction, layering, and CI

- **Reproduction.** A failing run MUST be replayable from its `(seed, configuration)` alone.
- **Layered checks.** Simulation covers the distributed invariants (1 to 19, 21, and 22). Compile-fail tests cover invariant 20: a compiler run that asserts the rejection of invalid sends. Because the simulator drives the mailbox and executor (§6) on a single-thread cooperative scheduler whose ready-task selection is seed-randomized (§18.3), it already explores interleavings deterministically and reproducibly; a separate `loom`/`kani` model-check of the executor across all interleavings is therefore an optional, complementary cross-check rather than a prerequisite.
- **Regression corpus.** A failure SHOULD be kept as a `(seed, configuration)` and replayed permanently. The fixed-seed swarm sweeps (the single-node and cluster swarm tests) serve as the standing corpus.
- **Continuous testing.** CI SHOULD run many seeds per change across different fault configurations; the coverage metric is cluster-hours exercised per change, not test count. (The reference CI runs the fixed-seed corpus on every change; per-run fresh-seed sweeps are a noted enhancement.)

---

## Appendix A: End-to-end example

```rust
// --- Define the actor; `register` lists the messages it accepts over the network ---
pub struct Greeter { greeting: String }
impl Actor for Greeter {
    type System = ClusterSystem;
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

// --- Host node ---
let system = ClusterSystem::start("node-a", config).await?;
let greeter = system.spawn(Greeter { greeting: "Hello".into() });
system.receptionist().register(GREETERS, &greeter);

// --- Client node (gossip-based mode, §9.4.4: joins via a seed; another mode
// ---  would find peers through its registry or configured roster instead) ---
let system = ClusterSystem::start("node-b", config.with_seed("node-a")).await?;
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
actor/                  # umbrella re-export
actor-core/             # Actor, Message, Handler, ActorRef, Ctx, ActorSystem,
                         #   HandlerRegistry, Manifest, CallError (§3, §4, §14)
actor-serialization/    # SerializationRequirement, dispatch registry, codecs (§5, §4.4)
actor-cluster/          # ClusterSystem: transport, membership control planes, SWIM, supervision,
                         #   death watch, receptionist (§7 to §13). The reference ActorSystem.
actor-runtime/          # Production seam: wall-clock Clock, OS-seeded Entropy,
                         #   multi-thread Spawner, mutual-TLS TCP Transport (§4.6, §7, §15)
actor-simulation/       # TEST-ONLY. Virtual Clock/Entropy/Spawner + in-memory Transport
                         #   and registry, the deterministic simulator, fault injection,
                         #   invariant checkers (§18)
```

Message identity is a `const`, remote dispatch is a hand-written `register` list (a defaulted `Actor` method, §4.4), and the call path is ordinary generic code in `actor-core`. The runtime-agnostic crates (`actor-core`, `actor-cluster`) take their `Clock`/`Entropy`/`Spawner`/`Transport` from a seam (§4.6, §7); `actor-runtime` supplies the production implementations and `actor-simulation` the virtual ones, so neither core nor cluster binds to a specific async runtime. No *required* macro crate exists; any `#[derive(Message)]` or `#[derive(RemoteActor)]` (§4.4) is an optional convenience layered above the model, not a dependency of it.
