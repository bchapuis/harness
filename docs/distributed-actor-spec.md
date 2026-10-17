# Distributed Actor Framework: Specification

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Async trait methods are shown as `async fn`; the framework writes them `fn … -> impl Future<Output = …> + Send` so generic code can require the future to be `Send`.

---

## 1. Scope

The framework is a location-transparent, fault-tolerant distributed actor system for Rust with a message-first API. Actors exchange **messages**, serializable values that each carry a hand-written wire identity (the **manifest**), and an actor implements one `Handler<M>` per message type it accepts. A send to a local actor and a send to a remote one are the same call (§3.2). Node and actor failures are explicit, observable events, and no request is dropped without an error (§4, §5). Serialization, transport, the membership control plane, consensus durability, and the actor system itself are traits, so a deployment or a simulator supplies its own implementation (§2). The framework is plain generic code: serde's derives are the only macros it relies on (§2.10).

It is the bottom layer of the tree and builds on no other specification except [compatibility](compatibility-spec.md) for its versioned formats. The [cluster utilities](cluster-utilities-spec.md) build on its membership view, receptionist, and event stream; the entity, workflow, and agent layers build on its `ActorSystem` (§2.8), its application consensus (§2.19), and its event stream (§2.7). A production node is assembled by `actor_runtime::join` from a `JoinOptions` (address plan, codec, cluster secret, encryption, membership mode), which starts the TCP transport and a `ClusterSystem` over it; the simulator assembles the same `ClusterSystem` over virtual host traits and an in-memory network (§2.12).

### 1.1 Non-goals

- **Generic messages over the wire.** Rust monomorphizes, so there is no runtime type to send: a message and its reply MUST be concrete, serializable types. Generic actors are fine.
- **Transparent failure masking.** The framework MUST NOT auto-retry messages with side effects; retries and idempotency are the caller's decision.
- **Strong consistency in the data plane.** Messaging, death watch, and the receptionist are eventually consistent in every mode; the leader-based mode (§6.4.3) runs consensus for control-plane state only, never on the message path.
- **Shared memory across nodes**, and **exactly-once or durable delivery**, which layers above build with idempotency keys (§4).

---

## 2. Seams

Each subsection names one seam: its public items, who implements and who consumes it, whether it is a plug point, and the obligations on each side. The seams application code uses come first (§2.1 to §2.7), then the seams a system is built from (§2.8 to §2.20).

### 2.1 Actors and messages

An application implements `Actor`, `Message`, and `Handler<M>`; the system (§2.8) runs them. This is the framework's main plug point.

```rust
pub trait Actor: Sized + Send + 'static {
    type System: ActorSystem;
    async fn started(&mut self, _ctx: &Ctx<Self>) -> Result<(), BoxError> { Ok(()) }
    async fn stopped(self, _reason: StopReason) {}
    fn register(_registry: &mut HandlerRegistry<Self>) {}
    fn supervision() -> Supervision { Supervision::stop() }
}

pub trait Message: SerializationRequirement {
    type Reply: SerializationRequirement;
    const MANIFEST: Manifest;
}

pub trait Handler<M: Message>: Actor {
    async fn handle(&mut self, msg: M, ctx: &Ctx<Self>) -> M::Reply;
}
```

`started` runs once before the first message, and an `Err` stops the actor with `StopReason::Failed`; `stopped` runs once when the actor stops, for any reason. `register` lists the messages the actor accepts over the network (§2.10); the default lists none, a purely local actor. A `Manifest` is a `const`-constructible newtype over `&'static str`, so one exists only for a statically declared identity.

Rules (MUST hold):
1. `handle` takes `&mut self`, which is sound because the executor is serial (§3.3).
2. `M` and `M::Reply` MUST satisfy `SerializationRequirement` (§2.11), checked at compile time.
3. `M` MUST be concrete at the point of the `Handler` impl (§1.1).
3a. `M::MANIFEST` MUST be a stable, author-chosen identity, unique among the message types a given actor accepts (§2.10). A local-only actor still declares it.
4. **Application errors live in the reply.** A handler that can fail uses `type Reply = Result<T, E>`: an application failure is a value, distinct from a failure to complete the call (`CallError`, §8).
5. An actor accepts exactly the `M` for which it implements `Handler<M>`. Anything else is a compile error at the call site, or `CallError::Unhandled` on the wire (§2.10).

**Isolation:** an actor value is owned by its executor and reachable through no other path; no API returns `&A` or `&mut A`. The only object held externally is the stateless `ActorRef` (§2.2).

### 2.2 Addressing

The system mints `ActorId`s and `ActorRef`s; application code holds and sends through them. They are one cluster-wide shape, not a plug point.

- `ActorRef<A>` offers `ask`, `ask_timeout`, `tell`, and `try_tell`, each bounded by `A: Handler<M>`, so an invalid send does not compile. It is `Clone + Serialize + DeserializeOwned + Send + Sync`, holds exactly an `ActorId` and a handle to the local system, and MUST NOT expose actor state or handlers. Equality and hashing derive from the `ActorId`.
- `ask` and `tell` are identical for local and remote targets; the system decides on each call whether to enqueue locally or send over a transport (§3.2).
- `ask` applies the framework's fixed default deadline, and `ask_timeout` an explicit one. `tell` errors only for a failure to enqueue or transmit, and awaits space in a full local mailbox. `try_tell` never awaits: it fails a full local mailbox with `MailboxFull` and, being local-only, a remote target with `Unreachable`.
- An `ActorRef` MAY travel inside a message or a reply. Only its `ActorId` travels, and the receiving system MUST rebind it on decode so it is usable there.

An `ActorId` is `{ node: NodeId, path: Path, incarnation: u64 }`, closed and self-describing; a `NodeId` is a bare uid, which the transport maps to an endpoint. An `ActorId` MUST be unique within a cluster for its lifetime; MUST let any node classify the target as local or remote from the id alone (§3.2); and MUST carry the owning **process** incarnation, so an id issued before a process restart under the same `NodeId` fails as `DeadLetter` instead of resolving to the actor the new process places at that path. Within one incarnation a node MUST NOT reuse a path. A deployment that restarts a node in place MUST give the new process an incarnation no predecessor used (`ClusterConfig::incarnation`, §2.15).

### 2.3 Handler context

Handlers and lifecycle hooks receive a `Ctx<A>`, which grants capabilities without exposing state: `id`, `this` (a self-reference), `system`, `spawn` and `spawn_with` (a child parented to this actor, §2.4; the factory form is restartable), `watch`, `watch_id`, and `unwatch` (§2.5), and `stop`, which stops the actor after the current message.

### 2.4 Supervision

An actor chooses a `Supervision`: `Supervision::stop`, `resume`, `restart`, or `with` a decider mapping each `Fault` (`Started`, `Message` for a handler panic, `Escalation` from a child) to a `SupervisionDirective` (`Stop`, `Restart { max, within, backoff }`, `Escalate`, `Resume`). The executor applies it; the decider is a plug point.

- Every actor except a root has a parent, the actor that spawned it; `Escalate` fails the parent, which applies its own strategy.
- The executor boundary catches faults: a panic MUST unwind into a supervision decision, never crash the node.
- The default directive MUST be `Stop`.
- `Restart` MUST construct a fresh actor value that keeps the `ActorId` and mailbox; the prior value's `stopped` runs with `StopReason::Failed`, then the fresh value's `started`. Only an actor spawned from a factory (`spawn_with`) can be re-created; for one spawned by value, `Restart` MAY degrade to `Stop`. Exceeding `max` restarts `within` the window MUST escalate to `Stop`.
- `Resume` drops the failed message and keeps the actor; for `Fault::Started`, which has no message to drop, it MUST degrade to `Stop`.
- Backoff between restarts MUST be supported (exponential with jitter RECOMMENDED), its delay and jitter drawn from the host environment (§2.12).
- The event stream MUST report the effective decision, after degradation and the restart window (§2.7).
- Supervision is local. Remote failures surface to callers as `CallError` (§5) and to watchers as `Terminated` (§2.5).

### 2.5 Death watch

A watcher calls `Ctx::watch` (or `watch_id`, for a bare id) and implements `Handler<Terminated>`; the system delivers `Terminated { id, reason }`, where `TerminationReason` is `Stopped`, `Failed`, or `NodeDown`. An actor's own `stopped` hook sees only `StopReason` (`Stopped`, `Failed`): `NodeDown` means its node died and no hook ran. `Terminated` has a framework `Message` impl (reserved manifest `actor.Terminated`) so the bound is well-formed, but it never travels in an envelope: the `Terminated` frame crosses the wire (§2.14), and the receiving node synthesizes the value.

Guarantees (MUST):
1. After `watch(target)`, if the target terminates for any reason, the watcher receives exactly one `Terminated` for it, in its mailbox.
2. **Node down implies termination.** When a node is declared `down` (§6.4), every watched actor on it MUST yield `Terminated { reason: NodeDown }`, synthesized by the watcher's own node.
3. Watching an already-terminated actor MUST immediately yield `Terminated`.
4. A `Terminated` arrives through the mailbox in the watcher's serial order, never out of band.

Guarantee 2 follows the cluster's `down` decisions, not physical reality: where no authority declares the node `down` (static mode; registry-based mode until the registry removes it), watchers learn nothing from a crash. Between nodes, a `Watch` frame registers interest, and the target's node sends one `Terminated` frame per remote watcher; a node MUST deliver that frame only to the watcher it addresses.

### 2.6 Discovery

The receptionist gives a node the initial `ActorRef` of a service without a hard-coded `ActorId`. The system provides `Receptionist<S>` (`ActorSystem::receptionist`); application code calls `register(key, who)`, `lookup(key)` for a `Listing`, and `subscribe(key)` for a stream of listings. `lookup` is synchronous: it reads replicated local state.

Requirements:
1. The receptionist is not an actor and has no resolvable path: it is a handle available on every node without prior introduction.
2. Registrations replicate over the `Receptionist` frames (§2.14) and are eventually consistent; concurrent registrations MUST merge without coordination (a CRDT keyed by registering node is RECOMMENDED).
3. When a node goes `down` (§6.4), the receptionist MUST prune every registration whose actor is hosted on it, whichever node registered it, and subscribers MUST receive an updated listing. A registration whose actor terminates is pruned likewise, through death watch (§2.5).
4. A listing MUST omit actors on a node out of rotation, one `draining` (§6.1) or `down`. A node confirmed `unreachable` but not `down` stays listed. A `draining` node's registrations are retained; `resume` restores them without re-registration, and a drained node still answers a direct `ask`.
5. `subscribe` MUST deliver the current listing on subscription and a fresh listing on every change.
6. `Key<A>` is typed by actor type, so `lookup` and `subscribe` return correctly typed refs.
7. **Order:** a `Listing` MUST order its entries by a deterministic function of the registered set, so two nodes holding the same registrations and the same rotation view return the same sequence.
8. **Convergence under loss:** once faults cease, every node's listing for a key MUST converge, within bounded logical time, on the registrations of live actors, even when individual replication messages were lost.

### 2.7 Observability

A system emits `Event`s onto one ordered stream per node (`ActorSystem::emit`), into the `EventSink` a deployment supplies (`()` discards); a simulator reads the same stream to check invariants (§10). The sink and `Event::App` are plug points.

- A system SHOULD emit, as named variants: the lifecycle steps (`AssignId`, `ActorReady`, `ResignId`, `Restarted`), mailbox enqueue and dispatch (`Enqueue`, `DispatchStart`, `DispatchEnd`), the ask bracket (`AskIssued`, `AskOutcome`), membership and reachability transitions, Raft elections (`LeaderElected`), supervision decisions (`Supervised`), `Terminated` deliveries (`TerminatedDelivered`), and the singleton pair (utilities §3).
- A system MUST emit `Undelivered`, with the refusing `CallError`, for every inbound remote message it refuses (§3.3 rule 4): the only trace a refused `tell` leaves.
- A named variant exists only for a property the framework catalogue checks by variant. A layer above MUST carry its own events as `Event::App` (any `Debug + Clone + PartialEq + Send + Sync` value is an `AppEvent`, read back with `Event::as_app`), so the core enum is never edited for a layer above it.
- A system SHOULD expose metrics (mailbox depths, throughput, call latency, restarts, associations, membership size), SHOULD propagate a trace context through envelopes, and SHOULD log lifecycle, membership, downing, and supervision actions at a defined, filterable level.

### 2.8 The actor system

`ActorSystem` is the runtime an actor runs on. `LocalSystem` (`actor-core`) implements it for one node, answering `Unreachable` to every remote send; `ClusterSystem` (`actor-cluster`) implements it for a cluster; the simulator's `SimNode` is a `ClusterSystem` over simulated host traits. Application code and the layers above consume it, generic over `S: ActorSystem`. A new runtime is a plug point, usually built from the toolkit (§2.9). One trait has two faces, so a system value is one cheap handle:

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
    fn sleep(&self, d: Duration) -> BoxFuture<'static, ()>;
    fn launch(&self, task: BoxFuture<'static, ()>);
    fn emit(&self, event: Event);
    fn next_random(&self) -> u64;

    // What an implementation supplies
    fn spawn_factory<A: Actor<System = Self>>(&self, factory: ActorFactory<A>, parent: Option<ActorId>) -> ActorRef<A>;
    fn resolve_local<A: Actor<System = Self>>(&self, id: &ActorId) -> Option<Mailbox<A>>;
    fn is_local(&self, id: &ActorId) -> bool;
    fn in_rotation(&self, node: NodeId) -> bool { true }
    fn codec(&self) -> Arc<dyn Codec>;
    async fn remote_ask(&self, recipient: &ActorId, manifest: Manifest, payload: Vec<u8>, within: Duration) -> Result<Vec<u8>, CallError>;
    async fn remote_tell(&self, recipient: &ActorId, manifest: Manifest, payload: Vec<u8>) -> Result<(), CallError>;
    fn watch(&self, target: ActorId, watcher: ActorId, deliver: WatchDelivery);
    fn unwatch(&self, target: &ActorId, watcher: &ActorId);
    fn receptionist_state(&self) -> Arc<ReceptionistState>;
    fn replicate_registration(&self, key: &str, origin: NodeId, id: ActorId) {}
}
```

- A layer above MUST take its node identity, time, randomness, task scheduling, and observability from the system, never from the host, so one seed reproduces it under simulation (§2.12.2).
- `spawn_factory` MUST follow the lifecycle of §3.1. `is_local` MUST classify from the id alone.
- `in_rotation(node)` MUST be `false` exactly when this node's view holds `node` `draining` or `down` (§2.6 requirement 4).
- `remote_ask` and `remote_tell` carry an encoded payload under the message's `Manifest`; `remote_ask` MUST resolve to the encoded reply or a definite `CallError`, waiting at most `within` for the reply (§4). Neither emits the ask bracket (§2.10).
- `watch` MUST report a target already terminated, or on a node already `down`, immediately, and MUST NOT block on the watcher's own mailbox, since a watcher may call it from its own handler.
- `replicate_registration` MUST carry a registration to every other node (§2.6).

### 2.9 Implementor toolkit

`actor_core::host` holds the node-local runtime a system is built from: `LocalHost`, `ActorFactory`, `WatchDelivery`, `ReceptionistState`, and `DispatchFn`. It is an internal boundary between `actor-core` and system implementations; application code never uses it. Both systems are built on `LocalHost`, so lifecycle, serial execution, supervision, local death watch, and dispatch are implemented once.

- `LocalHost` owns one node's actors: `spawn_actor` (§3.1), `resolve_local`, `deliver` (§2.10), the watch table (`watch`, `add_watch`, `remove_watch`), `synthesize_node_down`, and `deliver_terminated_to`. `LocalHost::with_incarnation` stamps the process incarnation on every id (§2.2).
- `ActorFactory<A>` yields the first instance and each restart's, then `None`; a value-spawned actor's factory yields once (§2.4).
- `WatchDelivery` delivers one `Terminated` to one watcher; for a remote watcher the implementation supplies one that forwards the signal over its transport.
- `ReceptionistState` is the registry behind the `Receptionist`; the implementation feeds it peers' registrations (`Receptionist::apply_remote_registration`) and reads `ReceptionistState::digest` for anti-entropy.

An implementation MUST route every inbound envelope through `LocalHost::deliver` after its authorization gate (§2.20), so refusals behave identically in every system; MUST call `synthesize_node_down` for each peer its view declares `down` (§5.1 step 4); and MUST hand an inbound `Terminated` frame to `deliver_terminated_to` for the one watcher it addresses.

### 2.10 Manifests, dispatch, and replies

An actor fills a `HandlerRegistry` in `register` (`accept::<M>()`); the system consults it for each inbound remote message (`dispatch(manifest)`) and answers through a `ReplyHandle` (`send` or `fail`). This is an internal boundary.

**Manifests:** a manifest MUST stay stable across recompiles and renames; an explicit string such as `"myapp.Greet"` is RECOMMENDED. A breaking change to a message's shape SHOULD become a new message type with a new manifest.

**Registration:** each `accept::<M>()` captures the monomorphized entry that decodes `M`, enqueues `Handler::<M>::handle`, and routes the reply. An actor type's registry MUST be complete before any message can reach an actor of that type, with no setup step and no link-time collection. A local send never consults it. An optional derive MAY generate a `MANIFEST` and a `register` body; an implementation MUST work with hand-written ones, and a hand-written manifest MUST override a derived default.

**Allowlist:** only registered message types are ever built from network bytes, and an envelope's manifest is untrusted text until the registry matches it (§9). An unregistered `(actor type, manifest)` MUST yield `CallError::Unhandled`. Accepting `Terminated` MUST register nothing, so no peer can inject a termination (§2.5).

**Outbound:** `ActorRef` enqueues a local target's message by value; it encodes a remote target's message with the system codec, sends it through `remote_ask` or `remote_tell`, and decodes the reply under the calling system. `ask` brackets every call, whatever its path or outcome, with one `AskIssued` and exactly one `AskOutcome` (invariant #1); the bracket lives in this layer, so `remote_ask` emits neither.

**Inbound:** a system MUST deliver an inbound envelope to its local recipient or refuse it with `DeadLetter` (no live actor holds the id), `Unhandled` (manifest not registered for the actor's type), `Serialization` (payload undecodable), or `MailboxFull` (the receive path never stalls on backpressure). Each refusal resolves the reply with its error and emits `Undelivered` (§2.7). An `ActorRef` inside the message MUST be rebound to the receiving system.

**Replies:** `deliver` MUST resolve exactly one of `send` and `fail` per `ask`. `send` carries successful and application-failed outcomes alike (§2.1 rule 4); `fail` carries only a failure the handler never produced. Neither applies backpressure to the handler.

### 2.11 Codec

`SerializationRequirement` (`Serialize + DeserializeOwned + Send + 'static`, blanket-implemented) is the bound on every wire-crossing value; `Codec` is the object-safe serializer a system holds as `Arc<dyn Codec>` (`actor-serialization`). The codec is a plug point: `JsonCodec`, `PostcardCodec`, or a deployment's own.

1. Every message type and reply type MUST satisfy `SerializationRequirement`; the `Message` and `Handler` bounds enforce it.
2. The codec is fixed per system, and both ends of an association MUST agree on it; the handshake compares `Codec::name` (§2.13).
3. Decoding MUST reject unknown manifests (`Unhandled`) and MUST NOT build arbitrary types from untrusted input (§2.10).
4. Serialization failures are reported as `CallError::Serialization` and MUST NOT panic the receive loop.

### 2.12 Host environment

`Clock` (`now`, `sleep`, `timeout`), `Entropy` (`next_u64`), and `Spawner` (`launch`) are what a system takes from its host (`actor-core`). They are plug points: `actor-runtime` supplies `TokioClock`, `OsEntropy`, and `TokioSpawner`; `actor-simulation` supplies `SimClock`, `SimEntropy`, and `SimSpawner`. Every system consumes them, and every layer above consumes them through the system (§2.8). `Instant` is the framework's own logical instant, so a virtual clock can manufacture it.

#### 2.12.1 Obligations

A system MUST obtain time, randomness, and task spawning through these traits, never from the host directly.

1. **All timing** MUST come from `Clock`: `ask` deadlines, SWIM intervals, gossip periods, registry sync intervals, Raft timers, and supervision backoff.
2. **All randomness** MUST come from `Entropy`: peer selection, SWIM's `k` helpers, Raft election jitter, backoff jitter, and minting a `NodeId` where a mode generates identity.
3. **All background tasks** MUST be created through `Spawner`; no executor or loop binds itself to a specific async runtime.
4. **No observable nondeterminism.** Anything that crosses the wire or appears on the event stream MUST have a deterministic order: unordered iteration (`HashMap` order, for instance) MUST NOT affect message ordering, peer selection, or reply timing.

#### 2.12.2 Determinism

A system SHOULD support deterministic simulation: it runs unchanged over simulated implementations of these traits and of the transport, so a whole cluster runs in one process over virtual time and the code under test is the production code. A system that supports it MUST satisfy:

1. **Seed-reproducibility.** Given the same seed, configuration, and workload, two runs MUST produce byte-identical event streams (§2.7), so a failing run replays from its seed and configuration alone. All nondeterminism MUST pass through `Clock`, `Entropy`, `Spawner`, `Transport` (§2.13), and, in registry-based mode, `RegistryClient` (§2.17).
2. **Quiescence-driven time.** Under simulation, logical time MUST advance only when no task is ready to run.
3. **No ambient nondeterminism.** A simulation build MUST NOT read the wall clock, spawn OS threads, or use an unseeded RNG, and SHOULD enforce this statically.

### 2.13 Transport

A `Transport` carries frames between nodes: `send(peer, frame)` and `shutdown()`. `ClusterSystem` consumes it; `TcpTransport` (`actor-runtime`) and `SimTransport` (`actor-simulation`) implement it. It is a plug point. The inbound half is a constructor convention: the constructor returns, beside the transport, the channel receiver of `(NodeId, Frame)` pairs that `ClusterSystem::start` reads.

1. **Associations.** Before exchanging actor traffic, two nodes MUST complete a handshake that establishes an association: a wire-version check, a node-identity exchange, codec agreement, and optional authentication (§9). A peer at a wire version other than this build's (`WIRE`, §2.14) MUST be refused, naming both (compatibility V2), before its codec, secret, or identity is examined, so version skew is never reported as a security failure.
2. **Multiplexing.** Many conversations share one association; each request carries a correlation id that its response references.
3. **Framing.** A malformed frame MUST tear down the association, not the node. A transport that encodes frames onto a byte stream MUST refuse a frame longer than a bound every node of the cluster shares, before allocating for it.
4. **No liveness decisions.** SWIM probes members independently of connection state (§7), so an association blip is never mistaken for a failed node. A transport MAY surface association loss as a hint, but the hint MUST feed only the detector's refutable `suspect` state, never `down`.
5. **Inbound.** A transport MUST deliver each inbound frame tagged with the node identity its association authenticated, in the order that peer sent it.
6. **Sending.** `send` dials and handshakes on first use and MUST NOT retransmit (§4); a peer it cannot reach is `TransportError::Unreachable`, which callers see as `CallError::Unreachable`. `shutdown` releases the transport's resources on a graceful stop (§6.3), and closing the inbound path ends the receive loop.

A transport MAY carry its own address-discovery traffic beside the frame vocabulary, never handed up to the system.

### 2.14 Wire protocol

`Frame` (`actor-cluster`) is the vocabulary every node of a cluster MUST speak; `WIRE` is its version. It is an interop boundary.

| Family | Frames | Carries |
|---|---|---|
| Actor traffic | `Envelope`, `Reply` | `Envelope { recipient, manifest, correlation, payload }`, with a `CallId` correlation for an `ask`; `Reply { correlation, outcome }`, the encoded reply or a `CallError` |
| Detection | `Ping`, `Ack`, `PingReq`, `IndirectAck` | probes, each with the sender's incarnation and a membership digest (§7) |
| Death watch | `Watch`, `Unwatch`, `Terminated` | a remote watcher's interest; a termination addressed to one watcher (§2.5) |
| Discovery | `Receptionist`, `ReceptionistSync` | one registration; a whole registry for anti-entropy (§2.6) |
| Consensus | `Raft`, `RaftHeartbeats`, `RaftHeartbeatReplies`, `RaftPropose` | a group's engine message; empty appends and their replies batched per peer; a command for a leader (§6.4.3) |

- `RaftHeartbeats` is framing only: a receiver MUST hand each beat to its group as the lone empty append it stands for.
- The handshake is a preamble exchanged before the vocabulary applies (§2.13), encoded independently of the codec under negotiation. Membership has no frame: each mode disseminates through a channel it already has (§6.4).
- The vocabulary is a versioned format (`actor.wire`, compatibility §4) whose version travels in the handshake, so no frame carries its own stamp. Under a positional encoding any change to it is a new wire version, which every node takes at once.

### 2.15 Control-plane configuration

A deployment configures a `ClusterSystem` with a `ClusterConfig`: codec, mailbox bound, event sink, membership mode, whether the node starts as a joiner (§6.3), authorizer (§2.20), and process incarnation (§2.2). The mode is a plug point: `MembershipMode` is `Static`, `Registry`, `Leader`, or `Gossip` (§6.4), and every node in a cluster MUST run the same one.

- `SwimConfig` carries the detector's `T_probe`, `T_rtt`, `T_suspect`, and `k` (§7), each of which MUST be configurable.
- `DowningPolicy` decides `unreachable → down` where a mode has an in-cluster down authority: `Conservative` never auto-downs; `Timeout` downs a member unreachable that long. The default MUST be conservative.
- `RaftConfig` names the control group's founding voters, which MUST be identical on every node, the election and heartbeat timing, and the durability factory (§2.18).
- `RegistryMode` names the registry client (§2.17) and its sync interval; `Static` MAY enable the detector, observe-only.

### 2.16 Membership view

`ClusterSystem::membership` exposes this node's `Membership`: each member's `MemberStatus` and `Reachability`. The utilities read it for placement, and layers above to rank replicas. It is a read-only boundary: only the control plane writes it.

- A clustered system MUST expose its local membership view (each member's state and reachability, and the serving set derived from it) as a synchronous read that generates no traffic.
- `Membership::serving_members` is the serving set: every member, this node included, that is `up` and not confirmed `unreachable`, in `NodeId` order; a merely suspected member stays in it. The rotation filter (`Membership::in_rotation`) is looser, excluding only `draining` and `down`.
- `Membership::failure` reports why this node stopped applying committed transitions (§6.4.3 item 1).

### 2.17 Registry client

`RegistryClient` (`fetch`, `register`, `set_state`, `deregister`) connects registry-based mode's sync loop (§6.4.2) to the external registry. It is a plug point: a deployment implements it against its platform, and the simulator supplies `SimRegistry`.

- `fetch` MUST return one consistent read of the full desired member set (`RegistrySnapshot`), stamped with the registry's global revision, each entry carrying the revision of the mutation that last changed it. Revisions MUST be monotonic and totally order the registry's states.
- Each mutation returns the revision it produced. `register` is idempotent and admits the node `Up`; `set_state` sets the `Up`/`Draining` cordon; `deregister` removes the entry, whose absence finalizes `down`.
- An unreachable registry MUST answer `RegistryError::Unavailable`, never a fabricated snapshot.

### 2.18 Raft durability

`RaftWAL` persists one voter's Raft state for one group, obtained per `(group, node)` from the factory `RaftConfig::storage`. It is a plug point: `FileRaftWAL` (`actor-runtime`) persists to the local filesystem; `InMemoryRaftWAL` serves simulation. `load` returns the `PersistedRaft` state; `save_term_and_vote`, `append`, and `save_snapshot` each answer a `WalAck`; `poisoned` reports a failed WAL.

- `WalAck::Persisted` MUST mean the write is on stable storage when the call returns: a crash at any later instant leaves `load` returning a state that includes it. The engine announces what a write records (a vote, an accepted append, a snapshot) only after `Persisted`.
- A write that did not reach stable storage MUST answer `WalAck::Failed`, never `Persisted`, and MUST NOT panic. After one `Failed` the WAL MUST refuse every later write, and `poisoned` MUST report the reason from then on.
- `load` MUST return the fold of every write acknowledged `Persisted`. `append` truncates at an absolute index and appends there; `save_snapshot` discards the log prefix the snapshot subsumes.
- The factory MUST be stable per `(group, node)`, returning the same durable state when called again, so a restarted voter reloads the term it voted in.

### 2.19 Application consensus

`RaftConsensus` is the consensus capability for agreement beyond membership, used as a generic bound `R: RaftConsensus`. `ClusterSystem` implements it, live in leader-based mode and inert elsewhere. Entity consumes it: its shard map and per-shard leader election run as groups (storage §6, §5). Its methods create, reconfigure, and remove groups, propose commands and subscribe to commits, report and transfer leadership, name the voters, and re-expose the system's `node`, `sleep`, and `launch`.

- Each group is an independent Raft instance with its own log, leadership, and terms, and MUST provide election safety, log matching, and leader completeness. `GroupId::CONTROL` is the membership group, which the cluster creates itself.
- A group MUST deliver to each subscriber every entry committed after it subscribed, exactly once and in commit order, on one stream that also carries snapshot installs (`Committed::Snapshot`), so an install and the commands after it never reorder. There is no replay from an index: a subscriber that needs a group's whole log subscribes right after `create_group`.
- `group_term` MUST report the group's current term, which never decreases on a node and has at most one leader, so a leader's term serves a layer above as a single-writer fencing token.
- `propose_to` offers a command to the group's leader; a non-leader forwards it, never applies it. It does not report the commit: a proposer learns of it from the committed stream and bounds its own wait.
- `configured_voters` is the founding voter set, identical on every node and the seed for creating a group; `cluster_voters` is the control group's current set. Learners replicate but never vote.
- `transfer_group_leadership` follows §6.4.3 item 8; `false` means the transfer did not start.

### 2.20 Authorization

An `Authorizer` (`ClusterConfig::authorizer`) answers `authorize(peer, recipient, manifest)` per inbound envelope. It is a plug point.

- A system MUST consult it before dispatch. A denied message MUST be rejected as `CallError::System` and never deserialized into the actor's type, so an unauthorized peer cannot trigger handler side effects; the refusal is recorded like any other (§2.10).
- With no `Authorizer`, every message that clears the handshake is admitted.

---

## 3. Actor lifecycle and execution

### 3.1 Lifecycle ordering

For each actor, the system MUST observe exactly this order:

```
1. id ← assign_id()   reserve identity; unique; not yet observable
2. mailbox created and buffering, id resolvable, executor started
3. actor_ready        started() returned Ok; buffered messages become processable
4. … the actor processes messages …
5. resign_id(id)      on stop or failure; stopped() runs; all resources released
```

- `assign_id` and `actor_ready` each run exactly once, `assign_id` before any message can be delivered.
- `resign_id` runs exactly once, even when spawn fails between steps 1 and 3, in which case it follows `assign_id` directly.
- Between `assign_id` and `actor_ready` the id is reserved but not deliverable: the system MUST buffer or dead-letter messages to it, never process them.

`spawn` is synchronous and returns the `ActorRef` at step 2; messages sent before step 3 queue. A supervised re-creation (§2.4) emits `Restarted`, not a second `ActorReady`.

### 3.2 Resolution

`resolve(id)` returns an `ActorRef` without a network round trip: a local id with a live actor gives a ref whose messages enqueue by value; a local id with no live actor, a ref that dead-letters; a remote id, a ref whose messages serialize and route through the transport. `resolve` MUST NOT block or contact the remote node; liveness surfaces on send or through failure detection (§7). It is infallible: malformed network input never reaches it, because the codec and the allowlist reject it first (§2.10). A local send performs no encode or decode, yet its observable result MUST equal the remote path's.

### 3.3 Execution model

1. **One mailbox per actor.** Each local actor has one bounded mailbox feeding exactly one executor task.
2. **Serial execution.** The executor processes one message to completion before the next, giving each actor a total order over the messages it observes.
3. **No reentrancy.** A handler MAY `.await`; while it is suspended, the executor MUST NOT begin another message for that actor. Handlers SHOULD offload blocking work.
4. **Backpressure.** When a mailbox is full, the system MUST apply a defined policy (await space, or fail with `MailboxFull`) and MUST NOT drop messages silently; `tell` and a local `ask` await. A remote `tell` refused on the receiving node (full mailbox, no live actor, no registration, undecodable payload, or a sender the authorizer refuses) has no reply to carry the error, so the receiving node MUST record the refusal on its event stream (§2.7).
5. **Ordering.** Messages from one sender to one recipient, `tell` and `ask` alike, arrive in send order, scoped to one association for a remote recipient (§4). No order holds across senders.
6. **Fairness.** The runtime SHOULD schedule executors fairly, so no ready actor starves.

---

## 4. Delivery guarantees

- **At-most-once** per call attempt: the framework MUST NOT transparently retransmit a delivered message.
- **No silent loss.** Every `ask` MUST terminate in a reply or a definite `CallError` (§8.1). A pending ask whose target node is declared `down` (§6.4) MUST complete with `Unreachable`; one whose target is confirmed `unreachable` (§7) MAY complete with `Unreachable` early, which at-most-once delivery makes safe; otherwise its deadline bounds it.
- **Ordering** per directed pair (§3.3 rule 5) holds end to end over one association. No order is defined between frames sent before an association was lost and frames sent over its replacement; a sender that needs it re-synchronizes with an `ask` round trip.

---

## 5. Failure model

Failures the system MUST represent explicitly, never mask:

| Failure | Surfaced as |
|---|---|
| Target actor does not exist or has resigned | `CallError::DeadLetter` |
| No handler registered for the message type | `CallError::Unhandled` |
| Application error from a handler | inside `M::Reply` |
| Call exceeded its deadline | `CallError::Timeout` |
| Mailbox full (§3.3) | `CallError::MailboxFull` |
| Recipient node `down`, or association lost | `CallError::Unreachable` |
| Encode or decode failure | `CallError::Serialization` |

Supervision (§2.4) handles actor-level faults; membership and failure detection (§6, §7) handle node-level faults and reach watchers through death watch (§2.5).

### 5.1 The node-down cascade

Declaring a node `N` `down` has six consequences, each specified where cited:

1. **Decision** by the mode's down authority (§6.4), usually after the detector confirms `N` `unreachable` (§7). Static mode has no down authority, so the cascade never runs there.
2. **Membership:** the transition propagates through the mode's channel, and `down` is terminal (§6.1).
3. **In-flight callers:** every pending `ask` to an actor on `N` completes with `Unreachable` (§4).
4. **Watchers:** each node synthesizes `Terminated { reason: NodeDown }` for every locally watched actor on `N` when its own view marks `N` `down` (§2.5).
5. **Receptionist:** those signals prune the registrations of actors hosted on `N` (§2.6).
6. **Routing afterward:** `resolve` of an id on `N` still returns a remote ref, and sends fail with `Unreachable` rather than blocking.

A graceful leave (§6.3) reaches the same state and steps 3 to 6 through `leaving` instead of suspicion; an operator decommission has the control plane declare `down` directly. A drain is not this cascade: it is reversible and leaves the node a member.

---

## 6. Membership

The member states (§6.1), the merge rule (§6.2), and the lifecycle (§6.3) are common to every cluster; who decides the member set is one of four control-plane modes (§6.4).

### 6.1 Member states

```
joining → up ──→ leaving ──→ down ──→ removed     (the monotonic ladder)
           ⇅        ▲           ▲
        draining ───┴───────────┘                 (reversible cordon, off the ladder)

reachability: reachable ⇄ unreachable             (orthogonal to every state above)
```

- **joining**: handshake complete, awaiting admission, where admission is an in-cluster decision (leader-based and gossip-based modes).
- **up**: a full member; may host and address actors.
- **draining** (registry-based and leader-based modes): a reversible maintenance cordon. The node stays a full member, discovery routes new work away from it (§2.6), and `resume` returns it to `up`. Being off the ladder, its transitions are ordered by the authority stamp, not by rank (§6.2).
- **leaving**: graceful shutdown initiated; the node drains its mailboxes.
- **down**: declared dead by the mode's down authority, or reached by completing a leave. Terminal: a node that was `down` MUST NOT rejoin under the same incarnation; it MUST rejoin as a new member with a new `NodeId`. Modes that require stable identity persist it across ordinary restarts; rejoining after `down` means wiping it.
- **removed**: a tombstone, eventually pruned.

**Reachability** is maintained by the failure detector wherever it runs (§7): a node is first `suspect`, then confirmed `unreachable` after `T_suspect`. Both are reversible by a higher incarnation; `down` is the separate, terminal decision of the mode's authority.

### 6.2 Views and the merge rule

Every node holds a membership view, one entry per member: `(NodeId, state, incarnation, stamp)`. Views merge entry-wise by the same rule in every mode:

1. **Authority stamp wins.** An entry with a higher authority stamp (the registry revision, or the log commit index) replaces one with a lower or absent stamp. A mode's stamps come from a single logical writer and totally order its decisions, which is what lets `up ⇄ draining` converge.
2. **Otherwise, the lattice.** At equal stamp, the higher state on the ladder `joining < up < leaving < down < removed` wins; incarnation plays no part on this axis.
3. **`down` is absorbing.** No merge result, stamped or not, returns a `down` member to a live state under the same incarnation.

Reachability is outside this merge: the detector orders suspicion and refutation by incarnation, the higher incarnation winning and, within one, the more severe view (§7). Static and gossip-based modes issue no stamps.

### 6.3 Joining and leaving

A joiner handshakes an existing member (a configured seed, or one learned from the registry); a leaver announces `leaving` and drains; a detector, where one runs, confirms a crash `unreachable`. Each transition that changes the member set needs the authority its mode names (§6.4.1 to §6.4.4). The `leaving` announcement decides nothing: it starts the drain and advises peers. Watchers learn of a departed node through the cascade (§5.1) in every mode that reaches `down`.

### 6.4 Control-plane modes

A control plane answers where the authoritative member set lives, how members learn it, and who may declare a member dead. The mode is chosen at node startup (§2.15).

| | **Static** | **Registry-based** | **Leader-based** | **Gossip-based** |
|---|---|---|---|---|
| Authoritative set | local configuration | an external registry | a replicated Raft log | none; the converged view |
| Dissemination | none | each node syncs the registry | log replication, plus stamped gossip | infection-style gossip |
| Detector (§7) | none, or observe-only | observe-only | a sensor feeding the leader | full SWIM |
| Declares `down` | nobody | the operator, via the registry | the leader, by committed entry | the coordinator, by policy |
| `draining` | no | yes | yes | no |

#### 6.4.1 Static

Membership is fixed at deployment: every node reads the full topology from configuration, all members start `up`, and changing the topology is a redeploy. By default no detector runs and no membership traffic flows.

- Node identity MUST be stable across restarts and derivable from configuration.
- A node that stops answering remains a member: sends to it fail with `Unreachable` or `Timeout`, no `Terminated` is synthesized for its actors, and the cascade never runs.
- A deployment MAY enable the detector observe-only, gaining `suspect` and `unreachable` events and nothing else: an `ask` to an `unreachable` peer runs to its deadline, and the receptionist keeps listing that peer's actors.

#### 6.4.2 Registry-based

The authoritative member set lives in an external registry (a platform API, a database, a coordination service) that the cluster reads but does not operate. An operator or platform mutates it; every member runs a sync loop that applies its state to the local view.

1. **Revisions.** The registry MUST expose a monotonic revision that totally orders its states. Each applied entry carries it as its authority stamp (§6.2), so members converge on the latest state regardless of sync order. Members MAY also gossip stamped entries, a safe accelerant, never a second authority.
2. **Join:** a node is admitted by appearing in the registry (registered by the platform or by itself); members mark it `up` at that revision. `joining` is unused.
3. **Leave:** whoever registered the node removes its entry, and the removal's revision finalizes `down` and runs the cascade.
4. **Down:** only a registry mutation declares `down`. A node that stops answering becomes `unreachable`, reversibly, and stays a member until its entry is removed.
5. **Drain and resume:** the cordon is a registry state. A `draining` or `unreachable` member keeps its membership across a restart, so node identity MUST be stable across restarts.
6. **Registry unavailability** pauses membership changes only; the data plane runs on the last-synced view. A node MUST NOT treat its own inability to reach the registry as evidence about peer liveness.
7. **Registry seam.** The cluster reaches the registry only through `RegistryClient` (§2.17).

#### 6.4.3 Leader-based

The cluster self-hosts its member set as a replicated log: an elected leader serializes every change, and a quorum of voters commits it. Any implementation MUST provide Raft's observable guarantees (election safety, log matching, leader completeness).

1. **Single writer, total order.** Every membership transition (admission, drain and resume, a leave committed at the departing node's request, anything `→ down`) MUST be a log entry committed through the current leader. The commit index is the authority stamp: members apply committed entries in log order. A command offered to a non-leader is redirected or rejected, never applied.

   **A committed entry a member cannot decode MUST stop that member's control plane, never be skipped:** a skipped entry makes its view a gap in the log, not a prefix. It MUST stop applying transitions, permanently, and MUST NOT proceed as though it had applied one. The transition payload carries its own version (`actor.membership`, compatibility §4), so another build's transition is told apart from damage. The stopped member SHOULD keep replicating and gossiping, SHOULD hand off leadership if it leads (item 8), and SHOULD expose its condition to an operator (§2.16).
2. **Voters.** A configured, modest subset of members (typically 3 or 5) are voters; only they elect leaders and form quorums. Voters MUST persist their Raft state (§2.18) and MUST keep stable identities across restarts. Voter-set changes are committed configuration entries. A voter declared `down` MUST be removed from the voter set by a committed change.

   **A voter that cannot persist MUST stop voting:** a write reported durable that was not lets it vote twice in a term across a restart, or acknowledge an entry it cannot replay. A failed write (`WalAck::Failed`) MUST be permanent for that voter: it steps down, grants no vote, starts no election, acknowledges no append, and MUST NOT resume on a later success, since nothing at this layer can establish that the volume recovered. It SHOULD stay reachable and refuse explicitly, so peers elect around it at once, SHOULD expose its condition, and SHOULD NOT be expressed as a panic unless a panic halts the whole node.
3. **Dissemination.** Voters learn state from log replication. Non-voting members MAY replicate the log as learners or receive committed state by gossip stamped with the commit index; by the merge rule both converge.
4. **Downing.** The detector is a sensor feeding the leader: on a confirmed `unreachable`, the leader MAY commit `down` under the downing policy. Downing is therefore quorum-gated, and a minority partition can never evict the majority. Raft's own heartbeats are internal to consensus, distinct from member reachability.
5. **Quorum loss** pauses the control plane only; members keep exchanging actor traffic on the last committed view.
6. **Beyond membership.** The engine MAY run further groups for other control-plane decisions, reached through `RaftConsensus` (§2.19). This specification defines only the membership entries.
7. **Runtime seams.** Consensus traffic rides the ordinary transport (§2.14), and its timers and jitter come from `Clock` and `Entropy` (§2.12).
8. **Leadership transfer (RECOMMENDED).** A group leader SHOULD be able to hand leadership to a named voter without waiting out an election timeout, so a planned departure does not cost one failover per led group. It MUST offer the transfer only to a voter that has replicated its last entry, the invitation MUST be accepted only from the leader of the recipient's current term, and the transfer starts an ordinary election, so it can move leadership but never split it.

#### 6.4.4 Gossip-based

Membership propagates peer to peer through SWIM and anti-entropy gossip, with no election, quorum, or external authority: every node converges on an eventually consistent view, at constant per-node load.

1. **Dissemination.** The detector's probes are the one channel: each carries a membership digest (§7 item 6) to a target drawn from `Entropy`, merged by §6.2.
2. **Refutation.** A node that sees a stale suspicion or state about itself increments its incarnation and gossips the override (§7).
3. **Coordinator.** Transitions that need a single actor (admitting `joining → up`, finalizing `leaving → down`) fall to the coordinator: a role each node derives from its own view, held by the member with the lowest `NodeId` among those `reachable` and neither terminal nor `leaving`. `joining` members are eligible, the deriving node included, which lets a fresh cluster bootstrap. The coordinator acts only on a locally stable, fully reachable view, except when applying the downing policy to an `unreachable` member (item 4). Two nodes that transiently both act as coordinator cannot conflict: their decisions merge through the lattice.
4. **Downing.** The detector drives `suspect → unreachable`; the `DowningPolicy`, applied by the coordinator, decides `unreachable → down`. The default policy MUST be conservative and MUST NOT auto-`down` across a partition.
5. **Join:** a new node handshakes a configured seed and enters `joining`; the coordinator admits it on convergence.

---

## 7. Failure detection

Every mode except static runs a SWIM-style detector on each node; static MAY enable it (§6.4.1). The detector is the cluster's reachability sensor, maintaining the `reachable`/`suspect`/`unreachable` axis (§6.1) identically wherever it runs; only its authority over `down` differs by mode (§6.4).

1. **Direct probing.** Every `T_probe`, a node probes one member, chosen through `Entropy`, with a `Ping`; a probed node MUST answer with an `Ack` carrying its incarnation.
2. **Indirect probing.** On a missed `Ack` within `T_rtt`, the prober SHOULD ask `k` other members to probe the target (`PingReq`); a helper that hears the target's `Ack` MUST relay an `IndirectAck` carrying the target's incarnation.
3. **Suspicion.** Only when the direct and the indirect probes both fail does the prober mark the target `suspect`, carrying its incarnation.
4. **Refutation.** A node that sees itself suspected MUST increment its incarnation and disseminate the override, clearing the suspicion cluster-wide.
5. **Confirmation.** A suspicion unrefuted for `T_suspect` becomes `unreachable`; the mode's down authority then MAY move the member to `down`.
6. **Piggybacking.** Membership and suspicion updates SHOULD ride on the probes.

`T_probe`, `T_rtt`, `k`, and `T_suspect` MUST be configurable (§2.15); `T_suspect` SHOULD scale with cluster size to keep the false-positive rate low.

---

## 8. Errors

### 8.1 `CallError`

`CallError` is the failure to complete a call: `Timeout`, `Unreachable` (node down or association lost), `DeadLetter` (no live actor for the id), `Unhandled` (no registration for the message type), `MailboxFull`, `Serialization`, or `System` (any other system failure). Application failures live inside `M::Reply` (§2.1 rule 4).

- When `M::Reply` is `Result<T, E>`, `ask` returns `Result<Result<T, E>, CallError>`: the outer result says whether the call completed, the inner one what the handler decided. The type MUST NOT collapse them: a transport failure the caller may retry is not an application failure it must not.
- `CallError` MUST be exhaustive at the public API, so callers handle every kind of partial failure.

### 8.2 Principles

- Errors are values; panics are never control flow across actors. Supervision (§2.4) contains a handler panic, which never crosses the wire: in-flight callers see `Unreachable` or `DeadLetter`.
- Every `ask` MUST carry an effective deadline, explicit or the framework default (§2.2).

---

## 9. Security (RECOMMENDED)

1. **Transport security.** Associations SHOULD support mutual TLS, and node identity SHOULD bind to a verified certificate.
2. **Authentication.** The handshake SHOULD authenticate the peer and MAY enforce an allowlist of node identities; a cluster secret prevents accidental cross-cluster association. The secret travels in the handshake, so on an unencrypted association anyone who observes one handshake can join: a secret guards against accident, not an on-path adversary.
3. **Encryption is a stated choice.** A configuration MUST make a plaintext association something a deployment names explicitly, not what it gets by omitting the TLS material. Plaintext remains correct on loopback, in tests, and on a network the operator controls end to end.
4. **Deserialization safety.** No path may lead from an incoming envelope to constructing a type outside the dispatch registry (§2.10, §2.11).
5. **Authorization.** A system MAY gate delivery per `(peer, actor, manifest)` through an `Authorizer` (§2.20).

---

## 10. Invariants

An implementation conforms iff the twenty-two invariants below hold under injected faults (frame loss, duplication, delay, and reordering; partitions; crashes; induced mailbox, scheduling, supervision, and control-plane faults) and four properties hold by construction: state is reachable only through the actor's own handlers (§2.1), every message and reply is serializable (§2.11), remote dispatch is listed by a hand-written `register` (§1, §2.10), and associations are established by an authenticated, version-checked handshake (§2.13, §9). The MUSTs each item cites are its normative statement. The Rust copy of this list, with the suites that verify each item, is `actor-simulation`'s core catalogue.

1. **No silent loss (§4, §8).** Every `ask` terminates in exactly one reply or `CallError`; none remains pending at final quiescence.
2. **Crash completes in-flight calls (§4, §6.4).** An `ask` whose target node is declared `down` completes with `Unreachable`.
3. **Per-pair FIFO (§3.3).** Messages from one sender to one recipient are observed in send order within one association.
4. **Serial, non-reentrant execution (§3.3).** An actor never processes two messages concurrently.
5. **Bounded, non-dropping mailbox (§3.3).** A full mailbox blocks or returns `MailboxFull`; it never drops silently.
6. **Lifecycle order and exactly-once (§3.1).** `assign_id`, `actor_ready`, and `resign_id` occur in order, each exactly once, even when spawn fails; the id is undeliverable between assign and ready.
7. **`resolve` is local (§3.2).** Locality classification performs no network round trip.
8. **Manifest dispatch and allowlist (§2.10, §2.11, §9).** An unregistered `(actor type, manifest)` yields `Unhandled`; no type outside the registry is built from network bytes.
9. **Local sends skip serialization (§3.2, §2.10).** A local send performs no encode or decode, yet observes what the remote path would.
10. **`ActorRef` rebinding (§2.10).** An `ActorRef` in a message or reply is rebound to the receiving system and usable there.
11. **Death-watch exactly-once (§2.5).** After `watch`, the watcher receives exactly one `Terminated` for any cause, `NodeDown` included.
12. **Watch-after-death (§2.5).** Watching an already-terminated actor yields `Terminated` immediately.
13. **Signal ordering (§2.5).** `Terminated` arrives through the mailbox in serial order.
14. **Membership convergence (§6.2, §6.4).** Once faults cease and partitions heal, all `up` members converge on one membership set within bounded logical time.
15. **`down` is terminal (§6.1).** A node observed `down` never reappears `up` under the same incarnation.
16. **Partition tolerance (§6.4).** Under the default downing policy a partition alone never moves a member to `down`; registry-based mode holds this unconditionally, leader-based mode for any side without quorum.
17. **SWIM refutation (§7).** A node that sees itself suspected refutes it with a higher incarnation, clearing the suspicion cluster-wide.
18. **Supervision containment (§2.4).** A handler panic never crashes the node; the default directive is `Stop`; restarts back off; exceeding `max` within the window escalates.
19. **Receptionist consistency (§2.6).** Registrations of actors on a `down` node are pruned and subscribers notified; `subscribe` delivers the current listing, then every change; concurrent registrations merge.
20. **Type-safety (§2.2).** An `ask` or `tell` of a message the actor has no `Handler` for does not compile.
21. **Location transparency (§2.2).** One workload with a target local or remote produces identical replies and ordering.
22. **Quorum-gated control plane (§6.4.3).** Every leader-based membership transition is a quorum-committed entry applied in log order; at most one leader per term issues them; a side without a voter quorum commits none; each member's view is a prefix of the log, never a gap.

---

## 11. Deferred

- **Trace context and metrics.** Envelopes carry no trace context and no metrics are exported (§2.7); requests are followed, and depths and latencies derived, only from events.
- **Certificate-bound node identity.** Mutual TLS authenticates the cluster, not the node, so a holder of the cluster certificate can claim any `NodeId` the allowlist admits (§9 item 1).
- **Derives.** No derive generates a manifest or a `register` body (§2.10); actors write them by hand.
- **A configurable default deadline.** The `ask` default is a framework constant (§2.2); a caller needing another bound uses `ask_timeout`.
- **Death watch over a lossy association.** A dropped `Watch` or `Terminated` frame is not retried, so a remote watcher learns of the termination only if the target's node is later declared `down` (§2.5 guarantee 1).
- **Receptionist repair in every mode.** Static mode without a detector runs no anti-entropy, so a lost registration frame is never repaired; and a registration whose watch frame was lost is pruned only when its node goes `down` (§2.6 requirement 8).
