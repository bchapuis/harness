# Software Design Principles

Distilled from John Ousterhout, *A Philosophy of Software Design*. Use these as defaults
when writing or reviewing code. The [specification](distributed-actor-spec.md) was written
against them, so the examples below are drawn from the framework itself and cite the spec
section that embodies the principle — read them as both guidance and a map of why the
design is shaped the way it is.

Project conventions are part of "obvious code": edition 2024, `unsafe_code = "forbid"`,
`clippy::all = "warn"`, and no framework macros of its own (serde derives only, §1.1).

## Manage complexity

Complexity makes software hard to understand and change. It is the enemy. It appears as
change amplification (one change forces many edits), cognitive load (you must know too
much), and unknown unknowns (you cannot tell what to change). Its two causes are
**dependencies** and **obscurity**.

Complexity accumulates one small compromise at a time. Tolerate none.

## Program strategically

Working code is not the goal; a good design is. Spend a steady fraction of effort
improving structure rather than bolting on the smallest patch that works. The spec's
design stance is this principle in the large: hand-written manifests and `register` lists
(§1.1, §4.4) cost the user boilerplate now to keep the wire contract explicit and the
framework simple — a deliberate investment, not the smallest thing that compiles.

```rust
// ✗ Each new failure mode bolted onto the call site.
if node_down  { return Err("unreachable".into()); }
if no_handler { return Err("unhandled".into()); }

// ✓ One exhaustive type the caller matches once (§14).
pub enum CallError { Timeout, Unreachable, DeadLetter, Unhandled, MailboxFull, Serialization(String), System(String) }
```

## Make modules deep

A module's interface is its cost; its functionality is its benefit. Deep modules hide
much behind little. Prefer a few deep modules to many shallow ones.

```rust
// ✗ Shallow: the caller resolves locality, serializes, correlates, awaits, decodes.
let payload = codec.serialize(&msg)?;
let bytes = system.remote_ask(&id, M::MANIFEST, payload, deadline).await?;
let reply: M::Reply = codec.deserialize(&bytes)?;

// ✓ Deep: one call hides local-vs-remote, codec, correlation, and timeout (§3.3, §4.4).
let reply = actor.ask(msg).await?;
```

## Hide information

Each module should encapsulate one design decision and expose none of it. When two
modules share a secret, changing it changes both. Consolidate the secret. The framework's
central secret is actor state: it lives inside the actor and is reachable only through its
own handlers (§3.5).

```rust
// ✗ A handle that hands out the actor's state couples every caller to it.

// ✓ ActorRef holds only { id, system } — never state or handlers (§3.3). The actor's
//   fields stay private; the codec is likewise a private, per-system choice (§5).
pub struct ActorRef<A: Actor> { id: ActorId, system: A::System }
```

## Generalize interfaces, specialize implementations

Make the interface general enough to describe without naming the caller; let the
implementation serve only today's need. General interfaces are smaller and decoupled. The
runtime seam is the prime example: production and the simulator are two implementations of
the *same* traits (§4.6, §18.2), so the real system code runs unchanged under test.

```rust
// ✗ Bake tokio + the wall clock + an OS RNG into the runtime; nothing can drive it deterministically.

// ✓ General capabilities; production and simulation each implement them (§4.6).
pub trait Clock:   Clone + Send + Sync + 'static { fn now(&self) -> Instant; /* sleep, timeout */ }
pub trait Entropy: Send + Sync + 'static { fn next_u64(&self) -> u64; /* buggify, … */ }
pub trait Spawner: Send + Sync + 'static { fn launch(&self, task: BoxFuture<'static, ()>); }
```

## Keep layers distinct

Each layer should offer a different abstraction. Delete layers that only relay. Remove
pass-through methods and pass-through variables; carry cross-cutting values in a context.
The typed `ActorRef` layer and the byte-level `ActorSystem` trait are genuinely different
abstractions — types and locality above, serialized payloads and transport below (§4.1) —
not one forwarding to the other.

```rust
// ✗ Adds nothing.
fn fetch(&self, id: ActorId) -> ActorRef<A> { self.system.resolve(id) }

// ✓ Thread shared capabilities through context, not every signature (§3.4).
async fn handle(&mut self, msg: M, ctx: &Ctx<Self>) -> M::Reply;
```

## Pull complexity down

A simple interface used by many beats a simple implementation seen by one. Let the
implementer absorb hard cases; give callers sane defaults.

```rust
// ✗ Make the user run the lifecycle: reserve an id, start a mailbox, mark ready…

// ✓ One call composes assign_id → register mailbox → actor_ready; the order is the
//   system's secret, not the caller's burden (§4.1, §4.2).
fn spawn<A: Actor<System = Self>>(&self, actor: A) -> ActorRef<A>;
```

Pull down complexity about *how* the module works. Leave up choices about *what* the
caller wants. A purely local actor inherits an empty `register` default and never thinks
about the wire (§4.4); a remote one overrides it — one spawn path serves both.

## Define errors out of existence

Each error is a path the caller must handle. Redefine operations so the error vanishes;
otherwise mask it inside the module or handle many sites in one place.

```rust
// ✗ If resolution could fail, every send site would branch on it.

// ✓ Every ActorId is well-formed and locality-classifiable, so resolve is infallible —
//   there is no failure case to handle (§3.6, §4.3).
fn resolve<A: Actor<System = Self>>(&self, id: ActorId) -> ActorRef<A>;
```

This is design, not `unwrap()`. Keep `Result`/`CallError` for genuine transport failures;
eliminate the fake ones. And keep the two kinds apart: an application failure is a *value*
inside `M::Reply` (e.g. `Result<T, E>`), distinct from a transport `CallError` (§3.2, §14)
— the type system then forces the caller to handle the real ones.

## Decide: together or apart

Combine code that shares information, is always used together, or cannot be understood
alone. Separate unrelated concerns. Splitting adds interfaces; split only for a clean
sub-task with a simple interface. The crate layout is this judgment applied (Appendix B):
`actor-core` (the model), `actor-cluster` (the reference distributed runtime),
`actor-runtime` and `actor-simulation` (the production and test seams) — each a concern
that stands alone behind a small interface.

Pull repeated logic into one function. Avoid conjoined methods you must read in pairs.

## Comment what code cannot

Comments record what the code cannot say: rationale, contracts, invariants, units. Do not
restate code. Document the interface apart from the implementation. If the contract is
hard to describe, the design is too complex.

```rust
/// Build a typed handle to `id`. Infallible: every `ActorId` is well-formed and
/// locality-classifiable (§3.6), so resolution never fails — callers need no error arm.
fn resolve<A: Actor<System = Self>>(&self, id: ActorId) -> ActorRef<A>;
```

## Name precisely

A good name evokes one clear image and rules out the wrong ones. Use one word per concept,
consistently. A name you cannot choose signals a muddled design.

```rust
// ✗ spawn() for both raw tasks and actors blurs two distinct concepts.
// ✓ launch() a raw future; spawn() an actor — one word per concept (§4.6, §3.4).
fn launch(&self, task: BoxFuture<'static, ()>);   // Spawner
fn spawn<A: Actor>(&self, actor: A) -> ActorRef<A>;
```

## Make code obvious

A reader should understand code quickly and correctly. Replace bare booleans and bare
numbers with named types. Follow the codebase's conventions even against your preference;
consistency cuts cognitive load.

```rust
// ✗ supervise(true, 5, false)
// ✓ A named directive states intent (§11.2).
SupervisionDirective::Restart { max: 5, within: Duration::from_secs(30), backoff: Backoff::exponential() }
```

## Weigh trends and performance by complexity

A pattern, framework, or method earns its place only when it cuts complexity here. Simple
code is often the fastest and the easiest to optimize. Measure, find the critical path,
and remove work there — which usually simplifies the code. The local fast path is design,
not micro-optimization: a local send enqueues by value and skips serialization entirely
(§4.3), yet its observable result is identical to the remote path. And the framework ships
no macros of its own (§1.1) — resisting the derive-everything trend except where an
*optional* derive genuinely lowers boilerplate.

## Checklist

1. Does this cut complexity, or add it?
2. Strategic, or the smallest patch?
3. Is each module deep?
4. Does each module hide a decision? Does any leak?
5. General interface, specific implementation?
6. Distinct layers? Any pass-throughs?
7. Complexity pulled down, not pushed onto callers?
8. Any error definable out of existence?
9. Together or apart — no repetition, no conjoined methods?
10. Do comments give why and contracts, not restate code?
11. Names precise and consistent?
12. Two designs considered?
13. Obvious to a first reader, consistent with the code?

## Red flags

- **Shallow module** — interface as complex as implementation.
- **Information leakage** — one decision in two modules.
- **Temporal decomposition** — structure follows execution order, not hiding.
- **Pass-through** — a method or variable that only forwards.
- **Repetition** — the same logic in many places.
- **Special-general mixture** — a mechanism tangled with one use of it.
- **Conjoined methods** — two pieces understood only together.
- **Comment repeats code** — adds nothing.
- **Vague or hard-to-pick name** — often a design smell.
- **Nonobvious code** — understanding needs inference or distant reading.

---
*Examples are original paraphrases for design guidance, not quotations.*
