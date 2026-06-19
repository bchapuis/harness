# Agentic Harness: Specification

**Status:** Draft v5
**Scope:** A distributed agentic runtime: compound AI systems (agent loops combining model calls, tools, and control logic) run as **grains** on a mutualized cluster. An agent session is a durable, virtually-activated, single-writer object, a grain ([`granary-spec.md`](granary-spec.md)), extended with a self-driving loop and two seams: the **model** and the **sandbox**. Granary supplies identity, durability, the single-writer fence, placement, activation, and hibernation; the harness adds only the agent.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Sections of the granary specification are cited as **grain §N** (its invariants as **G1–G15**); the actor framework ([`distributed-actor-spec.md`](distributed-actor-spec.md)) as **core §N**, the cluster utilities ([`cluster-utilities-spec.md`](cluster-utilities-spec.md)) as **util §N**, and the sandbox specification ([`sandbox-spec.md`](sandbox-spec.md)) as **sandbox §N**. Sections of this document are plain **§N**. Invariants defined here are numbered **H1, H3, …** (H2 is retired, §11), kept apart from the granary catalogue (G1–G15), the core catalogue (core §18.5), and the sandbox catalogue (S1–S5).

> **Design stance.** The runtime is one idea applied twice. Granary's idea is that **an object is an actor plus three things**: a name-based virtual identity, a durable event-sourced journal, and a durability barrier on the reply. Everything else (mailboxes, serial execution, location-transparent `ask`/`tell`, membership, supervision) is inherited from the actor model unchanged (grain §1). The harness's idea is the next layer up. **An agent is a grain plus three things**: a self-driving loop, a model seam, and a sandbox. Everything else (identity, the journal, the single-writer fence, placement, activation, hibernation, failover) is inherited from the grain unchanged.
>
> The inheritance is literal, not a metaphor. A session **is** a grain: its `SessionId` is a `GrainName`, its transcript records are the grain's journal events, its in-memory state is the fold of that journal (`apply`), and its single-writer guarantee is the shard's Raft leader (grain §8), not a fence the harness builds. So the harness has no journal of its own, no placement of its own, no resume protocol of its own, and no single-writer fence of its own: the journal (grain §7), the fence (grain §8), placement and activation (grain §5, §10), and resume (grain §9) are each a granary primitive the agent rides, never machinery the harness builds.
>
> What the agent *adds* to the grain is one isolation boundary, placed between **control** and **effect**. Above it, the **loop is the grain's activation behavior**: serial, journaled, touching the world only through the model seam (§4), the sandbox seam (§5.3), and the grain's own journal. A grain is reactive: it waits for a command. An agent is **autonomous**: its activation drives itself forward, model→tools→model, until a run reaches a terminal outcome. Below the boundary, the **effects live in a sandbox**: one isolated environment per session, colocated with the grain's shard leader (grain §5.2), where tools run arbitrarily without endangering the node or another session. §5.1 places the boundary and defends the placement.
>
> An agent **remains an actor**, and the developer experience is the actor's: address a session by name and `ask`/`tell` it, location-transparently (grain §4.3); define an agent by configuring a *kind*, not by writing a loop. The loop, the seams, and the durability barrier are runtime machinery, exactly as the journal and the fence are machinery the grain author never writes. Determinism extends for free: the agent's only new seams (model, sandbox) join the grain's already-virtualized journal, clock, entropy, transport, and spawner (grain §14, core §4.6), so one seed reproduces an entire multi-node agentic run: its model outputs, tool faults, crashes, elections, and resumes.

---

## 1. Scope and layering

The harness exists to run, on **mutualized infrastructure**, the workload a single-user agent process runs today: long-lived agentic sessions interleaving model calls, tool use, and delegation. Three ideas organize it, and each is discharged by the grain layer rather than re-implemented:

- **A session is a compound AI system, not a model call** (Zaharia et al., [*The Shift from Models to Compound AI Systems*](https://bair.berkeley.edu/blog/2024/02/18/compound-ai-systems/), 2024): model calls, tools, and control logic, composed. The harness is the runtime for the composition: the loop is explicit, journaled, and supervised, while the components, model and tools, sit behind seams (§4, §5).
- **What a session keeps must live outside the loop** (Anthropic, [*Effective harnesses for long-running agents*](https://www.anthropic.com/engineering/effective-harnesses-for-long-running-agents), 2025): a long-running agent works in discrete sessions, each beginning with no memory of the last, so progress must persist in durable artifacts the next activation reads, not in a context window or a process's memory. Here that artifact is **the grain's journal** (grain §7): the session *is* its journal, the activation a disposable fold of it (grain §1, §9), and anything not journaled is by definition lost.
- **One isolation boundary, deliberately placed.** The **loop runs in the grain's activation**, cheap, serial, and effect-free outside its seams, so a session waiting on a model, a tool, or its user holds no thread and, once hibernated, holds no memory either (grain §10); the **tool effects run in the session's sandbox**, colocated with the shard leader, where they may be arbitrary. This is the *agents as infrastructure* move (Cloudflare, [*Project Think*](https://blog.cloudflare.com/project-think/), 2026): a durable, addressable session that costs nothing while idle, with effects contained by the architecture rather than by trust in the model. §5.1 defends where the boundary cuts.

Everything in this document is built **on top of** the grain and modifies neither it nor the layers beneath it:

- the **grain** (granary §1–§16) is the session: identity (`GrainName`), the event-sourced journal, the single-writer fence, placement on a shard leader, virtual activation, hibernation, and lossless failover;
- **actors, messages, supervision, death watch** (core §3–§12), which the grain itself inherits, host the activation and carry every harness interaction as an ordinary typed message with a hand-written manifest (core §4.4);
- the **event stream** (core §16, grain §13) carries the harness's observability events through the core's application-event extension point: one totally ordered stream, with the harness's vocabulary defined in the harness (§10.4).

The core non-goals (core §1.2) hold unchanged and are reinforced by the grain: the harness never transparently retries a message with side effects, never places a quorum on the message path (the grain's quorum is on the *durability* path, not the message path, grain §7.2), and never masks a failure. Where the harness *does* make a retry safe, it does so the way core §7.2 prescribes and the grain requires (grain §2.2, at-most-once): an explicit idempotency key (the `TurnId`, §7.4), not transparent retransmission.

### 1.1 Non-goals

- **A model gateway.** The harness calls one configured model implementation per request; routing across providers, A/B serving, and quota brokering are not its concern.
- **Prompt management.** System prompts and tool descriptions are application data the harness stores and transmits, not a templating system it provides.
- **Exactly-once tool execution.** A tool call is at-most-once per attempt (inherited from grain §2.2); across a crash-resume boundary the recovery semantics are explicit and per-tool (§5.5), never silent.
- **An isolation technology.** The spec mandates *that* effectful tools execute behind the `Sandbox` seam (§5.3), not *how*: process, container, or microVM is the provider's choice. A tool declares which capability tier its calls require (§5.6); the realization of each tier is the provider's, bound by sandbox §3.
- **A new consensus mechanism.** The harness adds no Raft group, no shard, and no fence. Durability, ordering, and single-writer safety are the grain's (grain §7, §8); the harness consumes them.
- **Multi-tenant economics.** Quotas, fair scheduling, and billing live above the harness (§13); budgets (§9) bound a run's spend, not a tenant's share.
- **Context-window management.** A transcript that exceeds the model's context fails the run explicitly (§4.3); compaction and summarization are future work (§13).
- **Retrieval, vector stores, UI.** Applications build these as tools (§5) or clients (§7).

---

## 2. The session is a grain

### 2.1 Anatomy

A session is one durable thing and two disposable ones, exactly the grain's own anatomy (grain §1):

- the **grain** *is* the session: its journal is the durable, totally-ordered, single-writer record log (grain §7), and its identity (`GrainName`) is permanent. It is the only component whose loss loses the session.
- the **activation** is the session's working form while active: the fold of the journal plus the in-flight run, running on the shard leader (grain §5.2). Stopping it loses nothing; the next activation, on whichever node leads the shard, rebuilds it by snapshot-plus-replay (grain §9).
- the **sandbox** is where the session's effects land: one isolated environment, colocated with the activation, holding working state that is useful but never authoritative (§5.3). Losing it never loses the session (§5.5).

Each session concept is exactly one granary primitive:

| Session concept | Granary primitive |
|---|---|
| `SessionId` | `GrainName`: `(GrainType = KindId, key)` (grain §5.1) |
| Record (a transcript entry) | the grain's `Event` (grain §3) |
| Session state, `state = fold(records)` | the grain's `State`, folded by `apply` (grain §4.1) |
| Write-ahead append, durable before effect | the grain's **output gate** (grain §6) |
| Single-writer fence | the shard's Raft leader; one leader per term (grain §8) |
| Owner node / host / placement | the shard leader / gateway (grain §5) |
| Activation, fold, resume | rehydration: snapshot + replay (grain §9, §10) |
| Idle stop | hibernation (grain §10) |
| Lossless failover | Raft leader completeness (grain §8.3, G14) |

The loop reaches the world through two harness seams, **model** (§4) and **sandbox** (§5.3), plus the **grain's journal** it inherits (grain §7), and through nothing else; time, randomness, task spawning, and transport come from the core seams the grain already uses (core §4.6, §7). One sentence carries the design: **the grain is the session; the activation and the sandbox are disposable; the seams are the only world.**

```text
                      client (any node)                               §7.4
                         │  Submit { session, turn }  →  GrainRef::ask
                         │  routed: name → shard → leader (grain §5.4)
                         ▼
                       Shard leader's gateway                         grain §5.3
                         │  get-or-activate the grain (exactly-once, G6)
                         ▼
       ┌──────── the loop: the grain's activation (§3) ────────┐
       │   state = fold(journal) · serial · no inline I/O      │
       │   autonomous: drives itself model→tools→model         │
       └────┬──────────────────┬──────────────────┬───────────┘
            │                  │                  │
            ▼                  ▼                  ▼
     Model seam (§4)   the grain journal    Sandbox seam (§5.3)
     one inference     (grain §6, §7):      ══ isolation boundary (§5.1) ══
     call per step     records, fenced,     one environment per session,
                       output-gated         colocated with the leader:
                                            shell, files, processes, network
```

### 2.2 Vocabulary

| Term | Definition |
|---|---|
| **Session** | One agent conversation, **realized as a grain**: a durable identity (`SessionId` = `GrainName`), the grain's journal, and, while active, one activation on the shard leader. Survives activation restarts, hibernation, and shard-leadership moves; an `ActorId` does not (grain §1). |
| **Record** | One journal entry, the grain's `Event` (grain §3). The transcript is the sequence of records; session state is a fold over them (§6). "Record" is this document's word for a grain event, kept because "event" is overloaded by the observability stream (§10.4). |
| **Kind** | A named agent definition (`KindId` → system prompt, toolset, sandbox profile, model parameters, default budget, delegation allowlist). The `KindId` is the session's `GrainType` (grain §5.1); kinds are registered identically on every node (§7.1). |
| **Turn** | One submitted input: a user prompt, or a parent agent's delegation (§8). Carries a client-chosen `TurnId`, the idempotency key (§7.4). |
| **Run** | The execution a turn triggers: the model→tools→model loop until a terminal outcome. Identified by its turn's `TurnId`. |
| **`RunCompleted`** | The outbound notification carrying a run's outcome (`Result<Completion, RunError>`) to each reply-to registered for it; a `tell` delivered when `RunEnded` commits (§7.3). The journal's `RunEnded` record is the source of truth; the notification is delivery. |
| **Step** | One loop iteration within a run: one model call plus the resolution of every tool call and delegation it requested. |
| **Activation** | The grain's live, in-memory instance on its shard leader (grain §5), running the loop; the unit the single-activation guarantee (G6, H6) counts. |
| **Model** | The inference seam (§4): one trait, implemented by `harness-anthropic` in production and by a scripted model in simulation. |
| **Tool** | A capability the model may invoke, declared to the harness (§5.2). Every declared tool executes inside the session's sandbox; the single built-in exception, `delegate`, executes in the loop (§8). |
| **Sandbox** | The isolated execution environment bound to one activation, behind the third seam (§5.3), colocated with the shard leader: where sandboxed tools run, and the only place their effects land. |
| **Tier** | A named capability set a tool call requires and an activation acquires, journaled before first use (§5.6); semantics in sandbox §2. |
| **Budget** | A run's spending limit (model tokens and steps) from which child budgets are carved (§9). |
| **Root** | The parentless ancestor a delegation tree descends from. Every `SessionCreated` records its `root` (§10.3); the root's `SessionId` names the tree. Correlation metadata only; nothing routes or folds on it. |

Identity is layered deliberately, and the layering is now the grain's: `SessionId` (= `GrainName`: durable, application-chosen) → `ActorId` (one activation, system-assigned, core §3.6) → `TurnId` (one run). The harness owns the first mapping only as a naming convention atop the grain; granary owns name→shard→leader resolution (grain §5.4) and the framework owns the `ActorId`.

---

## 3. The agent: an autonomous grain

A session, while active, is the grain's activation running on its shard leader (grain §5.2): an ordinary actor (core §3) whose private state is the folded transcript plus the in-flight run, and whose handlers are the only access to it (core §3.5). What distinguishes an *agent* from a plain grain is **autonomy**: a grain waits for the next command, whereas an agent's activation drives itself forward, issuing model calls and tool calls and feeding their outcomes back, until the run reaches a terminal outcome.

### 3.1 The run loop

A run advances in **steps**. Each step:

1. checks the budget (§9.1); if exhausted, the run ends with `RunError::BudgetExhausted`;
2. issues one model call carrying the transcript (§4.2);
3. if the response is a final assistant message, journals it and ends the run with `Ok(Completion)`;
4. otherwise journals the model response (intent before effect, §6.4), then executes every requested tool call (§5) and delegation (§8);
5. journals each outcome as it arrives; when all are resolved, begins the next step.

A run MUST end in exactly one terminal outcome, journaled as the run's `RunEnded` record (invariant H3): `Ok(Completion)`, `RunError::BudgetExhausted`, `RunError::Cancelled`, or `RunError::Model(…)` (a model failure no retry policy absorbed, §4.3). All four are journalable, and that is the point: a terminal outcome *is* a committed record, so durability failure cannot be one of them. A shard that cannot commit cannot record "I could not record." Durability failure is therefore not a run outcome at all but the grain's outer `GrainError::Unavailable`, which **pauses** the run rather than ending it (§6.4, §7.5). `RunError` is an application error and therefore travels as a value (core §3.2 rule 4; the grain keeps application errors inside `M::Reply`, grain §4.2), not as a transport failure. It is carried in the run's **outcome** (`outcome: Result<Completion, RunError>`), which the caller receives as a `RunCompleted` notification, **not** as the `Submit` reply (§7.3 explains why an `ask` reply cannot be held across a run's many messages). Transport and durability failure stay separate, in the `GrainError` surfaced on the `Submit` ack (§7.3).

### 3.2 The loop drives itself but never blocks on I/O

The loop is **message-driven**, the discipline the actor and the grain both impose (core §6, grain §6): the activation MUST NOT await the model, a tool, or a durable append inline. It launches each external operation through `Spawner` (core §4.6), and the outcome returns to the activation as an ordinary message; a step is a state the fold tracks, not a stack frame the executor holds. This is precisely how "the agent does I/O" coexists with the grain's pure fold: the **I/O lives in the activation's loop, never inside `apply`** (grain §4.1, which MUST be pure and is replayed). The fold stays a deterministic function of the journal; the loop is the autonomous behavior wrapped around it.

Three consequences, each inherited rather than invented:

- **The input gate orders the run.** While a record append is in flight, the grain admits no new command to that session (grain §6, the input gate). A `Cancel` (§9.2) therefore takes effect at a message boundary, not mid-append, and never observes half-committed state.
- **The output gate is the write-ahead barrier.** A model response, a tier acquisition, a tool outcome, and the terminal outcome are each released to the next step (or to a `Submit` caller) only after the record commits on a shard quorum (grain §6, the output gate). The harness's "write-ahead discipline" (§6.4) is exactly this gate; it owns no second mechanism.
- **Straggling outcomes are discarded.** The activation MUST discard, not journal, an outcome message that arrives after its run has ended (a cancelled run's late tool result, §9.2) or after the activation has stepped down on a non-contiguous commit (grain §6, the contiguity guard).

### 3.3 Supervision, restart, and migration

Ordinary supervision (core §11) governs the activation's faults, and recovery is the grain's rehydration (grain §9, §10), exercised in three guises that are one mechanism:

- a **local restart** replays the journal and continues (the factory's source of truth is the journal, not memory; core §11, grain §1);
- a **migration** follows shard leadership: when the leader changes (grain §8.3), the activation moves to the new leader and rehydrates there, losing no acknowledged record (G14);
- a **resume after node failure** is the same rehydration triggered on a new leader by the next message (§7.5).

Because all three are snapshot-plus-replay of the journal, a restarted, migrated, or resumed activation behaves byte-identically to one that never stopped, given the same subsequent model and tool outcomes (H1, the harness face of G2/G3). The harness configures `Restart` with backoff for the activation, as the grain host already does.

---

## 4. The model seam

### 4.1 The trait

```rust
/// Inference: one request, one response; no streaming in v1 (§13). The first harness seam.
pub trait Model: Send + Sync + 'static {
    async fn complete(&self, req: ModelRequest) -> Result<ModelResponse, ModelError>;
}
```

`ModelRequest` carries the kind's system prompt, model parameters, the tool declarations (name, description, input schema) of the kind's toolset, the folded transcript, and `max_tokens`. `ModelResponse` carries the assistant content, zero or more requested tool calls, and the **reported usage** (input and output tokens) that feeds budget accounting (§9.1).

The model is a seam exactly like `Transport` (core §7) and the grain's `Journal` (grain §7.3): the harness core depends only on the trait. `harness-anthropic` implements it over the Anthropic Messages API for production; the simulator supplies a **scripted model**, a deterministic function of the request and the run's seed (§12.2).

### 4.2 Determinism rules

1. A `Model` implementation MUST take all timing from `Clock` and all randomness from `Entropy` (core §4.6), including HTTP retry backoff and jitter in the production client. The harness core crate MUST NOT read the wall clock, spawn OS threads, or use an unseeded RNG (core §18.1).
2. The model's *output* is inherently nondeterministic in production. The determinism contract is therefore scoped the same way the network's and the journal's are: given the same seed, **the simulated model** returns byte-identical responses, and everything downstream of any fixed response sequence is reproducible (H1).

### 4.3 Failure

`ModelError` distinguishes, at minimum: `RateLimited`, `Overloaded`, `ContextOverflow`, `InvalidRequest`, and `Api(String)`. A `Model` implementation MAY retry requests internally, with backoff from `Clock`/`Entropy` and a bounded policy; a completion request is side-effect-free, so this does not violate core §1.2. A model failure that survives the policy ends the run with `RunError::Model(…)`: journaled, reported, never silently swallowed (core §8).

---

## 5. Tools and the isolation boundary

### 5.1 The boundary

A run splits across one deliberately placed boundary:

| In the grain's activation: the **loop** | In the sandbox: the **effects** |
|---|---|
| control flow: steps, budgets, delegation | tool execution: shell, files, processes, network |
| model calls, through the `Model` seam (§4) | working state: a workspace that is *not* session state (§5.5) |
| the fold and the journal, through the grain (grain §6, §7) | everything a tool's side effects touch |

Above the boundary, the loop touches the world only through the three seams; it is serial, journaled, and cheap: the mutualization premise (§1), and the grain's premise too (grain §7.8, where activation is a local, consensus-free operation). Below it, a tool may do anything *inside* its session's sandbox and nothing outside it. Grain/actor isolation (core §3.5) protects sessions' state from one another; sandbox isolation protects the node, and every other session's effects, from tools.

**Why the boundary cuts here.** Three other placements lose:

- **No sandbox.** Tools run on the node, as in the single-user process. The first `rm -rf` or runaway build one session's model requests takes the node (now a shard leader hosting many grains, grain §5.2) and every co-located session with it. Mutualization voids the trust assumption that made the single-user shape acceptable.
- **Loop inside the sandbox.** Ship the whole session into the box, one box per session. That is today's architecture relocated, not replaced: every session pays for an isolated environment to host control flow that is mostly waiting, and the grain's discipline (the journal, the output gate, the deterministic fold) becomes as opaque to the runtime as the tools are.
- **A sandbox per tool call.** Maximal isolation, but consecutive calls share nothing, and the checkout, build cache, or running server one step produced is exactly what the next step needs. The workspace is the unit of tool-to-tool continuity.

Hence one sandbox **per session**, opened lazily per activation (§5.3) and colocated with the shard leader where the activation already runs (grain §5.2): coarse enough to keep working state across calls, fine enough that no effect escapes the session that made it. What this placement deliberately gives up is workspace durability: the workspace is working state, not session state, and when it is lost (including on every migration, since in-memory and on-leader state never moves with the grain, grain §10), §5.5 puts the loss on the record rather than papering over it.

### 5.2 Declarations and the registry

Every tool is **declared** to the harness; the model and the loop need its interface regardless of where it executes:

```rust
pub struct ToolDecl {
    pub name: &'static str,     // stable, author-chosen; the model selects by it (cf. manifests, core §4.4)
    pub description: String,
    pub input_schema: Schema,
    pub tier: Tier,              // the capability set the call requires (§5.6); part of the kind's digest (§7.1)
    pub on_dangling: OnDangling, // the declared recovery policy for a dangling call (§5.5)
    pub timeout: Option<Duration>, // per-call bound; the harness default applies when absent (§5.3)
}

/// Resume's policy for a dangling call: intent journaled, outcome not (§5.5).
pub enum OnDangling {
    Reexecute,  // blind re-execution is safe: the call is idempotent, or dedups (`delegate`, §8.1)
    Interrupt,  // resolve as ToolError::Interrupted; the model decides whether to retry
}

/// The capability set a tool call requires (§5.6); full semantics in sandbox §2.
pub enum Tier {
    Workspace, // the session's scoped filesystem, through host-implemented typed tools; no guest code
    Compute,   // arbitrary guest code over the workspace; no network, no ambient clock or entropy
    Network,   // compute, plus egress to the profile's allowlist (§5.3)
    Native,    // OS processes and native binaries inside the confined environment
}
```

The tier names are capability sets, not rungs of a number line: although some sets contain others (`Network` adds egress to `Compute`, which adds guest code to `Workspace`), the cap that bounds a kind is a *set* of tiers rather than a maximum on a ladder. A kind MAY hold `Native` without holding `Network`, for instance (sandbox §2.2). What each tier grants and withholds, and what a provider MUST guarantee to offer it, live in the sandbox specification (sandbox §2–§3); this document carries only the contract.

A kind's `ToolRegistry` is a hand-built list of declarations, in the spirit of `HandlerRegistry` (core §4.4) and the grain's dispatch registry (grain §5.5): explicit, inspectable, and the **allowlist**. A model's tool call dispatches by name against the registry and nothing else; no path leads from model output to code outside the declared set.

Every requested tool call carries a **`CallId`**, unique within its run (the model API's tool-use id, or one the harness assigns on receipt), journaled with the call's intent (§6.4). Outcomes reference it, dangling-call resolution (§5.5) matches by it, and child-session derivation (§8.1) is keyed by it; without a per-call identity, none of the three is well-defined.

Every declared tool is **sandboxed**: its call dispatches through the `Sandbox` seam (§5.3) and executes nowhere else. The single exception is the built-in **`delegate`** (§8), which executes in the loop: a delegation is control flow (a child `Submit`, confined to the seams), not an effect. v1 deliberately exposes no extension point for loop-executing tools; one is future work (§13).

### 5.3 The sandbox seam

```rust
/// Provisioning of isolated execution environments. The third harness seam.
pub trait SandboxProvider: Send + Sync + 'static {
    type Sandbox: Sandbox;
    async fn open(&self, session: &SessionId, profile: &SandboxProfile)
        -> Result<Self::Sandbox, SandboxError>;
}

/// One environment, bound to one activation, colocated with the shard leader.
pub trait Sandbox: Send + Sync + 'static {
    /// Execute one declared, sandboxed tool call to completion inside the
    /// environment, at the call's declared tier (§5.2, §5.6).
    async fn call(&self, tier: Tier, name: &str, input: Value) -> Result<Value, ToolError>;
    /// Tear down processes and working state, across every provisioned tier. Idempotent.
    async fn release(&self);
}
```

1. **Lazy, per tier.** An activation opens its sandbox on its first sandboxed call; a session that never needs one never pays for one. Tiers are provisioned the same way: the harness passes each call's declared tier (the provider holds no registry and cannot derive it), and the provider builds a tier's environment on the first call carrying it (§5.6).
2. **Bound per activation.** At most one live sandbox per activation, and deactivation MUST release it (invariant H8), whether the deactivation is a shard-leadership move (migration), a hibernation, or a forced step-down on a non-contiguous commit (grain §6). The sandbox lives on the activation's current leader node and never migrates; a new leader opens a fresh one (§5.5).
3. **Launched, never awaited inline** (§3.2): outcomes return as messages, bounded by a per-tool timeout from `Clock`: the call's declared `timeout` (§5.2), or, when it declares none, a configurable harness default (SHOULD default to about **5 minutes**).
4. **Profiled.** The kind's `SandboxProfile` (image or toolchain, resource limits, network policy) is deployment configuration, agreed cluster-wide like the kind itself (§7.1). The profile also carries the kind's **tier cap** (the set of tiers its sessions may hold, §5.6) and, when the cap includes `Network`, the egress allowlist that tier grants (sandbox §3.3): a `TierAcquired` record grants the profile's allowlist by reference, never an inline list. Declaring a tool whose tier the cap excludes is a deployment configuration error, surfaced at registration as loudly as a duplicate name (§5.2); the cap defaults to exactly the tiers the kind's declared tools require.
5. **Technology-opaque, within a tier.** Whether a tier's environment is a process, a container, or a microVM is the provider's secret; the simulator's scripted sandbox is one more implementation of the same trait (§12). *Which capabilities a call holds* is declared per tool (§5.2), journaled per acquisition (§5.6), and agreed cluster-wide (§7.1), because a tier ladder the provider improvises is an audit trail and a policy hook nobody has. The seam still sees one `Sandbox`: tiers are provisioned within it, never as separate seam objects.

### 5.4 Tool failure is a transcript value

A failing tool does not fail the run. The harness journals its `ToolError` (a timeout, a sandbox-side crash, a failed delegation (§8.2), an unknown tool name, or schema-rejected arguments) as that call's outcome and **returns it to the model** as the tool result, for the model to react to. This defines the error out of existence at the run level: the only abnormal run endings are the three `RunError` outcomes of §3.1, and "a tool misbehaved" is not one of them. Unknown and malformed calls in particular are synthesized outcomes, not protocol failures; the registry-as-allowlist already guarantees nothing was executed.

### 5.5 Crash, loss, and resume (no silent loss)

A tool call's **intent** is journaled before execution and its **outcome** after (§6.4); a crash, hibernation, or migration between the two leaves a *dangling call*. On the next activation (grain §10, §7.5):

- a dangling call declared `OnDangling::Reexecute` MUST be re-executed, in a fresh sandbox if the old one is gone; the re-execution may fail for exactly that reason, surfacing per §5.4;
- a dangling call declared `Interrupt` MUST be resolved as `ToolError::Interrupted`, fed to the model like any tool failure (§5.4), so the *model* decides whether to retry the side effect.

A dangling call is not always dead. The grain's single-writer fence guarantees one *transcript* (grain §8), but it cannot recall a launched effect. A leader deposed but not yet aware of it (the read-your-leader window, grain §7.5) may still be *executing* a slow call when the new leader resolves it: a `Reexecute` re-execution may then run concurrently with the original, and a model answering `ToolError::Interrupted` with a retry may duplicate a side effect the old leader is still producing. The window is the duration of the in-flight call, not a message round-trip. This is the cost granary itemizes for a deposed leader (grain §11) made concrete for effects; the fence guards the record, not the world, and this specification does not pretend otherwise. (`delegate` is the one call the architecture itself rescues: its re-execution is a child `Submit` carrying the same `TurnId`, which dedups into an attach rather than a second run, §7.4, §8.1.)

The sandbox itself is **not session state**: the fold (§6, grain §1) never reads it, no record depends on its contents, and a lost workspace is never reconstructed by the harness. Anything that must outlive the sandbox leaves it through a tool: push a branch, upload an artifact, return a result the loop journals. This is core §1.2's "retries are the caller's decision" applied to effects: the harness never re-fires a non-idempotent side effect on its own authority, and never pretends a lost workspace still exists.

Nor may the *model* be left pretending. The transcript asserts a workspace the world may no longer hold (files written, servers left running), and a model resumed into a fresh sandbox would otherwise discover the loss only through a confusing downstream failure. When an activation opens a fresh sandbox for a session whose journal already records sandboxed activity (after a hibernation §7.2, a migration §3.3, an environment loss, or a crash), it MUST journal a **`WorkspaceReset`** record before the next model call and surface it to the model with that request. The record answers no `CallId`, so it cannot ride a tool result: it enters the request as input content the harness authors. The loss thus enters the record, and the model re-derives what it needs instead of acting on state that is gone.

Tier loss follows the same rule at two grains: surfaced, never silently repaired. A fresh sandbox resets **every** tier the lost activation held: `WorkspaceReset` covers the whole environment, the new activation's held set restarts at `Workspace` (§5.6), and re-acquisition is re-journaled, never silently inherited. Loss of a single tier's environment while the sandbox survives surfaces as `ToolError` outcomes on the calls that needed it (§5.4); the provider MAY re-provision that tier lazily under the acquisition this activation already journaled, and MUST NOT grant a tier the activation never journaled (sandbox §4).

### 5.6 Execution tiers

A **tier** is a named capability set a tool call requires: `Workspace`, `Compute`, `Network`, or `Native` (§5.2). This document owns the contract (the declaration, the record, the cap, the agreement), and the sandbox specification owns the semantics: what each tier grants and withholds (sandbox §2), and what a provider MUST guarantee to offer it (sandbox §3).

1. **Opening grants `Workspace` and nothing else.** The lazy open of §5.3 builds the workspace tier; a session that never acquires more holds a directory's worth of capability and no more.
2. **Acquisition is journaled, intent before effect.** Before the first call at a tier the activation does not yet hold, the loop journals a **`TierAcquired { turn, tier }`** record: the output-gate discipline (§6.4, grain §6) applied to capability acquisition. The record is the audit trail (when did this session first run guest code, first touch the network?) and the policy hook (§13), both obtained from the journal the design already has.
3. **Held tiers are additive and activation-scoped.** Within an activation the held set only grows; nothing revokes a tier. A fresh activation starts again from `Workspace` (§5.5): held tiers are working state, like the sandbox they live in, and only the journal's acquisition records survive.
4. **The cap is unreachable by construction.** Acquisition beyond the kind's tier cap cannot be requested: declared tools are checked against the cap at registration (§5.3 item 4), and dispatch reaches declared tools only (§5.2). The cap is the invariant that construction discharges (sandbox §6 S4), not a runtime check the loop performs.
5. **Agreement.** Each tool's tier and the profile's cap are covered by the kind digest (§7.1): what a session may acquire is cluster-wide agreement, never a node-local choice.
6. **A record, not an event.** Acquisition is durable, user-facing audit, so §10.1 makes it a record. No §10.4 event accompanies it; the acquisition ordering of sandbox §6 S4 is verified by journal audit of the `TierAcquired` records, not the §10.4 event stream.

---

## 6. Durability is the grain's

This document specifies **no journal seam**. The session's durability is the grain's journal (grain §7), its ordering and single-writer safety are the grain's Raft leader (grain §8), and its write-ahead barrier is the grain's output gate (grain §6). This section states only how the agent *uses* those primitives; the mechanism, the trait, and the tiers (single-node and sharded-Raft) are granary's (grain §7.3, §7.4).

### 6.1 Records are the grain's events

A session's transcript is the grain's journal: each **record** is a grain `Event` (grain §3), appended in `Seq` order, totally ordered per session, durable on a shard quorum. The record types are the harness's event vocabulary: `SessionCreated`, `TurnSubmitted`, `ModelResponse`, `TierAcquired`, `ToolOutcome` (carrying a `CallId`), `ChildRun`, `WorkspaceReset`, and `RunEnded`. `apply` (grain §4.1) folds them into session state. `apply` MUST be pure and deterministic (grain G2): it runs on the live commit path and on every replay, and the agent's only non-determinism (model output, tool effects) is captured *as committed records*, never re-derived in the fold.

### 6.2 State is a fold; the fence is Raft

A session's state MUST be a pure, deterministic function of its journal prefix, `state = fold(records)` (grain §1, G3), with no information outside the fold influencing subsequent behavior except new inputs arriving as messages. Replay is therefore resume (§3.3, §7.5).

The single-writer guarantee is the grain's, not the harness's: a session's records are appended only by its shard leader, Raft elects one leader per term, and a deposed leader cannot commit (grain §8, G1). The harness builds no fence of its own: it does not compare `after` to a head, return `Stale`, or deactivate on rejection. Single-writer safety is Raft's. Where two nodes transiently believe they lead a shard (a placement disagreement), the minority leader reaches no quorum and commits nothing, so the transcript never forks (grain §8); the contiguity guard (grain §6) steps down any activation whose in-memory head can no longer be trusted, and a forced step-down releases the sandbox (§5.3 item 2), emitting the grain's `Passivated` (grain §13) and the harness's `SandboxReleased` (§10.4).

### 6.3 Write-ahead discipline is the output gate

Within a step, the loop journals, and the grain's output gate (grain §6) holds each effect until the record commits on a quorum:

- a **model response** before any of its tool calls execute (intent before effect, §5.5), each requested call identified by its `CallId` (§5.2); a model call itself is side-effect-free and needs no intent record;
- a **tier acquisition** (`TierAcquired`) before the first tool call at a tier the activation does not yet hold (§5.6);
- a **tool or delegation outcome** before the next step shows it to the model;
- the **terminal outcome** (`RunEnded`) before releasing the reply to `Submit` (§7.3), so a caller never holds a completion the journal could lose.

The barrier "fold and reply only after durability" is grain §6 verbatim (G5); the harness adds only *which records* a step writes and in what order.

### 6.4 Durability failure

A shard that cannot reach a quorum returns `AppendOutcome::Unavailable` (grain §7.3, §11): the grain pauses writes, the activation steps down, and the in-flight record does not commit. The loop MUST NOT proceed past an uncommitted record, and, because it cannot journal a terminal outcome either, the run does not *end*; it **pauses**. The caller's pending reply resolves as the grain's outer `GrainError::Unavailable` (or the caller's `ask` deadline lapses, core §14.2), and the run stays dangling until the shard recovers and a message next reaches the session, when it resumes from the last committed record (§7.5). This is the grain's CP stance (grain §2.2, G11) inherited verbatim: the harness pauses progress, never forks a transcript, never masks the loss, and never invents a run outcome it cannot record.

---

## 7. Sessions across the cluster

A session's cluster life is the grain's life cycle (grain §10), and the journal is the only thing that survives it: **created** (first turn) → **activated** on its shard leader (rehydrate → fold → run) → **serving** (runs, §3) → **deactivated** (hibernation, migration, or step-down) → **rehydrated** by the shard's next leader (§7.5). The harness adds no placement, no host, and no resume protocol; it consumes the grain's. Each subsection below specifies one arc.

### 7.1 Kinds

Each node's harness is configured with the same `KindId → Kind` map: system prompt, `ToolRegistry`, `SandboxProfile`, model parameters, default budget, delegation allowlist (§8.1). A `KindId` is a `GrainType` (grain §5.1); the kind is the grain type's configuration (its `GranaryConfig`: shard count, replication factor, idle window, snapshot policy; grain Appendix A). Kinds are code-and-config, agreed cluster-wide like the codec (core §5): a session created with a kind MUST be resumable on any node, so every node MUST register every kind. A `SessionCreated` record pins the session's kind and a **digest** of its definition (§10.5); the digest covers, among the definition's fields, each tool's declared tier and the profile's tier cap (§5.6). Activation on a node missing that kind is a deployment error that fails the triggering call with `GrainError::Call(System)` before any run starts (nothing journaled, no `RunStarted`), never a silent fallback.

### 7.2 Placement, activation, and hibernation

A session's placement is the grain's: its name hashes to a shard, the shard map gives the shard's current leader, and the session activates on that leader (grain §5.1, §5.2, §5.4). The harness defines no host registry and no placement function: name→shard→leader resolution does it all. The gateway (grain §5.3) gets-or-activates the session exactly-once per node (G6) and routes commands to it.

The leader activates a session lazily, on the first message that reaches it: rehydrate (snapshot + replay, grain §9) → fold → run the loop. Activation takes no consensus and no network (grain §5.2, §7.8): the leader holds the shard log locally.

**Hibernation (idle stop).** After a configurable idle interval the leader MAY hibernate a session (grain §10): run nothing that mutates state, snapshot to bound the next replay, release the sandbox (H8), and drop the activation. It MUST NOT hibernate a session whose run is live. Hibernation needs no flush (anything worth keeping is already a record, §6.2), but it releases the sandbox, and the workspace with it (§5.5). `idle_after` SHOULD default to about **10 seconds**, matching the grain default (grain §10) and the Durable Objects eviction window, because reactivation is a cheap local replay. The idle timeout is therefore not a tuning detail: it is the knob trading sandbox cost, the expensive resource mutualization actually shares, against workspace continuity.

**Stale-leader redirect.** A command arriving at a node that no longer leads the shard receives `NotLeader(hint)` (grain §5.4, grain §8); the runtime refreshes its shard-map cache and retries against the hint, bounded to avoid a loop. It is ordinary Raft client redirection, and the `TurnId` (§7.4) keeps the eventual retry safe.

### 7.3 The wire contract

A session is addressed as a grain, and its commands are grain commands (grain §4.2) with hand-written manifests (core §4.4):

| Command | Reply (`M::Reply`) | Manifest |
|---|---|---|
| `Submit { session, kind, turn, reply_to: Option<ActorRef<RunCompleted>> }` | `Accepted`† | `harness.Submit` |
| `Cancel { session, turn }` | `()` | `harness.Cancel` |
| `Tail { session, from, limit }` | `Vec<(Seq, Record)>` | `harness.Tail` |

†`Submit` replies with an **ack, not the run's outcome**: `Accepted { run: TurnId, status: Started | Attached | Ended }`, released by the output gate the moment the `TurnSubmitted` record commits, an ordinary output-gated grain reply (grain §6). Each command is delivered through `GrainRef::ask` (grain §4.3), so the call surfaces `GrainError` (transport, `NotLeader`, `Unavailable`, `Unhandled`, grain §12) outside the reply. `Cancel` likewise commits one `RunEnded { Cancelled }` record and acks; `Tail` emits no record and reads (grain §6, §7.5).

The run's outcome travels separately, as an **outbound notification** the activation `tell`s when the run's `RunEnded` commits:

| Notification | Payload | Delivered to |
|---|---|---|
| `RunCompleted` | `{ session, turn, outcome: Result<Completion, RunError> }` | every reply-to `ActorRef` registered for the run |

`RunCompleted` is a `tell` (core §3.3): fire-and-forget, at-most-once. It is **delivery, not the source of truth** (the durable `RunEnded` record is, §6, §10.1), so a lost notification is recovered by the caller re-contacting the session (§7.4), never by a transparent retry (core §1.2). It is a delivery message, not a §10.4 observability event: `RunEnded` already marks termination on the checker stream.

**Why the outcome is a notification, not the `Submit` reply.** A reply to an `ask` is an actor `ReplyHandle` (core §4.5): synchronous, resolved exactly once *within the delivery of that one message*, and **not storable across later messages**. But a run spans many inbound messages (each model and tool outcome, §3.2) between `Submit` and `RunEnded`, so the run's `Completion` *cannot* be the `Submit` reply: there is no handle to hold across that gap. This is not a stylistic choice; it is what the primitive permits. So `Submit` acks the moment its `TurnSubmitted` commits, and the outcome is delivered later to a reply-to `ActorRef` the caller placed in the command, the application-level reply-to pattern the framework intends (core §3.3: "An `ActorRef` MAY be a field of a message"). This is the agent being message-driven (§3.2), not new machinery: completion arrives as one more message into a loop, exactly like every tool outcome, and "attaching to a live run" (§7.4) is just registering another reply-to.

The `Submit` `ask` returns promptly (it commits one record), so its deadline is rarely the interesting bound. What bounds a *caller waiting for the outcome* is that caller's own patience for the `RunCompleted` notification, and a run answers to neither: budgets (§9) bound its tokens and steps, not its wall time (tool timeouts, model retries, and child runs compound), so a run MAY outlive any caller. A caller that gives up, times out its wait, or crashes leaves the run untouched; it recovers the outcome by re-submitting the same `TurnId` to re-register (§7.4) or by following `Tail` (§10.2). There is deliberately no status message: a status view (head `Seq`, live run if any, spend so far) is a client-side fold over `Tail`'s records, not a second reply type.

`Submit` carries `kind` on every call though it binds only on the first: creation is implicit in a session's first turn, and a separate create message would buy a two-phase client protocol to remove one field. After creation the field is a checked redundancy, rejected on mismatch (§7.4).

A read-only command (`Tail`, and any future query that emits no record) commits nothing (grain §6 step 2, §7.5) and is served from the leader's in-memory activation: a local, replication-free read. Its consistency is **read-your-leader**, not linearizable under partition (grain §7.5): an isolated minority leader, deposed but not yet fenced, MAY serve a slightly stale `Tail` until its activation steps down. Writes never fork; only these reads can be momentarily stale, and only on the minority side. A linearizable read is a deferred grain upgrade (grain §7.5, §16), not a harness mechanism.

### 7.4 The client view (`SessionRef`) and idempotent submission

```rust
let h = Harness::new(system, kinds, model, sandboxes);
//   per node: registers each kind's gateway and injects the model and sandbox seams (§4, §5.3);
//   the journal is the grain's, configured per kind (§7.1), not a harness seam.
let s = h.session("researcher", SESSION_ID);       // a GrainRef<Agent>; name→shard is a pure local hash,
                                                   //   no I/O, no failure case (grain §5.1, §5.4)
let out = s.prompt(Turn { id: TURN_ID, content }).await;   // blocking convenience: Submit (ack) + await
                                                   //   the RunCompleted notification (§7.3); presents
                                                   //   Result<Result<Completion, RunError>, GrainError>
```

`SessionRef` is `GrainRef<Agent>` (grain §4.3) with a thin agent-facing surface. `prompt(turn)` is a **blocking convenience composed at the edge, not a protocol primitive**: it spins an ephemeral local actor as the reply-to, `Submit`s the turn carrying that ref, awaits the one `RunCompleted` message, and presents `Result<Result<Completion, RunError>, GrainError>`. If its own wait lapses, it re-`Submit`s the same `TurnId` to re-register and keeps waiting. A caller that prefers not to block uses `Submit` with its own reply-to (a parent agent does exactly this, §8.1) or observes through `tail`; the blocking shape is one option layered over the message protocol, never baked into it.

`prompt` MUST NOT transparently retry a failed `Submit` (core §1.2, grain §2.2). The grain *does* re-issue a command that provably did not run, such as one to a hibernated stale host (`DeadLetter`) or a moved leader (`NotLeader`), neither of which commits (grain §4.3, §6); but an *ambiguous* failure (`Unreachable`/`Timeout`) is never auto-retried, because the command may have committed before the ack was lost. The **`TurnId`** makes the caller's own re-submission safe across that ambiguity:

- a `Submit` whose `TurnId` is already journaled MUST NOT start a second run (one `TurnSubmitted` record, G1): if the run is still live, the activation **registers the new reply-to** on it (`status: Attached`), to notify at `RunEnded`; if the run already ended, the activation immediately **`tell`s the recorded outcome** (`status: Ended`), read from the `RunEnded` record. The dedup, and the recorded outcome, are a fold over the session's own records, so both are exact even across a resume;
- the registration is race-free: the serial activation processes a re-submission and the run's `RunEnded` transition in order (the input gate, grain §6), so a caller is either added to the live run or told the finished outcome: never neither, never both;
- the `reply_to` is **transient routing, not part of the turn**: it is not journaled and is **excluded from the content-equality check**, so re-submitting the same `TurnId` with a fresh reply-to is the ordinary re-attach, not a mismatch. A re-submission whose turn *content* differs, or whose `kind` differs from the journaled `SessionCreated`, is a caller bug, rejected with `GrainError::Call(System)` and journaling nothing.

The **subscriber set** (the reply-tos registered for a live run) is ephemeral activation state: like the held tiers and the sandbox (§5.5, §5.6), it is rebuilt by callers re-contacting after a resume, never journaled. The `TurnId` is the idempotency key, and the outcome is delivered by notification rather than a held reply: the grain is at-most-once and never auto-retries effectful commands, so the agent layer owns the key.

### 7.5 Resume after node failure

When a session's shard leader fails or is partitioned away, the shard's replicas elect a new leader by ordinary Raft (grain §8.3); the new leader already holds every committed record (leader completeness, G14). The next message routed to the shard activates the session there: rehydrate (grain §9), fold (§6.2), dangling-call resolution (§5.5), `WorkspaceReset` if the journal records prior sandbox activity (§5.5), and continuation of any unfinished run. No coordinator hands the session over: placement is the shard map, the journal is the state, and Raft is the fence (§6.2). No acknowledged record is lost (G14, H1).

Resumption is **caller-driven**: the grain does not scan for orphaned work (grain has no cross-grain enumeration), so a run interrupted by a crash resumes only when a message next reaches the session: the re-submitted `TurnId` of a caller that wants its answer, a parent's re-executed delegation (§8.1), a `Cancel`. A run nobody asks after stays interrupted, and its budget bounds what it can ever cost (§9.1). H3's liveness is conditional on exactly this contact.

---

## 8. Sub-agent trees

### 8.1 Delegation is a tool

The harness provides one built-in tool, `delegate`, the only tool that executes in the loop rather than the sandbox (§5.2), present in a kind's registry iff the kind permits sub-agents. Its input names a child kind plus a prompt and, optionally, a budget request. The child kind MUST belong to the parent kind's **delegation allowlist** (§7.1): naming any other kind is a synthesized `ToolError` (§5.4), so a locked-down kind cannot escalate its privileges by delegating to a permissive one. Its execution is a child `Submit`:

1. the parent journals the delegation (a `ChildRun` record: the child `SessionId` and the child turn's `TurnId`, both derived deterministically from the parent session, the parent's `TurnId`, and the delegation's `CallId` (§5.2), plus the carved budget, §9.1; one run may delegate many times, so the call, not the turn, is the unit of derivation); a re-executed delegation re-derives the same pair (§5.5), and cancel propagation reads it from the record (§9.2);
2. the launched task `Submit`s to the child session (a `GrainRef`) carrying a **reply-to** ref and **awaits the child's `RunCompleted`** notification (§7.3); it never block-`ask`s the child, whose run may be arbitrarily long. The child is a **full session**: a grain in its own right, journaled, budgeted, placed on its own shard wherever the cluster puts it (grain §5), supervised on its leader;
3. the child's outcome arrives as one more message into the parent's loop (§3.2) and becomes the tool's outcome (§5.4): journaled, then shown to the parent's model.

Delegation thereby inherits every property of §7 (which is to say every property of the grain) with no new machinery: a child on a crashed leader resumes on the new leader (§7.5); a parent that crashes and resumes finds the delegation dangling and, since `delegate` is declared `Reexecute`, re-executes it; the child's journaled `TurnId` dedups the re-submission and re-registers the parent's fresh reply-to (§7.4), so the outcome is delivered whether the child is still running or already done. The retry is safe *because* of the idempotency key, not despite the at-most-once transport.

### 8.2 Tree shape and failure

- The tree is recorded in the grains' journals (`ChildRun` / `SessionCreated.parent`), not in any replicated structure; there is no global tree view to keep consistent. Each grain is its own consistency boundary (grain §2.2): the harness builds no cross-grain transaction.
- Fan-out is bounded by the budget: each delegation carves from the parent's remaining budget (§9.1), so a tree's total spend is bounded by the root's budget regardless of depth or width (H4).
- A child's failure is a tool outcome (§5.4): the parent's model sees `RunError` and decides. Failures never propagate as supervision faults across the tree: supervision is local (core §11.3), and the tree spans shards and nodes.

---

## 9. Budgets and cancellation

### 9.1 Budgets

```rust
pub struct Budget { pub tokens: u64, pub steps: u32 }
```

1. Every run has a budget: the turn's explicit budget, else the kind's default. Spend is the model-**reported** usage (§4.1) summed over the run's calls, plus the spend of its children. Spend is therefore **a projection of the journal, not separately-tracked state**: each `ModelResponse` record carries its call's usage and each `ChildRun` its carve-out, so current spend is recomputed by the fold (§6.2) like all session state. A resumed activation re-derives spend exactly by replay; nothing about budget accounting survives deactivation outside the records (H1).
2. **Pre-call enforcement.** A model call is issued only while `spent < tokens` and the step count is below `steps`; the request's `max_tokens` MUST be clamped to the remaining token budget, and no call may be issued while the remainder is below a configurable floor. Output overshoot is therefore zero, and total overshoot is bounded by one call's input, a bound that grows with the transcript and is stated honestly: a call's input size is known only on response. Exhaustion ends the run with `RunError::BudgetExhausted` (§3.1): journaled, reported, recoverable by a new turn with a new budget.
3. **Carve-outs.** A delegation reserves an explicit slice of the parent's remaining budget; the child enforces its slice locally, with no cross-shard accounting protocol. Hence the bound is compositional: own spend + Σ carve-outs ≤ budget, at every node of the tree (H4).
4. **What the bound is.** Spend is *journaled* usage. A model call whose response never commits (issued by a leader deposed mid-step, whose record loses to the contiguity guard, grain §6, §6.2) is discarded uncounted, though the provider bills it: the budget bounds the session's recorded spend, and the provider's invoice exceeds it by exactly the speculative work a leadership change cost.

### 9.2 Cancellation

1. `Cancel` is a message (§7.3); because handlers never block on I/O (§3.2) and the input gate admits it between appends (grain §6), it takes effect at the next message boundary, not after the current model call returns. It names the run it cancels (`turn`): a `Cancel` naming an ended or unknown run is an idempotent no-op, so one delayed in flight never kills the named run's successor.
2. On cancel, the session journals `RunEnded { Cancelled }`, notifies every registered reply-to with `RunCompleted { outcome: Err(RunError::Cancelled) }` (§7.3), discards subsequent outcome messages of that run (§3.2), and **propagates**: it sends `Cancel { session, turn }` to every live child recorded in the journal (the `ChildRun` record names the child's session and turn, §8.1), each of which cancels its own subtree.
3. Propagation is at-most-once per attempt (core §7.2, grain §2.2); a lost `Cancel` is not retried transparently. The guarantee is two-layered: once faults cease, propagation completes within bounded logical time (H5); under unhealed faults, the child's **budget is the backstop**, since every run is bounded with or without the cancel arriving (§9.1).
4. In-flight side effects of a cancelled run (a tool mid-execution, a model call mid-flight) are not undone and MAY complete externally; their outcomes are discarded. The harness reports what it stopped; it does not pretend to have un-run it.

---

## 10. Observability

In an agentic harness, observability is a product feature, not a diagnostic afterthought: operating agents means seeing what the model said, which tools ran, what a run cost, and how a delegation tree unfolded. The harness gets this nearly for free, because the design already centralizes the one record that matters: the grain's journal.

### 10.1 The journal is the record

The grain's journal (grain §7) is the single source of truth for what a session did, and every observation API is normatively a **read of it**. The harness MUST NOT keep a second durable account of session activity: a parallel trace or audit store would restate the journal's decision in a second module, and the two would drift. The division of labor:

- **Records** are durable and user-facing: the transcript, the tool calls and outcomes, the costs, the tree links. They *are* the grain's events (§6.1). If an observer needs it, it is a record.
- **Events** (§10.4) are ephemeral and checker-facing: they describe the *machinery around* sessions (activation, leadership, run pairing) so the H-invariants are checkable over a run's stream (core §16, grain §13). They carry nothing about a session's content.

Each record carries the leader's `Clock` reading at write time. The timestamp is observational metadata: the fold (§6.2) MUST NOT let it influence behavior. Under simulation it is virtual and therefore deterministic (core §18.1), so timestamped journals still reproduce byte-identically.

### 10.2 Reading a run (`Tail`)

`Tail { session, from: Seq, limit } → Vec<(Seq, Record)>` (§7.3) returns at most `limit` committed records after `from`: an idempotent, replication-free read served from the leader's activation (grain §7.5). It is read-your-leader, not linearizable under partition (§7.3): the honest cost is that an isolated minority leader serves slightly stale tails until it steps down, and an unreachable shard takes observation of its sessions with it until a new leader is elected; push-based observation (§13) is where that coupling gets revisited. Polling `Tail` follows a live run at whatever cadence the client chooses; `Submit`-and-await remains the one-shot form.

A `Tail` MUST NOT trigger a write or change activation lifecycle beyond an ordinary read; the leader serves it from the activation (or, after hibernation, by a local journal read, grain §7.3 `load`). Push-based observation is future work (§13); `Tail` is deliberately the smallest interface that makes a run watchable.

### 10.3 Tree correlation

Every `SessionCreated` records its session's **root**: its own `SessionId` for a session created with no parent, the parent's `root` for a session created by delegation (§8). The field is the transitive closure of the parent links, denormalized so any record, event, or log line can name its logical request in O(1). Together with the recorded parent links, `root` stitches one delegation tree across shards, nodes, journals, and logs: the harness-level instance of the trace propagation core §16 recommends, with no identifier minted for it. The root's `SessionId` *is* the trace. Mapping trees onto external trace systems is future work (§13).

### 10.4 Events (checker-facing)

Harness events are defined **in the harness** as their own typed enum and ride the core stream through its application-event extension point (core §16's `Event::App`), alongside the grain's own events (`Activated`, `Passivated`, `LeaderChanged`, `Committed`, …; grain §13): the core owns the mechanism, never the vocabulary. Checkers recover them by downcast, observing one totally ordered sequence; the seed-reproducibility contract (core §18.1) covers them for free.

These events deliberately restate information the journal already holds (a `RunStarted` echoes a `TurnSubmitted` record, a `ModelCompleted` echoes a `ModelResponse`'s usage), which is the one sanctioned exception to §10.1's single-source rule. Its justification is narrow: a continuous checker observes the *totally-ordered core stream*, not the per-session journals behind the seam, so an invariant about run pairing or spend has to ride the stream to be checkable (granary makes the same trade for `Committed`, grain §13). The vocabulary is therefore kept to the minimum a stream checker cannot reconstruct from the grain's own events: emit nothing whose check could read the grain's `Activated`/`Passivated`/`LeaderChanged` instead:

| Event | Fields | Meaning |
|---|---|---|
| `RunStarted` | `session`, `turn`, `parent?` | A run began for a newly journaled turn. |
| `ModelCompleted` | `session`, `turn`, `node`, `usage` | One model call finished; `usage` feeds the H4 checker, scoped to journaled spend (below). |
| `RunEnded` | `session`, `turn`, `outcome` | The run's exactly-one terminal outcome was journaled. |
| `SandboxBound` | `session`, `node` | The activation opened its sandbox (first sandboxed call, §5.3). |
| `SandboxReleased` | `session`, `node` | That sandbox was torn down (hibernation, migration, loss, or step-down). |

The sandbox pair is the load-bearing addition: it is *not* derivable from records, because the sandbox is not session state (§5.5). Run boundaries (`RunStarted`/`RunEnded`) and per-call `usage` (`ModelCompleted`) are the minimum needed to check H3/H4/H7 on the stream; everything else about a session's lifecycle is read from the grain's events.

Session activation, deactivation, and the single-writer fence are observed through the **grain's** events, not duplicated here: `Activated`/`Passivated` mark the activation lifecycle (grain §13), `LeaderChanged` marks a migration, and the absence of a forked log is G1, checked over the grain stream (grain §14). The harness's `SandboxBound`/`SandboxReleased` MUST alternate within the grain's `Activated`/`Passivated` window for that session and node (the H8 effect-containment check), and `SandboxReleased` MUST follow any `Passivated` with no intervening sandboxed call.

`RunStarted` fires exactly once per `(session, turn)`, on the activation whose append commits the turn; a resume (§7.5) emits **no** second `RunStarted`: a checker recognizes a resume as the grain's `Activated` followed by `ModelCompleted` on an already-started turn, so the harness mints no `RunResumed` event of its own. That keeps the H3 pairing and H7 counting checkers sound across crashes, and under a leadership race their soundness is the grain's fence (G1): the turn commits once, so one start is counted even when two leaders briefly raced to begin it. `ModelCompleted` carries its `node` so the H4 checker attributes each call to its enclosing activation and excludes one whose response never committed (a deposed leader's discarded work, §9.1.4).

### 10.5 Reconstruction and derived metrics

Because session state is a pure fold (§6.2, H1) and a model request is a deterministic function of that state (§4.1), the exact `ModelRequest` issued at any step is **reconstructible from the journal prefix**: debugging a production prompt needs no request logging, only the journal and the code. Reconstruction is faithful only against the same agent definition, which is why `SessionCreated` records a digest of the kind (§7.1): a reader can tell whether a reconstruction is exact, or merely indicative because a deployment changed the kind mid-session.

Aggregate metrics (spend per kind and per tree, grouped by `root`; run latency; tool failure rates; activation and leadership churn) are RECOMMENDED, not REQUIRED, and SHOULD be **derived** from records and events rather than instrumented separately.

---

## 11. Conformance

The harness catalogue mirrors the granary, core, and utilities catalogues and is machine-readable alongside the harness's conformance suite (`harness_catalogue()`), guarded by the same drift-test pattern. It lists only what is specific to the *agent*; the grain's own guarantees are cited in the grain-basis column rather than re-proved here. The deterministic fold (G2/G3) and single activation (G6) reappear as the agent-level H1 and H6 (restated because the agent adds resume-equivalence and autonomy to them), while the single-writer fence (G1) has **no** harness invariant at all: it is wholly the grain's, appearing only as the basis other agent invariants build on (H6's one leader, H7's single commit), never an obligation the harness discharges. (**H2** is intentionally retired, an earlier write-ahead invariant since absorbed into the grain's output gate (§6.3, G5); the numbers H3–H8 are kept stable rather than renumbered.)

| # | Invariant | Defined in | Verified by | Grain basis |
|---|---|---|---|---|
| H1 | **Deterministic fold and resume.** Session state is a pure fold of the journal; a session resumed from any committed prefix behaves byte-identically to one that never stopped, given the same subsequent model and tool outcomes. | §6.2, §7.5 | differential resume-vs-uninterrupted test; seed-reproducibility sweep | G2, G3 |
| H3 | **Run termination.** Every `RunStarted` is followed by exactly one `RunEnded`; once faults cease, partitions heal, and a message reaches the session, no run remains pending past its budget's bound. The `RunEnded` record is the single source of truth; the `RunCompleted` notification is best-effort (`tell`), and a lost one is recovered by re-contact (§7.4), never retried. | §3.1, §7.5, §9 | continuous checker (pairing); swarm sweep (bounded completion under caller re-submission) | G14 (lossless failover) |
| H4 | **Budget bound.** A run issues no model call after exhaustion; output spend never exceeds the remaining budget at call time; own spend plus children's carve-outs never exceeds the budget, at every level of a delegation tree. | §9.1 | continuous checker over `ModelCompleted`, scoped to journaled spend (§10.4); tree scenario tests | (none) |
| H5 | **Cancellation.** After a cancel is journaled, the run and, once faults cease, every descendant run end `Cancelled` within bounded logical time, issuing no further model calls. | §9.2 | scenario + swarm tests; checker for post-cancel `ModelCompleted` | (none) |
| H6 | **Single autonomous activation.** A node never runs two concurrent activations of one session; on a converged cluster, at most one activation per session is live; an addressed session activates within bounded logical time of a message reaching its shard leader. | §7.2 | inherited grain checker (G6) plus the agent-liveness scenario | G6, grain §8 |
| H7 | **Idempotent submission.** A re-submitted `TurnId` never starts a second run: it registers the caller's reply-to for notification, or `tell`s the recorded outcome if the run already ended, under any injected duplication or caller retry. | §7.4 | continuous checker (one `RunStarted` per `(session, turn)`); retry scenario tests | G1 (one commit) |
| H8 | **Effect containment.** An activation binds at most one live sandbox and releases it on deactivation; every sandboxed tool call executes in the sandbox bound to its issuing activation, never another session's; sandbox loss surfaces in the journal (`ToolError` outcomes, a `WorkspaceReset` record, §5.5), never as silent corruption. | §5.3, §5.5 | continuous checker (`SandboxBound`/`SandboxReleased` within the grain's activation window, calls within the bind window); crash/loss scenario tests; cross-session isolation by construction | grain §5.2 (colocation) |

The tier obligations of the sandbox seam carry their own catalogue, **S1–S5** (sandbox §6). The harness is also held to the core and granary testing contracts (core §18.1, §18.3; grain §14): seed-reproducibility of the full event stream including harness events, and fault-coverage accounting proving that model faults, sandbox faults, and the grain's own faults (leader crash mid-write, shard quorum loss, eviction mid-command) actually fired while agent traffic flowed.

---

## 12. Testability and deterministic simulation

### 12.1 Seams

The harness adds **two** rows to the table the grain already virtualizes (grain §14, core §18.2); the journal is not among them, because it is the grain's:

| Seam | Production | Simulation |
|---|---|---|
| `Model` (§4) | `harness-anthropic`: Anthropic Messages API over HTTPS; retry backoff from `Clock`/`Entropy` | **scripted model**: a deterministic function of the request and the seed; emits final messages, tool calls, malformed calls, and faults under seed control |
| `Sandbox` (§5.3) | a deployment-supplied `SandboxProvider` (process, container, or microVM; §13) | **scripted sandbox**: deterministic outcomes per call and seed; seeded open failures, latency, crashes, environment loss, and tier-provisioning failure |
| *(journal)* | *the grain's `Journal` (grain §7.3, §7.4): single-node or sharded-Raft* | *the grain's simulation seam (grain §14); the harness drives the real consensus code, not a model of it* |

The `harness` crate itself MUST satisfy core §18.1: no wall clock, no OS threads, no unseeded randomness; all I/O launched through `Spawner` (§3.2). `harness-anthropic` is production-only and the single place HTTP exists; production sandbox providers are likewise separate crates. Because the journal, the fence, placement, and resume are the grain's, simulating the harness *runs the real granary host, gateway, shard, and rehydration code* (grain §14) under the same seed. The agent's loop is the only new code on the simulated path, joined to consensus that is already exercised by granary's own suite.

### 12.2 Fault injection

Under seed control, a simulated harness run MUST be able to inject at least, on top of the core and grain faults (core §18.3, grain §14):

- **Model:** latency, `RateLimited`/`Overloaded` bursts, `ContextOverflow`, unknown tool names, schema-invalid arguments, pathological tool-call loops (exercising budgets).
- **Sandbox:** open failure, execution latency, a crash mid-call (dangling calls, §5.5), loss of the environment between steps (forcing a fresh sandbox and `WorkspaceReset`), and provisioning failure at an acquired tier.
- **Grain (inherited):** shard-leader crash and election mid-write (the fence, G1; a run resuming on the new leader, §7.5), shard quorum loss (`Unavailable`, §6.4), eviction mid-command (rehydration and the output gate), and a shard split under concurrent writes (grain §7.7).
- **Topology:** leadership moves under partition and heal (the dangling-effect race, §5.5), cancellation racing completion across the tree.

A run with no faults is the simplest case and MUST still pass.

---

## 13. Future work

- **Durable alarms for the agent.** A stored timer that re-activates a session to advance a run with no caller present (grain §16, durable alarms): the basis for scheduled runs, server-side timeouts, and proactive agents. v1's resumption is strictly caller-driven (§7.5); alarms would lift that.
- **Scheduler singleton.** Queued and recurring agent runs feeding ordinary `Submit`s; deliberately out of v1, and a natural client of durable alarms above.
- **Linearizable and follower reads.** A `Tail` (or a future query) that reflects committed state on the minority side of a partition, via the grain's deferred leader-lease upgrade (grain §7.5, §16); follower reads for read scale likewise ride the grain's extension.
- **Sandbox providers, snapshots, and pooling.** The first per-tier provider ships as `harness-sandbox` (sandbox §3.1–§3.5). Still open: a network dataplane behind the profile's allowlist (sandbox §3.3), workspace snapshot/restore across migration (the one piece of working state the grain's hibernation does *not* preserve, §5.5), and warm pools to cut open latency.
- **Multi-tenant scheduling.** Quotas, fair-share scheduling, and accounting across tenants sharing the cluster: the economics of mutualization (§1.1).
- **Context compaction.** Summarizing the transcript into a journaled checkpoint record so the fold, and the model request, start from it; must preserve H1. The grain's snapshot (grain §9) bounds *replay* cost; compaction bounds *context* cost, a separate concern.
- **Streaming.** Token streaming from `Model`, and push-based run observation: a subscription complement to `Tail` (§10.2).
- **Permission gating.** A per-tool authorization hook between intent and execution, the application-level analogue of the grain's per-`(peer, GrainName, manifest)` `Authorizer` (grain §13, core §15). The tier cap (§5.6) is the static half of authorization; this hook is the dynamic, per-call half.
- **Loop-executing tools.** A general extension point for tools that run in the loop with effects confined to the seams (ask-the-user, journal queries); v1's only loop-executing tool is the built-in `delegate` (§5.2).
- **Code mode.** Exposing the toolset as an API the model *programs against* rather than calls tool-by-tool, the generated program executing at the `Compute` tier (§5.6): fewer declaration tokens, fewer loop round-trips, same boundary.
- **External trace interop.** Mapping `root`/parent links (§10.3) onto W3C trace-context or OpenTelemetry ids at the boundary of an external collector.
- **Cross-session sagas.** Multi-session workflows above the single-session consistency boundary, built with idempotency the way the grain builds cross-grain sagas (grain §16).

---

## Appendix A: End-to-end example

```rust
// --- A kind: prompt, declared tools, sandbox profile; identical on every node (§7.1).
// --- The KindId is the grain type (grain §5.1); the registry is the allowlist (§5.2).
// --- `shell` and `read_file` execute in the session's sandbox (§5.3) at their declared
// --- tiers (§5.6); `delegate` executes in the loop (§8). The tier cap defaults to exactly
// --- what the declared tools require, here {Workspace, Native} (§5.3 item 4).
let researcher = Kind::new("You are a research agent.")
    .model(ModelParams::default())
    .sandboxed(Tier::Native, "shell", "Run a command in the workspace", &SHELL_SCHEMA)
    .sandboxed(Tier::Workspace, "read_file", "Read a file from the workspace", &READ_SCHEMA)
    .delegates_to(&["researcher"])          // the built-in `delegate` tool; allowlisted child kinds (§5.2, §8)
    .sandbox(SandboxProfile::image("workspace:base"))
    .grain(GranaryConfig {                  // the kind IS a grain type's config (§7.1, grain App. A)
        shards: 16, replication_factor: 3,
        idle_after: Duration::from_secs(10), snapshot_every: 256,
    })
    .budget(Budget { tokens: 200_000, steps: 50 });

// --- Per node: one Harness over the clustered actor system + granary; the model and
// --- sandbox seams injected here, the journal supplied by granary per kind (§7.4, §12.1).
let h = Harness::new(system, Kinds::new().register("researcher", researcher),
                     model, sandboxes);

// --- Any node: address the session by name; the shard leader hosts it (§7.2) ---
let s = h.session("researcher", SessionId::new("report-42"));   // GrainRef<Agent>
// prompt() = Submit (immediate ack) + await the RunCompleted notification (§7.3, §7.4);
//   the run outlives this call freely; re-prompt the same TurnId to re-attach.
match s.prompt(Turn::new(TurnId::new("t-1"), "Summarize the corpus on X.")).await {
    Ok(Ok(completion))                  => println!("{}", completion.text()),
    Ok(Err(RunError::BudgetExhausted))  => { /* grant a larger budget, resubmit a new turn */ }
    Ok(Err(run_err))                    => eprintln!("run failed: {run_err:?}"),   // Model | Cancelled
    Err(GrainError::Unavailable(_))     => { /* shard quorum lost; the run PAUSED, not ended;
                                                re-submit the SAME TurnId on recovery (§6.4, §7.5) */ }
    Err(GrainError::NotLeader(_))       => { /* retries exhausted; refresh shard map and retry */ }
    Err(GrainError::Call(Unreachable | Timeout))
        => { /* safe to re-submit the SAME TurnId: H7 dedups or attaches (§7.4) */ }
    Err(e)                              => eprintln!("call failed: {e:?}"),
}
```

## Appendix B: Crate and module layout

```
harness/                # the agentic harness (this spec): an agent is a grain (§2)
  agent.rs              #   the Agent grain: Grain/GrainHandler impls, the run loop,
                        #     steps, the fold over records (§3, §6)
  session.rs            #   SessionId (= GrainName), TurnId, Turn, Record (= the grain Event) (§2, §6)
  model.rs              #   Model trait, ModelRequest/Response/Error (§4)
  tool.rs               #   ToolDecl, ToolRegistry, the built-in `delegate` (§5.2, §8)
  sandbox.rs            #   Sandbox + SandboxProvider seams, SandboxProfile, Tier (§5.3, §5.6)
  kind.rs               #   Kind, Kinds, the kind→grain-type/GranaryConfig binding (§7.1)
  client.rs             #   Harness + SessionRef (= GrainRef<Agent>): addressing, routing, and the
                        #     ephemeral reply-to actor behind the blocking `prompt` convenience (§7.4)
  budget.rs             #   Budget, spend accounting, carve-outs, cancellation (§9)
  event.rs              #   the harness's checker-facing Event vocabulary (§10.4)

harness-anthropic/      # production Model only: Anthropic Messages API client;
                        #   backoff via Clock/Entropy; the single place HTTP exists (§12.1)

harness-sandbox/        # tiered SandboxProvider (sandbox §3): Workspace by cap-std capability
                        #   handle, Compute by hermetic wasmtime guests (raw modules, or
                        #   JavaScript via embedded QuickJS, feature-gated), Native by an OCI
                        #   container or a Firecracker microVM (feature-gated); ships s_catalogue()
```

`harness` depends on `granary` (the grain: identity, journal, fence, placement, activation, hibernation), and through it on `actor-core`, `actor-cluster`, and `actor-serialization`. It defines **no journal, no placement function, no fence, and no resume protocol** of its own; each is consumed from granary. The single durable thing the harness names is the `Record` type (the grain's event) and its `apply` fold. Test-only pieces (the scripted model, the scripted sandbox, `harness_catalogue()`, the conformance suites) live with the harness's tests, dev-depending on `actor-simulation` and granary's simulation seam. The crate observes the workspace conventions: edition 2024, `unsafe_code = "forbid"`, `clippy::all = "warn"`, serde derives only.
