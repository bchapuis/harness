# Agent: Specification

**Status:** Draft v5
**Scope:** A distributed agentic runtime: compound AI systems (agent loops combining model calls, tools, and control logic) run as **entities** on a mutualized cluster. An agent session is an entity ([`entity-spec.md`](entity-spec.md)): a durable, virtually-activated, single-writer object with a self-driving loop and two seams, the **model** and the **sandbox**. The entity layer supplies identity, durability, the single-writer fence, placement, activation, and hibernation; the agent runtime adds only the agent.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Sections of the entity specification are cited as **entity §N** (its invariants as **E1–E21**); the actor framework ([`distributed-actor-spec.md`](distributed-actor-spec.md)) as **core §N**, the cluster utilities ([`cluster-utilities-spec.md`](cluster-utilities-spec.md)) as **util §N**, and the sandbox specification ([`sandbox-spec.md`](sandbox-spec.md)) as **sandbox §N**. Sections of this document are plain **§N**. Invariants defined here are numbered **A1, A3, …** (there is no A2, §11), kept apart from the entity catalogue (E1–E21, plus the facet contract F1–F4), the core catalogue (core §18.5), and the sandbox catalogue (S1–S5).

> **Design stance.** In the entity layer, **an object is an actor plus three things**: a name-based virtual identity, a durable event-sourced journal, and a durability barrier on the reply. Everything else (mailboxes, serial execution, location-transparent `ask`/`tell`, membership, supervision) is inherited from the actor model unchanged (entity §1). **An agent is an entity plus three things**: a self-driving loop, a model seam, and a sandbox. Everything else (identity, the journal, the single-writer fence, placement, activation, hibernation, failover) is inherited from the entity unchanged.
>
> What the agent *adds* to the entity is one isolation boundary, placed between **control** and **effect**. Above it, the **loop is the entity's activation behavior**: serial, journaled, touching the world only through the model seam (§4), the sandbox seam (§5.3), and the entity's own journal. An entity is reactive: it waits for a command. An agent is **autonomous**: its activation drives itself forward, model→tools→model, until a run reaches a terminal outcome. Below the boundary, the **effects live in a sandbox**: one isolated environment per session, colocated with the entity's shard leader (entity §5.2), where tools run arbitrarily without endangering the node or another session (§5.1).
>
> An agent **remains an actor**: address a session by name and `ask`/`tell` it, location-transparently (entity §4.3), and define an agent by configuring a *kind*, not by writing a loop. Determinism extends for free: the agent's only new seams (model, sandbox) join the entity's already-virtualized journal, clock, entropy, transport, and spawner (entity §14, core §4.6), so one seed reproduces an entire multi-node agentic run: its model outputs, tool faults, crashes, elections, and resumes.

---

## 1. Scope and layering

The agent runtime runs, on **mutualized infrastructure**, the workload a single-user agent process runs today: long-lived agentic sessions interleaving model calls, tool use, and delegation. Three ideas organize it, each discharged by the entity layer rather than re-implemented:

- **A session is a compound AI system, not a model call** (Zaharia et al., [*The Shift from Models to Compound AI Systems*](https://bair.berkeley.edu/blog/2024/02/18/compound-ai-systems/), 2024): the loop is explicit, journaled, and supervised, while the components, model and tools, sit behind seams (§4, §5).
- **What a session keeps must live outside the loop** (Anthropic, [*Effective harnesses for long-running agents*](https://www.anthropic.com/engineering/effective-harnesses-for-long-running-agents), 2025): a long-running agent works in discrete sessions, each beginning with no memory of the last, so progress must persist in durable artifacts the next activation reads, not in a context window or a process's memory. Here that artifact is **the entity's journal** (entity §7): the session *is* its journal, the activation a disposable fold of it (entity §1, §9), and anything not journaled is lost.
- **One isolation boundary, deliberately placed.** The **loop runs in the entity's activation**, cheap, serial, and effect-free outside its seams, so a session waiting on a model, a tool, or its user holds no thread and, once hibernated, holds no memory either (entity §10); the **tool effects run in the session's sandbox**, colocated with the shard leader, where they may be arbitrary. This is the *agents as infrastructure* move (Cloudflare, [*Project Think*](https://blog.cloudflare.com/project-think/), 2026): a durable, addressable session that costs nothing while idle, with effects contained by the architecture rather than by trust in the model. §5.1 places the boundary.

Everything in this document is built **on top of** the entity and modifies neither it nor the layers beneath it:

- the **entity** (entity §1–§16) is the session: identity (`EntityName`), the event-sourced journal, the single-writer fence, placement on a shard leader, virtual activation, hibernation, and lossless failover;
- **actors, messages, supervision, death watch** (core §3–§12), which the entity itself inherits, host the activation and carry every agent interaction as an ordinary typed message with a hand-written manifest (core §4.4);
- the **event stream** (core §16, entity §13) carries the agent runtime's observability events through the core's application-event extension point: one totally ordered stream, with the vocabulary defined in the agent runtime (§10.4).

The core non-goals (core §1.2) hold unchanged, reinforced by the entity: the agent runtime never transparently retries a message with side effects, never places a quorum on the message path (the entity's quorum is on the *durability* path, not the message path, entity §7.2), and never masks a failure. Where the agent runtime *does* make a retry safe, it does so the way core §7.2 prescribes and the entity requires (entity §2.2, at-most-once): an explicit idempotency key (the `TurnId`, §7.4), not transparent retransmission.

The agent runtime is one consumer of the entity, not the only one. The **persistent machine** ([`machine-spec.md`](machine-spec.md)) is a sibling kind over the same substrate: an entity plus a durable disk (entity §7.15) and a network seam (machine §5), reached over SSH rather than driven by a loop, and reusing the sandbox's Firecracker `Native` mechanics (sandbox §3.5). It has no run loop, no model seam, and no sandbox seam. Where the two touch, the machine spec owns the difference; nothing in this document changes.

### 1.1 Non-goals

- **A model gateway.** The agent runtime calls one configured model implementation per request; routing across providers, A/B serving, and quota brokering are not its concern.
- **Prompt management.** System prompts and tool descriptions are application data the agent runtime stores and transmits, not a templating system it provides.
- **Exactly-once tool execution.** A tool call is at-most-once per attempt (inherited from entity §2.2); across a crash-resume boundary the recovery semantics are explicit and per-tool (§5.5), never silent.
- **An isolation technology.** The spec mandates *that* effectful tools execute behind the `Sandbox` seam (§5.3), not *how*: process, container, or microVM is the provider's choice. A tool declares which capability tier its calls require (§5.6); the realization of each tier is the provider's, bound by sandbox §3.
- **A new consensus mechanism.** The agent runtime adds no Raft group, no shard, and no fence. Durability, ordering, and single-writer safety are the entity's (entity §7, §8); the agent runtime consumes them.
- **Multi-tenant economics.** Quotas, fair scheduling, and billing live above the agent runtime (§13); budgets (§9) bound a run's spend, not a tenant's share.
- **Context-window management.** A transcript that exceeds the model's context fails the run explicitly (§4.3); compaction and summarization are future work (§13).
- **Retrieval, vector stores, UI.** Applications build these as tools (§5) or clients (§7).

---

## 2. The session is an entity

### 2.1 Anatomy

A session is one durable thing and two disposable ones, exactly the entity's own anatomy (entity §1):

- the **entity** *is* the session: its journal is the durable, totally-ordered, single-writer record log (entity §7), and its identity (`EntityName`) is permanent. It is the only component whose loss loses the session.
- the **activation** is the session's working form while active: the fold of the journal plus the in-flight run, on the shard leader (entity §5.2). Stopping it loses nothing; the next activation, on whichever node leads the shard, rebuilds it by snapshot-plus-replay (entity §9).
- the **sandbox** is where the session's effects land: one isolated environment, colocated with the activation, holding working state that is useful but never authoritative (§5.3). Losing it never loses the session (§5.5).

Each session concept is exactly one entity primitive:

| Session concept | Entity primitive |
|---|---|
| `SessionId` | `EntityName`: `(entity_type = KindId, key)` (entity §5.1) |
| Record (a transcript entry) | the entity's `Event` (entity §3) |
| Session state, `state = fold(records)` | the entity's `State`, folded by `apply` (entity §4.1) |
| Write-ahead append, durable before effect | the entity's **output gate** (entity §6) |
| Single-writer fence | the shard's Raft leader; one leader per term (entity §8) |
| Owner node / host / placement | the shard leader / gateway (entity §5) |
| Activation, fold, resume | rehydration: snapshot + replay (entity §9, §10) |
| Idle stop | hibernation (entity §10) |
| Lossless failover | Raft leader completeness (entity §8.3, E14) |

The loop reaches the world through two runtime seams, **model** (§4) and **sandbox** (§5.3), plus the **entity's journal** it inherits (entity §7), and through nothing else; time, randomness, task spawning, and transport come from the core seams the entity already uses (core §4.6, §7).

```text
                      client (any node)                               §7.4
                         │  Submit { session, turn }  →  EntityRef::ask
                         │  routed: name → shard → leader (entity §5.4)
                         ▼
                       Shard leader's gateway                         entity §5.3
                         │  get-or-activate the entity (exactly-once, E6)
                         ▼
       ┌──────── the loop: the entity's activation (§3) ────────┐
       │   state = fold(journal) · serial · no inline I/O      │
       │   autonomous: drives itself model→tools→model         │
       └────┬──────────────────┬──────────────────┬───────────┘
            │                  │                  │
            ▼                  ▼                  ▼
     Model seam (§4)   the entity journal    Sandbox seam (§5.3)
     one inference     (entity §6, §7):      ══ isolation boundary (§5.1) ══
     call per step     records, fenced,     one environment per session,
                       output-gated         colocated with the leader:
                                            shell, files, processes, network
```

### 2.2 Vocabulary

| Term | Definition |
|---|---|
| **Session** | One agent conversation, **realized as an entity**: a durable identity (`SessionId` = `EntityName`), the entity's journal, and, while active, one activation on the shard leader. Survives activation restarts, hibernation, and shard-leadership moves; an `ActorId` does not (entity §1). |
| **Record** | One journal entry, the entity's `Event` (entity §3). The transcript is the sequence of records; session state is a fold over them (§6). "Record" is this document's word for an entity event, kept because "event" is overloaded by the observability stream (§10.4). |
| **Kind** | A named agent definition (`KindId` → system prompt, toolset, sandbox profile, model parameters, default budget, delegation allowlist). The `KindId` is the session's `entity_type` (entity §5.1); kinds are registered identically on every node (§7.1). |
| **Turn** | One submitted input: a user prompt, or a parent agent's delegation (§8). Carries a client-chosen `TurnId`, the idempotency key (§7.4). |
| **Run** | The execution a turn triggers: the model→tools→model loop until a terminal outcome. Identified by its turn's `TurnId`. |
| **`RunCompleted`** | The outbound notification carrying a run's outcome (`Result<Completion, RunError>`) to each reply-to registered for it; a `tell` delivered when `RunEnded` commits (§7.3). The journal's `RunEnded` record is the source of truth; the notification is delivery. |
| **Step** | One loop iteration within a run: one model call plus the resolution of every tool call and delegation it requested. |
| **Activation** | The entity's live, in-memory instance on its shard leader (entity §5), running the loop; the unit the single-activation guarantee (E6, A6) counts. |
| **Model** | The inference seam (§4): one trait, implemented in production by `agent-anthropic` (Anthropic Messages API) and `agent-openai` (OpenAI-compatible Chat Completions), and by a scripted model in simulation. |
| **Tool** | A capability the model may invoke, declared to the agent runtime (§5.2). Every declared tool executes inside the session's sandbox; the single built-in exception, `delegate`, executes in the loop (§8). |
| **Sandbox** | The isolated execution environment bound to one activation, behind the third seam (§5.3), colocated with the shard leader: where sandboxed tools run, and the only place their effects land. |
| **Tier** | A named **capability** set a tool call requires and an activation acquires, journaled before first use (§5.6); semantics in sandbox §2. Always the capability sense in this document; entity's `Local`/`Quorum` *durability* tiers (entity §7.4) are an unrelated use of the word. |
| **Budget** | A run's spending limit (model tokens and steps) from which child budgets are carved (§9). |
| **Root** | The parentless ancestor a delegation tree descends from. Every `SessionCreated` records its `root` (§10.3); the root's `SessionId` names the tree. Correlation metadata only; nothing routes or folds on it. |

Identity is layered, and the layering is the entity's: `SessionId` (= `EntityName`: durable, application-chosen) → `ActorId` (one activation, system-assigned, core §3.6) → `TurnId` (one run). The agent runtime owns the first mapping only as a naming convention atop the entity; entity owns name→shard→leader resolution (entity §5.4) and the framework owns the `ActorId`.

---

## 3. The agent: an autonomous entity

A session, while active, is the entity's activation running on its shard leader (entity §5.2): an ordinary actor (core §3) whose private state is the folded transcript plus the in-flight run, and whose handlers are the only access to it (core §3.5). An agent adds **autonomy**: an entity waits for the next command, while an agent's activation drives itself forward, issuing model calls and tool calls and feeding their outcomes back, until the run reaches a terminal outcome.

### 3.1 The run loop

Runs serialize through the session's **turn queue**, a fold projection (§6.2): every accepted turn is journaled at once (`TurnSubmitted`, §7.3) and waits in acceptance order; when no run is live, the loop starts the head by committing **`TurnStarted`**: the one transition that begins a run, the only place the turn's content enters the model-visible transcript, and the append whose commit fires `RunStarted` (§10.4). At most one run is live per session, and because the queue is journal-derived, an acked turn survives crash, hibernation, and migration exactly like the run it waits behind (§7.5).

A run advances in **steps**. Each step:

1. checks the budget (§9.1); if exhausted, the run ends with `RunError::BudgetExhausted`;
2. issues one model call carrying the transcript (§4.2);
3. if the response is a final assistant message, journals it and ends the run with `Ok(Completion)`;
4. otherwise journals the model response (intent before effect, §6.4), then executes every requested tool call (§5) and delegation (§8);
5. journals each outcome as it arrives; when all are resolved, begins the next step.

A run MUST end in exactly one terminal outcome, journaled as the run's `RunEnded` record (invariant A3): `Ok(Completion)`, `RunError::BudgetExhausted`, `RunError::Cancelled`, or `RunError::Model(…)` (a model failure no retry policy absorbed, §4.3). All four are journalable: a terminal outcome *is* a committed record, so durability failure cannot be one of them. It surfaces instead as the entity's outer `EntityError::Unavailable`, which **pauses** the run rather than ending it (§6.4, §7.5). `RunError` is an application error and therefore travels as a value (core §3.2 rule 4; the entity keeps application errors inside `M::Reply`, entity §4.2), not as a transport failure. The run's **outcome** (`outcome: Result<Completion, RunError>`) carries it to the caller as a `RunCompleted` notification, **not** as the `Submit` reply (§7.3). Transport and durability failure stay separate, in the `EntityError` surfaced on the `Submit` ack (§7.3).

### 3.2 The loop drives itself but never blocks on I/O

The loop is **message-driven**, the discipline the actor and the entity both impose (core §6, entity §6): the activation MUST NOT await the model, a tool, or a durable append inline. It launches each external operation through `Spawner` (core §4.6), and the outcome returns to the activation as an ordinary message; a step is a state the fold tracks, not a stack frame the executor holds. The **I/O lives in the activation's loop, never inside `apply`** (entity §4.1, which MUST be pure and is replayed): the fold stays a deterministic function of the journal, and the loop is the autonomous behavior wrapped around it.

**The cycle itself is the workflow's** (workflow §3.2, §3.3, §4): this runtime implements `Workflow`, with `pending` the desired set as a pure function of committed state (`desired.rs`), `perform` one effect on the seams, and `settle` the fold of one outcome, while the environment reconciliation of §5.4, §5.5 and §5.6 runs as a **pre-pass ahead of every tick**, because no fold can express it.

Three consequences, each inherited:

- **The input gate orders the run.** While a record append is in flight, the entity admits no new command to that session (entity §6, the input gate). A `Cancel` (§9.2) therefore takes effect at a message boundary, not mid-append, and never observes half-committed state.
- **The output gate is the write-ahead barrier.** A model response, a tier acquisition, a tool outcome, and the terminal outcome are each released to the next step (or to a `Submit` caller) only after the record commits on a shard quorum (entity §6, the output gate). The agent runtime's "write-ahead discipline" (§6.4) is exactly this gate; it owns no second mechanism.
- **Straggling outcomes are discarded.** The activation MUST discard, not journal, an outcome message that arrives after its run has ended (a cancelled run's late tool result, §9.2) or after the activation has stepped down on a non-contiguous commit (entity §6, the contiguity guard).

### 3.3 Supervision, restart, and migration

Ordinary supervision (core §11) governs the activation's faults, and recovery is the entity's rehydration (entity §9, §10), exercised in three guises that are one mechanism:

- a **local restart** replays the journal and continues (the factory's source of truth is the journal, not memory; core §11, entity §1);
- a **migration** follows shard leadership: when the leader changes (entity §8.3), the activation moves to the new leader and rehydrates there, losing no acknowledged record (E14);
- a **resume after node failure** is the same rehydration triggered on a new leader by the next message (§7.5).

Because all three are snapshot-plus-replay of the journal, a restarted, migrated, or resumed activation behaves byte-identically to one that never stopped, given the same subsequent model and tool outcomes (A1, the agent runtime face of E2/E3). The agent runtime configures `Restart` with backoff for the activation, as the entity host already does.

---

## 4. The model seam

### 4.1 The trait

```rust
/// Inference: one request, one response; no streaming in v1 (§13). The first agent-runtime seam.
pub trait Model: Send + Sync + 'static {
    async fn complete(&self, req: ModelRequest) -> Result<ModelResponse, ModelError>;
}
```

This document writes the seams' methods as `async fn` for readability; the crate spells them as methods returning `BoxFuture`, here and at the sandbox seam (§5.3), because every seam is held behind a trait object (`Arc<dyn Model>`, `Arc<dyn SandboxProvider>`, `Arc<dyn Sandbox>`) and `async fn` in a trait is not object-safe.

`ModelRequest` carries the kind's system prompt, model parameters, the tool declarations (name, description, input schema) of the kind's toolset, the folded transcript, and `max_tokens`. `ModelResponse` carries the assistant content, zero or more requested tool calls, and the **reported usage** (input and output tokens) that feeds budget accounting (§9.1).

The model is a seam exactly like `Transport` (core §7) and the `EntityJournal` (entity §7.3): the agent runtime core depends only on the trait. In production `ProtocolModel` implements it over an injected `HttpPost` (§12.1), with `agent-anthropic` supplying the Anthropic Messages protocol and `agent-openai` the OpenAI-compatible Chat Completions protocol; the simulator supplies a **scripted model**, a deterministic function of the request and the run's seed (§12.2).

### 4.2 Determinism rules

1. A `Model` implementation MUST take all timing from `Clock` and all randomness from `Entropy` (core §4.6); the production retry engine (`ProtocolModel`, §12.1) sleeps its backoff on the injected `Clock` and draws its jitter from the injected `Entropy`, pre-drawing the whole schedule so the completion future owns no entropy handle. The agent runtime core crate MUST NOT read the wall clock, spawn OS threads, use an unseeded RNG, or open a socket: the HTTP seam it defines (`HttpPost`) is a trait a deployment implements (core §18.1).
2. The model's *output* is nondeterministic in production. The determinism contract is therefore scoped the same way the network's and the journal's are: given the same seed, **the simulated model** returns byte-identical responses, and everything downstream of any fixed response sequence is reproducible (A1).

### 4.3 Failure

`ModelError` distinguishes, at minimum: `RateLimited`, `Overloaded`, `ContextOverflow`, `InvalidRequest`, and `Api(String)`. A `Model` implementation MAY retry requests internally, with backoff from `Clock`/`Entropy` and a bounded policy; a completion request is side-effect-free, so this does not violate core §1.2. A model failure that survives the policy ends the run with `RunError::Model(…)`: journaled, reported, never silently swallowed (core §8).

---

## 5. Tools and the isolation boundary

### 5.1 The boundary

A run splits across one boundary:

| In the entity's activation: the **loop** | In the sandbox: the **effects** |
|---|---|
| control flow: steps, budgets, delegation | tool execution: shell, files, processes, network |
| model calls, through the `Model` seam (§4) | working state: processes and regenerable caches; the workspace's durable subtree is the session's own facet (entity §7.11, §5.5) |
| the fold and the journal, through the entity (entity §6, §7) | everything a tool's side effects touch |

Above the boundary, the loop touches the world only through the three seams; it is serial, journaled, and cheap, the mutualization premise of §1 (and entity §7.8, where activation is a local, consensus-free operation). Below it, a tool may do anything *inside* its session's sandbox and nothing outside it. Entity/actor isolation (core §3.5) protects sessions' state from one another; sandbox isolation protects the node, and every other session's effects, from tools.

**Why the boundary cuts here.** Three other placements lose: with **no sandbox**, one session's `rm -rf` or runaway build takes the node (a shard leader hosting many entities, entity §5.2) and every co-located session with it; with the **loop inside the sandbox**, every session pays for an isolated environment to host control flow that is mostly waiting, and the entity's discipline becomes as opaque to the runtime as the tools are; with **a sandbox per tool call**, consecutive calls share nothing, though the checkout, build cache, or running server one step produced is what the next step needs.

Hence one sandbox **per session**, opened lazily per activation (§5.3) and colocated with the shard leader where the activation already runs (entity §5.2): coarse enough to keep working state across calls, fine enough that no effect escapes the session that made it. The workspace directory the sandbox runs over is the session entity's own **workspace facet** (entity §7.11): each tool call's file delta commits atomically with its `ToolOutcome` record, and every activation, including one on a new leader after a migration, rematerializes the directory from the journal. What remains working state is the *environment*: running processes, containers, held tiers, and regenerable excluded trees; when that is lost mid-activation, §5.5 puts the loss on the record rather than papering over it. The facet's durable cap (64 MiB, the entity's `MAX_TREE_BYTES`) bounds that capture: a call whose post-call capture exceeds the cap is reported to the model as that call's `ToolError::Sandbox` (§5.4) with its file delta unpersisted, so an oversized tree is an application outcome the model trims rather than a failure of the run.

### 5.2 Declarations and the registry

Every tool is **declared** to the agent runtime; the model and the loop need its interface regardless of where it executes:

```rust
pub struct ToolDecl {
    pub name: String,           // stable, author-chosen; the model selects by it (cf. manifests, core §4.4)
    pub description: String,
    pub input_schema: Value,     // the tool's JSON Schema, as a `serde_json::Value` the model is shown (§4.1)
    pub tier: Tier,              // the capability set the call requires (§5.6); part of the kind's digest (§7.1)
    pub on_orphan: OnOrphan, // the declared recovery policy for an orphaned call (§5.5)
    pub timeout: Option<Duration>, // per-call bound; the agent runtime default applies when absent (§5.3)
}

/// Resume's policy for an orphaned call: intent journaled, outcome not (§5.5).
/// The workflow's type, re-exported rather than redeclared (workflow §3.1): a
/// declaration's recovery policy is exactly what a `Pending` carries, so the two
/// are one decision. `Reexecute` means blind re-execution is safe (the call is
/// idempotent, or dedups, `delegate` §8.1); `Interrupt` resolves as
/// ToolError::Interrupted and lets the model decide whether to retry.
pub use workflow::OnOrphan;

/// The capability set a tool call requires (§5.6); full semantics in sandbox §2.
pub enum Tier {
    Workspace, // the session's scoped filesystem, through host-implemented typed tools; no guest code
    Compute,   // arbitrary guest code over the workspace; no network, no ambient clock or entropy
    Network,   // compute, plus egress to the profile's allowlist (§5.3)
    Native,    // OS processes and native binaries inside the confined environment
}
```

The tier names are capability sets, not rungs of a ladder: the cap that bounds a kind is a *set* of tiers, not a maximum, so a kind MAY hold `Native` without holding `Network` (sandbox §2.2). What each tier grants and withholds, and what a provider MUST guarantee to offer it, live in the sandbox specification (sandbox §2–§3); this document carries only the contract.

A kind's `ToolRegistry` is a hand-built list of declarations, in the spirit of `HandlerRegistry` (core §4.4) and the entity's dispatch registry (entity §5.5): explicit, inspectable, and the **allowlist**. A model's tool call dispatches by name against the registry and nothing else; no path leads from model output to code outside the declared set.

Every requested tool call carries a **`CallId`**, unique within its run (the model API's tool-use id, or one the agent runtime assigns on receipt), journaled with the call's intent (§6.4). Outcomes reference it, orphaned-call resolution (§5.5) matches by it, and child-session derivation (§8.1) is keyed by it; without a per-call identity, none of the three is well-defined.

Every declared tool is **sandboxed**: its call dispatches through the `Sandbox` seam (§5.3) and executes nowhere else. The single exception is the built-in **`delegate`** (§8), which executes in the loop: a delegation is control flow (a child `Submit`, confined to the seams), not an effect. v1 deliberately exposes no extension point for loop-executing tools; one is future work (§13).

### 5.3 The sandbox seam

```rust
/// Provisioning of isolated execution environments. The third agent-runtime seam.
/// `workspace` is the session's workspace-facet directory (entity §7.11),
/// rematerialized by the entity before the open: the sandbox runs over it and
/// never owns its durability.
pub trait SandboxProvider: Send + Sync + 'static {
    async fn open(&self, session: &SessionId, profile: &SandboxProfile, workspace: &Path)
        -> Result<Arc<dyn Sandbox>, SandboxError>;
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

`open`, `call`, and `release` are written `async fn` above and spelled as `BoxFuture`-returning methods in the crate, for the object safety §4.1 states.

1. **Lazy, per tier.** An activation opens its sandbox on its first sandboxed call; a session that never needs one never pays for one. Tiers are provisioned the same way: the agent runtime passes each call's declared tier (the seam hands the sandbox no registry, so the tier must travel with the call), and the provider builds a tier's environment on the first call carrying it (§5.6).
2. **Bound per activation.** At most one live sandbox per activation, and deactivation MUST release it (invariant A8), whether the deactivation is a shard-leadership move (migration), a hibernation, or a forced step-down on a non-contiguous commit (entity §6). The sandbox lives on the activation's current leader node and never migrates; a new leader opens a fresh one (§5.5).
3. **Launched, never awaited inline** (§3.2): outcomes return as messages, bounded by a per-tool timeout from `Clock`: the call's declared `timeout` (§5.2), or, when it declares none, a configurable runtime default (SHOULD default to about **5 minutes**). The timeout bounds the *outcome*, not the effect: the sandbox-side process MAY still be running, and still writing, when the `Timeout` outcome is journaled, so the workspace capture that commits with it (§5.1, entity §7.11) can be **torn**, holding files the straggler had half-written. The journal stays consistent (it records what the directory held at capture), the next call's capture diffs against the last committed index and folds in whatever the straggler finished, and the model, told of the timeout, reacts with that transcript in view.
4. **Profiled.** The kind's `SandboxProfile` (image or toolchain, resource limits, network policy) is deployment configuration, agreed cluster-wide like the kind itself (§7.1). The profile also carries the kind's **tier cap** (the set of tiers its sessions may hold, §5.6) and, when the cap includes `Network`, the egress allowlist that tier grants (sandbox §3.3): a `TierAcquired` record grants the profile's allowlist by reference, never an inline list. Declaring a tool whose tier the cap excludes is a deployment configuration error, surfaced at registration as loudly as a duplicate name (§5.2); the cap defaults to exactly the tiers the kind's declared tools require.
5. **Technology-opaque, within a tier.** Whether a tier's environment is a process, a container, or a microVM is the provider's choice; the simulator's scripted sandbox is one more implementation of the same trait (§12). *Which capabilities a call holds* is declared per tool (§5.2), journaled per acquisition (§5.6), and agreed cluster-wide (§7.1), never improvised by the provider. The seam still sees one `Sandbox`: tiers are provisioned within it, never as separate seam objects.

### 5.4 Tool failure is a transcript value

A failing tool does not fail the run. The agent runtime journals its `ToolError` (a timeout, a sandbox-side crash, a failed delegation (§8.2), an unknown tool name, or malformed arguments) as that call's outcome and **returns it to the model** as the tool result. The only abnormal run endings are the three `RunError` outcomes of §3.1, and "a tool misbehaved" is not one of them. Unknown and malformed calls are synthesized outcomes, not protocol failures; the registry-as-allowlist already guarantees nothing was executed. Malformed is structural, not schematic: the loop rejects a call whose input is not a JSON object (and a `delegate` input that does not deserialize, §8.1) before dispatch, and does not validate arguments against the declared `input_schema`: the schema is interface metadata for the model (§4.1) and digest-covered agreement (§7.1), and validation beyond shape belongs to the executing tool, whose rejection surfaces as its own `ToolError` outcome like any other failure.

### 5.5 Crash, loss, and resume (no silent loss)

A tool call's **intent** is journaled before execution and its **outcome** after (§6.4); a crash, hibernation, or migration between the two leaves an *orphaned call*. On the next activation (entity §10, §7.5):

- an orphaned call declared `OnOrphan::Reexecute` MUST be re-executed, in a fresh sandbox if the old one is gone; the re-execution may fail for exactly that reason, surfacing per §5.4;
- an orphaned call declared `Interrupt` MUST be resolved as `ToolError::Interrupted`, fed to the model like any tool failure (§5.4), so the *model* decides whether to retry the side effect.

The same settling path maps every `EffectError` variant to a tool outcome, so the arms are the type's totality: `Interrupted` becomes `ToolError::Interrupted`, `Failed` (a `perform` that panicked, or a cancel the loop could not send, workflow §4.3) becomes `Sandbox`, and `Timeout` and `Lost` become `Timeout` and `EnvironmentLost` (§5.4). The runtime produces only the first two; a tool's own timeout and a lost environment reach the transcript as the `ToolError` the call returns, never as an `EffectError`.

An orphaned call is not always dead. The entity's single-writer fence guarantees one *transcript* (entity §8), but it cannot recall a launched effect. A leader deposed but not yet aware of it (the read-your-leader window, entity §7.5) may still be *executing* a slow call when the new leader resolves it: a `Reexecute` re-execution may then run concurrently with the original, and a model answering `ToolError::Interrupted` with a retry may duplicate a side effect the old leader is still producing. The window is the duration of the in-flight call, not a message round-trip. This is the cost entity itemizes for a deposed leader (entity §11) made concrete for effects: the fence guards the record, not the world. (`delegate` is the one call the architecture itself rescues: its re-execution is a child `Submit` carrying the same `TurnId`, which dedups into an attach rather than a second run, §7.4, §8.1.)

The sandbox itself is **not session state**: the fold (§6, entity §1) never reads it, and a lost *environment* (processes, containers, tier grants) is never reconstructed by the agent runtime. The workspace's **durable subtree** is different: it is the session entity's workspace facet (entity §7.11), captured into the journal after each tool call and rematerialized on every activation, so hibernation, migration, and crash-failover preserve it by construction. What lives outside that capture discipline (excluded trees like `target/`, running servers, anything a tool left in memory) must outlive the sandbox through a tool: push a branch, upload an artifact, return a result the loop journals. This is core §1.2's "retries are the caller's decision" applied to effects: the agent runtime never re-fires a non-idempotent side effect on its own authority.

Nor may the *model* be left pretending. A routine reactivation (hibernation §7.2, migration §3.3, crash-failover) journals **no reset**: the durable subtree the transcript asserts is rematerialized before the sandbox opens, so the model's picture of its files stays true. What the model must hear about is a **mid-activation environment loss**: the provider returned `EnvironmentLost`, so running processes, excluded trees, and held tiers are gone even though the durable files survive in the journal. The activation MUST then journal a **`WorkspaceReset`** record before the next model call and surface it to the model with that request. The record answers no `CallId`, so it cannot ride a tool result: it enters the request as input content the agent runtime authors.

Tier loss follows the same rule at two granularities, surfaced and never silently repaired:

- **The whole environment is lost.** A fresh sandbox resets **every** tier the lost one held. `WorkspaceReset` covers the environment, the held set restarts at `Workspace` in the same activation that journals the reset (§5.6), and re-acquisition is re-journaled rather than silently inherited: within one run the journal reads `TierAcquired`, the tool outcome, `WorkspaceReset`, then `TierAcquired` again before the next call at that tier.
- **One tier is lost while the sandbox survives.** The calls that needed it fail as `ToolError` outcomes (§5.4) and nothing is journaled about the tier itself. The provider MAY re-provision that tier lazily under the acquisition this activation already journaled, and MUST NOT grant a tier the activation never journaled (sandbox §4).

### 5.6 Execution tiers

A **tier** is a named capability set a tool call requires: `Workspace`, `Compute`, `Network`, or `Native` (§5.2). This document owns the contract (the declaration, the record, the cap, the agreement), and the sandbox specification owns the semantics: what each tier grants and withholds (sandbox §2), and what a provider MUST guarantee to offer it (sandbox §3).

1. **Opening grants `Workspace` and nothing else.** The lazy open of §5.3 builds the workspace tier; a session that never acquires more holds a directory's worth of capability and no more.
2. **Acquisition is journaled, intent before effect.** Before the first call at a tier the activation does not yet hold, the loop journals a **`TierAcquired { turn, tier }`** record: the output-gate discipline (§6.4, entity §6) applied to capability acquisition. The record is the audit trail (when did this session first run guest code, first touch the network?) and the policy hook (§13).
3. **Held tiers are additive and environment-scoped.** The held set grows with each acquisition and shrinks only with the environment itself: no call revokes a tier, but a mid-activation `EnvironmentLost` resets the held set to `Workspace` when the loop journals `WorkspaceReset` (§5.5), and a fresh activation starts again from `Workspace`. Held tiers are working state, like the sandbox they live in, and only the journal's acquisition records survive.
4. **The cap is unreachable by construction.** Acquisition beyond the kind's tier cap cannot be requested: declared tools are checked against the cap at registration (§5.3 item 4), and dispatch reaches declared tools only (§5.2). The cap is the invariant that construction discharges (sandbox §6 S4), not a runtime check the loop performs.
5. **Agreement.** Each tool's tier and the profile's cap are covered by the kind digest (§7.1): what a session may acquire is cluster-wide agreement, never a node-local choice.
6. **A record, not an event.** Acquisition is durable, user-facing audit, so §10.1 makes it a record. No §10.4 event accompanies it; the acquisition ordering of sandbox §6 S4 is verified by journal audit of the `TierAcquired` records, not the §10.4 event stream.

---

## 6. Durability is the entity's

This document specifies **no journal seam**. The session's durability is the entity's journal (entity §7), its ordering and single-writer safety are the entity's Raft leader (entity §8), and its write-ahead barrier is the entity's output gate (entity §6). This section states only how the agent *uses* those primitives; the mechanism, the trait, and the two **durability** tiers (single-node and sharded-Raft) are entity's (entity §7.3, §7.4).

### 6.1 Records are the entity's events

A session's transcript is the entity's journal: each **record** is an entity `Event` (entity §3), appended in `Seq` order, totally ordered per session, durable on a shard quorum. The record types are the agent runtime's event vocabulary: `SessionCreated`, `TurnSubmitted` (a turn accepted into the session's queue, §7.3), `TurnStarted` (the queue's head left the queue and its run began, §3.1), `ModelResponse`, `TierAcquired`, `ToolOutcome` (carrying a `CallId`), `ChildRun`, `WorkspaceReset`, `RunEnded`, and `CancelDelivered` (a propagated cancel's delivery receipt, §9.2). `apply` (entity §4.1) folds them into session state. `apply` MUST be pure and deterministic (entity E2): it runs on the live commit path and on every replay, and the agent's only non-determinism (model output, tool effects) is captured *as committed records*, never re-derived in the fold.

### 6.2 State is a fold; the fence is Raft

A session's state MUST be a pure, deterministic function of its journal prefix, `state = fold(records)` (entity §1, E3), with no information outside the fold influencing subsequent behavior except new inputs arriving as messages. Replay is therefore resume (§3.3, §7.5).

The single-writer guarantee is the entity's, not the agent runtime's: a session's records are appended only by its shard leader, Raft elects one leader per term, and a deposed leader cannot commit (entity §8, E1). The agent runtime builds no fence of its own. Where two nodes transiently believe they lead a shard (a placement disagreement), the minority leader reaches no quorum and commits nothing, so the transcript never forks (entity §8); the contiguity guard (entity §6) steps down any activation whose in-memory head can no longer be trusted, and a forced step-down releases the sandbox (§5.3 item 2), emitting the entity's `Deactivated` (entity §13) and the agent runtime's `SandboxReleased` (§10.4).

### 6.3 Write-ahead discipline is the output gate

Within a step, the loop journals, and the entity's output gate (entity §6) holds each effect until the record commits on a quorum:

- a **turn's acceptance** (`TurnSubmitted`) before the `Submit` ack releases (§7.3), and the **run start** (`TurnStarted`) before the run's first model call issues (§3.1);
- a **model response** before any of its tool calls execute (intent before effect, §5.5), each requested call identified by its `CallId` (§5.2); a model call itself is side-effect-free and needs no intent record;
- a **tier acquisition** (`TierAcquired`) before the first tool call at a tier the activation does not yet hold (§5.6);
- a **tool or delegation outcome** before the next step shows it to the model;
- the **terminal outcome** (`RunEnded`) before any `RunCompleted` notification carries it to a registered reply-to (§7.3): the notification is sent from the tick that follows the commit, or, for a re-submission of an already-ended turn, straight from the fold's recorded outcome, so a caller never holds a completion the journal could lose.

The barrier "fold and reply only after durability" is entity §6 verbatim (E5); the agent runtime adds only *which records* a step writes and in what order.

### 6.4 Durability failure

A shard that cannot reach a quorum returns `AppendOutcome::Unavailable` (entity §7.3, §11): the entity pauses writes, the activation steps down, and the in-flight record does not commit. The loop MUST NOT proceed past an uncommitted record, and, because it cannot journal a terminal outcome either, the run does not *end*; it **pauses**. The caller's pending reply resolves as the entity's outer `EntityError::Unavailable` (or the caller's `ask` deadline lapses, core §14.2), and the run stays orphaned until the shard recovers and a message next reaches the session, when it resumes from the last committed record (§7.5). This is the entity's CP stance (entity §2.2, E11) inherited verbatim: the agent runtime pauses progress, never forks a transcript, never masks the loss, and never invents a run outcome it cannot record.

---

## 7. Sessions across the cluster

A session's cluster life is the entity's life cycle (entity §10), and the journal is the only thing that survives it: **created** (first turn) → **activated** on its shard leader (rehydrate → fold → run) → **serving** (runs, §3) → **deactivated** (hibernation, migration, or step-down) → **rehydrated** by the shard's next leader (§7.5). The agent runtime adds no placement, no host, and no resume protocol; it consumes the entity's.

### 7.1 Kinds

Each node's agent runtime is configured with the same `KindId → Kind` map: system prompt, `ToolRegistry`, `SandboxProfile`, model parameters, default budget, delegation allowlist (§8.1). A `KindId` is an `entity_type` (entity §5.1); the kind is the entity type's configuration (its `EntityConfig`: shard count, replication factor, idle window, snapshot policy; entity Appendix A). Kinds are code-and-config, agreed cluster-wide like the codec (core §5): a session created with a kind MUST be resumable on any node, so every node MUST register every kind. A `SessionCreated` record pins the session's kind and a **digest** of its definition (§10.5); the digest covers, among the definition's fields, each tool's declared tier and the profile's tier cap (§5.6). A node cannot activate a kind it lacks: the per-kind factory captures the kind's definition, so an activation of a hosted entity type resolves its kind without a lookup; `HarnessBuilder::host` panics in the call itself for a kind the registry does not hold (the builder resolves the definition there, so the error surfaces at the line that names the kind, not at `build`), and `Harness::session` returns `UnknownKind` for a kind the node's directory neither hosts nor routes (§7.4). A missing kind is a deployment error surfaced before any run starts (nothing journaled, no `RunStarted`), never a silent fallback.

Registration is knowledge; a node's **directory**, the kinds it *hosts* (entity types it serves, seams injected) plus the kinds it only *routes to* (the cluster-client path, entity §5.3), is per-node deployment shape. Harness construction MUST reject a directory that does not close over delegation: every hosted kind's delegation allowlist (§8.1) must be contained in the directory, because the parent's own node launches the child `Submit` (§8.1) and delivers owed cancels (§9.2) through its route for the child's kind. Uncovered, every delegation to that kind would fail per-call, and an owed cancel would wait on a better-configured node leading the shard (A5's liveness). The rejection is at build, as loud as a duplicate tool name (§5.2), so the gap is a deployment error, never a mid-run discovery.

### 7.2 Placement, activation, and hibernation

A session's placement is the entity's: its name hashes to a shard, the shard map gives the shard's current leader, and the session activates on that leader (entity §5.1, §5.2, §5.4). The agent runtime defines no host registry and no placement function: name→shard→leader resolution does it all. The gateway (entity §5.3) gets-or-activates the session exactly-once per node (E6) and routes commands to it.

The leader activates a session lazily, on the first message that reaches it: rehydrate (snapshot + replay, entity §9) → fold → run the loop. Activation takes no consensus and no network (entity §5.2, §7.8): the leader holds the shard log locally.

**Hibernation (idle stop).** After a configurable idle interval the leader MAY hibernate a session (entity §10): run nothing that mutates state, snapshot to bound the next replay, release the sandbox (A8), and drop the activation. It MUST NOT hibernate a session whose run is live or whose queue holds an accepted turn (§3.1): both mean work is owed now. Hibernation needs no flush (anything worth keeping is already a record, §6.2); releasing the sandbox drops the environment's processes and containers, and the workspace's durable subtree survives in the journal, rematerializing on reactivation (entity §7.11, §5.5). `idle_after` SHOULD default to **minutes**, inheriting the entity default (entity §10) rather than the Durable Objects eviction window it is modelled on: reactivation is cheap but not free (a quorum head-recovery plus a snapshot-bounded replay), and the memory eviction reclaims is not the resource under pressure here. The idle timeout trades sandbox cost, the expensive resource mutualization shares, against environment continuity (warm containers, regenerable build caches).

**Stale-leader redirect.** A command arriving at a node that no longer leads the shard receives `NotLeader(hint)` (entity §5.4, entity §8); the runtime follows the hint to that node's gateway and retries there, within the caller's deadline. It is ordinary Raft client redirection, and the `TurnId` (§7.4) keeps the eventual retry safe.

### 7.3 The wire contract

A session is addressed as an entity, and its commands are entity commands (entity §4.2) with hand-written manifests (core §4.4):

| Command | Reply (`M::Reply`) | Manifest |
|---|---|---|
| `Submit { kind, turn, parent?, reply_to: Option<ActorRef<…>> }` | `Result<Accepted, SubmitReject>`† | `agent.Submit` |
| `Cancel { turn }` | `()` | `agent.Cancel` |

The session names no payload field: it is the entity name the command is addressed to (entity §5.4). `parent` carries a delegating parent's lineage (§8.1, §10.3) and is absent on a root submit.

Reading a run is deliberately **not** a session command: `tail` (§10.2) rides entity's gateway-level event read (entity §7.5), a framework built-in like `subscribe`, so it appears in no allowlist here and never touches the activation.

†`Submit` replies with an **ack, not the run's outcome**: `Accepted { turn: TurnId, status: Started | Attached | Ended }`, released the moment the `TurnSubmitted` record commits, an ordinary output-gated entity reply (entity §6). Acceptance and start are distinct journal facts: `TurnSubmitted` enqueues the turn (behind the live run, if any) and releases the ack, so **an acked turn is durable**, surviving crash and migration in the fold's queue (§3.1) rather than in any activation's memory, and the loop's dispatcher later commits `TurnStarted` when the turn reaches the head. `Started` therefore means "newly accepted", whether the run begins immediately or behind the queue; there is deliberately no distinct queued status, since every accepted turn is queued for at least the instant before its `TurnStarted`. Its `Err` arm, `SubmitReject`, is an *application* failure carried as a value inside the reply, kept distinct from the transport `EntityError` outside it (core §3.2, §14): a caller-contract violation is permanent (the same submit always rejects the same way), where a peer move could clear a transport failure. The two cases are `KindMismatch` and `ContentConflict` (§7.4; `prompt` re-presents either as `EntityError::Call(CallError::System(…))`, §7.4). Each command is delivered through `EntityRef::ask` (entity §4.3), so the call surfaces `EntityError` (a transport failure, an unregistered manifest included: `NotLeader`, `Unavailable`, entity §12) outside the reply. `Cancel` likewise commits one `RunEnded { Cancelled }` record and acks, ending the live run, or ending a still-queued turn before any run starts (§9.2).

The run's outcome travels separately, as an **outbound notification** the activation `tell`s when the run's `RunEnded` commits:

| Notification | Payload | Delivered to |
|---|---|---|
| `RunCompleted` | `{ session, turn, outcome: Result<Completion, RunError> }` | every reply-to `ActorRef` registered for the run |

`RunCompleted` is a `tell` (core §3.3): fire-and-forget, at-most-once. It is **delivery, not the source of truth** (the durable `RunEnded` record is, §6, §10.1), so a lost notification is recovered by the caller re-contacting the session (§7.4), never by a transparent retry (core §1.2). It is not a §10.4 observability event: `RunEnded` already marks termination on the checker stream.

**Why the outcome is a notification, not the `Submit` reply.** A reply to an `ask` is an actor `ReplyHandle` (core §4.5): synchronous, resolved exactly once *within the delivery of that one message*, and **not storable across later messages**. A run spans many inbound messages (each model and tool outcome, §3.2) between `Submit` and `RunEnded`, so the run's `Completion` *cannot* be the `Submit` reply: no handle survives that gap. `Submit` therefore acks the moment its `TurnSubmitted` commits, and the outcome is delivered later to a reply-to `ActorRef` the caller placed in the command (core §3.3: "An `ActorRef` MAY be a field of a message"). Completion arrives as one more message into the loop, exactly like every tool outcome, and "attaching to a live run" (§7.4) is registering another reply-to.

The `Submit` `ask` returns promptly (it commits one record), and nothing a caller holds bounds the run itself: budgets (§9) bound its tokens and steps, not its wall time (tool timeouts, model retries, and child runs compound), so a run MAY outlive any caller. A caller that gives up, times out its wait, or crashes leaves the run untouched; it recovers the outcome by re-submitting the same `TurnId` to re-register (§7.4) or by following `tail` (§10.2). There is deliberately no status message: a status view (head `Seq`, live run if any, spend so far) is a client-side fold over `tail`'s records, not a second reply type.

`Submit` carries `kind` on every call though it binds only on the first: creation is implicit in a session's first turn. After creation the field is a checked redundancy, rejected on mismatch (§7.4).

A read (`tail`, and any future query that emits no record) commits nothing (entity §6 step 2, §7.5) and is a local, replication-free read of the shard leader's journal (§10.2). Its consistency is **read-your-leader**, not linearizable under partition (entity §7.5): an isolated minority leader, deposed but not yet fenced, MAY serve a slightly stale `tail` until it steps down. Writes never fork; only these reads can be momentarily stale, and only on the minority side. A linearizable read is a deferred entity upgrade (entity §7.5, §16), not an agent-runtime mechanism.

### 7.4 The client view (`SessionRef`) and idempotent submission

```rust
let h = Harness::cluster(system, &kinds, model, sandboxes);
//   per node: registers each kind's gateway and injects the model and sandbox seams (§4, §5.3);
//   the journal is the entity's, configured per kind (§7.1), not an agent-runtime seam.
let s = h.session("researcher", SESSION_ID)?;      // an EntityRef<Agent>; name→shard is a pure local hash,
                                                   //   no I/O (entity §5.1, §5.4); the one failure is a
                                                   //   directory miss: a kind this node neither hosts nor
                                                   //   routes. Kind names arrive from outside (a URL path
                                                   //   at a gateway), so the miss is a value, not a panic
let out = s.prompt(Turn { id: TURN_ID, content }).await;   // blocking convenience: Submit (ack) + await
                                                   //   the RunCompleted notification (§7.3); presents
                                                   //   Result<Result<Completion, RunError>, EntityError>
```

`SessionRef` is `EntityRef<Agent>` (entity §4.3) with a thin agent-facing surface. `prompt(turn)` is a **blocking convenience composed at the edge, not a protocol primitive**: it spins an ephemeral local actor as the reply-to, `Submit`s the turn carrying that ref, awaits the one `RunCompleted` message, and presents `Result<Result<Completion, RunError>, EntityError>`. `prompt` is `prompt_within` at the agent runtime's `submit_deadline` (30 s by default) and hands the whole remaining wait to one `Submit`-and-attach attempt: a lapse of that wait surfaces as `EntityError::Call(CallError::Timeout)`, and `prompt` re-`Submit`s the same `TurnId` only when the attempt's mailbox drops without an outcome, re-registering on whatever activation now hosts the session. Delegation's `run_child` (§8.1) is the cadenced form: each attempt attaches for at most one cadence, and every lapse re-`Submit`s the same `TurnId` to re-register until the budget-scaled wait expires. A caller that prefers not to block uses `Submit` with its own reply-to (a parent agent does this, §8.1) or observes through `tail`. A lapsed attempt leaks no actor: its mailbox stops with it, and a notification racing that stop is an ordinary lost delivery, recovered by re-contact (§7.3).

`prompt` MUST NOT transparently retry a failed `Submit` (core §1.2, entity §2.2). The entity *does* re-issue a command that provably did not run, such as one to a hibernated stale host (`DeadLetter`) or a moved leader (`NotLeader`), neither of which commits (entity §4.3, §6); but an *ambiguous* failure (`Unreachable`/`Timeout`) is never auto-retried, because the command may have committed before the ack was lost. The **`TurnId`** makes the caller's own re-submission safe across that ambiguity:

- a `Submit` whose `TurnId` is already journaled MUST NOT start a second run (one `TurnSubmitted` record, E1): if the turn is still queued or its run live, the activation **registers the new reply-to** on it (`status: Attached`), to notify at `RunEnded`; if the turn already ended, the activation immediately **`tell`s the recorded outcome** (`status: Ended`), read from the `RunEnded` record. The dedup, and the recorded outcome, are a fold over the session's own records (queued turns included, so no ephemeral second check exists), and both are exact even across a resume;
- the registration is race-free: the serial activation processes a re-submission and the run's `RunEnded` transition in order (the input gate, entity §6), so a caller is either added to the live run or told the finished outcome: never neither, never both;
- the `reply_to` is **transient routing, not part of the turn**: it is not journaled and is **excluded from the content-equality check**, so re-submitting the same `TurnId` with a fresh reply-to is the ordinary re-attach, not a mismatch.

**What the equality check covers.** The turn's content **and its literal `budget` field**: a re-submission that changes the budget is not a silent attach, and `None` is not `Some(default)`, so whether two submissions conflict never depends on the kind's *current* default. The comparison digest is fold-local, recomputed from the journaled `TurnSubmitted`, which records the resolved budget plus whether it was explicit; the encoding is length-prefixed, so content and budget cannot collide by juxtaposition. Delegation re-execution stays safe by construction: a parent re-submits the budget read from its journaled `ChildRun`, re-deriving the same literal (§8.1).

**What the digest is not.** It is a fast 64-bit content hash: a guard against *accidental* reuse of a `TurnId` with different content, not a cryptographic commitment. A deliberately crafted collision would attach rather than conflict, so the check is a caller-bug detector, not a security boundary: a caller who can address the session at all already holds it. A gateway scopes session names by principal for exactly that reason.

**What a mismatch produces.** A re-submission whose turn differs under the check (`SubmitReject::ContentConflict`), or whose `kind` differs from the entity's own type (`SubmitReject::KindMismatch`), is a caller bug: rejected as a value in the reply, journaling nothing, and never raised as a transport `EntityError`, because a caller must not retry it (§7.3†). The blocking conveniences collapse the value into their own error form (`prompt` presents a `SubmitReject` as `EntityError::Call(CallError::System(…))` carrying the reject's display text, `run_child` as `ToolError::Delegation`), while the wire keeps the value distinct, so a caller that `Submit`s with its own reply-to reads the `Err` arm directly.

The **subscriber set** (the reply-tos registered for a live run) is ephemeral activation state: like the held tiers and the sandbox (§5.5, §5.6), it is rebuilt by callers re-contacting after a resume, never journaled.

### 7.5 Resume after node failure

When a session's shard leader fails or is partitioned away, the shard's replicas elect a new leader by ordinary Raft (entity §8.3); the new leader already holds every committed record (leader completeness, E14). The next message routed to the shard activates the session there: rehydrate (entity §9, rematerializing the workspace facet's directory), fold (§6.2), orphaned-call resolution (§5.5), and continuation of any unfinished run. No coordinator hands the session over: placement is the shard map, the journal is the state, and Raft is the fence (§6.2). No acknowledged record is lost (E14, A1).

Resumption is **caller-driven**: the entity does not scan for orphaned work (entity has no cross-entity enumeration), so a run interrupted by a crash resumes only when a message next reaches the session: the re-submitted `TurnId` of a caller that wants its answer, a parent's re-executed delegation (§8.1), a `Cancel` (idempotent on an ended run, and the explicit nudge for any cancel propagation the journal still owes, §9.2). The same contact resumes the **queue** (§3.1): an acked turn a crash left waiting is fold state, and the first message to reach the resumed session drives the dispatcher, which starts it once no run is live. A run nobody asks after stays interrupted, and its budget bounds what it can ever cost (§9.1). A3's liveness is conditional on exactly this contact, and so is A5's for a session whose owed propagation a crash interrupted.

---

## 8. Sub-agent trees

### 8.1 Delegation is a tool

The agent runtime provides one built-in tool, `delegate`, the only tool that executes in the loop rather than the sandbox (§5.2), present in a kind's registry iff the kind permits sub-agents. Its input names a child kind plus a prompt and, optionally, a budget request. The child kind MUST belong to the parent kind's **delegation allowlist** (§7.1): naming any other kind is a synthesized `ToolError` (§5.4), so a locked-down kind cannot escalate its privileges by delegating to a permissive one. Its execution is a child `Submit`:

1. the parent journals the delegation (a `ChildRun` record: the child `SessionId` and the child turn's `TurnId`, both derived deterministically from the parent session, the parent's `TurnId`, and the delegation's `CallId` (§5.2), plus the carved budget, §9.1; one run may delegate many times, so the call, not the turn, is the unit of derivation); the derivation length-prefixes each component, so distinct `(session, turn, call)` triples always derive distinct ids: ids are application-chosen, and a bare separator join would let crafted ids collide; a re-executed delegation re-derives the same pair (§5.5), and cancel propagation reads it from the record (§9.2);
2. the launched task `Submit`s to the child session (an `EntityRef`) carrying a **reply-to** ref and **awaits the child's `RunCompleted`** notification (§7.3); it never block-`ask`s the child, whose run may be arbitrarily long. The await re-attaches on the submit-deadline cadence (each lapse re-registers on whatever activation then hosts the child, §7.5; the derived `TurnId` keeps every re-attempt safe, A7) and is bounded in total by a **budget-scaled wait**: a configurable floor plus a configurable per-step allowance × the child's carved `steps` (the floor SHOULD default to about **10 minutes**, the allowance to about the per-tool timeout, §5.3 item 3). The carve is the right scale because the step bound is compositional (§9.1 item 3), so it bounds the whole subtree's model calls, not one level's. Past the bound the delegation resolves as a synthesized `ToolError` for the parent's model to react to (§5.4); the lapse cancels nothing: the child runs on, its budget the backstop (§9.2 item 3), and its `ChildRun` now has a journaled outcome, so a later cancel of the parent no longer propagates to it (§9.2 item 2). A *consecutively unreachable* child is bounded separately, by a transport retry limit with backoff (any accepted submit resets it), surfacing the same way (§5.4). The child is a **full session**: an entity in its own right, journaled, budgeted, placed on its own shard wherever the cluster puts it (entity §5), supervised on its leader;
3. the child's outcome arrives as one more message into the parent's loop (§3.2) and becomes the tool's outcome (§5.4): journaled, then shown to the parent's model.

Delegation thereby inherits every property of §7 with no new machinery: a child on a crashed leader resumes on the new leader (§7.5); a parent that crashes and resumes finds the delegation orphaned and, since `delegate` is declared `Reexecute`, re-executes it; the child's journaled `TurnId` dedups the re-submission and re-registers the parent's fresh reply-to (§7.4), so the outcome is delivered whether the child is still running or already done.

### 8.2 Tree shape and failure

- The tree is recorded in the entities' journals (`ChildRun` / `SessionCreated.parent`), not in any replicated structure; there is no global tree view to keep consistent. Each entity is its own consistency boundary (entity §2.2): the agent runtime builds no cross-entity transaction.
- Fan-out is bounded by the budget: each delegation carves from the parent's remaining budget (§9.1), so a tree's total spend is bounded by the root's budget regardless of depth or width (A4).
- A child's failure is a tool outcome (§5.4): the parent's model sees `RunError` and decides. Failures never propagate as supervision faults across the tree: supervision is local (core §11.3), and the tree spans shards and nodes.

---

## 9. Budgets and cancellation

### 9.1 Budgets

```rust
pub struct Budget { pub tokens: u64, pub steps: u32 }
```

1. Every run has a budget: the turn's explicit budget, else the kind's default. Spend is the model-**reported** usage (§4.1) summed over the run's calls, plus the spend of its children. Spend is therefore **a projection of the journal, not separately-tracked state**: each `ModelResponse` record carries its call's usage and each `ChildRun` its carve-out, so current spend is recomputed by the fold (§6.2) like all session state. A resumed activation re-derives spend exactly by replay; nothing about budget accounting survives deactivation outside the records (A1).
2. **Pre-call enforcement.** A model call is issued only while `spent < tokens` and the step count is below `steps`; the request's `max_tokens` MUST be clamped to the remaining token budget, and no call may be issued while the remainder is below a configurable floor. Output overshoot is therefore zero, and total overshoot is bounded by one call's input, a bound that grows with the transcript because a call's input size is known only on response. Exhaustion ends the run with `RunError::BudgetExhausted` (§3.1): journaled, reported, recoverable by a new turn with a new budget.
3. **Carve-outs.** A delegation reserves an explicit slice of the parent's remaining budget; the child enforces its slice locally, with no cross-shard accounting protocol. Hence the bound is compositional: own spend + Σ carve-outs ≤ budget, at every node of the tree (A4).
4. **What the bound is.** Spend is *journaled* usage. A model call whose response never commits (issued by a leader deposed mid-step (§6.2), whose record loses to the contiguity guard, entity §6) is discarded uncounted, though the provider bills it: the budget bounds the session's recorded spend, and the provider's invoice exceeds it by exactly the speculative work a leadership change cost.

### 9.2 Cancellation

1. `Cancel` is a message (§7.3); because handlers never block on I/O (§3.2) and the input gate admits it between appends (entity §6), it takes effect at the next message boundary, not after the current model call returns. It names the run it cancels (`turn`): a `Cancel` naming an ended or unknown run is an idempotent no-op, so one delayed in flight never kills the named run's successor. One naming a **still-queued** turn (§3.1) journals that turn's `RunEnded { Cancelled }` directly: the fold drops the queue entry, subscribers are notified from the recorded outcome, and, since no run ever began, no `RunStarted`/`RunEnded` event pair fires (§10.4).
2. On cancel, the session journals `RunEnded { Cancelled }`, notifies every registered reply-to with `RunCompleted { outcome: Err(RunError::Cancelled) }` (§7.3), discards subsequent outcome messages of that run (§3.2), and **owes propagation**: folding the terminal record moves the run's unresolved delegations (each `ChildRun` without a journaled outcome, §8.1) into the fold's *owed-cancels* fact, and each owed child is from then on a member of the desired set (workflow §3.1): a `Cancel { turn }` effect the cycle launches once per activation and relaunches under backoff until the child acks, each child cancelling its own subtree. The owed set is a projection of the journal, never activation state, so a crash between the terminal commit and the send loses nothing: the next activation re-derives the debt and re-sends when a message next drives the session (§7.5; a re-sent `Cancel` ticks the cycle explicitly).
3. Delivery is journaled. A child's ack releases only after the child's own commit (its `RunEnded { Cancelled }`, or nothing for an already-ended run, under the entity's output gate), and the parent then journals **`CancelDelivered { turn, call }`**, which clears that child's owed fact; until it commits, a re-send is safe because a cancel of an ended or unknown run is a no-op (item 1). Transport gives each send at-most-once (core §7.2, entity §2.2), so an unacked send is an effect that failed: the workflow backs its key off and relaunches it (workflow §3.4), and at the backoff cap the key comes to rest so the parent may hibernate (workflow §6); the debt stays in the fold, and the next message that drives the session, on this same activation or a later one, re-derives and relaunches it. The guarantee is two-layered: once faults cease and a message reaches the session, propagation completes within bounded logical time (A5); under unhealed faults, the child's **budget is the backstop**, since every run is bounded with or without the cancel arriving (§9.1).
4. In-flight side effects of a cancelled run (a tool mid-execution, a model call mid-flight) are not undone and MAY complete externally; their outcomes are discarded. The agent runtime reports what it stopped; it does not pretend to have un-run it.

---

## 10. Observability

Operating agents means seeing what the model said, which tools ran, what a run cost, and how a delegation tree unfolded. One record already carries all four: the entity's journal.

### 10.1 The journal is the record

The entity's journal (entity §7) is the single source of truth for what a session did, and every observation API is normatively a **read of it**. The agent runtime MUST NOT keep a second durable account of session activity: a parallel trace or audit store would restate the journal's decision in a second module, and the two would drift. The division of labor:

- **Records** are durable and user-facing: the transcript, the tool calls and outcomes, the costs, the tree links. They *are* the entity's events (§6.1). If an observer needs it, it is a record.
- **Events** (§10.4) are ephemeral and checker-facing: they describe the *machinery around* sessions (activation, leadership, run pairing) so the A-invariants are checkable over a run's stream (core §16, entity §13). They carry nothing about a session's content.

Each record carries the leader's `Clock` reading at write time. The timestamp is observational metadata: the fold (§6.2) MUST NOT let it influence behavior. Under simulation it is virtual and therefore deterministic (core §18.1), so timestamped journals still reproduce byte-identically.

### 10.2 Reading a run (`tail`)

`SessionRef::tail(from: Seq, limit) → Result<EventHistory<Record>, EntityError>` (§7.4; `EventHistory` is entity's event-read reply, entity §4.3) returns at most `limit` committed records after `from`, plus the journal's compaction `base`: an idempotent, replication-free **read of the leader's journal** (entity §7.3 `load`), riding entity's gateway-level event read (entity §7.5) rather than any session command (§7.3). The gateway serves it **without get-or-activating the session**, so a poll against a hibernated session leaves it asleep (observation costs a journal read, never a rehydration or a workspace rematerialization), and the activation keeps no mirror of the raw records: the fold holds projections only (§6.2), so session memory and snapshots do not grow a second copy of the transcript.

`tail` returns entity's **event projection** (entity §7.5) unchanged. Three consequences are the session's own:

- **The gaps are the workspace facet's.** A session's journal interleaves its records with its workspace facet's (entity §7.11, §7.12), which is what puts gaps in the projection's `Seq`s here. (They are the same slots the record subscription delivers (entity §7.9), so a follower reconciling `tail` pages against pushed batches compares like with like.)
- **`limit` is bounded twice.** `SessionRef::tail` clamps `limit` to a page cap (SHOULD default to about **1024** records) on top of the gateway's own server-side bound (entity §7.5), so no single read materializes an unbounded reply.
- **The projection's `base` becomes an explicit step in the push form.** A long transcript's oldest records eventually exist in no journal (folded state is untouched, the session's snapshot subsumes them; only *history* truncates), and `SessionRef::follow` surfaces that as a `Truncated { base }` step, told to the caller *before* the cursor resumes past the gap. The HTTP surface mirrors both forms: a `base` field beside the records page, and a `truncated` stream event whose event id is the base, so a `Last-Event-ID` reconnect resumes after it.

It is read-your-leader, not linearizable under partition (§7.3): an isolated minority leader serves slightly stale tails until it steps down, and an unreachable shard takes observation of its sessions with it until a new leader is elected. Polling `tail` follows a live run at whatever cadence the client chooses; `Submit`-and-await remains the one-shot form.

A `tail` MUST NOT trigger a write or change any activation lifecycle: served at the gateway, it neither wakes a hibernated session nor even refreshes an active one's idle clock, so continuous observation and hibernation's economics (§7.2) no longer trade against each other. `tail` is the smallest interface that makes a run watchable, and `SessionRef::follow` is its push complement: a `Follower` rides the entity's record subscription (`EntityRef::subscribe`, entity §7.9), which registers a sink in the session's activation, and reconciles every pushed batch by `Seq` against `tail` pages, backfilling from the journal on attach, on any gap, after the stream closes, and on a periodic re-sync that catches a silent leader move, so `Follower::next` yields the exact committed sequence, or `Followed::Truncated { base }` where compaction outran the cursor. Push is the fast path; the journal is the authority.

### 10.3 Tree correlation

Every `SessionCreated` records its session's **root**: its own `SessionId` for a session created with no parent, the parent's `root` for a session created by delegation (§8). The field is the transitive closure of the parent links, denormalized so any record, event, or log line can name its logical request in O(1). Together with the recorded parent links, `root` stitches one delegation tree across shards, nodes, journals, and logs: the trace propagation core §16 recommends, with no identifier minted for it. Mapping trees onto external trace systems is future work (§13).

### 10.4 Events (checker-facing)

Agent events are defined **in the agent runtime** as their own typed enum and ride the core stream through its application-event extension point (core §16's `Event::App`), alongside the entity's own events (`Activated`, `Deactivated`, `LeaderChanged`, `Committed`, …; entity §13): the core owns the mechanism, never the vocabulary. Checkers recover them by downcast, observing one totally ordered sequence; the seed-reproducibility contract (core §18.1) covers them for free.

These events deliberately restate information the journal already holds (a `RunStarted` echoes a `TurnStarted` record, a `ModelCompleted` echoes a committed `ModelResponse`), the one sanctioned exception to §10.1's single-source rule: a continuous checker observes the *totally-ordered core stream*, not the per-session journals behind the seam, so an invariant about run pairing or a call landing after a run's end has to ride the stream to be checkable (entity makes the same trade for `Committed`, entity §13). The vocabulary is therefore kept to the minimum a stream checker cannot reconstruct from the entity's own events: emit nothing whose check could read the entity's `Activated`/`Deactivated`/`LeaderChanged` instead:

| Event | Fields | Meaning |
|---|---|---|
| `RunStarted` | `session`, `turn`, `parent?` | A run began: the turn's `TurnStarted` committed (§3.1). |
| `ModelCompleted` | `session`, `turn`, `node` | One model call finished and its response committed, scoped to journaled spend (below): the stream half of A4 is that none follows the turn's `RunEnded`. |
| `RunEnded` | `session`, `turn`, `outcome` | The run's exactly-one terminal outcome was journaled. |
| `ToolCompleted` | `session`, `turn`, `node` | One tool call's `ToolOutcome` committed on a live activation on `node` (§5.4). |
| `SandboxBound` | `session`, `node` | The activation opened its sandbox (first sandboxed call, §5.3). |
| `SandboxReleased` | `session`, `node` | That sandbox was torn down (hibernation, migration, loss, or step-down). |

The sandbox pair is *not* derivable from records, because the sandbox is not session state (§5.5). Run boundaries (`RunStarted`/`RunEnded`) and per-call completions (`ModelCompleted`) are the minimum needed to check A3/A7 and the stream halves of A4/A5 (no `ModelCompleted` after `RunEnded`); the spend bound itself is audited from records (`ModelResponse` usage and `ChildRun` carve-outs, `conformance_budget.rs`), because the budget and the carve-outs are not on the stream. `ToolCompleted` marks one call's outcome landing in the journal, which the run-discipline checker reads to reject a straggler committing after its run ended (§3.2); everything else about a session's lifecycle is read from the entity's events.

Session activation, deactivation, and the single-writer fence are observed through the **entity's** events, not duplicated here: `Activated`/`Deactivated` mark the activation lifecycle (entity §13), `LeaderChanged` marks a migration, and the absence of a forked log is E1, checked over the entity stream (entity §14). The agent runtime's `SandboxBound`/`SandboxReleased` MUST alternate within the entity's `Activated`/`Deactivated` window for that session and node (the A8 effect-containment check), and a bound sandbox's `SandboxReleased` MUST precede that activation's `Deactivated`: the entity host runs the agent's `on_deactivate`, which releases the sandbox and emits the event, before it emits `Deactivated`, and the checker rejects a `Deactivated` that arrives with the sandbox still bound.

`RunStarted` fires exactly once per `(session, turn)`, on the activation whose append commits the turn's `TurnStarted` (§3.1); a resume (§7.5) emits **no** second `RunStarted`: a checker recognizes a resume as the entity's `Activated` followed by `ModelCompleted` on an already-started turn, so the agent runtime mints no `RunResumed` event of its own. A turn cancelled while still queued (§9.2) emits **neither** event: its terminal is a record with no run, and an unpaired `RunEnded` event would itself break the A3 pairing. That keeps the A3 pairing and A7 counting checkers sound across crashes, and under a leadership race their soundness is the entity's fence (E1): the turn commits once, so one start is counted even when two leaders briefly raced to begin it. `ModelCompleted` carries its `node` so the A6 checker attributes each call to its enclosing activation, and it is emitted only for a committed response, so a deposed leader's discarded work (§9.1 item 4) never appears on the stream.

### 10.5 Reconstruction and derived metrics

Because session state is a pure fold (§6.2, A1) and a model request is a deterministic function of that state (§4.1), the exact `ModelRequest` issued at any step is **reconstructible from the journal prefix**: debugging a production prompt needs no request logging, only the journal and the code. Reconstruction is faithful only against the same agent definition, which is why `SessionCreated` records a digest of the kind (§7.1): a reader can tell whether a reconstruction is exact, or merely indicative because a deployment changed the kind mid-session. The digest is a fast 64-bit content hash: it flags drift, it does not authenticate. Integrity rests on the journal's custody, not on collision resistance the hash does not have.

Aggregate metrics (spend per kind and per tree, grouped by `root`; run latency; tool failure rates; activation and leadership churn) are RECOMMENDED, not REQUIRED, and SHOULD be **derived** from records and events rather than instrumented separately.

---

## 11. Conformance

The agent runtime catalogue mirrors the entity, core, and utilities catalogues and is machine-readable alongside the agent runtime's conformance suite (`a_catalogue()`), guarded by the same drift-test pattern. It lists only what is specific to the *agent*; the entity's own guarantees are cited in the entity-basis column rather than re-proved here. The deterministic fold (E2/E3) and single activation (E6) reappear as the agent-level A1 and A6, which add resume-equivalence and autonomy to them, while the single-writer fence (E1) has **no** agent invariant: it is wholly the entity's, appearing only as the basis other agent invariants build on (A6's one leader, A7's single commit). There is deliberately no **A2**: the write-ahead obligation that number would carry is wholly the entity's output gate (§6.3, E5), and A3–A8 keep stable numbers.

| # | Invariant | Defined in | Verified by | Entity basis |
|---|---|---|---|---|
| A1 | **Deterministic fold and resume.** Session state is a pure fold of the journal; a session resumed from any committed prefix behaves byte-identically to one that never stopped, given the same subsequent model and tool outcomes. | §6.2, §7.5 | differential resume-vs-uninterrupted test (`conformance_resume.rs`); seed-reproducibility sweep (`agent_swarm.rs`: `the_event_stream_reproduces_byte_for_byte_per_seed`) | E2, E3 |
| A3 | **Run termination.** Every `RunStarted` is followed by exactly one `RunEnded`; once faults cease, partitions heal, and a message reaches the session, no run remains pending past its budget's bound. The `RunEnded` record is the single source of truth; the `RunCompleted` notification is best-effort (`tell`), and a lost one is recovered by re-contact (§7.4), never retried. | §3.1, §7.5, §9 | continuous checker (`agent-run-discipline`, pairing); scenario and swarm sweeps (`conformance_run.rs`, `cluster.rs`) for bounded completion under caller re-submission | E14 (lossless failover) |
| A4 | **Budget bound.** A run issues no model call after exhaustion; output spend never exceeds the remaining budget at call time; own spend plus children's carve-outs never exceeds the budget, at every level of a delegation tree. | §9.1 | continuous checker (`agent-run-discipline`): no `ModelCompleted` after `RunEnded` (§10.4); journal audit of spend against budget and tree scenario (`conformance_budget.rs`) | (none) |
| A5 | **Cancellation.** After a cancel is journaled, the run and, once faults cease, every descendant run end `Cancelled` within bounded logical time, issuing no further model calls. | §9.2 | scenario and swarm tests (`conformance_cancel.rs`); crash-window scenario (`cluster.rs`: leader crash between the terminal commit and the propagating send); checker (`agent-run-discipline`) for post-cancel `ModelCompleted` | (none) |
| A6 | **Single autonomous activation.** A node never runs two concurrent activations of one session; on a converged cluster, at most one activation per session is live; an addressed session activates within bounded logical time of a message reaching its shard leader. | §7.2, §7.5 | continuous checker (`agent-event-grammar`, the per-node half: no model completion outside an activation), inherited entity checker (E6), and the converged and liveness halves of the cluster scenario (`cluster.rs`) | E6, entity §8 |
| A7 | **Idempotent submission.** A re-submitted `TurnId` never starts a second run: it registers the caller's reply-to for notification, or `tell`s the recorded outcome if the run already ended, under any injected duplication or caller retry. | §7.4 | continuous checker (`agent-run-discipline`, one `RunStarted` per `(session, turn)`); retry scenarios (`conformance_run.rs`) | E1 (one commit) |
| A8 | **Effect containment.** An activation binds at most one live sandbox and releases it on deactivation; every sandboxed tool call executes in the sandbox bound to its issuing activation, never another session's; sandbox loss surfaces in the journal (`ToolError` outcomes, a `WorkspaceReset` record, §5.5), never as silent corruption. | §5.3, §5.5 | continuous checker (`agent-event-grammar`: `SandboxBound`/`SandboxReleased` within the entity's activation window); crash/loss scenarios (`conformance_sandbox.rs`); cross-session isolation by construction | entity §5.2 (colocation) |

The tier obligations of the sandbox seam carry their own catalogue, **S1–S5** (sandbox §6). The agent runtime is also held to the core and entity testing contracts (core §18.1, §18.3; entity §14): seed-reproducibility of the full event stream including agent events, and fault-coverage accounting proving that model faults actually fired while agent traffic flowed; the entity's own faults (leader crash mid-write, shard quorum loss, eviction mid-command) are accounted for by the entity's own suite (entity §14).

---

## 12. Testability and deterministic simulation

### 12.1 Seams

The agent runtime adds **two** rows to the table the entity already virtualizes (entity §14, core §18.2); the journal is not among them, because it is the entity's:

| Seam | Production | Simulation |
|---|---|---|
| `Model` (§4) | `ProtocolModel` over an injected `HttpPost`, bound to `agent-anthropic`'s Messages protocol or `agent-openai`'s Chat Completions protocol; retry backoff from `Clock`/`Entropy` | **scripted model**: a deterministic function of the request and the seed; emits final messages, tool calls, malformed calls, and faults under seed control |
| `Sandbox` (§5.3) | a deployment-supplied `SandboxProvider` (process, container, or microVM; §13) | **scripted sandbox**: outcomes scripted per call by a closure over the tool's name and input; a toggled open failure, a fixed per-call latency, and crash and environment-loss shapes the script returns as the call's `ToolError` |
| *(journal)* | *the `EntityJournal` (entity §7.3, §7.4): the single-node or sharded-Raft durability tier* | *the entity's simulation seam (entity §14); the agent runtime drives the real consensus code, not a model of it* |

The `agent` crate itself MUST satisfy core §18.1: no wall clock, no OS threads, no unseeded randomness; all I/O launched through `Spawner` (§3.2). The agent runtime's `http` module owns the HTTP seam the production adapters share: `HttpPost`, the one socket operation a deployment implements over its HTTP stack; the `Protocol` trait, what an adapter decides for its wire format; and `ProtocolModel`, the shared shell with a bounded, jittered retry (see `agent::http` for the policy). `HttpPost`'s production implementation (the standalone deployment's, built on the separate `agent-http` substrate) is injected at construction. Each production `Model` crate (`agent-anthropic`, `agent-openai`) is a `Protocol` impl plus a `ProtocolModel` alias, the single home of its own wire protocol and nothing else; production sandbox providers are likewise separate crates. Because the journal, the fence, placement, and resume are the entity's, simulating the agent runtime *runs the real entity host, gateway, shard, and rehydration code* (entity §14) under the same seed. The agent's loop is the only new code on the simulated path.

### 12.2 Fault injection

Under seed control, a simulated agent run MUST be able to inject at least, on top of the core and entity faults (core §18.3, entity §14):

- **Model:** latency, `RateLimited`/`Overloaded` bursts, `ContextOverflow`, unknown tool names, malformed or schema-violating arguments (rejected by the loop and the executing tool respectively, §5.4), pathological tool-call loops (exercising budgets).
- **Sandbox:** a toggled open failure, a fixed execution latency (a call outliving its timeout, §5.3 item 3), a crash mid-call (orphaned calls, §5.5), and loss of the environment between steps (forcing a fresh sandbox and `WorkspaceReset`). These are scripted per scenario, not drawn from the seed: the script is a function of the call, and the outcomes it returns carry the crash and loss shapes.
- **Entity (inherited):** shard-leader crash and election mid-write (the fence, E1; a run resuming on the new leader, §7.5), shard quorum loss (`Unavailable`, §6.4), eviction mid-command (rehydration and the output gate), and a shard split under concurrent writes (entity §7.7).
- **Topology:** leadership moves under partition and heal (the orphaned-effect race, §5.5), cancellation racing completion across the tree.

A run with no faults is the simplest case and MUST still pass.

---

## 13. Future work

- **Post-commit emission in the workflow.** The events this runtime owes once a record commits (§10.4) are staged on the activation by the handler that decides the record and emitted by the tick that follows the commit; the workflow's engine stages its own events the same way (workflow §8, §11), and one staging could serve both. Nothing blocks on it.
- **Durable alarms for the agent.** A stored timer that re-activates a session to advance a run with no caller present: the basis for scheduled runs, server-side timeouts, and proactive agents. The entity half now exists (the alarm facet and its per-shard index, entity §7.16); what remains is the agent runtime-side integration: v1's resumption is strictly caller-driven (§7.5), and alarms would lift that.
- **Scheduler singleton.** Queued and recurring agent runs feeding ordinary `Submit`s; deliberately out of v1, and a natural client of durable alarms above.
- **Linearizable and follower reads.** A `tail` (or a future query) that reflects committed state on the minority side of a partition, via the entity's deferred leader-lease upgrade (entity §7.5, §16); follower reads for read scale likewise ride the entity's extension.
- **Sandbox providers, snapshots, and pooling.** The first per-tier provider ships as `agent-sandbox` (sandbox §3.1–§3.5). Still open: a network dataplane behind the profile's allowlist (sandbox §3.3) and warm pools to cut open latency.
- **Multi-tenant scheduling.** Quotas, fair-share scheduling, and accounting across tenants sharing the cluster: the economics of mutualization (§1.1).
- **Context compaction.** Summarizing the transcript into a journaled checkpoint record so the fold, and the model request, start from it; must preserve A1. The entity's snapshot (entity §9) bounds *replay* cost; compaction bounds *context* cost, a separate concern.
- **Token streaming.** Streaming partial output from `Model` while a completion is in flight; `complete` is one request, one response (§4.1). Record-level push observation is built (`SessionRef::follow`, §10.2).
- **Permission gating.** A per-tool authorization hook between intent and execution, the application-level analogue of the entity's per-`(peer, EntityName, manifest)` `Authorizer` (entity §13, core §15). The tier cap (§5.6) is the static half of authorization; this hook is the dynamic, per-call half.
- **Loop-executing tools.** A general extension point for tools that run in the loop with effects confined to the seams (ask-the-user, journal queries); v1's only loop-executing tool is the built-in `delegate` (§5.2).
- **Code mode.** Exposing the toolset as an API the model *programs against* rather than calls tool-by-tool, the generated program executing at the `Compute` tier (§5.6): fewer declaration tokens, fewer loop round-trips, same boundary.
- **External trace interop.** Mapping `root`/parent links (§10.3) onto W3C trace-context or OpenTelemetry ids at the boundary of an external collector.
- **Cross-session sagas.** Multi-session workflows above the single-session consistency boundary, built with idempotency the way the entity builds cross-entity sagas (entity §16).

---

## Appendix A: End-to-end example

```rust
// --- A kind: prompt, declared tools, sandbox profile; identical on every node (§7.1).
// --- The KindId is the entity type (entity §5.1); the registry is the allowlist (§5.2).
// --- `shell` and `read_file` execute in the session's sandbox (§5.3) at their declared
// --- tiers (§5.6); `delegate` executes in the loop (§8). The tier cap defaults to exactly
// --- what the declared tools require, here {Workspace, Native} (§5.3 item 4).
let researcher = Kind::new("You are a research agent.")
    .model(ModelParams::default())
    .sandboxed("shell", "Run a command in the workspace", &SHELL_SCHEMA, Tier::Native)
    .sandboxed("read_file", "Read a file from the workspace", &READ_SCHEMA, Tier::Workspace)
    .delegates_to(&["researcher"])          // the built-in `delegate` tool; allowlisted child kinds (§5.2, §8)
    .sandbox(SandboxProfile::image("workspace:base"))
    .entity(EntityConfig {                  // the kind IS an entity type's config (§7.1, entity App. A)
        shards: 16, replication_factor: 3,
        idle_after: Duration::from_secs(300), snapshot_every: 4096,
    })
    .budget(Budget { tokens: 200_000, steps: 50 });

// --- Per node: one Harness over the clustered actor system + entity; the model and
// --- sandbox seams injected here, the journal supplied by entity per kind (§7.4, §12.1).
let kinds = Kinds::new().register("researcher", researcher);
let h = Harness::cluster(system, &kinds, model, sandboxes);

// --- Any node: address the session by name; the shard leader hosts it (§7.2) ---
let s = h.session("researcher", SessionId::new("report-42"))?;  // EntityRef<Agent>; Err ⇔ the kind
                                                                //   is not in this node's directory (§7.4)
// prompt() = Submit (immediate ack) + await the RunCompleted notification (§7.3, §7.4);
//   the run outlives this call freely; re-prompt the same TurnId to re-attach.
match s.prompt(Turn::new(TurnId::new("t-1"), "Summarize the corpus on X.")).await {
    Ok(Ok(completion))                  => println!("{}", completion.text()),
    Ok(Err(RunError::BudgetExhausted))  => { /* grant a larger budget, resubmit a new turn */ }
    Ok(Err(run_err))                    => eprintln!("run failed: {run_err:?}"),   // Model | Cancelled
    Err(EntityError::Unavailable(_))     => { /* shard quorum lost; the run PAUSED, not ended;
                                                re-submit the SAME TurnId on recovery (§6.4, §7.5) */ }
    Err(EntityError::NotLeader(_))       => { /* retries exhausted; refresh shard map and retry */ }
    Err(EntityError::Call(Unreachable | Timeout))
        => { /* safe to re-submit the SAME TurnId: A7 dedups or attaches (§7.4) */ }
    Err(e)                              => eprintln!("call failed: {e:?}"),
}
```

## Appendix B: Crate and module layout

```
agent/                # the agent runtime (this spec): an agent is an entity (§2)
  agent.rs              #   the Agent entity: Entity/EntityHandler and Workflow impls,
                        #     perform and settle, and the environment pre-pass
                        #     (§3, §5.4-§5.6, §6)
  desired.rs            #   the desired set as a pure function of committed state: the
                        #     runtime's Workflow::pending, the Effect the workflow carries
                        #     from it to perform, and the StepKey that names one (§3.1)
  config.rs             #   HarnessConfig, the tuning every node agrees on (§7.2, §9.1)
  session.rs            #   SessionId (= EntityName), TurnId, Turn, Record (= the entity Event),
                        #     and the fold over records (§2, §6)
  model.rs              #   Model trait, ModelRequest/Response/Error (§4)
  http.rs               #   the HTTP seam: HttpPost, HttpResponse/HttpError, the Protocol
                        #     trait, and ProtocolModel, the shared adapter shell and its
                        #     seeded, bounded retry engine (§4.2, §12.1)
  tool.rs               #   ToolDecl, ToolRegistry, the built-in `delegate` (§5.2, §8)
  sandbox.rs            #   Sandbox + SandboxProvider seams, SandboxProfile, Tier (§5.3, §5.6)
  kind.rs               #   Kind, Kinds, the kind→entity-type/EntityConfig binding (§7.1)
  client.rs             #   Harness + SessionRef (= EntityRef<Agent>): addressing, routing, and the
                        #     attach protocol (the ephemeral reply-to actor behind `prompt` and
                        #     behind a delegation's wait on its child) (§7.4, §8.1)
  budget.rs             #   Budget, spend accounting, carve-outs (§9.1)
  event.rs              #   the agent runtime's checker-facing Event vocabulary (§10.4)
  testing.rs            #   the continuous A-checkers over that stream: run discipline
                        #     (A3/A4/A5/A7) and effect containment (A6/A8), behind the
                        #     `testing` feature, not part of the agent runtime API (§11)

agent-anthropic/      # Protocol impl for the Anthropic Messages API: path, headers,
                        #   request JSON, response classification; AnthropicModel is
                        #   ProtocolModel bound to it (§12.1)

agent-openai/         # Protocol impl for OpenAI-compatible Chat Completions (OpenAI,
                        #   OpenRouter, vLLM, llama.cpp, …); OpenAiModel is ProtocolModel
                        #   bound to it (§12.1)

agent-http/           # hand-rolled HTTP/1.1 client substrate for the deployment binaries;
                        #   the standalone node's HttpPost implementation is built on it (§12.1)

agent-sandbox/        # tiered SandboxProvider (sandbox §3): Workspace by cap-std capability
                        #   handle, Compute by hermetic wasmtime guests (raw modules, or
                        #   JavaScript via embedded QuickJS, feature-gated), Native by an OCI
                        #   container or a Firecracker microVM (feature-gated); ships s_catalogue()

agent-gateway/        # the public multi-tenant HTTP/SSE edge: a routing-only cluster
                        #   client that terminates tenant auth and addresses each session's
                        #   entity directly (§7.4)

agent-standalone/     # the node binary: a cluster member hosting the kinds and voting in
                        #   Raft, with no client-facing protocol of its own

agent-tui/            # the terminal client: drives one session over a gateway's HTTP/SSE
                        #   surface and renders the run's records as a transcript
```

`agent` depends on `entity` (the entity: identity, journal, fence, placement, activation, hibernation), and through it on `actor-core`, `actor-cluster`, and `actor-serialization`. It defines **no journal, no placement function, no fence, and no resume protocol** of its own; each is consumed from entity. The single durable thing the agent runtime names is the `Record` type (the entity's event) and its `apply` fold. Test-only pieces (the scripted model, the scripted sandbox, `a_catalogue()`, the conformance suites) live with the agent runtime's tests, dev-depending on `actor-simulation` and entity's simulation seam; the continuous checkers are the exception, shipping in `testing.rs` behind the `testing` feature because they are claims about the *agent runtime's* contract that any consumer's swarm can mount, as entity's own `testing.rs` does for the entity. The crate observes the workspace conventions: edition 2024, `unsafe_code = "forbid"`, `clippy::all = "warn"`, serde derives only.
