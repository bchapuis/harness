# Agent: Specification

**Status:** Draft v5
**Scope:** A distributed agentic runtime: compound AI systems (agent loops of model calls, tools, and control logic) run as **entities** on a mutualized cluster. An agent session is an entity ([`entity-spec.md`](entity-spec.md)) with a self-driving loop and two seams, the **model** and the **sandbox**.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Sections of the entity specification are cited as **entity §N** (its invariants as **E1–E21**); the actor framework ([`distributed-actor-spec.md`](distributed-actor-spec.md)) as **core §N**, the cluster utilities ([`cluster-utilities-spec.md`](cluster-utilities-spec.md)) as **util §N**, and the sandbox specification ([`sandbox-spec.md`](sandbox-spec.md)) as **sandbox §N**. Sections of this document are plain **§N**. Invariants defined here are **A1–A7**, kept apart from the entity catalogue (E1–E21, plus the facet contract F1–F4), the core catalogue (core §18.5), and the sandbox catalogue (S1–S5).

> **Design stance.** An entity is an actor plus a name-based virtual identity, an event-sourced journal, and a durability barrier on the reply (entity §1). **An agent is an entity plus three things**: a self-driving loop, a model seam, and a sandbox. Everything else (identity, journal, fence, placement, activation, hibernation, failover) is the entity's, unchanged.
>
> The agent adds one isolation boundary, between **control** and **effect**. Above it, the **loop is the entity's activation behavior**: serial, journaled, touching the world only through the model seam (§4), the sandbox seam (§5.3), and the journal. Where an entity waits for a command, an agent is **autonomous**: its activation drives itself model→tools→model until a run reaches a terminal outcome. Below the boundary, **effects live in a sandbox**: one isolated environment per session, colocated with the shard leader (entity §5.2), where tools run arbitrarily without endangering the node or another session (§5.1).
>
> An agent **remains an actor**: a session is addressed by name with `ask`/`tell` (entity §4.3), and an agent is defined by configuring a *kind*, not by writing a loop. The two new seams join the entity's virtualized journal, clock, entropy, transport, and spawner (entity §14, core §4.6), so one seed reproduces an entire multi-node run: model outputs, tool faults, crashes, elections, and resumes.

---

## 1. Scope and layering

The agent runtime runs, on **mutualized infrastructure**, the workload of a single-user agent process: long-lived sessions interleaving model calls, tool use, and delegation. Three ideas organize it, each discharged by the entity layer:

- **A session is a compound AI system, not a model call** (Zaharia et al., [*The Shift from Models to Compound AI Systems*](https://bair.berkeley.edu/blog/2024/02/18/compound-ai-systems/), 2024): the loop is explicit, journaled, and supervised; model and tools sit behind seams (§4, §5).
- **What a session keeps must live outside the loop** (Anthropic, [*Effective harnesses for long-running agents*](https://www.anthropic.com/engineering/effective-harnesses-for-long-running-agents), 2025): each activation starts with no memory of the last, so progress must persist in a durable artifact the next one reads. That artifact is **the entity's journal** (entity §7): the session *is* its journal, the activation a disposable fold of it (entity §1, §9), and anything not journaled is lost.
- **One isolation boundary, deliberately placed** (§5.1): the loop runs in the activation, so a waiting session holds no thread and, once hibernated, no memory (entity §10), while tool effects run in its sandbox. This is the *agents as infrastructure* move (Cloudflare, [*Project Think*](https://blog.cloudflare.com/project-think/), 2026): a durable, addressable session that costs nothing while idle, with effects contained by the architecture rather than by trust in the model.

Everything here is built **on top of** the entity (entity §1–§16) and modifies neither it nor the layers beneath: **actors, messages, supervision, and death watch** (core §3–§12) host the activation and carry every agent interaction as a typed message with a hand-written manifest (core §4.4), and the **event stream** (core §16, entity §13) carries the agent's observability events through the core's application-event extension point (§10.4).

The core non-goals (core §1.2) hold: the agent runtime never transparently retries a message with side effects, never places a quorum on the message path (the entity's quorum is on the durability path, entity §7.2), and never masks a failure. Where it makes a retry safe, it does so with an explicit idempotency key (the `TurnId`, §7.4), as core §7.2 prescribes and entity §2.2 requires.

The **persistent machine** ([`machine-spec.md`](machine-spec.md)) is a sibling consumer of the entity: an entity plus a durable disk (entity §7.15) and a network seam (machine §5), reached over SSH, reusing the sandbox's Firecracker `Native` mechanics (sandbox §3.5), with no run loop, model seam, or sandbox seam. The machine spec owns the difference; nothing here changes.

### 1.1 Non-goals

- **A model gateway.** One configured model implementation per request; provider routing, A/B serving, and quota brokering are not the runtime's concern.
- **Prompt management.** System prompts and tool descriptions are application data the runtime stores and transmits, not templates it provides.
- **Exactly-once tool execution.** A tool call is at-most-once per attempt (entity §2.2); across a crash-resume boundary recovery is explicit and per-tool (§5.5), never silent.
- **An isolation technology.** The spec mandates *that* effectful tools execute behind the `Sandbox` seam (§5.3), not *how*: process, container, or microVM is the provider's choice. A tool declares the capability tier its calls require (§5.6); each tier's realization is the provider's, bound by sandbox §3.
- **A new consensus mechanism.** No Raft group, shard, or fence of its own: durability, ordering, and single-writer safety are the entity's (entity §7, §8).
- **Multi-tenant economics.** Quotas, fair scheduling, and billing live above the runtime (§13); budgets (§9) bound a run's spend, not a tenant's share.
- **Context-window management.** A transcript that exceeds the model's context fails the run explicitly (§4.3); compaction is future work (§13).
- **Retrieval, vector stores, UI.** Applications build these as tools (§5) or clients (§7).

---

## 2. The session is an entity

### 2.1 Anatomy

A session is one durable thing and two disposable ones (entity §1):

- the **entity** *is* the session: its journal is the durable, totally ordered, single-writer record log (entity §7), and its identity (`EntityName`) is permanent. Only its loss loses the session.
- the **activation** is the fold of the journal plus the in-flight run, on the shard leader (entity §5.2). Stopping it loses nothing; the next activation, wherever the shard is led, rebuilds it by snapshot-plus-replay (entity §9).
- the **sandbox** is one isolated environment, colocated with the activation, where effects land; its working state is useful but never authoritative (§5.3), and losing it never loses the session (§5.5).

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
| Lossless failover | quorum read-repair of each entity's head under the new leader (entity §8, E14) |

The loop reaches the world only through the **model** (§4), the **sandbox** (§5.3), and the **entity's journal** (entity §7); time, randomness, spawning, and transport come from the core seams (core §4.6, §7).

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
| **Session** | One agent conversation, **realized as an entity**: a durable identity (`SessionId` = `EntityName`), the journal, and, while active, one activation on the shard leader. Survives restarts, hibernation, and leadership moves; an `ActorId` does not (entity §1). |
| **Record** | One journal entry, the entity's `Event` (entity §3); the transcript is the sequence of records and session state a fold over them (§6). The word avoids "event", which the observability stream uses (§10.4). |
| **Kind** | A named agent definition (`KindId` → system prompt, toolset, sandbox profile, model parameters, default budget, delegation allowlist). The `KindId` is the session's `entity_type` (entity §5.1); every node registers the same kinds (§7.1). |
| **Turn** | One submitted input: a user prompt or a parent's delegation (§8), carrying a client-chosen `TurnId`, the idempotency key (§7.4). |
| **Run** | The model→tools→model loop a turn triggers, until a terminal outcome; identified by the turn's `TurnId`. |
| **`RunCompleted`** | The `tell` carrying a run's outcome (`Result<Completion, RunError>`) to each registered reply-to when `RunEnded` commits (§7.3). The `RunEnded` record is the source of truth; the notification is delivery. |
| **Step** | One loop iteration: one model call plus the resolution of every tool call and delegation it requested. |
| **Activation** | The entity's live instance on its shard leader (entity §5), running the loop; the unit E6 and A5 count. |
| **Model** | The inference seam (§4), implemented by `agent-anthropic` (Anthropic Messages API), `agent-openai` (OpenAI-compatible Chat Completions), and a scripted model in simulation. |
| **Tool** | A capability the model may invoke, declared to the runtime; all but the built-in `delegate` execute in the sandbox (§5.2). |
| **Sandbox** | The isolated environment bound to one activation behind the third seam (§5.3): the only place sandboxed tools run and their effects land. |
| **Tier** | A named **capability** set a tool call requires and an activation acquires, journaled before first use (§5.6); semantics in sandbox §2. Unrelated to the entity's `Local`/`Quorum` *durability* tiers (entity §7.4). |
| **Budget** | A run's spending limit (model tokens and steps), from which child budgets are carved (§9). |
| **Root** | The parentless ancestor of a delegation tree, recorded in every `SessionCreated` (§10.3); its `SessionId` names the tree. Correlation metadata only: nothing routes or folds on it. |

Identity is layered as the entity layers it: `SessionId` (= `EntityName`: durable, application-chosen) → `ActorId` (one activation, system-assigned, core §3.6) → `TurnId` (one run). The runtime owns only the first mapping, as a naming convention; entity owns name→shard→leader resolution (entity §5.4).

---

## 3. The agent: an autonomous entity

An active session is the entity's activation on its shard leader (entity §5.2): an ordinary actor (core §3) whose private state is the folded transcript plus the in-flight run, reachable only through its handlers (core §3.5). The agent adds **autonomy**: the activation drives itself, issuing model and tool calls and feeding their outcomes back, until the run reaches a terminal outcome.

### 3.1 The run loop

Runs serialize through the session's **turn queue**, a fold projection (§6.2): every accepted turn is journaled at once (`TurnSubmitted`, §7.3) and waits in acceptance order; when no run is live, the loop starts the head by committing **`TurnStarted`**, the one transition that begins a run, the only place the turn's content enters the model-visible transcript, and the commit that fires `RunStarted` (§10.4). At most one run is live per session; because the queue is journal-derived, an acked turn survives crash, hibernation, and migration like the run ahead of it (§7.5).

A run advances in **steps**. Each step:

1. checks the budget (§9.1); if exhausted, the run ends with `RunError::BudgetExhausted`;
2. issues one model call carrying the transcript (§4.2);
3. if the response is a final assistant message, journals it and ends the run with `Ok(Completion)`;
4. otherwise journals the model response (intent before effect, §6.3), then executes every requested tool call (§5) and delegation (§8);
5. journals each outcome as it arrives; when all are resolved, begins the next step.

A run MUST end in exactly one terminal outcome, journaled as its `RunEnded` record (A2): `Ok(Completion)`, `RunError::BudgetExhausted`, `RunError::Cancelled`, or `RunError::Model(…)` (a model failure no retry policy absorbed, §4.3). A terminal outcome *is* a committed record, so durability failure cannot be one: it surfaces as the entity's outer `EntityError::Unavailable` and **pauses** the run (§6.4, §7.5). `RunError` is an application error and travels as a value (core §3.2 rule 4, entity §4.2), in the `RunCompleted` notification, **not** the `Submit` reply; transport and durability failures stay in the `EntityError` on the `Submit` ack (§7.3).

### 3.2 The loop drives itself but never blocks on I/O

The loop is **message-driven** (core §6, entity §6): the activation MUST NOT await the model, a tool, or a durable append inline. It launches each external operation through `Spawner` (core §4.6) and receives the outcome as an ordinary message; a step is a state the fold tracks, not a stack frame. **I/O lives in the loop, never inside `apply`**, which MUST be pure because it is replayed (entity §4.1).

**The cycle is the workflow's** (workflow §3.2, §3.3, §4): this runtime implements `Workflow`, with `pending` the desired set as a pure function of committed state (`desired.rs`), `perform` one effect on the seams, and `settle` the fold of one outcome. The environment reconciliation of §5.4–§5.6 runs as a **pre-pass ahead of every tick**, because no fold can express it.

Three consequences, each inherited:

- **The input gate orders the run.** While an append is in flight, the entity admits no new command to the session (entity §6), so a `Cancel` (§9.2) takes effect at a message boundary and never observes half-committed state.
- **The output gate is the write-ahead barrier.** A model response, tier acquisition, tool outcome, or terminal outcome is released to the next step (or a `Submit` caller) only after its record commits on a shard quorum (entity §6). The runtime's write-ahead discipline (§6.3) is this gate and nothing more.
- **Straggling outcomes are discarded.** The activation MUST discard, not journal, an outcome that arrives after its run has ended (a cancelled run's late tool result, §9.2) or after the activation stepped down on a non-contiguous commit (entity §6, the contiguity guard).

### 3.3 Supervision, restart, and migration

Supervision (core §11) governs the activation's faults. The entity host keeps the default `Stop` directive (core §11.2), so a faulted activation stops and the gateway prunes it (entity §10); recovery is the entity's rehydration (entity §9, §10) in three guises:

- a **re-activation after a fault**, on the same leader, triggered by the next message (entity §10);
- a **migration** follows shard leadership (entity §8.3): the activation rehydrates on the new leader, losing no acknowledged record (E14);
- a **resume after node failure** is the same rehydration, triggered on a new leader by the next message (§7.5).

All three are snapshot-plus-replay, so the activation behaves byte-identically to one that never stopped, given the same subsequent model and tool outcomes (A1, over E2/E3).

---

## 4. The model seam

### 4.1 The trait

```rust
/// Inference: one request, one response; no streaming in v1 (§13). The first agent-runtime seam.
pub trait Model: Send + Sync + 'static {
    async fn complete(&self, req: ModelRequest) -> Result<ModelResponse, ModelError>;
}
```

The seams' methods are written `async fn` here and at the sandbox seam (§5.3); the crate spells them as methods returning `BoxFuture`, because every seam is held as a trait object (`Arc<dyn Model>`, `Arc<dyn SandboxProvider>`, `Arc<dyn Sandbox>`) and `async fn` in a trait is not object-safe.

`ModelRequest` carries the kind's system prompt, model parameters, the toolset's declarations (name, description, input schema), the folded transcript, and `max_tokens`. `ModelResponse` carries the assistant content, zero or more tool calls, and the **reported usage** (input and output tokens) that feeds budget accounting (§9.1).

Like `Transport` (core §7) and `EntityJournal` (entity §7.3), the runtime depends only on the trait. In production `ProtocolModel` implements it over an injected `HttpPost` (§12.1), with `agent-anthropic` and `agent-openai` supplying the protocols; the simulator supplies a **scripted model**, a deterministic function of the request and the seed (§12.2).

### 4.2 Determinism rules

1. A `Model` implementation MUST take all timing from `Clock` and all randomness from `Entropy` (core §4.6); `ProtocolModel` (§12.1) pre-draws its whole backoff schedule so the completion future owns no entropy handle. The runtime crate MUST NOT read the wall clock, spawn OS threads, use an unseeded RNG, or open a socket: `HttpPost` is a trait a deployment implements (core §18.1).
2. Model output is nondeterministic in production, so the contract is scoped like the network's and the journal's: given the same seed, **the simulated model** returns byte-identical responses, and everything downstream of a fixed response sequence is reproducible (A1).

### 4.3 Failure

`ModelError` distinguishes at least `RateLimited`, `Overloaded`, `ContextOverflow`, `InvalidRequest`, and `Api(String)`. A `Model` implementation MAY retry internally under a bounded policy with backoff from `Clock`/`Entropy`; a completion request is side-effect-free, so this respects core §1.2. A failure that survives the policy ends the run with `RunError::Model(…)`: journaled, reported, never swallowed (core §8).

---

## 5. Tools and the isolation boundary

### 5.1 The boundary

A run splits across one boundary:

| In the entity's activation: the **loop** | In the sandbox: the **effects** |
|---|---|
| control flow: steps, budgets, delegation | tool execution: shell, files, processes, network |
| model calls, through the `Model` seam (§4) | working state: processes and regenerable caches; the workspace's durable subtree is the session's own facet (entity §7.11, §5.5) |
| the fold and the journal, through the entity (entity §6, §7) | everything a tool's side effects touch |

Above the boundary the loop is serial, journaled, and cheap, the mutualization premise of §1 (activation is local and consensus-free, entity §7.8); below it, a tool may do anything *inside* its session's sandbox and nothing outside it. Actor isolation (core §3.5) protects sessions' state from one another; sandbox isolation protects the node and other sessions' effects from tools.

**Why the boundary cuts here.** With **no sandbox**, one session's `rm -rf` or runaway build takes down the node (a shard leader hosting many entities, entity §5.2) and every co-located session; with the **loop inside the sandbox**, every session pays for isolation to host control flow that mostly waits, and the loop becomes as opaque to the runtime as the tools; with **a sandbox per tool call**, consecutive calls lose the checkout, build cache, or running server the previous step produced.

Hence one sandbox **per session**, opened lazily per activation (§5.3) and colocated with the shard leader (entity §5.2): coarse enough to keep working state across calls, fine enough that no effect escapes its session. The directory the sandbox runs over is the session's **workspace facet** (entity §7.11): each tool call's file delta commits atomically with its `ToolOutcome` record, and every activation, including one on a new leader, rematerializes the directory from the journal. What remains working state is the *environment* (processes, containers, held tiers, regenerable excluded trees), whose mid-activation loss §5.5 puts on the record. The facet's durable cap (64 MiB, the entity's `MAX_TREE_BYTES`) bounds the capture: a call whose post-call capture exceeds it is reported to the model as `ToolError::Sandbox` (§5.4) with its file delta unpersisted, an application outcome the model trims rather than a run failure.

### 5.2 Declarations and the registry

Every tool is **declared**; the model and the loop need its interface wherever it executes:

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
/// The workflow's type, re-exported (workflow §3.1): a declaration's recovery
/// policy is exactly what a `Pending` carries. `Reexecute`: blind re-execution is
/// safe (idempotent, or dedups like `delegate`, §8.1); `Interrupt`: resolve as
/// ToolError::Interrupted and let the model decide whether to retry.
pub use workflow::OnOrphan;

/// The capability set a tool call requires (§5.6); full semantics in sandbox §2.
pub enum Tier {
    Workspace, // the session's scoped filesystem, through host-implemented typed tools; no guest code
    Compute,   // arbitrary guest code over the workspace; no network, no ambient clock or entropy
    Network,   // compute, plus egress to the profile's allowlist (§5.3)
    Native,    // OS processes and native binaries inside the confined environment
}
```

Tiers are capability sets, not rungs of a ladder: a kind's cap is a *set* of tiers, not a maximum, so a kind MAY hold `Native` without `Network` (sandbox §2.2).

A kind's `ToolRegistry` is a hand-built list of declarations, like `HandlerRegistry` (core §4.4) and the entity's dispatch registry (entity §5.5): explicit, inspectable, and the **allowlist**. A tool call dispatches by name against the registry and nothing else; no path leads from model output to code outside the declared set.

Every requested tool call carries a **`CallId`**, unique within its run (the model API's tool-use id, or one the runtime assigns on receipt), journaled with the call's intent (§6.3). Outcomes reference it, orphaned-call resolution matches by it (§5.5), and child-session derivation is keyed by it (§8.1).

Every declared tool is **sandboxed**: its call dispatches through the `Sandbox` seam (§5.3) and executes nowhere else. The one exception is the built-in **`delegate`** (§8), which executes in the loop because a delegation is control flow (a child `Submit`), not an effect. v1 exposes no extension point for loop-executing tools (§13).

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

1. **Lazy, per tier.** An activation opens its sandbox on its first sandboxed call; a session that never needs one never pays for one. Each call carries its declared tier (the seam hands the sandbox no registry), and the provider builds a tier's environment on the first call carrying it (§5.6).
2. **Bound per activation.** At most one live sandbox per activation, and deactivation MUST release it (A7), whether by migration, hibernation, or a forced step-down on a non-contiguous commit (entity §6). The sandbox never migrates; a new leader opens a fresh one (§5.5).
3. **Launched, never awaited inline** (§3.2): outcomes return as messages, bounded by a per-tool timeout from `Clock`: the call's declared `timeout` (§5.2), else a configurable runtime default (SHOULD default to about **5 minutes**). The timeout bounds the *outcome*, not the effect: the sandbox-side process MAY still be writing when the `Timeout` outcome is journaled, so the workspace capture that commits with it (entity §7.11) can be **torn**. The journal stays consistent (it records what the directory held at capture), the next call's capture diffs against the last committed index and folds in whatever the straggler finished, and the model reacts with the timeout in view.
4. **Profiled.** The kind's `SandboxProfile` (image or toolchain, resource limits, network policy) is deployment configuration, agreed cluster-wide like the kind (§7.1). It carries the kind's **tier cap** (the tiers its sessions may hold, §5.6) and, when the cap includes `Network`, that tier's egress allowlist (sandbox §3.3), which a `TierAcquired` record grants by reference, never inline. Declaring a tool whose tier the cap excludes is a configuration error, surfaced at registration as loudly as a duplicate name; the cap defaults to exactly the tiers the declared tools require.
5. **Technology-opaque, within a tier.** Process, container, or microVM is the provider's choice; the scripted sandbox is one more implementation (§12). *Which capabilities a call holds* is declared, journaled, and agreed (§5.6), never improvised by the provider. The seam sees one `Sandbox`; tiers are provisioned within it.

### 5.4 Tool failure is a transcript value

A failing tool does not fail the run. The runtime journals its `ToolError` (timeout, sandbox-side crash, failed delegation (§8.2), unknown tool name, or malformed arguments) as the call's outcome and **returns it to the model** as the tool result; the only abnormal run endings are the three `RunError` outcomes of §3.1. Unknown and malformed calls are synthesized outcomes, and the registry-as-allowlist guarantees nothing ran. Malformed is structural, not schematic: the loop rejects before dispatch a call whose input is not a JSON object (or a `delegate` input that does not deserialize, §8.1), but does not validate against `input_schema`, which is interface metadata for the model (§4.1) and digest-covered agreement (§7.1); validation beyond shape belongs to the executing tool, whose rejection is its own `ToolError` outcome.

### 5.5 Crash, loss, and resume (no silent loss)

A tool call's **intent** is journaled before execution and its **outcome** after (§6.3); a crash, hibernation, or migration between the two leaves an *orphaned call*. On the next activation (entity §10, §7.5):

- an orphaned call declared `OnOrphan::Reexecute` MUST be re-executed, in a fresh sandbox if the old one is gone (and may fail for that reason, per §5.4);
- an orphaned call declared `Interrupt` MUST be resolved as `ToolError::Interrupted` and fed to the model (§5.4), so the *model* decides whether to retry the side effect.

The same settling path maps every `EffectError` variant to a tool outcome: `Interrupted` to `ToolError::Interrupted`; `Failed` (a `perform` that panicked, or a cancel the loop could not send, workflow §4.3) to `Sandbox`; `Timeout` and `Lost` to `Timeout` and `EnvironmentLost`. The runtime produces only the first two; a tool's own timeout and a lost environment arrive as the `ToolError` the call returns.

An orphaned call is not always dead. The fence guarantees one *transcript* (entity §8) but cannot recall a launched effect: a deposed leader not yet aware of it (the read-your-leader window, entity §7.5) may still be *executing* a slow call when the new leader resolves it, so a `Reexecute` may run concurrently with the original, and a model retrying after `ToolError::Interrupted` may duplicate a side effect still in progress. The window is the duration of the in-flight call, not a round-trip: the deposed-leader cost of entity §11, made concrete for effects. `delegate` alone is rescued by construction: its re-execution is a child `Submit` with the same `TurnId`, which dedups into an attach (§7.4, §8.1).

The sandbox is **not session state**: the fold never reads it, and the runtime never reconstructs a lost *environment* (processes, containers, tier grants). The workspace's **durable subtree** survives hibernation, migration, and crash-failover by construction (§5.1, entity §7.11). Anything outside that capture (excluded trees like `target/`, running servers, in-memory state) must outlive the sandbox through a tool: push a branch, upload an artifact, return a result the loop journals. Per core §1.2, the runtime never re-fires a non-idempotent side effect on its own authority.

Nor may the *model* be left pretending. A routine reactivation (hibernation §7.2, migration §3.3, crash-failover) journals **no reset**: the durable subtree is rematerialized before the sandbox opens, so the model's picture of its files stays true. A **mid-activation environment loss** (the provider returned `EnvironmentLost`: processes, excluded trees, and held tiers gone, durable files intact) is different: the activation MUST journal a **`WorkspaceReset`** record before the next model call and surface it in that request. The record answers no `CallId`, so it enters the request as runtime-authored input content rather than a tool result.

Tier loss follows the same rule at two granularities, surfaced and never silently repaired:

- **The whole environment is lost.** A fresh sandbox resets **every** tier. The held set restarts at `Workspace` in the activation that journals `WorkspaceReset` (§5.6), and re-acquisition is re-journaled: within one run the journal reads `TierAcquired`, the tool outcome, `WorkspaceReset`, then `TierAcquired` again before the next call at that tier.
- **One tier is lost while the sandbox survives.** Calls that needed it fail as `ToolError` outcomes (§5.4); nothing is journaled about the tier. The provider MAY re-provision it lazily under the acquisition this activation already journaled, and MUST NOT grant a tier the activation never journaled (sandbox §4).

### 5.6 Execution tiers

A **tier** is a named capability set a tool call requires: `Workspace`, `Compute`, `Network`, or `Native` (§5.2). This document owns the contract (declaration, record, cap, agreement); the sandbox specification owns what each tier grants and withholds (sandbox §2) and what a provider MUST guarantee to offer it (sandbox §3).

1. **Opening grants `Workspace` and nothing else.** A session that never acquires more holds a directory's worth of capability.
2. **Acquisition is journaled, intent before effect.** Before the first call at a tier the activation does not yet hold, the loop journals a **`TierAcquired { turn, tier }`** record (the output gate, entity §6). The record is the audit trail (when did this session first run guest code, first touch the network?) and the policy hook (§13).
3. **Held tiers are additive and environment-scoped.** No call revokes a tier; the held set resets to `Workspace` only when the loop journals `WorkspaceReset` after a mid-activation `EnvironmentLost` (§5.5), and a fresh activation starts from `Workspace`. Held tiers are working state; only the acquisition records survive.
4. **The cap is unreachable by construction.** Declared tools are checked against the cap at registration (§5.3 item 4), and dispatch reaches declared tools only (§5.2), so acquisition beyond the cap cannot be requested. Construction discharges the invariant (sandbox §6 S4); the loop performs no runtime check.
5. **Agreement.** Each tool's tier and the profile's cap are covered by the kind digest (§7.1): cluster-wide agreement, never a node-local choice.
6. **A record, not an event.** Acquisition is durable, user-facing audit (§10.1), with no §10.4 event; the acquisition ordering of sandbox §6 S4 is verified by journal audit of `TierAcquired` records.

---

## 6. Durability is the entity's

This document specifies **no journal seam**: durability is the entity's journal (entity §7), ordering and single-writer safety its Raft leader (entity §8), and the write-ahead barrier its output gate (entity §6). The mechanism, the trait, and the two **durability** tiers (single-node, sharded-Raft) are entity §7.3–§7.4; this section states only how the agent uses them.

### 6.1 Records are the entity's events

Each **record** is an entity `Event` (entity §3), appended in `Seq` order, totally ordered per session, durable on a shard quorum. The record types: `SessionCreated`, `TurnSubmitted` (a turn accepted into the queue, §7.3), `TurnStarted` (the queue's head began its run, §3.1), `ModelResponse`, `TierAcquired`, `ToolOutcome` (carrying a `CallId`), `ChildRun`, `WorkspaceReset`, `RunEnded`, and `CancelDelivered` (a propagated cancel's delivery receipt, §9.2). `apply` (entity §4.1) folds them into session state and MUST be pure and deterministic (E2): it runs on the commit path and on every replay, and the agent's non-determinism (model output, tool effects) enters only *as committed records*.

### 6.2 State is a fold; the fence is Raft

A session's state MUST be a pure, deterministic function of its journal prefix, `state = fold(records)` (entity §1, E3); nothing outside the fold influences later behavior except new messages. Replay is therefore resume (§3.3, §7.5).

The single-writer guarantee is the entity's: only the shard leader appends, Raft elects one leader per term, and a deposed leader cannot commit (entity §8, E1). When two nodes transiently believe they lead a shard, the minority reaches no quorum and the transcript never forks; the contiguity guard (entity §6) steps down any activation whose in-memory head can no longer be trusted, and the forced step-down releases the sandbox (§5.3 item 2), emitting the entity's `Deactivated` (entity §13) and the runtime's `SandboxReleased` (§10.4).

### 6.3 Write-ahead discipline is the output gate

Within a step, the entity's output gate (entity §6) holds each effect until its record commits on a quorum:

- a **turn's acceptance** (`TurnSubmitted`) before the `Submit` ack releases (§7.3), and the **run start** (`TurnStarted`) before the run's first model call issues (§3.1);
- a **model response** before any of its tool calls execute (intent before effect, §5.5), each call identified by its `CallId` (§5.2); a model call is side-effect-free and needs no intent record;
- a **tier acquisition** (`TierAcquired`) before the first call at a tier not yet held (§5.6);
- a **tool or delegation outcome** before the next step shows it to the model;
- the **terminal outcome** (`RunEnded`) before any `RunCompleted` carries it (§7.3): the notification is sent from the tick after the commit, or, for a re-submission of an ended turn, from the fold's recorded outcome, so a caller never holds a completion the journal could lose.

The barrier is entity §6 (E5); the runtime adds only *which records* a step writes and in what order.

### 6.4 Durability failure

A shard that cannot reach a quorum returns `AppendOutcome::Unavailable` (entity §7.3, §11): the entity pauses writes, the activation steps down, and the in-flight record does not commit. The loop MUST NOT proceed past an uncommitted record, and since it cannot journal a terminal outcome either, the run **pauses** rather than ends. The caller's pending reply resolves as `EntityError::Unavailable` (or its `ask` deadline lapses, core §14.2), and the run stays orphaned until the shard recovers and a message reaches the session, which resumes it from the last committed record (§7.5). This is the entity's CP stance (entity §2.2, E11): pause, never fork, never mask the loss, never invent an outcome that cannot be recorded.

---

## 7. Sessions across the cluster

A session's cluster life is the entity's life cycle (entity §10), and only the journal survives it: **created** (first turn) → **activated** on its shard leader (rehydrate → fold → run) → **serving** (§3) → **deactivated** (hibernation, migration, or step-down) → **rehydrated** by the next leader (§7.5). The runtime adds no placement, host, or resume protocol.

### 7.1 Kinds

Every node's runtime is configured with the same `KindId → Kind` map: system prompt, `ToolRegistry`, `SandboxProfile`, model parameters, default budget, delegation allowlist (§8.1). A `KindId` is an `entity_type` (entity §5.1), and the kind carries that type's `EntityConfig` (shard count, replication factor, idle window, snapshot policy; entity Appendix A). Kinds are code-and-config, agreed cluster-wide like the codec (core §5): a session MUST be resumable on any node, so every node MUST register every kind. `SessionCreated` pins the session's kind and a **digest** of its definition (§10.5), covering among other fields each tool's tier and the profile's tier cap (§5.6). The per-kind factory captures the definition, so activating a hosted type needs no lookup; `HarnessBuilder::host` panics in the call itself for a kind the registry lacks (so the error points at the line naming the kind, not at `build`), and `Harness::session` returns `UnknownKind` for a kind the node's directory neither hosts nor routes (§7.4). A missing kind is a deployment error surfaced before any run starts (nothing journaled, no `RunStarted`), never a silent fallback.

Registration is knowledge; a node's **directory**, the kinds it *hosts* (serves, with seams injected) plus those it only *routes to* (the cluster-client path, entity §5.3), is per-node deployment shape. Harness construction MUST reject a directory that does not close over delegation: every hosted kind's delegation allowlist must be in the directory, because the parent's node launches the child `Submit` (§8.1) and delivers owed cancels (§9.2) through its route for the child's kind. Otherwise every delegation to that kind would fail per call, and an owed cancel would wait for a better-configured node to lead the shard (A4's liveness). Rejecting at build makes the gap a deployment error, never a mid-run discovery.

### 7.2 Placement, activation, and hibernation

Placement is the entity's: the name hashes to a shard, the shard map gives its leader, and the session activates there (entity §5.1, §5.2, §5.4). The runtime defines no host registry or placement function. The gateway (entity §5.3) gets-or-activates the session exactly once per node (E6) and routes commands to it.

The leader activates a session lazily on the first message: rehydrate (entity §9) → fold → run. Activation takes no consensus; on the clustered tier it costs one quorum head-recovery round trip, then reads locally (entity §7.8, §9).

**Hibernation (idle stop).** After a configurable idle interval the leader MAY hibernate a session (entity §10): mutate nothing, snapshot to bound the next replay, release the sandbox (A7), and drop the activation. It MUST NOT hibernate a session whose run is live or whose queue holds an accepted turn (§3.1). Hibernation needs no flush (everything worth keeping is a record); the environment's processes and containers go with the sandbox, and the workspace's durable subtree rematerializes on reactivation (entity §7.11, §5.5). `idle_after` SHOULD default to **minutes**, the entity default (entity §10), not the Durable Objects eviction window it is modelled on: reactivation costs a quorum head-recovery plus a snapshot-bounded replay, and memory is not the resource under pressure. The idle timeout trades sandbox cost, the expensive shared resource, against environment continuity (warm containers, build caches).

**Stale-leader redirect.** A command reaching a node that no longer leads the shard receives `NotLeader(hint)` (entity §5.4, §8); the runtime retries at the hinted node's gateway within the caller's deadline, ordinary Raft client redirection made safe by the `TurnId` (§7.4).

### 7.3 The wire contract

A session is addressed as an entity, and its commands are entity commands (entity §4.2) with hand-written manifests (core §4.4):

| Command | Reply (`M::Reply`) | Manifest |
|---|---|---|
| `Submit { kind, turn, parent?, reply_to: Option<ActorRef<…>> }` | `Result<Accepted, SubmitReject>`† | `agent.Submit` |
| `Cancel { turn }` | `()` | `agent.Cancel` |

The session is the entity name the command is addressed to, not a payload field (entity §5.4). `parent` carries a delegating parent's lineage (§8.1, §10.3) and is absent on a root submit.

Reading a run is **not** a session command: `tail` (§10.2) rides the entity's gateway-level event read (entity §7.5), a framework built-in like `subscribe`, so it appears in no allowlist and never touches the activation.

†`Submit` replies with an **ack, not the run's outcome**: `Accepted { turn: TurnId, status: Started | Attached | Ended }`, released when the `TurnSubmitted` record commits (an output-gated reply, entity §6), so **an acked turn is durable** in the fold's queue (§3.1). `Started` means "newly accepted", whether the run begins now or behind the queue; there is no separate queued status. The `Err` arm, `SubmitReject` (`KindMismatch` or `ContentConflict`, §7.4), is an *application* failure carried as a value, distinct from the transport `EntityError` outside the reply (core §3.2, §14): a caller-contract violation is permanent, where a peer move could clear a transport failure. Each command goes through `EntityRef::ask` (entity §4.3), which surfaces `EntityError` (`NotLeader`, `Unavailable`, an unregistered manifest; entity §12) outside the reply. `Cancel` commits one `RunEnded { Cancelled }` record and acks, ending the live run or a still-queued turn (§9.2).

The run's outcome travels separately, as a notification the activation `tell`s when `RunEnded` commits:

| Notification | Payload | Delivered to |
|---|---|---|
| `RunCompleted` | `{ session, turn, outcome: Result<Completion, RunError> }` | every reply-to `ActorRef` registered for the run |

`RunCompleted` is a `tell` (core §3.3): fire-and-forget, at-most-once. It is **delivery, not the source of truth** (the `RunEnded` record is, §10.1), so a lost notification is recovered by re-contacting the session (§7.4), never by transparent retry (core §1.2). It is not a §10.4 event: `RunEnded` already marks termination on the checker stream.

**Why the outcome is a notification, not the `Submit` reply.** An `ask` reply is a `ReplyHandle` (core §4.5), resolved exactly once *within the delivery of that one message* and not storable across later messages. A run spans many inbound messages (each model and tool outcome, §3.2), so no handle survives until `RunEnded`; the outcome goes instead to a reply-to `ActorRef` the caller placed in the command (core §3.3: "An `ActorRef` MAY be a field of a message"). Attaching to a live run (§7.4) is registering another reply-to.

The `Submit` `ask` returns promptly, and nothing a caller holds bounds the run: budgets (§9) bound tokens and steps, not wall time (tool timeouts, model retries, and child runs compound), so a run MAY outlive any caller. A caller that gives up or crashes leaves the run untouched and recovers the outcome by re-submitting the same `TurnId` (§7.4) or following `tail` (§10.2). There is no status message: a status view (head `Seq`, live run, spend) is a client-side fold over `tail`.

`Submit` carries `kind` on every call, though it binds only on the first (creation is implicit in the first turn); afterwards it is a checked redundancy, rejected on mismatch (§7.4).

A read (`tail`, or any future query that emits no record) commits nothing (entity §6 step 2) and is a local, replication-free read of the leader's journal (§10.2). It is **read-your-leader**, not linearizable under partition (entity §7.5): a deposed minority leader not yet fenced MAY serve a slightly stale `tail` until it steps down, though writes never fork. A linearizable read is a deferred entity upgrade (entity §16).

### 7.4 The client view (`SessionRef`) and idempotent submission

```rust
let h = Harness::cluster(system, &kinds, model, sandboxes);
//   per node: registers each kind's gateway and injects the model and sandbox seams (§4, §5.3)
let s = h.session("researcher", SESSION_ID)?;      // an EntityRef<Agent>; a local hash, no I/O (entity §5.1,
                                                   //   §5.4); Err only for a kind this node neither hosts nor
                                                   //   routes, a value since kind names arrive from outside
let out = s.prompt(Turn { id: TURN_ID, content }).await;   // Submit + await RunCompleted (§7.3):
                                                   //   Result<Result<Completion, RunError>, EntityError>
```

`SessionRef` is `EntityRef<Agent>` (entity §4.3) with a thin agent-facing surface. `prompt(turn)` is a **blocking convenience at the edge, not a protocol primitive**: it spins an ephemeral local actor as the reply-to, `Submit`s the turn carrying that ref, and awaits the one `RunCompleted`. `prompt` is `prompt_within` at the runtime's `submit_deadline` (30 s by default), handing the whole wait to one `Submit`-and-attach attempt: a lapse surfaces as `EntityError::Call(CallError::Timeout)`, and `prompt` re-`Submit`s the same `TurnId` only when the attempt's mailbox drops without an outcome, re-registering on whatever activation now hosts the session. Delegation's `run_child` (§8.1) is the cadenced form: each attempt attaches for at most one cadence, and every lapse re-`Submit`s the same `TurnId` until the budget-scaled wait expires. A non-blocking caller `Submit`s with its own reply-to (as a parent agent does, §8.1) or follows `tail`. A lapsed attempt leaks no actor: its mailbox stops with it, and a notification racing that stop is an ordinary lost delivery (§7.3).

`prompt` MUST NOT transparently retry a failed `Submit` (core §1.2, entity §2.2). The entity re-issues a command that provably did not commit (`DeadLetter` from a hibernated stale host, `NotLeader`; entity §4.3, §6), but never an *ambiguous* failure (`Unreachable`/`Timeout`), which may have committed before the ack was lost. The **`TurnId`** makes the caller's own re-submission safe:

- a `Submit` whose `TurnId` is already journaled MUST NOT start a second run (one `TurnSubmitted` record, E1): if the turn is queued or its run live, the activation **registers the new reply-to** (`status: Attached`); if it ended, the activation immediately **`tell`s the outcome** recorded in `RunEnded` (`status: Ended`). Dedup and recorded outcome are a fold over the session's records (queued turns included), exact across a resume;
- registration is race-free: the serial activation orders a re-submission against the `RunEnded` transition (the input gate, entity §6), so a caller is either added to the live run or told the outcome, never neither or both;
- `reply_to` is **transient routing, not part of the turn**: unjournaled and **excluded from the content-equality check**, so a fresh reply-to on the same `TurnId` is the ordinary re-attach.

**What the equality check covers.** The turn's content **and its literal `budget` field**: a changed budget is not a silent attach, and `None` is not `Some(default)`, so conflict never depends on the kind's *current* default. The digest is fold-local, recomputed from the journaled `TurnSubmitted` (which records the resolved budget and whether it was explicit), and length-prefixed so content and budget cannot collide by juxtaposition. Delegation re-execution re-submits the budget read from the journaled `ChildRun`, re-deriving the same literal (§8.1).

**What the digest is not.** A fast 64-bit content hash guards against *accidental* `TurnId` reuse, not a crafted collision (which would attach rather than conflict): it is a caller-bug detector, not a security boundary, since a caller who can address the session already holds it. A gateway scopes session names by principal for that reason.

**What a mismatch produces.** A re-submission whose turn differs (`SubmitReject::ContentConflict`) or whose `kind` differs from the entity's type (`SubmitReject::KindMismatch`) is a caller bug: rejected as a value, journaling nothing, never a transport `EntityError`, because it must not be retried (§7.3†). The blocking conveniences collapse it into their own error form (`prompt` as `EntityError::Call(CallError::System(…))` with the reject's display text, `run_child` as `ToolError::Delegation`); a caller that `Submit`s with its own reply-to reads the `Err` arm directly.

The **subscriber set** (reply-tos registered for a live run) is ephemeral activation state, like the held tiers and the sandbox: never journaled, rebuilt by callers re-contacting after a resume.

### 7.5 Resume after node failure

When a shard leader fails or is partitioned away, the replicas elect a new leader (entity §8.3), which recovers each entity's committed records from a quorum on first access (entity §8, E14). The next message routed to the shard activates the session there: rehydrate (entity §9, rematerializing the workspace facet), fold (§6.2), resolve orphaned calls (§5.5), and continue any unfinished run. No coordinator hands the session over, and no acknowledged record is lost (E14, A1).

Resumption is **caller-driven**: the entity has no cross-entity enumeration to scan for orphaned work, so an interrupted run resumes only when a message next reaches the session: a caller re-submitting its `TurnId`, a parent's re-executed delegation (§8.1), or a `Cancel` (idempotent on an ended run, and the explicit nudge for cancel propagation the journal still owes, §9.2). The same contact resumes the **queue** (§3.1): the first message drives the dispatcher, which starts a waiting acked turn once no run is live. A run nobody asks after stays interrupted, its cost bounded by its budget (§9.1). A2's liveness is conditional on this contact, and so is A4's for a session whose owed propagation a crash interrupted.

---

## 8. Sub-agent trees

### 8.1 Delegation is a tool

The built-in `delegate` is the only tool that executes in the loop rather than the sandbox (§5.2), present in a kind's registry iff the kind permits sub-agents. Its input names a child kind, a prompt, and optionally a budget request. The child kind MUST belong to the parent kind's **delegation allowlist** (§7.1); any other kind is a synthesized `ToolError` (§5.4), so a locked-down kind cannot escalate by delegating to a permissive one. Its execution is a child `Submit`:

1. the parent journals a `ChildRun` record: the carved budget (§9.1) and the child `SessionId` and `TurnId`, both derived deterministically from the parent session, the parent's `TurnId`, and the delegation's `CallId` (§5.2), since one run may delegate many times. The derivation length-prefixes each component, so distinct `(session, turn, call)` triples derive distinct ids even from crafted application-chosen ids. A re-executed delegation re-derives the same pair (§5.5), and cancel propagation reads it from the record (§9.2);
2. the launched task `Submit`s to the child (an `EntityRef`) with a **reply-to** and **awaits the child's `RunCompleted`** (§7.3), never block-`ask`ing a run that may be arbitrarily long. The await re-attaches on the submit-deadline cadence (each lapse re-registers on whatever activation hosts the child, §7.5; the derived `TurnId` keeps re-attempts safe, A6), bounded in total by a **budget-scaled wait**: a configurable floor plus a configurable per-step allowance × the child's carved `steps` (the floor SHOULD default to about **10 minutes**, the allowance to about the per-tool timeout, §5.3 item 3). Because the step bound is compositional (§9.1 item 3), the carve bounds the whole subtree's model calls. Past the bound the delegation resolves as a synthesized `ToolError` (§5.4) but cancels nothing: the child runs on, its budget the backstop (§9.2 item 3), and since its `ChildRun` now has a journaled outcome, a later cancel of the parent does not propagate to it (§9.2 item 2). A *consecutively unreachable* child is bounded separately by a transport retry limit with backoff (reset by any accepted submit), surfacing the same way. The child is a **full session**: its own entity, journaled, budgeted, placed on its own shard (entity §5), supervised on its leader;
3. the child's outcome arrives as one more message (§3.2) and becomes the tool's outcome (§5.4): journaled, then shown to the parent's model.

Delegation thereby inherits §7 with no new machinery: a child on a crashed leader resumes on the new one (§7.5); a resumed parent finds the delegation orphaned and, `delegate` being `Reexecute`, re-executes it; the child's journaled `TurnId` dedups the re-submission and registers the fresh reply-to (§7.4), so the outcome arrives whether the child is running or done.

### 8.2 Tree shape and failure

- The tree lives in the journals (`ChildRun` / `SessionCreated.parent`), not in a replicated structure; there is no global tree view. Each entity is its own consistency boundary (entity §2.2), and the runtime builds no cross-entity transaction.
- Each delegation carves from the parent's remaining budget (§9.1), so a tree's total spend is bounded by the root's budget regardless of depth or width (A3).
- A child's failure is a tool outcome (§5.4): the parent's model sees `RunError` and decides. Failures never propagate as supervision faults: supervision is local (core §11.3), and the tree spans shards and nodes.

---

## 9. Budgets and cancellation

### 9.1 Budgets

```rust
pub struct Budget { pub tokens: u64, pub steps: u32 }
```

1. Every run has a budget: the turn's explicit budget, else the kind's default. Spend is the model-**reported** usage (§4.1) summed over the run's calls, plus its children's spend. Spend is **a projection of the journal, not separately tracked state**: each `ModelResponse` carries its usage and each `ChildRun` its carve-out, so the fold (§6.2) recomputes spend, and a resumed activation re-derives it exactly by replay (A1).
2. **Pre-call enforcement.** A model call is issued only while `spent < tokens` and the step count is below `steps`; the request's `max_tokens` MUST be clamped to the remaining token budget, and no call may be issued while the remainder is below a configurable floor. Output overshoot is therefore zero, and total overshoot is bounded by one call's input, a bound that grows with the transcript because input size is known only on response. Exhaustion ends the run with `RunError::BudgetExhausted` (§3.1), recoverable by a new turn with a new budget.
3. **Carve-outs.** A delegation reserves an explicit slice of the parent's remaining budget, which the child enforces locally with no cross-shard accounting. The bound is compositional: own spend + Σ carve-outs ≤ budget, at every node of the tree (A3).
4. **What the bound is.** Spend is *journaled* usage. A model call whose response never commits (issued by a leader deposed mid-step, whose record loses to the contiguity guard, entity §6) goes uncounted though the provider bills it: the invoice exceeds recorded spend by exactly the speculative work a leadership change cost.

### 9.2 Cancellation

1. `Cancel` is a message (§7.3); since handlers never block on I/O (§3.2) and the input gate admits it between appends (entity §6), it takes effect at the next message boundary, not after the current model call returns. It names the run it cancels (`turn`); naming an ended or unknown run is an idempotent no-op, so a delayed `Cancel` never kills the named run's successor. Naming a **still-queued** turn (§3.1) journals that turn's `RunEnded { Cancelled }` directly: the fold drops the queue entry, subscribers are notified from the recorded outcome, and no `RunStarted`/`RunEnded` event pair fires, since no run began (§10.4).
2. On cancel, the session journals `RunEnded { Cancelled }`, notifies every registered reply-to with `RunCompleted { outcome: Err(RunError::Cancelled) }` (§7.3), discards the run's later outcome messages (§3.2), and **owes propagation**: folding the terminal record moves each `ChildRun` without a journaled outcome (§8.1) into the fold's *owed-cancels* fact, making each owed child a member of the desired set (workflow §3.1), a `Cancel { turn }` effect launched once per activation and relaunched under backoff until the child acks; each child cancels its own subtree. The owed set is a journal projection, so a crash between the terminal commit and the send loses nothing: the next activation re-derives the debt and re-sends when a message next drives the session (§7.5; a re-sent `Cancel` ticks the cycle explicitly).
3. Delivery is journaled. A child acks only after its own commit (its `RunEnded { Cancelled }`, or nothing for an ended run), and the parent then journals **`CancelDelivered { turn, call }`**, clearing that owed fact; until then a re-send is a safe no-op (item 1). Each send is at-most-once (core §7.2, entity §2.2), so an unacked send is a failed effect: the workflow backs its key off and relaunches it (workflow §3.4), and at the backoff cap the key rests so the parent may hibernate (workflow §6); the debt stays in the fold, and the next message that drives the session, on this activation or a later one, relaunches it. The guarantee is two-layered: once faults cease and a message reaches the session, propagation completes within bounded logical time (A4); under unhealed faults, the child's **budget is the backstop** (§9.1).
4. In-flight side effects of a cancelled run (a tool mid-execution, a model call mid-flight) are not undone and MAY complete externally; their outcomes are discarded. The runtime reports what it stopped; it does not pretend to have un-run it.

---

## 10. Observability

Operating agents means seeing what the model said, which tools ran, what a run cost, and how a delegation tree unfolded. The entity's journal already carries all four.

### 10.1 The journal is the record

The journal (entity §7) is the single source of truth for what a session did, and every observation API is normatively a **read of it**. The runtime MUST NOT keep a second durable account of session activity: a parallel trace or audit store would drift from it.

- **Records** are durable and user-facing: transcript, tool calls and outcomes, costs, tree links (§6.1). If an observer needs it, it is a record.
- **Events** (§10.4) are ephemeral and checker-facing: they describe the *machinery around* sessions (activation, leadership, run pairing) so the A-invariants are checkable over the stream (core §16, entity §13), and carry nothing of a session's content.

Each record carries the leader's `Clock` reading at write time, observational metadata the fold (§6.2) MUST NOT let influence behavior. Under simulation it is virtual (core §18.1), so timestamped journals still reproduce byte-identically.

### 10.2 Reading a run (`tail`)

`SessionRef::tail(from: Seq, limit) → Result<EventHistory<Record>, EntityError>` (§7.4; `EventHistory` is the entity's event-read reply, entity §4.3) returns at most `limit` committed records after `from`, plus the journal's compaction `base`: an idempotent, replication-free **read of the leader's journal** (entity §7.3 `load`) through the entity's gateway-level event read (§7.3). The gateway serves it **without get-or-activating the session**, so polling a hibernated session costs a journal read, never a rehydration or workspace rematerialization. The activation keeps no mirror of the raw records: the fold holds projections only (§6.2), so memory and snapshots hold no second copy of the transcript.

`tail` returns the entity's **event projection** (entity §7.5) unchanged, with three consequences for sessions:

- **The gaps are the workspace facet's.** The journal interleaves session records with the workspace facet's (entity §7.11, §7.12), leaving gaps in the projection's `Seq`s; they are the same slots the record subscription delivers (entity §7.9), so a follower reconciling `tail` pages against pushed batches compares like with like.
- **`limit` is bounded twice.** `SessionRef::tail` clamps `limit` to a page cap (SHOULD default to about **1024** records) on top of the gateway's server-side bound (entity §7.5).
- **`base` becomes an explicit step in the push form.** A long transcript's oldest records eventually exist in no journal (only *history* truncates; the snapshot subsumes them in folded state), and `SessionRef::follow` surfaces this as a `Truncated { base }` step, told *before* the cursor resumes past the gap. The HTTP surface mirrors both forms: a `base` field beside the records page, and a `truncated` stream event whose event id is the base, so a `Last-Event-ID` reconnect resumes after it.

It is read-your-leader (§7.3), and an unreachable shard takes observation of its sessions with it until a new leader is elected. Polling follows a live run at the client's cadence; `Submit`-and-await remains the one-shot form.

A `tail` MUST NOT trigger a write or change any activation lifecycle: it neither wakes a hibernated session nor refreshes an active one's idle clock, so continuous observation does not trade against hibernation's economics (§7.2). `SessionRef::follow` is the push complement: a `Follower` rides the entity's record subscription (`EntityRef::subscribe`, entity §7.9), which registers a sink in the activation, and reconciles every pushed batch by `Seq` against `tail` pages, backfilling on attach, on any gap, after the stream closes, and on a periodic re-sync that catches a silent leader move. `Follower::next` thus yields the exact committed sequence, or `Followed::Truncated { base }` where compaction outran the cursor. Push is the fast path; the journal is the authority.

### 10.3 Tree correlation

Every `SessionCreated` records its **root**: its own `SessionId` without a parent, else the parent's `root` (§8). The field denormalizes the transitive closure of the parent links so any record, event, or log line names its logical request in O(1); with the parent links, it stitches one delegation tree across shards, nodes, journals, and logs, the trace propagation core §16 recommends, with no minted identifier. External trace mapping is future work (§13).

### 10.4 Events (checker-facing)

Agent events are the runtime's own typed enum, riding the core stream through its application-event extension point (core §16's `Event::App`) beside the entity's events (`Activated`, `Deactivated`, `LeaderChanged`, `Committed`, …; entity §13): the core owns the mechanism, not the vocabulary. Checkers recover them by downcast from one totally ordered sequence, covered by seed reproducibility (core §18.1).

These events restate what the journal holds (`RunStarted` echoes `TurnStarted`, `ModelCompleted` a committed `ModelResponse`), the one sanctioned exception to §10.1: a continuous checker observes the totally ordered core stream, not the per-session journals, so run pairing or a call landing after a run's end must ride the stream to be checkable (the entity makes the same trade for `Committed`, entity §13). The vocabulary is the minimum a stream checker cannot reconstruct from the entity's `Activated`/`Deactivated`/`LeaderChanged`:

| Event | Fields | Meaning |
|---|---|---|
| `RunStarted` | `session`, `turn`, `parent?` | A run began: the turn's `TurnStarted` committed (§3.1). |
| `ModelCompleted` | `session`, `turn`, `node` | One model call finished and its response committed, scoped to journaled spend (below): the stream half of A3 is that none follows the turn's `RunEnded`. |
| `RunEnded` | `session`, `turn`, `outcome` | The run's exactly-one terminal outcome was journaled. |
| `ToolCompleted` | `session`, `turn`, `node` | One tool call's `ToolOutcome` committed on a live activation on `node` (§5.4). |
| `SandboxBound` | `session`, `node` | The activation opened its sandbox (first sandboxed call, §5.3). |
| `SandboxReleased` | `session`, `node` | That sandbox was torn down (hibernation, migration, loss, or step-down). |

The sandbox pair is *not* derivable from records, because the sandbox is not session state (§5.5). Run boundaries and `ModelCompleted` are the minimum to check A2/A6 and the stream halves of A3/A4 (no `ModelCompleted` after `RunEnded`); the spend bound itself is audited from records (`ModelResponse` usage, `ChildRun` carve-outs; `conformance_budget.rs`), since budgets are not on the stream. `ToolCompleted` lets the run-discipline checker reject a straggler committing after its run ended (§3.2).

Activation, deactivation, and the fence are observed through the **entity's** events: `Activated`/`Deactivated` (entity §13), `LeaderChanged` for a migration, and E1 over the entity stream (entity §14). `SandboxBound`/`SandboxReleased` MUST alternate within the entity's `Activated`/`Deactivated` window for that session and node (the A7 check), and a bound sandbox's `SandboxReleased` MUST precede that activation's `Deactivated`: the entity host runs the agent's `on_deactivate`, which releases the sandbox and emits the event, before emitting `Deactivated`, and the checker rejects a `Deactivated` with the sandbox still bound.

`RunStarted` fires exactly once per `(session, turn)`, on the activation that commits `TurnStarted` (§3.1). A resume (§7.5) emits **no** second `RunStarted`; a checker recognizes it as `Activated` followed by `ModelCompleted` on an already-started turn, so there is no `RunResumed` event. A turn cancelled while queued (§9.2) emits **neither** event, since an unpaired `RunEnded` would break A2 pairing. Under a leadership race, the A2 and A6 checkers stay sound by the fence (E1): the turn commits once, so one start is counted. `ModelCompleted` carries its `node` so the A5 checker attributes each call to its activation, and is emitted only for a committed response, so a deposed leader's discarded work (§9.1 item 4) never appears.

### 10.5 Reconstruction and derived metrics

Session state is a pure fold (§6.2, A1) and a model request a deterministic function of it (§4.1), so the exact `ModelRequest` of any step is **reconstructible from the journal prefix**: debugging a production prompt needs only the journal and the code. Reconstruction is exact only against the same kind, which is why `SessionCreated` records its digest (§7.1): a reader can tell an exact reconstruction from one a mid-session kind change made merely indicative. The digest is a fast 64-bit hash that flags drift but does not authenticate; integrity rests on the journal's custody.

Aggregate metrics (spend per kind and per tree by `root`, run latency, tool failure rates, activation and leadership churn) are RECOMMENDED, not REQUIRED, and SHOULD be **derived** from records and events rather than instrumented separately.

---

## 11. Conformance

The catalogue is machine-readable alongside the conformance suite (`a_catalogue()`), guarded by the same drift-test pattern as the entity, core, and utilities catalogues. It lists only what is specific to the *agent*, citing entity guarantees in the entity-basis column. The deterministic fold (E2/E3) and single activation (E6) reappear as A1 and A5, adding resume-equivalence and autonomy; the fence (E1) and the write-ahead obligation (§6.3, E5) have **no** agent invariant, being wholly the entity's (E1 is the basis of A5's one leader and A6's single commit).

| # | Invariant | Defined in | Verified by | Entity basis |
|---|---|---|---|---|
| A1 | **Deterministic fold and resume.** Session state is a pure fold of the journal; a session resumed from any committed prefix behaves byte-identically to one that never stopped, given the same subsequent model and tool outcomes. | §6.2, §7.5 | differential resume-vs-uninterrupted test (`conformance_resume.rs`); seed-reproducibility sweep (`agent_swarm.rs`: `the_event_stream_reproduces_byte_for_byte_per_seed`) | E2, E3 |
| A2 | **Run termination.** Every `RunStarted` is followed by exactly one `RunEnded`; once faults cease, partitions heal, and a message reaches the session, no run remains pending past its budget's bound. The `RunEnded` record is the single source of truth; the `RunCompleted` notification is best-effort (`tell`), and a lost one is recovered by re-contact (§7.4), never retried. | §3.1, §7.5, §9 | continuous checker (`agent-run-discipline`, pairing); scenario and swarm sweeps (`conformance_run.rs`, `cluster.rs`) for bounded completion under caller re-submission | E14 (lossless failover) |
| A3 | **Budget bound.** A run issues no model call after exhaustion; output spend never exceeds the remaining budget at call time; own spend plus children's carve-outs never exceeds the budget, at every level of a delegation tree. | §9.1 | continuous checker (`agent-run-discipline`): no `ModelCompleted` after `RunEnded` (§10.4); journal audit of spend against budget and tree scenario (`conformance_budget.rs`) | (none) |
| A4 | **Cancellation.** After a cancel is journaled, the run and, once faults cease, every descendant run end `Cancelled` within bounded logical time, issuing no further model calls. | §9.2 | scenario and swarm tests (`conformance_cancel.rs`); crash-window scenario (`cluster.rs`: leader crash between the terminal commit and the propagating send); checker (`agent-run-discipline`) for post-cancel `ModelCompleted` | (none) |
| A5 | **Single autonomous activation.** A node never runs two concurrent activations of one session; on a converged cluster, at most one activation per session is live; an addressed session activates within bounded logical time of a message reaching its shard leader. | §7.2, §7.5 | continuous checker (`agent-event-grammar`, the per-node half: no model completion outside an activation), inherited entity checker (E6), and the converged and liveness halves of the cluster scenario (`cluster.rs`) | E6, entity §8 |
| A6 | **Idempotent submission.** A re-submitted `TurnId` never starts a second run: it registers the caller's reply-to for notification, or `tell`s the recorded outcome if the run already ended, under any injected duplication or caller retry. | §7.4 | continuous checker (`agent-run-discipline`, one `RunStarted` per `(session, turn)`); retry scenarios (`conformance_run.rs`) | E1 (one commit) |
| A7 | **Effect containment.** An activation binds at most one live sandbox and releases it on deactivation; every sandboxed tool call executes in the sandbox bound to its issuing activation, never another session's; sandbox loss surfaces in the journal (`ToolError` outcomes, a `WorkspaceReset` record, §5.5), never as silent corruption. | §5.3, §5.5 | continuous checker (`agent-event-grammar`: `SandboxBound`/`SandboxReleased` within the entity's activation window); crash/loss scenarios (`conformance_sandbox.rs`); cross-session isolation by construction | entity §5.2 (colocation) |

The sandbox seam's tier obligations are their own catalogue, **S1–S5** (sandbox §6). The runtime is also held to the core and entity testing contracts (core §18.1, §18.3; entity §14): seed-reproducibility of the full event stream including agent events, and fault-coverage accounting proving model faults fired while agent traffic flowed; the entity's own faults are accounted for by its suite.

---

## 12. Testability and deterministic simulation

### 12.1 Seams

The runtime adds **two** seams to those the entity already virtualizes (entity §14, core §18.2); the journal stays the entity's, simulated by driving the real consensus code:

| Seam | Production | Simulation |
|---|---|---|
| `Model` (§4) | `ProtocolModel` over an injected `HttpPost`, bound to `agent-anthropic`'s Messages protocol or `agent-openai`'s Chat Completions protocol; retry backoff from `Clock`/`Entropy` | **scripted model**: a deterministic function of the request and the seed; emits final messages, tool calls, malformed calls, and faults under seed control |
| `Sandbox` (§5.3) | a deployment-supplied `SandboxProvider` (process, container, or microVM; §13) | **scripted sandbox**: outcomes scripted per call by a closure over the tool's name and input; a toggled open failure, a fixed per-call latency, and crash and environment-loss shapes the script returns as the call's `ToolError` |
The `agent` crate MUST satisfy core §18.1: no wall clock, no OS threads, no unseeded randomness; all I/O launched through `Spawner` (§3.2). Its `http` module owns the HTTP seam the production adapters share: `HttpPost`, the one socket operation a deployment implements; the `Protocol` trait, an adapter's wire-format decisions; and `ProtocolModel`, the shared shell with a bounded, jittered retry (policy in `agent::http`). `HttpPost`'s production implementation (the standalone deployment's, on the `agent-http` substrate) is injected at construction. Each production `Model` crate (`agent-anthropic`, `agent-openai`) is a `Protocol` impl plus a `ProtocolModel` alias and nothing else; production sandbox providers are likewise separate crates. Because journal, fence, placement, and resume are the entity's, simulation *runs the real entity host, gateway, shard, and rehydration code* (entity §14) under the same seed; the loop is the only new code on the simulated path.

### 12.2 Fault injection

Under seed control, a simulated agent run MUST be able to inject at least, on top of the core and entity faults (core §18.3, entity §14):

- **Model:** latency, `RateLimited`/`Overloaded` bursts, `ContextOverflow`, unknown tool names, malformed or schema-violating arguments (rejected by the loop and the executing tool respectively, §5.4), pathological tool-call loops (exercising budgets).
- **Sandbox:** a toggled open failure, a fixed execution latency (a call outliving its timeout, §5.3 item 3), a crash mid-call (§5.5), and environment loss between steps (forcing `WorkspaceReset`). These are scripted per scenario as a function of the call, not drawn from the seed.
- **Entity (inherited):** shard-leader crash and election mid-write (E1; resume on the new leader, §7.5), shard quorum loss (`Unavailable`, §6.4), eviction mid-command, and a shard split under concurrent writes (entity §7.7).
- **Topology:** leadership moves under partition and heal (the orphaned-effect race, §5.5), cancellation racing completion across the tree.

A run with no faults MUST still pass.

---

## 13. Future work

- **Post-commit emission in the workflow.** The events owed once a record commits (§10.4) are staged on the activation by the deciding handler and emitted by the tick after the commit; the workflow engine stages its own the same way (workflow §8, §11), and one staging could serve both. Nothing blocks on it.
- **Durable alarms for the agent.** A stored timer that re-activates a session to advance a run with no caller present: the basis for scheduled runs, server-side timeouts, and proactive agents. The entity half exists (the alarm facet and its per-shard index, entity §7.16); the runtime integration remains, lifting v1's strictly caller-driven resumption (§7.5).
- **Scheduler singleton.** Queued and recurring runs feeding ordinary `Submit`s; out of v1, and a natural client of durable alarms.
- **Linearizable and follower reads.** A `tail` that reflects committed state on the minority side of a partition, via the entity's deferred leader-lease upgrade (entity §7.5, §16); follower reads for read scale ride the same extension.
- **Sandbox providers, snapshots, and pooling.** The first per-tier provider is `agent-sandbox` (sandbox §3.1–§3.5). Open: a network dataplane behind the profile's allowlist (sandbox §3.3) and warm pools to cut open latency.
- **Multi-tenant scheduling.** Quotas, fair-share scheduling, and accounting across tenants: the economics of mutualization (§1.1).
- **Context compaction.** Summarizing the transcript into a journaled checkpoint record from which the fold and the model request start; must preserve A1. The entity's snapshot (entity §9) bounds *replay* cost; compaction bounds *context* cost.
- **Token streaming.** Streaming partial `Model` output; `complete` is one request, one response (§4.1). Record-level push observation exists (`SessionRef::follow`, §10.2).
- **Permission gating.** A per-tool authorization hook between intent and execution, the application-level analogue of the entity's per-`(peer, EntityName, manifest)` `Authorizer` (entity §13, core §15): the dynamic, per-call half to the tier cap's static half (§5.6).
- **Loop-executing tools.** An extension point for tools that run in the loop with effects confined to the seams (ask-the-user, journal queries); v1 has only `delegate` (§5.2).
- **Code mode.** Exposing the toolset as an API the model *programs against*, the generated program executing at the `Compute` tier (§5.6): fewer declaration tokens and loop round-trips, same boundary.
- **External trace interop.** Mapping `root`/parent links (§10.3) onto W3C trace-context or OpenTelemetry ids at an external collector's boundary.
- **Cross-session sagas.** Multi-session workflows above the single-session consistency boundary, built with idempotency as the entity builds cross-entity sagas (entity §16).

---

## Appendix A: End-to-end example

```rust
// --- A kind, identical on every node (§7.1); the registry is the allowlist (§5.2).
// --- `shell` and `read_file` run in the sandbox at their declared tiers (§5.6); `delegate`
// --- runs in the loop (§8). The tier cap defaults to {Workspace, Native} (§5.3 item 4).
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

// --- Per node: one Harness; model and sandbox seams injected, the journal the entity's (§7.4).
let kinds = Kinds::new().register("researcher", researcher);
let h = Harness::cluster(system, &kinds, model, sandboxes);

// --- Any node: address the session by name; the shard leader hosts it (§7.2) ---
let s = h.session("researcher", SessionId::new("report-42"))?;  // EntityRef<Agent>; Err ⇔ the kind
                                                                //   is not in this node's directory (§7.4)
// prompt() = Submit (ack) + await RunCompleted (§7.4); re-prompt the same TurnId to re-attach.
match s.prompt(Turn::new(TurnId::new("t-1"), "Summarize the corpus on X.")).await {
    Ok(Ok(completion))                  => println!("{}", completion.text()),
    Ok(Err(RunError::BudgetExhausted))  => { /* grant a larger budget, resubmit a new turn */ }
    Ok(Err(run_err))                    => eprintln!("run failed: {run_err:?}"),   // Model | Cancelled
    Err(EntityError::Unavailable(_))     => { /* shard quorum lost; the run PAUSED, not ended;
                                                re-submit the SAME TurnId on recovery (§6.4, §7.5) */ }
    Err(EntityError::NotLeader(_))       => { /* retries exhausted; refresh shard map and retry */ }
    Err(EntityError::Call(Unreachable | Timeout))
        => { /* safe to re-submit the SAME TurnId: A6 dedups or attaches (§7.4) */ }
    Err(e)                              => eprintln!("call failed: {e:?}"),
}
```

## Appendix B: Crate and module layout

```
agent/                # the agent runtime (this spec): an agent is an entity (§2)
  agent.rs              #   the Agent entity: Entity/EntityHandler and Workflow impls,
                        #     perform, settle, and the environment pre-pass (§3, §5.4-§5.6, §6)
  desired.rs            #   Workflow::pending, the desired set as a pure function of committed
                        #     state; the Effect it carries to perform; the StepKey naming one (§3.1)
  config.rs             #   HarnessConfig, the tuning every node agrees on (§7.2, §9.1)
  session.rs            #   SessionId (= EntityName), TurnId, Turn, Record (= the entity Event),
                        #     and the fold over records (§2, §6)
  model.rs              #   Model trait, ModelRequest/Response/Error (§4)
  http.rs               #   the HTTP seam: HttpPost, HttpResponse/HttpError, the Protocol trait,
                        #     and ProtocolModel with its seeded, bounded retry (§4.2, §12.1)
  tool.rs               #   ToolDecl, ToolRegistry, the built-in `delegate` (§5.2, §8)
  sandbox.rs            #   Sandbox + SandboxProvider seams, SandboxProfile, Tier (§5.3, §5.6)
  kind.rs               #   Kind, Kinds, the kind→entity-type/EntityConfig binding (§7.1)
  client.rs             #   Harness + SessionRef (= EntityRef<Agent>): addressing, routing, and the
                        #     attach protocol behind `prompt` and a delegation's wait (§7.4, §8.1)
  budget.rs             #   Budget, spend accounting, carve-outs (§9.1)
  event.rs              #   the agent runtime's checker-facing Event vocabulary (§10.4)
  testing.rs            #   the continuous A-checkers: run discipline (A2/A3/A4/A6) and effect
                        #     containment (A5/A7), behind the `testing` feature, not API (§11)

agent-anthropic/      # Protocol impl for the Anthropic Messages API; AnthropicModel is
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

`agent` depends on `entity`, and through it on `actor-core`, `actor-cluster`, and `actor-serialization`. It defines **no journal, placement function, fence, or resume protocol**; the one durable thing it names is the `Record` type (the entity's event) and its `apply` fold. Test-only pieces (scripted model and sandbox, `a_catalogue()`, the conformance suites) live with the tests, dev-depending on `actor-simulation` and the entity's simulation seam. The continuous checkers are the exception: they ship in `testing.rs` behind the `testing` feature because any consumer's swarm can mount them, as the entity's `testing.rs` does. The crate follows the workspace conventions: edition 2024, `unsafe_code = "forbid"`, `clippy::all = "warn"`, serde derives only.
