# Agentic Harness: Specification

**Status:** Draft v4
**Scope:** A distributed agentic runtime: compound AI systems (agent loops combining model calls, tools, and control logic) run as actors on a mutualized cluster. Built as an application on the core framework ([`distributed-actor-spec.md`](distributed-actor-spec.md)) and the cluster utilities ([`cluster-utilities-spec.md`](cluster-utilities-spec.md)).

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Sections of the core specification are cited as **core §N**, sections of the utilities specification as **util §N**, sections of the sandbox specification ([`sandbox-spec.md`](sandbox-spec.md)) as **sandbox §N**, and sections of this document as plain **§N**. Invariants defined here are numbered **H1, H2, …**, kept apart from the core catalogue (core §18.5 #1–#22), the utilities catalogue (util §6 U1–U2), and the sandbox catalogue (sandbox §6 S1–S5).

> **Design stance.** Today an agentic session is one OS process per user: the loop, the conversation, and unrestricted access to a machine, all in one box (the shape of a Claude Code session). The box is mostly idle: a session spends its life waiting on a model, a tool, or its user, yet holds the machine the whole time. The harness splits the box at **one isolation boundary**, placed between *control* and *effect*. Above it, the **loop is an actor**: serial, journaled, touching the world only through three seams, the **model** (§4), the **journal** (§6), and the **sandbox** (§5.3). A waiting session holds no thread, a deactivated one holds no memory, and the journal *is* the session, so thousands of sessions pack onto shared nodes. Below it, the **effects live in a sandbox**: one isolated environment per session where tools run, free to be arbitrarily effectful without endangering the node or any other session. §5.1 places the boundary and defends the placement. Everything else (time, randomness, task spawning, transport) comes from the core seams (core §4.6, §7), so deterministic simulation extends to the harness unchanged: one seed reproduces an entire multi-node agentic run, including its model outputs, tool faults, crashes, and resumes.
>
> The harness is also the worked example of the honesty clauses the lower specs end on. Placement is not a lease (util §2.3) and the singleton guarantee is per *converged* view (util §4.3); both tell applications needing exclusivity to fence by their own means. The harness does: the journal's **fenced append** (§6.2) is that fence, and it keeps a session's *transcript* safe even while two nodes transiently believe they own it. The fence guards the record, not the world: it cannot recall an effect already launched, and what divergence can and cannot cost is itemized where it arises (§5.5, §6.2).

---

## 1. Scope and layering

The harness exists to run, on **mutualized infrastructure**, the workload a single-user agent process runs today: long-lived agentic sessions interleaving model calls, tool use, and delegation. Three ideas organize it:

- **A session is a compound AI system, not a model call** (Zaharia et al., [*The Shift from Models to Compound AI Systems*](https://bair.berkeley.edu/blog/2024/02/18/compound-ai-systems/), 2024): model calls, tools, and control logic, composed. The harness is the runtime for the composition (the loop is explicit, journaled, and supervised) while the components, model and tools, sit behind seams (§4, §5).
- **What a session keeps must live outside the loop** (Anthropic, [*Effective harnesses for long-running agents*](https://www.anthropic.com/engineering/effective-harnesses-for-long-running-agents), 2025): a long-running agent works in discrete sessions, each beginning with no memory of the last, so progress must persist in durable artifacts the next activation reads, not in a context window or a process's memory. Here the artifact is the journal (§6): the session *is* its journal, the actor a disposable fold of it (§6.3), and anything not journaled is by definition lost.
- **One isolation boundary, deliberately placed.** The **loop runs in the agent actor**, cheap, serial, and effect-free outside its seams, so a session waiting on a model, a tool, or its user holds no thread, and once deactivated holds no memory either; the **tool effects run in the session's sandbox**, where they may be arbitrary. This is the *agents as infrastructure* move (Cloudflare, [*Project Think*](https://blog.cloudflare.com/project-think/), 2026): the session as a durable, addressable unit that costs nothing while idle, with effects contained by the architecture rather than by trust in the model's behavior. That split is the efficiency claim over one process per user; §5.1 defends where the boundary cuts.

Everything in this document is built **on top of** the core abstractions and the utilities, and modifies none of them:

- **actors, messages, supervision, death watch** (core §3–§12) host the session and carry every harness interaction as an ordinary typed message with a hand-written manifest (core §4.4);
- **rendezvous placement** (util §2) decides which node owns a session;
- the **receptionist** (core §13) is how a session's owner node is reached;
- the **event stream** (core §16) carries the harness's observability events through the core's application-event extension point — one totally ordered stream, with the harness's event vocabulary defined in the harness (§10.4).

The core non-goals (core §1.2) hold unchanged: the harness never transparently retries a message with side effects, never places a quorum on the message path, and never masks a failure. Where the harness *does* make a retry safe, it does so the way core §7.2 prescribes: an explicit idempotency key (the `TurnId`, §7.4), not transparent retransmission.

### 1.1 Non-goals

- **A model gateway.** The harness calls one configured model implementation per request; routing across providers, A/B serving, and quota brokering are not its concern.
- **Prompt management.** System prompts and tool descriptions are application data the harness stores and transmits, not a templating system it provides.
- **Exactly-once tool execution.** A tool call is at-most-once per attempt; across a crash-resume boundary the recovery semantics are explicit and per-tool (§5.5), never silent.
- **An isolation technology.** The spec mandates *that* effectful tools execute behind the `Sandbox` seam (§5.3), not *how*: process, container, or microVM is the provider's choice. v4 narrows the *what* without touching the *how*: a tool now declares which capability tier its calls require (§5.6), and the realization of each tier remains the provider's choice, bound by the obligations of sandbox §3.
- **Multi-tenant economics.** Quotas, fair scheduling, and billing live above the harness (§13); budgets (§9) bound a run's spend, not a tenant's share.
- **Context-window management.** A transcript that exceeds the model's context fails the run explicitly (§4.3); compaction and summarization are future work (§13).
- **Retrieval, vector stores, UI.** Applications build these as tools (§5) or clients (§7).

---

## 2. The session

### 2.1 Anatomy

A session is one durable thing and two disposable ones:

- the **journal** *is* the session: a fenced, totally-ordered record log (§6). It is the only component whose loss loses the session.
- the **actor** is the session's working form while active: the fold of the journal plus the in-flight run (§3). Stopping it loses nothing; the next activation, on any node, rebuilds it by replay.
- the **sandbox** is where the session's effects land: one isolated environment holding working state, useful but never authoritative (§5.3). Losing it never loses the session (§5.5).

The loop reaches the world through exactly three seams, **model** (§4), **journal** (§6), and **sandbox** (§5.3), and through nothing else; time, randomness, task spawning, and transport come from the core seams (core §4.6, §7). One sentence carries the whole design, and the rest of this document is its elaboration: **the journal is the session; the actor and the sandbox are disposable; the seams are the only world.**

```text
                      client (any node)                               §7.4
                         │  Submit { session, turn },
                         │  routed to owner(view, session)
                         ▼
                       Host on the owner node                         §7.2
                         │  activates lazily: load → fold → spawn
                         ▼
          ┌───────── the loop: one session actor (§3) ─────────┐
          │   state = fold(journal) · serial · no inline I/O   │
          └────┬──────────────────┬──────────────────┬─────────┘
               │                  │                  │
               ▼                  ▼                  ▼
        Model seam (§4)    Journal seam (§6)   Sandbox seam (§5.3)
        one inference      the durable         ══ isolation boundary (§5.1) ══
        call per step      session: fenced,    one environment per session:
                           ordered records     shell, files, processes, network
```

### 2.2 Vocabulary

| Term | Definition |
|---|---|
| **Session** | One agent conversation: a durable identity (`SessionId`), a journal, and, while active, one hosting actor. Survives actor restarts and node moves; an `ActorId` does not. |
| **Journal** | The durable, per-session, totally-ordered log of records, behind a trait (§6). |
| **Record** | One journal entry. The transcript is the sequence of records; agent state is a fold over them (§6.3). |
| **Kind** | A named agent definition (`KindId` → system prompt, toolset, sandbox profile, model parameters, default budget, delegation allowlist), registered identically on every node (§7.1). |
| **Turn** | One submitted input: a user prompt, or a parent agent's delegation (§8). Carries a client-chosen `TurnId`, the idempotency key (§7.4). |
| **Run** | The execution a turn triggers: the model→tools→model loop until a terminal outcome. Identified by its turn's `TurnId`. |
| **Step** | One loop iteration within a run: one model call plus the resolution of every tool call and delegation it requested. |
| **Host** | The per-node actor that activates, routes to, and deactivates the sessions its node owns (§7.2). |
| **Activation** | A host spawning a session's actor after replaying its journal; the unit the single-activation guarantee (H6) counts. |
| **Model** | The inference seam (§4): one trait, implemented by `harness-anthropic` in production and by a scripted model in simulation. |
| **Tool** | A capability the model may invoke, declared to the harness (§5.2). Every declared tool executes inside the session's sandbox; the single built-in exception, `delegate`, executes in the loop (§8). |
| **Sandbox** | The isolated execution environment bound to one session activation, behind the third seam (§5.3): where sandboxed tools run, and the only place their effects land. |
| **Tier** | A named capability set a tool call requires and an activation escalates to, journaled before first use (§5.6); semantics in sandbox §2. |
| **Budget** | A run's spending limit (model tokens and steps) from which child budgets are carved (§9). |
| **Root** | The parentless ancestor a delegation tree descends from. Every `SessionCreated` records its `root` (§10.3); the root's `SessionId` names the tree. Correlation metadata only; nothing routes or folds on it. |

Identity is layered deliberately: `SessionId` (durable, application-chosen) → `ActorId` (one activation, system-assigned, core §3.6) → `TurnId` (one run). The harness owns the first mapping; the framework owns the second.

---

## 3. The agent actor

A session, while active, is one ordinary actor (core §3): its private state is the folded transcript plus the in-flight run, and its handlers are the only access to it (core §3.5).

### 3.1 The run loop

A run advances in **steps**. Each step:

1. checks the budget (§9.1); if exhausted, the run ends with `RunError::BudgetExhausted`;
2. issues one model call carrying the transcript (§4.2);
3. if the response is a final assistant message, journals it and ends the run with `Ok(Completion)`;
4. otherwise journals the model response (intent before effect, §6.4), then executes every requested tool call (§5) and delegation (§8);
5. journals each outcome as it arrives; when all are resolved, begins the next step.

A run MUST end in exactly one terminal outcome, journaled as the run's `RunEnded` record (invariant H3): `Ok(Completion)`, `RunError::BudgetExhausted`, `RunError::Cancelled`, `RunError::Model(…)` (a model failure no retry policy absorbed, §4.3), or `RunError::Journal(…)` (the session cannot record, §6.5). `RunError` is an application error and therefore lives **inside the reply** (core §3.2 rule 4): the caller of `Submit` (§7.3) sees `Result<Completion, RunError>`, with transport failure separate in `CallError`.

### 3.2 Handlers never block on I/O

The loop is **message-driven**: the agent's handlers MUST NOT await the model, a tool, or the journal inline. The handler launches each external operation through `Spawner` (core §4.6), and the outcome returns to the actor as an ordinary message; the step is a state the fold tracks, not a stack frame the executor holds.

This is what keeps the actor honest under core §6: the mailbox stays live during a thirty-second model call, so a `Cancel` (§9.2) takes effect at message granularity instead of waiting behind I/O; serial execution still orders every state change. The actor MUST discard, not journal, an outcome message that arrives after its run has ended (a cancelled run's straggling tool result, §9.2).

### 3.3 Supervision and restart

Ordinary supervision (core §11) governs the session actor's faults. A restart is cheap by construction: state is a fold of the journal (§6.3), so the restarted actor replays and continues, the same mechanism as a cross-node resume (§7.5), exercised locally. The harness SHOULD therefore configure `Restart` with backoff for session actors, with the journal as the factory's source of truth.

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

The model is a seam exactly like `Transport` (core §7): the harness core depends only on the trait. `harness-anthropic` implements it over the Anthropic Messages API for production; the simulator supplies a **scripted model**, a deterministic function of the request and the run's seed (§12.2).

### 4.2 Determinism rules

1. A `Model` implementation MUST take all timing from `Clock` and all randomness from `Entropy` (core §4.6), including HTTP retry backoff and jitter in the production client. The harness core crate MUST NOT read the wall clock, spawn OS threads, or use an unseeded RNG (core §18.1).
2. The model's *output* is inherently nondeterministic in production. The determinism contract is therefore scoped the same way the network's is: given the same seed, **the simulated model** returns byte-identical responses, and everything downstream of any fixed response sequence is reproducible (H1).

### 4.3 Failure

`ModelError` distinguishes, at minimum: `RateLimited`, `Overloaded`, `ContextOverflow`, `InvalidRequest`, and `Api(String)`. A `Model` implementation MAY retry requests internally, with backoff from `Clock`/`Entropy` and a bounded policy; a completion request is side-effect-free, so this does not violate core §1.2. A model failure that survives the policy ends the run with `RunError::Model(…)`: journaled, reported, never silently swallowed (core §8).

---

## 5. Tools and the isolation boundary

### 5.1 The boundary

A run splits across one deliberately placed boundary:

| In the agent actor: the **loop** | In the sandbox: the **effects** |
|---|---|
| control flow: steps, budgets, delegation | tool execution: shell, files, processes, network |
| model calls, through the `Model` seam (§4) | working state: a workspace that is *not* session state (§5.5) |
| the fold and the journal, through the `Journal` seam (§6) | everything a tool's side effects touch |

Above the boundary, the loop touches the world only through the three seams; it is serial, journaled, and cheap: the mutualization premise (§1). Below it, a tool may do anything *inside* its session's sandbox and nothing outside it. Actor isolation (core §3.5) protects sessions' state from one another; sandbox isolation protects the node, and every other session's effects, from tools.

**Why the boundary cuts here.** Three other placements lose:

- **No sandbox.** Tools run on the node, as in the single-user process. The first `rm -rf` or runaway build one session's model requests takes the node, and every co-located session, with it. Mutualization voids the trust assumption that made the single-user shape acceptable.
- **Loop inside the sandbox.** Ship the whole session into the box, one box per session. That is today's architecture relocated, not replaced: every session pays for an isolated environment to host control flow that is mostly waiting, and the loop's discipline (journaling, write-ahead order, the deterministic fold) becomes as opaque to the runtime as the tools are.
- **A sandbox per tool call.** Maximal isolation, but consecutive calls share nothing, and the checkout, build cache, or running server one step produced is exactly what the next step needs. The workspace is the unit of tool-to-tool continuity.

Hence one sandbox **per session**, opened lazily per activation (§5.3): coarse enough to keep working state across calls, fine enough that no effect escapes the session that made it. What this placement deliberately gives up is workspace durability: the workspace is working state, not session state, and §5.5 is honest about losing it.

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
    Browser,   // a remote browser peripheral, driven through host functions
    Native,    // OS processes and native binaries inside the confined environment
}
```

The tier names are capability sets, not rungs of a number line: each names what a call may touch, and `Browser` is deliberately comparable to none of the other escalations (sandbox §2.2). What each tier grants and withholds, and what a provider MUST guarantee to offer it, live in the sandbox specification (sandbox §2–§3); this document carries only the contract.

A kind's `ToolRegistry` is a hand-built list of declarations, in the spirit of `HandlerRegistry` (core §4.4): explicit, inspectable, and the **allowlist**. A model's tool call dispatches by name against the registry and nothing else; no path leads from model output to code outside the declared set.

Every requested tool call carries a **`CallId`**, unique within its run (the model API's tool-use id, or one the harness assigns on receipt), journaled with the call's intent (§6.4). Outcomes reference it, dangling-call resolution (§5.5) matches by it, and child-session derivation (§8.1) is keyed by it; without a per-call identity, none of the three is well-defined.

Every declared tool is **sandboxed**: its call dispatches through the `Sandbox` seam (§5.3) and executes nowhere else. The single exception is the built-in **`delegate`** (§8), which executes in the loop: a delegation is control flow (a child `Submit`, confined to the seams), not an effect. v1 deliberately exposes no extension point for loop-executing tools; one is future work (§13), to be designed when a second inhabitant exists.

### 5.3 The sandbox seam

```rust
/// Provisioning of isolated execution environments. The third harness seam.
pub trait SandboxProvider: Send + Sync + 'static {
    type Sandbox: Sandbox;
    async fn open(&self, session: &SessionId, profile: &SandboxProfile)
        -> Result<Self::Sandbox, SandboxError>;
}

/// One environment, bound to one session activation.
pub trait Sandbox: Send + Sync + 'static {
    /// Execute one declared, sandboxed tool call to completion inside the
    /// environment, at the call's declared tier (§5.2, §5.6).
    async fn call(&self, tier: Tier, name: &str, input: Value) -> Result<Value, ToolError>;
    /// Tear down processes and working state, across every provisioned tier. Idempotent.
    async fn release(&self);
}
```

1. **Lazy, per tier.** An activation opens its sandbox on its first sandboxed call; a session that never needs one never pays for one. Tiers are provisioned the same way: the harness passes each call's declared tier — the provider holds no registry and cannot derive it — and the provider builds a tier's environment on the first call carrying it (§5.6), so a session that never escalates holds only its workspace.
2. **Bound per activation.** At most one live sandbox per activation, and deactivation, whether by ownership move, fence rejection (§6.2), or idle stop, MUST release it (invariant H8). The sandbox lives on the session's current owner node and never migrates.
3. **Launched, never awaited inline** (§3.2): outcomes return as messages, bounded by a per-tool timeout from `Clock`.
4. **Profiled.** The kind's `SandboxProfile` (image or toolchain, resource limits, network policy) is deployment configuration, agreed cluster-wide like the kind itself (§7.1). The profile also carries the kind's **tier cap** — the set of tiers its sessions may hold (§5.6) — and, when the cap includes `Network`, the egress allowlist that tier grants (sandbox §3.3): a `TierEscalated` record grants the profile's allowlist by reference, never an inline list. Declaring a tool whose tier the cap excludes is a deployment configuration error, surfaced at registration as loudly as a duplicate name (§5.2), never discovered at dispatch; the cap defaults to exactly the tiers the kind's declared tools require.
5. **Technology-opaque, within a tier.** Whether a tier's environment is a process, a container, or a microVM is the provider's secret; the simulator's scripted sandbox is one more implementation of the same trait (§12). Draft v3 left tiering itself to that secrecy — a provider MAY tier environments internally; the seam sees one `Sandbox` — and v4 supersedes it: *which capabilities a call holds* is now declared per tool (§5.2), journaled per escalation (§5.6), and agreed cluster-wide (§7.1), because an escalation ladder the provider improvises is an audit trail and a policy hook nobody has (cf. the execution ladder of Cloudflare's *Project Think*, cited by v3 as provider freedom and promoted by v4 to contract). The seam still sees one `Sandbox`: tiers are provisioned within it, never as separate seam objects.

### 5.4 Tool failure is a transcript value

A failing tool does not fail the run. The harness journals its `ToolError` (a timeout, a sandbox-side crash, a failed delegation (§8.2), an unknown tool name, or schema-rejected arguments) as that call's outcome and **returns it to the model** as the tool result, for the model to react to. This defines the error out of existence at the run level: the only abnormal run endings are the four of §3.1, and "a tool misbehaved" is not one of them. Unknown and malformed calls in particular are synthesized outcomes, not protocol failures; the registry-as-allowlist already guarantees nothing was executed.

### 5.5 Crash, loss, and resume (honesty)

A tool call's **intent** is journaled before execution and its **outcome** after (§6.4); a crash between the two leaves a *dangling call*. On resume (§7.5):

- a dangling call declared `OnDangling::Reexecute` MUST be re-executed, in a fresh sandbox if the old one is gone; the re-execution may fail for exactly that reason, surfacing per §5.4;
- a dangling call declared `Interrupt` MUST be resolved as `ToolError::Interrupted`, fed to the model like any tool failure (§5.4), so the *model* decides whether to retry the side effect.

A dangling call is not always dead. During divergence (§6.2) the stale activation may still be *executing* it: that activation discovers its staleness only at its next append, which for a slow call is after the effect lands. Resolution by the new owner therefore races the original — a `Reexecute` re-execution may run concurrently with it, and a model that answers `ToolError::Interrupted` with a retry may duplicate a side effect the old node is still producing. The window is the duration of the in-flight call, not a message round-trip. The fence guarantees one *transcript*; it cannot recall a launched effect, and this specification does not pretend otherwise. (`delegate` is the one call the architecture itself rescues: its re-execution is a child `Submit` carrying the same `TurnId`, which dedups into an attach rather than a second run, §7.4, §8.1.)

The sandbox itself is **not session state**: the fold (§6.3) never reads it, no record depends on its contents, and a lost workspace is never reconstructed by the harness. Anything that must outlive the sandbox leaves it through a tool: push a branch, upload an artifact, return a result the loop journals. This is core §1.2's "retries are the caller's decision" applied to effects: the harness never re-fires a non-idempotent side effect on its own authority, and it never pretends a lost workspace still exists.

Nor may the *model* be left pretending. The transcript asserts a workspace the world may no longer hold — files written, servers left running — and a model resumed into a fresh sandbox would otherwise discover the loss only through a confusing downstream failure. When an activation opens a fresh sandbox for a session whose journal already records sandboxed activity (after an idle stop §7.2, an environment loss, or a crash), it MUST journal a **`WorkspaceReset`** record before the next model call and surface it to the model with that request. The record answers no `CallId`, so it cannot ride a tool result: it enters the request as input content the harness authors, the encoding's analogue of a user message. The loss thus enters the record, and the model re-derives what it needs instead of acting on state that is gone.

Tier loss follows the same honesty, at two grains. A fresh sandbox resets **every** tier the lost activation held: `WorkspaceReset` covers the whole environment, the new activation's held set restarts at `Workspace` (§5.6), and re-escalation is re-journaled, never silently inherited. Loss of a single tier's environment while the sandbox survives surfaces as `ToolError` outcomes on the calls that needed it (§5.4); the provider MAY re-provision that tier lazily under the escalation this activation already journaled, and MUST NOT grant a tier the activation never journaled (sandbox §4).

### 5.6 Execution tiers

A **tier** is a named capability set a tool call requires: `Workspace`, `Compute`, `Network`, `Browser`, or `Native` (§5.2). This document owns the contract — the declaration, the record, the cap, the agreement — and the sandbox specification owns the semantics: what each tier grants and withholds (sandbox §2), and what a provider MUST guarantee to offer it (sandbox §3).

1. **Opening grants `Workspace` and nothing else.** The lazy open of §5.3 builds the workspace tier; a session that never escalates holds a directory's worth of capability and no more.
2. **Escalation is journaled, intent before effect.** Before the first call at a tier the activation does not yet hold, the actor journals a **`TierEscalated { turn, tier }`** record: the write-ahead discipline (§6.4) applied to capability acquisition. The record is the audit trail — when did this session first run guest code, first touch the network? — and the policy hook (§13), both obtained from the journal the design already has rather than from a second mechanism.
3. **Held tiers are additive and activation-scoped.** Within an activation the held set only grows; nothing revokes a tier. A fresh activation starts again from `Workspace` (§5.5): held tiers are working state, like the sandbox they live in, and only the journal's escalation records survive.
4. **The cap is unreachable by construction.** Escalation beyond the kind's tier cap cannot be requested: declared tools are checked against the cap at registration (§5.3 item 4), and dispatch reaches declared tools only (§5.2). The cap is stated as the invariant that construction discharges (sandbox §6 S4), not as a runtime check the loop performs.
5. **Agreement.** Each tool's tier and the profile's cap are covered by the kind digest (§7.1): what a session may escalate to is cluster-wide agreement, never a node-local choice.
6. **A record, not an event.** Escalation is durable, user-facing audit, so §10.1 makes it a record. No §10.4 event accompanies it: tool execution carries no events at all, so no stream checker could consume one; the escalation ordering of sandbox §6 S4 is verified by journal audit, the way H2's quiescence audit already works.

---

## 6. The journal seam

### 6.1 The trait

```rust
/// Durable, per-session, totally-ordered record log. The second harness seam.
pub trait Journal: Send + Sync + 'static {
    /// Fenced append (§6.2): commits `records` immediately after `after`,
    /// or rejects with the current head if `after` is stale.
    async fn append(&self, session: &SessionId, after: SeqNo, records: Vec<Record>)
        -> Result<SeqNo, AppendError>;

    /// Up to `limit` committed records from `from` (exclusive) toward the head.
    async fn load(&self, session: &SessionId, from: SeqNo, limit: usize)
        -> Result<Vec<(SeqNo, Record)>, JournalError>;
}

pub enum AppendError { Stale { head: SeqNo }, Unavailable(String) }
```

The journal is **one logical store shared by every node**: a session's records MUST be readable and appendable from any node of the cluster, and `append` MUST be linearizable per session across all of them — cross-node resume (§7.5) and the fence (§6.2) are meaningless against anything weaker. The harness ships an in-memory implementation, which is per-process and therefore confines a deployment to a **single node** (suitable for ephemeral single-node use and as the substrate the simulator wraps with faults, §12.2); durable store implementations are pluggable and out of scope for v1 (§13).

### 6.2 Fenced append (the single-writer guarantee)

`append` is **conditional**: it commits iff `after` equals the journal's current head for that session. This is the fence util §2.3 and util §4.3 tell exclusivity-needing applications to build:

1. Placement may transiently name two owners for one session (divergent views, util §2.3). Both may activate (H6 allows it per converged view), but their appends race on the same `after`: the journal accepts **one**; the other receives `Stale`.
2. An activation receiving `Stale` MUST deactivate: emit `AppendRejected` and `SessionDeactivated` (§10.4), stop its actor, journal nothing further, and issue no further model or tool calls for that session (invariant H2).
3. The journal's total order per session is therefore single-writer *per record*, with no consensus, no lease, and no quorum on the framework's message path (core §1.2). The coordination has not vanished; it has been **placed**: the fence is a conditional write, and a durable, distributed `Journal` implementation linearizes it with whatever consensus its store runs internally (§6.1). The harness buys its simplicity by outsourcing that one decision to the journal.
4. Divergence costs duplicated *speculative* work — model calls whose results lose the race and are discarded — and possibly **duplicated or concurrent side effects** of sandboxed calls launched before the loser learned it lost (§5.5). It never costs a forked transcript. The fence guards the record, not the world.

### 6.3 State is a fold

A session actor's state MUST be a pure, deterministic function of its journal prefix, `state = fold(records)`, with no information outside the fold influencing subsequent behavior except new inputs arriving as messages. Replay is therefore resume: activation loads the journal, folds it, and continues (H1). Anything worth surviving a move MUST be a `Record`; anything not journaled is, by definition, lost on deactivation: a design constraint, not an accident to discover.

### 6.4 Write-ahead discipline

Within a step, the actor journals:

- a **model response** before any of its tool calls execute (intent before effect, §5.5), each requested call identified by its `CallId` (§5.2); a model call itself is side-effect-free and needs no intent record;
- a **tier escalation** (`TierEscalated`) before the first tool call at a tier the activation does not yet hold (§5.6); like a model response, it is intent journaled before effect;
- a **tool or delegation outcome** before the next step shows it to the model;
- the **terminal outcome** (`RunEnded`) before releasing the reply to `Submit` (§7.3), so a caller never holds a completion the journal could lose.

### 6.5 Journal failure

`AppendError::Unavailable` pauses the session's *progress*, never corrupts it: the actor MUST NOT proceed past an uncommitted record. Bounded retries (via `Clock`) MAY absorb a transient outage; a persistent one ends the run with `RunError::Journal(…)` delivered on a best-effort basis (the journal being down is precisely what prevents recording it; the caller's `ask` deadline, core §14.2, is the backstop).

---

## 7. Sessions across the cluster

A session's cluster life is a cycle only the journal survives: **created** (first turn) → **owned** (placement names a node, §7.2) → **activated** (load → fold → spawn) → **serving** (runs, §3) → **deactivated** (ownership move, fence rejection, or idle) → **resumed** by its next owner (§7.5). Each subsection below specifies one arc of that cycle.

### 7.1 Kinds

Each node's harness is configured with the same `KindId → Kind` map: system prompt, `ToolRegistry`, `SandboxProfile`, model parameters, default budget, delegation allowlist (§8.1). Kinds are code-and-config, agreed cluster-wide like the codec (core §5): a session created with a kind MUST be resumable on any node, so every node MUST register every kind. A `SessionCreated` record pins the session's kind and a **digest** of its definition (§10.5); the digest covers, among the definition's fields, each tool's declared tier and the profile's tier cap (§5.6), so what a session may escalate to is pinned with the rest of its definition. Activation on a node missing that kind is a deployment error that fails the triggering call with `CallError::System` before any run starts — nothing journaled, no `RunStarted` — never a silent fallback.

### 7.2 The host and placement

Every node runs one **Host** actor, registered under a single receptionist key (`harness.hosts`, core §13). A session's **owner** is `owner(serving set, session id)` per util §2: a pure function of the local membership view, computed without I/O.

The host activates a session lazily, on the first message that reaches it for a session it owns: load journal → fold → spawn the session actor (restartable, §3.3) → route. When the host's view stops naming its node owner, it MUST deactivate the session: stop the actor after the in-flight step's records commit, releasing the session for the new owner. Activations of one session on one node are strictly sequential (the per-node half of H6, mirroring util §4 rule 2).

**Idle stop.** A host SHOULD deactivate a session that has had no live run for a configurable idle timeout (timed by `Clock`); it MUST NOT idle-stop a session whose run is live. Deactivation needs no flush — anything worth keeping is already a record (§6.3) — but it releases the sandbox (H8), and the workspace with it (§5.5). The idle timeout is therefore not a tuning detail: it is the knob trading sandbox cost, the expensive resource mutualization actually shares, against workspace continuity.

**Not the owner.** A session message arriving at a host whose view does not name its node owner (the sender computed placement on a divergent view) MUST fail fast with `DeadLetter`; the `TurnId` makes the caller's retry safe once views converge (§7.4). Serving it would be transcript-safe — the fence (§6.2) protects the journal regardless of who appends — so the rejection is routing discipline (one owner path), not a safety requirement.

### 7.3 The wire contract

The host accepts, with hand-written manifests (core §4.4):

| Message | Reply | Manifest |
|---|---|---|
| `Submit { session, kind, turn }` | `Result<Completion, RunError>` | `harness.Submit` |
| `Cancel { session, turn }` | `()` | `harness.Cancel` |
| `Tail { session, from, limit }` | `Vec<(SeqNo, Record)>` | `harness.Tail` |

`Submit` is an `ask`; its deadline bounds the caller's **wait**, never the run. Budgets (§9) bound a run's tokens and steps, not its wall time — tool timeouts, model retries, and child runs compound — so a run MAY outlive any deadline: it continues unaffected when its caller times out, and the caller re-attaches by re-submitting the same `TurnId` (§7.4) or follows it through `Tail` (§10.2). There is deliberately no status message: a status view (head `SeqNo`, live run if any, spend so far) is a client-side fold over `Tail`'s records, not a second reply type restating them (§10.1). `Tail` is specified in §10.2.

`Submit` carries `kind` on every call though it binds only on the first: creation is implicit in a session's first turn (§7), and the alternative — a separate create message — would buy a two-phase client protocol to remove one field. After creation the field is a checked redundancy, rejected on mismatch rather than ignored (§7.4).

### 7.4 The client view (`SessionRef`) and idempotent submission

```rust
let h = Harness::new(system, kinds, journal, model, sandboxes);
//   per node: spawns the Host and injects all three seams (§4, §6, §5.3) in one place
let s = h.session("researcher", SESSION_ID);       // pure: placement is a local function; no I/O,
                                                   //   no failure case (cf. resolve, core §4.3)
let out = s.prompt(Turn { id: TURN_ID, content }).await;   // Result<Result<Completion, RunError>, CallError>
```

`SessionRef` is the deep module of the client side: one call hides owner computation, receptionist lookup, host resolution, and activation. It MUST NOT transparently retry a failed `Submit` (core §1.2). Instead, the **`TurnId`** makes retries safe to the caller:

- a `Submit` whose `TurnId` is already journaled for that session MUST NOT start a second run: if the run ended, the host returns the recorded outcome; if it is in progress, the host attaches the caller to its completion;
- a caller that received `Unreachable` or `Timeout` therefore re-submits the same `TurnId` at will: the explicit idempotency key core §7.2 prescribes, owned by the application layer where it belongs;
- a re-submission whose turn *content* differs from the journaled turn, or a `Submit` whose `kind` differs from the session's journaled `SessionCreated`, is a caller bug, rejected with `CallError::System` and journaling nothing: the dedup returns *the* recorded turn's outcome; it never silently substitutes another.

An owner whose host is not yet in the local listing (listing lag, core §13) fails fast with `DeadLetter`; the `TurnId` makes the caller's retry safe.

### 7.5 Resume after node failure

When a session's owner node is declared `down` (core §8.1) or merely leaves the serving set, placement names a new owner on every converged view; the next message routed to it activates the session there: journal load, fold (§6.3), dangling-call resolution (§5.5), and continuation of any unfinished run. No coordinator hands the session over: ownership is computed, the journal is the state, and the fence (§6.2) makes the race with a not-yet-deactivated stale owner safe for the transcript (§5.5 itemizes what it cannot make safe).

Resumption is **caller-driven**: nothing scans for orphaned work — the `Journal` deliberately has no cross-session enumeration — so a run interrupted by a crash resumes only when a message next reaches the session: the re-submitted `TurnId` of a caller that wants its answer, a parent's re-executed delegation (§8.1), a `Cancel`. A run nobody asks after stays interrupted, and its budget bounds what it can ever cost (§9.1). H3's liveness is conditional on exactly this contact.

---

## 8. Sub-agent trees

### 8.1 Delegation is a tool

The harness provides one built-in tool, `delegate`, the only tool that executes in the loop rather than the sandbox (§5.2), present in a kind's registry iff the kind permits sub-agents. Its input names a child kind — which MUST belong to the parent kind's **delegation allowlist** (§7.1): naming any other kind is a synthesized `ToolError` (§5.4), so a locked-down kind cannot escalate its privileges by delegating to a permissive one — plus a prompt and optionally a budget request; its execution is a child `Submit`:

1. the parent journals the delegation (a `ChildRun` record: the child `SessionId` and the child turn's `TurnId`, both derived deterministically from the parent session, the parent's `TurnId`, and the delegation's `CallId` (§5.2) — one run may delegate many times, so the call, not the turn, is the unit of derivation — plus the carved budget, §9.1); a re-executed delegation re-derives the same pair (§5.5), and cancel propagation reads it from the record (§9.2);
2. the launched task submits to the child session through an ordinary `SessionRef`; the child is a **full session**: journaled, budgeted, placed by util §2 wherever the cluster puts it, supervised on its node;
3. the child's terminal outcome returns as the tool's outcome (§5.4): journaled, then shown to the parent's model.

Delegation thereby inherits every property of §7 with no new machinery: a child on a crashed node resumes on the new owner (§7.5); a parent that crashes and resumes finds the delegation dangling and, since `delegate` is declared `Reexecute`, re-executes it; the child's journaled `TurnId` dedups the re-submission into an attach (§7.4). The retry is safe *because* of the idempotency key, not despite the at-most-once transport.

### 8.2 Tree shape and failure

- The tree is recorded in the journals (`ChildRun` / `SessionCreated.parent`), not in any replicated structure; there is no global tree view to keep consistent.
- Fan-out is bounded by the budget: each delegation carves from the parent's remaining budget (§9.1), so a tree's total spend is bounded by the root's budget regardless of depth or width (H4).
- A child's failure is a tool outcome (§5.4): the parent's model sees `RunError` and decides. Failures never propagate as supervision faults across the tree: supervision is local (core §11.3), and the tree spans nodes.

---

## 9. Budgets and cancellation

### 9.1 Budgets

```rust
pub struct Budget { pub tokens: u64, pub steps: u32 }
```

1. Every run has a budget: the turn's explicit budget, else the kind's default. Spend is the model-**reported** usage (§4.1) summed over the run's calls, plus the spend of its children.
2. **Pre-call enforcement.** A model call is issued only while `spent < tokens` and the step count is below `steps`; the request's `max_tokens` MUST be clamped to the remaining token budget, and no call may be issued while the remainder is below a configurable floor (a near-zero-`max_tokens` call still pays its full input). Output overshoot is therefore zero, and total overshoot is bounded by one call's input — a bound that grows with the transcript, stated honestly: a call's input size is known only on response. Exhaustion ends the run with `RunError::BudgetExhausted` (§3.1): journaled, reported, recoverable by a new turn with a new budget.
3. **Carve-outs.** A delegation reserves an explicit slice of the parent's remaining budget; the child enforces its slice locally, with no cross-node accounting protocol. Hence the bound is compositional: own spend + Σ carve-outs ≤ budget, at every node of the tree (H4).
4. **What the bound is.** Spend is *journaled* usage. A divergence-losing activation's model calls are discarded unjournaled (§6.2) and never counted, though the provider bills them: the budget bounds the session's recorded spend, and the provider's invoice exceeds it by exactly the speculative work divergence cost.

### 9.2 Cancellation

1. `Cancel` is a message (§7.3); because handlers never block on I/O (§3.2), it takes effect at the next message boundary, not after the current model call returns. It names the run it cancels (`turn`, §7.3): a `Cancel` naming an ended or unknown run is an idempotent no-op, so one delayed in flight never kills the named run's successor.
2. On cancel, the session journals `RunEnded { Cancelled }`, releases any attached `Submit` callers with `RunError::Cancelled`, discards subsequent outcome messages of that run (§3.2), and **propagates**: it sends `Cancel { session, turn }` to every live child recorded in the journal — the `ChildRun` record names the child's session and turn (§8.1) — each of which cancels its own subtree.
3. Propagation is at-most-once per attempt (core §7.2); a lost `Cancel` is not retried transparently. The guarantee is two-layered: once faults cease, propagation completes within bounded logical time (H5); under unhealed faults, the child's **budget is the backstop**, since every run is bounded with or without the cancel arriving (§9.1).
4. In-flight side effects of a cancelled run (a tool mid-execution, a model call mid-flight) are not undone and MAY complete externally; their outcomes are discarded. The harness reports what it stopped; it does not pretend to have un-run it.

---

## 10. Observability

In an agentic harness, observability is a product feature, not a diagnostic afterthought: operating agents means seeing what the model said, which tools ran, what a run cost, and how a delegation tree unfolded. The harness gets this nearly for free, because the design already centralizes the one record that matters; this section says so normatively and fills the few gaps around it.

### 10.1 The journal is the record

The journal (§6) is the single source of truth for what a session did, and every observation API is normatively a **read of it**. The harness MUST NOT keep a second durable account of session activity: a parallel trace or audit store would restate the journal's decision in a second module, and the two would drift (the information-leakage red flag). The division of labor is:

- **Records** are durable and user-facing: the transcript, the tool calls and outcomes, the costs, the tree links. If an observer needs it, it is a record.
- **Events** (§10.4) are ephemeral and checker-facing: they describe the *machinery around* sessions (activation, fencing, run pairing) so that the H-invariants are checkable over a run's stream (core §16). They carry nothing about a session's content.

Each record additionally carries the writing node's `Clock` reading at write time. The timestamp is observational metadata: the fold (§6.3) MUST NOT let it influence behavior. Under simulation it is virtual and therefore deterministic (core §18.1), so timestamped journals still reproduce byte-identically.

### 10.2 Reading a run (`Tail`)

`Tail { session, from: SeqNo, limit } → Vec<(SeqNo, Record)>` (§7.3) returns at most `limit` committed records after `from`: an idempotent, fence-free journal read, routed through the session's owner like every other session message — for one routing path, not for consistency; any node could serve it correctly (§6.1). The coupling is the honest cost: an unreachable owner takes observation of its sessions with it until views converge on a new one; push-based observation (§13) is where that coupling gets revisited. Polling `Tail` follows a live run at whatever cadence the client chooses; `Submit`-and-await remains the one-shot form.

A `Tail` MUST NOT activate an inactive session: the host serves it from the journal directly. Push-based observation is future work (§13); `Tail` is deliberately the smallest interface that makes a run watchable.

### 10.3 Tree correlation

Every `SessionCreated` records its session's **root**: its own `SessionId` for a session created with no parent, the parent's `root` for a session created by delegation (§8). The field is the transitive closure of the parent links, denormalized so that any record, event, or application log line can name its logical request in O(1). Without it, the question "which tree does this grandchild belong to?" is a walk up ancestor journals, each hop a cross-node read. Together with the recorded parent links, `root` stitches one delegation tree across nodes, journals, and logs: the harness-level instance of the trace propagation core §16 recommends, with no identifier minted for it. The root's `SessionId` *is* the trace, one fact under one name (the §10.1 rule applied to naming). Mapping trees onto external trace systems is future work (§13).

### 10.4 Events (checker-facing)

Harness events are defined **in the harness** as their own typed enum and ride the core stream through its application-event extension point (core §16's `Event::App`): the core owns the mechanism, never the vocabulary, so layering an application adds no variant to the core enum — unlike the utilities' events (util §5), which are part of the framework proper. Checkers recover them from the shared stream by downcast, observing one totally ordered sequence interleaved with the core events; the seed-reproducibility contract (core §18.1) covers them for free. They exist to make the H-invariants (§11) checkable over the stream; per-message and per-fold events would add noise without enabling a check.

| Event | Fields | Meaning |
|---|---|---|
| `SessionActivated` | `session`, `node` | The host activated the session (journal folded, actor live). |
| `SessionDeactivated` | `session`, `node` | That activation stopped (ownership moved, fence rejection, idle stop, or fault). |
| `AppendRejected` | `session`, `node` | A fenced append lost the race (§6.2); the activation must now deactivate. |
| `RunStarted` | `session`, `turn`, `parent?` | A run began for a newly journaled turn. |
| `RunResumed` | `session`, `turn`, `node` | An activation picked up a journaled, unfinished run (§7.5). |
| `ModelCompleted` | `session`, `turn`, `node`, `usage` | One model call finished; `usage` feeds the H4 checker, scoped to journaled spend (below). |
| `RunEnded` | `session`, `turn`, `outcome` | The run's exactly-one terminal outcome was journaled. |
| `SandboxBound` | `session`, `node` | The activation opened its sandbox (first sandboxed call, §5.3). |
| `SandboxReleased` | `session`, `node` | That sandbox was torn down (deactivation, loss, or release). |

Per session and node, `SessionActivated`/`SessionDeactivated` strictly alternate, `SandboxBound`/`SandboxReleased` alternate within the activation they belong to, and an `AppendRejected` is followed by that node's `SessionDeactivated` with no intervening harness activity for the session. Continuous checkers enforce all three on every simulated run.

`RunStarted` fires once per `(session, turn)`, on the activation whose append commits the turn; a resume emits `RunResumed`, never a second `RunStarted`. That distinction is what keeps the H3 pairing and H7 counting checkers sound across crashes — and under divergence their soundness is the fence's (§6.2): the turn commits once, so one start is counted even when two activations raced to begin it.

`ModelCompleted` carries its `node` for the same reason. Spend is *journaled* usage (§9.1.4): a divergence-losing activation completes model calls whose responses never commit, so its events describe spend no record carries. The H4 checker therefore attributes each `ModelCompleted` to its enclosing activation — the `SessionActivated`/`SessionDeactivated` pair on that node — and excludes those of an activation fenced off before the corresponding response committed (`AppendRejected`, §6.2). The event stream restates §9.1.4; it does not define spend a second time (§10.1).

### 10.5 Reconstruction and derived metrics

Because session state is a pure fold (§6.3, H1) and a model request is a deterministic function of that state (§4.1), the exact `ModelRequest` issued at any step is **reconstructible from the journal prefix**: debugging a production prompt needs no request logging, only the journal and the code. Reconstruction is faithful only against the same agent definition, which is why `SessionCreated` records a digest of the kind's definition (§7.1): a reader can tell whether a reconstruction is exact, or merely indicative because a deployment changed the kind mid-session.

Aggregate metrics (spend per kind and per tree, grouped by `root`; run latency; tool failure rates; activation churn) are RECOMMENDED, not REQUIRED, and SHOULD be **derived** from records and events rather than instrumented separately (core §16): the two streams are the substrate everything else builds on.

---

## 11. Conformance

The harness catalogue mirrors the core and utilities catalogues (core §17, §18.5; util §6) and is machine-readable alongside the harness's conformance suite (`harness_catalogue()`), guarded by the same drift-test pattern.

| # | Invariant | Defined in | Verified by |
|---|---|---|---|
| H1 | **Deterministic fold and resume.** Session state is a pure fold of the journal; a session resumed from any committed prefix behaves byte-identically to one that never stopped, given the same subsequent model and tool outcomes. | §6.3, §7.5 | differential resume-vs-uninterrupted test; seed-reproducibility sweep |
| H2 | **Fenced single writer.** Per session, committed records form one total order; an activation whose append is rejected as stale deactivates and issues no further appends, model calls, or tool calls for that session. | §6.2 | continuous checker (`AppendRejected` ⇒ deactivation, no further activity); journal audit at quiescence |
| H3 | **Run termination.** Every `RunStarted` is followed by exactly one `RunEnded`; once faults cease, partitions heal, and a message reaches the session — resumption is caller-driven (§7.5) — no run remains pending past its budget's bound. | §3.1, §7.5, §9 | continuous checker (pairing); swarm sweep (bounded completion under caller re-submission) |
| H4 | **Budget bound.** A run issues no model call after exhaustion; output spend never exceeds the remaining budget at call time; own spend plus children's carve-outs never exceeds the budget, at every level of a delegation tree. | §9.1 | continuous checker over `ModelCompleted`, scoped to journaled spend (§10.4); tree scenario tests |
| H5 | **Cancellation.** After a cancel is journaled, the run and, once faults cease, every descendant run end `Cancelled` within bounded logical time, issuing no further model calls. | §9.2 | scenario + swarm tests; checker for post-cancel `ModelCompleted` |
| H6 | **Single activation per converged view.** A node never runs two concurrent activations of one session; on a healed, converged cluster, at most one activation per session is live cluster-wide; an owned session is activated within bounded logical time of a message reaching its owner's host. | §7.2, §7.5 | continuous checker (per-node alternation, the per-node half); scenario + swarm for the converged and liveness halves (mirrors util U2) |
| H7 | **Idempotent submission.** A re-submitted `TurnId` never starts a second run: it returns the recorded outcome or attaches to the live run, under any injected duplication or caller retry. | §7.4 | continuous checker (one `RunStarted` per `(session, turn)`); retry scenario tests |
| H8 | **Effect containment.** An activation binds at most one live sandbox and releases it on deactivation; every sandboxed tool call executes in the sandbox bound to its issuing activation, never another session's; sandbox loss surfaces in the journal — `ToolError` outcomes, a `WorkspaceReset` record (§5.5) — never as silent corruption. | §5.3, §5.5 | continuous checker (`SandboxBound`/`SandboxReleased` alternation, calls within the bind window); crash/loss scenario tests; cross-session isolation by construction of the provider |

The tier obligations of the sandbox seam carry their own catalogue, **S1–S5** (sandbox §6): they bind providers rather than the loop, and their verification machinery is tracked there.

The harness is also held to the core testing contract (core §18.1, §18.3): seed-reproducibility of the full event stream including harness events, and fault-coverage accounting proving that model faults, journal faults, and transport faults actually fired while agent traffic flowed.

---

## 12. Testability and deterministic simulation

### 12.1 Seams

The harness adds three rows to the core's virtualization table (core §18.2); everything else it uses is already virtualized:

| Seam | Production | Simulation |
|---|---|---|
| `Model` (§4) | `harness-anthropic`: Anthropic Messages API over HTTPS; retry backoff from `Clock`/`Entropy` | **scripted model**: a deterministic function of the request and the seed; emits final messages, tool calls, malformed calls, and faults under seed control |
| `Journal` (§6) | pluggable durable store (future work, §13); in-memory for ephemeral deployments | the in-memory journal wrapped with seeded latency, `Unavailable` windows, and crash-truncation of unacknowledged appends |
| `Sandbox` (§5.3) | a deployment-supplied `SandboxProvider` (process, container, or microVM; §13) | **scripted sandbox**: deterministic outcomes per call and seed; seeded open failures, latency, crashes, environment loss, and tier-provisioning failure |

The `harness` crate itself MUST satisfy core §18.1: no wall clock, no OS threads, no unseeded randomness; all I/O launched through `Spawner` (§3.2). `harness-anthropic` is production-only and is the single place HTTP exists; production sandbox providers are likewise separate crates: the harness core knows only the trait. The scripted sandbox remains the seam's single simulation implementation; the `Compute` tier's provider obligation (sandbox §3.2: no ambient clock, entropy, or network) is core §18.1's discipline applied below the seam, which is what would make a real compute engine *also* runnable under the simulator — claimed as a design property, not as shipped machinery.

### 12.2 Fault injection

Under seed control, a simulated harness run MUST be able to inject at least, on top of the core faults (core §18.3):

- **Model:** latency, `RateLimited`/`Overloaded` bursts, `ContextOverflow`, unknown tool names, schema-invalid arguments, pathological tool-call loops (exercising budgets).
- **Journal:** append/load latency, `Unavailable` windows, loss of unacknowledged appends at a crash (the torn tail a resume must tolerate).
- **Sandbox:** open failure, execution latency, a crash mid-call (dangling calls, §5.5), loss of the environment between steps, forcing resume into a fresh sandbox, and provisioning failure at an escalated tier (the calls that needed it fail per §5.4).
- **Topology:** node crash mid-step (dangling tool calls, §5.5), ownership moves under partition and heal (fence races, §6.2), cancellation racing completion across the tree.

A run with no faults is the simplest case and MUST still pass.

---

## 13. Future work

- **Scheduler singleton.** Queued and recurring agent runs owned by a cluster singleton (util §4), feeding ordinary `Submit`s; deliberately out of v1.
- **Durable journal implementations.** File- and store-backed `Journal`s; the trait (§6.1) is shaped for them (fenced append maps onto conditional writes).
- **Sandbox providers, snapshots, and pooling.** Production `SandboxProvider` crates implementing the per-tier obligations of sandbox §3–§4 (capability-handle workspaces, hermetic compute isolates, confined native environments); workspace snapshot/restore across ownership moves; warm pools to cut open latency.
- **Multi-tenant scheduling.** Quotas, fair-share scheduling, and accounting across tenants sharing the cluster: the economics of mutualization (§1.1).
- **Context compaction.** Summarizing the transcript into a journaled checkpoint record so the fold, and the model request, start from it; must preserve H1.
- **Streaming.** Token streaming from `Model`, and push-based run observation: a subscription complement to `Tail` (§10.2), likely over a pub/sub utility (util §7).
- **Permission gating.** A per-tool authorization hook between intent and execution, the application-level analogue of the core `Authorizer` (core §15). The tier cap (§5.6) is the static half of authorization, fixed at deployment; this hook is the dynamic, per-call half.
- **Loop-executing tools.** A general extension point for tools that run in the loop with effects confined to the seams (ask-the-user, journal queries); v1's only loop-executing tool is the built-in `delegate` (§5.2).
- **Code mode.** Exposing the toolset as an API the model *programs against* rather than calls tool-by-tool, the generated program executing at the `Compute` tier (§5.6) like any other effect (cf. Project Think's codemode): fewer declaration tokens, fewer loop round-trips, same boundary — and the tier the ladder was shaped for.
- **External trace interop.** Mapping `root`/parent links (§10.3) onto W3C trace-context or OpenTelemetry ids at the boundary of an external collector; the opaque ids those systems expect are minted there, not stored in the journal.
- **Sharding alignment.** If the planned sharding utility (util §7) lands, hosts become its entity hosts; placement semantics are already identical by construction.

---

## Appendix A: End-to-end example

```rust
// --- A kind: prompt, declared tools, sandbox profile; identical on every node (§7.1).
// --- The registry is hand-built and is the allowlist (§5.2); `shell` and `read_file`
// --- execute inside the session's sandbox (§5.3) at their declared tiers (§5.6),
// --- `delegate` inside the loop (§8). The tier cap defaults to exactly what the
// --- declared tools require — here {Workspace, Native} (§5.3 item 4).
let researcher = Kind::new("You are a research agent.")
    .model(ModelParams::default())
    .sandboxed(Tier::Native, "shell", "Run a command in the workspace", &SHELL_SCHEMA)
    .sandboxed(Tier::Workspace, "read_file", "Read a file from the workspace", &READ_SCHEMA)
    .delegates_to(&["researcher"])          // the built-in `delegate` tool; allowlisted child kinds (§5.2, §8)
    .sandbox(SandboxProfile::image("workspace:base"))
    .budget(Budget { tokens: 200_000, steps: 50 });

// --- Per node: one Harness over the actor system; all three seams injected here (§12.1) ---
let h = Harness::new(system, Kinds::new().register("researcher", researcher),
                     journal, model, sandboxes);

// --- Any node: drive a session; placement decides who hosts it (§7.2) ---
let s = h.session("researcher", SessionId::new("report-42"));
match s.prompt(Turn::new(TurnId::new("t-1"), "Summarize the corpus on X.")).await {
    Ok(Ok(completion))                  => println!("{}", completion.text()),
    Ok(Err(RunError::BudgetExhausted))  => { /* grant a larger budget, resubmit a new turn */ }
    Ok(Err(run_err))                    => eprintln!("run failed: {run_err:?}"),
    Err(CallError::Unreachable | CallError::Timeout)
        => { /* safe to re-submit the SAME TurnId: H7 dedups or attaches (§7.4) */ }
    Err(e)                              => eprintln!("call failed: {e:?}"),
}
```

## Appendix B: Crate and module layout

```
harness/                # the agentic harness (this spec)
  session.rs            #   SessionId, TurnId, Turn, Record, the fold (§2, §6.3)
  agent.rs              #   the session actor: message-driven run loop, steps (§3)
  model.rs              #   Model trait, ModelRequest/Response/Error (§4)
  tool.rs               #   ToolDecl, ToolRegistry, the built-in `delegate` (§5.2, §8)
  sandbox.rs            #   Sandbox + SandboxProvider seams, SandboxProfile, Tier (§5.3, §5.6)
  journal.rs            #   Journal trait, SeqNo, fenced append, in-memory impl (§6)
  host.rs               #   Host actor, kinds, activation/deactivation, wire messages (§7)
  client.rs             #   Harness + SessionRef: placement + receptionist routing (§7.4)
  budget.rs             #   Budget, spend accounting, carve-outs, cancellation (§9)

harness-anthropic/      # production Model only: Anthropic Messages API client;
                        #   backoff via Clock/Entropy; the single place HTTP exists (§12.1)
```

Test-only pieces (the scripted model, the scripted sandbox, the faulted journal wrapper, `harness_catalogue()`, and the conformance suites) live with the harness's tests, dev-depending on `actor-simulation` for the simulator, exactly as the utilities' suites do. The `harness` crate observes the workspace conventions: edition 2024, `unsafe_code = "forbid"`, `clippy::all = "warn"`, serde derives only.
