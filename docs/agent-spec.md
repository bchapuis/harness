# Agent: Specification

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, **RECOMMENDED**, and **MAY** carry the meanings defined in RFC 2119. Sections of this document are cited as plain **§N**; other documents carry a prefix: **entity §N**, **workflow §N**, **actor §N**, **sandbox §N**, **edge §N**, **machine §N**. Invariants defined here are **A1–A7** (§10).

## 1. Scope

The agent runtime runs compound AI sessions (loops of model calls, tool calls, and delegation) on a shared cluster. A session is an entity (entity §1) whose activation drives itself: a workflow (workflow §2) whose effects are model calls, sandboxed tool calls, child submissions, and owed cancels. Identity, the journal, the fence, placement, activation, hibernation, and failover are the entity's (entity §3–§7); deriving, launching, and settling effects is the workflow's (workflow §3, §4). The agent adds the transcript and its fold, two seams (the model, §2.1, and the sandbox, §2.3), kinds, budgets, delegation, and cancellation.

Model adapters (`agent-anthropic`, `agent-openai`) implement the model seam; sandbox providers implement the sandbox seam under the tier model (sandbox §1); the HTTP edge is a cluster client of the client view (edge §1). The persistent machine (machine §1) is a sibling consumer of the entity, with no run loop and no model seam.

### 1.1 Non-goals

- **A model gateway:** routing, A/B serving, and quota brokering across models.
- **Prompt management:** system prompts and tool descriptions are application data, stored and transmitted.
- **Exactly-once tool execution:** a call is at-most-once per attempt (entity §1.1); recovery across a crash is explicit and per tool (§4.4).
- **An isolation technology:** process, container, or microVM is the provider's choice (sandbox §1.1).
- **Consensus of its own:** no Raft group, shard, or fence beyond the entity's.
- **Multi-tenant economics:** a budget (§7.1) bounds a run, not a tenant.
- **Context-window management:** an over-long transcript fails the run (§2.1).

## 2. Seams

### 2.1 The model (`Model`)

A plug point: a model adapter implements it, and the run loop calls it once per step (§3.2).

```rust
pub trait Model: Send + Sync + 'static {
    fn complete(&self, req: ModelRequest) -> BoxFuture<'static, Result<ModelResponse, ModelError>>;
}
```

`ModelRequest` carries the kind's system prompt, its `ModelParams` with `max_tokens` clamped to the run's remaining tokens (§7.1), the toolset's interfaces (`ToolSpec`), and the transcript as a sequence of `Entry` values. `ModelResponse` carries the assistant content, zero or more `ToolCall`s, and the call's `Usage`. `ModelError` distinguishes `RateLimited`, `Overloaded`, `ContextOverflow`, `InvalidRequest`, and `Api`.

The runtime MUST build each request as a deterministic function of committed state and the kind (§8.4), MUST assign every returned `ToolCall` with an empty id one deterministic in the step and position, and ends the run with `RunError::Model` on any `ModelError` it receives (§3.2). An implementation:

- MUST present every `Entry` in transcript order, MUST pair each `Entry::ToolResult` with the call its `CallId` names, and MUST present `Entry::WorkspaceReset` as input content the runtime authored, never as a tool result, since it answers no call. It SHOULD render it as `WORKSPACE_RESET_NOTICE`.
- MUST send `params.max_tokens` as the output ceiling, and no larger one.
- MUST return in `usage` the usage the provider reports: budgets count nothing else (§7.1).
- MUST give returned calls ids unique within the run, or leave them empty.
- MUST map an over-long transcript to `ContextOverflow` and a request the provider calls malformed to `InvalidRequest`.
- MAY retry internally, a completion having no side effect (actor §1.1), but MUST NOT retry `ContextOverflow` or `InvalidRequest`, and MUST bound every other retry so that `complete` resolves in finite logical time.
- MUST take all timing from `Clock` and all randomness from `Entropy` (actor §2.12.1).

### 2.2 The HTTP adapter kit (`HttpPost`, `Protocol`)

Two plug points inside the model seam, so that a production adapter is a wire format and nothing else. `ProtocolModel` implements `Model` over them: encode through the protocol, post through the socket, classify the reply, and retry within §2.1's bounds, with backoff timed by `Clock` and jittered by `Entropy`. `AnthropicModel` (protocol `Messages`) and `OpenAiModel` (`ChatCompletions`) are aliases of it.

- `HttpPost`, supplied by the deployment, is the one socket operation: POST a body to a path under its base URL, and return the status and body, or an `HttpError` for an exchange that did not complete. It MUST resolve every post, under a timeout of its own, and SHOULD NOT retry: the shell's policy is the one retry bound.
- `Protocol`, supplied by an adapter crate, is the wire format: the path, the key-bearing headers, the request encoding (bound by §2.1's transcript obligations), and `classify` onto `ModelResponse` or `ModelError`. Retrying is the shell's decision: `RateLimited`, `Overloaded`, and `Api` retry; `ContextOverflow` and `InvalidRequest` never do.

`agent-http` is plumbing behind the standalone deployment's `HttpPost`, not a seam.

### 2.3 The sandbox (`SandboxProvider`, `Sandbox`)

A plug point: a deployment supplies a provider per hosted kind, and each activation holds at most one `Sandbox` from it. This section is the runtime's side; the provider's side and the tier semantics are the sandbox specification's (sandbox §2, §3).

```rust
pub trait SandboxProvider: Send + Sync + 'static {
    fn open(&self, session: &SessionId, profile: &SandboxProfile, workspace: &Path)
        -> BoxFuture<'static, Result<Arc<dyn Sandbox>, SandboxError>>;
}

pub trait Sandbox: Send + Sync + 'static {
    fn call(&self, tier: Tier, name: &str, input: Value) -> BoxFuture<'static, Result<Value, ToolError>>;
    fn release(&self) -> BoxFuture<'static, ()>;
}
```

The runtime:

- MUST open the sandbox lazily, on the activation's first sandboxed call, over the session's workspace-facet directory (facets §4.1), materialized by rehydration before any call can reach the open.
- MUST hold at most one live sandbox per activation and release it on every deactivation: hibernation, migration, or forced step-down (A7). A sandbox never migrates.
- MUST pass each call's declared tier, and only declared, cap-checked tiers (§4.5).
- MAY issue every call of a step concurrently on one `Sandbox` (sandbox §2.1).
- MUST bound each call's outcome by the declared `timeout`, else a runtime default, timed by `Clock` (§4.3).
- MUST, when an open fails, resolve every call that needed the sandbox as `ToolError::Sandbox` and retry the open on the next sandboxed call; an open failure never fails the run.
- MUST, on `ToolError::EnvironmentLost`, release the binding and journal `WorkspaceReset` before the next model call (§4.4).

`SandboxProfile` is the kind's sandbox configuration: a provider-interpreted image, the tier cap, the `Network` tier's egress allowlist, and `ComputeLimits`. It is covered by the kind digest (§5.1), and a `TierAcquired` record grants the allowlist by reference, never inline.

### 2.4 Kinds and tools (`Kind`, `Kinds`, `ToolDecl`, `ToolRegistry`)

A plug point: a deployment defines its agent types. A `Kind` is a system prompt, `ModelParams`, a `ToolRegistry`, a `SandboxProfile`, a default `Budget`, a delegation allowlist, and the `EntityConfig` of its entity type; `Kinds` maps each `KindId` (an entity type, entity §4.1) to its kind. A `ToolDecl` gives a tool's name, description, input schema, `Tier`, `OnOrphan` policy (workflow §3.1), and optional timeout.

- The `ToolRegistry` is the allowlist: a call dispatches by name against it and nothing else. `ToolRegistry::declare` MUST reject a duplicate name.
- Every declared tool is sandboxed, except the built-in `delegate` (§6.1), present iff the delegation allowlist is non-empty, which runs in the loop because a delegation is control flow, not an effect.
- The tier cap is a set, checked by membership (sandbox §3.2): `Kinds::register` MUST reject a tool whose tier the cap excludes. An unset cap is exactly the tiers the declared tools require.
- A sandboxed tool's name, tier, and input are the provider's (sandbox §2.2): a deployment MUST declare a sandboxed tool only as its provider answers it, and SHOULD register the provider's own declarations verbatim.
- The kind's digest MUST cover the whole definition, each tool's tier, and the effective cap (§5.1).

### 2.5 The client view (`Harness`, `SessionRef`)

The consumer-facing boundary, used by applications, the HTTP edge (edge §2.4), and delegating parents (§6.1). A `Harness` is one node's view of the session namespace: `Harness::builder` chooses per kind whether the node hosts it (runs its entity type with the node's seams) or only routes to it, and `Harness::client` builds a routing-only cluster client. `Harness::session` returns a `SessionRef` over the session's `EntityRef` (entity §2.2), with no I/O.

- `Harness::session` MUST return `UnknownKind` as a value for a kind the node neither hosts nor routes: kind names arrive from outside.
- `SessionRef::prompt` submits a turn and awaits its `RunOutcome` within the submit deadline (`prompt_within` takes an explicit one). It fails with `PromptError::Rejected` for a permanent rejection (§5.3) or `PromptError::Entity` for a failure to reach the session or commit the turn. A lapsed deadline MUST surface as `PromptError::Entity(EntityError::Call(CallError::Timeout))`, and the run continues.
- `prompt` MUST NOT transparently retry a failed submit (actor §1.1). It MAY re-submit the same `TurnId` when the attempt's reply mailbox drops without an outcome.
- `SessionRef::cancel` cancels the run a `TurnId` names (§7.2); `SessionRef::tail` and `SessionRef::follow` read committed records without activating the session (§8.2).

### 2.6 The session wire (`Submit`, `Cancel`, `RunCompleted`)

An internal boundary, crossing nodes, between `SessionRef` (or a delegating parent's node) and the session entity. The message types are private to the crate, and of the wire only `SubmitReject` is public. The manifests are interop constants every node agrees on.

| Message | Reply | Manifest |
|---|---|---|
| `Submit { kind, turn, parent, reply_to }` | `Result<Accepted, SubmitReject>` | `agent.Submit` |
| `Cancel { turn }` | `()` | `agent.Cancel` |
| `RunCompleted { session, turn, outcome }` | none: a `tell` | `agent.RunCompleted` |

- `Submit` replies with an ack, `Accepted { turn, status }` (`Started`, `Attached`, or `Ended`), released only when the turn's `TurnSubmitted` commits (entity §5): an acked turn is durable.
- `kind` rides every `Submit` and MUST equal the addressed entity's type; the first accepted `Submit` creates the session. `parent` carries a delegating parent's `Lineage`. `reply_to` is transient routing: never journaled, and outside the equality check (§5.3).
- `RunCompleted` MUST reach each registered reply-to only after `RunEnded` commits, as at-most-once delivery under the subscriber discipline of workflow §2.7; the record, not the notification, is the source of truth.
- Only `Submit` and `Cancel` are accepted from the network; the workflow's `Tick` and `Settled` are local-only, so no peer can inject an outcome.

### 2.7 The journal schema (`Record`, `RecordBody`)

An internal boundary between the session, the only writer, and every reader: `tail`, the edge's record stream, the terminal client, reconstruction (§8.4). A `Record` is the entity's event (entity §1.2): a `RecordBody` plus `at_nanos`, the writing leader's `Clock` reading.

| Record | Meaning |
|---|---|
| `SessionCreated` | Kind, kind digest, lineage, and the tree's root (§5.1, §8.3). Always first. |
| `TurnSubmitted` | A turn accepted into the queue: content, resolved budget, whether the budget was explicit (§5.3). |
| `TurnStarted` | The queue's head turn began its run (§3.2). |
| `ModelResponse` | One response: content, requested calls with their `CallId`s, usage. |
| `TierAcquired` | The activation's first call at a tier is about to execute (§4.5). |
| `ToolOutcome` | One tool or delegation outcome, by `CallId`. |
| `ChildRun` | A delegation's intent: child kind, session, turn, carved budget (§6.1). |
| `WorkspaceReset` | The environment the transcript asserts is gone (§4.4). |
| `RunEnded` | The turn's one terminal outcome (§3.2). |
| `CancelDelivered` | A propagated cancel reached a child (§7.2). |

`apply` MUST be pure and deterministic (entity §3.1, E2): the agent's nondeterminism enters only as committed records. The fold MUST NOT let `at_nanos` influence behavior; it is observational, and virtual under simulation.

### 2.8 The checker stream (`AgentEvent`)

An internal boundary between the runtime and stream checkers. `AgentEvent`s ride the actor event stream as application events (actor §2.7), interleaved with the entity's `Activated`, `Deactivated`, and `LeaderChanged` (entity §10) in one totally ordered, seed-reproducible sequence. They carry no session content, and are the one sanctioned restatement of the journal (§8.1): a continuous checker reads the stream, not per-session journals.

| Event | Fields | Emitted when |
|---|---|---|
| `RunStarted` | `session`, `turn`, `parent` | the turn's `TurnStarted` committed |
| `ModelCompleted` | `session`, `turn`, `node` | a model response committed |
| `ToolCompleted` | `session`, `turn`, `node` | a `ToolOutcome` committed |
| `RunEnded` | `session`, `turn`, `outcome` | the run's terminal record committed |
| `SandboxBound` | `session`, `node` | the activation opened its sandbox |
| `SandboxReleased` | `session`, `node` | that sandbox was released |

- `RunStarted`, `ModelCompleted`, `ToolCompleted`, and `RunEnded` MUST be emitted only after the record each announces commits, and only by the committing activation: a straggler or a deposed leader's uncommitted work emits nothing.
- `RunStarted` MUST fire exactly once per `(session, turn)`: a resume emits none (it reads as `Activated` then `ModelCompleted` on a started turn), and a turn cancelled while queued emits neither `RunStarted` nor `RunEnded`.
- `SandboxBound` and `SandboxReleased` MUST alternate within the entity's `Activated`/`Deactivated` window for the session and node, and a bound sandbox's `SandboxReleased` MUST precede that activation's `Deactivated`.

### 2.9 Implemented seams: `Entity` and `Workflow`

The session type implements `Entity` (state: the fold of §2.7; facet: the workspace, facets §4.1) and `Workflow`. Its desired set holds every owed cancel and, for a live run, the current step's model call (unless the budget forbids it) or every journaled call lacking an outcome, under its declared `OnOrphan`. Launch, orphan resolution, backoff, stragglers, quiescence, and subscribers are the workflow's (workflow §2.7, §3, §4, §6). The agent's obligations at that seam:

- Its `StepKey` qualifies every step ordinal and `CallId` with the turn, so no key re-enters the desired set (workflow §3.1).
- It MUST settle the gates the desired set cannot see (a sandbox to open, a tier to acquire, a lost workspace to report) before each tick, never inside a launch (workflow §2.3).
- Its `can_hibernate` narrows the workflow's twice: never while an accepted turn waits in the queue, never while a sandbox open is in flight (workflow §6).

## 3. Sessions and runs

### 3.1 A session is an entity

A session is one durable thing, its journal, and two disposable ones: the activation (the fold plus the in-flight run, on the shard leader) and the sandbox. Its `EntityName` has the `KindId` as entity type and the `SessionId` as key; a `SessionId` outlives every `ActorId`, and each `TurnId` names one run. A session's state MUST be a pure, deterministic function of its journal prefix (entity §1, E3), so replay is resume. Single-writer safety is the fence's (storage §5, E1), and the contiguity guard's forced step-down (entity §5) releases the sandbox.

A **turn** is one submitted input (a prompt, or a parent's delegation) carrying a client-chosen `TurnId`, its idempotency key. A **run** is the model-and-tools loop a turn triggers, named by the same `TurnId`. A **step** is one model call plus the resolution of every call it requested.

### 3.2 The run loop

Runs serialize through the session's turn queue, a fold projection: every accepted turn is journaled at once (`TurnSubmitted`) and waits in acceptance order. When no run is live, the session commits `TurnStarted` for the head, the one record that begins a run and the only place a turn's content enters the transcript. At most one run is live per session, and an acked turn survives crash, hibernation, and migration.

Each step of a run:

1. checks the budget (§7.1), and if it is exhausted ends the run with `RunError::BudgetExhausted`;
2. issues one model call (§2.1);
3. if the response requests no calls, journals it and ends the run with `Ok(Completion)`, in one batch;
4. otherwise journals the response, then executes every requested tool call (§4.2) and delegation (§6.1);
5. journals each outcome as it arrives, and begins the next step once all are resolved.

A run MUST end in exactly one terminal outcome, its `RunEnded` record (A2): `Ok(Completion)`, `RunError::BudgetExhausted`, `RunError::Cancelled`, or `RunError::Model`. A `RunError` is an application error carried in `RunCompleted`; transport and durability failures stay `EntityError`s on the `Submit` (§2.6).

The activation MUST NOT await the model, a tool, or an append inline: each operation is launched through `Spawner` and returns as a message (workflow §3.2). The input gate (entity §5) admits a `Cancel` only between appends, and the activation MUST discard an outcome that arrives after its run ended or its activation stepped down (workflow §4.4).

### 3.3 Write-ahead order

The entity's output gate (entity §5, E5) holds each effect until its record commits. A step's records commit in this order:

- `TurnSubmitted` before the `Submit` ack, and `TurnStarted` before the run's first model call;
- a `ModelResponse` before any call it requested executes (a model call itself needs no intent record);
- `TierAcquired` before the first call at a tier not yet held;
- a `ChildRun` before its child `Submit`;
- each tool or delegation outcome before the next step shows it to the model;
- `RunEnded` before any `RunCompleted` carries it, sent after the commit or, for a re-submitted ended turn, from the fold's recorded outcome.

### 3.4 Durability failure pauses the run

A shard without a quorum answers `Unavailable` (storage §2.1, §10): the record does not commit and the activation steps down. The loop MUST NOT proceed past an uncommitted record, and since a terminal outcome is itself a record, the run pauses rather than ends. The caller sees `EntityError::Unavailable` or a lapsed deadline, and the run resumes from its last committed record once the shard recovers and a message reaches the session (§5.4): the entity's CP stance (entity §1.1, E11).

## 4. Tools and the isolation boundary

### 4.1 The boundary

The loop (control flow, model calls, budgets, delegation, the fold) runs in the activation: serial, journaled, cheap. Tool effects run in the session's sandbox, where a tool may do anything inside and nothing outside. Actor isolation protects sessions' state from one another; sandbox isolation protects the node and other sessions from tools.

**Rationale:** with no sandbox, one runaway command takes down a node hosting many sessions; with the loop inside the sandbox, every session pays for isolation to host control flow that mostly waits; with a sandbox per call, consecutive calls lose the checkout or build cache the previous step produced. Hence one sandbox per session, opened lazily per activation on the shard leader (entity §4.2).

### 4.2 Dispatch, and tool failure as a transcript value

Every requested call carries a `CallId`, unique within its run and journaled with the call's intent; outcomes, orphan resolution, and child derivation key on it. A failing tool does not fail the run: the call's `ToolError` is journaled as its outcome and shown to the model as the tool result.

- A call naming a tool outside the registry MUST resolve as `ToolError::UnknownTool`. An input that is not a JSON object, or a `delegate` input that does not deserialize or names a kind outside the allowlist, MUST resolve as `ToolError::InvalidArguments`. Both are synthesized: nothing executes.
- The loop checks structure only, never the input schema, which is interface metadata for the model; validation beyond shape is the executing tool's own `ToolError`.

### 4.3 The workspace and its capture

The sandbox runs over the session's workspace facet (facets §4.1), rematerialized from the journal by every activation before the sandbox opens.

- After every sandboxed call, success or failure, the runtime MUST capture the workspace, so the call's file delta commits atomically with its `ToolOutcome` (entity E19). A delegation captures nothing, and neither does a call that reported `EnvironmentLost`.
- A capture over the facet's durable cap MUST resolve the call as `ToolError::Sandbox`, its delta unpersisted: an outcome the model can repair. The next capture diffs against the last committed index, so nothing is skipped.
- The timeout bounds the outcome, not the effect: a call MAY still be writing when its `Timeout` is journaled, so that capture can be torn. The journal stays consistent with what the directory held, and the next capture folds in what the straggler finished.
- A capture that finds the workspace root gone MUST resolve the call as `ToolError::EnvironmentLost` (§4.4).

### 4.4 Crash, loss, and resume

A call whose intent is journaled but whose outcome is not when an activation begins is orphaned (workflow §4.3): a `Reexecute` call MUST be re-executed, in a fresh sandbox if need be, and an `Interrupt` call MUST resolve as `ToolError::Interrupted`, so the model decides whether to retry the side effect. The fence guards the transcript, not the world (workflow §3.3): a deposed leader may still be executing a slow call when its successor resolves it, so a re-execution may overlap the original for the duration of that call. Only `delegate` is safe by construction, its re-execution attaching to the same derived `TurnId` (§5.3).

The sandbox is not session state: the fold never reads it, and the runtime never reconstructs a lost environment (processes, containers, trees outside the capture, held tiers). What must outlive it does so through a tool or through the workspace's durable subtree. The model MUST NOT be left asserting a world that is gone:

- A routine reactivation journals no reset: the durable subtree is rematerialized before the sandbox opens.
- An environment lost during an activation MUST be put on the record: the activation journals `WorkspaceReset` before the next model call, which presents it (§2.1). The loss comes from the provider answering `ToolError::EnvironmentLost` (sandbox §5.2) or from the runtime's own capture finding the root gone (§4.3); either way the binding is released, and the next sandboxed call opens afresh.
- Tier loss follows sandbox §5.1: a fresh environment restarts the held set at `Workspace`, and a lost tier fails its calls as `ToolError` outcomes, never silently re-granted.

### 4.5 Execution tiers

A tier is a capability set a call requires (`Workspace`, `Compute`, `Network`, `Native`; sandbox §3). The runtime's side:

1. Opening grants `Workspace` and nothing else.
2. Before the first call at a tier the activation does not hold, the loop MUST journal `TierAcquired { turn, tier }` (§3.3), the audit trail of when a session first ran guest code or touched the network.
3. Held tiers are additive and activation-scoped: the held set resets to `Workspace` when `WorkspaceReset` is journaled, and a fresh activation starts from `Workspace`. Only the acquisition records survive.
4. The cap is unreachable by construction: tiers are checked at registration (§2.4) and dispatch reaches declared tools only, so the loop performs no runtime cap check (sandbox §6, S4).
5. Each tool's tier and the profile's cap are digest-covered (§5.1), never a node-local choice.

## 5. Sessions across the cluster

A session's cluster life is the entity's lifecycle (entity §7); the runtime adds no placement, host, or resume protocol.

### 5.1 Kinds and the node directory

Kinds are agreed cluster-wide like the codec: a session MUST be resumable on any node hosting its kind, so every hosting node MUST register every kind with the same definition. `SessionCreated` pins the session's kind digest, so a reader can tell an exact reconstruction (§8.4) from one a kind change made indicative. The digest detects drift and authenticates nothing.

A node's directory is the kinds it hosts plus those it only routes to (entity §4.3). Building a harness MUST fail for a directory that does not close over delegation, because the parent's node launches child submits and owed cancels through its route for the child's kind (§6.1, §7.2); hosting a kind the registry lacks MUST fail at `HarnessBuilder::host`. A missing kind is a deployment error surfaced before any run, never a mid-run discovery.

### 5.2 Placement, activation, and hibernation

The session activates on its shard leader on the first message, exactly once per node (E6; entity §4).

- After an idle interval the leader MAY hibernate a session (entity §7), releasing the sandbox (A7). It MUST NOT hibernate a session whose run is live or whose queue holds an accepted turn.
- A command reaching a node that does not lead the shard receives `NotLeader`, and the runtime retries at the hinted leader within the caller's deadline, safe by the `TurnId`.

### 5.3 Submission and idempotency

A `Submit` returns promptly, and nothing a caller holds bounds the run: budgets bound tokens and steps, not wall time, so a run MAY outlive any caller, who recovers the outcome by re-submitting the same `TurnId` or following `tail`. The runtime never re-issues an ambiguous failure (entity §1.1); the `TurnId` makes the caller's own re-submission safe:

- A `Submit` whose `TurnId` is already journaled MUST NOT start a second run (A6). A queued or live turn registers the new reply-to (`Attached`); an ended one has its recorded outcome sent at once (`Ended`). Dedup is a fold over the records, exact across a resume.
- Registration is race-free: the input gate orders a re-submission against `RunEnded`, so a caller is either added to the live run or told the outcome, never neither or both.
- The equality check covers the turn's content and its literal budget field, so `None` is not `Some(default)` and a conflict never depends on the kind's current default. It is recomputed from the journaled `TurnSubmitted`.
- A re-submission whose content or budget differs (`SubmitReject::ContentConflict`), or whose `kind` is not the entity's type (`SubmitReject::KindMismatch`), MUST be rejected as a value, journaling nothing. The rejection is permanent and MUST NOT be retried.
- The check guards against accidental `TurnId` reuse, not a crafted collision: whoever can address a session already holds it. Isolating callers is the edge's job (edge §3.2).

The subscriber set is ephemeral (workflow §2.7), rebuilt by callers re-contacting after a resume.

### 5.4 Resume

Every recovery is the entity's rehydration (storage §8, entity §7): after a fault, after a migration (storage §5.3), and on a new leader after node failure. The next message activates the session, which rehydrates (rematerializing the workspace), folds, resolves orphaned calls (§4.4), and continues any unfinished run. No acknowledged record is lost (E14), and the resumed activation behaves identically to one that never stopped, given the same subsequent model and tool outcomes (A1).

Resumption is caller-driven: an interrupted run or a waiting turn resumes only when a message reaches the session (a re-submitted `TurnId`, a parent's re-executed delegation, or a `Cancel`, which also nudges a propagation still owed, §7.2). A run nobody asks after stays interrupted, its cost bounded by its budget. A2's liveness is conditional on this contact, and so is A4's for a propagation a crash interrupted.

## 6. Delegation

### 6.1 Delegation is a tool

The built-in `delegate` takes a child kind, a prompt, and optionally a budget (`DelegateInput`). The child kind MUST be in the parent kind's delegation allowlist, so a locked-down kind cannot escalate through a permissive one. A delegation:

1. journals a `ChildRun`: the carved budget (§7.1), and the child `SessionId` and `TurnId`, derived deterministically from the parent session, its `TurnId`, and the delegation's `CallId`. The derivation length-prefixes each component, so distinct triples derive distinct ids even from crafted ids, and every derived `SessionId` begins with a decimal length followed by `:` (edge §3.3 relies on this);
2. submits to the child with a reply-to and awaits its `RunCompleted`, re-attaching on the submit-deadline cadence, bounded in total by a budget-scaled wait (a configurable floor plus a configurable allowance per carved step). Past the wait, or after a bounded run of consecutive unreachable attempts, the call resolves as `ToolError::Delegation` and cancels nothing: the child runs on, its budget the backstop;
3. takes the child's outcome as the call's outcome; a child's `RunError` or `SubmitReject` is a `ToolError::Delegation`.

The child is a full session, with its own entity, journal, budget, and shard. A resumed parent re-executes the orphaned delegation (`Reexecute`), and the child's journaled `TurnId` turns the re-submission into an attach (§5.3), so the outcome arrives whether the child is running or done.

### 6.2 Tree shape and failure

- The tree lives in the journals (`ChildRun`, and `SessionCreated`'s lineage), not in a replicated structure; the runtime builds no cross-entity transaction (entity §1.1).
- Each delegation carves from the parent's remaining budget, so a tree's spend is bounded by the root's budget whatever its shape (A3).
- A child's failure is a tool outcome for the parent's model, never a supervision fault: supervision is local (actor §2.4), and the tree spans nodes.

## 7. Budgets and cancellation

### 7.1 Budgets

A `Budget` is a token limit and a step limit.

1. Every run has a budget: the turn's explicit one, else the kind's default. Spend is the model-reported usage of the run's calls plus its children's carve-outs, and MUST be a projection of the journal, so a resumed activation re-derives it by replay.
2. A model call is issued only while spend is below the token limit and the step count below the step limit. Its `max_tokens` MUST be clamped to the remaining tokens, and no call is issued while the remainder is below a configurable floor. Output overshoot is therefore zero, and total overshoot at most one call's input. Exhaustion ends the run with `RunError::BudgetExhausted`.
3. A delegation reserves an explicit slice of the parent's remainder, enforced by the child with no cross-shard accounting: own spend plus carve-outs never exceeds the budget, at every node of the tree (A3).
4. The bound is on journaled usage: a response that never commits (from a leader deposed mid-step) goes uncounted though the provider bills it.

### 7.2 Cancellation

1. A `Cancel` names its run by `TurnId` and takes effect at the next message boundary. Naming an ended or unknown run MUST be a no-op, so a delayed `Cancel` never kills a successor. Naming a still-queued turn MUST journal its `RunEnded { Cancelled }` directly: the turn leaves the queue, its subscribers are notified, and no run begins.
2. Cancelling the live run journals `RunEnded { Cancelled }`, notifies every registered reply-to, discards the run's later outcomes, and owes propagation: the fold moves each `ChildRun` without an outcome into an owed-cancels projection, each a member of the desired set, relaunched under the workflow's backoff (workflow §3.4) until the child acks. The debt is a journal projection, so a crash after the terminal commit loses nothing.
3. A child acks only after its own commit, and the parent then journals `CancelDelivered`, clearing that debt. At the backoff cap the key rests so the parent may hibernate (workflow §6), and the next message relaunches it. Once faults cease and a message reaches the session, propagation completes within bounded logical time (A4); under unhealed faults the child's budget is the backstop.
4. In-flight effects of a cancelled run are not undone and MAY complete; their outcomes are discarded.

## 8. Observability

### 8.1 The journal is the record

The journal is the single source of truth for what a session did, and every observation API is a read of it. The runtime MUST NOT keep a second durable account of session activity. Records (§2.7) are durable and user-facing; events (§2.8) are ephemeral and checker-facing.

### 8.2 Reading a run

`SessionRef::tail` returns a bounded page of committed records after a cursor, with the journal's compaction `base`, as an `EventHistory`: a replication-free read of the leader's journal (entity §6). `SessionRef::follow` returns the entity's `Follower` (entity §6.3), which yields the exact committed sequence, or `Followed::Truncated` where compaction outran the cursor.

- A read MUST NOT write, activate a hibernated session, or refresh an active one's idle clock.
- A page's `Seq`s are journal slots; the workspace facet's records occupy the gaps (facets §4.1). A cursor below `base` asks for records the snapshot has subsumed, which are not served.
- Reads are read-your-leader (entity §6): a deposed leader not yet fenced MAY serve a stale page, though writes never fork.

The edge serves both reads over HTTP (edge §2.1).

### 8.3 Tree correlation

Every `SessionCreated` records its root: its own `SessionId` without a parent, else the parent's root. With the parent links it names one delegation tree across shards and journals with no minted identifier; nothing routes or folds on it.

### 8.4 Reconstruction and metrics

A model request is a deterministic function of the fold and the kind, so the exact `ModelRequest` of any step is reconstructible from the journal prefix, and `SessionCreated`'s digest says whether the kind in hand is the one the session ran (§5.1). Aggregate metrics (spend per kind and per tree, run latency, tool failure rates) are RECOMMENDED, derived from records and events rather than instrumented separately.

## 9. Determinism

The `agent` crate MUST satisfy actor §2.12.2: no wall clock, no OS threads, no unseeded randomness, no socket; every operation is launched through `Spawner`, timed by `Clock`, and randomized by `Entropy`. Model output is nondeterministic in production, so reproducibility is scoped to fixed outcomes: with a scripted model and a scripted sandbox, one seed reproduces a whole multi-node run (model and tool faults, crashes, elections, resumes) through the real entity code (entity §11).

## 10. Invariants

Only what is specific to the agent: the fence (E1) and the write-ahead obligation (E5) are wholly the entity's. The Rust copy of this table, with the suites that verify each row, is `agent`'s conformance catalogue.

| # | Invariant | Defined in | Entity basis |
|---|---|---|---|
| A1 | **Deterministic fold and resume.** Session state is a pure fold of the journal; a session resumed from any committed prefix behaves identically to one that never stopped, given the same subsequent model and tool outcomes. | §3.1, §5.4 | E2, E3 |
| A2 | **Run termination.** Every `RunStarted` is followed by exactly one `RunEnded`; once faults cease, partitions heal, and a message reaches the session, no run remains pending past its budget's bound. The record is the source of truth; a lost `RunCompleted` is recovered by re-contact, never retried. | §3.2, §5.4, §7 | E14 (lossless failover) |
| A3 | **Budget bound.** A run issues no model call after exhaustion; output spend never exceeds the remaining budget at call time; own spend plus carve-outs never exceeds the budget, at every level of a delegation tree. | §7.1 | (none) |
| A4 | **Cancellation.** After a cancel is journaled, the run and, once faults cease, every descendant run end `Cancelled` within bounded logical time, issuing no further model calls. | §7.2 | (none) |
| A5 | **Single autonomous activation.** A node never runs two concurrent activations of one session; on a converged cluster at most one is live; an addressed session activates within bounded logical time of a message reaching its shard leader. | §5.2, §5.4 | E6, storage §5 |
| A6 | **Idempotent submission.** A re-submitted `TurnId` never starts a second run: it registers the caller's reply-to, or delivers the recorded outcome if the run ended, under any duplication or retry. | §5.3 | E1 (one commit) |
| A7 | **Effect containment.** An activation binds at most one live sandbox and releases it on deactivation; every sandboxed call executes in its issuing activation's sandbox, never another session's; sandbox loss surfaces in the journal (`ToolError` outcomes, `WorkspaceReset`), never as silent corruption. | §2.3, §4.4 | entity §4.2 (colocation) |

The sandbox's tier invariants are their own catalogue (sandbox §6).

## 11. Deferred

- **Durable alarms for sessions:** nothing reactivates a session without a caller, so resumption is caller-driven (§5.4); the entity half exists (facets §4.5).
- **Scheduler singleton:** no queued or recurring runs; it waits on durable alarms.
- **Linearizable reads:** a minority-side leader may serve a stale `tail` until the entity's leader lease lands (entity §13).
- **Warm pools and a network dataplane:** every open pays full provisioning, and no provider offers `Network` (sandbox §7).
- **Multi-tenant scheduling:** no quotas or fair share across tenants.
- **Context compaction:** an over-long transcript fails the run; a checkpoint record must preserve A1.
- **Token streaming:** `complete` is one request and one response, so a client sees a step only once it commits.
- **Permission gating:** no per-call authorization hook; the tier cap is the only, static, gate.
- **Loop-executing tools:** `delegate` is the only tool that runs in the loop.
- **Code mode:** the toolset is not an API the model programs against at the `Compute` tier, so every call costs a loop round trip.
- **External trace interop:** root and parent links (§8.3) are not mapped onto W3C trace-context ids.
- **Cross-session sagas:** nothing coordinates work above the single-session boundary.
- **Shared post-commit event staging:** the agent and the workflow each stage events owed after a commit (workflow §2.8).
