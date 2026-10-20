# Agent: Specification

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, **RECOMMENDED**, and **MAY** carry the meanings defined in RFC 2119. Sections of this document are cited as plain **§N**; other documents carry a prefix: **actor §N**, **entity §N**, **storage §N**, **facets §N**, **workflow §N**, **sandbox §N**, **edge §N**. Invariants defined here are **A1–A7** (§12).

## 1. Scope

An agent runtime runs compound AI sessions (loops of model calls, tool calls, and delegation) on a shared cluster. Each session is an entity (entity §1) that drives itself as a workflow (workflow §3). The entity owns identity, the journal, the fence, placement, activation, hibernation, and failover (entity §4–§8); the workflow owns deriving, launching, and settling effects (workflow §4, §5). The runtime adds the transcript, kinds, budgets, delegation, cancellation, and the model (§3.1, §3.2), sandbox (§3.3), and client view (§3.4) seams, which models, sandbox providers (sandbox §1), and the HTTP edge (edge §1) build on.

### 1.1 Non-goals

- **A model gateway:** no routing, A/B serving, or quota brokering across models.
- **Prompt management:** prompts and tool descriptions are application data.
- **Exactly-once tool execution:** a call is at-most-once per attempt (entity §1.1); crash recovery is explicit and per tool (§5.4).
- **An isolation technology:** the provider chooses process, container, or microVM (sandbox §1.1).
- **Consensus of its own:** no Raft group, shard, or fence beyond the entity's.
- **Multi-tenant economics:** a budget bounds a run, not a tenant (§9.1).
- **Context-window management:** an over-long transcript fails the run (§3.1).

### 1.2 Conformance classes

- An **agent runtime**: the runtime's side of §3.1, §3.3, and §3.4, and all of §4–§11. It MUST meet the deterministic profile (actor §11).
- A **model**: the model's side of §3.1. An adapter shell, protocol, or socket: §3.2.
- A **sandbox provider**: the provider's side of §3.3 (sandbox §3–§6).
- A **deployment**: §6, plus a conforming model and sandbox provider for every kind a node hosts.
- A **caller** of the client view: the caller's side of §3.4 and §7.2.

## 2. Model

- **Session:** a durable agent conversation: an entity whose type is its kind's name and whose key is its application-chosen **session id** (entity §5.1). Its **activation** (entity §5.2) holds the fold and the in-flight run.
- **Record:** one entry of a session's journal (entity §2), of a kind registered in §11.2, stamped with the writing leader's clock reading. Only the session writes its records.
- **Kind:** a named agent definition: a system prompt, model parameters (a model identifier and a per-call output ceiling), a tool registry, a sandbox profile (§3.3), a default budget, a **delegation allowlist** (the kinds its sessions can delegate to), and its entity type's configuration, all covered by its **kind digest** (§6.1).
- **Tool registry:** a kind's tool declarations, each a name, description, input schema, tier, orphan policy (reexecute or interrupt, workflow §5.3), and optional timeout.
- **Turn:** one submitted input (a prompt, or a parent's delegation) with a caller-chosen **turn id**, its idempotency key, and an optional budget. A session's **turn queue** holds its accepted, unstarted turns in acceptance order.
- **Run:** the loop of model and tool calls a turn triggers, named by the turn id. A **step** is one model call plus the resolution of every call it requested.
- **Call:** one tool invocation a model response requests: a **call id** unique within its run, a tool name, and a JSON input. Its **outcome** is a JSON value or a `ToolError` (§3.3).
- **Transcript:** what the model reads, derived from records: started turns, responses, call outcomes, and workspace resets (§3.1).
- **Run outcome:** a completion (the final assistant message and the run's journaled token spend) or a run error: `RunError::BudgetExhausted`, `RunError::Cancelled`, or `RunError::Model` (carrying a `ModelError`).
- **Sandbox:** the isolated environment one activation's tools run in, opened from a **sandbox provider** (§3.3).
- **Workspace:** the session's workspace facet directory (facets §5.1).
- **Tier:** a capability set a call requires (sandbox §4.1). A kind's **tier cap** is the set of tiers its sessions can hold; an activation's **held tiers** are those it has acquired.
- **Budget:** a token limit and a step limit on one run. **Spend**, in tokens and steps, is the run's model-reported usage plus its **carve-outs**, the budgets reserved for its children.
- **Lineage:** a delegated session's parent session and turn, and the **root** session of its delegation tree.
- **Reply-to:** an address a caller registers on a turn to receive its run outcome.
- **Owed cancel:** a cancel a cancelled run still owes a child it started.
- **Node view:** one node's view of the session namespace: per kind, the node **hosts** it (runs its sessions with its own model and sandbox provider) or only routes to it; its **directory** is the kinds it hosts or routes (entity §5.3).
- **Model:** an implementation of the model seam (§3.1).

## 3. Seams

### 3.1 The model (`Model`)

A plug point between the run loop, which calls it once per step (§4.2), and a model.

```rust
pub trait Model: Send + Sync + 'static {
    async fn complete(&self, req: ModelRequest) -> Result<ModelResponse, ModelError>;
}
```

A `ModelRequest` carries the kind's system prompt, its `ModelParams` (with `max_tokens`, §9.1), the toolset as `ToolSpec`s, and the transcript as `Entry` values: `Entry::User` (a turn's content), `Entry::Assistant` (a response's content and calls), `Entry::ToolResult` (a call's outcome, by `CallId`), and `Entry::WorkspaceReset` (§5.4). A `ModelResponse` carries the assistant content, its calls as `ToolCall`s, and the provider-reported `Usage` (input and output tokens). A `ModelError` is `RateLimited`, `Overloaded`, `ContextOverflow`, `InvalidRequest`, or `Api` (any other failure).

The runtime:

- MUST build each request as a deterministic function of committed state and the kind (§10.4).
- MUST give each returned call with an empty id an id deterministic in the step and the call's position.
- MUST end the run with `RunError::Model` on any `ModelError`.
- MUST answer, in the transcript, every call an `Entry::Assistant` requests with exactly one `Entry::ToolResult` before any later `Entry::User`, cancelled calls included (§9.2).

A model:

- MUST present every `Entry` in transcript order, MUST pair each `Entry::ToolResult` with the call its `CallId` names, and MUST present `Entry::WorkspaceReset` as runtime-authored input, never as a tool result, and SHOULD render it as the workspace-reset notice (§11.3).
- MUST send `max_tokens` as the output ceiling, never a larger one.
- MUST return in `Usage` the usage the provider reports; budgets count nothing else (§9.1).
- MUST give returned calls ids unique within the run, or leave them empty.
- MUST map an over-long transcript to `ContextOverflow` and a request the provider calls malformed to `InvalidRequest`.
- MAY retry internally, but MUST NOT retry `ContextOverflow` or `InvalidRequest`, and MUST bound every other retry, so `complete` always resolves in finite logical time.
- MUST meet the deterministic profile (actor §11).

### 3.2 The HTTP adapter kit (`HttpPost`, `Protocol`)

Two plug points inside the model seam, the deployment's **socket** and a per-wire-format **protocol**, which the **adapter shell** joins into a model.

```rust
pub trait HttpPost: Send + Sync + 'static {
    async fn post(&self, path: &str, headers: &[(String, String)], body: Vec<u8>) -> Result<HttpResponse, HttpError>;
}

pub trait Protocol: Send + Sync + 'static {
    const PATH: &'static str;
    fn headers(api_key: &str) -> Vec<(String, String)>;
    fn encode(req: &ModelRequest) -> Value;
    fn classify(response: &HttpResponse) -> Result<ModelResponse, ModelError>;
}
```

- **Socket:** `post` sends `body` with `headers` to `path` under the socket's base URL and returns the status and body, or an `HttpError` if the exchange did not complete. It MUST resolve every post under its own timeout, and SHOULD NOT retry.
- **Protocol:** `encode` MUST meet §3.1's transcript obligations, and `classify` maps a completed exchange onto a `ModelResponse` or a `ModelError` as §3.1 requires. A protocol decides no retry.
- **Adapter shell:** encodes, posts, and classifies, counting an `HttpError` as `Api`. It retries `RateLimited`, `Overloaded`, and `Api` with backoff up to a bound, then returns the last `ModelError`, and meets the model's side of §3.1.

### 3.3 The sandbox (`SandboxProvider`, `Sandbox`)

A plug point between the runtime and the sandbox provider a deployment supplies per hosted kind. This section is the runtime's side; the provider's side and the tier semantics are sandbox §3–§6.

```rust
pub trait SandboxProvider: Send + Sync + 'static {
    async fn open(&self, session: &SessionId, profile: &SandboxProfile, workspace: &Path) -> Result<Arc<dyn Sandbox>, SandboxError>;
}

pub trait Sandbox: Send + Sync + 'static {
    async fn call(&self, tier: Tier, name: &str, input: Value) -> Result<Value, ToolError>;
    async fn release(&self);
}
```

A `SandboxProfile` is a provider-interpreted image, the tier cap, the `Network` tier's egress allowlist, and `ComputeLimits` (sandbox §5.2); a `TierAcquired` record references the allowlist, never inlines it. A `Tier` is `Workspace`, `Compute`, `Network`, or `Native` (sandbox §4.1). A `ToolError` is one of:

| `ToolError` | Meaning |
|---|---|
| `Timeout` | The call's timeout elapsed (§5.3). |
| `Interrupted` | The call was cut off before its outcome was recorded (§5.4, §9.2). |
| `UnknownTool` | The named tool is outside the registry; nothing executed (§5.2). |
| `InvalidArguments` | The input is structurally malformed; nothing executed (§5.2). |
| `Sandbox` | The sandbox failed the call, its open failed, or the capture exceeded the durable cap (§5.3). |
| `EnvironmentLost` | The environment itself is gone (§5.4; sandbox §6.2). |
| `Delegation` | A delegation failed (§8.1). |

The runtime:

- MUST open the sandbox lazily, at the activation's first sandboxed call, over the workspace rehydration has already materialized.
- MUST hold at most one live sandbox per activation and release it on every deactivation (hibernation, migration, forced step-down; A7). A sandbox never migrates.
- MUST pass each call's declared tier, and only declared, cap-checked tiers (§5.5).
- MAY issue a step's calls concurrently on one sandbox (sandbox §3.1).
- MUST bound each call's outcome by its declared timeout, else a runtime default (§5.3).
- MUST, when an open fails, resolve every call needing the sandbox as `ToolError::Sandbox` and retry the open at the next sandboxed call; an open failure never fails the run.
- MUST, on `ToolError::EnvironmentLost`, release the sandbox and journal `WorkspaceReset` before the next model call (§5.4).

### 3.4 The client view

A boundary between the runtime and its callers: applications, the HTTP edge (edge §4.1), and delegating parents (§8.1).

| Operation | Takes | Yields |
|---|---|---|
| Open a session reference | a kind name, a session id | a session reference, or an unknown-kind error; no I/O |
| Prompt | a turn, optionally a deadline (else the submit deadline) | the run outcome, a rejection (§7.2), or an entity error (entity §3.2) |
| Cancel | a turn id | nothing, once the session has handled it (§9.2) |
| Outcome | a turn id | the turn's recorded run outcome, or none |
| Tail | a cursor, a limit | a bounded page of committed records, with the compaction base (§10.2) |
| Follow | a cursor | the committed records after the cursor, or a truncation (§10.2) |

- A session reference addresses one session through its entity reference (entity §3.2). Opening one MUST return the unknown-kind error as a value for a kind the node neither hosts nor routes.
- Prompt submits with a reply-to (§7.2). A lapsed deadline MUST surface as the entity's call error carrying a timeout (entity §3.2, actor §8.2); the run continues.
- Prompt MUST NOT transparently retry a failed submit (actor §1.1), but MAY re-submit the same turn id when its reply-to stops before delivering an outcome.
- Outcome yields none while the turn is queued or running or was never accepted. It commits nothing, activates the session, and answers even after compaction removed the turn's `RunEnded`.

## 4. Sessions and runs

### 4.1 A session is an entity

A session is one durable thing, its journal, and two disposable ones, its activation and its sandbox. Its state MUST be a pure, deterministic function of its journal prefix (entity §1, E3); replay is resume. Its fold MUST be pure and deterministic (entity §4.1, E2): every nondeterministic input (a model response, a tool outcome, a clock reading) enters only as a committed record, and the fold MUST NOT let a record's timestamp influence behavior. Single-writer safety is the fence's (storage §6, E1).

### 4.2 The run loop

Runs serialize through the turn queue: each accepted turn is journaled at once (`TurnSubmitted`). When no run is live, the session commits `TurnStarted` for the queue's head; that record alone begins the run and puts the turn's content in the transcript. At most one run is live per session, and an acked turn survives crash, hibernation, and migration.

Each step of a run:

1. checks the budget (§9.1) and, if it is exhausted, ends the run with `RunError::BudgetExhausted`;
2. issues one model call (§3.1);
3. if the response requests no calls, journals it and ends the run with a completion, in one batch;
4. otherwise journals the response, then executes every requested tool call (§5.2) and delegation (§8.1);
5. journals each outcome as it arrives, and begins the next step once all are resolved.

A run MUST end in exactly one run outcome, its `RunEnded` record (A2). A run error is an application outcome carried in `RunCompleted`; transport and durability failures are entity errors on the submit (entity §3.2).

The activation MUST NOT await the model, a tool, or an append inside a command: each effect's outcome returns as a message (workflow §4.2). The input gate (entity §6) admits a cancel only between appends, and the activation MUST discard an outcome arriving after its run ended or its activation stepped down (workflow §5.4).

### 4.3 Write-ahead order

The entity's output gate (entity §6, E5) holds each effect until its record commits. A step's records commit in this order:

- `TurnSubmitted` before the submit's ack, and `TurnStarted` before the run's first model call;
- a `ModelResponse` before any call it requested executes (a model call needs no intent record);
- `TierAcquired` before the first call at a tier not yet held;
- a `ChildRun` before its child submit;
- each tool or delegation outcome before the next step shows it to the model;
- `RunEnded` before any `RunCompleted` carrying it (§7.2).

### 4.4 Durability failure pauses the run

A shard without a quorum answers unavailable (storage §3.1, §11): the record does not commit and the activation steps down. The loop MUST NOT proceed past an uncommitted record; since a terminal outcome is itself a record, the run pauses rather than ends. The caller sees the entity's unavailable error or a lapsed deadline; the run resumes from its last committed record once the shard recovers and a message reaches the session (§7.3; entity §1.1, E11).

### 4.5 The session as a workflow

A session implements the entity seam (entity §3.1), with the workspace as its facet (facets §5.1), and the workflow seam (workflow §3.1). Its desired set (workflow §4) holds every owed cancel and, for a live run, either the current step's model call (absent when the budget forbids one, §9.1) or every journaled call lacking an outcome, each under its orphan policy (§5.4). Launch, orphan resolution, backoff, stragglers, quiescence, and reply-to registration follow the workflow (workflow §9, §4, §5, §7). Its pre-tick gate (workflow §3.3) is the sandbox open (§3.3), tier acquisition (§5.5), and a lost workspace's reset (§5.4).

## 5. Tools and the isolation boundary

### 5.1 The boundary

The loop (control flow, model calls, budgets, delegation, the fold) runs in the activation. Every tool effect runs in the session's sandbox, opened on the node hosting the activation (entity §5.2), and reaches nothing outside it (sandbox §5).

### 5.2 Dispatch, and tool failure as a transcript value

Every call's id is journaled with its intent; outcomes, orphan resolution, and child derivation key on it. A failing tool does not fail the run: its `ToolError` is journaled as the call's outcome and shown to the model as the tool result.

- A call naming a tool outside the kind's registry MUST resolve as `ToolError::UnknownTool`. An input that is not a JSON object, or a `delegate` input that does not parse or names a kind outside the delegation allowlist, MUST resolve as `ToolError::InvalidArguments`. Both are synthesized: nothing executes.
- The runtime checks structure only, never the input schema; the executing tool validates beyond shape.

### 5.3 The workspace and its capture

The sandbox runs over the session's workspace (§3.3; facets §5.1).

- After every sandboxed call, success or failure, the runtime MUST capture the workspace, and the call's file delta MUST commit atomically with its `ToolOutcome` (entity E19). A delegation captures nothing, nor does a call that reported `ToolError::EnvironmentLost`.
- A capture over the facet's durable cap MUST resolve the call as `ToolError::Sandbox`, its delta unpersisted; a later capture persists every change since the last committed one.
- The timeout bounds the outcome, not the effect: a call MAY still be writing when its `ToolError::Timeout` is journaled, so that capture can be torn. The journal stays consistent with what the directory held, and a later capture records what the straggler finished.
- A capture that finds the workspace root gone MUST resolve the call as `ToolError::EnvironmentLost` (§5.4).

### 5.4 Crash, loss, and resume

A call journaled without an outcome when an activation begins is orphaned (workflow §5.3). An orphan whose policy is reexecute MUST be re-executed, in a fresh sandbox if need be; one whose policy is interrupt MUST resolve as `ToolError::Interrupted`. The fence guards the transcript, not the world (workflow §4.3): a re-execution can overlap a deposed leader's still-running original while that call lasts. A re-executed `delegate` attaches to the child turn it derived before (§8.1).

The sandbox is not session state: the fold never reads it, and the runtime never reconstructs a lost environment (processes, containers, trees outside the capture, held tiers). Only the workspace's durable subtree (what the facet captures into the journal) and what tools persist themselves outlive it. The runtime MUST NOT leave the model asserting an environment that is gone:

- A routine reactivation journals no reset: the durable subtree is rematerialized before the sandbox opens.
- An environment lost during an activation, whether the provider reports it (`ToolError::EnvironmentLost`, sandbox §6.2) or the capture finds it (§5.3), goes on the record: the sandbox is released and `WorkspaceReset` journaled before the next model call (§3.3), which presents it (§3.1); the next sandboxed call opens a fresh sandbox.
- Tier loss follows sandbox §6.1: held tiers restart at `Workspace` (§5.5), and a lost tier fails its calls as `ToolError` outcomes, never silently re-granted.

### 5.5 Execution tiers

The runtime's side of the tier model (sandbox §4):

1. Opening grants `Workspace` only.
2. Before the first call at a tier the activation does not hold, the runtime MUST journal `TierAcquired` (§4.3).
3. Held tiers are additive and activation-scoped: they reset to `Workspace` when `WorkspaceReset` is journaled, and a fresh activation starts from `Workspace`. Only the acquisition records survive.
4. Every call's tier lies within the cap: tiers are checked at registration (§6.1), and dispatch reaches declared tools only (sandbox S4).
5. Each tool's tier and the cap belong to the kind (§6.2), never to a node.

## 6. Kinds

### 6.1 Kind definitions

- A call dispatches by name against its kind's tool registry and nothing else. The runtime MUST refuse a registry that declares a name twice.
- Every declared tool is sandboxed except the built-in `delegate` (§8.1), which runs in the loop and is in the model's toolset if and only if the delegation allowlist is non-empty.
- The tier cap is a set, checked by membership (sandbox §4.2): the runtime MUST refuse to register a kind that declares a tool whose tier the cap excludes. A kind that states no cap has exactly the tiers its declared tools require.
- A sandboxed tool's name, tier, and input are the provider's (sandbox §3.2): a deployment MUST declare a sandboxed tool only as its provider answers it, and SHOULD register the provider's own declarations verbatim.
- The kind digest MUST cover the whole definition, each tool's tier, and the effective cap.

### 6.2 Cluster-wide agreement and the directory

A session MUST be resumable on any node hosting its kind, so every hosting node MUST register every kind, with the same definition. `SessionCreated` pins the session's kind digest, so a reader can tell whether a reconstruction (§10.4) is exact; the digest detects drift and authenticates nothing.

Building a node view MUST fail when a hosted kind's delegation allowlist names a kind outside the directory, and MUST fail when it hosts a kind the node has not registered, so a missing kind surfaces before any run, never mid-run.

## 7. Sessions across the cluster

A session's cluster life is the entity's lifecycle (entity §8); the runtime adds no placement, host, or resume protocol.

### 7.1 Placement, activation, and hibernation

The session activates on its shard leader at the first message, exactly once per node (E6; entity §5). Routing, including retries at a hinted leader, is the entity's (entity §5.4); the turn id makes a retried submit safe (§7.2).

- After an idle interval the leader MAY hibernate a session (entity §8), releasing the sandbox (A7).
- The leader MUST NOT hibernate a session whose run is live, whose turn queue is non-empty, or whose sandbox open is in flight (workflow §8, §7).

### 7.2 Submission and idempotency

A caller submits a turn with `Submit` (§11.1).

- The session MUST ack a new turn (`Started`) only once its `TurnSubmitted` commits (entity §6).
- The submit's kind MUST equal the addressed entity's type; the first accepted submit creates the session. The reply-to is transient routing, never journaled and outside the equality check.
- `RunCompleted` MUST reach each registered reply-to only after `RunEnded` commits, at most once (workflow §9); the record, not the notification, is the source of truth. After a resume, callers re-register by re-contacting the session.

Nothing a caller holds bounds the run: budgets bound tokens and steps, not wall time, and a run MAY outlive any caller, who recovers the outcome by re-submitting the turn id or reading the journal (§10.2). The runtime never re-issues an ambiguous failure (entity §1.1); the turn id makes the caller's re-submission safe:

- A submit whose turn id is already journaled MUST NOT start a second run (A6): a queued or live turn registers the new reply-to (`Attached`); an ended one answers `Ended` and at once tells the recorded outcome to the new reply-to, if any, in a `RunCompleted`, without registering it (workflow §9). No `Submit` reply carries an outcome. Deduplication reads only committed records, so it is exact across a resume.
- A re-submission racing the run's `RunEnded` is either registered on the live run or told the outcome, never neither or both (entity §6).
- The equality check compares content and literal budget with the journaled `TurnSubmitted`: a turn stating no budget differs from one stating the kind's default.
- A submit whose kind is not the entity's type (`SubmitReject::KindMismatch`), or a re-submission whose content or budget differs (`SubmitReject::ContentConflict`), MUST be rejected as a value, journaling nothing. The rejection is permanent; a caller MUST NOT retry it.

### 7.3 Resume

Every recovery, whether after a fault, a migration (storage §6.3), or a node failure, is the entity's rehydration (storage §9, entity §8). The next message activates the session, which rehydrates (rematerializing the workspace), folds, resolves orphaned calls (§5.4), and continues any unfinished run. No acknowledged record is lost (E14), and the resumed activation behaves identically to one that never stopped, given the same subsequent model and tool outcomes (A1).

Resumption is caller-driven: an interrupted run or a waiting turn resumes only when a message reaches the session, such as a re-submitted turn id, a parent's re-executed delegation, or a cancel (which also drives an owed propagation, §9.2). A run nobody asks after stays interrupted, its cost bounded by its budget. A2's liveness depends on this contact, as does A4's for a propagation a crash interrupted.

## 8. Delegation

### 8.1 Delegation is a tool

The built-in `delegate` takes a child kind from the parent kind's delegation allowlist (§5.2), a prompt, and optionally a budget. A delegation:

1. journals a `ChildRun`: the child kind, the carved budget (§9.1), and the child session id and turn id, derived deterministically from the parent session id, the parent's turn id, and the delegation's call id. Distinct triples MUST derive distinct ids, even from crafted ids, and every derived session id MUST begin with a decimal length followed by `:` (§11.3);
2. submits the child turn with the carved budget, the parent's lineage, and a reply-to, from the parent's node through its route for the child's kind, then awaits the child's `RunCompleted`, re-submitting periodically, for at most a deployment-chosen floor plus a deployment-chosen allowance per carved step. Past that wait, or after a bounded run of consecutive failures to reach the child, the call resolves as `ToolError::Delegation` and cancels nothing: the child runs on, bounded by its budget;
3. takes the child's run outcome as the call's outcome; a child's run error or rejection is a `ToolError::Delegation`.

The child is a full session, with its own entity, journal, budget, and shard. A delegation's orphan policy is reexecute: a resumed parent re-submits the journaled child turn, which the child treats as an attach (§7.2), so the outcome arrives whether the child is running or done.

### 8.2 Tree shape and failure

- The tree lives in the journals (`ChildRun`, and `SessionCreated`'s lineage), not in a replicated structure; no transaction spans entities (entity §1.1).
- A tree's spend is bounded by the root's budget whatever its shape (§9.1, A3).
- A child's failure is a tool outcome for the parent's model, never a supervision fault (actor §9).

## 9. Budgets and cancellation

### 9.1 Budgets

1. Every run has a budget: the turn's own, else the kind's default, resolved when `TurnSubmitted` commits. Spend MUST be a function of the journal alone, re-derived by replay on resume.
2. A model call is issued only while spend is below the token limit and the step count below the step limit, and only if the remaining tokens exceed a deployment-chosen floor (zero or more). Its output ceiling (§3.1) MUST be clamped to the remaining tokens, so output overshoot is zero and total overshoot at most one call's input.
3. A delegation carves the requested budget, else the child kind's default, clamped to the parent's remaining tokens and steps. The child enforces its carve-out as its own budget, and no accounting crosses shards: own spend plus carve-outs never exceeds the budget, at every node of the tree (A3).
4. The bound is on journaled usage: a response that never commits (from a leader deposed mid-step) goes uncounted though the provider bills it.

### 9.2 Cancellation

1. A `Cancel` names its run by turn id and takes effect at the next command boundary (entity §6). Naming an ended or unknown run MUST be a no-op, so a delayed cancel never ends a later run. Naming a still-queued turn MUST journal its `RunEnded` with `RunError::Cancelled` directly: the turn leaves the queue, its reply-tos are notified, and no run begins.
2. Cancelling the live run journals its `RunEnded` with `RunError::Cancelled`, notifies every registered reply-to, discards the run's later outcomes (§4.2), answers each call lacking an outcome with `ToolError::Interrupted` in the transcript (derived from that record, journaling nothing more), and owes a cancel to each child whose `ChildRun` lacks an outcome. Owed cancels derive from the journal alone, so a crash after the terminal commit loses none; each joins the desired set (§4.5) and is re-sent under the workflow's backoff (workflow §4.4) until the child acknowledges.
3. A child acknowledges a cancel only after its own commit; the parent then journals `CancelDelivered`, clearing that owed cancel. At the backoff cap an owed cancel rests and the parent MAY hibernate (workflow §7); the next message re-drives it. Once faults cease and a message reaches the session, propagation completes within bounded logical time (A4); under faults that never heal, the child's budget bounds the child.
4. In-flight effects of a cancelled run are not undone and MAY complete.

## 10. Observability

### 10.1 The journal is the record

The journal is the single source of truth for what a session did; every observation is a read of it. The runtime MUST NOT keep a second durable account of session activity.

### 10.2 Reading a run

Tail is a replication-free read of the leader's journal (entity §7.2). Follow yields the exact committed sequence after a cursor, or a truncation where compaction outran the cursor (entity §7.3).

- A read MUST NOT write, activate a hibernated session, or refresh an active one's idle clock.
- A page's sequence numbers are journal slots, with the workspace facet's records in the gaps (facets §5.1). Records below the compaction base are subsumed by the snapshot and not served.
- Reads are read-your-leader (entity §7.1): a deposed leader not yet fenced MAY serve a stale page, though writes never fork.

### 10.3 Tree correlation

Every `SessionCreated` records its root: its own session id if it has no parent, else the parent's root. With the parent links it names one delegation tree across shards and journals without a minted identifier; nothing routes or folds on it.

### 10.4 Reconstruction and metrics

Since requests are deterministic in committed state and the kind (§3.1), any step's exact request is reconstructible from the journal prefix, given the kind whose digest `SessionCreated` pins (§6.2). Aggregate metrics (spend per kind and per tree, run latency, tool failure rates) are RECOMMENDED, derived from records rather than instrumented separately.

## 11. Registries

### 11.1 Session messages

Every node MUST agree on these manifests.

| Message | Carries | Reply | Manifest |
|---|---|---|---|
| `Submit` | kind, turn, parent lineage, an optional reply-to | `Accepted` (the turn id and a status, one of `Started`, `Attached`, or `Ended`; §7.2), or a `SubmitReject` | `agent.Submit` |
| `Cancel` | turn id | empty | `agent.Cancel` |
| `TurnOutcome` | turn id | the recorded run outcome, or none | `agent.TurnOutcome` |
| `RunCompleted` | session id, turn id, run outcome | none: a tell to a reply-to | `agent.RunCompleted` |

### 11.2 Record kinds

| Record | Meaning |
|---|---|
| `SessionCreated` | Kind, kind digest, lineage, and the tree's root (§6.2, §10.3). Always first. |
| `TurnSubmitted` | A turn accepted into the queue: content, resolved budget, whether the budget was explicit (§7.2). |
| `TurnStarted` | The queue's head turn began its run (§4.2). |
| `ModelResponse` | One response: content, requested calls with their call ids, usage. |
| `TierAcquired` | The activation's first call at a tier is about to execute (§5.5). |
| `ToolOutcome` | One tool or delegation outcome, by call id. |
| `ChildRun` | A delegation's intent: child kind, session, turn, carved budget (§8.1). |
| `WorkspaceReset` | The environment the transcript asserts is gone (§5.4). |
| `RunEnded` | The turn's one run outcome (§4.2). |
| `CancelDelivered` | A propagated cancel reached a child (§9.2). |

### 11.3 Names

| Name | Value |
|---|---|
| Built-in delegation tool (§8.1) | `delegate` |
| Prefix of every derived session id (§8.1) | a decimal length followed by `:` |
| Workspace-reset notice (§3.1) | "[agent] The workspace was reset: files, processes, and servers from earlier in this conversation no longer exist. Re-derive anything you need before relying on it." |

## 12. Invariants

Only what is specific to the agent: the fence (E1) and the write-ahead obligation (E5) are wholly the entity's.

| # | Invariant | Defined in | Entity basis |
|---|---|---|---|
| A1 | **Deterministic fold and resume.** Session state is a pure fold of the journal; a session resumed from any committed prefix behaves identically to one that never stopped, given the same subsequent model and tool outcomes. | §4.1, §7.3 | E2, E3 |
| A2 | **Run termination.** Every run that starts (its `TurnStarted` commits) ends in exactly one `RunEnded`; once faults cease, partitions heal, and a message reaches the session, no run remains pending past its budget's bound. The record is the source of truth; a lost `RunCompleted` is recovered by re-contact, never retried. | §4.2, §7.3, §9 | E14 (lossless failover) |
| A3 | **Budget bound.** A run issues no model call after exhaustion; output spend never exceeds the remaining budget at call time; own spend plus carve-outs never exceeds the budget, at every level of a delegation tree. | §9.1 | (none) |
| A4 | **Cancellation.** After a cancel is journaled, the run and, once faults cease, every descendant run end `Cancelled` within bounded logical time, issuing no further model calls. | §9.2 | (none) |
| A5 | **Single autonomous activation.** A node never runs two concurrent activations of one session; on a converged cluster at most one is live; an addressed session activates within bounded logical time of a message reaching its shard leader. | §7.1, §7.3 | E6, storage §6 |
| A6 | **Idempotent submission.** A re-submitted turn id never starts a second run: it registers the caller's reply-to, or delivers the recorded outcome if the run ended, under any duplication or retry. | §7.2 | E1 (one commit) |
| A7 | **Effect containment.** An activation binds at most one live sandbox and releases it on deactivation; every sandboxed call executes in its issuing activation's sandbox, never another session's; sandbox loss surfaces in the journal (`ToolError` outcomes, `WorkspaceReset`), never as silent corruption. | §3.3, §5.4 | entity §5.2 (colocation) |

The sandbox's tier invariants are their own catalogue (sandbox §7).

## 13. Security considerations

The runtime runs inside the cluster's trust boundary (cluster §11); outside callers reach sessions only through the edge (edge §4.1). Model output and tool behavior are untrusted.

- **Model output reaches code only through the registry** (§5.2, §6.1).
- **Tools are confined to their session's sandbox** (§5.1), under the provider's tier obligations (sandbox §5).
- **Capabilities are granted only as declared and journaled** (§5.2, §5.5, §8.1; sandbox S4): a locked-down kind cannot escalate by delegating to a permissive one.
- **Peers cannot inject outcomes:** a session MUST accept only `agent.Submit`, `agent.Cancel`, and `agent.TurnOutcome` from the network (entity §5.5); the workflow commands carrying effects' outcomes are local-only.
- **Turn ids and digests authenticate nothing** (§6.2, §7.2): the equality check guards against accidental turn-id reuse, not a crafted collision, since whoever can address a session already holds it; isolating callers is the edge's (edge §4.3, §4.4).
