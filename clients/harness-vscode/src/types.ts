// TypeScript mirrors of the gateway's serde types. These match the JSON the
// gateway emits, which is `serde_json`'s default (externally-tagged) encoding
// of the Rust types in `crates/harness/src/session.rs`, `model.rs`, `budget.rs`,
// `tool.rs`, and `sandbox.rs`. Newtypes such as `Seq(u64)`, `TurnId(String)`,
// and `CallId(String)` serialize transparently as their inner value.

/** `crates/harness/src/budget.rs` — Budget { tokens, steps }. */
export interface Budget {
  tokens: number;
  steps: number;
}

/** `crates/harness/src/budget.rs` — Usage { input_tokens, output_tokens }. */
export interface Usage {
  input_tokens: number;
  output_tokens: number;
}

/** `crates/harness/src/model.rs` — ToolCall { id, name, input }. */
export interface ToolCall {
  id: string;
  name: string;
  input: unknown;
}

/** `crates/harness/src/sandbox.rs` — Tier (unit enum → string). */
export type Tier = "Workspace" | "Compute" | "Network" | "Native";

/**
 * `crates/harness/src/tool.rs` — ToolError (externally tagged). Unit variants
 * are bare strings; data-carrying variants are single-key objects.
 */
export type ToolError =
  | "Timeout"
  | "Interrupted"
  | { UnknownTool: { name: string } }
  | { InvalidArguments: string }
  | { Sandbox: string }
  | { EnvironmentLost: string }
  | { Delegation: string };

/** A `Result<T, E>` as serde encodes it: `{ Ok }` or `{ Err }`. */
export type Result<T, E> = { Ok: T } | { Err: E };

/** `crates/harness/src/model.rs` — ModelError, opaque here. */
export type ModelError = unknown;

/** `crates/harness/src/session.rs` — RunError (externally tagged). */
export type RunError =
  | "BudgetExhausted"
  | "Cancelled"
  | { Model: ModelError };

/** `crates/harness/src/session.rs` — Completion { content, tokens }. */
export interface Completion {
  content: string;
  tokens: number;
}

/** `crates/harness/src/session.rs` — RunOutcome = Result<Completion, RunError>. */
export type RunOutcome = Result<Completion, RunError>;

/**
 * `crates/harness/src/session.rs` — RecordBody (externally tagged). Only the
 * fields this client reads are typed; unmentioned fields are ignored.
 */
export type RecordBody =
  | { SessionCreated: { kind: string; digest: number; root: string } }
  | { TurnSubmitted: { turn: string; content: string; budget: Budget } }
  | { ModelResponse: { turn: string; content: string; calls: ToolCall[]; usage: Usage } }
  | { ToolOutcome: { turn: string; call: string; outcome: Result<unknown, ToolError> } }
  | { ChildRun: { turn: string; call: string; child_kind: string; child_session: string } }
  | { TierAcquired: { turn: string; tier: Tier } }
  | { RunEnded: { turn: string; outcome: RunOutcome } }
  | "WorkspaceReset";

/** `crates/harness/src/session.rs` — Record { at_nanos, body }. */
export interface Record {
  at_nanos: number;
  body: RecordBody;
}

/** One `records` batch entry: `[seq, Record]`. */
export type SeqRecord = [number, Record];

/** The gateway's structured error envelope (transport layer). */
export interface GatewayErrorBody {
  code: string;
  message: string;
}

/**
 * A parsed SSE frame from the gateway. `records`/`outcome`/`error`/`end`
 * mirror `crates/harness-gateway/src/http.rs`.
 */
export type GatewayEvent =
  | { kind: "records"; seq: number | undefined; records: SeqRecord[] }
  | { kind: "outcome"; outcome: RunOutcome }
  | { kind: "error"; error: GatewayErrorBody }
  | { kind: "end" };

/** Entry of `GET /v1/sessions`. */
export interface SessionListEntry {
  session: string;
  label?: string;
}
