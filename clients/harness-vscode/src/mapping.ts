// Translate gateway records and run outcomes into an intermediate list of
// render parts. Kept free of any `vscode` import so it is unit-testable and so
// both the live-stream path and the history-replay path in `contentProvider.ts`
// can consume the same mapping. The provider turns these parts into VS Code
// chat response parts (markdown / progress).

import type { RecordBody, RunError, RunOutcome, ToolCall, ToolError } from "./types.js";

/** A neutral rendered fragment: prose (`markdown`) or a status line (`progress`). */
export type RenderPart =
  | { role: "markdown"; text: string }
  | { role: "progress"; text: string };

/**
 * A record's contribution to the *current response*. `TurnSubmitted` is not
 * rendered here — it starts a user turn, handled at the transcript level — and
 * neither is the terminal `RunEnded`, whose outcome is rendered by
 * `outcomeToParts` (the same value the SSE `outcome` event carries).
 */
export function recordToParts(body: RecordBody): RenderPart[] {
  if (body === "WorkspaceReset") {
    return [{ role: "progress", text: "workspace reset" }];
  }
  if ("SessionCreated" in body) {
    return [{ role: "progress", text: `session created (${body.SessionCreated.kind})` }];
  }
  if ("TurnSubmitted" in body) {
    return []; // A user turn boundary, not response content.
  }
  if ("ModelResponse" in body) {
    const parts: RenderPart[] = [];
    const { content, calls } = body.ModelResponse;
    if (content.trim().length > 0) {
      parts.push({ role: "markdown", text: content });
    }
    for (const call of calls) {
      parts.push({ role: "progress", text: describeToolCall(call) });
    }
    return parts;
  }
  if ("ToolOutcome" in body) {
    const { call, outcome } = body.ToolOutcome;
    if ("Err" in outcome) {
      // A failing tool never fails the run (harness spec §5.4) — surface it as a
      // note, not an error.
      return [{ role: "markdown", text: `⚠️ tool \`${call}\` failed: ${describeToolError(outcome.Err)}` }];
    }
    return [{ role: "progress", text: `tool \`${call}\` ok` }];
  }
  if ("ChildRun" in body) {
    const { child_kind, child_session } = body.ChildRun;
    return [{ role: "progress", text: `delegated to ${child_kind}/${child_session}` }];
  }
  if ("TierAcquired" in body) {
    return [{ role: "progress", text: `acquired ${body.TierAcquired.tier.toLowerCase()} tier` }];
  }
  if ("RunEnded" in body) {
    return []; // Rendered via outcomeToParts.
  }
  return [];
}

/**
 * The terminal outcome. `Ok` needs no prose (the content was streamed as
 * `ModelResponse` records); `Err` is rendered as a note. Errors are also
 * reported to VS Code as `ChatResult.errorDetails` by the caller.
 */
export function outcomeToParts(outcome: RunOutcome): RenderPart[] {
  if ("Ok" in outcome) {
    return [{ role: "progress", text: `done · ${outcome.Ok.tokens} tokens` }];
  }
  return [{ role: "markdown", text: `**Run failed:** ${describeRunError(outcome.Err)}` }];
}

/** A one-line, user-facing reason for a failed run (for `errorDetails`). */
export function runErrorMessage(err: RunError): string {
  return describeRunError(err);
}

function describeToolCall(call: ToolCall): string {
  const input = summarizeInput(call.input);
  return input ? `tool \`${call.name}\` ${input}` : `tool \`${call.name}\``;
}

function summarizeInput(input: unknown): string {
  if (input === null || input === undefined) {
    return "";
  }
  let text: string;
  try {
    text = typeof input === "string" ? input : JSON.stringify(input);
  } catch {
    return "";
  }
  return text.length > 120 ? `${text.slice(0, 117)}…` : text;
}

function describeToolError(err: ToolError): string {
  if (err === "Timeout") {
    return "timed out";
  }
  if (err === "Interrupted") {
    return "interrupted";
  }
  if ("UnknownTool" in err) {
    return `unknown tool ${err.UnknownTool.name}`;
  }
  if ("InvalidArguments" in err) {
    return `invalid arguments: ${err.InvalidArguments}`;
  }
  if ("Sandbox" in err) {
    return `sandbox: ${err.Sandbox}`;
  }
  if ("EnvironmentLost" in err) {
    return `environment lost: ${err.EnvironmentLost}`;
  }
  return `delegation: ${err.Delegation}`;
}

function describeRunError(err: RunError): string {
  if (err === "BudgetExhausted") {
    return "budget exhausted";
  }
  if (err === "Cancelled") {
    return "cancelled";
  }
  return `model failure${describeModel(err.Model)}`;
}

function describeModel(model: unknown): string {
  if (typeof model === "string") {
    return `: ${model}`;
  }
  if (model && typeof model === "object") {
    try {
      return `: ${JSON.stringify(model)}`;
    } catch {
      return "";
    }
  }
  return "";
}
