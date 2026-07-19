import { strict as assert } from "node:assert";
import { test } from "node:test";
import { outcomeToParts, recordToParts, runErrorMessage } from "../src/mapping.js";
import type { RecordBody } from "../src/types.js";

test("model response yields content markdown then tool progress", () => {
  const body: RecordBody = {
    ModelResponse: {
      turn: "t-1",
      content: "Working on it.",
      calls: [{ id: "c-1", name: "shell", input: { cmd: "ls" } }],
      usage: { input_tokens: 10, output_tokens: 5 },
    },
  };
  const parts = recordToParts(body);
  assert.deepEqual(parts[0], { role: "markdown", text: "Working on it." });
  assert.equal(parts[1].role, "progress");
  assert.match(parts[1].text, /shell/);
  assert.match(parts[1].text, /ls/);
});

test("empty model content is skipped, only tool calls render", () => {
  const body: RecordBody = {
    ModelResponse: { turn: "t-1", content: "   ", calls: [], usage: { input_tokens: 0, output_tokens: 0 } },
  };
  assert.deepEqual(recordToParts(body), []);
});

test("failed tool outcome is a warning note, not fatal", () => {
  const body: RecordBody = {
    ToolOutcome: { turn: "t-1", call: "c-1", outcome: { Err: "Timeout" } },
  };
  const parts = recordToParts(body);
  assert.equal(parts.length, 1);
  assert.equal(parts[0].role, "markdown");
  assert.match(parts[0].text, /timed out/);
});

test("successful tool outcome is a progress line", () => {
  const body: RecordBody = {
    ToolOutcome: { turn: "t-1", call: "c-2", outcome: { Ok: { ok: true } } },
  };
  assert.deepEqual(recordToParts(body), [{ role: "progress", text: "tool `c-2` ok" }]);
});

test("TurnSubmitted and RunEnded contribute no response parts", () => {
  const submitted: RecordBody = {
    TurnSubmitted: { turn: "t-1", content: "hello", budget: { tokens: 1000, steps: 8 } },
  };
  const ended: RecordBody = {
    RunEnded: { turn: "t-1", outcome: { Ok: { content: "done", tokens: 3 } } },
  };
  assert.deepEqual(recordToParts(submitted), []);
  assert.deepEqual(recordToParts(ended), []);
});

test("tier and workspace-reset are progress lines", () => {
  assert.deepEqual(recordToParts("WorkspaceReset"), [{ role: "progress", text: "workspace reset" }]);
  assert.deepEqual(recordToParts({ TierAcquired: { turn: "t-1", tier: "Network" } }), [
    { role: "progress", text: "acquired network tier" },
  ]);
});

test("Ok outcome reports token count; Err outcome renders a failure note", () => {
  assert.deepEqual(outcomeToParts({ Ok: { content: "x", tokens: 42 } }), [
    { role: "progress", text: "done · 42 tokens" },
  ]);
  const err = outcomeToParts({ Err: "BudgetExhausted" });
  assert.equal(err[0].role, "markdown");
  assert.match(err[0].text, /budget exhausted/);
});

test("runErrorMessage maps each RunError variant", () => {
  assert.equal(runErrorMessage("Cancelled"), "cancelled");
  assert.equal(runErrorMessage("BudgetExhausted"), "budget exhausted");
  assert.match(runErrorMessage({ Model: "rate limited" }), /model failure: rate limited/);
});
