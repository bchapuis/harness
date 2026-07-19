import { strict as assert } from "node:assert";
import { test } from "node:test";
import { parseFrame, SseParser } from "../src/sse.js";

test("parses a records frame with an id", () => {
  const frame = 'event: records\nid: 42\ndata: [[42,{"at_nanos":0,"body":"WorkspaceReset"}]]';
  const event = parseFrame(frame);
  assert.deepEqual(event, {
    kind: "records",
    seq: 42,
    records: [[42, { at_nanos: 0, body: "WorkspaceReset" }]],
  });
});

test("parses an Ok outcome frame", () => {
  const event = parseFrame('event: outcome\ndata: {"Ok":{"content":"hi","tokens":7}}');
  assert.deepEqual(event, { kind: "outcome", outcome: { Ok: { content: "hi", tokens: 7 } } });
});

test("parses a bare error frame", () => {
  const event = parseFrame('event: error\ndata: {"code":"timeout","message":"deadline exceeded"}');
  assert.deepEqual(event, { kind: "error", error: { code: "timeout", message: "deadline exceeded" } });
});

test("wraps non-JSON error data", () => {
  const event = parseFrame("event: error\ndata: boom");
  assert.deepEqual(event, { kind: "error", error: { code: "error", message: "boom" } });
});

test("parses an end frame with empty data", () => {
  assert.deepEqual(parseFrame("event: end\ndata: "), { kind: "end" });
});

test("skips comment (keep-alive) lines and unknown events", () => {
  assert.equal(parseFrame(": keep-alive"), undefined);
  assert.equal(parseFrame("event: message\ndata: whatever"), undefined);
});

test("joins multiple data lines with newlines", () => {
  const event = parseFrame('event: error\ndata: line1\ndata: line2');
  assert.deepEqual(event, { kind: "error", error: { code: "error", message: "line1\nline2" } });
});

test("streams frames split across chunk boundaries", () => {
  const parser = new SseParser();
  assert.deepEqual(parser.push("event: records\nid: 1\ndata: [[1,"), []);
  const events = parser.push('{"at_nanos":0,"body":"WorkspaceReset"}]]\n\nevent: end\ndata: \n\n');
  assert.equal(events.length, 2);
  assert.equal(events[0].kind, "records");
  assert.equal(events[1].kind, "end");
});

test("normalizes CRLF line endings", () => {
  const parser = new SseParser();
  const events = parser.push('event: outcome\r\ndata: {"Ok":{"content":"x","tokens":1}}\r\n\r\n');
  assert.equal(events.length, 1);
  assert.equal(events[0].kind, "outcome");
});
