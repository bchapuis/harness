// A streaming Server-Sent-Events parser for the gateway's stream, ported from
// the TUI reference client (`crates/harness-tui/src/client.rs`, `parse_frame`).
// Frames are separated by a blank line (`\n\n`); within a frame, `event:`,
// `data:`, and `id:` lines are recognized. Multiple `data:` lines join with a
// newline. A single leading space after the colon is stripped, per the SSE
// spec. We recognize the gateway's four event names and drop anything else.

import type { GatewayEvent, GatewayErrorBody, RunOutcome, SeqRecord } from "./types.js";

export class SseParser {
  private buffer = "";

  /** Feed a decoded text chunk; return any complete events it produced. */
  push(chunk: string): GatewayEvent[] {
    // Normalize CRLF so `\n\n` framing holds regardless of line endings.
    this.buffer += chunk.replace(/\r\n/g, "\n");
    const events: GatewayEvent[] = [];
    let sep: number;
    while ((sep = this.buffer.indexOf("\n\n")) !== -1) {
      const frame = this.buffer.slice(0, sep);
      this.buffer = this.buffer.slice(sep + 2);
      const event = parseFrame(frame);
      if (event) {
        events.push(event);
      }
    }
    return events;
  }
}

/** Parse one raw frame (without its trailing blank line) into an event. */
export function parseFrame(frame: string): GatewayEvent | undefined {
  let name = "message";
  let id: number | undefined;
  const dataLines: string[] = [];
  for (const line of frame.split("\n")) {
    if (line.startsWith(":")) {
      continue; // SSE comment (keep-alive ping).
    }
    const colon = line.indexOf(":");
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) {
      value = value.slice(1);
    }
    switch (field) {
      case "event":
        name = value.trim();
        break;
      case "data":
        dataLines.push(value);
        break;
      case "id": {
        const n = Number(value.trim());
        id = Number.isFinite(n) ? n : undefined;
        break;
      }
      default:
        break;
    }
  }
  const data = dataLines.join("\n");
  switch (name) {
    case "records": {
      const records = tryJson<SeqRecord[]>(data);
      return records ? { kind: "records", seq: id, records } : undefined;
    }
    case "outcome": {
      const outcome = tryJson<RunOutcome>(data);
      return outcome ? { kind: "outcome", outcome } : undefined;
    }
    case "error": {
      const parsed = tryJson<GatewayErrorBody>(data);
      const error: GatewayErrorBody =
        parsed && typeof parsed.code === "string"
          ? parsed
          : { code: "error", message: data };
      return { kind: "error", error };
    }
    case "end":
      return { kind: "end" };
    default:
      return undefined;
  }
}

function tryJson<T>(text: string): T | undefined {
  try {
    return JSON.parse(text) as T;
  } catch {
    return undefined;
  }
}
