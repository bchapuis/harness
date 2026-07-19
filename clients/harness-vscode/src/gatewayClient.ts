// A dependency-free client for the harness gateway's HTTP/SSE edge
// (`crates/harness-gateway/src/http.rs`). It mirrors the TUI reference client
// (`crates/harness-tui/src/client.rs`): a bearer token on every request, JSON
// bodies, and SSE for live runs. Uses the Node 18+ global `fetch` /
// `AbortController` that VS Code ships — no extra HTTP dependency.

import { SseParser } from "./sse.js";
import type { GatewayErrorBody, GatewayEvent, RunOutcome, SeqRecord, SessionListEntry } from "./types.js";

export interface GatewayConfig {
  /** Base URL, e.g. `http://127.0.0.1:8080`. */
  baseUrl: string;
  /** Bearer token; in the loopback dev gateway this IS the tenant. */
  token: string;
  /** Per-prompt wait ceiling (seconds); the gateway clamps to its own max. */
  withinSecs: number;
}

/** A transport/durability failure surfaced by the gateway (HTTP or SSE layer). */
export class GatewayError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly status?: number
  ) {
    super(message);
    this.name = "GatewayError";
  }
}

export class GatewayClient {
  private turnCounter = 0;
  private readonly nonce = Math.floor(Date.now() % 1_000_000).toString(36);

  constructor(private readonly config: GatewayConfig) {}

  /** A fresh idempotency key for a turn, in the TUI's `t-<nonce>-<n>` shape. */
  newTurnId(): string {
    this.turnCounter += 1;
    return `t-${this.nonce}-${this.turnCounter}`;
  }

  /** List this tenant's sessions of the given kind. */
  async listSessions(kind: string): Promise<SessionListEntry[]> {
    const res = await fetch(this.url(`/v1/sessions?kind=${encode(kind)}`), {
      headers: this.headers(),
    });
    await this.throwIfError(res);
    const body = (await res.json()) as { sessions?: SessionListEntry[] };
    return body.sessions ?? [];
  }

  /** Read a page of committed records (used to rebuild chat history). */
  async records(kind: string, session: string, from = 0, limit = 500): Promise<SeqRecord[]> {
    const path = `/v1/${encode(kind)}/${encode(session)}/records?from=${from}&limit=${limit}`;
    const res = await fetch(this.url(path), { headers: this.headers() });
    await this.throwIfError(res);
    const body = (await res.json()) as { records?: SeqRecord[] };
    return body.records ?? [];
  }

  /**
   * Submit a turn and stream the run as SSE events, resuming record delivery
   * from `from`. Yields `records`/`outcome`/`error` events; the generator ends
   * after the terminal `outcome` (or `error`).
   */
  async *prompt(
    kind: string,
    session: string,
    turn: string,
    content: string,
    from: number,
    signal: AbortSignal
  ): AsyncGenerator<GatewayEvent> {
    const path = `/v1/${encode(kind)}/${encode(session)}/prompt?from=${from}`;
    const body = JSON.stringify({ turn, content, within_secs: this.config.withinSecs });
    const res = await fetch(this.url(path), {
      method: "POST",
      headers: this.headers({ "content-type": "application/json", accept: "text/event-stream" }),
      body,
      signal,
    });
    await this.throwIfError(res);
    yield* this.readSse(res, signal);
  }

  /** Observe an in-flight or past run live (no new turn), from `from`. */
  async *stream(
    kind: string,
    session: string,
    from: number,
    signal: AbortSignal
  ): AsyncGenerator<GatewayEvent> {
    const path = `/v1/${encode(kind)}/${encode(session)}/stream?from=${from}`;
    const res = await fetch(this.url(path), {
      headers: this.headers({ accept: "text/event-stream" }),
      signal,
    });
    await this.throwIfError(res);
    yield* this.readSse(res, signal);
  }

  /** Cancel a run by its turn id (idempotent). */
  async cancel(kind: string, session: string, turn: string): Promise<void> {
    const path = `/v1/${encode(kind)}/${encode(session)}/cancel?turn=${encode(turn)}`;
    const res = await fetch(this.url(path), { method: "POST", headers: this.headers() });
    await this.throwIfError(res);
  }

  /** Blocking prompt (no SSE): returns the terminal outcome. */
  async promptUnary(
    kind: string,
    session: string,
    turn: string,
    content: string
  ): Promise<RunOutcome> {
    const path = `/v1/${encode(kind)}/${encode(session)}/prompt`;
    const body = JSON.stringify({ turn, content, within_secs: this.config.withinSecs });
    const res = await fetch(this.url(path), {
      method: "POST",
      headers: this.headers({ "content-type": "application/json" }),
      body,
    });
    await this.throwIfError(res);
    const parsed = (await res.json()) as { outcome: RunOutcome };
    return parsed.outcome;
  }

  private async *readSse(res: Response, signal: AbortSignal): AsyncGenerator<GatewayEvent> {
    if (!res.body) {
      throw new GatewayError("upstream", "gateway returned an empty stream body", res.status);
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    const parser = new SseParser();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }
        for (const event of parser.push(decoder.decode(value, { stream: true }))) {
          yield event;
          if (event.kind === "outcome" || event.kind === "error") {
            return;
          }
        }
      }
    } finally {
      // Abort mid-stream (e.g. the turn was cancelled) frees the socket.
      if (signal.aborted) {
        await reader.cancel().catch(() => undefined);
      }
    }
  }

  private headers(extra: Record<string, string> = {}): Record<string, string> {
    return { authorization: `Bearer ${this.config.token}`, ...extra };
  }

  private url(path: string): string {
    return `${this.config.baseUrl.replace(/\/+$/, "")}${path}`;
  }

  /** Raise a `GatewayError` from a non-2xx response's `{error:{code,message}}`. */
  private async throwIfError(res: Response): Promise<void> {
    if (res.ok) {
      return;
    }
    let code = "upstream";
    let message = `gateway returned ${res.status}`;
    try {
      const body = (await res.json()) as { error?: GatewayErrorBody };
      if (body.error?.code) {
        code = body.error.code;
        message = body.error.message ?? message;
      }
    } catch {
      // Non-JSON body; keep the status-derived message.
    }
    throw new GatewayError(code, message, res.status);
  }
}

function encode(segment: string): string {
  return encodeURIComponent(segment);
}
