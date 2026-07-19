import * as vscode from "vscode";
import { GatewayError, type GatewayClient } from "./gatewayClient.js";
import { outcomeToParts, recordToParts, runErrorMessage, type RenderPart } from "./mapping.js";
import type { Record as GwRecord, RunOutcome, SeqRecord } from "./types.js";

const PARTICIPANT = "harness";

/**
 * Resolves a session id to its content: the prior transcript (replayed from the
 * gateway journal) plus the handler that services new turns by streaming the
 * gateway's SSE run into the chat view. Cancellation aborts the stream and asks
 * the gateway to cancel the turn.
 */
export class HarnessSessionContentProvider implements vscode.ChatSessionContentProvider {
  /** Highest record seq seen per session — the exclusive resume cursor. */
  private readonly lastSeq = new Map<string, number>();

  constructor(
    private readonly makeClient: () => Promise<GatewayClient>,
    private readonly getKind: () => string
  ) {}

  async provideChatSessionContent(
    sessionId: string,
    _token: vscode.CancellationToken
  ): Promise<vscode.ChatSession> {
    const client = await this.makeClient();
    const kind = this.getKind();
    let records: SeqRecord[] = [];
    try {
      records = await client.records(kind, sessionId, 0, 1000);
    } catch (err) {
      // A brand-new session has no journal yet; start empty.
      if (!(err instanceof GatewayError)) {
        throw err;
      }
    }
    this.lastSeq.set(sessionId, highestSeq(records));
    return {
      history: buildHistory(records),
      requestHandler: this.makeHandler(kind, sessionId),
    };
  }

  private makeHandler(kind: string, sessionId: string): vscode.ChatRequestHandler {
    return async (request, _context, response, token): Promise<vscode.ChatResult> => {
      const client = await this.makeClient();
      const turn = client.newTurnId();
      const from = this.lastSeq.get(sessionId) ?? 0;
      const controller = new AbortController();
      const cancel = token.onCancellationRequested(() => {
        controller.abort();
        client.cancel(kind, sessionId, turn).catch(() => undefined);
      });
      try {
        for await (const event of client.prompt(kind, sessionId, turn, request.prompt, from, controller.signal)) {
          if (event.kind === "records") {
            for (const [seq, record] of event.records) {
              this.bump(sessionId, seq);
              renderToStream(recordToParts(record.body), response);
            }
          } else if (event.kind === "outcome") {
            renderToStream(outcomeToParts(event.outcome), response);
            return outcomeResult(event.outcome);
          } else if (event.kind === "error") {
            return { errorDetails: { message: `${event.error.code}: ${event.error.message}` } };
          }
        }
        return {};
      } catch (err) {
        if (controller.signal.aborted) {
          return { errorDetails: { message: "Cancelled" } };
        }
        const message = err instanceof GatewayError ? `${err.code}: ${err.message}` : describe(err);
        return { errorDetails: { message } };
      } finally {
        cancel.dispose();
      }
    };
  }

  private bump(sessionId: string, seq: number): void {
    this.lastSeq.set(sessionId, Math.max(this.lastSeq.get(sessionId) ?? 0, seq));
  }
}

function outcomeResult(outcome: RunOutcome): vscode.ChatResult {
  return "Err" in outcome ? { errorDetails: { message: runErrorMessage(outcome.Err) } } : {};
}

function highestSeq(records: SeqRecord[]): number {
  let last = 0;
  for (const [seq] of records) {
    last = Math.max(last, seq);
  }
  return last;
}

/** Apply render parts to a live response stream. */
function renderToStream(parts: RenderPart[], response: vscode.ChatResponseStream): void {
  for (const part of parts) {
    if (part.role === "markdown") {
      response.markdown(`${part.text}\n\n`);
    } else {
      response.progress(part.text);
    }
  }
}

/**
 * Rebuild the chat transcript from the journal: each `TurnSubmitted` opens a
 * user turn; the response records up to the next turn form the assistant turn.
 *
 * `ChatRequestTurn`/`ChatResponseTurn` construction is part of the *proposed*
 * chat-sessions API and its shape is still moving, so we construct through a
 * guarded `any` and fall back to an empty transcript where the runtime does not
 * expose the constructors. New turns always render live regardless.
 */
function buildHistory(records: SeqRecord[]): Array<vscode.ChatRequestTurn | vscode.ChatResponseTurn> {
  const api = vscode as unknown as {
    ChatRequestTurn?: new (prompt: string, command: string | undefined, references: unknown[], participant: string) => vscode.ChatRequestTurn;
    ChatResponseTurn?: new (response: unknown[], result: vscode.ChatResult, participant: string) => vscode.ChatResponseTurn;
    ChatResponseMarkdownPart?: new (value: string) => unknown;
  };
  if (!api.ChatRequestTurn || !api.ChatResponseTurn || !api.ChatResponseMarkdownPart) {
    return [];
  }
  const MarkdownPart = api.ChatResponseMarkdownPart;
  try {
    const turns: Array<vscode.ChatRequestTurn | vscode.ChatResponseTurn> = [];
    let responseParts: unknown[] = [];
    let result: vscode.ChatResult = {};
    let open = false;

    const flush = () => {
      if (open) {
        turns.push(new api.ChatResponseTurn!(responseParts, result, PARTICIPANT));
        responseParts = [];
        result = {};
        open = false;
      }
    };

    for (const [, record] of records) {
      const body = (record as GwRecord).body;
      if (typeof body === "object" && "TurnSubmitted" in body) {
        flush();
        turns.push(new api.ChatRequestTurn!(body.TurnSubmitted.content, undefined, [], PARTICIPANT));
        open = true;
        continue;
      }
      const parts =
        typeof body === "object" && "RunEnded" in body
          ? outcomeToParts(body.RunEnded.outcome)
          : recordToParts(body);
      for (const part of parts) {
        responseParts.push(new MarkdownPart(part.role === "markdown" ? `${part.text}\n\n` : `_${part.text}_\n\n`));
      }
      if (typeof body === "object" && "RunEnded" in body) {
        result = outcomeResult(body.RunEnded.outcome);
      }
    }
    flush();
    return turns;
  } catch (err) {
    console.warn(`harness: could not rebuild history — ${describe(err)}`);
    return [];
  }
}

function describe(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
