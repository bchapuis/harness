import * as vscode from "vscode";
import type { GatewayClient } from "./gatewayClient.js";

/**
 * Populates the Agents window's list of harness sessions. It merges the
 * gateway's own session list (`GET /v1/sessions`) with locally-created names
 * that have not yet been prompted — the gateway only lists a session once it
 * has records, so a freshly-named session would otherwise be invisible until
 * its first turn.
 */
export class HarnessSessionItemProvider implements vscode.ChatSessionItemProvider {
  private readonly emitter = new vscode.EventEmitter<void>();
  readonly onDidChangeChatSessionItems = this.emitter.event;
  private readonly pending = new Set<string>();

  constructor(
    private readonly makeClient: () => Promise<GatewayClient>,
    private readonly getKind: () => string
  ) {}

  /** Register a locally-created session name and reveal it in the list. */
  addPending(name: string): void {
    this.pending.add(name);
    this.refresh();
  }

  /** Re-query the gateway (e.g. after a settings change). */
  refresh(): void {
    this.emitter.fire();
  }

  async provideChatSessionItems(_token: vscode.CancellationToken): Promise<vscode.ChatSessionItem[]> {
    const names = new Set(this.pending);
    try {
      const client = await this.makeClient();
      for (const entry of await client.listSessions(this.getKind())) {
        names.add(entry.session);
      }
    } catch (err) {
      // Surface once but still show any pending names so the user isn't stuck.
      vscode.window.showWarningMessage(`Harness: could not list sessions — ${describe(err)}`);
    }
    return [...names].sort().map((session) => ({ id: session, label: session }));
  }

  dispose(): void {
    this.emitter.dispose();
  }
}

function describe(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
