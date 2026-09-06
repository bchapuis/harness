import * as vscode from "vscode";
import type { GatewayConfig } from "./gatewayClient.js";

/** Where a token stored via `Agent: Set Gateway Token` lives. */
export const TOKEN_SECRET_KEY = "agent.token";

/**
 * Build a gateway config from settings, preferring a token held in
 * SecretStorage over the plaintext `agent.token` setting.
 */
export async function loadConfig(secrets: vscode.SecretStorage): Promise<GatewayConfig> {
  const cfg = vscode.workspace.getConfiguration("agent");
  const secretToken = await secrets.get(TOKEN_SECRET_KEY);
  return {
    baseUrl: cfg.get<string>("gatewayUrl", "http://127.0.0.1:8080"),
    token: secretToken ?? cfg.get<string>("token", "anonymous"),
    withinSecs: cfg.get<number>("withinSecs", 600),
  };
}

/** The session kind (entity type) to list and address. */
export function currentKind(): string {
  return vscode.workspace.getConfiguration("agent").get<string>("kind", "assistant");
}
