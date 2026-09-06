import * as vscode from "vscode";
import { currentKind, loadConfig, TOKEN_SECRET_KEY } from "./config.js";
import { GatewayClient } from "./gatewayClient.js";
import { AgentSessionContentProvider } from "./contentProvider.js";
import { AgentSessionItemProvider } from "./sessionProvider.js";

const SESSION_TYPE = "agent";

export function activate(context: vscode.ExtensionContext): void {
  const makeClient = async () => new GatewayClient(await loadConfig(context.secrets));
  const getKind = () => currentKind();

  const items = new AgentSessionItemProvider(makeClient, getKind);
  const content = new AgentSessionContentProvider(makeClient, getKind);

  context.subscriptions.push(
    items,
    vscode.chat.registerChatSessionItemProvider(SESSION_TYPE, items),
    vscode.chat.registerChatSessionContentProvider(SESSION_TYPE, content, {
      supportsInterruptions: true,
    }),
    vscode.commands.registerCommand("agent.newSession", () => newSession(items)),
    vscode.commands.registerCommand("agent.setToken", () => setToken(context.secrets, items)),
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration("agent")) {
        items.refresh();
      }
    })
  );
}

export function deactivate(): void {
  // Disposables are registered on context.subscriptions; nothing extra to do.
}

async function newSession(items: AgentSessionItemProvider): Promise<void> {
  const name = await vscode.window.showInputBox({
    title: "New Agent Session",
    prompt: "Name for the new session (created on its first prompt)",
    placeHolder: "e.g. demo",
    validateInput: (value) =>
      /^[^/\s]+$/.test(value.trim()) ? undefined : "Use a non-empty name without spaces or '/'.",
  });
  const trimmed = name?.trim();
  if (!trimmed) {
    return;
  }
  items.addPending(trimmed);
  vscode.window.showInformationMessage(
    `Agent: session "${trimmed}" ready — pick it in the Agents window to start.`
  );
}

async function setToken(secrets: vscode.SecretStorage, items: AgentSessionItemProvider): Promise<void> {
  const token = await vscode.window.showInputBox({
    title: "Agent Gateway Token",
    prompt: "Bearer token sent to the gateway (stored in SecretStorage). Leave empty to clear.",
    password: true,
  });
  if (token === undefined) {
    return;
  }
  if (token.trim().length === 0) {
    await secrets.delete(TOKEN_SECRET_KEY);
  } else {
    await secrets.store(TOKEN_SECRET_KEY, token.trim());
  }
  items.refresh();
}
