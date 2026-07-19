// Hand-pinned subset of VS Code's PROPOSED `chatSessionsProvider` API.
//
// This is NOT the verbatim upstream file. The real proposed API is in flux —
// it is mid-migration from a provider model to a controller model
// (`createChatSessionItemController`, microsoft/vscode#288457) — so we declare
// only the symbols this extension actually calls, and augment the stable
// `vscode` module with them. When you target a specific VS Code build, replace
// this file with the exact upstream `vscode.proposed.chatSessionsProvider.d.ts`
// for that build (`npx @vscode/dts dev`) and reconcile the two provider methods
// below against it.
//
// The request handler, response stream, and request/response turn types this
// builds on are STABLE (chat participant API, VS Code 1.90+) and come from
// `@types/vscode`; only the pieces below are proposed.

declare module "vscode" {
  /** One selectable session in the Agents window / chat session picker. */
  export interface ChatSessionItem {
    /** Stable id used to address the session's content. */
    readonly id: string;
    /** Human-readable label shown in the picker. */
    label: string;
    /** Optional icon. */
    iconPath?: IconPath;
    /** Optional hover text. */
    tooltip?: string | MarkdownString;
  }

  /** Supplies the list of sessions for a contributed chat session type. */
  export interface ChatSessionItemProvider {
    /** Fires when the set of items changes (e.g. a session was created). */
    readonly onDidChangeChatSessionItems?: Event<void>;
    /** Enumerate the available sessions. */
    provideChatSessionItems(token: CancellationToken): ProviderResult<ChatSessionItem[]>;
  }

  /** Capabilities a session type advertises to the host. */
  export interface ChatSessionCapabilities {
    /** The agent can be interrupted mid-response (maps to cancel). */
    supportsInterruptions?: boolean;
  }

  /**
   * The resolved content of one session: its prior turns plus the handler that
   * services new requests. `requestHandler` reuses the stable
   * `ChatRequestHandler` shape from the chat participant API.
   */
  export interface ChatSession {
    /** Prior turns to replay into the view when the session is opened. */
    readonly history: ReadonlyArray<ChatRequestTurn | ChatResponseTurn>;
    /** Services a new request in this session. */
    readonly requestHandler: ChatRequestHandler;
    /**
     * Invoked when the session is opened with a run already in flight, to
     * stream its remaining output into the view. Optional.
     */
    readonly activeResponseCallback?: (
      stream: ChatResponseStream,
      token: CancellationToken
    ) => Thenable<void>;
  }

  /** Resolves a `ChatSessionItem` to its content. */
  export interface ChatSessionContentProvider {
    provideChatSessionContent(
      sessionId: string,
      token: CancellationToken
    ): ProviderResult<ChatSession>;
  }

  export namespace chat {
    export function registerChatSessionItemProvider(
      chatSessionType: string,
      provider: ChatSessionItemProvider
    ): Disposable;

    export function registerChatSessionContentProvider(
      chatSessionType: string,
      provider: ChatSessionContentProvider,
      capabilities?: ChatSessionCapabilities
    ): Disposable;
  }
}
