# harness-vscode

A VS Code extension that surfaces a **harness session** as an agent in the
**Agents window**, driven over the gateway's HTTP/SSE edge
(`crates/harness-gateway`). Pick a session, send a prompt, and watch the run —
model responses, tool calls, tier acquisitions, and the terminal outcome —
stream in live. Cancel interrupts the turn.

It talks directly to the gateway's existing REST + Server-Sent-Events endpoints
(the same protocol the Rust TUI in `crates/harness-tui` uses). There is no ACP
wire protocol on either side: the gateway is *ACP-aligned* in its semantics
(transport errors vs. in-run outcomes kept distinct), and this extension maps
those semantics onto VS Code's chat model.

## Layout

| File | Role |
| --- | --- |
| `src/gatewayClient.ts` | HTTP/SSE client (`fetch` + `AbortController`, no deps) |
| `src/sse.ts` | Streaming SSE frame parser (port of `client.rs`'s `parse_frame`) |
| `src/types.ts` | TS mirrors of the gateway's serde types |
| `src/mapping.ts` | Records / outcomes → neutral render parts (pure, unit-tested) |
| `src/sessionProvider.ts` | `ChatSessionItemProvider` — lists sessions |
| `src/contentProvider.ts` | `ChatSessionContentProvider` — history + live request handler |
| `src/extension.ts` | `activate()` — registers providers, commands, config |
| `vscode.proposed.chatSessionsProvider.d.ts` | Hand-pinned subset of the proposed API |

## Build

```bash
cd clients/harness-vscode
npm install
npm run build       # bundle to dist/extension.js
npm run typecheck   # tsc --noEmit
npm test            # node:test unit tests for sse.ts + mapping.ts
```

## Run (F5)

1. Start a local gateway + cluster. From the repo root, `./demo.sh` boots three
   `harness-standalone` nodes and the gateway on `127.0.0.1:8080` in **insecure
   loopback** mode (the bearer token is taken as the tenant, unverified).
   Requires `ANTHROPIC_API_KEY` and Docker (see the script header).
2. Open `clients/harness-vscode` in VS Code and press **F5** to launch the
   Extension Development Host. The extension needs proposed APIs, which the dev
   host enables automatically from `enabledApiProposals` in `package.json`.
3. Open the **Agents window**, pick the **Harness** agent, and select or create
   a session (`Harness: New Session`). Send a prompt; records stream in and the
   run ends with an outcome.

### Sanity-check the gateway contract first

```bash
curl -N -X POST http://127.0.0.1:8080/v1/assistant/demo/prompt \
  -H 'Authorization: Bearer anonymous' -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  -d '{"turn":"t-1","content":"Create numbers.txt holding 1..10, then tell me their sum."}'
```

You should see `event: records` frames followed by a terminal `event: outcome`.

## Settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `harness.gatewayUrl` | `http://127.0.0.1:8080` | Gateway base URL |
| `harness.token` | `anonymous` | Bearer token (prefer `Harness: Set Gateway Token`, which uses SecretStorage) |
| `harness.kind` | `assistant` | Session kind (grain type) |
| `harness.withinSecs` | `600` | Per-prompt wait ceiling (gateway clamps it) |

## Caveats

- **Proposed API.** The Agents window is fed by VS Code's proposed
  `chatSessionsProvider` API. This runs in the Extension Development Host and can
  be packaged as a VSIX / published to Open VSX, but **cannot go to the public
  Marketplace** while it depends on a proposed API. The API is also mid-migration
  from a provider model to a controller model — pin `engines.vscode` and replace
  the vendored `.d.ts` with the exact upstream file for your target build
  (`npx @vscode/dts dev`).
- **History replay** depends on the proposed `ChatRequestTurn`/`ChatResponseTurn`
  constructors; where the runtime does not expose them the transcript starts
  empty and new turns still render live.
