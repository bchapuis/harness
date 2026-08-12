# harness-tui

A terminal client for the agentic harness: a two-pane chat UI over the gateway's
HTTP/SSE edge (`crates/harness-gateway`). It authenticates as a tenant with a
bearer token, lists that tenant's sessions, and drives one — submit a prompt and
watch the run's records stream in live as a transcript.

It talks to the gateway and nothing else. There is no cluster membership here, no
actor transport, and no durable state: the gateway does the cluster work and this
is the edge UI in front of it. That is the same boundary the VS Code extension in
`clients/harness-vscode` sits on, over the same REST + Server-Sent-Events
protocol — one client in the terminal, one in an editor.

The one shared dependency is `harness` itself, for the record and outcome types
the gateway serializes onto the wire. Sharing them keeps the client in lockstep
with the server's JSON rather than mirroring a second copy that can drift.

## Keys map to endpoints

The UI is deliberately thin over the gateway's surface, and `?` shows this map
in-app:

| Key | Endpoint |
| --- | --- |
| `Enter` (prompt) | `POST /v1/{kind}/{session}/prompt` (SSE stream) |
| `Esc` (running) | `POST /v1/{kind}/{session}/cancel` |
| `↑`/`↓` in sessions | `GET /v1/{kind}/{session}/records` (load history) |
| startup / on end | `GET /v1/sessions?kind={kind}` (this tenant) |
| `n` · `Ctrl-N` | new session (recorded on its first prompt) |
| `Ctrl-R` | toggle the raw journal view (the `/records` payload) |

The rest is navigation: `Tab` switches focus between the session list and the
prompt, `PgUp`/`PgDn` and the mouse wheel scroll the transcript, `Home`/`End`
jump to top and bottom, and `Ctrl-C` quits.

## Layout

| File | Role |
| --- | --- |
| `src/main.rs` | Flag parsing, terminal setup, and the key/mouse event loop |
| `src/app.rs` | Application state and its transitions; network work is spawned and returns as `Update`s on a channel |
| `src/client.rs` | Minimal HTTP/1.1 client for the gateway's REST/SSE surface, with the streaming SSE frame parser |
| `src/ui.rs` | The two-pane-plus-prompt render, word-wrapping the transcript into physical rows so the scroll offset is exact |

Two shapes of call live in `client.rs`. **Request-response** (`list_sessions`,
`fetch_records`, `cancel`) sends, reads the whole reply, and parses the JSON, one
connection per request. **Streaming** (`open_prompt`) submits a turn with
`Accept: text/event-stream` and hands back an iterator yielding the run's records
live off the chunked SSE body, which is what lets the transcript render as the
agent works.

The HTTP client is hand-rolled rather than `reqwest`, the same choice the
standalone deployment's model seam makes, but it reads the response as a live
stream for SSE instead of to EOF. An `https://` base reuses rustls with webpki
trust anchors; a plain `http://` base — the loopback demo — skips TLS.

## Run

Start a gateway and cluster first. From the repo root, `./demo-agent.sh` boots
three `harness-standalone` nodes and the gateway on `127.0.0.1:8080` in
**insecure loopback** mode, where the bearer token is taken as the tenant
without verification. It needs `ANTHROPIC_API_KEY` and Docker; see the script
header.

```sh
cargo run -p harness-tui
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--url <base>` | `http://127.0.0.1:8080` | Gateway base URL, `http(s)://host[:port]` |
| `--token <t>` | `$HARNESS_TOKEN`, else `anonymous` | Tenant bearer token |
| `--kind <k>` | `assistant` | Agent kind to address |
| `--session <s>` | `demo` | Session to open on start |

The defaults match the loopback demo, so the default `--token anonymous` simply
acts as tenant "anonymous". Against an authenticated gateway, pass the opaque API
token instead — it is the tenant identity, and the gateway is where auth
terminates (`docs/multi-tenant-edge.md`).

## Caveats

- **No tests.** The crate is a client binary with no in-tree test suite; the
  protocol it speaks is covered on the server side by `harness-gateway`'s.
  The TypeScript SSE parser in `clients/harness-vscode` is a port of this
  crate's `parse_frame` and *is* unit-tested, so the frame grammar has coverage
  even though this copy of it does not.
- **Insecure by default.** The defaults target the loopback demo. Pointing the
  client at a real gateway means passing a real token over `https://`.
