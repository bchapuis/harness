# Edge: Specification

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119. Sections of this document are cited as plain **§N**; other documents carry a prefix: **agent §N**, **entity §N**.

## 1. Scope

The edge (the HTTP gateway, `agent-gateway`) is the public HTTP/SSE front of an agent cluster, and its trust boundary. It is a cluster client: it joins the actor transport as a non-voting member that hosts no entity, holds a routing-only harness (agent §2.5), and drives each caller's session by addressing the session's entity directly, with no control protocol in between. A request proves a principal with a bearer token; the edge scopes every session the request names under that principal, and records ownership in the tenancy directory.

The edge holds no durable state, so any number of replicas MAY run, each under its own client id. The entity layer stays tenant-blind: tenancy lives entirely at the edge and in the directory entity.

### 1.1 Non-goals

- **Per-tenant budgets and quotas:** a run's budget (agent §7.1) bounds a run, not a tenant.
- **Session deletion:** retiring a session and forgetting its directory entry.
- **Token introspection and signed tokens:** see §4.
- **A WebSocket transport.**

## 2. Seams

### 2.1 The HTTP/SSE surface

The client-facing boundary, implemented by the edge and consumed by front ends (`agent-tui` among them). Every route under `/v1` MUST authenticate (§3.1); `{session}` is the client's unscoped session id (§3.2).

| Route | Effect |
|---|---|
| `POST /v1/{kind}/{session}/prompt` | Submit a turn (body: `turn`, `content`, optional `within_secs`) and return `{"outcome": …}`, or, with `Accept: text/event-stream`, stream the run (below). |
| `GET /v1/{kind}/{session}/records?from=&limit=` | One page of committed records after `from`, with the compaction `base` (agent §8.2). |
| `GET /v1/{kind}/{session}/stream?turn=&from=` | Stream committed records after `from` until `turn`'s `RunEnded` arrives, or end at once if the turn ended behind `from`. |
| `POST /v1/{kind}/{session}/cancel?turn=` | Cancel the named run (agent §7.2); `204` on success. |
| `GET /v1/sessions?kind=` | The principal's sessions of one kind, from its directory (§3.4). |
| `GET /healthz`, `GET /readyz` | Unauthenticated liveness, and readiness (`503` once a shutdown drain begins). |

A stream is Server-Sent Events of these types:

| Event | Data | `id` |
|---|---|---|
| `records` | a batch of `(Seq, Record)` pairs | the batch's last `Seq` |
| `truncated` | `{"base": …}`: compaction outran the cursor, and the records up to `base` cannot be replayed | `base` |
| `outcome` | the run's `RunOutcome` (a streamed `prompt` only: after the `RunEnded` batch, or alone when that record lies behind the cursor) | none |
| `end` | empty (a `stream` only: after the `RunEnded` batch, or alone when that record lies behind the cursor) | none |
| `error` | `{"code", "message"}` (§3.5) | none |

- A stream MUST resume strictly after the cursor a reconnect presents (`Last-Event-ID`, else `from`), so no record is delivered twice.
- A stream MUST emit `truncated` before resuming past a compaction gap, never skip it silently.
- A `prompt` MUST bound the caller's wait by a configured ceiling, clamping a larger `within_secs`; the run itself continues past any wait (agent §5.3).
- A `stream` MUST end once its turn has ended: after the `RunEnded` batch, or at once with `end` when the turn's recorded outcome (agent §2.5) shows it ended behind the cursor. A turn not yet accepted keeps the stream open, since a client may watch before it submits.
- A streamed `prompt` MUST end: with `outcome` once its run has ended, and with `error` once its submit is refused (a `409`) or the caller's wait ends. It MUST NOT wait for a `RunEnded` behind the cursor, which the stream never replays: a re-prompt of a turn that ended before `from` answers with the recorded outcome alone.
- The edge MUST bound the request body.

### 2.2 Token verification (`TokenVerifier`)

A plug point: a deployment supplies the verifier, and the edge consults it on every authenticated request. `TokenVerifier::verify` maps a bearer token to a `PrincipalId`, or to nothing.

- A verifier runs at the edge, never in an entity, whose fold must stay pure (entity §3.1); it MAY be stateful and impure.
- A verifier MUST return only well-formed principals (§3.2).
- The edge MAY replace its verifier at runtime (a token rotation) without dropping connections.

`StaticTokens` maps opaque secrets to principals; `InsecureTokens` takes the token as the principal, for loopback development only (§3.6).

### 2.3 The tenancy directory (`Directory`)

An internal boundary between the edge and the tenancy entity: one `Directory` entity per principal, keyed by the principal id, whose journal records the `EntityName`s that principal owns, each with caller-supplied `Meta`. Its commands are `Record` (idempotent: an unchanged entry commits nothing), `Forget`, `Clear`, `Get`, `List`, and `ListByType`. The directory is an index, not an authority: it never reaches into the entities it names, and it is not an authorization boundary. Every field of `Meta` is caller-supplied, since the fold reads no clock (entity §3.1), and `Meta` MUST NOT mirror the named entity's live state, which would be a second source of truth.

### 2.4 The cluster client

The edge's side of the cluster: a routing-only `Harness` (`Harness::client`) and a client handle to the directory's entity type, both resolved through the receptionist before the edge serves. Because it holds `EntityRef`s and presents the cluster secret on the transport, the edge sits inside the cluster's trust boundary; untrusted callers reach it only over HTTP.

- The edge's node id MUST lie outside the voter roster, and each node MUST admit it explicitly, so it never votes or hosts.
- The edge MUST route only kinds the nodes host: it serves only once every routed kind and the directory have resolved, and a kind it does not route is a `404` (§3.5).

## 3. Semantics

### 3.1 Identity

A request names its principal by `Authorization: Bearer <token>`, and the edge alone decides identity.

- A request without a token, or with one the verifier rejects, MUST fail with `401` and a `WWW-Authenticate: Bearer` challenge, before any other effect.
- The edge MUST NOT log tokens. Tokens SHOULD be supplied by file or secret, not on a command line.

### 3.2 Principal-scoped session keys

Isolation comes from key prefixing, not from the directory. The edge MUST address the session `(kind, P/s)` for a request authenticated as principal `P` naming session `s`: the client never supplies the prefix, so it can name only entities in its own namespace.

- A `PrincipalId` MUST consist of ASCII alphanumerics, `_`, `-`, and `.` only, and be non-empty: in particular it excludes `/` and `:`.
- A client session id MUST be non-empty and free of `/`, or the request fails with `400`, so a scoped key splits back at its first `/` into exactly the principal and the client's id.
- The edge MUST validate the session id and resolve the kind before any side effect, so a malformed or unknown request records nothing in the directory.

### 3.3 Children are unaddressable

A delegated child session is reached only by the agent that derived it (agent §6.1), never by a client. A derived child `SessionId` MUST differ from every root id an edge can admit. It holds by construction: every derived id begins with a decimal length and a `:` (agent §6.1), so its first `/`-separated segment is not a `PrincipalId`, while every admitted root key is `P/s` for a principal `P`. No client can therefore name a child, and no listing returns one. Child ids are also distinct across tenants, since the parent's scoped key is one of the framed components.

### 3.4 The ownership index

- Before submitting a prompt, the edge MUST `Record` the root session under its principal. The record is best-effort: a failure is logged, the run proceeds, and the next prompt re-records it. Only root sessions are recorded.
- A listing MUST read the principal's own directory (`ListByType` for the requested kind) and return each entry's unscoped session id, dropping any entry that does not unscope under the principal.
- Reads (`records`, `stream`) and `cancel` are not gated on the directory: the prefix already isolates them.

### 3.5 Error model

Every failure a client sees is `{"error": {"code", "message"}}`, with a stable `code`, and the same envelope rides an SSE `error` event. Failure layers stay distinct (agent §5.3): a run that ran and failed is a `200` whose `outcome` carries the `RunError`, never an error status.

| Status | `code` | Cause |
|---|---|---|
| `400` | `bad_request` | malformed session id or body |
| `401` | `unauthorized` | missing or rejected token |
| `404` | `unknown_kind` | a kind the edge neither hosts nor routes |
| `409` | `turn_conflict`, `kind_mismatch` | a permanent `SubmitReject` (agent §5.3): change the request, never retry it |
| `503` | `unavailable` | unreachable, backpressure, no quorum, or a leadership move: retryable |
| `504` | `timeout` | the wait lapsed: re-submitting the same turn id re-attaches |
| `502` | `upstream` | any other failure reaching the cluster |

### 3.6 Transport security

- Public TLS terminates in front of the edge (an ingress or load balancer): a bearer token over plaintext is sniffable.
- The edge MUST refuse to serve with `InsecureTokens` on a non-loopback bind.
- The cluster transport is trusted-network plaintext guarded by the cluster secret; a deployment crossing untrusted links MUST provision mutual TLS on the transport.

## 4. Deferred

- **Signed tokens:** only the static tokens file ships, so every replica needs the token map; a stateless HMAC or JWT verifier behind `TokenVerifier` would remove it, at the cost of a crypto dependency.
- **Per-tenant quotas and session deletion:** a tenant's spend and session count are unbounded, and its sessions outlive its index.
- **Mutual TLS from configuration:** neither binary exposes a flag for it, so enabling it means a rebuild.
- **WebSocket transport and token introspection:** neither exists.
