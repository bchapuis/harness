# Edge: Specification

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** carry the meanings defined in RFC 2119. Sections of this document are cited as plain **§N**; other documents carry a prefix: **agent §N**, **entity §N**, **actor §N**.

## 1. Scope

The edge is the public HTTP/SSE front of an agent cluster, and its trust boundary. It is a cluster client: it joins the actor transport (cluster §4) as a non-voting member that hosts no entity, holds a routing-only client view of the session namespace (agent §3.4), and drives each caller's session by addressing the session's entity directly, with no control protocol in between. A request proves a principal with a bearer token; the edge scopes every session the request names under that principal, and records ownership in the tenancy directory, an entity (entity §3) this document specifies by its observable behavior. Front ends build on the edge's HTTP surface.

Tenancy lives entirely at the edge and in the directory: the entity layer stays tenant-blind. The edge holds no durable state, so any number of replicas MAY run, each joined under its own node id.

### 1.1 Non-goals

- **Per-tenant budgets and quotas:** a run's budget (agent §9.1) bounds a run, not a tenant.
- **Session deletion:** nothing retires a session or forgets its directory entry on a front end's behalf.
- **Token formats and introspection:** a token means what the deployment's verifier says it means (§3.2).
- **A WebSocket transport:** the edge speaks HTTP and Server-Sent Events only.

### 1.2 Conformance classes

- An **edge** conforms by meeting §3.1, §4, and §6, and by calling the directory as §3.3 states.
- A **token verifier** conforms by meeting §3.2.
- A **directory** conforms by meeting §3.3 and §5.5.
- A **deployment** conforms by meeting §4.1 and §6.

## 2. Model

- **Front end**: an HTTP client of the edge.
- **Principal**: the tenant identity a request acts as, named by a **principal id** (§4.3).
- **Token verifier**: the deployment's mapping from a bearer token to a principal id, or to nothing (§3.2). A **development verifier** takes the token itself as the principal id, verifying nothing.
- **Kind**: an agent kind (agent §6.1), named in a request path. A **routed kind** is one the edge serves.
- **Client session id**: the session id a front end names in a request path. The **scoped session key** is the key the edge addresses for it (§4.3).
- **Root session**: a session a front end names. A **child session** is one a delegation derived (agent §8.1).
- **Cursor**: a journal sequence number (agent §10.2). A read or a stream yields only records strictly after it.
- **Compaction base**: the journal's compaction floor (agent §10.2): records up to it are subsumed by a snapshot and not served.
- **Wait**: how long a prompt holds its caller for the run's outcome.
- **Tenancy directory**: one entity per principal, keyed by the principal id, that indexes the entity names the principal owns (§3.3). Each **entry** is a name with its **metadata**: a display label, a creation time, and free-form string attributes, every field supplied by the caller.

## 3. Seams

### 3.1 The HTTP/SSE surface

The client-facing boundary: the edge implements it, front ends consume it. Its operations are the routes of §5.1, and its headers, events, and codes are registered beside them (§5). Every route under `/v1` MUST authenticate (§4.2).

### 3.2 Token verification (`TokenVerifier`)

A plug point: a deployment supplies the verifier, and the edge consults it on every authenticated request.

```rust
pub trait TokenVerifier: Send + Sync {
    fn verify(&self, token: &str) -> Option<PrincipalId>;
}
```

`verify` maps a bearer token to the `PrincipalId` it authenticates, or to nothing when the token is unknown or malformed.

- A verifier runs at the edge, never inside an entity; it MAY be stateful and impure.
- A verifier MUST return only well-formed principal ids (§4.3).
- The edge MAY replace its verifier while serving (a token rotation) without dropping connections.

### 3.3 The tenancy directory

A boundary between the edge and the directory (§2), whose journal records the principal's entries. Its entity type and command manifests are registered in §5.5.

| Command | Reply | Commits |
|---|---|---|
| record (a name and metadata) | created, updated, or unchanged | the entry, unless the name is already held with identical metadata |
| forget (a name) | whether an entry was removed | the removal, if any |
| clear | the number of entries removed | every removal in one commit, if any |
| get (a name) | its metadata, if held | nothing |
| list | every entry | nothing |
| list by type (an entity type) | every entry of that type | nothing |

- The directory is an index, not an authority: it never reaches into the entities it names, and it is not an authorization boundary (§6).
- The directory reads no clock: every metadata field, the creation time included, is the caller's.
- A caller MUST NOT record metadata that mirrors the named entity's live state.

## 4. Semantics

### 4.1 Cluster membership

The edge reaches the cluster through a routing-only client view (agent §3.4) and a client of the directory's entity type, both resolved through discovery (actor §3.7) before it serves.

- The edge's node id MUST lie outside the voter roster, and each node MUST admit it explicitly, so it never votes or hosts.
- The edge MUST route only kinds the nodes host: it serves only once every routed kind and the directory have resolved, and a request naming a kind it does not route fails with `404` (§4.8).

### 4.2 Identity

A request names its principal by a bearer token (§5.2), and the edge alone decides identity.

- A request without a token, or with one the verifier rejects, MUST fail with `401` and a bearer challenge (§5.2), before any other effect.

### 4.3 Principal-scoped session keys

Isolation comes from key prefixing, not from the directory. For a request authenticated as principal `P` naming client session id `s`, the edge MUST address the session `(kind, P/s)`. The front end never supplies the prefix, so it can name only entities in its own namespace.

- A principal id MUST be non-empty and consist of ASCII alphanumerics, `_`, `-`, and `.` only; in particular it excludes `/` and `:`.
- A client session id MUST be non-empty and free of `/`, or the request fails with `400`. A scoped key therefore splits at its first `/` into exactly the principal id and the client session id.
- The edge MUST validate the session id and resolve the kind before any side effect: a malformed or unknown request records nothing in the directory.

### 4.4 Children are unaddressable

A child session is reached only by the agent that derived it (agent §8.1), never by a front end. A derived child session id MUST differ from every root key an edge can admit. This holds by construction: every derived id begins with a decimal length and a `:` (agent §8.1), so its first `/`-separated segment is not a principal id, while every admitted root key is `P/s` for a principal `P`. No front end can therefore name a child, and no listing returns one. Child ids are also distinct across tenants, since the parent's scoped key is one of the derivation's framed components.

### 4.5 Prompts

- A prompt MUST bound the caller's wait by a configured ceiling, clamping a larger requested wait; the run itself continues past any wait (agent §7.2).
- The edge MUST bound the request body.

### 4.6 Streams

A stream carries committed records as Server-Sent Events (§5.3).

- A stream MUST resume strictly after the cursor a reconnect presents (the last event id, else the `from` parameter), so no record is delivered twice.
- A stream MUST emit `truncated` before resuming past a compaction gap, never skip the gap silently.
- A follow stream MUST end once its turn has ended: after the batch holding the turn's `RunEnded`, or at once with `end` when the turn's recorded outcome (agent §3.4) shows it ended behind the cursor. A turn not yet accepted keeps the stream open.
- A streamed prompt MUST end: with `outcome` once its run has ended, and with `error` once its submit is refused (a `409`) or the caller's wait ends. It MUST NOT wait for a `RunEnded` behind the cursor, which the stream never replays: a re-prompt of a turn that ended behind the cursor answers with the recorded outcome alone.

### 4.7 The ownership index

- Before submitting a prompt, the edge MUST record the root session in its principal's directory, as the entity name of kind and scoped session key, labelled with the client session id. The record is best-effort: a failure does not fail the prompt, the run proceeds, and the next prompt records the session again. Only root sessions are recorded.
- A listing MUST read the principal's own directory (list by type, for the requested kind) and return each entry's client session id, dropping any entry that does not unscope under the principal.
- Reads (read records, follow) and cancel are not gated on the directory: the prefix already isolates them.

### 4.8 Error model

Every failure a front end sees is the error envelope (§5.1) carrying a stable code (§5.4), and the same envelope rides a stream's `error` event. Failure layers stay distinct (agent §7.2): a run that ran and failed is a `200` whose outcome carries the run's error, never an error status.

## 5. Registries

### 5.1 Routes

`{kind}` is the kind and `{session}` the client session id; `from` is a cursor, `limit` a page size, and `turn` a turn id (agent §7.2). Every failure answers with the envelope `{"error": {"code": …, "message": …}}` (§4.8).

| Route | Effect |
|---|---|
| `POST /v1/{kind}/{session}/prompt?from=` | Submit a turn (body `{"turn": …, "content": …, "within_secs": …}`, the wait optional) and answer `{"outcome": …}`, or, with `Accept: text/event-stream`, stream the run from `from` (§4.6). |
| `GET /v1/{kind}/{session}/records?from=&limit=` | One page of committed records after `from`, as `{"records": […], "base": …}`: (sequence number, record) pairs (agent §11.2) and the compaction base. |
| `GET /v1/{kind}/{session}/stream?turn=&from=` | Stream committed records after `from` until `turn` has ended (§4.6). |
| `POST /v1/{kind}/{session}/cancel?turn=` | Cancel the named run (agent §9.2); `204` on success. |
| `GET /v1/sessions?kind=` | The principal's root sessions of one kind, from its directory (§4.7), as `{"sessions": [{"session": …, "label": …}]}`. |
| `GET /healthz`, `GET /readyz` | Unauthenticated liveness, and readiness (`503` once a shutdown drain begins). |

### 5.2 Headers

| Header | On | Meaning |
|---|---|---|
| `Authorization: Bearer <token>` | every authenticated request | the bearer token (§4.2) |
| `WWW-Authenticate: Bearer` | every `401` response | the bearer challenge |
| `Accept: text/event-stream` | a prompt | answer with a stream |
| `Last-Event-ID` | a stream request | the reconnect cursor, taking precedence over `from` (§4.6) |

### 5.3 Stream events

| Event | Data | `id` |
|---|---|---|
| `records` | a batch of (sequence number, record) pairs | the batch's last sequence number |
| `truncated` | `{"base": …}`: compaction outran the cursor, and the records up to `base` cannot be replayed | `base` |
| `outcome` | the run's outcome (a streamed prompt only: after the batch holding `RunEnded`, or alone when that record lies behind the cursor) | none |
| `end` | empty (a follow stream only: after the batch holding `RunEnded`, or alone when that record lies behind the cursor) | none |
| `error` | `{"code", "message"}` (§4.8) | none |

### 5.4 Error codes

| Status | `code` | Cause |
|---|---|---|
| `400` | `bad_request` | malformed session id or body |
| `401` | `unauthorized` | missing or rejected token |
| `404` | `unknown_kind` | a kind the edge neither hosts nor routes |
| `409` | `turn_conflict` | the turn id is held with different content or budget |
| `409` | `kind_mismatch` | the session is not of the named kind |
| `503` | `unavailable` | unreachable, backpressure, no quorum, or a leadership move: retryable |
| `504` | `timeout` | the wait lapsed: re-submitting the same turn id re-attaches |
| `502` | `upstream` | any other failure reaching the cluster |

Both `409` codes are permanent submit rejections (agent §7.2): a front end changes the request, never retries it.

### 5.5 Directory manifests

The directory's entity type is `tenancy.Directory`. Its commands (§3.3):

| Command | Manifest |
|---|---|
| record | `tenancy.Record` |
| forget | `tenancy.Forget` |
| clear | `tenancy.Clear` |
| get | `tenancy.Get` |
| list | `tenancy.List` |
| list by type | `tenancy.ListByType` |

## 6. Security considerations

- **Trust boundary:** the edge holds references to session entities and presents the cluster secret on the transport, so it sits inside the cluster's trust boundary; untrusted callers reach it only over HTTP.
- **Identity and isolation:** the edge alone decides identity (§4.2). Key prefixing isolates principals (§4.3), and children are unaddressable (§4.4). The directory is not an authorization boundary: a reference to it is an in-cluster capability, never a tenant credential.
- **Tokens:** the edge MUST NOT log tokens. A deployment SHOULD supply tokens by file or secret store, not on a command line. The edge MUST refuse to serve with a development verifier on a non-loopback bind.
- **Transport:** public TLS terminates in front of the edge (an ingress or a load balancer): a bearer token over plaintext is readable on the path. The cluster transport is plaintext on a trusted network, guarded by the cluster secret; a deployment whose transport crosses untrusted links MUST provision mutual TLS on it.
- **Exhaustion:** the wait ceiling and the body bound (§4.5) cap what one request holds; a tenant's total spend is not bounded (§1.1).
