# Multi-tenant edge: the gateway as a cluster client

`agent-standalone` becomes multi-tenant through the `tenancy` directory entity
(`crates/tenancy`) and a single public edge: the **`agent-gateway`**, an
Orleans-style cluster *client*.

The trust boundary is the **gateway**; a request proves its principal with a
**bearer token**; the gateway reaches the cluster by joining the actor transport
as a non-voting, non-hosting member and addressing session entities directly,
with no control protocol in between.

## The shape of the problem

A session is an entity named `(KindId, SessionId)`, sharded by a blind hash;
entity is deliberately tenant-blind (`crates/tenancy/src/lib.rs` opening
docs). The `tenancy`
`Directory` entity records, per principal, which entity names that principal owns.
What is missing lives entirely at the network edge:

1. **Identity.** Something must verify a caller and bind it to a principal.
2. **Client-chosen names.** A handler routes to whatever `(kind, session)` string
   the caller sends. Nothing stops one caller from naming another's session
   unless the edge prevents it.

## Isolation and enumeration are different jobs

Only one of them is security-critical.

- **Isolation comes from edge-side key prefixing.** If the gateway prepends the
  *authenticated* principal to the session key
  (`SessionId::new(format!("{principal}/{session}"))`, via `auth::scoped_session`),
  a client can name only entities inside its own namespace: it never supplies the
  prefix, the gateway does.
- **Enumeration and lifecycle come from the `Directory`.** "List my sessions",
  "drop my whole index when I leave": the parts a client cannot derive from a
  key.

## Identity model

The gateway is the one place that decides identity. A request carries
`Authorization: Bearer <token>`; the gateway verifies it to a `PrincipalId` and
scopes the session key under it before addressing the entity:

```
client --(HTTP: Authorization: Bearer <token>)--> agent-gateway
                                                     |
                                  gateway verifies token -> principal P
                                  scopes key  P/<session>  -> EntityRef.ask(...)
```

Because the gateway holds `EntityRef`s and rides the receptionist gossip, it sits
**inside** the cluster's trust boundary (it presents the cluster secret on the
transport): the Akka-`ClusterClient` tradeoff. Untrusted callers reach it only
over HTTP with a bearer token; the nodes have no client-facing listener.

## Touchpoints

| File | Role |
|------|------|
| `agent-gateway/src/auth.rs` | The `TokenVerifier` seam: `verify(token) -> Option<PrincipalId>`, with `StaticTokens` (opaque secrets from `--auth-tokens`) and `InsecureTokens` (the token is the principal, loopback-only). Plus `scoped_session`/`unscope_session`. Runs at the edge, never in an entity (the determinism guard-rail). |
| `agent-gateway/src/http.rs` | Each handler verifies the bearer token to a principal, scopes the `SessionId`, and calls the entity directly (`SessionRef::prompt_within`/`tail`/`follow`/`cancel`). `Record`/`List` go through the client `Entities<Directory>`. SSE carries the live record stream. `/healthz` (liveness) and `/readyz` (200, then 503 once a shutdown signal starts the drain) are the two unauthenticated routes. |
| `agent-gateway/src/cluster.rs` | Joins the transport in `MembershipMode::Static` with the cluster secret, dials the nodes, and `add_member`s them, so the receptionist gossip (the host gateway refs) reaches this non-voting client. |
| `agent-gateway/src/lib.rs` | `Harness::client` (routing-only, no model/sandbox seams) + a client `Entities<Directory>`; `connect` polls until discovery, then assembles the `Gateway`. |
| `agent-standalone/src/node.rs` | The node hosts the Agent kinds and the `Directory` entity and admits the gateway with `--client <id>=<host>` (a non-voting id outside the Raft roster). No client-facing listener. |

## Delegated children are unaddressable, not prefixed

A delegated child's id is not `P/session/t-1/tu_42`. `derive_child`
(`agent/src/session.rs`) netstring-frames each component, so a child of the
scoped session `alice/demo` is `10:alice/demo/3:t-1/5:tu_42`: the principal is
inside the name but is not a prefix of it.

Tenant safety does not need it to be. A key a client can reach is always
`P/<segment>`, because the gateway supplies `P` itself and `is_valid_session`
refuses a segment holding `/`. A framed child id instead begins with digits and a
`:`, and `PrincipalId`'s charset excludes `:`, so `unscope_session` answers `None`
for such a key under every principal. No client can name a child, and a listing
never returns one. Uniqueness across tenants comes free from the same framing:
the parent's scoped key is one of the framed components.

Only **root** sessions (the ones the client names) get a `Directory` `Record`;
children are addressed internally by the agent, never by the client, so they
never reach the authorization gate.

## Directory enforcement (the feature layer)

- **Prompt**: `Record(P, name, meta)` then proceed. `Record` is idempotent, so
  every prompt re-asserts ownership and a transient index failure self-heals on
  the next turn (the run proceeds regardless; the index is best-effort).
- **List**: `GET /v1/sessions?kind=…` -> `Directory(P).ListByType(kind)`, with
  the principal prefix stripped back off each entry's key so the client sees the
  session ids it supplied.
- **Reads are not gated on `Contains`.** The principal prefix already isolates
  reads (a client cannot name a key outside its own principal), so a gate adds no
  security.
- **Later**: `session/delete` -> forget the entry and retire the entity.

## Security properties

- **Public TLS terminates at the edge.** Bearer tokens over plaintext are
  sniffable; terminate TLS at an ingress/LoadBalancer in front of the gateway.
  The gateway refuses an insecure (no `--auth-tokens`) mode on a non-loopback
  public bind.
- **The transport is plaintext, guarded by the cluster secret.** Fine within a
  trusted cluster network. The transport itself supports mutual TLS
  (`Encryption::MutualTls`), but neither binary exposes a flag for it: both name
  `Encryption::PlaintextTrusted` in code, so a deployment crossing untrusted links
  has to provision certificates there and rebuild.
- **Token handling.** Prefer a Secret/file over `argv` (visible in `ps`); never
  log tokens.
- **Determinism guard-rail.** Verification happens at the edge only;
  `Meta.created_at` stays caller-supplied.

## Limitations

Not implemented: per-tenant budgets and quotas, `session/delete` (forget plus
entity retire), token introspection, signed (HMAC/JWT) tokens, and a WebSocket
transport.

## Tests

- Unit: `TokenVerifier`; scoped-key construction (`agent-gateway/src/auth.rs`).
- Integration: `agent-gateway/tests/gateway.rs` drives the axum router against
  an in-process cluster, runs a prompt to a hosted entity, and asserts principal A
  cannot see principal B's sessions, the core isolation invariant.

## Token format

The deployment ships the **static tokens file** (`StaticTokens`): opaque secrets
mapped to principals, dependency-free so the build stays offline-clean. Not
implemented: a stateless **signed-token** verifier (HMAC/JWT, subject =
principal) behind the same `TokenVerifier` seam, which would scale the edge
without a shared token store but needs a crypto dependency.
