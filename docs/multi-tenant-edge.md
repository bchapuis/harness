# Multi-tenant edge: the gateway as a cluster client

`agent-standalone` is multi-tenant through the `tenancy` directory entity (`crates/tenancy`) and one public edge, the **`agent-gateway`**: an Orleans-style cluster *client* and the trust boundary. A request proves its principal with a **bearer token**; the gateway joins the actor transport as a non-voting, non-hosting member and addresses session entities directly, with no control protocol in between.

## The shape of the problem

A session is an entity named `(KindId, SessionId)`, sharded by a blind hash; entity is deliberately tenant-blind (`crates/tenancy/src/lib.rs` opening docs). The `tenancy` `Directory` entity records which entity names each principal owns. What is missing lives at the network edge:

1. **Identity.** Something must verify a caller and bind it to a principal.
2. **Client-chosen names.** A handler routes to whatever `(kind, session)` string the caller sends, so unless the edge prevents it, one caller can name another's session.

## Isolation and enumeration are different jobs

Only isolation is security-critical.

- **Isolation comes from edge-side key prefixing.** The gateway prepends the *authenticated* principal to the session key (`SessionId::new(format!("{principal}/{session}"))`, via `auth::scoped_session`). The client never supplies the prefix, so it can name only entities in its own namespace.
- **Enumeration and lifecycle come from the `Directory`**: listing a principal's sessions and dropping its whole index, which a client cannot derive from a key.

## Identity model

The gateway alone decides identity:

```
client --(HTTP: Authorization: Bearer <token>)--> agent-gateway
                                                     |
                                  gateway verifies token -> principal P
                                  scopes key  P/<session>  -> EntityRef.ask(...)
```

Because it holds `EntityRef`s and rides the receptionist gossip, the gateway sits **inside** the cluster's trust boundary (it presents the cluster secret on the transport): the Akka-`ClusterClient` tradeoff. Untrusted callers reach it only over HTTP with a bearer token; the nodes have no client-facing listener.

## Touchpoints

| File | Role |
|------|------|
| `agent-gateway/src/auth.rs` | The `TokenVerifier` seam: `verify(token) -> Option<PrincipalId>`, with `StaticTokens` (opaque secrets from `--auth-tokens`) and `InsecureTokens` (the token is the principal, loopback-only); plus `scoped_session`/`unscope_session`. |
| `agent-gateway/src/http.rs` | Each handler verifies the bearer token to a principal, scopes the `SessionId`, and calls the entity directly (`SessionRef::prompt_within`/`tail`/`follow`/`cancel`). `Record`/`ListByType` go through the client `Entities<Directory>`. SSE carries the live record stream. The only unauthenticated routes are `/healthz` (liveness) and `/readyz` (200, then 503 once a shutdown signal starts the drain). |
| `agent-gateway/src/cluster.rs` | Joins the transport in `MembershipMode::Static` with the cluster secret, dials the nodes, and `add_member`s them, so the receptionist gossip (the host gateway refs) reaches this non-voting client. |
| `agent-gateway/src/lib.rs` | `Harness::client` (routing-only, no model/sandbox seams) + a client `Entities<Directory>`; `connect` polls until discovery, then assembles the `Gateway`. |
| `agent-standalone/src/node.rs` | The node hosts the Agent kinds and the `Directory` entity and admits the gateway with `--client <id>=<host>` (a non-voting id outside the Raft roster). |

## Delegated children are unaddressable, not prefixed

A delegated child's id is not `P/session/t-1/tu_42`: `derive_child` (`agent/src/session.rs`) netstring-frames each component, so a child of the scoped session `alice/demo` is `10:alice/demo/3:t-1/5:tu_42`, with the principal inside the name but not a prefix of it.

Tenant safety does not need a prefix. A key a client can reach is always `P/<segment>`: the gateway supplies `P`, and `is_valid_session` refuses a segment holding `/`. A framed child id begins with digits and a `:`, which `PrincipalId`'s charset excludes, so `unscope_session` answers `None` for it under every principal: no client can name a child, and no listing returns one. The same framing makes child ids unique across tenants, since the parent's scoped key is one of the framed components.

Only **root** sessions (the ones the client names) get a `Directory` `Record`; the agent addresses children internally, so they never reach the authorization gate.

## Directory enforcement (the feature layer)

- **Prompt**: `Record(P, name, meta)`, then proceed. `Record` is idempotent, so every prompt re-asserts ownership and a transient index failure heals on the next turn; the run proceeds regardless, since the index is best-effort.
- **List**: `GET /v1/sessions?kind=…` -> `Directory(P).ListByType(kind)`, with the principal prefix stripped from each key so the client sees the session ids it supplied.
- **Reads are not gated on `Contains`.** The principal prefix already isolates reads, so a gate adds no security.

## Security properties

- **Public TLS terminates at the edge.** Bearer tokens over plaintext are sniffable, so terminate TLS at an ingress/LoadBalancer in front of the gateway. The gateway refuses insecure mode (no `--auth-tokens`) on a non-loopback public bind.
- **The transport is plaintext, guarded by the cluster secret**, which suits a trusted cluster network. The transport supports mutual TLS (`Encryption::MutualTls`), but neither binary exposes a flag for it: both name `Encryption::PlaintextTrusted` in code, so a deployment crossing untrusted links must provision certificates there and rebuild.
- **Token handling.** Prefer a Secret/file over `argv` (visible in `ps`); never log tokens.
- **Determinism guard-rail.** Token verification runs at the edge, never in an entity; `Meta.created_at` stays caller-supplied.

## Limitations

Not implemented: per-tenant budgets and quotas, `session/delete` (forget the `Directory` entry and retire the entity), token introspection, signed tokens (see Token format), and a WebSocket transport.

## Tests

- Unit: `TokenVerifier` and scoped-key construction (`agent-gateway/src/auth.rs`).
- Integration: `agent-gateway/tests/gateway.rs` drives the axum router against an in-process cluster, runs a prompt to a hosted entity, and asserts principal A cannot see principal B's sessions (the core isolation invariant).

## Token format

The deployment ships the **static tokens file** (`StaticTokens`): opaque secrets mapped to principals, dependency-free so the build stays offline-clean. A stateless **signed-token** verifier (HMAC/JWT, subject = principal) behind the same `TokenVerifier` seam would scale the edge without a shared token store but needs a crypto dependency; it is not implemented.
