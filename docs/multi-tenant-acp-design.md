# Multi-tenant edge: the gateway as a cluster client

This is the design for making `harness-standalone` multi-tenant. It is the
deployment expression of the project's goal — a distributed, multi-tenant
agentic runtime — built on the `tenancy` directory grain (`crates/tenancy`) and a
single public edge: the **`harness-gateway`**, an Orleans-style cluster *client*.

Decisions settled with the project owner: the trust boundary is the **gateway**;
a request proves its principal with a **bearer token**; and the gateway reaches
the cluster not over a bespoke control protocol but by joining the actor
transport as a non-voting, non-hosting member and addressing session grains
directly. (An earlier iteration put a line-delimited control protocol between a
REPL/ACP front-end and the nodes, with the gateway forwarding over it; that whole
layer was collapsed onto the cluster-client edge — see `.claude/plans/`.)

## The shape of the problem

The runtime is already built for this. A session is a grain named
`(KindId, SessionId)`, sharded by a blind hash; granary is deliberately
tenant-blind (`crates/tenancy/src/lib.rs` opening docs). The `tenancy`
`Directory` grain records, per principal, which grain names that principal owns.
What is missing lives entirely at the network edge:

1. **Identity.** Something must verify a caller and bind it to a principal.
2. **Client-chosen names.** A handler routes to whatever `(kind, session)` string
   the caller sends. Nothing stops one caller from naming another's session
   unless the edge prevents it.

Neither gap is in the core; both are at the edge — now the gateway.

## The key insight: isolation and enumeration are different jobs

Only one of them is security-critical, and it is the smaller change.

- **Isolation comes from edge-side key prefixing.** If the gateway prepends the
  *authenticated* principal to the session key —
  `SessionId::new(format!("{principal}/{session}"))`, via `auth::scoped_session`
  — a client can only ever name grains inside its own namespace, because it never
  supplies the prefix; the gateway does. This is the load-bearing isolation.
- **Enumeration and lifecycle come from the `Directory`.** "List my sessions",
  "drop my whole index when I leave" — the parts a client cannot derive from a
  key. This is the richer feature layer; the `Directory` is the enumeration
  primitive.

So the work splits into a small security-critical core and a feature layer.

## Identity model

The gateway is the one place that decides identity. A request carries
`Authorization: Bearer <token>`; the gateway verifies it to a `PrincipalId` and
scopes the session key under it before addressing the grain:

```
client --(HTTP: Authorization: Bearer <token>)--> harness-gateway
                                                     |
                                  gateway verifies token -> principal P
                                  scopes key  P/<session>  -> GrainRef.ask(...)
```

Because the gateway holds `GrainRef`s and rides the receptionist gossip, it sits
**inside** the cluster's trust boundary (it presents the cluster secret on the
transport). That is the Akka-`ClusterClient` tradeoff, accepted because the
gateway is already where tenant auth terminates. Untrusted callers reach it only
over HTTP with a bearer token; the nodes have no client-facing listener.

## Touchpoints

| File | Role |
|------|------|
| `harness-gateway/src/auth.rs` | The `TokenVerifier` seam: `verify(token) -> Option<PrincipalId>`, with `StaticTokens` (opaque secrets from `--auth-tokens`) and `InsecureTokens` (the token is the principal, loopback-only). Plus `scoped_session`/`unscope_session`. Runs at the edge, never in a grain (the determinism guard-rail). |
| `harness-gateway/src/http.rs` | Each handler verifies the bearer token to a principal, scopes the `SessionId`, and calls the grain directly (`SessionRef::prompt_within`/`tail`/`follow`/`cancel`). `Record`/`List` go through the client `Granary<Directory>`. SSE carries the live record stream. |
| `harness-gateway/src/cluster.rs` | Joins the transport in `MembershipMode::Static` with the cluster secret, dials the nodes, and `add_member`s them — so the receptionist gossip (the host gateway refs) reaches this non-voting client. |
| `harness-gateway/src/lib.rs` | `Harness::client` (routing-only, no model/sandbox seams) + a client `Granary<Directory>`; `connect` polls until discovery, then assembles the `Gateway`. |
| `harness-standalone/src/node.rs` | The node hosts the Agent kinds and the `Directory` grain and admits the gateway with `--client <id>=<host>` (a non-voting id outside the Raft roster). No client-facing listener. |

## Delegation falls out for free

Delegated children get ids like `parent/t-1/tu_42`. Because the parent session is
already `P/session`, a child becomes `P/session/t-1/tu_42` — the principal prefix
is inherited with no extra logic, so worker grains stay in-tenant. Only **root**
sessions (the ones the client names) get a `Directory` `Record`; children are
addressed internally by the agent, never by the client, so they never reach the
authorization gate.

## Directory enforcement (the feature layer)

- **Prompt** — `Record(P, name, meta)` then proceed. `Record` is idempotent, so
  every prompt re-asserts ownership and a transient index failure self-heals on
  the next turn (the run proceeds regardless — the index is best-effort).
- **List** — `GET /v1/sessions?kind=…` -> `Directory(P).ListByType(kind)`, with
  the principal prefix stripped back off each entry's key so the client sees the
  session ids it supplied.
- **Reads are not gated on `Contains`.** The principal prefix already isolates
  reads (a client cannot name a key outside its own principal), so a gate adds no
  security. The gateway records on prompt and lists, and leaves reads to
  prefix-isolation.
- **Later** — `session/delete` -> forget the entry and retire the grain.

## Security properties to bake in, not bolt on

- **Public TLS terminates at the edge.** Bearer tokens over plaintext are
  sniffable; terminate TLS at an ingress/LoadBalancer in front of the gateway.
  The gateway refuses an insecure (no `--auth-tokens`) mode on a non-loopback
  public bind.
- **The transport is plaintext, guarded by the cluster secret.** Fine within a
  trusted cluster network; a deployment crossing untrusted links provisions a
  transport cert (`TlsConfig`) on both the nodes and the gateways.
- **Token handling.** Prefer a Secret/file over `argv` (visible in `ps`); never
  log tokens.
- **Determinism guard-rail.** Verification happens at the edge only;
  `Meta.created_at` stays caller-supplied.

## How it stands

- **Isolation core** — an authenticated bearer token binds a request's principal;
  session keys are prefixed with it (`auth::scoped_session`); a `TokenVerifier`
  seam ships `StaticTokens` (opaque secrets from `--auth-tokens`, the secure mode)
  and `InsecureTokens` (the token is the principal, loopback-only).
- **Directory wiring** — the node hosts the `tenancy::Directory` grain (one per
  principal, sharing the kinds' durable store); the gateway records ownership on
  each prompt and answers `GET /v1/sessions` from it.
- **Gateway-as-cluster-client edge** — `harness-gateway` joins the transport as a
  non-voting member (`Harness::client` + `granary_client`), terminates tenant
  auth, and addresses session grains directly — no control protocol, no
  forwarding hop. SSE carries the live record stream.
- **Optional, later** — per-tenant budgets and quotas, `session/delete` (forget
  plus grain retire), token introspection, signed (HMAC/JWT) tokens, a WebSocket
  transport, and a re-introduced thin CLI as an HTTP client of the gateway.

## Tests

- Unit: `TokenVerifier`; scoped-key construction (`harness-gateway/src/auth.rs`).
- Integration: `harness-gateway/tests/gateway.rs` drives the axum router against
  an in-process cluster, runs a prompt to a hosted grain, and asserts principal A
  cannot see principal B's sessions — the core isolation invariant.

## Token format

The deployment ships the **static tokens file** (`StaticTokens`): opaque secrets
mapped to principals, dependency-free so the build stays offline-clean. A
stateless **signed-token** verifier (HMAC/JWT, subject = principal) is the next
implementation of the same `TokenVerifier` seam, for a runtime that wants to
scale the edge without a shared token store; it needs a crypto dependency, so it
lands once that can be added.
