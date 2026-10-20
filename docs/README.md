# Documentation

Start at the root [README](../README.md) for what the system is and how to run it, then [architecture](architecture.md) for the components, the seams between them, and how a deployment wires them. This directory holds the specifications, the deployment guide, and the testing conventions.

## Specifications

Each spec is the **contract** of one component: what it promises at its seams, whatever the implementation. Mechanism (how a crate meets the contract) is in the crate's rustdoc, private modules included (`cargo doc --workspace --no-deps --document-private-items`); which suites verify each invariant is in the crate's Rust catalogue beside its tests; what the implementation does not yet meet is in [status](status.md). The specs are normative (RFC 2119) and cite the ones beneath them by a short prefix (`actor §N`, `cluster §N`, `storage §N`, and so on).

| | Spec | Owns | Crates | Invariants |
|---|---|---|---|---|
| substrate | [distributed-actor-spec](distributed-actor-spec.md) | the actor model: actors, messages, references, the actor system, the codec, the host environment, the event stream, the receptionist, supervision, death watch, determinism | `actor-core`, `actor-serialization`, `actor-simulation` | #1–#22 |
| | [cluster-spec](cluster-spec.md) | the clustered actor system: transport, associations, membership and its four control planes, failure detection, consensus groups, authorization | `actor-cluster`, `actor-runtime` | defines a subset of #1–#22 |
| | [cluster-utilities-spec](cluster-utilities-spec.md) | rendezvous placement, group routers, the cluster singleton | `actor-cluster` | U1–U2 |
| storage | [compatibility-spec](compatibility-spec.md) | format stamps, the format registry, refusal by name | `compat` | V1–V3 |
| durable objects | [entity-spec](entity-spec.md) | entities: the model, the gates, activation, lifecycle, observation, and the whole E catalogue | `entity` | E1–E21 |
| | [entity-storage-spec](entity-storage-spec.md) | the journal and store seams, the fence, replication, shards, snapshots, blobs | `entity` | defines a subset of E1–E21 |
| | [entity-facets-spec](entity-facets-spec.md) | the facet contract and the six facets | `entity` | F1–F4, and a subset of E1–E21 |
| autonomous entities | [workflow-spec](workflow-spec.md) | the desired set of effects, intent before effect, orphan resolution, quiescence | `workflow` | W1–W7 |
| products | [agent-spec](agent-spec.md) | the agent: a session entity with a run loop, a model seam, and a sandbox seam | `agent`, `agent-anthropic`, `agent-openai`, `agent-http` | A1–A7 |
| | [sandbox-spec](sandbox-spec.md) | the execution tiers behind the agent's sandbox seam, provider side | `agent-sandbox` | S1–S5 |
| | [machine-spec](machine-spec.md) | a durable lightweight VM as an entity, reached over SSH | `machine-entity`, `machine-host`, `machine-proto`, `machine-frontdoor`, `machine-standalone` | M1–M6 |
| edge | [edge-spec](edge-spec.md) | the HTTP/SSE gateway, bearer-token identity, principal-scoped sessions, the tenancy directory | `agent-gateway`, `tenancy` | stated as obligations, no catalogue |

The write-ahead log (`wal`) is specified by its own crate docs, L1–L4 included: its only callers are file-backed implementations of two seams (`EntityStore`, `RaftWAL`), and nothing else reads its files. Its format is registered in compatibility §4.

### How a spec is written

A spec is the contract of a role, not a description of a crate. Five rules decide what belongs in one:

1. **Subjects are roles, objects are observables.** A requirement binds a role (a host, a journal, a caller, a peer, a provider, a deployment) and is stated in what its counterparts can observe: operations and their results, messages, errors, events, ordering, bounds, bytes on the wire or at rest. It never names a module, a helper type, an algorithm, or internal state.
2. **A backticked name is registered or a plug-point item.** In backticks go the interop constants a spec registers (manifests, frames, formats, magics, tags, routes, codes, event kinds) and the items of a plug point's signature. Every other concept is a term the spec defines once, in its Model section, and then writes in plain prose.
3. **Rust signatures only for plug points.** A **plug point** (a trait a deployment or third party implements or chooses, [architecture](architecture.md) §3) carries its signature in one fence in its seam subsection, async methods written `async fn`; types a fence names are defined in rustdoc. A **boundary** (one implementation) is specified by its abstract operations and their obligations.
4. **Each requirement once.** A requirement lives in the spec that owns the behavior, and every other spec cites it. Determinism is stated once (actor §11), as obligations and as the deterministic profile.
5. **Contract only.** No mechanism (algorithms, tunables, layouts beyond registered formats, stock implementations, configuration fields, the language's mechanics), no rationale (a requirement that needs its reason states the property the reason protects), no verification apparatus, no status, and no history.

Every spec has the same skeleton, in this order: **Scope** (what the component is, what it builds on, what builds on it, its non-goals, and its conformance classes: the roles that can claim conformance and the sections binding each); **Model** (each term and abstraction, defined once); **Seams** (one subsection per seam it owns: the roles on each side, plug point or boundary, the operations, the obligations on each side); the **semantics**, one section per behavior an independent implementation must reproduce; **Registries**, when it owns interop constants; **Invariants**, whose identifiers are stable; and **Security considerations**.

## Assumptions

- [hardware-envelope](hardware-envelope.md), the machine the specs are written against: a rented dedicated server, local NVMe, 128–256 GB, and a **1 Gbps uplink** that is the binding constraint on everything. It carries a 2026 latency table (with LLM rows, since the session's clock is the model's), the four inversions that follow from it, and the rules a review applies with them. Cited as `hw §N` wherever a performance choice rests on it; read it before importing a slow-disk-era technique or dropping a bound.

## Guides

- [standalone-deployment](standalone-deployment.md): install `agent-standalone`, run a three-node cluster behind the gateway, and walk the failure drill. Each binary's `--help` is its flag reference.
- [k8s/README](../k8s/README.md): the same cluster as a Kubernetes StatefulSet.
- [deploy/systemd/README](../deploy/systemd/README.md): the same cluster on bare metal (systemd units, the on-disk layout and which parts are rebuildable, the rolling restart, node replacement).

## Testing

- [`../crates/spec/`](../crates/spec/), the gate that holds these documents to the tree. It reads every document in its `DOCUMENTS` table (register a spec there when adding it) and every crate in its `CRATES` table, and checks:
  - **citations**: every `§N` in the docs *and in the code's comments* resolves to a section that exists;
  - **catalogues**: each spec's invariant table names the same invariants, defined in the same sections, as the Rust copy in its crate;
  - **ranges**: a cited extent such as `E1–E21` reaches no further than the catalogue it names, and some range reaches each catalogue's last invariant;
  - **identifiers**: every backticked name shaped like one the tree would own, and every name a ```rust fence uses without declaring, exists under `crates/`;
  - **architecture**: the dependency table in [architecture](architecture.md) equals the manifests and points only up the table, and every public trait's rustdoc cites the section that states its contract, or is listed as not a seam.
- When a spec's sections move, `cargo run -p spec --bin retarget` rewrites every citation in the docs and the code from an old-to-new section map.
- [simulation-testing](simulation-testing.md), the verification practice: why the simulator is owned rather than imported, what a run varies, the four shapes a test can take, seeds and the regression corpus, and the gaps the sweeps do not yet reach.
- [`../scripts/bench-machine-cost.sh`](../scripts/bench-machine-cost.sh), the wall-clock counterpart for what virtual time cannot price: it boots a real cluster and times two machine creates, the first paying every cold-start cost, the second pricing the path. Its header holds the figures it last produced, which code comments cite. Cold and warm are separate measurements, and per-block cost is the *slope* between two image sizes (a total divided by its block count measures the intercept).

## Unfinished work

[status](status.md) is the one place unfinished work is declared, one line per item under the spec it concerns, where a reader sees its cost and blast radius; the specs themselves state only the contract. `crates/` has no `todo!()` or `unimplemented!()`, and every `#[ignore]` or `TODO`/`FIXME`/`HACK` hit is in the `spec` crate, where the word is the subject. What the specs mandate that no sweep asserts is in [simulation-testing](simulation-testing.md), *Where the sweeps do not yet reach*.

## Elsewhere in the tree

- **The clients**, two front ends on the gateway's HTTP/SSE edge, outside the cluster: [`../crates/agent-tui/`](../crates/agent-tui/), a terminal chat UI, and [`../clients/agent-vscode/`](../clients/agent-vscode/), a VS Code extension that surfaces a session as an agent in the Agents window. Each has its own README. Neither has a spec: they hold no durable state and define no invariant; the edge spec binds them.
- [../design-principles.md](../design-principles.md), the design rubric the specs were written against; defaults for writing and reviewing code here.
- [../research/](../research/), background notes the specs cite as `DO §N`: [durable-objects](../research/durable-objects.md) and [durable-sqlite-and-filesystem](../research/durable-sqlite-and-filesystem.md).
