# Documentation

Start at the root [README](../README.md) for what the system is and how to run it. This directory holds the specifications, the deployment guide, and the testing conventions.

## Specifications

Normative (RFC 2119), each with its own invariant catalogue, and each but one held to it by a drift test. The exception is compatibility: its table names no defining section and no verifying suite, so there is no Rust copy to check it against; each format's own tests hold its stamp to V1 and V2 (compatibility §3).

The specs layer bottom-up: each cites the ones beneath it by a short prefix (`core §N`, `entity §N`, and so on), and the build resolves every citation (the `spec` crate, under Testing).

| | Spec | Owns | Crates | Invariants |
|---|---|---|---|---|
| substrate | [distributed-actor-spec](distributed-actor-spec.md) | actors, messages, transport, membership, SWIM, supervision, receptionist, deterministic simulation | `actor-core`, `actor-serialization`, `actor-cluster`, `actor-runtime`, `actor-simulation` | #1–#22 |
| | [cluster-utilities-spec](cluster-utilities-spec.md) | rendezvous placement, group routers, the cluster singleton | `actor-cluster` | U1–U2 |
| storage | [wal-spec](wal-spec.md) | the framed, checksummed write-ahead log primitive | `wal` | L1–L4 |
| | [entity-spec](entity-spec.md) | durable objects ("entities"): the journal, the single-writer fence, shards, snapshots, the facet model | `entity` | E1–E21, F1–F4 |
| consumers | [workflow-spec](workflow-spec.md) | the autonomous entity: the desired set of effects, intent before effect, orphan resolution, quiescence | `workflow` | W1–W7 |
| | [agent-spec](agent-spec.md) | the agent: an entity plus a run loop, a model seam, and a sandbox | `agent`, with `agent-anthropic` and `agent-openai` (model adapters), `agent-http`, `agent-gateway`, `agent-standalone` (the `agent-tui` client has no spec: see "Elsewhere in the tree") | A1–A7 |
| | [sandbox-spec](sandbox-spec.md) | the execution-tier model behind the agent runtime's sandbox seam | `agent-sandbox` | S1–S5 |
| | [machine-spec](machine-spec.md) | the agent's sibling: a durable lightweight VM as an entity, reached over SSH | `machine-entity`, `machine-host`, `machine-proto`, `machine-frontdoor`, `machine-standalone` | M1–M6 |
| cross-cutting | [compatibility-spec](compatibility-spec.md) | format stamps, the format registry, refusal by name | `compat`; `spec` (the drift gate, under Testing); `tenancy` (the directory of [multi-tenant-edge](multi-tenant-edge.md)) | V1–V2 |

One layer has a design note rather than a spec:

- [multi-tenant-edge](multi-tenant-edge.md), the gateway as a cluster client: bearer-token identity, principal-scoped session keys, the tenancy directory, and the trust boundary.

## Assumptions

- [hardware-envelope](hardware-envelope.md), the machine the specs are written against: a rented dedicated server, local NVMe, 128–256 GB, and a **1 Gbps uplink** that is the binding constraint on everything. It carries a 2026 latency table (with LLM rows, since the session's clock is the model's), the four inversions that follow from it, and the rules a review applies with them. Cited as `hw §N` wherever a performance choice rests on it; read it before importing a slow-disk-era technique or dropping a bound.

## Guides

- [standalone-deployment](standalone-deployment.md): install `agent-standalone`, run a three-node cluster behind the gateway, walk the failure drill, and read the flag reference.
- [k8s/README](../k8s/README.md): the same cluster as a Kubernetes StatefulSet.
- [deploy/systemd/README](../deploy/systemd/README.md): the same cluster on bare metal (systemd units, the on-disk layout and which parts are rebuildable, the rolling restart, node replacement).

## Testing

Citations run downward, and the gate below enforces it. A spec may still *name* a higher layer to explain a choice (actor §9.4.3 cites entity's group-per-shard count to motivate heartbeat coalescing; entity §3, §4.1, and §7.17 use the agent as an example). The workflow spec's argument runs upward on purpose: it generalizes the agent, so it cites the agent as motivation, worked example, and measure, while the crate dependency runs `agent` on `workflow` and the agent implements `Workflow`.

- [`../crates/spec/`](../crates/spec/), the drift gate for this directory: four checks over every document in its `DOCUMENTS` table (register a spec there when adding it; its `lib.rs` states each gate in full):
  - **citations**: every `§N` reference resolves to a section that exists;
  - **catalogues**: each spec's invariant table equals the Rust copy in its crate's `conformance_catalogue.rs`, section numbers and "Verified by" included;
  - **ranges**: a cited extent such as `E1–E21` reaches no further than the catalogue it names, and some range in the set reaches each catalogue's last invariant, so a growing catalogue is caught;
  - **identifiers**: every backticked name shaped like one the tree would own, and every name a ```rust fence uses without declaring, exists under `crates/`; the exemptions are checked in the other direction too.
- [simulation-testing](simulation-testing.md), the verification story: why the simulator is owned rather than imported, its primitives and strategies, the four shapes a test can take, seed sizing, the regression corpus, and the gaps the sweeps do not yet reach.
- [`../scripts/bench-machine-cost.sh`](../scripts/bench-machine-cost.sh), the wall-clock counterpart for what virtual time cannot price: it boots a real cluster and times two machine creates, the first paying every cold-start cost, the second pricing the path. Its header holds the figures it last produced, which code comments cite. Cold and warm are separate measurements, and per-block cost is the *slope* between two image sizes (a total divided by its block count measures the intercept).

## Unfinished work

Unfinished work is **declared in the spec that owns it**, where a reader sees its cost and blast radius, not left as a comment in the code. `crates/` has no `todo!()` or `unimplemented!()`, and every `#[ignore]` or `TODO`/`FIXME`/`HACK` hit is in the `spec` crate, where the word is the subject: two `#[ignore = "diagnostic"]` parser dumps and the identifier gate's exemption list. To see what is left, read:

- Each layer's declined deferrals, with reasons: [entity §16](entity-spec.md), [machine §8](machine-spec.md), [agent §13](agent-spec.md), [sandbox §7](sandbox-spec.md), [cluster-utilities §7](cluster-utilities-spec.md), [wal §8](wal-spec.md).
- [simulation-testing](simulation-testing.md), *Where the sweeps do not yet reach*: what the specs mandate that no sweep asserts.

## Elsewhere in the tree

- **The clients**, two front ends on the gateway's HTTP/SSE edge, outside the cluster: [`../crates/agent-tui/`](../crates/agent-tui/), a terminal chat UI, and [`../clients/agent-vscode/`](../clients/agent-vscode/), a VS Code extension that surfaces a session as an agent in the Agents window. Each has its own README. Neither has a spec: they hold no durable state and define no invariant; the gateway surface in [multi-tenant-edge](multi-tenant-edge.md) binds them.
- [../design-principles.md](../design-principles.md), the design rubric the specs were written against; defaults for writing and reviewing code here.
- [../research/](../research/), background notes the specs cite as `DO §N`: [durable-objects](../research/durable-objects.md) and [durable-sqlite-and-filesystem](../research/durable-sqlite-and-filesystem.md).
