# Documentation

Start at the root [README](../README.md) for what the system is and how to run
it. This directory holds the specifications, the deployment guide, and the
testing conventions.

## Specifications

Normative, RFC 2119, each carrying its own invariant catalogue, and all but one
a drift test that keeps the catalogue mechanically true. The exception is
compatibility, whose table names neither a defining section nor a verifying
suite, so there is no Rust copy to hold it against: each format's own tests hold
its stamp to V1 and V2 (compatibility §3).

They layer bottom-up: every spec cites the ones beneath it by a short prefix (`core §N`, `entity §N`, and so on), citations the build resolves rather than trusts (the `spec` crate, under Testing).

| | Spec | Owns | Crates | Invariants |
|---|---|---|---|---|
| substrate | [distributed-actor-spec](distributed-actor-spec.md) | actors, messages, transport, membership, SWIM, supervision, receptionist, deterministic simulation | `actor-core`, `actor-serialization`, `actor-cluster`, `actor-runtime`, `actor-simulation` | #1–#22 |
| | [cluster-utilities-spec](cluster-utilities-spec.md) | rendezvous placement, group routers, the cluster singleton | `actor-cluster` | U1–U2 |
| storage | [wal-spec](wal-spec.md) | the framed, checksummed write-ahead log primitive | `wal` | L1–L4 |
| | [entity-spec](entity-spec.md) | durable objects ("entities"): the journal, the single-writer fence, shards, snapshots, the facet model | `entity` | E1–E21, F1–F4 |
| | [blob-store-spec](blob-store-spec.md) | a namespaced, content-addressed object store beside entity (standalone; no in-tree consumer yet) | `blob-store` | B1–B7 |
| consumers | [workflow-spec](workflow-spec.md) | the autonomous entity: the desired set of effects, intent before effect, orphan resolution, quiescence | `workflow` | W1–W7 |
| | [agent-spec](agent-spec.md) | the agent: an entity plus a run loop, a model seam, and a sandbox | `agent`, with `agent-anthropic` and `agent-openai` (model adapters), `agent-http`, `agent-gateway`, `agent-standalone` (the `agent-tui` client has no spec: see "Elsewhere in the tree") | A1–A8 |
| | [sandbox-spec](sandbox-spec.md) | the execution-tier model behind the agent runtime's sandbox seam | `agent-sandbox` | S1–S5 |
| | [machine-spec](machine-spec.md) | the agent's sibling: a durable lightweight VM as an entity, reached over SSH | `machine-entity`, `machine-host`, `machine-proto`, `machine-frontdoor`, `machine-standalone` | M1–M6 |
| cross-cutting | [compatibility-spec](compatibility-spec.md) | format stamps, the format registry, refusal by name | `compat`; `spec` (the drift gate, under Testing); `tenancy` (the directory of [multi-tenant-edge](multi-tenant-edge.md)) | V1–V2 |

One layer has a design note rather than a spec:

- [multi-tenant-edge](multi-tenant-edge.md), the gateway as a cluster client:
  bearer-token identity, principal-scoped session keys, the tenancy directory,
  and the trust boundary.

## Assumptions

- [hardware-envelope](hardware-envelope.md), the machine the specs are written
  against: a rented dedicated server, local NVMe, 128–256 GB, and a **1 Gbps
  uplink** that is the binding constraint on everything. Carries a 2026 latency
  table (LLM rows included, since the session's clock is the model's), the four
  inversions that follow from it, and the rules a review applies with them.
  Cited as `hw §N` wherever a performance choice rests on it. Read it before
  arguing that a technique from the slow-disk era belongs here, or that a
  bound does not.

## Guides

- [standalone-deployment](standalone-deployment.md): install
  `agent-standalone`, run a three-node cluster with the gateway in front,
  walk the failure drill, and read the full flag reference.
- [k8s/README](../k8s/README.md): the same cluster as a Kubernetes
  StatefulSet.
- [deploy/systemd/README](../deploy/systemd/README.md), the same cluster on bare
  metal: systemd units, the on-disk layout and which parts of it are rebuildable,
  the rolling restart, and how to replace a node.

## Testing

The citations run downward, and the gate below holds them to it. A spec may
still *name* a layer above it where that explains a choice: actor §9.4.3 cites
entity's group-per-shard count to motivate heartbeat coalescing,
cluster-utilities §4 and §7 cite entity to disclaim a sharding utility, and
entity names the agent three times: entity §3 separates its durability tier
from the agent's capability tiers (agent §5.6), entity §4.1 takes the agent's
sandbox as its example of an activation-scoped resource (agent §5.3), and
entity §7.17 names the agent runtime as the steps facet's reference consumer
(agent §3). The workflow spec's *argument* runs upward: it cites the agent
runtime as its motivation, its worked example, and its measure, because the
agent is the layer it generalizes. The crate dependency runs the other way,
`agent` on `workflow`, and the agent implements `Workflow` rather than
re-deriving it, so those citations point at a worked consumer. Beside the
cycle the agent keeps the environment reconciliation of agent §5.4, §5.5 and
§5.6, activation state, and so not a function of any fold, run as a pre-pass
ahead of every tick.

- [`../crates/spec/`](../crates/spec/), the drift gate for this directory, four
  checks over every document in its `DOCUMENTS` table (a spec is registered there
  as part of adding it; its `lib.rs` states each gate in full):
  - **citations**: every `§N` reference, two thousand-odd of them, resolves to a section that exists;
  - **catalogues**: each spec's invariant table is held equal to the Rust copy its crate's `conformance_catalogue.rs` compares it against, section numbers and "Verified by" included;
  - **ranges**: a cited extent such as `E1–E21` reaches no further than the catalogue it names, and across the set some range reaches each catalogue's last invariant, so a catalogue that grows is caught;
  - **identifiers**: every backticked name of a shape the tree would own exists somewhere under `crates/`, with the listed exemptions checked in the other direction too.
- [simulation-testing](simulation-testing.md), the whole verification story:
  why the simulator is owned rather than imported, the primitives, the
  strategies, the four shapes a test can take, seed sizing, the regression
  corpus, and the gaps the sweeps do not yet reach.
- [`../scripts/bench-machine-cost.sh`](../scripts/bench-machine-cost.sh), the
  wall-clock counterpart, where the simulator's virtual time cannot price
  anything: it boots a real cluster and times two machine creates against it,
  the first paying every cold-start cost and the second pricing the path. Its
  header carries the figures it last produced, and several code comments cite it
  as the source of an end-to-end number. Read cold and warm as two different
  measurements, and per-block cost as the *slope* between two image sizes; a
  total divided by its block count measures the intercept.

## Unfinished work

The tree carries no TODO backlog: a sweep of `crates/` finds zero `todo!()` and
zero `unimplemented!()`, and every hit for `#[ignore]` or `TODO`/`FIXME`/`HACK`
is in the `spec` crate, where the word is the subject rather than the marker:
two `#[ignore = "diagnostic"]` dumps that print what a parser saw when one is
being changed, and the identifier gate's own list of words that look like names
the tree would own and are not. That is deliberate. Unfinished work is
**declared in the spec that owns it**, where a reader sees its cost and its
blast radius, rather than dropped as a comment beside the code that would have
to change. So "what is left here?" is answered by reading these sections rather
than by grepping the code:

- Each layer's own deferrals, declined with their reasons: [entity
  §16](entity-spec.md), [machine §8](machine-spec.md), [agent
  §13](agent-spec.md), [sandbox §7](sandbox-spec.md),
  [cluster-utilities §7](cluster-utilities-spec.md), [wal §8](wal-spec.md).
- [simulation-testing](simulation-testing.md), *Where the sweeps do not yet
  reach*: what the specs mandate that no sweep asserts.

## Elsewhere in the tree

- **The clients**, the two front ends on the gateway's HTTP/SSE edge, neither
  of them part of the cluster:
  [`../crates/agent-tui/`](../crates/agent-tui/), a terminal chat UI, and
  [`../clients/agent-vscode/`](../clients/agent-vscode/), a VS Code
  extension that surfaces a session as an agent in the Agents window. Each
  carries its own README. Neither has a spec: they hold no durable state and
  define no invariant, so what binds them is the gateway's surface, described in
  [multi-tenant-edge](multi-tenant-edge.md).
- [../design-principles.md](../design-principles.md), the design rubric the
  specs were written against; defaults for writing and reviewing code here.
- [../research/](../research/), background notes the specs cite as `DO §N`:
  [durable-objects](../research/durable-objects.md) and
  [durable-sqlite-and-filesystem](../research/durable-sqlite-and-filesystem.md).
