# Documentation

Start at the root [README](../README.md) for what the system is and how to run
it. This directory holds the specifications, the deployment guide, and the
testing conventions.

## Specifications

Normative, RFC 2119, each carrying its own invariant catalogue and a drift test
that keeps the catalogue mechanically true. They layer bottom-up: every spec
cites the ones beneath it by a short prefix (`core §N`, `grain §N`, and so on).

| | Spec | Owns | Invariants |
|---|---|---|---|
| substrate | [distributed-actor-spec](distributed-actor-spec.md) | actors, messages, transport, membership, SWIM, supervision, receptionist, deterministic simulation | #1–#22 |
| | [cluster-utilities-spec](cluster-utilities-spec.md) | rendezvous placement, group routers, the cluster singleton | U1–U2 |
| storage | [wal-spec](wal-spec.md) | the framed, checksummed write-ahead log primitive | — |
| | [granary-spec](granary-spec.md) | durable objects ("grains"): the journal, the single-writer fence, shards, snapshots, the facet model | G1–G20, F1–F4 |
| | [blob-store-spec](blob-store-spec.md) | a namespaced, content-addressed object store beside granary (standalone; no in-tree consumer yet) | B1–B7 |
| consumers | [agentic-harness-spec](agentic-harness-spec.md) | the agent: a grain plus a run loop, a model seam, and a sandbox | H1–H8 |
| | [sandbox-spec](sandbox-spec.md) | the execution-tier model behind the harness's sandbox seam | S1–S5 |
| | [machine-spec](machine-spec.md) | the agent's sibling: a durable lightweight VM as a grain, reached over SSH | M1–M6 |
| cross-cutting | [compatibility-spec](compatibility-spec.md) | format revisions, the boundary registry, the read-new-first policy | V1–V6 |

Two layers have a design note rather than a spec:

- [multi-tenant-edge](multi-tenant-edge.md) — the gateway as a cluster client:
  bearer-token identity, principal-scoped session keys, the tenancy directory,
  and the trust boundary.

## Assumptions

- [hardware-envelope](hardware-envelope.md) — the machine the specs are written
  against: a rented dedicated server, local NVMe, 128–256 GB, and a **1 Gbps
  uplink** that is the binding constraint on everything. Carries a 2026 latency
  table (LLM rows included, since the session's clock is the model's), the four
  inversions that follow from it, and the rules a review applies with them.
  Cited as `hw §N` wherever a performance choice rests on it. Read it before
  arguing that a technique from the slow-disk era belongs here — or that a
  bound does not.

## Guides

- [standalone-deployment](standalone-deployment.md) — install
  `harness-standalone`, run a three-node cluster with the gateway in front,
  walk the failure drill, and read the full flag reference.
- [k8s/README](../k8s/README.md) — the same cluster as a Kubernetes
  StatefulSet.

## Testing

- [simulation-testing](simulation-testing.md) — the whole verification story:
  why the simulator is owned rather than imported, the primitives, the
  strategies, the four shapes a test can take, seed sizing, the regression
  corpus, and the gaps the sweeps do not yet reach.

## Elsewhere in the tree

- [../design-principles.md](../design-principles.md) — the design rubric the
  specs were written against; defaults for writing and reviewing code here.
- [../TODO.md](../TODO.md) — work that survived a review with a reason to exist
  and no fix yet, each entry saying what would close it. Not a bug list.
- [../research/](../research/) — background notes the specs cite as `DO §N`:
  [durable-objects](../research/durable-objects.md) and
  [durable-sqlite-and-filesystem](../research/durable-sqlite-and-filesystem.md).
