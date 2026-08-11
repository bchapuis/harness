# Documentation

Start at the root [README](../README.md) for what the system is and how to run
it. This directory holds the specifications, the deployment guide, and the
testing conventions.

## Specifications

Normative, RFC 2119, each carrying its own invariant catalogue and a drift test
that keeps the catalogue mechanically true. They layer bottom-up: every spec
cites the ones beneath it by a short prefix (`core §N`, `grain §N`, and so on) —
citations the build resolves rather than trusts (`spec-xref`, under Testing).

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

One layer has a design note rather than a spec:

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
- [deploy/systemd/README](../deploy/systemd/README.md) — the same cluster on bare
  metal: systemd units, the on-disk layout and which parts of it are rebuildable,
  the rolling restart, and how to replace a node.

## Testing

- [`../crates/spec-xref/`](../crates/spec-xref/) — the drift gate for this
  directory. The layering above is made of citations, two thousand-odd of them,
  and `cargo test -p spec-xref` resolves every one against a section that
  exists. It is the `docs/` analogue of a crate's `conformance_catalogue.rs`,
  and it is there for the same reason: renumbering a section is a local edit
  whose citations are not local, so prose that points at nothing fails nothing.
  A document missing from its `DOCUMENTS` table is checked by nothing, so a new
  spec is registered there — with the convention its own unprefixed `§N` follows
  — as part of adding it.

  It also reads each spec's **invariant catalogue**, so the table a reader trusts
  and the machine-readable one beside the suite are held equal rather than
  maintained twice. Each crate's `conformance_catalogue.rs` runs the comparison,
  because a Rust catalogue lives in a `tests/` module only its own crate can
  import. All eight are compared on which invariants exist and which sections
  define them, and seven on their "Verified by" column as well — by file for
  granary, utilities, and sandbox, by test function for wal, blob, and machine,
  whichever the spec itself names. The two that are not say why at the site: the
  core catalogue names no suites at all (actor §18.6 carries that), and the
  harness's column is prose naming nothing a build can resolve.

  Its third gate is the **identifiers**. These documents are written in the
  code's vocabulary — `GrainHandler`, `ReadReply::head`, `load_snapshot()` — and
  a rename moves the code while the prose keeps the old word. Every backticked
  token of a shape the tree would own is checked against an index of every name
  declared under `crates/`. The index is a scan rather than rustdoc, because
  rustdoc's JSON needs nightly and the toolchain is pinned; it knows a name
  exists somewhere, not where or what it is, which is the whole of what the
  check claims. Names that are right and absent — std's, another system's, a
  table column, and the ones a spec writes to say the tree has no such thing —
  are listed with their reasons, and the list is checked in that direction too:
  an exemption the tree starts defining fails the build, so it cannot outlive
  its reason.
- [simulation-testing](simulation-testing.md) — the whole verification story:
  why the simulator is owned rather than imported, the primitives, the
  strategies, the four shapes a test can take, seed sizing, the regression
  corpus, and the gaps the sweeps do not yet reach.
- [`../scripts/bench-machine-cost.sh`](../scripts/bench-machine-cost.sh) — the wall-clock counterpart, where the
  simulator's virtual time cannot price anything: it boots a real cluster and times
  two machine creates against it, the first paying every cold-start cost and the
  second pricing the path. Its header carries the figures it last produced, and
  several code comments cite it as the source of an end-to-end number. Read cold and
  warm as two different measurements, and read per-block cost as the *slope* between
  two image sizes — a total divided by its block count measures the intercept.

## Unfinished work

The tree carries no TODO backlog: a sweep of `crates/` finds zero `todo!()`,
zero `unimplemented!()`, zero `#[ignore]`, and zero `TODO`/`FIXME`/`HACK`
comments. That is deliberate. Unfinished work is **declared in the spec that
owns it**, where a reader sees its cost and its blast radius, rather than
dropped as a comment beside the code that would have to change. The consequence
is that "what is left here?" cannot be answered by grepping the code, and is
answered by reading these sections instead:

- [compatibility §5](compatibility-spec.md) — the format machinery that waits on
  a boundary having a second revision. Every format in the tree is stamped, and
  no boundary is mid-flight; §4 records what was built and pulled back, and what
  would reopen it.
- Each layer's own deferrals, declined with their reasons: [granary
  §16](granary-spec.md), [machine §8](machine-spec.md), [harness
  §13](agentic-harness-spec.md), [sandbox §7](sandbox-spec.md),
  [cluster-utilities §7](cluster-utilities-spec.md), [wal §8](wal-spec.md).
- [simulation-testing](simulation-testing.md), *Where the sweeps do not yet
  reach* — what the specs mandate that no sweep asserts.

## Elsewhere in the tree

- [../design-principles.md](../design-principles.md) — the design rubric the
  specs were written against; defaults for writing and reviewing code here.
- [../research/](../research/) — background notes the specs cite as `DO §N`:
  [durable-objects](../research/durable-objects.md) and
  [durable-sqlite-and-filesystem](../research/durable-sqlite-and-filesystem.md).
