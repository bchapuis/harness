# Architecture

The system runs durable, journaled agent sessions and persistent machines on a cluster of nodes that place, replicate, and fail over their state without a coordinator. This page is the map: the components, the seams between them, and how a deployment wires them together. §5 says where each kind of fact about a component is written down.

## 1. Components

Six layers, each built only on the ones above it in this table.

| Layer | Component | Crates | Contract |
|---|---|---|---|
| Substrate | Actors: messages, addressing, transport, membership, failure detection, consensus, supervision, discovery | `actor-core`, `actor-serialization`, `actor-cluster`, `actor-runtime` | [actor](distributed-actor-spec.md) |
| | Cluster utilities: rendezvous placement, group routers, the cluster singleton | `actor-cluster` | [utilities](cluster-utilities-spec.md) |
| | Deterministic simulation of every seam above | `actor-simulation` | [actor](distributed-actor-spec.md) §2.12.2 and [simulation-testing](simulation-testing.md) |
| Storage primitives | The framed, checksummed write-ahead log | `wal` | its crate docs |
| | Format stamps and refusal by name | `compat` | [compatibility](compatibility-spec.md) |
| Durable objects | Entities: the model, gates, activation, and lifecycle | `entity` | [entity](entity-spec.md) |
| | Entity storage: journal, fence, replication, shards, snapshots, blobs | `entity` | [storage](entity-storage-spec.md) |
| | Entity facets: workspace, KV, SQL, disk, alarm, steps | `entity` | [facets](entity-facets-spec.md) |
| Autonomous entities | Workflows: the desired set of effects, intent before effect, orphan resolution | `workflow` | [workflow](workflow-spec.md) |
| Products | Agents: a session entity with a run loop, a model seam, and a sandbox seam | `agent`, `agent-anthropic`, `agent-openai`, `agent-http` | [agent](agent-spec.md) |
| | Sandbox execution tiers behind the agent's sandbox seam | `agent-sandbox` | [sandbox](sandbox-spec.md) |
| | Machines: a durable lightweight VM as an entity, reached over SSH | `machine-entity`, `machine-host`, `machine-proto`, `machine-frontdoor` | [machine](machine-spec.md) |
| Edge and deployment | The HTTP/SSE edge, bearer-token identity, and the tenancy directory | `agent-gateway`, `tenancy` | [edge](edge-spec.md) |
| | Binaries: the agent node, the gateway, the machine node, the terminal client | `agent-standalone`, `agent-gateway`, `machine-standalone`, `agent-tui` | [standalone-deployment](standalone-deployment.md) |

The `spec` crate is tooling: it holds these documents to the tree (citations in docs and code resolve, named identifiers exist, invariant catalogues match their Rust copies). The VS Code extension under `clients/` is a second client of the edge, outside the cluster.

## 2. Dependencies

Every crate depends only on crates listed above it. The `spec` crate checks this table against each crate's manifest, so an edge added in a `Cargo.toml` that this table does not show, or that points down the table, fails the build. Only normal dependencies on workspace crates are listed; `?` marks an optional one.

| Crate | Depends on |
|---|---|
| `actor-serialization` | |
| `compat` | |
| `wal` | `compat` |
| `actor-core` | `actor-serialization` |
| `actor-cluster` | `actor-core`, `compat` |
| `actor-simulation` | `actor-core`, `actor-cluster` |
| `actor-runtime` | `actor-core`, `actor-serialization`, `actor-cluster`, `compat`, `wal` |
| `entity` | `actor-core`, `actor-serialization`, `actor-cluster`, `actor-simulation?`, `compat`, `wal` |
| `tenancy` | `actor-core`, `entity` |
| `workflow` | `actor-core`, `actor-serialization`, `actor-simulation?`, `entity` |
| `agent-http` | |
| `agent` | `actor-core`, `actor-simulation?`, `entity`, `workflow` |
| `agent-anthropic` | `agent` |
| `agent-openai` | `agent` |
| `machine-proto` | |
| `machine-host` | |
| `agent-sandbox` | `agent`, `machine-host?` |
| `machine-entity` | `actor-core`, `entity`, `machine-host?`, `machine-proto?` |
| `machine-frontdoor` | `entity`, `machine-host`, `machine-proto` |
| `agent-gateway` | `actor-core`, `actor-cluster`, `actor-runtime`, `entity`, `tenancy`, `agent` |
| `agent-standalone` | `actor-core`, `actor-cluster`, `actor-runtime`, `entity`, `tenancy`, `agent`, `agent-anthropic`, `agent-http`, `agent-sandbox` |
| `agent-tui` | `agent`, `agent-http` |
| `machine-standalone` | `actor-core`, `actor-cluster`, `actor-runtime`, `entity`, `machine-entity`, `machine-host`, `machine-frontdoor` |
| `spec` | |

Four edges are worth knowing before changing one:

- **`entity` on `actor-cluster` is concrete.** Entity storage replicates through actor-cluster's consensus groups (`RaftConsensus`) and implements `EntitySystem` for `ClusterSystem` itself, so the quorum tier is tied to actor-cluster rather than to a seam of its own.
- **`actor-simulation` is an optional normal dependency** of `entity`, `workflow`, and `agent`, behind each crate's `testing` feature, so their test support can be reused by the crates above them.
- **`machine-host` is shared** by machines and by the agent's microVM and container tiers: it is the guest layer under both products.
- **`machine-frontdoor` uses `entity` only for names**, and `agent-tui` uses `agent` only for its wire types.

## 3. Seams

A **plug point** is a trait a deployment or consumer implements or chooses; a **boundary** is a public interface with one implementation that separates components the code keeps apart. Each row's contract is in the cited section.

| Seam | Owner | Implemented by | Used by | Kind | Contract |
|---|---|---|---|---|---|
| `Actor`, `Handler` | `actor-core` | every actor | the actor systems | plug point | actor §2.1 |
| `ActorSystem` | `actor-core` | `LocalSystem`, `ClusterSystem`, the simulator's nodes | every layer above | plug point | actor §2.8 |
| `Clock`, `Entropy`, `Spawner` | `actor-core` | `actor-runtime` (production), `actor-simulation` (simulation) | every layer above | plug point | actor §2.12 |
| `Codec` | `actor-serialization` | JSON and postcard codecs | an actor system, chosen per system | plug point | actor §2.11 |
| `Transport` | `actor-cluster` | TCP (`actor-runtime`), simulated (`actor-simulation`) | `ClusterSystem` | plug point | actor §2.13 |
| `RegistryClient` | `actor-cluster` | an external registry's client | registry-based membership | plug point | actor §2.17 |
| `RaftWAL` | `actor-cluster` | in-memory, file-backed (`actor-runtime`) | consensus groups | plug point | actor §2.18 |
| `RaftConsensus` | `actor-cluster` | `ClusterSystem` | entity storage | boundary | actor §2.19 |
| `Authorizer` | `actor-cluster` | a deployment's policy | the receive loop | plug point | actor §2.20 |
| `EventSink` | `actor-core` | recorders, checkers, deployment sinks | every layer's events | plug point | actor §2.7 |
| `Receptionist` | `actor-core` | each actor system | service discovery, routers | boundary | actor §2.6 |
| `Router`, `SingletonProxy` | `actor-cluster` | `actor-cluster` | layers needing placement or one instance | boundary | utilities §2.2, §2.3 |
| `Wal` | `wal` | `wal` | file-backed entity stores and Raft logs | boundary | the `wal` crate |
| `Format`, `Stamp` | `compat` | every persisted or wire format | each format's reader | convention | compatibility §2.1 |
| `Entity`, `EntityHandler` | `entity` | agents, machines, workflows, the tenancy directory | the entity runtime | plug point | entity §2.1 |
| `EntityRef` | `entity` | `entity` | callers of an entity | boundary | entity §2.2 |
| `EntitySystem` | `entity` | `LocalSystem`, `ClusterSystem` (in `entity`) | the entity runtime | boundary | entity §2.3 |
| `EntityStore` | `entity` | file-backed and in-memory stores | entity storage, per replica | plug point | storage §2.2 |
| `BlockingIo` | `entity` | inline, thread pool | store calls | plug point | storage §2.5 |
| `EntityJournal` | `entity` | the single-node and quorum journals | the entity host | boundary | storage §2.1 |
| `ReplicaTransport` | `entity` | `entity` | the quorum journal, between a shard leader and its replicas | boundary | storage §2.3 |
| `Facet` | `entity` | the six facets (sealed) | the entity host | boundary | facets §2.1 |
| `EntityMetrics` | `entity` | a deployment's metrics sink | the entity runtime | plug point | entity §2.4 |
| `Workflow` | `workflow` | `Agent` | the workflow engine | plug point | workflow §2.1 |
| `Model` | `agent` | `ProtocolModel` | the run loop | plug point | agent §2.1 |
| `Protocol`, `HttpPost` | `agent` | `agent-anthropic`, `agent-openai`; the deployment's HTTPS client | `ProtocolModel` | plug point | agent §2.2 |
| `SandboxProvider`, `Sandbox` | `agent` | `TieredSandboxes` (`agent-sandbox`) | the run loop | plug point | agent §2.3, sandbox §2.1 |
| `SessionRef` | `agent` | `agent` | the gateway, parent sessions, tests | boundary | agent §2.5 |
| `MachineRuntimeProvider` | `machine-entity` | the hosted binding over `machine-host` | the machine entity | plug point | machine §2.2 |
| `MachineHost`, `MachineGuest` | `machine-host` | microVM and container mechanisms | machines, the agent's sandbox tiers | plug point | machine §2.3 |
| `MachineAuthority`, `ChannelBackend` | `machine-frontdoor` | `machine-standalone` | the SSH front door | plug point | machine §2.5 |
| `TokenVerifier` | `agent-gateway` | static and development verifiers | the edge's authentication | plug point | edge §2.2 |

## 4. Deployments

**The agent cluster.** Three or more `agent-standalone` nodes form the cluster: each is a voting member that hosts agent sessions and the tenancy directory as entities, keeps its replicas in a file-backed store under its own data directory, reaches model providers over HTTPS, and runs each session's tools in a sandbox local to the node hosting the session. One or more `agent-gateway` processes join the same transport as non-voting, non-hosting members: each terminates bearer-token authentication and addresses session entities directly. Clients (`agent-tui`, the VS Code extension, `curl`) speak HTTP and Server-Sent Events to a gateway and never join the cluster.

```text
 clients ── HTTP/SSE ──▶ agent-gateway ──┐ (cluster client: no votes, no entities)
                                         ▼
          ┌──────────── actor transport (TCP, optional mutual TLS) ────────────┐
          ▼                         ▼                         ▼
   agent-standalone          agent-standalone          agent-standalone
   sessions, directory       sessions, directory       sessions, directory
   store, sandboxes          store, sandboxes          store, sandboxes
          │ HTTPS                   │                         │
          ▼                         ▼                         ▼
                         model providers (Anthropic, OpenAI-compatible)
```

**The machine cluster.** Three or more `machine-standalone` nodes host machine entities. Each node runs an SSH front door in process and an admin socket; a door bridges an attachment only for machines whose shard it leads, so a client tries each node's port. A machine's guest (a microVM, or a container in development) runs on the node hosting the entity and reaches the host through its guest agent.

```text
 ssh ──▶ front door (node leading the machine's shard) ──▶ guest agent ──▶ guest
                │                                                    ▲
                ▼                                                    │
         machine entity ── disk, workspace, alarm facets ── boots, captures
```

Operator guides: [standalone-deployment](standalone-deployment.md) for the agent cluster on one host, [k8s](../k8s/README.md) for a Kubernetes StatefulSet, and [systemd](../deploy/systemd/README.md) for bare metal.

## 5. Reading the specs

Each kind of fact has one home. A component's **contract** (what it promises at its seams, whatever the implementation) is its spec. **Mechanism** (how a crate meets that contract: algorithms, tunables, file layouts, the third-party technology it uses) is the crate's rustdoc, including its private modules: build it with `cargo doc --workspace --no-deps --document-private-items`, as CI does. **Verification** (which suites exercise each invariant) is the crate's Rust catalogue beside its tests, held to the spec's invariant table by the `spec` crate. This page holds only what spans components.

Every spec has the same shape: **Scope** (what the component is, what it builds on, what builds on it); **Seams** (one subsection per seam it owns: the two sides, what crosses, and the obligations on each); the **semantics** an independent implementation must reproduce; its **Invariants**; and what it **defers**. The specs cite downward, so they read bottom-up in the order of §1: a seam's subsection is where to start when a change crosses it.
