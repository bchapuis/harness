# Standalone deployment: a three-node agent runtime

Install `agent-standalone`, the runnable agent runtime, and run a three-node cluster (one OS process and data directory per node, sessions backed by the Anthropic API) through the failure drill: kill a node, watch its sessions resume on a survivor. It starts on one host; two flags span machines ("Across machines").

## What runs

```
 terminal 1            terminal 2            terminal 3
 ┌────────────────┐    ┌────────────────┐    ┌────────────────┐
 │ node 1 (silo)  │    │ node 2 (silo)  │    │ node 3 (silo)  │
 │ transport :7401│◄──►│ transport :7402│◄──►│ transport :7403│
 │ --data node1/  │    │ --data node2/  │    │ --data node3/  │
 └───────┬────────┘    └────────────────┘    └────────────────┘
         │   each node its own --data; the journal replicates
         │   over the transport links above (a quorum per entity).
         │   Nodes host entities and vote in Raft; no client listener.
 terminal 4 (joins the transport as a non-voting client)
 ┌────────────────────┐                    api.anthropic.com
 │ agent-gateway     │                   ▲ (one HTTPS call
 │ HTTP :8080 ─► entities│                   │  per model step,
 │ transport :7500     │                   │  from the owning node)
 └────────────────────┘
```

Each `node` process is a full cluster member: a TCP transport on loopback, a voter in the fixed three-node roster of the leader-based control plane (Raft, with SWIM as its failure sensor and conservative downing; cluster spec §5.4.3), and one node's agent runtime, its host actor, its session actors, the two seams (model, sandbox; agent §3.2), and the entity's journal:

- **Model**: the Anthropic Messages API (`agent-anthropic`) over a small tokio/rustls HTTP client.
- **EntityJournal**: a file-backed store in the node's own `--data` directory. The journal is one logical store for the cluster (spec §11.2) but *replicated*, not shared: a session's record is appended to a quorum of the shard's replicas over the transport, fenced by the shard's term (storage §5, §6). A new owner recovers an entity's head from a quorum on activation (storage §9, E14).
- **Sandbox**: one private workspace directory per session under `--data/workspaces/<node-id>/`, keyed by the entity, where the `shell` tool runs.

A session's name hashes to a shard; the consensus-agreed shard map names the shard's replicas, and the session activates on the shard's current leader (agent §7.1). The gateway holds an `EntityRef` per session, and the transport routes each `ask` to the current owner.

Two agent kinds are registered on every node:

| kind        | tools                                    | delegates to | budget             |
|-------------|------------------------------------------|--------------|--------------------|
| `assistant` | the file tools, `shell`, `run_js`, `delegate` | `worker` | 200k tokens, 50 steps |
| `worker`    | the file tools, `shell`, `run_js`        | none            | 100k tokens, 25 steps |

Every `--sandbox` mode carries the typed file tools (`read_file`, `write_file`, `edit_file`, `list_dir`, `remove`) over the session entity's workspace facet (facets §5.1), so its files survive hibernation, migration, and node loss. The shell-capable modes add `shell`, the Native tier (the container or microVM `--sandbox` selected), and `run_js`, the hermetic QuickJS Compute tier (sandbox spec §5.2), which runs JavaScript with no language runtime in the shell image. `--sandbox durable` has the file tools alone: the rows above lose `shell` and `run_js`.

## Prerequisites

- Rust ≥ 1.88 (the workspace MSRV; `rustup` recommended). Building the tree's tests needs the toolchain pinned in `rust-toolchain.toml` instead.
- An Anthropic API key in `ANTHROPIC_API_KEY`.
- macOS or Linux. Nodes share no filesystem (see "Across machines").

## Install

From the repository root:

```sh
cargo install --path crates/agent-standalone
```

Or run uninstalled with `cargo run -p agent-standalone --` in place of `agent-standalone` below.

## Start the cluster

One terminal per node, each with its own `--data` directory. `--sandbox` is required; with a live API key use a confined mode, since a real model composes the shell commands these nodes run:

```sh
export ANTHROPIC_API_KEY=sk-ant-…
docker pull python:3.12-slim   # once; the first shell call would otherwise eat its timeout

# terminal 1
agent-standalone node --id 1 --data ./agent-data/node1 --client 100=127.0.0.1 --sandbox docker --sandbox-image python:3.12-slim
# terminal 2
agent-standalone node --id 2 --data ./agent-data/node2 --client 100=127.0.0.1 --sandbox docker --sandbox-image python:3.12-slim
# terminal 3
agent-standalone node --id 3 --data ./agent-data/node3 --client 100=127.0.0.1 --sandbox docker --sandbox-image python:3.12-slim
```

On one host the nodes find each other on loopback (the default). `--client 100=127.0.0.1` admits the gateway (node id 100, outside the `1..=3` voter roster) as a non-voting member, so it can join and route through the receptionist gossip.

Any image works; it decides what `shell` finds on its `PATH`. `python:3.12-slim` carries python3, bash, and coreutils, enough for real work; `alpine:3.20` is leaner (sh + awk only) for file and text tasks. Without docker (or `/dev/kvm` for firecracker), run `--sandbox durable`: an entity-backed durable workspace with the typed file tools, no shell, and no container runtime.

Each node logs its bootstrap and then the cluster's life on stderr:

```
[node-2] joined (voter 2/3), data ./agent-data, model claude-sonnet-4-6
[node-2] cluster ready (leader elected) after 1.2s
[node-2] hosting entities; the public edge is agent-gateway (a cluster client). No client-facing listener on this node.
```

### Across machines

Two flags span hosts, the same on every node except `--id`:

- `--bind-host 0.0.0.0`: bind every interface, not just loopback.
- `--peer <id>=<host>`: the reachable host of each node; repeat for the whole roster. A node advertises its own entry to the others and dials them at theirs.

```sh
# on host-a (the others are identical but for --id):
agent-standalone node --id 1 --data ./agent-data \
  --bind-host 0.0.0.0 \
  --peer 1=host-a --peer 2=host-b --peer 3=host-c \
  --sandbox docker --sandbox-image python:3.12-slim
```

Hostnames go through the system resolver, so container or pod DNS names (`agent-0.agent.default.svc`, say) work directly. Read Limitations before crossing untrusted links: the transport is plaintext.

## Start the gateway

The gateway is the public edge: it joins the cluster as a non-voting client (node id 100, admitted by the nodes' `--client`) and serves HTTP/SSE on `127.0.0.1:8080`:

```sh
# terminal 4  (build with `cargo build -p agent-gateway`, or `cargo run -p agent-gateway --`)
agent-gateway --bind 127.0.0.1:8080 --node-id 100 --nodes 3
```

It logs `[gateway node-100] joined the cluster (client of nodes 1..=3)` on joining the transport, before discovery. `gateway listening on 127.0.0.1:8080` means discovery finished: the HTTP listener binds only after a host gateway resolves through the receptionist gossip. With no `--auth-tokens`, the bearer token is taken as the tenant, unverified (loopback dev mode).

## Talk to it

Drive sessions over the gateway's HTTP API; the bearer token names the tenant, and every session is scoped under it. Stream a turn as Server-Sent Events:

```sh
curl -N -X POST http://127.0.0.1:8080/v1/assistant/demo/prompt \
  -H 'Authorization: Bearer alice' -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  -d '{"turn":"t-1","content":"Create a file named numbers.txt that holds 1..10, then tell me their sum."}'
```

```
event: records   {"… one committed record per batch …"}
event: outcome   {"Ok":{"content":"…Their sum is 55.","tokens":1843}}
```

Drop the `Accept: text/event-stream` header to block and get the final outcome as JSON (`{"outcome":{"Ok":{…}}}`). The endpoints:

| method + path                                   | effect |
|-------------------------------------------------|--------|
| `POST /v1/{kind}/{session}/prompt`              | submit a turn (`{"turn","content","within_secs"}`); SSE with `Accept: text/event-stream` |
| `GET  /v1/{kind}/{session}/records?from=&limit=`| read a page of the journal |
| `GET  /v1/{kind}/{session}/stream?turn=&from=`  | observe a run live (SSE) |
| `POST /v1/{kind}/{session}/cancel?turn=`        | cancel a run (idempotent) |
| `GET  /v1/sessions?kind=`                       | this tenant's sessions |
| `GET  /healthz`                                 | liveness; 200 while the process runs, no auth |
| `GET  /readyz`                                  | readiness; 200, and 503 from the moment a shutdown signal starts the drain, so a load balancer deregisters the replica before axum finishes the in-flight requests |

Re-issuing the **same** `turn` id is the resume primitive: a completed run returns its recorded outcome, a live run re-attaches, and nothing runs twice (A6).

The records *are* the session: each is one `postcard`-encoded, length-framed, checksummed entry in the session's own segment under `./agent-data/entities/<entity_type>/<node>/segments/` (an append-only binary log, not text lines). Any node reconstructs the session by folding them; there is no other session state. The entity type comes first because each hosted type has its own store: the fence is keyed by shard index, and the same index is a different leader-election group per type (storage §4.2).

Prompt `demo` again days later, through any gateway replica and node, and it resumes the same transcript.

When the assistant farms work out it calls `delegate`: the child runs as its own `worker` session, possibly on another node, with a budget carved from the parent's. The parent's records carry a `ChildRun` entry naming the child's kind, session, turn, and carved budget; the child's result comes back as the `delegate` call's own `ToolOutcome`.

## The failure drill

A session survives the machine that was running it.

1. Submit a long-ish turn, e.g. `Write a fibonacci script, run it for n=1..20, and summarize the timing.`
2. Find the owner: the node whose stderr shows `RunStarted { session: SessionId("alice/demo"), … }` (the gateway scopes the session under its tenant, so the entity is `alice/demo`).
3. Kill that process (`Ctrl-C` or `kill <pid>`).
4. Within a few seconds (SWIM defaults: 1s probes, 3s suspicion) the survivors log `Suspected { … }` then `Unreachable { … }`, and placement stops routing to the dead node.
5. Re-issue the **same** prompt (same `turn` id) through the gateway; its `EntityRef` re-resolves to a live owner:
   ```sh
   curl -X POST http://127.0.0.1:8080/v1/assistant/demo/prompt \
     -H 'Authorization: Bearer alice' -H 'Content-Type: application/json' \
     -d '{"turn":"t-1","content":"…the same turn…"}'
   ```
6. The new owner recovers the entity's head from a quorum, folds the journal, resumes the run from its last committed record, and returns the outcome as if nothing happened. A tool call in flight when the node died resolves per its declared policy (`shell` interrupts: the model is told and decides whether to re-run it).
7. Restart the node with the same `node --id N …` command and flags (the sandbox mode shapes the kind digest existing sessions have pinned): survivors log `Reachable`, and new sessions place onto it again.

The fenced append means even a node that is *not actually dead* (a partition, a pause) cannot fork the transcript: the old owner's next append loses the fence and hibernates.

## Sizing the host

The tree's defaults target the machine in [hardware-envelope](hardware-envelope.md) (read it before changing a number here): a dedicated server with directly attached NVMe, tens of cores, 128–256 GB, and a **1 Gbps uplink**, shared by replication and public serving, that is the binding constraint.

**Per node.** Eight or more cores, 32 GB and up, and local NVMe. Undershoot and two things break first. Memory scales with entity *count*, not concurrency: the store holds its manifest whole in memory and caches segment handles and resolved host handles by the thousand. And replication rides the uplink, so a node's sustainable ingest of new data is roughly `link ÷ (replication_factor − 1)`, about 60 MB/s at 1 Gbps and R=3, against a device that would take gigabytes per second. If replication throughput binds, a 10 Gbps uplink buys more than any tuning.

**Descriptors.** The entity store keeps up to 65536 segment files open by default, plus transport connections and transient blob opens. Raise the limit to at least **70000**, or the process hits the kernel's limit before the store's cache evicts anything:

```ini
# /etc/systemd/system/agent-node.service
[Service]
LimitNOFILE=131072
```

[`deploy/systemd/`](../deploy/systemd/README.md) has ready-made units with this and the rest of the operational shape (the shutdown timeout the leadership handoff needs, the restart backoff, the sandbox caveat), plus the on-disk layout and which parts of it are rebuildable.

**Storage.** One unshared filesystem per node, on local NVMe. Network-attached block storage (EBS, Persistent Disk, Ceph) puts a durable flush back near a millisecond, reversing an assumption the storage layer is built on (hardware-envelope §6): it works, but slower than the design expects.

### Timeouts by topology

`EntityConfig::quorum_timeout` and `EntityConfig::recover_timeout` default to 2 s: safe everywhere, derived for nowhere. No flag reaches them, so tuning them is an edit to the `EntityConfig` that `agent-standalone` builds, and a rebuild. Set them from the p99.9 commit latency you measure, never from a median; the field docs say why too low is worse than too slow. Starting points, against the round trips in hardware-envelope §2:

| topology | round trip | commit p99.9 (expect) | timeouts |
|---|---|---|---|
| one datacenter | ~0.2 ms | single-digit ms | 250 ms |
| two nearby datacenters (~150 km) | ~3 ms | tens of ms | 500 ms–1 s |
| across a continent | ~25 ms | ~100 ms | 2 s (the default) |
| across continents | ~90 ms+ | hundreds of ms | 5 s+ |

### Failure domains

`EntityConfig::failure_domains` defaults to every node being its own domain, so nothing stops all *R* replicas of a shard landing in one rack, and one rack loss can take a shard below quorum on a cluster that still looks healthy. Its field docs and `FailureDomains` state the rules. On dedicated hardware the domains are racks and datacenter sites: map each host from static metadata you already have, never from anything read on the local host. A closure is not a command-line value, so this too is an edit to the `EntityConfig` that `agent-standalone` builds:

```rust
// Every node must evaluate this mapping identically.
let racks: entity::FailureDomains = std::sync::Arc::new(|node: NodeId| match node.uid() {
    1 | 2 => 0, // site A, rack 1
    3 | 4 => 1, // site A, rack 2
    _ => 2,     // site B
});
let config = EntityConfig {
    failure_domains: Some(racks),
    ..EntityConfig::default()
};
```

## Configuration

`agent-standalone node --help` and `agent-gateway --help` list every flag with its default; the binaries are the reference. Beyond the flags the walkthrough uses, four matter:

- `--model`, `--sandbox`, `--sandbox-image`, and `--fc-rootfs` shape the kind digest every session pins. Every node flag but `--id` and `--data` is deployment configuration in the spec's sense (§6.2): start all nodes with the same values.
- `--secret` is the cluster association secret; the gateway's must match the nodes'.
- `--api-url` points the nodes at a fake Messages API for offline testing.
- `--auth-tokens` gives the gateway a tenants file (`<principal> <token>` per line, reloaded on SIGHUP), required unless `--bind` is loopback.

`node` requires `ANTHROPIC_API_KEY`. SIGTERM and SIGINT drain the gateway gracefully.

`scripts/smoke-agent.sh` runs the whole walkthrough (three nodes plus the gateway against a canned fake API: prompt, records, kill, resume over HTTP) and needs no API key.

## How it maps to the spec

| this deployment | agent runtime spec |
|---|---|
| `--data/entities`, a quorum append per entity fenced by the shard term | the fenced, per-session journal (§11.2, §4.1, §7.1); the journal **is** the session (§4.1) |
| killing a node, then re-issuing the same `turn` id over HTTP | caller-driven resumption (§7.3), idempotent turns (A6) |
| the gateway's `EntityRef` reaches any session | placement is routing, not a lease (utilities spec §4.1); exclusivity lives in the fence |
| `--data/workspaces/<node-id>/…`, one directory per session entity | the sandbox seam (§3.3); working state, not session state (§5.4) |
| `assistant` → `delegate` → `worker` | delegation with budget carve-outs (§8, §9.1) |
| stderr `RunStarted`/`ModelCompleted`/`RunEnded`/… plus membership transitions | the observability stream (§10.1) |

## Limitations

- **The transport is plaintext.** Fine on loopback. The transport supports mutual TLS (`Encryption::MutualTls`, the `encryption` field of `TcpConfig`), but this deployment names `Encryption::PlaintextTrusted`, exposes no flag for the other, and provisions no certificates. Do not point the roster across untrusted networks.
- **`--sandbox` must be chosen explicitly; there is no unconfined mode.** `--sandbox docker --sandbox-image <ref>` (e.g. `alpine:3.20`) runs `shell` in a per-session OCI container via `agent-sandbox`'s `Native` tier: the workspace bind-mounted, no network, shared-kernel confinement (sandbox spec §5.4's SHOULD grade), not the microVM grade. Pre-pull the image, or the first call pulls it inside the 120s tool timeout. On Linux with `/dev/kvm`, `--sandbox firecracker --fc-kernel <vmlinux> --fc-rootfs <ext4>` (both built by `guest/fc-rootfs/build.sh`) gives the microVM grade: one Firecracker VM per activation, the workspace synced over vsock, no network device (sandbox spec §5's reference choice).
- **The fake-friendly `--api-url`** speaks HTTP/1.1 without retry-relevant streaming; long completions are bounded by a 300s per-request timeout under the model client's retry policy.
