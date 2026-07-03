# Standalone deployment: a three-node agentic harness

This guide installs `harness-standalone` — the runnable deployment of the
agentic harness — and walks through a three-node cluster: one OS process per
node, each with its own data directory, agentic sessions backed by the
Anthropic API, and the failure drill the architecture exists for (kill a node,
watch its sessions resume on a survivor). It starts on one host and spans
machines with two flags ("Across machines").

## What runs

```
 terminal 1            terminal 2            terminal 3
 ┌────────────────┐    ┌────────────────┐    ┌────────────────┐
 │ node 1 (silo)  │    │ node 2 (silo)  │    │ node 3 (silo)  │
 │ transport :7401│◄──►│ transport :7402│◄──►│ transport :7403│
 │ --data node1/  │    │ --data node2/  │    │ --data node3/  │
 └───────┬────────┘    └────────────────┘    └────────────────┘
         │   each node its own --data; the journal replicates
         │   over the transport links above (a quorum per grain).
         │   Nodes host grains and vote in Raft; no client listener.
 terminal 4 (joins the transport as a non-voting client)
 ┌────────────────────┐                    api.anthropic.com
 │ harness-gateway     │                   ▲ (one HTTPS call
 │ HTTP :8080 ─► grains│                   │  per model step,
 │ transport :7500     │                   │  from the owning node)
 └────────────────────┘
```

Each `node` process is a full cluster member: a TCP transport on loopback, a
static three-node roster with the SWIM failure detector running observe-only,
and one node's harness — host actor, session actors, and the three seams:

- **Model** — the Anthropic Messages API (`harness-anthropic`), over a small
  tokio/rustls HTTP client.
- **GrainJournal** — a file-backed store in each node's own `--data` directory.
  The journal is one logical store for the whole cluster (spec §6.1), but it is
  *replicated*, not shared: a session's record is appended to a quorum of the
  shard's replicas over the transport, fenced by the shard's term (spec §7.2,
  §8). A node writes only its own directory, so nodes share neither a directory
  nor a filesystem. It is durable, which is what makes the failure drill work:
  a new owner recovers a grain's head from a quorum on activation (§8, G14).
- **Sandbox** — one private workspace directory per session under
  `--data/workspaces`, where the `shell` tool runs.

Sessions are placed by rendezvous hashing over the members every node
currently considers serving. The gateway holds a `GrainRef` per session and the
transport routes each `ask` to the session's current owner, so the gateway
addresses any session regardless of which node hosts it.

Two agent kinds are registered on every node:

| kind        | tools                        | delegates to | budget             |
|-------------|------------------------------|--------------|--------------------|
| `assistant` | `shell`, `run_js`, `delegate` | `worker`    | 200k tokens, 50 steps |
| `worker`    | `shell`, `run_js`            | —            | 100k tokens, 25 steps |

`shell` is the Native tier (the container or microVM `--sandbox` selected);
`run_js` is the hermetic QuickJS Compute tier (sandbox spec §3.2), so the
model runs JavaScript without any language runtime in the shell image. Both
shell-capable modes back the workspace with a durable filesystem grain
(granary §7.10), so a session's files survive hibernation, migration, and node
loss. The runtime-free `--sandbox durable` mode offers the typed file tools
(`read_file`/`write_file`/`list_dir`/`remove`) only — a durable workspace with
no `shell` or `run_js`, so the `assistant`/`worker` kinds carry those tools
instead.

## Prerequisites

- Rust ≥ 1.85 (`rustup` recommended).
- An Anthropic API key in `ANTHROPIC_API_KEY`.
- macOS or Linux. Each node keeps its own `--data` directory; the journal
  replicates over the transport, so nodes need not share a filesystem (see
  "Across machines" below).

## Install

From the repository root:

```sh
cargo install --path crates/harness-standalone
```

Or run uninstalled with `cargo run -p harness-standalone --` in place of
`harness-standalone` below.

## Start the cluster

One terminal per node, each with its own `--data` directory. `--sandbox`
is required, and with a real API key the confined mode is the right one (a
real model composes the shell commands these nodes will run):

```sh
export ANTHROPIC_API_KEY=sk-ant-…
docker pull python:3.12-slim   # once; the first shell call would otherwise eat its timeout

# terminal 1
harness-standalone node --id 1 --data ./harness-data/node1 --client 100=127.0.0.1 --sandbox docker --sandbox-image python:3.12-slim
# terminal 2
harness-standalone node --id 2 --data ./harness-data/node2 --client 100=127.0.0.1 --sandbox docker --sandbox-image python:3.12-slim
# terminal 3
harness-standalone node --id 3 --data ./harness-data/node3 --client 100=127.0.0.1 --sandbox docker --sandbox-image python:3.12-slim
```

On one host the nodes find each other on loopback (the default). Nothing is
shared between the three directories; the journal replicates over the
transport. `--client 100=127.0.0.1` admits the gateway (node id 100, outside the
`1..=3` voter roster) as a non-voting member, so it can join and route through
the receptionist gossip.

### Across machines

The roster spans hosts once each node binds a reachable interface and learns
its peers by name instead of loopback. Two flags do it, the same on every
node except `--id`:

- `--bind-host 0.0.0.0` — bind every interface, not just loopback.
- `--peer <id>=<host>` — the reachable host of each node in the roster; repeat
  for the whole roster. A node advertises its own entry to the others and
  dials them at theirs.

```sh
# on host-a (the others are identical but for --id):
harness-standalone node --id 1 --data ./harness-data \
  --bind-host 0.0.0.0 \
  --peer 1=host-a --peer 2=host-b --peer 3=host-c \
  --sandbox docker --sandbox-image python:3.12-slim
```

A hostname resolves through the system resolver, so container or pod DNS names
(`harness-0.harness.default.svc`, say) work directly. The transport stays
plaintext, guarded by `--secret`; keep the roster on a trusted network, or
provision TLS, before crossing untrusted links (see Limitations).

Any image works; the choice is just what `shell` finds on its `PATH`.
`python:3.12-slim` carries python3, bash, and coreutils — enough for the
model to actually run things. `alpine:3.20` is leaner (sh + awk only) if you
only need file and text tasks.

Without docker (or `/dev/kvm` for firecracker), run `--sandbox durable`: a
grain-backed durable workspace with the typed file tools and no shell — no
container runtime required.

Each node logs its bootstrap and then the cluster's life on stderr:

```
[node-2] transport 127.0.0.1:7402, data ./harness-data, model claude-sonnet-4-6
[node-2] cluster ready (leader elected)
[node-2] hosting grains; the public edge is harness-gateway (a cluster client). No client-facing listener on this node.
```

## Start the gateway

The gateway is the public edge. It joins the same cluster as a non-voting
client (node id 100, the one the nodes admitted with `--client`) and serves
HTTP/SSE on `127.0.0.1:8080`:

```sh
# terminal 4  (build with `cargo build -p harness-gateway`, or `cargo run -p harness-gateway --`)
harness-gateway --bind 127.0.0.1:8080 --node-id 100 --nodes 3
```

It logs `joined the cluster (client of nodes 1..=3)` once it discovers a host
gateway through the receptionist gossip. With no `--auth-tokens`, the bearer
token is taken as the tenant, unverified — loopback dev mode.

## Talk to it

Drive sessions over the gateway's HTTP API. The bearer token names the tenant;
every session is scoped under it. Stream a turn as Server-Sent Events:

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

Drop the `Accept: text/event-stream` header to block and get the final outcome
as JSON (`{"outcome":{"Ok":{…}}}`). The endpoints:

| method + path                                   | effect |
|-------------------------------------------------|--------|
| `POST /v1/{kind}/{session}/prompt`              | submit a turn (`{"turn","content","within_secs"}`); SSE with `Accept: text/event-stream` |
| `GET  /v1/{kind}/{session}/records?from=&limit=`| read a page of the journal |
| `GET  /v1/{kind}/{session}/stream?turn=&from=`  | observe a run live (SSE) |
| `POST /v1/{kind}/{session}/cancel?turn=`        | cancel a run (idempotent) |
| `GET  /v1/sessions?kind=`                       | this tenant's sessions |

Re-issuing the **same** `turn` id is the resume primitive: a completed run
returns its recorded outcome, a live run re-attaches, never run twice (H7).

The records *are* the session: each is a line in
`./harness-data/grains/<shard>/<session>/`, and folding them back is how any
node reconstructs the session — there is no other session state.

Sessions are durable and named: prompt `demo` again days later (any gateway
replica, any node) and it resumes the same transcript.

Delegation works out of the box: ask the assistant to farm something out and
it calls the `delegate` tool; the child runs as its own session of the
`worker` kind — possibly on a different node — with a budget carved from the
parent's, and the records show the `delegated to …` entry.

## The failure drill

This is the deployment's reason to exist: a session survives the machine
that was running it.

1. Submit a long-ish turn, e.g.
   `Write a fibonacci script, run it for n=1..20, and summarize the timing.`
2. Find the owner: the node whose stderr shows
   `RunStarted { session: SessionId("demo"), … }`.
3. Kill that process (`Ctrl-C` or `kill <pid>`).
4. Watch the survivors: within a few seconds (SWIM defaults: 1s probes, 3s
   suspicion) they log `Suspected { … }` then `Unreachable { … }`, and
   placement stops routing to the dead node.
5. Re-issue the **same** prompt through the gateway (same `turn` id) — its
   `GrainRef` re-resolves to a live owner automatically:
   ```sh
   curl -X POST http://127.0.0.1:8080/v1/assistant/demo/prompt \
     -H 'Authorization: Bearer alice' -H 'Content-Type: application/json' \
     -d '{"turn":"t-1","content":"…the same turn…"}'
   ```
6. The new owner recovers the grain's head from a quorum, folds the journal,
   resumes the run from its last committed record, and the outcome comes back as
   if nothing happened. A tool call that was in flight when the node died is
   resolved per its declared policy (the `shell` tool interrupts: the model is
   told and decides whether to re-run it).
7. Restart the node (the same `node --id N …` command, same flags — the
   sandbox mode shapes the kind digest existing sessions have pinned):
   survivors log `Reachable`, and new sessions place onto it again.

Two details make this honest rather than staged: the journal's fenced append
means even a *not actually dead* node (a partition, a pause) cannot fork the
transcript — the old owner's next append loses the fence and deactivates —
and re-submitting a turn id is always safe: a completed run returns its
recorded outcome, a live run is re-attached, never run twice (invariant H7).

## Configuration

```
harness-standalone node --id <n> [options]
harness-gateway [options]                  # the public HTTP edge
```

| flag             | default                     | notes |
|------------------|-----------------------------|-------|
| `--id <n>`       | required                    | 1..=`--nodes` |
| `--nodes <n>`    | `3`                         | roster size; agree everywhere |
| `--data <dir>`   | `./harness-data`            | this node's own journal + workspaces |
| `--bind-host <addr>` | `127.0.0.1`             | interface the transport binds; `0.0.0.0` in a container |
| `--peer <id>=<host>` | all `127.0.0.1`         | each node's reachable host; repeat for the roster |
| `--client <id>=<host>` | —                     | admit a non-voting cluster client (the gateway); id outside `1..=--nodes`. Repeatable |
| `--port-base <p>`| `7401`                      | node/client *i*'s transport = p+i−1 |
| `--model <id>`   | `claude-sonnet-4-6`         | agree everywhere (kind digests are pinned per session) |
| `--secret <s>`   | `harness-standalone`        | cluster association secret |
| `--api-url <url>`| `https://api.anthropic.com` | `http://…` points at a fake for offline testing |
| `--sandbox <mode>` | — (required)              | `docker` or `firecracker` (confined shell, durable workspace), or `durable` (typed file tools, no shell); agree everywhere (the choice shapes the kind digest) |
| `--sandbox-image <r>` | —                       | container image for `--sandbox docker` (required there); agree everywhere |
| `--container-cli <c>` | `docker`                | the container CLI binary (podman's compatible CLI works) |
| `--fc-binary <path>` | `firecracker`            | the VMM executable for `--sandbox firecracker` |
| `--fc-kernel <path>` | —                        | vmlinux for `--sandbox firecracker` (required there); agree everywhere |
| `--fc-rootfs <path>` | —                        | base rootfs with `/sbin/fc-agent` (required there); agree everywhere |

Environment: `ANTHROPIC_API_KEY` (required by `node`).

Every flag marked "agree everywhere" is deployment configuration in the
spec's sense (§7.1): all nodes must be started with the same values.

The **gateway** (`harness-gateway`) takes: `--bind <host:port>` (public HTTP,
default `127.0.0.1:8080`), `--secret` / `--node-id` / `--nodes` / `--peer` /
`--port-base` (the transport join, mirroring the nodes; `--node-id` must be
outside `1..=--nodes` and admitted by the nodes' `--client`),
`--advertise-host` (the host the nodes dial it back at), and `--auth-tokens`
(a tenants file; without it the bearer token is the tenant, loopback only).

`crates/harness-standalone/smoke.sh` scripts the whole walkthrough — three
nodes plus the gateway against a canned fake API, prompt, records, kill, resume
over HTTP — and needs no API key.

## How it maps to the spec

| this deployment | agentic harness spec |
|---|---|
| `--data/grains`, a quorum append per grain fenced by the shard term | the fenced, per-session journal (§6.1–§6.2, §7.2); the journal **is** the session (§2.1) |
| killing a node, `:retry` | caller-driven resumption (§7.5), idempotent turns (H7) |
| the gateway's `GrainRef` reaches any session | placement is routing, not a lease (utilities spec §2.3); exclusivity lives in the fence |
| `--data/workspaces/<session>` | the sandbox seam (§5.3); working state, not session state (§5.5) |
| `assistant` → `delegate` → `worker` | delegation with budget carve-outs (§8, §9.1) |
| stderr `RunStarted`/`SessionActivated`/… | the observability stream (§10.4) |

## Limitations

- **The transport is plaintext.** Fine on loopback; the transport supports
  mutual TLS (`TcpConfig.tls`) but this deployment does not provision
  certificates. Do not point the roster across untrusted networks.
- **`--sandbox` must be chosen explicitly; there is no unconfined mode.** Pass
  `--sandbox docker --sandbox-image <ref>` (e.g. `alpine:3.20`) to run `shell`
  inside a per-session OCI container, via `harness-sandbox`'s `Native` tier:
  the workspace bind-mounted, no network — shared-kernel confinement (sandbox
  spec §3.4's SHOULD grade), still not the microVM grade. Pre-pull the image:
  the first call otherwise pulls it inside the 120s tool timeout. On Linux with
  `/dev/kvm`, pass `--sandbox firecracker --fc-kernel <vmlinux> --fc-rootfs
  <ext4>` (both built by `guest/fc-rootfs/build.sh`) for the microVM grade
  instead: one Firecracker VM per activation, the workspace synced over vsock,
  no network device (sandbox spec §3.5's reference choice). Both back the
  workspace with a durable filesystem grain, so a session's files survive
  hibernation, migration, and node loss. Where no container runtime is
  available, `--sandbox durable` gives the same durable workspace through typed
  file tools with no shell.
- **Each node keeps its own `--data` directory**; the journal replicates over
  the transport (a quorum append per grain, §7.2), so the roster spans machines
  — set `--bind-host` and `--peer` (see "Across machines"). The store is local
  to each node, never shared.
- **The fake-friendly `--api-url`** speaks HTTP/1.1 without retry-relevant
  streaming; long completions are bounded by a 300s per-request timeout
  under the model client's retry policy.
