# Standalone deployment: a three-node agentic harness on one machine

This guide installs `harness-standalone` — the runnable deployment of the
agentic harness — and walks through a local three-node cluster: one OS
process per node, agentic sessions backed by the Anthropic API, and the
failure drill the architecture exists for (kill a node, watch its sessions
resume on a survivor).

## What runs

```
 terminal 1            terminal 2            terminal 3
 ┌────────────────┐    ┌────────────────┐    ┌────────────────┐
 │ node 1         │    │ node 2         │    │ node 3         │
 │ transport :7401│◄──►│ transport :7402│◄──►│ transport :7403│
 │ control   :7501│    │ control   :7502│    │ control   :7503│
 └───────┬────────┘    └───────┬────────┘    └───────┬────────┘
         │      shared --data directory             │
         └──────┬───── journal/ ── workspaces/ ─────┘
                │
 terminal 4     │                          api.anthropic.com
 ┌──────────────┴─┐                        ▲ (one HTTPS call
 │ repl           │                        │  per model step,
 └────────────────┘                        │  from any node)
```

Each `node` process is a full cluster member: a TCP transport on loopback, a
static three-node roster with the SWIM failure detector running observe-only,
and one node's harness — host actor, session actors, and the three seams:

- **Model** — the Anthropic Messages API (`harness-anthropic`), over a small
  tokio/rustls HTTP client.
- **Journal** — a file-backed store in the shared `--data` directory. The
  journal is one logical store for the whole cluster (spec §6.1), which is
  why every node must point at the same directory; it is also durable, which
  is what makes the failure drill work. Each fenced append commits as an
  atomic `hard_link` of a fully-fsynced batch file — the conditional write of
  spec §6.2, enforced by the filesystem.
- **Sandbox** — one private workspace directory per session under
  `--data/workspaces`, where the `shell` tool runs.

Sessions are placed by rendezvous hashing over the members every node
currently considers serving. Any node accepts any request and routes it to
the session's owner, so the REPL can attach anywhere.

Two agent kinds are registered on every node:

| kind        | tools             | delegates to | budget             |
|-------------|-------------------|--------------|--------------------|
| `assistant` | `shell`, `delegate` | `worker`   | 200k tokens, 50 steps |
| `worker`    | `shell`           | —            | 100k tokens, 25 steps |

## Prerequisites

- Rust ≥ 1.85 (`rustup` recommended).
- An Anthropic API key in `ANTHROPIC_API_KEY`.
- macOS or Linux (the journal relies on POSIX hard-link semantics; all nodes
  must share a local filesystem).

## Install

From the repository root:

```sh
cargo install --path crates/harness-standalone
```

Or run uninstalled with `cargo run -p harness-standalone --` in place of
`harness-standalone` below.

## Start the cluster

One terminal per node, all pointing at the same data directory. `--sandbox`
is required, and with a real API key the confined mode is the right one (a
real model composes the shell commands these nodes will run):

```sh
export ANTHROPIC_API_KEY=sk-ant-…
docker pull python:3.12-slim   # once; the first shell call would otherwise eat its timeout

# terminal 1
harness-standalone node --id 1 --data ./harness-data --sandbox docker --sandbox-image python:3.12-slim
# terminal 2
harness-standalone node --id 2 --data ./harness-data --sandbox docker --sandbox-image python:3.12-slim
# terminal 3
harness-standalone node --id 3 --data ./harness-data --sandbox docker --sandbox-image python:3.12-slim
```

Any image works; the choice is just what `shell` finds on its `PATH`.
`python:3.12-slim` carries python3, bash, and coreutils — enough for the
model to actually run things. `alpine:3.20` is leaner (sh + awk only) if you
only need file and text tasks.

Without docker, `--sandbox local` runs `shell` directly as your user —
unconfined, trusted-input only (see Limitations); the node says so loudly at
startup.

Each node logs its bootstrap and then the cluster's life on stderr:

```
[node-2] transport 127.0.0.1:7402, data ./harness-data, model claude-sonnet-4-6
[node-2] all 3 hosts discovered
[node-2] control listening on 127.0.0.1:7502 — attach with: harness-standalone repl 127.0.0.1:7502
```

A node holds its control port closed until it has discovered every peer's
host, so the first prompt of a fresh cluster does not race membership
convergence.

## Talk to it

Attach a REPL to **any** node's control port (placement decides who actually
hosts the session, not the entry point):

```sh
harness-standalone repl 127.0.0.1:7501
```

A plain line submits a turn to the current session (`assistant/demo` by
default); `:`-commands do the rest:

```
assistant/demo> Create a file named numbers.txt that holds 1..10, then tell me their sum.
· submitted t-1 (waiting for the run; :retry re-attaches after a failure)
I created numbers.txt with the numbers 1 through 10, one per line. Their sum is 55.
· t-1 done, 1843 tokens
assistant/demo> :tail
@1 session created, kind assistant
@2 turn t-1 submitted: Create a file named numbers.txt that holds 1..10, then tell me their sum.
@3 model (792 tokens): I'll create the file and compute the sum. [calls: shell]
@4 tool tu_01 ok: {"exit_code":0,"stderr":"","stdout":"55\n"}
@5 model (1051 tokens): I created numbers.txt with the numbers 1 through 10…
@6 run t-1 ended ok (1843 tokens)
```

The `:tail` output *is* the session: every record above is a line in
`./harness-data/journal/<session>/`, and folding them back is how any node
reconstructs the session — there is no other session state.

Useful commands:

| command         | effect |
|-----------------|--------|
| `<text>`        | submit the text as the session's next turn |
| `:retry`        | re-submit the **same** turn id — re-attach after a failure |
| `:cancel`       | cancel the last submitted turn (idempotent) |
| `:tail`         | print the session's journal |
| `:session <id>` | switch session; it is created on its first turn |
| `:kind <name>`  | switch kind (`assistant` or `worker`) |
| `:quit`         | leave — the cluster and its sessions keep running |

Sessions are durable and named: `:session report-42` from any REPL, on any
node, days later, resumes the same transcript (the REPL seeds its turn
counter from the journal, so turn ids keep counting where they left off).

Delegation works out of the box: ask the assistant to farm something out and
it calls the `delegate` tool; the child runs as its own session of the
`worker` kind — possibly on a different node — with a budget carved from the
parent's, and `:tail` shows the `delegated to …` record.

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
5. If your REPL was attached to the killed node, re-attach to a survivor:
   `harness-standalone repl 127.0.0.1:7502`.
6. `:retry` — the same turn id is re-submitted. The new owner folds the
   shared journal, resumes the run from its last committed record, and the
   outcome comes back as if nothing happened. A tool call that was in flight
   when the node died is resolved per its declared policy (the `shell` tool
   interrupts: the model is told and decides whether to re-run it).
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
harness-standalone repl [host:port]        # default 127.0.0.1:7501
```

| flag             | default                     | notes |
|------------------|-----------------------------|-------|
| `--id <n>`       | required                    | 1..=`--nodes` |
| `--nodes <n>`    | `3`                         | roster size; agree everywhere |
| `--data <dir>`   | `./harness-data`            | the shared journal + workspaces |
| `--port-base <p>`| `7401`                      | node *i*'s transport = p+i−1 |
| `--control-base <p>` | `7501`                  | node *i*'s control = p+i−1 |
| `--model <id>`   | `claude-sonnet-4-6`         | agree everywhere (kind digests are pinned per session) |
| `--secret <s>`   | `harness-standalone`        | cluster association secret |
| `--api-url <url>`| `https://api.anthropic.com` | `http://…` points at a fake for offline testing |
| `--sandbox <mode>` | — (required)              | `docker`, `firecracker`, or `local` (unconfined, trusted-input only); agree everywhere (the choice shapes the kind digest) |
| `--sandbox-image <r>` | —                       | container image for `--sandbox docker` (required there); agree everywhere |
| `--container-cli <c>` | `docker`                | the container CLI binary (podman's compatible CLI works) |
| `--fc-binary <path>` | `firecracker`            | the VMM executable for `--sandbox firecracker` |
| `--fc-kernel <path>` | —                        | vmlinux for `--sandbox firecracker` (required there); agree everywhere |
| `--fc-rootfs <path>` | —                        | base rootfs with `/sbin/fc-agent` (required there); agree everywhere |

Environment: `ANTHROPIC_API_KEY` (required by `node`).

Every flag marked "agree everywhere" is deployment configuration in the
spec's sense (§7.1): all nodes must be started with the same values.

`crates/harness-standalone/smoke.sh` scripts the whole walkthrough — three
nodes against a canned fake API, prompt, tail, kill, resume — and needs no
API key.

## How it maps to the spec

| this deployment | agentic harness spec |
|---|---|
| `--data/journal`, batch files committed by `hard_link` | the fenced, per-session journal (§6.1–§6.2); the journal **is** the session (§2.1) |
| killing a node, `:retry` | caller-driven resumption (§7.5), idempotent turns (H7) |
| any control port accepts any session | placement is routing, not a lease (utilities spec §2.3); exclusivity lives in the fence |
| `--data/workspaces/<session>` | the sandbox seam (§5.3); working state, not session state (§5.5) |
| `assistant` → `delegate` → `worker` | delegation with budget carve-outs (§8, §9.1) |
| stderr `RunStarted`/`SessionActivated`/… | the observability stream (§10.4) |

## Limitations

- **The transport is plaintext.** Fine on loopback; the transport supports
  mutual TLS (`TcpConfig.tls`) but this deployment does not provision
  certificates. Do not point the roster across untrusted networks.
- **`--sandbox local` is a directory, not a boundary.** In that mode `shell`
  runs as your user with your permissions; only the working directory is
  per-session. In tier vocabulary (sandbox spec §2), `shell` declares
  `Tier::Native` and each kind's cap is that singleton: the degenerate
  one-tier provider of sandbox spec §5, running native environments
  unconfined, **trusted-input only** (sandbox spec §3.4). It is never the
  default — `--sandbox` must be chosen explicitly, and a node entering this
  mode warns on stderr at startup. Pass `--sandbox docker --sandbox-image
  <ref>` (e.g. `alpine:3.20`) to run `shell` inside a per-session OCI
  container instead,
  via `harness-sandbox`'s `Native` tier: the workspace bind-mounted, no
  network — shared-kernel confinement (sandbox spec §3.4's SHOULD grade),
  still not the microVM grade. Pre-pull the image: the first call otherwise
  pulls it inside the 120s tool timeout. On Linux with `/dev/kvm`, pass
  `--sandbox firecracker --fc-kernel <vmlinux> --fc-rootfs <ext4>` (both
  built by `guest/fc-rootfs/build.sh`) for the microVM grade instead: one
  Firecracker VM per activation, the workspace synced over vsock, no
  network device (sandbox spec §3.5's reference choice).
- **The journal needs one shared local filesystem**, so "cluster" here means
  one machine. A multi-host deployment needs a networked journal
  implementation (spec §13 leaves durable stores open).
- **The fake-friendly `--api-url`** speaks HTTP/1.1 without retry-relevant
  streaming; long completions are bounded by a 300s per-request timeout
  under the model client's retry policy.
