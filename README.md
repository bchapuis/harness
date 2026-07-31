# harness

Agentic sessions as journaled actors on a distributed cluster. The journal is
the session. Everything else (the agent's actor, its sandbox, the node it runs
on) is disposable, replaceable while the session lives.

## Two minutes

```sh
export ANTHROPIC_API_KEY=sk-ant-…
./demo.sh        # needs docker running: the model's shell commands execute confined
```

That builds the workspace and boots a three-node cluster: three OS processes on
your machine, one shared journal directory, each `shell` call inside a
per-session container. In front of it sits the HTTP gateway on
`127.0.0.1:8080`. Drive a session with `curl`, streaming the run as
Server-Sent Events:

```sh
curl -N -X POST http://127.0.0.1:8080/v1/assistant/demo/prompt \
  -H 'Authorization: Bearer alice' -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  -d '{"turn":"t-1","content":"Create numbers.txt holding 1..10, then tell me their sum."}'
```

```
event: records   …the transcript, one committed record at a time…
event: outcome    {"outcome":{"Ok":{"content":"…Their sum is 55.","tokens":1843}}}
```

Now the part that matters. The script printed each node's pid. Kill the one
hosting the session, then re-issue the **same** prompt (same `turn` id):
within seconds the survivors detect the death, placement routes around it, the
new owner folds the session back out of the shared journal, and the run
finishes as if nothing happened. No coordinator, no session server, no state
lost. That is the prototype.

`GET /v1/assistant/demo/records` shows what exists: the journal, one record per
line. Sessions are durable and named. Quit, reboot everything, and prompt
`demo` again to continue the same transcript.

Sessions are also multi-tenant. A bearer token names the tenant, and every
session is scoped under it, so tenants never see each other's work;
`GET /v1/sessions?kind=assistant` lists yours. The demo runs the gateway on
loopback in an insecure dev mode (the token *is* the tenant); `k8s/` shows the
authenticated form with opaque per-tenant tokens.

## What you are looking at

- A **distributed actor framework** (`crates/actor-*`): runtime-agnostic core,
  SWIM failure detection, rendezvous placement, a mutual-TLS-capable TCP
  transport. Seeded deterministic simulation tests all of it, FoundationDB-style:
  one seed reproduces an entire multi-node run, partitions and all.
- An **agentic harness** on top (`crates/harness`): each session is an actor
  whose only state is a fenced, append-only journal. Model, journal, and sandbox
  are injected seams (`crates/harness-anthropic` is the Anthropic one).
- A **standalone deployment** (`crates/harness-standalone`): the binary the demo
  runs. A file-backed journal fenced by an atomic `hard_link`, tokio/rustls HTTP
  to the Messages API, and per-session shell workspaces behind an explicit
  `--sandbox` choice: a `docker` container, a `firecracker` microVM (Linux/KVM),
  or an unconfined trusted-input-only `local` mode that is never the default.
- A **gateway-as-cluster-client edge** (`crates/harness-gateway`): an `axum`
  HTTP/SSE tier that is the single public boundary. It joins the actor transport
  as a non-voting, non-hosting member (the Orleans cluster-client pattern),
  terminates tenant auth (a bearer token → a principal), and addresses the
  session's grain **directly** — no control protocol, no per-node listener, no
  forwarding hop. It holds no durable state, so it scales independently; SSE
  carries the live record stream.
- **Multi-tenant isolation** (`crates/tenancy`): each tenant's sessions stay
  isolated under a principal-scoped grain name and are listed through an
  ownership-index grain (`tenancy`, itself another journaled actor). The gateway
  is where auth terminates, so it joins inside the cluster's trust boundary;
  untrusted callers reach it only over HTTP with a bearer token.
- **Persistent machines** (`crates/machine-grain`, `crates/machine-host`,
  `crates/machine-frontdoor`): the agent's sibling. Same grain, different
  consumer — a lightweight VM you address by name and reach over SSH, whose
  *whole rootfs* is the durable state. Not an agent: no run loop, no model, no
  autonomy, just a box that survives.

## The other demo: a machine you SSH into

```sh
./machine-demo.sh
```

Same cluster, different thing on top of it: three nodes, a machine named
`dev-box`, and an SSH front door on each node. `ssh alice@127.0.0.1 -p 2222`
lands you in a Firecracker microVM whose disk is a grain — installed packages
and all — so it outlives disconnection, idle hibernation, and the node it was
running on. The front door authenticates your key against the machine's
*journaled* key set and presents the machine's own journaled host key, so your
`known_hosts` pin survives every move the machine makes.

The failure drill is the same shape as the harness's: kill the node holding your
session, reconnect through another node's door, and find your files where you
left them — rewound at most to the last capture, never forked.

Which mechanism holds the guest is the node's `--machine` flag, and the script
fills it in (set `MACHINE_KIND` to choose yourself). A microVM per machine needs
Linux with `/dev/kvm` and the assets `guest/machine-rootfs/build.sh` produces;
elsewhere — macOS included — `--machine docker` holds the machine's *own* rootfs
in a privileged container that loop-mounts the disk image and chroots into it,
so the shell and the whole-rootfs persistence are real. It gives shared-kernel
isolation rather than the microVM boundary, it runs no init inside the rootfs,
and it has no network: a development mechanism, not the one to put untrusted
guests behind. With neither available the script picks `--machine fake` and says
so — everything durable still runs, and only the shell is missing.

## Going deeper

- [docs/standalone-deployment.md](docs/standalone-deployment.md): the full
  deployment guide, with configuration, the failure drill step by step, and
  limits.
- [docs/multi-tenant-acp-design.md](docs/multi-tenant-acp-design.md): the
  multi-tenant edge, covering bearer-token auth, per-tenant isolation, the
  tenancy directory, and the gateway-as-cluster-client trust model.
- [k8s/README.md](k8s/README.md): the cluster as a Kubernetes StatefulSet of
  silos, with the gateway joining it as a cluster client.
- [docs/agentic-harness-spec.md](docs/agentic-harness-spec.md): why the journal
  is the session.
- [docs/machine-spec.md](docs/machine-spec.md): why a machine is a grain plus
  durable storage and a network seam — the disk facet, the session-grained
  capture cadence, the self-fencing lease, and the SSH front door.
- [docs/distributed-actor-spec.md](docs/distributed-actor-spec.md) and
  [docs/cluster-utilities-spec.md](docs/cluster-utilities-spec.md): the framework
  underneath.
- [docs/wal-spec.md](docs/wal-spec.md): the framed, checksummed write-ahead log
  primitive for file-backed durable stores.
- [docs/verification-and-validation.md](docs/verification-and-validation.md): how
  the simulation testing earns the claims above.

No API key handy? `crates/harness-standalone/smoke.sh` runs the same story
against a canned fake model.
