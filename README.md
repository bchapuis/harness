# harness

Agentic sessions as journaled actors on a distributed cluster. The journal is
the session; everything else — the agent's actor, its sandbox, the node it
runs on — is disposable and replaceable while the session lives.

## Two minutes

```sh
export ANTHROPIC_API_KEY=sk-ant-…
./demo.sh        # needs docker running: the model's shell commands execute confined
```

That builds the workspace, boots a three-node cluster — three OS processes on
your machine, one shared journal directory, each `shell` call inside a
per-session container — and drops you into a REPL attached to node 1:

```
assistant/demo> Create numbers.txt holding 1..10, then tell me their sum.
· submitted t-1 (waiting for the run; :retry re-attaches after a failure)
I created numbers.txt with the numbers 1 through 10. Their sum is 55.
· t-1 done, 1843 tokens
```

Now the part that matters. The script printed each node's pid; kill one:

```sh
kill <pid-of-the-owner>     # in another terminal
```

then ask for the outcome again:

```
assistant/demo> :retry
```

Within a few seconds the survivors detect the death, placement routes around
it, the new owner folds the session back out of the shared journal, and the
run finishes as if nothing happened. No coordinator, no session server, no
state lost. That is the prototype.

`:tail` shows what actually exists — the journal, one record per line.
Sessions are durable and named: quit, reboot everything, `:session demo`
continues the same transcript.

## What you are looking at

- A **distributed actor framework** (`crates/actor-*`): runtime-agnostic
  core, SWIM failure detection, rendezvous placement, a mutual-TLS-capable
  TCP transport — all tested by seeded, deterministic simulation
  (FoundationDB-style: one seed reproduces an entire multi-node run,
  partitions and all).
- An **agentic harness** on top (`crates/harness`): each session is an actor
  whose only state is a fenced, append-only journal; model, journal, and
  sandbox are injected seams (`crates/harness-anthropic` is the Anthropic
  one).
- A **standalone deployment** (`crates/harness-standalone`): the binary the
  demo runs — file-backed journal whose fence is an atomic `hard_link`,
  tokio/rustls HTTP to the Messages API, per-session shell workspaces
  behind an explicit `--sandbox` choice — a container (`docker`), a
  Firecracker microVM (`firecracker`, Linux/KVM), or an unconfined
  trusted-input-only `local` mode that is never the default — and the REPL.

## Going deeper

- [docs/standalone-deployment.md](docs/standalone-deployment.md) — the full
  deployment guide: configuration, the failure drill step by step, limits.
- [docs/agentic-harness-spec.md](docs/agentic-harness-spec.md) — why the
  journal is the session.
- [docs/distributed-actor-spec.md](docs/distributed-actor-spec.md) and
  [docs/cluster-utilities-spec.md](docs/cluster-utilities-spec.md) — the
  framework underneath.
- [docs/verification-and-validation.md](docs/verification-and-validation.md)
  — how the simulation testing earns the claims above.

No API key handy? `crates/harness-standalone/smoke.sh` runs the same story
against a canned fake model.
