# Simulation testing

**Scope:** the conventions around deterministic simulation — the shapes a test
can take, where each belongs, and how a run decides how many seeds to spend. The
mechanism lives in `actor-simulation` (spec §18).

## The shapes

### 1. Scenario

One seed, one interleaving, built by hand:

```rust
let sim = Simulation::new(7);
// ... build actors, drive them, assert on the outcome ...
```

The seed is arbitrary but fixed, so the test is a specification of one concrete
history. Use this when the thing under test is a *particular* sequence — a
handshake, a restart, a specific race — and you want the failure to point at
that sequence rather than at a distribution.

Scenarios do not sweep and carry no corpus. They live in ordinary test files.

### 2. Invariant sweep

A [`Workload`] or [`ClusterWorkload`] run across many seeds, with invariants
checked on every run:

```rust
if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..48)) {
    panic!("{failure}");
}
```

Use this when the property should hold under *every* interleaving, not one. The
workload says what traffic to generate and which invariants must hold; the
runner varies scheduling, seeded transport faults, and the nemesis per seed.

Each round the nemesis draws one action: a symmetric partition, a one-way
partition, a crash (isolation — the process keeps running), a bounded process
freeze, a heal, or a quiet round, plus a registry outage in registry-based mode.

A **restart** — real process death, with durable state reloaded through the
storage seam — joins that list only for a workload that returns a `Rehost` hook
from `ClusterWorkload::rehost`, because the successor process comes up empty.
Whatever `setup` installed on that node is gone, so without the hook a restarted
node stops leading shards and stops counting toward a quorum, and the run has
quietly shrunk the cluster rather than faulted it. A consenting workload also
bounds its calls (`ask_timeout`) and keeps no long-lived handle on a node other
than the first, which the nemesis never restarts.

Restart ends the old process for real: its scheduler domain is retired, so none
of its tasks is ever polled again. That leaves brackets open — an actor stopped
between `DispatchStart` and `DispatchEnd`, an `ask` issued and never answered, an
identity assigned and never resigned — which is what dying looks like, not a
violation. `NodeRestarted` is the boundary a checker learns from: the `Checker`
calls `Invariant::forget_node` on every invariant before the successor's events
arrive reusing the predecessor's identities. An invariant that accumulates
per-node state overrides it; one whose claim survives a restart, like
`OneLeaderPerTerm` over a reloaded term and vote, does not.

### 3. Coverage sweep

An invariant sweep that additionally asserts fault injection actually fired:

```rust
let stats = run_cluster_swarm_coverage(&workload, coverage_seeds(0..32))?;
assert!(stats.dropped > 0 && stats.duplicated > 0, ...);
```

A sweep that *configures* faults but never *triggers* one gives false
confidence. Because the assertion is about the whole declared range,
`coverage_seeds` never narrows it (see below).

### 4. Reproducibility sweep

The determinism contract itself (spec §18.1 #1) — run a seed twice, demand
byte-identical event streams:

```rust
if let Err(divergence) = replay_cluster_swarm(&workload, sweep_seeds(0..24)) {
    panic!("{divergence}");
}
```

Everything else rests on this. A wall-clock read, an OS thread, an unseeded RNG,
or `HashMap` iteration order anywhere in the system breaks it.

### Escape hatch: `scenario_sweep`

For a scenario worth sweeping but too bespoke to fit `Workload` — it drives the
cluster through a hand-written narrative rather than generating traffic.
`scenario_sweep` gives such a loop the one thing it otherwise lacks, a name:

```rust
scenario_sweep("partition-safety/split-brain", sweep_seeds(0..12), |seed| {
    let sim = Simulation::new(seed);
    // ... the narrative ...
});
```

Prefer a `Workload` where one fits: the trait buys invariant checking and
reproducibility sweeps as well as a name. Reach for this when it does not.

## Where things live

**Sweeps go in `*swarm.rs`; scenarios go everywhere else.** Three clauses, all
checked by `tests/corpus_keys.rs`:

1. A file that calls `run_swarm`, `run_cluster_swarm`, or
   `run_cluster_swarm_coverage` is named `*swarm.rs`.
2. A file that calls `replay_swarm`, `replay_cluster_swarm`,
   `check_reproducible`, or `check_cluster_reproducible` is named `*swarm.rs`
   **or** `*determinism.rs`.
3. `scenario_sweep` is unconstrained — it lives with the scenarios it sweeps.

The two kinds fail differently: a scenario failure names a sequence you can read,
a sweep failure names a seed you have to replay. Separate binaries keep that
distinction legible, and keep a slow sweep from sitting between you and a fast
scenario suite.

Two organizing axes are both allowed, which is what clause 2 is for. Most crates
name a sweep file after its **area** — `grain_swarm.rs`, `sql_swarm.rs`,
`ws_swarm.rs` — and pair an invariant sweep with a reproducibility sweep over
the same workload. `actor-simulation` instead hoists every reproducibility sweep
in the crate into `conformance_determinism.rs`, by **shape**, so there is one
file to point at and call the §18.1 determinism gate. Either is fine; mixing
them within one crate is not.

Clause 3 exists because a `scenario_sweep` has no workload to pair with and
reads as narrative, so it stays with the scenarios it belongs to.
`partition_safety.rs` is the canonical case.

## Sizing: how many seeds a run spends

Per-seed cost spans roughly four orders of magnitude, about a millisecond for a
local actor sweep against ten seconds for one that drives a machine, so the call
site declares a cost class and the *run* decides the width:

| helper           | local width | use for                                        |
|------------------|-------------|------------------------------------------------|
| `sweep_seeds`    | 8           | ordinary sweeps                                |
| `slow_seeds`     | 1           | seconds per seed (disk, SQL, machine bindings) |
| `coverage_seeds` | declared    | coverage assertions — never narrowed           |

Sizing is **deterministic on purpose**. Deciding width from a wall clock would
make the seeds that ran depend on the machine that ran them, which spec §18.1
denies; `clippy.toml` bans the call in this crate. Wall-time budgeting belongs to
whatever drives the suite (`soak.yml` sizes per crate), never under a seed.

Reclassify by measuring, not guessing:

```bash
# per-seed cost = (t32 - t2) / 30
SWARM_SEEDS=2  ./target/debug/deps/<binary>
SWARM_SEEDS=32 ./target/debug/deps/<binary>
```

Warm the binary first — a cold start is ~0.6s and will swamp a cheap sweep.

## The three runs

| run   | seeds                          | job                                 |
|-------|--------------------------------|-------------------------------------|
| local | a few, by cost class           | fast feedback                       |
| CI    | from 0, the declared width     | regression: the pinned corpus       |
| soak  | from a fresh base, many        | discovery: seeds nobody has run     |

Controlled by two environment variables:

- `SWARM_SEEDS=<n>` — exactly `n` seeds, whatever the class. May exceed the
  declared width; that is how a soak asks for more than the corpus holds.
  `full` (or `0`) means the declared width. A malformed value falls back to the
  declared width, never to something narrower.
- `SWARM_SEED_BASE=<n>` — offset every sweep by `n`. `soak.yml` sets this fresh
  per run from the run id, so successive soaks explore disjoint ground.

CI pins `SWARM_SEEDS: full`. Local runs, with neither set, take the cost-class
width.

## Stopping at the first failure, or collecting them all

A sweep stops at the first failing seed. That is right for CI — the build is
already red — and wrong for a soak, whose job is to mine `(workload, seed)` pairs
for the corpus. It compounds: corpus seeds replay *ahead* of every sweep, so once
a workload has a failing seed on record, that replay fails first and the workload
stops exploring new ground until someone fixes the bug.

- `SWARM_CONTINUE=1` — run the sweep to the end and report every seed that
  failed, printed as `corpus.txt` lines ready to paste. `soak.yml` sets it; CI
  leaves it unset.

A collecting run also catches a panic *inside* a workload, attributes it to its
seed, and carries on. Sweeps fail two ways: an invariant returns a violation, or
the workload simply asserts (a `drive` that checks its own outcome, a
`scenario_sweep` body).

One seed reports once, even though the corpus replay and a wide sweep can both
reach it.

## Asserting at quiescence

An at-quiescence assertion is a claim about a run that has stopped happening,
and a run only stops happening if you let it. Two things get this wrong.

**Wait for the calls to close.** A call issued at the end of the drive carries
its own deadline, and a subsystem may fan out more calls behind it — granary's
quorum append returns at quorum latency and drains the slower replicas
afterwards, each with a seconds-long timeout. `drive_cluster` therefore flushes
and then keeps advancing while any `ask` is in flight, up to a cap. So "still
pending at quiescence" means what it says, and a workload should not paper over
it with a sleep of its own.

**A heal is not a quiet network.** `net.heal()` clears the nemesis's partitions.
It does not stop the seeded loss, duplication, and latency, which run for the
whole seed — deliberately, since the nemesis heals between rounds. But a
detector fed lossy probes keeps flipping peers in and out of every node's view,
so the cluster never converges, and a run can end mid-divergence however long it
waits. A workload that means to assert something about a **converged** cluster
calls `net.quiesce()` alongside the heal, then gives the views time to settle.
Faults already tallied stay counted, so coverage assertions are unaffected.

Related: judge liveness by the event that states it. `SingletonConverged` counts
an activation as over at `SingletonStopped` *or* `ResignId`, because the first is
the manager's report on its next tick and the second is the actor's own identity
being released. Between them sits a window a run can end inside.

## The regression corpus

`crates/actor-simulation/corpus.txt` records every seed that ever failed, keyed
by workload name:

```
singleton-chaos/leader 900006  # split-brain singleton after heal
```

Recorded seeds replay on **every** run of that workload — local, CI, and soak
alike, regardless of sizing. Corpus seeds are absolute, so `SWARM_SEED_BASE`
moves sweeps but never the regressions.

Adding one is a copy from the failure message:

```
workload 'singleton-chaos/leader' failed at seed 900006
└─ becomes: singleton-chaos/leader 900006
```

and it replays on its own with:

```bash
SWARM_SEED_BASE=900006 SWARM_SEEDS=1 cargo test -p <crate> --test <name>
```

Two properties make the key meaningful, and `tests/corpus_keys.rs` enforces
both: names are unique tree-wide (a shared name would make one workload replay
another's regressions), and every corpus key names a real workload (a typo would
otherwise silently guard nothing). The same file enforces the location rule
above.

A corpus seed guards a bug only while that seed still drives the code down the
same path, and any change to how much entropy anything draws re-randomizes what
a seed means. So the seed is the *discovery* mechanism; once the bug is
understood, the durable regression is a scenario test that reproduces its shape.
Write that too, and keep the corpus line as the cheap belt-and-braces.

## A workload outlives its runs

`run_cluster_swarm` drives every seed through the **same** workload value: `drive`
takes `&self`, and the sweep calls it once per seed. So a field on the workload is
sweep-scoped, not run-scoped, and the distinction decides whether an end-of-run
assertion means anything.

Two kinds of state end up on a workload, and only one belongs there:

- **Tallies** — "did this ever happen across the sweep": faults that fired,
  activations that hibernated, reads that were actually checked, a violation flag
  a client set. These are cumulative on purpose. `support::Exercised` is the
  shared one.
- **Expectations** — "what this run should find at the end": the names a run
  acknowledged, the blobs it stored. These are per *run*, and a field is the wrong
  place for them. Build them inside `drive` and share them among that run's client
  tasks with a local `Arc<Mutex<_>>`.

Getting this wrong does not fail loudly, it fails *convincingly*: seed N's
expectations are checked against seed N+1's freshly empty grains, so the sweep
reports acknowledged writes that vanished — a textbook G14 violation, on a system
that did nothing wrong.

The tell, when a sweep claims a durability violation: check whether it fails on
every seed *except the first*. Seed 0 has nothing to inherit.

## Adding a sweep

1. Write the workload — `Workload` for single-node, `ClusterWorkload` for a
   cluster — and give it a name unique across the workspace.
2. Put it in `<area>_swarm.rs`, with an invariant sweep and a reproducibility
   sweep over it — or, if the crate hoists determinism by shape, the
   reproducibility sweep goes in its `*determinism.rs` instead.
3. Size it: `sweep_seeds` unless a seed costs seconds, then `slow_seeds`.
4. If it injects faults, add a coverage sweep with `coverage_seeds`. That covers
   the transport's faults, the ones `FaultStats` counts. For a fault the wire
   cannot see — a grain that hibernates, a process that dies — tally the events
   that say so and assert once at the end, and then ask **which kind of claim it
   is**, because that decides where it goes:

   - Something the *workload* drives holds however narrow the run, so it can
     ride the invariant sweep. Make it actually deterministic: idle past
     `idle_after` on a fixed cadence rather than a seeded coin, and snapshot
     often enough that a grain has a checkpoint to return from before it first
     passivates. `granary`'s `support::Exercised` is the shared tally.
   - Something the *nemesis* draws is a property of the seed range, so it needs
     `coverage_seeds`, which never narrows. At `slow_seeds`' single local seed a
     six-round nemesis misses any one action about two runs in five, so the same
     assertion on an invariant sweep is not stricter, only flaky.
5. If it needs a grain or actor a scenario suite already drives, take it from
   `tests/support/` rather than writing a second one.
6. When soak finds a failing seed, add it to `corpus.txt` — and write the
   scenario test once you know what broke.

## Shared fixtures and invariants

Test code is shared rather than copied for one reason: an independently-
maintained copy can drift, and a weakened one silently stops checking what its
name claims.

### Where an invariant goes

One question decides it: **whose contract is this a claim about?**

| the claim is about | it lives in | example |
|--------------------|-------------|---------|
| the actor framework | `actor-simulation` | `NoSilentLoss`, `OneLeaderPerTerm` |
| one crate's contract | that crate's `testing` feature | `granary::testing::CommitMonotonic` |
| one suite's workload | that suite's own file | `SingletonConverged` in `conformance_swarm.rs` |

The middle tier is feature-gated because it is test support, not part of the
crate's API, and it must not ship in a production build. `granary` and `harness`
both have one; a crate reaches another's by enabling it in dev-dependencies
(`granary = { workspace = true, features = ["testing"] }`), and its own by
depending on itself (`harness = { path = ".", features = ["testing"] }`).

Each shared invariant is constructed with the label it reports under, so a suite
still names its own violations:

```rust
Box::new(CommitMonotonic::new("machine-commit-monotonic", "machine"))
```

Before writing an invariant, check whether one of the first two tiers already
has it. Near-identical names across layers are the trap — the cluster-utilities
`SingletonAtMostOnePerNode` (U2, off `SingletonStarted`) and the grain
`ActivationSingletonPerNode` (G6, off `GrainEvent::Activated`) are different
claims, and each doc comment points at the other.

### Where a fixture goes

**Fixtures** live in `tests/support/`, shared between the test binaries of one
crate. `support::Ledger` is the SQL-facet grain that `sql.rs` and `sql_swarm.rs`
both drive, and `support::CounterGrain` is what `grains.rs` and `grain_swarm.rs`
share — so a sweep covers the same grain the scenarios specify. A crate with no
`testing` feature may also keep suite-local invariants here; the moment a second
crate wants one, it has become a contract claim and moves up a tier.
