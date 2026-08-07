# Simulation testing

**Scope:** how the framework and everything above it is verified — the
strategies, the primitives, the shapes a test can take, where each belongs, and
how a run decides how many seeds to spend. The mechanism lives in
`actor-simulation`. The [specification](distributed-actor-spec.md) says *what*
must hold: §16 (events), §17 (conformance), §18 (deterministic simulation); this
document says how it is held up.

Three principles govern the strategy.

- **Determinism.** Replay every failure from a `(workload, seed)` pair alone
  (§18.1, §18.6). One seed drives time, randomness, scheduling, *and* the run's
  sampled fault configuration, so an entire multi-node run reproduces exactly —
  there is nothing else to carry.
- **Specification.** Assert the invariants of §18.5, not chosen outputs.
  Correctness is a small set of properties checked over the §16 event stream,
  not a pile of example assertions.
- **Fault injection.** Bugs hide in the failure paths. Inject partitions,
  crashes, loss, duplication, delay, and reordering under seed control (§18.3),
  and prove the injection actually fired.

## The harness is owned, not imported

Build the harness; do not import one. The spec routes every source of
nondeterminism through four traits — `Clock`, `Entropy`, `Spawner` (§4.6), and
`Transport` (§7) — and §18.2 mandates that simulation reuse *those same traits*,
swapping only the implementations. A generic network or scheduler crate
(`madsim`, `turmoil`, `shuttle`, `loom`, `stateright`) cannot satisfy that
contract: it would test a model of the system, not the real `ActorSystem`,
mailbox, SWIM, supervision, and receptionist code. So the simulator, the
invariant checkers, the reproducibility harness, and the linearizability
decision are all owned, in `actor-simulation`, atop `rand_chacha`.

The one external test-only tool is **`trybuild`**, which drives the compile-fail
cases for invariant #20 (an invalid `ask`/`tell` must not compile) — a property
no runtime test can express. Data-race tooling (`loom`, Miri's race detector) is
largely moot here: the workspace sets `unsafe_code = "forbid"`, and the
simulator already explores interleavings deterministically (see *Interleaving*,
below).

## Primitives

**One seeded stream.** `Entropy` is the single source of randomness in the
system (§4.6); every draw — application randomness, gossip peer selection,
SWIM's `k` members, backoff jitter, and the scheduler's own tie-breaks — comes
from it. Production seeds it from the OS (`OsEntropy`); simulation seeds one
`ChaCha8` stream from the run seed (`SimEntropy`), and cloning a handle shares
the *same* stream, which is what makes a run reproducible.

```rust
pub trait Entropy: Send + Sync + 'static {
    fn next_u64(&self) -> u64;
    fn pick_index(&self, len: usize) -> Option<usize> { /* uniform over 0..len */ }
}
```

**Fault gating lives on `SimEntropy`, not on the production trait.**
`SimEntropy::buggify(num, den)` fires with probability `num/den` from the seeded
stream, and it is deliberately *not* an `Entropy` method: fault injection is a
simulation-only concern, so nothing outside `actor-simulation` can turn a fault
on and the production seam carries no trace of it. The gate **always** consumes
one draw whether or not it fires, so call sites must guard it behind "are faults
configured?" — otherwise a fault-free run stops being byte-identical to one with
the gate absent (see `SimNetwork::route`).

**Quiescence-driven time.** The simulator's `Clock`, `Spawner`, and run loop
share one scheduler. It polls every ready task until none remain, and only then
advances virtual time to the next registered timer (§18.1 #2). A timeout, a SWIM
interval, or a backoff therefore costs no wall-clock time — a run covers hours
of cluster time per CPU-second — and ready-task selection is seed-randomized, so
scheduling itself is a fault dimension.

## Strategies

**Deterministic simulation (the core).** A whole cluster runs in one process, on
one logical thread, over virtual time, network, and randomness (§18). Construct
a `Simulation` from a seed, hand its `clock()`, `entropy()`, and `spawner()` to
a system, and drive it to quiescence. Because these are the *same* traits
production uses (§18.2), the codec stays real and every cross-node hop tests the
wire encoding.

```rust
// Single-node: the real LocalSystem on virtual time/entropy/scheduling.
let sim = Simulation::new(seed);
let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
    .mailbox_capacity(cap)
    .events(sink)            // §16 stream, the substrate every check reads
    .build();
sim.block_on(workload.run(system));

// Cluster: real ClusterSystem nodes over an in-memory Transport (SimNetwork)
// that injects seeded loss/dup/delay and partition/crash (§7, §18.2, §18.3).
```

**Workloads (§18.4).** A test is a `Workload`: build actors and registrations,
drive traffic through the **public API only** (never actor state — `when_local`
excepted, §3.5.1), then let the runner check invariants. `run_swarm` /
`run_cluster_swarm` sweep one workload across many seeds, sampling a
`FaultConfig` / `FaultPolicy` from each seed's stream. A failing run is reported
as a `RunFailure` carrying the `(workload, seed)` that replays it — the seed
regenerates the run's faults, so there is nothing run-shaped to carry beyond it.

Note the asymmetry the checkers have to respect: a single-node run is driven to
**quiescence**, but a cluster run is **time-bounded** (`run_for`) because the
failure detector never quiesces. A property that must hold for both has to hold
at every *prefix*. Getting an end-of-run claim to mean anything takes more than
waiting; see "Asserting at quiescence" below.

**Continuous invariant checking.** Rather than bespoke per-scenario assertions,
a small set of always-on `Invariant`s observe the event stream live through a
`Checker`, on every run and at final quiescence. Seven ship as continuous
checkers today — `NoSilentLoss` (#1), `SerialExecution` (#4),
`LifecycleExactlyOnce` (#6), `SignalInBand` (#13), `DownIsTerminal` (#15),
`OneLeaderPerTerm` (#22), and `SingletonAtMostOnePerNode` (utilities U2) —
chosen because each is a safety property ("a bad thing never happens")
expressible over the existing §16 events. `SignalInBand` (#13) holds the line
that a `Terminated` is delivered *through the watcher's mailbox*, never out of
band: since a signal flows through `enqueue_signal` (an `Enqueue` of the
`Terminated` manifest) before the serial loop dispatches it, a `DispatchStart`
of that signal with no matching prior `Enqueue` is an out-of-band delivery,
caught live. It is a *per-event* (prefix) property, so it is sound for both
quiescence-driven single-node runs and the time-bounded cluster runs (`run_for`)
that stop mid-flight.

Promoting a *true* safety invariant from a targeted test to a continuous checker
is always sound — but not every §18.5 invariant is one, and two are deliberately
left as targeted tests:

- **Death-watch exactly-once (#11)** is *not* "at most one `Terminated` per
  `(target, watcher)`": a watcher may legitimately `watch` the same target
  again, and watching an already-terminated actor yields a fresh `Terminated`
  (§12, #12) — the receptionist does exactly this under anti-entropy. The event
  stream carries no per-`watch` identity, so "exactly one *per watch*" is not
  expressible as a continuous safety property; #11 stays targeted.
- **Bounded, non-dropping mailbox (#5)** is structural and per-call, not
  emergent: the mailbox is a fixed-capacity channel (the bound cannot be
  exceeded), and backpressure is an API contract (`tell` awaits, `try_tell`
  returns `MailboxFull`). A depth checker would need per-actor capacity on the
  stream, and "depth 0 at quiescence" is unsound for `run_for` cluster runs. So
  #5 stays targeted.

**Reference-model testing (linearizability).** For a stateful actor, record the
client-observed `History` of operations (invoke → ok/info/fail) and decide it
against a sequential reference `Model` — `Register` and `Counter` ship — with
`check_linearizable`, a Wing & Gong search with `(used-bitmask, state)`
memoization (`MAX_HISTORY = 128`). This is the state-machine strategy: generate
concurrent operations, then prove the observed history is consistent with *some*
serial order of the model. Keep such a workload small and single-key: the search
is exponential in the number of *pending* operations, and under a full nemesis a
large share of calls end unknown, so breadth comes from the seeds rather than
from any one history.

**Compile-fail testing.** Invariant #20 — an `ask`/`tell` of a message an actor
has no `Handler` for must not compile — is asserted by `trybuild` cases under
`actor-core/tests/compile_fail`, not at runtime.

**Interleaving.** The simulator's single-thread cooperative scheduler selects
among ready tasks with seeded randomness (§18.3), so it already explores message
interleavings deterministically and reproducibly. A separate `loom` model-check
of the executor across *all* interleavings is an optional cross-check (§18.6),
not a prerequisite, and is not currently wired in.

**Fuzzing.** Frame corruption is meaningful only where real bytes exist. The
in-memory simulator carries *structured* frames (only the payload is
codec-encoded, §18.2), so it has nothing to bit-flip; the "malformed frame tears
down the association, not the node" requirement (§7) belongs to the
**production** TCP transport's framing, tested against real wire bytes. Not
implemented: a `cargo-fuzz` target over that framing and the codec.

**Exhaustive enumeration, where the state space allows it.** A seeded sweep is a
*sample*; where the space is small enough to enumerate, enumerate instead.
`wal`'s `tests/crash_points.rs` is the case: a log of a few records is a few
hundred bytes, so recovery is checked at every truncation offset and against
every single-byte corruption, for four properties (the recovered records are a
prefix of what was appended; truncating further never recovers more; the torn
tail is dropped durably so a reopen agrees; the recovered log still accepts an
append that lands after the prefix). A sweep would be strictly weaker there.

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

An **upgrade** joins it on the same terms, for a workload that returns a
`Rollout` from `ClusterWorkload::rollout`: a list of releases, oldest first, that
the nemesis walks one node one step at a time, forward or back, so a run is a
rolling upgrade with rollbacks rather than an arbitrary mix of windows. Because a
release is a process replacement, an upgrade also restarts the node wherever
restarts are allowed — which is everywhere but the first node. There the window
moves under the running process instead, the weaker model, and the reason a
workload can still drive traffic from node one while its own revision changes.

A `Rollout` is validated when it is built, not when it runs: adjacent releases
must share a revision (**V2**) and each must accept what its neighbour writes
(**V4** forward, **V5** back). An invalid sequence is a rollout no operator could
perform, so it panics at construction rather than failing a sweep for a reason
that has nothing to do with the workload.

Restart ends the old process for real: its scheduler domain is retired, so none
of its tasks is ever polled again. That leaves brackets open — an actor stopped
between `DispatchStart` and `DispatchEnd`, an `ask` issued and never answered, an
identity assigned and never resigned — which is what dying looks like, not a
violation. `NodeRestarted` is the boundary a checker learns from: the `Checker`
calls `Invariant::forget_node` on every invariant before the successor's events
arrive reusing the predecessor's identities. An invariant that accumulates
per-node state overrides it; one whose claim survives a restart, like
`OneLeaderPerTerm` over a reloaded term and vote, does not.

`NoSilentLoss` needs more than a reset there: an ask issued *by* a dead caller
(never to be answered, since nothing is left waiting) has to be told apart from
one issued *to* the dead node by a live caller, which must still resolve with
`Unreachable` (#2). `Event::AskIssued` and `AskOutcome` carry the issuing
`caller` alongside the target, and the count is per calling node.

### 3. Coverage sweep

An invariant sweep that additionally asserts fault injection actually fired:

```rust
let stats = run_cluster_swarm_coverage(&workload, coverage_seeds(0..32))?;
assert!(stats.dropped > 0 && stats.duplicated > 0, ...);
```

A sweep that *configures* faults but never *triggers* one gives false
confidence. `FaultStats` tallies the faults a run actually exercised (dropped,
duplicated, delayed, blocked), so a green sweep provably covered loss,
duplication, reordering, and partition/crash, not just the happy path. Because
the assertion is about the whole declared range, `coverage_seeds` never narrows
it (see below).

### 4. Reproducibility sweep

The determinism contract itself (spec §18.1 #1) — run a seed twice, demand
byte-identical event streams:

```rust
if let Err(divergence) = replay_cluster_swarm(&workload, sweep_seeds(0..24)) {
    panic!("{divergence}");
}
```

Everything else rests on this. A wall-clock read, an OS thread, an unseeded RNG,
or `HashMap` iteration order anywhere in the system breaks it. A `Recorder` runs
the workload twice under one seed and pinpoints any `Divergence`; it holds even
under cluster nemesis and transport faults (`check_reproducible` /
`replay_cluster_swarm`).

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
width. CI runs many seeds per change across fault configurations — the metric is
cluster-hours exercised, not tests counted.

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

And bound the check itself. An end-of-run verification that retries per name can
overrun the driver's time budget on a degraded cluster, failing the seed for
liveness rather than for anything observed. Bound it in total, so a seed that
runs out of budget makes no claim rather than a false one.

**Some subsystems never quiesce, and the answer is per checker, not per sweep.**
A background loop that polls for as long as its node lives has something in
flight at *any* stopping point, so a quiescence assertion over it fails on a
fraction of seeds no matter how long the runner waits. Granary's alarm driver
sweeps its shard's index every 500 ms (`ALARM_DRIVE_INTERVAL`), and
`blob-store`'s reconcile loop probes owners continuously, and both break the same
two default checkers — but they do not deserve the same remedy.
`no-silent-loss` is *entirely* a quiescence claim, so a workload over such a
subsystem drops it whole, which both swarms do. `serial-execution` is two claims
in one: `observe` asserts no reentrant dispatch, which is real safety and holds
fine, and only `at_quiescence` asserts nothing is left open. Dropping it whole
would throw away the safety half, so `alarm_swarm.rs` wraps the real checker and
overrides the final call alone. Read a failing checker before deleting it: the
question is not "does this subsystem satisfy it" but "which of its claims does
it satisfy". Dropping a quiescence claim stays honest only while the workload
awaits every op it issues to an outcome, which is what keeps the data path's
no-loss covered with the checker gone.

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

## What counts as an acknowledgement

A durability claim is only as good as the reply it trusts, and three mistakes
recur in workloads that assert one:

- **A no-op commits nothing.** Granary's `Unchanged` reports what the serving
  activation believed; it journals no record, so the output gate never held a
  reply for it and a quorum-less recovery may have seeded that belief with an
  uncommitted record (§7.5). Only outcomes that journal — `Created`, `Updated` —
  are acknowledgements.
- **A read is not a probe.** Granary's read contract is read-your-leader
  (relaxed), *not* linearizable under partition (§7.5). To observe committed
  state, issue a trivial *writing* command rather than a query.
- **A mutating command needs an idempotency key.** At-most-once delivery means a
  duplicated frame runs the command twice (§7.2); a deposit without a key
  deposits twice, and the sweep blames durability for a test bug.

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

## Invariants to assert

§18.5 is the catalogue — twenty-two numbered invariants, each a MUST stated
inline in the spec. The [cluster utilities](cluster-utilities-spec.md) carry
their own, separately numbered catalogue (U1, U2, …; machine-readable as
`utilities_catalogue()`), held to the same drift discipline, as do the layers
above: granary (G1–G20, plus the facet contract F1–F4), the harness (H1–H8), the
sandbox (S1–S5), the machine (M1–M6), the blob store (B1–B7), and format
compatibility (V1–V6). Assert them; do not re-derive them.

The core catalogue's properties are framework ones, not database ones: there is
no durability, serializability, or lost-update notion at that layer (the actor
model is in-memory, at-most-once, eventually consistent — §1.2, §7.2). The shape
worth internalizing:

- **Messaging & execution (#1, #3–#5, #9).** No silent loss; per-pair FIFO under
  reordering; serial, non-reentrant dispatch; bounded non-dropping mailbox;
  local sends skip serialization yet match the remote result.
- **Identity & dispatch (#6–#8, #10).** Lifecycle order and exactly-once;
  `resolve` classifies locality with no round-trip; unregistered
  `(type, manifest)` → `Unhandled`; `ActorRef`s rebind on decode.
- **Failure & monitoring (#2, #11–#13, #18).** A downed node completes in-flight
  `ask`s with `Unreachable`, never hangs; death-watch exactly-once including
  `NodeDown`; watch-after-death fires immediately; signal ordering; supervision
  contains panics (default `Stop`, restarts back off).
- **Membership (#14–#17, #19, #22).** Convergence after partitions heal; `down`
  is terminal; a partition alone never downs a member; SWIM refutation via
  incarnation; receptionist pruned on node `down`; the leader-based control
  plane is quorum-gated with at most one leader per term.
- **Type-safety & transparency (#20, #21).** Invalid sends do not compile; local
  vs remote targets produce identical replies and ordering.
- **Cluster utilities (U1, U2).** Placement is a pure, version-stable function
  of the serving set with minimal movement; singleton activations never overlap
  on one node, a healed converged cluster runs exactly one per name, and an
  anchor failure re-activates.

Verification is **layered**, not uniform (§18.6). The safety core runs
continuously; the rest are verified by the method that fits — a liveness or
scenario property by a targeted conformance test, #20 by a compile-fail case,
#21 by a differential local-vs-remote run. The machine-readable
`core_catalogue()` records, per invariant, which method applies, and the
`conformance_catalogue` test fails the build if a continuous checker and its
catalogue entry drift apart — so the §17 "Verified by" column stays mechanically
true.

## Checklist

For each component, write:

1. **Roundtrip** tests for every codec encode/decode pair, and for `ActorRef`
   rebinding across the wire (§5, §4.4; #10).
2. **Idempotency / duplicate-tolerance** tests: at-most-once delivery means a
   retried or transport-duplicated message can arrive twice (§7.2); a retriable
   operation must carry an explicit idempotency key and survive a duplication
   fault.
3. **Reference-model** tests for stateful actors: a `History` decided against a
   `Model` (§18.4).
4. **Simulation workloads** that assert the §18.5 invariants under the §18.3
   faults — partition, crash, loss, duplication, delay, reordering — not just
   the happy path.
5. **Node-crash** tests that abruptly crash a node mid-run and verify the
   cascade (§8.1): `Terminated { NodeDown }` to watchers, `Unreachable` to
   in-flight callers, receptionist pruning. (There is no durability to verify at
   the actor layer — a restart constructs fresh state, §11.2.)
6. **Compile-fail** tests (`trybuild`) for invalid sends (#20).
7. **Seed-reproducibility** checks: the same `(workload, seed)` yields a
   byte-identical event stream (§18.1 #1).
8. **Fault-coverage** assertions: the sweep actually fired each fault type
   (`FaultStats`), so a green run is not a silently happy-path run.

## Where the sweeps do not yet reach

Known gaps between what the sweeps exercise and what the specs mandate. Each is
a place a bug could live undetected today.

- **Granary workflows have no sweep at all, and the obvious shape does not
  reach the property.** The invariant worth asserting is not "the effect ran
  once" — `LaunchGuard` is per-activation and never journaled, so a
  re-activation legitimately re-launches an unresolved step and an effect may
  run many times (§7.17). It is that the **memo is write-once**: `complete_step`
  records only a step that is not already done, so the first committed result
  wins and every later drive resolves from it. Making that observable needs a
  fixture whose effect returns a *different value on each run*, so an overwrite
  shows up in the memo; a constant-valued effect cannot tell the two cases
  apart.
  
  What blocks a sweep is that the property needs a **chain**: the workflow must
  commit a step, be interrupted, re-launch, and then be readable. Other granary
  sweeps judge independent operations and tolerate individual failures, but here
  the seeds that get far enough to observe anything are the calm ones — which
  never re-launch — and the seeds that re-launch never get readable. At the
  nemesis's fault levels, roughly two seeds in twenty-four observe a memo at
  all, and none of those re-launched. The workload that measured this was never
  committed and does not survive. Landing one needs the chain shortened until a
  single commit suffices to observe the property — dropping the `Start` round
  trip in favour of activating on first touch is the obvious first cut — rather
  than more seeds or a longer settle, both of which were tried and moved nothing.
- **`harness-sandbox`, `harness-gateway`, and `machine-frontdoor` have no
  sweep.** These are I/O-boundary crates rather than distributed ones, so the
  simulator reaches them only indirectly; what a sweep would look like there is
  itself unsettled.
