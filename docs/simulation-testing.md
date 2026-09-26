# Simulation testing

**Scope:** how the framework and everything above it is verified: strategies, primitives, test shapes, where each belongs, and how a run sizes its seeds. The mechanism lives in `actor-simulation`; the [specification](distributed-actor-spec.md) says *what* must hold (§16 events, §17 conformance, §18 deterministic simulation). Three principles govern the strategy:

- **Determinism.** Replay every failure from a `(workload, seed)` pair alone (§18.1, §18.6). One seed drives time, randomness, scheduling, *and* the run's sampled fault configuration, so a whole multi-node run reproduces exactly.
- **Specification.** Assert the §18.5 invariants over the §16 event stream, not chosen outputs or a pile of example assertions.
- **Fault injection.** Bugs hide in the failure paths. Inject partitions, crashes, loss, duplication, delay, and reordering under seed control (§18.3), plus the external registry's faults (`RegistryFaultPolicy`: a slow, stale, or unavailable sync), and prove the injection fired.

## The simulator is owned, not imported

The spec routes all nondeterminism through four traits (`Clock`, `Entropy`, `Spawner` (§4.6), and `Transport` (§7)), and §18.2 requires simulation to reuse them, swapping only the implementations. A generic network or scheduler crate (`madsim`, `turmoil`, `shuttle`, `loom`, `stateright`) would test a model of the system, not the real `ActorSystem`, mailbox, SWIM, supervision, and receptionist code, so the simulator, invariant checkers, reproducibility harness, and linearizability checker are owned, in `actor-simulation`, atop `rand_chacha`.

The one external test-only tool is **`trybuild`**, for the compile-fail cases of invariant #20 (an invalid `ask`/`tell` must not compile), which no runtime test can express. Data-race tooling (`loom`, Miri's race detector) is largely moot: the workspace sets `unsafe_code = "forbid"`, and the simulator explores interleavings deterministically.

## Primitives

**One seeded stream.** `Entropy` is the system's single source of randomness (§4.6): application randomness, gossip peer selection, SWIM's `k` members, backoff jitter, and the scheduler's tie-breaks all draw from it. Production seeds it from the OS (`OsEntropy`); simulation seeds one `ChaCha8` stream from the run seed (`SimEntropy`), and cloning a handle shares the *same* stream.

```rust
pub trait Entropy: Send + Sync + 'static {
    fn next_u64(&self) -> u64;
    fn pick_index(&self, len: usize) -> Option<usize> { /* uniform over 0..len */ }
}
```

**Fault gating lives on `SimEntropy`, not on the production trait.** `SimEntropy::buggify(num, den)` fires with probability `num/den` from the seeded stream. It is not an `Entropy` method, so nothing outside `actor-simulation` can turn a fault on and the production seam carries no trace of it. The gate **always** consumes one draw, fired or not, so call sites must guard it behind "are faults configured?"; otherwise a fault-free run stops being byte-identical to one without the gate (see `SimNetwork::route`).

**Quiescence-driven time.** The simulator's `Clock`, `Spawner`, and run loop share one scheduler. It polls every ready task until none remain, and only then advances virtual time to the next registered timer (§18.1 #2), so a timeout, a SWIM interval, or a backoff costs no wall-clock time (a run covers hours of cluster time per CPU-second). Ready-task selection is seed-randomized, so scheduling is itself a fault dimension.

## Strategies

**Deterministic simulation (the core).** A whole cluster runs in one process, on one logical thread, over virtual time, network, and randomness (§18). A system takes the `Simulation`'s `clock()`, `entropy()`, and `spawner()`, the same traits production uses (§18.2), so the codec stays real and every cross-node hop tests the wire encoding.

```rust
// Single-node: the real LocalSystem on virtual time/entropy/scheduling.
let sim = Simulation::new(seed);
let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
    .mailbox_capacity(cap)
    .events(sink)            // §16 stream, the substrate every check reads
    .build();
sim.block_on(workload.run(system)); // returns when the workload's future resolves
sim.run();                          // the drain to quiescence is a separate, explicit step

// Cluster: real ClusterSystem nodes over an in-memory Transport (SimNetwork)
// that injects seeded loss/dup/delay and partition/crash (§7, §18.2, §18.3).
```

**Workloads (§18.4).** A test is a `Workload`: build actors and registrations, drive traffic through the **public API only** (never actor state, `when_local` excepted, §3.5.1), then let the runner check invariants. `run_swarm` / `run_cluster_swarm` sweep one workload across many seeds, sampling a `FaultConfig` / `FaultPolicy` from each seed's stream. A failing sweep reports a `SweepFailure` of `RunFailure`s, each carrying the `(workload, seed)` that replays it; the seed regenerates the run's faults, so nothing else needs carrying.

A single-node run is driven to **quiescence**, but a cluster run is **time-bounded** (`run_for`) because the failure detector never quiesces, so a property that must hold for both must hold at every *prefix*. What an end-of-run claim takes is in "Asserting at quiescence" below.

**Continuous invariant checking.** In place of per-scenario assertions, always-on `Invariant`s observe the event stream live through a `Checker`, on every run and at final quiescence. Seven ship, each a safety property expressible over the §16 events: `NoSilentLoss` (#1), `SerialExecution` (#4), `LifecycleExactlyOnce` (#6), `SignalInBand` (#13), `DownIsTerminal` (#15), `OneLeaderPerTerm` (#22), and `SingletonAtMostOnePerNode` (utilities U2). `SignalInBand`, for example, flags a `DispatchStart` of a `Terminated` with no prior `Enqueue` (a signal flows through `enqueue_signal` into the watcher's mailbox); as a per-event (prefix) property it is sound for both kinds of run.

Promoting a *true* safety invariant to a continuous checker is always sound, but two §18.5 invariants are not one and stay targeted tests:

- **Death-watch exactly-once (#11)** is not "at most one `Terminated` per `(target, watcher)`": a watcher may `watch` the same target again, and watching an already-terminated actor yields a fresh `Terminated` (§12, #12), as the receptionist does under anti-entropy. The event stream carries no per-`watch` identity, so "exactly one *per watch*" is not expressible.
- **Bounded, non-dropping mailbox (#5)** is structural: the mailbox is a fixed-capacity channel, and backpressure is an API contract (`tell` awaits, `try_tell` returns `MailboxFull`). A depth checker would need per-actor capacity on the stream, and "depth 0 at quiescence" is unsound for `run_for` runs.

**Reference-model testing (linearizability).** For a stateful actor, generate concurrent operations, record the client-observed `History` (invoke → ok/info/fail), and decide with `check_linearizable` whether it matches *some* serial order of a sequential reference `Model` (`Register` and `Counter` ship): a Wing & Gong search with `(used-bitmask, state)` memoization (`MAX_HISTORY = 128`). Keep such a workload small and single-key: the search is exponential in *pending* operations, and under a full nemesis many calls end unknown, so breadth comes from the seeds, not from one history.

**Compile-fail testing.** Invariant #20 (an `ask`/`tell` of a message an actor has no `Handler` for must not compile) is asserted by `trybuild` cases under `actor-core/tests/compile_fail`.

**Interleaving.** The scheduler picks among ready tasks with seeded randomness (§18.3), so it explores message interleavings deterministically. A `loom` model-check of the executor across *all* interleavings is an optional cross-check (§18.6) and is not wired in.

**Fuzzing.** The simulator carries *structured* frames (only the payload is codec-encoded, §18.2), so it has nothing to bit-flip. The "malformed frame tears down the association, not the node" requirement (§7) is tested against the production TCP transport's real bytes in `actor-runtime/tests/transport_invariants.rs` (`a_malformed_frame_tears_down_its_association_not_the_node`: a handshaken peer writes an undecodable payload and an over-cap length prefix). Not implemented: a `cargo-fuzz` target over that framing and the codec.

**Exhaustive enumeration, where the state space allows it.** A seeded sweep is a *sample*; where the space is small, enumerate. `wal`'s `tests/crash_points.rs` checks recovery of a few-record log at every truncation offset and every single-byte corruption, for five properties: the recovered records are a prefix of what was appended; truncating further never recovers more; the torn tail is dropped durably so a reopen agrees; the recovered log accepts an append after the prefix; and damage to any of the twelve header bytes makes `open` refuse, proving the header validates all of itself, not just its magic.

## The shapes

### 1. Scenario

One seed, one interleaving, built by hand:

```rust
let sim = Simulation::new(7);
// ... build actors, drive them, assert on the outcome ...
```

The fixed seed makes the test a specification of one concrete history. Use it when the thing under test is a *particular* sequence (a handshake, a restart, a specific race) and the failure should point at that sequence rather than at a distribution. Scenarios do not sweep, carry no corpus, and live in ordinary test files.

### 2. Invariant sweep

A [`Workload`] or [`ClusterWorkload`] run across many seeds, with invariants checked on every run:

```rust
if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..48)) {
    panic!("{failure}");
}
```

Use it when the property should hold under *every* interleaving. The workload says what traffic to generate and which invariants must hold; per seed, the runner varies the scheduling, the transport faults of `FaultPolicy`, the registry faults of `RegistryFaultPolicy` (latency, and a stale snapshot bounded by `max_staleness`), and the nemesis.

Each round the nemesis draws one action: a symmetric partition, a one-way partition, a crash (isolation: the process keeps running), a bounded process freeze, a heal, or a quiet round, plus a registry outage in registry-based mode.

A **restart** (real process death, durable state reloaded through the storage seam) joins that list only for a workload that returns a `Rehost` hook from `ClusterWorkload::rehost`. The successor process comes up empty, so without the hook a restarted node loses what `setup` installed, stops leading shards and counting toward a quorum, and the run has shrunk the cluster rather than faulted it. A consenting workload also bounds its calls (`ask_timeout`) and holds no long-lived handle on any node but the first, which the nemesis never restarts.

Restart retires the old process's scheduler domain, so none of its tasks is polled again. The brackets it leaves open (an actor stopped between `DispatchStart` and `DispatchEnd`, an `ask` never answered, an identity never resigned) are what dying looks like, not violations. On `NodeRestarted` the `Checker` calls `Invariant::forget_node` on every invariant before the successor's events reuse the predecessor's identities. An invariant that accumulates per-node state overrides it; one whose claim survives a restart, like `OneLeaderPerTerm` over a reloaded term and vote, does not.

`NoSilentLoss` needs more than a reset: an ask issued *by* a dead caller (nothing is left to wait for it) must be told apart from one issued *to* the dead node by a live caller, which must still resolve with `Unreachable` (#2). So `Event::AskIssued` and `AskOutcome` carry the issuing `caller`, and the count is per calling node.

### 3. Coverage sweep

An invariant sweep that also asserts fault injection fired:

```rust
let stats = run_cluster_swarm_coverage(&workload, coverage_seeds(0..32))?;
assert!(stats.dropped > 0 && stats.duplicated > 0, ...);
```

A sweep that *configures* faults but never *triggers* one gives false confidence. `FaultStats` tallies the faults a run exercised (dropped, duplicated, delayed, blocked, registry_outages, registry_stale), so a green sweep provably covered loss, duplication, reordering, partition/crash, and a registry that went away or answered from behind. The assertion is about the whole declared range, so `coverage_seeds` never narrows it.

### 4. Reproducibility sweep

The determinism contract itself (spec §18.1 #1): run a seed twice and demand byte-identical event streams:

```rust
if let Err(divergence) = replay_cluster_swarm(&workload, sweep_seeds(0..24)) {
    panic!("{divergence}");
}
```

Everything else rests on this. A wall-clock read, an OS thread, an unseeded RNG, or `HashMap` iteration order anywhere in the system breaks it. A `Recorder` runs the workload twice under one seed and pinpoints any `Divergence`, even under cluster nemesis and transport faults (`check_reproducible` / `replay_cluster_swarm`).

### Escape hatch: `scenario_sweep`

A scenario worth sweeping but too specific to fit `Workload` drives the cluster through a hand-written narrative rather than generated traffic. `scenario_sweep` gives such a loop the one thing it otherwise lacks, a name:

```rust
scenario_sweep("partition-safety/split-brain", sweep_seeds(0..12), |seed| {
    let sim = Simulation::new(seed);
    // ... the narrative ...
});
```

Prefer a `Workload` where one fits: it also buys invariant checking and reproducibility sweeps.

## Where things live

**Sweeps go in `*swarm.rs`; scenarios go everywhere else.** Three clauses, the first two checked by `tests/corpus_keys.rs`:

1. A file that calls `run_swarm`, `run_cluster_swarm`, or `run_cluster_swarm_coverage` is named `*swarm.rs`.
2. A file that calls `replay_swarm`, `replay_cluster_swarm`, `check_reproducible`, or `check_cluster_reproducible` is named `*swarm.rs` **or** `*determinism.rs`.
3. `scenario_sweep` is unconstrained: it has no workload to pair with and reads as narrative, so it lives with the scenarios it sweeps (`partition_safety.rs` is the canonical case), and the checker has nothing to enforce.

A scenario failure names a sequence you can read; a sweep failure names a seed you have to replay. Separate binaries keep that legible, keep a slow sweep out of a fast scenario suite's way, and give nextest a name to filter on: `.config/nextest.toml` keeps every sweep out of a plain `cargo nextest run` by `binary(/swarm/)` ([The three runs](#the-three-runs)), so the `*swarm.rs` name is load-bearing.

Clause 2 allows two organizing axes; mixing them within one crate is not allowed. Most crates name a sweep file after its **area** (`entity_swarm.rs`, `sql_swarm.rs`, `ws_swarm.rs`) and pair an invariant sweep with a reproducibility sweep over the same workload. `actor-simulation` instead hoists every reproducibility sweep into `conformance_determinism.rs`, by **shape**, so one file is the §18.1 determinism gate. Because the nextest filter follows the file name, a `*swarm.rs` reproducibility sweep stays out of the default profile with its invariant twin, while `conformance_determinism.rs` runs in the edit loop at `sweep_seeds` width.

## Sizing: how many seeds a run spends

Per-seed cost spans about four orders of magnitude (a millisecond for a local actor sweep, ten seconds for one that drives a machine), so the call site declares a cost class and the *run* decides the width:

| helper           | local width | use for                                        |
|------------------|-------------|------------------------------------------------|
| `sweep_seeds`    | 8           | ordinary sweeps                                |
| `slow_seeds`     | 1           | seconds per seed (disk, SQL, machine bindings) |
| `coverage_seeds` | declared    | coverage assertions, never narrowed            |

Whether a sweep runs at all is the nextest profile's decision ([The three runs](#the-three-runs)).

Sizing is **deterministic on purpose**: deciding width from a wall clock would make the seeds that ran depend on the machine that ran them, which spec §18.1 denies, and `clippy.toml` bans the call in this crate. Wall-time budgeting belongs to whatever drives the suite (`soak.yml` sizes per crate), never under a seed.

Reclassify by measuring, not guessing:

```bash
# per-seed cost = (t32 - t2) / 30
SWARM_SEEDS=2  ./target/debug/deps/<binary>
SWARM_SEEDS=32 ./target/debug/deps/<binary>
```

Warm the binary first: a cold start is ~0.6s and swamps a cheap sweep.

## The three runs

| run   | sweeps run   | seeds                      | job                             |
|-------|--------------|----------------------------|---------------------------------|
| local | on request   | a few, by cost class       | fast feedback                   |
| CI    | all          | from 0, the declared width | regression: the pinned corpus   |
| soak  | all          | from a fresh base, many    | discovery: seeds nobody has run |

`.config/nextest.toml` decides which sweeps a run executes. `[profile.default]` filters out `binary(/swarm/)` and the other suites measured too slow for the edit loop (`native.rs`, `quickjs.rs`, `disk_rounds.rs`, `store_oracle.rs`), so a plain `cargo nextest run` compiles every `*swarm.rs` binary and runs none, about 350 s of serial sweep time saved. The sweeps run four ways:

- `cargo nextest run -E 'binary(/swarm/)'`: just the sweeps, at cost-class width.
- `cargo nextest run -p <crate> --ignore-default-filter`: an excluded suite, when it is the one being edited.
- `cargo nextest run --profile ci`: `all()` with `fail-fast = false`, so one sweep failure does not hide the rest of the corpus; CI runs this.
- plain `cargo test`: ignores the filter; `soak.yml` drives this.

Two environment variables set how many seeds a running sweep takes:

- `SWARM_SEEDS=<n>`: exactly `n` seeds, whatever the class, and may exceed the declared width (how a soak asks for more than the corpus holds). `full` (or `0`) means the declared width; a malformed value falls back to the declared width, never narrower.
- `SWARM_SEED_BASE=<n>`: offset every sweep by `n`. `soak.yml` sets it fresh per run from the run id, so successive soaks explore disjoint ground.

CI pins `SWARM_SEEDS: full`; its metric is cluster-hours exercised across fault configurations, not tests counted. Local runs, with neither set, take the cost-class width.

## Stopping at the first failure, or collecting them all

A sweep stops at the first failing seed. That suits CI (the build is already red) but not a soak, whose job is to mine `(workload, seed)` pairs for the corpus; worse, corpus seeds replay *ahead* of every sweep, so a workload with a failing seed on record fails that replay first and stops exploring until the bug is fixed.

- `SWARM_CONTINUE=1`: run the sweep to the end and report every failing seed as a `corpus.txt` line ready to paste. `soak.yml` sets it; CI leaves it unset.

A collecting run also catches a panic inside a workload (a `drive` that checks its own outcome, a `scenario_sweep` body), attributes it to its seed, and carries on. A seed reached by both the corpus replay and a wide sweep reports once.

## Asserting at quiescence

An at-quiescence assertion is a claim about a run that has stopped, and a run stops only if you let it.

**Wait for the calls to close.** A call issued at the end of the workflow carries its own deadline, and a subsystem may fan out more behind it: entity's quorum append returns at quorum latency and drains the slower replicas afterwards, each with a seconds-long timeout. So `drive_cluster` flushes, then keeps advancing while any `ask` is in flight, up to a cap; "still pending at quiescence" means what it says, and a workload should not add a sleep of its own.

**A heal is not a quiet network.** `net.heal()` clears the nemesis's partitions but not the seeded loss, duplication, and latency, which run for the whole seed. A detector fed lossy probes keeps flipping peers in and out of every view, so the cluster never converges and a run can end mid-divergence however long it waits. A workload asserting something about a **converged** cluster calls `net.quiesce()` alongside the heal, then lets the views settle. Faults already tallied stay counted, so coverage assertions are unaffected.

**Judge liveness by the event that states it.** `SingletonConverged` counts an activation as over at `SingletonStopped` (the manager's report on its next tick) *or* `ResignId` (the actor's identity released), because a run can end between them.

**Bound the check itself.** An end-of-run verification that retries per name can overrun the driver's time budget on a degraded cluster and fail the seed for liveness rather than for anything observed. Bound it in total, so a seed that runs out of budget makes no claim rather than a false one.

**Some subsystems never quiesce, and the remedy is per checker, not per sweep.** A background loop that polls for its node's whole life has something in flight at *any* stopping point, so a quiescence assertion over it fails on some seeds however long the runner waits. The entity alarm driver sweeps its shard's index every 500 ms (`ALARM_DRIVE_INTERVAL`) and breaks two default checkers; `blob-store`'s reconcile loop probes owners continuously and breaks one. `no-silent-loss` is *entirely* a quiescence claim, so `alarm_swarm.rs` and `blob-store`'s `swarm.rs` both drop it. `serial-execution` is two claims: `observe` asserts no reentrant dispatch (real safety, which holds), and `at_quiescence` asserts nothing is left open. `blob-store`'s swarm keeps it whole; the alarm driver re-activates entities forever and leaves a dispatch open at an arbitrary stop, so `alarm_swarm.rs` wraps the real checker in `NotReentrant` and overrides only the final call, keeping the safety half. Before deleting a failing checker, ask which of its claims the subsystem satisfies. Dropping a quiescence claim stays honest only while the workload awaits every op it issues to an outcome, which keeps the data path's no-loss covered without the checker.

## The regression corpus

`crates/actor-simulation/corpus.txt` holds representative failing seeds keyed by workload name, pruned to a few per workload (a collecting soak reports one finding a hundred times over, and a few seeds guard it as well as all):

```
singleton-chaos/leader 900006  # split-brain singleton after heal
```

Recorded seeds replay on **every** run of that workload (local, CI, and soak), regardless of sizing. Corpus seeds are absolute, so `SWARM_SEED_BASE` moves sweeps but never the regressions.

Adding one is a copy from the failure message:

```
workload 'singleton-chaos/leader' failed at seed 900006
└─ becomes: singleton-chaos/leader 900006
```

and it replays on its own with:

```bash
SWARM_SEED_BASE=900006 SWARM_SEEDS=1 cargo test -p <crate> --test <name>
```

`tests/corpus_keys.rs` enforces, besides the location rule above, that names are unique tree-wide (a shared name would make one workload replay another's regressions) and that every corpus key names a real workload (a typo would silently guard nothing).

A corpus seed guards a bug only while it still drives the code down the same path, and any change to how much entropy anything draws re-randomizes what a seed means. So the seed is the *discovery* mechanism; once the bug is understood, the durable regression is a scenario test that reproduces its shape. Write that too, and keep the corpus line as a cheap backstop.

## A workload outlives its runs

`run_cluster_swarm` drives every seed through the **same** workload value (`drive` takes `&self`), so a field on the workload is sweep-scoped, not run-scoped. Of the two kinds of state that end up there, only one belongs:

- **Tallies** ("did this ever happen across the sweep": faults that fired, activations that hibernated, reads that were actually checked, a violation flag a client set) are cumulative on purpose. `support::Exercised` is the shared one.
- **Expectations** ("what this run should find at the end": the names a run acknowledged, the blobs it stored) are per *run*, and a field is the wrong place for them. Build them inside `drive` and share them among that run's client tasks with a local `Arc<Mutex<_>>`.

Getting this wrong fails *convincingly*: seed N's expectations are checked against seed N+1's freshly empty entities, so the sweep reports acknowledged writes that vanished, a textbook E14 violation, on a system that did nothing wrong. The tell: it fails on every seed *except the first*, which has nothing to inherit.

## What counts as an acknowledgement

A durability claim is only as good as the reply it trusts. Three mistakes recur:

- **A no-op commits nothing.** `tenancy`'s directory entity answers a `Record` whose name is already owned with identical metadata with `Recorded::Unchanged`. That reports what the serving activation believed but journals no record, so entity's output gate never held its reply, and a quorum-less recovery may have seeded that belief with an uncommitted record (entity §7.5). Only outcomes that journal, `Recorded::Created` and `Recorded::Updated`, are acknowledgements.
- **A read is not a probe.** The entity layer's read contract is read-your-leader (relaxed), *not* linearizable under partition (entity §7.5). To observe committed state, issue a trivial *writing* command rather than a query.
- **A mutating command needs an idempotency key.** Delivery is at-most-once per attempt (§7.2), but a caller's retry, or the sweep's frame-duplication fault, delivers the same command twice; a deposit without a key deposits twice, and the sweep blames durability for a test bug.

## Adding a sweep

1. Write the workload (`Workload` for single-node, `ClusterWorkload` for a cluster) and give it a name unique across the workspace.
2. Put it in `<area>_swarm.rs` with an invariant sweep and a reproducibility sweep over it; if the crate hoists determinism by shape, the reproducibility sweep goes in its `*determinism.rs`.
3. Size it: `sweep_seeds`, or `slow_seeds` if a seed costs seconds.
4. If it injects faults, add a coverage sweep with `coverage_seeds` for the transport faults `FaultStats` counts. For a fault the wire cannot see (an entity that hibernates, a process that dies), tally the events that say so and assert once at the end; where that goes depends on **which kind of claim it is**:
   - Something the *workload* drives holds however narrow the run, so it can ride the invariant sweep. Make it deterministic: idle past `idle_after` on a fixed cadence rather than a seeded coin, and snapshot often enough that an entity has a checkpoint to return from before it first hibernates. `entity`'s `support::Exercised` is the shared tally.
   - Something the *nemesis* draws is a property of the seed range and needs `coverage_seeds`, which never narrows. At `slow_seeds`' single local seed a six-round nemesis misses any one action about two runs in five, so on an invariant sweep the assertion is only flaky, not stricter.
5. If it needs an entity or actor a scenario suite already drives, take it from `tests/support/`. A fixture whose *shape* must differ between scenarios and sweep takes a config field, not a second copy; `entity`'s `support::pipeline` switches its alarm-backed sleep off for the sweep that way.
6. When soak finds a failing seed, add it to `corpus.txt`, and write the scenario test once you know what broke.

Two shaping mistakes produce a sweep that runs and asserts nothing:

- **A constant-valued fixture cannot say which write won.** For a property about *which* attempt is durable (a memoized step result, a first-writer-wins register, a deduplicated command), the fixture's effect must produce a **different value each time it runs**; with a constant, an overwritten memo and a preserved one read identically. `entity`'s steps sweep hands every step launch a fresh ordinal for this reason.
- **Pace a round by the clock, not by its calls.** A drive that awaits round *N* before opening round *N+1* lets one refused call stretch a 500 ms round into a `CALL_DEADLINE` one, and the drive shares one virtual-time budget, so on the faulted seeds the sweep exists for, later rounds are never issued. Sleep to each round's *offset* and let rounds overlap.

## Shared fixtures and invariants

Test code is shared rather than copied because an independently maintained copy can drift, and a weakened one silently stops checking what its name claims.

### Where an invariant goes

One question decides it: **whose contract is this a claim about?**

| the claim is about | it lives in | example |
|--------------------|-------------|---------|
| the actor framework | `actor-simulation` | `NoSilentLoss`, `OneLeaderPerTerm` |
| one crate's contract | that crate's `testing` feature | `entity::testing::CommitMonotonic` |
| one suite's workload | that suite's own file | `SingletonConverged` in `conformance_swarm.rs` |

The middle tier is feature-gated because it is test support, not crate API, and must not ship in a production build. `entity`, `workflow`, and `agent` each have one; a crate reaches another's by enabling it in dev-dependencies (`entity = { workspace = true, features = ["testing"] }`), and its own by depending on itself (`agent = { path = ".", features = ["testing"] }`).

Each shared invariant is constructed with the label it reports under, so a suite still names its own violations:

```rust
Box::new(CommitMonotonic::new("machine-commit-monotonic", "machine"))
```

Before writing an invariant, check whether one of the first two tiers has it. Near-identical names across layers are the trap: the cluster-utilities `SingletonAtMostOnePerNode` (U2, off `SingletonStarted`) and the entity `ActivationSingletonPerNode` (E6, off `EntityEvent::Activated`) are different claims, and each doc comment points at the other.

### Where a fixture goes

**Fixtures** live in `tests/support/`, shared between one crate's test binaries so a sweep covers the same entity the scenarios specify: `support::Ledger` is the SQL-facet entity that `sql.rs` and `sql_swarm.rs` both drive, and `support::CounterEntity` is shared by `entities.rs` and `entity_swarm.rs`. A crate with no `testing` feature may also keep suite-local invariants here; once a second crate wants one, it is a contract claim and moves up a tier.

## Invariants to assert

§18.5 is the catalogue: twenty-two numbered invariants, each a MUST stated inline in the spec. The [cluster utilities](cluster-utilities-spec.md) carry their own, separately numbered catalogue (U1, U2, …; machine-readable as `utilities_catalogue()`), held to the same drift discipline, as do the layers above: the wal (L1–L4), entity (E1–E21, plus the facet contract F1–F4), the workflow (W1–W7), the agent (A1–A7), the sandbox (S1–S5), the machine (M1–M6), and the blob store (B1–B7). Format compatibility (V1–V2) is the one catalogue with no Rust copy to drift from: each format's own tests hold its stamp to them (compatibility §3). Assert them; do not re-derive them.

The core catalogue's properties are framework ones, not database ones: the actor layer has no durability, serializability, or lost-update notion (it is in-memory, at-most-once, eventually consistent, §1.2, §7.2).

Verification is **layered** (§18.6): the safety core runs continuously, and the rest use the method that fits (a targeted conformance test for a liveness or scenario property, a compile-fail case for #20, a differential local-vs-remote run for #21). The machine-readable `core_catalogue()` records each invariant's method, and the `conformance_catalogue` test fails the build if a continuous checker and its catalogue entry drift apart, keeping the §17 conformance criterion mechanically true.

## Checklist

For each component, write:

1. **Roundtrip** tests for every codec encode/decode pair, and for `ActorRef` rebinding across the wire (§5, §4.4; #10).
2. **Idempotency / duplicate-tolerance** tests: a retried or transport-duplicated message can arrive twice (§7.2), so a retriable operation must carry an explicit idempotency key and survive a duplication fault.
3. **Reference-model** tests for stateful actors (§18.4).
4. **Simulation workloads** asserting the §18.5 invariants under the §18.3 faults.
5. **Node-crash** tests that crash a node mid-run and verify the cascade (§8.1): `Terminated { NodeDown }` to watchers, `Unreachable` to in-flight callers, receptionist pruning. There is no durability to verify at the actor layer: a restart constructs fresh state (§11.2).
6. **Compile-fail** tests (`trybuild`) for invalid sends (#20).
7. **Seed-reproducibility** checks (§18.1 #1).
8. **Fault-coverage** assertions (`FaultStats`).

## Where the sweeps do not yet reach

Known gaps between what the sweeps exercise and what the specs mandate, each a place a bug could live undetected.

- **`agent-sandbox`, `agent-gateway`, and `machine-frontdoor` have no sweep.** They are I/O-boundary crates, which the simulator reaches only indirectly. The sandbox states its fault model (sandbox §4.1): four faults, and the two of its five invariants a sweep over them can assert, so a workload there has something to inject and something to claim. The gateway and front door have no such statement, and a sweep whose faults are not the seam's faults asserts nothing while looking like coverage, which is worse than the gap. For those two, decide first: pick one, answer what a fault is in its own spec's terms, and only then write a workload.
