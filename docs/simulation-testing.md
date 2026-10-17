# Simulation testing

**Scope:** how the framework and everything above it is verified: what the simulator varies, the shapes a test takes, and the practice around seeds, the corpus, and assertions. The [actor specification](distributed-actor-spec.md) states what must hold (§2.7 events, §2.12.2 determinism, §10 invariants); `actor-simulation` is the mechanism, and its rustdoc is the API manual, with examples. Three principles govern the practice:

- **Determinism:** every failure replays from a `(workload, seed)` pair alone (§2.12.2). One seed drives time, randomness, scheduling, and the run's sampled fault configuration.
- **Specification:** assert the catalogued invariants over the §2.7 event stream, not a pile of example outputs.
- **Fault injection:** bugs hide in the failure paths, so inject faults under seed control and prove the injection fired.

## The simulator is owned, not imported

All nondeterminism passes through `Clock`, `Entropy`, `Spawner` (§2.12.1), the `Transport` (§2.13), and the registry client (§6.4.2), so a simulation swaps implementations and runs the real system code, codec included. A generic simulation crate would test a model of the system instead. The one external test tool is `trybuild`, for invariant #20. Data-race tooling adds little: the workspace forbids `unsafe_code`, and the scheduler explores interleavings deterministically.

## What a run varies

From its seed, each run samples a random subset of faults at random intensities, and a run with no faults must still pass:

- **Time** is logical and advances only when nothing is ready (§2.12.2), so timeouts cost no wall-clock time.
- **Randomness** is one seeded stream; fault gates draw from it but exist only in the simulator.
- **Scheduling** picks among ready tasks at random, so interleaving is a fault dimension, and a single-node run draws its mailbox capacity, so backpressure is one too.
- **The network** drops, duplicates, delays, and reorders frames, and a nemesis adds symmetric and one-way partitions, isolation crashes, bounded freezes, and heals.
- **The registry**, in registry-based mode, answers late, stale, or not at all.
- **A restart** (process death, durable state reloaded) happens only to a workload that consents through `ClusterWorkload::rehost`.

Byte corruption is tested against the production transport's real framing, since the simulator carries structured frames; handler faults come from workload actors that fault on demand.

## Strategies

- **Continuous invariants:** always-on checkers watch the event stream on every run and at final quiescence. The shipped ones cover #1, #4, #6, #13, #15, #22, and utilities U2. Promoting a true safety property to a checker is always sound; #5 (the mailbox bound is structural) and #11 (exactly once *per watch*, and the stream carries no watch identity) stay targeted tests.
- **Reference models:** record a client-observed `History` and decide it with `check_linearizable` against a sequential `Model`. The search is exponential in pending operations, so keep such workloads small and single-key and take breadth from the seeds.
- **Compile-fail and differential checks:** `trybuild` cases for #20; a local-versus-remote run for #21.
- **Exhaustive enumeration:** a sweep is a sample, so where the state space is small, enumerate it (every truncation offset and every single-byte corruption of a short log, for instance).

## The shapes

### 1. Scenario

One fixed seed, one hand-built interleaving: a specification of one concrete history. Use it when the thing under test is a particular sequence (a handshake, a restart, a race) and a failure should point at that sequence. Scenarios do not sweep and carry no corpus.

### 2. Invariant sweep

A `Workload` or `ClusterWorkload` run across many seeds by `run_swarm` or `run_cluster_swarm`, its invariants checked on every run. Use it when a property must hold under every interleaving. A single-node run drives to quiescence, but a cluster run is time-bounded (the failure detector never quiesces), so a property claimed for both must hold at every prefix.

### 3. Coverage sweep

An invariant sweep that also asserts its faults fired, from the `FaultStats` that `run_cluster_swarm_coverage` returns. A sweep that configures faults but never triggers one gives false confidence.

### 4. Reproducibility sweep

The determinism contract itself (§2.12.2): run each seed twice and demand byte-identical event streams (`replay_cluster_swarm`). Everything else rests on it; a wall-clock read, an OS thread, an unseeded RNG, or hash-map iteration order anywhere breaks it.

**Escape hatch:** a narrative too specific for a workload can still sweep under a name with `scenario_sweep`. Prefer a workload where one fits: it also buys invariant checking and reproducibility.

## Where things live

Invariant and coverage sweeps live in files named `*swarm.rs`, reproducibility sweeps in `*swarm.rs` or `*determinism.rs`, and scenarios everywhere else; a test in `actor-simulation` enforces it. A scenario failure names a sequence you can read, a sweep failure a seed you must replay, and the default nextest profile skips sweep binaries by that name, so the name is load-bearing. A crate pairs each invariant sweep with a reproducibility sweep over the same workload.

## Seeds: sizing, runs, and the corpus

A call site declares a cost class (`sweep_seeds`, `slow_seeds` for seconds per seed, `coverage_seeds` for a coverage claim that never narrows), and the run decides the width: a local run takes a few seeds, CI takes the declared width from seed 0 (regression), and a soak takes many from a fresh base (discovery). Sizing never reads a clock, because the seeds that ran must not depend on the machine (§2.12.2). Plain `cargo nextest run` runs no sweep; `--profile ci` runs them all. The environment knobs are documented in `actor-simulation`.

Failing seeds go in `corpus.txt`, keyed by workload name, and replay on every run of that workload, whatever the sizing; corpus seeds are absolute. A corpus seed guards a bug only while it drives the code down the same path, and any change to how much entropy something draws re-randomizes it. So the seed is the discovery; once the bug is understood, write the scenario test that reproduces its shape and keep the corpus line as a backstop.

## Writing an assertion that means something

**Asserting at quiescence:** let the driver wait for in-flight calls rather than adding sleeps. A heal clears partitions but not seeded loss, so call `SimNetwork::quiesce` before asserting anything about a converged cluster. Judge liveness by the event that states it, and bound an end-of-run check in total, so a seed out of budget makes no claim rather than a false one. A subsystem that polls forever never quiesces: keep the safety half of the checker, and drop a quiescence claim only while the workload awaits every operation it issues.

**A workload outlives its runs:** every seed of a sweep drives the same workload value, so a field is sweep-scoped. Tallies ("did this ever happen") belong there; per-run expectations ("what this run should find") belong inside the drive. The tell of getting it wrong: every seed fails except the first.

**What counts as an acknowledgement:** a no-op journals nothing, so its reply proves nothing durable. A read is not a probe: entity reads are read-your-leader, not linearizable under partition (entity §6), so observe committed state with a trivial write. A mutating command needs an idempotency key, because retries and the duplication fault deliver it twice (§4).

**Adding a sweep:** name the workload uniquely across the workspace, pair its invariant and reproducibility sweeps, and size it. A claim about what the workload drives can ride the invariant sweep if the workload makes it deterministic; a claim about what the nemesis draws needs `coverage_seeds`. A constant-valued fixture cannot say which write won, so make each run's effect distinct, and pace rounds by the clock, so one refused call cannot starve the budget.

## Shared fixtures and invariants

An invariant lives with the contract it is a claim about: the framework's in `actor-simulation`, one crate's in that crate's `testing` feature (test support, never shipped), one suite's in that suite's file. Fixtures live in a crate's `tests/support/`, so a sweep drives the same entity its scenarios specify. Each shared invariant is built with the label it reports under. Check the first two tiers before writing a new one, and mind near-identical names across layers: utilities U2 and entity E6 are different claims.

## Invariants to assert

Assert the catalogues; do not re-derive them: the actor core (§10, #1–#22), the cluster utilities (U1–U2), the wal log (L1–L4, catalogued in its crate docs), the entity (E1–E21, plus the facet contract F1–F4), the workflow (W1–W7), the agent (A1–A7), the sandbox (S1–S5), and the machine (M1–M6). Format compatibility (V1–V3) has no Rust copy; each format's tests hold its stamp to it. Per component, write codec roundtrips, duplicate-tolerance tests for retriable operations, reference-model tests for stateful actors, sweeps under faults, node-crash tests of the cascade (§5.1), compile-fail tests, reproducibility checks, and coverage assertions.

## Where the sweeps do not yet reach

- **No sweep for `agent-sandbox`, `agent-gateway`, or `machine-frontdoor`:** the sandbox states a fault model a workload could inject (sandbox §5.3); the gateway and the front door state none, and a sweep whose faults are not the seam's faults only looks like coverage. Define the faults first.
- **No fuzz target** covers the production transport's framing and the codec.
- **No `loom` model-check** of the executor; the seeded scheduler is the substitute.
