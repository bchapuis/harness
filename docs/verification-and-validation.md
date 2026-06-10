# Verification & Validation

**Scope:** How the distributed actor framework is tested. The companion to the [specification](distributed-actor-spec.md): §16 (events), §17 (conformance), and §18 (deterministic simulation) say *what* must hold and define the trait contract that makes testing possible; this document is the *how* — the strategies, the primitives, and the shape they take in `actor-simulation`.

Three principles govern the strategy.

- **Determinism.** Replay every failure from a `(seed, configuration)` alone (§18.1, §18.6). One seed drives time, randomness, and scheduling, so an entire multi-node run — including its faults — reproduces exactly.
- **Specification.** Assert the invariants of §18.5, not chosen outputs. Correctness is a small set of properties checked over the §16 event stream, not a pile of example assertions.
- **Fault injection.** Bugs hide in the failure paths. Inject partitions, crashes, loss, duplication, delay, and reordering under seed control (§18.3), and prove the injection actually fired.

## Dependencies

Build the harness; do not import one. The spec routes every source of nondeterminism through four traits — `Clock`, `Entropy`, `Spawner` (§4.6), and `Transport` (§7) — and §18.2 mandates that simulation reuse *those same traits*, swapping only the implementations. A generic network or scheduler crate (`madsim`, `turmoil`, `shuttle`, `loom`, `stateright`) cannot satisfy that contract: it would test a model of the system, not the real `ActorSystem`, mailbox, SWIM, supervision, and receptionist code. So the simulator, the invariant checkers, the reproducibility harness, and the linearizability decision are all owned, in `actor-simulation`, atop `rand_chacha`.

The one external test-only tool is **`trybuild`**, which drives the compile-fail cases for invariant #20 (an invalid `ask`/`tell` must not compile) — a property no runtime test can express. Data-race tooling (`loom`, Miri's race detector) is largely moot here: the workspace sets `unsafe_code = "forbid"`, and the simulator already explores interleavings deterministically (see *Interleaving*, below). Fuzzing has one valid home — the production transport's framing — noted under *Fuzzing*.

## Primitives

**One seeded stream.** `Entropy` is the single source of randomness in the system (§4.6); every draw — application randomness, gossip peer selection, SWIM's `k` members, backoff jitter, and the scheduler's own tie-breaks — comes from it. Production seeds it from the OS (`OsEntropy`); simulation seeds one `ChaCha8` stream from the run seed (`SimEntropy`), and cloning a handle shares the *same* stream, which is what makes a run reproducible.

```rust
pub trait Entropy: Send + Sync + 'static {
    fn next_u64(&self) -> u64;
    fn pick_index(&self, len: usize) -> Option<usize> { /* uniform over 0..len */ }

    /// A fault gate (spec §18.3). Fires with probability numerator/denominator,
    /// drawn from this stream. Off in production (the default returns false), so
    /// `buggify` call-sites in the runtime cost nothing outside simulation.
    fn buggify(&self, _numerator: u64, _denominator: u64) -> bool { false }
}
```

**`buggify` is a method, not a macro.** A runtime call-site such as `if entropy.buggify(1, 4) { /* inject */ }` vanishes in production (the trait default is `false`) and, under `SimEntropy`, fires deterministically from the seeded stream. Faults thus live inline in the real code path, gated by the same entropy that drives the rest of the run.

**Quiescence-driven time.** The simulator's `Clock`, `Spawner`, and run loop share one scheduler. It polls every ready task until none remain, and only then advances virtual time to the next registered timer (§18.1 #2). A timeout, a SWIM interval, or a backoff therefore costs no wall-clock time — a run covers hours of cluster time per CPU-second — and ready-task selection is seed-randomized, so scheduling itself is a fault dimension.

## Strategies

**Deterministic simulation (the core).** A whole cluster runs in one process, on one logical thread, over virtual time, network, and randomness (§18). Construct a `Simulation` from a seed, hand its `clock()`, `entropy()`, and `spawner()` to a system, and drive it to quiescence. Because these are the *same* traits production uses (§18.2), the codec stays real and every cross-node hop tests the wire encoding.

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

**Workloads (§18.4).** A test is a `Workload`: build actors and registrations, drive traffic through the **public API only** (never actor state — `when_local` excepted, §3.5.1), then let the runner assert invariants at quiescence. `run_swarm` / `run_cluster_swarm` sweep one workload across many seeds, sampling a `FaultConfig` / `FaultPolicy` from each seed's stream. Coverage is *cluster-time exercised per change*, not test count. A failing run is reported as a `RunFailure` carrying the `(seed, faults)` needed to replay it.

**Continuous invariant checking.** Rather than bespoke per-scenario assertions, a small set of always-on `Invariant`s observe the event stream live through a `Checker`, on every run and at final quiescence. Seven ship as continuous checkers today — `NoSilentLoss` (#1), `SerialExecution` (#4), `LifecycleExactlyOnce` (#6), `SignalInBand` (#13), `DownIsTerminal` (#15), `OneLeaderPerTerm` (#22), and `SingletonAtMostOnePerNode` (utilities U2) — chosen because each is a safety property ("a bad thing never happens") expressible over the existing §16 events. `SignalInBand` (#13) holds the line that a `Terminated` is delivered *through the watcher's mailbox*, never out of band: since a signal flows through `enqueue_signal` (an `Enqueue` of the `Terminated` manifest) before the serial loop dispatches it, a `DispatchStart` of that signal with no matching prior `Enqueue` is an out-of-band delivery, caught live. It is a *per-event* (prefix) property, so it is sound for both quiescence-driven single-node runs and the time-bounded cluster runs (`run_for`) that stop mid-flight.

Promoting a *true* safety invariant from a targeted test to a continuous checker is always sound — but not every §18.5 invariant is one, and two are deliberately left as targeted tests:

- **Death-watch exactly-once (#11)** is *not* "at most one `Terminated` per `(target, watcher)`": a watcher may legitimately `watch` the same target again, and watching an already-terminated actor yields a fresh `Terminated` (§12, #12) — the receptionist does exactly this under anti-entropy. The event stream carries no per-`watch` identity, so "exactly one *per watch*" is not expressible as a continuous safety property; #11 stays targeted.
- **Bounded, non-dropping mailbox (#5)** is structural and per-call, not emergent: the mailbox is a fixed-capacity channel (the bound cannot be exceeded), and backpressure is an API contract (`tell` awaits, `try_tell` returns `MailboxFull`). A depth checker would need per-actor capacity on the stream, and "depth 0 at quiescence" is unsound for `run_for` cluster runs. So #5 stays targeted.

**Reference-model testing (linearizability).** For a stateful actor, record the client-observed `History` of operations (invoke → ok/info/fail) and decide it against a sequential reference `Model` — `Register` and `Counter` ship — with `check_linearizable`, a Wing & Gong search with `(used-bitmask, state)` memoization (`MAX_HISTORY = 128`). This is the state-machine strategy: generate concurrent operations, then prove the observed history is consistent with *some* serial order of the model.

**Seed-reproducibility.** The determinism contract (§18.1 #1) is itself a tested property, enforced over the *real* event stream: a `Recorder` runs a workload twice under one seed and asserts byte-identical `Vec<Event>`, pinpointing any `Divergence`. This holds even under cluster nemesis and transport faults — `check_reproducible` / `replay_cluster_swarm`. A single leak (a wall-clock read, an OS thread, an unseeded RNG) breaks it, which is the point.

**Fault coverage.** A sweep that *configures* faults but, by seed luck, never *triggers* one gives false confidence. `FaultStats` tallies the faults a run actually exercised (dropped, duplicated, delayed, blocked); `run_cluster_swarm_coverage` asserts, across the seed range, that each fault type fired at least once — so a green sweep provably covered loss, duplication, reordering, and partition/crash, not just the happy path.

**Compile-fail testing.** Invariant #20 — an `ask`/`tell` of a message an actor has no `Handler` for must not compile — is asserted by `trybuild` cases under `actor-core/tests/compile_fail`, not at runtime.

**Interleaving.** The simulator's single-thread cooperative scheduler selects among ready tasks with seeded randomness (§18.3), so it already explores message interleavings deterministically and reproducibly. A separate `loom` model-check of the executor across *all* interleavings is therefore an optional, complementary cross-check (§18.6) — not a prerequisite — and is not currently wired in.

**Fuzzing.** Frame corruption is meaningful only where real bytes exist. The in-memory simulator carries *structured* frames (only the payload is codec-encoded, §18.2), so it has nothing to bit-flip; the "malformed frame tears down the association, not the node" requirement (§7) belongs to the **production** TCP transport's framing, tested against real wire bytes. A `cargo-fuzz` target over that framing and the codec is the natural place for byte-level fuzzing — a noted enhancement.

## Invariants to assert

§18.5 is the catalogue — twenty-two numbered invariants, each a MUST stated inline in the spec. The [cluster utilities](cluster-utilities-spec.md) carry their own, separately numbered catalogue (U1, U2, …; machine-readable as `utilities_catalogue()`), held to the same drift discipline. Assert them; do not re-derive them. They are framework properties, not database ones: there is no durability, serializability, or lost-update notion here (the actor model is in-memory, at-most-once, eventually consistent — §1.2, §7.2). The shape worth internalizing:

- **Messaging & execution (#1, #3–#5, #9).** No silent loss; per-pair FIFO under reordering; serial, non-reentrant dispatch; bounded non-dropping mailbox; local sends skip serialization yet match the remote result.
- **Identity & dispatch (#6–#8, #10).** Lifecycle order and exactly-once; `resolve` classifies locality with no round-trip; unregistered `(type, manifest)` → `Unhandled`; `ActorRef`s rebind on decode.
- **Failure & monitoring (#2, #11–#13, #18).** A downed node completes in-flight `ask`s with `Unreachable`, never hangs; death-watch exactly-once including `NodeDown`; watch-after-death fires immediately; signal ordering; supervision contains panics (default `Stop`, restarts back off).
- **Membership (#14–#17, #19, #22).** Convergence after partitions heal; `down` is terminal; a partition alone never downs a member; SWIM refutation via incarnation; receptionist pruned on node `down`; the leader-based control plane is quorum-gated with at most one leader per term.
- **Type-safety & transparency (#20, #21).** Invalid sends do not compile; local vs remote targets produce identical replies and ordering.
- **Cluster utilities (U1, U2).** Placement is a pure, version-stable function of the serving set with minimal movement; singleton activations never overlap on one node, a healed converged cluster runs exactly one per name, and an anchor failure re-activates.

Verification is **layered**, not uniform (§18.6). The safety core runs continuously; the rest are verified by the method that fits — a liveness or scenario property by a targeted conformance test, #20 by a compile-fail case, #21 by a differential local-vs-remote run. The machine-readable `catalogue()` records, per invariant, which method applies, and the `conformance_catalogue` test fails the build if a continuous checker and its catalogue entry drift apart — so the §17 "Verified by" column stays mechanically true.

## Checklist

For each component, write:

1. **Roundtrip** tests for every codec encode/decode pair, and for `ActorRef` rebinding across the wire (§5, §4.4; #10).
2. **Idempotency / duplicate-tolerance** tests: at-most-once delivery means a retried or transport-duplicated message can arrive twice (§7.2); a retriable operation must carry an explicit idempotency key and survive a duplication fault.
3. **Reference-model** tests for stateful actors: a `History` decided against a `Model` (§18.4).
4. **Simulation workloads** that assert the §18.5 invariants under the §18.3 faults — partition, crash, loss, duplication, delay, reordering — not just the happy path.
5. **Node-crash** tests that abruptly crash a node mid-run and verify the cascade (§8.1): `Terminated { NodeDown }` to watchers, `Unreachable` to in-flight callers, receptionist pruning. (There is no durability to verify — a restart constructs fresh state, §11.2.)
6. **Compile-fail** tests (`trybuild`) for invalid sends (#20).
7. **Seed-reproducibility** checks: the same `(seed, config)` yields a byte-identical event stream (§18.1 #1).
8. **Fault-coverage** assertions: the sweep actually fired each fault type (`FaultStats`), so a green run is not a silently happy-path run.

Commit every failing `(seed, configuration)` as a fixed-seed regression case and replay it permanently; the standing swarm sweeps are the corpus. CI runs many seeds per change across fault configurations — the metric is cluster-hours exercised, not tests counted. That habit makes the suite a ratchet.
