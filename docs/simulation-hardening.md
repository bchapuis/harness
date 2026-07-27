# Simulation hardening: where the sweeps do not yet reach

An audit of what deterministic simulation currently exercises against what the
specs mandate, written after the round of fixes recorded in `corpus.txt`. The
mechanism and conventions are in [simulation testing](simulation-testing.md);
this file is the list of gaps, ranked by leverage, and it should be deleted once
its findings are resolved or refuted.

Sections 1–6 were the original audit and are now resolved except where noted.
Section 0 is what closing them turned up, and it is the one thing here that is a
**defect in the system rather than in its tests**.

## 0. OPEN — acknowledged writes lost through the quorum-less recovery fallback

A committed, acknowledged grain write can be permanently invisible after a
partition heals, and a later write can commit **onto the slot it occupied**.
Found by two of the sweeps added below, on two unrelated grains, so it is
granary's and not any one consumer's.

### The reproduction

```bash
cargo test -p tenancy --test directory_swarm -- --ignored --exact \
    the_directory_index_survives_the_cluster_swarm
```

Fails on nearly every seed past 0. It is `#[ignore]`d rather than left red so the
rest of the tree stays green; the other tests in that file drive the same traffic
and hold. The account grain shows the same thing independently — widen the
existing sweep past its declared range and it fails too:

```bash
SWARM_SEEDS=60 cargo test -p granary --test grain_swarm --exact \
    the_account_grain_is_linearizable_under_cluster_faults
```

### What is observed

Minimised to: one client, one principal, keep-only traffic (no removals), no
hibernation, no restarts, plain nemesis, and a `heal()` + `quiesce()` plus a
settle before reading. Three `Record`s return `Ok(Created)` — so each journaled
its events and passed the output gate, which releases a reply only after commit
(G5). The index then reads **empty**, and stays empty across twelve seconds of
probes on a quiesced cluster, so it is not a transient stale read. A fresh
`Record` afterwards returns `Ok(Created)` and leaves the index holding exactly
**one** entry: the new write took seq 1, the slot the first committed record
already held.

### The mechanism

`Replicator::recover` (`crates/granary/src/replicator.rs`) falls back to the
node's local view when a recovery cannot reach a read quorum, and its comment
argues this is safe because "a write from it still needs a quorum and a
stale-head append is rejected by an up-to-date replica's optimistic check (§8)".
Two things appear to go wrong with that reasoning:

1. A leader that recovers `head = 0` from an empty local view serves an empty
   grain, and — because the recovered head is written back under its own, higher
   term — a subsequent append at seq 1 is not a *stale* head to the replicas that
   hold the real records. The optimistic check compares terms, and this term is
   newer.
2. Nothing re-recovers once a quorum becomes reachable again. The activation
   keeps serving its adopted head, so the loss outlives the partition rather than
   converging after it.

§7.5 sanctions a relaxed "read-your-leader" read that may be stale. An empty view
that never converges, and that a later write overwrites committed slots from, is
past what that sanctions and squarely contradicts **G14** ("no acknowledged write
is lost") and **G11** (a shard that cannot reach a quorum pauses rather than
serving a fork).

### Why the existing sweeps missed it

None of them ever asked. `granary-account-swarm` checks only commit
monotonicity; `granary-ws-swarm`'s read check is deliberately permissive about
ambiguous outcomes; every other suite reads a value back *immediately* after
writing it, while the writing activation is still the one serving. Nothing read
the accumulated state back at the end of a run and held it to what had been
acknowledged — which is exactly the gap §5 below was about.

### Deciding the fix

Not attempted here: it is a CP-correctness change in granary's core, and the
options trade against each other (make the read path require a quorum and return
`Unavailable`, per G11; or keep the relaxed read but force re-recovery once a
quorum is reachable and refuse appends derived from a quorum-less head). That is
a design decision, not a patch.

## 1. The nemesis vocabulary is narrower than the fault library

`nemesis()` (`actor-simulation/src/cluster_swarm.rs`) draws from four actions —
two-way partition, crash, heal, quiet — plus a registry outage in registry-based
mode. `SimNetwork` implements three faults it never draws:

- **`partition_one_way`**. The asymmetric partition, whose doc names it "the
  source of zombie leaders — the case symmetric partition cannot make". It
  appears in one file tree-wide, `conformance_leader.rs`. No sweep produces one.
- **`pause`/`resume`**. A process freeze: the node keeps its state, its inbound
  frames queue, and its overdue timers all fire at once on thaw. A paused leader
  does not climb its term, so it wakes already deposed. Used only in
  `conformance_leader.rs` and `firecracker_e2e.rs`; never swept.
- **`restart`**. Real process death — volatile state lost, durable state
  reloaded through the storage seam. `crash` is only *isolation*; the process
  keeps running. The one sweep that restarts anything is `restart-churn`
  (`conformance_swarm.rs`), at the framework tier. No granary, machine,
  blob-store, or harness sweep ever restarts a node, so journal recovery after
  process death is never crossed with concurrent traffic — which is the §8
  quorum-head-recovery path the worst finding in the corpus (the G14 stale-head
  overwrite) came from.

Widening the vocabulary is one edit to shared code that reaches every cluster
sweep at once, rather than a new workload per gap.

Two constraints shape how. A restart invalidates the `SimNode` handles a
workload cloned out of `ClusterCtx::nodes()`, so it cannot be unconditional —
`restart-churn`'s own discipline (keep the first node as the stable caller) is
the pattern to generalize. A pause must be bounded within its round, or the run
stalls rather than being faulted.

Widening also re-randomizes what every existing seed means, so the recorded
corpus stops guarding what it was recorded for. That is expected and already
documented; the durable guards are the scenario tests written alongside each
corpus entry.

**Status.** Resolved. The one-way partition and the bounded freeze are in the
nemesis unconditionally; the restart is in it for any workload that supplies a
`rehost` hook, and five granary workloads now do.

## 1a. A restarted node's old process kept running — fixed

`SimNetwork::restart` documented that "the old instance is shut down *before* its
successor exists, and a shut-down node processes nothing further". The second
half did not hold. `ClusterSystem::shutdown` sets a flag and drops the transport;
it does not stop the system's actor tasks, and the successor was brought up on a
spawner domain keyed by `node.uid()` — the *same* domain the predecessor's tasks
carried. The two incarnations ran concurrently, and because a fresh process
numbers paths and incarnations from zero, they shared one `ActorId` space.

It stayed invisible while nothing re-hosted a restarted node: `restart-churn`
never re-spawns its actors, so the dead process simply went quiet. Giving the
node real work to resume — a granary host, with a gateway, a shard-map group, a
replicator and live activations — made it loud. A 500-seed soak of the
hibernating account workload failed 4 seeds (0.8%), every failure an overlap
artifact: `resigned before assign` and `ready before assign` as the old process
wound down past its replacement, a `reentrant dispatch`, and one G6 `activated
while already live` for a grain both incarnations held at once.

**The fix, in two halves.**

*The executor ends the process.* A scheduler domain is now per **process
incarnation** rather than per node (`SimSpawner::with_fresh_domain`), and
`restart` retires the outgoing one (`Inner::retire_domain`): its tasks leave the
ready set and the task table and are never polled again. `SimNetwork` keeps a
node → live-domain registry so `pause`/`resume` still find the running process.
Retired futures are dropped after the scheduler lock is released, because a
future's destructor can re-enter the scheduler.

*The checkers learn the boundary.* Ending a process mid-flight leaves brackets
open — a `DispatchStart` with no end, an `ask` with no outcome, an identity
assigned and never resigned. That is what dying is, not a violation, so
`Invariant::forget_node` is dispatched by the `Checker` on `NodeRestarted`,
before the successor's events arrive reusing the identities. Six invariants
override it; `OneLeaderPerTerm` deliberately does not, since a restarted voter
reloads its persisted term and vote and stays bound by every election it joined.

`NoSilentLoss` needed more than a reset: it counted asks in one total, and an ask
issued *by* a dead caller (never to be answered, since nothing is left waiting)
had to be told apart from one issued *to* the dead node by a live caller (which
must still resolve with `Unreachable`, invariant #2). `Event::AskIssued` and
`AskOutcome` now carry the issuing `caller` alongside the target, and the count is
per calling node.

All 4 previously-failing seeds pass, and 2,000 fresh seeds of hibernation plus
restarts are clean. Across the original 500, under hibernation, restarts, and the
full nemesis, `CommitMonotonic` never fired — nothing suggested a lost or
duplicated write on the recovery path.

## 2. No grain ever hibernates while faults are flowing

Every clustered workload pins `idle_after: 60s` — `grain_swarm.rs`,
`blob_swarm.rs`, `ws_swarm.rs`, `disk_swarm.rs`, `sql_swarm.rs`,
`machine_swarm.rs` — and runs are far shorter, so passivation never fires under
the nemesis.

The one workload that does hibernate, `SqlSwarm`, sets `idle_after: 50ms` and
sleeps past it deliberately, but is single-node `run_swarm`: no transport, no
nemesis. So G12 (hibernation round-trip) and G14 (quorum recovery on activation)
are each exercised and never together. Snapshot → passivate → rehydrate from a
quorum under partition is unswept, for every facet.

**Status.** Resolved, for the plain grain and every facet. Five workloads now
pair a resident sweep with a hibernating one that also restarts processes:

| workload | what its rehydration has to rebuild |
|---|---|
| `granary-account-hibernating-swarm` | head and tail from a write quorum |
| `granary-blob-hibernating-swarm` | the blob area, root set re-fanned (G17) |
| `granary-ws-hibernating-swarm` | the directory, from captured bytes (F1) |
| `granary-disk-box-hibernating-swarm` | the image, block by block from blobs |
| `granary-sql-account-hibernating-swarm` | the database, pages plus replayed frames |

Each asserts, over the sweep, that activations really passivated and really came
back *from a snapshot* — `support::Exercised`, shared rather than copied five
times. Those hold at any width because the workloads drive them: idling past
`idle_after` on a fixed cadence rather than a seeded coin, and snapshotting often
enough that a grain has a checkpoint before it first passivates. Whether the
nemesis *draws* a restart is a claim about the seed range instead, so it is
stated once, on a `coverage_seeds` sweep that never narrows.

## 3. Record subscriptions: the §14 mandate is not swept — resolved

The granary spec requires fault injection to produce four subscription cases —
leader move mid-stream, slow sink whose buffer overflows, hibernation and
reactivation under a live subscription, and a timed-out append that commits late
— with G16 holding in each. The `record-subscription` workload is single-node,
no faults, one subscriber, writes-then-drain. `subscription_faults.rs` covers
the cases as scenarios, but no `ClusterWorkload` carries G16 as a continuous
invariant.

**Status.** Resolved by `granary-subscription-swarm` (`subscription_swarm.rs`),
which induces all four rather than scripting them: the nemesis moves leaders
(including by killing processes), writers burst while the collector is away on a
journal read so the bounded sink overflows, `idle_after` is short and writers
pause past it so the grain passivates under a live subscription, and every append
carries a deadline shorter than a faulted quorum round so some commit late. The
claim is the spec's own — the sink's reconciled sequence is the prefix `load`
returns — and the reconciler is now shared from `tests/support/log.rs` with
`subscription_faults.rs` rather than copied. Two counters guard against a vacuous
pass: a seed whose shard never becomes readable, and a sink that reconstructed
nothing.

## 4. Four fault-injecting sweeps never assert a fault fired

Coverage sweeps (V&V checklist #8) exist for `grain_swarm.rs`, `blob_swarm.rs`,
`ws_swarm.rs`, `blob-store/swarm.rs`, and `conformance_faults_swarm.rs`. They
are missing from:

- `granary-sql-account-swarm`
- `granary-disk-box-swarm`
- `machine-swarm`
- `linearizable-remote-register`

The last matters most: its recorded bug needs duplication *and* loss at once and
appears at roughly 0.2% of seeds, and nothing proves duplication still fires.
The three slow ones carry a real cost tension, since `coverage_seeds` never
narrows — a cheaper coverage variant beats no assertion.

**Status.** Resolved; all four have one, and the slow ones declare a deliberately
narrow range (8 seeds for SQL, 4 for disk and machine) because with
`coverage_seeds` the declared range *is* the cost. Each seed draws its own
transport fault rates and runs six nemesis rounds, so a handful carries "each
fault type fired at least once" without the coverage check dominating the suite.
`granary-subscription-swarm` and `tenancy-directory-swarm` were given one at
birth.

## 5. Reference models cover two objects

`check_linearizable` decides histories for the remote register, the counter
grain, and shard-split traffic. The account grain's balance is trivially
modelable and strictly stronger than the `CommitMonotonic` check now guarding
it: monotonicity caught the stale-head bug indirectly, where a linearizable
history would have named the lost write.

**Status.** Resolved by `linearizable-account-grain` (`grain_swarm.rs`), which
decides deposit/read histories against the shared `Counter` model under the full
nemesis. Deliberately small and single-key: a linearizability check is
exponential in the number of *pending* operations, and under this nemesis a large
share of calls end unknown, so breadth comes from the seeds rather than from any
one history. It found §0 within 60 seeds — which is the point.

## 6. Subsystems with no sweep at all

- **`tenancy`** — resolved by `tenancy-directory-swarm`. On the idempotency
  question the answer turned out to be the register's: a bare directory is not a
  linearizable ownership index over a duplicating wire, and that is the
  framework's stated contract (§7.2 puts exactly-once out of scope, "built atop
  this layer with explicit idempotency keys") rather than a directory defect. The
  wire duplicates *and* delays, so a duplicate `Forget` can land after a later
  `Record` and undo it — the sweep's first version demanded otherwise and failed
  within a handful of seeds on a claim the layer never made. It now splits names
  into *keep* (only ever recorded, so effect-idempotent and decidable) and *churn*
  (recorded and forgotten, carrying no end-state claim). The keep half found §0.
- **`blob-store` reconcile** — check written, **outcome unresolved**. The
  reconcile loop was already running throughout the swarm; nothing checked it
  *achieved* anything, because the drive read each blob back immediately after its
  put while every replica still held it. The sweep can now heal, quiesce, wait,
  and re-read every acknowledged blob **through a different node than stored it**,
  so a surviving local copy cannot answer for the cluster (B6). Namespaces any
  client ever tried to delete are excluded — a failed `delete_namespace` is
  ambiguous exactly as a failed write is, and the check's first version failed on
  that rather than on re-replication.

  With that corrected it still fails past the local width: a blob acknowledged by
  `put`, in a namespace nobody tried to delete, reads back `Unavailable ("no owner
  yielded blob …")` on a healed, quiesced cluster. Not a settling bound — 45
  virtual seconds changes nothing. Quarantined behind
  `acknowledged_blobs_are_re_replicated_after_a_heal` (`#[ignore]`) because it is
  not yet established which side is wrong, and this particular claim has already
  been too strong twice. Worth resolving next: it is either a B6 defect or a third
  ambiguity the claim has to account for.
- **granary alarms** — partly resolved. `alarm-cluster/leader-crash` now sweeps
  24 seeds rather than 4: which node leads the shard, which survivor wins the
  re-election, and where the deadline falls relative to the driver's sweep are all
  seed-dependent, so four was a sample of leader placements rather than a cover.
  **Still open:** a `ClusterWorkload` for alarms under the continuous nemesis with
  at-most-once firing as a checker. That needs the alarm-index wiring
  (`granary_with_alarms`) threaded through a workload, which is more than a
  widening.
- **granary workflows** — **still open.** "A step's effect runs at most once
  across passivation" is a natural continuous invariant and there is no sweep.
- **`wal`** — resolved by `tests/crash_points.rs`, and *exhaustively* rather than
  by sweeping. A log of a few records is a few hundred bytes, so every crash point
  can be enumerated: recovery is checked at every truncation offset and against
  every single-byte corruption, for four properties (the recovered records are a
  prefix of what was appended; truncating further never recovers more; the torn
  tail is dropped durably so a reopen agrees; the recovered log still accepts an
  append that lands after the prefix). A seeded sweep would be strictly weaker
  here, and there would be no seed worth recording if it failed.
- **`harness-sandbox`, `harness-gateway`, `machine-frontdoor`** — **still open.**
  These are I/O-boundary crates rather than distributed ones, so they are the
  lowest-value of the six; a sweep for each is new work rather than an extension
  of an existing workload.
</content>
</invoke>
