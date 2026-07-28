# Simulation hardening: where the sweeps do not yet reach

An audit of what deterministic simulation exercises against what the specs
mandate. The mechanism and conventions are in
[simulation testing](simulation-testing.md); this file is the list of gaps, and
it should be deleted once its findings are resolved or refuted.

## 0. FIXED — a command applied to the wrong grain after a node restart

A `Record` addressed to one principal's directory was applied to **another's**,
committed there, and answered `Created`. The addressed grain never saw it.

`ActorId` is `(node, path, incarnation)`. `assign_id` handed out paths from a
counter starting at zero and always stamped incarnation `0`, which is unique
within a process and *not* across a restart: the successor re-issues
`node-2//user/2#0`, the very id its predecessor used, and spawn order does not
survive a restart, so that id now names a different actor. Granary caches a
resolved host ref per grain (`HostCache`), guarded before each send by a
leadership check but not by liveness, so a ref cached before the restart still
looked good afterwards and delivered to whatever now sat at that path.

Fixed by stamping the **process** incarnation onto every id a host assigns
(`LocalHost::with_incarnation`, threaded through `ClusterConfig::incarnation`);
the simulation passes the scheduler domain, which is already one per incarnation
and monotonic. A stale ref now fails to resolve, which is a `DeadLetter` — the one
outcome that proves the command never ran (§2.2) — so `GrainRef` re-resolves and
re-issues. Seed `9300730` stays in `corpus.txt`.

Four *test* bugs sat in front of it, each failing in the same shape as a
durability violation:

1. **Expectations held on the workload.** See "A workload outlives its runs" in
   [simulation testing](simulation-testing.md).
2. **`Unchanged` counted as an acknowledgement.** It commits nothing; it reports
   what the serving activation believed, which §7.5 lets a quorum-less recovery
   seed with an uncommitted record.
3. **Reading with a query.** §7.5 is explicit that a read is "read-your-leader
   (relaxed), not linearizable under partition", and names the interim
   construction: issue a trivial *writing* command.
4. **An unbounded end-of-run check.** Verification retried per name, so a
   degraded cluster overran the driver's time budget — failing the seed for
   liveness rather than for anything observed. It is now bounded in total, and a
   seed that runs out of budget makes no claim rather than a false one.

A fifth, in the account sweep: a mutating command with **no idempotency key**,
so a duplicated frame deposited twice (§7.2).

## 1. The nemesis vocabulary is narrower than the fault library

`nemesis()` (`actor-simulation/src/cluster_swarm.rs`) drew from four actions —
two-way partition, crash, heal, quiet — plus a registry outage in registry-based
mode, while `SimNetwork` implemented three faults it never drew:

- **`partition_one_way`**. The asymmetric partition, "the source of zombie
  leaders — the case symmetric partition cannot make".
- **`pause`/`resume`**. A process freeze: the node keeps its state, its inbound
  frames queue, and its overdue timers all fire at once on thaw. A paused leader
  does not climb its term, so it wakes already deposed. A pause must be bounded
  within its round, or the run stalls rather than being faulted.
- **`restart`**. Real process death — volatile state lost, durable state
  reloaded through the storage seam. `crash` is only *isolation*; the process
  keeps running. Without it, journal recovery after process death is never
  crossed with concurrent traffic, which is the §8 quorum-head-recovery path the
  G14 stale-head overwrite came from. A restart invalidates the `SimNode` handles
  a workload cloned out of `ClusterCtx::nodes()`, so it cannot be unconditional.

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

`NoSilentLoss` needed more than a reset: an ask issued *by* a dead caller (never
to be answered, since nothing is left waiting) has to be told apart from one
issued *to* the dead node by a live caller, which must still resolve with
`Unreachable` (invariant #2). `Event::AskIssued` and `AskOutcome` carry the
issuing `caller` alongside the target, and the count is per calling node.

## 2. No grain ever hibernates while faults are flowing

Every clustered workload pinned `idle_after: 60s` and ran far shorter, so
passivation never fired under the nemesis; the one workload that did hibernate
was single-node `run_swarm`, with no transport and no nemesis. So G12
(hibernation round-trip) and G14 (quorum recovery on activation) were each
exercised and never together: snapshot → passivate → rehydrate from a quorum
under partition was unswept, for every facet.

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
back *from a snapshot* (`support::Exercised`). Whether the nemesis *draws* a
restart is a claim about the seed range instead, so it is stated once, on a
`coverage_seeds` sweep that never narrows.

## 3. Record subscriptions: the §14 mandate is not swept — resolved

The granary spec requires fault injection to produce four subscription cases —
leader move mid-stream, slow sink whose buffer overflows, hibernation and
reactivation under a live subscription, and a timed-out append that commits late
— with G16 holding in each. `subscription_faults.rs` covered the cases as
scenarios, but no `ClusterWorkload` carried G16 as a continuous invariant.

**Status.** Resolved by `granary-subscription-swarm` (`subscription_swarm.rs`),
which induces all four rather than scripting them: the nemesis moves leaders
(including by killing processes), writers burst while the collector is away on a
journal read so the bounded sink overflows, `idle_after` is short and writers
pause past it so the grain passivates under a live subscription, and every append
carries a deadline shorter than a faulted quorum round so some commit late. The
claim is the spec's own — the sink's reconciled sequence is the prefix `load`
returns — and the reconciler is shared from `tests/support/log.rs`. Two counters
guard against a vacuous pass: a seed whose shard never becomes readable, and a
sink that reconstructed nothing.

## 4. Four fault-injecting sweeps never assert a fault fired

Coverage sweeps (V&V checklist #8) exist for `grain_swarm.rs`, `blob_swarm.rs`,
`ws_swarm.rs`, `blob-store/swarm.rs`, and `conformance_faults_swarm.rs`. They
are missing from:

- `granary-sql-account-swarm`
- `granary-disk-box-swarm`
- `machine-swarm`
- `linearizable-remote-register`

The last matters most: its recorded bug needs duplication *and* loss at once and
appears at roughly 0.2% of seeds.

**Status.** Resolved; all four have one, and the slow ones declare a deliberately
narrow range (8 seeds for SQL, 4 for disk and machine) because with
`coverage_seeds` the declared range *is* the cost. Each seed draws its own
transport fault rates and runs six nemesis rounds, so a handful carries "each
fault type fired at least once" without the coverage check dominating the suite.

## 5. Reference models cover two objects

`check_linearizable` decided histories for the remote register, the counter
grain, and shard-split traffic. The account grain's balance is modelable and
strictly stronger than the `CommitMonotonic` check guarding it: a linearizable
history names the lost write, where monotonicity only catches it indirectly.

**Status.** Resolved by `linearizable-account-grain` (`grain_swarm.rs`), which
decides deposit/read histories against the shared `Counter` model under the full
nemesis. Deliberately small and single-key: a linearizability check is
exponential in the number of *pending* operations, and under this nemesis a large
share of calls end unknown, so breadth comes from the seeds rather than from any
one history.

## 6. Subsystems with no sweep at all

- **`tenancy`** — resolved by `tenancy-directory-swarm`. A bare directory is not
  a linearizable ownership index over a duplicating wire; §7.2 puts exactly-once
  out of scope, "built atop this layer with explicit idempotency keys". The wire
  duplicates *and* delays, so a duplicate `Forget` can land after a later `Record`
  and undo it. The sweep therefore splits names into *keep* (only ever recorded,
  so effect-idempotent and decidable) and *churn* (recorded and forgotten,
  carrying no end-state claim). Only `Created` and `Updated` count as
  acknowledged: they journal a `Put`, so the output gate holds the reply until it
  commits, while `Unchanged` commits nothing (§0).
- **`blob-store` reconcile** — resolved. The reconcile loop was already running
  throughout the swarm; nothing checked it *achieved* anything, because the drive
  read each blob back immediately after its put while every replica still held
  it. The sweep now heals, quiesces, waits for reconcile, and re-reads every
  acknowledged blob **through a different node than stored it**, so a surviving
  local copy cannot answer for the cluster (B6). Namespaces any client ever
  *tried* to delete are excluded, because a failed `delete_namespace` is ambiguous
  exactly as a failed write is — the tombstone may sit on a minority and be
  adopted later.
- **granary alarms** — partly resolved. `alarm-cluster/leader-crash` sweeps 24
  seeds rather than 4: which node leads the shard, which survivor wins the
  re-election, and where the deadline falls relative to the driver's sweep are all
  seed-dependent. **Still open:** a `ClusterWorkload` for alarms under the
  continuous nemesis with at-most-once firing as a checker. That needs the
  alarm-index wiring (`granary_with_alarms`) threaded through a workload.
- **granary workflows** — **still open.** "A step's effect runs at most once
  across passivation" is a natural continuous invariant and there is no sweep.
- **`wal`** — resolved by `tests/crash_points.rs`, and *exhaustively* rather than
  by sweeping. A log of a few records is a few hundred bytes, so every crash point
  can be enumerated: recovery is checked at every truncation offset and against
  every single-byte corruption, for four properties (the recovered records are a
  prefix of what was appended; truncating further never recovers more; the torn
  tail is dropped durably so a reopen agrees; the recovered log still accepts an
  append that lands after the prefix). A seeded sweep would be strictly weaker
  here.
- **`harness-sandbox`, `harness-gateway`, `machine-frontdoor`** — **still open.**
  These are I/O-boundary crates rather than distributed ones.
