//! Grains under the cluster fault swarm (granary §14, V&V checklist #4, #7, #8).
//!
//! `tests/clustered_grains.rs` drives the `Quorum`-tier paths through *scripted* faults
//! (a crash here, a quorum loss there). This file applies the V&V doctrine the
//! other way round: a [`ClusterWorkload`] is swept across many seeds while a
//! seeded nemesis injects partitions, crashes, heals, loss, duplication, and
//! delay (spec §18.3) and a [`Checker`] watches the §13 event stream live. Three
//! properties are asserted the way the actor framework asserts its own:
//!
//! - **Continuous invariants under faults (#4).** The safety core
//!   ([`default_invariants`]) plus the grain-specific `CommitMonotonic` (G3/G5)
//!   hold on every run; a violation is reported with the `(seed)` to replay it.
//! - **Seed-reproducibility (#7).** The same seed yields a byte-identical event
//!   stream, grain `App` events included ([`check_cluster_reproducible`]).
//! - **Fault coverage (#8).** Across the sweep, each transport fault type
//!   actually fired ([`run_cluster_swarm_coverage`]), so a green run is provably
//!   not a silent happy-path run.
//!
//! The grain is the Appendix A `Account`, hosted on the leader-based clustered
//! system the shard map requires (§7.6).

mod support;

use std::sync::Arc;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::Counter;
use actor_simulation::CounterOp;
use actor_simulation::CounterRet;
use actor_simulation::History;
use actor_simulation::Invariant;
use actor_simulation::Rehost;
use actor_simulation::SimEntropy;
use actor_simulation::SimNode;
use actor_simulation::SimSystem;
use actor_simulation::Workload;
use actor_simulation::check_linearizable;
use actor_simulation::coverage_seeds;
use actor_simulation::default_invariants;
use actor_simulation::replay_cluster_swarm;
use actor_simulation::replay_swarm;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_cluster_swarm_coverage;
use actor_simulation::run_swarm;
use actor_simulation::sweep_seeds;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GrainRef;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::Seq;
use granary::testing::ActivationSingletonPerNode;
use granary::testing::CommitMonotonic;
use serde::Deserialize;
use serde::Serialize;

use support::Add;
use support::CounterEvent;
use support::CounterGrain;
use support::Exercised;
use support::ReadCount;

// --- The Appendix A account grain (system-generic over the cluster) -----------

#[derive(Default)]
struct Account;

#[derive(Default, Serialize, Deserialize)]
struct Balance {
    cents: i64,
}

#[derive(Serialize, Deserialize)]
enum Ledger {
    Deposited(u64),
}

impl Grain for Account {
    type System = SimNode;
    type State = Balance;
    type Event = Ledger;
    type Facets = ();
    const GRAIN_TYPE: &'static str = "bank.Account";

    fn apply(state: &mut Balance, event: &Ledger) {
        match event {
            Ledger::Deposited(n) => state.cents += *n as i64,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Deposit>();
        r.accept::<ReadBalance>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Deposit {
    cents: u64,
}
impl Message for Deposit {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.Deposit");
}

impl GrainHandler<Deposit> for Account {
    async fn handle(
        &self,
        state: &Balance,
        msg: Deposit,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<Ledger>, i64) {
        (
            vec![Ledger::Deposited(msg.cents)],
            state.cents + msg.cents as i64,
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadBalance;
impl Message for ReadBalance {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.ReadBalance");
}

impl GrainHandler<ReadBalance> for Account {
    async fn handle(
        &self,
        state: &Balance,
        _msg: ReadBalance,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<Ledger>, i64) {
        (vec![], state.cents)
    }
}

// --- A grain-specific continuous safety checker -------------------------------

// --- The workload -------------------------------------------------------------

/// `hibernating` picks the activation lifetime: `false` keeps every activation
/// resident for the whole run, `true` gives it a lifetime short enough that
/// clients can idle past it, so grains passivate and rehydrate from a quorum
/// while the nemesis is still running. The snapshot cadence follows, because a
/// hibernating grain that never snapshotted comes back by replaying from an
/// empty base and never exercises the snapshot-plus-tail restore.
///
/// The hibernating cadence is `1` — a snapshot at every commit — because idle
/// eviction takes one only when §9's threshold has been crossed, exactly as the
/// write path does (`host.rs::passivate`). A faulted client here often lands
/// fewer than two commits on a grain before idling past `idle_after`, so any
/// higher cadence leaves the restore path unexercised, which
/// `Exercised::assert_hibernated` then reports.
fn config(hibernating: bool) -> GranaryConfig {
    GranaryConfig {
        shards: 2,
        replication_factor: 3,
        idle_after: if hibernating { IDLE_AFTER } else { RESIDENT },
        snapshot_every: if hibernating { 1 } else { 8 },
        ..GranaryConfig::default()
    }
}

/// Activation lifetime for the resident sweeps: longer than any run, so nothing
/// passivates.
const RESIDENT: Duration = Duration::from_secs(60);
/// Activation lifetime for the hibernating sweep.
const IDLE_AFTER: Duration = Duration::from_millis(200);
/// How long a hibernating client idles when it idles — comfortably past
/// [`IDLE_AFTER`], so the host really does passivate rather than nearly.
const IDLE_FOR: Duration = Duration::from_millis(500);

/// Deposit-and-read traffic against a handful of grains, hosted on a leader-based
/// cluster, driven through the public `GrainRef` API only (spec §18.4). Every
/// call is faulted by the nemesis and the transport; a failed call is recorded as
/// nothing and the client moves on, so the drive future always completes and the
/// invariants are checked over whatever the run produced.
struct AccountSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
    /// Idle past `idle_after` between operations, so activations passivate and
    /// rehydrate mid-run.
    hibernating: bool,
    /// Let the nemesis kill and re-launch node processes, not just isolate them.
    restarting: bool,
    /// What the sweep actually exercised, accumulated across its seeds.
    exercised: Exercised,
}

impl ClusterWorkload for AccountSwarm {
    // Both corpus keys are literals here rather than a field, because that is
    // where `tests/corpus_keys.rs` looks for them: a key it cannot see is one
    // `corpus.txt` cannot guard (docs/simulation-testing.md).
    fn name(&self) -> &'static str {
        if self.hibernating {
            "granary-account-hibernating-swarm"
        } else {
            "granary-account-swarm"
        }
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn rehost(&self) -> Option<Rehost> {
        if !self.restarting {
            return None;
        }
        // A restarted node comes up empty: it no longer hosts `Account`, so
        // without this it would stop leading shards and stop counting toward a
        // quorum — the run would shrink the cluster rather than fault it.
        let hibernating = self.hibernating;
        Some(Arc::new(move |node: &SimNode| {
            node.granary::<Account>(config(hibernating));
        }))
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(300),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        // Granary requires the leader-based control plane to host the shard map
        // (§7.6); every node is a control voter so the map group can form.
        ClusterModeSpec::Leader {
            swim: self.swim(),
            voters: self.nodes,
            election_timeout: Duration::from_millis(500),
            heartbeat_interval: Duration::from_millis(100),
            downing: DowningPolicy::Conservative,
        }
    }

    fn setup(&self, _ctx: &ClusterCtx) {}

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        let hibernating = self.hibernating;
        let restarting = self.restarting;
        Box::pin(async move {
            // Host the type on every node: each starts its gateway and joins/leads
            // its shards (§5.3). Done at drive start; the bounded redirect absorbs
            // the bootstrap window.
            let granaries: Vec<_> = nodes
                .iter()
                .map(|s| s.granary::<Account>(config(hibernating)))
                .collect();
            let clock = nodes[0].clock().clone();
            let entropy = nodes[0].entropy().clone();
            // Let the control-plane and shard groups elect before traffic.
            clock.sleep(Duration::from_secs(3)).await;

            let mut tasks = Vec::new();
            for c in 0..clients {
                // Spread the clients across the nodes' gateways — except under
                // restarts, where every client goes through node 1, the one node
                // the nemesis leaves alone. A handle for a restarted node points
                // at a system that has been shut down, and location transparency
                // (G13) means routing through one gateway costs no coverage: the
                // call still lands on whichever node leads the grain's shard,
                // which is exactly the set being restarted.
                let granary = if restarting {
                    granaries[0].clone()
                } else {
                    granaries[c % granaries.len()].clone()
                };
                let clock = clock.clone();
                let entropy = entropy.clone();
                tasks.push(async move {
                    for op in 0..ops {
                        // A small key space so several grains share each shard.
                        let key = format!("account/{}", entropy.next_u64() % 4);
                        let acct = granary.grain(key);
                        if entropy.next_u64().is_multiple_of(2) {
                            // A short deadline so a faulted call fails fast and the
                            // client keeps issuing traffic rather than blocking.
                            let _ = acct
                                .ask_timeout(Deposit { cents: 1 }, Duration::from_secs(2))
                                .await;
                        } else {
                            let _ = acct.ask_timeout(ReadBalance, Duration::from_secs(2)).await;
                        }
                        // Idle past the activation lifetime often enough that the
                        // next call to this grain rehydrates it — from a snapshot
                        // plus a replayed tail, recovered from a write quorum, on
                        // whichever node leads the shard by then (§8, §9).
                        if hibernating && op % 2 == 1 {
                            clock.sleep(IDLE_FOR).await;
                        }
                    }
                });
            }
            futures::future::join_all(tasks).await;
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        let mut invariants = default_invariants();
        invariants.push(Box::new(CommitMonotonic::new(
            "grain-commit-monotonic",
            "grain",
        )));
        invariants.push(Box::new(ActivationSingletonPerNode::new(
            "grain-activation-singleton-per-node",
            "grain",
        )));
        invariants.push(Box::new(self.exercised.clone()));
        invariants
    }
}

/// The resident sweeps' workload: activations live for the whole run and the
/// nemesis only isolates nodes.
fn resident(clients: usize, ops: u64) -> AccountSwarm {
    AccountSwarm {
        nodes: 3,
        clients,
        ops,
        hibernating: false,
        restarting: false,
        exercised: Exercised::default(),
    }
}

/// The hibernating sweeps' workload: short activation lifetime, clients that
/// idle past it, and a nemesis allowed to kill node processes outright.
///
/// Restarts matter most to *this* workload. A crash only isolates a process that
/// keeps running, so the grains it hosts stay activated behind the partition and
/// a heal finds them warm; killing the process is what forces the next call to
/// rebuild the grain's head and tail from a write quorum on a node that holds
/// none of it in memory.
fn hibernating(clients: usize, ops: u64) -> AccountSwarm {
    AccountSwarm {
        nodes: 3,
        clients,
        ops,
        hibernating: true,
        restarting: true,
        exercised: Exercised::default(),
    }
}

#[test]
fn grain_invariants_hold_under_the_cluster_swarm() {
    // #4: the safety core plus G3/G5 commit-monotonicity hold on every seeded run
    // under partitions, crashes, loss, duplication, and delay.
    if let Err(failure) = run_cluster_swarm(&resident(3, 6), sweep_seeds(0..24)) {
        panic!("{failure}");
    }
}

#[test]
fn grain_swarm_is_reproducible() {
    // #7: the same seed replays to a byte-identical event stream — grain `App`
    // events included — even under cluster nemesis and transport faults.
    if let Err(divergence) = replay_cluster_swarm(&resident(2, 5), sweep_seeds(0..12)) {
        panic!("{divergence}");
    }
}

// --- The account as a linearizable object under faults (G2/G14) ---------------
//
// `CommitMonotonic` is a claim about the *journal*: seqs advance, nothing lands
// twice on a slot. It caught the stale-head bug in `corpus.txt` only indirectly —
// what actually went wrong there was an acknowledged deposit that no later read
// could see, and monotonicity happened to notice the slot collision underneath.
// A reference model says the thing directly (V&V checklist #3): every read must
// be explicable by *some* sequential order of the deposits, so a lost or
// double-applied write is named as what it is.
//
// Deliberately small, and one grain rather than four. A linearizability check is
// exponential in the number of *pending* operations, and under this nemesis a
// large share of calls end unknown — each one an op the checker may place
// anywhere or drop. Few ops keep that search bounded; the breadth comes from the
// seeds, not from any one history.

/// The account this sweep decides, whose deposit carries an **idempotency key**.
///
/// The Appendix A `Account` above cannot be held to a linearizable history over
/// this wire, and that is the framework's contract rather than a defect in it.
/// Delivery is at-most-once *at the caller* (§7.2): a duplicated request frame is
/// handled twice, so a bare `Deposit { cents: 1 }` lands twice for one logical
/// operation and no sequential order explains the balance that follows. The spec
/// names the remedy in the same section — higher guarantees are "built atop this
/// layer with explicit idempotency keys" — and `tests/support/mod.rs` in
/// `actor-simulation` is the same construction for the register, recorded in
/// `corpus.txt` after the identical mistake.
///
/// The key lives in the *event*, so the applied set is folded from the journal
/// and survives passivation and failover: a duplicate that arrives after a
/// rehydration is still recognized. A read needs no key — it changes nothing, and
/// a duplicate's reply is the value at its own instant, which falls inside the
/// caller's window and linearizes there.
///
/// Its **read** is a writing command, for the other half of the same reason. A
/// query commits nothing and is served from the activation, and §7.5 is explicit
/// that this is "read-your-leader (relaxed), not linearizable under partition":
/// a deposed-but-unfenced leader MAY serve a stale value, and a quorum-less
/// recovery may even seed that state with an uncommitted record. Linearizable
/// reads are a deferred upgrade (§16). The spec names the interim construction in
/// the same paragraph — "a caller that needs a linearizable read in the meantime
/// issues a trivial *writing* command (one that emits an event): it rides the §6
/// output gate, so it commits through the shard leader and reflects committed
/// state, or fails". `Probed` is that event: the fold ignores it, so the reply is
/// the committed balance at the seq the read itself commits at.
#[derive(Default)]
struct KeyedAccount;

#[derive(Default, Serialize, Deserialize)]
struct KeyedBalance {
    cents: i64,
    applied: std::collections::BTreeSet<(u64, u64)>,
}

#[derive(Serialize, Deserialize)]
enum KeyedLedger {
    Deposited {
        req: (u64, u64),
        cents: u64,
    },
    /// What makes a read linearizable: an event, and nothing else.
    Probed,
}

impl Grain for KeyedAccount {
    type System = SimNode;
    type State = KeyedBalance;
    type Event = KeyedLedger;
    type Facets = ();
    const GRAIN_TYPE: &'static str = "bank.KeyedAccount";

    fn apply(state: &mut KeyedBalance, event: &KeyedLedger) {
        match event {
            KeyedLedger::Deposited { req, cents } => {
                if state.applied.insert(*req) {
                    state.cents += *cents as i64;
                }
            }
            KeyedLedger::Probed => {}
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<KeyedDeposit>();
        r.accept::<ReadKeyedBalance>();
    }
}

/// A deposit named by its caller's `(client, seq)` — identical across every copy
/// of the request the wire may deliver.
#[derive(Clone, Serialize, Deserialize)]
struct KeyedDeposit {
    req: (u64, u64),
    cents: u64,
}
impl Message for KeyedDeposit {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.KeyedDeposit");
}

impl GrainHandler<KeyedDeposit> for KeyedAccount {
    async fn handle(
        &self,
        state: &KeyedBalance,
        msg: KeyedDeposit,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<KeyedLedger>, i64) {
        // A re-delivery journals nothing and answers from the state it already
        // produced; the history records `AddOk` either way, so the balance the
        // duplicate reports is not what the model is deciding.
        if state.applied.contains(&msg.req) {
            return (vec![], state.cents);
        }
        (
            vec![KeyedLedger::Deposited {
                req: msg.req,
                cents: msg.cents,
            }],
            state.cents + msg.cents as i64,
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadKeyedBalance;
impl Message for ReadKeyedBalance {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.ReadKeyedBalance");
}

impl GrainHandler<ReadKeyedBalance> for KeyedAccount {
    async fn handle(
        &self,
        state: &KeyedBalance,
        _msg: ReadKeyedBalance,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<KeyedLedger>, i64) {
        // Emitting an event is the whole point (see the type's doc): it puts this
        // read behind the output gate, so the balance returned is the committed one
        // and a deposed leader fails the call instead of answering staler.
        (vec![KeyedLedger::Probed], state.cents)
    }
}

/// Deposits and reads against a single account, recorded as a [`Counter`]
/// history. A deposit is `Add`, a read is `Read`, and a call that fails is left
/// pending (`info`) — its effect may or may not have landed, which is exactly
/// what the checker is entitled to decide either way.
struct LinearizableAccountSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
}

impl ClusterWorkload for LinearizableAccountSwarm {
    fn name(&self) -> &'static str {
        "linearizable-account-grain"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(300),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::Leader {
            swim: self.swim(),
            voters: self.nodes,
            election_timeout: Duration::from_millis(500),
            heartbeat_interval: Duration::from_millis(100),
            downing: DowningPolicy::Conservative,
        }
    }

    fn setup(&self, _ctx: &ClusterCtx) {}

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        Box::pin(async move {
            let granaries: Vec<_> = nodes
                .iter()
                .map(|s| s.granary::<KeyedAccount>(config(false)))
                .collect();
            let clock = nodes[0].clock().clone();
            let entropy = nodes[0].entropy().clone();
            clock.sleep(Duration::from_secs(3)).await;

            let history: History<Counter> = History::new();
            let mut tasks = Vec::new();
            for c in 0..clients {
                // Each client through a different node's gateway, so the history
                // mixes local and forwarded calls against one object (G13).
                let granary = granaries[c % granaries.len()].clone();
                let history = history.clone();
                let entropy = entropy.clone();
                tasks.push(async move {
                    let acct = granary.grain("account/linearizable");
                    for seq in 0..ops {
                        if entropy.next_u64().is_multiple_of(2) {
                            let delta = 1 + (entropy.next_u64() % 3) as i64;
                            let id = history.invoke(CounterOp::Add(delta));
                            // One key per attempt, distinct across clients: what a
                            // duplicated frame is recognized by.
                            match acct
                                .ask_timeout(
                                    KeyedDeposit {
                                        req: (c as u64, seq),
                                        cents: delta as u64,
                                    },
                                    Duration::from_secs(2),
                                )
                                .await
                            {
                                Ok(_) => history.ok(id, CounterRet::AddOk),
                                // Ambiguous or refused alike: the record may sit on
                                // a minority and be adopted by a later recovery
                                // (§7.2, §11), so the checker decides.
                                _ => history.info(id),
                            }
                        } else {
                            let id = history.invoke(CounterOp::Read);
                            match acct
                                .ask_timeout(ReadKeyedBalance, Duration::from_secs(2))
                                .await
                            {
                                Ok(balance) => history.ok(id, CounterRet::Read(balance)),
                                _ => history.info(id),
                            }
                        }
                    }
                });
            }
            futures::future::join_all(tasks).await;

            let verdict = check_linearizable(&history);
            assert!(
                verdict.is_ok(),
                "account history was not linearizable — a deposit was lost or \
                 applied twice: {verdict}",
            );
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        let mut invariants = default_invariants();
        invariants.push(Box::new(CommitMonotonic::new(
            "linearizable-account-commit-monotonic",
            "grain",
        )));
        invariants
    }
}

#[test]
fn the_account_grain_is_linearizable_under_cluster_faults() {
    let workload = LinearizableAccountSwarm {
        nodes: 3,
        clients: 2,
        ops: 4,
    };
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..24)) {
        panic!("{failure}");
    }
}

#[test]
fn the_linearizable_account_sweep_is_reproducible() {
    let workload = LinearizableAccountSwarm {
        nodes: 3,
        clients: 2,
        ops: 3,
    };
    if let Err(divergence) = replay_cluster_swarm(&workload, sweep_seeds(0..12)) {
        panic!("{divergence}");
    }
}

// --- Hibernation crossed with the nemesis (G12 × G14) -------------------------
//
// The sweep above keeps every activation resident for the whole run, so it never
// reaches the path where a grain is evicted and the *next* call has to rebuild it
// — head and tail recovered from a write quorum, on whatever node leads the shard
// by then (§8, §9). That path is where the acknowledged-write-lost bug in
// `corpus.txt` lived, and until now G12 (hibernation round-trip) and G14
// (lossless failover) were each exercised alone: `sql_swarm.rs`'s single-node
// workload hibernates with no faults at all, and every clustered workload faults
// without ever hibernating.
//
// This one crosses them: grains passivate and snapshot mid-run, and the next
// call rehydrates through the quorum barrier while the nemesis partitions,
// freezes, and kills the processes around it. `CommitMonotonic` is the checker
// that would name a write the recovery dropped or replayed onto an occupied
// slot.

#[test]
fn hibernating_grain_invariants_hold_under_restarts() {
    let workload = hibernating(3, 6);
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..24)) {
        panic!("{failure}");
    }
    // #8, for the faults the transport's `FaultStats` cannot see.
    workload.exercised.assert_hibernated();
}

#[test]
fn the_nemesis_actually_restarts_a_node() {
    // #8 for the fault the wire cannot see. Stated once, here, for every
    // hibernating sweep in the crate: whether the nemesis *draws* a restart is a
    // property of its vocabulary and the seed range, not of any one facet's
    // traffic, and this is the cheapest workload to ask it of.
    //
    // `coverage_seeds`, not `sweep_seeds`, because the claim is about the whole
    // declared range. One seed runs six nemesis rounds against a seven-action
    // vocabulary, so it draws no restart about two runs in five — narrowing this
    // would not weaken the assertion, it would make it flaky.
    let workload = hibernating(3, 6);
    if let Err(failure) = run_cluster_swarm(&workload, coverage_seeds(0..16)) {
        panic!("{failure}");
    }
    assert!(
        workload.exercised.restarted() > 0,
        "the nemesis never restarted a node across the declared range: process \
         death went unexercised, so every hibernating sweep's restart is a claim \
         about nothing",
    );
}

#[test]
fn hibernating_grain_swarm_is_reproducible() {
    // Passivation, snapshot, rehydration, and process restart all ride the
    // Clock/Entropy/Spawner seams, so the stream stays byte-identical per seed.
    if let Err(divergence) = replay_cluster_swarm(&hibernating(2, 5), sweep_seeds(0..12)) {
        panic!("{divergence}");
    }
}

#[test]
fn grain_swarm_actually_fires_each_fault_type() {
    // #8: a green sweep must not be a silent happy-path sweep. Across the seed
    // range the transport injected loss, duplication, reordering (delay), and
    // partition/crash blocking at least once each.
    let workload = resident(3, 6);
    let stats = match run_cluster_swarm_coverage(&workload, coverage_seeds(0..32)) {
        Ok(stats) => stats,
        Err(failure) => panic!("{failure}"),
    };
    assert!(
        stats.dropped > 0,
        "the sweep never dropped a frame (loss uncovered): {stats:?}"
    );
    assert!(
        stats.duplicated > 0,
        "the sweep never duplicated a frame: {stats:?}"
    );
    assert!(
        stats.delayed > 0,
        "the sweep never delayed a frame (reordering uncovered): {stats:?}"
    );
    assert!(
        stats.blocked > 0,
        "the sweep never blocked a frame (partition/crash uncovered): {stats:?}"
    );
}

// =============================================================================
// Single-node sweeps (granary §14)
// =============================================================================
//
// The cluster sweeps above put the grain under a nemesis; these put it under
// concurrency alone, on the single-node `Local` tier: a linearizability sweep
// over the shared `CounterGrain` and a record-subscription sweep, each paired
// with its reproducibility sweep. They live here rather than beside the
// scenarios in `grains.rs` because they are sweeps — they fail by naming a seed
// (docs/simulation-testing.md).

// --- Grain invariant checkers over the §13 event stream -----------------------

/// The grain safety core for these single-node suites: G3/G5 commit
/// monotonicity and G6 per-node exactly-once activation, both taken from
/// [`granary::testing`] rather than restated here. `ActivationSingletonPerNode`
/// additionally clears a node's live set on `NodeDown`, which is inert in the
/// no-fault runs below and correct in the faulted ones.
fn grain_invariants() -> Vec<Box<dyn Invariant>> {
    let mut invariants = default_invariants();
    invariants.push(Box::new(ActivationSingletonPerNode::new(
        "grain-exactly-once-activation",
        "grain",
    )));
    invariants.push(Box::new(CommitMonotonic::new(
        "grain-commit-monotonic",
        "grain",
    )));
    invariants
}

// --- Linearizability workload (G2) --------------------------------------------

async fn counter_client(
    counter: GrainRef<CounterGrain>,
    history: History<Counter>,
    entropy: SimEntropy,
    ops: u64,
) {
    for _ in 0..ops {
        if entropy.next_u64().is_multiple_of(2) {
            let delta = 1 + (entropy.next_u64() % 3) as i64;
            let id = history.invoke(CounterOp::Add(delta));
            match counter.ask(Add(delta)).await {
                Ok(_value) => history.ok(id, CounterRet::AddOk),
                Err(_) => history.info(id),
            }
        } else {
            let id = history.invoke(CounterOp::Read);
            match counter.ask(ReadCount).await {
                Ok(value) => history.ok(id, CounterRet::Read(value)),
                Err(_) => history.info(id),
            }
        }
    }
}

struct CounterWorkload {
    clients: usize,
    ops: u64,
}

impl Workload for CounterWorkload {
    fn name(&self) -> &'static str {
        "linearizable-counter-grain"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let clients = self.clients;
        let ops = self.ops;
        Box::pin(async move {
            let counters = system.granary::<CounterGrain>(GranaryConfig::default());
            // One grain, hammered concurrently — the single linearizable object.
            let counter = counters.grain("counter/0");
            let history: History<Counter> = History::new();
            let mut tasks = Vec::new();
            for _ in 0..clients {
                tasks.push(counter_client(
                    counter.clone(),
                    history.clone(),
                    system.entropy().clone(),
                    ops,
                ));
            }
            futures::future::join_all(tasks).await;

            let verdict = check_linearizable(&history);
            assert!(
                verdict.is_ok(),
                "counter grain history was not linearizable: {verdict}",
            );
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        grain_invariants()
    }
}

#[test]
fn counter_grain_is_linearizable_across_seeds() {
    let workload = CounterWorkload { clients: 4, ops: 8 };
    if let Err(failure) = run_swarm(&workload, sweep_seeds(0..96)) {
        panic!("{failure}");
    }
}

#[test]
fn counter_grain_run_is_reproducible() {
    // The determinism contract (§14): the same seed yields a byte-identical event
    // stream — and grain `App` events are part of it, so this guards G2 too.
    let workload = CounterWorkload { clients: 3, ops: 6 };
    if let Err(divergence) = replay_swarm(&workload, sweep_seeds(0..64)) {
        panic!("{divergence}");
    }
}

// --- Record subscription (§7.9, G16) ------------------------------------------

/// A subscriber registers from the empty head, the grain takes a run of writes,
/// and the pushed stream is reconciled by `Seq`. The reconstructed sequence MUST
/// equal the committed records — contiguous `1..=writes`, in order, no gap or
/// duplicate — which is exactly what `load` to the head would return (**G16**).
struct SubscriptionWorkload {
    writes: u64,
}

impl Workload for SubscriptionWorkload {
    fn name(&self) -> &'static str {
        "record-subscription"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let writes = self.writes;
        Box::pin(async move {
            let counters = system.granary::<CounterGrain>(GranaryConfig::default());
            let counter = counters.grain("counter/sub");

            // Subscribe from the empty head, before any write, so every commit is
            // pushed live.
            let sub = counter.subscribe(Seq::ZERO).await.expect("subscribe");
            assert_eq!(sub.head, Seq::ZERO, "a fresh grain's head is ZERO");

            let mut expected = Vec::new();
            for i in 0..writes {
                let delta = 1 + (i as i64 % 3);
                counter.ask(Add(delta)).await.expect("add commits");
                expected.push(delta);
            }

            // Drain the stream, reconciling by seq (§7.9): each batch must begin
            // exactly after the last seq seen, and seqs strictly increase.
            let mut deltas = Vec::new();
            let mut last = 0u64;
            while (deltas.len() as u64) < writes {
                let batch = sub.records.recv().await.expect("a live batch");
                assert_eq!(
                    batch.from.value(),
                    last,
                    "batch begins after the last seq (no gap)"
                );
                for (seq, event) in batch.records {
                    assert_eq!(seq.value(), last + 1, "seqs are contiguous and ordered");
                    last = seq.value();
                    match event {
                        CounterEvent::Added(d) => deltas.push(d),
                    }
                }
            }

            assert_eq!(
                deltas, expected,
                "pushed records match the committed writes, in order"
            );
            assert_eq!(
                last, writes,
                "the stream reached the committed head (push == load, G16)"
            );
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        grain_invariants()
    }
}

#[test]
fn a_subscription_streams_the_committed_records_in_order() {
    let workload = SubscriptionWorkload { writes: 8 };
    if let Err(failure) = run_swarm(&workload, sweep_seeds(0..32)) {
        panic!("{failure}");
    }
}

#[test]
fn a_subscription_run_is_reproducible() {
    // Delivery rides the Spawner/Transport seams, so a seeded run's event stream
    // stays byte-identical (§7.9, §14).
    let workload = SubscriptionWorkload { writes: 6 };
    if let Err(divergence) = replay_swarm(&workload, sweep_seeds(0..32)) {
        panic!("{divergence}");
    }
}
