//! Entities under the cluster fault swarm (entity §14, V&V checklist #4, #7, #8).
//!
//! `tests/clustered_entities.rs` drives the `Quorum`-tier paths through
//! *scripted* faults. This file works the other way round: a
//! [`ClusterWorkload`] is swept across many seeds while a seeded nemesis
//! injects partitions, crashes, heals, loss, duplication, and delay (spec
//! actor §18.3) and a [`Checker`] watches the §13 event stream live. Three
//! properties are asserted:
//!
//! - **Continuous invariants under faults (#4).** The safety core
//!   ([`default_invariants`]) plus `CommitMonotonic` (E3/E5) hold on every
//!   run; a violation is reported with the `(seed)` to replay it.
//! - **Seed-reproducibility (#7).** The same seed yields a byte-identical
//!   event stream, entity `App` events included
//!   ([`check_cluster_reproducible`]).
//! - **Fault coverage (#8).** Across the sweep, each transport fault type
//!   actually fired ([`run_cluster_swarm_coverage`]), so a green run is not a
//!   silent happy-path run.
//!
//! The entity is the Appendix A `Account`, hosted on the leader-based clustered
//! system the shard map requires (§7.6).

mod support;

use std::sync::Arc;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_core::ActorSystem;
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
use entity::Entity;
use entity::EntityConfig;
use entity::EntityCtx;
use entity::EntityExt;
use entity::EntityHandler;
use entity::EntityRef;
use entity::Seq;
use entity::testing::ActivationSingletonPerNode;
use entity::testing::CommitMonotonic;
use serde::Deserialize;
use serde::Serialize;

use support::Add;
use support::CounterEntity;
use support::CounterEvent;
use support::Exercised;
use support::ReadCount;

// --- The Appendix A account entity (system-generic over the cluster) -----------

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

impl Entity for Account {
    type System = SimNode;
    type State = Balance;
    type Event = Ledger;
    type Facets = ();
    const ENTITY_TYPE: &'static str = "bank.Account";

    fn apply(state: &mut Balance, event: &Ledger) {
        match event {
            Ledger::Deposited(n) => state.cents += *n as i64,
        }
    }

    fn register(r: &mut entity::EntityRegistry<Self>) {
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

impl EntityHandler<Deposit> for Account {
    async fn handle(
        &self,
        state: &Balance,
        msg: Deposit,
        _ctx: &EntityCtx<Self>,
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

impl EntityHandler<ReadBalance> for Account {
    async fn handle(
        &self,
        state: &Balance,
        _msg: ReadBalance,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<Ledger>, i64) {
        (vec![], state.cents)
    }
}

// --- The workload -------------------------------------------------------------

/// `hibernating` picks the activation lifetime: `false` keeps every activation
/// resident for the whole run, `true` shortens it enough that clients can idle
/// past it, so entities hibernate and rehydrate from a quorum while the
/// nemesis runs. The snapshot cadence follows: a hibernating entity that never
/// snapshotted comes back by replaying from an empty base and never exercises
/// the snapshot-plus-tail restore.
///
/// The hibernating cadence is `1` (a snapshot at every commit) because idle
/// eviction takes one only once §9's threshold has been crossed, as the write
/// path does (`host.rs::hibernate`). A faulted client often lands fewer than
/// two commits on an entity before idling past `idle_after`, so any higher
/// cadence leaves the restore path unexercised, which
/// `Exercised::assert_hibernated` then reports.
fn config(hibernating: bool) -> EntityConfig {
    EntityConfig {
        shards: 2,
        replication_factor: 3,
        idle_after: if hibernating { IDLE_AFTER } else { RESIDENT },
        snapshot_every: if hibernating { 1 } else { 8 },
        ..EntityConfig::default()
    }
}

/// Activation lifetime for the resident sweeps: longer than any run, so nothing
/// hibernates.
const RESIDENT: Duration = Duration::from_secs(60);
/// Activation lifetime for the hibernating sweep.
const IDLE_AFTER: Duration = Duration::from_millis(200);
/// How long a hibernating client idles: past [`IDLE_AFTER`], so the host
/// really does hibernate.
const IDLE_FOR: Duration = Duration::from_millis(500);

/// Deposit-and-read traffic against a handful of entities on a leader-based
/// cluster, driven through the public `EntityRef` API only (actor §18.4). Every
/// call is faulted by the nemesis and the transport; a failed call records
/// nothing and the client moves on, so the drive future always completes and
/// the invariants are checked over whatever the run produced.
struct AccountSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
    /// Idle past `idle_after` between operations, so activations hibernate and
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
            "entity-account-hibernating-swarm"
        } else {
            "entity-account-swarm"
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
        // quorum; the run would shrink the cluster rather than fault it.
        let hibernating = self.hibernating;
        Some(Arc::new(move |node: &SimNode| {
            node.entities::<Account>(config(hibernating));
        }))
    }

    fn mode(&self) -> ClusterModeSpec {
        // The entity layer requires the leader-based control plane to host the
        // shard map (§7.6); every node is a control voter so the map group can
        // form.
        ClusterModeSpec::leader(self.swim(), self.nodes, DowningPolicy::Conservative)
    }

    fn setup(&self, _ctx: &ClusterCtx) {}

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        let hibernating = self.hibernating;
        let restarting = self.restarting;
        Box::pin(async move {
            // Host the type on every node: each starts its gateway and
            // joins/leads its shards (§5.3). Done at drive start; the bounded
            // redirect absorbs the bootstrap window.
            let hosts: Vec<_> = nodes
                .iter()
                .map(|s| s.entities::<Account>(config(hibernating)))
                .collect();
            let clock = nodes[0].clock().clone();
            let entropy = nodes[0].entropy().clone();
            // Let the control-plane and shard groups elect before traffic.
            clock.sleep(Duration::from_secs(3)).await;

            let mut tasks = Vec::new();
            for c in 0..clients {
                // Spread the clients across the nodes' gateways, except under
                // restarts, where every client goes through node 1, the one
                // node the nemesis leaves alone. A handle for a restarted node
                // points at a system that has been shut down, and location
                // transparency (E13) means one gateway costs no coverage: the
                // call still lands on whichever node leads the entity's shard,
                // which is the set being restarted.
                let entities = if restarting {
                    hosts[0].clone()
                } else {
                    hosts[c % hosts.len()].clone()
                };
                let clock = clock.clone();
                let entropy = entropy.clone();
                tasks.push(async move {
                    for op in 0..ops {
                        // A small key space so several entities share each shard.
                        let key = format!("account/{}", entropy.next_u64() % 4);
                        let acct = entities.entity(key);
                        if entropy.next_u64().is_multiple_of(2) {
                            // A short deadline so a faulted call fails fast and the
                            // client keeps issuing traffic rather than blocking.
                            let _ = acct
                                .ask_timeout(Deposit { cents: 1 }, Duration::from_secs(2))
                                .await;
                        } else {
                            let _ = acct.ask_timeout(ReadBalance, Duration::from_secs(2)).await;
                        }
                        // Idle past the activation lifetime often enough that
                        // the next call rehydrates the entity from a snapshot
                        // plus a replayed tail, recovered from a write quorum
                        // on whichever node leads the shard by then (§8, §9).
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
            "entity-commit-monotonic",
            "entity",
        )));
        invariants.push(Box::new(ActivationSingletonPerNode::new(
            "entity-activation-singleton-per-node",
            "entity",
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
/// Restarts matter most here. A crash only isolates a process that keeps
/// running, so the entities it hosts stay activated behind the partition and a
/// heal finds them warm; killing the process forces the next call to rebuild
/// the entity's head and tail from a write quorum on a node that holds none of
/// it in memory.
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
fn entity_invariants_hold_under_the_cluster_swarm() {
    // #4: the safety core plus E3/E5 commit-monotonicity hold on every seeded run
    // under partitions, crashes, loss, duplication, and delay.
    if let Err(failure) = run_cluster_swarm(&resident(3, 6), sweep_seeds(0..24)) {
        panic!("{failure}");
    }
}

#[test]
fn entity_swarm_is_reproducible() {
    // #7: the same seed replays to a byte-identical event stream (entity `App`
    // events included) even under cluster nemesis and transport faults.
    if let Err(divergence) = replay_cluster_swarm(&resident(2, 5), sweep_seeds(0..12)) {
        panic!("{divergence}");
    }
}

// --- The account as a linearizable object under faults (E2/E14) ---------------
//
// `CommitMonotonic` is a claim about the *journal*: seqs advance, nothing
// lands twice on a slot. It caught the stale-head bug in `corpus.txt` only
// indirectly: the real fault was an acknowledged deposit no later read could
// see, and monotonicity happened to notice the slot collision underneath. A
// reference model says it directly (V&V checklist #3): every read must be
// explicable by *some* sequential order of the deposits, so a lost or
// double-applied write is named as what it is.
//
// Deliberately small, and one entity rather than four. A linearizability check
// is exponential in the number of *pending* operations, and under this nemesis
// many calls end unknown: each is an op the checker may place anywhere or
// drop. Few ops keep the search bounded; the breadth comes from the seeds, not
// from any one history.

/// The account this sweep decides, whose deposit carries an **idempotency key**.
///
/// The Appendix A `Account` above cannot be held to a linearizable history
/// over this wire, by contract rather than by defect. Delivery is at-most-once
/// *at the caller* (§7.2): a duplicated request frame is handled twice, so a
/// bare `Deposit { cents: 1 }` lands twice for one logical operation and no
/// sequential order explains the balance that follows. The spec names the
/// remedy in the same section (higher guarantees are "built atop this layer
/// with explicit idempotency keys"), and `tests/support/mod.rs` in
/// `actor-simulation` is the same construction for the register, recorded in
/// `corpus.txt` after the identical mistake.
///
/// The key lives in the *event*, so the applied set folds from the journal and
/// survives deactivation and failover: a duplicate arriving after a
/// rehydration is still recognized. A read needs no key: it changes nothing,
/// and a duplicate's reply is the value at its own instant, which falls inside
/// the caller's window and linearizes there.
///
/// Its **read** is a writing command, for the other half of the same reason. A
/// query commits nothing and is served from the activation, and §7.5 is
/// explicit that this is "read-your-leader (relaxed), not linearizable under
/// partition": a deposed-but-unfenced leader MAY serve a stale value, and a
/// quorum-less recovery may even seed that state with an uncommitted record.
/// Linearizable reads are a deferred upgrade (§16), and the spec names the
/// interim construction in the same paragraph: "a caller that needs a
/// linearizable read in the meantime issues a trivial *writing* command (one
/// that emits an event): it rides the §6 output gate, so it commits through
/// the shard leader and reflects committed state, or fails". `Probed` is that
/// event: the fold ignores it, so the reply is the committed balance at the
/// seq the read commits at.
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

impl Entity for KeyedAccount {
    type System = SimNode;
    type State = KeyedBalance;
    type Event = KeyedLedger;
    type Facets = ();
    const ENTITY_TYPE: &'static str = "bank.KeyedAccount";

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

    fn register(r: &mut entity::EntityRegistry<Self>) {
        r.accept::<KeyedDeposit>();
        r.accept::<ReadKeyedBalance>();
    }
}

/// A deposit named by its caller's `(client, seq)`: identical across every copy
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

impl EntityHandler<KeyedDeposit> for KeyedAccount {
    async fn handle(
        &self,
        state: &KeyedBalance,
        msg: KeyedDeposit,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<KeyedLedger>, i64) {
        // A re-delivery journals nothing and answers from the state it already
        // produced; the history records `AddOk` either way, so the balance a
        // duplicate reports is not what the model decides.
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

impl EntityHandler<ReadKeyedBalance> for KeyedAccount {
    async fn handle(
        &self,
        state: &KeyedBalance,
        _msg: ReadKeyedBalance,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<KeyedLedger>, i64) {
        // Emitting an event is the point (see the type's doc): it puts this
        // read behind the output gate, so the balance returned is the
        // committed one and a deposed leader fails the call instead of
        // answering staler.
        (vec![KeyedLedger::Probed], state.cents)
    }
}

/// Deposits and reads against a single account, recorded as a [`Counter`]
/// history. A deposit is `Add`, a read is `Read`, and a call that fails is
/// left pending (`info`): its effect may or may not have landed, and the
/// checker may decide either way.
struct LinearizableAccountSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
}

impl ClusterWorkload for LinearizableAccountSwarm {
    fn name(&self) -> &'static str {
        "linearizable-account-entity"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::leader(self.swim(), self.nodes, DowningPolicy::Conservative)
    }

    fn setup(&self, _ctx: &ClusterCtx) {}

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        Box::pin(async move {
            let hosts: Vec<_> = nodes
                .iter()
                .map(|s| s.entities::<KeyedAccount>(config(false)))
                .collect();
            let clock = nodes[0].clock().clone();
            let entropy = nodes[0].entropy().clone();
            clock.sleep(Duration::from_secs(3)).await;

            let history: History<Counter> = History::new();
            let mut tasks = Vec::new();
            for c in 0..clients {
                // Each client through a different node's gateway, so the history
                // mixes local and forwarded calls against one object (E13).
                let entities = hosts[c % hosts.len()].clone();
                let history = history.clone();
                let entropy = entropy.clone();
                tasks.push(async move {
                    let acct = entities.entity("account/linearizable");
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
                "account history was not linearizable: a deposit was lost or \
                 applied twice: {verdict}",
            );
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        let mut invariants = default_invariants();
        invariants.push(Box::new(CommitMonotonic::new(
            "linearizable-account-commit-monotonic",
            "entity",
        )));
        invariants
    }
}

#[test]
fn the_account_entity_is_linearizable_under_cluster_faults() {
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

// --- Hibernation crossed with the nemesis (E12 × E14) -------------------------
//
// The sweep above keeps every activation resident, so it never reaches the
// path where an entity is evicted and the *next* call has to rebuild it: head
// and tail recovered from a write quorum, on whatever node leads the shard by
// then (§8, §9). That path is where the acknowledged-write-lost bug in
// `corpus.txt` lived, and E12 (hibernation round-trip) and E14 (lossless
// failover) are otherwise exercised alone: `sql_swarm.rs`'s single-node
// workload hibernates with no faults, and every clustered workload faults
// without hibernating.
//
// This one crosses them: entities hibernate and snapshot mid-run, and the next
// call rehydrates through the quorum barrier while the nemesis partitions,
// freezes, and kills the processes around it. `CommitMonotonic` names a write
// the recovery dropped or replayed onto an occupied slot.

#[test]
fn hibernating_entity_invariants_hold_under_restarts() {
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
    // hibernating sweep in the crate: whether the nemesis *draws* a restart
    // depends on its vocabulary and the seed range, not on any one facet's
    // traffic, and this is the cheapest workload to ask it of.
    //
    // `coverage_seeds`, not `sweep_seeds`, because the claim is about the
    // whole declared range. One seed runs six nemesis rounds against a
    // seven-action vocabulary, so it draws no restart about two runs in five;
    // narrowing the range would not tighten the assertion, only make it flaky.
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
fn hibernating_entity_swarm_is_reproducible() {
    // Hibernation, snapshot, rehydration, and process restart all ride the
    // Clock/Entropy/Spawner seams, so the stream stays byte-identical per seed.
    if let Err(divergence) = replay_cluster_swarm(&hibernating(2, 5), sweep_seeds(0..12)) {
        panic!("{divergence}");
    }
}

#[test]
fn entity_swarm_actually_fires_each_fault_type() {
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
// Single-node sweeps (entity §14)
// =============================================================================
//
// The cluster sweeps above put the entity under a nemesis; these put it under
// concurrency alone, on the single-node `Local` tier: a linearizability sweep
// over the shared `CounterEntity` and a record-subscription sweep, each paired
// with its reproducibility sweep. They live here rather than beside the
// scenarios in `entities.rs` because they are sweeps: they fail by naming a seed
// (docs/simulation-testing.md).

// --- Entity invariant checkers over the §13 event stream -----------------------

/// The entity safety core for these single-node suites: E3/E5 commit
/// monotonicity and E6 per-node exactly-once activation, both taken from
/// [`entity::testing`] rather than restated here. `ActivationSingletonPerNode`
/// also clears a node's live set on `NodeDown`, inert in the no-fault runs
/// below and correct in the faulted ones.
fn entity_invariants() -> Vec<Box<dyn Invariant>> {
    let mut invariants = default_invariants();
    invariants.push(Box::new(ActivationSingletonPerNode::new(
        "entity-exactly-once-activation",
        "entity",
    )));
    invariants.push(Box::new(CommitMonotonic::new(
        "entity-commit-monotonic",
        "entity",
    )));
    invariants
}

// --- Linearizability workload (E2) --------------------------------------------

async fn counter_client(
    counter: EntityRef<CounterEntity>,
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
        "linearizable-counter-entity"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let clients = self.clients;
        let ops = self.ops;
        Box::pin(async move {
            let counters = system.entities::<CounterEntity>(EntityConfig::default());
            // One entity, hammered concurrently: the single linearizable object.
            let counter = counters.entity("counter/0");
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
                "counter entity history was not linearizable: {verdict}",
            );
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        entity_invariants()
    }
}

#[test]
fn counter_entity_is_linearizable_across_seeds() {
    let workload = CounterWorkload { clients: 4, ops: 8 };
    if let Err(failure) = run_swarm(&workload, sweep_seeds(0..96)) {
        panic!("{failure}");
    }
}

#[test]
fn counter_entity_run_is_reproducible() {
    // The determinism contract (§14): the same seed yields a byte-identical event
    // stream, and entity `App` events are part of it, so this guards E2 too.
    let workload = CounterWorkload { clients: 3, ops: 6 };
    if let Err(divergence) = replay_swarm(&workload, sweep_seeds(0..64)) {
        panic!("{divergence}");
    }
}

// --- Record subscription (§7.9, E16) ------------------------------------------

/// A subscriber registers from the empty head, the entity takes a run of writes,
/// and the pushed stream is reconciled by `Seq`. The reconstructed sequence MUST
/// equal the committed records: contiguous `1..=writes`, in order, no gap or
/// duplicate, which is exactly what `load` to the head would return (**E16**).
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
            let counters = system.entities::<CounterEntity>(EntityConfig::default());
            let counter = counters.entity("counter/sub");

            // Subscribe from the empty head, before any write, so every commit is
            // pushed live.
            let sub = counter.subscribe(Seq::ZERO).await.expect("subscribe");
            assert_eq!(sub.head, Seq::ZERO, "a fresh entity's head is ZERO");

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
                "the stream reached the committed head (push == load, E16)"
            );
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        entity_invariants()
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
