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

use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::Counter;
use actor_simulation::CounterOp;
use actor_simulation::CounterRet;
use actor_simulation::History;
use actor_simulation::SimEntropy;
use actor_simulation::SimSystem;
use actor_simulation::Workload;
use actor_simulation::check_linearizable;
use actor_simulation::replay_swarm;
use actor_simulation::run_swarm;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::SimNode;
use actor_simulation::coverage_seeds;
use actor_simulation::default_invariants;
use actor_simulation::replay_cluster_swarm;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_cluster_swarm_coverage;
use actor_simulation::sweep_seeds;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GranaryConfig;
use granary::GrainRef;
use granary::GranaryExt;
use granary::Seq;
use granary::testing::ActivationSingletonPerNode;
use granary::testing::CommitMonotonic;
use serde::Deserialize;
use serde::Serialize;

use support::Add;
use support::CounterEvent;
use support::CounterGrain;
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

fn config() -> GranaryConfig {
    GranaryConfig {
        shards: 2,
        replication_factor: 3,
        idle_after: Duration::from_secs(60),
        snapshot_every: 8,
        ..GranaryConfig::default()
    }
}

/// Deposit-and-read traffic against a handful of grains, hosted on a leader-based
/// cluster, driven through the public `GrainRef` API only (spec §18.4). Every
/// call is faulted by the nemesis and the transport; a failed call is recorded as
/// nothing and the client moves on, so the drive future always completes and the
/// invariants are checked over whatever the run produced.
struct AccountSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
}

impl ClusterWorkload for AccountSwarm {
    fn name(&self) -> &'static str {
        "granary-account-swarm"
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
        Box::pin(async move {
            // Host the type on every node: each starts its gateway and joins/leads
            // its shards (§5.3). Done at drive start; the bounded redirect absorbs
            // the bootstrap window.
            let granaries: Vec<_> = nodes
                .iter()
                .map(|s| s.granary::<Account>(config()))
                .collect();
            let clock = nodes[0].clock().clone();
            let entropy = nodes[0].entropy().clone();
            // Let the control-plane and shard groups elect before traffic.
            clock.sleep(Duration::from_secs(3)).await;

            let mut tasks = Vec::new();
            for c in 0..clients {
                let granary = granaries[c % granaries.len()].clone();
                let entropy = entropy.clone();
                tasks.push(async move {
                    for _ in 0..ops {
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
        invariants
    }
}

#[test]
fn grain_invariants_hold_under_the_cluster_swarm() {
    // #4: the safety core plus G3/G5 commit-monotonicity hold on every seeded run
    // under partitions, crashes, loss, duplication, and delay.
    let workload = AccountSwarm {
        nodes: 3,
        clients: 3,
        ops: 6,
    };
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..24)) {
        panic!("{failure}");
    }
}

#[test]
fn grain_swarm_is_reproducible() {
    // #7: the same seed replays to a byte-identical event stream — grain `App`
    // events included — even under cluster nemesis and transport faults.
    let workload = AccountSwarm {
        nodes: 3,
        clients: 2,
        ops: 5,
    };
    if let Err(divergence) = replay_cluster_swarm(&workload, sweep_seeds(0..12)) {
        panic!("{divergence}");
    }
}

#[test]
fn grain_swarm_actually_fires_each_fault_type() {
    // #8: a green sweep must not be a silent happy-path sweep. Across the seed
    // range the transport injected loss, duplication, reordering (delay), and
    // partition/crash blocking at least once each.
    let workload = AccountSwarm {
        nodes: 3,
        clients: 3,
        ops: 6,
    };
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
                "counter grain history was not linearizable: {verdict:?}",
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
