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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::SimCluster;
use actor_simulation::check_cluster_reproducible;
use actor_simulation::default_invariants;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_cluster_swarm_coverage;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainEvent;
use granary::GrainHandler;
use granary::GrainName;
use granary::GranaryConfig;
use granary::GranaryExt;
use serde::Deserialize;
use serde::Serialize;

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
    type System = SimCluster;
    type State = Balance;
    type Event = Ledger;
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
    async fn handle(&self, state: &Balance, msg: Deposit, _ctx: &GrainCtx<Self>) -> (Vec<Ledger>, i64) {
        (vec![Ledger::Deposited(msg.cents)], state.cents + msg.cents as i64)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadBalance;
impl Message for ReadBalance {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.ReadBalance");
}

impl GrainHandler<ReadBalance> for Account {
    async fn handle(&self, state: &Balance, _msg: ReadBalance, _ctx: &GrainCtx<Self>) -> (Vec<Ledger>, i64) {
        (vec![], state.cents)
    }
}

// --- A grain-specific continuous safety checker -------------------------------

/// **Commit head is monotonic** (invariants **G3**, **G5**): a grain's committed
/// seq strictly increases and never regresses. Sound across the cluster: only a
/// shard leader commits (a quorum append, §7.2), a new leader inherits every
/// committed entry (leader completeness, G14) and continues at a higher seq, and
/// a minority "leader" never commits — so no `Committed` for a name ever names a
/// seq at or below one already seen, even across failover.
#[derive(Default)]
struct CommitMonotonic {
    last: BTreeMap<GrainName, u64>,
}

impl Invariant for CommitMonotonic {
    fn name(&self) -> &'static str {
        "grain-commit-monotonic"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        if let Some(GrainEvent::Committed { name, seq, .. }) = event.as_app::<GrainEvent>() {
            let prev = self.last.get(name).copied().unwrap_or(0);
            if *seq <= prev {
                return Err(format!(
                    "grain {name} committed seq {seq} not after previous head {prev} (G3/G5)"
                ));
            }
            self.last.insert(name.clone(), *seq);
        }
        Ok(())
    }
}

/// **Exactly-once activation per node** (invariant **G6**): on any one node, a
/// grain is never live twice at once. Keyed by `(node, name)`, so an activation
/// that migrates to another leader on failover is not mistaken for a second one.
/// Crash-sound: a node's live set is cleared when the stream reports that node
/// `NodeDown` (its activations are gone), so a re-activation after the node
/// rejoins and re-leads is not a false positive.
#[derive(Default)]
struct ActivationSingletonPerNode {
    live: BTreeSet<(NodeId, GrainName)>,
}

impl Invariant for ActivationSingletonPerNode {
    fn name(&self) -> &'static str {
        "grain-activation-singleton-per-node"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        // A node declared down loses its activations; drop them so a later
        // re-activation on the recovered node is sound (G6 is per live node).
        if let Event::NodeDown { node, .. } = event {
            self.live.retain(|(n, _)| n != node);
            return Ok(());
        }
        match event.as_app::<GrainEvent>() {
            Some(GrainEvent::Activated { node, name }) => {
                let fresh = self.live.insert((*node, name.clone()));
                if !fresh {
                    return Err(format!("grain {name} activated while already live on {node} (G6)"));
                }
            }
            Some(GrainEvent::Passivated { node, name }) => {
                self.live.remove(&(*node, name.clone()));
            }
            _ => {}
        }
        Ok(())
    }
}

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
        let nodes: Vec<SimCluster> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        Box::pin(async move {
            // Host the type on every node: each starts its gateway and joins/leads
            // its shards (§5.3). Done at drive start; the bounded redirect absorbs
            // the bootstrap window.
            let granaries: Vec<_> = nodes.iter().map(|s| s.granary::<Account>(config())).collect();
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
        invariants.push(Box::new(CommitMonotonic::default()));
        invariants.push(Box::new(ActivationSingletonPerNode::default()));
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
    if let Err(failure) = run_cluster_swarm(&workload, 0..24) {
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
    for seed in 0..12 {
        if let Err(divergence) = check_cluster_reproducible(&workload, seed) {
            panic!("{divergence}");
        }
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
    let stats = match run_cluster_swarm_coverage(&workload, 0..32) {
        Ok(stats) => stats,
        Err(failure) => panic!("{failure}"),
    };
    assert!(stats.dropped > 0, "the sweep never dropped a frame (loss uncovered): {stats:?}");
    assert!(stats.duplicated > 0, "the sweep never duplicated a frame: {stats:?}");
    assert!(stats.delayed > 0, "the sweep never delayed a frame (reordering uncovered): {stats:?}");
    assert!(stats.blocked > 0, "the sweep never blocked a frame (partition/crash uncovered): {stats:?}");
}
