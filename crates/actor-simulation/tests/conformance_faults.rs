//! Conformance: transport fault injection (spec §7.2, §18.3). Per-pair FIFO
//! survives latency jitter (#3); total loss surfaces as `Timeout` rather than a
//! hang (#1); and duplication is tolerated — the framework gives at-most-once
//! *at the caller*, not exactly-once delivery (§7.2). Fault-coverage swarms then
//! prove each fault type provably fired across a seed sweep — drops, duplication,
//! delay/reorder, partition/crash, and registry outage/staleness — so a green
//! sweep can't secretly have only ever run the happy path.

mod support;

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RouteStrategy;
use actor_cluster::Router;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::CallError;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Key;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::FaultPolicy;
use actor_simulation::RegistryFaultPolicy;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use actor_simulation::run_cluster_swarm_coverage;
use serde::Deserialize;
use serde::Serialize;

use support::Counter;
use support::Get;
use support::Greet;
use support::Greeter;
use support::Inc;

fn latency_only(max_ms: u64) -> FaultPolicy {
    FaultPolicy {
        max_latency: Duration::from_millis(max_ms),
        ..FaultPolicy::default()
    }
}

// --- #3: per-pair FIFO survives latency jitter -------------------------------

#[derive(Serialize, Deserialize)]
struct Seq(u64);
impl Message for Seq {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("faults.Seq");
}

// An `ask` counterpart to `Seq`: it logs the same way but expects a reply, so a
// sender can interleave `tell` and `ask` to one recipient (spec §6 #3).
#[derive(Serialize, Deserialize)]
struct SeqAsk(u64);
impl Message for SeqAsk {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("faults.SeqAsk");
}

struct Order<S> {
    log: Arc<Mutex<Vec<u64>>>,
    _system: PhantomData<fn() -> S>,
}
impl<S: ActorSystem> Actor for Order<S> {
    type System = S;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Seq>();
        r.accept::<SeqAsk>();
    }
}
impl<S: ActorSystem> Handler<Seq> for Order<S> {
    async fn handle(&mut self, msg: Seq, _ctx: &Ctx<Self>) {
        self.log.lock().unwrap().push(msg.0);
    }
}
impl<S: ActorSystem> Handler<SeqAsk> for Order<S> {
    async fn handle(&mut self, msg: SeqAsk, _ctx: &Ctx<Self>) -> u64 {
        self.log.lock().unwrap().push(msg.0);
        msg.0
    }
}

#[test]
fn per_pair_fifo_survives_latency_jitter() {
    let sim = Simulation::new(1);
    let net = SimNetwork::new(&sim).with_faults(latency_only(50));
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&log);
    sim.block_on(async move {
        let order = node_a.spawn(Order {
            log: observed,
            _system: PhantomData,
        });
        let remote = node_b.resolve::<Order<SimCluster>>(order.id().clone());
        for i in 0..10 {
            remote.tell(Seq(i)).await.unwrap();
        }
    });

    // Despite per-frame jitter, frames on one directed pair arrive in send order.
    assert_eq!(*log.lock().unwrap(), (0..10).collect::<Vec<_>>());
}

#[test]
fn per_pair_fifo_holds_across_interleaved_tell_and_ask() {
    let sim = Simulation::new(7);
    let net = SimNetwork::new(&sim).with_faults(latency_only(50));
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&log);
    sim.block_on(async move {
        let order = node_a.spawn(Order {
            log: observed,
            _system: PhantomData,
        });
        let remote = node_b.resolve::<Order<SimCluster>>(order.id().clone());
        // Issue `tell` and `ask` from one sender to one recipient *concurrently*
        // (`join!` initiates all six in argument order), so jittered frames race
        // in flight. Spec §6 #3: `tell` and `ask` from the same sender share one
        // FIFO order — the recipient must still observe 0,1,2,3,4,5.
        let (t0, a1, t2, a3, t4, a5) = futures::join!(
            remote.tell(Seq(0)),
            remote.ask(SeqAsk(1)),
            remote.tell(Seq(2)),
            remote.ask(SeqAsk(3)),
            remote.tell(Seq(4)),
            remote.ask(SeqAsk(5)),
        );
        t0.unwrap();
        t2.unwrap();
        t4.unwrap();
        assert_eq!(
            (a1.unwrap(), a3.unwrap(), a5.unwrap()),
            (1, 3, 5),
            "each ask resolved to its own reply",
        );
    });

    assert_eq!(
        *log.lock().unwrap(),
        (0..6).collect::<Vec<_>>(),
        "interleaved tell and ask from one sender are observed in send order (spec §6 #3)",
    );
}

// --- #1 / §7.2: total loss completes with Timeout, never hangs ---------------

#[test]
fn total_loss_yields_timeout_not_a_hang() {
    let sim = Simulation::new(2);
    let net = SimNetwork::new(&sim).with_faults(FaultPolicy {
        drop_num: 1,
        drop_den: 1, // every frame is lost
        ..FaultPolicy::default()
    });
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let outcome = sim.block_on(async move {
        let greeter = node_a.spawn(Greeter::<SimCluster>::new("Hello"));
        node_b
            .resolve::<Greeter<SimCluster>>(greeter.id().clone())
            .ask_timeout(
                Greet {
                    name: "world".into(),
                },
                Duration::from_secs(1),
            )
            .await
    });
    assert_eq!(outcome, Err(CallError::Timeout));
}

// --- §7.2: duplication double-handles the server but the caller resolves once -

#[test]
fn duplication_is_tolerated_with_one_outcome_at_the_caller() {
    let sim = Simulation::new(3);
    let net = SimNetwork::new(&sim).with_faults(FaultPolicy {
        duplicate_num: 1,
        duplicate_den: 1, // every frame is duplicated
        ..FaultPolicy::default()
    });
    let node_a = net.join(NodeId::new(1));
    let node_b = net.join(NodeId::new(2));

    let count = sim.block_on(async move {
        let counter = node_a.spawn(Counter::<SimCluster>::new());
        let remote = node_b.resolve::<Counter<SimCluster>>(counter.id().clone());
        remote.tell(Inc).await.unwrap(); // duplicated → handled twice on the server
        remote.ask(Get).await // reply duplicated → caller resolves once
    });
    // Server double-handled the Inc (count == 2); the caller still got a single
    // well-formed reply.
    assert_eq!(count, Ok(2));
}

// --- §18.3: fault-injection coverage across a seed sweep ---------------------
//
// A swarm that *configures* faults but, by seed luck, never *triggers* one gives
// false confidence: a green sweep that secretly only ever ran the happy path
// proves much less than it appears to. So the cluster swarm tallies the faults
// it actually exercised across its seed range, and these tests assert that every
// fault type fired at least once. If a future change to the fault sampling or
// the network silently stopped injecting (say) duplication, these tests go red
// even though no invariant was violated — surfacing a coverage regression that
// the invariant checks alone could never see.

struct CovGreeter;

impl Actor for CovGreeter {
    type System = SimCluster;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<CovGreet>();
    }
}

#[derive(Serialize, Deserialize)]
struct CovGreet;

impl Message for CovGreet {
    type Reply = String;
    const MANIFEST: Manifest = Manifest::new("cov.Greet");
}

impl Handler<CovGreet> for CovGreeter {
    async fn handle(&mut self, _msg: CovGreet, _ctx: &Ctx<Self>) -> String {
        "hi".into()
    }
}

const GREETERS: Key<CovGreeter> = Key::new("cov.greeters");

struct Chatter {
    nodes: usize,
    rounds: u64,
}

impl ClusterWorkload for Chatter {
    fn name(&self) -> &'static str {
        "cov-chatter"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(200),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::Gossip {
            swim: self.swim(),
            downing: DowningPolicy::Timeout(Duration::from_millis(300)),
        }
    }

    fn setup(&self, ctx: &ClusterCtx) {
        for node in ctx.nodes() {
            let greeter = node.spawn(CovGreeter);
            node.receptionist().register(GREETERS, &greeter);
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let caller = ctx.nodes()[0].clone();
        let rounds = self.rounds;
        Box::pin(async move {
            let clock = caller.clock().clone();
            for _ in 0..rounds {
                clock.sleep(Duration::from_millis(150)).await;
                for service in caller.receptionist().lookup(GREETERS).iter() {
                    let _ = service
                        .ask_timeout(CovGreet, Duration::from_millis(500))
                        .await;
                }
            }
        })
    }
}

#[test]
fn the_cluster_swarm_actually_exercises_every_fault_type() {
    let workload = Chatter {
        nodes: 3,
        rounds: 10,
    };
    // Sweep a range wide enough that the seed-sampled fault policy and the
    // nemesis between them trigger every kind of fault. The sweep also checks
    // invariants on each run, so this is coverage *and* correctness.
    let stats = match run_cluster_swarm_coverage(&workload, 0..64) {
        Ok(stats) => stats,
        Err(failure) => panic!("{failure}"),
    };

    assert!(
        stats.dropped > 0,
        "no frame was ever dropped across the sweep: {stats:?}",
    );
    assert!(
        stats.duplicated > 0,
        "no frame was ever duplicated across the sweep: {stats:?}",
    );
    assert!(
        stats.delayed > 0,
        "no frame was ever delayed/reordered across the sweep: {stats:?}",
    );
    assert!(
        stats.blocked > 0,
        "no frame was ever blocked by a partition/crash across the sweep: {stats:?}",
    );
}

/// The same chatter under the **registry-based** control plane (spec §9.4.2):
/// proves the registry-specific faults — outage windows opened by the nemesis,
/// stale snapshots served by the seeded policy — actually fired across the
/// sweep, on top of the transport faults (spec §18.3).
struct RegistryChatter {
    nodes: usize,
    rounds: u64,
}

impl ClusterWorkload for RegistryChatter {
    fn name(&self) -> &'static str {
        "cov-registry-chatter"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(200),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::Registry {
            swim: self.swim(),
            sync_interval: Duration::from_millis(100),
            faults: RegistryFaultPolicy {
                max_latency: Duration::from_millis(40),
                stale_num: 1,
                stale_den: 3,
                max_staleness: 4,
            },
        }
    }

    fn setup(&self, ctx: &ClusterCtx) {
        for node in ctx.nodes() {
            let greeter = node.spawn(CovGreeter);
            node.receptionist().register(GREETERS, &greeter);
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let caller = ctx.nodes()[0].clone();
        let registry = ctx.registry().expect("registry mode").clone();
        let victim = ctx.nodes()[1].node();
        let rounds = self.rounds;
        Box::pin(async move {
            let clock = caller.clock().clone();
            for round in 0..rounds {
                clock.sleep(Duration::from_millis(150)).await;
                // Churn the registry so stale reads have revisions to lag behind.
                if round % 2 == 0 {
                    registry.drain(victim);
                } else {
                    registry.resume(victim);
                }
                for service in caller.receptionist().lookup(GREETERS).iter() {
                    let _ = service
                        .ask_timeout(CovGreet, Duration::from_millis(500))
                        .await;
                }
            }
        })
    }
}

#[test]
fn the_registry_swarm_actually_exercises_registry_faults() {
    let workload = RegistryChatter {
        nodes: 3,
        rounds: 10,
    };
    let stats = match run_cluster_swarm_coverage(&workload, 0..64) {
        Ok(stats) => stats,
        Err(failure) => panic!("{failure}"),
    };

    assert!(
        stats.registry_outages > 0,
        "the nemesis never opened a registry outage across the sweep: {stats:?}",
    );
    assert!(
        stats.registry_stale > 0,
        "no stale registry snapshot was ever served across the sweep: {stats:?}",
    );
}

// --- Cluster utilities under the full fault space (utilities spec §2–§4) ------

/// The singleton's handoff message; delivered locally by the manager, so it
/// needs no `register` entry.
#[derive(Clone, Serialize, Deserialize)]
struct Halt;

impl Message for Halt {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("cov.Halt");
}

impl Handler<Halt> for CovGreeter {
    async fn handle(&mut self, _msg: Halt, ctx: &Ctx<Self>) {
        ctx.stop();
    }
}

const COV_SINGLETON: &str = "cov-singleton";

/// All traffic flows through the cluster utilities — group routers (round-robin,
/// seeded random, rendezvous-hashed, and `tell`) and a singleton proxy — while
/// the nemesis partitions and crashes nodes. The coverage sweep then proves the
/// utilities really ran under loss, duplication, reordering, and partition, not
/// just under the controlled faults of their conformance tests; the standing
/// invariants (including the singleton's per-node discipline, U2) hold on every
/// seed.
struct UtilityChatter {
    nodes: usize,
    rounds: u64,
}

impl ClusterWorkload for UtilityChatter {
    fn name(&self) -> &'static str {
        "cov-utility-chatter"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(200),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::Gossip {
            swim: self.swim(),
            downing: DowningPolicy::Timeout(Duration::from_millis(300)),
        }
    }

    fn setup(&self, ctx: &ClusterCtx) {
        for node in ctx.nodes() {
            let greeter = node.spawn(CovGreeter);
            node.receptionist().register(GREETERS, &greeter);
            node.singleton(COV_SINGLETON, || CovGreeter, Halt);
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimCluster> = ctx.nodes().to_vec();
        let rounds = self.rounds;
        Box::pin(async move {
            let clock = nodes[0].clock().clone();
            for round in 0..rounds {
                clock.sleep(Duration::from_millis(150)).await;
                for node in &nodes {
                    let rr = Router::new(node, GREETERS, RouteStrategy::RoundRobin);
                    if let Some(routee) = rr.route() {
                        let _ = routee
                            .ask_timeout(CovGreet, Duration::from_millis(500))
                            .await;
                    }
                    let key = format!("k-{round}");
                    if let Some(routee) = rr.route_by(key.as_bytes()) {
                        let _ = routee
                            .ask_timeout(CovGreet, Duration::from_millis(500))
                            .await;
                    }
                    // Fire-and-forget through the router, and a call through the
                    // singleton proxy; any outcome is acceptable under faults.
                    let random = Router::new(node, GREETERS, RouteStrategy::Random);
                    let _ = random.tell(CovGreet).await;
                    let proxy = node.singleton_proxy::<CovGreeter>(COV_SINGLETON);
                    if let Some(instance) = proxy.resolve() {
                        let _ = instance
                            .ask_timeout(CovGreet, Duration::from_millis(500))
                            .await;
                    }
                }
            }
        })
    }
}

#[test]
fn the_utilities_swarm_actually_exercises_every_fault_type() {
    let workload = UtilityChatter {
        nodes: 3,
        rounds: 10,
    };
    let stats = match run_cluster_swarm_coverage(&workload, 0..64) {
        Ok(stats) => stats,
        Err(failure) => panic!("{failure}"),
    };

    assert!(
        stats.dropped > 0,
        "no frame was ever dropped across the utilities sweep: {stats:?}",
    );
    assert!(
        stats.duplicated > 0,
        "no frame was ever duplicated across the utilities sweep: {stats:?}",
    );
    assert!(
        stats.delayed > 0,
        "no frame was ever delayed/reordered across the utilities sweep: {stats:?}",
    );
    assert!(
        stats.blocked > 0,
        "no frame was ever blocked by a partition/crash across the utilities sweep: {stats:?}",
    );
}
