//! Conformance: linearizability checking over a live actor (spec §18.4).
//!
//! A register actor is hammered by several concurrent client processes issuing
//! reads, writes, and compare-and-sets. Each client records its operations into a
//! shared [`History`] through the public API only (spec §18.4) — `invoke` just
//! before the `ask`, `ok`/`info` just after — so the recorded order is the real
//! (virtual) time interleaving. At quiescence the history is checked for
//! linearizability against the [`Register`] reference model.
//!
//! The actor mailbox imposes a true serial order, so a correct implementation is
//! always linearizable; the value here is twofold. First, it runs the
//! linearizability machinery end to end on histories with genuine concurrency
//! (overlapping invoke/complete windows) across many seeds, so the checker is
//! exercised on real recorded traffic, not just hand-built unit histories.
//! Second, it is the standing guard that would catch any future change that broke
//! serial execution and let two operations interleave.

use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Entropy;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Key;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::History;
use actor_simulation::Register;
use actor_simulation::RegisterOp;
use actor_simulation::RegisterRet;
use actor_simulation::SimCluster;
use actor_simulation::SimSystem;
use actor_simulation::Workload;
use actor_simulation::check_linearizable;
use actor_simulation::run_cluster_seed;
use actor_simulation::run_seed;
use serde::Deserialize;
use serde::Serialize;

// --- The register actor -------------------------------------------------------

// A system-generic register so the same actor runs on `SimSystem` and
// `SimCluster` (generic actors are allowed by the spec, §1.2).
use std::marker::PhantomData;

struct RegisterActorIn<S> {
    value: i64,
    _s: PhantomData<fn() -> S>,
}

impl<S> RegisterActorIn<S> {
    fn new() -> Self {
        RegisterActorIn {
            value: 0,
            _s: PhantomData,
        }
    }
}

impl<S: ActorSystem> Actor for RegisterActorIn<S> {
    type System = S;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Read>();
        r.accept::<Write>();
        r.accept::<Cas>();
    }
}

#[derive(Serialize, Deserialize)]
struct Read;
impl Message for Read {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("lin.Read");
}

#[derive(Serialize, Deserialize)]
struct Write(i64);
impl Message for Write {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("lin.Write");
}

#[derive(Serialize, Deserialize)]
struct Cas(i64, i64);
impl Message for Cas {
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("lin.Cas");
}

impl<S: ActorSystem> Handler<Read> for RegisterActorIn<S> {
    async fn handle(&mut self, _msg: Read, _ctx: &Ctx<Self>) -> i64 {
        self.value
    }
}

impl<S: ActorSystem> Handler<Write> for RegisterActorIn<S> {
    async fn handle(&mut self, msg: Write, _ctx: &Ctx<Self>) {
        self.value = msg.0;
    }
}

impl<S: ActorSystem> Handler<Cas> for RegisterActorIn<S> {
    async fn handle(&mut self, msg: Cas, _ctx: &Ctx<Self>) -> bool {
        if self.value == msg.0 {
            self.value = msg.1;
            true
        } else {
            false
        }
    }
}

// --- A client process: picks ops from the seeded stream and records them ------

/// One client's traffic: `ops` operations against the shared register, each
/// recorded into the shared history. Values are drawn from a small domain so
/// reads, writes, and CASes actually interact (CASes sometimes match).
async fn client<R>(
    reg: R,
    history: History<Register>,
    entropy: actor_simulation::SimEntropy,
    ops: u64,
) where
    R: RegisterRef,
{
    for _ in 0..ops {
        match entropy.next_u64() % 3 {
            0 => {
                let id = history.invoke(RegisterOp::Read);
                match reg.read().await {
                    Ok(v) => history.ok(id, RegisterRet::Read(v)),
                    Err(()) => history.info(id),
                }
            }
            1 => {
                let v = (entropy.next_u64() % 4) as i64;
                let id = history.invoke(RegisterOp::Write(v));
                match reg.write(v).await {
                    Ok(()) => history.ok(id, RegisterRet::WriteOk),
                    Err(()) => history.info(id),
                }
            }
            _ => {
                let old = (entropy.next_u64() % 4) as i64;
                let new = (entropy.next_u64() % 4) as i64;
                let id = history.invoke(RegisterOp::Cas(old, new));
                match reg.cas(old, new).await {
                    Ok(b) => history.ok(id, RegisterRet::Cas(b)),
                    Err(()) => history.info(id),
                }
            }
        }
    }
}

/// A uniform calling surface over the register, so the same client code drives it
/// both locally and across the network. `Err(())` means the outcome is unknown
/// (any `CallError`) — recorded as a pending operation.
trait RegisterRef: Clone {
    fn read(&self) -> BoxFuture<'static, Result<i64, ()>>;
    fn write(&self, v: i64) -> BoxFuture<'static, Result<(), ()>>;
    fn cas(&self, old: i64, new: i64) -> BoxFuture<'static, Result<bool, ()>>;
}

// --- Single-node workload -----------------------------------------------------

#[derive(Clone)]
struct LocalReg(actor_core::ActorRef<RegisterActorIn<SimSystem>>);

impl RegisterRef for LocalReg {
    fn read(&self) -> BoxFuture<'static, Result<i64, ()>> {
        let r = self.0.clone();
        Box::pin(async move { r.ask(Read).await.map_err(|_| ()) })
    }
    fn write(&self, v: i64) -> BoxFuture<'static, Result<(), ()>> {
        let r = self.0.clone();
        Box::pin(async move { r.ask(Write(v)).await.map_err(|_| ()) })
    }
    fn cas(&self, old: i64, new: i64) -> BoxFuture<'static, Result<bool, ()>> {
        let r = self.0.clone();
        Box::pin(async move { r.ask(Cas(old, new)).await.map_err(|_| ()) })
    }
}

struct RegisterWorkload {
    clients: usize,
    ops: u64,
}

impl Workload for RegisterWorkload {
    fn name(&self) -> &'static str {
        "linearizable-register"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let clients = self.clients;
        let ops = self.ops;
        Box::pin(async move {
            let reg = LocalReg(system.spawn(RegisterActorIn::<SimSystem>::new()));
            let history: History<Register> = History::new();
            let mut tasks = Vec::new();
            for _ in 0..clients {
                tasks.push(client(
                    reg.clone(),
                    history.clone(),
                    system.entropy().clone(),
                    ops,
                ));
            }
            // join_all interleaves the clients at every await point, so their
            // invoke/complete windows genuinely overlap.
            futures::future::join_all(tasks).await;

            let verdict = check_linearizable(&history);
            assert!(
                verdict.is_ok(),
                "register history was not linearizable: {verdict:?}",
            );
        })
    }
}

// --- A deliberately broken register, to prove the check has teeth ------------

/// A register whose `Read` returns a value that was never written. Any history
/// it produces is non-linearizable, so the checker MUST reject it — the live
/// analogue of the unit tests, proving the record-and-check pipeline catches a
/// real violation rather than passing everything (cf. the determinism leak test).
struct BuggyRegister;

impl Actor for BuggyRegister {
    type System = SimSystem;

    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Read>();
    }
}

impl Handler<Read> for BuggyRegister {
    async fn handle(&mut self, _msg: Read, _ctx: &Ctx<Self>) -> i64 {
        999 // never written; init is 0 and clients only write 0..4
    }
}

#[test]
fn the_checker_catches_a_non_linearizable_register() {
    use actor_core::LocalSystem;
    use actor_simulation::Simulation;

    let sim = Simulation::new(0);
    let system = LocalSystem::new(sim.clock(), sim.entropy(), sim.spawner());
    let history: History<Register> = History::new();
    let h = history.clone();
    sim.block_on(async move {
        let reg = system.spawn(BuggyRegister);
        let id = h.invoke(RegisterOp::Read);
        let v = reg.ask(Read).await.expect("local ask succeeds");
        h.ok(id, RegisterRet::Read(v));
    });

    let verdict = check_linearizable(&history);
    assert!(
        !verdict.is_ok(),
        "a register returning a never-written value must be flagged non-linearizable",
    );
}

#[test]
fn register_is_linearizable_across_seeds() {
    let workload = RegisterWorkload {
        clients: 4,
        ops: 10,
    };
    if let Err(failure) = run_seed_sweep(&workload, 0..128) {
        panic!("{failure}");
    }
}

fn run_seed_sweep(
    workload: &RegisterWorkload,
    seeds: std::ops::Range<u64>,
) -> Result<(), actor_simulation::RunFailure> {
    for seed in seeds {
        run_seed(workload, seed)?;
    }
    Ok(())
}

// --- Cluster workload: a remote register under faults -------------------------

const REG: Key<RegisterActorIn<SimCluster>> = Key::new("lin.register");

struct RemoteRegisterWorkload {
    nodes: usize,
    clients: usize,
    ops: u64,
}

#[derive(Clone)]
struct RemoteReg(actor_core::ActorRef<RegisterActorIn<SimCluster>>);

impl RegisterRef for RemoteReg {
    fn read(&self) -> BoxFuture<'static, Result<i64, ()>> {
        let r = self.0.clone();
        Box::pin(async move {
            r.ask_timeout(Read, Duration::from_millis(500))
                .await
                .map_err(|_| ())
        })
    }
    fn write(&self, v: i64) -> BoxFuture<'static, Result<(), ()>> {
        let r = self.0.clone();
        Box::pin(async move {
            r.ask_timeout(Write(v), Duration::from_millis(500))
                .await
                .map_err(|_| ())
        })
    }
    fn cas(&self, old: i64, new: i64) -> BoxFuture<'static, Result<bool, ()>> {
        let r = self.0.clone();
        Box::pin(async move {
            r.ask_timeout(Cas(old, new), Duration::from_millis(500))
                .await
                .map_err(|_| ())
        })
    }
}

impl ClusterWorkload for RemoteRegisterWorkload {
    fn name(&self) -> &'static str {
        "linearizable-remote-register"
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
        // A single register lives on node 0 — the one linearizable object.
        let host = &ctx.nodes()[0];
        let reg = host.spawn(RegisterActorIn::<SimCluster>::new());
        host.receptionist().register(REG, &reg);
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        // Clients run on the *other* nodes, so their calls cross the faulted
        // network: a drop/partition/crash surfaces as Unreachable/Timeout and is
        // recorded as a pending (info) op — exactly the unknown-outcome case the
        // checker must handle.
        let nodes: Vec<SimCluster> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        Box::pin(async move {
            let caller = nodes[0].clone();
            // Discover the register from a peer (location-transparent ref).
            let entropy = caller.entropy().clone();
            let clock = caller.clock().clone();
            // Let membership and registry replication settle so the lookup lands.
            clock.sleep(Duration::from_millis(400)).await;

            let history: History<Register> = History::new();
            let mut tasks = Vec::new();
            for c in 0..clients {
                let node = nodes[c % nodes.len()].clone();
                let history = history.clone();
                let entropy = entropy.clone();
                tasks.push(async move {
                    // Re-discover from this client's own node.
                    let listing = node.receptionist().lookup(REG);
                    if let Some(reg) = listing.iter().next() {
                        let reg = RemoteReg(reg.clone());
                        client(reg, history, entropy, ops).await;
                    }
                });
            }
            futures::future::join_all(tasks).await;

            // Whatever the faults did, the observed history must be linearizable:
            // unknown-outcome calls are pending ops the checker may place or drop.
            let verdict = check_linearizable(&history);
            assert!(
                verdict.is_ok(),
                "remote register history was not linearizable: {verdict:?}",
            );
        })
    }
}

#[test]
fn remote_register_is_linearizable_under_faults() {
    let workload = RemoteRegisterWorkload {
        nodes: 3,
        clients: 3,
        ops: 6,
    };
    for seed in 0..24 {
        if let Err(failure) = run_cluster_seed(&workload, seed) {
            panic!("{failure}");
        }
    }
}
