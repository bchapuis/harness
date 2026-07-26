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
//!
//! The register object itself lives in `tests/support/`, shared with
//! `conformance_linearizability_swarm.rs` so the local scenarios and the
//! cluster sweep decide histories against the same object.

mod support;

use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_simulation::History;
use actor_simulation::Register;
use actor_simulation::RegisterOp;
use actor_simulation::RegisterRet;
use actor_simulation::SimSystem;
use actor_simulation::Workload;
use actor_simulation::check_linearizable;
use actor_simulation::run_seed;
use actor_simulation::sweep_seeds;

use support::Cas;
use support::Read;
use support::RegisterActorIn;
use support::RegisterRef;
use support::Write;
use support::client;

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
    if let Err(failure) = run_seed_sweep(&workload, sweep_seeds(0..128)) {
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
