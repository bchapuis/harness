//! Alarm wiring costs a granary no commit progress under frame loss (§7.16).
//!
//! **What this settled.** A commit on this branch, `932ebca`, recorded that an
//! alarm-wired granary does not commit *at all* under plain frame loss — no
//! partition, no crash — the grain activating, never committing, being
//! passivated, while the caller hangs to its own deadline. `557ad1a` reverted
//! that prose with no reason written down, and the workload behind it was never
//! committed, so the claim survived as an assertion nobody could check. This file
//! is the check. **It does not reproduce**, under the conditions the commit named
//! or under harsher ones: at one-in-six, one-in-three, and one-in-two frame loss,
//! with passivation on and off, and with the settle window removed entirely, the
//! alarm-wired arm committed on every configuration — more often than the plain
//! arm on four of the six, never categorically less.
//!
//! It stays as a regression guard rather than being deleted with the question,
//! because the property it now asserts is worth holding: adding the alarm index
//! and its driver to a grain type must not cost that type its ability to commit.
//!
//! **The differential is controlled.** Both arms host the `AlarmIndex` granary,
//! so the Raft group count is identical and neither carries a group the other
//! lacks; both run the same `Timer`, the same seeds, and the same
//! [`FaultPolicy`]. Only the timer's wiring differs — `granary_with_alarms`
//! against plain `granary`, which is exactly `Some(index)` to the host plus the
//! driver loop.
//!
//! **What is asserted is the relationship, not a rate.** Frame loss makes any
//! given `ask` failable in both arms, so "the alarm arm committed less often" is
//! not a defect and a threshold on it would be a flake. The defect described was
//! categorical — the alarm arm commits nothing while the plain arm commits — so
//! that is what this checks.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::NodeId;
use actor_simulation::FaultPolicy;
use actor_simulation::SimNetwork;
use actor_simulation::SimNode;
use actor_simulation::Simulation;
use actor_simulation::sweep_seeds;
use granary::AlarmIndex;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;

use support::timer::Arm;

/// The `Timer` at this suite's tier.
type Timer = support::timer::Timer<SimNode>;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

const SHARDS: usize = 2;

/// The loss rates swept. One-in-six needs a retry on most quorum round trips;
/// one-in-three is where the plain arm starts failing most of its calls, which is
/// the band where a wiring-induced stall would be easiest to mistake for the
/// network — and so the band worth covering.
const LOSS_RATES: [u64; 2] = [6, 3];

/// How many arms one run issues.
const OPS: usize = 8;

/// Per-call deadline, generous relative to a quorum round trip so a failure means
/// the call was stuck rather than merely unlucky.
const CALL_DEADLINE: Duration = Duration::from_secs(10);

/// How long the cluster settles before traffic. Longer than the no-loss harness
/// needs: under loss the shard groups take several election rounds, and starting
/// traffic before they have measures the bootstrap rather than the steady state.
const SETTLE: Duration = Duration::from_secs(10);

// --- Harness (mirrors alarm_cluster.rs) ---------------------------------------

fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

fn raft() -> RaftConfig {
    let mut config = RaftConfig::new(vec![A, B, C]);
    config.election_timeout = Duration::from_millis(500);
    config.heartbeat_interval = Duration::from_millis(100);
    config
}

fn config() -> GranaryConfig {
    GranaryConfig {
        shards: SHARDS,
        idle_after: Duration::from_secs(600),
        snapshot_every: 8,
        ..GranaryConfig::default()
    }
}

fn lossy(one_in: u64) -> FaultPolicy {
    FaultPolicy {
        drop_num: 1,
        drop_den: one_in,
        ..FaultPolicy::default()
    }
}

/// Drive an async call to completion under the perpetually-running cluster loops.
fn drive<T: Send + 'static>(
    sim: &Simulation,
    settle: Duration,
    future: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    use actor_core::Spawner;
    let cell: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let out = Arc::clone(&cell);
    sim.spawner().launch(Box::pin(async move {
        *out.lock().unwrap() = Some(future.await);
    }));
    sim.run_for(settle);
    cell.lock()
        .unwrap()
        .take()
        .expect("future did not complete")
}

/// Whether this arm wires the timer to the alarm index — the one variable.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Wiring {
    /// `granary_with_alarms`: the host registers deadlines in the index and the
    /// per-type driver sweeps the shards this node leads (§7.16).
    Alarms,
    /// Plain `granary`: the same grain, no index registration, no driver.
    Plain,
}

/// Bring up a 3-node leader cluster under `one_in` frame loss, hosting the
/// `AlarmIndex` on every node in **both** arms, so the two differ by the timer's
/// wiring alone and not by how many Raft groups are running.
fn cluster(sim: &Simulation, wiring: Wiring, one_in: u64) -> Vec<Granary<Timer>> {
    let net = SimNetwork::new(sim).with_faults(lossy(one_in)).with_leader(
        swim(),
        raft(),
        DowningPolicy::Conservative,
    );
    let systems = [net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let indexes: Vec<Granary<AlarmIndex<SimNode>>> = systems
        .iter()
        .map(|s| s.granary::<AlarmIndex<SimNode>>(config()))
        .collect();
    let timers: Vec<Granary<Timer>> = systems
        .iter()
        .zip(&indexes)
        .map(|(s, idx)| match wiring {
            Wiring::Alarms => s.granary_with_alarms::<Timer>(config(), idx.clone()),
            Wiring::Plain => s.granary::<Timer>(config()),
        })
        .collect();
    sim.run_for(SETTLE);
    timers
}

/// Run one arm and return how many of its `OPS` arms committed.
fn commits(seed: u64, wiring: Wiring, one_in: u64) -> usize {
    let sim = Simulation::new(seed);
    let timers = cluster(&sim, wiring, one_in);
    let mut committed = 0;
    for op in 0..OPS {
        let key = format!("t/{op}");
        let ok = drive(&sim, CALL_DEADLINE + Duration::from_secs(2), {
            let g = timers[op % timers.len()].grain(key);
            async move {
                g.ask_timeout(Arm { after_ms: 30_000 }, CALL_DEADLINE)
                    .await
                    .is_ok()
            }
        });
        committed += usize::from(ok);
    }
    committed
}

// --- The differential ---------------------------------------------------------

#[test]
fn alarm_wiring_costs_no_commit_progress_under_frame_loss() {
    let mut dry = Vec::new();
    let mut summary = Vec::new();
    let mut plain_ever_committed = false;

    for one_in in LOSS_RATES {
        for seed in sweep_seeds(0..8) {
            let alarms = commits(seed, Wiring::Alarms, one_in);
            let plain = commits(seed, Wiring::Plain, one_in);
            plain_ever_committed |= plain > 0;
            summary.push(format!(
                "1-in-{one_in} seed {seed}: alarms {alarms}/{OPS}, plain {plain}/{OPS}"
            ));
            // Collected rather than asserted per seed, so a failing run reports
            // every configuration that shows it instead of stopping at the first.
            if alarms == 0 && plain > 0 {
                dry.push((one_in, seed));
            }
        }
    }
    let summary = summary.join("\n");

    assert!(
        dry.is_empty(),
        "an alarm-wired granary committed nothing under frame loss at \
         (loss, seed) {dry:?} while the plain arm committed — the finding recorded in \
         932ebca. Replay one with `commits(<seed>, Wiring::Alarms, <loss>)`.\n{summary}",
    );

    // The differential only means something if the plain arm committed at all;
    // if loss made *both* arms dry, the run measured an unusable network and says
    // nothing about alarm wiring.
    assert!(
        plain_ever_committed,
        "no seed committed anything even without alarm wiring, so this run compared \
         two failures rather than testing the property:\n{summary}",
    );
    println!("{summary}");
}
