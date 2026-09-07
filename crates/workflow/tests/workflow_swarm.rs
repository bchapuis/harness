//! The workflow under the cluster fault swarm (workflow spec §9, §10).
//!
//! The scenario suites assert W1–W7 through scripted runs. This file applies them
//! the V&V way: a [`ClusterWorkload`] of workflow entities swept across seeds while
//! the nemesis injects partitions, crashes, restarts, pauses, heals, loss,
//! duplication, and delay, with the workflow's continuous checker (`workflow-cycle`)
//! and entity's commit monotonicity watching the whole stream.
//!
//! **What is scripted.** Every effect is a deterministic function of its key
//! (§10): a seeded latency, a failure on every fourth key, and a recovery policy
//! that alternates `Reexecute` and `Interrupt` — so the sweep drives both
//! orphaned resolutions whenever the nemesis kills a leader mid-effect.
//!
//! **What re-activates an entity.** Resumption is caller-driven: an entity whose
//! leader died stays cold until a message reaches it, so the workload polls every
//! entity's `Status` on a fixed cadence. Each poll is at once the touch that
//! re-activates a cold entity (whose first tick is the orphan set, §4.4), the
//! wake for an entity that hibernated at quiescence under the short idle window,
//! and an observation. A poll the faults refuse is simply not an observation.
//!
//! **What it cannot promise per seed.** Whether a leader dies with an effect in
//! flight is the nemesis's to decide, so the coverage claims — some entity finished,
//! some effect was relaunched across activations, every fault type fired — sit on
//! their own `coverage_seeds` sweep, which never narrows, rather than on the
//! invariant sweep (docs/simulation-testing.md, *Adding a sweep*).

mod support;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::Rehost;
use actor_simulation::SimNode;
use actor_simulation::coverage_seeds;
use actor_simulation::replay_cluster_swarm;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_cluster_swarm_coverage;
use actor_simulation::sweep_seeds;
use entity::EntityConfig;
use entity::testing::CommitMonotonic;
use support::Outcome;
use support::Seam;
use support::Status;
use support::TaskId;
use support::cluster;
use support::interrupting_task;
use support::read_script;
use support::request;
use support::seam;
use support::task;
use support::with_script;
use workflow::testing::workflow_invariants_under_failover;

const SHARDS: usize = 2;
/// How many workflow entities one run requests work of.
const ENTITIES: usize = 4;
/// How many effects each entity fans out.
const TASKS: TaskId = 6;
/// The hibernation window: short, so an entity that reaches quiescence hibernates
/// and the next poll re-activates it, exercising W7's round trip under faults.
const IDLE_AFTER: Duration = Duration::from_millis(150);
/// Per-call deadline. Short so a refused call fails fast and the run keeps moving.
const CALL_DEADLINE: Duration = Duration::from_secs(3);
/// How many poll rounds a run makes over every entity.
const ROUNDS: usize = 14;
/// The gap between rounds: past `IDLE_AFTER`, and inside the span of the slower
/// effects, so a crash mid-effect is re-driven by the next round.
const ROUND_GAP: Duration = Duration::from_millis(500);

/// The effect a key performs: a latency that grows with the key, a failure on
/// every fourth one. A function of the key alone, so one seed reproduces it.
fn outcome(key: TaskId) -> Outcome {
    let delay = Duration::from_millis(150 * u64::from(key + 1));
    if key % 4 == 3 {
        Outcome::Fail { delay }
    } else {
        Outcome::Answer { delay }
    }
}

/// The sweep-wide tallies that say what the sweep reached.
#[derive(Clone, Default)]
struct Reached {
    /// Entities observed `finished` by a poll, across the sweep.
    finished: Arc<AtomicUsize>,
    /// Keys whose effect ran on more than one activation, across the sweep — the
    /// orphaned chain (§4.4) the nemesis has to produce for W6 to be tested at all.
    relaunched: Arc<AtomicUsize>,
}

struct DriveSwarm {
    nodes: usize,
    script: Seam,
    reached: Reached,
}

fn config() -> EntityConfig {
    EntityConfig {
        shards: SHARDS,
        idle_after: IDLE_AFTER,
        snapshot_every: 4,
        ..EntityConfig::default()
    }
}

impl ClusterWorkload for DriveSwarm {
    fn name(&self) -> &'static str {
        "workflow-swarm"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn rehost(&self) -> Option<Rehost> {
        let script = self.script.clone();
        Some(Arc::new(move |node: &SimNode| {
            cluster::host_with(node, &script, config());
        }))
    }

    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::leader(self.swim(), self.nodes, DowningPolicy::Conservative)
    }

    fn setup(&self, _ctx: &ClusterCtx) {}

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let script = self.script.clone();
        let reached = self.reached.clone();
        Box::pin(async move {
            // One run's script and ledger, not the sweep's: emptied before any
            // entity can be touched, so the ledger is a function of the seed alone.
            with_script(&script, |s| {
                s.reset();
                for key in 0..TASKS {
                    s.set(key, outcome(key));
                }
            });
            let hosts: Vec<_> = nodes
                .iter()
                .map(|n| cluster::host_with(n, &script, config()))
                .collect();
            let clock = nodes[0].clock().clone();
            clock.sleep(Duration::from_secs(3)).await; // elections before traffic

            // Every request and every poll is issued from the first node's handle,
            // which the nemesis never restarts; routing is unaffected.
            let requests = (0..ENTITIES).map(|g| {
                let entity = hosts[0].entity(format!("job/{g}"));
                async move {
                    let tasks = (0..TASKS)
                        .map(|k| {
                            if k % 2 == 0 {
                                task(k)
                            } else {
                                interrupting_task(k)
                            }
                        })
                        .collect();
                    let _ = entity.ask_timeout(request(tasks), CALL_DEADLINE).await;
                }
            });
            futures::future::join_all(requests).await;

            // Poll rounds, all in flight at once and each opening at a fixed offset
            // from the last, so the cadence never depends on a refused call.
            let finished = Arc::new(AtomicUsize::new(0));
            let rounds = (0..ROUNDS).map(|round| {
                let last = round + 1 == ROUNDS;
                let front = hosts[0].clone();
                let finished = Arc::clone(&finished);
                let polls = (0..ENTITIES).map(move |g| {
                    let entity = front.entity(format!("job/{g}"));
                    let finished = Arc::clone(&finished);
                    async move {
                        if let Ok((_, true, _)) = entity.ask_timeout(Status, CALL_DEADLINE).await
                            && last
                        {
                            finished.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
                let clock = clock.clone();
                async move {
                    clock.sleep(ROUND_GAP * (round as u32 + 1)).await;
                    futures::future::join_all(polls).await;
                }
            });
            futures::future::join_all(rounds).await;

            reached
                .finished
                .fetch_add(finished.load(Ordering::Relaxed), Ordering::Relaxed);
            let relaunched = read_script(&script, |s| s.relaunched());
            reached.relaunched.fetch_add(relaunched, Ordering::Relaxed);
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        let mut invariants = workflow_invariants_under_failover();
        invariants.push(Box::new(CommitMonotonic::new(
            "workflow-commit-monotonic",
            "entity",
        )));
        invariants
    }
}

fn workload() -> DriveSwarm {
    DriveSwarm {
        nodes: 3,
        script: seam(),
        reached: Reached::default(),
    }
}

#[test]
fn the_drive_cycle_holds_under_the_cluster_swarm() {
    // The safety claims, and only them: W3, W5, and W7's checkable half through
    // `workflow-cycle`, and the entity's commit monotonicity beneath it.
    let workload = workload();
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..24)) {
        panic!("{failure}");
    }
    println!(
        "finished {} entity poll(s), relaunched {} key(s) across activations",
        workload.reached.finished.load(Ordering::Relaxed),
        workload.reached.relaunched.load(Ordering::Relaxed),
    );
}

/// What the sweep actually reached, on a range that never narrows: some entity
/// finished its work under faults (W2), some effect was relaunched by a
/// successor activation (the orphaned chain of W6, which only a leader death
/// mid-effect produces), and every transport fault type fired.
#[test]
fn the_workflow_swarm_reaches_a_finish_a_relaunch_and_every_fault() {
    let workload = workload();
    let stats = match run_cluster_swarm_coverage(&workload, coverage_seeds(0..24)) {
        Ok(stats) => stats,
        Err(failure) => panic!("{failure}"),
    };
    let finished = workload.reached.finished.load(Ordering::Relaxed);
    let relaunched = workload.reached.relaunched.load(Ordering::Relaxed);
    println!("finished {finished}, relaunched {relaunched}, faults {stats:?}");
    assert!(
        finished > 0,
        "no entity finished its work on any seed, so the sweep asserted the cycle \
         over runs that never completed"
    );
    assert!(
        relaunched > 0,
        "no effect was relaunched by a successor activation on any seed, so the \
         orphaned path (§4.4) was never exercised under faults"
    );
    assert!(
        stats.dropped > 0 && stats.duplicated > 0 && stats.delayed > 0 && stats.blocked > 0,
        "the sweep did not fire every fault type: {stats:?}",
    );
}

/// The determinism contract over this workload (actor §18.1): the same seed
/// twice, byte-identical event streams — workflow events included (§9).
#[test]
fn the_workflow_swarm_replays_identically() {
    if let Err(divergence) = replay_cluster_swarm(&workload(), sweep_seeds(0..8)) {
        panic!("{divergence}");
    }
}
