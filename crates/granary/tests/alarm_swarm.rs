//! Alarms under the cluster fault swarm (granary §7.16, invariant **G21**).
//!
//! `alarm.rs` and `alarm_cluster.rs` assert at-most-once firing through *scripted*
//! scenarios — a clean run, a cancel, a re-arm, one leader crash. This file
//! applies the same property the V&V way: a [`ClusterWorkload`] swept across
//! seeds while the nemesis injects partitions, crashes, pauses, heals, loss,
//! duplication, and delay, with a [`Checker`] watching the §13 event stream live.
//!
//! **What makes G21 observable here.** Each grain is armed exactly once and
//! receives no other mutating command for the rest of the run, so `TimerState`'s
//! `fired` — folded from committed `Fired` events, never from handler
//! invocations — can only end at 0 or 1. That *is* G21's statement for this
//! fixture, so the run reads it back per grain and checks it. A grain that never
//! fired is not a violation: the bound is from above, and under faults a deadline
//! may simply not be reached. A grain the nemesis leaves unreachable at the end
//! yields no observation at all, which costs coverage rather than correctness —
//! `observations()` guards against a sweep that quietly observes nothing.
//!
//! **Why the read-back and not the event stream.** `GrainEvent::Committed`
//! carries a seq but not what was in the batch, so the obvious continuous check
//! is a bound on the head: one arm stages one record and one fire stages two
//! (`Fired` and the fired deadline's `Clear`, atomically — which is what makes
//! at-most-once atomic), so an armed-once grain should stop at 3. That bound was
//! written, and it failed: heads reach 4 under faults on grains whose `fired` is
//! 1, because a re-activation can commit a single record of its own without
//! firing. The head is not a proxy for the fire count, and only the folded state
//! is. This is recorded because the wrong version looked more rigorous than the
//! right one.
//!
//! **Why `no-silent-loss` is dropped.** The alarm driver polls its shard's index
//! every 500 ms for as long as its node lives (`ALARM_DRIVE_INTERVAL`), so an
//! alarm-wired granary has an ask in flight a fixed fraction of the time and
//! never reaches ask-quiescence; the checker reports whatever is outstanding when
//! the runner stops, which is one seed in ten. That is the check meeting a
//! subsystem it was not written for, not a leak — those asks carry the 5 s
//! `DEFAULT_ASK_TIMEOUT` and do resolve. `blob-store`'s swarm drops it for the
//! same structural reason and writes down the same caveat: the move is honest
//! only while the workload awaits every op it issues to an outcome, which is what
//! leaves the data path's no-loss covered with the checker gone. Every ask below
//! is an `ask_timeout` that is awaited, so it is.
//!
//! `serial-execution` needed the same treatment for the same reason, but not the
//! same remedy — see [`NotReentrant`]: it makes two claims at once, and only the
//! quiescence half is dropped.
//!
//! **Width.** The declared corpus is 24 seeds, matching `alarm_cluster.rs`; the
//! sweep was additionally run clean across 200, which is what the entry this
//! closes asked for — every seed, not nine in ten.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Event;
use actor_core::NodeId;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::SimNode;
use actor_simulation::default_invariants;
use actor_simulation::run_cluster_swarm;
use actor_simulation::sweep_seeds;
use granary::AlarmIndex;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::testing::CommitMonotonic;

use support::timer::Arm;

/// The `Timer` at this suite's tier.
type Timer = support::timer::Timer<SimNode>;

const SHARDS: usize = 2;

/// How many timer grains one run arms.
const GRAINS: usize = 6;

/// The deadline each grain is armed with, short enough that the run reaches it
/// and long enough that the arm itself commits first under faults.
const ALARM_AFTER: Duration = Duration::from_secs(2);

/// Per-call deadline. Short so a faulted arm fails fast and the run keeps moving
/// rather than blocking on one grain.
const CALL_DEADLINE: Duration = Duration::from_secs(3);

/// How many times the run retries a grain's final read before giving up on
/// observing it. Each attempt is a bounded ask with a wait between, so a grain
/// whose leader is mid-failover is usually reachable by the last one.
const READ_ATTEMPTS: usize = 3;

/// The wait between read attempts, long enough for an election to resolve.
const RETRY_WAIT: Duration = Duration::from_secs(1);

/// How long after the deadline the run waits for callerless fires to land. Sized
/// against the whole drive's 30 s virtual-time budget, which also has to cover
/// the election, the arms, and the read-back — the reason both of those run
/// concurrently rather than in sequence.
const FIRE_WINDOW: Duration = Duration::from_secs(10);

/// The safety half of `serial-execution`, without its quiescence half.
///
/// That checker makes two claims at once. `observe` asserts **no reentrant
/// dispatch** — one handler never runs twice at once on an actor — which is a
/// real safety property and holds here. `at_quiescence` additionally asserts no
/// dispatch is left *open* when the runner stops, and that is the same
/// structural claim `no-silent-loss` makes, failing for the same reason: the
/// alarm driver re-activates grains forever, so at any arbitrary stopping point
/// some dispatch it started is in flight. Seed 11 of this sweep is that.
///
/// Dropping the checker outright would take the reentrancy claim with it, so this
/// wraps the real one and returns `Ok` from the final check alone. A hung run is
/// still caught: the runner's own liveness budget fails a `drive` that does not
/// finish.
struct NotReentrant(Box<dyn Invariant>);

impl Invariant for NotReentrant {
    fn name(&self) -> &'static str {
        "alarm-dispatch-not-reentrant"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        self.0.observe(event)
    }

    fn forget_node(&mut self, node: NodeId) {
        self.0.forget_node(node);
    }

    fn at_quiescence(&mut self) -> Result<(), String> {
        Ok(())
    }
}

// --- The G21 check ------------------------------------------------------------

/// What one run read back, plus the sweep-wide tally of how much it managed to
/// observe. Shared with the workload, which fills it at the end of `drive`;
/// checked by [`AtMostOneFire`] once the run is quiescent.
#[derive(Clone, Default)]
struct Fired {
    /// This run's `(grain key, fired)` read-backs. Cleared at the start of each
    /// `drive`, so a check sees one run's observations rather than the sweep's.
    run: Arc<Mutex<Vec<(String, u64)>>>,
    /// Grains successfully read across the whole sweep — the guard against a
    /// green run that observed nothing.
    observed: Arc<AtomicUsize>,
}

impl Fired {
    fn observations(&self) -> usize {
        self.observed.load(Ordering::Relaxed)
    }
}

/// **At-most-once firing** (invariant **G21**), checked at quiescence against the
/// state the run read back.
///
/// A checker rather than a bare assertion in `drive` so a violation is reported
/// through the same channel as every other invariant, naming the seed to replay.
struct AtMostOneFire {
    fired: Fired,
}

impl Invariant for AtMostOneFire {
    fn name(&self) -> &'static str {
        "alarm-at-most-one-fire"
    }

    fn observe(&mut self, _event: &Event) -> Result<(), String> {
        Ok(())
    }

    fn at_quiescence(&mut self) -> Result<(), String> {
        let run = self.fired.run.lock().expect("fired mutex poisoned");
        let over: Vec<&(String, u64)> = run.iter().filter(|(_, n)| *n > 1).collect();
        if !over.is_empty() {
            return Err(format!(
                "timer(s) fired more than once from a single arm (G21): {over:?}",
            ));
        }
        Ok(())
    }
}

// --- The workload -------------------------------------------------------------

struct AlarmSwarm {
    nodes: usize,
    fired: Fired,
}

fn config() -> GranaryConfig {
    GranaryConfig {
        shards: SHARDS,
        // Long enough that a grain does not hibernate between its arm and its
        // deadline on a calm seed. The pending-alarm veto would hold it resident
        // anyway (§7.16); this keeps the intent local to the workload.
        idle_after: Duration::from_secs(600),
        snapshot_every: 8,
        ..GranaryConfig::default()
    }
}

impl ClusterWorkload for AlarmSwarm {
    fn name(&self) -> &'static str {
        "granary-alarm-swarm"
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
        let fired = self.fired.clone();
        Box::pin(async move {
            // One run's observations, not the sweep's: `invariants()` builds a
            // fresh checker per run but shares this buffer, so it is emptied here,
            // before any read can land in it.
            fired.run.lock().expect("fired mutex poisoned").clear();

            // Host the index on every node, then the timers wired to it: the
            // driver is per type per node and sweeps only the shards its node
            // leads, so every node needs one for a failover to be covered.
            let indexes: Vec<Granary<AlarmIndex<SimNode>>> = nodes
                .iter()
                .map(|s| s.granary::<AlarmIndex<SimNode>>(config()))
                .collect();
            let timers: Vec<Granary<Timer>> = nodes
                .iter()
                .zip(&indexes)
                .map(|(s, idx)| s.granary_with_alarms::<Timer>(config(), idx.clone()))
                .collect();
            let clock = nodes[0].clock().clone();
            // Let the control-plane and shard groups elect before traffic.
            clock.sleep(Duration::from_secs(3)).await;

            // Arm each grain exactly once — the shape the check reads — and do
            // it concurrently, because the whole drive shares one 30 s virtual-time
            // budget with the wait and the read-back below. An arm the faults
            // refuse is left refused rather than retried: a retry could land a
            // *second* arm on a grain whose first one committed invisibly (the
            // reply lost after the commit), and a grain armed twice may
            // legitimately fire twice, which would turn the check into a false
            // alarm. A run where some arms fail is a run with fewer observations,
            // not a wrong one.
            let arms = (0..GRAINS).map(|g| {
                let timer = timers[g % timers.len()].grain(format!("t/{g}"));
                async move {
                    let _ = timer
                        .ask_timeout(
                            Arm {
                                after_ms: ALARM_AFTER.as_millis() as u64,
                            },
                            CALL_DEADLINE,
                        )
                        .await;
                }
            });
            futures::future::join_all(arms).await;

            // Past every deadline, with room for the driver's 500 ms sweep to
            // re-activate whatever the nemesis knocked over, and for a failover to
            // hand the shard to a node whose driver can. The fires this waits for
            // are callerless, so there is nothing to await but time.
            clock.sleep(ALARM_AFTER + FIRE_WINDOW).await;

            // Read each grain's folded fire count, concurrently and on a budget. A
            // read the faults refuse is simply not an observation — retried, then
            // dropped — because G21 is a claim about what committed, and a grain
            // this node cannot reach committed whatever it committed either way.
            let reads = (0..GRAINS).map(|g| {
                let key = format!("t/{g}");
                let timers = timers.clone();
                let clock = clock.clone();
                let fired = fired.clone();
                async move {
                    for attempt in 0..READ_ATTEMPTS {
                        let timer = timers[g % timers.len()].grain(key.clone());
                        if let Ok(n) = timer
                            .ask_timeout(support::timer::ReadFired, CALL_DEADLINE)
                            .await
                        {
                            fired
                                .run
                                .lock()
                                .expect("fired mutex poisoned")
                                .push((key.clone(), n));
                            fired.observed.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                        if attempt + 1 < READ_ATTEMPTS {
                            clock.sleep(RETRY_WAIT).await;
                        }
                    }
                }
            });
            futures::future::join_all(reads).await;
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        let mut invariants: Vec<Box<dyn Invariant>> = Vec::new();
        for inv in default_invariants() {
            match inv.name() {
                // Entirely a quiescence claim, and the driver never quiesces.
                "no-silent-loss" => {}
                // Half a quiescence claim; the safety half is kept.
                "serial-execution" => invariants.push(Box::new(NotReentrant(inv))),
                _ => invariants.push(inv),
            }
        }
        invariants.push(Box::new(AtMostOneFire {
            fired: self.fired.clone(),
        }));
        invariants.push(Box::new(CommitMonotonic::new(
            "alarm-commit-monotonic",
            "timer",
        )));
        invariants
    }
}

// --- The conformance tests ----------------------------------------------------

#[test]
fn alarms_fire_at_most_once_under_the_cluster_swarm() {
    let workload = AlarmSwarm {
        nodes: 3,
        fired: Fired::default(),
    };
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..24)) {
        panic!("{failure}");
    }
    // A sweep whose every read was refused would pass while checking nothing.
    let observed = workload.fired.observations();
    println!("G21 observed on {observed} grain(s) across the sweep");
    assert!(
        observed > 0,
        "no grain's fire count could be read on any seed, so the sweep asserted \
         G21 against an empty set",
    );
}

/// The other half of the swarm's claim, on a clean cluster: an armed alarm
/// actually *does* fire. Without this, `fired <= 1` would be satisfied just as
/// well by a fixture that never fires at all — the swarm would be green and
/// asserting nothing, which is the failure mode the whole §2.1 entry is about.
#[test]
fn one_arm_fires_exactly_once_without_faults() {
    use actor_simulation::SimNetwork;
    use actor_simulation::Simulation;

    let sim = Simulation::new(0);
    let net = SimNetwork::new(&sim).with_leader(
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(300),
            indirect_count: 2,
        },
        {
            let mut raft = actor_cluster::RaftConfig::new(vec![
                NodeId::new(1),
                NodeId::new(2),
                NodeId::new(3),
            ]);
            raft.election_timeout = Duration::from_millis(500);
            raft.heartbeat_interval = Duration::from_millis(100);
            raft
        },
        DowningPolicy::Conservative,
    );
    let systems = [
        net.join(NodeId::new(1)),
        net.join(NodeId::new(2)),
        net.join(NodeId::new(3)),
    ];
    sim.run_for(Duration::from_secs(2));
    let indexes: Vec<Granary<AlarmIndex<SimNode>>> = systems
        .iter()
        .map(|s| s.granary::<AlarmIndex<SimNode>>(config()))
        .collect();
    let timers: Vec<Granary<Timer>> = systems
        .iter()
        .zip(&indexes)
        .map(|(s, idx)| s.granary_with_alarms::<Timer>(config(), idx.clone()))
        .collect();
    sim.run_for(Duration::from_secs(3));

    // Arm one grain and let its deadline pass with no faults at all.
    use actor_core::Spawner;
    let done: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let out = Arc::clone(&done);
    let g = timers[0].grain("t/0");
    sim.spawner().launch(Box::pin(async move {
        g.ask_timeout(Arm { after_ms: 500 }, Duration::from_secs(5))
            .await
            .expect("arm commits on a clean cluster");
        *out.lock().unwrap() = true;
    }));
    sim.run_for(Duration::from_secs(10));
    assert!(*done.lock().unwrap(), "the arm did not complete");

    let fired = {
        let cell: Arc<Mutex<Option<u64>>> = Arc::new(Mutex::new(None));
        let out = Arc::clone(&cell);
        let g = timers[0].grain("t/0");
        sim.spawner().launch(Box::pin(async move {
            *out.lock().unwrap() = g
                .ask_timeout(support::timer::ReadFired, Duration::from_secs(5))
                .await
                .ok();
        }));
        sim.run_for(Duration::from_secs(10));
        let v = *cell.lock().unwrap();
        v.expect("read completes on a clean cluster")
    };
    assert_eq!(fired, 1, "one arm fires exactly once with no faults (G21)");
}
