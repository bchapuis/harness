//! Durable alarms across a **real node failover** on the clustered `Quorum` tier
//! (granary §16): the end-to-end proof that an alarm fires with no caller present
//! after the leader that armed it has crashed.
//!
//! The Local-tier tests (`alarm_index.rs`) prove each mechanism — the index grain,
//! the host registering as it arms, the driver re-activating an indexed grain. This
//! test composes them under an actual shard-leader crash: arm an alarm, crash the
//! grain's leader before the deadline, then advance time **without touching the
//! grain**. The new leader's driver must re-activate it from the index and let it
//! fire. The proof is observed through the *index* (a different grain, so reading it
//! never re-activates the timer): a fired alarm consumes itself and the host clears
//! its index entry, so the entry's disappearance means the grain fired callerlessly.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use granary::Alarm;
use granary::AlarmIndex;
use granary::AllPending;
use granary::DueBefore;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GrainName;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::index_key;
use granary::shard_for;
use serde::Deserialize;
use serde::Serialize;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

const TIMER_TYPE: &str = "test.Timer";
const SHARDS: usize = 2;

// --- An alarm-bearing grain on the clustered system ---------------------------

#[derive(Default)]
struct Timer;

#[derive(Default, Serialize, Deserialize)]
struct TimerState {
    fired: u64,
}

#[derive(Serialize, Deserialize)]
enum TimerEvent {
    Fired,
}

impl Grain for Timer {
    type System = SimCluster;
    type State = TimerState;
    type Event = TimerEvent;
    type Facets = (Alarm,);
    const GRAIN_TYPE: &'static str = "test.Timer";

    fn apply(state: &mut TimerState, event: &TimerEvent) {
        match event {
            TimerEvent::Fired => state.fired += 1,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Arm>();
        r.accept::<ReadFired>();
    }

    async fn on_alarm(&self, _s: &TimerState, _ctx: &GrainCtx<Self>) -> Vec<TimerEvent> {
        vec![TimerEvent::Fired]
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Arm {
    after_ms: u64,
}
impl Message for Arm {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.timer.Arm");
}
impl GrainHandler<Arm> for Timer {
    async fn handle(&self, _s: &TimerState, m: Arm, ctx: &GrainCtx<Self>) -> (Vec<TimerEvent>, ()) {
        ctx.alarm().set_after(Duration::from_millis(m.after_ms));
        (vec![], ())
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadFired;
impl Message for ReadFired {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("test.timer.ReadFired");
}
impl GrainHandler<ReadFired> for Timer {
    async fn handle(&self, s: &TimerState, _m: ReadFired, _ctx: &GrainCtx<Self>) -> (Vec<TimerEvent>, u64) {
        (vec![], s.fired)
    }
}

// --- Cluster harness (mirrors clustered_grains.rs) ----------------------------

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
    cell.lock().unwrap().take().expect("future did not complete")
}

type Indexes = Vec<Granary<AlarmIndex<SimCluster>>>;

/// Bring up a 3-node leader cluster hosting the shared `AlarmIndex` and the `Timer`
/// type wired to it (`granary_with_alarms`) on every node. Returns the network (for
/// the crash), the per-node timer handles, and the per-node index handles.
fn cluster(sim: &Simulation) -> (SimNetwork, Vec<Granary<Timer>>, Indexes) {
    let net = SimNetwork::new(sim).with_leader(swim(), raft(), DowningPolicy::Conservative);
    let systems = [net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let indexes: Vec<Granary<AlarmIndex<SimCluster>>> =
        systems.iter().map(|s| s.granary::<AlarmIndex<SimCluster>>(config())).collect();
    let timers: Vec<Granary<Timer>> = systems
        .iter()
        .zip(&indexes)
        .map(|(s, idx)| s.granary_with_alarms::<Timer>(config(), idx.clone()))
        .collect();
    sim.run_for(Duration::from_secs(3)); // elect each shard group's leader
    (net, timers, indexes)
}

/// The `AlarmIndex` grain holding `key`'s registration, on any node's handle.
fn index_for(
    indexes: &[Granary<AlarmIndex<SimCluster>>],
    node: usize,
    timer_key: &str,
) -> granary::GrainRef<AlarmIndex<SimCluster>> {
    let shard = shard_for(TIMER_TYPE, timer_key, SHARDS).index as usize;
    indexes[node].grain(index_key(TIMER_TYPE, shard))
}

// --- The test -----------------------------------------------------------------

#[test]
fn alarm_fires_callerlessly_after_a_leader_crash() {
    // Several seeds: different leader placement and failover timing, one generous
    // post-crash window absorbing them all.
    for seed in 0..4 {
        run_failover(seed);
    }
}

fn run_failover(seed: u64) {
    let sim = Simulation::new(seed);
    let (net, timers, indexes) = cluster(&sim);
    let key = "t/7";
    let target = GrainName::new(TIMER_TYPE, key);

    // Arm a far-future alarm through the grain's leader. Arming registers the
    // deadline in the shard's index (a durable, quorum-replicated grain).
    drive(&sim, Duration::from_secs(5), {
        let g = timers[0].grain(key);
        async move { g.ask(Arm { after_ms: 30_000 }).await.expect("arm commits") }
    });

    // The registration is visible in the index, and the grain has not fired.
    let pending = drive(&sim, Duration::from_secs(3), {
        let idx = index_for(&indexes, 1, key);
        async move { idx.ask(AllPending).await.expect("index read") }
    });
    assert!(
        pending.iter().any(|(n, _)| *n == target),
        "arming registered the grain in its shard's index: {pending:?}",
    );

    // Crash the grain's shard leader before the deadline; a survivor re-elects.
    let leader = timers[0].leader(key).expect("the shard elected a leader");
    let survivor = [A, B, C].iter().position(|&n| n != leader).expect("a survivor");
    net.crash(leader);

    // Advance well past re-election AND past the 30s deadline — with NO caller ever
    // touching the timer grain. Its only path back to life is the driver.
    sim.run_for(Duration::from_secs(45));

    // Proof, observed through the index (never re-activating the timer): the entry is
    // gone, which only a fire-and-consume clears — so the grain fired callerlessly on
    // the new leader.
    let after = drive(&sim, Duration::from_secs(3), {
        let idx = index_for(&indexes, survivor, key);
        async move { idx.ask(DueBefore { before: u64::MAX }).await.expect("index read") }
    });
    assert!(
        !after.contains(&target),
        "the alarm fired and cleared its index entry with no caller after the crash: still {after:?}",
    );

    // Corroboration: a read (the first caller since the crash) sees the durable fire.
    let fired = drive(&sim, Duration::from_secs(6), {
        let g = timers[survivor].grain(key);
        async move { g.ask_timeout(ReadFired, Duration::from_secs(5)).await.expect("read") }
    });
    assert_eq!(fired, 1, "the alarm fired exactly once, callerlessly, after failover");
}
