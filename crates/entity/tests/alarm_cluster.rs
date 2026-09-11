//! Durable alarms across a **real node failover** on the clustered `Quorum` tier
//! (entity §16): an alarm fires with no caller present after the leader that armed
//! it has crashed.
//!
//! The Local-tier tests (`alarm_index.rs`) prove each mechanism on its own: the
//! index entity, the host registering as it arms, the driver re-activating an
//! indexed entity. This test composes them under an actual shard-leader crash and
//! observes the result through the *index* (a different entity, so reading it never
//! re-activates the timer): a fired alarm consumes itself and the host clears its
//! index entry, so the entry's disappearance means the entity fired callerlessly.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_core::NodeId;
use actor_simulation::SimNetwork;
use actor_simulation::SimNode;
use actor_simulation::Simulation;
use actor_simulation::raft_config;
use actor_simulation::scenario_sweep;
use actor_simulation::sweep_seeds;
use actor_simulation::swim_config;
use entity::Entities;
use entity::EntityConfig;
use entity::EntityExt;
use entity::EntityName;
use entity::cluster::AlarmIndex;
use entity::cluster::alarm_index::AllPending;
use entity::cluster::alarm_index::DueBefore;
use entity::cluster::alarm_index::index_key;
use entity::cluster::shard_for;

use support::timer::Arm;
use support::timer::ReadFired;
use support::timer::TIMER_TYPE;

/// The `Timer` at this suite's tier.
type Timer = support::timer::Timer<SimNode>;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

const SHARDS: usize = 2;

// --- Cluster harness (mirrors clustered_entities.rs) ----------------------------

fn config() -> EntityConfig {
    EntityConfig {
        shards: SHARDS,
        idle_after: Duration::from_secs(600),
        snapshot_every: 8,
        ..EntityConfig::default()
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

type Indexes = Vec<Entities<AlarmIndex<SimNode>>>;

/// Bring up a 3-node leader cluster hosting the shared `AlarmIndex` and the `Timer`
/// type wired to it (`entities_with_alarms`) on every node.
fn cluster(sim: &Simulation) -> (SimNetwork, Vec<Entities<Timer>>, Indexes) {
    let net = SimNetwork::new(sim).with_leader(
        swim_config(),
        raft_config(vec![A, B, C]),
        DowningPolicy::Conservative,
    );
    let systems = [net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let indexes: Vec<Entities<AlarmIndex<SimNode>>> = systems
        .iter()
        .map(|s| s.entities::<AlarmIndex<SimNode>>(config()))
        .collect();
    let timers: Vec<Entities<Timer>> = systems
        .iter()
        .zip(&indexes)
        .map(|(s, idx)| s.entities_with_alarms::<Timer>(config(), idx.clone()))
        .collect();
    sim.run_for(Duration::from_secs(3)); // elect each shard group's leader
    (net, timers, indexes)
}

/// The `AlarmIndex` entity holding `key`'s registration, on any node's handle.
fn index_for(
    indexes: &[Entities<AlarmIndex<SimNode>>],
    node: usize,
    timer_key: &str,
) -> entity::EntityRef<AlarmIndex<SimNode>> {
    let shard = shard_for(TIMER_TYPE, timer_key, SHARDS).index as usize;
    indexes[node].entity(index_key(TIMER_TYPE, shard))
}

// --- The test -----------------------------------------------------------------

#[test]
fn alarm_fires_callerlessly_after_a_leader_crash() {
    // Several seeds, with one generous post-crash window absorbing them all: which
    // node leads the shard, which survivor wins the re-election, and how the
    // deadline falls relative to the driver's sweep are all seed-dependent, and
    // the callerless re-activation has to hold for every combination.
    scenario_sweep("alarm-cluster/leader-crash", sweep_seeds(0..24), |seed| {
        run_failover(seed);
    });
}

fn run_failover(seed: u64) {
    let sim = Simulation::new(seed);
    let (net, timers, indexes) = cluster(&sim);
    let key = "t/7";
    let target = EntityName::new(TIMER_TYPE, key);

    // Arm a far-future alarm through the entity's leader. Arming registers the
    // deadline in the shard's index (a durable, quorum-replicated entity).
    drive(&sim, Duration::from_secs(5), {
        let g = timers[0].entity(key);
        async move { g.ask(Arm { after_ms: 30_000 }).await.expect("arm commits") }
    });

    // The registration is visible in the index, and the entity has not fired.
    let pending = drive(&sim, Duration::from_secs(3), {
        let idx = index_for(&indexes, 1, key);
        async move { idx.ask(AllPending).await.expect("index read") }
    });
    assert!(
        pending.iter().any(|(n, _)| *n == target),
        "arming registered the entity in its shard's index: {pending:?}",
    );

    // Crash the entity's shard leader before the deadline; a survivor re-elects.
    let leader = timers[0].leader(key).expect("the shard elected a leader");
    let survivor = [A, B, C]
        .iter()
        .position(|&n| n != leader)
        .expect("a survivor");
    net.crash(leader);

    // Advance well past re-election AND past the 30s deadline, with NO caller ever
    // touching the timer entity. Its only path back to life is the driver.
    sim.run_for(Duration::from_secs(45));

    // Proof, observed through the index (never re-activating the timer): the entry is
    // gone, which only a fire-and-consume clears, so the entity fired callerlessly on
    // the new leader.
    let after = drive(&sim, Duration::from_secs(3), {
        let idx = index_for(&indexes, survivor, key);
        async move {
            idx.ask(DueBefore { before: u64::MAX })
                .await
                .expect("index read")
        }
    });
    assert!(
        !after.contains(&target),
        "the alarm fired and cleared its index entry with no caller after the crash: still {after:?}",
    );

    // Corroboration: a read (the first caller since the crash) sees the durable fire.
    let fired = drive(&sim, Duration::from_secs(6), {
        let g = timers[survivor].entity(key);
        async move {
            g.ask_timeout(ReadFired, Duration::from_secs(5))
                .await
                .expect("read")
        }
    });
    assert_eq!(
        fired, 1,
        "the alarm fired exactly once, callerlessly, after failover"
    );
}

// --- Alarm reconciliation across a shard split (§7.7, §7.16) ------------------

#[test]
fn an_alarm_fires_after_a_split_moves_its_entity() {
    // §7.16 across §7.7: alarms armed on one shard's entities must still fire after
    // a split moves roughly half of them to a fresh child, with no caller touching
    // any timer. The child's driver picks a moved entity up from the child's index
    // (it re-registered there on the split's re-activation), and the parent's
    // driver must clear the stale entries it inherited rather than churn on them
    // forever. A recorder observes the committed `ShardSplit`, so a green run
    // cannot come from alarms firing trivially on shard 0.
    use actor_core::EventSink;
    use actor_simulation::Recorder;
    use entity::EntityEvent;

    let sim = Simulation::new(7);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let net = SimNetwork::new(&sim)
        .with_leader(
            swim_config(),
            raft_config(vec![A, B, C]),
            DowningPolicy::Conservative,
        )
        .with_events(sink);
    let systems = [net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2));
    let cfg = EntityConfig {
        shards: 1,
        idle_after: Duration::from_secs(600),
        snapshot_every: 8,
        ..EntityConfig::default()
    };
    let indexes: Vec<Entities<AlarmIndex<SimNode>>> = systems
        .iter()
        .map(|s| s.entities::<AlarmIndex<SimNode>>(cfg.clone()))
        .collect();
    let timers: Vec<Entities<Timer>> = systems
        .iter()
        .zip(&indexes)
        .map(|(s, idx)| s.entities_with_alarms::<Timer>(cfg.clone(), idx.clone()))
        .collect();
    sim.run_for(Duration::from_secs(3));

    let keys: Vec<String> = (0..16).map(|i| format!("t/{i}")).collect();

    // Arm a far-future alarm on each entity (registers it in shard 0's index).
    for key in &keys {
        drive(&sim, Duration::from_secs(5), {
            let g = timers[0].entity(key.clone());
            async move { g.ask(Arm { after_ms: 30_000 }).await.expect("arm commits") }
        });
    }

    // Split shard 0; wait for the committed `ShardSplit` on the stream.
    timers[0].split_shard(0);
    let split = |recorder: &Recorder| {
        recorder.events().iter().any(|e| {
            matches!(
                e.as_app::<EntityEvent>(),
                Some(EntityEvent::ShardSplit { .. })
            )
        })
    };
    let mut moved = false;
    for _ in 0..60 {
        sim.run_for(Duration::from_millis(500));
        if split(&recorder) {
            moved = true;
            break;
        }
    }
    assert!(
        moved,
        "the split committed and moved some entities off shard 0"
    );

    // Advance past the 30s deadline with no caller ever touching a timer: only
    // the drivers (parent and child) can re-activate them.
    sim.run_for(Duration::from_secs(45));

    // Every entity fired exactly once, callerlessly: whichever shard owns it now.
    for key in &keys {
        let fired = drive(&sim, Duration::from_secs(8), {
            let g = timers[1].entity(key.clone());
            async move {
                g.ask_timeout(ReadFired, Duration::from_secs(6))
                    .await
                    .expect("read")
            }
        });
        assert_eq!(
            fired, 1,
            "{key}: alarm fired once callerlessly across the split"
        );
    }
}
