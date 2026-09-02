//! Record subscriptions under faults, on the clustered `Quorum` journal
//! (granary §7.9, invariant **G16**), under deterministic simulation (§14).
//!
//! A subscriber reconciles by `Seq`: it rides a grain record subscription and
//! backfills from the journal on any gap or after the stream goes dead. The
//! property under test is that the reconstructed sequence equals the committed
//! one — contiguous, in order, no gap or duplicate — regardless of buffer
//! overflow (a burst writer) or a shard-leader crash mid-stream (push stops; the
//! re-sync backfill recovers every post-move record). The collector below is the
//! reference reconciler; `harness::Follower` implements the same contract.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::ActorSystem;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::SimNetwork;
use actor_simulation::SimNode;
use actor_simulation::Simulation;
use granary::GrainRef;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::Seq;

use support::log::Append;
use support::log::LogGrain;
use support::log::RESYNC;
use support::log::collect;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

/// Append `val`, retrying through a failover (`NotLeader`/`Unavailable`) until it
/// commits — the writer's at-least-once discipline across an election.
async fn append_retry(system: &SimNode, grain: &GrainRef<LogGrain>, val: i64) {
    loop {
        match grain.ask(Append(val)).await {
            Ok(_) => return,
            Err(_) => system.sleep(RESYNC).await,
        }
    }
}

// --- Cluster harness (mirrors clustered_grains.rs) ----------------------------

fn raft() -> RaftConfig {
    let mut config = RaftConfig::new(vec![A, B, C]);
    config.election_timeout = Duration::from_millis(500);
    config
}

fn leader_net(sim: &Simulation) -> SimNetwork {
    SimNetwork::new(sim).with_leader(SwimConfig::default(), raft(), DowningPolicy::Conservative)
}

fn config() -> GranaryConfig {
    GranaryConfig {
        shards: 2,
        idle_after: Duration::from_secs(60),
        snapshot_every: 8,
        ..GranaryConfig::default()
    }
}

fn drive<T: Send + 'static>(
    sim: &Simulation,
    settle: Duration,
    future: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
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

fn cluster(sim: &Simulation) -> (SimNetwork, Vec<SimNode>, Vec<Granary<LogGrain>>) {
    cluster_with(sim, config())
}

fn cluster_with(
    sim: &Simulation,
    config: GranaryConfig,
) -> (SimNetwork, Vec<SimNode>, Vec<Granary<LogGrain>>) {
    let net = leader_net(sim);
    let systems = vec![net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2));
    let granaries: Vec<Granary<LogGrain>> = systems
        .iter()
        .map(|system| system.granary::<LogGrain>(config.clone()))
        .collect();
    sim.run_for(Duration::from_secs(3));
    (net, systems, granaries)
}

fn surviving_caller(
    sim: &Simulation,
    systems: &[SimNode],
    granaries: &[Granary<LogGrain>],
    key: &str,
) -> usize {
    // Poll: the shard's first election lands at a schedule-dependent instant, so
    // wait it out rather than assuming a fixed settle covered it.
    let leader = {
        let mut found = None;
        for _ in 0..20 {
            if let Some(leader) = granaries[0].leader(key) {
                found = Some(leader);
                break;
            }
            sim.run_for(Duration::from_millis(500));
        }
        found.expect("the shard elected a leader")
    };
    systems
        .iter()
        .position(|s| s.node() != leader)
        .expect("a non-leader node hosts the client")
}

// --- Tests --------------------------------------------------------------------

#[test]
fn subscription_reconstructs_the_log_with_no_faults() {
    let sim = Simulation::new(1);
    let (_net, systems, granaries) = cluster(&sim);
    let key = "log/clean";
    let caller = surviving_caller(&sim, &systems, &granaries, key);
    let system = systems[caller].clone();
    let granary = granaries[caller].clone();
    const N: usize = 16;

    let out = drive(&sim, Duration::from_secs(20), async move {
        let grain = granary.grain(key);
        let writer = {
            let system = system.clone();
            let grain = grain.clone();
            async move {
                for i in 0..N as i64 {
                    append_retry(&system, &grain, i).await;
                }
            }
        };
        let collector = collect(system, grain, N);
        let (_, out) = futures::future::join(writer, collector).await;
        out
    });

    assert_eq!(
        out,
        (0..N as i64).collect::<Vec<_>>(),
        "pushed stream reconstructs the log (G16)"
    );
}

#[test]
fn subscription_survives_a_leader_crash_mid_stream() {
    let sim = Simulation::new(7);
    let (net, systems, granaries) = cluster(&sim);
    let key = "log/crash";
    let leader = granaries[0]
        .leader(key)
        .expect("the shard elected a leader");
    let caller = surviving_caller(&sim, &systems, &granaries, key);
    let system = systems[caller].clone();
    let granary = granaries[caller].clone();
    const N: usize = 16;

    let out = drive(&sim, Duration::from_secs(40), async move {
        let grain = granary.grain(key);
        let writer = {
            let system = system.clone();
            let grain = grain.clone();
            async move {
                for i in 0..N as i64 {
                    // Crash the grain's shard leader halfway through; the writer
                    // and collector both re-route to the new leader.
                    if i as usize == N / 2 {
                        net.crash(leader);
                    }
                    append_retry(&system, &grain, i).await;
                }
            }
        };
        let collector = collect(system, grain, N);
        let (_, out) = futures::future::join(writer, collector).await;
        out
    });

    assert_eq!(
        out,
        (0..N as i64).collect::<Vec<_>>(),
        "every record is reconstructed across the leader crash (G16)"
    );
}

#[test]
fn hibernation_closes_the_subscribers_stream() {
    // §7.9: when the host drops its sink set — here by hibernating (§10) — a
    // dropped sink *observes its stream close* and re-subscribes. The close has to
    // be told to the sink, which lives on the subscriber's node: the host merely
    // dropping its end would leave the stream open and silent, and a subscriber
    // waiting on it would wait forever. The caller is a non-leader node, so the
    // close crosses the wire as it does in production.
    let sim = Simulation::new(5);
    let idle_after = Duration::from_secs(2);
    let (_net, systems, granaries) = cluster_with(
        &sim,
        GranaryConfig {
            idle_after,
            ..config()
        },
    );
    let key = "log/hibernate";
    let caller = surviving_caller(&sim, &systems, &granaries, key);
    let system = systems[caller].clone();
    let granary = granaries[caller].clone();

    #[derive(Debug, PartialEq)]
    enum Stream {
        Closed,
        Silent,
    }

    let outcome = drive(&sim, Duration::from_secs(30), async move {
        let grain = granary.grain(key);
        append_retry(&system, &grain, 1).await;
        let sub = grain
            .subscribe(Seq::new(1))
            .await
            .expect("subscribed to a resident grain");
        // Nothing more is written, so the grain idles past `idle_after` and
        // hibernates. Race the stream against a wait several times that long: a
        // close is the expected outcome, and the timer winning is the hang.
        let timeout = system.sleep(idle_after * 5);
        futures::pin_mut!(timeout);
        loop {
            let recv = sub.records.recv();
            futures::pin_mut!(recv);
            match futures::future::select(recv, timeout.as_mut()).await {
                futures::future::Either::Left((Ok(_), _)) => continue, // a stray batch
                futures::future::Either::Left((Err(_), _)) => return Stream::Closed,
                futures::future::Either::Right(_) => return Stream::Silent,
            }
        }
    });

    assert_eq!(
        outcome,
        Stream::Closed,
        "the subscriber's stream must end when the host hibernates (§7.9), \
         not go silent"
    );
}

#[test]
fn subscription_reconstructs_a_burst_that_overflows_the_buffer() {
    // A burst far exceeding the delivery buffer (SUB_BUFFER = 128) forces drops;
    // the collector backfills the gaps, so the reconstruction is still exact.
    let sim = Simulation::new(3);
    let (_net, systems, granaries) = cluster(&sim);
    let key = "log/burst";
    let caller = surviving_caller(&sim, &systems, &granaries, key);
    let system = systems[caller].clone();
    let granary = granaries[caller].clone();
    const N: usize = 400;

    let out = drive(&sim, Duration::from_secs(40), async move {
        let grain = granary.grain(key);
        let writer = {
            let system = system.clone();
            let grain = grain.clone();
            async move {
                for i in 0..N as i64 {
                    append_retry(&system, &grain, i).await;
                }
            }
        };
        let collector = collect(system, grain, N);
        let (_, out) = futures::future::join(writer, collector).await;
        out
    });

    assert_eq!(
        out.len(),
        N,
        "every committed record is reconstructed despite drops (G16)"
    );
    assert_eq!(
        out,
        (0..N as i64).collect::<Vec<_>>(),
        "in order, no gap or duplicate"
    );
}
