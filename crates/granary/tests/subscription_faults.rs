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

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GrainRef;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::GranarySystem;
use granary::Seq;
use granary::Subscription;
use serde::Deserialize;
use serde::Serialize;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

/// How long a caught-up collector waits for a live record before re-checking the
/// journal — the liveness net that detects a silent leader move.
const RESYNC: Duration = Duration::from_millis(400);

// --- A grain whose records are an appendable log, readable by seq -------------

#[derive(Default)]
struct LogGrain;

#[derive(Default, Serialize, Deserialize)]
struct Log {
    events: Vec<i64>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Val(i64);

impl Grain for LogGrain {
    type System = SimCluster;
    type State = Log;
    type Event = Val;
    type Facets = ();
    const GRAIN_TYPE: &'static str = "test.Log";

    fn apply(state: &mut Log, event: &Val) {
        state.events.push(event.0);
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Append>();
        r.accept::<ReadFrom>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Append(i64);
impl Message for Append {
    type Reply = u64; // the seq the appended record occupies
    const MANIFEST: Manifest = Manifest::new("test.Append");
}
impl GrainHandler<Append> for LogGrain {
    async fn handle(&self, state: &Log, msg: Append, _: &GrainCtx<Self>) -> (Vec<Val>, u64) {
        (vec![Val(msg.0)], state.events.len() as u64 + 1)
    }
}

/// The backfill read (the §7.3 `load` a subscriber reconciles against): records
/// after `from`, as `(seq, value)`.
#[derive(Clone, Serialize, Deserialize)]
struct ReadFrom {
    from: u64,
}
impl Message for ReadFrom {
    type Reply = Vec<(u64, i64)>;
    const MANIFEST: Manifest = Manifest::new("test.ReadFrom");
}
impl GrainHandler<ReadFrom> for LogGrain {
    async fn handle(
        &self,
        state: &Log,
        msg: ReadFrom,
        _: &GrainCtx<Self>,
    ) -> (Vec<Val>, Vec<(u64, i64)>) {
        let recs = (msg.from as usize..state.events.len())
            .map(|i| (i as u64 + 1, state.events[i]))
            .collect();
        (vec![], recs)
    }
}

// --- The reference reconciler (the contract `harness::Follower` implements) ----

/// Collect the grain's records up to `target` by reconciling a subscription with
/// journal backfill: subscribe, backfill the gap, take live batches, and on a
/// silent move re-check the journal after `RESYNC`. The returned values are the
/// reconstructed committed sequence.
async fn collect(system: SimCluster, grain: GrainRef<LogGrain>, target: usize) -> Vec<i64> {
    let mut last: u64 = 0;
    let mut out: Vec<i64> = Vec::new();
    let mut sub: Option<Subscription<LogGrain>> = None;
    while out.len() < target {
        if sub.is_none() {
            match grain.subscribe(Seq::new(last)).await {
                Ok(s) => sub = Some(s),
                Err(_) => {
                    system.sleep(RESYNC).await; // shard still electing; retry
                    continue;
                }
            }
        }
        // Backfill from the journal until caught up to the head.
        match grain.ask(ReadFrom { from: last }).await {
            Ok(recs) if !recs.is_empty() => {
                for (seq, v) in recs {
                    if seq > last {
                        last = seq;
                        out.push(v);
                    }
                }
                continue;
            }
            Ok(_) => {} // at the head
            Err(_) => {
                sub = None; // leader moved; re-subscribe + backfill
                system.sleep(RESYNC).await;
                continue;
            }
        }
        // Caught up: race a live batch against the re-sync timer.
        let rx = sub.as_ref().expect("subscribed").records.clone();
        let recv = rx.recv();
        let resync = system.sleep(RESYNC);
        futures::pin_mut!(recv);
        match futures::future::select(recv, resync).await {
            futures::future::Either::Left((Ok(stream), _)) => {
                if stream.from.value() <= last {
                    for (seq, v) in stream.records {
                        if seq.value() > last {
                            last = seq.value();
                            out.push(v.0);
                        }
                    }
                }
            }
            // Stream closed: re-subscribe.
            futures::future::Either::Left((Err(_), _)) => sub = None,
            // Timer won: the backfill at the loop top recovers any post-move
            // records the dead push path never delivered.
            futures::future::Either::Right(_) => {}
        }
    }
    out
}

/// Append `val`, retrying through a failover (`NotLeader`/`Unavailable`) until it
/// commits — the writer's at-least-once discipline across an election.
async fn append_retry(system: &SimCluster, grain: &GrainRef<LogGrain>, val: i64) {
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

fn cluster(sim: &Simulation) -> (SimNetwork, Vec<SimCluster>, Vec<Granary<LogGrain>>) {
    let net = leader_net(sim);
    let systems = vec![net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2));
    let granaries: Vec<Granary<LogGrain>> = systems
        .iter()
        .map(|system| system.granary::<LogGrain>(config()))
        .collect();
    sim.run_for(Duration::from_secs(3));
    (net, systems, granaries)
}

fn surviving_caller(
    sim: &Simulation,
    systems: &[SimCluster],
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
