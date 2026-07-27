//! Record subscriptions under the cluster fault swarm (granary §7.9, §14,
//! invariant **G16**).
//!
//! `tests/subscription_faults.rs` scripts the §14 subscription cases one at a
//! time — a leader crash mid-stream, a burst that overflows the sink's buffer —
//! each as a narrative you can read. This file makes the same claim the other
//! way round: sweep seeds while the nemesis partitions, freezes, crashes, and
//! restarts nodes underneath a live subscription, and check G16 on whatever
//! history each seed produced.
//!
//! The spec (§14) requires fault injection to produce four cases. Here they are
//! not scripted but *induced*, and the workload is arranged so each is reachable:
//!
//! - **A leader move mid-stream.** The nemesis partitions and crashes; a shard
//!   whose leader is cut off elects another, and push stops without notice. The
//!   collector's re-sync timer is what notices.
//! - **A slow sink whose buffer overflows.** Writers burst without pacing while
//!   the collector is off doing a journal read, so the bounded sink channel fills
//!   and the host drops the subscription. Recovery is a re-subscribe plus
//!   backfill, not a retry of the lost batch.
//! - **Hibernation and reactivation under a live subscription.** `idle_after` is
//!   short and writers pause past it, so the grain passivates with a subscriber
//!   attached and the next append re-activates it.
//! - **A timed-out append that commits late.** Every append carries a deadline
//!   shorter than a faulted quorum round, so some return `Unavailable` and land
//!   afterwards — delivered or backfilled once, at their slot.
//!
//! The claim is the spec's own wording: the sink's seq-reconciled sequence equals
//! what `load` to the head returns. It is checked against the journal at the end
//! of every seed, so a run where faults happened to be mild still proves
//! something, and one where they were savage proves the same thing.

mod support;

use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::Rehost;
use actor_simulation::SimNode;
use actor_simulation::coverage_seeds;
use actor_simulation::default_invariants;
use actor_simulation::replay_cluster_swarm;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_cluster_swarm_coverage;
use actor_simulation::sweep_seeds;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::testing::CommitMonotonic;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use support::log::Append;
use support::log::LogGrain;
use support::log::ReadFrom;
use support::log::collect_until;

/// Short enough that the pauses below cross it, so the grain passivates with a
/// subscriber attached (§14's hibernation case).
const IDLE_AFTER: Duration = Duration::from_millis(200);
/// A writer's pause between bursts — past [`IDLE_AFTER`] on purpose.
const IDLE_FOR: Duration = Duration::from_millis(400);
/// Shorter than a faulted quorum round, so some appends time out and commit
/// afterwards (§14's late-commit case).
const APPEND_DEADLINE: Duration = Duration::from_millis(600);
/// How long the collector keeps reconciling after the writers stop. Long enough
/// for a post-move backfill to run; the run's own settle window covers the rest.
const DRAIN: Duration = Duration::from_secs(6);

fn config() -> GranaryConfig {
    GranaryConfig {
        shards: 2,
        replication_factor: 3,
        idle_after: IDLE_AFTER,
        snapshot_every: 4,
        ..GranaryConfig::default()
    }
}

/// Writers append to one log grain while a subscriber reconciles it, all under
/// the nemesis. The subscriber is judged against the journal, never against what
/// the writers believe they wrote — an append that returned `Unavailable` may or
/// may not have committed, and only the grain knows which.
struct SubscriptionSwarm {
    nodes: usize,
    writers: usize,
    ops: u64,
    /// What the sweep actually compared, accumulated across seeds. Two escape
    /// hatches below would otherwise let a green run mean nothing: a seed whose
    /// shard never became readable returns without comparing, and a seed whose
    /// sink reconstructed nothing compares two empty slices. Neither is wrong on
    /// its own — but a sweep where *every* seed took one of them has asserted
    /// G16 against no evidence.
    compared: Arc<AtomicUsize>,
    records: Arc<AtomicUsize>,
}

impl ClusterWorkload for SubscriptionSwarm {
    fn name(&self) -> &'static str {
        "granary-subscription-swarm"
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
        ClusterModeSpec::Leader {
            swim: self.swim(),
            voters: self.nodes,
            election_timeout: Duration::from_millis(500),
            heartbeat_interval: Duration::from_millis(100),
            downing: DowningPolicy::Conservative,
        }
    }

    fn rehost(&self) -> Option<Rehost> {
        // Process death is the sharpest way to move a shard's leader mid-stream,
        // which is the first case §14 asks for; a re-hosted successor is what
        // keeps the shard servable afterwards.
        Some(Arc::new(|node: &SimNode| {
            node.granary::<LogGrain>(config());
        }))
    }

    fn setup(&self, _ctx: &ClusterCtx) {}

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let writers = self.writers;
        let ops = self.ops;
        let compared = Arc::clone(&self.compared);
        let records = Arc::clone(&self.records);
        Box::pin(async move {
            let granaries: Vec<_> = nodes
                .iter()
                .map(|s| s.granary::<LogGrain>(config()))
                .collect();
            let clock = nodes[0].clock().clone();
            let entropy = nodes[0].entropy().clone();
            clock.sleep(Duration::from_secs(3)).await;

            // Everything goes through node 1's gateway: the nemesis may restart
            // any other node, and a handle for a restarted one points at a system
            // that has been shut down. Location transparency (G13) keeps the
            // coverage — calls still land on whichever node leads the shard.
            let granary = granaries[0].clone();
            let grain = granary.grain("log/subscribed");

            // The subscriber, running for the whole drive. It stops on a deadline
            // rather than a record count: under faults, how many records will ever
            // commit is not knowable up front.
            let deadline = clock.now() + DRAIN;
            let collector = {
                let node = nodes[0].clone();
                let grain = grain.clone();
                let clock = clock.clone();
                async move { collect_until(node, grain, move |_| clock.now() >= deadline).await }
            };

            let writes = {
                let grain = grain.clone();
                let clock = clock.clone();
                let entropy = entropy.clone();
                async move {
                    let mut tasks = Vec::new();
                    for w in 0..writers {
                        let grain = grain.clone();
                        let clock = clock.clone();
                        let entropy = entropy.clone();
                        tasks.push(async move {
                            for op in 0..ops {
                                // Burst without pacing: several appends land while
                                // the collector is away on a journal read, which is
                                // what overflows the bounded sink channel.
                                let value = (w as i64 + 1) * 1000 + op as i64;
                                let _ = grain.ask_timeout(Append(value), APPEND_DEADLINE).await;
                                // Then idle past `idle_after`, so the grain
                                // passivates under the live subscription.
                                if op % 2 == 1 {
                                    clock.sleep(IDLE_FOR).await;
                                }
                                // A little seeded jitter so writers interleave
                                // differently per seed.
                                if entropy.next_u64().is_multiple_of(3) {
                                    clock.sleep(Duration::from_millis(50)).await;
                                }
                            }
                        });
                    }
                    futures::future::join_all(tasks).await;
                }
            };

            let (reconstructed, ()) = futures::future::join(collector, writes).await;

            // G16, in the spec's own words: what the sink reconstructed equals
            // what `load` to the head returns. Read the journal until it answers —
            // a read that fails mid-election says nothing about the subscription.
            let mut journal = None;
            for _ in 0..40 {
                match grain
                    .ask_timeout(ReadFrom { from: 0 }, Duration::from_secs(2))
                    .await
                {
                    Ok(recs) => {
                        journal = Some(recs);
                        break;
                    }
                    Err(_) => clock.sleep(Duration::from_millis(250)).await,
                }
            }
            let Some(journal) = journal else {
                // The shard never became readable again in the window. That is a
                // liveness observation about the cluster, not a G16 counterexample,
                // and the safety core is still checked over the run.
                return;
            };
            compared.fetch_add(1, Ordering::Relaxed);
            records.fetch_add(reconstructed.len(), Ordering::Relaxed);

            let committed: Vec<i64> = journal.iter().map(|(_, v)| *v).collect();
            let seqs: Vec<u64> = journal.iter().map(|(seq, _)| *seq).collect();
            let contiguous: Vec<u64> = (1..=journal.len() as u64).collect();
            assert_eq!(
                seqs, contiguous,
                "the journal itself is not a contiguous seq run — G3/G5, before \
                 G16 gets a say",
            );

            // The sink may legitimately trail the head: it stopped on a deadline
            // while writes were still landing. What it did reconstruct must be an
            // exact prefix — every record it produced is the committed record at
            // that slot, in order, with no gap or duplicate (§7.9, G16).
            assert!(
                reconstructed.len() <= committed.len(),
                "the sink reconstructed {} records but only {} ever committed — \
                 a duplicate or an invention (G16)",
                reconstructed.len(),
                committed.len(),
            );
            assert_eq!(
                reconstructed,
                committed[..reconstructed.len()],
                "the sink's reconciled sequence is not the committed prefix \
                 `load` returns (G16)",
            );
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        let mut invariants = default_invariants();
        invariants.push(Box::new(CommitMonotonic::new(
            "subscription-commit-monotonic",
            "grain",
        )));
        invariants
    }
}

impl SubscriptionSwarm {
    fn new(nodes: usize, writers: usize, ops: u64) -> SubscriptionSwarm {
        SubscriptionSwarm {
            nodes,
            writers,
            ops,
            compared: Arc::new(AtomicUsize::new(0)),
            records: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[test]
fn a_subscription_reconstructs_the_committed_prefix_under_the_swarm() {
    let workload = SubscriptionSwarm::new(3, 2, 6);
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..24)) {
        panic!("{failure}");
    }
    assert!(
        workload.compared.load(Ordering::Relaxed) > 0,
        "no seed ever got a readable journal to compare against — G16 was \
         asserted against nothing",
    );
    assert!(
        workload.records.load(Ordering::Relaxed) > 0,
        "the sink reconstructed no records on any seed — the comparison held \
         two empty sequences and proved nothing",
    );
}

#[test]
fn the_subscription_swarm_is_reproducible() {
    // Delivery, backfill, re-subscription, and the passivation underneath them
    // all ride the Clock/Entropy/Spawner seams, so a seed replays byte-identically.
    let workload = SubscriptionSwarm::new(3, 2, 4);
    if let Err(divergence) = replay_cluster_swarm(&workload, sweep_seeds(0..12)) {
        panic!("{divergence}");
    }
}

#[test]
fn the_subscription_swarm_actually_fires_each_fault_type() {
    let workload = SubscriptionSwarm::new(3, 2, 6);
    let stats = match run_cluster_swarm_coverage(&workload, coverage_seeds(0..16)) {
        Ok(stats) => stats,
        Err(failure) => panic!("{failure}"),
    };
    assert!(
        stats.dropped > 0,
        "the sweep never dropped a frame (loss uncovered): {stats:?}"
    );
    assert!(
        stats.duplicated > 0,
        "the sweep never duplicated a frame — a re-delivered batch is the case \
         seq reconciliation exists for: {stats:?}"
    );
    assert!(
        stats.delayed > 0,
        "the sweep never delayed a frame (reordering uncovered): {stats:?}"
    );
    assert!(
        stats.blocked > 0,
        "the sweep never blocked a frame (partition/crash uncovered): {stats:?}"
    );
}
