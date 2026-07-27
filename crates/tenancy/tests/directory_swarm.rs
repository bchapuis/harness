//! The directory grain under the cluster fault swarm (granary §14).
//!
//! `tests/directory.rs` drives the ownership contract on the single-node `Local`
//! tier, where nothing is lost, duplicated, or delayed. This file puts the same
//! grain on a leader-based cluster and sweeps seeds while the nemesis partitions,
//! freezes, crashes, and restarts nodes under it, with the §13 event stream
//! watched live.
//!
//! # What the sweep can and cannot claim
//!
//! The directory is an ordinary grain, so the guarantees it inherits are
//! granary's: a `Record` that *acknowledges* is committed and survives failover
//! (G14), the journal's seqs advance and never land twice on a slot (G3/G5), and
//! a grain is activated at most once per live node (G6). Those are what this
//! sweep asserts, plus the actor safety core.
//!
//! What it deliberately does **not** assert is any end state a *re-delivered
//! mutation* could change. The wire is at-most-once and may duplicate a request
//! frame and delay it (actor §7.2, §18.3), so a duplicate of an earlier `Forget`
//! can arrive **after** a later `Record` and undo it. Nothing in the directory
//! rejects that: its mutating operations carry no idempotency key, so a re-applied
//! one is indistinguishable from a fresh one.
//!
//! That is the framework's contract rather than a defect in it — spec §7.2 puts
//! exactly-once out of scope, "built atop this layer with explicit idempotency
//! keys" — and it is the same shape the `linearizable-remote-register` corpus
//! entries record, where the resolution was to give the object the key the spec
//! prescribes rather than to weaken the wire. It is also the same trap: the first
//! version of this sweep tracked every acknowledged name and demanded it be
//! present at the end, and failed within a handful of seeds on a claim the layer
//! never made.
//!
//! (The second version failed for a duller reason worth recording: it kept those
//! names on the *workload*, which a sweep shares across every seed, so each run
//! was judged against its predecessors' names too. Expectations are per run and
//! live in `drive` — see "A workload outlives its runs" in
//! `docs/simulation-testing.md`.)
//!
//! So the names are split by what can be claimed about them:
//!
//! - **Keep names** are only ever recorded, never forgotten. `Record` is
//!   effect-idempotent — applying it twice leaves the same entry — so no amount
//!   of duplication, delay, or reordering changes where they end up. Every one
//!   whose `Record` acknowledged **must** be in the index at the end, and that is
//!   a real G14 claim: it is the acknowledged-write-survives-failover property.
//! - **Churn names** are recorded and forgotten in turn. They exercise the
//!   removal path, the fold, and the invariants, but carry **no** end-state
//!   assertion, because a delayed duplicate of either operation can legitimately
//!   decide their fate. A consumer that needs them decided supplies the key.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
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
use granary::GrainName;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::testing::ActivationSingletonPerNode;
use granary::testing::CommitMonotonic;
use tenancy::Directory;
use tenancy::Forget;
use tenancy::Meta;
use tenancy::Record;
use tenancy::Recorded;

/// Short enough that clients idle past it, so a principal's index is passivated
/// and rebuilt from its journal mid-run (G12 crossed with G14).
const IDLE_AFTER: Duration = Duration::from_millis(200);
/// A client's idle, comfortably past [`IDLE_AFTER`].
const IDLE_FOR: Duration = Duration::from_millis(500);
/// How long the whole end-of-run verification may take.
///
/// It has to be bounded, and bounded *in total* rather than per name. Each probe
/// is a real quorum write that can sit out its own timeout, so a per-name retry
/// budget multiplies by however many names the run acknowledged — enough, on a
/// cluster still recovering, to overrun the driver's own time budget and fail the
/// seed for liveness rather than for anything it observed. When this runs out the
/// remaining names go unchecked, which is the right trade: a seed that makes no
/// claim is better than a seed that makes a false one.
const VERIFY_BUDGET: Duration = Duration::from_secs(30);

fn config() -> GranaryConfig {
    GranaryConfig {
        shards: 2,
        replication_factor: 3,
        idle_after: IDLE_AFTER,
        snapshot_every: 3,
        ..GranaryConfig::default()
    }
}

fn meta(label: &str) -> Meta {
    Meta {
        label: Some(label.to_string()),
        created_at: Some(0),
        attrs: BTreeMap::new(),
    }
}

/// A name this client only ever records. Its presence at the end is decidable.
fn keep(client: usize, n: u64) -> GrainName {
    GrainName::new("app.Session", format!("keep-c{client}-{n}"))
}

/// A name this client records and forgets in turn. Its fate is not decidable
/// without an idempotency key, so nothing is claimed about it.
fn churn(client: usize, n: u64) -> GrainName {
    GrainName::new("app.Session", format!("churn-c{client}-{n}"))
}

/// The keep names whose `Record` was acknowledged, per principal. A call that
/// failed says nothing either way — the record may sit on a replica minority and
/// be adopted by a later recovery (§7.2, §11) — so it is not tracked.
#[derive(Default)]
struct Acked {
    recorded: BTreeSet<(String, GrainName)>,
}

/// Record-and-forget traffic against a handful of principals' directories,
/// driven through the public `GrainRef` API only (spec §18.4).
struct DirectorySwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
    /// Names checked against a final index, tallied across the whole sweep so a
    /// green run is not one where every call happened to fail. Cumulative on
    /// purpose, unlike the per-run expectations — which live in [`drive`] and
    /// must not be workload state.
    ///
    /// [`drive`]: ClusterWorkload::drive
    verified: Arc<AtomicUsize>,
    /// Whether to hold the final index to what was acknowledged.
    ///
    /// Off for the reproducibility and coverage sweeps: they are about the
    /// *traffic* — that a seed replays byte-identically, and that the faults it
    /// configures actually fire — and the end check costs them a heal, a quiesce,
    /// and a settle they have no reason to pay for.
    check_index: bool,
}

impl DirectorySwarm {
    fn new(nodes: usize, clients: usize, ops: u64) -> DirectorySwarm {
        DirectorySwarm {
            nodes,
            clients,
            ops,
            verified: Arc::new(AtomicUsize::new(0)),
            check_index: false,
        }
    }

    /// The same traffic, with the end-state index claim turned on.
    fn checking_index(nodes: usize, clients: usize, ops: u64) -> DirectorySwarm {
        DirectorySwarm {
            check_index: true,
            ..DirectorySwarm::new(nodes, clients, ops)
        }
    }
}

impl ClusterWorkload for DirectorySwarm {
    fn name(&self) -> &'static str {
        "tenancy-directory-swarm"
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

    fn rehost(&self) -> Option<Rehost> {
        Some(Arc::new(|node: &SimNode| {
            node.granary::<Directory<SimNode>>(config());
        }))
    }

    fn setup(&self, _ctx: &ClusterCtx) {}

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let clients = self.clients;
        let ops = self.ops;
        let verified = Arc::clone(&self.verified);
        let check_index = self.check_index;
        let net = ctx.net().clone();
        Box::pin(async move {
            // Per **run**, not per workload. A sweep drives every seed through the
            // same `&self`, so a field here would carry seed N's acknowledged names
            // into seed N+1's freshly empty grains and report them as lost writes.
            // (It did: that is what made this sweep look like a G14 defect.)
            let acked: Arc<Mutex<Acked>> = Arc::new(Mutex::new(Acked::default()));
            let granaries: Vec<_> = nodes
                .iter()
                .map(|s| s.granary::<Directory<SimNode>>(config()))
                .collect();
            let clock = nodes[0].clock().clone();
            let entropy = nodes[0].entropy().clone();
            clock.sleep(Duration::from_secs(3)).await;

            // Node 1's gateway only: the nemesis may restart any other node, and a
            // handle for a restarted one points at a shut-down system. Location
            // transparency (G13) keeps the coverage.
            let granary = granaries[0].clone();

            let mut tasks = Vec::new();
            for c in 0..clients {
                let granary = granary.clone();
                let clock = clock.clone();
                let entropy = entropy.clone();
                let acked = Arc::clone(&acked);
                tasks.push(async move {
                    for op in 0..ops {
                        // A small principal space, so several directories share
                        // each shard and some collide across clients.
                        let principal = format!("p{}", entropy.next_u64() % 3);
                        let dir = granary.grain(&principal);
                        // Name spaces are private per client, so two clients never
                        // race one name and every claim below stays decidable.
                        let slot = entropy.next_u64() % 3;

                        if entropy.next_u64().is_multiple_of(3) {
                            // CHURN: record then forget. No end-state claim — a
                            // delayed duplicate of either call can decide it.
                            let name = churn(c, slot);
                            let _ = dir
                                .ask_timeout(
                                    Record {
                                        name: name.clone(),
                                        meta: meta("churn"),
                                    },
                                    Duration::from_secs(2),
                                )
                                .await;
                            let _ = dir
                                .ask_timeout(Forget { name }, Duration::from_secs(2))
                                .await;
                        } else {
                            // KEEP: record only. Any acknowledged outcome —
                            // `Created`, `Updated`, or `Unchanged` — means the name
                            // is owned once the call returns, and nothing in this
                            // run ever removes it, so it must still be owned at the
                            // end however the wire behaved.
                            let name = keep(c, slot);
                            let outcome = dir
                                .ask_timeout(
                                    Record {
                                        name: name.clone(),
                                        meta: meta("keep"),
                                    },
                                    Duration::from_secs(2),
                                )
                                .await;
                            if matches!(outcome, Ok(Recorded::Created | Recorded::Updated)) {
                                let mut acked = acked.lock().expect("acked mutex");
                                acked.recorded.insert((principal.clone(), name));
                            }
                        }

                        // Idle past the activation lifetime, so the index
                        // passivates and the next call rebuilds it from a quorum.
                        if op % 2 == 1 {
                            clock.sleep(IDLE_FOR).await;
                        }
                    }
                });
            }
            futures::future::join_all(tasks).await;
            if !check_index {
                return;
            }

            // Put the cluster somewhere a read can be *judged* before reading
            // anything back. `heal()` clears the nemesis's partitions but leaves
            // the wire's seeded loss running, and a detector fed lossy probes keeps
            // flipping peers in and out of every node's view, so the shard map
            // never settles and a read can be served by a node that is no longer
            // the leader. `quiesce()` is the other half, and the settle that
            // follows is what makes "the index does not hold it" mean the index
            // (docs/simulation-testing.md, "Asserting at quiescence").
            net.heal();
            net.quiesce();
            clock.sleep(Duration::from_secs(5)).await;

            // Read each principal's index back and hold it to what was
            // acknowledged. Snapshot out of the mutex first: a `MutexGuard` held
            // across an await would make this future non-`Send`.
            let recorded = {
                let acked = acked.lock().expect("acked mutex");
                acked.recorded.clone()
            };
            // Ask about each name with a *writing* command, not `List`.
            //
            // `List` is a query, and a query commits nothing, so §7.5 serves it
            // from the activation: "read-your-leader (relaxed), not linearizable
            // under partition" — a deposed-but-unfenced leader may answer from
            // stale state, and a quorum-less recovery may have seeded that state
            // with an uncommitted record. Asserting a durability property against
            // it asks the wrong question, and a `List`-based version of this check
            // failed about one seed in eight hundred for exactly that reason.
            //
            // The spec names the construction in the same paragraph: issue a
            // trivial writing command, which rides the §6 output gate and so
            // "commits through the shard leader and reflects committed state, or
            // fails". `Record` with *different* metadata is one: it commits either
            // way, and its reply is the committed answer to the question asked —
            // `Updated` iff the name was already owned, `Created` iff it was not.
            let verify_by = clock.now() + VERIFY_BUDGET;
            for (principal, name) in &recorded {
                if clock.now() >= verify_by {
                    break;
                }
                let dir = granary.grain(principal);
                let probe = Meta {
                    label: Some("probe".into()),
                    created_at: Some(1),
                    attrs: BTreeMap::new(),
                };
                // A failed probe says nothing about the index, so retry; if it
                // never commits, this seed makes no claim rather than a false one.
                let mut answer = None;
                while clock.now() < verify_by {
                    match dir
                        .ask_timeout(
                            Record {
                                name: name.clone(),
                                meta: probe.clone(),
                            },
                            Duration::from_secs(2),
                        )
                        .await
                    {
                        // `Unchanged` cannot come from a first delivery here (the
                        // metadata differs), only from a duplicate of a probe that
                        // already landed — which itself proves the name is owned.
                        Ok(outcome) => {
                            answer = Some(outcome);
                            break;
                        }
                        Err(_) => clock.sleep(Duration::from_millis(250)).await,
                    }
                }
                let Some(answer) = answer else { continue };
                assert_ne!(
                    answer,
                    Recorded::Created,
                    "{principal} acknowledged Record({name}) — a name nothing in \
                     this run ever forgets — and a committed probe found it absent: \
                     an acknowledged write was lost (G14)",
                );
                verified.fetch_add(1, Ordering::Relaxed);
            }
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        let mut invariants = default_invariants();
        invariants.push(Box::new(CommitMonotonic::new(
            "directory-commit-monotonic",
            "directory",
        )));
        invariants.push(Box::new(ActivationSingletonPerNode::new(
            "directory-activation-singleton-per-node",
            "directory",
        )));
        invariants
    }
}

#[test]
fn the_directory_index_survives_the_cluster_swarm() {
    let workload = DirectorySwarm::checking_index(3, 3, 6);
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..24)) {
        panic!("{failure}");
    }
    assert!(
        workload.verified.load(Ordering::Relaxed) > 0,
        "no acknowledged name was ever checked against a readable index — the \
         sweep asserted nothing about ownership",
    );
}

#[test]
fn the_directory_swarm_is_reproducible() {
    let workload = DirectorySwarm::new(3, 2, 5);
    if let Err(divergence) = replay_cluster_swarm(&workload, sweep_seeds(0..12)) {
        panic!("{divergence}");
    }
}

#[test]
fn the_directory_swarm_actually_fires_each_fault_type() {
    let workload = DirectorySwarm::new(3, 3, 6);
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
        "the sweep never duplicated a frame: {stats:?}"
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
