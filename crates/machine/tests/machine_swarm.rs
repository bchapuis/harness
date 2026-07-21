//! The machine grain under the cluster fault swarm (machine §7; grain §14).
//!
//! `tests/machine_sim.rs` proves the lifecycle on the `Local` tier; this file
//! hosts machines on the leader-based clustered system and sweeps seeds while
//! the nemesis injects partitions, crashes, heals, loss, duplication, and
//! delay. What that uniquely exercises:
//!
//! - **M1/M3 under crashes.** A leader crash mid-session ends the activation;
//!   the next attach rebuilds the image from the last committed capture —
//!   never a fork, never a torn image (the disk facet's swarm obligations,
//!   re-run here with a live "guest" dirtying the image between captures).
//! - **M5's self-fence.** A deposed activation's next checkpoint-alarm append
//!   cannot commit; the host steps down and `on_passivate` kills the fake VM.
//!   The safety checkers below would catch the forbidden alternative (a
//!   deposed activation still committing).
//! - **Front-door loss.** Node crashes kill door stubs; death watch folds
//!   `Detached { FrontDoorLost }` and the pin releases.
//! - **Seed-reproducibility.** One seed replays byte-identically, fake guest
//!   writes included.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::NodeId;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::SimCluster;
use actor_simulation::check_cluster_reproducible;
use actor_simulation::default_invariants;
use actor_simulation::run_cluster_swarm;
use granary::GrainEvent;
use granary::GrainName;
use granary::GranaryConfig;
use granary::GranaryExt;
use machine::Attach;
use machine::Detach;
use machine::Machine;
use machine::Provision;
use machine::Status;
use machine::fake::FakeVmProvider;

type ClusterMachine = Machine<SimCluster, FakeVmProvider<SimCluster>>;

/// The front-door member stand-in (machine §5.1): one per node; a node crash
/// kills it, and the machines watching it fold `Detached { FrontDoorLost }`.
#[derive(Default)]
struct DoorStub;

impl Actor for DoorStub {
    type System = SimCluster;
}

// --- Grain-specific continuous safety checkers (as in the disk/sql swarms) -----

/// **Commit head is monotonic** (invariants **G3**, **G5**) — the checker that
/// would catch a deposed activation still committing (M1/M5's forbidden
/// alternative).
#[derive(Default)]
struct CommitMonotonic {
    last: BTreeMap<GrainName, u64>,
}

impl Invariant for CommitMonotonic {
    fn name(&self) -> &'static str {
        "machine-commit-monotonic"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        if let Some(GrainEvent::Committed { name, seq, .. }) = event.as_app::<GrainEvent>() {
            let prev = self.last.get(name).copied().unwrap_or(0);
            if *seq <= prev {
                return Err(format!(
                    "machine {name} committed seq {seq} not after previous head {prev} (G3/G5)"
                ));
            }
            self.last.insert(name.clone(), *seq);
        }
        Ok(())
    }
}

/// **Exactly-once activation per node** (invariant **G6**), crash-sound.
#[derive(Default)]
struct ActivationSingletonPerNode {
    live: BTreeSet<(NodeId, GrainName)>,
}

impl Invariant for ActivationSingletonPerNode {
    fn name(&self) -> &'static str {
        "machine-activation-singleton-per-node"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        if let Event::NodeDown { node, .. } = event {
            self.live.retain(|(n, _)| n != node);
            return Ok(());
        }
        match event.as_app::<GrainEvent>() {
            Some(GrainEvent::Activated { node, name }) => {
                let fresh = self.live.insert((*node, name.clone()));
                if !fresh {
                    return Err(format!(
                        "machine {name} activated while already live on {node} (G6)"
                    ));
                }
            }
            Some(GrainEvent::Passivated { node, name }) => {
                self.live.remove(&(*node, name.clone()));
            }
            _ => {}
        }
        Ok(())
    }
}

// --- The workload ---------------------------------------------------------------

/// Attach/dwell/detach sessions against a handful of machines under the
/// nemesis, driven through the public `GrainRef` API only. A faulted call is
/// recorded as nothing and the client moves on; sessions long enough for the
/// checkpoint alarm to fire give every run mid-session captures with a live
/// (fake) guest dirtying the image.
struct MachineSwarm {
    nodes: usize,
    clients: usize,
    ops: u64,
    dir: PathBuf,
}

impl MachineSwarm {
    fn config(&self) -> GranaryConfig {
        GranaryConfig {
            shards: 2,
            replication_factor: 3,
            idle_after: Duration::from_secs(60),
            snapshot_every: 4,
            data_dir: Some(self.dir.clone()),
            ..GranaryConfig::default()
        }
    }

    fn base_image(&self) -> PathBuf {
        self.dir.join("base.img")
    }
}

impl ClusterWorkload for MachineSwarm {
    fn name(&self) -> &'static str {
        "machine-swarm"
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

    fn setup(&self, _ctx: &ClusterCtx) {
        // A deterministic ~1.5 MiB base image (two blocks, one partial).
        let len = (1u64 << 20) + (1 << 19);
        let bytes: Vec<u8> = (0..len).map(|i| (i / 17 % 239) as u8).collect();
        std::fs::write(self.base_image(), bytes).expect("write base image");
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimCluster> = ctx.nodes().to_vec();
        let net = ctx.net().clone();
        let clients = self.clients;
        let ops = self.ops;
        let config = self.config();
        let base = self.base_image().to_string_lossy().into_owned();
        Box::pin(async move {
            let granaries: Vec<_> = nodes
                .iter()
                .map(|s| {
                    let provider = std::sync::Arc::new(FakeVmProvider::new(
                        s.clone(),
                        Duration::from_millis(50),
                    ));
                    s.granary_named::<ClusterMachine>(
                        machine::MACHINE_TYPE,
                        config.clone(),
                        std::sync::Arc::new(move || Machine::new(std::sync::Arc::clone(&provider))),
                    )
                })
                .collect();
            let doors: Vec<_> = nodes.iter().map(|s| s.spawn(DoorStub)).collect();
            let clock = nodes[0].clock().clone();
            let entropy = nodes[0].entropy().clone();
            // Let the control-plane and shard groups elect before traffic.
            clock.sleep(Duration::from_secs(3)).await;

            let mut tasks = Vec::new();
            for c in 0..clients {
                let granary = granaries[c % granaries.len()].clone();
                let door = doors[c % doors.len()].id().clone();
                let entropy = entropy.clone();
                let clock = clock.clone();
                let base = base.clone();
                tasks.push(async move {
                    for _ in 0..ops {
                        let key = format!("box/{}", entropy.next_u64() % 3);
                        let grain = granary.grain(key);
                        // Provision on first touch; AlreadyProvisioned is fine.
                        let _ = grain
                            .ask_timeout(
                                Provision {
                                    owner: format!("client-{c}"),
                                    base_image: base.clone(),
                                    vcpus: 1,
                                    mem_mib: 128,
                                    checkpoint: Duration::from_millis(500),
                                    lease: Duration::from_millis(500),
                                    authorized_keys: BTreeMap::new(),
                                },
                                Duration::from_secs(2),
                            )
                            .await;
                        // A session: attach, dwell past a checkpoint interval
                        // (the alarm captures mid-session), detach.
                        let attached = grain
                            .ask_timeout(
                                Attach {
                                    principal: format!("client-{c}"),
                                    front_door: door.clone(),
                                },
                                Duration::from_secs(2),
                            )
                            .await;
                        clock.sleep(Duration::from_millis(700)).await;
                        if let Ok(Ok(reply)) = attached {
                            let _ = grain
                                .ask_timeout(
                                    Detach {
                                        attachment: reply.attachment,
                                    },
                                    Duration::from_secs(2),
                                )
                                .await;
                        }
                        let _ = grain.ask_timeout(Status, Duration::from_secs(2)).await;
                    }
                });
            }
            futures::future::join_all(tasks).await;

            // Cleanup: unlike the disk/sql swarm grains, an *attached* machine
            // never goes quiet — the checkpoint alarm chains by design
            // (machine §4) — and a faulted `Detach` can leak an attachment. So
            // the drive ends by outlasting the bounded nemesis, healing
            // whatever it left torn, and detaching every surviving attachment,
            // letting each machine run its final capture and consume its
            // alarm; only then can the cluster quiesce.
            clock.sleep(Duration::from_secs(6)).await;
            net.heal();
            clock.sleep(Duration::from_secs(2)).await;
            for _ in 0..5 {
                let mut all_clear = true;
                for k in 0..3 {
                    let grain = granaries[0].grain(format!("box/{k}"));
                    let Ok(status) = grain.ask_timeout(Status, Duration::from_secs(2)).await else {
                        all_clear = false;
                        continue;
                    };
                    for (id, _) in status.attachments {
                        all_clear = false;
                        let _ = grain
                            .ask_timeout(Detach { attachment: id }, Duration::from_secs(2))
                            .await;
                    }
                }
                if all_clear {
                    break;
                }
                clock.sleep(Duration::from_secs(1)).await;
            }
            // Let the final captures commit and the alarms consume.
            clock.sleep(Duration::from_secs(2)).await;
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        let mut invariants = default_invariants();
        invariants.push(Box::new(CommitMonotonic::default()));
        invariants.push(Box::new(ActivationSingletonPerNode::default()));
        invariants
    }
}

#[test]
fn machine_invariants_hold_under_the_cluster_swarm() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = MachineSwarm {
        nodes: 3,
        clients: 3,
        ops: 3,
        dir: dir.path().to_path_buf(),
    };
    if let Err(failure) = run_cluster_swarm(&workload, 0..12) {
        panic!("{failure}");
    }
}

#[test]
fn machine_cluster_swarm_is_reproducible() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = MachineSwarm {
        nodes: 3,
        clients: 2,
        ops: 2,
        dir: dir.path().to_path_buf(),
    };
    for seed in 0..6 {
        if let Err(divergence) = check_cluster_reproducible(&workload, seed) {
            panic!("{divergence}");
        }
    }
}
