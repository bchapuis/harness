//! The machine's conformance narrative (machine spec §9): one seeded run
//! driving attach → mid-session crash → partition with a doomed session →
//! failover → reconnect, asserting M1–M3 and M5 along the way, plus the disk
//! facet's F2/F4 (rehydration reproduces the last committed capture).
//!
//! This is the machine analogue of entity's `sql_swarm`/`disk_swarm` seed
//! narratives: the fence, the capture cadence, the lease, and the crash rewind
//! are all seed-driven, so one seed reproduces the whole story deterministically.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Event;
use actor_core::NodeId;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::SimNode;
use actor_simulation::run_cluster_seed;
use entity::EntityConfig;
use entity::EntityEvent;
use entity::EntityExt;
use entity::EntityName;
use entity::testing::ActivationSingletonPerNode;
use machine_entity::Attach;
use machine_entity::Detach;
use machine_entity::Machine;
use machine_entity::MachineRuntimeProvider;
use machine_entity::Provision;
use machine_entity::Status;
use machine_entity::fake::FakeRuntimeProvider;

type ClusterMachine = Machine<SimNode>;

#[derive(Default)]
struct DoorStub;
impl Actor for DoorStub {
    type System = SimNode;
}

/// **M1: single disk, never forked.** Committed seq is strictly monotonic per
/// machine; a deposed activation that kept committing (the forbidden
/// alternative to M1/M5) would regress or duplicate a seq.
#[derive(Default)]
struct NeverForks {
    last: BTreeMap<EntityName, u64>,
}
impl Invariant for NeverForks {
    fn name(&self) -> &'static str {
        "machine-never-forks"
    }
    fn observe(&mut self, event: &Event) -> Result<(), String> {
        if let Some(EntityEvent::Committed { name, seq, .. }) = event.as_app::<EntityEvent>() {
            let prev = self.last.get(name).copied().unwrap_or(0);
            if *seq <= prev {
                return Err(format!(
                    "machine {name} committed seq {seq} not after {prev}: a fork (M1)"
                ));
            }
            self.last.insert(name.clone(), *seq);
        }
        Ok(())
    }
}

// E6 ("at most one committing activation per node") is *not* redefined here:
// it is `entity::testing::ActivationSingletonPerNode`, shared with
// `machine_swarm.rs` and entity's own suites.

/// Which nodes hold a live activation of the story's machine, read off the
/// event stream so the narrative can partition the holder and then assert it
/// stepped down within the lease bound (M5). Shared between the invariant that
/// observes and the drive that asserts.
#[derive(Default)]
struct StoryWatch {
    live: BTreeSet<NodeId>,
}

struct StoryObserver(Arc<Mutex<StoryWatch>>);

impl Invariant for StoryObserver {
    fn name(&self) -> &'static str {
        "machine-story-observer"
    }
    fn observe(&mut self, event: &Event) -> Result<(), String> {
        let mut watch = self.0.lock().expect("story watch");
        if let Some(e) = event.as_app::<EntityEvent>() { eprintln!("DBG ev {e:?}"); }
        match event.as_app::<EntityEvent>() {
            Some(EntityEvent::Activated { node, name }) if name.key() == "story" => {
                watch.live.insert(*node);
            }
            Some(EntityEvent::Deactivated { node, name }) if name.key() == "story" => {
                watch.live.remove(node);
            }
            _ => {}
        }
        Ok(())
    }
}

struct Narrative {
    dir: PathBuf,
    watch: Arc<Mutex<StoryWatch>>,
}

impl Narrative {
    fn config(&self) -> EntityConfig {
        EntityConfig {
            shards: 2,
            replication_factor: 3,
            idle_after: Duration::from_secs(60),
            snapshot_every: 4,
            data_dir: Some(self.dir.clone()),
            ..EntityConfig::default()
        }
    }
    fn base_image(&self) -> PathBuf {
        self.dir.join("base.img")
    }
}

impl ClusterWorkload for Narrative {
    fn name(&self) -> &'static str {
        "machine-conformance-narrative"
    }
    fn node_count(&self) -> usize {
        3
    }
    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::leader(self.swim(), 3, DowningPolicy::Conservative)
    }
    fn setup(&self, _ctx: &ClusterCtx) {
        let len = (1u64 << 20) + (1 << 19);
        let bytes: Vec<u8> = (0..len).map(|i| (i / 7 % 251) as u8).collect();
        std::fs::write(self.base_image(), bytes).expect("write base image");
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let nodes: Vec<SimNode> = ctx.nodes().to_vec();
        let net = ctx.net().clone();
        let config = self.config();
        let base = self.base_image().to_string_lossy().into_owned();
        let watch = Arc::clone(&self.watch);
        Box::pin(async move {
            let hosts: Vec<_> = nodes
                .iter()
                .map(|s| {
                    let provider: std::sync::Arc<dyn MachineRuntimeProvider> =
                        std::sync::Arc::new(FakeRuntimeProvider::new(s.clone(), Duration::from_millis(50)));
                    s.entity_node().entities_named::<ClusterMachine>(
                        machine_entity::MACHINE_TYPE,
                        config.clone(),
                        std::sync::Arc::new(move || Machine::new(std::sync::Arc::clone(&provider))),
                    )
                })
                .collect();
            let door = nodes[0].spawn(DoorStub);
            let clock = nodes[0].clock().clone();
            clock.sleep(Duration::from_secs(3)).await;

            let g = hosts[0].clone();
            let entity = g.entity("story");
            // Every committed image the story observes before the fault: the
            // disk after the failover must be one of them (M3), never a
            // capture the deposed side could not commit.
            let mut committed_digests = BTreeSet::new();

            // 1. Provision, then attach: a session opens, the guest starts
            //    writing. The machine must be durably provisioned before the
            //    fault sequence: a name→shard partition places "story" on some
            //    shard whose leader must be settled for the provision commit to
            //    land, so re-issue until Status confirms it (the Provision
            //    handler rejects a second provision, so re-issue only while
            //    unprovisioned). The attach retries the same way, since an
            //    attach before the provision lands answers `NotProvisioned`
            //    and opens no session, which would leave M3 and M5 nothing to
            //    exercise.
            let lease = Duration::from_millis(400);
            for _ in 0..20 {
                if let Ok(status) = entity.ask_timeout(Status, Duration::from_secs(2)).await
                    && status.provisioned
                {
                    committed_digests.extend(status.image_digest);
                    break;
                }
                let _ = entity
                    .ask_timeout(
                        Provision {
                            owner: "alice".into(),
                            base_image: base.clone(),
                            vcpus: 1,
                            mem_mib: 128,
                            checkpoint: lease,
                            lease,
                            authorized_keys: BTreeMap::new(),
                        },
                        Duration::from_secs(2),
                    )
                    .await;
            }
            let mut attachment_id = None;
            for _ in 0..20 {
                if let Ok(Ok(reply)) = entity
                    .ask_timeout(
                        Attach {
                            principal: "alice".into(),
                            front_door: door.id().clone(),
                        },
                        Duration::from_secs(2),
                    )
                    .await
                {
                    attachment_id = Some(reply.attachment);
                    break;
                }
                clock.sleep(Duration::from_millis(500)).await;
            }
            assert!(attachment_id.is_some(), "the story needs a live session");

            // 2. Mid-session: dwell past a checkpoint interval so the alarm
            //    captures the guest's writes (M3's cadence), then partition
            //    the node holding the activation alone: a doomed minority
            //    with a live guest (M5).
            let mut captured = false;
            for _ in 0..10 {
                clock.sleep(Duration::from_millis(300)).await;
                if let Ok(status) = entity.ask_timeout(Status, Duration::from_secs(2)).await {
                    committed_digests.extend(status.image_digest);
                    if status.captures >= 1 {
                        assert!(status.running, "a session keeps its guest live");
                        captured = true;
                        break;
                    }
                }
            }
            assert!(captured, "the checkpoint alarm must capture mid-session (M3)");
            let holder = *watch
                .lock()
                .expect("story watch")
                .live
                .iter()
                .next()
                .expect("the attach activated the machine somewhere");
            let others: Vec<NodeId> = nodes
                .iter()
                .map(|n| n.node())
                .filter(|n| *n != holder)
                .collect();
            eprintln!("DBG partition holder={holder} others={others:?}");
            net.partition(&[holder], &others);
            let t0 = clock.now();
            for _ in 0..40 {
                clock.sleep(Duration::from_millis(250)).await;
                let live = watch.lock().expect("story watch").live.clone();
                eprintln!("DBG +{:?} live={live:?}", clock.now().duration_since(t0));
                if !live.contains(&holder) { break; }
            }

            // M5's bound: the alarm fires within one lease interval, its
            // append cannot reach a quorum and fails at the quorum timeout,
            // and the activation ends. Dwell that long, then the holder must
            // have stepped down, before anything heals.
            clock
                .sleep(lease + config.quorum_timeout + Duration::from_millis(600))
                .await;
            assert!(
                !watch.lock().expect("story watch").live.contains(&holder),
                "the partitioned holder {holder} must deactivate within the lease \
                 plus the append timeout (M5)",
            );

            // 3. Failover: heal; the majority side leads, the doomed session is
            //    fenced (M5), and a reconnect re-resolves the new leader.
            net.heal();
            clock.sleep(Duration::from_secs(3)).await;

            // 4. Reconnect: reach the machine at its (possibly new) leader; its
            //    disk is the last committed capture (M3/F4), never a fork (M1).
            let mut reached = false;
            for node in &hosts {
                let reread = node.entity("story");
                if let Ok(status) = reread.ask_timeout(Status, Duration::from_secs(2)).await {
                    assert!(status.provisioned, "the machine survived the story");
                    assert!(
                        status
                            .image_digest
                            .is_some_and(|digest| committed_digests.contains(&digest)),
                        "after failover the disk must be a capture committed before the \
                         partition (M3), got {:?} from {committed_digests:?}",
                        status.image_digest,
                    );
                    reached = true;
                    break;
                }
            }
            assert!(reached, "the machine must be reachable after failover");

            // Detach so the machine runs its final capture and quiesces (an
            // attached machine's alarm chains by design, machine §6).
            // `ask_timeout` re-resolves the leader on `NotLeader`, so one ref
            // suffices; poll Status until no attachment survives, then dwell
            // past the final capture so nothing is in flight at quiescence
            // (the no-silent-loss invariant).
            let handle = hosts[0].entity("story");
            for _ in 0..6 {
                if let Some(id) = attachment_id {
                    let _ = handle
                        .ask_timeout(Detach { attachment: id }, Duration::from_secs(2))
                        .await;
                }
                match handle.ask_timeout(Status, Duration::from_secs(2)).await {
                    Ok(status) if status.attachments.is_empty() => break,
                    _ => clock.sleep(Duration::from_secs(1)).await,
                }
            }
            clock.sleep(Duration::from_secs(3)).await;
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        let mut invariants = actor_simulation::default_invariants();
        invariants.push(Box::new(NeverForks::default()));
        invariants.push(Box::new(ActivationSingletonPerNode::new(
            "machine-singleton-per-node",
            "machine",
        )));
        invariants.push(Box::new(StoryObserver(Arc::clone(&self.watch))));
        invariants
    }
}

#[test]
fn the_machine_survives_the_attach_crash_partition_failover_reconnect_story() {
    // machine §9: one seed drives the whole narrative; M1, M3, M5, and E6 hold
    // throughout, and the machine is reachable and provisioned at the end. The
    // swarm runner's nemesis faults the cluster underneath the story's own
    // partition, and its heals (one follows its last round) can clear that
    // partition early, so many seeds strand the story (no session, no capture,
    // no deactivation while cut off) without breaking any invariant: the seed
    // is chosen to let the story reach its end.
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = Narrative {
        dir: dir.path().to_path_buf(),
        watch: Arc::default(),
    };
    if let Err(failure) = run_cluster_seed(&workload, 11) {
        panic!("{failure}");
    }
}
