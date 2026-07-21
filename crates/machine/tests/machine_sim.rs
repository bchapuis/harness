//! Machine lifecycle under deterministic simulation, `Local` tier (machine
//! §4, §6): provision from a base image, attach boots the fake VM, the
//! checkpoint alarm captures mid-session (M3's cadence), detach schedules the
//! final capture (M2's graceful boundary), hibernation finds a clean disk, and
//! reactivation reproduces the image byte-identically. Front-door death folds
//! `Detached { FrontDoorLost }` (machine §5.1). Crash/partition behavior (M3
//! rewind, M5 self-fence) is `tests/machine_swarm.rs`'s job.

use std::sync::Arc;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::Ctx;
use actor_core::EventSink;
use actor_core::Handler;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use granary::GrainEvent;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::GranarySystem;
use machine::Attach;
use machine::Detach;
use machine::Machine;
use machine::MachineError;
use machine::Provision;
use machine::Status;
use machine::WsList;
use machine::WsRead;
use machine::WsRemove;
use machine::WsWrite;
use machine::fake::FakeVmProvider;
use serde::Deserialize;
use serde::Serialize;

type SimMachine = Machine<SimSystem, FakeVmProvider<SimSystem>>;

/// A stand-in for the front-door member holding an attachment (machine §5.1):
/// the machine death-watches this actor's id.
#[derive(Default)]
struct DoorStub;

impl Actor for DoorStub {
    type System = SimSystem;

    fn register(registry: &mut actor_core::HandlerRegistry<DoorStub>) {
        registry.accept::<Die>();
    }
}

/// Stop the stub — simulating the front-door member dying without detaching.
#[derive(Clone, Serialize, Deserialize)]
struct Die;
impl Message for Die {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.DoorDie");
}
impl Handler<Die> for DoorStub {
    async fn handle(&mut self, _msg: Die, ctx: &Ctx<DoorStub>) {
        ctx.stop();
    }
}

fn sim_system(sim: &Simulation, recorder: &Recorder) -> SimSystem {
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build()
}

/// A deterministic ~2.5 MiB base image (three 1 MiB blocks, the last partial).
fn write_base_image(path: &std::path::Path) -> u64 {
    let len = 2 * (1u64 << 20) + (1 << 19);
    let bytes: Vec<u8> = (0..len).map(|i| (i / 13 % 241) as u8).collect();
    std::fs::write(path, &bytes).expect("write base image");
    len
}

fn provision(base: &std::path::Path) -> Provision {
    Provision {
        owner: "alice".into(),
        base_image: base.to_string_lossy().into_owned(),
        vcpus: 1,
        mem_mib: 128,
        checkpoint: Duration::from_millis(100),
        lease: Duration::from_millis(100),
        authorized_keys: [("fp1".to_string(), "ssh-ed25519 AAAA...".to_string())].into(),
    }
}

/// The shared per-test rig: scratch dir with a base image, seeded sim,
/// recorded system, fake VM provider, and the machine granary. Each test
/// contributes only its seed and grain name.
struct Fixture {
    _scratch: tempfile::TempDir,
    base: std::path::PathBuf,
    image_len: u64,
    sim: Simulation,
    recorder: Recorder,
    system: SimSystem,
    machines: granary::Granary<SimMachine>,
}

fn fixture(seed: u64) -> Fixture {
    let scratch = tempfile::tempdir().expect("tempdir");
    let base = scratch.path().join("base.img");
    let image_len = write_base_image(&base);
    let sim = Simulation::new(seed);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let provider = Arc::new(FakeVmProvider::new(
        system.clone(),
        Duration::from_millis(10),
    ));
    let machines = system.granary_named::<SimMachine>(
        machine::MACHINE_TYPE,
        GranaryConfig {
            idle_after: Duration::from_millis(200),
            snapshot_every: 100,
            data_dir: Some(scratch.path().to_path_buf()),
            ..GranaryConfig::default()
        },
        Arc::new(move || Machine::new(Arc::clone(&provider))),
    );
    Fixture {
        _scratch: scratch,
        base,
        image_len,
        sim,
        recorder,
        system,
        machines,
    }
}

#[test]
fn attach_capture_detach_hibernate_reattach_round_trips() {
    let Fixture {
        _scratch,
        base,
        image_len,
        sim,
        recorder,
        system,
        machines,
    } = fixture(41);

    let door = system.spawn(DoorStub);
    let door_id = door.id().clone();
    let grain = machines.grain("dev-box");
    let sys = system.clone();
    let base_for_task = base.clone();
    let digest_after_final = sim.block_on(async move {
        // Provision: the base image is the disk's first, full-coverage capture.
        grain
            .ask(provision(&base_for_task))
            .await
            .expect("ask")
            .expect("provision");
        let status = grain.ask(Status).await.expect("status");
        assert!(status.provisioned);
        assert_eq!(status.image_bytes, image_len);
        assert!(!status.vm_running);

        // Attach: boots the VM; the guest (fake) starts writing.
        let reply = grain
            .ask(Attach {
                principal: "alice".into(),
                front_door: door_id,
            })
            .await
            .expect("ask")
            .expect("attach");
        let status = grain.ask(Status).await.expect("status");
        assert!(status.vm_running);
        assert_eq!(status.attachments, vec![(reply.attachment, "alice".into())]);

        // Three checkpoint intervals of dirtying guest writes: the alarm
        // captures mid-session (machine §4, quiescent point 3).
        sys.sleep(Duration::from_millis(350)).await;
        let status = grain.ask(Status).await.expect("status");
        assert!(
            status.captures >= 1,
            "the checkpoint alarm must have captured mid-session (got {})",
            status.captures
        );

        // Detach: the final capture runs immediately (machine §4, quiescent
        // point 1) — guest stopped, image settled, grain clean.
        assert!(
            grain
                .ask(Detach {
                    attachment: reply.attachment
                })
                .await
                .expect("detach")
        );
        sys.sleep(Duration::from_millis(50)).await;
        let status = grain.ask(Status).await.expect("status");
        assert!(!status.vm_running, "the final capture stops the guest");
        assert!(status.attachments.is_empty());
        status.image_digest.expect("digest of the settled image")
    });

    // Idle: with the disk captured and no attachment, the machine hibernates
    // (M2's graceful boundary; `can_passivate` no longer refuses).
    sim.run();
    assert!(
        recorder.events().iter().any(|e| matches!(
            e.as_app::<GrainEvent>(),
            Some(GrainEvent::Passivated { .. })
        )),
        "the idle machine must hibernate",
    );

    // Reactivate: the rehydrated image is byte-identical to the final capture.
    let reread = machines.grain("dev-box");
    sim.block_on(async move {
        let status = reread.ask(Status).await.expect("status");
        assert_eq!(
            status.image_digest,
            Some(digest_after_final),
            "rehydration reproduces the final captured image byte-identically (M2)",
        );
        assert!(!status.vm_running);
    });
}

/// The workspace facet's narrative (machine §3, §4): file commands work
/// without booting the VM, refuse while one is live, the boot pushes the
/// host-written files into the guest, the capture cadence pulls the guest's
/// writes back (one atomic batch with the disk manifest), and the whole
/// workspace survives hibernation and reactivation alongside the disk (M2).
#[test]
fn workspace_files_survive_boot_capture_and_hibernation() {
    let Fixture {
        _scratch,
        base,
        image_len: _image_len,
        sim,
        recorder,
        system,
        machines,
    } = fixture(47);

    let door = system.spawn(DoorStub);
    let door_id = door.id().clone();
    let grain = machines.grain("ws-box");
    let sys = system.clone();
    let base_for_task = base.clone();
    sim.block_on(async move {
        grain
            .ask(provision(&base_for_task))
            .await
            .expect("ask")
            .expect("provision");

        // File commands without a VM: write, read back, list; an
        // escape-shaped path is refused.
        grain
            .ask(WsWrite {
                path: "notes/hello.txt".into(),
                bytes: b"written while hibernated".to_vec(),
            })
            .await
            .expect("ask")
            .expect("ws write without a vm");
        assert_eq!(
            grain
                .ask(WsRead {
                    path: "notes/hello.txt".into()
                })
                .await
                .expect("ask")
                .expect("ws read without a vm"),
            b"written while hibernated".to_vec()
        );
        assert!(matches!(
            grain
                .ask(WsWrite {
                    path: "../escape".into(),
                    bytes: b"x".to_vec()
                })
                .await
                .expect("ask"),
            Err(MachineError::Ws(_))
        ));

        // Attach: the boot pushes the workspace into the guest; while the VM
        // is live the guest owns /workspace and file commands refuse.
        let reply = grain
            .ask(Attach {
                principal: "alice".into(),
                front_door: door_id,
            })
            .await
            .expect("ask")
            .expect("attach");
        assert!(matches!(
            grain
                .ask(WsWrite {
                    path: "blocked.txt".into(),
                    bytes: b"x".to_vec()
                })
                .await
                .expect("ask"),
            Err(MachineError::VmLive)
        ));
        assert!(matches!(
            grain
                .ask(WsRead {
                    path: "notes/hello.txt".into()
                })
                .await
                .expect("ask"),
            Err(MachineError::VmLive)
        ));

        // Checkpoint intervals pass: the cadence pulls the guest's workspace
        // writes (the fake guest dirties `guest.log`) and stages them with
        // the disk manifest — the ws capture counter advances.
        sys.sleep(Duration::from_millis(350)).await;
        let status = grain.ask(Status).await.expect("status");
        assert!(
            status.ws_captures >= 1,
            "the checkpoint cadence must have captured the guest's workspace \
             writes (got {})",
            status.ws_captures
        );

        // Detach: the final capture pulls once more and stops the guest;
        // the pulled guest file is then readable host-side.
        assert!(
            grain
                .ask(Detach {
                    attachment: reply.attachment
                })
                .await
                .expect("detach")
        );
        sys.sleep(Duration::from_millis(50)).await;
        let status = grain.ask(Status).await.expect("status");
        assert!(!status.vm_running);
        let log = grain
            .ask(WsRead {
                path: "guest.log".into(),
            })
            .await
            .expect("ask")
            .expect("the guest's workspace write survives the final pull");
        assert!(!log.is_empty());
        // The host-written file survived the round trip through the guest.
        assert_eq!(
            grain
                .ask(WsRead {
                    path: "notes/hello.txt".into()
                })
                .await
                .expect("ask")
                .expect("host file survives the guest round trip"),
            b"written while hibernated".to_vec()
        );
    });

    // Idle: clean disk, no attachment — the machine hibernates.
    sim.run();
    assert!(
        recorder.events().iter().any(|e| matches!(
            e.as_app::<GrainEvent>(),
            Some(GrainEvent::Passivated { .. })
        )),
        "the idle machine must hibernate",
    );

    // Reactivate: the workspace rehydrates with the disk (M2 for both
    // facets); list, read, and remove all operate on the rehydrated tree.
    let reread = machines.grain("ws-box");
    sim.block_on(async move {
        let listed = reread
            .ask(WsList {
                path: String::new(),
            })
            .await
            .expect("ask")
            .expect("list rehydrated root");
        let names: Vec<&str> = listed.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"guest.log"), "{names:?}");
        assert!(names.contains(&"notes"), "{names:?}");
        assert_eq!(
            reread
                .ask(WsRead {
                    path: "notes/hello.txt".into()
                })
                .await
                .expect("ask")
                .expect("read rehydrated file"),
            b"written while hibernated".to_vec()
        );
        reread
            .ask(WsRemove {
                path: "guest.log".into(),
            })
            .await
            .expect("ask")
            .expect("remove");
        assert!(matches!(
            reread
                .ask(WsRead {
                    path: "guest.log".into()
                })
                .await
                .expect("ask"),
            Err(MachineError::Ws(_))
        ));
    });
}

#[test]
fn front_door_death_folds_detached_and_releases_the_pin() {
    let Fixture {
        _scratch,
        base,
        image_len: _image_len,
        sim,
        recorder,
        system,
        machines,
    } = fixture(43);

    let door = system.spawn(DoorStub);
    let door_id = door.id().clone();
    let grain = machines.grain("watched-box");
    let sys = system.clone();
    let base_for_task = base.clone();
    sim.block_on(async move {
        grain
            .ask(provision(&base_for_task))
            .await
            .expect("ask")
            .expect("provision");
        grain
            .ask(Attach {
                principal: "alice".into(),
                front_door: door_id,
            })
            .await
            .expect("ask")
            .expect("attach");
        assert_eq!(
            grain.ask(Status).await.expect("status").attachments.len(),
            1
        );

        // The front-door member dies without detaching: the death watch folds
        // `Detached { FrontDoorLost }` (machine §5.1) and the final capture
        // releases the pin.
        door.tell(Die).await.expect("die");
        sys.sleep(Duration::from_millis(50)).await;
        let status = grain.ask(Status).await.expect("status");
        assert!(
            status.attachments.is_empty(),
            "front-door death must fold Detached for its attachments",
        );
        assert!(!status.vm_running, "the final capture stops the guest");
    });

    // With the pin released, the machine hibernates.
    sim.run();
    assert!(
        recorder.events().iter().any(|e| matches!(
            e.as_app::<GrainEvent>(),
            Some(GrainEvent::Passivated { .. })
        )),
        "the machine must hibernate after the front door died",
    );
}
