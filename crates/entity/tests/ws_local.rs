//! End-to-end workspace facet tests on the `Local` tier (spec §7.11): captured
//! file deltas committing atomically with the command, zero-record captures of
//! an unchanged tree, exclusion of regenerable caches, the checkpoint manifest +
//! delta-record rehydration round-trip, and the E20 discard-and-rebuild path.

use std::sync::Arc;
use std::time::Duration;

use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use entity::Entity;
use entity::EntityConfig;
use entity::EntityCtx;
use entity::EntityEvent;
use entity::EntityExt;
use entity::EntityHandler;
use entity::NoEvent;
use entity::Ws;
use entity::WsError;
use serde::Deserialize;
use serde::Serialize;

// --- An entity whose durable state is entirely its workspace directory ----------

#[derive(Default)]
struct Studio;

impl Entity for Studio {
    type System = SimSystem;
    type State = ();
    type Event = NoEvent;
    type Facets = (Ws,);
    const ENTITY_TYPE: &'static str = "test.WsStudio";

    fn apply(_state: &mut (), event: &NoEvent) {
        event.unreachable()
    }
}

/// Write a file into the workspace directory (as a tool would, out of band) and
/// capture — the delta record joins this command's batch (E19).
#[derive(Clone, Serialize, Deserialize)]
struct Put {
    path: String,
    content: Vec<u8>,
}
impl Message for Put {
    type Reply = Result<usize, WsError>;
    const MANIFEST: Manifest = Manifest::new("test.WsPut");
}
impl EntityHandler<Put> for Studio {
    async fn handle(
        &self,
        _state: &(),
        msg: Put,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<NoEvent>, Result<usize, WsError>) {
        let dir = match ctx.ws().dir_path() {
            Ok(dir) => dir,
            Err(e) => return (vec![], Err(e)),
        };
        let disk = dir.join(&msg.path);
        if let Some(parent) = disk.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::fs::write(&disk, &msg.content).expect("write");
        let capture = match ctx.ws().capture() {
            Ok(capture) => capture,
            Err(e) => return (vec![], Err(e)),
        };
        (vec![], Ok(capture.written + capture.removed))
    }
}

/// Delete a path from the directory and capture the removal.
#[derive(Clone, Serialize, Deserialize)]
struct Drop_ {
    path: String,
}
impl Message for Drop_ {
    type Reply = Result<usize, WsError>;
    const MANIFEST: Manifest = Manifest::new("test.WsDrop");
}
impl EntityHandler<Drop_> for Studio {
    async fn handle(
        &self,
        _state: &(),
        msg: Drop_,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<NoEvent>, Result<usize, WsError>) {
        let dir = ctx.ws().dir_path().expect("dir");
        let _ = std::fs::remove_file(dir.join(&msg.path));
        let capture = match ctx.ws().capture() {
            Ok(capture) => capture,
            Err(e) => return (vec![], Err(e)),
        };
        (vec![], Ok(capture.written + capture.removed))
    }
}

/// Read a file straight off the materialized directory — a pure read: no
/// capture, no record, no commit (§7.5).
#[derive(Clone, Serialize, Deserialize)]
struct Get {
    path: String,
}
impl Message for Get {
    type Reply = Option<Vec<u8>>;
    const MANIFEST: Manifest = Manifest::new("test.WsGet");
}
impl EntityHandler<Get> for Studio {
    async fn handle(
        &self,
        _state: &(),
        msg: Get,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<NoEvent>, Option<Vec<u8>>) {
        let dir = ctx.ws().dir_path().expect("dir");
        (vec![], std::fs::read(dir.join(&msg.path)).ok())
    }
}

fn committed_count(recorder: &Recorder) -> usize {
    recorder
        .events()
        .iter()
        .filter(|e| {
            matches!(
                e.as_app::<EntityEvent>(),
                Some(EntityEvent::Committed { .. })
            )
        })
        .count()
}

fn studio_system(
    seed: u64,
    data_dir: &std::path::Path,
    snapshot_every: u64,
) -> (Simulation, Recorder, entity::Entities<Studio>) {
    let sim = Simulation::new(seed);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let studios = system.entities::<Studio>(EntityConfig {
        idle_after: Duration::from_millis(10),
        snapshot_every,
        data_dir: Some(data_dir.to_path_buf()),
        ..EntityConfig::default()
    });
    (sim, recorder, studios)
}

#[test]
fn captured_writes_survive_hibernation_and_unchanged_trees_commit_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (sim, recorder, studios) = studio_system(7, dir.path(), 2);

    let entity = studios.entity("studio/0");
    sim.block_on(async move {
        assert_eq!(
            entity
                .ask(Put {
                    path: "src/main.rs".into(),
                    content: b"fn main() {}".to_vec()
                })
                .await
                .expect("put"),
            Ok(1),
            "one changed file staged",
        );
        assert_eq!(
            entity
                .ask(Put {
                    path: "notes/todo.md".into(),
                    content: b"- ship it".to_vec()
                })
                .await
                .expect("put"),
            Ok(1),
        );
        // Re-writing identical bytes: content hash unchanged, nothing staged,
        // nothing committed (the read path, §7.5).
        assert_eq!(
            entity
                .ask(Put {
                    path: "src/main.rs".into(),
                    content: b"fn main() {}".to_vec()
                })
                .await
                .expect("noop put"),
            Ok(0),
            "an unchanged tree stages no records",
        );
    });
    assert_eq!(
        committed_count(&recorder),
        2,
        "exactly the two changing commands committed",
    );

    // Hibernate: checkpoint chunks go to blobs, the manifest joins the
    // composite snapshot, and the activation drops its materialization.
    sim.run();
    assert!(
        recorder.events().iter().any(|e| matches!(
            e.as_app::<EntityEvent>(),
            Some(EntityEvent::Deactivated { .. })
        )),
        "the idle entity must hibernate",
    );

    // Reactivate: the directory rematerializes from the checkpoint manifest
    // plus replayed delta records, byte-identical (F2).
    let reread = studios.entity("studio/0");
    sim.block_on(async move {
        assert_eq!(
            reread
                .ask(Get {
                    path: "src/main.rs".into()
                })
                .await
                .expect("get"),
            Some(b"fn main() {}".to_vec()),
            "acknowledged workspace writes survive hibernation (E12)",
        );
        assert_eq!(
            reread
                .ask(Get {
                    path: "notes/todo.md".into()
                })
                .await
                .expect("get"),
            Some(b"- ship it".to_vec()),
        );
    });
}

#[test]
fn deletions_are_captured_and_survive_rehydration() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (sim, _recorder, studios) = studio_system(11, dir.path(), 2);

    let entity = studios.entity("studio/1");
    sim.block_on(async move {
        entity
            .ask(Put {
                path: "a.txt".into(),
                content: b"a".to_vec(),
            })
            .await
            .expect("put")
            .expect("staged");
        entity
            .ask(Put {
                path: "b.txt".into(),
                content: b"b".to_vec(),
            })
            .await
            .expect("put")
            .expect("staged");
        assert_eq!(
            entity
                .ask(Drop_ {
                    path: "a.txt".into()
                })
                .await
                .expect("drop"),
            Ok(1),
            "one removal staged",
        );
    });

    sim.run(); // hibernate

    let reread = studios.entity("studio/1");
    sim.block_on(async move {
        assert_eq!(
            reread
                .ask(Get {
                    path: "a.txt".into()
                })
                .await
                .expect("get"),
            None,
            "a captured deletion survives rehydration",
        );
        assert_eq!(
            reread
                .ask(Get {
                    path: "b.txt".into()
                })
                .await
                .expect("get"),
            Some(b"b".to_vec()),
        );
    });
}

#[test]
fn excluded_trees_are_never_captured() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (sim, recorder, studios) = studio_system(13, dir.path(), 64);

    let entity = studios.entity("studio/2");
    sim.block_on(async move {
        // A write landing only under an excluded tree stages nothing.
        assert_eq!(
            entity
                .ask(Put {
                    path: "target/debug/app".into(),
                    content: b"\x7fELF".to_vec()
                })
                .await
                .expect("put"),
            Ok(0),
            "excluded trees stage no records",
        );
    });
    assert_eq!(committed_count(&recorder), 0, "nothing committed");

    sim.run(); // hibernate

    let reread = studios.entity("studio/2");
    sim.block_on(async move {
        assert_eq!(
            reread
                .ask(Get {
                    path: "target/debug/app".into()
                })
                .await
                .expect("get"),
            None,
            "excluded content is not restored on a fresh materialization",
        );
    });
}
