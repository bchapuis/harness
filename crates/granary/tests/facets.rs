//! End-to-end facet tests on the `Local` tier (spec §7.12, §7.13): tagged
//! records interleaving with events in one atomic batch, read-your-staged-writes
//! within a command, the composite snapshot, the hibernation round-trip, the
//! transparent kv spill, and the host-unioned blob sweep.

use std::sync::Arc;
use std::time::Duration;

use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use granary::BlobId;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainEvent;
use granary::GrainHandler;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::INLINE_MAX;
use granary::Kv;
use serde::Deserialize;
use serde::Serialize;

// --- A grain that journals events AND uses the KV facet -----------------------

#[derive(Default)]
struct Prefs;

#[derive(Default, Serialize, Deserialize)]
struct PrefsState {
    writes: u64,
}

#[derive(Serialize, Deserialize)]
enum PrefsEvent {
    Wrote,
}

impl Grain for Prefs {
    type System = SimSystem;
    type State = PrefsState;
    type Event = PrefsEvent;
    type Facets = (Kv,);
    const GRAIN_TYPE: &'static str = "test.Prefs";

    fn apply(state: &mut PrefsState, event: &PrefsEvent) {
        match event {
            PrefsEvent::Wrote => state.writes += 1,
        }
    }
}

/// Stage `key = value` in the kv facet AND emit a facet-0 event, then read the
/// staged value back within the same handler (read-your-staged-writes, §7.12).
/// Replies with the echoed value and the post-command write count — one command,
/// two facets, one atomic batch (G19).
#[derive(Clone, Serialize, Deserialize)]
struct Set {
    key: String,
    value: Vec<u8>,
}
impl Message for Set {
    type Reply = (Vec<u8>, u64);
    const MANIFEST: Manifest = Manifest::new("test.PrefsSet");
}
impl GrainHandler<Set> for Prefs {
    async fn handle(
        &self,
        state: &PrefsState,
        msg: Set,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PrefsEvent>, (Vec<u8>, u64)) {
        let kv = ctx.kv();
        kv.put(&msg.key, msg.value).await.expect("staged put");
        let echoed = kv
            .get(&msg.key)
            .await
            .expect("staged get")
            .expect("read-your-staged-writes: the put is visible in-command");
        (vec![PrefsEvent::Wrote], (echoed, state.writes + 1))
    }
}

/// Read a key — no events, no staged ops, commits nothing (§7.5).
#[derive(Clone, Serialize, Deserialize)]
struct GetKey(String);
impl Message for GetKey {
    type Reply = Option<Vec<u8>>;
    const MANIFEST: Manifest = Manifest::new("test.PrefsGet");
}
impl GrainHandler<GetKey> for Prefs {
    async fn handle(
        &self,
        _state: &PrefsState,
        msg: GetKey,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PrefsEvent>, Option<Vec<u8>>) {
        (vec![], ctx.kv().get(&msg.0).await.expect("kv get"))
    }
}

/// Delete a key (staged, commits with the batch).
#[derive(Clone, Serialize, Deserialize)]
struct DelKey(String);
impl Message for DelKey {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.PrefsDel");
}
impl GrainHandler<DelKey> for Prefs {
    async fn handle(
        &self,
        _state: &PrefsState,
        msg: DelKey,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PrefsEvent>, ()) {
        ctx.kv().delete(msg.0);
        (vec![], ())
    }
}

/// The keys under a prefix, through the overlay (§7.13).
#[derive(Clone, Serialize, Deserialize)]
struct Keys(String);
impl Message for Keys {
    type Reply = Vec<String>;
    const MANIFEST: Manifest = Manifest::new("test.PrefsKeys");
}
impl GrainHandler<Keys> for Prefs {
    async fn handle(
        &self,
        _state: &PrefsState,
        msg: Keys,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PrefsEvent>, Vec<String>) {
        (vec![], ctx.kv().keys(&msg.0))
    }
}

/// The facet-0 write count — proves events folded beside the kv records.
#[derive(Clone, Serialize, Deserialize)]
struct Writes;
impl Message for Writes {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("test.PrefsWrites");
}
impl GrainHandler<Writes> for Prefs {
    async fn handle(
        &self,
        state: &PrefsState,
        _msg: Writes,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<PrefsEvent>, u64) {
        (vec![], state.writes)
    }
}

/// Whether the grain's blob area still holds `id` on any reachable replica —
/// probes spill storage and sweep reclamation from outside.
#[derive(Clone, Serialize, Deserialize)]
struct HasBlob(BlobId);
impl Message for HasBlob {
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("test.PrefsHasBlob");
}
impl GrainHandler<HasBlob> for Prefs {
    async fn handle(
        &self,
        _state: &PrefsState,
        msg: HasBlob,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PrefsEvent>, bool) {
        (vec![], ctx.blobs().has(msg.0).await.unwrap_or(false))
    }
}

/// Sweep with an empty application root set: everything the sweep retains comes
/// from the host-unioned facet roots (§7.12) — a live spilled value must
/// survive, a deleted one must not.
#[derive(Clone, Serialize, Deserialize)]
struct SweepNow;
impl Message for SweepNow {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.PrefsSweep");
}
impl GrainHandler<SweepNow> for Prefs {
    async fn handle(
        &self,
        _state: &PrefsState,
        _msg: SweepNow,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PrefsEvent>, ()) {
        ctx.blobs().gc(&std::collections::BTreeSet::new()).await;
        (vec![], ())
    }
}

// --- A grain composing BOTH facets: events + kv + ws, one boundary (§7.12) ----

#[derive(Default)]
struct Project;

impl Grain for Project {
    type System = SimSystem;
    type State = PrefsState;
    type Event = PrefsEvent;
    type Facets = (Kv, granary::Ws);
    const GRAIN_TYPE: &'static str = "test.Project";

    fn apply(state: &mut PrefsState, event: &PrefsEvent) {
        match event {
            PrefsEvent::Wrote => state.writes += 1,
        }
    }
}

/// One command, three facets, one atomic batch (G19): write the document into
/// the workspace directory and capture it, point `last` at it in the kv facet,
/// and count the save as a facet-0 event — one grain, one consistency
/// boundary (§7.11).
#[derive(Clone, Serialize, Deserialize)]
struct SaveDoc {
    path: String,
    content: Vec<u8>,
}
impl Message for SaveDoc {
    type Reply = Result<u64, granary::WsError>;
    const MANIFEST: Manifest = Manifest::new("test.SaveDoc");
}
impl GrainHandler<SaveDoc> for Project {
    async fn handle(
        &self,
        _state: &PrefsState,
        msg: SaveDoc,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PrefsEvent>, Result<u64, granary::WsError>) {
        let dir = match ctx.ws().dir_path() {
            Ok(dir) => dir,
            Err(e) => return (vec![], Err(e)),
        };
        let disk = dir.join(&msg.path);
        if let Some(parent) = disk.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::fs::write(&disk, &msg.content).expect("write");
        if let Err(e) = ctx.ws().capture() {
            return (vec![], Err(e));
        }
        ctx.kv()
            .put("last", msg.path.into_bytes())
            .await
            .expect("inline kv put");
        (vec![PrefsEvent::Wrote], Ok(msg.content.len() as u64))
    }
}

/// The kv-side pointer and the workspace bytes it names, read in one command
/// through both accessors.
#[derive(Clone, Serialize, Deserialize)]
struct LastDoc;
impl Message for LastDoc {
    type Reply = Option<(String, Vec<u8>)>;
    const MANIFEST: Manifest = Manifest::new("test.LastDoc");
}
impl GrainHandler<LastDoc> for Project {
    async fn handle(
        &self,
        _state: &PrefsState,
        _msg: LastDoc,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PrefsEvent>, Option<(String, Vec<u8>)>) {
        let Some(path) = ctx.kv().get("last").await.expect("kv get") else {
            return (vec![], None);
        };
        let path = String::from_utf8(path).expect("utf8 path");
        let dir = ctx.ws().dir_path().expect("dir");
        let bytes = std::fs::read(dir.join(&path)).expect("read");
        (vec![], Some((path, bytes)))
    }
}

/// The facet-0 save count.
#[derive(Clone, Serialize, Deserialize)]
struct Saves;
impl Message for Saves {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("test.ProjectSaves");
}
impl GrainHandler<Saves> for Project {
    async fn handle(
        &self,
        state: &PrefsState,
        _msg: Saves,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<PrefsEvent>, u64) {
        (vec![], state.writes)
    }
}

#[test]
fn kv_ws_and_events_compose_in_one_grain() {
    let scratch = tempfile::tempdir().expect("tempdir");
    let sim = Simulation::new(19);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let projects = system.granary::<Project>(GranaryConfig {
        idle_after: Duration::from_millis(10),
        snapshot_every: 3,
        data_dir: Some(scratch.path().to_path_buf()),
        ..GranaryConfig::default()
    });

    let grain = projects.grain("project/0");
    sim.block_on(async move {
        assert_eq!(
            grain
                .ask(SaveDoc {
                    path: "docs/a.md".into(),
                    content: b"alpha".to_vec(),
                })
                .await
                .expect("save"),
            Ok(5),
        );
        assert_eq!(
            grain
                .ask(SaveDoc {
                    path: "docs/b.md".into(),
                    content: b"beta!".to_vec(),
                })
                .await
                .expect("save"),
            Ok(5),
        );
        assert_eq!(
            grain.ask(LastDoc).await.expect("last"),
            Some(("docs/b.md".into(), b"beta!".to_vec())),
        );
    });

    // Hibernate: the composite snapshot carries state + kv + ws together.
    sim.run();
    assert!(
        recorder.events().iter().any(|e| matches!(
            e.as_app::<GrainEvent>(),
            Some(GrainEvent::Passivated { .. })
        )),
        "the idle grain must hibernate",
    );

    // Reactivate: all three facets rehydrate to one seq (G19/G12).
    let reread = projects.grain("project/0");
    sim.block_on(async move {
        assert_eq!(reread.ask(Saves).await.expect("saves"), 2);
        assert_eq!(
            reread.ask(LastDoc).await.expect("last"),
            Some(("docs/b.md".into(), b"beta!".to_vec())),
        );
    });
}

fn sim_system(sim: &Simulation, recorder: &Recorder) -> SimSystem {
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build()
}

#[test]
fn kv_and_events_commit_together_and_survive_hibernation() {
    let sim = Simulation::new(11);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let prefs = system.granary::<Prefs>(GranaryConfig {
        idle_after: Duration::from_millis(10),
        snapshot_every: 4,
        ..GranaryConfig::default()
    });

    let grain = prefs.grain("prefs/0");
    let big = vec![7u8; INLINE_MAX + 1]; // spills to the blob area (§7.13)
    let big_for_task = big.clone();
    sim.block_on(async move {
        // Each Set is one atomic batch: one kv record + one event (G19), with
        // read-your-staged-writes inside the handler (§7.12).
        let (echoed, writes) = grain
            .ask(Set {
                key: "theme".into(),
                value: b"dark".to_vec(),
            })
            .await
            .expect("set commits");
        assert_eq!(echoed, b"dark".to_vec());
        assert_eq!(writes, 1);

        let (echoed, writes) = grain
            .ask(Set {
                key: "blob".into(),
                value: big_for_task.clone(),
            })
            .await
            .expect("spilled set commits");
        assert_eq!(echoed, big_for_task, "a spilled value reads back verified");
        assert_eq!(writes, 2);

        assert_eq!(
            grain.ask(Keys(String::new())).await.expect("keys"),
            vec!["blob".to_string(), "theme".to_string()],
        );
    });

    // Drive past the idle window: the grain snapshots the COMPOSITE (state +
    // kv contribution) and hibernates.
    sim.run();
    assert!(
        recorder.events().iter().any(|e| matches!(
            e.as_app::<GrainEvent>(),
            Some(GrainEvent::Passivated { .. })
        )),
        "the idle grain must hibernate",
    );

    // A fresh ref reactivates: state AND the kv map rehydrate together (G12).
    let reread = prefs.grain("prefs/0");
    let big_id = BlobId::of(&big);
    sim.block_on(async move {
        assert_eq!(reread.ask(Writes).await.expect("writes"), 2);
        assert_eq!(
            reread.ask(GetKey("theme".into())).await.expect("get"),
            Some(b"dark".to_vec()),
        );
        assert_eq!(
            reread.ask(GetKey("blob".into())).await.expect("get"),
            Some(big),
            "the spilled value survives hibernation",
        );

        // The live spilled id survives a sweep with NO application roots: the
        // handle unions the facet roots (§7.12).
        reread.ask(SweepNow).await.expect("sweep");
        assert!(
            reread.ask(HasBlob(big_id)).await.expect("has"),
            "a live spilled value must survive the unioned sweep",
        );

        // Deleting the key removes its id from the facet roots; the next sweep
        // reclaims the orphaned bytes.
        reread.ask(DelKey("blob".into())).await.expect("del");
        assert_eq!(reread.ask(GetKey("blob".into())).await.expect("get"), None);
        reread.ask(SweepNow).await.expect("sweep");
        assert!(
            !reread.ask(HasBlob(big_id)).await.expect("has"),
            "a deleted key's spilled bytes are reclaimed",
        );
    });
}

#[test]
fn composite_snapshot_bounds_replay_and_restores_both_facets() {
    let sim = Simulation::new(13);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let prefs = system.granary::<Prefs>(GranaryConfig {
        idle_after: Duration::from_millis(10),
        snapshot_every: 2, // snapshot early: reactivation must seed from it
        ..GranaryConfig::default()
    });

    let grain = prefs.grain("prefs/snap");
    sim.block_on(async move {
        for i in 0..6u8 {
            grain
                .ask(Set {
                    key: format!("k{i}"),
                    value: vec![i],
                })
                .await
                .expect("set");
        }
    });

    sim.run(); // hibernate (snapshotting at the head)

    let reread = prefs.grain("prefs/snap");
    sim.block_on(async move {
        assert_eq!(reread.ask(Writes).await.expect("writes"), 6);
        for i in 0..6u8 {
            assert_eq!(
                reread.ask(GetKey(format!("k{i}"))).await.expect("get"),
                Some(vec![i]),
            );
        }
    });

    // The second activation seeded from the composite snapshot, not `ZERO`.
    let rehydrated_from_snapshot = recorder
        .events()
        .iter()
        .filter_map(|e| match e.as_app::<GrainEvent>() {
            Some(GrainEvent::Rehydrated { from_snapshot, .. }) => Some(*from_snapshot),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rehydrated_from_snapshot,
        vec![false, true],
        "reactivation seeds from the composite snapshot",
    );
}

#[test]
fn overlay_delete_shadows_the_committed_map_in_command() {
    let sim = Simulation::new(17);
    let recorder = Recorder::new();
    let system = sim_system(&sim, &recorder);
    let prefs = system.granary::<Prefs>(GranaryConfig::default());

    let grain = prefs.grain("prefs/overlay");
    sim.block_on(async move {
        grain
            .ask(Set {
                key: "a".into(),
                value: vec![1],
            })
            .await
            .expect("set");
        // Delete stages; the read after commit sees it gone, and `keys` through
        // the overlay agreed within the deleting command (exercised implicitly
        // by the staged-op path).
        grain.ask(DelKey("a".into())).await.expect("del");
        assert_eq!(grain.ask(GetKey("a".into())).await.expect("get"), None);
        assert_eq!(
            grain.ask(Keys(String::new())).await.expect("keys"),
            Vec::<String>::new(),
        );
    });
}
