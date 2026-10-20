//! A failed store is refused, not read as empty (entity §7, storage §11).
//!
//! The `Local` tier over a file-backed store (storage §3.2) has one store and no quorum to
//! outvote it. After that store fails, its unfenced reads describe an entity with
//! no history; served as-is, the next activation would rehydrate from nothing and
//! answer a fresh default state as the entity's truth, and a gateway read would
//! serve an empty page as its history. §7 allows staleness and forbids
//! invention, so the journal answers `Unavailable` to both (the unit tests in
//! `memory.rs` pin that outcome at the seam); here both public paths are seen
//! refusing, which without the gate they do not.

use std::fs;
use std::sync::Arc;
use std::time::Duration;

use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_serialization::Codec;
use actor_serialization::JsonCodec;
use actor_simulation::Recorder;
use actor_simulation::Simulation;
use entity::Entity;
use entity::EntityConfig;
use entity::EntityEvent;
use entity::EntityExt;
use entity::EntityName;
use entity::Seq;
use entity::store::EntityStore;
use entity::store::FileEntityStore;
use entity::store::StoreAck;
use entity::store::Term;
use entity::store::WriteKind;
mod support;

use support::Add;
use support::CounterEntity;
use support::ReadCount;

#[test]
fn a_failed_store_is_unavailable_not_empty() {
    let sim = Simulation::new(29);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();

    // The store is opened by hand rather than through `FileEntityStore::factory`, so
    // the test keeps a handle to poison it after the system has used it.
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FileEntityStore::open(dir.path(), JsonCodec.name()).expect("open"));
    let shared: Arc<dyn EntityStore> = store.clone();
    let counters = system.entities::<CounterEntity>(EntityConfig {
        idle_after: Duration::from_millis(10),
        entity_store: Some(Arc::new(move |_, _| Arc::clone(&shared))),
        ..EntityConfig::default()
    });

    let counter = counters.entity("counter/0");
    sim.block_on(async move {
        for _ in 0..3 {
            counter.ask(Add(1)).await.expect("add commits");
        }
    });
    // Drive past the idle window: the entity hibernates, so the reads below go
    // back to the store.
    sim.run();
    let deactivated = |recorder: &Recorder| {
        recorder
            .events()
            .iter()
            .filter(|e| {
                matches!(
                    e.as_app::<EntityEvent>(),
                    Some(EntityEvent::Deactivated { .. })
                )
            })
            .count()
    };
    assert_eq!(deactivated(&recorder), 1, "the idle entity must hibernate");

    // Poison the store the way a failing device would: take the segment area away,
    // then ask it to persist an entity that needs a new segment file.
    fs::remove_dir_all(dir.path().join("segments")).unwrap();
    let doomed = EntityName::new(CounterEntity::ENTITY_TYPE, "counter/doomed");
    assert_eq!(
        store.store_record(
            0,
            &doomed,
            Seq::ZERO,
            Term::ZERO,
            vec![b"x".to_vec()],
            WriteKind::Append
        ),
        StoreAck::Failed,
    );
    assert!(store.failure().is_some(), "the store is poisoned");

    // The gateway's non-activating read (§7): refused, never an empty history.
    // The gateway's floor recovery retries a failed `head` until the read's
    // deadline, which the redirect loop reports as the last leader hint.
    let reread = counters.entity("counter/0");
    let history = sim
        .block_on(async move { reread.events(Seq::ZERO, 100).await })
        .map(|history| history.events.len());
    assert!(
        history.is_err(),
        "a failed store's history is refused, not served empty: {history:?}",
    );

    // Activation: `head` is the rehydration barrier, and it refuses, so the entity
    // never rehydrates from nothing and never serves a default count.
    let reread = counters.entity("counter/0");
    let count = sim.block_on(async move { reread.ask(ReadCount).await });
    assert!(
        count.is_err(),
        "an activation over a failed store must fail, not serve a fresh state: {count:?}",
    );
    let rehydrated = recorder
        .events()
        .iter()
        .filter(|e| {
            matches!(
                e.as_app::<EntityEvent>(),
                Some(EntityEvent::Rehydrated { .. })
            )
        })
        .count();
    assert_eq!(
        rehydrated, 1,
        "only the first activation rehydrated; none folded a state from the failed store",
    );
}
