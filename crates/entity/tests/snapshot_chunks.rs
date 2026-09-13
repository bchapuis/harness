//! Facet 0's state through the blob area (entity §9).
//!
//! A composite snapshot used to carry the entity's whole encoded `State` inline, so
//! a snapshot cost what the entity had accumulated, and on the `Quorum` tier that
//! whole payload went to every replica each time the threshold came round
//! (`docs/hardware-envelope.md` §3.9). Past a threshold the state now goes through
//! the same content-addressed chunking the checkpointing facets use, and the record
//! carries a manifest of ids.
//!
//! These tests assert what makes that a win rather than a rearrangement: the round
//! trip is lossless, the snapshot *record* stops growing with the state, and a
//! snapshot after a small append stores only the chunks the append touched.

use std::sync::Arc;
use std::time::Duration;

use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use entity::Entity;
use entity::EntityConfig;
use entity::EntityCtx;
use entity::EntityExt;
use entity::EntityHandler;
use entity::EntityName;
use entity::store::EntityStore;
use entity::store::MemoryEntityStore;
use serde::Deserialize;
use serde::Serialize;

// --- An entity whose state grows, the shape facet-0 chunking exists for ---------

/// A folded transcript: state grows by an entry per command and is never
/// rewritten. The agent entity's shape (agent §7), reduced to what matters here.
#[derive(Default)]
struct Transcript;

#[derive(Default, Serialize, Deserialize)]
struct TranscriptState {
    entries: Vec<String>,
}

#[derive(Serialize, Deserialize)]
enum TranscriptEvent {
    Appended(String),
}

impl Entity for Transcript {
    type System = SimSystem;
    type State = TranscriptState;
    type Event = TranscriptEvent;
    type Facets = ();
    const ENTITY_TYPE: &'static str = "test.Transcript";

    fn apply(state: &mut TranscriptState, event: &TranscriptEvent) {
        match event {
            TranscriptEvent::Appended(entry) => state.entries.push(entry.clone()),
        }
    }

    fn register(r: &mut entity::EntityRegistry<Self>) {
        r.accept::<Append>();
        r.accept::<Len>();
    }
}

/// Append an entry of `bytes` filler, distinct per call so nothing dedups by
/// accident.
#[derive(Clone, Serialize, Deserialize)]
struct Append {
    nth: usize,
    bytes: usize,
}

impl Message for Append {
    type Reply = usize; // the post-command entry count
    const MANIFEST: Manifest = Manifest::new("test.Append");
}

impl EntityHandler<Append> for Transcript {
    async fn handle(
        &self,
        state: &TranscriptState,
        msg: Append,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<TranscriptEvent>, usize) {
        let entry = filler(msg.nth, msg.bytes);
        (
            vec![TranscriptEvent::Appended(entry)],
            state.entries.len() + 1,
        )
    }
}

/// The total encoded length of the transcript.
#[derive(Clone, Serialize, Deserialize)]
struct Len;

impl Message for Len {
    type Reply = usize;
    const MANIFEST: Manifest = Manifest::new("test.Len");
}

impl EntityHandler<Len> for Transcript {
    async fn handle(
        &self,
        state: &TranscriptState,
        _msg: Len,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<TranscriptEvent>, usize) {
        (Vec::new(), state.entries.iter().map(String::len).sum())
    }
}

/// `bytes` of deterministic, high-entropy filler, distinct for each `nth`.
///
/// Entropy matters: the rolling hash finds boundaries in varied content, and a run
/// of one repeated byte would cut only at the maximum chunk size, the degenerate
/// case rather than the one under test.
fn filler(nth: usize, bytes: usize) -> String {
    let mut out = String::with_capacity(bytes);
    let mut z = (nth as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    while out.len() < bytes {
        z = z
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        out.push_str(&format!("{z:016x}"));
    }
    out.truncate(bytes);
    out
}

// --- The fixture --------------------------------------------------------------

/// A single-node deployment over a store the test keeps a handle on, so it can
/// read back the snapshot record and the blob area the entity actually wrote.
struct Fixture {
    sim: Simulation,
    store: Arc<MemoryEntityStore>,
    entities: entity::Entities<Transcript>,
}

fn fixture(snapshot_every: u64) -> Fixture {
    let sim = Simulation::new(7);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let store = Arc::new(MemoryEntityStore::new());
    let shared = Arc::clone(&store);
    let entities = system.entities::<Transcript>(EntityConfig {
        // One shard, so the test's own store lookups need no name-to-shard hash.
        shards: 1,
        idle_after: Duration::from_millis(10),
        snapshot_every,
        entity_store: Some(Arc::new(move |_ty, _node| {
            Arc::clone(&shared) as Arc<dyn EntityStore>
        })),
        ..EntityConfig::default()
    });
    Fixture {
        sim,
        store,
        entities,
    }
}

impl Fixture {
    fn append(&self, name: &str, nth: usize, bytes: usize) -> usize {
        let entity = self.entities.entity(name);
        self.sim.block_on(async move {
            entity
                .ask(Append { nth, bytes })
                .await
                .expect("append commits")
        })
    }

    fn len(&self, name: &str) -> usize {
        let entity = self.entities.entity(name);
        self.sim
            .block_on(async move { entity.ask(Len).await.expect("read commits") })
    }

    /// The durable snapshot record's size in bytes, or 0 if none was written.
    fn snapshot_bytes(&self, name: &str) -> usize {
        self.store
            .snapshot(0, &EntityName::new(Transcript::ENTITY_TYPE, name))
            .map(|(_seq, bytes)| bytes.len())
            .unwrap_or(0)
    }

    /// How many blobs the entity's area holds.
    fn blob_count(&self, name: &str) -> usize {
        self.store
            .blob_ids(0, &EntityName::new(Transcript::ENTITY_TYPE, name))
            .len()
    }
}

// --- The tests -----------------------------------------------------------------

#[test]
fn a_large_state_survives_the_blob_round_trip() {
    // 24 entries of 32 KiB is ~768 KiB of state, well past the inline threshold.
    let f = fixture(4);
    for nth in 0..24 {
        f.append("t/0", nth, 32 * 1024);
    }
    let before = f.len("t/0");

    // Drive past the idle window: the entity snapshots and hibernates.
    f.sim.run();
    assert!(
        f.blob_count("t/0") > 1,
        "a state this size must have gone through the blob area"
    );

    // A fresh activation rebuilds from the manifest plus its chunks (E12).
    assert_eq!(
        f.len("t/0"),
        before,
        "rehydration from a chunked snapshot must reproduce the state exactly"
    );
}

#[test]
fn a_small_state_stays_inline_and_touches_no_blob() {
    // Under the threshold the state still rides inline: no chunk puts, no roots
    // to keep, no blobs to fetch on the way back.
    let f = fixture(4);
    for nth in 0..8 {
        f.append("t/1", nth, 512);
    }
    let before = f.len("t/1");
    f.sim.run();

    assert_eq!(
        f.blob_count("t/1"),
        0,
        "a small state must not reach the blob area"
    );
    assert!(f.snapshot_bytes("t/1") > before, "the state rode inline");
    assert_eq!(f.len("t/1"), before);
}

#[test]
fn the_snapshot_record_stops_growing_with_the_state() {
    // The record is what a `Quorum` snapshot broadcasts to every replica, so its
    // size is the per-snapshot wire cost. Once the state is chunked, the record
    // holds a manifest: it grows with the *number of chunks*, at 32 bytes an id,
    // not with the state.
    let f = fixture(4);
    for nth in 0..64 {
        f.append("t/2", nth, 32 * 1024);
    }
    f.sim.run();

    let state = f.len("t/2");
    let record = f.snapshot_bytes("t/2");
    assert!(
        state >= 2 * 1024 * 1024,
        "the fixture must build a big state"
    );
    assert!(
        record * 50 < state,
        "the snapshot record is {record} bytes for {state} bytes of state: \
         it should be a manifest, not the state"
    );
}

#[test]
fn a_small_append_stores_only_the_chunks_it_disturbed() {
    // A snapshot after one more entry must add a couple of blobs, not re-store the
    // whole transcript. Content-defined boundaries make this true: cut at fixed
    // offsets, the length prefix ahead of the appended entry would shift every
    // chunk after it.
    let f = fixture(1);
    for nth in 0..48 {
        f.append("t/3", nth, 32 * 1024);
    }
    f.sim.run();
    let settled = f.blob_count("t/3");
    assert!(settled > 8, "the transcript must be many chunks: {settled}");

    // One more entry, then another snapshot.
    f.append("t/3", 100, 32 * 1024);
    f.sim.run();
    let after = f.blob_count("t/3");

    let added = after - settled;
    assert!(
        added <= 3,
        "an append re-stored {added} chunks; only the disturbed tail should be new"
    );
    assert_eq!(
        f.len("t/3"),
        49 * 32 * 1024,
        "and the state still rehydrates whole"
    );
}
