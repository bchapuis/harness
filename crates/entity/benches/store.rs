//! What committing an entity's events costs.
//!
//! Every state change an entity makes is journaled through a store, so `store_record` is
//! the write path under the whole system: an agent session's transcript reaches disk one
//! of these per commit. Two stores implement it, and running both is what separates the
//! costs:
//!
//! - `memory` is the in-memory slot map alone: no framing, no file, no fsync. It measures
//!   what the store layer itself adds.
//! - `file` is that plus a framed, checksummed, fsynced append.
//!
//! The difference between them is durability. The *allocations* are the store layer's
//! own doing: the file store's per-commit allocation count should not exceed the memory
//! store's by more than the framing needs.
//!
//! `read_head` is here because activating an entity asks its store for the head sequence
//! number, and how much work that costs decides part of a session's time-to-first-token.

use actor_serialization::Codec;
use actor_serialization::JsonCodec;
use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;
use entity::EntityName;
use entity::Seq;
use entity::store::EntityStore;
use entity::store::FileEntityStore;
use entity::store::MemoryEntityStore;
use entity::store::Term;
use entity::store::WriteKind;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// The shard every benchmark writes to. Which one is immaterial; holding it fixed keeps
/// the name-to-segment lookup on its hit path, which is the steady state.
const SHARD: u32 = 0;

/// One commit's worth of events, at a size typical of a transcript record.
fn batch(records: usize, bytes: usize) -> Vec<Vec<u8>> {
    (0..records).map(|_| vec![0x5a; bytes]).collect()
}

fn name() -> EntityName {
    EntityName::new("bench.session", "s-1")
}

/// One commit against `store`, with the batch built outside the measured region.
///
/// `store_record` takes its events by value, so a loop that reuses one batch has to clone
/// it. That clone is the benchmark's cost, not the store's, and inside the timed closure
/// it would roughly double the reported allocation count of the memory store.
/// `with_inputs` hands each iteration its own batch, built outside the measurement.
///
/// The ack is bound and dropped rather than checked. `store_record` is `#[must_use]` so
/// a real caller cannot let a refusal fall on the floor (**E14**), but both stores settle
/// synchronously: by the time the call returns there is nothing left to wait for and
/// nothing a check would change.
fn commit(bencher: Bencher, store: impl EntityStore, records: usize) {
    let entity = name();
    let events = batch(records, 256);
    let mut head = Seq::ZERO;
    bencher
        .counter(ItemsCount::new(records))
        .with_inputs(|| events.clone())
        .bench_local_values(|events| {
            let _ = black_box(store.store_record(
                SHARD,
                black_box(&entity),
                head,
                Term::ZERO,
                events,
                WriteKind::Append,
            ));
            head = Seq::new(head.value() + records as u64);
        });
}

/// The in-memory store: slot map, name lookup, and the ack. No disk.
///
/// The floor `file` is measured against: cost the store layer adds regardless of
/// durability.
#[divan::bench(args = [1, 8, 64])]
fn memory(bencher: Bencher, records: usize) {
    commit(bencher, MemoryEntityStore::new(), records);
}

/// The file store: everything `memory` does, plus framing the op and fsyncing it.
///
/// The timing is fsync-bound and therefore a property of the filesystem; the allocation
/// count is not, and is the number that moves when the write path stops copying.
#[divan::bench(args = [1, 8, 64])]
fn file(bencher: Bencher, records: usize) {
    let dir = tempfile::tempdir().expect("scratch dir");
    commit(
        bencher,
        FileEntityStore::open(dir.path(), JsonCodec.name()).expect("open the store"),
        records,
    );
}

/// A store holding `history` single-record commits for one entity.
fn seeded(history: usize) -> (MemoryEntityStore, EntityName) {
    let store = MemoryEntityStore::new();
    let entity = name();
    let events = batch(1, 256);
    for i in 0..history {
        let _ = store.store_record(
            SHARD,
            &entity,
            Seq::new(i as u64),
            Term::ZERO,
            events.clone(),
            WriteKind::Append,
        );
    }
    (store, entity)
}

/// Asking an entity for its head, swept over how much history it has.
///
/// Activation asks this before the entity can serve anything, so its shape is part of a
/// session's time-to-first-token. The sweep is the point: a flat line means the answer is
/// reached directly, a climbing one means the store materializes the journal to find it.
/// Compare against `read_full` below, the same question asked the expensive way.
#[divan::bench(args = [1, 100, 1_000])]
fn head(bencher: Bencher, history: usize) {
    let (store, entity) = seeded(history);
    bencher.bench_local(|| black_box(store.head(SHARD, black_box(&entity))));
}

/// The same entity read in full, every occupied slot's bytes plus the snapshot.
///
/// A recovering leader needs all of it to merge a write quorum by highest-term-per-slot
/// (**E14**), so this cost is real and stays. It is the baseline that shows what asking
/// only for the head saves.
#[divan::bench(args = [1, 100, 1_000])]
fn read_full(bencher: Bencher, history: usize) {
    let (store, entity) = seeded(history);
    bencher.bench_local(|| black_box(store.read(SHARD, black_box(&entity))));
}
