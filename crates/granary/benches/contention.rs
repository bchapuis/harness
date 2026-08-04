//! Whether the node's shared locks show under many-core concurrency.
//!
//! Two structures in this crate take one mutex on a path that every grain on the node
//! goes through, and both were left alone deliberately: sharding a lock is easy, and
//! doing it without a number is how a codebase accumulates complexity that never paid
//! for itself. This file produces the numbers.
//!
//! 1. **[`HostCache`]'s handle map.** One `Mutex<Generations<..>>` per grain type,
//!    taken on *every* call including a cache hit. The deployment that would feel it
//!    is the gateway: one long-lived process fronting every tenant on a many-core box
//!    (`docs/hardware-envelope.md` §3.4), where every request begins with this lookup.
//!    The critical section is a hash lookup, so the question is purely whether the
//!    lock itself becomes the serial point.
//!
//! 2. **[`FileGrainStore`]'s manifest and segment maps.** The manifest lock is the
//!    interesting one, and reading the code says why: `segment_id` appends to the
//!    manifest's log — an fsync — *while holding the mutex*, so every grain whose
//!    segment is being created for the first time serializes behind every other one's
//!    flush. On the host `benches/flush.rs` measured, that flush is ~9.4 ms. Whether
//!    that matters depends on how often a node creates grains it has never seen, which
//!    is exactly what a create storm does.
//!
//! Read the **item/s counters**, not the wall times: per-iteration time grows with the
//! thread count by construction, and the question is whether aggregate throughput
//! keeps up. A structure that scales shows item/s climbing with threads; one that is
//! serialized shows it flat, and one that is actively contended shows it falling.
//!
//! Run with `cargo bench -p granary --bench contention`.

use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use actor_core::LocalSystemBuilder;
use actor_runtime::OsEntropy;
use actor_runtime::TokioClock;
use actor_runtime::TokioSpawner;
use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;
use granary::FileGrainStore;
use granary::Grain;
use granary::GrainName;
use granary::GrainStore;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::NoEvent;
use granary::Seq;
use granary::StoreAck;
use granary::Term;
use granary::WriteKind;

fn main() {
    divan::main();
}

/// The thread counts swept everywhere below: one, a small box, and past the core
/// count of most nodes this targets.
const THREADS: &[usize] = &[1, 2, 4, 8, 16];

/// How many distinct names each measurement spreads its work over. Large enough that
/// threads are not all hammering one key (which would measure the cache line, not the
/// lock) and that the `HostCache`'s two generations both stay populated.
const NAMES: usize = 4096;

/// Lookups each thread performs inside one timed iteration.
///
/// Not a tuning knob — a correctness requirement for the measurement. Spawning a
/// thread costs ~20 µs on this host and a locked hash lookup costs tens of
/// *nanoseconds*, so a bench that spawned a thread per operation would report thread
/// creation under a lock's name and would show contention that is not there (and hide
/// contention that is). Batching puts three orders of magnitude of real work between
/// the barrier and the join, leaving spawn cost as rounding error.
const OPS: usize = 20_000;

// --- 1. The host cache's handle map ------------------------------------------------

/// A grain with no state and no behavior: this measures the lookup in front of the
/// grain, never the grain.
#[derive(Default)]
struct Cached;

impl Grain for Cached {
    type System = actor_core::LocalSystem<TokioClock, OsEntropy, TokioSpawner>;
    type State = ();
    type Event = NoEvent;
    type Facets = ();
    const GRAIN_TYPE: &'static str = "bench.Cached";

    fn apply(_state: &mut (), event: &NoEvent) {
        event.unreachable()
    }
}

/// Concurrent lookups through the real [`HostCache`] mutex.
///
/// `is_cached` is the narrowest public path that takes it: a hash lookup under the
/// lock and nothing else. That is deliberate — a wider call would fold the shard-map
/// read and the leader check into the number, and those sit behind *different* locks.
/// What is wanted here is the cost of this one.
#[divan::bench(args = THREADS)]
fn host_cache_lookup(bencher: Bencher, threads: usize) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.max(1))
        .enable_all()
        .build()
        .expect("runtime");
    let _guard = runtime.enter();
    let system = LocalSystemBuilder::new(
        TokioClock::new(),
        OsEntropy::new(),
        TokioSpawner::new(runtime.handle().clone()),
    )
    .build();
    let granary: Granary<Cached> = system.granary(GranaryConfig {
        shards: 1,
        replication_factor: 1,
        ..GranaryConfig::default()
    });
    let keys: Vec<String> = (0..NAMES).map(|i| format!("grain-{i}")).collect();

    let cursor = AtomicU64::new(0);
    bencher.counter(ItemsCount::new(threads * OPS)).bench(|| {
        let start = Arc::new(Barrier::new(threads));
        std::thread::scope(|scope| {
            for _ in 0..threads {
                let start = Arc::clone(&start);
                let granary = &granary;
                let keys = &keys;
                let cursor = &cursor;
                scope.spawn(move || {
                    let base = cursor.fetch_add(OPS as u64, Ordering::Relaxed) as usize;
                    start.wait();
                    for k in 0..OPS {
                        black_box(granary.is_cached(keys[(base + k) % keys.len()].as_str()));
                    }
                });
            }
        });
    });
}

// --- 2. The file store's manifest and segment maps ---------------------------------

fn store(dir: &std::path::Path) -> FileGrainStore {
    FileGrainStore::open(dir).expect("open the store")
}

fn name(i: usize) -> GrainName {
    GrainName::new("bench.Stored", format!("grain-{i}"))
}

/// Concurrent **first** writes to grains the store has never seen — the path that
/// takes the manifest lock and fsyncs the manifest log while holding it.
///
/// This is the create storm: a node bringing up many grains at once. If the manifest
/// lock is the serial point, aggregate throughput here is flat at every width, and no
/// amount of per-segment parallelism below it helps.
#[divan::bench(args = THREADS)]
fn store_first_write(bencher: Bencher, threads: usize) {
    let dir = tempfile::tempdir().expect("scratch dir");
    let store = store(dir.path());
    let next = AtomicU64::new(0);

    bencher.counter(ItemsCount::new(threads)).bench(|| {
        let start = Arc::new(Barrier::new(threads));
        std::thread::scope(|scope| {
            for _ in 0..threads {
                let start = Arc::clone(&start);
                let store = &store;
                let next = &next;
                scope.spawn(move || {
                    // A name this store has never seen, so `segment_id` must create.
                    let i = next.fetch_add(1, Ordering::Relaxed) as usize;
                    let grain = name(i);
                    start.wait();
                    let ack = store.store_record(
                        0,
                        &grain,
                        Seq::ZERO,
                        Term::new(1),
                        vec![b"e".to_vec()],
                        WriteKind::Append,
                    );
                    assert!(matches!(ack, StoreAck::Stored(_)), "the write must land");
                });
            }
        });
    });
}

/// Concurrent reads of grains the store already holds — the steady-state path, where
/// the manifest lock is a hash hit and the per-segment locks are what serialize.
///
/// The control for [`store_first_write`]: same store, same number of threads, but the
/// manifest's slow path is never entered. A gap between the two is the manifest lock's
/// fsync; agreement means the cost is somewhere else.
#[divan::bench(args = THREADS)]
fn store_warm_read(bencher: Bencher, threads: usize) {
    let dir = tempfile::tempdir().expect("scratch dir");
    let store = store(dir.path());
    // Populate, so every read below takes the manifest's fast path.
    for i in 0..NAMES {
        let ack = store.store_record(
            0,
            &name(i),
            Seq::ZERO,
            Term::new(1),
            vec![b"e".to_vec()],
            WriteKind::Append,
        );
        assert!(matches!(ack, StoreAck::Stored(_)));
    }
    let cursor = AtomicU64::new(0);

    bencher.counter(ItemsCount::new(threads * OPS)).bench(|| {
        let start = Arc::new(Barrier::new(threads));
        std::thread::scope(|scope| {
            for _ in 0..threads {
                let start = Arc::clone(&start);
                let store = &store;
                let cursor = &cursor;
                scope.spawn(move || {
                    let base = cursor.fetch_add(OPS as u64, Ordering::Relaxed) as usize;
                    start.wait();
                    for k in 0..OPS {
                        black_box(store.head(0, &name((base + k) % NAMES)));
                    }
                });
            }
        });
    });
}
