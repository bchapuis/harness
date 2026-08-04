//! What a durable flush costs on this host's drive, and how many run at once.
//!
//! [`ThreadPoolIo`] is justified in `blocking.rs` by **tail** isolation: a stalled
//! device must not block the async worker that is also driving Raft heartbeats and
//! other shards' quorum waits. That argument is about the tail and is right. But the
//! pool's *width* — `sized_for_host`'s `clamp(2, 8)` — was set from the median, and no
//! measurement of that median existed. It matters because the two devices a deployment
//! might be on differ by more than an order of magnitude:
//!
//! - NVMe **with** power-loss protection: the flush is acknowledged out of a
//!   capacitor-backed write cache, ~30 µs.
//! - NVMe **without** it: the flush waits on the NAND program, ~500 µs.
//!
//! At 30 µs a couple of threads clear far more work than a node generates and the
//! width is irrelevant; at 500 µs the queue in front of the pool is sixteen times
//! deeper for the same offered load, and the width is the difference between a bounded
//! wait and an unbounded one. So the number decides whether the clamp is right, and
//! nothing in the tree recorded it.
//!
//! Two measurements, because they answer different halves:
//!
//! 1. [`one_flush`] — the cost of a single durable write, the `atomic_replace`
//!    primitive every store write ends in (write, fsync, rename, fsync the directory).
//!    Its **median** places the host on the scale above; its **max** is the tail the
//!    pool exists to isolate, and the two are usually far apart.
//! 2. [`concurrent_flushes`] — the same write from `N` threads at once, swept across
//!    the clamp's range and past it. This is the sizing answer directly: throughput
//!    that keeps climbing past 8 says the ceiling is too low for this device, and
//!    throughput that flattens at 2 says the extra threads are only queueing.
//!
//! Run with `cargo bench -p granary --bench flush`. It writes to a temp directory, so
//! it measures **that** filesystem — point `TMPDIR` at the volume a deployment will
//! actually use, because a laptop's internal SSD and a server's NVMe are exactly the
//! comparison this file exists to stop people from assuming away.

use std::sync::Arc;
use std::sync::Barrier;

use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

fn main() {
    divan::main();
}

/// The payload size. Deliberately small: this measures the *flush*, not the transfer,
/// and a large write would fold the device's bandwidth into a number that is supposed
/// to be about its acknowledgement latency.
const PAYLOAD: usize = 4096;

/// One durable write through the same primitive the stores use.
///
/// The median answers "does this drive have power-loss protection"; the max is the
/// tail the I/O pool exists to keep off the executor.
#[divan::bench]
fn one_flush(bencher: Bencher) {
    let dir = tempfile::tempdir().expect("scratch dir");
    let bytes = vec![0xa5u8; PAYLOAD];
    let mut seq = 0u64;
    bencher.counter(ItemsCount::new(1usize)).bench_local(|| {
        // A fresh name per iteration, so this never measures an overwrite of a file
        // the page cache is already holding open.
        seq += 1;
        let name = format!("flush-{seq}");
        wal::atomic_replace(dir.path(), &name, black_box(&bytes)).expect("durable write");
    });
}

/// `threads` durable writes in flight at once, each on its own file.
///
/// Swept across `sized_for_host`'s clamp and past both ends of it. Read the counter
/// (items per second), not the wall time: the per-iteration time necessarily grows
/// with the thread count, and what the sizing question asks is whether *aggregate*
/// durable writes per second are still improving at a given width.
#[divan::bench(args = [1, 2, 4, 8, 16, 32])]
fn concurrent_flushes(bencher: Bencher, threads: usize) {
    let dir = tempfile::tempdir().expect("scratch dir");
    let bytes = Arc::new(vec![0xa5u8; PAYLOAD]);
    let seq = std::sync::atomic::AtomicU64::new(0);

    bencher.counter(ItemsCount::new(threads)).bench(|| {
        // The barrier makes the flushes actually concurrent rather than merely
        // spawned: without it the first thread can finish before the last starts,
        // and the bench would report the serial cost at every width.
        let start = Arc::new(Barrier::new(threads));
        std::thread::scope(|scope| {
            for _ in 0..threads {
                let start = Arc::clone(&start);
                let bytes = Arc::clone(&bytes);
                let dir = dir.path();
                let seq = &seq;
                scope.spawn(move || {
                    let n = seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let name = format!("flush-{n}");
                    start.wait();
                    wal::atomic_replace(dir, &name, black_box(&bytes)).expect("durable write");
                });
            }
        });
    });
}
