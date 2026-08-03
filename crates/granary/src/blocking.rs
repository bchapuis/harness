//! Where a store's blocking file I/O runs (spec §7.4, §14).
//!
//! A [`GrainStore`](crate::GrainStore) call is synchronous by contract: when it
//! returns, its outcome is settled, and for the file-backed store that means the
//! bytes have been written and fsynced. Someone has to block for that. The question
//! this module answers is *which thread*.
//!
//! Run inline, an fsync blocks the async worker that happened to be driving the
//! append. That worker is also driving Raft heartbeats, election timers, and every
//! other shard's quorum wait, so one slow device does not merely slow one write: it
//! stalls heartbeats past the election timeout, the shard elects a new leader, the
//! grains on the old one step down (§6), and the next access rehydrates them — a
//! cluster-wide event produced by a local hiccup, and one that makes more I/O while
//! the disk is already the bottleneck.
//!
//! **The argument is about the tail, not the median.** On a datacenter NVMe with
//! power-loss protection a flush of a small append costs tens of microseconds, which
//! against a 250 ms heartbeat, on a node with dozens of cores, is nothing — the
//! feedback loop above cannot start from a *typical* write
//! (`docs/hardware-envelope.md` §2). It starts from the atypical one: the same device
//! stalls for hundreds of milliseconds during garbage collection, a RAID rebuild, or
//! a queue that has filled behind a large checkpoint rewrite, and a drive line without
//! power-loss protection flushes an order of magnitude slower all the time (hw §3.7,
//! §6). This seam exists so that those events cost one pool thread rather than an
//! executor worker. Sizing it therefore follows the concurrency needed to ride out a
//! stall, not the throughput of the median flush.
//!
//! So the blocking call is submitted here instead, and the caller awaits the result.
//! The store's own lock discipline is untouched: the guard, the fence promise, and
//! the in-memory apply still happen under the grain's segment lock, in the same
//! order, just on a different thread. Two concurrent appends contend for that lock
//! exactly as they do today.
//!
//! **Which calls come through here.** Everything that writes the device:
//! `store_record`, `store_snapshot`, `prepare` (it rewrites the shard's fence file
//! whenever the term advances), `seal_range`, `put_blob`, and the reclamation calls
//! `delete_blob`, `retain_blobs` and `delete_blobs` — a sweep is not an fsync but it
//! unlinks one file per reclaimed blob, so a grain that has churned a large disk
//! image makes thousands of synchronous unlinks against the device the durability
//! path needs. Reads do not — `head`, `read`, `snapshot`, `get_blob`, `has_blob`,
//! `blob_ids`, `grains` — and that is a deliberate line rather than an omission. The argument above is about a device
//! stalling *while a durability barrier is open*; a read has no barrier to hold, is
//! usually served from the store's loaded segment without touching the device at all,
//! and sits on the activation-latency path where an extra hop between threads is a
//! cost with nothing to buy. A deployment that finds its reads stalling should move
//! them across too, but that is a decision to take on evidence, not a gap to close on
//! symmetry.
//!
//! Call it through [`on_store`] rather than [`offload`] directly, so the line above is
//! one a reader can check by grep instead of by reading every call site. It has not
//! always been checkable: `put_blob` on the leader's own quorum path was inline until
//! the disk facet's capture path was measured, which meant a machine create fsynced
//! five hundred blobs on the async worker, one per block, with the heartbeats.
//!
//! **Why a seam and not just a thread pool.** The deterministic simulator (§14) runs
//! the *production* store — `raft_journal.rs` cold-restarts a real
//! [`FileGrainStore`](crate::FileGrainStore) under virtual time — and a real thread
//! pool there would make a run's outcome depend on OS scheduling, breaking the
//! seed-reproducibility the whole test strategy rests on (actor §18.1). [`InlineIo`]
//! is therefore the default and runs the job on the calling thread, reproducing
//! today's behaviour exactly; a deployment opts into [`ThreadPoolIo`].

use std::sync::Arc;

/// A unit of blocking work: one synchronous store call.
pub(crate) type Job = Box<dyn FnOnce() + Send + 'static>;

/// Where the blocking file I/O of a durable store runs.
///
/// The contract is only about *threads*, never about order: submitting does not
/// serialize anything, and two jobs may run concurrently. Ordering between store
/// operations remains the store's own, enforced by its per-grain segment lock, so an
/// implementation is free to run jobs on as many threads as it likes.
pub trait BlockingIo: Send + Sync + 'static {
    /// Take `job` and run it, now or on another thread. Returns `false` only if the
    /// job was **refused and will never run** — a pool that is shutting down — which
    /// the caller must distinguish from "not finished yet", since it is waiting on a
    /// signal the job itself sends.
    ///
    /// Completion is not reported here: [`offload`] carries the job's value back, and
    /// one signal is enough. A second one is a second thing that can disagree with it.
    fn submit(&self, job: Job) -> bool;
}

/// Run `f` on `io` and await its value — the ergonomic form of
/// [`BlockingIo::submit`], which deals only in erased `()` jobs so it can stay
/// object-safe.
///
/// The value comes back through a channel rather than a shared cell so the job owns
/// everything it touches; `f` therefore captures by move and needs no lifetime tie to
/// the caller. That channel is also the completion signal: the send is the job's last
/// act, so there is nothing else to wait for.
pub(crate) async fn offload<T, F>(io: &Arc<dyn BlockingIo>, f: F) -> T
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (tx, rx) = futures::channel::oneshot::channel();
    // A dropped receiver means the caller went away; the work still ran, which is what
    // a durable write requires, so the send failing is nothing to report.
    let accepted = io.submit(Box::new(move || {
        let _ = tx.send(f());
    }));
    assert!(
        accepted,
        "store io refused the job: the pool is shut down while a store call is in \
         flight, so this write cannot be made durable",
    );
    rx.await.expect("an accepted job always sends its value")
}

/// Run one [`GrainStore`] call on `io` and await its outcome — [`offload`] in the
/// shape every replication seam wants, and **the** way a store call reaches the pool.
///
/// The `Arc::clone` is here rather than at each call site because it is not a choice:
/// the job must own everything it touches to be `'static`, so every caller was
/// writing the same two lines ahead of the same `offload`. Collapsing them matters
/// less for the lines saved than for what the name does — a reader can now tell, at a
/// glance and by grep, which store calls run on the pool and which run inline, which
/// is a policy question (see the module docs) rather than an accident of how each
/// site happened to be written.
///
/// The outcome comes back as whatever the call returns, `Reserved` and all: the
/// durability marker is the caller's to discharge where it means something, and a
/// helper that unwrapped it here would move that decision away from the site that
/// makes it.
pub(crate) async fn on_store<T, F>(
    io: &Arc<dyn BlockingIo>,
    store: &Arc<dyn crate::store::GrainStore>,
    call: F,
) -> T
where
    T: Send + 'static,
    F: FnOnce(&dyn crate::store::GrainStore) -> T + Send + 'static,
{
    let store = Arc::clone(store);
    offload(io, move || call(store.as_ref())).await
}

/// Run the job on the calling thread — the default, and the only implementation the
/// deterministic simulation may use (see the module docs).
///
/// This is not a degenerate case to be tolerated: it is the behaviour the store had
/// before there was a seam here, so a deployment that has not opted into a pool
/// behaves exactly as it always did.
pub struct InlineIo;

impl BlockingIo for InlineIo {
    fn submit(&self, job: Job) -> bool {
        job();
        true
    }
}

/// A fixed pool of OS threads that store I/O runs on, keeping fsync off the async
/// executor (see the module docs).
///
/// Sized rather than unbounded because the work is device-bound: past the point where
/// the device is saturated, more threads add queueing and context switches, not
/// throughput. Jobs queue when every thread is busy, which is the backpressure a
/// saturated disk should apply.
pub struct ThreadPoolIo {
    jobs: async_channel::Sender<Job>,
    /// Joined on drop so a store's last writes are not abandoned mid-fsync.
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl ThreadPoolIo {
    /// A pool of `threads` workers (at least one).
    pub fn new(threads: usize) -> ThreadPoolIo {
        let threads = threads.max(1);
        // `async_channel` is the workspace's queue (actor §4.2 mailboxes use it) and is
        // multi-consumer, so each worker holds its own receiver and the first one free
        // takes the next job — the scheduling a device-bound queue wants, without a
        // shared dequeue lock in front of it.
        let (jobs, rx) = async_channel::unbounded::<Job>();
        let workers = (0..threads)
            .map(|_| {
                let rx = rx.clone();
                std::thread::Builder::new()
                    .name("granary-store-io".to_string())
                    // `recv_blocking` ends when the sender is closed, which is how the
                    // pool shuts its workers down (see `Drop`).
                    .spawn(move || {
                        while let Ok(job) = rx.recv_blocking() {
                            job();
                        }
                    })
                    .expect("spawning a store io worker")
            })
            .collect();
        ThreadPoolIo { jobs, workers }
    }

    /// A pool sized to the machine, the default for a deployment that does not choose.
    ///
    /// The width is set by how many writes must keep making progress *while the device
    /// is stalled* (see the module docs), not by throughput: at tens of microseconds a
    /// flush, even two threads clear far more work than a node generates, and past the
    /// point where the device is saturated more threads add queueing and context
    /// switches rather than bandwidth. The floor keeps a stalled write from blocking an
    /// unrelated one; the ceiling keeps a large host from spawning a thread per core for
    /// work that is not CPU-bound.
    pub fn sized_for_host() -> ThreadPoolIo {
        let cores = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
        ThreadPoolIo::new(cores.clamp(2, 8))
    }
}

impl BlockingIo for ThreadPoolIo {
    fn submit(&self, job: Job) -> bool {
        // The queue is unbounded, so this fails only when the pool has been closed:
        // the job did not run and never will, and the caller is told so rather than
        // left waiting on a value nothing will send.
        self.jobs.try_send(job).is_ok()
    }
}

impl Drop for ThreadPoolIo {
    fn drop(&mut self) {
        // Close the queue so the workers' `recv_blocking` ends, then wait for the job
        // each is running to finish. A worker abandoned mid-fsync would leave a store
        // call that already reported its outcome without the bytes behind it.
        self.jobs.close();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block<T>(f: impl std::future::Future<Output = T>) -> T {
        futures::executor::block_on(f)
    }

    #[test]
    fn inline_io_runs_the_job_before_the_call_returns() {
        // The property the simulation depends on: no thread, no scheduling, so a run
        // is a pure function of its seed (§14). The job must have run by the time
        // `submit` returns, not at some later poll.
        let io: Arc<dyn BlockingIo> = Arc::new(InlineIo);
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&ran);
        let accepted = io.submit(Box::new(move || {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        }));
        assert!(accepted, "inline io never refuses a job");
        assert!(
            ran.load(std::sync::atomic::Ordering::SeqCst),
            "inline io runs the job on the calling thread, before returning"
        );
        assert_eq!(
            block(offload(&io, || std::thread::current().id())),
            std::thread::current().id(),
            "and offload runs on the caller's thread too"
        );
    }

    #[test]
    fn a_pool_runs_jobs_off_the_calling_thread_and_returns_their_values() {
        let io: Arc<dyn BlockingIo> = Arc::new(ThreadPoolIo::new(2));
        let caller = std::thread::current().id();
        let (value, ran_on) = block(offload(&io, move || (41 + 1, std::thread::current().id())));
        assert_eq!(value, 42, "the job's value comes back to the caller");
        assert_ne!(ran_on, caller, "and it did not run on the calling thread");
    }

    /// Concurrency is the point: a pool with N threads runs N jobs at once, so one
    /// slow device does not serialize every other shard behind it.
    #[test]
    fn a_pool_runs_jobs_concurrently() {
        let io: Arc<dyn BlockingIo> = Arc::new(ThreadPoolIo::new(4));
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let jobs: Vec<_> = (0..4)
            .map(|i| {
                let barrier = Arc::clone(&barrier);
                offload(&io, move || {
                    // Deadlocks unless all four run at once.
                    barrier.wait();
                    i
                })
            })
            .collect();
        assert_eq!(block(futures::future::join_all(jobs)), vec![0, 1, 2, 3]);
    }
}
