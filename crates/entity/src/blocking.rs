//! Where a store's blocking file I/O runs (spec storage §3.2, entity §10).
//!
//! An [`EntityStore`] call is synchronous by contract: when it
//! returns, its outcome is settled, and for the file-backed store that means the
//! bytes have been written and fsynced. Something has to block for that; this module
//! decides which thread.
//!
//! Run inline, an fsync blocks the async worker that happened to be driving the
//! append. That worker is also driving Raft heartbeats, election timers, and every
//! other shard's quorum wait, so one slow device does not merely slow one write: it
//! stalls heartbeats past the election timeout, the shard elects a new leader, the
//! entities on the old one step down (§6), and the next access rehydrates them; a
//! cluster-wide event produced by a local hiccup, and one that makes more I/O while
//! the disk is already the bottleneck.
//!
//! **The argument is about the tail, not the median.** On a datacenter NVMe with
//! power-loss protection a flush of a small append costs tens of microseconds, which
//! against a 250 ms heartbeat, on a node with dozens of cores, is nothing: the
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
//! the in-memory apply still happen under the entity's segment lock, in the same
//! order, just on a different thread.
//!
//! **Which calls come through here.** Everything that writes the device:
//! `store_record`, `store_snapshot`, `truncate`, `prepare` (it rewrites the shard's
//! fence file whenever the term advances), `put_blob`, the split/merge calls
//! `seal_range`, `unseal`, `remove_range` and `drop_shard`, and the reclamation calls
//! `delete_blob`, `retain_blobs` and `delete_blobs`. A sweep or a range removal is not
//! an fsync, but it unlinks one file per reclaimed blob or entity, so an entity that
//! has churned a large disk image, or a split that moves thousands of entities, makes
//! thousands of synchronous unlinks against the device the durability path needs. Reads do not (`head`, `read`, `snapshot`, `get_blob`, `has_blob`,
//! `blob_ids`, `entities`), and that is a deliberate line rather than an omission:
//! the argument above is about a device stalling *while a durability barrier is
//! open*; a read has no barrier to hold, is usually served from the store's loaded
//! segment without touching the device at all, and sits on the activation-latency
//! path where an extra hop between threads is a cost with nothing to buy. A
//! deployment that finds its reads stalling should move them across too, but that is
//! a decision to take on evidence, not a gap to close on symmetry.
//!
//! The runtime reaches its store only through [`NodeStore`], whose write methods offload
//! themselves and whose reads run inline, so the line above is the handle's interface:
//! a write cannot be issued on the calling thread by forgetting to wrap it, and a
//! reader checks the policy in one place instead of at every call site.
//!
//! **Why a seam and not just a thread pool.** The deterministic simulator (§10) runs
//! the *production* store: `raft_journal.rs` cold-restarts a real
//! [`FileEntityStore`](crate::file_store::FileEntityStore) under virtual time, and a real thread
//! pool there would make a run's outcome depend on OS scheduling, breaking the
//! seed-reproducibility the whole test strategy rests on (actor §11). [`InlineIo`]
//! is therefore the default and runs the job on the calling thread, reproducing
//! today's behaviour exactly; a deployment opts into [`ThreadPoolIo`].

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::blobs::BlobId;
use crate::entity::EntityName;
use crate::journal::RecordPage;
use crate::journal::Seq;
use crate::journal::Term;
use crate::store::BlobAck;
use crate::store::EntityStore;
use crate::store::ReadOutcome;
use crate::store::ReadReply;
use crate::store::StoreAck;
use crate::store::WriteKind;

/// A unit of blocking work: one synchronous store call.
pub(crate) type Job = Box<dyn FnOnce() + Send + 'static>;

/// Where the blocking file I/O of a durable store runs (storage §3.4).
///
/// The contract is only about *threads*, never about order: submitting does not
/// serialize anything, and two jobs may run concurrently. Ordering between store
/// operations remains the store's own, enforced by its per-entity segment lock, so an
/// implementation is free to run jobs on as many threads as it likes.
pub trait BlockingIo: Send + Sync + 'static {
    /// Take `job` and run it, now or on another thread. Returns `false` only if the
    /// job was **refused and will never run** (a pool that is shutting down) which
    /// the caller must distinguish from "not finished yet", since it is waiting on a
    /// signal the job itself sends.
    ///
    /// Completion is not reported here: `offload` carries the job's value back, and
    /// one signal is enough. A second one is a second thing that can disagree with it.
    fn submit(&self, job: Job) -> bool;
}

/// Run `f` on `io` and await its value: the ergonomic form of
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

/// This node's durable store as the runtime calls it (spec storage §3.2): the [`EntityStore`]
/// plug point paired with the [`BlockingIo`] its writes run on. Built once per hosted
/// entity type at `entities()` time and cloned into every journal, replicator, shard
/// map, and replica-store actor that type uses.
///
/// **Writes** (everything that changes the device: the module docs list them) are
/// `async` and run on the pool. Each returns a future that owns what it needs, so it
/// can join a quorum's fan-out or outlive its caller's borrow, and that submits its job
/// on **first poll**, not when it is built: a quorum append builds the local write
/// beside the peer asks and the peers go on the wire first (see `local_store_ack` in
/// the replicator).
///
/// The pool promises threads, not order ([`BlockingIo`]), so a caller that needs one
/// write to land before its next step **awaits** it: the store's effect has happened
/// when the future resolves, with the guarantees [`EntityStore`] states for the call.
///
/// **Reads** stay synchronous and inline, for the reason the module docs give: no
/// durability barrier is open across them, and they sit on the activation path.
#[derive(Clone)]
pub struct NodeStore {
    store: Arc<dyn EntityStore>,
    io: Arc<dyn BlockingIo>,
}

impl NodeStore {
    /// `store`'s calls, with its writes on `io`.
    pub(crate) fn new(store: Arc<dyn EntityStore>, io: Arc<dyn BlockingIo>) -> NodeStore {
        NodeStore { store, io }
    }

    /// Run one store write on the pool. The outcome comes back unexamined: a store
    /// call's ack is the caller's to read where it means something (**E14**).
    fn write<T, F>(&self, call: F) -> impl Future<Output = T> + Send + use<T, F>
    where
        T: Send + 'static,
        F: FnOnce(&dyn EntityStore) -> T + Send + 'static,
    {
        let (io, store) = (Arc::clone(&self.io), Arc::clone(&self.store));
        // An `async` block runs nothing until polled, which is what defers the submit.
        async move { offload(&io, move || call(store.as_ref())).await }
    }

    // --- writes: offloaded, see the type docs ---

    /// [`EntityStore::store_record`] on the pool.
    pub(crate) fn store_record(
        &self,
        shard: u32,
        entity: &EntityName,
        after: Seq,
        term: Term,
        records: Vec<Vec<u8>>,
        kind: WriteKind,
    ) -> impl Future<Output = StoreAck> + Send + use<> {
        let entity = entity.clone();
        self.write(move |store| store.store_record(shard, &entity, after, term, records, kind))
    }

    /// [`EntityStore::store_snapshot`] on the pool.
    pub(crate) fn store_snapshot(
        &self,
        shard: u32,
        entity: &EntityName,
        at: Seq,
        term: Term,
        state: Vec<u8>,
        kind: WriteKind,
    ) -> impl Future<Output = StoreAck> + Send + use<> {
        let entity = entity.clone();
        self.write(move |store| store.store_snapshot(shard, &entity, at, term, state, kind))
    }

    /// [`EntityStore::prepare`] on the pool: it rewrites the shard's fence file
    /// whenever the term advances, so every failover makes it a durable write.
    pub(crate) fn prepare(
        &self,
        shard: u32,
        entity: &EntityName,
        term: Term,
    ) -> impl Future<Output = ReadOutcome> + Send + use<> {
        let entity = entity.clone();
        self.write(move |store| store.prepare(shard, &entity, term))
    }

    /// [`EntityStore::truncate`] on the pool.
    pub(crate) fn truncate(
        &self,
        shard: u32,
        entity: &EntityName,
        after: Seq,
        term: Term,
    ) -> impl Future<Output = ()> + Send + use<> {
        let entity = entity.clone();
        self.write(move |store| store.truncate(shard, &entity, after, term))
    }

    /// [`EntityStore::put_blob`] on the pool.
    pub(crate) fn put_blob(
        &self,
        shard: u32,
        entity: &EntityName,
        id: BlobId,
        bytes: Vec<u8>,
    ) -> impl Future<Output = BlobAck> + Send + use<> {
        let entity = entity.clone();
        self.write(move |store| store.put_blob(shard, &entity, id, bytes))
    }

    /// [`EntityStore::delete_blob`] on the pool.
    pub(crate) fn delete_blob(
        &self,
        shard: u32,
        entity: &EntityName,
        id: BlobId,
    ) -> impl Future<Output = ()> + Send + use<> {
        let entity = entity.clone();
        self.write(move |store| store.delete_blob(shard, &entity, id))
    }

    /// [`EntityStore::delete_blobs`] on the pool.
    pub(crate) fn delete_blobs(
        &self,
        shard: u32,
        entity: &EntityName,
    ) -> impl Future<Output = ()> + Send + use<> {
        let entity = entity.clone();
        self.write(move |store| store.delete_blobs(shard, &entity))
    }

    /// [`EntityStore::retain_blobs`] on the pool.
    pub(crate) fn retain_blobs(
        &self,
        shard: u32,
        entity: &EntityName,
        retain: BTreeSet<BlobId>,
    ) -> impl Future<Output = ()> + Send + use<> {
        let entity = entity.clone();
        self.write(move |store| store.retain_blobs(shard, &entity, &retain))
    }

    /// [`EntityStore::seal_range`] on the pool.
    pub(crate) fn seal_range(
        &self,
        shard: u32,
        from: u64,
    ) -> impl Future<Output = ()> + Send + use<> {
        self.write(move |store| store.seal_range(shard, from))
    }

    /// [`EntityStore::unseal`] on the pool.
    pub(crate) fn unseal(&self, shard: u32) -> impl Future<Output = ()> + Send + use<> {
        self.write(move |store| store.unseal(shard))
    }

    /// [`EntityStore::remove_range`] on the pool: one unlink per moved entity.
    pub(crate) fn remove_range(
        &self,
        shard: u32,
        from: u64,
    ) -> impl Future<Output = ()> + Send + use<> {
        self.write(move |store| store.remove_range(shard, from))
    }

    /// [`EntityStore::drop_shard`] on the pool: one unlink per entity in the shard.
    pub(crate) fn drop_shard(&self, shard: u32) -> impl Future<Output = ()> + Send + use<> {
        self.write(move |store| store.drop_shard(shard))
    }

    // --- reads: inline, see the type docs ---

    pub(crate) fn read(&self, shard: u32, entity: &EntityName) -> ReadReply {
        self.store.read(shard, entity)
    }

    pub(crate) fn head(&self, shard: u32, entity: &EntityName) -> Seq {
        self.store.head(shard, entity)
    }

    pub(crate) fn snapshot(&self, shard: u32, entity: &EntityName) -> Option<(Seq, Vec<u8>)> {
        self.store.snapshot(shard, entity)
    }

    pub(crate) fn read_from(
        &self,
        shard: u32,
        entity: &EntityName,
        from: Seq,
        limit: usize,
    ) -> RecordPage {
        self.store.read_from(shard, entity, from, limit)
    }

    pub(crate) fn failed(&self) -> bool {
        self.store.failed()
    }

    pub(crate) fn get_blob(&self, shard: u32, entity: &EntityName, id: BlobId) -> Option<Vec<u8>> {
        self.store.get_blob(shard, entity, id)
    }

    pub(crate) fn has_blob(&self, shard: u32, entity: &EntityName, id: BlobId) -> bool {
        self.store.has_blob(shard, entity, id)
    }

    pub(crate) fn blob_ids(&self, shard: u32, entity: &EntityName) -> Vec<BlobId> {
        self.store.blob_ids(shard, entity)
    }

    pub(crate) fn entities(&self, shard: u32) -> Vec<EntityName> {
        self.store.entities(shard)
    }

    pub(crate) fn shard_bytes(&self, shard: u32) -> u64 {
        self.store.shard_bytes(shard)
    }
}

/// Run the job on the calling thread: the default, and the only implementation the
/// deterministic simulation may use (see the module docs). A deployment that has not
/// opted into a pool gets it.
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
        // `async_channel` is the workspace's queue (actor §4 mailboxes use it) and is
        // multi-consumer, so each worker holds its own receiver and the first one free
        // takes the next job: the scheduling a device-bound queue wants, without a
        // shared dequeue lock in front of it.
        let (jobs, rx) = async_channel::unbounded::<Job>();
        let workers = (0..threads)
            .map(|_| {
                let rx = rx.clone();
                std::thread::Builder::new()
                    .name("entity-store-io".to_string())
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
    /// The **floor** is set by how many writes must keep making progress *while the
    /// device is stalled* (see the module docs), not by throughput: it keeps a stalled
    /// write from blocking an unrelated one.
    ///
    /// The **ceiling** is a throughput claim, and it is measured: `benches/flush.rs`
    /// sweeps concurrent `atomic_replace`s across this clamp and past both ends of it.
    /// On the one host measured so far (an Apple-silicon laptop, APFS, where `sync_all`
    /// is `F_FULLFSYNC` and so a real media flush) aggregate durable writes per second
    /// climb monotonically to eight threads and *fall* beyond it: 100, 118, 160, 191 at
    /// 1/2/4/8, then 154 at 16 and 151 at 32. Eight is the peak, so that is where the
    /// ceiling belongs, and past it the extra threads only queue.
    ///
    /// One caveat this must not lose: that host's median flush is **9.4 ms**, not the
    /// tens of microseconds Linux `fsync` costs on enterprise NVMe (~30 µs with
    /// power-loss protection, ~500 µs without); a full drive-cache flush on a consumer
    /// SSD is two orders away from it. The deployment's own figure is still unmeasured,
    /// and the shape of the curve (not its height) is what licenses the ceiling. Re-run
    /// the bench with `TMPDIR` on the deployment volume before treating either end as
    /// settled.
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

/// A pool that accepts jobs and runs none of them until told, standing in for a
/// device that has not answered yet. [`InlineIo`] cannot express that, because it runs
/// the job inside `submit` and so can never be observed mid-flight.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct DeferredIo {
    queued: std::sync::Mutex<Vec<Job>>,
    submitted: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl DeferredIo {
    /// How many jobs have been submitted so far, run or not.
    pub(crate) fn submitted(&self) -> usize {
        self.submitted.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Let the device answer: run every queued job, in submission order.
    pub(crate) fn run_queued(&self) {
        let jobs: Vec<Job> = std::mem::take(&mut *self.queued.lock().unwrap());
        for job in jobs {
            job();
        }
    }
}

#[cfg(test)]
impl BlockingIo for DeferredIo {
    fn submit(&self, job: Job) -> bool {
        self.submitted
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.queued.lock().unwrap().push(job);
        true
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
        // is a pure function of its seed (§10). The job must have run by the time
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
