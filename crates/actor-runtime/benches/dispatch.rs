//! What one local message costs.
//!
//! The agent loop is built from actor sends — a run step journals, dispatches tools, and
//! collects outcomes by telling and asking — so the per-message floor here is the floor
//! under everything above it. The benchmark lives in this crate rather than in
//! `actor-core` because the numbers only mean something through the production runtime
//! seam: `TokioClock`, `OsEntropy`, `TokioSpawner`. Those are what a deployed node runs.
//!
//! Three costs are separated:
//!
//! - `tell` / `ask` measure the whole path, `ActorRef` down to the handler.
//! - `resolve` isolates what `ActorRef` adds over an already-resolved mailbox — the
//!   registry lookup every send repeats — and sweeps the live-actor count, because that
//!   lookup walks a map keyed by actor id.
//! - `spawn` covers actor creation, which the harness pays per delegation.
//!
//! Allocation counts are reported beside the timings and are the steadier number of the
//! two: a send is short enough that scheduler noise moves the median more than a removed
//! allocation does, but the allocation count does not move at all unless the code did.

use std::hint::black_box;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::LocalSystem;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::system::LocalSystemBuilder;
use actor_runtime::OsEntropy;
use actor_runtime::TokioClock;
use actor_runtime::TokioSpawner;
use divan::Bencher;
use divan::counter::ItemsCount;
use serde::Deserialize;
use serde::Serialize;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// The smallest actor that still exercises the whole path: it holds state, so the
/// handler is a real `&mut self` borrow, and it replies, so `ask` has something to carry.
struct Counter {
    count: u64,
}

impl Actor for Counter {
    type System = Sys;
}

/// A fire-and-forget message — the `tell` path, where the reply is dropped.
#[derive(Serialize, Deserialize)]
struct Bump;

impl Message for Bump {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("bench.Bump");
}

impl Handler<Bump> for Counter {
    async fn handle(&mut self, _msg: Bump, _ctx: &Ctx<Self>) {
        self.count += 1;
    }
}

/// A request/response message — the `ask` path, which adds a reply channel.
#[derive(Serialize, Deserialize)]
struct Read;

impl Message for Read {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("bench.Read");
}

impl Handler<Read> for Counter {
    async fn handle(&mut self, _msg: Read, _ctx: &Ctx<Self>) -> u64 {
        self.count
    }
}

type Sys = LocalSystem<TokioClock, OsEntropy, TokioSpawner>;

/// A system on a current-thread runtime. Single-threaded on purpose: it keeps the
/// measurement to the send path rather than to how a work-stealing scheduler happened to
/// place two tasks, which on a benchmark this short is most of the variance.
fn system(runtime: &tokio::runtime::Runtime) -> Sys {
    let _guard = runtime.enter();
    LocalSystemBuilder::new(
        TokioClock::new(),
        OsEntropy::new(),
        TokioSpawner::new(runtime.handle().clone()),
    )
    .node(NodeId::new(0))
    .build()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime")
}

/// Run every already-spawned task up to its first await.
///
/// `spawn` only queues the actor's task; on a current-thread runtime it does not start
/// until something drives the runtime. Without this, a benchmark that spawns during
/// setup pays for those startups inside its first measured iteration — at ten thousand
/// actors that showed up as a single sample eighty thousand allocations heavier than its
/// neighbours. Settling first is what makes the measurement steady-state.
fn settle(rt: &tokio::runtime::Runtime, actor: &ActorRef<Counter>) {
    rt.block_on(async { actor.ask(Read).await })
        .expect("the target actor is alive");
}

/// How many messages one `tell` iteration sends.
///
/// A `tell` returns once the message is queued, so a single one measures the sender's
/// half and leaves the handler to run whenever the executor next gets the thread — which
/// is what made a one-message loop swing between two and eight allocations depending on
/// where the mailbox's bounded queue happened to be. Sending a batch and then draining it
/// with one `ask` puts the whole cost, both halves, inside the iteration. The batch stays
/// under the default mailbox capacity of 64 so the send never blocks on a full queue.
const BATCH: usize = 32;

/// The full per-message cost: enqueue, dispatch, and handler, amortized over a batch.
///
/// Read the per-item counters rather than the iteration time — the trailing `ask` that
/// drains the batch is one round trip spread across `BATCH` messages.
#[divan::bench]
fn tell(bencher: Bencher) {
    let rt = runtime();
    let sys = system(&rt);
    let actor = sys.spawn(Counter { count: 0 });
    settle(&rt, &actor);
    bencher
        .counter(ItemsCount::new(BATCH))
        .bench_local(|| {
            rt.block_on(async {
                for _ in 0..BATCH {
                    black_box(&actor).tell(Bump).await.expect("alive");
                }
                actor.ask(Read).await.expect("alive")
            })
        });
}

/// One request/response round trip: enqueue, run the handler, carry the reply back.
///
/// The delta against a `tell` is the reply channel and the two extra events an ask emits
/// to bracket itself.
#[divan::bench]
fn ask(bencher: Bencher) {
    let rt = runtime();
    let sys = system(&rt);
    let actor = sys.spawn(Counter { count: 0 });
    settle(&rt, &actor);
    bencher.bench_local(|| rt.block_on(async { black_box(&actor).ask(Read).await }));
}

/// The same round trip as `ask`, swept over how many actors share the node.
///
/// Named for what it is measuring *across* the sweep, not for an isolated operation: the
/// body is `ask`'s, and the question is whether the number moves as the host's registry
/// fills. Every send looks its target up in a map keyed by actor id, so a rising line
/// would mean that lookup is worth attacking and a flat one means it is not. The absolute
/// value at any single point is just `ask`.
#[divan::bench(args = [100, 10_000])]
fn resolve(bencher: Bencher, live: usize) {
    let rt = runtime();
    let sys = system(&rt);
    // Fill the registry, then measure against one target among them. The other actors are
    // held to the end of the benchmark — dropping an `ActorRef` does not retire the actor,
    // but letting the vector fall out of scope early would still be misleading to read.
    let others: Vec<ActorRef<Counter>> = (0..live.saturating_sub(1))
        .map(|_| sys.spawn(Counter { count: 0 }))
        .collect();
    let actor = sys.spawn(Counter { count: 0 });
    for other in &others {
        settle(&rt, other);
    }
    settle(&rt, &actor);
    bencher.bench_local(|| rt.block_on(async { black_box(&actor).ask(Read).await }));
}

/// Actor creation: id assignment, the mailbox channel, the registry insert, and the task
/// launch. The harness pays this per delegation, so it is on the loop's path, not only at
/// startup.
///
/// Each iteration gets a fresh system, built in `with_inputs` so its construction is
/// outside the measurement. Spawning into one shared system instead would insert into a
/// registry that grows for the whole run and is never swept — the id map's lookup cost
/// climbs with it, so the benchmark would measure a moving target and retain a mailbox
/// and a queued task per iteration. That growth is `resolve`'s subject, deliberately, and
/// does not belong in this one.
#[divan::bench]
fn spawn(bencher: Bencher) {
    let rt = runtime();
    bencher
        .with_inputs(|| system(&rt))
        .bench_local_refs(|sys| black_box(sys.spawn(Counter { count: 0 })));
}
