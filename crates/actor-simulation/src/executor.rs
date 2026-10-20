//! The deterministic single-thread executor (spec §11, §11).
//!
//! All three runtime capabilities share one [`Inner`] behind an `Arc<Mutex<_>>`:
//! the [`SimSpawner`] enqueues tasks, the
//! [`SimClock`] registers timers, and [`Simulation::run`]
//! drives them. The run loop is **quiescence-driven**: it polls ready tasks
//! until none remain, and only then advances virtual time to the next timer.
//! Logical time therefore costs no wall-clock time.
//!
//! The waker uses [`std::task::Wake`] via `Arc`, so the crate honors the
//! workspace `unsafe_code = "forbid"`.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::BinaryHeap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use actor_core::BoxFuture;
use actor_core::Entropy;
use actor_core::Instant;

use crate::clock::SimClock;
use crate::entropy::SimEntropy;

/// Shared scheduler state. Single-threaded in practice, but `Send + Sync`
/// because the runtime traits demand it (the same traits production uses).
pub(crate) type Shared = Arc<Mutex<Inner>>;

/// A unit of asynchronous work owned by the scheduler.
struct Task {
    /// `None` only while the task is mid-poll (taken out to avoid re-entrancy).
    future: Mutex<Option<BoxFuture<'static, ()>>>,
    /// The **process incarnation** ("domain") this task belongs to, for
    /// pause/resume and retirement (docs/simulation-testing.md). `None` for test-driver tasks
    /// (`block_on`, a bare `sim.spawner()`), which are never frozen or retired.
    /// Set from the spawning system's tagged spawner, and inherited by child
    /// tasks via [`CURRENT_DOMAIN`].
    ///
    /// Per incarnation rather than per node, so a restart can retire the
    /// predecessor's tasks without touching the successor's, even though both
    /// run under the same [`NodeId`](actor_core::NodeId).
    domain: Option<u64>,
}

thread_local! {
    /// The node whose task is currently being polled, so any child task spawned
    /// during that poll inherits its domain even through an untagged spawner. The
    /// executor is single-threaded, so a thread-local `Cell` is deterministic.
    static CURRENT_DOMAIN: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// A registered timer: fire `waker` once virtual time reaches `deadline`.
struct Timer {
    deadline: Instant,
    /// Strictly increasing; breaks deadline ties deterministically.
    seq: u64,
    waker: Waker,
}

impl PartialEq for Timer {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.seq == other.seq
    }
}
impl Eq for Timer {}

impl Ord for Timer {
    fn cmp(&self, other: &Self) -> Ordering {
        // `BinaryHeap` is a max-heap; reverse so the *earliest* deadline (then
        // lowest seq) sits at the top.
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for Timer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The scheduler's shared mutable state.
pub(crate) struct Inner {
    now: Instant,
    /// Ids of tasks ready to be polled. A `Vec`, not a queue, because selection
    /// among ready tasks is seed-randomized (docs/simulation-testing.md), not FIFO.
    ready: Vec<u64>,
    /// Live tasks, keyed by id. `BTreeMap` keeps iteration deterministic.
    tasks: BTreeMap<u64, Arc<Task>>,
    timers: BinaryHeap<Timer>,
    next_task_id: u64,
    next_seq: u64,
    /// Frozen domains (docs/simulation-testing.md). A paused node's tasks stay ready but are not
    /// polled, so it makes no progress and its inbound frames queue; its timers
    /// still fire (re-readying the task) but never run until it resumes. Empty in
    /// every run that uses no pause, so scheduling is then byte-identical.
    paused: BTreeSet<u64>,
    /// Source of process-incarnation ids. A plain counter, drawing no entropy, so
    /// handing out domains cannot perturb a seeded run.
    next_domain: u64,
}

impl Inner {
    fn new() -> Inner {
        Inner {
            now: Instant::ZERO,
            ready: Vec::new(),
            tasks: BTreeMap::new(),
            timers: BinaryHeap::new(),
            next_task_id: 0,
            next_seq: 0,
            paused: BTreeSet::new(),
            next_domain: 0,
        }
    }

    /// Freeze (`paused = true`) or thaw a node's tasks (docs/simulation-testing.md).
    pub(crate) fn set_paused(&mut self, domain: u64, paused: bool) {
        if paused {
            self.paused.insert(domain);
        } else {
            self.paused.remove(&domain);
        }
    }

    /// Hand out the next process-incarnation id.
    fn fresh_domain(&mut self) -> u64 {
        let domain = self.next_domain;
        self.next_domain += 1;
        domain
    }

    /// Forget every task belonging to `domain`, the scheduler half of a process
    /// death (docs/simulation-testing.md). The tasks leave the ready set and the task table, so
    /// nothing in that incarnation is ever polled again.
    ///
    /// Returns the removed tasks rather than dropping them, because dropping a
    /// future runs its destructors and those can re-enter the scheduler (closing
    /// a channel, releasing a guard that launches a task). Dropping them while
    /// holding the scheduler lock would deadlock, so the caller drops outside it.
    fn retire_domain(&mut self, domain: u64) -> Vec<Arc<Task>> {
        let doomed: BTreeSet<u64> = self
            .tasks
            .iter()
            .filter(|(_, task)| task.domain == Some(domain))
            .map(|(id, _)| *id)
            .collect();
        self.ready.retain(|id| !doomed.contains(id));
        self.paused.remove(&domain);
        doomed
            .iter()
            .filter_map(|id| self.tasks.remove(id))
            .collect()
    }

    pub(crate) fn now(&self) -> Instant {
        self.now
    }

    /// Register `waker` to fire once virtual time reaches `deadline`.
    pub(crate) fn register_timer(&mut self, deadline: Instant, waker: Waker) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.timers.push(Timer {
            deadline,
            seq,
            waker,
        });
    }

    fn spawn(&mut self, future: BoxFuture<'static, ()>, domain: Option<u64>) {
        let id = self.next_task_id;
        self.next_task_id += 1;
        self.tasks.insert(
            id,
            Arc::new(Task {
                future: Mutex::new(Some(future)),
                domain,
            }),
        );
        self.ready.push(id);
    }
}

/// Wakes a single task by id by returning it to the ready set. Holds a `Weak`
/// reference to break the `Inner → Timer → Waker → Inner` cycle.
struct TaskWaker {
    shared: Weak<Mutex<Inner>>,
    id: u64,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(shared) = self.shared.upgrade() {
            let mut inner = shared.lock().expect("scheduler mutex poisoned");
            // Re-ready only a live task that is not already queued.
            if inner.tasks.contains_key(&self.id) && !inner.ready.contains(&self.id) {
                inner.ready.push(self.id);
            }
        }
    }
}

/// Task spawner backed by the simulation scheduler (spec §11.1).
#[derive(Clone)]
pub struct SimSpawner {
    shared: Shared,
    /// The node every task launched through this handle belongs to (pause/resume).
    /// `None` for the bare `Simulation::spawner()` used by test drivers.
    domain: Option<u64>,
}

impl SimSpawner {
    /// A clone of this spawner tagged with a **fresh process incarnation**, whose
    /// id is returned alongside it so the caller can later freeze, thaw, or retire
    /// that incarnation as a unit (docs/simulation-testing.md). Clones keep the tag, so the whole
    /// subtree the incarnation spawns belongs to it.
    pub(crate) fn with_fresh_domain(&self) -> (SimSpawner, u64) {
        let domain = self
            .shared
            .lock()
            .expect("scheduler mutex poisoned")
            .fresh_domain();
        (
            SimSpawner {
                shared: Arc::clone(&self.shared),
                domain: Some(domain),
            },
            domain,
        )
    }

    /// Freeze or thaw a node's tasks in the shared scheduler.
    pub(crate) fn set_paused(&self, domain: u64, paused: bool) {
        self.shared
            .lock()
            .expect("scheduler mutex poisoned")
            .set_paused(domain, paused);
    }

    /// End a process incarnation: drop every task it owns, so none is ever polled
    /// again (docs/simulation-testing.md). The futures are dropped after the scheduler lock is
    /// released; see [`Inner::retire_domain`].
    pub(crate) fn retire_domain(&self, domain: u64) {
        let retired = {
            let mut inner = self.shared.lock().expect("scheduler mutex poisoned");
            inner.retire_domain(domain)
        };
        drop(retired);
    }
}

impl actor_core::Spawner for SimSpawner {
    fn launch(&self, task: BoxFuture<'static, ()>) {
        // A tagged spawner forces its node; an untagged one inherits the domain of
        // the task being polled (so child tasks of a node stay with that node).
        let domain = self.domain.or_else(|| CURRENT_DOMAIN.with(|d| d.get()));
        self.shared
            .lock()
            .expect("scheduler mutex poisoned")
            .spawn(task, domain);
    }
}

/// A deterministic simulation runtime: one seed drives time, randomness, task
/// scheduling, and the in-memory network.
///
/// A system takes the simulation's [`clock`](Simulation::clock),
/// [`entropy`](Simulation::entropy), and [`spawner`](Simulation::spawner), the
/// same traits production uses, so the system code and its codec run unchanged.
/// A single-node test builds the real local system on them:
///
/// ```ignore
/// let sim = Simulation::new(seed);
/// let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
///     .mailbox_capacity(cap)
///     .events(sink) // the event stream every check reads
///     .build();
/// sim.block_on(workload.run(system)); // returns when the future resolves
/// sim.run(); // the drain to quiescence is a separate, explicit step
/// ```
///
/// A cluster test runs real cluster nodes over a [`SimNetwork`](crate::SimNetwork)
/// on the same simulation; a [`ClusterWorkload`](crate::ClusterWorkload) and its
/// runners do that wiring.
pub struct Simulation {
    shared: Shared,
    entropy: SimEntropy,
}

impl Simulation {
    /// Create a runtime seeded by `seed`.
    pub fn new(seed: u64) -> Simulation {
        Simulation {
            shared: Arc::new(Mutex::new(Inner::new())),
            entropy: SimEntropy::new(seed),
        }
    }

    /// A clock handle backed by this runtime.
    pub fn clock(&self) -> SimClock {
        SimClock::new(Arc::clone(&self.shared))
    }

    /// A spawner handle backed by this runtime. Untagged (domain `None`), so the
    /// tasks a test driver launches through it are never frozen by pause/resume.
    pub fn spawner(&self) -> SimSpawner {
        SimSpawner {
            shared: Arc::clone(&self.shared),
            domain: None,
        }
    }

    /// The run's single entropy source.
    pub fn entropy(&self) -> SimEntropy {
        self.entropy.clone()
    }

    /// Current virtual time.
    pub fn now(&self) -> Instant {
        self.shared.lock().expect("scheduler mutex poisoned").now()
    }

    /// Drive the runtime to quiescence: poll ready tasks until none remain,
    /// advancing virtual time to each successive timer deadline, and return when
    /// nothing is ready and no timers remain.
    pub fn run(&self) {
        loop {
            // Phase 1: drain all ready work. Polling may ready more tasks, so
            // loop until the ready set is empty.
            while let Some(id) = self.take_ready() {
                self.poll_task(id);
            }

            // Phase 2: quiescent. Advance virtual time to the earliest timer
            // and fire every timer now due. If there are none, the run is over.
            if !self.advance_time() {
                break;
            }
        }
    }

    /// Drive the runtime for `dur` of virtual time, then stop, even if tasks or
    /// timers remain. Needed for perpetual workloads: a failure detector probes
    /// forever and so never quiesces.
    ///
    /// Every event due at or before the limit is processed, then the clock is
    /// advanced to the limit itself.
    pub fn run_for(&self, dur: Duration) {
        let limit = self.now() + dur;
        loop {
            while let Some(id) = self.take_ready() {
                self.poll_task(id);
            }
            let next = self
                .shared
                .lock()
                .expect("scheduler mutex poisoned")
                .timers
                .peek()
                .map(|t| t.deadline);
            match next {
                Some(deadline) if deadline <= limit => {
                    self.advance_time();
                }
                _ => break,
            }
        }
        let mut inner = self.shared.lock().expect("scheduler mutex poisoned");
        if inner.now < limit {
            inner.now = limit;
        }
    }

    /// Spawn `future`, drive the runtime until it resolves, and return its
    /// output.
    ///
    /// Returns at the poll that completes the future: whatever else is ready or
    /// timed at that point stays in the scheduler for the next [`run`],
    /// [`run_for`], or `block_on`. Draining the world is a separate operation: a
    /// workload that never quiesces (an entity whose idle check re-arms while it
    /// vetoes hibernation, a failure detector) would otherwise turn an awaited
    /// reply into a loop that never returns and records events without bound.
    ///
    /// Panics if the runtime quiesces (nothing ready, no timer left) before
    /// the future completes: the future was waiting on something nobody will
    /// ever send.
    ///
    /// [`run`]: Simulation::run
    /// [`run_for`]: Simulation::run_for
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let cell: Arc<Mutex<Option<F::Output>>> = Arc::new(Mutex::new(None));
        let out = Arc::clone(&cell);
        {
            let mut inner = self.shared.lock().expect("scheduler mutex poisoned");
            inner.spawn(
                Box::pin(async move {
                    let value = future.await;
                    *out.lock().expect("result mutex poisoned") = Some(value);
                }),
                None,
            );
        }
        loop {
            while let Some(id) = self.take_ready() {
                self.poll_task(id);
                if let Some(value) = cell.lock().expect("result mutex poisoned").take() {
                    return value;
                }
            }
            assert!(
                self.advance_time(),
                "block_on: the runtime quiesced before the future completed"
            );
        }
    }

    /// Spawn `future`, drive the runtime for exactly `settle` of virtual time
    /// (as [`run_for`] does), then return the future's output.
    ///
    /// The fixed-window sibling of [`block_on`], for workloads that never
    /// quiesce (a failure detector, Raft loops). `block_on` returns at the poll
    /// that resolves the future; this always runs the whole window, so each step
    /// of a scenario advances the clock by the same span however fast the reply
    /// came, and whatever the reply set in motion within the window has run by
    /// the time it returns.
    ///
    /// Panics if the future has not resolved by the end of the window.
    ///
    /// [`run_for`]: Simulation::run_for
    /// [`block_on`]: Simulation::block_on
    pub fn run_for_output<F>(&self, settle: Duration, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let cell: Arc<Mutex<Option<F::Output>>> = Arc::new(Mutex::new(None));
        let out = Arc::clone(&cell);
        self.shared.lock().expect("scheduler mutex poisoned").spawn(
            Box::pin(async move {
                let value = future.await;
                *out.lock().expect("result mutex poisoned") = Some(value);
            }),
            None,
        );
        self.run_for(settle);
        cell.lock()
            .expect("result mutex poisoned")
            .take()
            .expect("run_for_output: the future did not complete in the settle window")
    }

    /// Pick the next ready task. With more than one ready, selection is
    /// seed-randomized (docs/simulation-testing.md) to surface ordering-dependent bugs.
    fn take_ready(&self) -> Option<u64> {
        let mut inner = self.shared.lock().expect("scheduler mutex poisoned");
        // Fast path: no node is frozen. A run that never pauses draws entropy and
        // orders tasks byte-for-byte as one with pause absent would (the
        // reproducibility contract, spec §11).
        if inner.paused.is_empty() {
            return match inner.ready.len() {
                0 => None,
                1 => Some(inner.ready.swap_remove(0)),
                n => {
                    let idx = self.entropy.pick_index(n).expect("non-empty");
                    Some(inner.ready.swap_remove(idx))
                }
            };
        }
        // A node is frozen: choose only among ready tasks whose domain is not
        // paused. The rest stay in `ready`, unpolled, until the node resumes.
        let eligible: Vec<usize> = inner
            .ready
            .iter()
            .enumerate()
            .filter(|(_, id)| match inner.tasks.get(id).and_then(|t| t.domain) {
                Some(domain) => !inner.paused.contains(&domain),
                None => true, // untagged (driver) tasks are never frozen
            })
            .map(|(i, _)| i)
            .collect();
        match eligible.len() {
            0 => None,
            1 => Some(inner.ready.swap_remove(eligible[0])),
            n => {
                let k = self.entropy.pick_index(n).expect("non-empty");
                Some(inner.ready.swap_remove(eligible[k]))
            }
        }
    }

    /// Poll one task once. Its future is taken out for the duration so a
    /// self-wake cannot re-enter it.
    fn poll_task(&self, id: u64) {
        let Some(task) = self
            .shared
            .lock()
            .expect("scheduler mutex poisoned")
            .tasks
            .get(&id)
            .cloned()
        else {
            return;
        };

        let waker = Waker::from(Arc::new(TaskWaker {
            shared: Arc::downgrade(&self.shared),
            id,
        }));
        let mut cx = Context::from_waker(&waker);

        let mut slot = task.future.lock().expect("task future mutex poisoned");
        let Some(mut future) = slot.take() else {
            return;
        };
        // Make this task's node the current domain for the poll, so anything it
        // spawns inherits it; restore the previous domain afterward.
        let prev = CURRENT_DOMAIN.with(|d| d.replace(task.domain));
        let poll = future.as_mut().poll(&mut cx);
        CURRENT_DOMAIN.with(|d| d.set(prev));
        match poll {
            Poll::Ready(()) => {
                drop(slot);
                self.shared
                    .lock()
                    .expect("scheduler mutex poisoned")
                    .tasks
                    .remove(&id);
            }
            Poll::Pending => {
                *slot = Some(future);
            }
        }
    }

    /// Advance virtual time to the next timer deadline and fire every timer due
    /// at that instant. Returns `false` when no timers remain (full quiescence).
    fn advance_time(&self) -> bool {
        let due: Vec<Waker> = {
            let mut inner = self.shared.lock().expect("scheduler mutex poisoned");
            let Some(next) = inner.timers.peek().map(|t| t.deadline) else {
                return false;
            };
            inner.now = next;
            let mut due = Vec::new();
            while let Some(top) = inner.timers.peek() {
                if top.deadline <= inner.now {
                    due.push(inner.timers.pop().expect("peeked").waker);
                } else {
                    break;
                }
            }
            due
        };
        // Wake outside the lock: `TaskWaker::wake` re-acquires it.
        for waker in due {
            waker.wake();
        }
        true
    }
}
