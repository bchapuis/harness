//! The deterministic single-thread executor (spec §18.1, §18.2).
//!
//! All three runtime capabilities share one [`Inner`] behind an `Arc<Mutex<_>>`:
//! the [`SimSpawner`](crate::SimSpawner) enqueues tasks, the
//! [`SimClock`](crate::SimClock) registers timers, and [`Simulation::run`]
//! drives them. The run loop is **quiescence-driven**: it polls ready tasks
//! until none remain, and only then advances virtual time to the next timer.
//! Logical time therefore costs no wall-clock time.
//!
//! Everything is built on safe primitives — the waker uses [`std::task::Wake`]
//! via `Arc`, so the crate honors the workspace `unsafe_code = "forbid"`.

use std::cmp::Ordering;
use std::collections::BTreeMap;
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
    /// among ready tasks is seed-randomized (spec §18.3), not FIFO.
    ready: Vec<u64>,
    /// Live tasks, keyed by id. `BTreeMap` keeps iteration deterministic.
    tasks: BTreeMap<u64, Arc<Task>>,
    timers: BinaryHeap<Timer>,
    next_task_id: u64,
    next_seq: u64,
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
        }
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

    fn spawn(&mut self, future: BoxFuture<'static, ()>) {
        let id = self.next_task_id;
        self.next_task_id += 1;
        self.tasks.insert(
            id,
            Arc::new(Task {
                future: Mutex::new(Some(future)),
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

/// Task spawner backed by the simulation scheduler (spec §4.6).
#[derive(Clone)]
pub struct SimSpawner {
    shared: Shared,
}

impl actor_core::Spawner for SimSpawner {
    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.shared
            .lock()
            .expect("scheduler mutex poisoned")
            .spawn(task);
    }
}

/// A deterministic simulation runtime: one seed drives time, randomness, task
/// scheduling, and (later) the in-memory network.
///
/// Hand out [`clock`](Simulation::clock), [`entropy`](Simulation::entropy), and
/// [`spawner`](Simulation::spawner) to construct a system, then drive it with
/// [`run`](Simulation::run) or [`block_on`](Simulation::block_on).
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

    /// A spawner handle backed by this runtime.
    pub fn spawner(&self) -> SimSpawner {
        SimSpawner {
            shared: Arc::clone(&self.shared),
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
            // Phase 1 — drain all ready work. Polling may ready more tasks, so
            // loop until the ready set is empty.
            while let Some(id) = self.take_ready() {
                self.poll_task(id);
            }

            // Phase 2 — quiescent. Advance virtual time to the earliest timer
            // and fire every timer now due. If there are none, the run is over.
            if !self.advance_time() {
                break;
            }
        }
    }

    /// Drive the runtime for `dur` of virtual time, then stop — even if tasks or
    /// timers remain. Needed for perpetual workloads (a failure detector probes
    /// forever and so never quiesces); a bounded run lets a test advance the
    /// cluster a fixed span and then inspect it.
    pub fn run_for(&self, dur: Duration) {
        let limit = self.now() + dur;
        self.run_until(limit);
    }

    /// Drive the runtime until virtual time reaches `limit`, processing every
    /// event due at or before it, then advance the clock to `limit`.
    pub fn run_until(&self, limit: Instant) {
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

    /// Spawn `future`, run to quiescence, and return its output. Panics if the
    /// future has not completed once the runtime is quiescent.
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let cell: Arc<Mutex<Option<F::Output>>> = Arc::new(Mutex::new(None));
        let out = Arc::clone(&cell);
        {
            let mut inner = self.shared.lock().expect("scheduler mutex poisoned");
            inner.spawn(Box::pin(async move {
                let value = future.await;
                *out.lock().expect("result mutex poisoned") = Some(value);
            }));
        }
        self.run();
        cell.lock()
            .expect("result mutex poisoned")
            .take()
            .expect("future did not complete at quiescence")
    }

    /// Pick the next ready task. With more than one ready, selection is
    /// seed-randomized (spec §18.3) to surface ordering-dependent bugs.
    fn take_ready(&self) -> Option<u64> {
        let mut inner = self.shared.lock().expect("scheduler mutex poisoned");
        match inner.ready.len() {
            0 => None,
            1 => Some(inner.ready.swap_remove(0)),
            n => {
                let idx = self.entropy.pick_index(n).expect("non-empty");
                Some(inner.ready.swap_remove(idx))
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
        match future.as_mut().poll(&mut cx) {
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
