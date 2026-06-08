//! History recording and linearizability checking (spec §18.4).
//!
//! The invariant catalogue (§18.5) checks *safety predicates over the event
//! stream* — "a bad thing never happens". This module attacks correctness from
//! the other side: record the history of operations a *client* observed — each as
//! an invoke/complete pair, with concurrency preserved — then ask whether that
//! history could have been produced by some legal sequential ordering of a
//! reference object. That is **linearizability**.
//!
//! The machinery:
//!
//! - A [`History`] an in-simulation client records into, marking each operation
//!   `invoke` → (`ok` with the observed return | `info` unknown | `fail` did not
//!   happen). Recording is by the public API only (spec §18.4), so the checker
//!   sees exactly what a real client would.
//! - A [`check`] that decides linearizability by the **Wing & Gong** algorithm:
//!   linearize operations one at a time, only ever choosing one whose invoke
//!   precedes the earliest still-pending completion, backtracking on a dead end,
//!   memoizing visited `(linearized-set, state)` pairs so the exponential search
//!   stays tractable.
//! - A [`Model`] trait with two reference objects, [`Register`] and [`Counter`].
//!
//! `info` (unknown-outcome) operations are the crux of distributed testing: an
//! `ask` that returns `Unreachable` or `Timeout` under fault injection *may or
//! may not* have taken effect. The checker models that honestly — a pending
//! operation may be linearized at any later point, or not at all.

use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;

/// A deterministic sequential reference object: the "model" a history is checked
/// against. `Register` and `Counter` implement it; any single-object,
/// deterministic ADT can.
pub trait Model {
    /// The object's abstract state.
    type State: Clone + Eq + Hash;
    /// An operation a client can invoke.
    type Op: Clone + Debug;
    /// The value an operation returns.
    type Ret: Clone + Eq + Debug;

    /// The initial state.
    fn init() -> Self::State;

    /// Apply `op` to `state`, returning the resulting state and the value the
    /// object would return. Deterministic: one input, one output.
    fn step(state: &Self::State, op: &Self::Op) -> (Self::State, Self::Ret);
}

/// What a client learned about an invoked operation's fate.
enum Outcome<R> {
    /// Completed; the client observed this return value.
    Ok(R),
    /// Outcome unknown (e.g. `Unreachable`/`Timeout`): may or may not have taken
    /// effect.
    Info,
    /// Definitely did not take effect; excluded from the model entirely.
    Fail,
}

/// One recorded event, in real (virtual) time order: position in the log is the
/// timestamp.
enum Mark<M: Model> {
    Invoke { id: usize, op: M::Op },
    Complete { id: usize, outcome: Outcome<M::Ret> },
}

struct Inner<M: Model> {
    marks: Vec<Mark<M>>,
    next_id: usize,
}

/// A handle to an in-flight operation, returned by [`History::invoke`] and
/// passed to one of `ok`/`info`/`fail` once its fate is known.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpId(usize);

/// A client-observed operation history (spec §18.4), recorded live during a run.
/// Clone to share one log among several concurrent client tasks; the recorded
/// order is their real-time interleaving (the executor is single-threaded, so
/// the order is exactly the virtual-time order in which calls returned).
pub struct History<M: Model> {
    inner: Arc<Mutex<Inner<M>>>,
}

impl<M: Model> Clone for History<M> {
    fn clone(&self) -> Self {
        History {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<M: Model> Default for History<M> {
    fn default() -> Self {
        History::new()
    }
}

impl<M: Model> History<M> {
    /// A fresh, empty history.
    pub fn new() -> History<M> {
        History {
            inner: Arc::new(Mutex::new(Inner {
                marks: Vec::new(),
                next_id: 0,
            })),
        }
    }

    /// Record the invocation of `op`; the returned [`OpId`] completes it later.
    pub fn invoke(&self, op: M::Op) -> OpId {
        let mut inner = self.inner.lock().expect("history mutex poisoned");
        let id = inner.next_id;
        inner.next_id += 1;
        inner.marks.push(Mark::Invoke { id, op });
        OpId(id)
    }

    /// Record that `id` completed, observing return value `ret`.
    pub fn ok(&self, id: OpId, ret: M::Ret) {
        self.complete(id, Outcome::Ok(ret));
    }

    /// Record that `id`'s outcome is unknown (e.g. `Unreachable`/`Timeout`): it
    /// may or may not have taken effect.
    pub fn info(&self, id: OpId) {
        self.complete(id, Outcome::Info);
    }

    /// Record that `id` definitely did not take effect; it is excluded from the
    /// model.
    pub fn fail(&self, id: OpId) {
        self.complete(id, Outcome::Fail);
    }

    fn complete(&self, id: OpId, outcome: Outcome<M::Ret>) {
        self.inner
            .lock()
            .expect("history mutex poisoned")
            .marks
            .push(Mark::Complete { id: id.0, outcome });
    }

    /// The number of operations invoked so far.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("history mutex poisoned").next_id
    }

    /// Whether no operation has been invoked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Lower the recorded marks into the operation list the linearizer searches.
    /// `Fail` operations are dropped (they had no effect); `Info` and never-
    /// completed operations become *pending* (completion at `usize::MAX`), so the
    /// search may linearize them anywhere after their invoke, or not at all.
    fn ops(&self) -> Vec<LinOp<M>> {
        let inner = self.inner.lock().expect("history mutex poisoned");
        let mut invoke: Vec<Option<(usize, M::Op)>> = (0..inner.next_id).map(|_| None).collect();
        let mut complete: Vec<Option<(usize, Outcome<M::Ret>)>> =
            (0..inner.next_id).map(|_| None).collect();

        for (ts, mark) in inner.marks.iter().enumerate() {
            match mark {
                Mark::Invoke { id, op } => invoke[*id] = Some((ts, op.clone())),
                Mark::Complete { id, outcome } => {
                    let cloned = match outcome {
                        Outcome::Ok(r) => Outcome::Ok(r.clone()),
                        Outcome::Info => Outcome::Info,
                        Outcome::Fail => Outcome::Fail,
                    };
                    complete[*id] = Some((ts, cloned));
                }
            }
        }

        let mut ops = Vec::new();
        for id in 0..inner.next_id {
            let Some((invoke_ts, op)) = invoke[id].take() else {
                continue;
            };
            let (complete_ts, ret) = match complete[id].take() {
                Some((_, Outcome::Fail)) => continue, // no effect: drop
                Some((ts, Outcome::Ok(r))) => (ts, Some(r)),
                Some((_, Outcome::Info)) | None => (usize::MAX, None), // pending
            };
            ops.push(LinOp {
                op,
                ret,
                invoke_ts,
                complete_ts,
            });
        }
        ops
    }
}

/// An operation lowered for the linearizer: its invoke time, its completion time
/// (`usize::MAX` if pending), and the observed return (`None` if pending).
struct LinOp<M: Model> {
    op: M::Op,
    ret: Option<M::Ret>,
    invoke_ts: usize,
    complete_ts: usize,
}

/// The verdict of a linearizability [`check`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Linearization {
    /// A legal sequential ordering consistent with the history exists.
    Ok,
    /// No such ordering exists; the history violates linearizability.
    Violation { detail: String },
}

impl Linearization {
    /// Whether the history was linearizable.
    pub fn is_ok(&self) -> bool {
        matches!(self, Linearization::Ok)
    }
}

/// The largest history the bitmask-based linearizer accepts. Wing & Gong is
/// worst-case exponential; memoization keeps realistic histories fast, but
/// workloads should keep a single checked history within this bound (long
/// histories are best checked in chunks).
pub const MAX_HISTORY: usize = 128;

/// Decide whether `history` is linearizable with respect to model `M`, by the
/// Wing & Gong search (spec §18.4). Returns [`Linearization::Ok`] with a
/// witnessing order found, or [`Linearization::Violation`] if none exists.
pub fn check<M: Model>(history: &History<M>) -> Linearization {
    let ops = history.ops();
    assert!(
        ops.len() <= MAX_HISTORY,
        "history of {} ops exceeds the linearizer bound of {MAX_HISTORY}; \
         shorten the workload or check in chunks",
        ops.len(),
    );

    let mut used: u128 = 0;
    let mut memo: HashSet<(u128, M::State)> = HashSet::new();
    if search::<M>(&ops, &mut used, M::init(), &mut memo) {
        Linearization::Ok
    } else {
        Linearization::Violation {
            detail: format!(
                "no sequential order linearizes the {} completed/pending operations",
                ops.len()
            ),
        }
    }
}

/// The Wing & Gong recursion. `used` is a bitmask of already-linearized ops;
/// `state` is the model state after them. Returns whether the remaining ops can
/// be linearized from here.
fn search<M: Model>(
    ops: &[LinOp<M>],
    used: &mut u128,
    state: M::State,
    memo: &mut HashSet<(u128, M::State)>,
) -> bool {
    // The earliest completion still outstanding. An operation may be linearized
    // next only if it was invoked before this instant — otherwise the operation
    // that completes here, having both started and finished first, would have to
    // come after it, which no real-time order allows.
    let mut min_complete = usize::MAX;
    let mut any_completed = false;
    for (i, o) in ops.iter().enumerate() {
        if *used & (1 << i) != 0 {
            continue;
        }
        if o.complete_ts != usize::MAX {
            any_completed = true;
            min_complete = min_complete.min(o.complete_ts);
        }
    }

    // Only pending (unknown-outcome) operations remain: drop them all. A pending
    // op never *has* to take effect, so an all-pending tail is always linearized.
    if !any_completed {
        return true;
    }

    let key = (*used, state.clone());
    if memo.contains(&key) {
        return false;
    }

    for (i, o) in ops.iter().enumerate() {
        if *used & (1 << i) != 0 || o.invoke_ts >= min_complete {
            continue;
        }
        let (next_state, expected) = M::step(&state, &o.op);
        // A completed op must return what the model says; a pending op may take
        // any legal step (we never observed its return).
        let consistent = match &o.ret {
            Some(observed) => *observed == expected,
            None => true,
        };
        if consistent {
            *used |= 1 << i;
            if search::<M>(ops, used, next_state, memo) {
                *used &= !(1 << i);
                return true;
            }
            *used &= !(1 << i);
        }
    }

    memo.insert(key);
    false
}

// --- Reference models ---------------------------------------------------------

/// A linearizable read/write/compare-and-set register over `i64` — the canonical
/// reference object.
pub struct Register;

/// An operation on a [`Register`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegisterOp {
    Read,
    Write(i64),
    /// Compare-and-set: if the value equals the first field, set it to the second.
    Cas(i64, i64),
}

/// The return value of a [`RegisterOp`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegisterRet {
    /// The value observed by a `Read`.
    Read(i64),
    /// A `Write` completed.
    WriteOk,
    /// A `Cas` ran; the flag is whether it swapped.
    Cas(bool),
}

impl Model for Register {
    type State = i64;
    type Op = RegisterOp;
    type Ret = RegisterRet;

    fn init() -> i64 {
        0
    }

    fn step(state: &i64, op: &RegisterOp) -> (i64, RegisterRet) {
        match *op {
            RegisterOp::Read => (*state, RegisterRet::Read(*state)),
            RegisterOp::Write(v) => (v, RegisterRet::WriteOk),
            RegisterOp::Cas(old, new) => {
                if *state == old {
                    (new, RegisterRet::Cas(true))
                } else {
                    (*state, RegisterRet::Cas(false))
                }
            }
        }
    }
}

/// A linearizable counter supporting add and read — the other canonical
/// reference object. Useful because add is *not* idempotent, so a duplicated or
/// replayed operation that took effect twice is a linearizability violation the
/// checker catches.
pub struct Counter;

/// An operation on a [`Counter`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CounterOp {
    Add(i64),
    Read,
}

/// The return value of a [`CounterOp`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CounterRet {
    AddOk,
    Read(i64),
}

impl Model for Counter {
    type State = i64;
    type Op = CounterOp;
    type Ret = CounterRet;

    fn init() -> i64 {
        0
    }

    fn step(state: &i64, op: &CounterOp) -> (i64, CounterRet) {
        match *op {
            CounterOp::Add(d) => (*state + d, CounterRet::AddOk),
            CounterOp::Read => (*state, CounterRet::Read(*state)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a history by replaying a script of marks, so tests can pin exact
    /// real-time interleavings. Each tuple is (kind, id-or-op).
    fn reg() -> History<Register> {
        History::new()
    }

    #[test]
    fn empty_history_is_linearizable() {
        assert!(check(&reg()).is_ok());
    }

    #[test]
    fn sequential_consistent_history_is_linearizable() {
        let h = reg();
        let w = h.invoke(RegisterOp::Write(1));
        h.ok(w, RegisterRet::WriteOk);
        let r = h.invoke(RegisterOp::Read);
        h.ok(r, RegisterRet::Read(1));
        assert!(check(&h).is_ok());
    }

    #[test]
    fn a_read_that_misses_a_committed_write_is_not_linearizable() {
        // write(1) completes, *then* a read begins and returns 0. No order
        // explains this: the read is wholly after the write.
        let h = reg();
        let w = h.invoke(RegisterOp::Write(1));
        h.ok(w, RegisterRet::WriteOk);
        let r = h.invoke(RegisterOp::Read);
        h.ok(r, RegisterRet::Read(0));
        assert!(!check(&h).is_ok());
    }

    #[test]
    fn a_concurrent_stale_read_is_linearizable() {
        // A read overlapping a write may linearize before it, so observing the
        // old value 0 is legal.
        let h = reg();
        let w = h.invoke(RegisterOp::Write(1)); // invoke write
        let r = h.invoke(RegisterOp::Read); // read begins while write in flight
        h.ok(r, RegisterRet::Read(0)); // read returns old value
        h.ok(w, RegisterRet::WriteOk); // write completes
        assert!(check(&h).is_ok());
    }

    #[test]
    fn two_reads_straddling_a_write_must_agree_with_some_order() {
        // P1 reads 0 then P2 reads 1 while a write(1) is in flight: linearizable
        // (write linearizes between the two reads).
        let h = reg();
        let w = h.invoke(RegisterOp::Write(1));
        let r1 = h.invoke(RegisterOp::Read);
        h.ok(r1, RegisterRet::Read(0));
        let r2 = h.invoke(RegisterOp::Read);
        h.ok(r2, RegisterRet::Read(1));
        h.ok(w, RegisterRet::WriteOk);
        assert!(check(&h).is_ok());
    }

    #[test]
    fn a_read_reverting_to_an_old_value_after_a_later_read_is_not_linearizable() {
        // write(1) commits; read sees 1; a *later* read sees 0. The register
        // cannot un-write, and no concurrency excuses it.
        let h = reg();
        let w = h.invoke(RegisterOp::Write(1));
        h.ok(w, RegisterRet::WriteOk);
        let r1 = h.invoke(RegisterOp::Read);
        h.ok(r1, RegisterRet::Read(1));
        let r2 = h.invoke(RegisterOp::Read);
        h.ok(r2, RegisterRet::Read(0));
        assert!(!check(&h).is_ok());
    }

    #[test]
    fn cas_success_and_failure_are_modeled() {
        let h = reg();
        let w = h.invoke(RegisterOp::Write(1));
        h.ok(w, RegisterRet::WriteOk);
        let c1 = h.invoke(RegisterOp::Cas(1, 2)); // matches -> swaps
        h.ok(c1, RegisterRet::Cas(true));
        let c2 = h.invoke(RegisterOp::Cas(1, 3)); // no longer 1 -> fails
        h.ok(c2, RegisterRet::Cas(false));
        let r = h.invoke(RegisterOp::Read);
        h.ok(r, RegisterRet::Read(2));
        assert!(check(&h).is_ok());
    }

    #[test]
    fn a_lying_cas_is_not_linearizable() {
        // The register is 0, but a Cas(1,2) claims it swapped.
        let h = reg();
        let c = h.invoke(RegisterOp::Cas(1, 2));
        h.ok(c, RegisterRet::Cas(true));
        assert!(!check(&h).is_ok());
    }

    #[test]
    fn a_pending_write_may_be_ignored() {
        // A write whose outcome is unknown (info) never observed to take effect:
        // dropping it leaves a linearizable history.
        let h = reg();
        let w = h.invoke(RegisterOp::Write(7));
        h.info(w); // unknown
        let r = h.invoke(RegisterOp::Read);
        h.ok(r, RegisterRet::Read(0)); // never saw the write
        assert!(check(&h).is_ok());
    }

    #[test]
    fn a_pending_write_may_also_be_taken_to_have_happened() {
        // The same unknown write, but a later read *does* see it: linearizable by
        // taking the pending write to have committed.
        let h = reg();
        let w = h.invoke(RegisterOp::Write(7));
        h.info(w);
        let r = h.invoke(RegisterOp::Read);
        h.ok(r, RegisterRet::Read(7));
        assert!(check(&h).is_ok());
    }

    #[test]
    fn counter_detects_a_double_applied_add() {
        // add(1) once, but the counter reads 2: an add took effect twice (e.g. a
        // duplicated, non-idempotent operation). Not linearizable.
        let h: History<Counter> = History::new();
        let a = h.invoke(CounterOp::Add(1));
        h.ok(a, CounterRet::AddOk);
        let r = h.invoke(CounterOp::Read);
        h.ok(r, CounterRet::Read(2));
        assert!(!check(&h).is_ok());
    }

    #[test]
    fn counter_accepts_concurrent_adds() {
        // Two concurrent add(1)s then a read of 2: linearizable in either order.
        let h: History<Counter> = History::new();
        let a1 = h.invoke(CounterOp::Add(1));
        let a2 = h.invoke(CounterOp::Add(1));
        h.ok(a1, CounterRet::AddOk);
        h.ok(a2, CounterRet::AddOk);
        let r = h.invoke(CounterOp::Read);
        h.ok(r, CounterRet::Read(2));
        assert!(check(&h).is_ok());
    }
}
