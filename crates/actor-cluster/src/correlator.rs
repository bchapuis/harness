//! Request/reply correlation on an association (spec §7.1, §10).
//!
//! Outbound requests that expect a reply — an `ask` awaiting its `Reply`, a SWIM
//! probe awaiting its `Ack` — register a waiter under a fresh id and park until
//! the reply arrives or a deadline fires. A [`Correlator`] is that id-source plus
//! waiter registry, the one mechanism behind both the `ask` and the SWIM probe
//! paths (which previously kept parallel counters and maps).

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// A monotonic id source plus a registry of in-flight waiters keyed by id. `K`
/// is the correlation key (e.g. `CallId` or a SWIM `seq`); `V` is the parked
/// waiter (typically a `oneshot::Sender`, paired with whatever context the reply
/// path needs).
pub(crate) struct Correlator<K, V> {
    next: AtomicU64,
    waiting: Mutex<BTreeMap<K, V>>,
}

impl<K, V> Correlator<K, V>
where
    K: Ord + Copy + From<u64>,
{
    pub(crate) fn new() -> Correlator<K, V> {
        Correlator {
            next: AtomicU64::new(0),
            waiting: Mutex::new(BTreeMap::new()),
        }
    }

    /// Allocate the next correlation id. Monotonic and never reused within a run.
    pub(crate) fn next_id(&self) -> K {
        K::from(self.next.fetch_add(1, Ordering::Relaxed))
    }

    /// Park `waiter` under `id` until its reply arrives (or it is taken by a
    /// timeout/cascade).
    pub(crate) fn register(&self, id: K, waiter: V) {
        self.lock().insert(id, waiter);
    }

    /// Remove and return the waiter for `id`, if it is still registered. Used to
    /// complete a reply, and to clean up after a deadline.
    pub(crate) fn take(&self, id: K) -> Option<V> {
        self.lock().remove(&id)
    }

    /// Remove and return every waiter whose value matches `pred`. The node-down
    /// cascade uses this to fail all in-flight calls bound for a dead node
    /// (spec §8.1 step 3).
    pub(crate) fn take_matching(&self, pred: impl Fn(&V) -> bool) -> Vec<V> {
        let mut waiting = self.lock();
        let ids: Vec<K> = waiting
            .iter()
            .filter(|(_, v)| pred(v))
            .map(|(id, _)| *id)
            .collect();
        ids.into_iter()
            .filter_map(|id| waiting.remove(&id))
            .collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<K, V>> {
        self.waiting.lock().expect("correlator mutex poisoned")
    }
}
