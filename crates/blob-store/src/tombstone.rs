//! Namespace deletion: the tombstone record and the awareness set (spec §5.3,
//! §4, **B7**).
//!
//! `delete_namespace` reclaims a whole namespace without reference tracking. The
//! mechanism is a **tombstone**: a tiny `(ns, deleted_at)` record meaning "no
//! blob of `ns` may exist or be (re-)created". Because a namespace's blobs
//! scatter across the whole cluster (owner selection hashes `(ns, id)`, spec
//! §5.2), every serving node holds the set ([`TombstoneSet`]) so it can refuse a
//! `StoreBlob`, short-circuit a `get`, and skip a reconcile copy. The set is small
//! (one entry per deleted namespace), monotonic (set-once), and gossip-able, so it
//! needs no ordering and no term. A tombstone is never reclaimed (spec §10), which
//! is what lets B7 hold by retention alone.
//!
//! The cluster fan-out that anchors a tombstone on `W` owners and disseminates it
//! (spec §5.3) is the `Clustered` tier's orchestration.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use serde::Deserialize;
use serde::Serialize;

use crate::blob::Namespace;

/// A namespace tombstone: "no blob of `ns` may exist or be (re-)created" (spec
/// §5.3). `deleted_at` is the anchoring stamp, carried for diagnostics and gossip
/// convergence; tombstone *presence*, not its stamp, makes a namespace gone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    pub ns: Namespace,
    pub deleted_at: u64,
}

/// The set of tombstoned namespaces a node is aware of (spec §5.3): the
/// **cluster-wide awareness** home of the tombstone. Shared (cloning shares the
/// handle), interior-mutable, and monotonic: a namespace, once tombstoned, stays
/// tombstoned.
///
/// It is a grow-only set keyed by namespace; the associated `deleted_at` converges
/// to the **minimum** seen, so learning the same tombstone from several peers is
/// order-independent.
#[derive(Clone, Default)]
pub struct TombstoneSet {
    inner: Arc<Mutex<BTreeMap<Namespace, u64>>>,
}

impl TombstoneSet {
    /// An empty set.
    pub fn new() -> TombstoneSet {
        TombstoneSet::default()
    }

    /// Record `ns` as tombstoned. Returns `true` if it was newly added (so a
    /// caller can fan it out or persist it), `false` if already known. On conflict
    /// the stored stamp converges to the minimum, keeping the set order-independent.
    pub fn insert(&self, ns: &Namespace, deleted_at: u64) -> bool {
        let mut map = self.lock();
        match map.get_mut(ns) {
            Some(existing) => {
                *existing = (*existing).min(deleted_at);
                false
            }
            None => {
                map.insert(ns.clone(), deleted_at);
                true
            }
        }
    }

    /// Whether `ns` is tombstoned: the check `put`/`get`/reconcile consult.
    pub fn contains(&self, ns: &Namespace) -> bool {
        self.lock().contains_key(ns)
    }

    /// Every tombstone, for answering a peer's anti-entropy pull (spec §5.3).
    pub fn snapshot(&self) -> Vec<Tombstone> {
        self.lock()
            .iter()
            .map(|(ns, &deleted_at)| Tombstone {
                ns: ns.clone(),
                deleted_at,
            })
            .collect()
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<Namespace, u64>> {
        self.inner.lock().expect("tombstone set mutex poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns(name: &str) -> Namespace {
        Namespace::new(name.as_bytes().to_vec())
    }

    #[test]
    fn the_set_is_monotonic_and_consulted_by_presence() {
        let set = TombstoneSet::new();
        let a = ns("alpha");
        assert!(!set.contains(&a));
        assert!(set.insert(&a, 5), "first insert is new");
        assert!(set.contains(&a));
        assert!(
            !set.insert(&a, 9),
            "re-inserting an existing namespace is not new"
        );
        assert_eq!(set.snapshot().len(), 1, "the re-insert added no entry");
    }

    #[test]
    fn the_stamp_converges_to_the_minimum_regardless_of_order() {
        // Two nodes that learn the same tombstone with different stamps, in either
        // order, end with the same entry.
        let left = TombstoneSet::new();
        let right = TombstoneSet::new();
        left.insert(&ns("b"), 20);
        left.insert(&ns("b"), 15);
        right.insert(&ns("b"), 15);
        right.insert(&ns("b"), 20);
        assert_eq!(left.snapshot(), right.snapshot());
        assert_eq!(
            left.snapshot(),
            vec![Tombstone {
                ns: ns("b"),
                deleted_at: 15
            }]
        );
    }
}
