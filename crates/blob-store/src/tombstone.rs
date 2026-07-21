//! Namespace deletion: the tombstone set and its membership-gated retention
//! (spec §5.3, §4, **B7**).
//!
//! `delete_namespace` reclaims a whole namespace without reference tracking. The
//! mechanism is a **tombstone** — a tiny `(ns, deleted_at)` record meaning "no
//! blob of `ns` may exist or be (re-)created" — not a sweep of known-live roots.
//! Because a namespace's blobs scatter across the whole cluster (owner selection
//! hashes `(ns, id)`, spec §5.2), a tombstone has two homes:
//!
//! - **cluster-wide awareness** ([`TombstoneSet`]): every serving node holds the
//!   set so it can refuse a `StoreBlob`, short-circuit a `get`, and skip a
//!   reconcile copy. The set is small (one entry per deleted namespace), monotonic
//!   (set-once), and gossip-able, so it needs no ordering and no term — the spec
//!   §4 thesis applied to deletion: a flag fanned out and gossiped, not an
//!   agreement round.
//! - a **durable anchor** with sweep tracking ([`AnchorTracker`]): the namespace's
//!   tombstone owners hold the loss-proof copy and record which nodes have finished
//!   sweeping, so the tombstone can be retained exactly as long as B7 safety
//!   requires and no longer.
//!
//! This module owns those two structures and, critically, the **retention
//! decision** ([`AnchorTracker::reclaimable`]): a tombstone outlives every node
//! that could still carry a stale copy, where *which* nodes those are is a
//! membership fact, not a clock reading. The cluster fan-out that anchors a
//! tombstone on `W` owners and disseminates it (spec §5.3) is the `Clustered`
//! tier's orchestration; the safety boundary lives here.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use actor_core::NodeId;
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
/// handle), interior-mutable, and monotonic — a namespace, once tombstoned, stays
/// tombstoned until it is *reclaimed* ([`AnchorTracker`]).
///
/// It is a grow-only set keyed by namespace; the associated `deleted_at` converges
/// to the **minimum** seen, so `merge` is commutative and idempotent regardless of
/// gossip order (namespaces are single-use, spec §2, so in practice one delete
/// stamps one value and the min is that value).
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

    /// Whether `ns` is tombstoned — the check `put`/`get`/reconcile consult.
    pub fn contains(&self, ns: &Namespace) -> bool {
        self.lock().contains_key(ns)
    }

    /// Every tombstone, for gossiping the set to a peer or answering a rejoining
    /// node's re-sync (spec §5.3).
    pub fn snapshot(&self) -> Vec<Tombstone> {
        self.lock()
            .iter()
            .map(|(ns, &deleted_at)| Tombstone {
                ns: ns.clone(),
                deleted_at,
            })
            .collect()
    }

    /// Absorb a peer's tombstones (gossip / rejoin re-sync). Monotonic and
    /// commutative — merging the same set twice, or in either direction, converges.
    pub fn merge(&self, tombstones: impl IntoIterator<Item = Tombstone>) {
        for tombstone in tombstones {
            self.insert(&tombstone.ns, tombstone.deleted_at);
        }
    }

    /// Drop `ns` from the awareness set once it has been **reclaimed** — every
    /// holder has swept or reached terminal `down`, so no copy can be resurrected
    /// and the namespace resolves nowhere anyway (spec §5.3). Driven only by the
    /// anchor's reclamation decision, never by a timer.
    pub fn remove(&self, ns: &Namespace) {
        self.lock().remove(ns);
    }

    /// How many namespaces are tombstoned.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<Namespace, u64>> {
        self.inner.lock().expect("tombstone set mutex poisoned")
    }
}

/// The **durable anchor's** sweep tracker for the namespaces it owns (spec §5.3):
/// it decides, per tombstoned namespace, when the tombstone may be reclaimed
/// without risking resurrection.
///
/// The hazard B7 closes is a node partitioned during the delete: it still holds
/// blobs of `ns`, and on rejoin reconcile would re-push them to owners that had
/// swept. So the tombstone must **outlive every node that could still carry a
/// stale copy**, and which nodes those are is a *membership fact*: every node that
/// was a serving member when the tombstone was anchored, until each has either
/// acked its sweep or reached the terminal `down`/`removed` state (actor §9.1).
/// That terminal state is load-bearing — `down` is irrevocable and absorbing, and
/// a downed node rejoins only under a fresh, empty identity, so it can never
/// return carrying an un-swept blob under its old id. A merely `unreachable` node
/// is **not** enough: that state is reversible, so its tombstone is held until it
/// returns and sweeps or is downed. The store owns **no grace timer**; it inherits
/// its safety boundary from the membership lattice.
#[derive(Default)]
pub struct AnchorTracker {
    inner: Mutex<BTreeMap<Namespace, AnchorState>>,
}

struct AnchorState {
    /// The serving set when the tombstone was anchored — the exact nodes that
    /// could hold a stale copy (spec §5.3).
    members_at_anchor: BTreeSet<NodeId>,
    /// Members that have acked their sweep.
    swept: BTreeSet<NodeId>,
}

impl AnchorTracker {
    /// A tracker holding no anchors.
    pub fn new() -> AnchorTracker {
        AnchorTracker::default()
    }

    /// Begin tracking `ns`, anchored over the serving set `members_at_anchor`.
    /// Set-once and monotonic: re-anchoring an already-tracked namespace (a
    /// redelivered delete) is a no-op, so it never resets sweep progress or the
    /// anchor's membership snapshot. The `deleted_at` stamp lives in the
    /// [`TombstoneSet`]; the retention decision needs only the membership snapshot.
    pub fn anchor(&self, ns: &Namespace, members_at_anchor: impl IntoIterator<Item = NodeId>) {
        self.lock()
            .entry(ns.clone())
            .or_insert_with(|| AnchorState {
                members_at_anchor: members_at_anchor.into_iter().collect(),
                swept: BTreeSet::new(),
            });
    }

    /// Record that `node` has acked its sweep of `ns`. A no-op if `ns` is not
    /// tracked (already reclaimed) or `node` was not a member at anchor time.
    pub fn record_sweep(&self, ns: &Namespace, node: NodeId) {
        if let Some(state) = self.lock().get_mut(ns)
            && state.members_at_anchor.contains(&node)
        {
            state.swept.insert(node);
        }
    }

    /// The members the tombstone is still waiting on: those that have neither
    /// acked their sweep nor reached a terminal state. `is_terminal(node)` reports
    /// whether `node` is `down`/`removed` (or otherwise gone for good) — the
    /// caller supplies it from membership, so this stays free of that dependency.
    /// An **empty** result for a tracked namespace means it is reclaimable.
    pub fn pending(&self, ns: &Namespace, is_terminal: impl Fn(NodeId) -> bool) -> Vec<NodeId> {
        // Collect the not-yet-swept members under the lock, then apply the caller's
        // predicate without holding it (the predicate may take a membership lock).
        let waiting: Vec<NodeId> = match self.lock().get(ns) {
            None => return Vec::new(),
            Some(state) => state
                .members_at_anchor
                .iter()
                .copied()
                .filter(|node| !state.swept.contains(node))
                .collect(),
        };
        waiting
            .into_iter()
            .filter(|node| !is_terminal(*node))
            .collect()
    }

    /// Whether `ns`'s tombstone may be reclaimed: it is tracked and **every**
    /// member at anchor time has either swept or reached a terminal state (spec
    /// §5.3, **B7**). A still-`unreachable` (reversible) member keeps it alive.
    pub fn reclaimable(&self, ns: &Namespace, is_terminal: impl Fn(NodeId) -> bool) -> bool {
        self.lock().contains_key(ns) && self.pending(ns, is_terminal).is_empty()
    }

    /// Stop tracking `ns` (it has been reclaimed). Returns whether it was tracked.
    /// Pair with [`TombstoneSet::remove`] to forget the namespace entirely.
    pub fn reclaim(&self, ns: &Namespace) -> bool {
        self.lock().remove(ns).is_some()
    }

    /// The namespaces currently tracked (for the reclamation pass and diagnostics).
    pub fn tracked(&self) -> Vec<Namespace> {
        self.lock().keys().cloned().collect()
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<Namespace, AnchorState>> {
        self.inner.lock().expect("anchor tracker mutex poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns(name: &str) -> Namespace {
        Namespace::new(name.as_bytes().to_vec())
    }

    fn node(id: u64) -> NodeId {
        NodeId::new(id)
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
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn merge_converges_regardless_of_order_or_repetition() {
        // The set is a grow-only CRDT keyed by namespace, with the stamp resolved
        // by min — so two nodes that gossip in either order converge identically.
        let left = TombstoneSet::new();
        let right = TombstoneSet::new();
        left.insert(&ns("a"), 10);
        left.insert(&ns("b"), 20);
        right.insert(&ns("b"), 15); // a lower stamp for the same namespace
        right.insert(&ns("c"), 30);

        left.merge(right.snapshot());
        right.merge(left.snapshot());
        left.merge(right.snapshot()); // idempotent: merging again changes nothing

        let mut l = left.snapshot();
        let mut r = right.snapshot();
        l.sort_by(|x, y| x.ns.cmp(&y.ns));
        r.sort_by(|x, y| x.ns.cmp(&y.ns));
        assert_eq!(l, r, "both nodes converge to the same set");
        assert_eq!(
            l,
            vec![
                Tombstone {
                    ns: ns("a"),
                    deleted_at: 10
                },
                Tombstone {
                    ns: ns("b"),
                    deleted_at: 15
                }, // min(20, 15)
                Tombstone {
                    ns: ns("c"),
                    deleted_at: 30
                },
            ],
        );
    }

    #[test]
    fn a_tombstone_is_held_until_every_member_sweeps() {
        // B7 safety: with no node terminal, the tombstone is retained until the
        // last member at anchor time has swept.
        let tracker = AnchorTracker::new();
        let none_terminal = |_n: NodeId| false;
        let a = ns("a");
        tracker.anchor(&a, [node(1), node(2), node(3)]);

        assert!(!tracker.reclaimable(&a, none_terminal));
        tracker.record_sweep(&a, node(1));
        tracker.record_sweep(&a, node(2));
        assert_eq!(tracker.pending(&a, none_terminal), vec![node(3)]);
        assert!(
            !tracker.reclaimable(&a, none_terminal),
            "one member still holds blobs"
        );

        tracker.record_sweep(&a, node(3));
        assert!(
            tracker.reclaimable(&a, none_terminal),
            "all swept → reclaimable"
        );
    }

    #[test]
    fn a_downed_member_releases_the_tombstone_but_an_unreachable_one_does_not() {
        // The load-bearing B7 case (spec §5.3, §8): a node partitioned through the
        // delete keeps the tombstone alive while it is merely `unreachable`
        // (reversible — it could return with its disk), and only releases it once
        // downed (irrevocable — it can rejoin only as a fresh, empty identity).
        let tracker = AnchorTracker::new();
        let a = ns("a");
        tracker.anchor(&a, [node(1), node(2), node(3)]);
        tracker.record_sweep(&a, node(1));
        // node(2) is partitioned and never sweeps; node(3) swept.
        tracker.record_sweep(&a, node(3));

        // While node(2) is only unreachable (not terminal), the tombstone is held —
        // forgetting it now is the one move that could resurrect a blob.
        let unreachable_2 = |_n: NodeId| false; // nobody terminal
        assert_eq!(tracker.pending(&a, unreachable_2), vec![node(2)]);
        assert!(
            !tracker.reclaimable(&a, unreachable_2),
            "an unreachable node holds it"
        );

        // Once node(2) is downed (terminal), no node can carry a stale copy: release.
        let downed_2 = |n: NodeId| n == node(2);
        assert!(
            tracker.reclaimable(&a, downed_2),
            "a terminal member releases the tombstone"
        );
    }

    #[test]
    fn retention_survives_an_unbounded_partition() {
        // B7: retention is gated on membership, not a clock. However many reclaim
        // passes run, a never-sweeping, never-downed member keeps the tombstone.
        let tracker = AnchorTracker::new();
        let a = ns("a");
        tracker.anchor(&a, [node(1), node(2)]);
        tracker.record_sweep(&a, node(1));
        let held = |_n: NodeId| false;
        for _ in 0..1_000 {
            assert!(
                !tracker.reclaimable(&a, held),
                "no timer can expire the tombstone"
            );
        }
    }

    #[test]
    fn anchoring_is_set_once_and_does_not_reset_progress() {
        let tracker = AnchorTracker::new();
        let a = ns("a");
        tracker.anchor(&a, [node(1), node(2)]);
        tracker.record_sweep(&a, node(1));
        // A redelivered delete re-anchors with a different (later) view; it must
        // not discard node(1)'s sweep or widen the member set.
        tracker.anchor(&a, [node(1), node(2), node(3)]);
        let none_terminal = |_n: NodeId| false;
        assert_eq!(
            tracker.pending(&a, none_terminal),
            vec![node(2)],
            "progress and the original anchor membership are preserved",
        );
    }

    #[test]
    fn reclaim_forgets_a_namespace() {
        let tracker = AnchorTracker::new();
        let a = ns("a");
        tracker.anchor(&a, [node(1)]);
        assert_eq!(tracker.tracked(), vec![a.clone()]);
        assert!(tracker.reclaim(&a));
        assert!(tracker.tracked().is_empty());
        // Reclaiming an untracked namespace is a harmless no-op.
        assert!(!tracker.reclaim(&a));
        // An untracked namespace is not reclaimable (nothing to reclaim).
        assert!(!tracker.reclaimable(&a, |_| true));
    }
}
