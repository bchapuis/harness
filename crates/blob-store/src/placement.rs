//! Owner selection: a thin, deterministic reuse of `actor-cluster`'s rendezvous
//! hashing (spec §5.2, §5.3, **B5**).
//!
//! Every node computes the identical owner list for a given key and membership
//! view, so a writer and a reader agree on where a blob lives with no directory
//! lookup. Two keys are derived here:
//!
//! - [`key`] hashes `(namespace, content hash)` **together**, so a blob's `R`
//!   owners ([`owners`]) are spread evenly across the cluster and no single
//!   namespace concentrates load on `R` nodes (spec §5.2). The cost is that a
//!   namespace's blobs scatter cluster-wide, which is why deletion fans out to
//!   every node rather than to `R` (spec §5.3).
//! - [`key_ns`] hashes the **namespace alone**, giving a namespace's tombstone a
//!   stable home — its [`tombstone_owners`] — independent of any blob, where the
//!   durable tombstone is anchored and sweep completion is reported (spec §5.3).
//!
//! Both are pure functions of their inputs, and [`placement::top`] reassigns only
//! the keys whose owner set actually changed when membership changes (utilities
//! U1), so a single join or leave moves a minimal share of blobs.

use actor_cluster::placement;
use actor_core::NodeId;

use crate::blob::BlobId;
use crate::blob::Namespace;

/// The rendezvous key that places a blob's owners: the namespace bytes followed
/// by the 32-byte content hash (spec §5.2). Hashing them together — rather than
/// the id alone — spreads each namespace's blobs across the whole cluster.
pub fn key(ns: &Namespace, id: &BlobId) -> Vec<u8> {
    let ns = ns.as_bytes();
    let mut key = Vec::with_capacity(ns.len() + 32);
    key.extend_from_slice(ns);
    key.extend_from_slice(id.as_bytes());
    key
}

/// The rendezvous key that places a namespace's **tombstone anchor**: the
/// namespace bytes alone (spec §5.3). Independent of any blob, so the anchor is a
/// stable home for the durable tombstone even as the namespace's blobs come and
/// go.
pub fn key_ns(ns: &Namespace) -> Vec<u8> {
    ns.as_bytes().to_vec()
}

/// The `R` owner nodes of `(ns, id)`: the `R` highest-ranked `members` under
/// rendezvous hashing of [`key`] (spec §5.2). Returns fewer than `r` entries only
/// when the candidate set is smaller. `members` is the serving set
/// (`Membership::serving_members`); the result is independent of its order.
pub fn owners(members: &[NodeId], ns: &Namespace, id: &BlobId, r: usize) -> Vec<NodeId> {
    placement::top(members, &key(ns, id), r)
}

/// The `R` tombstone-anchor owners of `ns`: the `R` highest-ranked `members`
/// under rendezvous hashing of [`key_ns`] (spec §5.3). `delete_namespace` anchors
/// the tombstone on `W` of these and they track each node's sweep completion.
pub fn tombstone_owners(members: &[NodeId], ns: &Namespace, r: usize) -> Vec<NodeId> {
    placement::top(members, &key_ns(ns), r)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(ids: &[u64]) -> Vec<NodeId> {
        ids.iter().map(|&i| NodeId::new(i)).collect()
    }

    fn ns() -> Namespace {
        Namespace::new(b"workspace-7".to_vec())
    }

    #[test]
    fn owners_are_deterministic_and_order_independent() {
        // B5: every node agrees on a blob's owners for a given view, regardless of
        // the order its membership list happens to be in (`top` ranks by weight).
        let id = BlobId::of(b"a block");
        let forward = owners(&nodes(&[1, 2, 3, 4, 5]), &ns(), &id, 3);
        let shuffled = owners(&nodes(&[4, 1, 5, 3, 2]), &ns(), &id, 3);
        assert_eq!(
            forward, shuffled,
            "owner list must not depend on input order"
        );
        assert_eq!(
            forward.len(),
            3,
            "R owners selected from a larger candidate set"
        );
    }

    #[test]
    fn owners_clamp_to_the_candidate_set() {
        // Fewer than R members yields fewer owners, never a panic (spec §5.2).
        let id = BlobId::of(b"a block");
        assert_eq!(owners(&nodes(&[1, 2]), &ns(), &id, 3).len(), 2);
        assert!(owners(&[], &ns(), &id, 3).is_empty());
    }

    #[test]
    fn the_blob_key_and_the_namespace_key_are_distinct() {
        // A blob's owners (keyed on ns+id) and the namespace's anchor (keyed on ns
        // alone) are placed by different keys, so they need not coincide.
        let ns = ns();
        let id = BlobId::of(b"a block");
        assert_ne!(key(&ns, &id), key_ns(&ns));
        // The anchor depends only on the namespace, not on any particular blob.
        let other = BlobId::of(b"another block");
        assert_eq!(key_ns(&ns), key_ns(&ns));
        assert_ne!(key(&ns, &id), key(&ns, &other));
    }

    #[test]
    fn tombstone_owners_depend_only_on_the_namespace() {
        let r = 3;
        let members = nodes(&[1, 2, 3, 4, 5]);
        let a = tombstone_owners(&members, &Namespace::new(b"alpha".to_vec()), r);
        let a_again = tombstone_owners(&members, &Namespace::new(b"alpha".to_vec()), r);
        let b = tombstone_owners(&members, &Namespace::new(b"beta".to_vec()), r);
        assert_eq!(a, a_again, "same namespace, same anchor");
        // Different namespaces generally anchor differently (spreads the load).
        assert_ne!(a, b);
    }

    #[test]
    fn one_membership_change_moves_minimal_blobs() {
        // U1 / B5 minimal-movement: adding a node only ever pulls a key's primary
        // owner *onto* the new node; every key whose owner changed must now be
        // owned by the newcomer, and no key moves between two pre-existing nodes.
        let before = nodes(&[1, 2, 3, 4]);
        let newcomer = NodeId::new(5);
        let mut after = before.clone();
        after.push(newcomer);

        let ns = ns();
        let mut moved = 0;
        for i in 0..512u64 {
            let id = BlobId::of(&i.to_le_bytes());
            let owner_before = owners(&before, &ns, &id, 1);
            let owner_after = owners(&after, &ns, &id, 1);
            if owner_before != owner_after {
                moved += 1;
                assert_eq!(
                    owner_after,
                    vec![newcomer],
                    "a key only moves onto the new node, never between existing ones",
                );
            }
        }
        // Some keys moved (the change took effect) but far from all of them
        // (movement is ~1/N, not a reshuffle).
        assert!(moved > 0, "adding a node must capture some keys");
        assert!(
            moved < 256,
            "movement must be a minority of keys, not a reshuffle"
        );
    }
}
