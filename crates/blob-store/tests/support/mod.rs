//! The machine-readable B-catalogue, B1–B7 (blob-store spec §9).
//!
//! The blob store's analogue of the core, utilities, harness, sandbox, granary,
//! and machine catalogues: one table linking each invariant to the code that
//! verifies it, guarded by the drift test in `conformance_catalogue.rs`.
//! `invariant: n` reads as "Bn".
//!
//! This crate is why the pattern exists. Its §9 column was prose, and prose rots
//! silently: seven of its cited test names had drifted from the suites they named
//! — every one of them still describing a real property, none of them still
//! naming a real test — and nothing failed, because nothing was checking. The
//! pointers below are checked.
//!
//! Verification is split between this crate's `tests/` and the unit tests beside
//! the code in `src/` (address integrity, dedup, placement, and tombstone
//! retention are all local properties with no cluster in the loop), so a pointer
//! containing a `/` resolves relative to `crates/` and a bare filename relative to
//! this crate's `tests/`.

#![allow(dead_code)]

use actor_simulation::CatalogueEntry;
use actor_simulation::Verify;

/// The blob-store invariant catalogue, B1–B7 (blob-store spec §9).
pub fn b_catalogue() -> &'static [CatalogueEntry] {
    B_CATALOGUE
}

const B_CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        spec: "blob §2, §4",
        property: "Address integrity: get returns bytes whose BLAKE3 hash equals the id, or an error, verified on the read path after any network transfer; a tampered copy falls through to a good owner",
        verify: &[Verify::TestFn(
            "a_corrupt_blob_is_detected_and_never_returned, \
             a_tampered_copy_falls_through_to_a_good_owner",
        )],
    },
    CatalogueEntry {
        invariant: 2,
        spec: "blob §2, §5",
        property: "Idempotent, dedup'd put: equal content under one namespace yields one stored copy, and a put of already-present content writes nothing new and re-acknowledges, regardless of which node writes it",
        verify: &[Verify::TestFn(
            "putting_the_same_bytes_twice_stores_once, \
             equal_content_converges_to_one_id_across_nodes",
        )],
    },
    CatalogueEntry {
        invariant: 3,
        spec: "blob §5.2",
        property: "Durability target: a put is acknowledged only once at least W copies are stored, so the blob survives losing any R - W owners; a put that cannot reach W is refused rather than acked at one copy",
        verify: &[Verify::TestFn(
            "a_blob_put_on_one_node_is_gettable_from_every_node, \
             a_put_below_write_quorum_is_refused",
        )],
    },
    CatalogueEntry {
        invariant: 4,
        spec: "blob §4",
        property: "No consensus on the data path: no election, term, agreement round, or write-time read-repair; concurrent writers of the same content do not coordinate and do not fork",
        // Structural, and checked as such: the crate depends on `actor-cluster`, not
        // `granary`, so there is no consensus group available for the data path to
        // run. The drift test asserts that from Cargo.toml — the day someone adds a
        // granary dependency, this invariant needs re-arguing rather than assuming.
        verify: &[Verify::CompileTime(
            "blob-store depends on actor-cluster, not granary: no consensus group exists to run \
             (asserted from Cargo.toml by conformance_catalogue.rs), plus the B2 convergence tests",
        )],
    },
    CatalogueEntry {
        invariant: 5,
        spec: "blob §5.2",
        property: "Deterministic placement: a blob's owners are a pure, version-stable function of the serving set and the (namespace, content hash) key, and one membership change reassigns only the blobs whose owners changed",
        verify: &[Verify::TestFn("one_membership_change_moves_minimal_blobs")],
    },
    CatalogueEntry {
        invariant: 6,
        spec: "blob §7",
        property: "Repair restores the target: after a node leaves, reconcile restores >= R copies of every live blob the cluster still holds; rebalancing is additive and never drops the last verifying copy of a non-deleted blob",
        verify: &[Verify::TestFn(
            "a_departed_owner_is_re_replicated, rebalancing_never_deletes, \
             acknowledged_blobs_are_re_replicated_after_a_heal",
        )],
    },
    CatalogueEntry {
        invariant: 7,
        spec: "blob §4, §5.3",
        property: "Monotonic deletion, no resurrection: delete_namespace is set-once and commutes; no node aware of the tombstone resolves a blob of the namespace, no put into a deleted namespace leaves a resolvable blob, and reconcile never resurrects one across a partition of unbounded duration",
        verify: &[Verify::TestFn(
            "a_deleted_namespace_stays_deleted, \
             a_deleted_namespace_resolves_nowhere_and_refuses_puts, \
             a_rejoining_unaware_holder_does_not_resurrect_a_deleted_namespace, \
             retention_survives_an_unbounded_partition",
        )],
    },
];
