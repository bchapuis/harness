//! The machine-readable G-catalogue, G1–G21 (granary spec §15).
//!
//! The granary analogue of the core, utilities, harness, and sandbox catalogues:
//! one table linking each invariant to the code that verifies it, guarded by the
//! drift test in `conformance_catalogue.rs`. `invariant: n` reads as "Gn".
//!
//! It exists because the spec's "Verified by" column is prose, and prose rots
//! silently: a renamed or deleted suite leaves the column pointing at nothing, and
//! nothing fails. That is exactly how the blob-store spec came to cite seven test
//! names that no longer existed. Recording the pointers here instead makes the
//! column mechanically true — a rename that misses this table fails the build.
//!
//! `SimTest` pointers name files under this crate's `tests/` directory;
//! `CompileFail` names a path relative to `crates/`, as the core catalogue does.
//! No entry claims a `Verify::Checker`: granary ships continuous checkers in
//! `granary::testing`, but they are constructed per-suite with the label that suite
//! reports under (`machine-commit-monotonic`, `disk-grain-commit-monotonic`, …),
//! so there is no fixed global name for this table to cross-check against, the way
//! `default_invariants()` gives the core one.

#![allow(dead_code)]

use actor_simulation::CatalogueEntry;
use actor_simulation::Verify;

/// The granary invariant catalogue, G1–G21 (granary spec §15).
pub fn g_catalogue() -> &'static [CatalogueEntry] {
    G_CATALOGUE
}

const G_CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        spec: "granary §6, §8",
        property: "Single writer per grain: only the shard leader appends, term-fenced; a commit advances the head by exactly the batch appended, and the host folds only such a contiguous commit",
        verify: &[Verify::SimTest(
            "clustered_grains.rs, grains.rs, partition_safety.rs",
        )],
    },
    CatalogueEntry {
        invariant: 2,
        spec: "granary §4.1",
        property: "Deterministic fold: apply produces identical state on live commit and on replay from any snapshot/journal prefix",
        verify: &[Verify::SimTest(
            "clustered_grains.rs, grains.rs, grain_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 3,
        spec: "granary §1, §9",
        property: "GrainJournal is the source of truth: head and state derive only from journal/snapshot returns, never from a prior activation's memory",
        verify: &[Verify::SimTest(
            "grains.rs, grain_swarm.rs, disk_swarm.rs, sql_swarm.rs, subscription_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 4,
        spec: "granary §9",
        property: "Snapshot never shortens the log: a head is the best snapshot's seq plus the contiguous run above it, so a snapshot cannot outrun it; the host refuses one the head does not cover and replays from the journal",
        verify: &[Verify::SimTest("grains.rs")],
    },
    CatalogueEntry {
        invariant: 5,
        spec: "granary §6",
        property: "Reply iff durable: a reply is released, and its events folded, only after the entry commits; a NotLeader/Unavailable outcome yields an error, no fold, no success reply",
        verify: &[Verify::SimTest(
            "clustered_grains.rs, grains.rs, grain_swarm.rs, disk_swarm.rs, sql_swarm.rs, subscription_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 6,
        spec: "granary §5.3",
        property: "Exactly-once activation per node: the serial gateway activates a name at most once; concurrent requests find the same host",
        verify: &[Verify::SimTest(
            "grains.rs, grain_swarm.rs, disk_swarm.rs, sql_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 7,
        spec: "granary §7.1, §7.2, §7.6, §7.7",
        property: "Bounded consensus groups: O(shards) + O(grain types) Raft groups, kept bounded by split/merge, with no consensus on the data path",
        verify: &[Verify::SimTest("clustered_grains.rs, shard_split.rs")],
    },
    CatalogueEntry {
        invariant: 8,
        spec: "granary §5.2, §7.8, §10",
        property: "Activation without consensus: activating or hibernating touches no consensus group, paying at most one quorum head-recovery round-trip on the Quorum tier",
        verify: &[Verify::SimTest("clustered_grains.rs")],
    },
    CatalogueEntry {
        invariant: 9,
        spec: "granary §7.6",
        property: "Control plane off the data path: the shard map changes only on cluster events; no grain write or activation contacts it",
        verify: &[Verify::SimTest("clustered_grains.rs")],
    },
    CatalogueEntry {
        invariant: 10,
        spec: "granary §4.3",
        property: "Type-safe calls: a command a grain has no GrainHandler for does not compile",
        verify: &[Verify::CompileFail("granary/tests/compile_fail")],
    },
    CatalogueEntry {
        invariant: 11,
        spec: "granary §11",
        property: "CP under partition: a shard that cannot reach a quorum pauses its grains' writes (Unavailable) and never forks; other shards serve",
        verify: &[Verify::SimTest(
            "clustered_grains.rs, grains.rs, partition_safety.rs",
        )],
    },
    CatalogueEntry {
        invariant: 12,
        spec: "granary §9, §10",
        property: "Hibernation round-trip: a grain evicted when idle re-activates with identical state via snapshot + replay; no acknowledged write is lost",
        verify: &[Verify::SimTest(
            "clustered_grains.rs, grains.rs, grain_swarm.rs, facets.rs, disk_local.rs, sql.rs, ws_local.rs",
        )],
    },
    CatalogueEntry {
        invariant: 13,
        spec: "granary §5.4",
        property: "Location transparency: a call to a local versus remote grain produces observably identical replies and ordering",
        verify: &[Verify::SimTest(
            "clustered_grains.rs, grains.rs, grain_swarm.rs, blobs.rs, blob_swarm.rs, disk_swarm.rs, sql_swarm.rs, subscription_swarm.rs, ws_clustered.rs, ws_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 14,
        spec: "granary §8, §8.3",
        property: "Lossless failover: a new shard leader recovers each grain's committed head from a write quorum; by quorum intersection no acknowledged write is lost across a leadership change",
        verify: &[Verify::SimTest(
            "clustered_grains.rs, grain_swarm.rs, partition_safety.rs, raft_journal.rs, ws_clustered.rs, ws_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 15,
        spec: "granary §7.7",
        property: "Split/merge safety: a grain is writable in exactly one shard at any time; a split or merge transfers the committed prefix atomically and loses or duplicates no write",
        verify: &[Verify::SimTest("grains.rs, shard_split.rs")],
    },
    CatalogueEntry {
        invariant: 16,
        spec: "granary §7.9",
        property: "Subscriptions are observational and lossless-by-seq: delivery never gates a commit, forks state, or advances a head, and a sink reconstructs the committed sequence by seq reconciliation",
        verify: &[Verify::SimTest(
            "subscription_faults.rs, subscription_swarm.rs, grain_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 17,
        spec: "granary §7.10",
        property: "Blob address integrity: get returns bytes whose BLAKE3 hash equals the id, or an error, verified after any network transfer; a corrupt copy falls through to a verifying replica",
        verify: &[Verify::SimTest(
            "blobs.rs, blob_swarm.rs, disk_local.rs, disk_swarm.rs, sql_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 18,
        spec: "granary §7.10",
        property: "Blob durability, idempotence, and grain-scoped reclamation: a put acks only on a write quorum including the leader; equal content stores once; gc/destroy is monotonic, idempotent, and never resurrects a referenced blob",
        verify: &[Verify::SimTest("blobs.rs, blob_swarm.rs")],
    },
    CatalogueEntry {
        invariant: 19,
        spec: "granary §7.12",
        property: "Facet atomicity and interpretation safety: a command's records across all facets commit as one atomic batch; replay dispatches by tag and aborts on an unrecognized one; the composite snapshot restores every facet to one seq",
        verify: &[Verify::SimTest(
            "facets.rs, disk_swarm.rs, sql_swarm.rs, ws_local.rs",
        )],
    },
    CatalogueEntry {
        invariant: 20,
        spec: "granary §7.12, §7.14",
        property: "Physical facets expose only durable state: the materialization is a rebuildable cache, unobservable before the commit and discarded on any non-committed outcome; rehydration reproduces it exactly",
        verify: &[Verify::SimTest("sql.rs, ws_local.rs")],
    },
    CatalogueEntry {
        invariant: 21,
        spec: "granary §7.16",
        property: "An alarm fires at most once per arm: the fired deadline's Clear joins the same atomic batch as on_alarm's records and the epoch guard voids a superseded timer, so one armed deadline commits at most one callerless effect — the bound is on effects that commit, not on handler invocations",
        verify: &[Verify::SimTest(
            "alarm.rs, alarm_cluster.rs, alarm_swarm.rs",
        )],
    },
];
