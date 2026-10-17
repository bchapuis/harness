//! The machine-readable E-catalogue, E1–E21 (entity spec §12).
//!
//! The entity analogue of the core, utilities, agent, and sandbox catalogues:
//! one table linking each invariant to the code that verifies it, guarded by the
//! drift test in `conformance_catalogue.rs`. `invariant: n` reads as "En".
//!
//! It exists because the spec's "Verified by" column is prose, and prose rots
//! silently: a renamed or deleted suite leaves the column pointing at nothing, and
//! nothing fails. Recording the pointers here makes the column mechanically true:
//! a rename that misses this table fails the build.
//!
//! `SimTest` pointers name files under this crate's `tests/` directory;
//! `CompileFail` names a path relative to `crates/`, as the core catalogue does.
//! No entry claims a `Verify::Checker`; `conformance_catalogue.rs` says why.

#![allow(dead_code)]

use actor_simulation::CatalogueEntry;
use actor_simulation::Verify;

/// The entity invariant catalogue, E1–E21 (entity spec §12).
pub fn e_catalogue() -> &'static [CatalogueEntry] {
    E_CATALOGUE
}

const E_CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        spec: "entity §5, storage §5",
        property: "Single writer per entity: only the shard leader appends, term-fenced; a commit advances the head by exactly the batch appended, and the host folds only such a contiguous commit",
        verify: &[Verify::SimTest(
            "clustered_entities.rs, entities.rs, partition_safety.rs",
        )],
    },
    CatalogueEntry {
        invariant: 2,
        spec: "entity §3.1",
        property: "Deterministic fold: apply produces identical state on live commit and on replay from any snapshot/journal prefix",
        verify: &[Verify::SimTest(
            "clustered_entities.rs, entities.rs, entity_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 3,
        spec: "entity §1, storage §8",
        property: "EntityJournal is the source of truth: head and state derive only from journal/snapshot returns, never from a prior activation's memory",
        verify: &[Verify::SimTest(
            "entities.rs, entity_swarm.rs, disk_swarm.rs, sql_swarm.rs, subscription_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 4,
        spec: "storage §8",
        property: "Snapshot never shortens the log: a head is the best snapshot's seq plus the contiguous run above it, so a snapshot cannot outrun it; the host refuses one the head does not cover and replays from the journal",
        verify: &[Verify::SimTest("entities.rs")],
    },
    CatalogueEntry {
        invariant: 5,
        spec: "entity §5",
        property: "Reply iff durable: a reply is released, and its events folded, only after the entry commits; a NotLeader/Unavailable outcome yields an error, no fold, no success reply",
        verify: &[Verify::SimTest(
            "clustered_entities.rs, entities.rs, entity_swarm.rs, disk_swarm.rs, sql_swarm.rs, subscription_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 6,
        spec: "entity §4.3",
        property: "Exactly-once activation per node: the serial gateway activates a name at most once; concurrent requests find the same host (a count of Activated events in entities.rs, the ActivationSingletonPerNode checker in the swarm suites)",
        verify: &[Verify::SimTest(
            "entities.rs, entity_swarm.rs, disk_swarm.rs, sql_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 7,
        spec: "storage §3.1, §4, §6, §7",
        property: "Bounded consensus groups: O(shards) + O(entity types) Raft groups, kept bounded by split/merge, with no consensus on the data path",
        verify: &[Verify::SimTest("clustered_entities.rs, shard_split.rs")],
    },
    CatalogueEntry {
        invariant: 8,
        spec: "entity §4.2, storage §3.2, entity §7",
        property: "Activation without consensus: activating or hibernating touches no consensus group, paying at most one quorum head-recovery round-trip on the Quorum tier",
        verify: &[Verify::SimTest("clustered_entities.rs")],
    },
    CatalogueEntry {
        invariant: 9,
        spec: "storage §6",
        property: "Control plane off the data path: the shard map changes only on cluster events; no entity write or activation contacts it",
        verify: &[Verify::SimTest("clustered_entities.rs")],
    },
    CatalogueEntry {
        invariant: 10,
        spec: "entity §2.2",
        property: "Type-safe calls: a command an entity has no EntityHandler for does not compile",
        verify: &[Verify::CompileFail("entity/tests/compile_fail")],
    },
    CatalogueEntry {
        invariant: 11,
        spec: "storage §10",
        property: "CP under partition: a shard that cannot reach a quorum pauses its entities' writes (Unavailable) and never forks; other shards serve",
        verify: &[Verify::SimTest(
            "clustered_entities.rs, partition_safety.rs",
        )],
    },
    CatalogueEntry {
        invariant: 12,
        spec: "storage §8, entity §7",
        property: "Hibernation round-trip: an entity evicted when idle re-activates with identical state via snapshot + replay; no acknowledged write is lost",
        verify: &[Verify::SimTest(
            "clustered_entities.rs, entities.rs, entity_swarm.rs, facets.rs, disk_local.rs, sql.rs, ws_local.rs",
        )],
    },
    CatalogueEntry {
        invariant: 13,
        spec: "entity §4.4",
        property: "Location transparency: a call to a local versus remote entity produces observably identical replies and ordering",
        verify: &[Verify::SimTest(
            "clustered_entities.rs, entity_swarm.rs, blobs.rs, blob_swarm.rs, disk_swarm.rs, sql_swarm.rs, subscription_swarm.rs, ws_clustered.rs, ws_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 14,
        spec: "storage §5, §5.3",
        property: "Lossless failover: a new shard leader recovers each entity's committed head by fence-reading a read quorum, taking the highest-term record per slot, and writing the merged tail back to a quorum under its own term before serving; by quorum intersection no acknowledged write is lost across a leadership change",
        verify: &[Verify::SimTest(
            "clustered_entities.rs, entity_swarm.rs, partition_safety.rs, raft_journal.rs, ws_clustered.rs, ws_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 15,
        spec: "storage §7",
        property: "Split/merge safety: an entity is writable in exactly one shard at any time; a split or merge transfers the committed prefix atomically and loses or duplicates no write",
        verify: &[Verify::SimTest("shard_split.rs")],
    },
    CatalogueEntry {
        invariant: 16,
        spec: "entity §6.3",
        property: "Subscriptions are observational and lossless-by-seq: delivery never gates a commit, forks state, or advances a head, and a sink reconstructs the committed sequence by seq reconciliation",
        verify: &[Verify::SimTest(
            "subscription_faults.rs, subscription_swarm.rs, entity_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 17,
        spec: "storage §9",
        property: "Blob address integrity: get returns bytes whose BLAKE3 hash equals the id, or an error, verified after any network transfer; a corrupt copy falls through to a verifying replica",
        verify: &[Verify::SimTest(
            "blobs.rs, blob_swarm.rs, disk_local.rs, disk_swarm.rs, sql_swarm.rs",
        )],
    },
    CatalogueEntry {
        invariant: 18,
        spec: "storage §9",
        property: "Blob durability, idempotence, and entity-scoped reclamation: a put acks only on a write quorum including the leader; equal content stores once; gc/destroy is monotonic, idempotent, and never resurrects a referenced blob",
        verify: &[Verify::SimTest("blobs.rs, blob_swarm.rs")],
    },
    CatalogueEntry {
        invariant: 19,
        spec: "facets §3",
        property: "Facet atomicity and interpretation safety: a command's records across all facets commit as one atomic batch; replay dispatches by tag and aborts on an unrecognized one; the composite snapshot restores every facet to one seq",
        verify: &[Verify::SimTest(
            "facets.rs, disk_swarm.rs, sql_swarm.rs, ws_local.rs",
        )],
    },
    CatalogueEntry {
        invariant: 20,
        spec: "facets §3, §4.3",
        property: "Physical facets expose only durable state: the materialization is a rebuildable cache, unobservable before the commit and discarded on any non-committed outcome; rehydration reproduces it exactly",
        verify: &[Verify::SimTest("sql.rs, ws_local.rs")],
    },
    CatalogueEntry {
        invariant: 21,
        spec: "facets §4.5",
        property: "An alarm fires at most once per arm: the fired deadline's Clear joins the same atomic batch as on_alarm's records and the epoch guard voids a superseded timer, so one armed deadline commits at most one callerless effect; the bound is on effects that commit, not on handler invocations",
        verify: &[Verify::SimTest(
            "alarm.rs, alarm_cluster.rs, alarm_swarm.rs",
        )],
    },
];
