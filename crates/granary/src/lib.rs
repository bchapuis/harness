//! Granary: durable objects ("grains") on the distributed actor framework.
//!
//! A **grain** is an actor plus three things (granary spec): a name-based virtual
//! identity, a durable event-sourced journal, and a durability barrier on the
//! reply. Everything else — mailboxes, serial execution, location-transparent
//! `ask`/`tell`, supervision, death watch — is inherited unchanged from
//! `actor-core`.
//!
//! Author a grain by implementing [`Grain`] (state, events, the pure
//! [`apply`](Grain::apply) fold) and a [`GrainHandler`] per command (the
//! decide/apply split, §4.2), then host it with
//! [`system.granary::<G>(config)`](GranaryExt::granary) and address it by name
//! with [`Granary::grain`]. A command's reply is held until its events are
//! durable (the §6 output gate); a crash loses no acknowledged write.
//!
//! # Facets: one grain, many storage features
//!
//! Beyond the event fold, a grain composes **facets** (spec §7.12) by declaring
//! `type Facets`: the [`Kv`] map (§7.13), the [`Ws`] workspace directory
//! (§7.11), the SQL database (§7.14, behind the `sql` cargo feature), the
//! [`Alarm`] durable timer (§16), and the [`Workflow`] step memo (§16). A
//! handler writes through the compile-time-gated `ctx` accessors —
//! [`kv()`](GrainCtx::kv), [`ws()`](GrainCtx::ws), `sql()`,
//! [`alarm()`](GrainCtx::alarm), [`workflow()`](GrainCtx::workflow) — and all of a
//! command's records, events and facet operations alike, commit as **one
//! atomic tagged batch** in the grain's single journal, snapshot as one
//! composite, and share one unioned blob root set. One grain, one consistency
//! boundary, however many storage features it declares.
//!
//! # Durable alarms and workflows
//!
//! The [`Alarm`] facet stores a single per-grain deadline; when it passes the
//! runtime fires [`Grain::on_alarm`] with **no caller present** (spec §16), the
//! basis for retries, timeouts, and the [`Workflow`] step memo — a
//! Cloudflare-Workflows-style `step`/`sleep`/`retry` where each step's effect runs
//! at most once across crashes because its result is journaled and memoized. An
//! alarm fires while its grain is resident and re-arms on re-activation. To fire
//! with **no access after a node failover or hibernation**, host a type with
//! [`granary_with_alarms`](GranaryExt::granary_with_alarms): each host registers its
//! deadline in a per-shard [`AlarmIndex`] (an acknowledged registration, which is
//! what lets an alarmed grain hibernate — an unacked one pins it resident), and a
//! driver re-activates due grains on the shards this node leads.
//!
//! # Scope: two durability tiers, one model
//!
//! The full grain programming model runs on either durability tier, selected by
//! the system a grain is hosted on (§7.4):
//!
//! - **`Local`, single-node** ([`LocalGrainJournal`]): one linearizable local store,
//!   CP trivially, the sweet spot for embedded use, tests, and the deterministic
//!   simulator. Hosted on a [`LocalSystem`](actor_core::LocalSystem).
//! - **`Quorum`, clustered** ([`QuorumGrainJournal`]): the namespace is partitioned
//!   into shards (`GranaryConfig::shards`), each a leader-election Raft group holding
//!   no grain data (§7.1) over which a per-grain **quorum append** (§7.2) makes a
//!   grain's records durable on the shard's replicas, fenced by the shard term. A
//!   grain activates on its shard's leader (§5.2), a [`GrainRef`] call from any node
//!   routes name→shard→leader (§5.4), the §8 single-writer fence and the
//!   `NotLeader`/`Unavailable` durability outcomes (§11) are real, and a new leader
//!   recovers each grain's head from a write quorum by read-repair on activation, so
//!   committed state survives failover (G14). Hosted on an `ActorSystem` that
//!   implements [`GranarySystem`] over consensus (the cluster's `ClusterSystem`). The
//!   over-the-wire command dispatch (§5.5) reuses the actor framework's own dispatch
//!   registry, fed by [`Grain::register`] (see [`accepted_manifests`]); granary adds
//!   no transport.
//!
//! The control-plane-stored shard map (§7.6) is **built**: a per-type Raft group
//! whose committed log is the allocation ([`ShardMapSource`]), storing each
//! shard's **key range** and replica set, so every node agrees on the partition
//! regardless of join order. As cluster membership changes, a shard rebalances by
//! **joint-quorum migration** (§7.7): the new set commits as a `target` (writes
//! and recoveries then need a majority of BOTH sets), the shard leader's driver
//! catches every grain's records, snapshot, and blobs up on the target, and only
//! then does the map flip — so a committed write's durability never rests on
//! replicas that left (G14).
//!
//! The partition itself is **elastic** (§7.7): a shard **splits** — its key range
//! divides in two, the parent keeping the low half and a fresh child taking the
//! high half on the same replicas — when it grows past `shard_target_bytes` or on
//! an explicit request, and two adjacent shards **merge** the reverse way,
//! reclaiming a leader-election group (G7). A split (or merge) seals the moving
//! range on a quorum of stores — from which no append to it can reach a write
//! quorum at any term — transfers each moved grain's committed prefix, snapshot,
//! and blobs to the destination keys, and only then commits the new mapping, so a
//! grain is writable in exactly one shard at any time and no write is lost or
//! duplicated across the boundary (G15).
//!
//! The following remain **deferred** and are documented where their surface
//! appears:
//!
//! - Linearizable reads (§7.5): reads are **read-your-leader** (relaxed) — served
//!   locally from the leader's activation, so a deposed-but-unfenced minority
//!   leader can serve a stale read (writes never fork, §8). The DO-faithful
//!   upgrade is a check-quorum **leader lease** that self-fences the activation
//!   (§16), not a per-read Raft read-index (which would defeat read scaling, §7.8).
//! - Hibernatable connections, follower reads, and cross-grain sagas (§16).

mod alarm;
mod alarm_index;
mod blobs;
mod config;
mod disk;
mod election;
mod error;
mod event;
mod facet;
mod facet_blobs;
mod file_store;
mod gateway;
mod grain;
mod grainref;
mod host;
mod journal;
mod kv;
mod memory;
mod replica_store;
mod replicator;
mod shard;
mod shardmap;
#[cfg(feature = "sql")]
mod sql;
mod store;
mod subscription;
mod system;
mod workflow;
mod ws;

pub use alarm::Alarm;
pub use alarm::AlarmHandle;
pub use alarm_index::AlarmIndex;
// The alarm-index runtime vocabulary: internal machinery published for the
// crate's own integration tests, not part of the supported API.
#[doc(hidden)]
pub use alarm_index::ALARM_INDEX_TYPE;
#[doc(hidden)]
pub use alarm_index::AllPending;
#[doc(hidden)]
pub use alarm_index::DueBefore;
#[doc(hidden)]
pub use alarm_index::Pending;
#[doc(hidden)]
pub use alarm_index::Sync as AlarmSync;
#[doc(hidden)]
pub use alarm_index::index_key;
pub use blobs::BlobId;
pub use blobs::GrainBlobs;
pub use config::GranaryConfig;
pub use disk::Disk;
pub use disk::DiskCaptureStats;
pub use disk::DiskError;
pub use disk::DiskHandle;
pub use disk::MAX_IMAGE_BYTES;
pub use error::GrainError;
pub use event::GrainEvent;
pub use facet::Facet;
pub use facet::FacetError;
pub use facet::FacetSet;
pub use facet::HasFacet;
pub use facet::Here;
pub use facet::There;
pub use file_store::FileGrainStore;
pub use grain::Grain;
pub use grain::GrainCtx;
pub use grain::GrainHandler;
pub use grain::GrainName;
pub use grain::GrainRegistry;
pub use grain::NoEvent;
pub use grain::accepted_manifests;
pub use grainref::GrainRef;
pub use grainref::Granary;
pub use grainref::GranaryExt;
pub use journal::AppendOutcome;
pub use journal::DynGrainJournal;
pub use journal::GrainJournal;
pub use journal::GrainJournalError;
pub use journal::Seq;
pub use journal::Term;
pub use kv::INLINE_MAX;
pub use kv::Kv;
pub use kv::KvHandle;
pub use memory::LocalGrainJournal;
pub use replica_store::ReplicaTransport;
pub use shard::QuorumGrainJournal;
pub use shardmap::ShardMapSource;
#[cfg(feature = "sql")]
pub use sql::MAX_QUERY_ROWS;
#[cfg(feature = "sql")]
pub use sql::QueryResult;
#[cfg(feature = "sql")]
pub use sql::Sql;
#[cfg(feature = "sql")]
pub use sql::SqlError;
#[cfg(feature = "sql")]
pub use sql::SqlHandle;
#[cfg(feature = "sql")]
pub use sql::SqlRow;
#[cfg(feature = "sql")]
pub use sql::SqlValue;
pub use store::GrainStore;
pub use store::GrainStoreFactory;
pub use store::MemoryGrainStore;
pub use store::ReadOutcome;
pub use store::ReadReply;
pub use store::RecordSlot;
pub use store::StoreAck;
pub use store::WriteKind;
pub use subscription::RecordBatch;
pub use subscription::RecordSink;
pub use subscription::RecordStream;
pub use subscription::Subscribe;
pub use subscription::Subscribed;
pub use subscription::Subscription;
pub use system::GranarySystem;
pub use system::ShardId;
pub use system::shard_for;
pub use workflow::LaunchGuard;
pub use workflow::StepDone;
pub use workflow::StepId;
pub use workflow::Workflow;
pub use workflow::WorkflowHandle;
pub use workflow::complete_step;
pub use ws::MAX_TREE_BYTES;
pub use ws::Ws;
pub use ws::WsCapture;
pub use ws::WsError;
pub use ws::WsHandle;
