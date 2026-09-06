//! Entities: durable objects ("entities") on the distributed actor framework.
//!
//! A **entity** is an actor plus three things (entity spec): a name-based virtual
//! identity, a durable event-sourced journal, and a durability barrier on the
//! reply. Everything else — mailboxes, serial execution, location-transparent
//! `ask`/`tell`, supervision, death watch — is inherited unchanged from
//! `actor-core`.
//!
//! Author an entity by implementing [`Entity`] (state, events, the pure
//! [`apply`](Entity::apply) fold) and a [`EntityHandler`] per command (the
//! decide/apply split, §4.2), then host it with
//! [`system.entities::<G>(config)`](EntityExt::entity) and address it by name
//! with [`Entities::entity`]. A command's reply is held until its events are
//! durable (the §6 output gate), so a crash loses no acknowledged write.
//!
//! # Facets: one entity, many storage features
//!
//! Beyond the event fold, an entity composes **facets** (spec §7.12) by declaring
//! `type Facets`: the [`Kv`] map (§7.13), the [`Ws`] workspace directory
//! (§7.11), the SQL database (§7.14, behind the `sql` cargo feature), the
//! [`Alarm`] durable timer (§7.16), and the [`Steps`] step memo (§7.17). A
//! handler writes through the compile-time-gated `ctx` accessors —
//! [`kv()`](EntityCtx::kv), [`ws()`](EntityCtx::ws), `sql()`,
//! [`alarm()`](EntityCtx::alarm), [`steps()`](EntityCtx::steps) — and all of a
//! command's records, events and facet operations alike, commit as **one
//! atomic tagged batch** in the entity's single journal, snapshot as one
//! composite, and share one unioned blob root set.
//!
//! # Durable alarms and workflows
//!
//! The [`Alarm`] facet stores a single per-entity deadline; when it passes the
//! runtime fires [`Entity::on_alarm`] with **no caller present** (spec §7.16). The
//! [`Steps`] step memo builds on it: each step's effect runs at most once
//! across crashes because its result is journaled and memoized. An alarm fires
//! while its entity is resident and re-arms on re-activation. To fire
//! with **no access after a node failover or hibernation**, host a type with
//! [`entities_with_alarms`](EntityExt::entities_with_alarms): each host registers its
//! deadline in a per-shard [`AlarmIndex`] (an acknowledged registration, which is
//! what lets an alarmed entity hibernate — an unacked one pins it resident), and a
//! driver re-activates due entities on the shards this node leads.
//!
//! # Scope: two durability tiers, one model
//!
//! The full entity programming model runs on either durability tier, selected by
//! the system an entity is hosted on (§7.4):
//!
//! - **`Local`, single-node** ([`LocalEntityJournal`]): one linearizable local store,
//!   CP trivially. Hosted on a [`LocalSystem`](actor_core::LocalSystem).
//! - **`Quorum`, clustered** (the shard replicator, replicator.rs): the namespace is partitioned
//!   into shards (`EntityConfig::shards`), each a leader-election Raft group holding
//!   no entity data (§7.1) over which a per-entity **quorum append** (§7.2) makes a
//!   entity's records durable on the shard's replicas, fenced by the shard term. A
//!   entity activates on its shard's leader (§5.2), a [`EntityRef`] call from any node
//!   routes name→shard→leader (§5.4), the §8 single-writer fence and the
//!   `NotLeader`/`Unavailable` durability outcomes (§11) apply, and a new leader
//!   recovers each entity's head from a write quorum by read-repair on activation, so
//!   committed state survives failover (E14). Hosted on an `ActorSystem` that
//!   implements [`EntitySystem`] over consensus (the cluster's `ClusterSystem`). The
//!   over-the-wire command dispatch (§5.5) reuses the actor framework's own dispatch
//!   registry, fed by [`Entity::register`] (see [`accepted_manifests`]).
//!
//! The shard map (§7.6) is a per-type Raft group whose committed log is the
//! allocation ([`ShardMapSource`]), storing each shard's **key range** and replica
//! set. As cluster membership changes, a shard rebalances by **joint-quorum
//! migration** (§7.7): the new set commits as a `target` (writes and recoveries
//! then need a majority of BOTH sets), the shard leader's driver catches every
//! entity's records, snapshot, and blobs up on the target, and only then does the
//! map flip — so a committed write's durability never rests on replicas that left
//! (E14).
//!
//! The partition itself is **elastic** (§7.7): a shard **splits** — its key range
//! divides in two, the parent keeping the low half and a fresh child taking the
//! high half on the same replicas — when it grows past `shard_target_bytes` or on
//! an explicit request, and two adjacent shards **merge** the reverse way,
//! reclaiming a leader-election group (E7). A split (or merge) seals the moving
//! range on a quorum of stores — from which no append to it can reach a write
//! quorum at any term — transfers each moved entity's committed prefix, snapshot,
//! and blobs to the destination keys, and only then commits the new mapping, so a
//! entity is writable in exactly one shard at any time and no write is lost or
//! duplicated across the boundary (E15).
//!
//! The following remain **deferred**:
//!
//! - Linearizable reads (§7.5): reads are **read-your-leader** (relaxed) — served
//!   locally from the leader's activation, so a deposed-but-unfenced minority
//!   leader can serve a stale read (writes never fork, §8).
//! - Hibernatable connections, follower reads, and cross-entity sagas (§16).

#[cfg(feature = "testing")]
pub mod testing;

mod alarm;
mod alarm_index;
mod blobs;
mod blocking;
mod cdc;
mod config;
#[cfg(test)]
mod corpus;
mod disk;
mod election;
mod entity;
mod entityref;
mod error;
mod event;
mod facet;
mod facet_blobs;
mod file_store;
mod gateway;
mod host;
mod journal;
mod kv;
mod memory;
mod metrics;
mod node;
mod replica_store;
mod replicator;
mod shardmap;
#[cfg(feature = "sql")]
mod sql;
mod steps;
mod store;
mod subscription;
mod system;
mod ws;

pub use alarm::Alarm;
pub use alarm::AlarmHandle;
pub use alarm_index::AlarmIndex;
// The alarm-index runtime vocabulary, published for the crate's own integration
// tests.
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
pub use blobs::EntityBlobs;
pub use blocking::BlockingIo;
pub use blocking::InlineIo;
pub use blocking::ThreadPoolIo;
pub use config::EntityConfig;
pub use disk::Disk;
pub use disk::DiskCaptureStats;
pub use disk::DiskError;
pub use disk::DiskHandle;
pub use disk::MAX_IMAGE_BYTES;
pub use entity::Entity;
pub use entity::EntityCtx;
pub use entity::EntityHandler;
pub use entity::EntityName;
pub use entity::EntityRegistry;
pub use entity::EventHistory;
pub use entity::EventsFuture;
pub use entity::NoEvent;
pub use entity::accepted_manifests;
pub use entityref::Entities;
pub use entityref::EntityExt;
pub use entityref::EntityRef;
pub use error::EntityError;
pub use event::EntityEvent;
pub use facet::Facet;
pub use facet::FacetError;
pub use facet::FacetSet;
pub use facet::HasFacet;
pub use facet::Here;
pub use facet::There;
pub use file_store::FileEntityStore;
pub use journal::AppendOutcome;
pub use journal::DynEntityJournal;
pub use journal::EntityJournal;
pub use journal::EntityJournalError;
pub use journal::RecordPage;
pub use journal::Seq;
pub use journal::Term;
pub use kv::INLINE_MAX;
pub use kv::Kv;
pub use kv::KvHandle;
pub use memory::LocalEntityJournal;
pub use metrics::AtomicEntityMetrics;
pub use metrics::CommitOutcome;
pub use metrics::EntityMetrics;
pub use node::EntityNode;
pub use replica_store::ReplicaTransport;
pub use replicator::Deadlines;
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
pub use steps::LaunchGuard;
pub use steps::StepDone;
pub use steps::StepId;
pub use steps::Steps;
pub use steps::StepsHandle;
pub use steps::complete_step;
pub use store::BlobAck;
pub use store::EntityBlobStore;
pub use store::EntityStore;
pub use store::EntityStoreFactory;
pub use store::MemoryEntityStore;
pub use store::ReadOutcome;
pub use store::ReadReply;
pub use store::StoreAck;
pub use store::WriteKind;
pub use subscription::RecordBatch;
pub use subscription::RecordSink;
pub use subscription::RecordStream;
pub use subscription::Subscribe;
pub use subscription::Subscribed;
pub use subscription::Subscription;
pub use system::EntitySystem;
pub use system::FailureDomains;
pub use system::ShardId;
pub use system::shard_for;
pub use ws::MAX_TREE_BYTES;
pub use ws::Ws;
pub use ws::WsCapture;
pub use ws::WsError;
pub use ws::WsHandle;
