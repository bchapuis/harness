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
//! # Scope: two durability tiers, one model
//!
//! The full grain programming model runs on either durability tier, selected by
//! the system a grain is hosted on (§7.4):
//!
//! - **Tier 1, single-node** ([`MemoryJournal`]): one linearizable local store, CP
//!   trivially, the sweet spot for embedded use, tests, and the deterministic
//!   simulator. Hosted on a [`LocalSystem`](actor_core::LocalSystem).
//! - **Tier 2, sharded Raft** ([`RaftJournal`]): the namespace is partitioned into
//!   shards (`GranaryConfig::shards`), each a Raft group on a clustered system. A
//!   grain activates on its shard's leader (§5.2), a [`GrainRef`] call from any
//!   node routes name→shard→leader (§5.4), the §8 single-writer fence and the
//!   `NotLeader`/`Unavailable` durability outcomes (§11) are real, and committed
//!   state survives leader failover (G14). Hosted on an
//!   `ActorSystem` that implements [`GranarySystem`] over consensus (the cluster's
//!   `ClusterSystem`). The over-the-wire command dispatch (§5.5) reuses the actor
//!   framework's own dispatch registry, fed by [`Grain::register`] (see
//!   [`accepted_manifests`]); granary adds no transport.
//!
//! The control-plane-stored shard map (§7.6) is **built**: a per-type Raft group
//! whose committed log is the allocation ([`ShardMapSource`]), so every node agrees
//! on each shard's replica set regardless of join order, and a shard's voter set is
//! reconfigured as cluster membership changes (the allocator and reconcile loops).
//!
//! The following remain **deferred** and are documented where their surface
//! appears:
//!
//! - Dynamic shard split & merge (§7.7): the shard count is fixed at `granary()`
//!   time.
//! - Linearizable reads (§7.5): reads are **read-your-leader** (relaxed) — served
//!   locally from the leader's activation, so a deposed-but-unfenced minority
//!   leader can serve a stale read (writes never fork, §8). The DO-faithful
//!   upgrade is a check-quorum **leader lease** that self-fences the activation
//!   (§16), not a per-read Raft read-index (which would defeat read scaling, §7.8).
//! - Durable alarms, hibernatable connections, follower reads, and cross-grain
//!   sagas (§16).

mod config;
mod error;
mod event;
mod gateway;
mod grain;
mod grainref;
mod host;
mod journal;
mod memory;
mod shard;
mod shardmap;
mod system;

pub use config::GranaryConfig;
pub use error::GrainError;
pub use event::GrainEvent;
pub use grain::Grain;
pub use grain::GrainCtx;
pub use grain::GrainHandler;
pub use grain::GrainName;
pub use grain::GrainRegistry;
pub use grain::accepted_manifests;
pub use grainref::Granary;
pub use grainref::GranaryExt;
pub use grainref::GrainRef;
pub use journal::AppendOutcome;
pub use journal::DynJournal;
pub use journal::Journal;
pub use journal::JournalError;
pub use journal::Seq;
pub use memory::MemoryJournal;
pub use shard::RaftJournal;
pub use shardmap::ShardMapSource;
pub use system::GranarySystem;
pub use system::ShardId;
pub use system::shard_for;
