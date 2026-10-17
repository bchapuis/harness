//! Entities: virtual, durable, single-activation objects on the distributed actor
//! framework.
//!
//! An entity is an actor plus a name-based virtual identity, a durable
//! event-sourced journal, and a durability barrier on the reply: the runtime
//! activates it on its shard leader on first use, holds each reply until the
//! command's records are durable, and rebuilds it from the journal after a
//! hibernation or a failover. Workflows, agents, machines, and the tenancy
//! directory are built on it.
//!
//! # Seams
//!
//! - **Authoring** (a plug point every entity type implements): [`Entity`],
//!   [`EntityHandler`], [`EntityRegistry`], with [`EntityCtx`] as what a decision
//!   receives. Callers hold an [`EntityRef`] from [`Entities`]; types are hosted
//!   through [`EntityExt`] or the node's [`EntityNode`](cluster::EntityNode).
//! - **Storage plug points**, in [`store`]: the per-replica
//!   [`EntityStore`](store::EntityStore) (supplied on
//!   [`EntityConfig::entity_store`]), the node's
//!   [`BlockingIo`](store::BlockingIo) pool, and the
//!   [`EntityMetrics`](store::EntityMetrics) sink.
//! - **Internal boundaries**: [`EntitySystem`] (implemented here for actor-core's
//!   `LocalSystem` and actor-cluster's `ClusterSystem`, which choose the durability
//!   tier), the [`EntityJournal`](store::EntityJournal) the host appends through, the
//!   [`ShardMapSource`](cluster::ShardMapSource) placement view, the
//!   [`ReplicaTransport`](cluster::ReplicaTransport) replica protocol, and the
//!   sealed [`Facet`](facet::Facet) seam behind the built-in facets.
//! - **Consumed**: actor-cluster's `RaftConsensus` for leader election and the shard
//!   maps, the `wal` crate under [`FileEntityStore`](store::FileEntityStore), and the
//!   `compat` stamps on records, snapshots, and stores.
//!
//! # How it works
//!
//! The `host` module is an entity's activation: the per-command protocol (decide,
//! append, fold only after a contiguous commit, then reply), rehydration, idle
//! hibernation, and the alarm timer. The `gateway` module routes a call to its
//! shard leader, owns the activation table, and serves the non-activating event
//! read. On the clustered tier the `replicator` module is the per-shard journal: a
//! quorum append fenced by the shard term, and the read-repair recovery a new
//! leader runs before serving an entity. The `shardmap` module is the per-type
//! consensus group that allocates shards, drives joint-quorum migrations, and
//! commits splits and merges. The [`facet`] module carries the record envelope, the
//! composite snapshot, and the unioned blob roots the built-in facets share.
//!
//! Specified by `docs/entity-spec.md` (the model and runtime),
//! `docs/entity-storage-spec.md` (the substrate), and `docs/entity-facets-spec.md`
//! (the facets).
//!
//! # Example
//!
//! ```ignore
//! pub struct Account;
//! #[derive(Default, Serialize, Deserialize)]
//! pub struct Balance { cents: i64 }
//! #[derive(Serialize, Deserialize)]
//! pub enum Ledger { Deposited(u64), Withdrew(u64) }
//! pub struct Overdraft; // an application error, carried inside the reply
//!
//! impl Entity for Account {
//!     type System = ClusterSystem;
//!     type State = Balance;
//!     type Event = Ledger;
//!     type Facets = ();
//!     const ENTITY_TYPE: &'static str = "bank.Account"; // rename-stable
//!     fn apply(state: &mut Balance, e: &Ledger) {
//!         match e {
//!             Ledger::Deposited(n) => state.cents += *n as i64,
//!             Ledger::Withdrew(n) => state.cents -= *n as i64,
//!         }
//!     }
//!     fn register(r: &mut EntityRegistry<Self>) {
//!         r.accept::<Withdraw>();
//!     }
//! }
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! pub struct Withdraw { cents: u64 }
//! impl Message for Withdraw {
//!     type Reply = Result<i64, Overdraft>;
//!     const MANIFEST: Manifest = Manifest::new("bank.Withdraw");
//! }
//!
//! impl EntityHandler<Withdraw> for Account {
//!     async fn handle(&self, state: &Balance, msg: Withdraw, _: &EntityCtx<Self>)
//!         -> (Vec<Ledger>, Result<i64, Overdraft>) {
//!         if (state.cents as u64) < msg.cents {
//!             return (vec![], Err(Overdraft)); // no event: nothing commits
//!         }
//!         (vec![Ledger::Withdrew(msg.cents)], Ok(state.cents - msg.cents as i64))
//!     }
//! }
//!
//! // One node of a cluster in the leader-based mode, over real storage.
//! let system = ClusterSystem::start("node-a", config_leader_based).await?;
//! let (node, store) = EntityNode::durable(system.clone(), data_dir);
//! let accounts: Entities<Account> = node.entities(EntityConfig {
//!     entity_store: Some(store),
//!     ..EntityConfig::default()
//! });
//!
//! // Activated on first use; the reply waits until the event is durable.
//! match accounts.entity("account/42").ask(Withdraw { cents: 500 }).await {
//!     Ok(Ok(balance)) => println!("balance now {balance}"),
//!     Ok(Err(Overdraft)) => { /* the application's own outcome */ }
//!     Err(EntityError::Unavailable(_)) => { /* no quorum: the write MAY still land */ }
//!     Err(other) => eprintln!("{other}"),
//! }
//!
//! // At a graceful stop, hand every led shard to another replica.
//! node.hand_off_leadership().await;
//! ```

#[cfg(feature = "testing")]
pub mod testing;

// The crate's three deep modules, beside a root that keeps the authoring
// surface: a durable-object programming model (root), a family of composable
// storage features ([`facet`]), the durable substrate a deployment wires
// ([`store`]), and the sharding and replication that make it clustered
// ([`cluster`]). `Entity`, `EntityCtx`, `EntityHandler` and their neighbours
// stay at the root because they are the interface, not a fourth namespace.
pub mod cluster;
pub mod facet;
pub mod store;

mod alarm;
mod alarm_index;
mod blobs;
mod blocking;
mod cdc;
mod config;
mod disk;
mod election;
mod entity;
mod entity_ref;
mod error;
mod event;
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
mod subscription;
mod system;
mod ws;

// The authoring surface: what writing an entity is written in.
pub use config::EntityConfig;
pub use entity::Entity;
pub use entity::EntityCtx;
pub use entity::EntityHandler;
pub use entity::EntityName;
pub use entity::EntityRegistry;
pub use entity::EventHistory;
pub use entity::NoEvent;
pub use entity::accepted_manifests;
pub use entity_ref::Entities;
pub use entity_ref::EntityExt;
pub use entity_ref::EntityRef;
pub use error::EntityError;
pub use event::EntityEvent;
// The journal position an `EntityRef` read reports and a caller waits on:
// authoring vocabulary, not store vocabulary, so it alone of the journal names
// sits here.
pub use journal::Seq;
// What `EntityRef::subscribe` and `EntityRef::follow` hand back: read
// vocabulary beside `EventHistory`, so a caller following an entity opens no
// storage namespace.
pub use subscription::Followed;
pub use subscription::Follower;
pub use subscription::RecordStream;
pub use subscription::Subscription;
pub use system::EntitySystem;
