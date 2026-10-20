//! Sharding, replication, and the node (spec storage §4.1, §5, §7, §8).
//!
//! What makes an entity's journal survive a node: the partition of a type's
//! namespace into shards, the map that allocates them, the transport a shard
//! leader quorum-appends over, and the per-node runtime that hosts the whole
//! thing. A single-node deployment (the `Local` tier, storage §3.2) names nothing here.
//!
//! This is the half of the crate that knows about *placement*; [`crate::store`]
//! is the half that knows about *durability*, and neither knows what an entity
//! is beyond its [`EntityName`](crate::EntityName).

pub use crate::alarm_index::AlarmIndex;
pub use crate::node::EntityNode;
pub use crate::replica_store::ReplicaTransport;
pub use crate::replicator::Deadlines;
pub use crate::shardmap::ShardMapSource;
pub use crate::system::FailureDomains;
pub use crate::system::ShardId;
pub use crate::system::shard_for;

/// The replica-store protocol (spec storage §5, §6): the messages a shard leader sends a
/// replica's store, one per [`ReplicaTransport`] method, public because the trait's
/// signatures carry them.
pub mod replica {
    pub use crate::replica_store::GetBlob;
    pub use crate::replica_store::HasBlob;
    pub use crate::replica_store::ListBlobs;
    pub use crate::replica_store::ListEntities;
    pub use crate::replica_store::Prepare;
    pub use crate::replica_store::PutBlob;
    pub use crate::replica_store::SealRange;
    pub use crate::replica_store::StoreRecord;
    pub use crate::replica_store::StoreSnapshot;
    pub use crate::replica_store::SweepBlobs;
}

/// The alarm index's runtime vocabulary (spec facets §5.5): the per-shard entity that
/// lets an alarmed entity hibernate without losing its deadline, and the commands
/// its driver speaks to it.
pub mod alarm_index {
    pub use crate::alarm_index::ALARM_INDEX_TYPE;
    pub use crate::alarm_index::AllPending;
    pub use crate::alarm_index::DueBefore;
    pub use crate::alarm_index::Pending;
    // Aliased, as it was at the root: a bare `Sync` in a consumer's scope
    // shadows the auto-trait.
    pub use crate::alarm_index::Sync as AlarmSync;
    pub use crate::alarm_index::index_key;
}
