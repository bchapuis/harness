//! Sharding, replication, and the node (spec §7.1, §7.2, §7.6, §7.7).
//!
//! What makes an entity's journal survive a node: the partition of a type's
//! namespace into shards, the map that allocates them, the transport a shard
//! leader quorum-appends over, and the per-node runtime that hosts the whole
//! thing. A single-node deployment (the `Local` tier, §7.4) names nothing here.
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

/// The alarm index's runtime vocabulary (spec §7.16): the per-shard entity that
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
