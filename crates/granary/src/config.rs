//! Deployment configuration for a grain type (spec Appendix A).

use std::time::Duration;

/// Per-grain-type runtime configuration (spec Appendix A).
///
/// `shards` partitions the type's namespace into a fixed number of consensus
/// groups (§7.1); `idle_after` drives hibernation (§10) and `snapshot_every`
/// drives snapshotting (§9). `replication_factor` bounds a clustered shard's voter
/// set (§7.6): the allocator selects this many replicas per shard by rendezvous
/// hashing and the reconcile loop reconfigures the group as membership changes —
/// a no-op in Tier 1 (one local store). One field describes elasticity the runtime
/// does not yet perform, kept so a deployment need not change shape later:
/// - `shard_target_bytes` — the split threshold (§7.7); split/merge is deferred,
///   so `shards` is fixed at `granary()` time.
#[derive(Clone, Debug)]
pub struct GranaryConfig {
    /// The number of shards this grain type's namespace is partitioned into
    /// (§7.1). Each shard is one consensus group; a name maps to a shard by a
    /// stable hash. Fixed at `granary()` time (dynamic split/merge is deferred,
    /// §7.7).
    pub shards: usize,
    /// Replicas per shard (§7.1): the allocator bounds each shard's voter set to
    /// this many nodes by rendezvous hashing (§7.6). **No-op in Tier 1** (one
    /// local store).
    pub replication_factor: usize,
    /// Split a shard once it grows past this size (§7.7). **Deferred** — shards
    /// are fixed; **no-op in Tier 1.**
    pub shard_target_bytes: u64,
    /// Hibernate a grain after this idle interval (§10). Matches the Durable
    /// Objects eviction window by default (DO §5).
    pub idle_after: Duration,
    /// Persist a snapshot every this many committed events (§9). `0` disables
    /// snapshotting (every activation replays the full log).
    pub snapshot_every: u64,
}

impl Default for GranaryConfig {
    fn default() -> Self {
        GranaryConfig {
            shards: 4,
            replication_factor: 3,
            shard_target_bytes: 256 << 20,
            idle_after: Duration::from_secs(10),
            snapshot_every: 256,
        }
    }
}
