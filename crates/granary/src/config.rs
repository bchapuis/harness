//! Deployment configuration for a grain type (spec Appendix A).

use std::time::Duration;

use crate::store::GrainStoreFactory;

/// Per-grain-type runtime configuration (spec Appendix A).
///
/// `shards` partitions the type's namespace into a fixed number of consensus
/// groups (§7.1); `idle_after` drives hibernation (§10) and `snapshot_every`
/// drives snapshotting (§9). `replication_factor` bounds a clustered shard's voter
/// set (§7.6): the allocator selects this many replicas per shard by rendezvous
/// hashing and the reconcile loop reconfigures the group as membership changes —
/// a no-op on the `Local` tier (one local store). One field describes elasticity
/// the runtime does not yet perform, kept so a deployment need not change shape
/// later:
/// - `shard_target_bytes` — the split threshold (§7.7); split/merge is deferred,
///   so `shards` is fixed at `granary()` time.
#[derive(Clone)]
pub struct GranaryConfig {
    /// The number of shards this grain type's namespace is partitioned into
    /// (§7.1). Each shard is one consensus group; a name maps to a shard by a
    /// stable hash. Fixed at `granary()` time (dynamic split/merge is deferred,
    /// §7.7).
    pub shards: usize,
    /// Replicas per shard (§7.1): the allocator bounds each shard's voter set to
    /// this many nodes by rendezvous hashing (§7.6). **No-op on the `Local` tier**
    /// (one local store).
    pub replication_factor: usize,
    /// Split a shard once it grows past this size (§7.7). **Deferred** — shards
    /// are fixed; **no-op on the `Local` tier.**
    pub shard_target_bytes: u64,
    /// Hibernate a grain after this idle interval (§10). Matches the Durable
    /// Objects eviction window by default (DO §5).
    pub idle_after: Duration,
    /// Persist a snapshot every this many committed events (§9). `0` disables
    /// snapshotting (every activation replays the full log).
    pub snapshot_every: u64,
    /// How each node obtains its durable [`GrainStore`](crate::store::GrainStore)
    /// (spec §7.4). `None` (the default) gives every node a fresh in-memory store,
    /// lost on restart. A deployment that must survive a full-cluster cold restart
    /// supplies a factory that caches per node and outlives a restart — the grain
    /// analogue of the Raft WAL storage seam (actor §9.4.3).
    pub grain_store: Option<GrainStoreFactory>,
    /// The node-local scratch directory a **physical facet** materializes under
    /// (spec §7.12/§7.14): the SQL facet's database files live here, keyed by
    /// grain. Rebuildable caches only, never a source of truth (§1); safe to
    /// wipe between runs. `None` (the default) uses the system temp directory.
    pub data_dir: Option<std::path::PathBuf>,
}

impl GranaryConfig {
    /// The resolved physical-facet scratch directory:
    /// [`data_dir`](GranaryConfig::data_dir), or its documented system-temp
    /// default.
    pub(crate) fn scratch_dir(&self) -> std::path::PathBuf {
        self.data_dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join("granary"))
    }
}

impl Default for GranaryConfig {
    fn default() -> Self {
        GranaryConfig {
            shards: 4,
            replication_factor: 3,
            shard_target_bytes: 256 << 20,
            idle_after: Duration::from_secs(10),
            snapshot_every: 256,
            grain_store: None,
            data_dir: None,
        }
    }
}

impl std::fmt::Debug for GranaryConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GranaryConfig")
            .field("shards", &self.shards)
            .field("replication_factor", &self.replication_factor)
            .field("shard_target_bytes", &self.shard_target_bytes)
            .field("idle_after", &self.idle_after)
            .field("snapshot_every", &self.snapshot_every)
            .field(
                "grain_store",
                &self.grain_store.as_ref().map(|_| "<factory>"),
            )
            .field("data_dir", &self.data_dir)
            .finish()
    }
}
