//! Deployment configuration for a grain type (spec Appendix A).

use std::sync::Arc;
use std::time::Duration;

use crate::store::GrainStoreFactory;

/// Per-grain-type runtime configuration (spec Appendix A).
#[derive(Clone)]
pub struct GranaryConfig {
    /// The number of shards this grain type's namespace is **initially**
    /// partitioned into (§7.1): `granary()` founds this many equal key ranges.
    /// Each shard is one consensus group; a name maps to a shard by a stable
    /// hash onto the range partition. The partition then grows and shrinks with
    /// load through split/merge (§7.7).
    pub shards: usize,
    /// Replicas per shard (§7.1): the allocator bounds each shard's voter set to
    /// this many nodes by rendezvous hashing, and the reconcile loop reconfigures
    /// the group as membership changes (§7.6). **No-op on the `Local` tier**
    /// (one local store).
    pub replication_factor: usize,
    /// Auto-split a shard once its durable footprint grows past this many bytes
    /// (§7.7): the shard leader measures its local store and requests a split,
    /// which divides the shard's key range in two. **`0` (the default) disables
    /// the size trigger**, since a good threshold depends on how large a type's
    /// grains are individually. Split and merge remain available explicitly
    /// ([`Granary::split_shard`](crate::Granary::split_shard) /
    /// [`Granary::merge_shards`](crate::Granary::merge_shards)). **No-op on the
    /// `Local` tier** (split/merge is `Quorum` elasticity).
    pub shard_target_bytes: u64,
    /// Hibernate a grain after this idle interval (§10).
    ///
    /// The mechanism comes from Durable Objects (DO §5); the *number* does not. DO
    /// evicts after ten seconds because it multiplexes tenants onto shared hosts and
    /// prices resident memory. On a dedicated node the two sides of the trade read
    /// differently: eviction reclaims a grain's state — kilobytes against a machine
    /// sized in the hundreds of gigabytes — and costs a quorum head-recovery round
    /// trip on the next touch, plus a snapshot on the way out if enough has
    /// accumulated (§9). Memory is the abundant resource here and the round trip is
    /// the scarce one, so the window is set long enough that a grain in intermittent
    /// use stays resident between touches (`docs/hardware-envelope.md` §3.1, §3.2).
    ///
    /// Shorten it for a type with many rarely-touched grains and large state;
    /// lengthen it for one whose grains are touched in bursts minutes apart — an
    /// agent session between turns is exactly that shape.
    pub idle_after: Duration,
    /// Persist a snapshot every this many committed events (§9). `0` disables
    /// snapshotting (every activation replays the full log).
    ///
    /// The trigger for **both** snapshot paths, the write path and idle hibernation
    /// (§10), so this one number decides how often a grain pays what a snapshot
    /// costs — and on the `Quorum` tier that cost lands on every replica (§7.3),
    /// billed R−1 times to the uplink (`docs/hardware-envelope.md` §3.9). It is no
    /// longer O(state): past 64 KiB facet 0's state travels as content-defined
    /// chunks and only the chunks that changed are sent (§7.12), so a snapshot is
    /// roughly O(delta) plus a manifest. What it buys is a shorter replay, and
    /// replay reads are **local** (§9). The default stays high — the thing being
    /// saved is still cheap and a snapshot is still not free — but the penalty for
    /// lowering it is far smaller for a grain whose state grows by accretion.
    ///
    /// The counter-pressure is failover, not steady state: a new leader recovers the
    /// records above the last snapshot from a quorum (§8), so a larger gap means a
    /// larger one-off recovery read per grain. That is once per leadership change
    /// against a broadcast every threshold, which is why the balance sits where it
    /// does. A type whose events are large relative to its state should lower it.
    pub snapshot_every: u64,
    /// How each node obtains its durable [`GrainStore`](crate::store::GrainStore)
    /// (spec §7.4). `None` (the default) gives every node a fresh in-memory store,
    /// lost on restart. A deployment that must survive a full-cluster cold restart
    /// supplies a factory that caches per node and outlives a restart — the grain
    /// analogue of the Raft WAL storage seam (actor §9.4.3).
    pub grain_store: Option<GrainStoreFactory>,
    /// Where this node's blocking store I/O runs (spec §7.4). `None` (the default)
    /// runs it on the calling async worker — correct, and what the deterministic
    /// simulation requires (§14), but it means a *stalled* device blocks a thread that
    /// is also driving Raft heartbeats and other shards' quorum waits. A deployment on
    /// real storage supplies [`ThreadPoolIo`](crate::ThreadPoolIo); see
    /// [`crate::blocking`] for why this is a seam rather than a default, and why the
    /// case for it is the tail rather than the median.
    ///
    /// Shared across every grain type hosted on the node, since the pool exists to
    /// bound *the node's* concurrent device work, not one type's.
    pub blocking_io: Option<Arc<dyn crate::BlockingIo>>,
    /// How this deployment groups nodes into failure domains — racks, zones,
    /// whatever fails together (spec §7.1). `None` (the default) treats every node as
    /// its own domain, which is the historical behaviour: replicas spread across the
    /// cluster but nothing stops all R of a shard landing in one zone, so a single
    /// zone loss can take shards below quorum while the cluster looks healthy.
    ///
    /// Supplying it makes the allocator take at most `ceil(R / domains)` replicas per
    /// domain. The mapping MUST agree on every node — see
    /// [`FailureDomains`](crate::FailureDomains).
    pub failure_domains: Option<crate::FailureDomains>,
    /// How long a per-grain quorum append, snapshot, or blob put waits before
    /// reporting `Unavailable` (spec §11).
    ///
    /// Configurable because the right value is a property of the deployment's
    /// network and storage, not of the code, and the spread is two orders of
    /// magnitude: a cluster inside one datacenter park commits in single-digit
    /// milliseconds, one spanning two parks adds a few more per round trip, and one
    /// spanning regions is in the hundreds. `docs/standalone-deployment.md` carries
    /// the per-topology table; `docs/hardware-envelope.md` §2 carries the round-trip
    /// numbers it is derived from. Set it well above the p99.9 commit latency you
    /// actually observe, never against a median (hw §3.7). The cost of
    /// setting it too low is not a slow write but a *false* one: every timeout is
    /// ambiguous (the append MAY still commit later, §7.2), so it steps the
    /// activation down and forces a full rehydration, turning transient slowness into
    /// activation churn — which itself generates more I/O.
    pub quorum_timeout: Duration,
    /// How long a recovery read quorum waits before falling back to local state
    /// (§7.5, read-your-leader) or reporting `Unavailable` (spec §8, §11).
    ///
    /// Kept separate from [`quorum_timeout`](GranaryConfig::quorum_timeout) because
    /// the two fail differently: a slow append is a stalled write, while a slow
    /// recovery is a grain that cannot activate at all, and after a failover every
    /// grain on the shard pays it at once.
    pub recover_timeout: Duration,
    /// How many resolved host handles a node caches per grain type (spec §5.4).
    ///
    /// The cache is what keeps a repeat call off the serial gateway, so it wants to
    /// be large; it is also per *name*, and a long-lived client — the gateway fronts
    /// every tenant — would otherwise grow one entry for every name it has ever
    /// addressed. A miss is not an error, only one gateway round-trip — which is
    /// precisely the resource this deployment is short of, so the bound is set by what
    /// the entries cost (a handle and a name, tens of megabytes at this size) rather
    /// than by how many are expected to be live (`docs/hardware-envelope.md` §3.1,
    /// §3.2). The cache holds up
    /// to twice this many entries while an older generation ages out.
    pub host_cache_capacity: usize,
    /// Where this node reports its operator-facing measurements (spec §13). `None`
    /// (the default) discards them. Distinct from the event stream, which is the
    /// checker's interface: see [`crate::metrics`] for why both exist.
    pub metrics: Option<Arc<dyn crate::GrainMetrics>>,
    /// The node-local scratch directory a **physical facet** materializes under
    /// (spec §7.12/§7.14): the SQL facet's database files live here, keyed by
    /// grain. Rebuildable caches only, never a source of truth (§1); safe to
    /// wipe between runs. `None` (the default) uses the system temp directory.
    pub data_dir: Option<std::path::PathBuf>,
}

impl GranaryConfig {
    /// This node's blocking-I/O seam: the configured pool, or the inline default.
    pub(crate) fn blocking_io(&self) -> Arc<dyn crate::BlockingIo> {
        self.blocking_io
            .clone()
            .unwrap_or_else(|| Arc::new(crate::InlineIo))
    }

    /// This node's metrics sink: the configured one, or the discarding default.
    pub(crate) fn metrics(&self) -> Arc<dyn crate::GrainMetrics> {
        self.metrics.clone().unwrap_or_else(|| Arc::new(()))
    }

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
            shard_target_bytes: 0, // auto-split off by default; opt in per type
            // Five minutes, not the ten seconds Durable Objects evicts at: this node's
            // memory is not the resource under pressure, and each eviction cycle costs
            // a recovery round trip and possibly a full-state snapshot broadcast.
            idle_after: Duration::from_secs(300),
            // A snapshot is O(state) on the wire to every replica; a replay is local.
            // Bias hard toward replaying (see the field docs).
            snapshot_every: 4096,
            grain_store: None,
            blocking_io: None,
            metrics: None,
            failure_domains: None,
            // Comfortably above a healthy quorum round-trip (milliseconds) yet short
            // enough that a write to an unreachable shard fails fast rather than
            // pinning the host's serial executor.
            quorum_timeout: Duration::from_secs(2),
            recover_timeout: Duration::from_secs(2),
            // Sized by what the entries cost, not by the working set: a miss is a
            // gateway round trip, and round trips are the scarce resource here.
            host_cache_capacity: 65536,
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
            .field(
                "failure_domains",
                &self.failure_domains.as_ref().map(|_| "<map>"),
            )
            .field("quorum_timeout", &self.quorum_timeout)
            .field("recover_timeout", &self.recover_timeout)
            .field("host_cache_capacity", &self.host_cache_capacity)
            .field("metrics", &self.metrics.as_ref().map(|_| "<sink>"))
            .field(
                "blocking_io",
                &self.blocking_io.as_ref().map_or("<inline>", |_| "<pool>"),
            )
            .field("data_dir", &self.data_dir)
            .finish()
    }
}
