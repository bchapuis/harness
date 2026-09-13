//! The capabilities a **node** provides to every entity type it hosts (spec §7.4,
//! §13), as distinct from the per-type settings in [`EntityConfig`].
//!
//! Two things in this crate are scoped to the node rather than to an entity type: the
//! blocking-I/O pool, which bounds *this node's* concurrent device work, and the
//! metrics registry, which an operator reads to judge *this node's* health. Per-type
//! copies would give a node hosting three types three pools, and a type that omitted
//! one an inline pool rather than the node's, which stays invisible until a stalled
//! device blocks the executor the pool was added to protect.
//!
//! A deployment builds one [`EntityNode`], sets what it has, and hosts every type
//! through it; the handles are resolved once, when the node is built.

use std::sync::Arc;

use crate::alarm_index::AlarmIndex;
use crate::config::EntityConfig;
use crate::entity::Entity;
use crate::entity_ref::Entities;
use crate::system::EntitySystem;

/// The node-scoped handles every entity type on a node shares.
///
/// Cheap to clone (two `Arc`s) and resolved at construction, so a hot path reads a
/// handle rather than building the default one again.
#[derive(Clone)]
pub(crate) struct NodeCapabilities {
    io: Arc<dyn crate::blocking::BlockingIo>,
    metrics: Arc<dyn crate::metrics::EntityMetrics>,
}

impl Default for NodeCapabilities {
    /// The documented defaults: blocking store I/O runs inline on the calling async
    /// worker, and measurements are discarded. Correct, and what the deterministic
    /// simulation requires (§14); see `crate::blocking` for why the pool is a seam
    /// rather than a default.
    fn default() -> NodeCapabilities {
        NodeCapabilities {
            io: Arc::new(crate::blocking::InlineIo),
            metrics: Arc::new(()),
        }
    }
}

impl NodeCapabilities {
    /// This node's blocking-I/O seam (§7.4).
    pub(crate) fn blocking_io(&self) -> &Arc<dyn crate::blocking::BlockingIo> {
        &self.io
    }

    /// This node's metrics sink (§13).
    pub(crate) fn metrics(&self) -> &Arc<dyn crate::metrics::EntityMetrics> {
        &self.metrics
    }
}

impl std::fmt::Debug for NodeCapabilities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeCapabilities")
            .field("blocking_io", &"<seam>")
            .field("metrics", &"<sink>")
            .finish()
    }
}

/// A node's entity hosting, carrying the capabilities every type it hosts shares
/// (spec §7.4, §13).
///
/// Obtained from [`EntityExt::entity_node`](crate::EntityExt::entity_node),
/// configured once, and then used in place of the `system.entities(..)` calls:
///
/// ```ignore
/// let node = system
///     .entity_node()
///     .blocking_io(Arc::new(ThreadPoolIo::sized_for_host()))
///     .metrics(Arc::clone(&metrics));
///
/// let alarms: Entities<AlarmIndex<_>> = node.entities(alarm_config);
/// let machines: Entities<Machine> = node.entities_named_with_alarms(..., alarms);
/// ```
///
/// Hosting a type straight off the system (`system.entities(..)`) still works and takes
/// the defaults; the shape every test and the `Local` tier want.
#[derive(Clone)]
pub struct EntityNode<S: EntitySystem> {
    system: S,
    capabilities: NodeCapabilities,
}

impl<S: EntitySystem> EntityNode<S> {
    pub(crate) fn new(system: S) -> EntityNode<S> {
        EntityNode {
            system,
            capabilities: NodeCapabilities::default(),
        }
    }

    /// A production node over real storage under `data`: the durable entity store
    /// every type it hosts shares (spec §7.4), the blocking-I/O pool, and the
    /// operator metrics registry (§13), the three node-scoped choices a deployment
    /// otherwise makes one by one. Returns the store factory beside the node because
    /// hosting is per type ([`EntityConfig::entity_store`]) while the store is per
    /// node: its factory caches per `(type, node)`, so every type shares one on-disk
    /// store keyed by shard and entity, the entity analogue of the Raft WAL.
    ///
    /// The store stamps the codec's name and refuses to open under another (§7.4),
    /// and the codec is read off `system`, so the stamp cannot name a value other
    /// than the one the messages are encoded with.
    ///
    /// The pool: without it every fsync runs on the async worker that is also
    /// driving this node's Raft heartbeats and its shards' quorum waits, so a slow
    /// disk becomes spurious elections and a rehydration storm rather than slow
    /// writes. The metrics: commit latency by outcome, rehydration cost and the
    /// resident count, the signals that say whether this node's shards are healthy,
    /// distinct from the event stream the simulator's checkers read.
    pub fn durable(
        system: S,
        data: &std::path::Path,
    ) -> (EntityNode<S>, crate::store::EntityStoreFactory) {
        let store =
            crate::file_store::FileEntityStore::factory(data.join("entities"), &*system.codec());
        let node = EntityNode::new(system)
            .blocking_io(Arc::new(crate::blocking::ThreadPoolIo::sized_for_host()))
            .metrics(Arc::new(crate::metrics::AtomicEntityMetrics::new()));
        (node, store)
    }

    /// Where this node's blocking store I/O runs (spec §7.4).
    ///
    /// Without it every fsync runs on the async worker that is also driving this
    /// node's Raft heartbeats and its other shards' quorum waits, so a slow device
    /// becomes spurious elections and a rehydration storm rather than merely slow
    /// writes. Supply [`ThreadPoolIo`](crate::blocking::ThreadPoolIo) on real storage; see
    /// `crate::blocking` for why the case for it is the tail rather than the median.
    #[must_use]
    pub fn blocking_io(mut self, io: Arc<dyn crate::blocking::BlockingIo>) -> EntityNode<S> {
        self.capabilities.io = io;
        self
    }

    /// Where this node reports its operator-facing measurements (spec §13).
    ///
    /// Distinct from the event stream, which is the checker's interface: see
    /// `crate::metrics` for why both exist.
    #[must_use]
    pub fn metrics(mut self, metrics: Arc<dyn crate::metrics::EntityMetrics>) -> EntityNode<S> {
        self.capabilities.metrics = metrics;
        self
    }

    /// Host entities of type `G` under its own `G::ENTITY_TYPE`, building each
    /// activation with `G::default`: this node's form of
    /// [`EntityExt::entity`](crate::EntityExt::entity).
    pub fn entities<G>(&self, config: EntityConfig) -> Entities<G>
    where
        G: Entity<System = S> + Default,
    {
        self.entities_named(G::ENTITY_TYPE, config, Arc::new(G::default))
    }

    /// Host entities of type `G` under an explicit runtime **type name** (spec
    /// §5.1), with a caller-supplied **factory** for each activation's behavior.
    ///
    /// Two extension points over [`entities`](EntityNode::entities), both for one
    /// Rust entity that must be **many** entity types at runtime:
    ///
    /// - `entity_type` overrides `G::ENTITY_TYPE`, so the same `G` hosts under
    ///   several names: distinct gateways, shard maps, and consensus groups. It
    ///   MUST be stable cluster-wide and across runs, exactly as `G::ENTITY_TYPE`
    ///   must be (§5.1); a `&'static str` makes that lifetime explicit.
    /// - `factory` replaces `G::default`, so the runtime can inject per-node seam
    ///   handles into each fresh activation (the entity needs no `Default`).
    pub fn entities_named<G>(
        &self,
        entity_type: &'static str,
        config: EntityConfig,
        factory: Arc<dyn Fn() -> G + Send + Sync>,
    ) -> Entities<G>
    where
        G: Entity<System = S>,
    {
        crate::entity_ref::build_entities::<S, G>(
            &self.system,
            entity_type,
            config,
            &self.capabilities,
            factory,
            None,
        )
    }

    /// Host entities of type `G` with durable-alarm firing: this node's form of
    /// [`EntityExt::entities_with_alarms`](crate::EntityExt::entities_with_alarms).
    pub fn entities_with_alarms<G>(
        &self,
        config: EntityConfig,
        index: Entities<AlarmIndex<S>>,
    ) -> Entities<G>
    where
        G: Entity<System = S> + Default,
    {
        self.entities_named_with_alarms(G::ENTITY_TYPE, config, Arc::new(G::default), index)
    }

    /// [`entities_named`](EntityNode::entities_named) with durable-alarm firing
    /// (spec §7.16): each host registers its pending deadline with the per-shard
    /// `index`, and a background driver re-activates due entities on the shards
    /// this node leads. See [`EntityExt::entities_with_alarms`](crate::EntityExt::entities_with_alarms).
    pub fn entities_named_with_alarms<G>(
        &self,
        entity_type: &'static str,
        config: EntityConfig,
        factory: Arc<dyn Fn() -> G + Send + Sync>,
        index: Entities<AlarmIndex<S>>,
    ) -> Entities<G>
    where
        G: Entity<System = S>,
    {
        crate::entity_ref::build_entities::<S, G>(
            &self.system,
            entity_type,
            config,
            &self.capabilities,
            factory,
            Some(index),
        )
    }
}
