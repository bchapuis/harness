//! The capabilities a **node** provides to every entity type it hosts (spec storage §3.2,
//! facets §5.5, entity §11), as distinct from the per-type settings in [`EntityConfig`].
//!
//! Three things in this crate are scoped to the node rather than to an entity type:
//! the blocking-I/O pool, which bounds *this node's* concurrent device work, the
//! metrics registry, which an operator reads to judge *this node's* health, and the
//! alarm index, which wakes this node's alarmed entities across hibernation and
//! failover. Per-type copies would give a node hosting three types three pools, and
//! a type that omitted one an inline pool rather than the node's, which stays
//! invisible until a stalled device blocks the executor the pool was added to
//! protect. The index fails the same silent way per type: a type with the `Alarm`
//! facet hosted without it never fires across hibernation or failover, and a type
//! without the facet given it runs a driver sweep for nothing. Set once on the node,
//! it reaches exactly the types whose facet set declares `Alarm`.
//!
//! A deployment builds one [`EntityNode`], sets what it has, and hosts every type
//! through it; the handles are resolved once, when the node is built. The node also
//! remembers each type it hosted, so a graceful stop hands off every led shard in
//! one call ([`EntityNode::hand_off_leadership`]).

use std::sync::Arc;
use std::sync::Mutex;

use crate::alarm_index::AlarmIndex;
use crate::config::EntityConfig;
use crate::entity::Entity;
use crate::entity_ref::Entities;
use crate::shardmap::ShardMapSource;
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
    /// simulation requires (§10); see `crate::blocking` for why the pool is a seam
    /// rather than a default.
    fn default() -> NodeCapabilities {
        NodeCapabilities {
            io: Arc::new(crate::blocking::InlineIo),
            metrics: Arc::new(()),
        }
    }
}

impl NodeCapabilities {
    /// This node's blocking-I/O seam (storage §3.2).
    pub(crate) fn blocking_io(&self) -> &Arc<dyn crate::blocking::BlockingIo> {
        &self.io
    }

    /// This node's metrics sink (§3.4).
    pub(crate) fn metrics(&self) -> &Arc<dyn crate::metrics::EntityMetrics> {
        &self.metrics
    }
}

/// A node's entity hosting, carrying the capabilities every type it hosts shares
/// (spec storage §3.2, facets §5.5, entity §11).
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
/// let node = node.alarm_index(alarms);
/// let machines: Entities<Machine> = node.entities_named(..., factory);
///
/// // At the graceful stop: every type hosted above, in one call.
/// node.hand_off_leadership().await;
/// ```
///
/// Hosting a type straight off the system (`system.entities(..)`) still works and takes
/// the defaults; the shape every test and the `Local` tier want.
#[derive(Clone)]
pub struct EntityNode<S: EntitySystem> {
    system: S,
    capabilities: NodeCapabilities,
    /// The per-shard alarm index (facets §5.5) wired into every type hosted from here on
    /// whose facet set declares the `Alarm` facet.
    alarm_index: Option<Entities<AlarmIndex<S>>>,
    /// The shard map of every type hosted through this node, shared by every clone
    /// and every builder-method result, so the one registry
    /// [`hand_off_leadership`](EntityNode::hand_off_leadership) reads sees them all.
    hosted: Arc<Mutex<Vec<Arc<dyn ShardMapSource>>>>,
}

impl<S: EntitySystem> EntityNode<S> {
    pub(crate) fn new(system: S) -> EntityNode<S> {
        EntityNode {
            system,
            capabilities: NodeCapabilities::default(),
            alarm_index: None,
            hosted: Arc::default(),
        }
    }

    /// A production node over real storage under `data`: the durable entity store
    /// every type it hosts shares (spec storage §3.2), the blocking-I/O pool, and the
    /// operator metrics registry (§3.4), the three node-scoped choices a deployment
    /// otherwise makes one by one. Returns the store factory beside the node because
    /// hosting is per type ([`EntityConfig::entity_store`]) while the store is per
    /// node: its factory caches per `(type, node)`, so each type hosted on the node
    /// has one on-disk store of its own, keyed by shard and entity, and the fence of
    /// one type never touches another's shards (storage §6).
    ///
    /// The store stamps the codec's name and refuses to open under another (storage §3.2),
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

    /// Where this node's blocking store I/O runs (spec storage §3.2).
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

    /// Where this node reports its operator-facing measurements (spec §3.4).
    ///
    /// Distinct from the event stream, which is the checker's interface: see
    /// `crate::metrics` for why both exist.
    #[must_use]
    pub fn metrics(mut self, metrics: Arc<dyn crate::metrics::EntityMetrics>) -> EntityNode<S> {
        self.capabilities.metrics = metrics;
        self
    }

    /// The per-shard alarm index this node's alarmed entities fire through (spec
    /// facets §5.5): host one [`AlarmIndex`] type and hand its handle here, before hosting
    /// the types that alarm.
    ///
    /// Wired into exactly the types hosted afterwards whose facet set declares the
    /// [`Alarm`](crate::facet::Alarm) facet: each of their hosts registers its
    /// pending deadline with the index, and a per-type driver re-activates due
    /// entities on the shards this node leads, so an alarm fires across hibernation
    /// and failover rather than only while its entity is resident. A type without
    /// the facet gets neither. Without an index an alarm still fires, but its
    /// pending deadline pins the entity resident.
    #[must_use]
    pub fn alarm_index(mut self, index: Entities<AlarmIndex<S>>) -> EntityNode<S> {
        self.alarm_index = Some(index);
        self
    }

    /// Host entities of type `G` under its own `G::ENTITY_TYPE`, building each
    /// activation with `G::default`: this node's form of
    /// [`EntityExt::entities`](crate::EntityExt::entities).
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
        let (entities, shard_map) = crate::entity_ref::build_entities::<S, G>(
            &self.system,
            entity_type,
            config,
            &self.capabilities,
            factory,
            self.alarm_index.clone(),
        );
        self.hosted
            .lock()
            .expect("hosted-types mutex poisoned")
            .push(shard_map);
        entities
    }

    /// Hand every shard this node leads, across every type hosted through it, to
    /// another of that shard's replicas before the node departs (spec storage §6.3),
    /// returning how many were still led when the attempt gave up: `0` when they all
    /// moved.
    ///
    /// Call this from a graceful shutdown, after the node stops accepting new work
    /// and before the process exits. Skipping it is *safe* but expensive: each shard
    /// this node led then waits a full leader-election timeout before its replicas
    /// elect, and every entity on those shards rehydrates on the new leader, so a
    /// rolling restart becomes one failover per led shard. The types hand off
    /// concurrently, each bounded on its own.
    ///
    /// Best-effort: a shard whose other replicas are all lagging or unreachable keeps
    /// its leader here and fails over the slow way. A non-zero return is information
    /// for the operator, not an error to retry: the node is leaving regardless. A
    /// no-op on the `Local` tier, which has no other replicas.
    ///
    /// Covers the types hosted through this node and its clones, and no others: a
    /// type hosted straight off the system (`system.entities(..)`) goes through a
    /// node of its own that nothing keeps, so a deployment that hands off hosts every
    /// type through one `EntityNode`.
    pub async fn hand_off_leadership(&self) -> usize {
        // Copied out so no handoff runs under the lock.
        let hosted = self
            .hosted
            .lock()
            .expect("hosted-types mutex poisoned")
            .clone();
        futures::future::join_all(hosted.iter().map(|map| map.hand_off_leadership()))
            .await
            .into_iter()
            .sum()
    }
}
