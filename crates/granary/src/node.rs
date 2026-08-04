//! The capabilities a **node** provides to every grain type it hosts (spec §7.4,
//! §13), as distinct from the per-type settings in [`GranaryConfig`].
//!
//! Two things in this crate are scoped to the node rather than to a grain type: the
//! blocking-I/O pool, which exists to bound *this node's* concurrent device work, and
//! the metrics registry, which an operator reads to judge *this node's* health. Both
//! once lived in [`GranaryConfig`], which is per grain type, and that placement was
//! wrong in a way that showed up at every call site: a node hosting three types wrote
//! the same two handles into three configs, and a config that omitted one silently got
//! a *second* pool — an inline one — rather than the node's. Neither mistake is
//! visible in a type signature, and the second is invisible at runtime too, right up
//! until a stalled device blocks the executor the pool was added to protect.
//!
//! So they hang here instead. A deployment builds one [`GranaryNode`], sets what it
//! has, and hosts every type through it; the handles are resolved once, when the node
//! is built, rather than rebuilt per call.

use std::sync::Arc;

use crate::alarm_index::AlarmIndex;
use crate::config::GranaryConfig;
use crate::grain::Grain;
use crate::grainref::Granary;
use crate::system::GranarySystem;

/// The node-scoped handles every grain type on a node shares.
///
/// Cheap to clone (two `Arc`s) and resolved at construction, so a hot path reads a
/// handle rather than building the default one again.
#[derive(Clone)]
pub(crate) struct NodeCapabilities {
    io: Arc<dyn crate::BlockingIo>,
    metrics: Arc<dyn crate::GrainMetrics>,
}

impl Default for NodeCapabilities {
    /// The documented defaults: blocking store I/O runs inline on the calling async
    /// worker, and measurements are discarded. Correct, and what the deterministic
    /// simulation requires (§14) — see [`crate::blocking`] for why the pool is a seam
    /// rather than a default.
    fn default() -> NodeCapabilities {
        NodeCapabilities {
            io: Arc::new(crate::InlineIo),
            metrics: Arc::new(()),
        }
    }
}

impl NodeCapabilities {
    /// This node's blocking-I/O seam (§7.4).
    pub(crate) fn blocking_io(&self) -> &Arc<dyn crate::BlockingIo> {
        &self.io
    }

    /// This node's metrics sink (§13).
    pub(crate) fn metrics(&self) -> &Arc<dyn crate::GrainMetrics> {
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

/// A node's grain hosting, carrying the capabilities every type it hosts shares
/// (spec §7.4, §13).
///
/// Obtained from [`GranaryExt::granary_node`](crate::GranaryExt::granary_node),
/// configured once, and then used in place of the `system.granary(..)` calls:
///
/// ```ignore
/// let node = system
///     .granary_node()
///     .blocking_io(Arc::new(ThreadPoolIo::sized_for_host()))
///     .metrics(Arc::clone(&metrics));
///
/// let alarms: Granary<AlarmIndex<_>> = node.granary(alarm_config);
/// let machines: Granary<Machine> = node.granary_named_with_alarms(..., alarms);
/// ```
///
/// Hosting a type straight off the system (`system.granary(..)`) still works and takes
/// the defaults — the shape every test and the `Local` tier want.
pub struct GranaryNode<S: GranarySystem> {
    system: S,
    capabilities: NodeCapabilities,
}

impl<S: GranarySystem> Clone for GranaryNode<S> {
    fn clone(&self) -> GranaryNode<S> {
        GranaryNode {
            system: self.system.clone(),
            capabilities: self.capabilities.clone(),
        }
    }
}

impl<S: GranarySystem> GranaryNode<S> {
    pub(crate) fn new(system: S) -> GranaryNode<S> {
        GranaryNode {
            system,
            capabilities: NodeCapabilities::default(),
        }
    }

    /// Where this node's blocking store I/O runs (spec §7.4).
    ///
    /// Without it every fsync runs on the async worker that is also driving this
    /// node's Raft heartbeats and its other shards' quorum waits, so a slow device
    /// becomes spurious elections and a rehydration storm rather than merely slow
    /// writes. Supply [`ThreadPoolIo`](crate::ThreadPoolIo) on real storage; see
    /// [`crate::blocking`] for why the case for it is the tail rather than the median.
    #[must_use]
    pub fn blocking_io(mut self, io: Arc<dyn crate::BlockingIo>) -> GranaryNode<S> {
        self.capabilities.io = io;
        self
    }

    /// Where this node reports its operator-facing measurements (spec §13).
    ///
    /// Distinct from the event stream, which is the checker's interface: see
    /// [`crate::metrics`] for why both exist.
    #[must_use]
    pub fn metrics(mut self, metrics: Arc<dyn crate::GrainMetrics>) -> GranaryNode<S> {
        self.capabilities.metrics = metrics;
        self
    }

    /// Host grains of type `G` under its own `G::GRAIN_TYPE`, building each
    /// activation with `G::default` — this node's form of
    /// [`GranaryExt::granary`](crate::GranaryExt::granary).
    pub fn granary<G>(&self, config: GranaryConfig) -> Granary<G>
    where
        G: Grain<System = S> + Default,
    {
        self.granary_named(G::GRAIN_TYPE, config, Arc::new(G::default))
    }

    /// Host grains of type `G` under an explicit runtime type name with a
    /// caller-supplied activation factory — this node's form of
    /// [`GranaryExt::granary_named`](crate::GranaryExt::granary_named).
    pub fn granary_named<G>(
        &self,
        grain_type: &'static str,
        config: GranaryConfig,
        factory: Arc<dyn Fn() -> G + Send + Sync>,
    ) -> Granary<G>
    where
        G: Grain<System = S>,
    {
        crate::grainref::build_granary::<S, G>(
            &self.system,
            grain_type,
            config,
            &self.capabilities,
            factory,
            None,
        )
    }

    /// Host grains of type `G` with durable-alarm firing — this node's form of
    /// [`GranaryExt::granary_with_alarms`](crate::GranaryExt::granary_with_alarms).
    pub fn granary_with_alarms<G>(
        &self,
        config: GranaryConfig,
        index: Granary<AlarmIndex<S>>,
    ) -> Granary<G>
    where
        G: Grain<System = S> + Default,
    {
        self.granary_named_with_alarms(G::GRAIN_TYPE, config, Arc::new(G::default), index)
    }

    /// Host grains of type `G` under an explicit runtime type name with durable-alarm
    /// firing — this node's form of
    /// [`GranaryExt::granary_named_with_alarms`](crate::GranaryExt::granary_named_with_alarms).
    pub fn granary_named_with_alarms<G>(
        &self,
        grain_type: &'static str,
        config: GranaryConfig,
        factory: Arc<dyn Fn() -> G + Send + Sync>,
        index: Granary<AlarmIndex<S>>,
    ) -> Granary<G>
    where
        G: Grain<System = S>,
    {
        crate::grainref::build_granary_with_alarms::<S, G>(
            &self.system,
            grain_type,
            config,
            &self.capabilities,
            factory,
            index,
        )
    }
}
