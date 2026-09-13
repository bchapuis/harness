//! The `BlobSystem` capability seam (the `EntitySystem` analogue, entity §7.3).
//!
//! The `Clustered` tier and its background loops need capabilities the bare
//! [`ActorSystem`] trait does not expose: virtual time (for reconcile and tombstone
//! intervals), task launching (to drain stragglers and run the reconcile loop), and
//! the **serving set** that places a blob's owners (spec §5.2). Requiring this one
//! object-friendly trait keeps [`ClusterSystem`]'s
//! `Clock`/`Entropy`/`Spawner`/`Transport` type parameters out of the tier and the
//! reconcile loop.
//!
//! Implemented for [`LocalSystem`] (a single node, itself the only serving
//! member) and [`ClusterSystem`] (the SWIM serving set), so the same store code
//! runs on both without naming either.

use actor_cluster::ClusterSystem;
use actor_cluster::Transport;
use actor_core::ActorSystem;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::LocalSystem;
use actor_core::NodeId;
use actor_core::Spawner;

use crate::event::BlobEvent;

/// An [`ActorSystem`] that can host a blob store: it exposes virtual time, task
/// launching, and the serving set that owner selection hashes against (spec §5.2,
/// **B5**).
pub trait BlobSystem: ActorSystem {
    /// The set of nodes currently serving, the candidate set for owner selection
    /// (spec §5.2). The `Local` tier is a single node, itself; the `Clustered`
    /// tier reads the SWIM serving set (`Membership::serving_members`).
    fn serving_members(&self) -> Vec<NodeId>;

    /// Emit a [`BlobEvent`] onto the framework's observability stream (actor §16),
    /// wrapped as an application event so the checkers and the reproducibility
    /// recorder observe it in the one ordered stream (spec §8, §9). (Time,
    /// sleeping, and task launching come from the [`ActorSystem`] supertrait.)
    fn emit_blob_event(&self, event: BlobEvent) {
        self.emit(Event::app(event));
    }
}

impl<C: Clock, E: Entropy, S: Spawner> BlobSystem for LocalSystem<C, E, S> {
    fn serving_members(&self) -> Vec<NodeId> {
        vec![self.node()]
    }
}

impl<C, E, S, T> BlobSystem for ClusterSystem<C, E, S, T>
where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    fn serving_members(&self) -> Vec<NodeId> {
        self.membership().serving_members()
    }
}
