//! The `BlobSystem` capability seam (the `GranarySystem` analogue, granary §7.3).
//!
//! The `Clustered` tier and its background loops need capabilities the bare
//! [`ActorSystem`] trait does not expose: virtual time (for reconcile and tombstone
//! intervals), task launching (to drain stragglers and run the reconcile loop), and
//! the **serving set** that places a blob's owners (spec §5.2). Rather than thread
//! the concrete `Clock`/`Entropy`/`Spawner`/`Transport` type parameters of
//! [`ClusterSystem`] through the replica transport, the tier, and the reconcile
//! loop — which would leak them everywhere — the store requires its system to
//! implement this one object-friendly trait, exactly as Granary requires
//! `GranarySystem`.
//!
//! It is implemented for [`LocalSystem`] (the `Local` tier: a single node, itself
//! the only serving member) and [`ClusterSystem`] (the `Clustered` tier: the SWIM
//! serving set), so the same store code runs on both without naming either.

use std::time::Duration;

use actor_cluster::ClusterSystem;
use actor_cluster::MemberStatus;
use actor_cluster::Transport;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::Instant;
use actor_core::LocalSystem;
use actor_core::NodeId;
use actor_core::Spawner;

use crate::event::BlobEvent;

/// An [`ActorSystem`] that can host a blob store: it exposes virtual time, task
/// launching, and the serving set that owner selection hashes against (spec §5.2,
/// **B5**).
pub trait BlobSystem: ActorSystem {
    /// The current virtual time (for reconcile/tombstone scheduling).
    fn now(&self) -> Instant;

    /// A future that completes after `dur` of virtual time — the period between
    /// reconcile passes (spec §7) and tombstone re-syncs (spec §5.3).
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()>;

    /// Launch a detached background task: the reconcile loop, and the straggler
    /// drain of a `put` that already reached `W` (spec §5.2, §7).
    fn launch(&self, task: BoxFuture<'static, ()>);

    /// The set of nodes currently serving, the candidate set for owner selection
    /// (spec §5.2). The `Local` tier is a single node — itself; the `Clustered`
    /// tier reads the SWIM serving set (`Membership::serving_members`).
    fn serving_members(&self) -> Vec<NodeId>;

    /// Whether `node` has reached a **terminal** membership state — `down` or
    /// `removed`, or pruned from the roster entirely (actor §9.1). This is the
    /// load-bearing predicate for tombstone reclamation (spec §5.3, **B7**): a
    /// terminal node can rejoin only under a fresh, empty identity, so it can never
    /// return carrying an un-swept blob. A merely `unreachable` (reversible) node
    /// is **not** terminal. The `Local` tier is a single node that is never
    /// terminal to itself.
    fn is_terminal(&self, node: NodeId) -> bool;

    /// Emit a [`BlobEvent`] onto the framework's observability stream (actor §16),
    /// wrapped as an application event so the checkers and the reproducibility
    /// recorder observe it in the one ordered stream (spec §8, §9).
    fn emit_blob_event(&self, event: BlobEvent);
}

impl<C: Clock, E: Entropy, S: Spawner> BlobSystem for LocalSystem<C, E, S> {
    fn now(&self) -> Instant {
        self.clock().now()
    }

    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        let clock = self.clock().clone();
        Box::pin(async move { clock.sleep(dur).await })
    }

    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.spawner().launch(task);
    }

    fn serving_members(&self) -> Vec<NodeId> {
        vec![self.node()]
    }

    fn is_terminal(&self, _node: NodeId) -> bool {
        false
    }

    fn emit_blob_event(&self, event: BlobEvent) {
        self.emit(Event::app(event));
    }
}

impl<C, E, S, T> BlobSystem for ClusterSystem<C, E, S, T>
where
    C: Clock,
    E: Entropy,
    S: Spawner,
    T: Transport,
{
    fn now(&self) -> Instant {
        self.clock().now()
    }

    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        let clock = self.clock().clone();
        Box::pin(async move { clock.sleep(dur).await })
    }

    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.launch_task(task);
    }

    fn serving_members(&self) -> Vec<NodeId> {
        self.membership().serving_members()
    }

    fn is_terminal(&self, node: NodeId) -> bool {
        // Terminal = down/removed, or pruned from the roster entirely (`None`): a
        // member at anchor time that is no longer present has been removed and
        // pruned, which is terminal. A reachable-or-unreachable member is not.
        matches!(
            self.membership().status(node),
            None | Some(MemberStatus::Down) | Some(MemberStatus::Removed)
        )
    }

    fn emit_blob_event(&self, event: BlobEvent) {
        self.emit(Event::app(event));
    }
}
