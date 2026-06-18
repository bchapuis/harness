//! Grain and shard observability events (spec §13).
//!
//! Granary emits its events on the actor framework's single extensible `Event`
//! stream (actor §16) as an application event ([`Event::app`](actor_core::Event)),
//! so a grain run produces one totally-ordered stream interleaved with the core
//! actor events, the seed-reproducibility contract covers grain events for free,
//! and the simulator's invariant checkers (§14) read them with
//! [`Event::as_app::<GrainEvent>`](actor_core::Event::as_app). `GrainEvent`
//! qualifies as an `AppEvent` through the framework's blanket impl (it is
//! `Debug + Clone + PartialEq + Send + Sync + 'static`); no hand-written impl is
//! needed.
//!
//! This enum is the grain-lifecycle subset. The shard events of §13
//! (`LeaderChanged`, `ShardSplit`, `ShardMerged`) remain **deferred**: leadership
//! is observed through the system seam rather than emitted, and split/merge (§7.7)
//! is not yet implemented.

use actor_core::NodeId;

use crate::grain::GrainName;

/// A grain-lifecycle event on the framework's `Event` stream (spec §13).
///
/// Every variant carries the `node` that hosts the activation (its shard's
/// leader, §5.2). The node is what makes the per-node activation guarantee
/// (**G6**, "exactly-once activation *per node*") expressible as a continuous
/// checker over the stream: without it, an activation that migrates on failover
/// is indistinguishable from a second live activation of the same name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GrainEvent {
    /// An activation rehydrated and is about to serve its first command (§10).
    Activated { node: NodeId, name: GrainName },
    /// An activation rebuilt its state from the journal (§9): `from_snapshot`
    /// records whether a snapshot seeded the replay, `replayed` how many events
    /// were folded after it.
    Rehydrated {
        node: NodeId,
        name: GrainName,
        from_snapshot: bool,
        replayed: u64,
    },
    /// A command's events committed at the grain's new head (§6, §7). The reply
    /// is released only after this (the output gate, invariant **G5**).
    Committed { node: NodeId, name: GrainName, seq: u64 },
    /// A snapshot was persisted at a committed seq (§9).
    Snapshotted { node: NodeId, name: GrainName, at: u64 },
    /// An activation hibernated on idle and dropped its in-memory state (§10);
    /// the journal survives, so the next message rehydrates it (invariant **G12**).
    Passivated { node: NodeId, name: GrainName },
}
