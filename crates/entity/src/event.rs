//! Entity and shard observability events (spec §13).
//!
//! The entity crate emits its events on the actor framework's single extensible `Event`
//! stream (actor §16) as an application event ([`Event::app`](actor_core::Event)),
//! so an entity run produces one totally-ordered stream interleaved with the core
//! actor events and the simulator's invariant checkers (§14) read them with
//! [`Event::as_app::<EntityEvent>`](actor_core::Event::as_app). `EntityEvent`
//! qualifies as an `AppEvent` through the framework's blanket impl; no
//! hand-written impl is needed.

use actor_core::NodeId;

use crate::entity::EntityName;

/// An entity-lifecycle or shard event on the framework's `Event` stream (spec §13).
///
/// Every entity variant carries the `node` that hosts the activation (its shard's
/// leader, §5.2). The node is what makes the per-node activation guarantee
/// (**E6**, "exactly-once activation *per node*") expressible as a continuous
/// checker over the stream: without it, an activation that migrates on failover
/// is indistinguishable from a second live activation of the same name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntityEvent {
    /// An activation rehydrated and is about to serve its first command (§10).
    Activated { node: NodeId, name: EntityName },
    /// An activation rebuilt its state from the journal (§9): `from_snapshot`
    /// records whether a snapshot seeded the replay, `replayed` how many events
    /// were folded after it.
    Rehydrated {
        node: NodeId,
        name: EntityName,
        from_snapshot: bool,
        replayed: u64,
    },
    /// A command's events committed at the entity's new head (§6, §7). The reply
    /// is released only after this (the output gate, invariant **E5**). Carries
    /// the `shard` the activation serves under: across a split, a moved entity's
    /// `shard` transitions parent→child exactly once, with `seq` still strictly
    /// increasing (**E15**).
    Committed {
        node: NodeId,
        name: EntityName,
        shard: u32,
        seq: u64,
    },
    /// A snapshot was persisted at a committed seq (§9).
    Snapshotted {
        node: NodeId,
        name: EntityName,
        at: u64,
    },
    /// An activation ended and dropped its in-memory state: idle hibernation
    /// (§10) or a forced step-down on a leadership move or quorum loss (§8,
    /// §11). The journal survives either, so the next message rehydrates it
    /// (invariant **E12**).
    Deactivated { node: NodeId, name: EntityName },
    /// This node won a shard's leader election at `term` (§8, §13). Emitted by
    /// the new leader itself, once per term it observes, so the stream carries
    /// at most one per (shard, term) from the node that actually serves.
    LeaderChanged {
        node: NodeId,
        entity_type: &'static str,
        shard: u32,
        term: u64,
    },
    /// A committed shard split was applied on `node` (§7.7): the parent's range
    /// now ends just below `boundary` and the fresh `child` owns `[boundary, ..]`
    /// with its own leader-election group. Emitted by every node as it applies
    /// the commit (dedup by `(parent, child)` when counting splits).
    ShardSplit {
        node: NodeId,
        entity_type: &'static str,
        parent: u32,
        child: u32,
        boundary: u64,
    },
    /// A committed shard merge was applied on `node` (§7.7): `left` absorbed the
    /// adjacent `right` shard's range and entities; `right` is retired and its
    /// leader-election group reclaimed (E7). Emitted by every node as it applies
    /// the commit.
    ShardMerged {
        node: NodeId,
        entity_type: &'static str,
        left: u32,
        right: u32,
    },
}
