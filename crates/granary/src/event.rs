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
//! This enum carries the grain-lifecycle events and the shard events of §13:
//! `LeaderChanged` (emitted by the node that wins a shard's election, once per
//! term it observes), and `ShardSplit`/`ShardMerged` (emitted by each node as it
//! applies the committed partition change, §7.7). The split/merge events plus
//! the `shard` on `Committed` are what let the simulator's checkers verify G15
//! — a grain writable in exactly one shard — over the ordered stream.

use actor_core::NodeId;

use crate::grain::GrainName;

/// A grain-lifecycle or shard event on the framework's `Event` stream (spec §13).
///
/// Every grain variant carries the `node` that hosts the activation (its shard's
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
    /// is released only after this (the output gate, invariant **G5**). Carries
    /// the `shard` the activation serves under: the G15 checker's hook — across
    /// a split, a moved grain's `shard` transitions parent→child exactly once,
    /// with `seq` still strictly increasing.
    Committed {
        node: NodeId,
        name: GrainName,
        shard: u32,
        seq: u64,
    },
    /// A snapshot was persisted at a committed seq (§9).
    Snapshotted {
        node: NodeId,
        name: GrainName,
        at: u64,
    },
    /// An activation hibernated on idle and dropped its in-memory state (§10);
    /// the journal survives, so the next message rehydrates it (invariant **G12**).
    Passivated { node: NodeId, name: GrainName },
    /// This node won a shard's leader election at `term` (§8, §13). Emitted by
    /// the new leader itself, once per term it observes, so the stream carries
    /// at most one per (shard, term) from the node that actually serves.
    LeaderChanged {
        node: NodeId,
        grain_type: &'static str,
        shard: u32,
        term: u64,
    },
    /// A committed shard split was applied on `node` (§7.7): the parent's range
    /// now ends just below `boundary` and the fresh `child` owns `[boundary, ..]`
    /// with its own leader-election group. Emitted by every node as it applies
    /// the commit (dedup by `(parent, child)` when counting splits).
    ShardSplit {
        node: NodeId,
        grain_type: &'static str,
        parent: u32,
        child: u32,
        boundary: u64,
    },
    /// A committed shard merge was applied on `node` (§7.7): `left` absorbed the
    /// adjacent `right` shard's range and grains; `right` is retired and its
    /// leader-election group reclaimed (G7). Emitted by every node as it applies
    /// the commit.
    ShardMerged {
        node: NodeId,
        grain_type: &'static str,
        left: u32,
        right: u32,
    },
}
