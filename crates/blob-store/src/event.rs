//! Blob-store observability events (spec §8, §9).
//!
//! The store emits its events on the actor framework's single extensible `Event`
//! stream (actor §16) as application events ([`Event::app`](actor_core::Event)),
//! exactly as granary emits `GrainEvent`: one totally-ordered stream interleaved
//! with the core actor events, so the seed-reproducibility contract covers blob
//! events for free, and the simulator's invariant checkers (spec §8, §9) read them
//! with [`Event::as_app::<BlobEvent>`](actor_core::Event::as_app). `BlobEvent`
//! qualifies as an `AppEvent` through the framework's blanket impl (it is
//! `Debug + Clone + PartialEq + Send + Sync + 'static`); no hand-written impl is
//! needed.

use actor_core::NodeId;

use crate::blob::BlobId;
use crate::blob::Namespace;

/// A blob-store event on the framework's `Event` stream (spec §8, §9).
///
/// The per-node variants ([`Stored`](BlobEvent::Stored),
/// [`Tombstoned`](BlobEvent::Tombstoned)) carry the `node` that acted, which is
/// what makes the safety invariants expressible as continuous checkers: a
/// `Stored` for a namespace a node has already `Tombstoned` would be a
/// resurrection (**B7**), and that ordering is only visible per node. The
/// coordinator variants record the outcome a caller observed
/// ([`PutAcked`](BlobEvent::PutAcked), [`GetVerified`](BlobEvent::GetVerified),
/// [`GetCorrupt`](BlobEvent::GetCorrupt)).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobEvent {
    /// A `put` reached its `W` durability target and returned the id (spec §5.2,
    /// **B3**) — emitted by the coordinating node.
    PutAcked { ns: Namespace, id: BlobId },
    /// A replica durably stored a blob (a `put` fan-out or a reconcile copy),
    /// or re-acknowledged one already present (**B2**). Never emitted for a store
    /// into a tombstoned namespace, which is refused (**B7**).
    Stored {
        node: NodeId,
        ns: Namespace,
        id: BlobId,
    },
    /// A `get` returned bytes that verified against the id (spec §4, **B1**).
    GetVerified { ns: Namespace, id: BlobId },
    /// A `get` found a copy but none verified — the data is corrupt, not merely
    /// absent (spec §4, §5.2, **B1**).
    GetCorrupt { ns: Namespace, id: BlobId },
    /// A node recorded a namespace tombstone and swept its local bytes (spec §5.3,
    /// **B7**). After this, that node refuses stores and resolves the namespace
    /// nowhere.
    Tombstoned { node: NodeId, ns: Namespace },
}
