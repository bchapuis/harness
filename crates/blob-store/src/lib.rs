//! A namespaced, content-addressed object store on the actor framework
//! (blob-store spec).
//!
//! The crate sits **beside** granary, not above it (spec §1): a blob store is
//! built from plain cluster actors, because it needs none of the grain
//! machinery — no virtual identity, no journal, no single-activation lease. A
//! blob is an immutable byte string named by the BLAKE3 hash of its content
//! **within a consumer-chosen namespace** (spec §2). Immutable content needs
//! durability and deletion *without consensus*: a content hash names exactly one
//! byte sequence for all time, so there is nothing to order and nothing to agree
//! on (spec §4). The namespace is the unit of deletion (spec §5.3), so storage is
//! reclaimed by deleting a namespace, not by reference-tracking individual blobs.
//!
//! Two tiers sit behind one [`BlobStore`] seam: a single-node on-disk store
//! (`Local`) and a clustered replicate-by-hash store (`Clustered`). The seam and
//! the blob model are stable; the tiers differ only in *where the bytes live*.
//!
//! This module tree follows the spec's Appendix B layout. The two tiers are
//! constructed through [`local`] and [`clustered`] and used behind the
//! [`BlobStore`] seam (or its object-safe [`DynBlobStore`] mirror).

use std::io;
use std::path::Path;

pub mod blob;
pub mod cluster;
pub mod event;
pub mod local;
pub mod placement;
pub mod reconcile;
pub mod replica;
pub mod system;
pub mod tombstone;

pub use blob::{verify, BlobConfig, BlobError, BlobId, BlobStore, DynBlobStore, Namespace};
pub use cluster::ClusteredBlobStore;
pub use event::BlobEvent;
pub use local::LocalBlobStore;
pub use replica::{
    blob_replica_key, ActorBlobTransport, BlobReplica, BlobTransport, DeleteAck, DeleteNamespace,
    FetchBlob, HasBlob, StoreAck, StoreBlob,
};
pub use system::BlobSystem;
pub use tombstone::{AnchorTracker, Tombstone, TombstoneSet};

/// Open (creating if absent) the single-node, on-disk `Local` tier rooted at
/// `path` (spec §5.1) — the embedded, test, and simulator tier. The free-function
/// spelling of Appendix A's `BlobStore::local`, a free function because
/// [`BlobStore`] is a trait, not a type.
pub fn local(path: impl AsRef<Path>) -> io::Result<LocalBlobStore> {
    LocalBlobStore::open(path.as_ref())
}

/// Bring up the fault-tolerant `Clustered` tier on `system` with this node's
/// on-disk `local` store (spec §5.2): replicate-by-hash with a `W`-of-`R` put,
/// verified rank-order read, namespace deletion, and a background reconcile loop.
/// The free-function spelling of Appendix A's `BlobStore::clustered`; `local` is
/// passed in (one store per node) so the tier stays agnostic to where bytes live
/// on disk, which lets the deterministic simulator give each node its own
/// directory (spec §8).
pub fn clustered<S: BlobSystem>(
    system: S,
    config: BlobConfig,
    local: LocalBlobStore,
) -> ClusteredBlobStore<S> {
    ClusteredBlobStore::start(system, config, local)
}
