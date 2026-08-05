//! A namespaced, content-addressed object store on the actor framework
//! (blob-store spec).
//!
//! The crate sits beside granary, not above it (spec §1): a blob store is built
//! from plain cluster actors and needs none of the grain machinery — no virtual
//! identity, no journal, no single-activation lease. A blob is an immutable byte
//! string named by the BLAKE3 hash of its content **within a consumer-chosen
//! namespace** (spec §2). A content hash names exactly one byte sequence for all
//! time, so durability and deletion need no consensus: there is nothing to order
//! and nothing to agree on (spec §4). The namespace is the unit of deletion
//! (spec §5.3), so storage is reclaimed by deleting a namespace, not by
//! reference-tracking individual blobs.
//!
//! Two tiers sit behind one [`BlobStore`] seam (or its object-safe
//! [`DynBlobStore`] mirror): a single-node on-disk store (`Local`) and a
//! clustered replicate-by-hash store (`Clustered`). They differ only in *where
//! the bytes live*.

use std::io;
use std::path::Path;

pub mod blob;
pub mod cluster;
#[cfg(test)]
mod corpus;
pub mod event;
pub mod local;
pub mod placement;
pub mod reconcile;
pub mod replica;
pub mod system;
pub mod tombstone;

pub use blob::{BlobConfig, BlobError, BlobId, BlobStore, DynBlobStore, Namespace, verify};
pub use cluster::ClusteredBlobStore;
pub use event::BlobEvent;
pub use local::LocalBlobStore;
pub use replica::{
    ActorBlobTransport, BlobReplica, BlobTransport, DeleteAck, DeleteNamespace, FetchBlob, HasBlob,
    StoreAck, StoreBlob, blob_replica_key,
};
pub use system::BlobSystem;
pub use tombstone::{AnchorTracker, Tombstone, TombstoneSet};

/// Open (creating if absent) the single-node, on-disk `Local` tier rooted at
/// `path` (spec §5.1) — the embedded, test, and simulator tier. The
/// free-function spelling of Appendix A's `BlobStore::local`.
pub fn local(path: impl AsRef<Path>) -> io::Result<LocalBlobStore> {
    LocalBlobStore::open(path.as_ref())
}

/// Bring up the fault-tolerant `Clustered` tier on `system` with this node's
/// on-disk `local` store (spec §5.2): replicate-by-hash with a `W`-of-`R` put,
/// verified rank-order read, namespace deletion, and a background reconcile loop.
/// `local` is passed in (one store per node), so the deterministic simulator can
/// give each node its own directory (spec §8).
pub fn clustered<S: BlobSystem>(
    system: S,
    config: BlobConfig,
    local: LocalBlobStore,
) -> ClusteredBlobStore<S> {
    ClusteredBlobStore::start(system, config, local)
}
