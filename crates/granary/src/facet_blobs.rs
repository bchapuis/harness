//! Shared blob machinery for the checkpointing facets (spec §7.12): the F3
//! root-keeping discipline and the chunked blob-area transfer helpers the SQL
//! (§7.14), workspace (§7.11), and disk (§7.15) facets have in common. Each
//! facet keeps its own chunking policy and manifest shape; what lives here is
//! the part that must be identical for GC safety to hold.

use std::collections::BTreeSet;

use crate::blobs::BlobId;
use crate::blobs::GrainBlobs;
use crate::facet::FacetError;

/// The blob ids a facet's activation must keep alive (**F3**): the restored
/// manifest's plus every later checkpoint's or capture's. The union is kept —
/// never pruned mid-activation — so a failed `save_snapshot` can never leave the
/// *current* durable manifest's blobs sweepable; the next activation restores
/// from the durable manifest and resets the set. Plain data: each facet guards
/// it with its own lock (the SQL and workspace facets a dedicated `Mutex`, the
/// disk facet its one state lock).
#[derive(Default)]
pub(crate) struct RootSet(BTreeSet<BlobId>);

impl RootSet {
    /// Adopt the durable manifest's ids wholesale — the restore path, the one
    /// place the union may shrink (a fresh activation starts from the durable
    /// truth, so nothing live is dropped). Only the SQL facet restores into a
    /// long-lived set; the workspace and disk facets build a fresh one.
    #[cfg(feature = "sql")]
    pub(crate) fn reset(&mut self, ids: impl IntoIterator<Item = BlobId>) {
        self.0 = ids.into_iter().collect();
    }

    /// Union in a checkpoint's or capture's ids (never prune, see the type doc).
    pub(crate) fn extend(&mut self, ids: impl IntoIterator<Item = BlobId>) {
        self.0.extend(ids);
    }

    /// The kept ids — the facet's [`Facet::roots`](crate::facet::Facet::roots)
    /// contribution.
    pub(crate) fn ids(&self) -> BTreeSet<BlobId> {
        self.0.clone()
    }
}

/// Store `chunks` in the grain's blob area, returning their ids in order. The
/// puts are independent and issue concurrently; dedup makes a chunk already
/// stored ~free (§7.10). `what` labels a failure (e.g. `"sql checkpoint"`).
pub(crate) async fn put_chunked(
    blobs: &GrainBlobs,
    chunks: Vec<Vec<u8>>,
    what: &str,
) -> Result<Vec<BlobId>, FacetError> {
    futures::future::try_join_all(chunks.into_iter().map(|chunk| blobs.put(chunk)))
        .await
        .map_err(|e| FacetError(format!("{what} put: {e:?}")))
}

/// Fetch `ids` from the grain's blob area and concatenate them in order — the
/// restore half of [`put_chunked`]. The gets are independent and issue
/// concurrently; each verifies by content (G17). The caller applies its own
/// length discipline to the result (the manifest, not the chunks, carries the
/// exact byte count).
pub(crate) async fn get_concat(
    blobs: &GrainBlobs,
    ids: &[BlobId],
    what: &str,
) -> Result<Vec<u8>, FacetError> {
    let parts = futures::future::try_join_all(ids.iter().map(|id| blobs.get(*id, None)))
        .await
        .map_err(|e| FacetError(format!("{what} get: {e:?}")))?;
    Ok(parts.concat())
}

/// The facet-I/O error shape (`"<facet> io: <cause>"`), curried for `map_err`:
/// `file.read(..).map_err(io_facet_err("sql"))`.
pub(crate) fn io_facet_err(facet: &'static str) -> impl Fn(std::io::Error) -> FacetError {
    move |e| FacetError(format!("{facet} io: {e}"))
}
