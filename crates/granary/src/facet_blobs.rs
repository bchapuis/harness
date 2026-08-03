//! Shared blob machinery for the checkpointing facets (spec §7.12): the F3
//! root-keeping discipline and the chunked blob-area transfer helpers the SQL
//! (§7.14), workspace (§7.11), and disk (§7.15) facets have in common. Each
//! facet keeps its own chunking policy and manifest shape; what lives here is
//! the part that must be identical for GC safety to hold.

use std::collections::BTreeSet;

use futures::StreamExt;

use crate::blobs::BlobId;
use crate::blobs::GrainBlobs;
use crate::facet::FacetError;

/// The blob ids a facet's activation must keep alive (**F3**): the restored
/// manifest's plus every later checkpoint's or capture's. The union is kept —
/// never pruned mid-activation — so a failed `save_snapshot` can never leave the
/// *current* durable manifest's blobs sweepable; the next activation restores
/// from the durable manifest and resets the set. Plain data: each facet guards
/// it with its own lock.
#[derive(Default)]
pub(crate) struct RootSet(BTreeSet<BlobId>);

impl RootSet {
    /// Adopt the durable manifest's ids wholesale — the restore path, the one
    /// place the union may shrink (a fresh activation starts from the durable
    /// truth, so nothing live is dropped).
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

/// How many chunk transfers a facet keeps in flight at once.
///
/// The transfers are not free-floating work: each is a quorum round carrying a whole
/// chunk to every replica, so the bytes in flight are roughly
/// `IN_FLIGHT_CHUNKS × chunk size × replicas`. Unbounded, a large artifact issues one
/// per chunk — a 16 GiB disk image at 1 MiB blocks is over sixteen thousand
/// concurrent quorum puts — and the resulting buffers and sockets are what fails,
/// long before the device does. The point of a bound is that past the width that
/// keeps the link busy, more concurrency adds queueing rather than throughput, so
/// this costs nothing on small artifacts and is the difference between working and
/// OOM on large ones.
const IN_FLIGHT_CHUNKS: usize = 16;

/// Await every future in `work` with at most [`IN_FLIGHT_CHUNKS`] outstanding,
/// returning their values **in the original order**.
///
/// Every future is polled to completion even after one fails — the reason
/// `join_all_results` was chosen over `try_join_all` here. A short-circuiting
/// combinator drops the transfers still in flight and abandons the peer `ask`s they
/// had already issued (core spec §18.5 #1); the transfers are independent and
/// content-addressed, so finishing them costs nothing and leaves more of the
/// checkpoint durable. Buffering preserves that: it only declines to *start* work,
/// which is not the same as abandoning work already issued.
async fn bounded_in_order<F, T>(work: Vec<F>, what: &str) -> Result<Vec<T>, FacetError>
where
    F: std::future::Future<Output = Result<T, crate::error::GrainError>>,
{
    // Tagged with its position, because completion order is not submission order.
    let mut done: Vec<(usize, Result<T, crate::error::GrainError>)> = futures::stream::iter(
        work.into_iter()
            .enumerate()
            .map(|(i, f)| async move { (i, f.await) }),
    )
    .buffer_unordered(IN_FLIGHT_CHUNKS)
    .collect()
    .await;
    done.sort_by_key(|(i, _)| *i);
    done.into_iter()
        .map(|(_, r)| r.map_err(|e| FacetError(format!("{what}: {e:?}"))))
        .collect()
}

/// Store `chunks` in the grain's blob area, returning their ids in order. The puts
/// are independent and issue concurrently, [`IN_FLIGHT_CHUNKS`] at a time; dedup
/// makes a chunk already stored ~free (§7.10). `what` labels a failure (e.g.
/// `"sql checkpoint"`).
///
/// Takes the whole artifact up front, which suits a facet whose artifact is already
/// in memory and bounded (the workspace tree's 64 MiB cap, a SQL checkpoint's
/// serialized pages). A facet whose artifact is not — the disk facet's 16 GiB image
/// — wants [`put_pulled`], which is the same transfer with the chunks produced as
/// slots free rather than collected first.
pub(crate) async fn put_chunked(
    blobs: &GrainBlobs,
    chunks: Vec<Vec<u8>>,
    what: &str,
) -> Result<Vec<BlobId>, FacetError> {
    put_pulled(blobs, chunks.into_iter().map(Ok), what).await
}

/// Store chunks **pulled on demand** in the grain's blob area, returning their ids
/// in the order `chunks` yielded them. [`IN_FLIGHT_CHUNKS`] transfers run at once,
/// exactly as in [`put_chunked`]; what differs is where the artifact lives.
///
/// `chunks` is advanced only when a transfer slot frees, so peak memory is
/// `IN_FLIGHT_CHUNKS` chunks whatever the artifact's total size, and a source that
/// reads from a file paces itself against the puts instead of racing ahead of them.
/// That is what lets the disk facet (§7.15) pipeline a 16 GiB image's blocks without
/// materializing it: the alternative — collect every block, then hand the vector to
/// [`put_chunked`] — trades the serialized round trips for an allocation the facet's
/// whole point is to avoid.
///
/// A chunk the source fails to produce ends the pull: the error is returned, and no
/// *further* chunk is asked for. That is the same discipline [`bounded_in_order`]
/// applies to the transfers themselves, read from the other side — declining to
/// start work is allowed, abandoning work already issued is not, so the transfers
/// already in flight still run to completion.
pub(crate) async fn put_pulled<I>(
    blobs: &GrainBlobs,
    chunks: I,
    what: &str,
) -> Result<Vec<BlobId>, FacetError>
where
    I: Iterator<Item = Result<Vec<u8>, FacetError>>,
{
    // Checks the flag *before* pulling, which `Iterator::scan` cannot: after a
    // source failure the next chunk is never asked for, not asked for and dropped.
    let mut chunks = chunks;
    let mut stopped = false;
    let source = std::iter::from_fn(move || {
        if stopped {
            return None;
        }
        let chunk = chunks.next()?;
        stopped = chunk.is_err();
        Some(chunk)
    });

    // The same tag-buffer-sort as [`bounded_in_order`], repeated rather than shared
    // because that one takes its work already collected. Folding the two together
    // needs an iterator of futures borrowing `blobs`, and inference will not prove
    // such a closure general enough over the borrow's lifetime.
    //
    // `buffer_unordered` polls its inner stream only while it is under capacity, and
    // `stream::iter` pulls one iterator item per poll — which is what makes the
    // source demand-driven rather than eager, and so bounds the memory.
    let mut done: Vec<(usize, Result<BlobId, FacetError>)> =
        futures::stream::iter(source.enumerate())
            .map(|(i, chunk)| async move {
                let put = match chunk {
                    Ok(bytes) => blobs
                        .put(bytes)
                        .await
                        .map_err(|e| FacetError(format!("{what} put: {e:?}"))),
                    Err(source) => Err(source),
                };
                (i, put)
            })
            .buffer_unordered(IN_FLIGHT_CHUNKS)
            .collect()
            .await;
    done.sort_by_key(|(i, _)| *i);
    done.into_iter().map(|(_, r)| r).collect()
}

/// Fetch `ids` from the grain's blob area and concatenate them in order — the
/// restore half of [`put_chunked`]. The gets issue [`IN_FLIGHT_CHUNKS`] at a time and
/// each verifies by content (G17). The caller applies its own length discipline to
/// the result (the manifest, not the chunks, carries the exact byte count).
///
/// Note this materializes the whole artifact in memory, so it suits facets whose
/// manifests are themselves bounded (the workspace tree's 64 MiB cap). A facet
/// restoring something larger should write chunk by chunk into its target instead,
/// as the disk facet's `apply_manifest` does.
pub(crate) async fn get_concat(
    blobs: &GrainBlobs,
    ids: &[BlobId],
    what: &str,
) -> Result<Vec<u8>, FacetError> {
    let work: Vec<_> = ids.iter().map(|id| blobs.get(*id, None)).collect();
    let parts = bounded_in_order(work, &format!("{what} get")).await?;
    Ok(parts.concat())
}

/// The facet-I/O error shape (`"<facet> io: <cause>"`), curried for `map_err`:
/// `file.read(..).map_err(io_facet_err("sql"))`.
pub(crate) fn io_facet_err(facet: &'static str) -> impl Fn(std::io::Error) -> FacetError {
    move |e| FacetError(format!("{facet} io: {e}"))
}
