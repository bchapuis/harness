//! The blob model and the [`BlobStore`] seam (spec §2, §3, §4).
//!
//! A **blob** is an immutable, finite byte string; its content names it and a
//! **namespace** scopes that name (spec §2). The address of a blob is the pair
//! `(Namespace, BlobId)`: the [`BlobId`] is a pure function of the bytes and is
//! identical across namespaces, while the [`Namespace`] selects *which copy* a
//! `get` reads and *which owners* hold it. This module owns the types every tier
//! shares and the single read-path verification chokepoint ([`verify`], B1), so
//! corruption and misdelivery are detectable at the point of use, never silent.

use std::fmt;
use std::future::Future;
use std::ops::Range;

use actor_core::CallError;
use serde::Deserialize;
use serde::Serialize;

/// The 32-byte BLAKE3 digest of a blob's bytes — its content address (spec §2).
///
/// A `BlobId` is a pure function of the bytes: the same content yields the same
/// id wherever and however it is stored, so a writer and a reader agree on a
/// blob's name with no coordination, and a reader proves it received the right
/// bytes by re-hashing them ([`verify`], B1). BLAKE3 is chosen over SHA-256
/// because every `get` re-hashes its bytes to verify them, so hashing throughput
/// sits on the read path, not only the write path (spec §2).
///
/// Rendered in lowercase hex for display.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BlobId([u8; 32]);

impl BlobId {
    /// The content id of `bytes`: `BLAKE3(bytes)`. Identical for equal content
    /// regardless of tier or namespace (spec §2, §3).
    pub fn of(bytes: &[u8]) -> BlobId {
        BlobId(*blake3::hash(bytes).as_bytes())
    }

    /// The raw 32-byte digest.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Wrap a raw 32-byte digest. Used when an id arrives off the wire or is
    /// reconstructed from storage; the bytes it names are still verified against
    /// it on read ([`verify`], B1), so a wrong id cannot yield wrong bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> BlobId {
        BlobId(bytes)
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The hex digest already identifies the blob; a `BlobId(..)` wrapper around
        // 32 raw bytes would only add noise to a panic or a log line.
        write!(f, "BlobId({self})")
    }
}

/// An opaque, consumer-chosen identifier that scopes a blob's content id and is
/// the **unit of deletion** (spec §2, §5.3).
///
/// A namespace carries no meaning to the store beyond grouping blobs that share a
/// lifecycle (a tenant, a workspace, a snapshot-set): every blob is stored under
/// exactly one namespace, and `delete_namespace` removes all of them. A namespace
/// is **single-use**: a consumer MUST NOT reuse an id after deleting it (the
/// motivating filesystem grain mints a fresh UUID), so a delete tombstone can
/// never be confused with a later recreation.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Namespace(Vec<u8>);

impl Namespace {
    /// Wrap an opaque identifier (a short byte string: a UUID, a tenant id).
    pub fn new(id: impl Into<Vec<u8>>) -> Namespace {
        Namespace(id.into())
    }

    /// The opaque identifier's bytes — hashed into the rendezvous key that places
    /// a blob's owners (spec §5.2), never into the [`BlobId`].
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Render as hex: a namespace is opaque bytes (often a UUID), not
        // necessarily UTF-8, and the hex form is also the on-disk directory name
        // for the `Local` tier (spec §5.1).
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Namespace({self})")
    }
}

/// A failure to store, fetch, or reclaim a blob (spec §3, Appendix A).
///
/// Like the grain error model (granary §12), this carries only the failures of
/// *reaching and committing*, kept distinct from a blob's bytes: a `get` either
/// returns verified bytes or one of these, never wrong bytes (B1). The variants
/// are exhaustive by design — a caller handles every real partial failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobError {
    /// Could not reach `W` copies on `put`, or any owner on `get` (spec §5.2).
    /// On `put` the blob MAY be partially stored; a retry is safe and idempotent,
    /// because the id is a pure function of the bytes (spec §3, B2).
    Unavailable(String),
    /// A copy was found but none verified against the requested id (spec §4). The
    /// id names the bytes the caller asked for; this means every reachable copy
    /// failed that check, so the data is lost rather than silently wrong.
    Corrupt(BlobId),
    /// The target namespace has been deleted (spec §5.3). Monotonic: once a
    /// namespace is gone it stays gone, so this never reverts to a live blob.
    Deleted(Namespace),
    /// An underlying actor transport or system failure (actor §14.1).
    Transport(CallError),
}

impl From<CallError> for BlobError {
    fn from(err: CallError) -> Self {
        BlobError::Transport(err)
    }
}

impl fmt::Display for BlobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlobError::Unavailable(why) => write!(f, "blob store unavailable: {why}"),
            BlobError::Corrupt(id) => write!(f, "no verifying copy of blob {id}"),
            BlobError::Deleted(ns) => write!(f, "namespace {ns} has been deleted"),
            BlobError::Transport(e) => write!(f, "blob transport failed: {e}"),
        }
    }
}

impl std::error::Error for BlobError {}

/// Deployment knobs for the `Clustered` tier (spec §5.2, Appendix A).
///
/// `W` and `R` are independent durability and availability knobs, not a
/// correctness constraint: with immutable content there is no write-quorum ∩
/// read-quorum requirement (spec §5.2, contrast granary §8). The `Local` tier
/// ignores the replication knobs (one node, one durable copy).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobConfig {
    /// `R`: the number of owner nodes a blob is replicated to (spec §5.2).
    pub replication_factor: usize,
    /// `W ≤ R`: the number of stored copies before a `put` returns durable.
    pub write_quorum: usize,
    /// An upper bound on one blob's size; a consumer chunks beyond it (spec §2).
    pub max_blob_bytes: usize,
}

/// Re-hash `bytes` and compare to `id` — the read-path integrity check (spec §4,
/// **B1**).
///
/// Every `get` MUST call this before returning bytes, *after* any network
/// transfer, so corruption and misdelivery are caught at the point of use. This
/// is the blob store's analogue of `wal`'s torn-tail rejection (wal §3.1),
/// strengthened from a checksum to a cryptographic digest because the bytes may
/// have crossed the network. On mismatch the caller MUST NOT return the bytes: on
/// the clustered tier it tries the next owner; if none verify it returns
/// [`BlobError::Corrupt`].
pub fn verify(id: &BlobId, bytes: &[u8]) -> Result<(), BlobError> {
    if BlobId::of(bytes) == *id {
        Ok(())
    } else {
        Err(BlobError::Corrupt(*id))
    }
}

/// Slice an already-verified whole blob to `range`, clamped to the blob's length
/// so an out-of-bounds range yields the in-bounds intersection rather than a panic
/// (efficient range streaming is deferred, spec §2, §10). A blob is obtained and
/// verified whole, then sliced — on both tiers — so verification always covers the
/// id's full preimage (**B1**).
pub(crate) fn slice(bytes: Vec<u8>, range: Option<Range<u64>>) -> Vec<u8> {
    match range {
        None => bytes,
        Some(range) => {
            let len = bytes.len() as u64;
            let end = range.end.min(len) as usize;
            let start = range.start.min(end as u64) as usize;
            bytes[start..end].to_vec()
        }
    }
}

/// A content-addressed store for immutable blobs, scoped by namespace (spec §3).
///
/// This is the simulation and deployment seam, like `GrainJournal`, `Transport`,
/// and `Clock` (granary §7.3, actor §4.6): the [`Local`](crate) and `Clustered`
/// tiers are two implementations of it, satisfying the contract identically and
/// differing only in where the bytes live. The trait is codec-agnostic — it moves
/// raw bytes, and the [`BlobId`] is computed from those bytes, so no serialization
/// format leaks across the seam.
///
/// `Clone` (like `GrainJournal`) so the object-safe [`DynBlobStore`] mirror can
/// adapt any implementation into `'static` boxed futures; tiers wrap an
/// `Arc<Inner>`, so cloning is cheap.
pub trait BlobStore: Clone + Send + Sync + 'static {
    /// Store `bytes` under `ns` and return its content id. Idempotent and dedup'd
    /// within the namespace (**B2**): storing content already present in `ns`
    /// re-acknowledges and writes nothing new. Storing into a deleted namespace is
    /// an error ([`BlobError::Deleted`]); namespaces are single-use (spec §2).
    /// Returns [`BlobError::Unavailable`] if the durability target (spec §5.2)
    /// could not be met, in which case the blob MAY or MAY NOT be partially
    /// stored, and the caller retries (the id is a pure function of the bytes, so
    /// a retry carries no double-write risk).
    fn put(
        &self,
        ns: &Namespace,
        bytes: Vec<u8>,
    ) -> impl Future<Output = Result<BlobId, BlobError>> + Send;

    /// Fetch `(ns, id)`, or a byte range of it (`None` = the whole blob). The
    /// returned bytes are verified against `id` before return (spec §4, **B1**):
    /// an absent or corrupt blob is an error, never wrong bytes. A node that knows
    /// `ns` is deleted returns [`BlobError::Deleted`] (spec §5.3); a node not yet
    /// aware of the tombstone may still serve the real bytes until it learns of it
    /// (B7 liveness). A ranged request is served by obtaining and verifying the
    /// whole blob, then slicing (spec §2); efficient range streaming is deferred.
    fn get(
        &self,
        ns: &Namespace,
        id: &BlobId,
        range: Option<Range<u64>>,
    ) -> impl Future<Output = Result<Vec<u8>, BlobError>> + Send;

    /// Whether `(ns, id)` is durably present: at least `W` copies on the
    /// `Clustered` tier (spec §5.2), one durable copy on `Local`. A namespace
    /// known to be deleted reports `false`.
    fn has(
        &self,
        ns: &Namespace,
        id: &BlobId,
    ) -> impl Future<Output = Result<bool, BlobError>> + Send;

    /// Reclaim an entire namespace: every blob stored under `ns` becomes
    /// permanently unresolvable (spec §5.3). Idempotent and monotonic: a
    /// namespace, once deleted, stays deleted, and re-deleting is a no-op. Returns
    /// once the tombstone is durably anchored (`W` of the namespace's `R`
    /// tombstone owners, spec §5.3), after which it cannot be lost and is
    /// disseminated to the rest of the cluster; from that point no surviving or
    /// rejoining copy can resurrect a blob of `ns`. The bytes are swept in the
    /// background.
    fn delete_namespace(
        &self,
        ns: &Namespace,
    ) -> impl Future<Output = Result<(), BlobError>> + Send;
}

/// The boxed result of [`DynBlobStore::put`].
pub type PutFuture = BoxFuture<Result<BlobId, BlobError>>;
/// The boxed result of [`DynBlobStore::get`].
pub type GetFuture = BoxFuture<Result<Vec<u8>, BlobError>>;
/// The boxed result of [`DynBlobStore::has`].
pub type HasFuture = BoxFuture<Result<bool, BlobError>>;
/// The boxed result of [`DynBlobStore::delete_namespace`].
pub type DeleteFuture = BoxFuture<Result<(), BlobError>>;

type BoxFuture<T> = actor_core::BoxFuture<'static, T>;

/// The object-safe form of [`BlobStore`], so a consumer can hold a store as
/// `Arc<dyn DynBlobStore>` and **select the durability tier at construction**
/// without threading a tier type parameter through its own code — mirroring
/// `DynGrainJournal` (granary §7.3).
///
/// [`BlobStore`]'s `impl Future` returns are not object-safe; this mirror boxes
/// them ([`BoxFuture`](actor_core::BoxFuture)). The blanket impl below adapts any
/// [`BlobStore`]: it clones the store (cheap — tiers wrap an `Arc`) and the
/// namespace/id into each boxed future, so the returned future is `'static` and
/// the caller borrows nothing.
pub trait DynBlobStore: Send + Sync + 'static {
    /// See [`BlobStore::put`].
    fn put(&self, ns: &Namespace, bytes: Vec<u8>) -> PutFuture;
    /// See [`BlobStore::get`].
    fn get(&self, ns: &Namespace, id: &BlobId, range: Option<Range<u64>>) -> GetFuture;
    /// See [`BlobStore::has`].
    fn has(&self, ns: &Namespace, id: &BlobId) -> HasFuture;
    /// See [`BlobStore::delete_namespace`].
    fn delete_namespace(&self, ns: &Namespace) -> DeleteFuture;
}

impl<B: BlobStore> DynBlobStore for B {
    fn put(&self, ns: &Namespace, bytes: Vec<u8>) -> PutFuture {
        let store = self.clone();
        let ns = ns.clone();
        Box::pin(async move { store.put(&ns, bytes).await })
    }

    fn get(&self, ns: &Namespace, id: &BlobId, range: Option<Range<u64>>) -> GetFuture {
        let store = self.clone();
        let ns = ns.clone();
        let id = *id;
        Box::pin(async move { store.get(&ns, &id, range).await })
    }

    fn has(&self, ns: &Namespace, id: &BlobId) -> HasFuture {
        let store = self.clone();
        let ns = ns.clone();
        let id = *id;
        Box::pin(async move { store.has(&ns, &id).await })
    }

    fn delete_namespace(&self, ns: &Namespace) -> DeleteFuture {
        let store = self.clone();
        let ns = ns.clone();
        Box::pin(async move { store.delete_namespace(&ns).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_is_the_blake3_digest_and_dedups_equal_content() {
        // The id is a pure function of the bytes (spec §2): equal content, equal
        // id; different content, different id.
        let a = BlobId::of(b"the quick brown fox");
        let b = BlobId::of(b"the quick brown fox");
        let c = BlobId::of(b"the quick brown cat");
        assert_eq!(a, b, "equal content must yield one id (B2)");
        assert_ne!(a, c, "different content must differ");
        // Matches a known BLAKE3 vector: the digest of the empty input.
        assert_eq!(
            BlobId::of(b"").to_string(),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
        );
    }

    #[test]
    fn id_round_trips_through_hex_and_raw_bytes() {
        let id = BlobId::of(b"content");
        assert_eq!(id.to_string().len(), 64, "32 bytes render as 64 hex chars");
        assert_eq!(BlobId::from_bytes(*id.as_bytes()), id);
        assert_eq!(format!("{id:?}"), format!("BlobId({id})"));
    }

    #[test]
    fn verify_accepts_matching_bytes_and_rejects_tampering() {
        // B1: verification is the read-path chokepoint. Matching bytes pass;
        // a single flipped bit is reported as Corrupt against the requested id.
        let bytes = b"a bounded block of bytes".to_vec();
        let id = BlobId::of(&bytes);
        assert_eq!(verify(&id, &bytes), Ok(()));

        let mut tampered = bytes.clone();
        tampered[0] ^= 0x01;
        assert_eq!(verify(&id, &tampered), Err(BlobError::Corrupt(id)));
    }

    #[test]
    fn namespace_is_opaque_and_renders_as_hex() {
        let ns = Namespace::new(*b"\x00\xffworkspace");
        assert_eq!(ns.as_bytes(), b"\x00\xffworkspace");
        assert!(ns.to_string().starts_with("00ff"));
        assert_eq!(Namespace::new(b"x".to_vec()), Namespace::new(*b"x"));
    }

    #[test]
    fn errors_display_without_leaking_bytes() {
        // BlobError carries only reach/commit failures, never blob bytes.
        let id = BlobId::of(b"z");
        assert_eq!(
            BlobError::Corrupt(id).to_string(),
            format!("no verifying copy of blob {id}"),
        );
        assert!(
            BlobError::Deleted(Namespace::new(b"ns".to_vec()))
                .to_string()
                .contains("has been deleted")
        );
    }
}
