//! The grain-native content-addressed blob store (durable-workspace design).
//!
//! Beside its ordered, term-fenced journal (§7.2), a grain node owns an
//! **immutable content-addressed store** — a per-grain blob area replicated to the
//! *same* shard replica set through the *same* [`ReplicaStore`](crate::replica_store)
//! actor, but with the term, order, and read-repair removed: content addressing
//! needs none of them, so this is the journal's durability half with its hard half
//! gone (the same subset relationship the `blob-store` crate's replica draws to
//! granary's). It is the colocated, zero-latency storage a Durable Object keeps on
//! the machine where it runs (DO §2.3): a grain stores its bulk bytes here and
//! references them by [`BlobId`] from the small foldable state in its journal.
//!
//! The grain reaches it through [`GrainCtx::blobs`](crate::GrainCtx::blobs), which
//! returns a [`GrainBlobs`] scoped to the grain. Because the grain knows its own
//! live id set, reclamation is a **per-blob mark-from-roots sweep**
//! ([`GrainBlobs::gc`]) — something a liveness-blind shared blob store cannot do —
//! plus a whole-area drop on destroy ([`GrainBlobs::destroy`]).

use std::collections::BTreeSet;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::error::GrainError;
use crate::grain::GrainName;
use crate::journal::DynGrainJournal;
use crate::journal::GrainJournalError;

/// The 32-byte BLAKE3 digest of a blob's bytes — its content address.
///
/// A `BlobId` is a pure function of the bytes: equal content yields the same id
/// wherever it is stored, so a writer and a reader agree on a blob's name with no
/// coordination, and a reader proves it received the right bytes by re-hashing them
/// (the read path verifies before returning). BLAKE3 is chosen over SHA-256 because
/// every fetch re-hashes to verify, so hashing throughput sits on the read path.
///
/// Granary defines its own id rather than depending on `blob-store`, which sits
/// *beside* granary, not under it; the digest is the same BLAKE3 root either way.
/// Rendered in lowercase hex for display.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BlobId([u8; 32]);

impl BlobId {
    /// The content id of `bytes`: `BLAKE3(bytes)`.
    pub fn of(bytes: &[u8]) -> BlobId {
        BlobId(*blake3::hash(bytes).as_bytes())
    }

    /// The raw 32-byte digest.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Wrap a raw 32-byte digest. The bytes it names are still verified against it
    /// on read, so a wrong id can never yield wrong bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> BlobId {
        BlobId(bytes)
    }

    /// Parse the hex form produced by [`Display`](fmt::Display) — 64 lowercase hex
    /// chars — back into a [`BlobId`], or `None` if it is not that shape. Kept beside
    /// the `Display` impl so the encoding lives in one place.
    pub fn from_hex(hex: &str) -> Option<BlobId> {
        if hex.len() != 64 {
            return None;
        }
        let mut bytes = [0u8; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok()?;
        }
        Some(BlobId(bytes))
    }

    /// Whether `bytes` hash to this id — the read-path integrity check (B1/G17),
    /// named once so every fetch site re-hashes the same way and none can forget it.
    pub fn verifies(&self, bytes: &[u8]) -> bool {
        BlobId::of(bytes) == *self
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
        write!(f, "BlobId({self})")
    }
}

/// A grain-scoped handle to its content-addressed blob area, returned by
/// [`GrainCtx::blobs`](crate::GrainCtx::blobs).
///
/// Every method addresses *this grain's* blobs only; the journal seam underneath
/// routes to the grain's shard replicas (colocated with the activation, so a
/// [`get`](GrainBlobs::get) is a local read in steady state). Cheap to clone (an
/// `Arc` and a name).
#[derive(Clone)]
pub struct GrainBlobs {
    journal: Arc<dyn DynGrainJournal>,
    grain: GrainName,
    /// The facets' live blob roots (spec §7.12), unioned into every
    /// [`gc`](GrainBlobs::gc) sweep by the handle itself — so an application's
    /// mark-from-roots sweep can never drop a facet's bytes (F3). Empty for a
    /// grain with no facets.
    facet_roots: Arc<dyn Fn() -> BTreeSet<BlobId> + Send + Sync>,
}

impl GrainBlobs {
    pub(crate) fn new(journal: Arc<dyn DynGrainJournal>, grain: GrainName) -> GrainBlobs {
        GrainBlobs {
            journal,
            grain,
            facet_roots: Arc::new(BTreeSet::new),
        }
    }

    /// The grain this handle is scoped to.
    pub(crate) fn grain(&self) -> &GrainName {
        &self.grain
    }

    /// Re-replicate every facet-root blob to the grain's current shard replicas
    /// (spec §7.10 B6): source a verifying copy of each (G17) and re-`put` it,
    /// re-fanning the bytes to the current replica set — holders dedup (a
    /// no-op), a replica that lacked it receives it. Root-driven, additive, and
    /// idempotent; best-effort (a blob no reachable replica holds is skipped —
    /// loss repair cannot undo). Kick it as a background task off the
    /// activation latency path, like [`gc`](GrainBlobs::gc).
    pub async fn repair_facets(&self) {
        for id in (self.facet_roots)() {
            if let Ok(bytes) = self.get(id, None).await {
                let _ = self.put(bytes).await;
            }
        }
    }

    /// Attach the facet root supplier (spec §7.12). Built by
    /// [`GrainCtx::blobs`](crate::GrainCtx::blobs); the roots are computed at
    /// sweep time, so they always reflect the *committed* forms.
    pub(crate) fn with_facet_roots(
        mut self,
        roots: Arc<dyn Fn() -> BTreeSet<BlobId> + Send + Sync>,
    ) -> GrainBlobs {
        self.facet_roots = roots;
        self
    }

    /// Store `bytes` and return their content id. Idempotent and dedup'd: storing
    /// content already present re-acknowledges and writes nothing new (the id is a
    /// pure function of the bytes). On the `Quorum` tier the bytes are durable on a
    /// write quorum of the grain's replicas — always including this leader, so a
    /// later [`get`](GrainBlobs::get) reads locally — before this returns; if a
    /// quorum is unreachable it is [`GrainError::Unavailable`] and the caller
    /// retries (a retry carries no double-write risk).
    pub async fn put(&self, bytes: Vec<u8>) -> Result<BlobId, GrainError> {
        let id = BlobId::of(&bytes);
        self.journal
            .put_blob(&self.grain, id, bytes)
            .await
            .map_err(into_grain_error)?;
        Ok(id)
    }

    /// Fetch `id`, or a byte range of it (`None` = the whole blob). The returned
    /// bytes are verified against `id` by the seam before return: an absent or
    /// irrecoverably-corrupt blob is [`GrainError::Unavailable`], never wrong bytes.
    /// A ranged request is served by obtaining and verifying the whole blob, then
    /// slicing — efficient range streaming is a later refinement.
    pub async fn get(&self, id: BlobId, range: Option<Range<u64>>) -> Result<Vec<u8>, GrainError> {
        let bytes = self
            .journal
            .get_blob(&self.grain, id)
            .await
            .map_err(into_grain_error)?
            .ok_or_else(|| GrainError::Unavailable(format!("blob {id} not found")))?;
        Ok(match range {
            None => bytes,
            Some(range) => {
                let len = bytes.len() as u64;
                let start = range.start.min(len) as usize;
                let end = range.end.min(len) as usize;
                bytes[start..end.max(start)].to_vec()
            }
        })
    }

    /// Whether `id` is present on **any reachable replica** of this grain's blob area
    /// (the local copy, else the first peer that holds it — short-circuit, not a quorum
    /// count). A `true` answers "a [`get`](GrainBlobs::get) can source these bytes"; it
    /// is not a durability assertion, since a blob is made quorum-durable at
    /// [`put`](GrainBlobs::put) time, not measured here.
    pub async fn has(&self, id: BlobId) -> Result<bool, GrainError> {
        self.journal
            .has_blob(&self.grain, id)
            .await
            .map_err(into_grain_error)
    }

    /// Drop every blob of this grain **not** in `live` — a mark-from-roots sweep the
    /// grain drives from its own metadata (the ids its state still references). The
    /// grain alone knows liveness, so this reclaims blocks orphaned by overwrites
    /// without any cluster-wide reference tracking. Best-effort and idempotent (so
    /// it cannot fail — a missed replica keeps its garbage until the next sweep).
    pub async fn gc(&self, live: &BTreeSet<BlobId>) {
        // The retained set is the caller's roots ∪ the facets' roots (spec
        // §7.12): only the host-built handle knows the facets, so the union
        // lives here, not in any one facet or in application code.
        let mut retain: Vec<BlobId> = live.iter().copied().collect();
        retain.extend((self.facet_roots)());
        self.journal.retain_blobs(&self.grain, retain).await;
    }

    /// Drop **all** of this grain's blobs — the grain-scoped reclamation on destroy
    /// (§ no namespace tombstone, no membership gating: the area lives only on the
    /// grain's known replicas). Best-effort and idempotent (so it cannot fail).
    pub async fn destroy(&self) {
        self.journal.delete_blobs(&self.grain).await;
    }
}

/// Map a seam-level blob failure onto the grain error model. The seam reports only
/// `Unavailable` (a quorum could not be reached, or every reachable copy failed
/// verification); the grain surfaces it as the same durability outcome a write pause
/// uses (§11).
fn into_grain_error(err: GrainJournalError) -> GrainError {
    match err {
        GrainJournalError::Unavailable(why) => GrainError::Unavailable(why),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_is_the_blake3_digest_and_dedups_equal_content() {
        // Equal content addresses one blob; different content, different ids (B2).
        assert_eq!(BlobId::of(b"abc"), BlobId::of(b"abc"));
        assert_ne!(BlobId::of(b"abc"), BlobId::of(b"abd"));
        assert_eq!(
            BlobId::of(b"abc"),
            BlobId::from_bytes(*blake3::hash(b"abc").as_bytes())
        );
    }

    #[test]
    fn id_round_trips_through_hex_and_raw_bytes() {
        let id = BlobId::of(b"some bytes");
        // The raw digest round-trips.
        assert_eq!(BlobId::from_bytes(*id.as_bytes()), id);
        // Display is a 64-char lowercase hex string.
        let hex = id.to_string();
        assert_eq!(hex.len(), 64);
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert_eq!(format!("{id:?}"), format!("BlobId({hex})"));
    }
}
