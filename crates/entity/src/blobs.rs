//! The entity-native content-addressed blob store (durable-workspace design).
//!
//! Beside its ordered, term-fenced journal (§7.2), an entity node owns an
//! **immutable content-addressed store**: a per-entity blob area replicated to the
//! *same* shard replica set through the *same* [`ReplicaStore`](crate::replica_store)
//! actor, but with the term, order, and read-repair removed: content addressing
//! needs none of them. It is the colocated, zero-latency storage a Durable Object
//! keeps on the machine where it runs (DO §2.3): an entity stores its bulk bytes
//! here and references them by [`BlobId`] from the small foldable state in its
//! journal.
//!
//! Because the entity alone knows its own live id set, reclamation is a **per-blob
//! mark-from-roots sweep** ([`EntityBlobs::gc`]) plus a whole-area drop on destroy
//! ([`EntityBlobs::destroy`]).
//!
//! **A sweep and a put never interleave.** A writer puts first and roots later (plan,
//! then stage, §7.12): a facet's checkpoint puts its chunks and then extends its root
//! set, a KV spill is rooted only when its command commits. A sweep that read the
//! roots in between would delete bytes a manifest is about to reference, and the
//! writer, having cached the ids as stored, would never put them again. So every put
//! records its id in the activation's [`PendingRoots`] before the bytes leave, a
//! sweep retains those ids on top of the roots, and the two are ordered by one gate
//! (see [`PendingRoots`]).
//!
//! **Dedup saves bandwidth, not only space.** Content addressing was adopted so an
//! unchanged region of a checkpointed facet hashes to something already stored
//! (§7.10); on this deployment its larger effect is that the region is not
//! *sent*. A byte costs roughly fifty times more to move to a replica than to write
//! locally, and a replicated byte is billed once per peer, so a chunk that dedups is
//! the cheapest byte in the system (`docs/hardware-envelope.md` §3.9, I2).
//!
//! The saving does not come from [`put`](EntityBlobs::put), which does **not**
//! negotiate: it sends the bytes to every peer, and a holder writes nothing new. The
//! bandwidth is saved by the *caller* declining to put at all, having recognized the
//! content as one it already holds. Every writer into this area is built that way:
//! the workspace facet caches each file's chunk ids, the disk facet captures only
//! dirty blocks, and facet 0's state checks its own root set before putting a chunk
//! (§7.12). A future writer that skips that check gets the space saving and none of
//! the bandwidth one.

use std::collections::BTreeSet;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;

use serde::Deserialize;
use serde::Serialize;

use crate::entity::EntityName;
use crate::error::EntityError;
use crate::journal::DynEntityJournal;
use crate::journal::EntityJournalError;

/// The 32-byte BLAKE3 digest of a blob's bytes, its content address.
///
/// A `BlobId` is a pure function of the bytes: equal content yields the same id
/// wherever it is stored, so a writer and a reader agree on a blob's name with no
/// coordination, and a reader proves it received the right bytes by re-hashing them
/// (the read path verifies before returning). Rendered in lowercase hex for display.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BlobId([u8; 32]);

impl BlobId {
    /// The content id of `bytes`: `BLAKE3(bytes)`.
    pub fn of(bytes: &[u8]) -> BlobId {
        BlobId(*blake3::hash(bytes).as_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Wrap a raw 32-byte digest. The bytes it names are still verified against it
    /// on read, so a wrong id can never yield wrong bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> BlobId {
        BlobId(bytes)
    }

    /// Parse the hex form produced by [`Display`](fmt::Display) (64 lowercase hex
    /// chars) back into a [`BlobId`], or `None` if it is not that shape.
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

    /// Whether `bytes` hash to this id: the read-path integrity check (B1/E17),
    /// named once so every fetch site re-hashes the same way.
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

/// The blobs an activation has put that its roots may not show yet, and the gate that
/// orders its puts against its sweeps (spec §7.12, **F3**). One per activation, shared
/// by every [`EntityBlobs`] handle the activation builds.
///
/// **Why both halves.** A put that lands while a sweep is in flight is deleted if the
/// sweep's retain set was read before the put's root became visible, and a writer's
/// root becomes visible only after its put returns (a checkpoint extends its root set
/// once its chunks are durable; a KV spill is rooted at its command's commit). The
/// set closes that window: a put records its id *before* its bytes leave, and every
/// sweep retains the set on top of the roots. The gate closes the remaining one: a
/// sweep holds it from reading its retain set until every replica has answered, and a
/// put records its id under it, so a put either is recorded before the sweep reads
/// (and is retained) or starts only after the sweep is done (and lands after it).
///
/// **When an id leaves.** At the start of each command ([`start_turn`](Self::start_turn)),
/// because by then the previous turn is over: its commit folded its records, so a KV
/// spill is in the committed map, its captures and snapshots extended their facets'
/// root sets, and anything it put that no root names belongs to a commit that failed,
/// an orphan the next sweep may reclaim. A blob put in the current command therefore
/// survives a sweep made in that same command.
///
/// The ids cover puts through this activation's handles. A blob an application puts
/// from a task it detached is the application's to root, or to pass in `live`, before
/// it sweeps.
#[derive(Default)]
pub(crate) struct PendingRoots {
    /// Held by a sweep across its whole run, taken briefly by a put to record its id.
    /// An async lock, so a waiting put or sweep yields its worker instead of blocking it.
    gate: futures::lock::Mutex<()>,
    ids: Mutex<BTreeSet<BlobId>>,
}

impl PendingRoots {
    /// Record `id` as put but perhaps not yet rooted. Waits out a sweep in flight.
    async fn record(&self, id: BlobId) {
        let _gate = self.gate.lock().await;
        self.ids.lock().expect("pending roots lock").insert(id);
    }

    /// Forget the previous turn's puts: called as a command begins (see the type docs).
    pub(crate) fn start_turn(&self) {
        self.ids.lock().expect("pending roots lock").clear();
    }
}

/// An entity-scoped handle to its content-addressed blob area, returned by
/// [`EntityCtx::blobs`](crate::EntityCtx::blobs).
///
/// Every method addresses *this entity's* blobs only; the journal seam underneath
/// routes to the entity's shard replicas (colocated with the activation, so a
/// [`get`](EntityBlobs::get) is a local read in steady state). Cheap to clone (an
/// `Arc` and a name).
#[derive(Clone)]
pub struct EntityBlobs {
    journal: Arc<dyn DynEntityJournal>,
    entity: EntityName,
    /// The facets' live blob roots (spec §7.12), unioned into every
    /// [`gc`](EntityBlobs::gc) sweep by the handle itself, so an application's
    /// mark-from-roots sweep can never drop a facet's bytes (F3). Empty for an
    /// entity with no facets.
    facet_roots: Arc<dyn Fn() -> BTreeSet<BlobId> + Send + Sync>,
    /// The activation's puts not yet known to be rooted, and the gate ordering puts
    /// against sweeps ([`PendingRoots`]).
    pending: Arc<PendingRoots>,
}

impl EntityBlobs {
    pub(crate) fn new(
        journal: Arc<dyn DynEntityJournal>,
        entity: EntityName,
        pending: Arc<PendingRoots>,
    ) -> EntityBlobs {
        EntityBlobs {
            journal,
            entity,
            facet_roots: Arc::new(BTreeSet::new),
            pending,
        }
    }

    pub(crate) fn entity(&self) -> &EntityName {
        &self.entity
    }

    /// Re-replicate every facet-root blob to the entity's current shard replicas
    /// (spec §7.10): source a verifying copy of each (E17) and re-`put` it,
    /// re-fanning the bytes to the current replica set; holders dedup (a
    /// no-op), a replica that lacked it receives it. Root-driven, additive, and
    /// idempotent; best-effort (a blob no reachable replica holds is skipped:
    /// loss repair cannot undo). Kick it as a background task off the
    /// activation latency path, like [`gc`](EntityBlobs::gc).
    pub async fn repair_facets(&self) {
        for id in (self.facet_roots)() {
            if let Ok(bytes) = self.get(id, None).await {
                let _ = self.put(bytes).await;
            }
        }
    }

    /// Attach the facet root supplier (spec §7.12). Built by
    /// [`EntityCtx::blobs`](crate::EntityCtx::blobs); the roots are computed at
    /// sweep time, so they always reflect the *committed* forms.
    pub(crate) fn with_facet_roots(
        mut self,
        roots: Arc<dyn Fn() -> BTreeSet<BlobId> + Send + Sync>,
    ) -> EntityBlobs {
        self.facet_roots = roots;
        self
    }

    /// Store `bytes` and return their content id. Idempotent and dedup'd: storing
    /// content already present re-acknowledges and writes nothing new (the id is a
    /// pure function of the bytes). On the `Quorum` tier the bytes are durable on a
    /// write quorum of the entity's replicas before this returns, always including
    /// this leader, so a later [`get`](EntityBlobs::get) reads locally; if a quorum
    /// is unreachable it is [`EntityError::Unavailable`] and the caller retries (a
    /// retry carries no double-write risk).
    pub async fn put(&self, bytes: Vec<u8>) -> Result<BlobId, EntityError> {
        let id = BlobId::of(&bytes);
        // Before the bytes go anywhere: from here until the next command begins, no
        // sweep of this activation drops the id (see `PendingRoots`).
        self.pending.record(id).await;
        self.journal
            .put_blob(&self.entity, id, bytes)
            .await
            .map_err(into_entity_error)?;
        Ok(id)
    }

    /// Fetch `id`, or a byte range of it (`None` = the whole blob). The returned
    /// bytes are verified against `id` by the seam before return: an absent or
    /// irrecoverably-corrupt blob is [`EntityError::Unavailable`], never wrong bytes.
    /// A ranged request is served by obtaining and verifying the whole blob, then
    /// slicing. Not implemented: streaming a range.
    pub async fn get(&self, id: BlobId, range: Option<Range<u64>>) -> Result<Vec<u8>, EntityError> {
        let bytes = self
            .journal
            .get_blob(&self.entity, id)
            .await
            .map_err(into_entity_error)?
            .ok_or_else(|| EntityError::Unavailable(format!("blob {id} not found")))?;
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

    /// Whether `id` is present on **any reachable replica** of this entity's blob area
    /// (the local copy, else the first peer that holds it: short-circuit, not a quorum
    /// count). A `true` answers "a [`get`](EntityBlobs::get) can source these bytes"; it
    /// is not a durability assertion, since a blob is made quorum-durable at
    /// [`put`](EntityBlobs::put) time, not measured here.
    pub async fn has(&self, id: BlobId) -> Result<bool, EntityError> {
        self.journal
            .has_blob(&self.entity, id)
            .await
            .map_err(into_entity_error)
    }

    /// Drop every blob of this entity not in `live` ∪ the facets' roots (F3): a
    /// mark-from-roots sweep the entity drives from its own metadata (the ids its
    /// state still references), reclaiming blocks orphaned by overwrites without any
    /// cluster-wide reference tracking. Best-effort and idempotent (so it cannot
    /// fail: a missed replica keeps its garbage until the next sweep).
    ///
    /// Safe to run beside the activation's own commands and snapshots, detached or
    /// not: a blob put through this activation since its current or latest command
    /// began is kept too, whether or not a root names it yet, and no put starts while
    /// the sweep runs; it waits for the sweep to finish (see [`PendingRoots`]).
    pub async fn gc(&self, live: &BTreeSet<BlobId>) {
        // Held until every replica has answered: a put recorded after the retain set
        // is read must not land before the sweep that missed it.
        let _gate = self.pending.gate.lock().await;
        let mut retain: Vec<BlobId> = live.iter().copied().collect();
        retain.extend((self.facet_roots)());
        retain.extend(
            self.pending
                .ids
                .lock()
                .expect("pending roots lock")
                .iter()
                .copied(),
        );
        self.journal.retain_blobs(&self.entity, retain).await;
    }

    /// Drop **all** of this entity's blobs: the entity-scoped reclamation on destroy
    /// (no namespace tombstone, no membership gating; the area lives only on the
    /// entity's known replicas). Best-effort and idempotent (so it cannot fail).
    pub async fn destroy(&self) {
        self.journal.delete_blobs(&self.entity).await;
    }
}

/// The seam reports only `Unavailable` (a quorum could not be reached, or every
/// reachable copy failed verification); the entity surfaces it as the same
/// durability outcome a write pause uses (§11).
fn into_entity_error(err: EntityJournalError) -> EntityError {
    match err {
        EntityJournalError::Unavailable(why) => EntityError::Unavailable(why),
    }
}

#[cfg(test)]
mod tests {
    use std::task::Context;
    use std::task::Poll;

    use futures::FutureExt;

    use super::*;
    use crate::blocking::BlockingIo;
    use crate::blocking::DeferredIo;
    use crate::blocking::InlineIo;
    use crate::blocking::NodeStore;
    use crate::memory::LocalEntityJournal;
    use crate::store::EntityStore;
    use crate::store::MemoryEntityStore;

    /// One activation's blob handle over a single in-memory store, with a facet root
    /// set the test extends by hand, the way a checkpoint does after its puts.
    struct Activation {
        store: Arc<MemoryEntityStore>,
        pending: Arc<PendingRoots>,
        facet_roots: Arc<Mutex<BTreeSet<BlobId>>>,
        blobs: EntityBlobs,
    }

    impl Activation {
        fn over(io: Arc<dyn BlockingIo>) -> Activation {
            let store = Arc::new(MemoryEntityStore::new());
            let journal = LocalEntityJournal::over(NodeStore::new(store.clone(), io), 0);
            let pending = Arc::new(PendingRoots::default());
            let facet_roots = Arc::new(Mutex::new(BTreeSet::new()));
            let roots = Arc::clone(&facet_roots);
            let blobs = EntityBlobs::new(
                Arc::new(journal),
                EntityName::new("t", "k"),
                Arc::clone(&pending),
            )
            .with_facet_roots(Arc::new(move || roots.lock().unwrap().clone()));
            Activation {
                store,
                pending,
                facet_roots,
                blobs,
            }
        }

        fn holds(&self, id: BlobId) -> bool {
            self.store.has_blob(0, self.blobs.entity(), id)
        }
    }

    fn poll<F: std::future::Future + Unpin>(future: &mut F) -> Poll<F::Output> {
        future.poll_unpin(&mut Context::from_waker(futures::task::noop_waker_ref()))
    }

    /// The race a detached sweep runs into: a checkpoint puts its chunk, the sweep
    /// reads the roots before the checkpoint extends them, and the sweep would then
    /// delete the chunk the manifest is about to reference.
    #[test]
    fn a_sweep_between_a_put_and_its_root_keeps_the_blob() {
        let act = Activation::over(Arc::new(InlineIo));
        let chunk = futures::executor::block_on(act.blobs.put(b"chunk".to_vec())).unwrap();

        // The facet has not rooted the chunk yet, and the application's live set is
        // empty: only the pending record stands between the chunk and the sweep.
        futures::executor::block_on(act.blobs.gc(&BTreeSet::new()));
        assert!(
            act.holds(chunk),
            "a sweep deleted a blob put before its root was visible",
        );

        // The checkpoint roots it; an orphan from a failed command does not get rooted.
        act.facet_roots.lock().unwrap().insert(chunk);
        let orphan = futures::executor::block_on(act.blobs.put(b"orphan".to_vec())).unwrap();
        // The next command begins: the previous turn's puts are rooted or orphaned.
        act.pending.start_turn();
        futures::executor::block_on(act.blobs.gc(&BTreeSet::new()));
        assert!(act.holds(chunk), "a rooted blob survives");
        assert!(
            !act.holds(orphan),
            "an orphan of an earlier turn is reclaimed"
        );
    }

    /// The other half: a put cannot land inside a sweep that has already read its
    /// retain set. The device is held so the sweep is observably in flight, and the
    /// put must not even reach the store until the sweep has finished.
    #[test]
    fn a_put_waits_for_a_sweep_in_flight() {
        let io = Arc::new(DeferredIo::default());
        let act = Activation::over(io.clone());
        let live = BTreeSet::new();

        let mut sweep = Box::pin(act.blobs.gc(&live));
        assert!(
            poll(&mut sweep).is_pending(),
            "the sweep waits on the device"
        );
        assert_eq!(io.submitted(), 1, "the sweep's store call is queued");

        let mut put = Box::pin(act.blobs.put(b"late".to_vec()));
        assert!(poll(&mut put).is_pending());
        assert_eq!(
            io.submitted(),
            1,
            "a put started while a sweep that never saw it was in flight",
        );

        io.run_queued();
        assert!(poll(&mut sweep).is_ready());
        assert!(
            poll(&mut put).is_pending(),
            "the put now waits on the device"
        );
        assert_eq!(io.submitted(), 2);
        io.run_queued();
        let Poll::Ready(Ok(late)) = poll(&mut put) else {
            panic!("the put did not complete");
        };
        assert!(act.holds(late));

        // And a sweep in the same turn keeps it, rooted or not.
        let mut again = Box::pin(act.blobs.gc(&live));
        assert!(poll(&mut again).is_pending());
        io.run_queued();
        assert!(poll(&mut again).is_ready());
        assert!(act.holds(late));
    }

    #[test]
    fn id_is_the_blake3_digest_and_dedups_equal_content() {
        // Equal content addresses one blob; different content, different ids (E18).
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
        assert_eq!(BlobId::from_bytes(*id.as_bytes()), id);
        let hex = id.to_string();
        assert_eq!(hex.len(), 64);
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert_eq!(format!("{id:?}"), format!("BlobId({hex})"));
    }
}
