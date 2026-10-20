//! The single-node `Local` journal (spec storage §3.2).
//!
//! An entity's records live in this node's [`EntityStore`](crate::store::EntityStore) and
//! each `append` commits immediately: one writer, one store, not fault-tolerant to
//! node loss. It adds no network and no nondeterminism, so the deterministic
//! simulator (§10) runs an entity's whole lifecycle under one seed.
//!
//! It never returns `NotLeader`: with a single store there is always a leader
//! (`NotLeader` arises on the clustered `Quorum` tier, `EntityJournal for
//! Arc<QuorumReplicator>` in replicator.rs). `Unavailable` it answers only when the
//! single store has failed (storage §11): a write it refused, or a read it can no longer
//! vouch for. Every method here writes and reads the [`EntityStore`](crate::store::EntityStore) seam this tier
//! shares with the clustered one; there is no durability machinery under it.

use crate::blobs::BlobId;
use crate::blocking::NodeStore;
use crate::entity::EntityName;
use crate::journal::AppendOutcome;
use crate::journal::EntityJournal;
use crate::journal::EntityJournalError;
use crate::journal::RecordPage;
use crate::journal::Seq;
use crate::journal::Term;
use crate::store::BlobAck;
use crate::store::StoreAck;
use crate::store::WriteKind;

/// The single-node `Local` journal (spec storage §3.2). Cloning shares one underlying store,
/// so every host spawned for the same shard writes to the same log.
#[derive(Clone)]
pub struct LocalEntityJournal {
    /// This node's store, its writes on the node's blocking pool (storage §3.2). On this tier
    /// the local fsync is the commit, so it is the entire cost of an append and the
    /// worst thing to run on an async worker (see [`crate::blocking`]).
    store: NodeStore,
    shard: u32,
}

impl LocalEntityJournal {
    /// A journal over a fresh, empty in-memory store for shard 0.
    #[cfg(test)]
    pub(crate) fn new() -> LocalEntityJournal {
        LocalEntityJournal::over(
            NodeStore::new(
                std::sync::Arc::new(crate::store::MemoryEntityStore::new()),
                std::sync::Arc::new(crate::blocking::InlineIo),
            ),
            0,
        )
    }

    /// A journal over `store`, keying its records under shard index `shard` (so a
    /// single node can back several shards from one store, storage §7).
    pub(crate) fn over(store: NodeStore, shard: u32) -> LocalEntityJournal {
        LocalEntityJournal { store, shard }
    }

    /// The storage §11 gate on every read: a failed store answers its reads as an entity
    /// with no history, and the only store there is cannot be outvoted, so the read
    /// is refused as `Unavailable` rather than served as an empty entity (§7:
    /// staleness is the budget, invention is not).
    fn usable(&self) -> Result<(), EntityJournalError> {
        if self.store.failed() {
            return Err(EntityJournalError::Unavailable(
                "local store unusable".into(),
            ));
        }
        Ok(())
    }
}

impl EntityJournal for LocalEntityJournal {
    async fn append(&self, entity: &EntityName, after: Seq, events: Vec<Vec<u8>>) -> AppendOutcome {
        // A single writer at term 0 is never fenced or stale (its fence stays 0 and
        // `after` always equals the head behind the input gate, §6). On this tier the
        // local fsync IS the commit (storage §3.2), so the await is the durability the
        // `Committed` outcome asserts.
        let stored = self
            .store
            .store_record(
                self.shard,
                entity,
                after,
                Term::ZERO,
                events,
                WriteKind::Append,
            )
            .await;
        match stored {
            StoreAck::Stored(head) => AppendOutcome::Committed(head),
            other => {
                AppendOutcome::Unavailable(format!("local store rejected the append: {other:?}"))
            }
        }
    }

    async fn load(
        &self,
        entity: &EntityName,
        from: Seq,
        limit: usize,
    ) -> Result<RecordPage, EntityJournalError> {
        self.usable()?;
        Ok(self.store.read_from(self.shard, entity, from, limit))
    }

    async fn head(&self, entity: &EntityName) -> Result<Seq, EntityJournalError> {
        self.usable()?;
        Ok(self.store.head(self.shard, entity))
    }

    fn term(&self) -> Option<Term> {
        // The single node always leads and never elects (storage §3.2), so the gateway
        // read's leadership token never expires here.
        Some(Term::ZERO)
    }

    async fn save_snapshot(&self, entity: &EntityName, at: Seq, state: Vec<u8>) -> AppendOutcome {
        let stored = self
            .store
            .store_snapshot(self.shard, entity, at, Term::ZERO, state, WriteKind::Append)
            .await;
        match stored {
            StoreAck::Stored(seq) => AppendOutcome::Committed(seq),
            other => {
                AppendOutcome::Unavailable(format!("local store rejected the snapshot: {other:?}"))
            }
        }
    }

    async fn load_snapshot(
        &self,
        entity: &EntityName,
    ) -> Result<Option<(Seq, Vec<u8>)>, EntityJournalError> {
        self.usable()?;
        Ok(self.store.snapshot(self.shard, entity))
    }

    // --- The entity-native content-addressed blob store (single-node) --------------

    async fn put_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
        bytes: Vec<u8>,
    ) -> Result<(), EntityJournalError> {
        match self.store.put_blob(self.shard, entity, id, bytes).await {
            BlobAck::Stored => Ok(()),
            // The single store IS the durability on this tier (storage §3.2), so a store that
            // could not write means the blob is not durable anywhere.
            BlobAck::Failed => Err(EntityJournalError::Unavailable(
                "local store could not persist the blob".into(),
            )),
        }
    }

    async fn get_blob(
        &self,
        entity: &EntityName,
        id: BlobId,
    ) -> Result<Option<Vec<u8>>, EntityJournalError> {
        // Verify the stored bytes against the id (B1): a single store can still suffer
        // on-disk bit-rot, which must surface as an error, never as wrong bytes.
        match self.store.get_blob(self.shard, entity, id) {
            Some(bytes) if id.verifies(&bytes) => Ok(Some(bytes)),
            Some(_) => Err(EntityJournalError::Unavailable(format!(
                "blob {id} failed verification"
            ))),
            None => Ok(None),
        }
    }

    async fn has_blob(&self, entity: &EntityName, id: BlobId) -> Result<bool, EntityJournalError> {
        Ok(self.store.has_blob(self.shard, entity, id))
    }

    async fn retain_blobs(&self, entity: &EntityName, retain: Vec<BlobId>) {
        self.store
            .retain_blobs(self.shard, entity, retain.into_iter().collect())
            .await;
    }

    async fn delete_blobs(&self, entity: &EntityName) {
        self.store.delete_blobs(self.shard, entity).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(key: &str) -> EntityName {
        EntityName::new("test.Entity", key)
    }

    /// A bare executor: the local journal's futures never yield, so polling once
    /// drives them to completion without a runtime.
    fn run<F: std::future::Future>(future: F) -> F::Output {
        use std::task::Context;
        use std::task::Poll;
        let mut future = Box::pin(future);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(out) => out,
            Poll::Pending => panic!("local journal future parked unexpectedly"),
        }
    }

    #[test]
    fn append_commits_at_sequential_heads() {
        let j = LocalEntityJournal::new();
        let n = name("a");
        assert_eq!(run(j.head(&n)), Ok(Seq::ZERO));
        assert_eq!(
            run(j.append(&n, Seq::ZERO, vec![b"e1".to_vec()])),
            AppendOutcome::Committed(Seq::new(1))
        );
        // A two-event batch is one atomic entry; the head jumps by the batch size.
        assert_eq!(
            run(j.append(&n, Seq::new(1), vec![b"e2".to_vec(), b"e3".to_vec()])),
            AppendOutcome::Committed(Seq::new(3))
        );
        assert_eq!(run(j.head(&n)), Ok(Seq::new(3)));
    }

    #[test]
    fn load_is_exclusive_of_from_and_bounded_by_limit() {
        let j = LocalEntityJournal::new();
        let n = name("a");
        run(j.append(
            &n,
            Seq::ZERO,
            vec![b"e1".to_vec(), b"e2".to_vec(), b"e3".to_vec()],
        ));
        assert_eq!(
            run(j.load(&n, Seq::ZERO, 10)),
            Ok(RecordPage {
                base: Seq::ZERO,
                records: vec![
                    (Seq::new(1), b"e1".to_vec()),
                    (Seq::new(2), b"e2".to_vec()),
                    (Seq::new(3), b"e3".to_vec()),
                ],
            })
        );
        assert_eq!(
            run(j.load(&n, Seq::new(1), 1)),
            Ok(RecordPage {
                base: Seq::ZERO,
                records: vec![(Seq::new(2), b"e2".to_vec())],
            })
        );
        assert_eq!(
            run(j.load(&n, Seq::new(3), 10)),
            Ok(RecordPage {
                base: Seq::ZERO,
                records: Vec::new(),
            })
        );
    }

    #[test]
    fn snapshot_round_trips_and_entities_are_independent() {
        let j = LocalEntityJournal::new();
        let a = name("a");
        let b = name("b");
        assert_eq!(run(j.load_snapshot(&a)), Ok(None));
        run(j.save_snapshot(&a, Seq::new(2), b"state-a".to_vec()));
        assert_eq!(
            run(j.load_snapshot(&a)),
            Ok(Some((Seq::new(2), b"state-a".to_vec())))
        );
        assert_eq!(run(j.head(&b)), Ok(Seq::ZERO));
        assert_eq!(run(j.load_snapshot(&b)), Ok(None));
    }

    /// storage §11 on the single-store tier: once the store has failed, every read is
    /// `Unavailable` rather than the empty entity the store's reads describe, so an
    /// activation over it refuses instead of rehydrating from nothing (§7).
    #[test]
    fn a_failed_store_makes_every_read_unavailable() {
        use crate::file_store::FileEntityStore;

        let dir = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(FileEntityStore::open(dir.path(), "json").expect("open"));
        let j = LocalEntityJournal::over(
            NodeStore::new(
                store.clone(),
                std::sync::Arc::new(crate::blocking::InlineIo),
            ),
            0,
        );
        let entity = name("served");
        assert_eq!(
            run(j.append(&entity, Seq::ZERO, vec![b"a".to_vec()])),
            AppendOutcome::Committed(Seq::new(1))
        );
        assert_eq!(run(j.head(&entity)), Ok(Seq::new(1)));

        // Poison the store as a failing device would: the segment area is gone, so
        // the next entity's segment file cannot be created.
        std::fs::remove_dir_all(dir.path().join("segments")).unwrap();
        assert!(matches!(
            run(j.append(&name("doomed"), Seq::ZERO, vec![b"b".to_vec()])),
            AppendOutcome::Unavailable(_)
        ));
        assert!(store.failure().is_some(), "poisoned by the failed write");

        // The served entity's own history is refused, not read back as empty.
        assert!(matches!(
            run(j.head(&entity)),
            Err(EntityJournalError::Unavailable(_))
        ));
        assert!(matches!(
            run(j.load(&entity, Seq::ZERO, 10)),
            Err(EntityJournalError::Unavailable(_))
        ));
        assert!(matches!(
            run(j.load_snapshot(&entity)),
            Err(EntityJournalError::Unavailable(_))
        ));
    }
}
