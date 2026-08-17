//! The single-node `Local` journal (spec §7.4).
//!
//! A grain's records live in this node's [`GrainStore`](crate::store::GrainStore) and
//! each `append` commits immediately: one writer, one store, not fault-tolerant to
//! node loss. It adds no network and no nondeterminism, so the deterministic
//! simulator (§14) runs a grain's whole lifecycle under one seed.
//!
//! It never returns `NotLeader` or `Unavailable`: with a single store there is always
//! a leader and always a quorum (those outcomes arise on the clustered
//! [`QuorumGrainJournal`](crate::shard::QuorumGrainJournal)). Every method here is the
//! whole of what this tier does — it writes and reads the [`GrainStore`] seam it
//! shares with the clustered tier, and there is no durability machinery under it to
//! hold apart.

use std::sync::Arc;

use crate::blobs::BlobId;
use crate::blocking::BlockingIo;
use crate::blocking::on_store;
use crate::grain::GrainName;
use crate::journal::AppendOutcome;
use crate::journal::GrainJournal;
use crate::journal::GrainJournalError;
use crate::journal::Seq;
use crate::journal::Term;
use crate::store::BlobAck;
use crate::store::GrainStore;
use crate::store::MemoryGrainStore;
use crate::store::StoreAck;
use crate::store::WriteKind;

/// The single-node `Local` journal (spec §7.4). Cloning shares one underlying store,
/// so every host spawned for the same shard writes to the same log.
#[derive(Clone)]
pub struct LocalGrainJournal {
    store: Arc<dyn GrainStore>,
    shard: u32,
    /// Where the store's blocking writes run (§7.4). On this tier the local fsync is
    /// the commit, so it is the entire cost of an append.
    io: Arc<dyn BlockingIo>,
}

impl LocalGrainJournal {
    /// A journal over a fresh, empty in-memory store for shard 0.
    pub fn new() -> LocalGrainJournal {
        LocalGrainJournal::over(
            Arc::new(MemoryGrainStore::new()),
            0,
            Arc::new(crate::InlineIo),
        )
    }

    /// A journal over `store`, keying its records under shard index `shard` (so a
    /// single node can back several shards from one store, §7.6).
    /// `io` is where the store's blocking writes run (§7.4): the single-node tier's
    /// fsync IS its commit, so it is the whole latency of an append and the worst
    /// thing to run on an async worker (see [`crate::blocking`]).
    pub(crate) fn over(
        store: Arc<dyn GrainStore>,
        shard: u32,
        io: Arc<dyn BlockingIo>,
    ) -> LocalGrainJournal {
        LocalGrainJournal { store, shard, io }
    }
}

impl Default for LocalGrainJournal {
    fn default() -> Self {
        LocalGrainJournal::new()
    }
}

impl GrainJournal for LocalGrainJournal {
    async fn append(&self, grain: &GrainName, after: Seq, events: Vec<Vec<u8>>) -> AppendOutcome {
        // A single writer at term 0 is never fenced or stale (its fence stays 0 and
        // `after` always equals the head behind the input gate, §6). On this tier the
        // local fsync IS the commit (§7.4), so the await is the durability the
        // `Committed` outcome asserts.
        let (name, shard) = (grain.clone(), self.shard);
        let stored = on_store(&self.io, &self.store, move |store| {
            store.store_record(shard, &name, after, Term::ZERO, events, WriteKind::Append)
        })
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
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<(Seq, Vec<u8>)>, GrainJournalError> {
        Ok(self.store.read_from(self.shard, grain, from, limit))
    }

    async fn head(&self, grain: &GrainName) -> Result<Seq, GrainJournalError> {
        Ok(self.store.head(self.shard, grain))
    }

    fn term(&self) -> Option<Term> {
        // The single node always leads and never elects (§7.4), so the gateway
        // read's leadership token never expires here.
        Some(Term::ZERO)
    }

    async fn save_snapshot(&self, grain: &GrainName, at: Seq, state: Vec<u8>) -> AppendOutcome {
        let (name, shard) = (grain.clone(), self.shard);
        let stored = on_store(&self.io, &self.store, move |store| {
            store.store_snapshot(shard, &name, at, Term::ZERO, state, WriteKind::Append)
        })
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
        grain: &GrainName,
    ) -> Result<Option<(Seq, Vec<u8>)>, GrainJournalError> {
        Ok(self.store.snapshot(self.shard, grain))
    }

    // --- The grain-native content-addressed blob store (single-node) --------------

    async fn put_blob(
        &self,
        grain: &GrainName,
        id: BlobId,
        bytes: Vec<u8>,
    ) -> Result<(), GrainJournalError> {
        let (name, shard) = (grain.clone(), self.shard);
        match on_store(&self.io, &self.store, move |store| {
            store.put_blob(shard, &name, id, bytes)
        })
        .await
        {
            BlobAck::Stored => Ok(()),
            // The single store IS the durability on this tier (§7.4), so a store that
            // could not write means the blob is not durable anywhere.
            BlobAck::Failed => Err(GrainJournalError::Unavailable(
                "local store could not persist the blob".into(),
            )),
        }
    }

    async fn get_blob(
        &self,
        grain: &GrainName,
        id: BlobId,
    ) -> Result<Option<Vec<u8>>, GrainJournalError> {
        // Verify the stored bytes against the id (B1): a single store can still suffer
        // on-disk bit-rot, which must surface as an error, never as wrong bytes.
        match self.store.get_blob(self.shard, grain, id) {
            Some(bytes) if id.verifies(&bytes) => Ok(Some(bytes)),
            Some(_) => Err(GrainJournalError::Unavailable(format!(
                "blob {id} failed verification"
            ))),
            None => Ok(None),
        }
    }

    async fn has_blob(&self, grain: &GrainName, id: BlobId) -> Result<bool, GrainJournalError> {
        Ok(self.store.has_blob(self.shard, grain, id))
    }

    async fn retain_blobs(&self, grain: &GrainName, retain: Vec<BlobId>) {
        let (name, shard) = (grain.clone(), self.shard);
        on_store(&self.io, &self.store, move |store| {
            store.retain_blobs(shard, &name, &retain.into_iter().collect())
        })
        .await;
    }

    async fn delete_blobs(&self, grain: &GrainName) {
        let (name, shard) = (grain.clone(), self.shard);
        on_store(&self.io, &self.store, move |store| {
            store.delete_blobs(shard, &name)
        })
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(key: &str) -> GrainName {
        GrainName::new("test.Grain", key)
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
        let j = LocalGrainJournal::new();
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
        let j = LocalGrainJournal::new();
        let n = name("a");
        run(j.append(
            &n,
            Seq::ZERO,
            vec![b"e1".to_vec(), b"e2".to_vec(), b"e3".to_vec()],
        ));
        assert_eq!(
            run(j.load(&n, Seq::ZERO, 10)),
            Ok(vec![
                (Seq::new(1), b"e1".to_vec()),
                (Seq::new(2), b"e2".to_vec()),
                (Seq::new(3), b"e3".to_vec()),
            ])
        );
        assert_eq!(
            run(j.load(&n, Seq::new(1), 1)),
            Ok(vec![(Seq::new(2), b"e2".to_vec())])
        );
        assert_eq!(run(j.load(&n, Seq::new(3), 10)), Ok(Vec::new()));
    }

    #[test]
    fn snapshot_round_trips_and_grains_are_independent() {
        let j = LocalGrainJournal::new();
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
}
