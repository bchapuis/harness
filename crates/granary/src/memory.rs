//! The single-node `Local` journal (spec §7.4).
//!
//! One linearizable local store: a grain's records live in this node's
//! [`GrainStore`](crate::store::GrainStore), each `append` "commits" immediately (the
//! single-node analogue of a quorum append). CP trivially — one writer, one store —
//! but not fault-tolerant to node loss. Its sweet spot is embedded use, tests, and
//! the deterministic simulator (§14): it adds no network and no nondeterminism, so a
//! grain's whole lifecycle runs under one seed.
//!
//! It never returns `NotLeader` or `Unavailable`: with a single store there is always
//! a leader and always a quorum (those outcomes arise on the clustered
//! [`QuorumGrainJournal`](crate::shard::QuorumGrainJournal)). It is a thin
//! [`GrainJournal`] over a [`LocalReplicator`], which shares the [`GrainStore`] seam
//! with the clustered tier.

use std::sync::Arc;

use crate::grain::GrainName;
use crate::journal::AppendOutcome;
use crate::journal::GrainJournal;
use crate::journal::GrainJournalError;
use crate::journal::Seq;
use crate::replicator::LocalReplicator;
use crate::store::GrainStore;
use crate::store::MemoryGrainStore;

/// The single-node `Local` journal (spec §7.4). Cloning shares one underlying store,
/// so every host spawned for the same shard writes to the same log.
#[derive(Clone)]
pub struct LocalGrainJournal {
    replicator: Arc<LocalReplicator>,
}

impl LocalGrainJournal {
    /// A journal over a fresh, empty in-memory store for shard 0.
    pub fn new() -> LocalGrainJournal {
        LocalGrainJournal::over(Arc::new(MemoryGrainStore::new()), 0)
    }

    /// A journal over `store`, keying its records under shard index `shard` (so a
    /// single node can back several shards from one store, §7.6).
    pub(crate) fn over(store: Arc<dyn GrainStore>, shard: u32) -> LocalGrainJournal {
        LocalGrainJournal {
            replicator: Arc::new(LocalReplicator::new(store, shard)),
        }
    }
}

impl Default for LocalGrainJournal {
    fn default() -> Self {
        LocalGrainJournal::new()
    }
}

impl GrainJournal for LocalGrainJournal {
    async fn append(&self, grain: &GrainName, after: Seq, events: Vec<Vec<u8>>) -> AppendOutcome {
        self.replicator.append(grain, after, events).await
    }

    async fn load(
        &self,
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<(Seq, Vec<u8>)>, GrainJournalError> {
        self.replicator.load(grain, from, limit).await
    }

    async fn head(&self, grain: &GrainName) -> Result<Seq, GrainJournalError> {
        self.replicator.head(grain).await
    }

    async fn save_snapshot(&self, grain: &GrainName, at: Seq, state: Vec<u8>) -> AppendOutcome {
        self.replicator.save_snapshot(grain, at, state).await
    }

    async fn load_snapshot(
        &self,
        grain: &GrainName,
    ) -> Result<Option<(Seq, Vec<u8>)>, GrainJournalError> {
        self.replicator.load_snapshot(grain).await
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
