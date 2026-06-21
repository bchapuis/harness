//! The single-node in-memory journal — durability Tier 1 (spec §7.4).
//!
//! One linearizable local store: a grain's events live in an in-memory log, each
//! `append` "commits" immediately (the single-node analogue of a quorum append).
//! CP trivially — one writer, one store — but not fault-tolerant to node loss.
//! Its sweet spot is embedded use, tests, and the deterministic simulator (§14):
//! it adds no network and no nondeterminism, so a grain's whole lifecycle
//! (activate, commit, snapshot, rehydrate, hibernate) runs under one seed.
//!
//! It never returns `NotLeader` or `Unavailable`: with a single store there is
//! always a leader and always a quorum (the deferred Tier-2 sharded-Raft journal
//! is where those outcomes arise).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use crate::grain::GrainName;
use crate::journal::AppendOutcome;
use crate::journal::GrainJournal;
use crate::journal::GrainJournalError;
use crate::journal::Seq;
use crate::journal::head_of;
use crate::journal::slice;

/// One grain's log: its events in `Seq` order (event at `Seq` `i` is
/// `events[i - 1]`) and its latest snapshot.
#[derive(Default)]
struct GrainLog {
    events: Vec<Vec<u8>>,
    snapshot: Option<(Seq, Vec<u8>)>,
}

impl GrainLog {
    /// The committed head: the seq of the last event, or `ZERO` when empty.
    fn head(&self) -> Seq {
        head_of(&self.events)
    }
}

/// The Tier-1 single-node journal (spec §7.4). Cloning shares one underlying
/// store, so every host spawned for the same `Granary` writes to the same log.
#[derive(Clone, Default)]
pub struct MemoryGrainJournal {
    grains: Arc<Mutex<HashMap<GrainName, GrainLog>>>,
}

impl MemoryGrainJournal {
    /// A fresh, empty journal.
    pub fn new() -> MemoryGrainJournal {
        MemoryGrainJournal::default()
    }
}

impl GrainJournal for MemoryGrainJournal {
    async fn append(&self, grain: &GrainName, after: Seq, events: Vec<Vec<u8>>) -> AppendOutcome {
        let mut grains = self.grains.lock().expect("journal mutex poisoned");
        let log = grains.entry(grain.clone()).or_default();
        // The host appends only behind the input gate from the single writer, so
        // `after` is always the current head (§7.3). A mismatch is a runtime bug,
        // not a recoverable durability outcome.
        assert_eq!(
            after,
            log.head(),
            "append `after` ({after}) must equal the grain's head ({}) — single-writer invariant (§7.3)",
            log.head()
        );
        log.events.extend(events);
        AppendOutcome::Committed(log.head())
    }

    async fn load(
        &self,
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<(Seq, Vec<u8>)>, GrainJournalError> {
        let grains = self.grains.lock().expect("journal mutex poisoned");
        let Some(log) = grains.get(grain) else {
            return Ok(Vec::new());
        };
        Ok(slice(&log.events, from, limit))
    }

    async fn head(&self, grain: &GrainName) -> Result<Seq, GrainJournalError> {
        let grains = self.grains.lock().expect("journal mutex poisoned");
        Ok(grains.get(grain).map(GrainLog::head).unwrap_or(Seq::ZERO))
    }

    async fn save_snapshot(&self, grain: &GrainName, at: Seq, state: Vec<u8>) -> AppendOutcome {
        let mut grains = self.grains.lock().expect("journal mutex poisoned");
        let log = grains.entry(grain.clone()).or_default();
        log.snapshot = Some((at, state));
        AppendOutcome::Committed(at)
    }

    async fn load_snapshot(
        &self,
        grain: &GrainName,
    ) -> Result<Option<(Seq, Vec<u8>)>, GrainJournalError> {
        let grains = self.grains.lock().expect("journal mutex poisoned");
        Ok(grains.get(grain).and_then(|log| log.snapshot.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(key: &str) -> GrainName {
        GrainName::new("test.Grain", key)
    }

    /// A bare executor: the memory journal's futures never yield, so polling once
    /// drives them to completion without a runtime.
    fn run<F: std::future::Future>(future: F) -> F::Output {
        use std::task::Context;
        use std::task::Poll;
        let mut future = Box::pin(future);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(out) => out,
            Poll::Pending => panic!("memory journal future parked unexpectedly"),
        }
    }

    #[test]
    fn append_commits_at_sequential_heads() {
        let j = MemoryGrainJournal::new();
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
        let j = MemoryGrainJournal::new();
        let n = name("a");
        run(j.append(&n, Seq::ZERO, vec![b"e1".to_vec(), b"e2".to_vec(), b"e3".to_vec()]));
        // from = ZERO returns the whole log, first event at seq 1.
        assert_eq!(
            run(j.load(&n, Seq::ZERO, 10)),
            Ok(vec![
                (Seq::new(1), b"e1".to_vec()),
                (Seq::new(2), b"e2".to_vec()),
                (Seq::new(3), b"e3".to_vec()),
            ])
        );
        // from is exclusive; limit bounds the batch.
        assert_eq!(
            run(j.load(&n, Seq::new(1), 1)),
            Ok(vec![(Seq::new(2), b"e2".to_vec())])
        );
        // Reading at the head yields nothing.
        assert_eq!(run(j.load(&n, Seq::new(3), 10)), Ok(Vec::new()));
    }

    #[test]
    fn snapshot_round_trips_and_grains_are_independent() {
        let j = MemoryGrainJournal::new();
        let a = name("a");
        let b = name("b");
        assert_eq!(run(j.load_snapshot(&a)), Ok(None));
        run(j.save_snapshot(&a, Seq::new(2), b"state-a".to_vec()));
        assert_eq!(
            run(j.load_snapshot(&a)),
            Ok(Some((Seq::new(2), b"state-a".to_vec())))
        );
        // Another grain's log and snapshot are untouched.
        assert_eq!(run(j.head(&b)), Ok(Seq::ZERO));
        assert_eq!(run(j.load_snapshot(&b)), Ok(None));
    }

    #[test]
    #[should_panic(expected = "single-writer invariant")]
    fn append_with_stale_after_panics() {
        let j = MemoryGrainJournal::new();
        let n = name("a");
        run(j.append(&n, Seq::ZERO, vec![b"e1".to_vec()]));
        // A second writer with a stale view of the head violates the single-writer
        // contract the input gate upholds (§6, §7.3).
        run(j.append(&n, Seq::ZERO, vec![b"e2".to_vec()]));
    }
}
