//! The journal seam (harness spec §6): the durable, per-session, totally
//! ordered record log. **The journal is the session** (§2.1): the actor and
//! the sandbox are disposable folds and workspaces around it.
//!
//! [`Journal::append`] is **fenced** (§6.2): it commits a batch iff `after`
//! equals the session's current head, else rejects with `Stale`. That
//! conditional write is the fence util §2.3 and util §4.3 tell
//! exclusivity-needing applications to build: divergent placement views may
//! activate two owners, but their appends race on the same `after` and the
//! journal accepts one — the transcript never forks (invariant H2). The fence
//! guards the record, not the world (§5.5).
//!
//! A batch commits atomically: the write-ahead discipline (§6.4) journals a
//! final assistant message and its `RunEnded` in one batch, so no prefix can
//! end between them.
//!
//! The journal is one logical store shared by every node (§6.1): a session's
//! records must be readable and appendable from any node, with `append`
//! linearizable per session. The in-memory implementation here is per-process
//! and therefore confines a deployment to a single node — and is the
//! substrate the simulator wraps with faults (§12.2); durable stores are
//! future work (§13).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use actor_core::BoxFuture;
use serde::Deserialize;
use serde::Serialize;

use crate::session::Record;
use crate::session::SessionId;

/// The position of a committed record in its session's total order. `ZERO` is
/// the head of an empty journal; the first record commits at 1.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct SeqNo(pub u64);

impl SeqNo {
    /// The head of an empty journal.
    pub const ZERO: SeqNo = SeqNo(0);
}

impl std::fmt::Display for SeqNo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "@{}", self.0)
    }
}

/// Why an append did not commit (harness spec §6.2, §6.5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AppendError {
    /// `after` is not the current head: another activation won the race. The
    /// loser must deactivate (invariant H2).
    Stale { head: SeqNo },
    /// The store cannot commit right now. Pauses the session's progress,
    /// never corrupts it: the actor must not proceed past an uncommitted
    /// record (§6.5).
    Unavailable(String),
}

/// Why a read failed (harness spec §6.5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JournalError {
    Unavailable(String),
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalError::Unavailable(e) => write!(f, "journal unavailable: {e}"),
        }
    }
}

/// Durable, per-session, totally-ordered record log. The second harness seam.
///
/// Object-safe (`BoxFuture` rather than `async fn`) so the harness injects it
/// as `Arc<dyn Journal>`.
pub trait Journal: Send + Sync + 'static {
    /// Fenced append (§6.2): commits `records` immediately after `after`, or
    /// rejects with the current head if `after` is stale. The batch commits
    /// atomically; the returned `SeqNo` is the new head.
    fn append(
        &self,
        session: &SessionId,
        after: SeqNo,
        records: Vec<Record>,
    ) -> BoxFuture<'static, Result<SeqNo, AppendError>>;

    /// Up to `limit` committed records from `from` (exclusive) toward the
    /// head. An idempotent, fence-free read (§10.2).
    fn load(
        &self,
        session: &SessionId,
        from: SeqNo,
        limit: usize,
    ) -> BoxFuture<'static, Result<Vec<(SeqNo, Record)>, JournalError>>;
}

/// The in-memory journal (harness spec §6.1): one mutex over one map, which
/// makes the per-session fence trivially linearizable. Per-process, so a
/// deployment on it is single-node; the simulator wraps it with seeded faults
/// (§12.2). Cloning shares the store.
#[derive(Clone, Default)]
pub struct InMemoryJournal {
    sessions: Arc<Mutex<BTreeMap<SessionId, Vec<Record>>>>,
}

impl InMemoryJournal {
    pub fn new() -> InMemoryJournal {
        InMemoryJournal::default()
    }

    /// Synchronous fenced append — the one place the fence is decided; the
    /// trait method wraps it.
    fn append_sync(
        &self,
        session: &SessionId,
        after: SeqNo,
        records: Vec<Record>,
    ) -> Result<SeqNo, AppendError> {
        let mut sessions = self.sessions.lock().expect("journal mutex poisoned");
        let log = sessions.entry(session.clone()).or_default();
        let head = SeqNo(log.len() as u64);
        if after != head {
            return Err(AppendError::Stale { head });
        }
        log.extend(records);
        Ok(SeqNo(log.len() as u64))
    }

    /// Synchronous read; the trait method wraps it.
    fn load_sync(&self, session: &SessionId, from: SeqNo, limit: usize) -> Vec<(SeqNo, Record)> {
        let sessions = self.sessions.lock().expect("journal mutex poisoned");
        let Some(log) = sessions.get(session) else {
            return Vec::new();
        };
        log.iter()
            .enumerate()
            .skip(from.0 as usize)
            .take(limit)
            .map(|(i, r)| (SeqNo(i as u64 + 1), r.clone()))
            .collect()
    }

    /// Every committed record of `session` — the audit view scenario tests
    /// read at quiescence (§11).
    pub fn records(&self, session: &SessionId) -> Vec<Record> {
        self.sessions
            .lock()
            .expect("journal mutex poisoned")
            .get(session)
            .cloned()
            .unwrap_or_default()
    }

    /// The sessions with at least one committed record, in id order. An
    /// audit-only view: the harness itself never enumerates sessions —
    /// resumption is caller-driven (§7.5).
    pub fn session_ids(&self) -> Vec<SessionId> {
        self.sessions
            .lock()
            .expect("journal mutex poisoned")
            .keys()
            .cloned()
            .collect()
    }
}

impl Journal for InMemoryJournal {
    fn append(
        &self,
        session: &SessionId,
        after: SeqNo,
        records: Vec<Record>,
    ) -> BoxFuture<'static, Result<SeqNo, AppendError>> {
        let result = self.append_sync(session, after, records);
        Box::pin(async move { result })
    }

    fn load(
        &self,
        session: &SessionId,
        from: SeqNo,
        limit: usize,
    ) -> BoxFuture<'static, Result<Vec<(SeqNo, Record)>, JournalError>> {
        let result = Ok(self.load_sync(session, from, limit));
        Box::pin(async move { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::RecordBody;

    fn record() -> Record {
        Record {
            at_nanos: 0,
            body: RecordBody::WorkspaceReset,
        }
    }

    #[test]
    fn fenced_append_accepts_one_writer() {
        let journal = InMemoryJournal::new();
        let session = SessionId::new("s");
        // Two activations race on the same head: one commits, one is stale.
        let a = journal.append_sync(&session, SeqNo::ZERO, vec![record()]);
        let b = journal.append_sync(&session, SeqNo::ZERO, vec![record()]);
        assert_eq!(a, Ok(SeqNo(1)));
        assert_eq!(b, Err(AppendError::Stale { head: SeqNo(1) }));
    }

    #[test]
    fn batches_commit_atomically_and_load_pages() {
        let journal = InMemoryJournal::new();
        let session = SessionId::new("s");
        let head = journal
            .append_sync(&session, SeqNo::ZERO, vec![record(), record(), record()])
            .expect("append");
        assert_eq!(head, SeqNo(3));
        let page = journal.load_sync(&session, SeqNo(1), 10);
        assert_eq!(
            page.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
            vec![SeqNo(2), SeqNo(3)]
        );
    }
}
