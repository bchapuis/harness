//! The file-backed [`Journal`]: one logical store shared by every node of a
//! single-machine deployment (harness spec §6.1), durable across process
//! restarts — the property the kill-a-node demo rests on.
//!
//! Layout: one directory per session (`ids::sanitize` keeps the mapping
//! injective); each committed batch is one `<first-seq>.jsonl` file
//! (20-digit, zero-padded), one JSON record per line. The head is whatever
//! the last batch file ends at; there is no index to keep consistent.
//!
//! The commit point is `hard_link(temp, batch)`. A hard link fails if the
//! target exists, so two activations racing on the same `after` collide on
//! the same batch name and exactly one wins — the fenced append of §6.2,
//! enforced by the filesystem. The batch is written and fsynced under a
//! dot-prefixed temp name first, so a batch file is fully durable before it
//! becomes visible: readers never observe a torn batch. (The two obvious
//! alternatives both fail: `rename` silently replaces an existing target, so
//! both racers would "win"; `create_new`-then-write publishes the name
//! before the bytes.) A crash can only leave an invisible temp orphan, which
//! [`FileJournal::new`] sweeps for its own node id.

use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use actor_core::BoxFuture;
use actor_core::NodeId;
use harness::AppendError;
use harness::Journal;
use harness::JournalError;
use harness::Record;
use harness::SeqNo;
use harness::SessionId;

use crate::ids::sanitize;

/// The shared-directory journal. One instance per node process, all pointed
/// at the same root.
pub struct FileJournal {
    root: PathBuf,
    node: NodeId,
    /// Distinguishes concurrent temp files within this process; the temp
    /// name also carries the node id and pid, so no two live writers ever
    /// share one.
    counter: AtomicU64,
}

impl FileJournal {
    /// Open the journal at `root`, sweeping any temp orphans a previous
    /// incarnation of `node` left behind mid-crash.
    pub fn new(root: impl Into<PathBuf>, node: NodeId) -> FileJournal {
        let root = root.into();
        sweep_orphans(&root, node);
        FileJournal {
            root,
            node,
            counter: AtomicU64::new(0),
        }
    }

    fn session_dir(&self, session: &SessionId) -> PathBuf {
        self.root.join(sanitize(session.as_str()))
    }
}

impl Journal for FileJournal {
    fn append(
        &self,
        session: &SessionId,
        after: SeqNo,
        records: Vec<Record>,
    ) -> BoxFuture<'static, Result<SeqNo, AppendError>> {
        let dir = self.session_dir(session);
        let temp = format!(
            ".tmp-{}-{}-{}",
            self.node.uid(),
            std::process::id(),
            self.counter.fetch_add(1, Ordering::Relaxed)
        );
        Box::pin(async move {
            tokio::task::spawn_blocking(move || append_sync(&dir, after, &records, &temp))
                .await
                .map_err(|e| AppendError::Unavailable(format!("journal task: {e}")))?
        })
    }

    fn load(
        &self,
        session: &SessionId,
        from: SeqNo,
        limit: usize,
    ) -> BoxFuture<'static, Result<Vec<(SeqNo, Record)>, JournalError>> {
        let dir = self.session_dir(session);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || load_sync(&dir, from, limit))
                .await
                .map_err(|e| JournalError::Unavailable(format!("journal task: {e}")))?
        })
    }
}

fn append_sync(
    dir: &Path,
    after: SeqNo,
    records: &[Record],
    temp_name: &str,
) -> Result<SeqNo, AppendError> {
    std::fs::create_dir_all(dir).map_err(|e| unavailable("create session dir", e))?;
    // Pre-check the fence. Relying on the hard link alone would let a buggy
    // `after` beyond the head commit into a seq gap; a spurious `Stale` from
    // the listing/link race is always safe (the activation refolds).
    let head = head_of(dir).map_err(|e| unavailable("read head", e))?;
    if after != head {
        return Err(AppendError::Stale { head });
    }
    if records.is_empty() {
        return Ok(head);
    }
    let temp = dir.join(temp_name);
    write_batch(&temp, records).map_err(|e| {
        let _ = std::fs::remove_file(&temp);
        unavailable("write batch", e)
    })?;
    let target = dir.join(batch_name(after.0 + 1));
    let linked = std::fs::hard_link(&temp, &target);
    let _ = std::fs::remove_file(&temp);
    match linked {
        Ok(()) => {}
        // The fence fired: another activation committed this seq first.
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let head = head_of(dir).map_err(|e| unavailable("read head", e))?;
            return Err(AppendError::Stale { head });
        }
        Err(e) => return Err(unavailable("link batch", e)),
    }
    // Make the new name durable. Best-effort: the append was already acked
    // to no one, and a lost name surfaces as a Stale retry, never a fork.
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(SeqNo(after.0 + records.len() as u64))
}

fn load_sync(dir: &Path, from: SeqNo, limit: usize) -> Result<Vec<(SeqNo, Record)>, JournalError> {
    let firsts = match batch_firsts(dir) {
        Ok(firsts) => firsts,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(JournalError::Unavailable(format!("list session dir: {e}"))),
    };
    let mut out = Vec::new();
    for (i, first) in firsts.iter().copied().enumerate() {
        // Skip a batch that ends before `from`: it ends where its successor
        // starts. The last batch has no successor and is read regardless.
        if firsts.get(i + 1).is_some_and(|next| *next <= from.0 + 1) {
            continue;
        }
        let path = dir.join(batch_name(first));
        let content = std::fs::read_to_string(&path)
            .map_err(|e| JournalError::Unavailable(format!("read batch {first}: {e}")))?;
        for (j, line) in content.lines().enumerate() {
            let seq = first + j as u64;
            if seq <= from.0 {
                continue;
            }
            let record: Record = serde_json::from_str(line).map_err(|e| {
                JournalError::Unavailable(format!("corrupt batch {first} line {j}: {e}"))
            })?;
            out.push((SeqNo(seq), record));
            if out.len() >= limit {
                return Ok(out);
            }
        }
    }
    Ok(out)
}

/// The session's current head: where the last batch ends, `ZERO` if none.
fn head_of(dir: &Path) -> std::io::Result<SeqNo> {
    let firsts = batch_firsts(dir)?;
    let Some(last) = firsts.last().copied() else {
        return Ok(SeqNo::ZERO);
    };
    let content = std::fs::read_to_string(dir.join(batch_name(last)))?;
    Ok(SeqNo(last + content.lines().count() as u64 - 1))
}

/// The first seq of every committed batch, ascending. Temp files and any
/// other stray names are invisible here.
fn batch_firsts(dir: &Path) -> std::io::Result<Vec<u64>> {
    let mut firsts = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name();
        if let Some(first) = parse_batch_name(&name.to_string_lossy()) {
            firsts.push(first);
        }
    }
    firsts.sort_unstable();
    Ok(firsts)
}

fn batch_name(first_seq: u64) -> String {
    format!("{first_seq:020}.jsonl")
}

fn parse_batch_name(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(".jsonl")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Write and fsync the batch under its temp name; the contents are durable
/// before the hard link makes them visible.
fn write_batch(path: &Path, records: &[Record]) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create_new(path)?;
    for record in records {
        serde_json::to_writer(&mut file, record)?;
        file.write_all(b"\n")?;
    }
    file.sync_all()
}

fn unavailable(what: &str, e: std::io::Error) -> AppendError {
    AppendError::Unavailable(format!("{what}: {e}"))
}

/// Remove temp files a crashed predecessor with our node id left behind. No
/// live writer shares our node id (one process per node), so this never
/// races a commit.
fn sweep_orphans(root: &Path, node: NodeId) {
    let prefix = format!(".tmp-{}-", node.uid());
    let Ok(sessions) = std::fs::read_dir(root) else {
        return;
    };
    for session in sessions.flatten() {
        let Ok(entries) = std::fs::read_dir(session.path()) else {
            continue;
        };
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with(&prefix) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use harness::RecordBody;

    use super::*;

    fn record() -> Record {
        Record {
            at_nanos: 0,
            body: RecordBody::WorkspaceReset,
        }
    }

    fn journal(root: &Path, node: u64) -> Arc<FileJournal> {
        Arc::new(FileJournal::new(root, NodeId::new(node)))
    }

    #[tokio::test]
    async fn the_fence_accepts_exactly_one_of_two_racing_appends() {
        let dir = tempfile::tempdir().expect("tempdir");
        let session = SessionId::new("s");
        // Two journals (two "nodes") race the same `after` on a shared root.
        let a = journal(dir.path(), 1);
        let b = journal(dir.path(), 2);
        let (ra, rb) = tokio::join!(
            a.append(&session, SeqNo::ZERO, vec![record()]),
            b.append(&session, SeqNo::ZERO, vec![record()]),
        );
        let oks = [&ra, &rb].iter().filter(|r| r.is_ok()).count();
        assert_eq!(oks, 1, "exactly one commit: {ra:?} {rb:?}");
        let stale = [ra, rb].into_iter().find(|r| r.is_err()).expect("a loser");
        assert_eq!(stale, Err(AppendError::Stale { head: SeqNo(1) }));
    }

    #[tokio::test]
    async fn stale_afters_are_rejected_in_both_directions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let session = SessionId::new("s");
        let j = journal(dir.path(), 1);
        j.append(&session, SeqNo::ZERO, vec![record(), record()])
            .await
            .expect("first batch");
        // Below the head: another activation already won.
        let low = j.append(&session, SeqNo::ZERO, vec![record()]).await;
        assert_eq!(low, Err(AppendError::Stale { head: SeqNo(2) }));
        // Beyond the head: a bug, not a gap.
        let high = j.append(&session, SeqNo(7), vec![record()]).await;
        assert_eq!(high, Err(AppendError::Stale { head: SeqNo(2) }));
    }

    #[tokio::test]
    async fn batches_commit_atomically_and_loads_page_across_them() {
        let dir = tempfile::tempdir().expect("tempdir");
        let session = SessionId::new("s");
        let j = journal(dir.path(), 1);
        let head = j
            .append(&session, SeqNo::ZERO, vec![record(), record(), record()])
            .await
            .expect("batch one");
        assert_eq!(head, SeqNo(3));
        let head = j
            .append(&session, head, vec![record(), record()])
            .await
            .expect("batch two");
        assert_eq!(head, SeqNo(5));
        // `from` is exclusive; the page crosses the batch-file boundary.
        let page = j.load(&session, SeqNo(1), 10).await.expect("load");
        let seqs: Vec<u64> = page.iter().map(|(n, _)| n.0).collect();
        assert_eq!(seqs, vec![2, 3, 4, 5]);
        // The limit truncates mid-batch.
        let page = j.load(&session, SeqNo(1), 2).await.expect("load");
        assert_eq!(page.len(), 2);
        // A fresh journal over the same root sees the same records (durable).
        let other = journal(dir.path(), 2);
        let all = other.load(&session, SeqNo::ZERO, 100).await.expect("load");
        assert_eq!(all.len(), 5);
    }

    #[tokio::test]
    async fn temp_orphans_are_invisible_and_swept_for_the_owning_node() {
        let dir = tempfile::tempdir().expect("tempdir");
        let session = SessionId::new("s");
        let j = journal(dir.path(), 1);
        j.append(&session, SeqNo::ZERO, vec![record()])
            .await
            .expect("append");
        // A crashed writer's leftovers: node 1's own, and another node's.
        let session_dir = dir.path().join(sanitize("s"));
        std::fs::write(session_dir.join(".tmp-1-999-0"), b"torn").expect("orphan");
        std::fs::write(session_dir.join(".tmp-2-999-0"), b"torn").expect("orphan");
        // Invisible to reads and to the head.
        assert_eq!(
            j.load(&session, SeqNo::ZERO, 10).await.expect("load").len(),
            1
        );
        let head = j
            .append(&session, SeqNo(1), vec![record()])
            .await
            .expect("append after orphan");
        assert_eq!(head, SeqNo(2));
        // A restart of node 1 sweeps its own orphan and nobody else's.
        let _ = journal(dir.path(), 1);
        assert!(!session_dir.join(".tmp-1-999-0").exists());
        assert!(session_dir.join(".tmp-2-999-0").exists());
    }

    #[tokio::test]
    async fn sessions_with_hostile_ids_never_share_a_log() {
        let dir = tempfile::tempdir().expect("tempdir");
        let j = journal(dir.path(), 1);
        let slashed = SessionId::new("a/b");
        let underscored = SessionId::new("a_b");
        j.append(&slashed, SeqNo::ZERO, vec![record()])
            .await
            .expect("append slashed");
        // If the ids collided on one directory, this `after` would be stale.
        j.append(&underscored, SeqNo::ZERO, vec![record(), record()])
            .await
            .expect("append underscored");
        assert_eq!(
            j.load(&slashed, SeqNo::ZERO, 10).await.expect("load").len(),
            1
        );
        assert_eq!(
            j.load(&underscored, SeqNo::ZERO, 10)
                .await
                .expect("load")
                .len(),
            2
        );
    }
}
