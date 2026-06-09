//! Durable Raft state on the local filesystem (spec §9.4.3 item 2).
//!
//! [`FileRaftStorage`] is the production [`RaftStorage`]: a voter's term, vote,
//! and log survive a process restart, so a restarted voter can never grant a
//! second vote in a term it already voted in (election safety, invariant #22).
//! The layout matches the trait's two write paths, each with the durability
//! technique that fits it:
//!
//! - **`term`** — one tiny JSON record `{term, voted_for}`, rewritten on every
//!   [`save_term_and_vote`](RaftStorage::save_term_and_vote) by atomic replace
//!   (write `term.tmp` → fsync → rename → fsync dir). A torn write is
//!   impossible: a reader sees either the old record or the new one.
//! - **`log`** — append-only framed records, one per [`LogEntry`]:
//!   `[u32 length][JSON payload][u64 FNV-1a checksum]`. Raft's
//!   truncate-then-append maps to `set_len` at the entry's recorded offset
//!   followed by appends; every write is fsynced before the method returns.
//!
//! **Recovery.** At [`open`](FileRaftStorage::open), the log is scanned from
//! the start; the first incomplete or checksum-failing record ends the valid
//! prefix and everything after it is discarded (the file is truncated back).
//! A torn tail is an entry whose write never returned, so the caller never
//! acknowledged it — dropping it is correct, the standard WAL recovery. A
//! corrupt `term` file is different: silently resetting the term could let the
//! voter vote twice, so it is a hard error at open.
//!
//! **Failure policy.** [`RaftStorage`]'s methods are infallible by signature;
//! a voter whose state cannot be made durable cannot safely continue (it might
//! otherwise announce un-persisted state). This implementation therefore
//! panics on an I/O error after open, taking the consensus task down rather
//! than risking a safety violation; peers observe the node unreachable.
//!
//! **Single writer.** A storage directory must belong to one process at a
//! time. Advisory locking (`File::try_lock`) needs a newer toolchain than the
//! workspace MSRV, so this is documented, not enforced; the
//! [`factory`](FileRaftStorage::factory) layout (one subdirectory per node)
//! makes accidental sharing unlikely.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use actor_cluster::LogEntry;
use actor_cluster::PersistedRaft;
use actor_cluster::RaftStorage;
use actor_core::NodeId;
use serde::Deserialize;
use serde::Serialize;

/// Upper bound on one framed record's payload, as a sanity check while
/// scanning: a length above this is treated as corruption, not an allocation.
const MAX_RECORD: u32 = 1 << 20;

/// The `term` file's content (spec §9.4.3 item 2): the current term and the
/// vote cast in it, always written together — they are one atomic decision.
#[derive(Serialize, Deserialize)]
struct TermRecord {
    term: u64,
    voted_for: Option<NodeId>,
}

/// FNV-1a 64. Detects torn and partial writes (not adversarial tampering),
/// which is all a local WAL needs; hand-rolled to avoid a dependency.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

/// Frame one log entry: `[u32 len][payload][u64 checksum]`, all little-endian.
fn encode_record(entry: &LogEntry) -> Vec<u8> {
    let payload = serde_json::to_vec(entry).expect("a LogEntry always serializes");
    let mut record = Vec::with_capacity(4 + payload.len() + 8);
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.extend_from_slice(&payload);
    record.extend_from_slice(&fnv1a(&payload).to_le_bytes());
    record
}

/// Scan a log file's bytes into `(entries, per-entry start offsets, valid
/// length)`. The scan stops at the first incomplete, oversized,
/// checksum-failing, or unparsable record — the recovery rule: the valid
/// prefix is the log; the tail was never acknowledged.
fn scan_log(bytes: &[u8]) -> (Vec<LogEntry>, Vec<u64>, u64) {
    let mut entries = Vec::new();
    let mut offsets = Vec::new();
    let mut pos = 0usize;
    while let Some(header) = bytes.get(pos..pos + 4) {
        let len = u32::from_le_bytes(header.try_into().expect("4-byte slice"));
        if len > MAX_RECORD {
            break;
        }
        let len = len as usize;
        let Some(payload) = bytes.get(pos + 4..pos + 4 + len) else {
            break;
        };
        let Some(check) = bytes.get(pos + 4 + len..pos + 4 + len + 8) else {
            break;
        };
        if u64::from_le_bytes(check.try_into().expect("8-byte slice")) != fnv1a(payload) {
            break;
        }
        let Ok(entry) = serde_json::from_slice::<LogEntry>(payload) else {
            break;
        };
        offsets.push(pos as u64);
        entries.push(entry);
        pos += 4 + len + 8;
    }
    (entries, offsets, pos as u64)
}

/// Make a directory entry durable. File data is covered by `sync_all` on the
/// file itself; creations and renames live in the directory, which needs its
/// own fsync on unix. Elsewhere (Windows) directories cannot be opened for
/// sync and the rename itself is the durability point.
fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

struct Inner {
    /// The open append handle to the `log` file. With `O_APPEND`, writes land
    /// at the current end even right after a truncating `set_len`.
    log: File,
    /// Byte offset where each entry's record starts, parallel to `state.log` —
    /// what makes truncate-then-append a single `set_len`.
    offsets: Vec<u64>,
    /// The log file's current (valid) length — where the next record lands.
    end: u64,
    /// The in-memory mirror of the durable state; every write updates it after
    /// the disk write succeeds, and [`RaftStorage::load`] clones it.
    state: PersistedRaft,
}

/// The production [`RaftStorage`]: a voter's Raft state on the local
/// filesystem, durable before every method returns (see the module docs for
/// the layout, recovery, and failure policy).
pub struct FileRaftStorage {
    dir: PathBuf,
    inner: Mutex<Inner>,
}

impl FileRaftStorage {
    /// Open (creating if needed) the storage directory, recover the log's
    /// valid prefix — truncating any torn tail — and load the persisted state.
    ///
    /// # Errors
    ///
    /// Any filesystem error, and — deliberately — a corrupt `term` file:
    /// guessing a term could let the voter vote twice (election safety), so
    /// only the operator may resolve that.
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<FileRaftStorage> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;

        let term_path = dir.join("term");
        let (term, voted_for) = match fs::read(&term_path) {
            Ok(bytes) => {
                let record: TermRecord = serde_json::from_slice(&bytes).map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "corrupt raft term file {} ({err}); refusing to guess a term \
                             (a wrong one risks a double vote) — restore or remove the \
                             node's state and rejoin it as a new member",
                            term_path.display()
                        ),
                    )
                })?;
                (record.term, record.voted_for)
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => (0, None),
            Err(err) => return Err(err),
        };

        let log_path = dir.join("log");
        let bytes = match fs::read(&log_path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(err) => return Err(err),
        };
        let (entries, offsets, valid_end) = scan_log(&bytes);
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;
        if (valid_end as usize) < bytes.len() {
            // A torn tail: the write never returned, so the entry was never
            // announced. Truncate it away before anything is appended.
            log.set_len(valid_end)?;
            log.sync_all()?;
        }
        // Make the directory entries (a freshly created `log`) durable.
        sync_dir(&dir)?;

        Ok(FileRaftStorage {
            dir,
            inner: Mutex::new(Inner {
                log,
                offsets,
                end: valid_end,
                state: PersistedRaft {
                    term,
                    voted_for,
                    log: entries,
                },
            }),
        })
    }

    /// A [`RaftConfig::storage`] factory rooted at `data_dir`: each node's
    /// state lives in its own `data_dir/<node>/` subdirectory. Panics if a
    /// directory cannot be opened — a voter without durable storage must not
    /// start (spec §9.4.3 item 2).
    ///
    /// [`RaftConfig::storage`]: actor_cluster::RaftConfig
    pub fn factory(data_dir: PathBuf) -> Arc<dyn Fn(NodeId) -> Arc<dyn RaftStorage> + Send + Sync> {
        Arc::new(move |node| {
            let dir = data_dir.join(node.to_string());
            let storage = FileRaftStorage::open(&dir).unwrap_or_else(|err| {
                panic!("cannot open raft storage at {}: {err}", dir.display())
            });
            Arc::new(storage)
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("raft storage mutex poisoned")
    }

    /// The fallible body of [`RaftStorage::save_term_and_vote`]: atomic
    /// replace of the `term` file.
    fn persist_term(&self, record: &TermRecord) -> io::Result<()> {
        let tmp_path = self.dir.join("term.tmp");
        let final_path = self.dir.join("term");
        let bytes = serde_json::to_vec(record).expect("a TermRecord always serializes");
        let mut tmp = File::create(&tmp_path)?;
        tmp.write_all(&bytes)?;
        tmp.sync_all()?;
        fs::rename(&tmp_path, &final_path)?;
        sync_dir(&self.dir)
    }

    /// The fallible body of [`RaftStorage::append`]: truncate the log file at
    /// `from_index`'s recorded offset, then append the framed records.
    fn persist_append(&self, from_index: u64, entries: &[LogEntry]) -> io::Result<()> {
        let mut inner = self.lock();
        let from = from_index as usize;
        assert!(
            from <= inner.offsets.len(),
            "append at index {from} beyond a log of {} entries",
            inner.offsets.len()
        );
        if let Some(&cut) = inner.offsets.get(from) {
            // Truncation: make the cut durable before any conflicting entry
            // can be appended after it.
            inner.log.set_len(cut)?;
            inner.log.sync_all()?;
            inner.offsets.truncate(from);
            inner.state.log.truncate(from);
            inner.end = cut;
        }

        let mut buf = Vec::new();
        for entry in entries {
            let offset = inner.end + buf.len() as u64;
            inner.offsets.push(offset);
            buf.extend_from_slice(&encode_record(entry));
            inner.state.log.push(*entry);
        }
        inner.log.write_all(&buf)?;
        inner.log.sync_all()?;
        inner.end += buf.len() as u64;
        Ok(())
    }
}

impl RaftStorage for FileRaftStorage {
    fn load(&self) -> PersistedRaft {
        self.lock().state.clone()
    }

    fn save_term_and_vote(&self, term: u64, voted_for: Option<NodeId>) {
        self.persist_term(&TermRecord { term, voted_for })
            .unwrap_or_else(|err| {
                panic!(
                    "raft term persistence failed at {}: {err} — a voter that cannot \
                     persist its vote cannot safely continue",
                    self.dir.display()
                )
            });
        let mut inner = self.lock();
        inner.state.term = term;
        inner.state.voted_for = voted_for;
    }

    fn append(&self, from_index: u64, entries: &[LogEntry]) {
        self.persist_append(from_index, entries)
            .unwrap_or_else(|err| {
                panic!(
                    "raft log persistence failed at {}: {err} — a voter that cannot \
                 persist its log cannot safely continue",
                    self.dir.display()
                )
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actor_cluster::InMemoryRaftStorage;
    use actor_cluster::RaftCommand;

    fn entry(term: u64, command: RaftCommand) -> LogEntry {
        LogEntry { term, command }
    }

    fn node(uid: u64) -> NodeId {
        NodeId::new(uid)
    }

    #[test]
    fn state_round_trips_across_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftStorage::open(dir.path()).unwrap();
        storage.save_term_and_vote(3, Some(node(2)));
        storage.append(
            0,
            &[
                entry(1, RaftCommand::Noop),
                entry(3, RaftCommand::Admit(node(4))),
            ],
        );
        drop(storage);

        let reopened = FileRaftStorage::open(dir.path()).unwrap();
        let state = reopened.load();
        assert_eq!(state.term, 3);
        assert_eq!(state.voted_for, Some(node(2)));
        assert_eq!(
            state.log,
            vec![
                entry(1, RaftCommand::Noop),
                entry(3, RaftCommand::Admit(node(4))),
            ],
        );
    }

    #[test]
    fn a_fresh_directory_loads_the_default_state() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftStorage::open(dir.path().join("sub")).unwrap();
        assert_eq!(storage.load(), PersistedRaft::default());
    }

    #[test]
    fn truncate_then_append_replaces_the_conflicting_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftStorage::open(dir.path()).unwrap();
        storage.append(
            0,
            &[
                entry(1, RaftCommand::Noop),
                entry(1, RaftCommand::Admit(node(4))),
                entry(1, RaftCommand::Drain(node(4))),
            ],
        );
        // Raft conflict resolution: overwrite from index 1 with a higher term.
        storage.append(
            1,
            &[
                entry(2, RaftCommand::Noop),
                entry(2, RaftCommand::Down(node(4))),
            ],
        );
        drop(storage);

        let reopened = FileRaftStorage::open(dir.path()).unwrap();
        assert_eq!(
            reopened.load().log,
            vec![
                entry(1, RaftCommand::Noop),
                entry(2, RaftCommand::Noop),
                entry(2, RaftCommand::Down(node(4))),
            ],
        );
    }

    #[test]
    fn a_torn_tail_is_discarded_and_appends_continue() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftStorage::open(dir.path()).unwrap();
        storage.append(
            0,
            &[
                entry(1, RaftCommand::Noop),
                entry(1, RaftCommand::Leave(node(9))),
            ],
        );
        drop(storage);

        // A torn write: garbage lands after the valid records (a record whose
        // write never completed).
        let log_path = dir.path().join("log");
        let mut file = OpenOptions::new().append(true).open(&log_path).unwrap();
        file.write_all(&[0x12, 0x34, 0x56]).unwrap();
        drop(file);

        let reopened = FileRaftStorage::open(dir.path()).unwrap();
        assert_eq!(
            reopened.load().log,
            vec![
                entry(1, RaftCommand::Noop),
                entry(1, RaftCommand::Leave(node(9)))
            ],
            "the torn tail is not part of the log",
        );
        // The recovery truncated the garbage; appends land cleanly after it.
        reopened.append(2, &[entry(2, RaftCommand::Noop)]);
        drop(reopened);
        let again = FileRaftStorage::open(dir.path()).unwrap();
        assert_eq!(again.load().log.len(), 3);
    }

    #[test]
    fn a_record_cut_mid_payload_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftStorage::open(dir.path()).unwrap();
        storage.append(
            0,
            &[entry(1, RaftCommand::Noop), entry(1, RaftCommand::Noop)],
        );
        drop(storage);

        // Cut the file mid-record, as a crash during a write would.
        let log_path = dir.path().join("log");
        let len = fs::metadata(&log_path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&log_path).unwrap();
        file.set_len(len - 3).unwrap();
        drop(file);

        let reopened = FileRaftStorage::open(dir.path()).unwrap();
        assert_eq!(
            reopened.load().log,
            vec![entry(1, RaftCommand::Noop)],
            "the half-written record is dropped; the valid prefix survives",
        );
    }

    #[test]
    fn a_corrupted_checksum_ends_the_valid_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftStorage::open(dir.path()).unwrap();
        storage.append(
            0,
            &[entry(1, RaftCommand::Noop), entry(1, RaftCommand::Noop)],
        );
        drop(storage);

        // Flip a byte inside the second record's payload.
        let log_path = dir.path().join("log");
        let mut bytes = fs::read(&log_path).unwrap();
        let second_start = {
            let (_, offsets, _) = scan_log(&bytes);
            offsets[1] as usize
        };
        bytes[second_start + 5] ^= 0xff;
        fs::write(&log_path, &bytes).unwrap();

        let reopened = FileRaftStorage::open(dir.path()).unwrap();
        assert_eq!(
            reopened.load().log.len(),
            1,
            "the corrupt record and after are dropped"
        );
    }

    #[test]
    fn a_corrupt_term_file_is_a_hard_error() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftStorage::open(dir.path()).unwrap();
        storage.save_term_and_vote(7, None);
        drop(storage);

        fs::write(dir.path().join("term"), b"not json").unwrap();
        let err = match FileRaftStorage::open(dir.path()) {
            Err(err) => err,
            Ok(_) => panic!("a corrupt term must not be guessed"),
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// The differential workhorse: drive the same operation sequence through
    /// `FileRaftStorage` — reopening from disk before every step — and
    /// `InMemoryRaftStorage`; `load()` must agree at every step. Covers the
    /// offset index, truncation, and reopen logic across interleavings.
    #[test]
    fn file_storage_matches_in_memory_storage_across_reopens() {
        enum Op {
            Save(u64, Option<u64>),
            Append(u64, Vec<LogEntry>),
        }
        let ops = vec![
            Op::Save(1, Some(1)),
            Op::Append(0, vec![entry(1, RaftCommand::Noop)]),
            Op::Append(1, vec![entry(1, RaftCommand::Admit(node(4)))]),
            Op::Save(2, None),
            Op::Save(2, Some(3)),
            // Conflict: overwrite index 1 onward at the new term.
            Op::Append(
                1,
                vec![
                    entry(2, RaftCommand::Noop),
                    entry(2, RaftCommand::Drain(node(4))),
                ],
            ),
            Op::Append(3, vec![entry(2, RaftCommand::Resume(node(4)))]),
            // Truncate everything back to empty, then rebuild.
            Op::Append(0, vec![entry(3, RaftCommand::Noop)]),
            Op::Save(4, Some(2)),
            Op::Append(
                1,
                vec![
                    entry(4, RaftCommand::AddVoter(node(5))),
                    entry(4, RaftCommand::Down(node(4))),
                ],
            ),
        ];

        let dir = tempfile::tempdir().unwrap();
        let mirror = InMemoryRaftStorage::new();
        for (step, op) in ops.iter().enumerate() {
            // A fresh open every step: the state must come back from disk.
            let file = FileRaftStorage::open(dir.path()).unwrap();
            assert_eq!(file.load(), mirror.load(), "diverged before step {step}");
            match op {
                Op::Save(term, voted_for) => {
                    let vote = voted_for.map(node);
                    file.save_term_and_vote(*term, vote);
                    mirror.save_term_and_vote(*term, vote);
                }
                Op::Append(from, entries) => {
                    file.append(*from, entries);
                    mirror.append(*from, entries);
                }
            }
            assert_eq!(file.load(), mirror.load(), "diverged after step {step}");
        }
        let final_state = FileRaftStorage::open(dir.path()).unwrap();
        assert_eq!(final_state.load(), mirror.load(), "diverged at the end");
    }
}
