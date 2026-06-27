//! Durable Raft state on the local filesystem (spec §9.4.3 item 2, §9).
//!
//! [`FileRaftWAL`] is the production [`RaftWAL`]: a voter's term, vote, log, and
//! state-machine snapshot survive a process restart, so a restarted voter can never
//! grant a second vote in a term it already voted in (election safety, invariant
//! #22) and comes back over its compacted log rather than a blank one. The layout
//! matches the trait's write paths, each with the durability technique that fits it:
//!
//! - **`term`** — one tiny JSON record `{term, voted_for}`, rewritten on every
//!   [`save_term_and_vote`](RaftWAL::save_term_and_vote) by atomic replace
//!   ([`wal::atomic_replace`]). A torn write is impossible: a reader sees either the
//!   old record or the new one. It stays JSON on purpose — its parse-failure is the
//!   corruption check that protects election safety.
//! - **`log`** — a framed, checksummed append-only [`wal::Wal`] of `(absolute index,
//!   entry)` records. Carrying the absolute index makes a crash mid-compaction
//!   self-healing (below). Raft's truncate-then-append maps to
//!   [`Wal::truncate`](wal::Wal::truncate) at the entry's recorded offset followed by
//!   [`Wal::append_batch`](wal::Wal::append_batch); both fsync before returning.
//! - **`snapshot`** — one postcard record `{index, term, data}` for the compacted
//!   prefix (§9), rewritten by the same atomic replace as `term`. Written *before*
//!   the log prefix it subsumes is dropped, so a crash can leave a snapshot newer
//!   than the log but never the reverse.
//!
//! **Recovery.** At [`open`](FileRaftWAL::open), the snapshot is loaded, then the log
//! is recovered by [`Wal::open`](wal::Wal::open) (which discards a torn tail). Records
//! whose absolute index is `≤` the snapshot index are discarded too — that is the
//! self-heal for a crash between persisting a snapshot and rewriting the log: the
//! stale prefix is dropped and the log rewritten to the retained suffix. A corrupt
//! `term` or `snapshot` file is a hard error: silently resetting either could violate
//! safety, so only the operator may resolve it.
//!
//! **Failure policy.** [`RaftWAL`]'s methods are infallible by signature; a voter
//! whose state cannot be made durable cannot safely continue (it might announce
//! un-persisted state). This implementation panics on an I/O error after open,
//! taking the consensus task down rather than risking a safety violation; peers
//! observe the node unreachable.
//!
//! **Single writer.** A storage directory must belong to one process at a time.
//! Advisory locking (`File::try_lock`) needs a newer toolchain than the workspace
//! MSRV, so this is documented, not enforced; the [`factory`](FileRaftWAL::factory)
//! layout (one subdirectory per node) makes accidental sharing unlikely.

use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use actor_cluster::GroupId;
use actor_cluster::PersistedRaft;
use actor_cluster::RaftEntry;
use actor_cluster::RaftWAL;
use actor_core::NodeId;
use serde::Deserialize;
use serde::Serialize;
use wal::Wal;

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

/// The `snapshot` file's content (spec §9): the compacted prefix's last index and
/// term, and the application snapshot taken at it.
#[derive(Serialize, Deserialize)]
struct SnapshotRecord {
    index: u64,
    term: u64,
    data: Vec<u8>,
}

struct Inner {
    /// The framed log of `(absolute index, entry)` records — the retained suffix
    /// above `snapshot_index`. Owns the file handle and the per-record offsets.
    log: Wal<(u64, RaftEntry)>,
    /// The in-memory mirror of the durable state; every write updates it after
    /// the disk write succeeds, and [`RaftWAL::load`] clones it.
    state: PersistedRaft,
}

/// The production [`RaftWAL`]: a voter's Raft state on the local
/// filesystem, durable before every method returns (see the module docs for
/// the layout, recovery, and failure policy).
pub struct FileRaftWAL {
    dir: PathBuf,
    inner: Mutex<Inner>,
}

impl FileRaftWAL {
    /// Open (creating if needed) the storage directory: load the term and snapshot,
    /// recover the log's valid prefix — discarding a torn tail and any records the
    /// snapshot subsumes — and load the persisted state.
    ///
    /// # Errors
    ///
    /// Any filesystem error, and — deliberately — a corrupt `term` or `snapshot`
    /// file: guessing either could violate a safety property (a wrong term risks a
    /// double vote), so only the operator may resolve it.
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<FileRaftWAL> {
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

        let snapshot_path = dir.join("snapshot");
        let (snapshot_index, snapshot_term, snapshot) = match fs::read(&snapshot_path) {
            Ok(bytes) => {
                let record: SnapshotRecord = postcard::from_bytes(&bytes).map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "corrupt raft snapshot file {} ({err}); refusing to guess a \
                             compacted prefix — restore or remove the node's state and \
                             rejoin it as a new member",
                            snapshot_path.display()
                        ),
                    )
                })?;
                (record.index, record.term, Some(record.data))
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => (0, 0, None),
            Err(err) => return Err(err),
        };

        // The log: the shared WAL recovers the valid prefix and truncates a torn tail.
        let (mut log, records) = Wal::<(u64, RaftEntry)>::open(dir.join("log"), MAX_RECORD)?;

        // Discard records the snapshot subsumes (absolute index `≤ snapshot_index`):
        // the self-heal for a crash between persisting a snapshot and rewriting the
        // log. The retained suffix begins at `snapshot_index + 1`.
        let dropped = records
            .iter()
            .take_while(|(index, _)| *index <= snapshot_index)
            .count();
        let retained: Vec<(u64, RaftEntry)> = records[dropped..].to_vec();
        assert_contiguous(&retained, snapshot_index, &dir);

        if dropped > 0 {
            // A stale prefix (or any torn tail past it): rewrite to the retained
            // suffix, normalizing the file and reclaiming the prefix's space.
            log.rewrite(&retained)?;
        }
        // No directory fsync here: every file in `dir` is written through a wal
        // primitive (`Wal::open` for `log`, `atomic_replace` for `term`/`snapshot`) that
        // makes its own entry durable, and this layout creates no subdirectory of its own.

        Ok(FileRaftWAL {
            dir,
            inner: Mutex::new(Inner {
                log,
                state: PersistedRaft {
                    term,
                    voted_for,
                    log: retained.into_iter().map(|(_, entry)| entry).collect(),
                    snapshot_index,
                    snapshot_term,
                    snapshot,
                },
            }),
        })
    }

    /// A [`RaftConfig::storage`] factory rooted at `data_dir`: each
    /// `(group, node)`'s state lives in its own `data_dir/<group>/<node>/`
    /// subdirectory, so a node's several Raft groups (the membership control
    /// group plus, for granary, a group per shard) never share a log. Panics if
    /// a directory cannot be opened — a voter without durable storage must not
    /// start (spec §9.4.3 item 2).
    ///
    /// [`RaftConfig::storage`]: actor_cluster::RaftConfig
    pub fn factory(
        data_dir: PathBuf,
    ) -> Arc<dyn Fn(GroupId, NodeId) -> Arc<dyn RaftWAL> + Send + Sync> {
        Arc::new(move |group, node| {
            let dir = data_dir.join(group.to_string()).join(node.to_string());
            let storage = FileRaftWAL::open(&dir).unwrap_or_else(|err| {
                panic!("cannot open raft storage at {}: {err}", dir.display())
            });
            Arc::new(storage)
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("raft storage mutex poisoned")
    }

    /// The fallible body of [`RaftWAL::save_term_and_vote`]: atomic
    /// replace of the `term` file.
    fn persist_term(&self, record: &TermRecord) -> io::Result<()> {
        let bytes = serde_json::to_vec(record).expect("a TermRecord always serializes");
        wal::atomic_replace(&self.dir, "term", &bytes)
    }

    /// The fallible body of [`RaftWAL::append`]: truncate the log at `from_index`'s
    /// recorded position, then append the framed records. `from_index` is absolute;
    /// the retained log begins at `snapshot_index + 1`.
    fn persist_append(&self, from_index: u64, entries: &[RaftEntry]) -> io::Result<()> {
        let mut inner = self.lock();
        let base = inner.state.snapshot_index;
        let from = from_index
            .checked_sub(base)
            .expect("append below the compacted prefix") as usize;
        assert!(
            from <= inner.log.len(),
            "append at index {from_index} beyond a log of {} entries (base {base})",
            inner.log.len()
        );
        // Truncation: drop any conflicting suffix (durable before the new entries land),
        // and mirror it in memory. A no-op when appending at the end.
        inner.log.truncate(from)?;
        inner.state.log.truncate(from);

        // Entry at local position `from + i` has absolute index
        // `base + (from + i) + 1` (the log is 1-based above the snapshot). Clone each
        // entry once into the indexed batch, append it durably, then move the entries
        // out of the batch into the in-memory mirror — one clone per entry, not two.
        let records: Vec<(u64, RaftEntry)> = entries
            .iter()
            .enumerate()
            .map(|(i, entry)| (base + from as u64 + 1 + i as u64, entry.clone()))
            .collect();
        inner.log.append_batch(&records)?;
        for (_, entry) in records {
            inner.state.log.push(entry);
        }
        Ok(())
    }

    /// The fallible body of [`RaftWAL::save_snapshot`]: persist the snapshot file
    /// (durable *before* the prefix is dropped), then rewrite the log to the
    /// retained suffix.
    fn persist_snapshot(&self, index: u64, term: u64, data: &[u8]) -> io::Result<()> {
        let mut inner = self.lock();
        let base = inner.state.snapshot_index;
        // Persist the snapshot first: a crash here leaves a snapshot newer than the
        // log, which `open` self-heals; the reverse would lose the prefix.
        let record = SnapshotRecord {
            index,
            term,
            data: data.to_vec(),
        };
        wal::atomic_replace(
            &self.dir,
            "snapshot",
            &postcard::to_allocvec(&record).expect("a SnapshotRecord always serializes"),
        )?;

        // Drop the prefix the snapshot subsumes, then rewrite the log to what remains.
        // Mirrors `InMemoryRaftWAL`: a stale/duplicate index discards nothing.
        let drop = index.saturating_sub(base).min(inner.state.log.len() as u64) as usize;
        inner.state.log.drain(..drop);
        inner.state.snapshot_index = index;
        inner.state.snapshot_term = term;
        inner.state.snapshot = Some(data.to_vec());

        let retained: Vec<(u64, RaftEntry)> = inner
            .state
            .log
            .iter()
            .enumerate()
            .map(|(i, entry)| (index + i as u64 + 1, entry.clone()))
            .collect();
        inner.log.rewrite(&retained)?;
        Ok(())
    }
}

/// A retained log suffix must be contiguous from `snapshot_index + 1`; anything else
/// is corruption the recovery rule above does not cover, so fail loudly.
fn assert_contiguous(retained: &[(u64, RaftEntry)], snapshot_index: u64, dir: &Path) {
    for (offset, (index, _)) in retained.iter().enumerate() {
        let expected = snapshot_index + 1 + offset as u64;
        assert!(
            *index == expected,
            "non-contiguous raft log at {}: expected index {expected}, found {index}",
            dir.display()
        );
    }
}

impl RaftWAL for FileRaftWAL {
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

    fn append(&self, from_index: u64, entries: &[RaftEntry]) {
        self.persist_append(from_index, entries)
            .unwrap_or_else(|err| {
                panic!(
                    "raft log persistence failed at {}: {err} — a voter that cannot \
                 persist its log cannot safely continue",
                    self.dir.display()
                )
            });
    }

    fn save_snapshot(&self, index: u64, term: u64, data: &[u8]) {
        self.persist_snapshot(index, term, data)
            .unwrap_or_else(|err| {
                panic!(
                    "raft snapshot persistence failed at {}: {err} — a voter that cannot \
                     persist its snapshot cannot safely continue",
                    self.dir.display()
                )
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actor_cluster::EntryPayload;
    use actor_cluster::InMemoryRaftWAL;
    use std::fs::OpenOptions;
    use std::io::Write;

    fn entry(term: u64, payload: EntryPayload) -> RaftEntry {
        RaftEntry { term, payload }
    }

    /// A distinct opaque app payload (`tag` + node uid), standing in for the
    /// membership commands these storage round-trip/truncation tests used to
    /// carry — the engine treats `App` bytes opaquely, so any distinct,
    /// comparable value exercises the log machinery.
    fn app(tag: u8, uid: u64) -> EntryPayload {
        let mut bytes = vec![tag];
        bytes.extend_from_slice(&uid.to_le_bytes());
        EntryPayload::App(bytes)
    }

    fn node(uid: u64) -> NodeId {
        NodeId::new(uid)
    }

    #[test]
    fn state_round_trips_across_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftWAL::open(dir.path()).unwrap();
        storage.save_term_and_vote(3, Some(node(2)));
        storage.append(0, &[entry(1, EntryPayload::Noop), entry(3, app(0, 4))]);
        drop(storage);

        let reopened = FileRaftWAL::open(dir.path()).unwrap();
        let state = reopened.load();
        assert_eq!(state.term, 3);
        assert_eq!(state.voted_for, Some(node(2)));
        assert_eq!(
            state.log,
            vec![entry(1, EntryPayload::Noop), entry(3, app(0, 4)),],
        );
    }

    #[test]
    fn a_fresh_directory_loads_the_default_state() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftWAL::open(dir.path().join("sub")).unwrap();
        assert_eq!(storage.load(), PersistedRaft::default());
    }

    #[test]
    fn truncate_then_append_replaces_the_conflicting_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftWAL::open(dir.path()).unwrap();
        storage.append(
            0,
            &[
                entry(1, EntryPayload::Noop),
                entry(1, app(0, 4)),
                entry(1, app(1, 4)),
            ],
        );
        // Raft conflict resolution: overwrite from index 1 with a higher term.
        storage.append(1, &[entry(2, EntryPayload::Noop), entry(2, app(4, 4))]);
        drop(storage);

        let reopened = FileRaftWAL::open(dir.path()).unwrap();
        assert_eq!(
            reopened.load().log,
            vec![
                entry(1, EntryPayload::Noop),
                entry(2, EntryPayload::Noop),
                entry(2, app(4, 4)),
            ],
        );
    }

    #[test]
    fn a_snapshot_compacts_the_prefix_and_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftWAL::open(dir.path()).unwrap();
        storage.append(
            0,
            &[
                entry(1, EntryPayload::Noop), // index 1
                entry(1, app(1, 9)),          // index 2
                entry(2, app(2, 9)),          // index 3
                entry(2, app(3, 9)),          // index 4
            ],
        );
        // Compact through index 2: indices 1..=2 are subsumed by the snapshot.
        storage.save_snapshot(2, 1, b"state@2");
        // A fresh append lands contiguously at absolute index 5.
        storage.append(4, &[entry(3, app(5, 9))]);
        drop(storage);

        let reopened = FileRaftWAL::open(dir.path()).unwrap();
        let state = reopened.load();
        assert_eq!(state.snapshot_index, 2);
        assert_eq!(state.snapshot_term, 1);
        assert_eq!(state.snapshot.as_deref(), Some(&b"state@2"[..]));
        // Only the retained suffix (indices 3, 4, 5) survives in the log.
        assert_eq!(
            state.log,
            vec![
                entry(2, app(2, 9)),
                entry(2, app(3, 9)),
                entry(3, app(5, 9))
            ],
        );
    }

    #[test]
    fn a_torn_tail_is_discarded_and_appends_continue() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftWAL::open(dir.path()).unwrap();
        storage.append(0, &[entry(1, EntryPayload::Noop), entry(1, app(3, 9))]);
        drop(storage);

        // A torn write: garbage lands after the valid records (a record whose
        // write never completed).
        let log_path = dir.path().join("log");
        let mut file = OpenOptions::new().append(true).open(&log_path).unwrap();
        file.write_all(&[0x12, 0x34, 0x56]).unwrap();
        drop(file);

        let reopened = FileRaftWAL::open(dir.path()).unwrap();
        assert_eq!(
            reopened.load().log,
            vec![entry(1, EntryPayload::Noop), entry(1, app(3, 9))],
            "the torn tail is not part of the log",
        );
        // The recovery truncated the garbage; appends land cleanly after it.
        reopened.append(2, &[entry(2, EntryPayload::Noop)]);
        drop(reopened);
        let again = FileRaftWAL::open(dir.path()).unwrap();
        assert_eq!(again.load().log.len(), 3);
    }

    #[test]
    fn a_record_cut_mid_payload_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftWAL::open(dir.path()).unwrap();
        storage.append(
            0,
            &[entry(1, EntryPayload::Noop), entry(1, EntryPayload::Noop)],
        );
        drop(storage);

        // Cut the file mid-record, as a crash during a write would.
        let log_path = dir.path().join("log");
        let len = fs::metadata(&log_path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&log_path).unwrap();
        file.set_len(len - 3).unwrap();
        drop(file);

        let reopened = FileRaftWAL::open(dir.path()).unwrap();
        assert_eq!(
            reopened.load().log,
            vec![entry(1, EntryPayload::Noop)],
            "the half-written record is dropped; the valid prefix survives",
        );
    }

    #[test]
    fn a_corrupted_checksum_ends_the_valid_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftWAL::open(dir.path()).unwrap();
        storage.append(
            0,
            &[entry(1, EntryPayload::Noop), entry(1, EntryPayload::Noop)],
        );
        drop(storage);

        // Flip a byte inside the second record's payload. Each frame is
        // `[u32 len][payload][u64 checksum]`, so the second frame starts just past
        // the first.
        let log_path = dir.path().join("log");
        let mut bytes = fs::read(&log_path).unwrap();
        let len0 = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let second_start = 4 + len0 + 8;
        bytes[second_start + 5] ^= 0xff;
        fs::write(&log_path, &bytes).unwrap();

        let reopened = FileRaftWAL::open(dir.path()).unwrap();
        assert_eq!(
            reopened.load().log.len(),
            1,
            "the corrupt record and after are dropped"
        );
    }

    /// The differential workhorse: drive the same operation sequence through
    /// `FileRaftWAL` — reopening from disk before every step — and
    /// `InMemoryRaftWAL`; `load()` must agree at every step. Covers the
    /// offset index, truncation, snapshot compaction, and reopen logic across
    /// interleavings.
    #[test]
    fn file_storage_matches_in_memory_storage_across_reopens() {
        enum Op {
            Save(u64, Option<u64>),
            Append(u64, Vec<RaftEntry>),
            Snapshot(u64, u64, Vec<u8>),
        }
        let ops = vec![
            Op::Save(1, Some(1)),
            Op::Append(0, vec![entry(1, EntryPayload::Noop)]),
            Op::Append(1, vec![entry(1, app(0, 4))]),
            Op::Save(2, None),
            Op::Save(2, Some(3)),
            // Conflict: overwrite index 1 onward at the new term.
            Op::Append(1, vec![entry(2, EntryPayload::Noop), entry(2, app(1, 4))]),
            Op::Append(3, vec![entry(2, app(2, 4))]),
            // Compact through index 2, then keep appending past the new base.
            Op::Snapshot(2, 2, b"snap@2".to_vec()),
            Op::Append(3, vec![entry(2, app(7, 4))]),
            Op::Append(4, vec![entry(3, app(8, 4))]),
            // A second compaction over the now-shorter log.
            Op::Snapshot(4, 3, b"snap@4".to_vec()),
            Op::Save(4, Some(2)),
            Op::Append(
                4,
                vec![
                    entry(4, EntryPayload::AddVoter(node(5))),
                    entry(4, app(4, 4)),
                ],
            ),
        ];

        let dir = tempfile::tempdir().unwrap();
        let mirror = InMemoryRaftWAL::new();
        for (step, op) in ops.iter().enumerate() {
            // A fresh open every step: the state must come back from disk.
            let file = FileRaftWAL::open(dir.path()).unwrap();
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
                Op::Snapshot(index, term, data) => {
                    file.save_snapshot(*index, *term, data);
                    mirror.save_snapshot(*index, *term, data);
                }
            }
            assert_eq!(file.load(), mirror.load(), "diverged after step {step}");
        }
        let final_state = FileRaftWAL::open(dir.path()).unwrap();
        assert_eq!(final_state.load(), mirror.load(), "diverged at the end");
    }
}
