//! Durable Raft state on the local filesystem (spec §9.4.3 item 2, §9).
//!
//! [`FileRaftWAL`] is the production [`RaftWAL`]: a voter's term, vote, log, and
//! state-machine snapshot survive a process restart, so a restarted voter can never
//! grant a second vote in a term it already voted in (election safety, invariant
//! #22) and comes back over its compacted log rather than a blank one. Each write
//! path carries the durability technique that fits it:
//!
//! - **`term`** — one JSON record `{term, voted_for}`, rewritten on every
//!   [`save_term_and_vote`](RaftWAL::save_term_and_vote) by atomic replace
//!   ([`wal::atomic_replace`]). A torn write is impossible: a reader sees either the
//!   old record or the new one. It stays JSON so that its parse failure is the
//!   corruption check protecting election safety. The stamp ahead of it
//!   (`actor.raft.term`, compatibility §3) answers a different question — that
//!   the bytes are this format at all — so a build skew is refused *by name*
//!   rather than reported as the corruption this JSON body exists to detect. The
//!   two have opposite fixes: roll the binary back, versus re-seed the node and
//!   lose its vote history. The reader also accepts the unstamped predecessor.
//! - **`log`** — a framed, checksummed append-only [`wal::Wal`] of `(absolute index,
//!   entry)` records. Carrying the absolute index makes a crash mid-compaction
//!   self-healing (below). Raft's truncate-then-append maps to
//!   [`Wal::truncate`](wal::Wal::truncate) at the entry's recorded offset followed by
//!   [`Wal::append_batch`](wal::Wal::append_batch); both fsync before returning.
//! - **`snapshot`** — one postcard record `{index, term, data}` for the compacted
//!   prefix (§9), rewritten by the same atomic replace as `term`. Written *before*
//!   the log prefix it subsumes is dropped, so a crash can leave a snapshot newer
//!   than the log but never the reverse. Stamped as `actor.raft.snapshot`
//!   (compatibility §3), with the reader accepting the unstamped predecessor. This
//!   is the boundary where a *missing* stamp was most dangerous: an unstamped
//!   `postcard` body has no first byte a reader can reject, so a wrong-format file
//!   decodes to a plausible index and `open` then discards the log prefix below it.
//!
//! **Recovery.** At [`open`](FileRaftWAL::open), the snapshot is loaded, then the log
//! is recovered by [`Wal::open`](wal::Wal::open) (which discards a torn tail). Records
//! whose absolute index is `≤` the snapshot index are discarded too, the self-heal
//! for a crash between persisting a snapshot and rewriting the log. A corrupt
//! `term` or `snapshot` file is a hard error: silently resetting either could violate
//! safety, so only the operator may resolve it.
//!
//! **Failure policy.** A voter whose state cannot be made durable cannot safely
//! continue: it might announce un-persisted state — vote twice in a term, or
//! acknowledge an entry it cannot replay. An I/O error after open therefore **poisons**
//! this WAL: the reason is recorded and logged once, every later write answers
//! [`WalAck::Failed`] without touching the disk, and the engine steps the node out of
//! consensus for good (it stops voting, leading, and acknowledging appends).
//!
//! It does *not* panic, and the reason is specific to this process. An unwind here
//! does not reliably stop a voter: a panic inside an actor is caught by supervision
//! and becomes a restart, and one inside a spawned consensus loop kills that task
//! alone. Either leaves the node up, gossiping, and counted in quorums with nothing
//! durable behind it — which is the exact failure this policy exists to prevent, made
//! harder to see. Poisoning stops the voter *and* leaves it answering, so peers elect
//! around it at once and [`poisoned`](RaftWAL::poisoned) tells an operator which node
//! to replace.
//!
//! Poisoning is one-way. Nothing here can establish that the volume recovered, and a
//! log that resumed after a gap would be worse than one that stopped: the records
//! either side of the gap would look contiguous.
//!
//! **Single writer.** A storage directory must belong to one process at a time.
//! Not enforced (advisory locking needs a newer toolchain than the workspace
//! MSRV); the [`factory`](FileRaftWAL::factory) layout gives each node its own
//! subdirectory.

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
use actor_cluster::WalAck;
use actor_core::NodeId;
use serde::Deserialize;
use serde::Serialize;
use wal::Wal;

/// Upper bound on one framed record's payload, as a sanity check while
/// scanning: a length above this is treated as corruption, not an allocation.
const MAX_RECORD: u32 = 1 << 20;

/// The schema revision of this log's `(index, RaftEntry)` records, stamped into the
/// log's header (compatibility spec §3).
///
/// A node that cannot read its own log cannot rejoin without losing committed
/// state, so bumping this follows **V4** without exception: widen the accepted
/// range one release before anything writes the new revision.
const LOG_RECORDS: compat::Window = compat::Window::at("actor.raft.log", 1);

/// The stamp on the `term` file (compatibility spec §3, `actor.raft.term`).
///
/// `\x89RFTERM\n` is `wal::MAGIC`'s convention, and the discrimination it buys
/// here is total in both directions — the rare case. A `term` file that predates
/// the stamp is `serde_json` of a struct, so it always begins `{`, and no JSON
/// value may begin `0x89`. A stamped reader can never take the predecessor for a
/// stamp, and a build predating the stamp fails at byte 0 rather than parsing part
/// of one.
const TERM: compat::Stamp =
    compat::Stamp::new(b"\x89RFTERM\n", compat::Window::at("actor.raft.term", 1));

/// The critical extension keys the `term` file implements: none yet.
const TERM_EXT_KNOWN: &[u16] = &[];

/// The `term` file's content (spec §9.4.3 item 2): the current term and the
/// vote cast in it, always written together — they are one atomic decision.
#[derive(Serialize, Deserialize)]
struct TermRecord {
    term: u64,
    voted_for: Option<NodeId>,
    /// Room to grow without a revision bump (compatibility spec §2.1).
    ///
    /// JSON already tolerates a field a reader does not know, so the *ancillary*
    /// half of the rule comes free. What it cannot express is the other half —
    /// "a reader that does not know this MUST refuse" — and the term file is
    /// exactly where a silently-ignored field would cost election safety. A
    /// pre-vote term or a lease recorded here and skipped by an older build is a
    /// double vote waiting to happen.
    ///
    /// `default` is what makes adoption free: a legacy record with no such field
    /// decodes into this struct with no second decoder anywhere.
    /// `skip_serializing_if` keeps revision 1's bytes identical to what this file
    /// held before the stamp, which the corpus fixture then pins.
    #[serde(default, skip_serializing_if = "compat::Extensions::is_empty")]
    ext: compat::Extensions,
}

/// The stamp on the `snapshot` file (compatibility spec §3, `actor.raft.snapshot`).
///
/// The magic buys less here than at any other boundary in the tree, and it is
/// worth being plain about why. A `snapshot` file predating the stamp begins with
/// a `postcard` varint, so *every* byte is a possible first byte of one: no magic
/// can make the older decoder safe against stamped bytes. Read as a
/// [`SnapshotRecord`], `\x89RFSNAP\n` decodes to `index = 10505`, `term = 70`, and
/// a payload length of 83 — and `postcard::from_bytes` ignores trailing bytes, so
/// a snapshot with 83 bytes of state or more decodes *successfully* to a wrong
/// index, after which [`FileRaftWAL::open`] drops every log record at or below it.
/// Committed entries, gone silently.
///
/// What the magic buys is the forward direction, which is the one this build
/// controls: a reader that has it never takes a stamped file for an unstamped one.
/// The backward direction is why the writer flips a release later than the reader
/// (**V4**), and `a_stamped_snapshot_misparses_under_the_older_decoder` pins the
/// hazard so the ordering is not mistaken for ceremony.
const SNAPSHOT: compat::Stamp = compat::Stamp::new(
    b"\x89RFSNAP\n",
    compat::Window::at("actor.raft.snapshot", 1),
);

/// The critical extension keys the `snapshot` file implements: none yet.
const SNAPSHOT_EXT_KNOWN: &[u16] = &[];

/// The `snapshot` file's content behind the stamp (spec §9): the compacted
/// prefix's last index and term, and the application snapshot taken at it.
///
/// A second type rather than a field added to [`SnapshotRecord`], because
/// `postcard` is positional: adding one there would change what every stored
/// snapshot means, and **V4** wants the previous definition kept behind its own
/// decoder anyway.
#[derive(Serialize, Deserialize)]
struct SnapshotBody {
    index: u64,
    term: u64,
    data: Vec<u8>,
    /// Room to grow without a revision bump (compatibility spec §2.1). Mandatory
    /// here: the body is positional, so without it every added field is a revision
    /// with a second decoder to keep.
    ext: compat::Extensions,
}

/// The `snapshot` file's content **before** it carried a stamp: the shape a
/// reader adopts, kept verbatim so the predecessor decodes exactly as it did.
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
    /// Why this WAL stopped accepting writes, or `None` while healthy.
    ///
    /// **One-way, and store-wide.** One-way because nothing here can establish that
    /// the volume recovered, and a Raft log that resumed writing after a gap would be
    /// worse than one that stopped: the entries written either side of the gap would
    /// look contiguous. Store-wide because the failures it catches — a full or
    /// read-only filesystem, a device that has stopped acknowledging — are properties
    /// of the volume, not of the record that happened to hit them.
    poison: Mutex<Option<String>>,
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
                // Two questions, asked in this order and answered differently. Bytes
                // carrying the magic are *this* format, so a revision outside the
                // window is refused as a version skew (**V2**) and never handed to
                // the JSON decoder; only bytes without it are the unstamped
                // predecessor, which JSON reads exactly as it always did.
                let body = if TERM.is_stamped(&bytes) {
                    let (_, body) = TERM.unstamp(&bytes).map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "raft term file {} was written by a different build \
                                 ({err}); this is a version skew, not corruption — run \
                                 this node on a build that accepts it, and keep the \
                                 node's state",
                                term_path.display()
                            ),
                        )
                    })?;
                    body
                } else {
                    &bytes[..]
                };
                let record: TermRecord = serde_json::from_slice(body).map_err(|err| {
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
                record
                    .ext
                    .admit(TERM.window().boundary(), TERM_EXT_KNOWN)
                    .map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("raft term file {}: {err}", term_path.display()),
                        )
                    })?;
                (record.term, record.voted_for)
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => (0, None),
            Err(err) => return Err(err),
        };

        let snapshot_path = dir.join("snapshot");
        let (snapshot_index, snapshot_term, snapshot) = match fs::read(&snapshot_path) {
            // As with the term file: the magic decides which decoder runs, so a
            // revision this build refuses is a named skew (**V2**) rather than
            // bytes fed to the predecessor's decoder.
            Ok(bytes) if SNAPSHOT.is_stamped(&bytes) => {
                let (_, body) = SNAPSHOT.unstamp(&bytes).map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "raft snapshot file {} was written by a different build \
                             ({err}); this is a version skew, not corruption — run this \
                             node on a build that accepts it, and keep the node's state",
                            snapshot_path.display()
                        ),
                    )
                })?;
                let record: SnapshotBody = postcard::from_bytes(body).map_err(|err| {
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
                record
                    .ext
                    .admit(SNAPSHOT.window().boundary(), SNAPSHOT_EXT_KNOWN)
                    .map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("raft snapshot file {}: {err}", snapshot_path.display()),
                        )
                    })?;
                (record.index, record.term, Some(record.data))
            }
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
        let (mut log, records) =
            Wal::<(u64, RaftEntry)>::open(dir.join("log"), MAX_RECORD, &LOG_RECORDS)
                .map_err(|e| e.into_io())?;

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
        // primitive (`Wal::open` for `log`, `atomic_replace` for `term`/`snapshot`)
        // that makes its own entry durable, and this layout creates no subdirectory.

        Ok(FileRaftWAL {
            dir,
            poison: Mutex::new(None),
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
    /// subdirectory, so a node's several Raft groups never share a log. Panics if
    /// a directory cannot be opened: a voter without durable storage must not
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

    /// Poison this WAL and report the failure to the engine, which steps the node out
    /// of consensus (see [`RaftWAL`]'s failure policy). The first reason is kept: it
    /// is the one that describes the volume, and everything after it is a consequence.
    ///
    /// Logged to stderr as well as recorded, because a node that has stopped voting
    /// looks from the outside like a node that is merely slow, and the difference is
    /// the whole operational question.
    fn fail(&self, why: String) -> WalAck {
        let mut poison = self.poison.lock().expect("raft storage poison mutex");
        if poison.is_none() {
            eprintln!("[raft] {why}");
            *poison = Some(why);
        }
        WalAck::Failed
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
        // `base + (from + i) + 1` (the log is 1-based above the snapshot).
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

    fn save_term_and_vote(&self, term: u64, voted_for: Option<NodeId>) -> WalAck {
        if self.poisoned().is_some() {
            return WalAck::Failed;
        }
        let record = TermRecord {
            term,
            voted_for,
            ext: compat::Extensions::new(),
        };
        if let Err(err) = self.persist_term(&record) {
            return self.fail(format!(
                "raft term persistence failed at {}: {err} — a voter that cannot \
                 persist its vote cannot safely continue",
                self.dir.display()
            ));
        }
        // In memory only after the disk write landed, so `load` after a failure still
        // reports the last state this node can actually reconstruct.
        let mut inner = self.lock();
        inner.state.term = term;
        inner.state.voted_for = voted_for;
        WalAck::Persisted
    }

    fn append(&self, from_index: u64, entries: &[RaftEntry]) -> WalAck {
        if self.poisoned().is_some() {
            return WalAck::Failed;
        }
        if let Err(err) = self.persist_append(from_index, entries) {
            return self.fail(format!(
                "raft log persistence failed at {}: {err} — a voter that cannot \
                 persist its log cannot safely continue",
                self.dir.display()
            ));
        }
        WalAck::Persisted
    }

    fn poisoned(&self) -> Option<String> {
        self.poison
            .lock()
            .expect("raft storage poison mutex")
            .clone()
    }

    fn save_snapshot(&self, index: u64, term: u64, data: &[u8]) -> WalAck {
        if self.poisoned().is_some() {
            return WalAck::Failed;
        }
        if let Err(err) = self.persist_snapshot(index, term, data) {
            return self.fail(format!(
                "raft snapshot persistence failed at {}: {err} — a voter that cannot \
                 persist its snapshot cannot safely continue",
                self.dir.display()
            ));
        }
        WalAck::Persisted
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

    /// A distinct opaque app payload (`tag` + node uid). The engine treats `App`
    /// bytes opaquely, so any distinct, comparable value will do.
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
        assert!(storage.save_term_and_vote(3, Some(node(2))).persisted());
        assert!(
            storage
                .append(0, &[entry(1, EntryPayload::Noop), entry(3, app(0, 4))])
                .persisted()
        );
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
        assert!(
            storage
                .append(
                    0,
                    &[
                        entry(1, EntryPayload::Noop),
                        entry(1, app(0, 4)),
                        entry(1, app(1, 4)),
                    ],
                )
                .persisted()
        );
        // Raft conflict resolution: overwrite from index 1 with a higher term.
        assert!(
            storage
                .append(1, &[entry(2, EntryPayload::Noop), entry(2, app(4, 4))])
                .persisted()
        );
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
        assert!(
            storage
                .append(
                    0,
                    &[
                        entry(1, EntryPayload::Noop), // index 1
                        entry(1, app(1, 9)),          // index 2
                        entry(2, app(2, 9)),          // index 3
                        entry(2, app(3, 9)),          // index 4
                    ],
                )
                .persisted()
        );
        // Compact through index 2: indices 1..=2 are subsumed by the snapshot.
        assert!(storage.save_snapshot(2, 1, b"state@2").persisted());
        // A fresh append lands contiguously at absolute index 5.
        assert!(storage.append(4, &[entry(3, app(5, 9))]).persisted());
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
        assert!(
            storage
                .append(0, &[entry(1, EntryPayload::Noop), entry(1, app(3, 9))])
                .persisted()
        );
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
        assert!(
            reopened
                .append(2, &[entry(2, EntryPayload::Noop)])
                .persisted()
        );
        drop(reopened);
        let again = FileRaftWAL::open(dir.path()).unwrap();
        assert_eq!(again.load().log.len(), 3);
    }

    #[test]
    fn a_record_cut_mid_payload_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftWAL::open(dir.path()).unwrap();
        assert!(
            storage
                .append(
                    0,
                    &[entry(1, EntryPayload::Noop), entry(1, EntryPayload::Noop)],
                )
                .persisted()
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
        assert!(
            storage
                .append(
                    0,
                    &[entry(1, EntryPayload::Noop), entry(1, EntryPayload::Noop)],
                )
                .persisted()
        );
        drop(storage);

        // Flip a byte inside the second record's payload. Frames follow the file
        // header (wal §2.1) and each is `[u32 len][payload][u64 checksum]`, so the
        // second frame starts just past the first.
        let log_path = dir.path().join("log");
        let mut bytes = fs::read(&log_path).unwrap();
        let first = wal::HEADER_LEN;
        let len0 = u32::from_le_bytes(bytes[first..first + 4].try_into().unwrap()) as usize;
        let second_start = first + 4 + len0 + 8;
        bytes[second_start + 5] ^= 0xff;
        fs::write(&log_path, &bytes).unwrap();

        let reopened = FileRaftWAL::open(dir.path()).unwrap();
        assert_eq!(
            reopened.load().log.len(),
            1,
            "the corrupt record and after are dropped"
        );
    }

    /// Drive the same operation sequence through `FileRaftWAL`, reopening from
    /// disk before every step, and through `InMemoryRaftWAL`; `load()` must agree
    /// at every step. Covers the offset index, truncation, snapshot compaction,
    /// and reopen logic across interleavings.
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
                    assert!(file.save_term_and_vote(*term, vote).persisted());
                    assert!(mirror.save_term_and_vote(*term, vote).persisted());
                }
                Op::Append(from, entries) => {
                    assert!(file.append(*from, entries).persisted());
                    assert!(mirror.append(*from, entries).persisted());
                }
                Op::Snapshot(index, term, data) => {
                    assert!(file.save_snapshot(*index, *term, data).persisted());
                    assert!(mirror.save_snapshot(*index, *term, data).persisted());
                }
            }
            assert_eq!(file.load(), mirror.load(), "diverged after step {step}");
        }
        let final_state = FileRaftWAL::open(dir.path()).unwrap();
        assert_eq!(final_state.load(), mirror.load(), "diverged at the end");
    }

    // --- Golden corpus (compatibility spec §4) --------------------------------

    /// The corpus log records, and they must never change: the checked-in bytes
    /// *are* these values.
    ///
    /// One of each [`EntryPayload`] variant, because `postcard` encodes a variant
    /// as a positional discriminant — inserting a variant anywhere but the end
    /// silently reinterprets every entry written under the ones after it. `App`
    /// carries the `serde_bytes` shape whose encoding must stay byte-identical
    /// (see its own note), so the fixture pins that too.
    fn corpus_log() -> Vec<(u64, RaftEntry)> {
        vec![
            (1, entry(1, EntryPayload::Noop)),
            (2, entry(1, EntryPayload::AddVoter(node(2)))),
            (3, entry(2, EntryPayload::RemoveVoter(node(3)))),
            (4, entry(2, app(0xA5, 9))),
        ]
    }

    #[test]
    fn actor_raft_log_v1_still_recovers_its_entries() {
        let bytes = crate::corpus::golden("actor.raft.log", 1, || {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("log");
            let (mut log, _) = wal::Wal::<(u64, RaftEntry)>::open(&path, MAX_RECORD, &LOG_RECORDS)
                .expect("open a fresh log");
            log.append_batch(&corpus_log()).expect("append");
            drop(log);
            std::fs::read(&path).expect("read back the produced log")
        });

        // Staged, never opened in place: `Wal::open` truncates a torn tail, so a
        // build that could not read the fixture would rewrite it and erase the
        // failure it is here to catch.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        std::fs::write(&path, &bytes).unwrap();

        let (_log, records) = wal::Wal::<(u64, RaftEntry)>::open(&path, MAX_RECORD, &LOG_RECORDS)
            .expect("this build must read an actor.raft.log v1 log it accepts");
        assert_eq!(
            records,
            corpus_log(),
            "a node cannot read its own log: the entry schema moved without a \
             revision bump (compatibility V4, §3.2.1)",
        );
    }

    /// A `term` file in the shape it had before the format carried a stamp.
    fn legacy_term(term: u64, voted_for: Option<NodeId>) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "term": term, "voted_for": voted_for }))
            .expect("encode a legacy term record")
    }

    /// A stamped `term` file, encoded the way this build reads one. Written here
    /// rather than taken from `persist_term`, because the writer still emits the
    /// unstamped form until the release that flips it (**V4**).
    fn stamped_term(term: u64, voted_for: Option<NodeId>) -> Vec<u8> {
        let body = serde_json::to_vec(&TermRecord {
            term,
            voted_for,
            ext: compat::Extensions::new(),
        })
        .expect("encode a term record");
        TERM.stamp(&body)
    }

    #[test]
    fn actor_raft_term_v1_still_loads_its_term_and_vote() {
        let bytes = crate::corpus::golden("actor.raft.term", 1, || stamped_term(7, Some(node(3))));

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("term"), &bytes).expect("stage the fixture");

        let wal =
            FileRaftWAL::open(dir.path()).expect("this build must read an actor.raft.term v1");
        let state = wal.load();
        assert_eq!(state.term, 7);
        assert_eq!(state.voted_for, Some(node(3)));
    }

    /// A `term` file predating the stamp is *adopted*: read by the JSON decoder it
    /// always had. Refusing would make the stamp a migration for every node, and
    /// the operator advice attached to that refusal destroys vote history.
    #[test]
    fn an_unstamped_term_file_is_adopted_rather_than_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("term"), legacy_term(4, Some(node(2))))
            .expect("stage a legacy record");

        let wal = FileRaftWAL::open(dir.path()).expect("an unstamped term file must still load");
        let state = wal.load();
        assert_eq!(state.term, 4);
        assert_eq!(state.voted_for, Some(node(2)));
    }

    /// A version skew and a corrupt file must not read alike: their fixes are
    /// opposite. Corruption says restore or remove the node's state, which throws
    /// away its vote history; a skew says roll the binary back and *keep* it. An
    /// operator who follows the wrong one destroys recoverable state.
    #[test]
    fn a_term_from_another_revision_is_refused_as_a_revision_not_as_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = stamped_term(7, Some(node(3)));
        let head = TERM.magic().len();
        bytes[head..head + 2].copy_from_slice(&9u16.to_le_bytes());
        std::fs::write(dir.path().join("term"), &bytes).expect("stage a future revision");

        let Err(err) = FileRaftWAL::open(dir.path()) else {
            panic!("a revision this build does not accept must not open");
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let message = err.to_string();
        assert!(
            message.contains("actor.raft.term") && message.contains("v9"),
            "the refusal must name the boundary and what it found: {message}",
        );
        assert!(
            !message.contains("remove the"),
            "a version skew must not carry the corruption advice, which discards \
             the node's vote history: {message}",
        );
    }

    /// **V4**, read-new first: this release reads both forms and still writes the
    /// unstamped one, so a rollback onto the previous build finds a `term` file it
    /// can parse. Inverts in the release that flips the writer.
    #[test]
    fn a_term_is_still_written_unstamped_until_the_write_release() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileRaftWAL::open(dir.path()).expect("open");
        assert_eq!(wal.save_term_and_vote(5, Some(node(1))), WalAck::Persisted);

        let bytes = std::fs::read(dir.path().join("term")).expect("read the term file");
        assert!(
            !TERM.is_stamped(&bytes),
            "the term file must still be written unstamped in the read release",
        );
        assert!(bytes.starts_with(b"{"), "and still be plain JSON");
    }

    /// A `snapshot` file in the shape it had before the format carried a stamp.
    fn legacy_snapshot(index: u64, term: u64, data: Vec<u8>) -> Vec<u8> {
        postcard::to_allocvec(&SnapshotRecord { index, term, data })
            .expect("encode a legacy snapshot record")
    }

    /// A stamped `snapshot` file, encoded the way this build reads one. Written
    /// here rather than taken from `persist_snapshot`, because the writer still
    /// emits the unstamped form until the release that flips it (**V4**).
    fn stamped_snapshot(index: u64, term: u64, data: Vec<u8>) -> Vec<u8> {
        let body = postcard::to_allocvec(&SnapshotBody {
            index,
            term,
            data,
            ext: compat::Extensions::new(),
        })
        .expect("encode a snapshot body");
        SNAPSHOT.stamp(&body)
    }

    #[test]
    fn actor_raft_snapshot_v1_still_loads_its_compacted_prefix() {
        // The payload is deliberately longer than 83 bytes, which is what the
        // magic's own bytes decode as a length under the older decoder. That makes
        // this fixture double as the input to
        // `a_stamped_snapshot_misparses_under_the_older_decoder`: shorten it and
        // that test stops demonstrating anything.
        let data: Vec<u8> = (0u8..=199).collect();
        let bytes = crate::corpus::golden("actor.raft.snapshot", 1, || {
            stamped_snapshot(4, 2, data.clone())
        });

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot"), &bytes).expect("stage the fixture");

        let wal = FileRaftWAL::open(dir.path())
            .expect("this build must read an actor.raft.snapshot v1 it accepts");
        let state = wal.load();
        assert_eq!(state.snapshot_index, 4);
        assert_eq!(state.snapshot_term, 2);
        assert_eq!(state.snapshot.as_deref(), Some(&data[..]));
    }

    #[test]
    fn an_unstamped_snapshot_file_is_adopted_rather_than_refused() {
        let dir = tempfile::tempdir().unwrap();
        let data = b"the compacted prefix".to_vec();
        std::fs::write(
            dir.path().join("snapshot"),
            legacy_snapshot(6, 3, data.clone()),
        )
        .expect("stage a legacy record");

        let wal = FileRaftWAL::open(dir.path()).expect("an unstamped snapshot must still load");
        let state = wal.load();
        assert_eq!(state.snapshot_index, 6);
        assert_eq!(state.snapshot_term, 3);
        assert_eq!(state.snapshot.as_deref(), Some(&data[..]));
    }

    #[test]
    fn a_snapshot_from_another_revision_is_refused_as_a_revision_not_as_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = stamped_snapshot(4, 2, vec![7; 90]);
        let head = SNAPSHOT.magic().len();
        bytes[head..head + 2].copy_from_slice(&9u16.to_le_bytes());
        std::fs::write(dir.path().join("snapshot"), &bytes).expect("stage a future revision");

        let Err(err) = FileRaftWAL::open(dir.path()) else {
            panic!("a revision this build does not accept must not open");
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let message = err.to_string();
        assert!(
            message.contains("actor.raft.snapshot") && message.contains("v9"),
            "the refusal must name the boundary and what it found: {message}",
        );
        assert!(
            !message.contains("remove the"),
            "a version skew must not carry the corruption advice: {message}",
        );
    }

    /// Why the writer flips a release after the reader (**V4**), demonstrated
    /// rather than asserted.
    ///
    /// A stamped snapshot handed to the decoder that predates the stamp does not
    /// fail — it *succeeds*, at a fabricated index, because the magic's own bytes
    /// are a valid `postcard` prefix and `from_bytes` ignores whatever trails. A
    /// build reading that would then drop every log record at or below index
    /// 10505: committed entries destroyed with no diagnostic. Nothing but the
    /// release ordering stands between that and a rollback.
    #[test]
    fn a_stamped_snapshot_misparses_under_the_older_decoder() {
        let bytes = stamped_snapshot(4, 2, (0u8..=199).collect());
        let misread: SnapshotRecord = postcard::from_bytes(&bytes)
            .expect("the older decoder accepts these bytes — that is the hazard");

        assert_eq!(
            misread.index, 10505,
            "the magic's leading bytes decode as this index, and `open` would \
             discard every log record at or below it",
        );
        assert_ne!(misread.index, 4, "and it is not the index that was written");
    }

    /// **V4**, read-new first: this release reads both forms and still writes the
    /// unstamped one. Inverts in the release that flips the writer.
    #[test]
    fn a_snapshot_is_still_written_unstamped_until_the_write_release() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileRaftWAL::open(dir.path()).expect("open");
        assert!(wal.append(0, &[entry(1, app(1, 9))]).persisted());
        assert!(wal.save_snapshot(1, 1, b"state@1").persisted());

        let bytes = std::fs::read(dir.path().join("snapshot")).expect("read the snapshot file");
        assert!(
            !SNAPSHOT.is_stamped(&bytes),
            "the snapshot must still be written unstamped in the read release",
        );
    }
}
