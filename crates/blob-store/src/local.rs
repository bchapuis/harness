//! The `Local` tier: a single-node, on-disk content-addressed store (spec §5.1).
//!
//! This is the embedded, test, and simulator tier, and it is also the per-node
//! engine the `Clustered` tier's replica actor owns (spec §6) — so its mutating
//! operations are exposed as plain synchronous methods ([`store`](LocalBlobStore::store),
//! [`fetch`](LocalBlobStore::fetch), …) that both the [`BlobStore`] impl here and
//! the replica wrap.
//!
//! A blob is written to `blobs/<ns>/<hh>/<hex>` via the `wal` atomic-replace
//! discipline (wal §5): write a temp file, fsync, rename onto the final path,
//! fsync the directory — so a reader sees either the whole blob or no blob, never
//! a torn one. `<ns>` is the namespace (the unit of deletion), and `<hh>` is the
//! first hex byte of the content hash, fanning each namespace so no directory
//! grows unbounded. Because the path *is* the namespace plus the content hash, a
//! `put` of an already-present blob is a no-op (the file exists), giving B2 for
//! free; a `get` verifies what it read (spec §4, B1), so on-disk bit-rot surfaces
//! as [`BlobError::Corrupt`], never as wrong bytes.
//!
//! Deletion is a **tombstone** plus a sweep (spec §5.3): `delete_namespace`
//! records a tombstone for `<ns>` (so a later `put` into it is refused) and then
//! removes the `blobs/<ns>` subtree. A partial removal interrupted by a crash is
//! harmless and re-driven, because the tombstone — not the presence of files —
//! makes the namespace gone. `Local` is CP trivially (one store, one writer) and
//! cannot survive losing that node's disk.

use std::fs;
use std::future::Future;
use std::io;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::blob::BlobError;
use crate::blob::BlobId;
use crate::blob::BlobStore;
use crate::blob::Namespace;
use crate::blob::slice;
use crate::blob::verify;

/// Bytes of a tombstone record on disk: `deleted_at` (u64 LE) followed by its
/// checksum (u64 LE), mirroring the grain store's fence file (granary
/// `file_store`). The checksum guards against a torn read, though `atomic_replace`
/// already makes the write whole-or-nothing.
const TOMBSTONE_BYTES: usize = 16;

/// A single-node, on-disk content-addressed store (spec §5.1).
///
/// Clone is cheap — the handle is an `Arc<Inner>` — so it satisfies the
/// [`BlobStore`] `Clone` bound and can be shared between the tier and the replica
/// actor that owns one of these per node.
#[derive(Clone)]
pub struct LocalBlobStore {
    inner: Arc<Inner>,
}

struct Inner {
    /// `<root>/blobs`: the `<ns>/<hh>/<hex>` tree of stored blobs.
    blobs: PathBuf,
    /// `<root>/tombstones`: one tiny file per deleted namespace.
    tombstones: PathBuf,
    /// Serializes mutations (blob writes and deletes) so a `put` and a
    /// `delete_namespace` of the same namespace cannot interleave: a `put` either
    /// completes before the tombstone (then the sweep removes it) or observes the
    /// tombstone and is refused (spec §5.3, B7). Reads stay lock-free — the atomic
    /// rename gives them a whole blob or none.
    write: Mutex<()>,
    /// A process-local, monotonic stamp source for `Local` tombstones. The
    /// `Clustered` tier supplies a clock-derived `deleted_at`; `Local` has no
    /// injected clock, and the value is informational here (tombstone *presence*,
    /// not its stamp, makes a namespace gone), so a counter keeps distinct deletes
    /// distinct and the on-disk store deterministic.
    deletes: AtomicU64,
}

impl LocalBlobStore {
    /// Open (creating if absent) the on-disk store rooted at `root`. Fsyncs the
    /// root so the `blobs/` and `tombstones/` directory entries survive a crash.
    pub fn open(root: impl Into<PathBuf>) -> io::Result<LocalBlobStore> {
        let root = root.into();
        let blobs = root.join("blobs");
        let tombstones = root.join("tombstones");
        fs::create_dir_all(&blobs)?;
        fs::create_dir_all(&tombstones)?;
        wal::sync_dir(&root)?;
        Ok(LocalBlobStore {
            inner: Arc::new(Inner {
                blobs,
                tombstones,
                write: Mutex::new(()),
                deletes: AtomicU64::new(0),
            }),
        })
    }

    /// Store `bytes` under `(ns, id)`, where `id` is already known to be
    /// `BLAKE3(bytes)` (the caller computed it). Idempotent and dedup'd within the
    /// namespace (**B2**): if the blob's file already exists this writes nothing.
    /// Refuses with [`BlobError::Deleted`] if `ns` is tombstoned (spec §5.3).
    ///
    /// This is the engine the replica actor's `StoreBlob` handler calls (spec §6),
    /// and that [`BlobStore::put`] wraps after hashing.
    pub fn store(&self, ns: &Namespace, id: &BlobId, bytes: &[u8]) -> Result<(), BlobError> {
        let _guard = self.lock();
        if self.is_tombstoned(ns) {
            return Err(BlobError::Deleted(ns.clone()));
        }
        let ns_dir = ensure_dir(&self.inner.blobs, &ns.to_string())?;
        let hh_dir = ensure_dir(&ns_dir, &fanout(id))?;
        let name = id.to_string();
        if hh_dir.join(&name).exists() {
            return Ok(()); // B2: equal content already stored is a no-op.
        }
        wal::atomic_replace(&hh_dir, &name, bytes).map_err(unavailable)
    }

    /// Fetch `(ns, id)`, or a byte range of it (`None` = the whole blob),
    /// verifying the bytes against `id` before returning (spec §4, **B1**). A
    /// tombstoned namespace returns [`BlobError::Deleted`]; an absent blob is
    /// [`BlobError::Unavailable`] (on one node, no copy is reachable); on-disk
    /// bit-rot is [`BlobError::Corrupt`]. A range is served by reading and
    /// verifying the whole blob, then slicing (spec §2).
    pub fn fetch(
        &self,
        ns: &Namespace,
        id: &BlobId,
        range: Option<Range<u64>>,
    ) -> Result<Vec<u8>, BlobError> {
        if self.is_tombstoned(ns) {
            return Err(BlobError::Deleted(ns.clone()));
        }
        let bytes = match fs::read(self.blob_path(ns, id)) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                // A concurrent delete may have swept the file between the tombstone
                // check above and this read; re-check so the caller gets the
                // precise `Deleted` rather than a generic "not present".
                if self.is_tombstoned(ns) {
                    return Err(BlobError::Deleted(ns.clone()));
                }
                return Err(BlobError::Unavailable(format!("blob {id} not present")));
            }
            Err(err) => return Err(unavailable(err)),
        };
        verify(id, &bytes)?;
        Ok(slice(bytes, range))
    }

    /// Read the **whole** blob's raw on-disk bytes **without verifying** them,
    /// or `None` if the namespace is tombstoned or the blob is absent.
    ///
    /// This is what the replica actor's `FetchBlob` handler returns (spec §6): the
    /// bytes may be locally corrupt, and the *caller* re-hashes them after the
    /// network transfer (spec §4, §5.2, **B1**), so an owner that returned
    /// non-verifying bytes is distinguishable from one that had none — the
    /// distinction `get` needs to choose [`BlobError::Corrupt`] over
    /// [`BlobError::Unavailable`]. (Contrast [`fetch`](Self::fetch), which verifies
    /// for the single-node `Local` tier.) The `range` of a `FetchBlob` is reserved
    /// for range-verified streaming (spec §10); v1 returns the whole blob, because
    /// a range cannot be verified against the id without it.
    pub fn read_raw(&self, ns: &Namespace, id: &BlobId) -> Option<Vec<u8>> {
        if self.is_tombstoned(ns) {
            return None;
        }
        fs::read(self.blob_path(ns, id)).ok()
    }

    /// Whether `(ns, id)` is durably present. A tombstoned namespace reports
    /// `false`; otherwise presence is the existence of the blob's file (an atomic
    /// rename means a present file is a whole, durable blob).
    pub fn present(&self, ns: &Namespace, id: &BlobId) -> bool {
        !self.is_tombstoned(ns) && self.blob_path(ns, id).exists()
    }

    /// Record a tombstone for `ns` (durable) and sweep its blobs (spec §5.3).
    /// Idempotent and monotonic: re-tombstoning a namespace overwrites with the
    /// same meaning and re-sweeps harmlessly. The tombstone is written *before*
    /// the sweep, so even a crash mid-sweep leaves the namespace gone.
    pub fn tombstone(&self, ns: &Namespace, deleted_at: u64) -> Result<(), BlobError> {
        let _guard = self.lock();
        let mut record = [0u8; TOMBSTONE_BYTES];
        record[..8].copy_from_slice(&deleted_at.to_le_bytes());
        record[8..].copy_from_slice(&wal::checksum(&deleted_at.to_le_bytes()).to_le_bytes());
        wal::atomic_replace(&self.inner.tombstones, &ns.to_string(), &record)
            .map_err(unavailable)?;
        self.sweep(ns);
        Ok(())
    }

    /// Whether `ns` has been tombstoned. The tombstone file's presence is the
    /// answer (the atomic write makes it whole-or-absent).
    pub fn is_tombstoned(&self, ns: &Namespace) -> bool {
        self.inner.tombstones.join(ns.to_string()).exists()
    }

    /// Remove a namespace's blob subtree. Best-effort and re-drivable: an absent
    /// or partially-removed subtree is not an error, because the tombstone (not
    /// the files) defines the namespace as gone (spec §5.3).
    pub fn sweep(&self, ns: &Namespace) {
        let _ = fs::remove_dir_all(self.inner.blobs.join(ns.to_string()));
        let _ = wal::sync_dir(&self.inner.blobs);
    }

    /// Enumerate every blob this node holds on disk, as `(namespace, id)` pairs —
    /// what the reconcile loop walks to re-push under-replicated blobs to their
    /// current owners (spec §7, **B6**). Reconstructs each `Namespace` and
    /// [`BlobId`] from its hex path component; entries that do not decode (e.g. a
    /// stray temp file) are skipped. Best-effort: an I/O error mid-walk truncates
    /// the listing rather than failing, since reconcile re-runs each pass.
    pub fn blobs(&self) -> Vec<(Namespace, BlobId)> {
        let mut out = Vec::new();
        let Ok(ns_dirs) = fs::read_dir(&self.inner.blobs) else {
            return out;
        };
        for ns_dir in ns_dirs.flatten() {
            let Some(ns_bytes) = ns_dir.file_name().to_str().and_then(decode_hex) else {
                continue;
            };
            let ns = Namespace::new(ns_bytes);
            let Ok(fanouts) = fs::read_dir(ns_dir.path()) else {
                continue;
            };
            for fanout in fanouts.flatten() {
                let Ok(blobs) = fs::read_dir(fanout.path()) else {
                    continue;
                };
                for blob in blobs.flatten() {
                    if let Some(id) = blob.file_name().to_str().and_then(decode_id) {
                        out.push((ns.clone(), id));
                    }
                }
            }
        }
        // Sort so the listing is independent of `read_dir` order (which is
        // OS-dependent): reconcile then visits blobs in a stable order, keeping the
        // emitted event stream seed-reproducible (spec §8).
        out.sort();
        out
    }

    /// The on-disk path of a blob: `blobs/<ns>/<hh>/<hex>` (spec §5.1).
    pub(crate) fn blob_path(&self, ns: &Namespace, id: &BlobId) -> PathBuf {
        self.inner
            .blobs
            .join(ns.to_string())
            .join(fanout(id))
            .join(id.to_string())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.inner
            .write
            .lock()
            .expect("blob store write lock poisoned")
    }

    fn next_deleted_at(&self) -> u64 {
        self.inner.deletes.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// The two-hex-character fan-out directory for `id`: its first byte (spec §5.1).
fn fanout(id: &BlobId) -> String {
    format!("{:02x}", id.as_bytes()[0])
}

/// Decode a lowercase-hex string to bytes, the inverse of the [`Namespace`] and
/// [`BlobId`] `Display` used for on-disk path components. `None` on any non-hex or
/// odd-length input, so a stray file (e.g. a leftover temp) is skipped by
/// [`LocalBlobStore::blobs`] rather than mis-parsed.
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        out.push((hex_val(bytes[i])? << 4) | hex_val(bytes[i + 1])?);
        i += 2;
    }
    Some(out)
}

/// Decode a 64-hex-character blob-file name into a [`BlobId`]; `None` if it is not
/// exactly a 32-byte digest in hex.
fn decode_id(s: &str) -> Option<BlobId> {
    let bytes = decode_hex(s)?;
    let array: [u8; 32] = bytes.try_into().ok()?;
    Some(BlobId::from_bytes(array))
}

/// The value of one lowercase-hex digit, or `None` if `b` is not `0-9a-f`.
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}

/// Create `parent/name` if absent, fsyncing `parent` so the new directory entry
/// survives a crash, and return the child path. `wal::atomic_replace` later
/// fsyncs the leaf directory for the blob file itself, so this chains the
/// durability of every ancestor it creates.
fn ensure_dir(parent: &Path, name: &str) -> Result<PathBuf, BlobError> {
    let child = parent.join(name);
    if !child.exists() {
        fs::create_dir(&child).map_err(unavailable)?;
        wal::sync_dir(parent).map_err(unavailable)?;
    }
    Ok(child)
}

/// Map a local I/O failure to [`BlobError::Unavailable`]: on one node, a disk
/// error means the durability target could not be met.
fn unavailable(err: io::Error) -> BlobError {
    BlobError::Unavailable(err.to_string())
}

impl BlobStore for LocalBlobStore {
    fn put(
        &self,
        ns: &Namespace,
        bytes: Vec<u8>,
    ) -> impl Future<Output = Result<BlobId, BlobError>> + Send {
        let this = self.clone();
        let ns = ns.clone();
        async move {
            let id = BlobId::of(&bytes);
            this.store(&ns, &id, &bytes)?;
            Ok(id)
        }
    }

    fn get(
        &self,
        ns: &Namespace,
        id: &BlobId,
        range: Option<Range<u64>>,
    ) -> impl Future<Output = Result<Vec<u8>, BlobError>> + Send {
        let this = self.clone();
        let ns = ns.clone();
        let id = *id;
        async move { this.fetch(&ns, &id, range) }
    }

    fn has(
        &self,
        ns: &Namespace,
        id: &BlobId,
    ) -> impl Future<Output = Result<bool, BlobError>> + Send {
        let this = self.clone();
        let ns = ns.clone();
        let id = *id;
        async move { Ok(this.present(&ns, &id)) }
    }

    fn delete_namespace(
        &self,
        ns: &Namespace,
    ) -> impl Future<Output = Result<(), BlobError>> + Send {
        let this = self.clone();
        let ns = ns.clone();
        async move {
            let deleted_at = this.next_deleted_at();
            this.tombstone(&ns, deleted_at)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    fn store() -> (tempfile::TempDir, LocalBlobStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalBlobStore::open(dir.path()).expect("open");
        (dir, store)
    }

    fn ns() -> Namespace {
        Namespace::new(b"workspace-1".to_vec())
    }

    #[test]
    fn put_get_has_round_trip() {
        let (_dir, store) = store();
        let ns = ns();
        let bytes = b"a bounded block".to_vec();

        let id = block_on(store.put(&ns, bytes.clone())).expect("put");
        assert_eq!(id, BlobId::of(&bytes));
        assert_eq!(block_on(store.get(&ns, &id, None)), Ok(bytes));
        assert_eq!(block_on(store.has(&ns, &id)), Ok(true));
    }

    #[test]
    fn putting_the_same_bytes_twice_stores_once() {
        // B2: equal content under one namespace yields one stored copy, and the
        // second put writes nothing new while re-acknowledging the same id.
        let (_dir, store) = store();
        let ns = ns();
        let bytes = b"dedup me".to_vec();

        let first = block_on(store.put(&ns, bytes.clone())).expect("first put");
        let second = block_on(store.put(&ns, bytes.clone())).expect("second put");
        assert_eq!(first, second, "equal content must re-acknowledge one id");
        // Exactly one file exists under the fan-out directory.
        let hh_dir = store.blob_path(&ns, &first).parent().unwrap().to_path_buf();
        let count = fs::read_dir(&hh_dir).unwrap().count();
        assert_eq!(count, 1, "the same content must occupy one file, not two");
    }

    #[test]
    fn a_corrupt_blob_is_detected_and_never_returned() {
        // B1: on-disk bit-rot is caught on read and surfaced as Corrupt, never
        // returned as valid bytes.
        let (_dir, store) = store();
        let ns = ns();
        let id = block_on(store.put(&ns, b"trust but verify".to_vec())).expect("put");

        // Tamper the stored file behind the store's back.
        let path = store.blob_path(&ns, &id);
        fs::write(&path, b"tampered contents of a different length").expect("tamper");

        assert_eq!(
            block_on(store.get(&ns, &id, None)),
            Err(BlobError::Corrupt(id))
        );
    }

    #[test]
    fn a_ranged_get_returns_a_verified_slice() {
        let (_dir, store) = store();
        let ns = ns();
        let bytes = b"0123456789".to_vec();
        let id = block_on(store.put(&ns, bytes)).expect("put");

        assert_eq!(
            block_on(store.get(&ns, &id, Some(2..5))),
            Ok(b"234".to_vec())
        );
        // An out-of-range slice clamps to the blob rather than panicking.
        assert_eq!(
            block_on(store.get(&ns, &id, Some(8..100))),
            Ok(b"89".to_vec())
        );
    }

    #[test]
    fn an_absent_blob_is_unavailable_not_wrong_bytes() {
        let (_dir, store) = store();
        let missing = BlobId::of(b"never stored");
        match block_on(store.get(&ns(), &missing, None)) {
            Err(BlobError::Unavailable(_)) => {}
            other => panic!("expected Unavailable, got {other:?}"),
        }
    }

    #[test]
    fn a_deleted_namespace_stays_deleted() {
        // B7 (single-node slice): after delete, the namespace resolves nowhere,
        // its bytes are swept, and a put back into it is refused.
        let (_dir, store) = store();
        let ns = ns();
        let id = block_on(store.put(&ns, b"to be reclaimed".to_vec())).expect("put");
        assert!(store.blob_path(&ns, &id).exists());

        block_on(store.delete_namespace(&ns)).expect("delete");

        assert_eq!(
            block_on(store.get(&ns, &id, None)),
            Err(BlobError::Deleted(ns.clone()))
        );
        assert_eq!(block_on(store.has(&ns, &id)), Ok(false));
        assert!(!store.blob_path(&ns, &id).exists(), "bytes must be swept");
        assert_eq!(
            block_on(store.put(&ns, b"resurrect?".to_vec())),
            Err(BlobError::Deleted(ns.clone())),
            "a put into a deleted namespace must be refused",
        );
    }

    #[test]
    fn delete_is_idempotent_and_monotonic() {
        let (_dir, store) = store();
        let ns = ns();
        block_on(store.put(&ns, b"x".to_vec())).expect("put");
        block_on(store.delete_namespace(&ns)).expect("first delete");
        // Re-deleting is a no-op, not an error (spec §3, §5.3).
        block_on(store.delete_namespace(&ns)).expect("re-delete is a no-op");
        assert!(store.is_tombstoned(&ns));
    }

    #[test]
    fn distinct_namespaces_store_the_same_content_independently() {
        // Across namespaces the same content is stored once per namespace, and
        // deleting one leaves the other resolvable (spec §2).
        let (_dir, store) = store();
        let a = Namespace::new(b"a".to_vec());
        let b = Namespace::new(b"b".to_vec());
        let bytes = b"shared content".to_vec();
        let id_a = block_on(store.put(&a, bytes.clone())).expect("put a");
        let id_b = block_on(store.put(&b, bytes.clone())).expect("put b");
        assert_eq!(id_a, id_b, "the id is namespace-independent");

        block_on(store.delete_namespace(&a)).expect("delete a");
        assert_eq!(
            block_on(store.get(&a, &id_a, None)),
            Err(BlobError::Deleted(a))
        );
        assert_eq!(
            block_on(store.get(&b, &id_b, None)),
            Ok(bytes),
            "b is untouched"
        );
    }
}
