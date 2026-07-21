//! The workspace facet (spec §7.11): a real directory per grain, durable by capture.
//!
//! The second **physical facet** (§7.12), modeled on the SQL facet (§7.14). The
//! grain's workspace is an ordinary node-local directory under the scratch dir —
//! shells, containers, microVMs, and typed file tools all read and write the same
//! real files, with no interposition. Durability comes from **capture**: after a
//! command's side effects have landed on disk, the handler calls
//! [`WsHandle::capture`], which diffs the durable subtree against the committed
//! index and stages one record per changed file — the file's bytes **inline**, so
//! replay is a byte-deterministic fold (F1 holds on the bytes, exactly as the SQL
//! facet's F1 holds on WAL frames, never on re-execution). The staged records join
//! the command's atomic batch (G19): the tool outcome and the workspace delta
//! commit together or not at all.
//!
//! **The physical discipline (G20/F4).** The directory mutates before durability
//! (tools write it directly). On any non-committed outcome the host
//! [`Facet::discard`]s the materialization outright and the next activation
//! rebuilds it from the composite snapshot plus committed records — the local
//! directory is a rebuildable cache, never a source of truth (§1).
//!
//! **Checkpoints are the snapshot contribution.** At snapshot time every durable
//! file is chunked into content-addressed blobs (§7.10) and the contribution is a
//! small `path → chunk ids` manifest. Unchanged files hash to blobs already
//! stored, so a checkpoint uploads only new content — incremental by dedup. The
//! manifests' chunk ids are the facet's blob roots (F3); §9 compaction then drops
//! the delta records the checkpoint subsumes. A snapshot taken while an in-flight
//! tool has dirtied the directory fails (never snapshot uncaptured bytes); the
//! idle/passivation snapshot runs quiesced and lands.
//!
//! **Files-only model.** Directories are implied by the files within them; empty
//! directories, symlinks, and special files are not durable (matching what the
//! sandbox tar/untar paths preserve). Trees named in [`EXCLUDED`] (regenerable
//! caches like `target/`) live in the same directory but are never captured,
//! journaled, or restored — a rebuild on a new node regenerates them.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::marker::PhantomData;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;

use serde::Deserialize;
use serde::Serialize;

use crate::blobs::BlobId;
use crate::facet::Facet;
use crate::facet::FacetCell;
use crate::facet::FacetEnv;
use crate::facet::FacetError;
use crate::facet::HasFacet;
use crate::facet::decode_payload;
use crate::facet::encode_payload;
use crate::facet::sealed::Sealed;
use crate::grain::Grain;
use crate::grain::GrainCtx;

/// Cap on the durable subtree, matching the sandbox tar bound: eager capture and
/// checkpointing are cheap under it and loud past it, until ranged sub-records
/// and lazy hydration land (§16).
pub const MAX_TREE_BYTES: u64 = 64 << 20;

/// Checkpoint chunk size: content-addressed per file, so an unchanged file's
/// chunks hash to blobs already stored regardless of its neighbors.
const CHUNK_BYTES: usize = 1 << 20;

/// Path components that are never durable: regenerable build/dependency caches.
/// They live in the workspace directory for the activation but are not captured,
/// journaled, checkpointed, or restored.
const EXCLUDED: &[&str] = &[
    "node_modules",
    "target",
    ".venv",
    ".git",
    "__pycache__",
    "dist",
    "build",
];

/// Only trust the (mtime, len) prune cache for files at least this much older
/// than the scan: a write landing within the same mtime granule as a prior
/// capture would otherwise be invisible. Younger files are re-hashed.
const STAT_TRUST_AGE: Duration = Duration::from_secs(2);

/// The workspace facet marker (spec §7.11): declare `type Facets = (Ws, …)` and
/// reach the directory through [`GrainCtx::ws`](crate::GrainCtx::ws).
pub struct Ws;

impl Sealed for Ws {}

/// A workspace operation failed — an *application-level* outcome the handler maps
/// into its reply, distinct from a durability failure (§12).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WsError {
    /// The form is not materialized (only possible outside an activation).
    NotMaterialized,
    /// The materialization root vanished out from under the facet. Never
    /// interpreted as a mass deletion; the next command's `begin` fails and the
    /// host rebuilds the directory on the next activation (G20/F4).
    RootLost,
    /// The durable subtree exceeds [`MAX_TREE_BYTES`]. Nothing was staged; once
    /// files are removed or excluded, the next capture diffs against the last
    /// *committed* index and picks up everything since — self-healing.
    TooLarge { bytes: u64, cap: u64 },
    /// A local filesystem error.
    Io(String),
}

impl std::fmt::Display for WsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WsError::NotMaterialized => write!(f, "workspace not materialized"),
            WsError::RootLost => write!(f, "workspace root lost"),
            WsError::TooLarge { bytes, cap } => {
                write!(
                    f,
                    "durable workspace is {bytes} bytes, over the {cap}-byte cap"
                )
            }
            WsError::Io(e) => write!(f, "workspace io: {e}"),
        }
    }
}

impl std::error::Error for WsError {}

/// One journaled record (spec §7.11): a captured whole-file write (bytes inline,
/// applied byte-deterministically on replay — F1) or the paths a capture found
/// deleted. Per-file `Write` records keep each record bounded by the largest
/// single file while `drain` still joins them in ONE atomic batch (G19).
/// Encoded with `postcard` — facet payloads are runtime-internal (§7.12).
#[derive(Serialize, Deserialize)]
enum WsRecord {
    Write { path: String, bytes: Vec<u8> },
    Remove { paths: Vec<String> },
}

/// The checkpoint manifest (spec §7.11): the facet's composite-snapshot
/// contribution — every durable file as content-addressed chunks in the grain's
/// blob area, in path order.
#[derive(Serialize, Deserialize)]
struct WsManifest {
    chunk_bytes: u32,
    files: Vec<WsManifestFile>,
}

#[derive(Serialize, Deserialize)]
struct WsManifestFile {
    path: String,
    len: u64,
    chunks: Vec<BlobId>,
}

/// One durable file in the committed index.
struct WsEntry {
    len: u64,
    /// Content hash — the change detector capture diffs against.
    hash: BlobId,
    /// The last checkpoint's chunk ids, if the file is unchanged since: lets the
    /// next snapshot skip re-reading and re-putting it. `None` = dirty.
    chunks: Option<Vec<BlobId>>,
    /// LOCAL-ONLY (mtime, len) prune cache: lets capture skip hashing files the
    /// disk says are untouched. Never journaled, never restored (`None` after
    /// restore/replay means one re-hash on the next capture).
    stat: Option<(SystemTime, u64)>,
}

/// The materialization handle: the workspace root, the committed index, and the
/// live checkpoint-chunk roots. Shared by `Arc` so a forms clone (the host's
/// snapshot path) sees the same materialization — the `SqlDb` analogue (§7.14).
struct WsDir {
    root: PathBuf,
    index: Mutex<BTreeMap<String, WsEntry>>,
    /// The checkpoint-chunk ids this activation must keep alive (F3): the
    /// restored manifest's plus every later checkpoint's. Union-kept — never
    /// pruned mid-activation — so a failed `save_snapshot` can never leave the
    /// *current* durable manifest's chunks sweepable; the next activation
    /// restores from the durable manifest and resets the set.
    roots: Mutex<BTreeSet<BlobId>>,
}

/// The committed form: `None` until [`Facet::restore`] materializes (the host
/// always restores on rehydration, snapshot or not).
#[derive(Clone, Default)]
pub struct WsForm(Option<Arc<WsDir>>);

impl WsForm {
    fn dir(&self) -> Result<&Arc<WsDir>, FacetError> {
        self.0
            .as_ref()
            .ok_or_else(|| FacetError("ws: workspace not materialized".into()))
    }
}

/// One captured write, staged until the commit point.
struct StagedWrite {
    path: String,
    bytes: Vec<u8>,
    hash: BlobId,
    stat: Option<(SystemTime, u64)>,
}

/// The per-command stage: the delta capture found, if any.
#[derive(Default)]
pub struct WsStage {
    writes: Vec<StagedWrite>,
    removes: Vec<String>,
}

impl Facet for Ws {
    const TAG: u8 = 2;
    const PHYSICAL: bool = true;

    type Form = WsForm;
    type Stage = WsStage;

    /// Cheap guard on every command: the materialization root must still exist.
    /// A vanished root (wiped scratch dir, external deletion) errors here; the
    /// host answers with a forced step-down and discard, and the next activation
    /// restores the directory cleanly (G20/F4) — the recovery path for a lost
    /// materialization.
    fn begin(form: &mut WsForm, _stage: &mut WsStage) -> Result<(), FacetError> {
        let dir = form.dir()?;
        if !dir.root.is_dir() {
            return Err(FacetError("ws: materialization root lost".into()));
        }
        Ok(())
    }

    /// Fold the staged delta into the committed index. The index mutates HERE,
    /// not in capture: the pre-seal `abandon` path (an event that will not
    /// encode) leaves the form untouched, and every post-seal failure routes
    /// through discard (G20) — so the index can never silently diverge from the
    /// journal.
    fn seal(form: &mut WsForm, stage: &mut WsStage) -> Result<(), FacetError> {
        if stage.writes.is_empty() && stage.removes.is_empty() {
            return Ok(());
        }
        let dir = form.dir()?;
        let mut index = dir.index.lock().expect("ws index lock");
        for write in &stage.writes {
            index.insert(
                write.path.clone(),
                WsEntry {
                    len: write.bytes.len() as u64,
                    hash: write.hash,
                    chunks: None,
                    stat: write.stat,
                },
            );
        }
        for path in &stage.removes {
            index.remove(path);
        }
        Ok(())
    }

    fn drain(stage: WsStage) -> Vec<Vec<u8>> {
        let mut records: Vec<Vec<u8>> = stage
            .writes
            .into_iter()
            .map(|w| {
                encode_payload(&WsRecord::Write {
                    path: w.path,
                    bytes: w.bytes,
                })
            })
            .collect();
        if !stage.removes.is_empty() {
            records.push(encode_payload(&WsRecord::Remove {
                paths: stage.removes,
            }));
        }
        records
    }

    /// Replay only: the live path skips physical facets (§7.12) — the bytes were
    /// on disk before the record was captured. Applies one record to the
    /// directory and the index, byte-deterministically (F1).
    fn fold(form: &mut WsForm, payload: &[u8]) -> Result<(), FacetError> {
        let dir = form.dir()?;
        match decode_payload("ws record", payload)? {
            WsRecord::Write { path, bytes } => {
                let disk = rel_to_disk(&dir.root, &path)?;
                if let Some(parent) = disk.parent() {
                    fs::create_dir_all(parent).map_err(io_facet_err)?;
                }
                fs::write(&disk, &bytes).map_err(io_facet_err)?;
                dir.index.lock().expect("ws index lock").insert(
                    path,
                    WsEntry {
                        len: bytes.len() as u64,
                        hash: BlobId::of(&bytes),
                        chunks: None,
                        stat: None,
                    },
                );
            }
            WsRecord::Remove { paths } => {
                let mut index = dir.index.lock().expect("ws index lock");
                for path in paths {
                    let disk = rel_to_disk(&dir.root, &path)?;
                    match fs::remove_file(&disk) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => return Err(io_facet_err(e)),
                    }
                    index.remove(&path);
                }
            }
        }
        Ok(())
    }

    /// Delete the materialization — the discard of G20. The next activation
    /// rebuilds from the composite snapshot plus committed delta records.
    fn discard(form: &mut WsForm) {
        if let Some(dir) = &form.0 {
            let _ = fs::remove_dir_all(&dir.root);
        }
        form.0 = None;
    }

    async fn snapshot(form: &WsForm, env: &FacetEnv) -> Result<Vec<u8>, FacetError> {
        let dir = form.dir()?;
        // Plan under the lock, then read/hash/put outside it. No command can
        // interleave (the host awaits the snapshot on its serial mailbox), so
        // the index is stable; only out-of-command tool writes can race, and
        // those fail the hash check below by design.
        let planned: Vec<(String, u64, BlobId, Option<Vec<BlobId>>)> = {
            let index = dir.index.lock().expect("ws index lock");
            index
                .iter()
                .map(|(path, e)| (path.clone(), e.len, e.hash, e.chunks.clone()))
                .collect()
        };
        let total: u64 = planned.iter().map(|(_, len, _, _)| *len).sum();
        if total > MAX_TREE_BYTES {
            return Err(FacetError(format!(
                "ws: durable tree is {total} bytes, over the {MAX_TREE_BYTES}-byte cap"
            )));
        }
        let mut files = Vec::with_capacity(planned.len());
        let mut fresh: Vec<(String, Vec<BlobId>)> = Vec::new();
        let mut to_put: Vec<Vec<u8>> = Vec::new();
        for (path, len, hash, cached) in planned {
            let chunks = match cached {
                // Unchanged since the last checkpoint: its chunks are already
                // durable blobs; no read, no put.
                Some(chunks) => chunks,
                None => {
                    let bytes = fs::read(rel_to_disk(&dir.root, &path)?).map_err(io_facet_err)?;
                    // The snapshot must be the COMMITTED image (G4). Bytes that
                    // do not hash to the committed entry are an in-flight tool's
                    // uncaptured dirt: fail — a snapshot is only an optimization
                    // and the idle/passivation snapshot runs quiesced.
                    if BlobId::of(&bytes) != hash {
                        return Err(FacetError(format!(
                            "ws: {path} dirty (uncaptured writes); snapshot deferred"
                        )));
                    }
                    // Chunk ids are pure functions of the bytes, so they are
                    // known before the puts; the puts below make them durable
                    // before the manifest that references them commits (§7.10).
                    let ids: Vec<BlobId> = bytes
                        .chunks(CHUNK_BYTES)
                        .map(|chunk| {
                            to_put.push(chunk.to_vec());
                            BlobId::of(chunk)
                        })
                        .collect();
                    fresh.push((path.clone(), ids.clone()));
                    ids
                }
            };
            files.push(WsManifestFile { path, len, chunks });
        }
        // The chunk puts are independent; issue them concurrently. Dedup makes
        // a chunk already stored ~free (§7.10).
        futures::future::try_join_all(to_put.into_iter().map(|chunk| env.blobs().put(chunk)))
            .await
            .map_err(|e| FacetError(format!("ws checkpoint put: {e:?}")))?;
        {
            // All chunks durable: keep them alive alongside the prior roots
            // (see `WsDir::roots` — the composite may not commit), and cache
            // the fresh ids so the next checkpoint skips unchanged files.
            let mut roots = dir.roots.lock().expect("ws roots lock");
            for file in &files {
                roots.extend(file.chunks.iter().copied());
            }
            let mut index = dir.index.lock().expect("ws index lock");
            for (path, ids) in fresh {
                if let Some(entry) = index.get_mut(&path) {
                    entry.chunks = Some(ids);
                }
            }
        }
        Ok(encode_payload(&WsManifest {
            chunk_bytes: CHUNK_BYTES as u32,
            files,
        }))
    }

    async fn restore(part: Option<&[u8]>, env: &FacetEnv) -> Result<WsForm, FacetError> {
        let root = env.scratch_path("ws");
        // Drop any stale local cache before materializing: the manifest +
        // committed delta records are the truth (§1); the prior activation's
        // directory is not trusted.
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).map_err(io_facet_err)?;
        let dir = WsDir {
            root,
            index: Mutex::new(BTreeMap::new()),
            roots: Mutex::new(BTreeSet::new()),
        };
        if let Some(bytes) = part {
            let manifest: WsManifest = decode_payload("ws restore", bytes)?;
            // Fetch every file's chunks concurrently; each get verifies by
            // content (G17).
            let contents = futures::future::try_join_all(manifest.files.iter().map(|file| {
                let blobs = env.blobs();
                async move {
                    let parts = futures::future::try_join_all(
                        file.chunks.iter().map(|id| blobs.get(*id, None)),
                    )
                    .await?;
                    Ok::<Vec<u8>, crate::error::GrainError>(parts.concat())
                }
            }))
            .await
            .map_err(|e| FacetError(format!("ws checkpoint get: {e:?}")))?;
            let mut index = dir.index.lock().expect("ws index lock");
            let mut roots = dir.roots.lock().expect("ws roots lock");
            for (file, content) in manifest.files.iter().zip(contents) {
                if content.len() as u64 != file.len {
                    return Err(FacetError(format!(
                        "ws restore: {} is {} bytes, manifest says {}",
                        file.path,
                        content.len(),
                        file.len
                    )));
                }
                let disk = rel_to_disk(&dir.root, &file.path)?;
                if let Some(parent) = disk.parent() {
                    fs::create_dir_all(parent).map_err(io_facet_err)?;
                }
                fs::write(&disk, &content).map_err(io_facet_err)?;
                index.insert(
                    file.path.clone(),
                    WsEntry {
                        len: file.len,
                        hash: BlobId::of(&content),
                        chunks: Some(file.chunks.clone()),
                        stat: None,
                    },
                );
                roots.extend(file.chunks.iter().copied());
            }
            drop(index);
            drop(roots);
        }
        Ok(WsForm(Some(Arc::new(dir))))
    }

    fn roots(form: &WsForm) -> BTreeSet<BlobId> {
        match &form.0 {
            Some(dir) => dir.roots.lock().expect("ws roots lock").clone(),
            None => BTreeSet::new(),
        }
    }
}

/// What a capture staged: how many files changed, how many vanished, and the
/// durable subtree's total size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WsCapture {
    pub written: usize,
    pub removed: usize,
    pub tree_bytes: u64,
}

/// The handler-facing workspace accessor (spec §7.11), obtained from
/// [`GrainCtx::ws`](crate::GrainCtx::ws). The directory is the live truth for
/// the activation; [`capture`](WsHandle::capture) stages its changes into the
/// current command's atomic batch (G19).
pub struct WsHandle<'a, G: Grain, I>
where
    G::Facets: HasFacet<Ws, I>,
{
    cell: &'a Arc<FacetCell<G::Facets>>,
    _index: PhantomData<I>,
}

impl<G: Grain> GrainCtx<G> {
    /// The grain's workspace directory (spec §7.11). Compiles exactly when the
    /// grain declares the [`Ws`] facet (`type Facets = (Ws, …)`) — the G10
    /// discipline applied to storage. [`capture`](WsHandle::capture) is valid
    /// only inside a command handler (§7.12).
    pub fn ws<I>(&self) -> WsHandle<'_, G, I>
    where
        G::Facets: HasFacet<Ws, I>,
    {
        WsHandle {
            cell: self.facet_cell(),
            _index: PhantomData,
        }
    }
}

impl<G: Grain, I> WsHandle<'_, G, I>
where
    G::Facets: HasFacet<Ws, I>,
{
    /// The materialization root — the real directory every sandbox tier runs
    /// over. Valid whenever the form is materialized (any time after
    /// rehydration).
    pub fn dir_path(&self) -> Result<PathBuf, WsError> {
        self.cell.with_form::<Ws, I, _>(|form| {
            form.0
                .as_ref()
                .map(|dir| dir.root.clone())
                .ok_or(WsError::NotMaterialized)
        })
    }

    /// Diff the durable subtree against the committed index and stage the delta
    /// into the current command's batch (§7.11). Synchronous disk I/O on the
    /// actor thread — the SQL facet's established conduct — bounded by
    /// [`MAX_TREE_BYTES`]. An unchanged tree stages nothing (the read path,
    /// §7.5). Valid only inside a command handler.
    pub fn capture(&self) -> Result<WsCapture, WsError> {
        self.cell.with_form_and_stage::<Ws, I, _>(|form, stage| {
            let dir = form.0.as_ref().ok_or(WsError::NotMaterialized)?;
            capture_into(dir, stage)
        })
    }
}

/// The capture scan (spec §7.11): deterministic walk, stat-pruned re-hash,
/// stage what changed.
fn capture_into(dir: &WsDir, stage: &mut WsStage) -> Result<WsCapture, WsError> {
    if !dir.root.is_dir() {
        // Never interpret a vanished root as a mass deletion.
        return Err(WsError::RootLost);
    }
    // Wall clock, not `Clock::now` (§18.1 exception): the prune compares
    // against file *mtimes*, which are real-filesystem wall-clock facts the
    // sim clock cannot be compared to — the same class of escape as the
    // physical facet's disk I/O itself. Local-only, never journaled.
    #[allow(clippy::disallowed_methods)]
    let scan_start = SystemTime::now();
    let mut found: BTreeMap<String, (PathBuf, u64, Option<SystemTime>)> = BTreeMap::new();
    walk(&dir.root, "", &mut found)?;
    let tree_bytes: u64 = found.values().map(|(_, len, _)| *len).sum();
    if tree_bytes > MAX_TREE_BYTES {
        return Err(WsError::TooLarge {
            bytes: tree_bytes,
            cap: MAX_TREE_BYTES,
        });
    }
    let mut index = dir.index.lock().expect("ws index lock");
    let mut written = 0;
    for (path, (disk, len, mtime)) in &found {
        if let (Some(entry), Some(mtime)) = (index.get(path), mtime) {
            // Prune: the disk says the file is untouched since the entry's
            // capture, and the mtime is old enough that a same-granule write
            // cannot hide behind it.
            let trusted = scan_start
                .duration_since(*mtime)
                .is_ok_and(|age| age >= STAT_TRUST_AGE);
            if entry.stat == Some((*mtime, *len)) && trusted {
                continue;
            }
        }
        let bytes = fs::read(disk).map_err(io_ws_err)?;
        let hash = BlobId::of(&bytes);
        let stat = (bytes.len() as u64 == *len)
            .then_some(())
            .and(mtime.map(|m| (m, *len)));
        if let Some(entry) = index.get_mut(path)
            && entry.hash == hash
        {
            // Same committed content: refresh the local prune cache only
            // (never a form change the journal would need to know about).
            entry.stat = stat;
            continue;
        }
        stage.writes.push(StagedWrite {
            path: path.clone(),
            bytes,
            hash,
            stat,
        });
        written += 1;
    }
    let removes: Vec<String> = index
        .keys()
        .filter(|path| !found.contains_key(*path))
        .cloned()
        .collect();
    let removed = removes.len();
    stage.removes.extend(removes);
    Ok(WsCapture {
        written,
        removed,
        tree_bytes,
    })
}

/// Walk the durable subtree: regular files only, [`EXCLUDED`] names (and any
/// non-UTF-8 name) skipped at every level, name-recursive so the collected
/// relative paths are deterministic.
fn walk(
    disk: &Path,
    rel: &str,
    out: &mut BTreeMap<String, (PathBuf, u64, Option<SystemTime>)>,
) -> Result<(), WsError> {
    for entry in fs::read_dir(disk).map_err(io_ws_err)? {
        let entry = entry.map_err(io_ws_err)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue; // non-UTF-8 names are not workspace content the facet models
        };
        if EXCLUDED.contains(&name) {
            continue;
        }
        let ftype = entry.file_type().map_err(io_ws_err)?;
        let child = if rel.is_empty() {
            name.to_string()
        } else {
            format!("{rel}/{name}")
        };
        if ftype.is_dir() {
            walk(&entry.path(), &child, out)?;
        } else if ftype.is_file() {
            let meta = entry.metadata().map_err(io_ws_err)?;
            out.insert(child, (entry.path(), meta.len(), meta.modified().ok()));
        }
        // Symlinks and special files: not durable (files-only model, §7.11).
    }
    Ok(())
}

/// Resolve a journaled relative path under `root`, refusing anything that could
/// escape it. Defensive: records are self-produced by `capture`, but a corrupt
/// record must fail activation, never write outside the materialization.
fn rel_to_disk(root: &Path, rel: &str) -> Result<PathBuf, FacetError> {
    if rel.is_empty() {
        return Err(FacetError("ws: empty record path".into()));
    }
    let mut disk = root.to_path_buf();
    for component in rel.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(FacetError(format!("ws: unsafe record path {rel:?}")));
        }
        disk.push(component);
    }
    Ok(disk)
}

fn io_facet_err(e: std::io::Error) -> FacetError {
    FacetError(format!("ws io: {e}"))
}

fn io_ws_err(e: std::io::Error) -> WsError {
    WsError::Io(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_round_trips() {
        let record = encode_payload(&WsRecord::Write {
            path: "a/b.txt".into(),
            bytes: vec![1, 2, 3],
        });
        match decode_payload::<WsRecord>("ws record", &record).unwrap() {
            WsRecord::Write { path, bytes } => {
                assert_eq!(path, "a/b.txt");
                assert_eq!(bytes, vec![1, 2, 3]);
            }
            WsRecord::Remove { .. } => panic!("wrong variant"),
        }
    }

    #[test]
    fn unsafe_paths_are_refused() {
        let root = Path::new("/tmp/ws-root");
        assert!(rel_to_disk(root, "ok/file.txt").is_ok());
        assert!(rel_to_disk(root, "").is_err());
        assert!(rel_to_disk(root, "../escape").is_err());
        assert!(rel_to_disk(root, "a//b").is_err());
        assert!(rel_to_disk(root, "a/./b").is_err());
    }

    #[test]
    fn fold_writes_and_removes_files() {
        let scratch = tempfile::tempdir().expect("tempdir");
        let mut form = WsForm(Some(Arc::new(WsDir {
            root: scratch.path().to_path_buf(),
            index: Mutex::new(BTreeMap::new()),
            roots: Mutex::new(BTreeSet::new()),
        })));
        let write = encode_payload(&WsRecord::Write {
            path: "src/main.rs".into(),
            bytes: b"fn main() {}".to_vec(),
        });
        Ws::fold(&mut form, &write).unwrap();
        assert_eq!(
            fs::read(scratch.path().join("src/main.rs")).unwrap(),
            b"fn main() {}"
        );
        let remove = encode_payload(&WsRecord::Remove {
            paths: vec!["src/main.rs".into()],
        });
        Ws::fold(&mut form, &remove).unwrap();
        assert!(!scratch.path().join("src/main.rs").exists());
        assert!(form.dir().unwrap().index.lock().unwrap().is_empty());
    }

    #[test]
    fn corrupt_record_is_a_facet_error() {
        let scratch = tempfile::tempdir().expect("tempdir");
        let mut form = WsForm(Some(Arc::new(WsDir {
            root: scratch.path().to_path_buf(),
            index: Mutex::new(BTreeMap::new()),
            roots: Mutex::new(BTreeSet::new()),
        })));
        assert!(Ws::fold(&mut form, &[0xFF, 0xFF, 0xFF]).is_err());
    }
}
