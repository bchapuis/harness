//! The disk facet (spec §7.15): a raw block image per grain.
//!
//! A **physical facet** (§7.12) on the SQL facet's seam (§7.14), with **dirty
//! blocks** for WAL frames and a **raw image** for the database file: a
//! fixed-size disk a microVM mounts as its rootfs. A session's dirty set can
//! run to gigabytes, far past the replicator's message bounds, so block bytes
//! never ride a record: a **capture** `put`s each dirty block as a
//! content-addressed blob (§7.10) and stages exactly one record, a **capture
//! manifest** of `(block index, BlobId)` pairs, joining the command's atomic
//! batch (G19) — one capture, one record, one commit, so a crash can never
//! commit part of one.
//!
//! **Capture rides a command.** The guest writes the image *between* commands
//! (the one departure §7.15 owns), so nothing captures implicitly at
//! [`Facet::seal`]: the consumer drives every capture through
//! [`DiskHandle::capture`] inside an explicit command — the machine's
//! checkpoint alarm and quiescent points (machine §4). Between captures the
//! guest's writes live only in the activation-local image, a rebuildable cache
//! (§1) the fence renders unrecoverable-if-lost: a non-committed outcome
//! discards it (G20).
//!
//! **Dirty tracking is content-hash pruning** (§7.15's reference mechanism):
//! the committed form keeps a per-block [`BlobId`] index, and a capture scans
//! the image, diffing block hashes against it — the mismatches *are* the dirty
//! set. Unprivileged, deterministic under simulation, and the scan is the
//! capture's only cost; the copy-on-write overlay is the deferred upgrade
//! (§16).
//!
//! **Checkpoints upload nothing.** The composite-snapshot contribution is the
//! block index serialized as a full-coverage manifest: every referenced blob
//! was already put by the capture (or import) that committed it, and the live
//! image's *uncaptured* writes are not committed state and MUST NOT enter the
//! snapshot. Restore materializes the checkpoint manifest; replayed capture
//! manifests decode into a pending set ([`Facet::fold`], pure — F1 holds on
//! the recorded bytes) that [`Facet::rehydrate`] fetches and applies before
//! the first command, blocks verified by content (G17).

use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use serde::Deserialize;
use serde::Serialize;

use crate::blobs::BlobId;
use crate::blobs::GrainBlobs;
use crate::facet::Facet;
use crate::facet::FacetEnv;
use crate::facet::FacetError;
use crate::facet::HasFacet;
use crate::facet::sealed::Sealed;
use crate::grain::Grain;
use crate::grain::GrainCtx;

/// Capture/checkpoint block size: 1 MiB, content-addressed, so an unchanged
/// region hashes to a blob already stored (incremental by dedup, §7.15).
const BLOCK_BYTES: usize = 1 << 20;

/// The most bytes one image may hold (spec §7.15: the image is fixed-size, a
/// configured maximum, so a guest cannot make the host materialize an
/// unmetered allocation). Bounds [`DiskHandle::import`] and every folded
/// manifest.
pub const MAX_IMAGE_BYTES: u64 = 16 << 30;

/// The disk facet marker (spec §7.15): declare `type Facets = (Disk, …)` and
/// reach the image through [`GrainCtx::disk`](crate::GrainCtx::disk).
pub struct Disk;

impl Sealed for Disk {}

/// A disk operation failed — an *application-level* outcome the handler maps
/// into its reply, distinct from a durability failure (§12).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskError(pub String);

impl std::fmt::Display for DiskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "disk error: {}", self.0)
    }
}

impl std::error::Error for DiskError {}

/// What one [`DiskHandle::capture`] or [`DiskHandle::import`] staged: the
/// dirty-block count and their byte total.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskCaptureStats {
    pub blocks: u32,
    pub bytes: u64,
}

/// The capture manifest (spec §7.15): the facet's record payload and its
/// composite-snapshot contribution. Indices and ids, never bytes — the block
/// bytes live in the grain's blob area. A capture record carries the dirty
/// subset; the checkpoint contribution carries full coverage.
#[derive(Serialize, Deserialize)]
struct DiskManifest {
    /// Image size in bytes after this manifest applies (the truncate target).
    image_bytes: u64,
    block_bytes: u32,
    /// `(block index, blob)` pairs, ascending. The final block may be partial
    /// (`image_bytes` need not be block-aligned).
    blocks: Vec<(u32, BlobId)>,
}

impl DiskManifest {
    /// Reject a manifest that breaches the facet's bounds (spec §7.15): a
    /// hostile or corrupt record cannot make the host materialize past the
    /// fixed maximum, place a block outside the image, or mix block sizes.
    fn validate(&self) -> Result<(), FacetError> {
        if self.block_bytes as usize != BLOCK_BYTES {
            return Err(FacetError(format!(
                "disk: manifest block size {}, facet fixes {BLOCK_BYTES}",
                self.block_bytes
            )));
        }
        if self.image_bytes > MAX_IMAGE_BYTES {
            return Err(FacetError(format!(
                "disk: manifest image size {} exceeds the {MAX_IMAGE_BYTES} bound",
                self.image_bytes
            )));
        }
        for (idx, _) in &self.blocks {
            if (*idx as u64) * BLOCK_BYTES as u64 >= self.image_bytes {
                return Err(FacetError(format!(
                    "disk: manifest block {idx} lies outside the {}-byte image",
                    self.image_bytes
                )));
            }
        }
        Ok(())
    }
}

/// Blocks an image of `image_bytes` spans (the final one possibly partial).
fn block_count(image_bytes: u64) -> usize {
    image_bytes.div_ceil(BLOCK_BYTES as u64) as usize
}

/// The byte length of block `idx` in an image of `image_bytes`.
fn block_len(image_bytes: u64, idx: u32) -> usize {
    let start = idx as u64 * BLOCK_BYTES as u64;
    ((image_bytes - start).min(BLOCK_BYTES as u64)) as usize
}

/// The materialization handle: the image file path, the committed size, the
/// per-block hash index (the dirty-tracking baseline), and the live blob
/// roots. Shared by `Arc` so a forms clone (the host's snapshot and rehydrate
/// paths) sees the same materialization.
struct DiskImage {
    path: PathBuf,
    state: Mutex<DiskState>,
}

/// The committed materialization's mutable state, under one lock: the size,
/// the per-block index, the live blob roots, and the replay accumulator.
/// One lock (rather than one per field) keeps every update atomic and rules
/// out lock-ordering hazards.
#[derive(Default)]
struct DiskState {
    /// Committed image size in bytes. `0` until an import commits.
    image_bytes: u64,
    /// The committed [`BlobId`] of each block, `None` where no manifest ever
    /// covered it (such a region is all zeros — the image file is created
    /// sparse and only manifests write it — and a `None` always reads as
    /// dirty, the conservative side).
    index: Vec<Option<BlobId>>,
    /// The blob ids this activation must keep alive (**F3**): the restored
    /// checkpoint's, plus every capture's and later checkpoint's. The union is
    /// kept — never pruned mid-activation — so a failed `save_snapshot` can
    /// never leave the current durable manifest's blocks sweepable; the next
    /// activation restores from the durable manifest and resets the set.
    roots: BTreeSet<BlobId>,
    /// Replayed capture manifests awaiting [`Facet::rehydrate`]'s blob
    /// fetches, in journal order.
    pending: Vec<DiskManifest>,
}

impl DiskState {
    /// Fold one manifest's ids into the committed state — size, index, roots.
    /// `replace` clears the index first (an import covers the whole image; a
    /// capture is incremental). Shared by the live commit path and the
    /// replay/restore apply path, so the two can never drift.
    fn apply(&mut self, manifest: &DiskManifest, replace: bool) {
        if replace {
            self.index.clear();
        }
        self.image_bytes = manifest.image_bytes;
        self.index.resize(block_count(manifest.image_bytes), None);
        for (idx, id) in &manifest.blocks {
            self.index[*idx as usize] = Some(*id);
        }
        self.roots.extend(manifest.blocks.iter().map(|(_, id)| *id));
    }

    /// The committed block index as a full-coverage manifest — the snapshot
    /// contribution (§7.15) and the basis of [`content_digest`](Self::content_digest).
    fn manifest(&self) -> DiskManifest {
        DiskManifest {
            image_bytes: self.image_bytes,
            block_bytes: BLOCK_BYTES as u32,
            blocks: self
                .index
                .iter()
                .enumerate()
                .filter_map(|(idx, id)| id.map(|id| (idx as u32, id)))
                .collect(),
        }
    }

    /// A cheap content identifier for the committed image: the hash of its
    /// block index, reflecting committed state without re-reading the
    /// multi-MiB image file, and identical across a rehydration that
    /// reproduces the same committed blocks.
    fn content_digest(&self) -> BlobId {
        BlobId::of(&crate::facet::encode_payload(&self.manifest()))
    }
}

impl DiskImage {
    fn state(&self) -> std::sync::MutexGuard<'_, DiskState> {
        self.state.lock().expect("disk state lock")
    }

    /// Apply one manifest to the image file: size it, fetch each block
    /// (content-verified, **G17**), write it at its offset, and fold the ids
    /// into the committed state. Byte-deterministic in manifest order (**F1**).
    /// Blocks fetch sequentially, bounding memory to one block. This is the
    /// single [`DiskManifest::validate`] choke point for every replayed or
    /// restored manifest (a live staged manifest is built in-crate from
    /// bounded data and needs no re-check).
    async fn apply_manifest(
        &self,
        manifest: &DiskManifest,
        blobs: &GrainBlobs,
    ) -> Result<(), FacetError> {
        manifest.validate()?;
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path)
            .map_err(io_facet_err)?;
        file.set_len(manifest.image_bytes).map_err(io_facet_err)?;
        for (idx, id) in &manifest.blocks {
            let bytes = blobs
                .get(*id, None)
                .await
                .map_err(|e| FacetError(format!("disk block get: {e:?}")))?;
            let expected = block_len(manifest.image_bytes, *idx);
            if bytes.len() != expected {
                return Err(FacetError(format!(
                    "disk: block {idx} is {} bytes, manifest places {expected}",
                    bytes.len()
                )));
            }
            file.seek(SeekFrom::Start(*idx as u64 * BLOCK_BYTES as u64))
                .map_err(io_facet_err)?;
            file.write_all(&bytes).map_err(io_facet_err)?;
        }
        self.state().apply(manifest, false);
        Ok(())
    }

    /// Delete the materialization — the discard of G20. The next activation
    /// rematerializes from the composite snapshot plus committed manifests.
    fn delete_file(&self) {
        let _ = fs::remove_file(&self.path);
        *self.state() = DiskState::default();
    }
}

/// The committed form: `None` until [`Facet::restore`] materializes (the host
/// always restores on rehydration, snapshot or not).
#[derive(Clone, Default)]
pub struct DiskForm(Option<Arc<DiskImage>>);

impl DiskForm {
    fn image(&self) -> Result<&Arc<DiskImage>, FacetError> {
        self.0
            .as_ref()
            .ok_or_else(|| FacetError("disk: image not materialized".into()))
    }
}

/// The per-command stage: the one capture manifest this command staged, if a
/// [`DiskHandle::capture`] or [`DiskHandle::import`] ran (§7.15: one capture,
/// one record).
#[derive(Default)]
pub struct DiskStage {
    manifest: Option<Vec<u8>>,
}

impl Facet for Disk {
    const TAG: u8 = 6;
    const PHYSICAL: bool = true;

    type Form = DiskForm;
    type Stage = DiskStage;

    fn begin(form: &mut DiskForm, _stage: &mut DiskStage) -> Result<(), FacetError> {
        // Guard that the materialized image still backs the committed size (the
        // workspace facet's begin guard): a file deleted out from under the
        // activation means the materialization can no longer be trusted; the
        // error forces a step-down and the next activation rematerializes.
        let image = form.image()?;
        if image.state().image_bytes > 0 && !image.path.exists() {
            return Err(FacetError("disk: image file vanished".into()));
        }
        Ok(())
    }

    // `seal` keeps the default no-op: nothing captures implicitly (§7.15 —
    // the guest writes between commands, so only the explicit capture command
    // stages, and a lifecycle boundary runs that command to completion first).

    fn drain(stage: DiskStage) -> Vec<Vec<u8>> {
        stage.manifest.into_iter().collect()
    }

    fn fold(form: &mut DiskForm, payload: &[u8]) -> Result<(), FacetError> {
        // Replay only: the live path skips physical facets (§7.12) — the
        // capture that staged this record already mutated the materialization.
        // Decode and queue (pure, F1 on the recorded bytes); `rehydrate`
        // validates and applies each pending manifest before the first
        // command, so the bounds check lives at that single choke point.
        let manifest: DiskManifest = crate::facet::decode_payload("disk record", payload)?;
        form.image()?.state().pending.push(manifest);
        Ok(())
    }

    async fn rehydrate(form: &DiskForm, env: &FacetEnv) -> Result<(), FacetError> {
        let image = form.image()?;
        let pending = std::mem::take(&mut image.state().pending);
        for manifest in &pending {
            image.apply_manifest(manifest, env.blobs()).await?;
        }
        Ok(())
    }

    fn discard(form: &mut DiskForm) {
        if let Some(image) = &form.0 {
            image.delete_file();
        }
        form.0 = None;
    }

    async fn snapshot(form: &DiskForm, _env: &FacetEnv) -> Result<Vec<u8>, FacetError> {
        // The contribution is the committed block index, serialized as a
        // full-coverage manifest — no scan and no uploads: every referenced
        // blob was put by the capture that committed it, and the live image's
        // uncaptured writes are not committed state and must not enter the
        // snapshot (§7.15). `None` entries (never covered by a manifest) are
        // all-zero regions and restore as such.
        let manifest = form.image()?.state().manifest();
        Ok(crate::facet::encode_payload(&manifest))
    }

    async fn restore(part: Option<&[u8]>, env: &FacetEnv) -> Result<DiskForm, FacetError> {
        let path = env.scratch_path("img");
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir).map_err(io_facet_err)?;
        }
        let image = DiskImage {
            path,
            state: Mutex::new(DiskState::default()),
        };
        // Drop any stale local cache before materializing: the manifest +
        // committed capture records are the truth (§1); the prior activation's
        // file is not trusted.
        image.delete_file();
        if let Some(bytes) = part {
            let manifest: DiskManifest = crate::facet::decode_payload("disk restore", bytes)?;
            image.apply_manifest(&manifest, env.blobs()).await?;
        }
        Ok(DiskForm(Some(Arc::new(image))))
    }

    fn roots(form: &DiskForm) -> BTreeSet<BlobId> {
        match &form.0 {
            Some(image) => image.state().roots.clone(),
            None => BTreeSet::new(),
        }
    }
}

/// The handler-facing disk accessor (spec §7.15), obtained from
/// [`GrainCtx::disk`](crate::GrainCtx::disk). [`capture`](DiskHandle::capture)
/// and [`import`](DiskHandle::import) stage one manifest into the command's
/// atomic batch (G19) — or are discarded with the materialization (G20).
///
/// **Dirtiness is the consumer's to track.** The host observes guest writes
/// only at a capture's scan, so "the image holds uncaptured writes" is a
/// conservative activation-local flag the consumer owns (machine §4: set it
/// whenever the guest has run since the last committed capture) and consults
/// in `can_passivate`, so the idle path cannot strand a dirty image.
pub struct DiskHandle<'a, G: Grain, I>
where
    G::Facets: HasFacet<Disk, I>,
{
    ctx: &'a GrainCtx<G>,
    _index: std::marker::PhantomData<I>,
}

impl<G: Grain> GrainCtx<G> {
    /// The grain's raw block image (spec §7.15). Compiles exactly when the
    /// grain declares the [`Disk`] facet (`type Facets = (Disk, …)`) — the G10
    /// discipline applied to storage. Capture and import are valid only inside
    /// a command handler: the capture command is the only durability window,
    /// so out-of-command captures are refused rather than silently
    /// un-journaled.
    pub fn disk<I>(&self) -> DiskHandle<'_, G, I>
    where
        G::Facets: HasFacet<Disk, I>,
    {
        DiskHandle {
            ctx: self,
            _index: std::marker::PhantomData,
        }
    }
}

impl<G: Grain, I> DiskHandle<'_, G, I>
where
    G::Facets: HasFacet<Disk, I>,
{
    /// The image file's path — what the consumer hands its microVM as the
    /// drive's backing file (machine §5.1). The file exists once an import has
    /// committed; before that it is absent (a zero-byte disk).
    pub fn path(&self) -> Result<PathBuf, DiskError> {
        self.ctx
            .facet_cell()
            .with_form::<Disk, I, _>(|form| form.image().map(|image| image.path.clone()))
            .map_err(|e| DiskError(e.to_string()))
    }

    /// The committed image size in bytes (`0` until an import commits).
    pub fn image_bytes(&self) -> Result<u64, DiskError> {
        self.ctx
            .facet_cell()
            .with_form::<Disk, I, _>(|form| form.image().map(|image| image.state().image_bytes))
            .map_err(|e| DiskError(e.to_string()))
    }

    /// A cheap content identifier for the committed image — the hash of its
    /// block index, without re-reading the image file. `None` before an
    /// import commits. Stable across a rehydration that reproduces the same
    /// committed blocks, so a consumer can verify durability without paying a
    /// multi-MiB read on a read command.
    pub fn content_digest(&self) -> Result<Option<BlobId>, DiskError> {
        self.ctx
            .facet_cell()
            .with_form::<Disk, I, _>(|form| {
                form.image().map(|image| {
                    let state = image.state();
                    (state.image_bytes > 0).then(|| state.content_digest())
                })
            })
            .map_err(|e| DiskError(e.to_string()))
    }

    /// Provision the image from `src`, staging one **full-coverage** manifest
    /// — the base image *is* a capture (§7.15), so a fresh machine boots
    /// against shared, content-addressed base blocks and diverges by dirty
    /// blocks as the guest writes. Replaces any prior image wholesale (also
    /// the restore-from-checkpoint path, machine §8). One capture or import
    /// per command.
    pub async fn import(&self, src: &std::path::Path) -> Result<DiskCaptureStats, DiskError> {
        let (path, image) = self.begin_staged_op()?;
        let src_bytes = fs::metadata(src).map_err(io_disk_err)?.len();
        if src_bytes > MAX_IMAGE_BYTES {
            return Err(DiskError(format!(
                "import of {src_bytes} bytes exceeds the {MAX_IMAGE_BYTES} bound"
            )));
        }
        // Copy block by block, hashing and putting each — sequential, so
        // memory is bounded by one block. Every put precedes staging (plan,
        // then stage, §7.12): a failed put stages nothing, and blobs orphaned
        // by a failed commit are unrooted and swept by the host's unioned GC.
        let mut reader = fs::File::open(src).map_err(io_disk_err)?;
        let mut writer = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(io_disk_err)?;
        writer.set_len(src_bytes).map_err(io_disk_err)?;
        let blobs = self.ctx.blobs();
        let mut blocks = Vec::with_capacity(block_count(src_bytes));
        for idx in 0..block_count(src_bytes) as u32 {
            let mut block = vec![0u8; block_len(src_bytes, idx)];
            reader.read_exact(&mut block).map_err(io_disk_err)?;
            writer.write_all(&block).map_err(io_disk_err)?;
            let id = blobs
                .put(block)
                .await
                .map_err(|e| DiskError(format!("block put: {e:?}")))?;
            blocks.push((idx, id));
        }
        let stats = DiskCaptureStats {
            blocks: blocks.len() as u32,
            bytes: src_bytes,
        };
        self.commit_staged_op(
            &image,
            DiskManifest {
                image_bytes: src_bytes,
                block_bytes: BLOCK_BYTES as u32,
                blocks,
            },
            true,
        )?;
        Ok(stats)
    }

    /// The capture command's body (§7.15): scan the image at a quiescent point
    /// (the consumer pauses or stops the guest first, machine §4 — the host
    /// never captures a running guest's concurrently-written image), diff
    /// block hashes against the committed index, `put` the dirty blocks, and
    /// stage one manifest. A clean image stages nothing and the command rides
    /// the §7.5 read path. One capture or import per command.
    pub async fn capture(&self) -> Result<DiskCaptureStats, DiskError> {
        let (path, image) = self.begin_staged_op()?;
        let (image_bytes, index) = {
            let state = image.state();
            (state.image_bytes, state.index.clone())
        };
        if image_bytes == 0 {
            return Ok(DiskCaptureStats {
                blocks: 0,
                bytes: 0,
            });
        }
        let live = fs::metadata(&path).map_err(io_disk_err)?.len();
        if live != image_bytes {
            // The image is fixed-size (§7.15): a block device never resizes,
            // so a length change means the file was tampered with out of band.
            return Err(DiskError(format!(
                "image is {live} bytes, committed size is {image_bytes}"
            )));
        }
        let blobs = self.ctx.blobs();
        let mut file = fs::File::open(&path).map_err(io_disk_err)?;
        let mut blocks = Vec::new();
        let mut bytes = 0u64;
        for idx in 0..block_count(image_bytes) as u32 {
            let mut block = vec![0u8; block_len(image_bytes, idx)];
            file.read_exact(&mut block).map_err(io_disk_err)?;
            let id = BlobId::of(&block);
            if index.get(idx as usize).copied().flatten() == Some(id) {
                continue;
            }
            bytes += block.len() as u64;
            let put = blobs
                .put(block)
                .await
                .map_err(|e| DiskError(format!("block put: {e:?}")))?;
            debug_assert_eq!(put, id);
            blocks.push((idx, id));
        }
        if blocks.is_empty() {
            return Ok(DiskCaptureStats {
                blocks: 0,
                bytes: 0,
            });
        }
        let stats = DiskCaptureStats {
            blocks: blocks.len() as u32,
            bytes,
        };
        self.commit_staged_op(
            &image,
            DiskManifest {
                image_bytes,
                block_bytes: BLOCK_BYTES as u32,
                blocks,
            },
            false,
        )?;
        Ok(stats)
    }

    /// Check this command may stage a capture — inside a handler, nothing
    /// staged yet — and hand back the shared materialization handle.
    fn begin_staged_op(&self) -> Result<(PathBuf, Arc<DiskImage>), DiskError> {
        self.ctx
            .facet_cell()
            .with_overlay::<Disk, I, _>(|form, stage| {
                let Some(stage) = stage else {
                    return Err(DiskError(
                        "disk captures are only valid inside a command handler (spec §7.15)".into(),
                    ));
                };
                if stage.manifest.is_some() {
                    return Err(DiskError(
                        "one capture or import per command (spec §7.15)".into(),
                    ));
                }
                let image = form.image().map_err(|e| DiskError(e.to_string()))?;
                Ok((image.path.clone(), Arc::clone(image)))
            })
    }

    /// Fold a finished capture into the committed form (interior mutability; a
    /// non-committed outcome discards the whole materialization, G20, so no
    /// rollback is needed) — and stage its manifest into the command's batch.
    /// `replace` resets the index first (an import covers the whole image; a
    /// capture is incremental), sharing [`DiskState::apply`] with the
    /// replay/restore path.
    fn commit_staged_op(
        &self,
        image: &Arc<DiskImage>,
        manifest: DiskManifest,
        replace: bool,
    ) -> Result<(), DiskError> {
        image.state().apply(&manifest, replace);
        self.ctx.facet_cell().with_stage::<Disk, I, _>(|stage| {
            stage.manifest = Some(crate::facet::encode_payload(&manifest));
        });
        Ok(())
    }
}

fn io_facet_err(e: std::io::Error) -> FacetError {
    FacetError(format!("disk io: {e}"))
}

fn io_disk_err(e: std::io::Error) -> DiskError {
    DiskError(format!("io: {e}"))
}
