//! The filesystem facet (spec §7.11, §7.12).
//!
//! [`Fs`] carries the workspace filesystem's metadata as facet records: an
//! [`FsOp`] under the fs tag, folded into the [`FsTree`] form; file blocks live
//! in the grain's blob area (§7.10) and the tree's live block set is the facet's
//! root contribution (F3). A grain declares `type Facets = (Fs, …)` and
//! reaches the filesystem through [`GrainCtx::fs`](crate::GrainCtx::fs) —
//! beside `kv()` and its own events, in **one consistency boundary**, instead of
//! holding a `GrainRef` to a separate workspace grain (a separate shard map,
//! generally a different leader node, and no cross-grain atomicity).
//!
//! **Plan, then stage.** Every mutating operation computes its op batch against
//! the overlay view first, performs its fallible blob writes second, and stages
//! the ops **last** — so a failed write stages nothing, exactly as the §4.2
//! decide discipline demands (an operation that returns `Err` leaves no trace in
//! the command's batch). Blocks are durable on a write quorum *before* the
//! metadata that references them is staged (§7.10); a block stored for an
//! operation that then failed is an orphan the next sweep reclaims.
//!
//! The standalone [`Workspace`](super::Workspace) grain (§7.11) is this facet's thinnest
//! consumer: its command handlers delegate here one-for-one.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use crate::blobs::BlobId;
use crate::blobs::GrainBlobs;
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

use super::chunk;
use super::grain::DirEntry;
use super::grain::FsError;
use super::grain::Metadata;
use super::meta::FsTree;
use super::meta::Ino;
use super::meta::Inode;
use super::meta::ROOT;
use super::meta::components;
use super::op::FsOp;

/// The filesystem facet marker (spec §7.11/§7.12): declare
/// `type Facets = (Fs, …)` and reach the workspace through
/// [`GrainCtx::fs`](crate::GrainCtx::fs).
pub struct Fs;

impl Sealed for Fs {}

/// The per-command stage (spec §7.12): the staged ops in order, plus a scratch
/// tree — the committed form cloned on first write — that the ops are folded
/// into as they stage, so later operations in the same command read their
/// predecessors (read-your-staged-writes).
#[derive(Default)]
pub struct FsStage {
    ops: Vec<FsOp>,
    scratch: Option<FsTree>,
}

impl Facet for Fs {
    const TAG: u8 = 2;

    type Form = FsTree;
    type Stage = FsStage;

    fn drain(stage: FsStage) -> Vec<Vec<u8>> {
        stage.ops.iter().map(encode_payload).collect()
    }

    fn fold(form: &mut FsTree, payload: &[u8]) -> Result<(), FacetError> {
        let op: FsOp = decode_payload("fs record", payload)?;
        super::op::apply(form, &op);
        Ok(())
    }

    async fn snapshot(form: &FsTree, _env: &FacetEnv) -> Result<Vec<u8>, FacetError> {
        Ok(encode_payload(form))
    }

    async fn restore(part: Option<&[u8]>, _env: &FacetEnv) -> Result<FsTree, FacetError> {
        match part {
            Some(bytes) => decode_payload("fs restore", bytes),
            None => Ok(FsTree::default()),
        }
    }

    fn roots(form: &FsTree) -> BTreeSet<BlobId> {
        form.live_blobids()
    }
}

/// The handler-facing filesystem accessor (spec §7.11), obtained from
/// [`GrainCtx::fs`](crate::GrainCtx::fs). Reads see committed-plus-staged;
/// mutations stage `FsOp`s into the current command's atomic batch (§7.12).
pub struct FsHandle<'a, G: Grain, I>
where
    G::Facets: HasFacet<Fs, I>,
{
    cell: &'a Arc<FacetCell<G::Facets>>,
    blobs: GrainBlobs,
    _index: std::marker::PhantomData<I>,
}

impl<G: Grain> GrainCtx<G> {
    /// The grain's workspace filesystem (spec §7.11). Compiles exactly when the
    /// grain declares the [`Fs`] facet (`type Facets = (Fs, …)`) — the
    /// G10 discipline applied to storage. Mutations are valid only inside a
    /// command handler (§7.12).
    pub fn fs<I>(&self) -> FsHandle<'_, G, I>
    where
        G::Facets: HasFacet<Fs, I>,
    {
        FsHandle {
            cell: self.facet_cell(),
            blobs: self.blobs(),
            _index: std::marker::PhantomData,
        }
    }
}

impl<G: Grain, I> FsHandle<'_, G, I>
where
    G::Facets: HasFacet<Fs, I>,
{
    /// Run `read` against the overlay view: the scratch tree if this command has
    /// staged fs ops, else the committed form (read-your-staged-writes, §7.12).
    fn with_tree<R>(&self, read: impl FnOnce(&FsTree) -> R) -> R {
        self.cell
            .with_overlay::<Fs, I, _>(|form, stage| match stage {
                Some(FsStage {
                    scratch: Some(tree),
                    ..
                }) => read(tree),
                _ => read(form),
            })
    }

    /// Stage a planned op batch: fold each op into the scratch tree (cloned from
    /// the committed form on first staging) and append it to the command's ops.
    /// Infallible by design — every fallible step (path resolution, blob puts)
    /// happened during planning, so an operation that failed staged nothing.
    fn stage(&self, ops: Vec<FsOp>) {
        self.cell.with_form_and_stage::<Fs, I, _>(|form, stage| {
            let scratch = stage.scratch.get_or_insert_with(|| form.clone());
            for op in ops {
                super::op::apply(scratch, &op);
                stage.ops.push(op);
            }
        });
    }

    /// Write (replace) the whole file at `path`, creating parent directories as
    /// needed. Blocks are chunked and stored durably before any metadata is
    /// staged; the staged batch is `[creates…, truncate?, write]`, atomic with
    /// the rest of the command (G19). Orphaned means the write replaced a
    /// non-empty file.
    pub async fn write_file(&self, path: &str, content: &[u8]) -> Result<FsMutation, FsError> {
        // Plan against the overlay view; stage nothing yet.
        let plan = self.with_tree(|tree| -> Result<WritePlan, FsError> {
            let mut ops = Vec::new();
            let mut next_ino = tree.next_ino;
            let mut pending = HashMap::new();
            let (parent, name) = ensure_parents(tree, path, &mut ops, &mut next_ino, &mut pending)?;
            let (ino, seq, orphaned) = match lookup(tree, &pending, parent, &name) {
                Some((_, true)) => return Err(FsError::IsADirectory),
                Some((ino, false)) => {
                    ops.push(FsOp::Truncate { ino, size: 0 }); // replace whole file
                    let file = tree.file(ino);
                    (
                        ino,
                        file.map_or(0, |f| f.next_seq),
                        file.is_some_and(|f| f.size > 0),
                    )
                }
                None => {
                    let ino = next_ino;
                    ops.push(FsOp::Create {
                        parent,
                        name,
                        ino,
                        dir: false,
                    });
                    (ino, 0, false)
                }
            };
            Ok(WritePlan {
                ops,
                ino,
                seq,
                orphaned,
            })
        })?;

        // Store the blocks (durable before the metadata that references them,
        // §7.10). A failure here staged nothing; any stored block is an orphan
        // reclaimed by the next sweep.
        let slice = chunk::write_slice(&self.blobs, plan.seq, 0, content)
            .await
            .map_err(|e| FsError::Storage(e.to_string()))?;

        let mut ops = plan.ops;
        ops.push(FsOp::Write {
            ino: plan.ino,
            slice,
        });
        self.stage(ops);
        Ok(FsMutation {
            orphaned: plan.orphaned,
        })
    }

    /// Read `[start, end)` of the file at `path` (`None` = the whole file),
    /// through the overlay, each block verified by content (G17).
    pub async fn read_file(
        &self,
        path: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Vec<u8>, FsError> {
        let file = self.with_tree(|tree| -> Result<_, FsError> {
            let ino = tree.resolve(path)?;
            tree.file(ino).cloned().ok_or(FsError::IsADirectory)
        })?;
        let (start, end) = range.unwrap_or((0, file.size));
        chunk::read_file(&self.blobs, &file, start, end)
            .await
            .map_err(|e| FsError::Storage(e.to_string()))
    }

    /// The directory entries at `path`, name-sorted, through the overlay.
    pub fn list_dir(&self, path: &str) -> Result<Vec<DirEntry>, FsError> {
        self.with_tree(|tree| {
            let ino = tree.resolve(path)?;
            let entries = tree.dir(ino).ok_or(FsError::NotADirectory)?;
            Ok(entries
                .iter()
                .map(|(name, &child)| {
                    let (dir, size) = tree
                        .inodes
                        .get(&child)
                        .map_or((false, 0), Inode::entry_meta);
                    DirEntry {
                        name: name.clone(),
                        dir,
                        size,
                    }
                })
                .collect())
        })
    }

    /// Stat `path` through the overlay.
    pub fn stat(&self, path: &str) -> Result<Metadata, FsError> {
        self.with_tree(|tree| {
            let ino = tree.resolve(path)?;
            let inode = tree.inodes.get(&ino).ok_or(FsError::NotFound)?;
            let (dir, size) = inode.entry_meta();
            Ok(Metadata { dir, size })
        })
    }

    /// Remove the file or directory at `path` (a directory only if empty,
    /// unless `recursive`). Returns whether blocks were orphaned.
    pub fn remove(&self, path: &str, recursive: bool) -> Result<FsMutation, FsError> {
        let (op, orphaned) = self.with_tree(|tree| -> Result<_, FsError> {
            let (parent, name) = tree.resolve_parent(path)?;
            let child = tree
                .dir(parent)
                .and_then(|d| d.get(&name))
                .copied()
                .ok_or(FsError::NotFound)?;
            if let Some(entries) = tree.dir(child)
                && !entries.is_empty()
                && !recursive
            {
                return Err(FsError::NotEmpty);
            }
            let orphaned = tree.file(child).is_some_and(|f| f.size > 0)
                || tree.dir(child).is_some_and(|d| !d.is_empty());
            Ok((FsOp::Unlink { parent, name }, orphaned))
        })?;
        self.stage(vec![op]);
        Ok(FsMutation { orphaned })
    }

    /// Move `from` to `to`, replacing any existing target. Returns whether the
    /// displaced target orphaned blocks.
    pub fn rename(&self, from: &str, to: &str) -> Result<FsMutation, FsError> {
        let (op, orphaned) = self.with_tree(|tree| -> Result<_, FsError> {
            let (from_parent, from) = tree.resolve_parent(from)?;
            if tree.dir(from_parent).and_then(|d| d.get(&from)).is_none() {
                return Err(FsError::NotFound);
            }
            let (to_parent, to) = tree.resolve_parent(to)?;
            let orphaned = tree.dir(to_parent).and_then(|d| d.get(&to)).is_some();
            Ok((
                FsOp::Rename {
                    from_parent,
                    from,
                    to_parent,
                    to,
                },
                orphaned,
            ))
        })?;
        self.stage(vec![op]);
        Ok(FsMutation { orphaned })
    }

    /// Set the file at `path` to `size` (grow zero-fills on read; shrink clips).
    /// Returns whether the shrink orphaned blocks.
    pub fn truncate(&self, path: &str, size: u64) -> Result<FsMutation, FsError> {
        let (op, orphaned) = self.with_tree(|tree| -> Result<_, FsError> {
            let ino = tree.resolve(path)?;
            let file = tree.file(ino).ok_or(FsError::IsADirectory)?;
            Ok((FsOp::Truncate { ino, size }, size < file.size))
        })?;
        self.stage(vec![op]);
        Ok(FsMutation { orphaned })
    }

    /// Reset the workspace to an empty tree (spec §7.11). The orphaned blocks —
    /// the whole prior block set — are reclaimed by the next sweep, whose
    /// retained roots come from the *committed* empty tree.
    pub fn destroy(&self) {
        self.stage(vec![FsOp::Destroyed]);
    }

    /// The committed tree's live block set (spec §7.10) — the facet's own roots,
    /// exposed for grain-driven repair (§7.11). Reads the committed form, not
    /// the overlay: repair targets durable state only.
    pub fn live_blocks(&self) -> BTreeSet<BlobId> {
        self.cell.with_form::<Fs, I, _>(FsTree::live_blobids)
    }
}

/// A staged mutation's outcome: whether the operation **orphaned** committed
/// blocks (replaced, unlinked, or clipped them) — the caller's cue to schedule
/// a post-commit sweep.
pub struct FsMutation {
    pub orphaned: bool,
}

/// The plan of a `write_file`: everything decided before the fallible blob puts.
struct WritePlan {
    ops: Vec<FsOp>,
    ino: Ino,
    seq: u64,
    orphaned: bool,
}

/// Resolve a path's parent directory against `tree`, planning `mkdir -p`
/// creates for missing intermediates into `ops` (visible to later lookups
/// through `pending`), and return the parent inode and final component name.
fn ensure_parents(
    tree: &FsTree,
    path: &str,
    ops: &mut Vec<FsOp>,
    next_ino: &mut Ino,
    pending: &mut HashMap<(Ino, String), (Ino, bool)>,
) -> Result<(Ino, String), FsError> {
    let comps = components(path)?;
    let (name, dirs) = comps.split_last().ok_or(FsError::InvalidPath)?;
    let mut cur = ROOT;
    for comp in dirs {
        let key = (cur, (*comp).to_string());
        let found = pending.get(&key).copied().or_else(|| {
            tree.dir(cur)
                .and_then(|d| d.get(*comp))
                .map(|&ino| (ino, tree.dir(ino).is_some()))
        });
        cur = match found {
            Some((ino, true)) => ino,
            Some((_, false)) => return Err(FsError::NotADirectory),
            None => {
                let ino = *next_ino;
                *next_ino += 1;
                ops.push(FsOp::Create {
                    parent: cur,
                    name: (*comp).to_string(),
                    ino,
                    dir: true,
                });
                pending.insert(key, (ino, true));
                ino
            }
        };
    }
    Ok((cur, (*name).to_string()))
}

/// Look up `name` under directory `parent`, considering this plan's pending
/// creations, returning `(ino, is_dir)` if present.
fn lookup(
    tree: &FsTree,
    pending: &HashMap<(Ino, String), (Ino, bool)>,
    parent: Ino,
    name: &str,
) -> Option<(Ino, bool)> {
    pending
        .get(&(parent, name.to_string()))
        .copied()
        .or_else(|| {
            tree.dir(parent)
                .and_then(|d| d.get(name))
                .map(|&ino| (ino, tree.dir(ino).is_some()))
        })
}
