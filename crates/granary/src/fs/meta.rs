//! The durable workspace metadata model — the fold `State` (durable-workspace design).
//!
//! This is the **JuiceFS split** (research/durable-sqlite-and-filesystem.md §4): the
//! *metadata* — the inode table, the directory tree, and each file's `slice → block`
//! map — is small and foldable, so it lives in the grain's journal and snapshots as
//! one buffer regardless of total file bytes. The *data* — the file blocks — are
//! immutable, content-addressed blobs in the grain's colocated blob area (granary
//! §7.10), referenced from here by [`BlobId`] and nothing more. The metadata is the
//! durable source of truth; the materialized directory and block bytes are a
//! rebuildable cache (research §5).

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;

use crate::BlobId;

use super::grain::FsError;

/// An inode number. The root directory is always [`ROOT`].
pub type Ino = u64;

/// The root directory's inode. A fresh [`FsTree`] seeds it; the fold never creates
/// it, so live-commit and replay agree (granary G2).
pub const ROOT: Ino = 1;

/// The target size of a content block. A write is chunked into blocks of at most this
/// many bytes, each stored as one immutable blob; the last block of a write may be
/// shorter. MUST be ≤ the replica blob bound (a consumer chunks beyond it, §7.10).
pub const BLOCK_BYTES: usize = 4 << 20; // 4 MiB

/// One content block: the immutable blob holding its bytes and its length (the last
/// block of a slice may be shorter than [`BLOCK_BYTES`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Block {
    pub id: BlobId,
    pub len: u32,
}

/// One immutable, append-only write to a file (JuiceFS split, research §4.1): the
/// bytes `[off, off+len)` as of this write, stored as a run of content blocks. A
/// later slice with a higher `seq` **shadows** an earlier one where they overlap
/// (last-writer-wins); a slice is never mutated in place — an overwrite mints a new
/// one. `seq` is the file's birth-order, minted by the writer from `next_seq`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Slice {
    pub seq: u64,
    pub off: u64,
    pub len: u64,
    pub blocks: Vec<Block>,
}

/// A regular file: its logical size, the next slice birth-order to mint, and its
/// shadowing slice map (kept bounded by slice GC, see [`crate::op`]).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileData {
    pub size: u64,
    pub next_seq: u64,
    pub slices: Vec<Slice>,
}

/// A file or a directory. A directory maps child names to their inodes; the
/// `BTreeMap` keeps listings (and the fold) deterministic (G2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Inode {
    File(FileData),
    Dir(BTreeMap<String, Ino>),
}

/// The whole durable metadata image: the inode table and the next inode to mint.
/// Small and foldable (structure + 32-byte block ids), so it snapshots as a single
/// buffer however large the files are — their bytes live in the blob area (§7.10).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsTree {
    pub inodes: BTreeMap<Ino, Inode>,
    pub next_ino: Ino,
}

impl Default for FsTree {
    fn default() -> Self {
        // A fresh tree has only the root directory. The fold never creates root, so a
        // replay from `Default` reaches the same tree as the live commit path (G2).
        let mut inodes = BTreeMap::new();
        inodes.insert(ROOT, Inode::Dir(BTreeMap::new()));
        FsTree {
            inodes,
            next_ino: ROOT + 1,
        }
    }
}

/// Split a path into its non-empty components, ignoring leading/trailing slashes and
/// `.` segments. `""` and `"/"` yield no components (the root).
///
/// A `..` component is refused as [`FsError::InvalidPath`]: the tree keeps no parent
/// links, so relative navigation cannot be resolved here — and treating `..` as a
/// literal name would silently mint a directory *called* `..`. Callers that need
/// relative paths normalize them before sending.
pub fn components(path: &str) -> Result<Vec<&str>, FsError> {
    let comps: Vec<&str> = path
        .split('/')
        .filter(|c| !c.is_empty() && *c != ".")
        .collect();
    if comps.contains(&"..") {
        return Err(FsError::InvalidPath);
    }
    Ok(comps)
}

impl FsTree {
    /// The directory entries at `ino`, or `None` if it is absent or a file.
    pub fn dir(&self, ino: Ino) -> Option<&BTreeMap<String, Ino>> {
        match self.inodes.get(&ino)? {
            Inode::Dir(entries) => Some(entries),
            Inode::File(_) => None,
        }
    }

    /// The file data at `ino`, or `None` if it is absent or a directory.
    pub fn file(&self, ino: Ino) -> Option<&FileData> {
        match self.inodes.get(&ino)? {
            Inode::File(file) => Some(file),
            Inode::Dir(_) => None,
        }
    }

    /// Resolve a path to its inode, walking from the root. [`FsError::NotFound`] if
    /// any component is missing or names a non-directory mid-path;
    /// [`FsError::InvalidPath`] for a path with a `..` component.
    pub fn resolve(&self, path: &str) -> Result<Ino, FsError> {
        let mut ino = ROOT;
        for comp in components(path)? {
            ino = *self
                .dir(ino)
                .and_then(|d| d.get(comp))
                .ok_or(FsError::NotFound)?;
        }
        Ok(ino)
    }

    /// Resolve a path's parent directory inode and the final component name.
    /// [`FsError::NotFound`] if the path is the root (no parent) or an intermediate
    /// directory is missing; [`FsError::InvalidPath`] for a `..` component.
    pub fn resolve_parent(&self, path: &str) -> Result<(Ino, String), FsError> {
        let comps = components(path)?;
        let (name, dirs) = comps.split_last().ok_or(FsError::NotFound)?;
        let mut ino = ROOT;
        for comp in dirs {
            ino = *self
                .dir(ino)
                .and_then(|d| d.get(*comp))
                .ok_or(FsError::NotFound)?;
        }
        Ok((ino, (*name).to_string()))
    }

    /// Every block id any file still references — the grain's **live root set** for
    /// blob GC and repair (§7.10). Because the grain knows this, reclamation and
    /// re-replication are root-driven, not liveness-blind probing.
    pub fn live_blobids(&self) -> BTreeSet<BlobId> {
        let mut ids = BTreeSet::new();
        for inode in self.inodes.values() {
            if let Inode::File(file) = inode {
                for slice in &file.slices {
                    for block in &slice.blocks {
                        ids.insert(block.id);
                    }
                }
            }
        }
        ids
    }
}
