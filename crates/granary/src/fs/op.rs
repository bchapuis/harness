//! The filesystem mutation event and its pure fold (durable-workspace design).
//!
//! [`FsOp`] is the grain's `Event` (granary §4.1): the unit of durable change, one
//! per metadata mutation. [`apply`] folds an op into the [`FsTree`] — pure and
//! deterministic, so it runs identically on the live commit path and on replay (G2).
//! The writer mints the inode numbers and slice `seq`s from the current state in the
//! decide phase (the decide/apply split, §4.2); `apply` records them and advances the
//! tree's counters so a replay re-derives the same ids.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;

use super::meta::Block;
use super::meta::FileData;
use super::meta::FsTree;
use super::meta::Ino;
use super::meta::Inode;
use super::meta::Slice;

/// One durable filesystem mutation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FsOp {
    /// Create a file or directory `ino` named `name` under directory `parent`.
    Create {
        parent: Ino,
        name: String,
        ino: Ino,
        dir: bool,
    },
    /// Append an immutable slice to a file (an overwrite is a new shadowing slice).
    Write { ino: Ino, slice: Slice },
    /// Set a file's logical size, dropping slices that fall entirely beyond it.
    Truncate { ino: Ino, size: u64 },
    /// Remove `name` from directory `parent`, deleting its inode (and, for a
    /// directory, its whole subtree — the handler only emits this once it has decided
    /// the removal is allowed).
    Unlink { parent: Ino, name: String },
    /// Move `from` under `from_parent` to `to` under `to_parent`, replacing any
    /// existing target (whose subtree is deleted).
    Rename {
        from_parent: Ino,
        from: String,
        to_parent: Ino,
        to: String,
    },
    /// Reset the workspace to an empty tree — the logical half of `Destroy` (the blob
    /// area is dropped separately, §7.10). The grain's identity is eternal; this is a
    /// reset, not a deletion.
    Destroyed,
}

/// Fold one op into the tree. Pure and deterministic (G2): no clock, no entropy, no
/// I/O — the writer already resolved any nondeterminism (block ids, minted inodes)
/// before the op was journaled.
pub fn apply(tree: &mut FsTree, op: &FsOp) {
    match op {
        FsOp::Create {
            parent,
            name,
            ino,
            dir,
        } => {
            let inode = if *dir {
                Inode::Dir(BTreeMap::new())
            } else {
                Inode::File(FileData::default())
            };
            tree.inodes.insert(*ino, inode);
            if let Some(Inode::Dir(entries)) = tree.inodes.get_mut(parent) {
                entries.insert(name.clone(), *ino);
            }
            tree.next_ino = tree.next_ino.max(*ino + 1);
        }
        FsOp::Write { ino, slice } => {
            if let Some(Inode::File(file)) = tree.inodes.get_mut(ino) {
                file.size = file.size.max(slice.off + slice.len);
                file.next_seq = file.next_seq.max(slice.seq + 1);
                file.slices.push(slice.clone());
                gc_slices(file);
            }
        }
        FsOp::Truncate { ino, size } => {
            if let Some(Inode::File(file)) = tree.inodes.get_mut(ino) {
                file.size = *size;
                // Discard all data at or beyond the new size, so a later grow reads the
                // gap back as zeros (POSIX truncate): drop slices that begin beyond
                // `size`, and clip a slice that straddles it down to `[off, size)`. The
                // clipped block keeps its content id (the blob still holds — and
                // verifies — its full bytes); only how many bytes the slice reads from
                // it shrinks.
                let mut kept = Vec::new();
                for mut slice in file.slices.drain(..) {
                    if slice.off >= *size {
                        continue;
                    }
                    if slice.off + slice.len > *size {
                        let new_len = *size - slice.off;
                        clip_blocks(&mut slice.blocks, new_len);
                        slice.len = new_len;
                    }
                    kept.push(slice);
                }
                file.slices = kept;
                gc_slices(file);
            }
        }
        FsOp::Unlink { parent, name } => {
            let child = match tree.inodes.get_mut(parent) {
                Some(Inode::Dir(entries)) => entries.remove(name),
                _ => None,
            };
            if let Some(child) = child {
                remove_subtree(tree, child);
            }
        }
        FsOp::Rename {
            from_parent,
            from,
            to_parent,
            to,
        } => {
            let child = match tree.inodes.get_mut(from_parent) {
                Some(Inode::Dir(entries)) => entries.remove(from),
                _ => None,
            };
            if let Some(child) = child {
                let displaced = match tree.inodes.get_mut(to_parent) {
                    Some(Inode::Dir(entries)) => entries.insert(to.clone(), child),
                    _ => None,
                };
                if let Some(old) = displaced {
                    remove_subtree(tree, old);
                }
            }
        }
        FsOp::Destroyed => {
            *tree = FsTree::default();
        }
    }
}

/// Remove an inode and, if it is a directory, its whole subtree. Children are
/// collected before recursing so the borrow of the tree is released between levels.
fn remove_subtree(tree: &mut FsTree, ino: Ino) {
    if let Some(Inode::Dir(entries)) = tree.inodes.get(&ino) {
        let children: Vec<Ino> = entries.values().copied().collect();
        for child in children {
            remove_subtree(tree, child);
        }
    }
    tree.inodes.remove(&ino);
}

/// Clip a slice's block run to the first `new_len` bytes, trimming the last retained
/// block's effective length. The block ids are unchanged — each blob still holds (and
/// verifies against) its full bytes; the slice simply reads fewer of them.
fn clip_blocks(blocks: &mut Vec<Block>, new_len: u64) {
    let mut acc = 0u64;
    let mut out = Vec::new();
    for block in blocks.iter() {
        if acc >= new_len {
            break;
        }
        let take = (block.len as u64).min(new_len - acc) as u32;
        out.push(Block {
            id: block.id,
            len: take,
        });
        acc += take as u64;
    }
    *blocks = out;
}

/// Drop every slice fully contained, by byte range, within a strictly-higher-`seq`
/// slice (durable-workspace design). The common whole-file overwrite produces a slice
/// covering `[0, size)` that shadows all earlier slices, so the slice count stays
/// bounded under repeated rewrites — protecting the "small foldable metadata" premise
/// that lets the snapshot stay a single buffer. (Partial overwrites shadowed only by
/// the *union* of several later slices are kept; reads still resolve them correctly,
/// they are merely not yet reclaimed.)
fn gc_slices(file: &mut FileData) {
    let slices = &file.slices;
    let kept: Vec<Slice> = slices
        .iter()
        .filter(|s| {
            !slices
                .iter()
                .any(|t| t.seq > s.seq && t.off <= s.off && t.off + t.len >= s.off + s.len)
        })
        .cloned()
        .collect();
    file.slices = kept;
}

#[cfg(test)]
mod tests {
    use super::super::meta::Block;
    use super::super::meta::ROOT;
    use super::*;
    use crate::BlobId;

    fn block(bytes: &[u8]) -> Block {
        Block {
            id: BlobId::of(bytes),
            len: bytes.len() as u32,
        }
    }

    fn whole_file_slice(seq: u64, bytes: &[u8]) -> Slice {
        Slice {
            seq,
            off: 0,
            len: bytes.len() as u64,
            blocks: vec![block(bytes)],
        }
    }

    #[test]
    fn create_inserts_an_entry_and_advances_the_inode_counter() {
        let mut tree = FsTree::default();
        apply(
            &mut tree,
            &FsOp::Create {
                parent: ROOT,
                name: "a.txt".into(),
                ino: 2,
                dir: false,
            },
        );
        assert_eq!(tree.resolve("a.txt"), Some(2));
        assert_eq!(tree.next_ino, 3);
        assert!(tree.file(2).is_some());
    }

    #[test]
    fn write_sets_size_and_advances_seq() {
        let mut tree = FsTree::default();
        apply(
            &mut tree,
            &FsOp::Create {
                parent: ROOT,
                name: "a".into(),
                ino: 2,
                dir: false,
            },
        );
        apply(
            &mut tree,
            &FsOp::Write {
                ino: 2,
                slice: whole_file_slice(0, b"hello"),
            },
        );
        let file = tree.file(2).unwrap();
        assert_eq!(file.size, 5);
        assert_eq!(file.next_seq, 1);
        assert_eq!(file.slices.len(), 1);
    }

    #[test]
    fn repeated_whole_file_overwrites_keep_one_slice() {
        let mut tree = FsTree::default();
        apply(
            &mut tree,
            &FsOp::Create {
                parent: ROOT,
                name: "a".into(),
                ino: 2,
                dir: false,
            },
        );
        // Same-extent whole-file overwrites: each new slice covers the previous, so GC
        // keeps the count O(1) (the foldability premise). A shrinking write would leave
        // the old tail live; the real `WriteFile` truncates first, which `op` models via
        // `Truncate` dropping the prior slices — exercised in the grain tests.
        for i in 0..1000u64 {
            let bytes = vec![b'x'; 16];
            apply(
                &mut tree,
                &FsOp::Write {
                    ino: 2,
                    slice: whole_file_slice(i, &bytes),
                },
            );
        }
        assert_eq!(tree.file(2).unwrap().slices.len(), 1);
        assert_eq!(tree.file(2).unwrap().next_seq, 1000);
    }

    #[test]
    fn unlink_removes_a_directory_subtree() {
        let mut tree = FsTree::default();
        // /dir, /dir/f
        apply(
            &mut tree,
            &FsOp::Create {
                parent: ROOT,
                name: "dir".into(),
                ino: 2,
                dir: true,
            },
        );
        apply(
            &mut tree,
            &FsOp::Create {
                parent: 2,
                name: "f".into(),
                ino: 3,
                dir: false,
            },
        );
        assert_eq!(tree.resolve("dir/f"), Some(3));
        apply(
            &mut tree,
            &FsOp::Unlink {
                parent: ROOT,
                name: "dir".into(),
            },
        );
        assert_eq!(tree.resolve("dir"), None);
        // The child inode is gone too (no orphan left in the table).
        assert!(!tree.inodes.contains_key(&3));
    }

    #[test]
    fn rename_moves_and_replaces() {
        let mut tree = FsTree::default();
        apply(
            &mut tree,
            &FsOp::Create {
                parent: ROOT,
                name: "a".into(),
                ino: 2,
                dir: false,
            },
        );
        apply(
            &mut tree,
            &FsOp::Create {
                parent: ROOT,
                name: "b".into(),
                ino: 3,
                dir: false,
            },
        );
        // Rename a -> b: a moves onto b, the old b inode is removed.
        apply(
            &mut tree,
            &FsOp::Rename {
                from_parent: ROOT,
                from: "a".into(),
                to_parent: ROOT,
                to: "b".into(),
            },
        );
        assert_eq!(tree.resolve("a"), None);
        assert_eq!(tree.resolve("b"), Some(2));
        assert!(!tree.inodes.contains_key(&3));
    }

    #[test]
    fn destroyed_resets_to_an_empty_root() {
        let mut tree = FsTree::default();
        apply(
            &mut tree,
            &FsOp::Create {
                parent: ROOT,
                name: "a".into(),
                ino: 2,
                dir: false,
            },
        );
        apply(&mut tree, &FsOp::Destroyed);
        assert_eq!(tree, FsTree::default());
        assert!(tree.live_blobids().is_empty());
    }

    #[test]
    fn live_blobids_collects_every_referenced_block() {
        let mut tree = FsTree::default();
        apply(
            &mut tree,
            &FsOp::Create {
                parent: ROOT,
                name: "a".into(),
                ino: 2,
                dir: false,
            },
        );
        apply(
            &mut tree,
            &FsOp::Write {
                ino: 2,
                slice: whole_file_slice(0, b"data"),
            },
        );
        let ids = tree.live_blobids();
        assert!(ids.contains(&BlobId::of(b"data")));
        assert_eq!(ids.len(), 1);
    }
}
