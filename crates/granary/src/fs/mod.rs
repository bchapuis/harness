//! The durable workspace filesystem grain (durable-workspace design).
//!
//! A **durable workspace** is a filesystem that survives a grain's hibernation,
//! migration, and node loss — built as the **JuiceFS split**
//! (research/durable-sqlite-and-filesystem.md §4) directly on granary: the *metadata*
//! (the inode tree, the directory structure, each file's `slice → block` map) is a
//! small foldable value in the grain's journal, while the *file blocks* are immutable,
//! content-addressed blobs in the grain's colocated blob area (§7.10), referenced by
//! [`BlobId`](crate::BlobId) from the metadata and nothing more.
//!
//! It is an ordinary granary grain ([`Fs`]) — one per workspace, keyed by the
//! session/workspace id — so it inherits identity, the journal, the single-writer
//! fence, placement, activation, and hibernation unchanged. The metadata is the
//! durable source of truth; the materialized directory and block bytes are a
//! rebuildable cache (research §5). This module lives *in* granary (rather than a
//! separate crate) so the agentic harness depends only on granary for it.
//!
//! - [`meta`] — the fold `State`: [`FsTree`], [`Inode`], [`FileData`], [`Slice`],
//!   [`Block`], path resolution, and the live-block root set.
//! - [`op`] — the `Event` [`FsOp`] and its pure fold (`apply`) with slice GC.
//! - [`grain`] — the [`Fs`] grain and its file commands.
//! - [`chunk`] — the write (bytes → blocks → blobs) and read (slices → bytes) paths.
//! - [`repair`] — grain-driven re-replication of the live block set (§7.10 B6).

pub mod chunk;
pub mod grain;
pub mod meta;
pub mod op;
pub mod repair;

pub use grain::Destroy;
pub use grain::DirEntry;
pub use grain::Fs;
pub use grain::FsError;
pub use grain::ListDir;
pub use grain::Metadata;
pub use grain::ReadFile;
pub use grain::Remove;
pub use grain::Rename;
pub use grain::Stat;
pub use grain::Truncate;
pub use grain::WriteFile;
pub use meta::BLOCK_BYTES;
pub use meta::Block;
pub use meta::FileData;
pub use meta::FsTree;
pub use meta::Ino;
pub use meta::Inode;
pub use meta::Slice;
pub use op::FsOp;
