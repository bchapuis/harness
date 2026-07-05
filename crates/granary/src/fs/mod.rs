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
//! The filesystem is a **logical facet** (granary §7.11,
//! §7.12): [`Fs`] carries the metadata as tagged records folded into the
//! [`FsTree`] form, any grain declares it with `type Facets = (Fs, …)` and
//! reaches it through [`ctx.fs()`](crate::GrainCtx::fs) — beside its own events
//! and other facets, in one consistency boundary. The [`Workspace`] grain — one per
//! workspace, keyed by the session/workspace id — is the facet's **thinnest
//! consumer**, so a workspace can also be addressed as a grain of its own (the
//! harness's use). Either way the metadata is the durable source of truth; the
//! materialized directory and block bytes are a rebuildable cache (research §5).
//! This module lives *in* granary (rather than a separate crate) so the agentic
//! harness depends only on granary for it.
//!
//! - [`meta`] — the facet's form: [`FsTree`], [`Inode`], [`FileData`], [`Slice`],
//!   [`Block`], path resolution, and the live-block root set.
//! - [`op`] — the record [`FsOp`] and its pure fold (`apply`) with slice GC.
//! - [`facet`] — the [`Fs`] facet and the [`FsHandle`] accessor (§7.11).
//! - [`grain`] — the [`Workspace`] grain: thin command handlers over the facet.
//! - [`chunk`] — the write (bytes → blocks → blobs) and read (slices → bytes) paths.
//! - [`repair`] — grain-driven re-replication of the live block set (§7.10 B6).

pub mod chunk;
pub mod facet;
pub mod grain;
pub mod meta;
pub mod op;
pub mod repair;

pub use facet::Fs;
pub use facet::FsHandle;
pub use facet::FsMutation;

pub use grain::Destroy;
pub use grain::DirEntry;
pub use grain::Workspace;
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
