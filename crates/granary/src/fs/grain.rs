//! The durable workspace filesystem grain (durable-workspace design).
//!
//! [`Fs`] is an ordinary granary grain whose folded `State` is the metadata tree
//! ([`FsTree`]) and whose `Event` is a metadata mutation ([`FsOp`]). Its file
//! commands follow the decide/apply split (§4.2): a handler reads the current tree,
//! performs the immutable blob writes for a `WriteFile` (durable before the metadata
//! that references them, §7.10), mints inode numbers and slice `seq`s, and returns the
//! ops to journal plus the reply. The commands are byte-oriented; the 256 KiB read
//! cap and UTF-8 handling of the harness `Workspace` tier are a tool-mapping concern
//! layered above, so the grain stays exact.

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;

use actor_core::BoxError;
use actor_core::Manifest;
use actor_core::Message;
use serde::Deserialize;
use serde::Serialize;

use crate::Grain;
use crate::GrainCtx;
use crate::GrainHandler;
use crate::GrainRegistry;
use crate::GranarySystem;

use super::chunk;
use super::meta::FsTree;
use super::meta::Ino;
use super::meta::Inode;
use super::meta::ROOT;
use super::meta::components;
use super::op::FsOp;
use super::repair;

/// A failure of a filesystem command — an *application* error that lives inside the
/// reply (`Result<T, FsError>`), distinct from a `GrainError` durability/transport
/// failure (§4.2). A blob-area failure surfaces as [`FsError::Storage`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FsError {
    /// The path, or a component of it, does not exist.
    NotFound,
    /// A path component that must be a directory is a file.
    NotADirectory,
    /// The target is a directory where a file was required.
    IsADirectory,
    /// A non-recursive remove of a non-empty directory.
    NotEmpty,
    /// An empty or otherwise unusable path.
    InvalidPath,
    /// The colocated blob area could not store or fetch the bytes (a durability
    /// outcome surfaced as an application error, §7.10).
    Storage(String),
}

/// One entry of a [`ListDir`] result: a child's name, whether it is a directory, and
/// a file's size (0 for a directory). Name-sorted by construction (the tree is a
/// `BTreeMap`), with no mtime — a pure function of the tree (sandbox S2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirEntry {
    pub name: String,
    pub dir: bool,
    pub size: u64,
}

/// A [`Stat`] result: whether the path is a directory and a file's logical size.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metadata {
    pub dir: bool,
    pub size: u64,
}

/// The durable workspace filesystem grain. One per workspace, keyed by the
/// session/workspace id. Generic over the system, like `tenancy::Directory`, so it
/// hosts on the `Local` and `Quorum` tiers unchanged.
pub struct Fs<S>(PhantomData<fn() -> S>);

impl<S> Default for Fs<S> {
    fn default() -> Self {
        Fs(PhantomData)
    }
}

impl<S: GranarySystem> Grain for Fs<S> {
    type System = S;
    type State = FsTree;
    type Event = FsOp;
    const GRAIN_TYPE: &'static str = "granary.fs.Workspace";

    fn apply(state: &mut FsTree, event: &FsOp) {
        super::op::apply(state, event)
    }

    fn register(r: &mut GrainRegistry<Self>) {
        r.accept::<WriteFile>();
        r.accept::<ReadFile>();
        r.accept::<ListDir>();
        r.accept::<Remove>();
        r.accept::<Rename>();
        r.accept::<Truncate>();
        r.accept::<Stat>();
        r.accept::<Destroy>();
        r.accept::<Repair>();
        r.accept::<Sweep>();
    }

    fn on_activate(
        &mut self,
        ctx: &GrainCtx<Self>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        // Kick grain-driven blob repair (§7.10 B6) off the activation path: tell self
        // a `Repair`, whose handler reads the folded tree for the live block set and
        // launches the background re-replication. `on_activate` has no access to the
        // state, so the self-tell is how repair reaches it. A `Sweep` rides along so
        // blocks orphaned by a crash between a commit and its post-commit sweep are
        // reclaimed on the next activation.
        let this = ctx.this();
        let system = ctx.system().clone();
        async move {
            system.launch(Box::pin(async move {
                let _ = this.tell(Repair).await;
                let _ = this.tell(Sweep).await;
            }));
            Ok(())
        }
    }
}

/// Schedule a post-commit [`Sweep`] for a command being decided that may orphan
/// blocks. A detached self-tell, so it lands in the host's serial mailbox AFTER the
/// current command's commit: the sweep then reads the *committed* tree — the new one
/// when the commit landed, the old (unchanged) one when it failed — and so can never
/// reclaim a block a failed commit still references.
fn sweep_later<S: GranarySystem>(ctx: &GrainCtx<Fs<S>>) {
    let this = ctx.this();
    ctx.system().launch(Box::pin(async move {
        let _ = this.tell(Sweep).await;
    }));
}

/// Resolve a path's parent directory, creating missing intermediate directories
/// (`mkdir -p`, matching the harness `write_file`), and return the parent inode and
/// the final component name. Creations made earlier in this same batch are tracked in
/// `pending`, so a deep new path resolves in one command. `(ino, is_dir)` values let a
/// path through a file component fail as [`FsError::NotADirectory`].
fn ensure_parents(
    state: &FsTree,
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
            state
                .dir(cur)
                .and_then(|d| d.get(*comp))
                .map(|&ino| (ino, state.dir(ino).is_some()))
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

/// Look up `name` under directory `parent`, considering this batch's pending
/// creations, returning `(ino, is_dir)` if present.
fn lookup(
    state: &FsTree,
    pending: &HashMap<(Ino, String), (Ino, bool)>,
    parent: Ino,
    name: &str,
) -> Option<(Ino, bool)> {
    pending
        .get(&(parent, name.to_string()))
        .copied()
        .or_else(|| {
            state
                .dir(parent)
                .and_then(|d| d.get(name))
                .map(|&ino| (ino, state.dir(ino).is_some()))
        })
}

/// Write (replace) a whole file at `path`, creating parent directories as needed.
/// Reply: the number of bytes written.
#[derive(Clone, Serialize, Deserialize)]
pub struct WriteFile {
    pub path: String,
    pub content: Vec<u8>,
}
impl Message for WriteFile {
    type Reply = Result<u64, FsError>;
    const MANIFEST: Manifest = Manifest::new("granary.fs.WriteFile");
}
impl<S: GranarySystem> GrainHandler<WriteFile> for Fs<S> {
    async fn handle(
        &self,
        state: &FsTree,
        msg: WriteFile,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<FsOp>, Result<u64, FsError>) {
        let mut ops = Vec::new();
        let mut next_ino = state.next_ino;
        let mut pending = HashMap::new();
        let (parent, name) =
            match ensure_parents(state, &msg.path, &mut ops, &mut next_ino, &mut pending) {
                Ok(parent) => parent,
                Err(e) => return (vec![], Err(e)),
            };
        // Reuse an existing file (overwrite), or mint a new one. A directory in the
        // way is an error.
        let (ino, seq) = match lookup(state, &pending, parent, &name) {
            Some((_, true)) => return (vec![], Err(FsError::IsADirectory)),
            Some((ino, false)) => {
                ops.push(FsOp::Truncate { ino, size: 0 }); // replace whole file
                // Overwriting a non-empty file orphans its old blocks: reclaim
                // them once this command's ops have committed.
                if state.file(ino).is_some_and(|f| f.size > 0) {
                    sweep_later(ctx);
                }
                (ino, state.file(ino).map_or(0, |f| f.next_seq))
            }
            None => {
                let ino = next_ino;
                ops.push(FsOp::Create {
                    parent,
                    name,
                    ino,
                    dir: false,
                });
                (ino, 0)
            }
        };
        // Chunk + store the blocks (durable before the metadata, §7.10). A blob-area
        // failure aborts the write with no ops journaled (any stored block is an
        // orphan reclaimed by GC).
        let slice = match chunk::write_slice(&ctx.blobs(), seq, 0, &msg.content).await {
            Ok(slice) => slice,
            Err(e) => return (vec![], Err(FsError::Storage(e.to_string()))),
        };
        let written = msg.content.len() as u64;
        ops.push(FsOp::Write { ino, slice });
        (ops, Ok(written))
    }
}

/// Read a file, optionally a `[start, end)` byte range (`None` = the whole file).
#[derive(Clone, Serialize, Deserialize)]
pub struct ReadFile {
    pub path: String,
    pub range: Option<(u64, u64)>,
}
impl Message for ReadFile {
    type Reply = Result<Vec<u8>, FsError>;
    const MANIFEST: Manifest = Manifest::new("granary.fs.ReadFile");
}
impl<S: GranarySystem> GrainHandler<ReadFile> for Fs<S> {
    async fn handle(
        &self,
        state: &FsTree,
        msg: ReadFile,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<FsOp>, Result<Vec<u8>, FsError>) {
        let ino = match state.resolve(&msg.path) {
            Ok(ino) => ino,
            Err(e) => return (vec![], Err(e)),
        };
        let Some(file) = state.file(ino) else {
            return (vec![], Err(FsError::IsADirectory));
        };
        let (start, end) = msg.range.unwrap_or((0, file.size));
        let read = chunk::read_file(&ctx.blobs(), file, start, end)
            .await
            .map_err(|e| FsError::Storage(e.to_string()));
        (vec![], read)
    }
}

/// List a directory's entries, name-sorted, with no mtime (sandbox S2).
#[derive(Clone, Serialize, Deserialize)]
pub struct ListDir {
    pub path: String,
}
impl Message for ListDir {
    type Reply = Result<Vec<DirEntry>, FsError>;
    const MANIFEST: Manifest = Manifest::new("granary.fs.ListDir");
}
impl<S: GranarySystem> GrainHandler<ListDir> for Fs<S> {
    async fn handle(
        &self,
        state: &FsTree,
        msg: ListDir,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<FsOp>, Result<Vec<DirEntry>, FsError>) {
        let ino = match state.resolve(&msg.path) {
            Ok(ino) => ino,
            Err(e) => return (vec![], Err(e)),
        };
        let Some(entries) = state.dir(ino) else {
            return (vec![], Err(FsError::NotADirectory));
        };
        let list = entries
            .iter()
            .map(|(name, &child)| {
                let (dir, size) = match state.inodes.get(&child) {
                    Some(Inode::Dir(_)) => (true, 0),
                    Some(Inode::File(f)) => (false, f.size),
                    None => (false, 0),
                };
                DirEntry {
                    name: name.clone(),
                    dir,
                    size,
                }
            })
            .collect();
        (vec![], Ok(list))
    }
}

/// Remove a file, or a directory (only if empty, unless `recursive`).
#[derive(Clone, Serialize, Deserialize)]
pub struct Remove {
    pub path: String,
    pub recursive: bool,
}
impl Message for Remove {
    type Reply = Result<(), FsError>;
    const MANIFEST: Manifest = Manifest::new("granary.fs.Remove");
}
impl<S: GranarySystem> GrainHandler<Remove> for Fs<S> {
    async fn handle(
        &self,
        state: &FsTree,
        msg: Remove,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<FsOp>, Result<(), FsError>) {
        let (parent, name) = match state.resolve_parent(&msg.path) {
            Ok(parent) => parent,
            Err(e) => return (vec![], Err(e)),
        };
        let Some(&child) = state.dir(parent).and_then(|d| d.get(&name)) else {
            return (vec![], Err(FsError::NotFound));
        };
        if let Some(entries) = state.dir(child)
            && !entries.is_empty()
            && !msg.recursive
        {
            return (vec![], Err(FsError::NotEmpty));
        }
        // Unlinking a non-empty file (or a subtree that may hold files) orphans
        // its blocks: reclaim them once the unlink has committed.
        if state.file(child).is_some_and(|f| f.size > 0)
            || state.dir(child).is_some_and(|d| !d.is_empty())
        {
            sweep_later(ctx);
        }
        (vec![FsOp::Unlink { parent, name }], Ok(()))
    }
}

/// Move `from` to `to`, replacing any existing target. Both parents must exist.
#[derive(Clone, Serialize, Deserialize)]
pub struct Rename {
    pub from: String,
    pub to: String,
}
impl Message for Rename {
    type Reply = Result<(), FsError>;
    const MANIFEST: Manifest = Manifest::new("granary.fs.Rename");
}
impl<S: GranarySystem> GrainHandler<Rename> for Fs<S> {
    async fn handle(
        &self,
        state: &FsTree,
        msg: Rename,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<FsOp>, Result<(), FsError>) {
        let (from_parent, from) = match state.resolve_parent(&msg.from) {
            Ok(parent) => parent,
            Err(e) => return (vec![], Err(e)),
        };
        if state.dir(from_parent).and_then(|d| d.get(&from)).is_none() {
            return (vec![], Err(FsError::NotFound));
        }
        let (to_parent, to) = match state.resolve_parent(&msg.to) {
            Ok(parent) => parent,
            Err(e) => return (vec![], Err(e)),
        };
        // Renaming over an existing target replaces it, orphaning its blocks:
        // reclaim them once the rename has committed.
        if state.dir(to_parent).and_then(|d| d.get(&to)).is_some() {
            sweep_later(ctx);
        }
        (
            vec![FsOp::Rename {
                from_parent,
                from,
                to_parent,
                to,
            }],
            Ok(()),
        )
    }
}

/// Set a file's size (grow with zero-fill on read, or shrink).
#[derive(Clone, Serialize, Deserialize)]
pub struct Truncate {
    pub path: String,
    pub size: u64,
}
impl Message for Truncate {
    type Reply = Result<(), FsError>;
    const MANIFEST: Manifest = Manifest::new("granary.fs.Truncate");
}
impl<S: GranarySystem> GrainHandler<Truncate> for Fs<S> {
    async fn handle(
        &self,
        state: &FsTree,
        msg: Truncate,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<FsOp>, Result<(), FsError>) {
        let ino = match state.resolve(&msg.path) {
            Ok(ino) => ino,
            Err(e) => return (vec![], Err(e)),
        };
        let Some(file) = state.file(ino) else {
            return (vec![], Err(FsError::IsADirectory));
        };
        // Shrinking may clip whole blocks off the tail: reclaim them once the
        // truncate has committed. (Growing zero-fills and orphans nothing.)
        if msg.size < file.size {
            sweep_later(ctx);
        }
        (
            vec![FsOp::Truncate {
                ino,
                size: msg.size,
            }],
            Ok(()),
        )
    }
}

/// Stat a path: directory-or-file and a file's size.
#[derive(Clone, Serialize, Deserialize)]
pub struct Stat {
    pub path: String,
}
impl Message for Stat {
    type Reply = Result<Metadata, FsError>;
    const MANIFEST: Manifest = Manifest::new("granary.fs.Stat");
}
impl<S: GranarySystem> GrainHandler<Stat> for Fs<S> {
    async fn handle(
        &self,
        state: &FsTree,
        msg: Stat,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<FsOp>, Result<Metadata, FsError>) {
        let ino = match state.resolve(&msg.path) {
            Ok(ino) => ino,
            Err(e) => return (vec![], Err(e)),
        };
        let meta = match state.inodes.get(&ino) {
            Some(Inode::Dir(_)) => Metadata { dir: true, size: 0 },
            Some(Inode::File(f)) => Metadata {
                dir: false,
                size: f.size,
            },
            None => return (vec![], Err(FsError::NotFound)),
        };
        (vec![], Ok(meta))
    }
}

/// Reclaim the whole workspace: reset the tree and drop the blob area. The grain's
/// identity is eternal; this is a logical reset (like `tenancy::Clear`), not a delete.
#[derive(Clone, Serialize, Deserialize)]
pub struct Destroy;
impl Message for Destroy {
    type Reply = Result<(), FsError>;
    const MANIFEST: Manifest = Manifest::new("granary.fs.Destroy");
}
impl<S: GranarySystem> GrainHandler<Destroy> for Fs<S> {
    async fn handle(
        &self,
        _state: &FsTree,
        _msg: Destroy,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<FsOp>, Result<(), FsError>) {
        // Journal the tree reset FIRST; the post-commit sweep reclaims the blob area
        // (an empty tree has an empty live set, so the sweep drops everything).
        // Deleting the blobs here, in the decide phase, would race the commit: a
        // failed or crashed append rehydrates the OLD tree over an already-deleted
        // blob area, leaving every file permanently unreadable — the §6 output gate
        // holds the reply, not a decide-phase side effect.
        sweep_later(ctx);
        (vec![FsOp::Destroyed], Ok(()))
    }
}

/// Reclaim orphaned blocks: drop every blob the folded tree no longer references
/// (the grain's mark-from-roots GC, §7.10). Internal: the grain tells itself this
/// after any command that may orphan blocks — overwrite, shrink, remove,
/// rename-over, destroy ([`sweep_later`]) — and on activation, so the sweep always
/// runs against the *committed* tree. Commits nothing; idempotent and best-effort.
#[derive(Clone, Serialize, Deserialize)]
pub struct Sweep;
impl Message for Sweep {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("granary.fs.Sweep");
}
impl<S: GranarySystem> GrainHandler<Sweep> for Fs<S> {
    async fn handle(&self, state: &FsTree, _msg: Sweep, ctx: &GrainCtx<Self>) -> (Vec<FsOp>, ()) {
        ctx.blobs().gc(&state.live_blobids()).await;
        (vec![], ())
    }
}

/// Trigger grain-driven blob repair (§7.10 B6). Internal: the grain tells itself this
/// on activation so the handler can read the folded tree for the live block set and
/// launch the background re-replication. Commits nothing.
#[derive(Clone, Serialize, Deserialize)]
pub struct Repair;
impl Message for Repair {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("granary.fs.Repair");
}
impl<S: GranarySystem> GrainHandler<Repair> for Fs<S> {
    async fn handle(&self, state: &FsTree, _msg: Repair, ctx: &GrainCtx<Self>) -> (Vec<FsOp>, ()) {
        let live = state.live_blobids();
        if !live.is_empty() {
            ctx.system()
                .launch(Box::pin(repair::repair(ctx.blobs(), live)));
        }
        (vec![], ())
    }
}
