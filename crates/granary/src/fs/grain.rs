//! The durable workspace filesystem grain (spec §7.11).
//!
//! [`Workspace`] is the **thinnest consumer** of the filesystem facet (§7.11): a
//! grain whose durable state lives entirely in
//! [`Fs`](super::facet::Fs) — `type State = ()`,
//! `type Event = NoEvent`, `type Facets = (Fs,)` — and whose command
//! handlers delegate one-for-one to [`ctx.fs()`](crate::GrainCtx::fs). It exists
//! so a workspace can be addressed as its own grain (one per session/workspace
//! id, the harness's use); a grain that needs a filesystem *beside* other
//! durable state declares the facet itself instead of holding a `GrainRef`
//! here.
//!
//! The commands are byte-oriented; the 256 KiB read cap and UTF-8 handling of
//! the harness `Workspace` tier are a tool-mapping concern layered above, so
//! the grain stays exact.

use std::collections::BTreeSet;
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
use crate::grain::NoEvent;

use super::facet::Fs;
use super::facet::FsMutation;
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
pub struct Workspace<S>(PhantomData<fn() -> S>);

impl<S> Default for Workspace<S> {
    fn default() -> Self {
        Workspace(PhantomData)
    }
}

impl<S: GranarySystem> Grain for Workspace<S> {
    type System = S;
    type State = ();
    type Event = NoEvent;
    type Facets = (Fs,);
    const GRAIN_TYPE: &'static str = "granary.fs.Workspace";

    fn apply(_state: &mut (), event: &NoEvent) {
        event.unreachable()
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
        // a `Repair`, whose handler reads the facet's committed tree for the live
        // block set and launches the background re-replication. A `Sweep` rides along
        // so blocks orphaned by a crash between a commit and its post-commit sweep
        // are reclaimed on the next activation.
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

/// Schedule a post-commit [`Sweep`] for a command that orphaned blocks. A
/// detached self-tell, so it lands in the host's serial mailbox AFTER the
/// current command's commit: the sweep then reads the *committed* tree — the new
/// one when the commit landed, the old (unchanged) one when it failed — and so
/// can never reclaim a block a failed commit still references.
fn sweep_later<S: GranarySystem>(ctx: &GrainCtx<Workspace<S>>) {
    let this = ctx.this();
    ctx.system().launch(Box::pin(async move {
        let _ = this.tell(Sweep).await;
    }));
}

/// Reduce a facet mutation outcome to a handler reply, scheduling a post-commit
/// [`Sweep`] when the operation orphaned committed blocks.
fn swept<S: GranarySystem>(
    ctx: &GrainCtx<Workspace<S>>,
    outcome: Result<FsMutation, FsError>,
) -> (Vec<NoEvent>, Result<(), FsError>) {
    if let Ok(FsMutation { orphaned: true }) = &outcome {
        sweep_later(ctx);
    }
    (vec![], outcome.map(|_| ()))
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
impl<S: GranarySystem> GrainHandler<WriteFile> for Workspace<S> {
    async fn handle(
        &self,
        _state: &(),
        msg: WriteFile,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Result<u64, FsError>) {
        // Overwriting a non-empty file orphans its old blocks: reclaim them
        // once this command's ops have committed.
        let (events, outcome) = swept(ctx, ctx.fs().write_file(&msg.path, &msg.content).await);
        (events, outcome.map(|()| msg.content.len() as u64))
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
impl<S: GranarySystem> GrainHandler<ReadFile> for Workspace<S> {
    async fn handle(
        &self,
        _state: &(),
        msg: ReadFile,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Result<Vec<u8>, FsError>) {
        (vec![], ctx.fs().read_file(&msg.path, msg.range).await)
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
impl<S: GranarySystem> GrainHandler<ListDir> for Workspace<S> {
    async fn handle(
        &self,
        _state: &(),
        msg: ListDir,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Result<Vec<DirEntry>, FsError>) {
        (vec![], ctx.fs().list_dir(&msg.path))
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
impl<S: GranarySystem> GrainHandler<Remove> for Workspace<S> {
    async fn handle(
        &self,
        _state: &(),
        msg: Remove,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Result<(), FsError>) {
        // Unlinking a non-empty file (or a subtree that may hold files)
        // orphans its blocks: reclaim them once the unlink has committed.
        swept(ctx, ctx.fs().remove(&msg.path, msg.recursive))
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
impl<S: GranarySystem> GrainHandler<Rename> for Workspace<S> {
    async fn handle(
        &self,
        _state: &(),
        msg: Rename,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Result<(), FsError>) {
        // Renaming over an existing target replaces it, orphaning its blocks:
        // reclaim them once the rename has committed.
        swept(ctx, ctx.fs().rename(&msg.from, &msg.to))
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
impl<S: GranarySystem> GrainHandler<Truncate> for Workspace<S> {
    async fn handle(
        &self,
        _state: &(),
        msg: Truncate,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Result<(), FsError>) {
        // Shrinking may clip whole blocks off the tail: reclaim them once the
        // truncate has committed. (Growing orphans nothing.)
        swept(ctx, ctx.fs().truncate(&msg.path, msg.size))
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
impl<S: GranarySystem> GrainHandler<Stat> for Workspace<S> {
    async fn handle(
        &self,
        _state: &(),
        msg: Stat,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Result<Metadata, FsError>) {
        (vec![], ctx.fs().stat(&msg.path))
    }
}

/// Reclaim the whole workspace: reset the tree; the post-commit sweep drops the
/// block set. The grain's identity is eternal; this is a logical reset (like
/// `tenancy::Clear`), not a delete.
#[derive(Clone, Serialize, Deserialize)]
pub struct Destroy;
impl Message for Destroy {
    type Reply = Result<(), FsError>;
    const MANIFEST: Manifest = Manifest::new("granary.fs.Destroy");
}
impl<S: GranarySystem> GrainHandler<Destroy> for Workspace<S> {
    async fn handle(
        &self,
        _state: &(),
        _msg: Destroy,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Result<(), FsError>) {
        // Stage the tree reset; the post-commit sweep reclaims the blocks (an
        // empty tree has an empty root set, so the unioned sweep drops them).
        // Deleting blobs here, in the decide phase, would race the commit: a
        // failed or crashed append rehydrates the OLD tree over an
        // already-deleted block set, leaving every file permanently unreadable —
        // the §6 output gate holds the reply, not a decide-phase side effect.
        ctx.fs().destroy();
        sweep_later(ctx);
        (vec![], Ok(()))
    }
}

/// Reclaim orphaned blocks: sweep with an empty application root set — the
/// handle unions the facet's live roots (§7.12), so everything the committed
/// tree still references survives and everything else is dropped. Internal: the
/// grain tells itself this after any command that may orphan blocks and on
/// activation, so the sweep always runs against the *committed* tree. Commits
/// nothing; idempotent and best-effort.
#[derive(Clone, Serialize, Deserialize)]
pub struct Sweep;
impl Message for Sweep {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("granary.fs.Sweep");
}
impl<S: GranarySystem> GrainHandler<Sweep> for Workspace<S> {
    async fn handle(&self, _state: &(), _msg: Sweep, ctx: &GrainCtx<Self>) -> (Vec<NoEvent>, ()) {
        ctx.blobs().gc(&BTreeSet::new()).await;
        (vec![], ())
    }
}

/// Trigger grain-driven blob repair (§7.10 B6). Internal: the grain tells itself this
/// on activation so the handler can read the facet's committed tree for the live
/// block set and launch the background re-replication. Commits nothing.
#[derive(Clone, Serialize, Deserialize)]
pub struct Repair;
impl Message for Repair {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("granary.fs.Repair");
}
impl<S: GranarySystem> GrainHandler<Repair> for Workspace<S> {
    async fn handle(&self, _state: &(), _msg: Repair, ctx: &GrainCtx<Self>) -> (Vec<NoEvent>, ()) {
        let live = ctx.fs().live_blocks();
        if !live.is_empty() {
            ctx.system()
                .launch(Box::pin(repair::repair(ctx.blobs(), live)));
        }
        (vec![], ())
    }
}
