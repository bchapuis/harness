//! The workspace file commands (machine §3): read and mutate the machine's
//! `/workspace` **without booting it** — the last committed state while
//! hibernated. A self-contained sub-concern of the grain: it touches the
//! activation only through [`Machine::ws_command_guard`] (no live microVM —
//! while one runs the guest owns `/workspace`).

use serde::Deserialize;
use serde::Serialize;

use actor_core::Manifest;
use actor_core::Message;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GranarySystem;
use granary::WsError;

use super::Machine;
use super::MachineError;
use super::MachineEvent;
use super::MachineState;
use crate::vm::MachineVmProvider;

// --- Workspace file commands (machine §3) --------------------------------------

/// Cap on one file command's bytes, both directions: a [`WsWrite`]'s record
/// and a [`WsRead`]'s reply must stay bounded (the ws facet's records carry
/// bytes inline, grain §7.11).
pub const MAX_WS_FILE: usize = 1024 * 1024;

/// Validate a workspace-relative path: non-empty, normal components only.
/// The subsequent operations go through a capability handle over the facet's
/// directory, so an escape is unrepresentable even past this check (S1's
/// belt-and-suspenders, as in the sandbox's Workspace tier).
fn ws_rel_path(path: &str) -> Result<&std::path::Path, MachineError> {
    let p = std::path::Path::new(path);
    if p.as_os_str().is_empty()
        || !p
            .components()
            .all(|c| matches!(c, std::path::Component::Normal(_)))
    {
        return Err(MachineError::Ws(format!(
            "invalid workspace path: {path:?}"
        )));
    }
    Ok(p)
}

/// Open the workspace facet's directory as a capability handle.
fn ws_open<S: GranarySystem, P: MachineVmProvider>(
    ctx: &GrainCtx<Machine<S, P>>,
) -> Result<cap_std::fs::Dir, MachineError> {
    let root = ctx
        .ws()
        .dir_path()
        .map_err(|e| MachineError::Ws(e.to_string()))?;
    cap_std::fs::Dir::open_ambient_dir(&root, cap_std::ambient_authority())
        .map_err(|e| MachineError::Ws(format!("open workspace: {e}")))
}

impl<S: GranarySystem, P: MachineVmProvider> Machine<S, P> {
    /// The shared guard of every workspace file command: the machine must be
    /// provisioned, and no microVM may be live — while one runs the guest
    /// owns `/workspace` (machine §3) and a host mutation would be clobbered
    /// by the next pull.
    fn ws_command_guard(&self, state: &MachineState) -> Result<(), MachineError> {
        if !state.provisioned {
            return Err(MachineError::NotProvisioned);
        }
        if self.lock().vm.is_some() {
            return Err(MachineError::VmLive);
        }
        Ok(())
    }

    /// Stage a mutating file command's delta into its own batch: the write
    /// and its durability record are one commit. A capture failure fails the
    /// command — the caller must not believe an unstaged write durable; the
    /// materialized file is picked up by the next successful capture (the
    /// facet diffs against the committed index, self-healing).
    fn ws_stage(&self, ctx: &GrainCtx<Self>) -> Result<(), MachineError> {
        match ctx.ws().capture() {
            Ok(_) => Ok(()),
            Err(WsError::TooLarge { bytes, cap }) => Err(MachineError::Ws(format!(
                "durable workspace is {bytes} bytes, over the {cap}-byte cap"
            ))),
            Err(e) => Err(MachineError::Ws(e.to_string())),
        }
    }
}

/// Write one file into the machine's `/workspace` without booting it
/// (machine §3): the bytes land in the workspace facet's directory and their
/// delta commits in this command's batch. The next boot pushes them into the
/// guest.
#[derive(Clone, Serialize, Deserialize)]
pub struct WsWrite {
    pub path: String,
    pub bytes: Vec<u8>,
}

impl Message for WsWrite {
    type Reply = Result<(), MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.WsWrite");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<WsWrite> for Machine<S, P> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: WsWrite,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<(), MachineError>) {
        let outcome = (|| {
            self.ws_command_guard(state)?;
            if msg.bytes.len() > MAX_WS_FILE {
                return Err(MachineError::Ws(format!(
                    "file is {} bytes, over the {MAX_WS_FILE}-byte command cap",
                    msg.bytes.len()
                )));
            }
            let rel = ws_rel_path(&msg.path)?;
            let dir = ws_open(ctx)?;
            if let Some(parent) = rel.parent()
                && !parent.as_os_str().is_empty()
            {
                dir.create_dir_all(parent)
                    .map_err(|e| MachineError::Ws(e.to_string()))?;
            }
            dir.write(rel, &msg.bytes)
                .map_err(|e| MachineError::Ws(e.to_string()))?;
            self.ws_stage(ctx)
        })();
        (vec![], outcome)
    }
}

/// Read one file from the machine's `/workspace` without booting it: the last
/// committed state while hibernated. Stages nothing (grain §7.5).
#[derive(Clone, Serialize, Deserialize)]
pub struct WsRead {
    pub path: String,
}

impl Message for WsRead {
    type Reply = Result<Vec<u8>, MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.WsRead");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<WsRead> for Machine<S, P> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: WsRead,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<Vec<u8>, MachineError>) {
        let outcome = (|| {
            self.ws_command_guard(state)?;
            let rel = ws_rel_path(&msg.path)?;
            let dir = ws_open(ctx)?;
            let len = dir
                .metadata(rel)
                .map_err(|e| MachineError::Ws(e.to_string()))?
                .len();
            if len as usize > MAX_WS_FILE {
                return Err(MachineError::Ws(format!(
                    "file is {len} bytes, over the {MAX_WS_FILE}-byte command cap"
                )));
            }
            dir.read(rel).map_err(|e| MachineError::Ws(e.to_string()))
        })();
        (vec![], outcome)
    }
}

/// One [`WsList`] entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WsFileInfo {
    pub name: String,
    pub len: u64,
    pub is_dir: bool,
}

/// List one level of the machine's `/workspace` without booting it. An empty
/// `path` lists the root. Stages nothing (grain §7.5).
#[derive(Clone, Serialize, Deserialize)]
pub struct WsList {
    pub path: String,
}

impl Message for WsList {
    type Reply = Result<Vec<WsFileInfo>, MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.WsList");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<WsList> for Machine<S, P> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: WsList,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<Vec<WsFileInfo>, MachineError>) {
        let outcome = (|| {
            self.ws_command_guard(state)?;
            let dir = ws_open(ctx)?;
            let listed = if msg.path.is_empty() {
                dir
            } else {
                dir.open_dir(ws_rel_path(&msg.path)?)
                    .map_err(|e| MachineError::Ws(e.to_string()))?
            };
            let mut entries = Vec::new();
            for entry in listed
                .entries()
                .map_err(|e| MachineError::Ws(e.to_string()))?
            {
                let entry = entry.map_err(|e| MachineError::Ws(e.to_string()))?;
                let meta = entry
                    .metadata()
                    .map_err(|e| MachineError::Ws(e.to_string()))?;
                entries.push(WsFileInfo {
                    name: entry.file_name().to_string_lossy().into_owned(),
                    len: meta.len(),
                    is_dir: meta.is_dir(),
                });
            }
            entries.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(entries)
        })();
        (vec![], outcome)
    }
}

/// Remove a file or directory tree from the machine's `/workspace` without
/// booting it; the deletion commits in this command's batch.
#[derive(Clone, Serialize, Deserialize)]
pub struct WsRemove {
    pub path: String,
}

impl Message for WsRemove {
    type Reply = Result<(), MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.WsRemove");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<WsRemove> for Machine<S, P> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: WsRemove,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<(), MachineError>) {
        let outcome = (|| {
            self.ws_command_guard(state)?;
            let rel = ws_rel_path(&msg.path)?;
            let dir = ws_open(ctx)?;
            let meta = dir
                .metadata(rel)
                .map_err(|e| MachineError::Ws(e.to_string()))?;
            let removed = if meta.is_dir() {
                dir.remove_dir_all(rel)
            } else {
                dir.remove_file(rel)
            };
            removed.map_err(|e| MachineError::Ws(e.to_string()))?;
            self.ws_stage(ctx)
        })();
        (vec![], outcome)
    }
}
