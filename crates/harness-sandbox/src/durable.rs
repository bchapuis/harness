//! The durable workspace provider (granary §7.10; harness §5.5 reversal).
//!
//! The harness `Workspace` tier is normally **working state, not session state**: a
//! cap-std directory released on every hibernation and migration, with the loss put on
//! the record as a `WorkspaceReset` (harness §5.5). This provider reverses that for the
//! durable subtree: it backs the tier with a **durable filesystem grain**
//! ([`granary::fs::Fs`]) — metadata journaled, file blocks in the grain's colocated
//! content-addressed blob area — so a workspace survives hibernation, migration, and
//! node loss. Only the **non-durable** subtree (regenerable trees like `node_modules`,
//! `target`, chosen by [`DurabilityRules`]) stays ephemeral, in a per-activation
//! cap-std **overlay**; it alone can still trigger a (narrowed) `WorkspaceReset`.
//!
//! The same four typed tools (`read_file`/`write_file`/`list_dir`/`remove`) keep their
//! exact JSON contract (the harness `Workspace` tier, [`crate::workspace`]); each call
//! is routed by path: a durable path to the grain, an excluded path to the overlay,
//! and `list_dir` merges the two. Other tiers are not offered.
//!
//! **The reversal, precisely:** a durable-path failure surfaces as
//! [`ToolError::Sandbox`] (transient — the grain rehydrates), **never**
//! [`ToolError::EnvironmentLost`], so the harness's §5.5 reset path is not entered for
//! durable content. Only a lost *overlay* escalates to `EnvironmentLost`, scoping the
//! reset to the regenerable scratch the model can rebuild.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use actor_core::BoxFuture;
use cap_std::fs::Dir;
use granary::GrainError;
use granary::GrainRef;
use granary::Granary;
use granary::GranarySystem;
use granary::fs::Fs;
use granary::fs::FsError;
use granary::fs::ListDir;
use granary::fs::ReadFile;
use granary::fs::Remove;
use granary::fs::WriteFile;
use harness::Sandbox;
use harness::SandboxError;
use harness::SandboxProfile;
use harness::SandboxProvider;
use harness::SessionId;
use harness::Tier;
use harness::ToolError;
use serde_json::Value;
use serde_json::json;

use crate::ids::sanitize;
use crate::workspace::cap_and_decode;
use crate::workspace::required_str;

/// Which paths are durable. A path is **non-durable** (routed to the ephemeral
/// overlay, never journaled) if any of its components is an excluded name — the
/// regenerable, churn-heavy trees a build reproduces (research §4.3). The default set
/// covers the common cases; a deployment can replace it.
#[derive(Clone, Debug)]
pub struct DurabilityRules {
    excludes: Vec<String>,
}

impl Default for DurabilityRules {
    fn default() -> DurabilityRules {
        DurabilityRules {
            excludes: ["node_modules", "target", ".venv", ".git", "__pycache__", "dist", "build"]
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
        }
    }
}

impl DurabilityRules {
    /// Rules with an explicit exclude set (component names).
    pub fn new(excludes: impl IntoIterator<Item = impl Into<String>>) -> DurabilityRules {
        DurabilityRules {
            excludes: excludes.into_iter().map(Into::into).collect(),
        }
    }

    /// Whether `path` is durable (journaled + blob-backed) rather than ephemeral.
    pub fn is_durable(&self, path: &str) -> bool {
        !Path::new(path)
            .components()
            .filter_map(|c| c.as_os_str().to_str())
            .any(|comp| self.excludes.iter().any(|e| e == comp))
    }
}

/// A durable workspace provider over a granary system. Holds the [`Granary`] handle
/// that hosts the [`Fs`] grain type and the cap-std root under which each session's
/// ephemeral overlay lives.
pub struct DurableWorkspaces<S: GranarySystem> {
    granary: Granary<Fs<S>>,
    overlay_root: Arc<Dir>,
    rules: Arc<DurabilityRules>,
}

impl<S: GranarySystem> DurableWorkspaces<S> {
    /// Build the provider over a `Granary<Fs<S>>` (host it with
    /// `system.granary::<Fs<_>>(config)`) and an `overlay_root` directory for the
    /// non-durable scratch, with the default [`DurabilityRules`].
    ///
    /// # Errors
    /// If the overlay root cannot be created or opened as a capability handle.
    pub fn new(
        granary: Granary<Fs<S>>,
        overlay_root: impl AsRef<Path>,
    ) -> std::io::Result<DurableWorkspaces<S>> {
        Self::with_rules(granary, overlay_root, DurabilityRules::default())
    }

    /// As [`new`](Self::new), with explicit durability rules.
    pub fn with_rules(
        granary: Granary<Fs<S>>,
        overlay_root: impl AsRef<Path>,
        rules: DurabilityRules,
    ) -> std::io::Result<DurableWorkspaces<S>> {
        std::fs::create_dir_all(overlay_root.as_ref())?;
        let root = Dir::open_ambient_dir(overlay_root.as_ref(), cap_std::ambient_authority())?;
        Ok(DurableWorkspaces {
            granary,
            overlay_root: Arc::new(root),
            rules: Arc::new(rules),
        })
    }
}

impl<S: GranarySystem> SandboxProvider for DurableWorkspaces<S> {
    fn open(
        &self,
        session: &SessionId,
        _profile: &SandboxProfile,
    ) -> BoxFuture<'static, Result<Arc<dyn Sandbox>, SandboxError>> {
        // The workspace IS the grain, addressed by the session id — it already exists
        // (virtually) and survives this activation, so "open" only binds a ref and a
        // fresh ephemeral overlay.
        let grain = self.granary.grain(session.as_str());
        let name = sanitize(session.as_str());
        let root = Arc::clone(&self.overlay_root);
        let rules = Arc::clone(&self.rules);
        Box::pin(async move {
            root.create_dir_all(&name)
                .and_then(|()| root.open_dir(&name))
                .map(|overlay| {
                    Arc::new(DurableSandbox {
                        grain,
                        overlay: Arc::new(overlay),
                        overlay_root: root,
                        overlay_name: name,
                        rules,
                    }) as Arc<dyn Sandbox>
                })
                .map_err(|e| SandboxError(format!("durable overlay open: {e}")))
        })
    }
}

/// One session's durable workspace binding: the grain ref (durable subtree) and the
/// cap-std overlay (non-durable subtree).
struct DurableSandbox<S: GranarySystem> {
    grain: GrainRef<Fs<S>>,
    overlay: Arc<Dir>,
    overlay_root: Arc<Dir>,
    overlay_name: String,
    rules: Arc<DurabilityRules>,
}

impl<S: GranarySystem> Sandbox for DurableSandbox<S> {
    fn call(
        &self,
        tier: Tier,
        name: &str,
        input: Value,
    ) -> BoxFuture<'static, Result<Value, ToolError>> {
        if tier != Tier::Workspace {
            let message = format!("tier {tier:?} is not offered by the durable workspace provider");
            return Box::pin(async move { Err(ToolError::Sandbox(message)) });
        }
        let grain = self.grain.clone();
        let overlay = Arc::clone(&self.overlay);
        let overlay_root = Arc::clone(&self.overlay_root);
        let overlay_name = self.overlay_name.clone();
        let rules = Arc::clone(&self.rules);
        let tool = name.to_string();
        Box::pin(async move {
            dispatch(&grain, &overlay, &overlay_root, &overlay_name, &rules, &tool, input).await
        })
    }

    fn release(&self) -> BoxFuture<'static, ()> {
        // The reversal (harness §5.5): drop only the ephemeral overlay; the grain and
        // its blobs are durable, so the next activation re-binds the same workspace.
        let root = Arc::clone(&self.overlay_root);
        let name = self.overlay_name.clone();
        Box::pin(async move {
            let _ = root.remove_dir_all(&name);
        })
    }
}

/// Route one workspace tool by path durability, preserving the tier's JSON contract.
async fn dispatch<S: GranarySystem>(
    grain: &GrainRef<Fs<S>>,
    overlay: &Dir,
    overlay_root: &Dir,
    overlay_name: &str,
    rules: &DurabilityRules,
    tool: &str,
    input: Value,
) -> Result<Value, ToolError> {
    match tool {
        "read_file" => {
            let path = required_str(&input, "path")?;
            if rules.is_durable(path) {
                read_durable(grain, path).await
            } else {
                overlay_call(overlay, overlay_root, overlay_name, "read_file", &input)
            }
        }
        "write_file" => {
            let path = required_str(&input, "path")?;
            let content = required_str(&input, "content")?;
            if rules.is_durable(path) {
                write_durable(grain, path, content).await
            } else {
                overlay_call(overlay, overlay_root, overlay_name, "write_file", &input)
            }
        }
        "remove" => {
            let path = required_str(&input, "path")?;
            let recursive = input.get("recursive").and_then(Value::as_bool).unwrap_or(false);
            if rules.is_durable(path) {
                remove_durable(grain, path, recursive).await
            } else {
                overlay_call(overlay, overlay_root, overlay_name, "remove", &input)
            }
        }
        "list_dir" => {
            // A directory may hold both durable and non-durable children, so merge the
            // grain's listing with the overlay's at the same path.
            let path = match input.get("path") {
                None | Some(Value::Null) => ".".to_string(),
                Some(v) => v
                    .as_str()
                    .ok_or_else(|| ToolError::InvalidArguments("`path` must be a string".into()))?
                    .to_string(),
            };
            list_merged(grain, overlay, overlay_root, overlay_name, &path, &input).await
        }
        other => Err(ToolError::Sandbox(format!(
            "tool not provided by this sandbox: {other}"
        ))),
    }
}

async fn read_durable<S: GranarySystem>(
    grain: &GrainRef<Fs<S>>,
    path: &str,
) -> Result<Value, ToolError> {
    let bytes = match grain.ask(ReadFile { path: path.into(), range: None }).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(e)) => return Err(fs_error(path, "read_file", e)),
        Err(e) => return Err(grain_error("read_file", e)),
    };
    Ok(cap_and_decode(&bytes))
}

async fn write_durable<S: GranarySystem>(
    grain: &GrainRef<Fs<S>>,
    path: &str,
    content: &str,
) -> Result<Value, ToolError> {
    match grain
        .ask(WriteFile { path: path.into(), content: content.as_bytes().to_vec() })
        .await
    {
        Ok(Ok(_)) => Ok(json!({ "bytes": content.len() })),
        Ok(Err(e)) => Err(fs_error(path, "write_file", e)),
        Err(e) => Err(grain_error("write_file", e)),
    }
}

async fn remove_durable<S: GranarySystem>(
    grain: &GrainRef<Fs<S>>,
    path: &str,
    recursive: bool,
) -> Result<Value, ToolError> {
    match grain.ask(Remove { path: path.into(), recursive }).await {
        // A missing path is success, matching the cap-std tier's `remove`.
        Ok(Ok(())) | Ok(Err(FsError::NotFound)) => Ok(json!({})),
        Ok(Err(e)) => Err(fs_error(path, "remove", e)),
        Err(e) => Err(grain_error("remove", e)),
    }
}

async fn list_merged<S: GranarySystem>(
    grain: &GrainRef<Fs<S>>,
    overlay: &Dir,
    overlay_root: &Dir,
    overlay_name: &str,
    path: &str,
    input: &Value,
) -> Result<Value, ToolError> {
    // Name-keyed merge keeps the result name-sorted (a `BTreeMap`) and deterministic.
    let mut merged: BTreeMap<String, Value> = BTreeMap::new();

    // Durable children, if this directory exists in the grain.
    match grain.ask(ListDir { path: path.into() }).await {
        Ok(Ok(entries)) => {
            for e in entries {
                merged.insert(
                    e.name.clone(),
                    json!({ "name": e.name, "kind": if e.dir { "dir" } else { "file" }, "size": e.size }),
                );
            }
        }
        // A path that lives only in the overlay (or an empty workspace) has no durable
        // listing — not an error for the merge.
        Ok(Err(FsError::NotFound)) | Ok(Err(FsError::NotADirectory)) => {}
        Ok(Err(e)) => return Err(fs_error(path, "list_dir", e)),
        Err(e) => return Err(grain_error("list_dir", e)),
    }

    // Non-durable children from the overlay (the overlay may not have the path).
    if let Ok(Value::Object(map)) =
        overlay_call(overlay, overlay_root, overlay_name, "list_dir", input)
        && let Some(Value::Array(entries)) = map.get("entries")
    {
        for entry in entries {
            if let Some(name) = entry.get("name").and_then(Value::as_str) {
                merged.entry(name.to_string()).or_insert_with(|| entry.clone());
            }
        }
    }

    Ok(json!({ "entries": merged.into_values().collect::<Vec<_>>() }))
}

/// Run a tool against the ephemeral overlay (the cap-std tier), escalating a failure to
/// `EnvironmentLost` only when the overlay directory itself is gone — the one place a
/// (narrowed) `WorkspaceReset` still applies (harness §5.5).
fn overlay_call(
    overlay: &Dir,
    overlay_root: &Dir,
    overlay_name: &str,
    tool: &str,
    input: &Value,
) -> Result<Value, ToolError> {
    match crate::workspace::call(overlay, tool, input) {
        Err(ToolError::Sandbox(e)) if overlay_root.metadata(overlay_name).is_err() => Err(
            ToolError::EnvironmentLost(format!("overlay directory is gone: {e}")),
        ),
        other => other,
    }
}

/// Map a filesystem application error to a tool error. A durable-path failure is a
/// `Sandbox` outcome (transient — the grain rehydrates), **never** `EnvironmentLost`
/// (harness §5.5 reversal).
fn fs_error(path: &str, tool: &str, e: FsError) -> ToolError {
    match e {
        FsError::InvalidPath => ToolError::InvalidArguments(format!("{tool}: invalid path {path}")),
        other => ToolError::Sandbox(format!("{tool}: {path}: {other:?}")),
    }
}

/// Map a grain transport/durability error to a `Sandbox` tool error — transient, and
/// never `EnvironmentLost` (the grain is the durable source of truth, §7.10).
fn grain_error(tool: &str, e: GrainError) -> ToolError {
    ToolError::Sandbox(format!("{tool}: {e}"))
}

