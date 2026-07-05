//! A shell-capable sandbox whose workspace is durable (granary §7.10; harness §5.5).
//!
//! [`TieredSandboxes`] gives the model a real shell (a container or a microVM) but
//! tears its workspace down on every release, so a session's files do not survive
//! hibernation or migration. [`DurableWorkspaces`](crate::DurableWorkspaces) survives,
//! but only behind typed file tools — no shell, because its filesystem is a journaled
//! grain with no on-disk form.
//!
//! `MaterializedSandboxes` is the union: it wraps a [`TieredSandboxes`] and backs its
//! workspace directory with the same durable `Fs` grain. On `open` it
//! [`hydrate`](crate::materialize::hydrate)s the grain into the real directory the
//! inner provider hands to the container/VM; on `release` it
//! [`sync_back`](crate::materialize::sync_back)s the directory's durable subtree into
//! the grain before the inner teardown. The grain — replicated journal plus
//! content-addressed blobs — is the durable source of truth; the shell runs against
//! ordinary files materialized from it.
//!
//! **Sync cadence: on release only.** Hibernation, graceful migration, and forced
//! step-down all pass through the harness's `on_passivate` → `release`, so each syncs.
//! A hard node crash *before* a clean release loses that activation's writes — the
//! accepted v1 durability window. `release` returns `()` and cannot report a failed
//! sync, so a sync failure is logged loudly rather than lost silently; a future
//! upgrade can sync after each call (the inner realizations already leave the host dir
//! current after every call) without changing this seam.

use std::path::Path;
use std::sync::Arc;

use actor_core::BoxFuture;
use cap_std::fs::Dir;
use granary::GrainRef;
use granary::Granary;
use granary::GranarySystem;
use granary::fs::Workspace;
use harness::Sandbox;
use harness::SandboxError;
use harness::SandboxProfile;
use harness::SandboxProvider;
use harness::SessionId;
use harness::Tier;
use harness::ToolError;
use serde_json::Value;

use crate::durable::DurabilityRules;
use crate::ids::sanitize;
use crate::materialize::hydrate;
use crate::materialize::sync_back;
use crate::provider::TieredSandboxes;

/// A [`TieredSandboxes`] whose per-session workspace is backed by a durable `Fs`
/// grain (one grain per session, keyed by session id). Construct it over the same
/// workspaces root the inner provider uses.
pub struct MaterializedSandboxes<S: GranarySystem> {
    inner: TieredSandboxes,
    granary: Granary<Workspace<S>>,
    /// A capability handle to the workspaces root, used to materialize and sync each
    /// session's `<root>/<session>` directory. The inner provider holds its own handle
    /// to the same root; two handles to one tree is fine (each re-resolves per path).
    root: Arc<Dir>,
    rules: Arc<DurabilityRules>,
}

impl<S: GranarySystem> MaterializedSandboxes<S> {
    /// Wrap `inner` with grain-backed durability over `root` (the same path `inner`
    /// was opened on), using `granary` to host the per-session filesystem grains and
    /// `rules` to decide which paths are durable.
    pub fn new(
        inner: TieredSandboxes,
        granary: Granary<Workspace<S>>,
        root: impl AsRef<Path>,
        rules: DurabilityRules,
    ) -> std::io::Result<MaterializedSandboxes<S>> {
        std::fs::create_dir_all(root.as_ref())?;
        let root = Dir::open_ambient_dir(root.as_ref(), cap_std::ambient_authority())?;
        Ok(MaterializedSandboxes {
            inner,
            granary,
            root: Arc::new(root),
            rules: Arc::new(rules),
        })
    }
}

impl<S: GranarySystem> SandboxProvider for MaterializedSandboxes<S> {
    fn open(
        &self,
        session: &SessionId,
        profile: &SandboxProfile,
    ) -> BoxFuture<'static, Result<Arc<dyn Sandbox>, SandboxError>> {
        let grain = self.granary.grain(session.as_str());
        let name = sanitize(session.as_str());
        let root = Arc::clone(&self.root);
        let rules = Arc::clone(&self.rules);
        // Kick off the inner open eagerly (its future is `'static`); it creates its own
        // handle to `<root>/<name>`, which we hydrate first below.
        let inner_open = self.inner.open(session, profile);
        Box::pin(async move {
            root.create_dir_all(&name)
                .map_err(|e| SandboxError(format!("durable workspace mkdir: {e}")))?;
            let dir = Arc::new(
                root.open_dir(&name)
                    .map_err(|e| SandboxError(format!("durable workspace open: {e}")))?,
            );
            // Rebuild the workspace from the grain before the shell sees it. A failure
            // here is fatal to the open: the model must not run against a half-built
            // workspace.
            hydrate(&grain, &dir, &rules)
                .await
                .map_err(|e| SandboxError(format!("hydrate: {e}")))?;
            let inner = inner_open.await?;
            Ok(Arc::new(MaterializedSandbox { inner, grain, dir, rules }) as Arc<dyn Sandbox>)
        })
    }

    fn workspace_durable(&self) -> bool {
        true
    }
}

/// One session's materialized workspace: the inner shell-capable sandbox plus the
/// grain and directory handle that make it durable.
struct MaterializedSandbox<S: GranarySystem> {
    inner: Arc<dyn Sandbox>,
    grain: GrainRef<Workspace<S>>,
    dir: Arc<Dir>,
    rules: Arc<DurabilityRules>,
}

impl<S: GranarySystem> Sandbox for MaterializedSandbox<S> {
    fn call(
        &self,
        tier: Tier,
        name: &str,
        input: Value,
    ) -> BoxFuture<'static, Result<Value, ToolError>> {
        // The shell/compute/workspace tiers run unchanged against the real directory;
        // durability is a property of open/release, not of the call.
        self.inner.call(tier, name, input)
    }

    fn release(&self) -> BoxFuture<'static, ()> {
        let grain = self.grain.clone();
        let dir = Arc::clone(&self.dir);
        let rules = Arc::clone(&self.rules);
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            // Persist the durable subtree before tearing down the environment. The seam
            // cannot return an error, so a failure is logged loudly — never a silent
            // loss (the next activation would rehydrate stale content).
            if let Err(e) = sync_back(&grain, &dir, &rules).await {
                eprintln!(
                    "durable workspace sync_back failed; this activation's writes may \
                     not be persisted: {e}"
                );
            }
            inner.release().await;
        })
    }
}
