//! The provider: one workspace directory per session, held as a capability
//! handle; tier environments built lazily on the first call that carries
//! them (sandbox spec §2.3 item 2).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use actor_core::BoxFuture;
use cap_std::fs::Dir;
use harness::ComputeLimits;
use harness::Sandbox;
use harness::SandboxError;
use harness::SandboxProfile;
use harness::SandboxProvider;
use harness::SessionId;
use harness::Tier;
use harness::ToolError;
use harness::session::content_digest;
use serde_json::Value;

use crate::ids::sanitize;

/// Observable provider activity, for the S5 accounting tests: every
/// environment opened is eventually released, and tier environments are
/// built only on first use.
#[derive(Clone, Default)]
pub struct TierStats {
    opened: Arc<AtomicUsize>,
    released: Arc<AtomicUsize>,
    compute_built: Arc<AtomicUsize>,
    modules_compiled: Arc<AtomicUsize>,
}

impl TierStats {
    pub fn opened(&self) -> usize {
        self.opened.load(Ordering::SeqCst)
    }

    pub fn released(&self) -> usize {
        self.released.load(Ordering::SeqCst)
    }

    pub fn compute_built(&self) -> usize {
        self.compute_built.load(Ordering::SeqCst)
    }

    /// Modules actually compiled (cache misses): the module cache makes a
    /// repeated call compile nothing.
    pub fn modules_compiled(&self) -> usize {
        self.modules_compiled.load(Ordering::SeqCst)
    }

    #[cfg(feature = "compute")]
    pub(crate) fn count_module_compiled(&self) {
        self.modules_compiled.fetch_add(1, Ordering::SeqCst);
    }
}

/// The tiered sandbox provider (sandbox spec §3): `Workspace` by capability
/// handle, `Compute` (feature `compute`) by hermetic wasmtime guests, and
/// nothing else — see the crate docs for the offered set.
pub struct TieredSandboxes {
    /// The root, held as a capability handle from construction: even
    /// provisioning and teardown go through cap-std, never through an
    /// ambient path (S1).
    root: Arc<Dir>,
    /// Base seed for compute determinism (S2). The per-session seed is a
    /// digest of base and session id, so fixing the base fixes every guest.
    seed: u64,
    /// Deployment-registered compute modules (the QuickJS runner and kin):
    /// resolved by name before any workspace path, so a guest write can
    /// never shadow them. Sharing them across sessions is pre-warming, which
    /// sandbox spec §2.3 item 2 permits: a module is code, not working
    /// state — every call still gets a fresh store.
    modules: BTreeMap<String, Arc<[u8]>>,
    pub stats: TierStats,
}

impl TieredSandboxes {
    /// Open the provider over `root`, creating it if absent. This is the
    /// crate's only use of ambient authority: everything after this call
    /// reaches the filesystem through the returned handle.
    pub fn new(root: impl AsRef<std::path::Path>) -> std::io::Result<TieredSandboxes> {
        std::fs::create_dir_all(root.as_ref())?;
        let root = Dir::open_ambient_dir(root.as_ref(), cap_std::ambient_authority())?;
        Ok(TieredSandboxes {
            root: Arc::new(root),
            seed: 0,
            modules: BTreeMap::new(),
            stats: TierStats::default(),
        })
    }

    /// Fix the base seed (S2): a deployment that pins it makes every guest's
    /// injected entropy reproducible.
    pub fn with_seed(mut self, seed: u64) -> TieredSandboxes {
        self.seed = seed;
        self
    }

    /// Register a compute module under a name (the QuickJS runner, a future
    /// Python runner). Registered names win over workspace paths in
    /// `run_module`, and `run_js` requires its runner to be registered.
    pub fn with_module(
        mut self,
        name: impl Into<String>,
        bytes: impl Into<Arc<[u8]>>,
    ) -> TieredSandboxes {
        self.modules.insert(name.into(), bytes.into());
        self
    }

    /// Register the embedded QuickJS runner so `run_js` works (feature
    /// `quickjs`). Shorthand for `with_module(QJS_MODULE, quickjs_module())`.
    #[cfg(feature = "quickjs")]
    pub fn with_quickjs(self) -> TieredSandboxes {
        self.with_module(crate::compute::QJS_MODULE, quickjs_module())
    }
}

/// The committed QuickJS runner artifact (feature `quickjs`): a hermetic
/// interpreter compiled from `guest/qjs-runner`. Register it under
/// [`TieredSandboxes::with_quickjs`], or hand it to `with_module` under a
/// name of your own.
#[cfg(feature = "quickjs")]
pub fn quickjs_module() -> Arc<[u8]> {
    Arc::from(*include_bytes!("../modules/qjs.wasm"))
}

impl SandboxProvider for TieredSandboxes {
    fn open(
        &self,
        session: &SessionId,
        profile: &SandboxProfile,
    ) -> BoxFuture<'static, Result<Arc<dyn Sandbox>, SandboxError>> {
        let name = sanitize(session.as_str());
        let root = Arc::clone(&self.root);
        let seed = content_digest(&format!("{:016x}|{}", self.seed, session.as_str()));
        let limits = profile.compute;
        #[cfg(feature = "compute")]
        let modules = Arc::new(self.modules.clone());
        let stats = self.stats.clone();
        Box::pin(async move {
            root.create_dir_all(&name)
                .and_then(|()| root.open_dir(&name))
                .map(|dir| {
                    stats.opened.fetch_add(1, Ordering::SeqCst);
                    // Opening grants `Workspace` and nothing else (harness
                    // spec §5.6 item 1): no other tier environment exists
                    // until a call carries its tier.
                    Arc::new(TieredSandbox {
                        name,
                        root,
                        dir: Arc::new(dir),
                        seed,
                        limits,
                        #[cfg(feature = "compute")]
                        modules,
                        #[cfg(feature = "compute")]
                        compute: std::sync::Mutex::new(None),
                        stats,
                    }) as Arc<dyn Sandbox>
                })
                .map_err(|e| SandboxError(format!("workspace open: {e}")))
        })
    }
}

/// One session's environments: the workspace handle always, the compute
/// engine once a call needs it.
struct TieredSandbox {
    /// The workspace's directory name under the root, for teardown.
    name: String,
    root: Arc<Dir>,
    /// The capability handle (S1): the only filesystem any tier sees.
    dir: Arc<Dir>,
    /// The session's injected seed (S2). Consumed by the compute tier only.
    #[cfg_attr(not(feature = "compute"), allow(dead_code))]
    seed: u64,
    /// Compute resource limits from the profile (sandbox spec §3.2).
    #[cfg_attr(not(feature = "compute"), allow(dead_code))]
    limits: ComputeLimits,
    /// The provider's registered compute modules, shared across sessions.
    #[cfg(feature = "compute")]
    modules: Arc<BTreeMap<String, Arc<[u8]>>>,
    /// The compute tier, built on the first `Compute` call (§2.3 item 2).
    #[cfg(feature = "compute")]
    compute: std::sync::Mutex<Option<Arc<crate::compute::ComputeTier>>>,
    stats: TierStats,
}

/// Distinguish a lost environment from an ordinary failure (harness spec
/// §5.5): when a call failed and the session's workspace directory no longer
/// exists under the root — an external deletion, or a concurrent activation
/// of the same session releasing it — the environment itself is gone.
/// `EnvironmentLost` is the outcome that engages the harness's reset
/// protocol (drop the binding, journal `WorkspaceReset`); reporting the same
/// condition as `ToolError::Sandbox` would have the model retrying against
/// state that no longer exists.
fn escalate_loss(
    result: Result<Value, ToolError>,
    root: &Dir,
    workspace: &str,
) -> Result<Value, ToolError> {
    match result {
        Err(ToolError::Sandbox(e)) if root.metadata(workspace).is_err() => Err(
            ToolError::EnvironmentLost(format!("workspace directory is gone: {e}")),
        ),
        other => other,
    }
}

impl Sandbox for TieredSandbox {
    fn call(
        &self,
        tier: Tier,
        name: &str,
        input: Value,
    ) -> BoxFuture<'static, Result<Value, ToolError>> {
        let root = Arc::clone(&self.root);
        let workspace = self.name.clone();
        match tier {
            Tier::Workspace => {
                let dir = Arc::clone(&self.dir);
                let name = name.to_string();
                Box::pin(async move {
                    escalate_loss(
                        crate::workspace::call(&dir, &name, &input),
                        &root,
                        &workspace,
                    )
                })
            }
            #[cfg(feature = "compute")]
            Tier::Compute => {
                let engine = {
                    // Poison recovery, not propagation: a panic in a prior
                    // call must degrade to per-call ToolError outcomes
                    // (harness spec §5.4), never poison every later call.
                    let mut slot = self
                        .compute
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    match slot.as_ref() {
                        Some(engine) => Ok(Arc::clone(engine)),
                        None => {
                            // Provisioned lazily, on the first call that
                            // carries the tier (sandbox spec §2.3 item 2).
                            crate::compute::ComputeTier::new(
                                self.limits,
                                Arc::clone(&self.modules),
                                self.stats.clone(),
                            )
                            .map(|engine| {
                                self.stats.compute_built.fetch_add(1, Ordering::SeqCst);
                                let engine = Arc::new(engine);
                                *slot = Some(Arc::clone(&engine));
                                engine
                            })
                        }
                    }
                };
                let dir = Arc::clone(&self.dir);
                let seed = self.seed;
                let name = name.to_string();
                Box::pin(async move {
                    escalate_loss(engine?.run(&dir, seed, &name, &input), &root, &workspace)
                })
            }
            other => {
                // Not offered by this provider (crate docs): a per-call
                // failure the model reacts to (harness spec §5.4), and
                // unreachable in a correctly capped deployment (§5.3 item 4).
                let message = format!("tier {other:?} is not offered by this provider");
                Box::pin(async move { Err(ToolError::Sandbox(message)) })
            }
        }
    }

    fn release(&self) -> BoxFuture<'static, ()> {
        // Tear down every provisioned tier's environment (S5): the compute
        // engine drops here; the workspace directory is removed through the
        // root handle. Idempotent — NotFound is success.
        #[cfg(feature = "compute")]
        {
            self.compute
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
        }
        let root = Arc::clone(&self.root);
        let name = self.name.clone();
        let stats = self.stats.clone();
        Box::pin(async move {
            let _ = root.remove_dir_all(&name);
            stats.released.fetch_add(1, Ordering::SeqCst);
        })
    }
}
