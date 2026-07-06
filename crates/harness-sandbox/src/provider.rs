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

#[cfg(feature = "native")]
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
    native_built: Arc<AtomicUsize>,
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

    /// Containers actually provisioned: lazy on first use, once per held
    /// tier per activation barring single-tier loss (sandbox spec §4).
    pub fn native_built(&self) -> usize {
        self.native_built.load(Ordering::SeqCst)
    }

    #[cfg(feature = "native")]
    pub(crate) fn count_native_built(&self) {
        self.native_built.fetch_add(1, Ordering::SeqCst);
    }
}

/// The tiered sandbox provider (sandbox spec §3): `Workspace` by capability
/// handle, `Compute` (feature `compute`) by hermetic wasmtime guests,
/// `Native` (feature `native`) by an OCI container behind the docker CLI —
/// or, configured via [`TieredSandboxes::with_firecracker`] (feature
/// `firecracker`), by a Firecracker microVM — and nothing else; see the
/// crate docs for the offered set.
pub struct TieredSandboxes {
    /// Base seed for compute determinism (S2). The per-session seed is a
    /// digest of base and session id, so fixing the base fixes every guest.
    seed: u64,
    /// Deployment-registered compute modules (the QuickJS runner and kin):
    /// resolved by name before any workspace path, so a guest write can
    /// never shadow them. Sharing them across sessions is pre-warming, which
    /// sandbox spec §2.3 item 2 permits: a module is code, not working
    /// state — every call still gets a fresh store.
    modules: BTreeMap<String, Arc<[u8]>>,
    /// The container CLI binary for the native tier; `docker` by default.
    #[cfg(feature = "native")]
    container_cli: String,
    /// When set, the native tier runs at the microVM grade instead of the
    /// docker fallback (sandbox spec §3.5): one Firecracker VM per
    /// activation, workspace synced over vsock.
    #[cfg(feature = "firecracker")]
    firecracker: Option<Arc<crate::firecracker::FirecrackerConfig>>,
    pub stats: TierStats,
}

impl Default for TieredSandboxes {
    fn default() -> TieredSandboxes {
        TieredSandboxes::new()
    }
}

impl TieredSandboxes {
    /// Build the provider. Each session's workspace directory arrives at
    /// [`open`](SandboxProvider::open) — owned by the caller (the agent's
    /// workspace facet), opened here as a capability handle so every tier
    /// after the open reaches the filesystem through that handle (S1).
    pub fn new() -> TieredSandboxes {
        TieredSandboxes {
            seed: 0,
            modules: BTreeMap::new(),
            #[cfg(feature = "native")]
            container_cli: "docker".to_string(),
            #[cfg(feature = "firecracker")]
            firecracker: None,
            stats: TierStats::default(),
        }
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

    /// Set the container CLI binary the native tier drives (feature
    /// `native`); `docker` by default. Podman's docker-compatible CLI and
    /// colima answer to the same vocabulary.
    #[cfg(feature = "native")]
    pub fn with_container_cli(mut self, cli: impl Into<String>) -> TieredSandboxes {
        self.container_cli = cli.into();
        self
    }

    /// Run the native tier at the microVM grade (feature `firecracker`):
    /// one Firecracker VM per activation instead of the docker fallback.
    /// Runtime needs Linux and `/dev/kvm`; where they are absent the first
    /// `Native` call fails as a `ToolError` outcome (harness spec §5.4). The
    /// profile's `image`, when non-empty, selects a base rootfs path over
    /// `config.rootfs`.
    #[cfg(feature = "firecracker")]
    pub fn with_firecracker(
        mut self,
        config: crate::firecracker::FirecrackerConfig,
    ) -> TieredSandboxes {
        self.firecracker = Some(Arc::new(config));
        self
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
        workspace: &std::path::Path,
    ) -> BoxFuture<'static, Result<Arc<dyn Sandbox>, SandboxError>> {
        #[cfg(feature = "native")]
        let name = sanitize(session.as_str());
        let workspace = workspace.to_path_buf();
        let seed = content_digest(&format!("{:016x}|{}", self.seed, session.as_str()));
        let limits = profile.compute;
        #[cfg(feature = "compute")]
        let modules = Arc::new(self.modules.clone());
        // §2.2 of the sandbox spec: holding `Native` implies `Network`'s
        // grants. Neither native realization has a dataplane — the container
        // runs `--network none`, the microVM has no network device — so both
        // deliver exactly an *empty* egress allowlist and nothing more. A
        // profile that names egress (or explicitly caps in `Network`) asks
        // for a dataplane this provider does not have: fail the open loudly,
        // never silently withhold a granted capability. Feature-gated
        // because without `native` a Network-capped profile already fails
        // per call as an unoffered tier — the established conduct.
        #[cfg(feature = "native")]
        if !profile.egress.is_empty()
            || profile
                .tier_cap
                .as_ref()
                .is_some_and(|cap| cap.contains(&Tier::Network))
        {
            return Box::pin(async move {
                Err(SandboxError(
                    "this provider offers Native without a network dataplane: the profile's \
                     egress allowlist must be empty and the cap must not include Network"
                        .to_string(),
                ))
            });
        }
        #[cfg(feature = "native")]
        let container_cli = self.container_cli.clone();
        #[cfg(feature = "native")]
        let image = profile.image.clone();
        #[cfg(feature = "firecracker")]
        let firecracker = self.firecracker.clone();
        let stats = self.stats.clone();
        Box::pin(async move {
            // The caller (the agent's workspace facet) owns and materialized
            // the directory; opening it as a capability handle is this crate's
            // only use of ambient authority — every tier after this reaches
            // the filesystem through the handle (S1). `create_dir_all` is a
            // defensive no-op on the normal path.
            std::fs::create_dir_all(&workspace)
                .map_err(|e| SandboxError(format!("workspace open: {e}")))?;
            // Canonicalized so docker's `-v` (and the microVM's tar source)
            // gets an absolute, symlink-free host path (macOS `/tmp` is a
            // symlink into `/private`).
            #[cfg(feature = "native")]
            let host_path = std::fs::canonicalize(&workspace)
                .map_err(|e| SandboxError(format!("workspace open: {e}")))?;
            let dir = Dir::open_ambient_dir(&workspace, cap_std::ambient_authority())
                .map_err(|e| SandboxError(format!("workspace open: {e}")))?;
            stats.opened.fetch_add(1, Ordering::SeqCst);
            let dir = Arc::new(dir);
            // The tier struct is cheap and eager; the *environment*
            // (container or microVM) is what stays lazy (sandbox spec §2.3
            // item 2). Only the configured realization is constructed.
            #[cfg(feature = "native")]
            let native = {
                #[cfg(feature = "firecracker")]
                let native = match firecracker {
                    Some(config) => {
                        NativeEnv::MicroVm(Arc::new(crate::firecracker::FirecrackerTier::new(
                            config,
                            image,
                            Arc::clone(&dir),
                            &host_path,
                            stats.clone(),
                        )))
                    }
                    None => NativeEnv::Docker(Arc::new(crate::native::NativeTier::new(
                        container_cli,
                        image,
                        host_path.clone(),
                        &name,
                        stats.clone(),
                    ))),
                };
                #[cfg(not(feature = "firecracker"))]
                let native = NativeEnv::Docker(Arc::new(crate::native::NativeTier::new(
                    container_cli,
                    image,
                    host_path.clone(),
                    &name,
                    stats.clone(),
                )));
                native
            };
            // Opening grants `Workspace` and nothing else (harness spec §5.6
            // item 1): no other tier environment exists until a call carries
            // its tier.
            Ok(Arc::new(TieredSandbox {
                path: workspace,
                dir,
                seed,
                limits,
                #[cfg(feature = "compute")]
                modules,
                #[cfg(feature = "compute")]
                compute: std::sync::Mutex::new(None),
                #[cfg(feature = "native")]
                native,
                stats,
            }) as Arc<dyn Sandbox>)
        })
    }
}

/// One session's environments: the workspace handle always, the compute
/// engine once a call needs it.
struct TieredSandbox {
    /// The workspace's host path, retained solely to detect a vanished
    /// directory (see [`escalate_loss`]). The directory itself is owned by
    /// the caller; this sandbox never deletes it.
    path: std::path::PathBuf,
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
    /// The native tier: the struct is eager, its *environment* (container or
    /// microVM) is built on the first `Native` call (§2.3 item 2).
    #[cfg(feature = "native")]
    native: NativeEnv,
    stats: TierStats,
}

/// Which realization answers `Native` for this sandbox (sandbox spec §3.5):
/// the docker fallback, or — when the provider was configured with
/// [`TieredSandboxes::with_firecracker`] — the microVM grade.
#[cfg(feature = "native")]
#[derive(Clone)]
enum NativeEnv {
    Docker(Arc<crate::native::NativeTier>),
    #[cfg(feature = "firecracker")]
    MicroVm(Arc<crate::firecracker::FirecrackerTier>),
}

#[cfg(feature = "native")]
impl NativeEnv {
    async fn call(&self, name: &str, input: &Value) -> Result<Value, ToolError> {
        match self {
            NativeEnv::Docker(tier) => tier.call(name, input).await,
            #[cfg(feature = "firecracker")]
            NativeEnv::MicroVm(tier) => tier.call(name, input).await,
        }
    }

    async fn release(&self) {
        match self {
            NativeEnv::Docker(tier) => tier.release().await,
            #[cfg(feature = "firecracker")]
            NativeEnv::MicroVm(tier) => tier.release().await,
        }
    }
}

/// Distinguish a lost environment from an ordinary failure (harness spec
/// §5.5): when a call failed and the session's workspace directory no longer
/// exists on disk — an external deletion, or a wiped scratch dir — the
/// environment itself is gone. `EnvironmentLost` is the outcome that engages
/// the harness's reset protocol (drop the binding, journal `WorkspaceReset`);
/// reporting the same condition as `ToolError::Sandbox` would have the model
/// retrying against state that no longer exists. An ambient stat, not a
/// handle read: the open handle keeps a deleted directory's fd alive, so only
/// the path can witness the deletion.
fn escalate_loss(result: Result<Value, ToolError>, path: &std::path::Path) -> Result<Value, ToolError> {
    match result {
        Err(ToolError::Sandbox(e)) if std::fs::metadata(path).is_err() => Err(
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
        let path = self.path.clone();
        match tier {
            Tier::Workspace => {
                let dir = Arc::clone(&self.dir);
                let name = name.to_string();
                Box::pin(async move {
                    escalate_loss(crate::workspace::call(&dir, &name, &input), &path)
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
                Box::pin(
                    async move { escalate_loss(engine?.run(&dir, seed, &name, &input), &path) },
                )
            }
            #[cfg(feature = "native")]
            Tier::Native => {
                let native = self.native.clone();
                let name = name.to_string();
                Box::pin(async move {
                    // escalate_loss gives §4's asymmetry: container gone but
                    // workspace intact stays a Sandbox outcome (single-tier
                    // loss); workspace gone is EnvironmentLost.
                    escalate_loss(native.call(&name, &input).await, &path)
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
        // engine drops here; the native container is removed (a no-op
        // touching no tokio API when none was ever provisioned, so this
        // future stays pollable outside a tokio runtime for workspace-only
        // sessions). The workspace directory is NOT removed — the caller owns
        // it (the agent's workspace facet), and its durable content survives
        // the activation by design.
        #[cfg(feature = "compute")]
        {
            self.compute
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
        }
        #[cfg(feature = "native")]
        let native = self.native.clone();
        let stats = self.stats.clone();
        Box::pin(async move {
            #[cfg(feature = "native")]
            native.release().await;
            stats.released.fetch_add(1, Ordering::SeqCst);
        })
    }
}
