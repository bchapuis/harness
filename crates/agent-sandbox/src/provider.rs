//! The provider: one workspace directory per session, held as a capability
//! handle; tier environments built lazily on the first call that carries
//! them (sandbox spec §3.3 item 2).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use cap_std::fs::Dir;
use agent::BoxFuture;
use agent::ComputeLimits;
use agent::Sandbox;
use agent::SandboxError;
use agent::SandboxProfile;
use agent::SandboxProvider;
use agent::SessionId;
use agent::Tier;
use agent::ToolDecl;
use agent::ToolError;
use serde_json::Value;

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

    fn count_opened(&self) {
        self.opened.fetch_add(1, Ordering::SeqCst);
    }

    pub fn released(&self) -> usize {
        self.released.load(Ordering::SeqCst)
    }

    fn count_released(&self) {
        self.released.fetch_add(1, Ordering::SeqCst);
    }

    #[cfg(feature = "compute")]
    fn count_compute_built(&self) {
        self.compute_built.fetch_add(1, Ordering::SeqCst);
    }

    pub fn compute_built(&self) -> usize {
        self.compute_built.load(Ordering::SeqCst)
    }

    /// Modules actually compiled (cache misses).
    pub fn modules_compiled(&self) -> usize {
        self.modules_compiled.load(Ordering::SeqCst)
    }

    #[cfg(feature = "compute")]
    pub(crate) fn count_module_compiled(&self) {
        self.modules_compiled.fetch_add(1, Ordering::SeqCst);
    }

    /// Containers actually provisioned: lazy on first use, once per held
    /// tier per activation barring single-tier loss (sandbox spec §5).
    pub fn native_built(&self) -> usize {
        self.native_built.load(Ordering::SeqCst)
    }

    #[cfg(feature = "native")]
    pub(crate) fn count_native_built(&self) {
        self.native_built.fetch_add(1, Ordering::SeqCst);
    }
}

/// The tiered sandbox provider (sandbox spec §4): `Workspace` by capability
/// handle, `Compute` by hermetic wasmtime guests, `Native` by an OCI
/// container or a Firecracker microVM, and nothing else. The crate docs hold
/// the offered set.
pub struct TieredSandboxes {
    /// Base seed for compute determinism (S2). The per-session seed is a
    /// digest of base and session id, so fixing the base fixes every guest.
    seed: u64,
    /// Deployment-registered compute modules (the QuickJS runner and kin):
    /// resolved by name before any workspace path, so a guest write can
    /// never shadow them. Shared across sessions, which sandbox spec §3.3
    /// item 2 permits: a module is code, not working state; every call
    /// still gets a fresh store.
    modules: BTreeMap<String, Arc<[u8]>>,
    /// The container CLI binary the native tier drives, when the deployment
    /// names one. `None` names no container realization at all, which is how a
    /// deployment asks for a provider without a `Native` tier: no `shell` is
    /// declared, and every `Native` call fails as unconfigured.
    #[cfg(feature = "native")]
    container_cli: Option<String>,
    /// When set, the native tier runs at the microVM grade instead of the
    /// docker fallback (sandbox spec §4): one Firecracker VM per
    /// activation, workspace synced over vsock.
    #[cfg(feature = "firecracker")]
    firecracker: Option<Arc<crate::firecracker::FirecrackerConfig>>,
    stats: TierStats,
}

impl Default for TieredSandboxes {
    fn default() -> TieredSandboxes {
        TieredSandboxes::new()
    }
}

impl TieredSandboxes {
    /// Build the provider. Each session's workspace directory arrives at
    /// [`open`](SandboxProvider::open), owned by the caller (the agent's
    /// workspace facet) and opened there as a capability handle (S1).
    pub fn new() -> TieredSandboxes {
        TieredSandboxes {
            seed: 0,
            modules: BTreeMap::new(),
            #[cfg(feature = "native")]
            container_cli: None,
            #[cfg(feature = "firecracker")]
            firecracker: None,
            stats: TierStats::default(),
        }
    }

    /// Fix the base seed (S2): pinning it makes every guest's injected
    /// entropy reproducible.
    pub fn with_seed(mut self, seed: u64) -> TieredSandboxes {
        self.seed = seed;
        self
    }

    /// Register a compute module under a name. Registered names win over
    /// workspace paths in `run_module`, and `run_js` requires its runner to
    /// be registered.
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

    /// Run the native tier through this container CLI binary (feature
    /// `native`), usually `docker`. Podman's docker-compatible CLI and colima
    /// answer to the same vocabulary. Naming one is what configures the
    /// container realization: a provider that names neither a CLI here nor a
    /// Firecracker profile has no native realization, so it declares no
    /// `shell` and fails every `Native` call.
    #[cfg(feature = "native")]
    pub fn with_container_cli(mut self, cli: impl Into<String>) -> TieredSandboxes {
        self.container_cli = Some(cli.into());
        self
    }

    /// Run the native tier at the microVM grade (feature `firecracker`):
    /// one Firecracker VM per activation instead of the docker fallback.
    /// Runtime needs Linux and `/dev/kvm`; where they are absent the first
    /// `Native` call fails as a `ToolError` outcome (agent spec §4.2). The
    /// profile's `image` names the base rootfs a session boots.
    #[cfg(feature = "firecracker")]
    pub fn with_firecracker(
        mut self,
        config: crate::firecracker::FirecrackerConfig,
    ) -> TieredSandboxes {
        self.firecracker = Some(Arc::new(config));
        self
    }

    /// Observable provider activity ([`TierStats`]).
    pub fn stats(&self) -> &TierStats {
        &self.stats
    }

    /// The tool declarations this provider, as configured, answers: the
    /// `Workspace` file tools always; `run_js` once the QuickJS runner is
    /// registered; and `shell` in the declaration of the native realization
    /// configured (the microVM's sync semantics are model-visible, so its
    /// declaration differs from the container's). A deployment registers
    /// exactly these on its kinds, so the digest cannot assert an environment
    /// this provider does not have.
    pub fn tools(&self) -> Vec<ToolDecl> {
        let mut tools = crate::workspace::workspace_tools();
        tools.extend(self.run_js());
        tools.extend(self.shell());
        tools
    }

    /// `run_js`, once the QuickJS runner module is registered.
    fn run_js(&self) -> Option<ToolDecl> {
        #[cfg(feature = "compute")]
        {
            self.modules
                .contains_key(crate::compute::QJS_MODULE)
                .then(crate::compute::run_js_tool)
        }
        #[cfg(not(feature = "compute"))]
        {
            None
        }
    }

    /// Whether this provider serves a `shell` tool. The deployment's guidance to
    /// the model turns on it, and which tiers are configured is the provider's to
    /// answer, not something a caller should re-derive from a declaration's name.
    pub fn has_shell(&self) -> bool {
        self.shell().is_some()
    }

    /// `shell`, in the declaration of the native realization configured, and
    /// `None` when none is: a provider that names neither a Firecracker
    /// profile nor a container CLI builds an unconfigured native tier, where
    /// every `Native` call fails, so declaring `shell` there would advertise
    /// an environment the provider does not have.
    fn shell(&self) -> Option<ToolDecl> {
        #[cfg(feature = "firecracker")]
        if self.firecracker.is_some() {
            return Some(shell_decl(crate::firecracker::SHELL_DESCRIPTION));
        }
        #[cfg(feature = "native")]
        {
            self.container_cli
                .as_ref()
                .map(|_| shell_decl(crate::native::SHELL_DESCRIPTION))
        }
        #[cfg(not(feature = "native"))]
        {
            None
        }
    }
}

/// The `shell` declaration, ready for [`agent::Kind::declare`], in the words of
/// one native realization: the two realizations share everything but the
/// description, and the description differs on purpose, so that the digest
/// keeps a mixed-realization cluster from agreeing silently. Arbitrary
/// commands are not idempotent, so an orphaned call interrupts and the model
/// decides whether to retry the side effect (`OnOrphan::Interrupt`, agent
/// spec §4.4).
#[cfg(feature = "native")]
fn shell_decl(description: &str) -> ToolDecl {
    ToolDecl {
        name: "shell".to_string(),
        description: description.to_string(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "The shell command to run."}
            },
            "required": ["command"]
        }),
        tier: Tier::Native,
        on_orphan: agent::OnOrphan::Interrupt,
        timeout: None,
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
    /// Open a session's environment over `workspace`, the directory the caller
    /// owns (S1). Refused, as a [`SandboxError`], is any profile whose egress
    /// allowlist is non-empty or whose explicit cap contains `Network`:
    /// holding `Native` implies `Network`'s grants (sandbox spec §3.2), and
    /// neither native realization has a dataplane (`--network none` for the
    /// container, no network device in the microVM), so both deliver exactly
    /// an empty allowlist, and a profile asking for more must fail loudly here
    /// rather than have a granted capability silently withheld. The refusal
    /// is unconditional under every feature set, so this contract does not
    /// vary with which tiers a build compiles in (sandbox spec §4).
    fn open(
        &self,
        session: &SessionId,
        profile: &SandboxProfile,
        workspace: &std::path::Path,
    ) -> BoxFuture<'static, Result<Arc<dyn Sandbox>, SandboxError>> {
        let workspace = workspace.to_path_buf();
        let seed = crate::fnv64(format!("{:016x}|{}", self.seed, session.as_str()).as_bytes());
        let limits = profile.compute;
        #[cfg(feature = "compute")]
        let modules = Arc::new(self.modules.clone());
        // The `Network` refusal this method's contract states.
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
        let spec = NativeSpec {
            cli: self.container_cli.clone(),
            image: profile.image.clone(),
            session: session.as_str().to_string(),
            #[cfg(feature = "firecracker")]
            firecracker: self.firecracker.clone(),
        };
        let stats = self.stats.clone();
        Box::pin(async move {
            // The caller owns the directory and usually materialized it
            // already; open creates-if-missing rather than demanding it. The
            // loss detector below is unaffected: it watches for the directory
            // *vanishing after* this open. Workspace *contents* are reached
            // only through the capability handle opened here (S1); the
            // crate's ambient calls are this open, the loss stat sandbox
            // spec §5 requires, the bind-mount and tar-restore host path,
            // and the microVM's own assets, none of which reads the
            // workspace by path.
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
            stats.count_opened();
            let dir = Arc::new(dir);
            // The tier struct is cheap and eager; the *environment*
            // (container or microVM) is what stays lazy (sandbox spec §3.3
            // item 2).
            #[cfg(feature = "native")]
            let native = NativeEnv::build(spec, &dir, &host_path, stats.clone());
            // Opening grants `Workspace` and nothing else (agent spec §4.5
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

/// One session's environments: the workspace handle always; the compute
/// engine and the native tier's container or microVM once a call needs them.
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
    /// Compute resource limits from the profile (sandbox spec §4.2).
    #[cfg_attr(not(feature = "compute"), allow(dead_code))]
    limits: ComputeLimits,
    /// The provider's registered compute modules, shared across sessions.
    #[cfg(feature = "compute")]
    modules: Arc<BTreeMap<String, Arc<[u8]>>>,
    /// The compute tier, built on the first `Compute` call (§3.3 item 2).
    #[cfg(feature = "compute")]
    compute: std::sync::Mutex<Option<Arc<crate::compute::ComputeTier>>>,
    /// The native tier: the struct is eager, its *environment* (container or
    /// microVM) is built on the first `Native` call (§3.3 item 2).
    #[cfg(feature = "native")]
    native: NativeEnv,
    stats: TierStats,
}

/// The provider-level inputs the native tier's construction needs, cloned
/// into `open`'s future in one piece.
#[cfg(feature = "native")]
struct NativeSpec {
    cli: Option<String>,
    image: String,
    /// The raw session id, the readable half of the container's name.
    session: String,
    #[cfg(feature = "firecracker")]
    firecracker: Option<Arc<crate::firecracker::FirecrackerConfig>>,
}

/// Which realization answers `Native` for this sandbox (sandbox spec §4):
/// the docker fallback, or the microVM grade.
#[cfg(feature = "native")]
#[derive(Clone)]
enum NativeEnv {
    /// The kind's profile names no image, or the provider names no native
    /// realization to run it under: every `Native` call fails with the
    /// carried reason, with or without a container runtime or a hypervisor
    /// installed, and no environment is ever provisioned. The provider
    /// declares no `shell` in that second case (`TieredSandboxes::shell`),
    /// so the model is never offered a tool that can only fail.
    Unconfigured(&'static str),
    Docker(Arc<crate::native::NativeTier>),
    #[cfg(feature = "firecracker")]
    MicroVm(Arc<crate::firecracker::FirecrackerTier>),
}

#[cfg(feature = "native")]
impl NativeEnv {
    /// Build the configured realization.
    fn build(
        spec: NativeSpec,
        dir: &Arc<Dir>,
        host_path: &std::path::Path,
        stats: TierStats,
    ) -> NativeEnv {
        if spec.image.is_empty() {
            return NativeEnv::Unconfigured(
                "native: the kind's SandboxProfile.image is empty; the Native tier needs an \
                 image (a container image, or the microVM's base rootfs) and pulls no default",
            );
        }
        #[cfg(feature = "firecracker")]
        if let Some(config) = spec.firecracker {
            return NativeEnv::MicroVm(Arc::new(crate::firecracker::FirecrackerTier::new(
                config,
                spec.image,
                Arc::clone(dir),
                host_path,
                stats,
            )));
        }
        #[cfg(not(feature = "firecracker"))]
        let _ = dir;
        match spec.cli {
            Some(cli) => NativeEnv::Docker(Arc::new(crate::native::NativeTier::new(
                cli,
                spec.image,
                host_path.to_path_buf(),
                &spec.session,
                stats,
            ))),
            None => NativeEnv::Unconfigured(
                "native: this provider names neither a container CLI nor a Firecracker \
                 profile, so it has no native realization to run the image under",
            ),
        }
    }
    /// Execute one Native call (`shell` only): the declaration's checks once,
    /// then the configured realization runs the command.
    async fn call(&self, name: &str, input: &Value) -> Result<Value, ToolError> {
        if name != "shell" {
            return Err(unknown_tool(name));
        }
        let command = input
            .get("command")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidArguments("`command` must be a string".to_string()))?;
        match self {
            NativeEnv::Unconfigured(reason) => Err(ToolError::Sandbox(reason.to_string())),
            NativeEnv::Docker(tier) => tier.run(command).await,
            #[cfg(feature = "firecracker")]
            NativeEnv::MicroVm(tier) => tier.run(command).await,
        }
    }

    async fn release(&self) {
        match self {
            NativeEnv::Unconfigured(_) => {}
            NativeEnv::Docker(tier) => tier.release().await,
            #[cfg(feature = "firecracker")]
            NativeEnv::MicroVm(tier) => tier.release().await,
        }
    }
}

/// The shared refusal for a tool name no tier of this sandbox provides:
/// one message, so identical failures read identically in the journal.
pub(crate) fn unknown_tool(name: &str) -> ToolError {
    ToolError::Sandbox(format!("tool not provided by this sandbox: {name}"))
}

/// Cap on a captured output stream in a tool reply (the journal carries it).
#[cfg(feature = "native")]
const OUTPUT_CAP: usize = 16 * 1024;

/// UTF-8-safe truncation of a captured stream to [`OUTPUT_CAP`], shared by
/// the native realizations (docker and microVM).
#[cfg(feature = "native")]
pub(crate) fn capped(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.len() <= OUTPUT_CAP {
        return text.into_owned();
    }
    let mut end = OUTPUT_CAP;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… [truncated {} bytes]", &text[..end], text.len() - end)
}

/// Distinguish a lost environment from an ordinary outcome (agent spec
/// §4.4): when the session's workspace directory no longer exists on disk
/// the environment itself is gone, whatever the call reported.
/// `EnvironmentLost` engages the agent runtime's reset protocol (drop the binding,
/// journal `WorkspaceReset`). The loss outranks even an `Ok`: the open handle
/// keeps a deleted directory's fd alive, so a call can "succeed" against it
/// (an empty listing, a tar sync of nothing) while its effects have no
/// durable home; an ambient stat of the path, not a handle read, is the
/// only witness of the deletion.
fn escalate_loss(
    result: Result<Value, ToolError>,
    path: &std::path::Path,
) -> Result<Value, ToolError> {
    if let Err(stat) = std::fs::metadata(path) {
        let detail = match &result {
            Err(e) => format!("workspace directory is gone: {e} ({stat})"),
            Ok(_) => format!("workspace directory is gone: {stat}"),
        };
        return Err(ToolError::EnvironmentLost(detail));
    }
    result
}

#[cfg(feature = "compute")]
impl TieredSandbox {
    /// Provision-or-get the compute engine under the lock, lazily on the first
    /// call that carries the tier (sandbox spec §3.3 item 2). Poison recovery,
    /// not propagation: a panic in a prior call must degrade to per-call
    /// `ToolError` outcomes (agent spec §4.2), never poison every later call.
    fn compute_tier(&self) -> Result<Arc<crate::compute::ComputeTier>, ToolError> {
        let mut slot = self
            .compute
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match slot.as_ref() {
            Some(engine) => Ok(Arc::clone(engine)),
            None => crate::compute::ComputeTier::new(
                self.limits,
                Arc::clone(&self.modules),
                self.stats.clone(),
            )
            .map(|engine| {
                self.stats.count_compute_built();
                let engine = Arc::new(engine);
                *slot = Some(Arc::clone(&engine));
                engine
            }),
        }
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
                let engine = self.compute_tier();
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
                    // escalate_loss gives §5's asymmetry: container gone but
                    // workspace intact stays a Sandbox outcome (single-tier
                    // loss); workspace gone is EnvironmentLost.
                    escalate_loss(native.call(&name, &input).await, &path)
                })
            }
            other => {
                // Not offered by this provider (crate docs): a per-call
                // failure (agent spec §4.2), unreachable in a correctly
                // capped deployment (agent §2.4).
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
        // sessions). The workspace directory is NOT removed: the caller owns
        // it, and its durable content survives the activation.
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
            stats.count_released();
        })
    }
}
