//! The `Native` tier (sandbox spec §3.4): OS processes inside an OCI container
//! — the development fallback of sandbox spec §3.5, not the microVM grade.
//!
//! The container itself is `machine_host::container`'s, shared with the persistent
//! machine's binding (machine §2.1); what is here is what a `Native` call means:
//! the shell tool, the bind-mounted workspace, and the loss conduct of §4.
//!
//! Confinement here is **shared-kernel** (§3.4's SHOULD grade): the
//! container sees the session workspace bind-mounted at `/workspace`, no
//! network (`--network none`), and nothing else, but the guest still speaks
//! to the host kernel, so the guarantee is priced by kernel
//! privilege-escalation bugs. Multi-tenant deployments SHOULD NOT rely on it
//! alone.
//!
//! Conduct:
//!
//! - **Timeouts bound the outcome, not the effect.** The harness enforces a
//!   tool timeout by dropping the call future, which kills the `docker exec`
//!   *client* only (`kill_on_drop`); the process inside the container
//!   survives until [`NativeTier::release`]'s `rm -f`. That is the contract
//!   `ToolError::Timeout` documents ("the call's effects may still land"),
//!   and `--pids-limit` is the fork-bomb guard in the meantime.
//! - **The bind mount is composed, not opened.** The provider retains the
//!   workspaces root as a host path solely to compose the `-v` argument it
//!   hands to docker: the mount is performed by the docker daemon, never an
//!   ambient filesystem operation by this crate, so S1 survives with this
//!   one documented composition.
//! - **Loss is the mechanism's to discriminate.** `machine_host::container` tells
//!   the container being gone (`GuestError::Gone`, judged from the daemon's own
//!   stderr rather than an exit code a user command can forge) from a command
//!   that merely failed. This tier turns the former into single-tier loss
//!   (sandbox spec §4): the slot is forgotten and the next call re-provisions
//!   lazily under the acquisition this activation already journaled. A vanished
//!   *workspace* escalates to `EnvironmentLost` in the provider, as for every
//!   tier.
//! - **Native calls need a tokio runtime** (`tokio::process`). The
//!   workspace and compute tiers remain runtime-agnostic.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use harness::OnDangling;
use harness::Tier;
use harness::ToolDecl;
use harness::ToolError;
use machine_host::GuestError;
use machine_host::GuestKey;
use machine_host::MachineGuest;
use machine_host::container::ContainerGuest;
use machine_host::container::ContainerHost;
use machine_host::container::ContainerSpec;
use serde_json::Value;
use serde_json::json;

use crate::provider::TierStats;
use crate::provider::capped;

/// Fork-bomb guard on the container (`--pids-limit`).
const PIDS_LIMIT: u32 = 512;

/// Names this tier's containers apart from any other consumer's on one host.
const CONTAINER_PREFIX: &str = "harness-sb";

/// The native tier's tool declaration, ready for [`harness::Kind::tool`]:
/// arbitrary commands are not idempotent, so a dangling call interrupts and
/// the model decides whether to retry the side effect
/// (`OnDangling::Interrupt`, harness spec §5.5).
pub fn shell_tool() -> ToolDecl {
    ToolDecl {
        name: "shell".to_string(),
        description: "Run a POSIX shell command (`/bin/sh -c`) inside the session's \
                      container. The session workspace is mounted at /workspace (the \
                      working directory); there is no network. Returns exit_code, \
                      stdout, and stderr."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "The shell command to run."}
            },
            "required": ["command"]
        }),
        tier: Tier::Native,
        on_dangling: OnDangling::Interrupt,
        timeout: None,
    }
}

/// One session's native tier: a container provisioned lazily on the first
/// `Native` call (sandbox spec §2.3 item 2), removed on release (S5).
pub(crate) struct NativeTier {
    /// This node's container mechanism, which owns every CLI invocation
    /// (`machine_host::container`): the tier owns what a `Native` call *means*.
    host: ContainerHost,
    /// The profile's image reference, provider-interpreted (harness spec
    /// §5.3 item 4). Empty fails per-call: no silent network-pulled default.
    image: String,
    /// Host path of the session workspace, retained for the bind mount only
    /// (module docs).
    host_workspace: PathBuf,
    /// Names this session's container. A name, not just a handle: a provision
    /// future dropped after `docker run` succeeds would leak a container no
    /// handle records — the name keeps the orphan addressable. Carries a digest
    /// of the host workspace path so two providers holding the same session id
    /// never contend for one name.
    key: GuestKey,
    /// The provisioned container. tokio's mutex, deliberately: provisioning
    /// awaits across the lock, and tokio mutexes cannot poison.
    container: tokio::sync::Mutex<Option<Arc<ContainerGuest>>>,
    /// Whether provisioning was ever attempted. Lets `release` return before
    /// constructing any `tokio::process::Command` when no Native call ever
    /// ran, so workspace-only callers can poll the release future outside a
    /// tokio runtime; set *before* the first docker invocation so a dropped
    /// provision still gets its release-time sweep.
    attempted: AtomicBool,
    stats: TierStats,
}

impl NativeTier {
    pub(crate) fn new(
        cli: String,
        image: String,
        host_workspace: PathBuf,
        workspace_name: &str,
        stats: TierStats,
    ) -> NativeTier {
        let disambiguator = harness::session::content_digest(&host_workspace.display().to_string());
        NativeTier {
            host: ContainerHost::new(cli, CONTAINER_PREFIX),
            image,
            host_workspace,
            // No node scope: a sandbox's containers are named by the session
            // workspace they mount, which is already node-local.
            key: GuestKey::new("", format!("{workspace_name}-{:08x}", disambiguator as u32)),
            container: tokio::sync::Mutex::new(None),
            attempted: AtomicBool::new(false),
            stats,
        }
    }

    /// Execute one Native call (`shell` only).
    pub(crate) async fn call(&self, name: &str, input: &Value) -> Result<Value, ToolError> {
        if name != "shell" {
            return Err(crate::provider::unknown_tool(name));
        }
        let command = input
            .get("command")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidArguments("`command` must be a string".to_string()))?
            .to_string();
        if self.image.is_empty() {
            // Checked before any docker invocation: an unconfigured profile
            // fails identically with or without docker installed.
            return Err(ToolError::Sandbox(
                "native: the kind's SandboxProfile.image is empty; the docker-backed \
                 Native tier needs an image reference and pulls no default"
                    .to_string(),
            ));
        }
        let guest = self.container().await?;
        match guest.exec(&["/bin/sh", "-c", &command]).await {
            // A nonzero exit is an outcome the model reacts to, not an error;
            // the mechanism has already told them apart (`GuestError::Gone` is
            // the container being gone, never a command's own failure).
            Ok(output) => Ok(json!({
                "exit_code": output.status.code(),
                "stdout": capped(&output.stdout),
                "stderr": capped(&output.stderr),
            })),
            Err(e) => {
                // The container (or the daemon under it) is gone while the
                // workspace survives: single-tier loss (sandbox spec §4).
                // Forget the slot — the next call MAY re-provision lazily under
                // the acquisition this activation already journaled — and fail
                // this call as an ordinary outcome, never a silent re-grant.
                if matches!(e, GuestError::Gone(_)) {
                    self.forget(&guest).await;
                }
                Err(ToolError::Sandbox(format!("native: exec: {e}")))
            }
        }
    }

    /// Provision-or-get under the lock. The lock is held across the whole
    /// provision so two concurrent first calls cannot create two containers;
    /// the exec itself runs after the guard drops.
    async fn container(&self) -> Result<Arc<ContainerGuest>, ToolError> {
        let mut slot = self.container.lock().await;
        if let Some(guest) = slot.as_ref() {
            return Ok(Arc::clone(guest));
        }
        let guest = Arc::new(self.provision().await?);
        *slot = Some(Arc::clone(&guest));
        Ok(guest)
    }

    /// Run the session's container (lazily, on the first call that carries the
    /// tier — sandbox spec §2.3 item 2).
    async fn provision(&self) -> Result<ContainerGuest, ToolError> {
        self.attempted.store(true, Ordering::SeqCst);
        let mut spec = ContainerSpec::image(self.key.clone(), &self.image)
            .mount(&self.host_workspace, "/workspace", false)
            .workdir("/workspace")
            .pids_limit(PIDS_LIMIT)
            // `--entrypoint sleep` rather than trusting the image's CMD;
            // 2147483647 seconds rather than `infinity`, which busybox rejects.
            .entrypoint("sleep", ["2147483647".to_string()]);
        // Without `--user`, files the container creates in the mount are
        // root-owned on Linux and release()'s host-side removal fails (S5).
        // The uid usually has no passwd entry in the image — acceptable for
        // `shell`. (Docker Desktop on macOS maps ownership regardless.)
        #[cfg(unix)]
        {
            spec = spec.user(format!(
                "{}:{}",
                rustix::process::getuid().as_raw(),
                rustix::process::getgid().as_raw()
            ));
        }
        let guest = self
            .host
            .start_container(spec)
            .await
            .map_err(|e| ToolError::Sandbox(format!("native: provision: {e}")))?;
        self.stats.count_native_built();
        Ok(guest)
    }

    /// Clear the slot iff it still holds `failed`, so a concurrent
    /// re-provision is never forgotten by a straggling loser. Identity is the
    /// handle, not the name: a re-provision reuses the name.
    async fn forget(&self, failed: &Arc<ContainerGuest>) {
        let mut slot = self.container.lock().await;
        if slot.as_ref().is_some_and(|held| Arc::ptr_eq(held, failed)) {
            *slot = None;
        }
    }

    /// Remove the container (S5). Idempotent, and *awaited*: when this returns
    /// the container is gone, not being removed — a caller may create the next
    /// one under the same name immediately.
    ///
    /// Stopping the guest we hold, rather than removing by name over the top of
    /// it, is what keeps that true: `stop` latches the handle so its `Drop` does
    /// not fire a second, detached removal that would still be in flight here.
    /// The name-based sweep is for the orphan case alone — a provision dropped
    /// after `docker run` succeeded, whose handle no caller ever received.
    pub(crate) async fn release(&self) {
        if let Some(guest) = self.container.lock().await.take() {
            guest.stop().await;
            return;
        }
        if !self.attempted.load(Ordering::SeqCst) {
            return;
        }
        self.host.remove(&self.key).await;
    }
}
