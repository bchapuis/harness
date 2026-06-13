//! The `Native` tier (sandbox spec §3.4): OS processes inside an OCI
//! container driven through the `docker` CLI — the development fallback of
//! sandbox spec §3.5, not the microVM grade.
//!
//! Confinement here is **shared-kernel** (§3.4's SHOULD grade): the
//! container sees the session workspace bind-mounted at `/workspace`, no
//! network (`--network none`), and nothing else, but the guest still speaks
//! to the host kernel, so the guarantee is priced by kernel
//! privilege-escalation bugs. Multi-tenant deployments SHOULD NOT rely on it
//! alone. On macOS the container additionally sits behind Docker Desktop's
//! Linux VM — an incidental layer, still not the per-environment microVM
//! grade Firecracker gives.
//!
//! Conduct notes, in the spec's vocabulary:
//!
//! - **Timeouts bound the outcome, not the effect.** The harness enforces a
//!   tool timeout by dropping the call future, which kills the `docker exec`
//!   *client* only (`kill_on_drop`); the process inside the container
//!   survives until [`NativeTier::release`]'s `rm -f`. That is the contract
//!   `ToolError::Timeout` documents ("the call's effects may still land"),
//!   and `--pids-limit` is the cheap fork-bomb guard in the meantime.
//! - **The bind mount is composed, not opened.** The provider retains the
//!   workspaces root as a host path solely to compose the `-v` argument it
//!   hands to docker: the mount is performed by the docker daemon, an
//!   external confinement mechanism, never an ambient filesystem operation
//!   by this crate — the cap-std stance of S1 survives with this one
//!   documented composition.
//! - **Loss is discriminated by stderr, not exit code.** `docker exec` uses
//!   125/126/127 for its own failures, but a user command can exit 125 too;
//!   a daemon-reported error (the `Error response from daemon:` prefix and
//!   kin) is what marks the *container* gone. That is single-tier loss
//!   (sandbox spec §4): the slot is forgotten and the next call
//!   re-provisions lazily under the acquisition this activation already
//!   journaled. A vanished *workspace* escalates to `EnvironmentLost` in the
//!   provider, as for every tier.
//! - **Native calls need a tokio runtime** (`tokio::process`). The
//!   workspace and compute tiers remain runtime-agnostic.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use harness::OnDangling;
use harness::Tier;
use harness::ToolDecl;
use harness::ToolError;
use serde_json::Value;
use serde_json::json;

use crate::provider::TierStats;

/// Cap on each captured stream, so one chatty command cannot blow up the
/// journal and the model context it feeds. Shared with the firecracker
/// realization, which answers the same `shell` shape.
pub(crate) const OUTPUT_CAP: usize = 16 * 1024;

/// Fork-bomb guard on the container (`--pids-limit`).
const PIDS_LIMIT: &str = "512";

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
    /// The container CLI binary (`docker`; podman's compatible CLI and
    /// colima answer to the same vocabulary).
    cli: String,
    /// The profile's image reference, provider-interpreted (harness spec
    /// §5.3 item 4). Empty fails per-call: no silent network-pulled default.
    image: String,
    /// Host path of the session workspace, retained for the bind mount only
    /// (module docs).
    host_workspace: PathBuf,
    /// Deterministic container name. A name, not just an id: a provision
    /// future dropped after `docker run` succeeds would leak a container no
    /// id records — the name keeps the orphan addressable, for the
    /// pre-provision sweep and for release. Suffixed with a digest of the
    /// host workspace path so two providers (or two deployments) holding the
    /// same session id never contend for one name.
    container_name: String,
    /// The provisioned container id. tokio's mutex, deliberately:
    /// provisioning awaits across the lock, and tokio mutexes cannot poison
    /// — the same degrade-not-poison conduct the compute tier gets from
    /// poison recovery.
    container: tokio::sync::Mutex<Option<String>>,
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
            cli,
            image,
            container_name: format!("harness-sb-{workspace_name}-{:08x}", disambiguator as u32),
            host_workspace,
            container: tokio::sync::Mutex::new(None),
            attempted: AtomicBool::new(false),
            stats,
        }
    }

    /// Execute one Native call (`shell` only).
    pub(crate) async fn call(&self, name: &str, input: &Value) -> Result<Value, ToolError> {
        if name != "shell" {
            return Err(ToolError::Sandbox(format!(
                "tool not provided by this sandbox: {name}"
            )));
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
        let id = self.container_id().await?;
        let output = docker(&self.cli, ["exec", &id, "/bin/sh", "-c", &command])
            .await
            .map_err(|e| ToolError::Sandbox(format!("native: spawn {}: {e}", self.cli)))?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        if daemon_error(output.status.code(), &stderr) {
            // The container (or the daemon under it) is gone while the
            // workspace survives: single-tier loss (sandbox spec §4). Forget
            // the slot — the next call MAY re-provision lazily under the
            // acquisition this activation already journaled — and fail this
            // call as an ordinary outcome, never a silent re-grant.
            self.forget(&id).await;
            return Err(ToolError::Sandbox(format!(
                "native: exec: {}",
                capped(&output.stderr)
            )));
        }
        // A nonzero exit is an outcome the model reacts to, not an error.
        Ok(json!({
            "exit_code": output.status.code(),
            "stdout": capped(&output.stdout),
            "stderr": capped(&output.stderr),
        }))
    }

    /// Provision-or-get under the lock. The lock is held across the whole
    /// provision so two concurrent first calls cannot create two containers;
    /// the exec itself runs after the guard drops.
    async fn container_id(&self) -> Result<String, ToolError> {
        let mut slot = self.container.lock().await;
        if let Some(id) = slot.as_ref() {
            return Ok(id.clone());
        }
        let id = self.provision().await?;
        *slot = Some(id.clone());
        Ok(id)
    }

    /// `docker run` the session's container (lazily, on the first call that
    /// carries the tier — sandbox spec §2.3 item 2).
    async fn provision(&self) -> Result<String, ToolError> {
        self.attempted.store(true, Ordering::SeqCst);
        // Sweep any leftover from a previously dropped provision: the
        // deterministic name makes the orphan addressable. Best-effort.
        let _ = docker(&self.cli, ["rm", "-f", &self.container_name]).await;
        let mount = format!("{}:/workspace", self.host_workspace.display());
        let mut args: Vec<String> = [
            "run",
            "-d",
            "--name",
            &self.container_name,
            "--network",
            "none",
            "--pids-limit",
            PIDS_LIMIT,
        ]
        .map(str::to_string)
        .into();
        // Without `--user`, files the container creates in the mount are
        // root-owned on Linux and release()'s host-side removal fails (S5).
        // The uid usually has no passwd entry in the image — acceptable for
        // `shell`. (Docker Desktop on macOS maps ownership regardless.)
        #[cfg(unix)]
        {
            args.push("--user".to_string());
            args.push(format!(
                "{}:{}",
                rustix::process::getuid().as_raw(),
                rustix::process::getgid().as_raw()
            ));
        }
        // `--entrypoint sleep` rather than trusting the image's CMD;
        // 2147483647 seconds rather than `infinity`, which busybox rejects.
        args.extend(
            [
                "-v",
                &mount,
                "-w",
                "/workspace",
                "--entrypoint",
                "sleep",
                &self.image,
                "2147483647",
            ]
            .map(str::to_string),
        );
        let output = docker(&self.cli, &args)
            .await
            .map_err(|e| ToolError::Sandbox(format!("native: spawn {}: {e}", self.cli)))?;
        if !output.status.success() {
            return Err(ToolError::Sandbox(format!(
                "native: provision: {}",
                capped(&output.stderr)
            )));
        }
        let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if id.is_empty() {
            return Err(ToolError::Sandbox(
                "native: provision: docker run printed no container id".to_string(),
            ));
        }
        self.stats.count_native_built();
        Ok(id)
    }

    /// Clear the slot iff it still holds `failed`, so a concurrent
    /// re-provision is never forgotten by a straggling loser.
    async fn forget(&self, failed: &str) {
        let mut slot = self.container.lock().await;
        if slot.as_deref() == Some(failed) {
            *slot = None;
        }
    }

    /// Remove the container (S5). Idempotent; by *name*, covering the
    /// dropped-provision orphan whose id was never recorded.
    pub(crate) async fn release(&self) {
        self.container.lock().await.take();
        if !self.attempted.load(Ordering::SeqCst) {
            // No Native call ever ran: nothing to remove, and constructing a
            // tokio Command here would demand a runtime the caller may not
            // have (struct docs).
            return;
        }
        let _ = docker(&self.cli, ["rm", "-f", &self.container_name]).await;
    }
}

/// One docker CLI invocation. `kill_on_drop`: the harness enforces the tool
/// timeout by dropping the call future (harness spec §5.3 item 3) — the
/// client must die with it.
async fn docker<I, S>(cli: &str, args: I) -> std::io::Result<std::process::Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    tokio::process::Command::new(cli)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .output()
        .await
}

/// Container-level failure vs the command's own failure (module docs): the
/// docker CLI reports daemon errors on stderr with a fixed prefix; a user
/// command forging that exact shape *as its first stderr bytes* is contrived
/// enough to accept. The looser phrases are real (CLIs vary the framing),
/// but they appear anywhere in stderr, where an innocent command can echo
/// them — so they count only alongside the CLI's own exit codes (125–127),
/// which a user command rarely shares and never accidentally pairs with the
/// phrase.
fn daemon_error(code: Option<i32>, stderr: &str) -> bool {
    if stderr.starts_with("Error response from daemon:")
        || stderr.starts_with("Cannot connect to the Docker daemon")
    {
        return true;
    }
    matches!(code, Some(125..=127))
        && (stderr.contains("Error response from daemon:")
            || stderr.contains("Cannot connect to the Docker daemon")
            || stderr.contains("No such container")
            || stderr.contains("is not running"))
}

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
