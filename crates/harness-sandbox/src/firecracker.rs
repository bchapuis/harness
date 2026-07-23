//! The `Native` tier at the microVM grade (sandbox spec §3.4, §3.5): OS
//! processes inside a Firecracker microVM, one VM per activation,
//! provisioned lazily on the first `Native` call and killed on release.
//!
//! Confinement here is **hardware virtualization**, §3.4's stronger grade:
//! the guest speaks to a virtual machine monitor, not the host kernel, so a
//! kernel privilege-escalation bug in the guest buys a second, far smaller
//! escape problem rather than the host. The price is provisioning cost
//! (a boot per activation; snapshot warm pools are sandbox spec §7) and a
//! Linux + `/dev/kvm` host at runtime — this module *builds* everywhere, and
//! its provisioning path simply fails as a `ToolError` outcome where KVM is
//! absent.
//!
//! ## How the workspace travels: tar over vsock, not a mount
//!
//! Firecracker exposes block devices and vsock, never a shared filesystem,
//! so the bind-mount of the docker fallback has no analogue. Instead every
//! call brackets the command with a synchronization pair:
//!
//! 1. **push** — the session workspace is walked through its cap-std handle
//!    (S1: the walk cannot represent a path outside it), packed as a tar
//!    stream, and sent to the guest agent, which replaces `/workspace` with
//!    it;
//! 2. **exec** — `/bin/sh -c <command>` runs with `/workspace` as its
//!    working directory;
//! 3. **pull** — the agent tars `/workspace` back, and the host replaces the
//!    workspace's contents with it, again entirely through the handle.
//!
//! The host workspace is therefore authoritative *between* calls (a
//! Workspace-tier write lands in the guest's next call) and the guest's view
//! is authoritative *across* one call. Guest state outside `/workspace`
//! (installed packages, `/tmp`) persists for the activation and dies with
//! it. Effects a backgrounded process makes after the command returns are
//! not pulled until a later call returns. Both tar directions are capped
//! ([`MAX_TAR`]): a workspace beyond the cap fails the call as an outcome,
//! never an unmetered host allocation (the §3.2 stance, applied here).
//!
//! What does **not** survive the pull, deliberately: symlinks whose target
//! is an absolute path (they would name a path outside the workspace — the
//! representation S1 forbids, so the entry is dropped here, explicitly),
//! hard links, device or fifo nodes, and the suid/sgid/sticky bits (a guest
//! must not mint a suid host file; modes are masked to `0o777` in both
//! directions). Regular files, directories, relative symlinks, and the
//! executable bit round-trip. The codec realizing this — the deterministic
//! budgeted pack and the escape-dropping unpack — is the shared
//! [`microvm::ws_sync`] module (the persistent machine syncs its workspace
//! facet with the same one, machine spec §4); this tier keeps the protocol
//! around it.
//!
//! ## The wire protocol (v1), mirrored by `guest/fc-agent`
//!
//! The host connects to Firecracker's host-side vsock unix socket and speaks
//! the muxer's line handshake: `CONNECT 52\n` → `OK <port>\n`. After the
//! handshake, every message is a frame: a `u32` little-endian length, then
//! that many bytes — JSON unless stated. One connection serves any number of
//! sequential requests:
//!
//! - `{"op":"ping"}` → `{"ok":true}`
//! - `{"op":"push"}` + one raw tar frame → `{"ok":true}`
//! - `{"op":"exec","command":s}` → `{"exit_code":n|null,"stdout":s,"stderr":s}`
//! - `{"op":"pull"}` → `{"ok":true}` + one raw tar frame
//!
//! Any reply may instead be `{"error":s}`: an op the agent could not honor,
//! reported over a transport that still works — the call fails as an
//! ordinary outcome and the VM is kept. A *transport* failure (connect
//! refused, short read, frame over cap) is single-tier loss (sandbox spec
//! §4): the VM is killed and forgotten, the call fails as a `ToolError`, and
//! the next call MAY re-provision lazily under the acquisition this
//! activation already journaled. A vanished workspace escalates to
//! `EnvironmentLost` in the provider, as for every tier.
//!
//! Conduct shared with the docker fallback (see `native.rs`): timeouts bound
//! the outcome, not the effect (a dropped call future kills the host-side
//! client; the guest process runs on until release); native calls need a
//! tokio runtime; provisioning is counted by the same `TierStats` the S5
//! accounting tests read.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use cap_std::fs::Dir;
use harness::OnDangling;
use harness::Tier;
use harness::ToolDecl;
use harness::ToolError;
use microvm::MicroVm;
use microvm::vsock;
use microvm::ws_sync::MAX_TAR;
use microvm::ws_sync::tar_workspace;
use serde_json::Value;
use serde_json::json;
use tokio::net::UnixStream;

use crate::provider::TierStats;
use crate::provider::capped;

/// The vsock port the guest agent listens on; part of the protocol version.
const VSOCK_PORT: u32 = 52;

/// The control-directory prefix ([`microvm::control_path`]) for this tier's
/// VMs, distinct from the machine's `harness-machine`.
const CONTROL_PREFIX: &str = "harness-fc";

/// Cap on any single non-tar (JSON) frame. The agent caps each captured
/// stream guest-side; this bounds a malicious or corrupted frame header.
const MAX_FRAME: usize = 1024 * 1024;

/// How long provisioning waits for the guest agent to answer a ping. A
/// Firecracker boot of the CI kernel is tens of milliseconds; ten seconds is
/// generous enough for a cold host.
const READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The poll interval inside [`READY_TIMEOUT`].
const READY_POLL: std::time::Duration = std::time::Duration::from_millis(100);

/// The microVM `shell` declaration, ready for [`harness::Kind::tool`]. A
/// distinct declaration from the docker fallback's [`crate::shell_tool`] —
/// the sync-around-each-command semantics are model-visible, and the digest
/// difference keeps a mixed-realization cluster from agreeing silently.
pub fn fc_shell_tool() -> ToolDecl {
    ToolDecl {
        name: "shell".to_string(),
        description: "Run a POSIX shell command (`/bin/sh -c`) inside the session's \
                      microVM. The session workspace is synchronized to /workspace (the \
                      working directory) before the command and back after it; there is \
                      no network. Returns exit_code, stdout, and stderr."
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

/// Deployment-level Firecracker configuration, handed to
/// [`crate::TieredSandboxes::with_firecracker`]: the node's VMM assets. The
/// per-kind half — which base rootfs a session boots — is solely the
/// profile's `image` (harness spec §5.3 item 4), a host path to an ext4
/// containing `/sbin/fc-agent`; keeping it out of this config keeps the
/// choice inside the digest the cluster agrees on.
#[derive(Clone, Debug)]
pub struct FirecrackerConfig {
    /// The `firecracker` executable.
    pub binary: PathBuf,
    /// An uncompressed kernel image (`vmlinux`).
    pub kernel: PathBuf,
    /// vCPUs per VM.
    pub vcpus: u32,
    /// Guest memory in MiB.
    pub mem_mib: u32,
    /// Kernel boot arguments. The default boots the agent as init with the
    /// rootfs on `/dev/vda`.
    pub boot_args: String,
}

impl FirecrackerConfig {
    pub fn new(binary: impl Into<PathBuf>, kernel: impl Into<PathBuf>) -> FirecrackerConfig {
        FirecrackerConfig {
            binary: binary.into(),
            kernel: kernel.into(),
            vcpus: 1,
            mem_mib: 256,
            // `quiet`: the serial console is an emulated device, so kernel
            // chatter measurably slows the boot; panics still print.
            boot_args: "console=ttyS0 reboot=k panic=1 pci=off quiet root=/dev/vda rw \
                        init=/sbin/fc-agent"
                .to_string(),
        }
    }
}

/// The shared boot document's inputs ([`microvm::config_json`]) for this
/// tier: the agent as init over one root drive, vsock on, no network (a
/// sandboxed guest has no NIC by construction, sandbox spec §1.1).
fn vm_config(config: &FirecrackerConfig, rootfs: &Path) -> microvm::VmConfig {
    let mut vm =
        microvm::VmConfig::rooted(&config.binary, &config.kernel, &config.boot_args, rootfs);
    vm.vcpus = config.vcpus;
    vm.mem_mib = config.mem_mib;
    vm
}

/// One session's native tier at the microVM grade: a VM provisioned lazily
/// on the first `Native` call (sandbox spec §2.3 item 2), killed on release
/// (S5).
pub(crate) struct FirecrackerTier {
    config: Arc<FirecrackerConfig>,
    /// The profile's `image`: the base-rootfs path a session boots (struct
    /// docs on [`FirecrackerConfig`]).
    image: String,
    /// The workspace's capability handle, for the pack half of the tar sync:
    /// the walk goes through it (S1).
    dir: Arc<Dir>,
    /// The workspace's host path, for the staged restore
    /// ([`microvm::ws_sync::restore_workspace`]).
    ws: PathBuf,
    /// The digest key naming this VM's control directory
    /// ([`microvm::control_path`]), so two providers holding the same
    /// session id never contend. The directory holds the api socket, vsock
    /// socket, config, the per-VM rootfs copy, and the console log.
    control_key: String,
    /// The VM slot. tokio's mutex, held across the whole call: provisioning
    /// awaits inside it, and one VM serves one push/exec/pull bracket at a
    /// time — the sync pair must not interleave between concurrent calls.
    vm: tokio::sync::Mutex<Option<MicroVm>>,
    /// Whether provisioning was ever attempted, letting `release` return
    /// before touching any tokio API when no Native call ever ran (the same
    /// conduct as the docker fallback, for the same reason).
    attempted: AtomicBool,
    stats: TierStats,
}

impl FirecrackerTier {
    pub(crate) fn new(
        config: Arc<FirecrackerConfig>,
        image: String,
        dir: Arc<Dir>,
        host_workspace: &Path,
        stats: TierStats,
    ) -> FirecrackerTier {
        let digest = harness::session::content_digest(&host_workspace.display().to_string());
        let control_key = format!("{digest:016x}");
        FirecrackerTier {
            config,
            image,
            dir,
            ws: host_workspace.to_path_buf(),
            control_key,
            vm: tokio::sync::Mutex::new(None),
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
            .ok_or_else(|| ToolError::InvalidArguments("`command` must be a string".to_string()))?;
        // The slot lock brackets the whole call (field docs on `vm`).
        let mut slot = self.vm.lock().await;
        if let Some(vm) = slot.as_mut() {
            // A VM that exited behind our back is single-tier loss now, not
            // a connect timeout later.
            if vm.try_exited().is_some() {
                self.teardown(&mut slot).await;
            }
        }
        if slot.is_none() {
            *slot = Some(self.provision().await?);
        }
        let vsock = slot.as_ref().expect("provisioned above").vsock_path();
        match exec_bracket(&self.dir, &self.ws, &vsock, command).await {
            Ok(outcome) => Ok(outcome),
            Err(BracketError::Agent(e)) => {
                // The transport works; the agent refused the op. An ordinary
                // outcome, the VM is kept (module docs).
                Err(ToolError::Sandbox(format!("firecracker: agent: {e}")))
            }
            Err(BracketError::Transport(e)) => {
                // Single-tier loss (sandbox spec §4): kill and forget; the
                // next call MAY re-provision under the journaled acquisition.
                self.teardown(&mut slot).await;
                Err(ToolError::Sandbox(format!("firecracker: {e}")))
            }
        }
    }

    /// Boot the session's microVM (lazily, on the first call that carries
    /// the tier — sandbox spec §2.3 item 2). The control-directory sweep,
    /// spawn hygiene (kill_on_drop, the stale-orphan pid guard), and the
    /// console-tailing readiness poll are the shared `microvm` mechanics;
    /// this tier contributes the per-VM rootfs copy and the agent's ping as
    /// the readiness probe.
    async fn provision(&self) -> Result<MicroVm, ToolError> {
        self.attempted.store(true, Ordering::SeqCst);
        let fail = |e: String| ToolError::Sandbox(format!("firecracker: provision: {e}"));
        let control = microvm::control_dir(CONTROL_PREFIX, &self.control_key)
            .await
            .map_err(|e| fail(e.to_string()))?;
        // Each VM boots a private copy of the base rootfs: the guest writes
        // to its root, and the base must serve every later activation. The
        // profile's `image` is its only home (struct docs on
        // [`FirecrackerConfig`]) — the docker realization refuses an empty
        // image the same way.
        if self.image.is_empty() {
            return Err(fail(
                "no base rootfs: the profile's `image` names it".to_string(),
            ));
        }
        let base = PathBuf::from(&self.image);
        let rootfs = control.join("rootfs.ext4");
        tokio::fs::copy(&base, &rootfs)
            .await
            .map_err(|e| fail(format!("rootfs {}: {e}", base.display())))?;
        let mut vm = MicroVm::launch(&vm_config(&self.config, &rootfs), &control)
            .await
            .map_err(|e| fail(e.to_string()))?;
        // Readiness: the agent answers a ping. A VMM that exited early fails
        // with its console tail, not a timeout.
        let vsock = vm.vsock_path();
        vm.wait_ready(READY_TIMEOUT, READY_POLL, || {
            let vsock = vsock.clone();
            async move { ping(&vsock).await.is_ok() }
        })
        .await
        .map_err(|e| fail(e.to_string()))?;
        self.stats.count_native_built();
        Ok(vm)
    }

    /// Kill and forget the VM under an already-held slot lock.
    async fn teardown(&self, slot: &mut Option<MicroVm>) {
        if let Some(mut vm) = slot.take() {
            vm.kill().await;
        }
    }

    /// Kill the VM and remove the control directory (S5). Idempotent.
    pub(crate) async fn release(&self) {
        if !self.attempted.load(Ordering::SeqCst) {
            // No Native call ever ran: nothing was provisioned, and touching
            // tokio here would demand a runtime the caller may not have.
            return;
        }
        let mut slot = self.vm.lock().await;
        self.teardown(&mut slot).await;
        let _ = tokio::fs::remove_dir_all(microvm::control_path(CONTROL_PREFIX, &self.control_key))
            .await;
    }
}

/// Why a push/exec/pull bracket failed: the agent answering with an error
/// over a working transport, or the transport itself (module docs — only the
/// latter is single-tier loss).
#[derive(Debug)]
enum BracketError {
    Agent(String),
    Transport(String),
}

impl From<std::io::Error> for BracketError {
    fn from(e: std::io::Error) -> BracketError {
        BracketError::Transport(e.to_string())
    }
}

/// One call's push → exec → pull bracket over one connection (module docs).
async fn exec_bracket(
    dir: &Dir,
    ws: &Path,
    vsock: &Path,
    command: &str,
) -> Result<Value, BracketError> {
    let tar = tar_workspace(dir).map_err(|e| BracketError::Transport(format!("pack: {e}")))?;
    let mut stream = connect(vsock).await?;
    vsock::send_json(&mut stream, &json!({"op": "push"})).await?;
    vsock::send_frame(&mut stream, &tar).await?;
    expect_ok(recv_json(&mut stream).await?)?;
    vsock::send_json(&mut stream, &json!({"op": "exec", "command": command})).await?;
    let outcome = recv_json(&mut stream).await?;
    if let Some(e) = outcome.get("error").and_then(Value::as_str) {
        return Err(BracketError::Agent(e.to_string()));
    }
    vsock::send_json(&mut stream, &json!({"op": "pull"})).await?;
    expect_ok(recv_json(&mut stream).await?)?;
    let tar = vsock::recv_frame(&mut stream, MAX_TAR).await?;
    // Staged, two-phase: the workspace is durably captured after every call,
    // so a truncated guest tar must leave it untouched, never half-cleared.
    microvm::ws_sync::restore_workspace(ws, &tar)
        .map_err(|e| BracketError::Transport(format!("unpack: {e}")))?;
    // A nonzero exit is an outcome the model reacts to, not an error; the
    // agent caps the streams guest-side, the host re-caps for the journal.
    Ok(json!({
        "exit_code": outcome.get("exit_code").cloned().unwrap_or(Value::Null),
        "stdout": capped(outcome.get("stdout").and_then(Value::as_str).unwrap_or("").as_bytes()),
        "stderr": capped(outcome.get("stderr").and_then(Value::as_str).unwrap_or("").as_bytes()),
    }))
}

fn expect_ok(reply: Value) -> Result<(), BracketError> {
    if reply.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(());
    }
    match reply.get("error").and_then(Value::as_str) {
        Some(e) => Err(BracketError::Agent(e.to_string())),
        None => Err(BracketError::Transport(format!(
            "unexpected reply: {reply}"
        ))),
    }
}

/// Connect to the guest agent (the shared muxer handshake, at this
/// protocol's port).
async fn connect(uds: &Path) -> Result<UnixStream, std::io::Error> {
    vsock::connect(uds, VSOCK_PORT).await
}

async fn recv_json(stream: &mut UnixStream) -> Result<Value, std::io::Error> {
    vsock::recv_json(stream, MAX_FRAME).await
}

/// Probe the agent: connect, ping, expect `{"ok":true}`.
async fn ping(uds: &Path) -> Result<(), std::io::Error> {
    let mut stream = connect(uds).await?;
    vsock::send_json(&mut stream, &json!({"op": "ping"})).await?;
    let reply = recv_json(&mut stream).await?;
    if reply.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(())
    } else {
        Err(std::io::Error::other(format!("ping reply: {reply}")))
    }
}

#[cfg(test)]
mod tests;
