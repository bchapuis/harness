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
//! executable bit round-trip.
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
use serde_json::Value;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

use crate::native::capped;
use crate::provider::TierStats;

/// The vsock port the guest agent listens on; part of the protocol version.
const VSOCK_PORT: u32 = 52;

/// Cap on one tar stream, either direction (module docs): what a guest can
/// make the host materialize must be bounded before it sizes anything.
const MAX_TAR: usize = 64 * 1024 * 1024;

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
/// [`crate::TieredSandboxes::with_firecracker`]. The per-kind half — which
/// rootfs a session boots — remains the profile's `image` (harness spec §5.3
/// item 4), interpreted here as a host path to a base rootfs; empty means
/// this config's [`rootfs`](FirecrackerConfig::rootfs).
#[derive(Clone, Debug)]
pub struct FirecrackerConfig {
    /// The `firecracker` executable.
    pub binary: PathBuf,
    /// An uncompressed kernel image (`vmlinux`).
    pub kernel: PathBuf,
    /// The base rootfs (ext4) containing `/sbin/fc-agent`. Copied per VM —
    /// the guest writes to its root — so the base is never mutated.
    pub rootfs: PathBuf,
    /// vCPUs per VM.
    pub vcpus: u32,
    /// Guest memory in MiB.
    pub mem_mib: u32,
    /// Kernel boot arguments. The default boots the agent as init with the
    /// rootfs on `/dev/vda`.
    pub boot_args: String,
}

impl FirecrackerConfig {
    pub fn new(
        binary: impl Into<PathBuf>,
        kernel: impl Into<PathBuf>,
        rootfs: impl Into<PathBuf>,
    ) -> FirecrackerConfig {
        FirecrackerConfig {
            binary: binary.into(),
            kernel: kernel.into(),
            rootfs: rootfs.into(),
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

/// The `--config-file` document Firecracker boots from: one PUT-free start,
/// no API client. A pure function of its inputs, unit-tested as such.
fn config_json(config: &FirecrackerConfig, rootfs: &Path, vsock: &Path) -> Value {
    json!({
        "boot-source": {
            "kernel_image_path": config.kernel.display().to_string(),
            "boot_args": config.boot_args,
        },
        "drives": [{
            "drive_id": "rootfs",
            "path_on_host": rootfs.display().to_string(),
            "is_root_device": true,
            "is_read_only": false,
        }],
        "machine-config": {
            "vcpu_count": config.vcpus,
            "mem_size_mib": config.mem_mib,
            "smt": false,
        },
        "vsock": {
            "guest_cid": 3,
            "uds_path": vsock.display().to_string(),
        },
    })
}

/// One provisioned microVM. The child carries `kill_on_drop`, so a dropped
/// provision future or a dropped tier can never leak a running VM the way a
/// detached `docker run -d` could.
struct Vm {
    child: tokio::process::Child,
    vsock: PathBuf,
}

/// One session's native tier at the microVM grade: a VM provisioned lazily
/// on the first `Native` call (sandbox spec §2.3 item 2), killed on release
/// (S5).
pub(crate) struct FirecrackerTier {
    config: Arc<FirecrackerConfig>,
    /// The profile's `image`, interpreted as a base-rootfs path; empty means
    /// the config's default (struct docs on [`FirecrackerConfig`]).
    image: String,
    /// The workspace's capability handle, for the tar sync of the module
    /// docs: both directions are walked and written through it (S1).
    dir: Arc<Dir>,
    /// Host-side control files (api socket, vsock socket, config, the per-VM
    /// rootfs copy, console log). Under the OS temp directory rather than
    /// beside the workspace: unix socket paths have a ~100-byte limit a deep
    /// workspaces root would breach. Digest-named, so two providers holding
    /// the same session id never contend.
    control: PathBuf,
    /// The VM slot. tokio's mutex, held across the whole call: provisioning
    /// awaits inside it, and one VM serves one push/exec/pull bracket at a
    /// time — the sync pair must not interleave between concurrent calls.
    vm: tokio::sync::Mutex<Option<Vm>>,
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
        FirecrackerTier {
            config,
            image,
            dir,
            control: std::env::temp_dir().join(format!("harness-fc-{digest:016x}")),
            vm: tokio::sync::Mutex::new(None),
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
            .ok_or_else(|| ToolError::InvalidArguments("`command` must be a string".to_string()))?;
        // The slot lock brackets the whole call (field docs on `vm`).
        let mut slot = self.vm.lock().await;
        if let Some(vm) = slot.as_mut() {
            // A VM that exited behind our back is single-tier loss now, not
            // a connect timeout later.
            if vm.child.try_wait().ok().flatten().is_some() {
                self.teardown(&mut slot).await;
            }
        }
        if slot.is_none() {
            *slot = Some(self.provision().await?);
        }
        let vsock = slot.as_ref().expect("provisioned above").vsock.clone();
        match exec_bracket(&self.dir, &vsock, command).await {
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
    /// the tier — sandbox spec §2.3 item 2).
    async fn provision(&self) -> Result<Vm, ToolError> {
        self.attempted.store(true, Ordering::SeqCst);
        let fail = |e: String| ToolError::Sandbox(format!("firecracker: provision: {e}"));
        // Sweep a previous activation's debris: the deterministic name makes
        // both the control directory and — when the harness *process* died
        // without dropping the child, the one leak `kill_on_drop` cannot
        // cover — the orphan VM addressable. Best-effort, like the docker
        // tier's named-container sweep.
        self.kill_stale_vm();
        let _ = tokio::fs::remove_dir_all(&self.control).await;
        tokio::fs::create_dir_all(&self.control)
            .await
            .map_err(|e| fail(format!("control dir {}: {e}", self.control.display())))?;
        // Each VM boots a private copy of the base rootfs: the guest writes
        // to its root, and the base must serve every later activation.
        let base = if self.image.is_empty() {
            self.config.rootfs.clone()
        } else {
            PathBuf::from(&self.image)
        };
        let rootfs = self.control.join("rootfs.ext4");
        tokio::fs::copy(&base, &rootfs)
            .await
            .map_err(|e| fail(format!("rootfs {}: {e}", base.display())))?;
        let vsock = self.control.join("v.sock");
        let config_path = self.control.join("config.json");
        let document = config_json(&self.config, &rootfs, &vsock);
        tokio::fs::write(&config_path, document.to_string())
            .await
            .map_err(|e| fail(format!("config: {e}")))?;
        // The boot console goes to a file, for the diagnosis tail below.
        let console = std::fs::File::create(self.control.join("console.log"))
            .map_err(|e| fail(format!("console log: {e}")))?;
        let console_err = console.try_clone().map_err(|e| fail(e.to_string()))?;
        let mut child = tokio::process::Command::new(&self.config.binary)
            .arg("--api-sock")
            .arg(self.control.join("api.sock"))
            .arg("--config-file")
            .arg(&config_path)
            .stdin(std::process::Stdio::null())
            .stdout(console)
            .stderr(console_err)
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| fail(format!("spawn {}: {e}", self.config.binary.display())))?;
        // The pid file is what `kill_stale_vm` sweeps by next activation.
        if let Some(pid) = child.id() {
            let _ = std::fs::write(self.control.join("fc.pid"), pid.to_string());
        }
        // Readiness: the agent answers a ping. Polling, with a deadline; a
        // VMM that exited early fails with its console tail, not a timeout.
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        loop {
            if let Ok(Some(status)) = child.try_wait() {
                return Err(fail(format!(
                    "firecracker exited during boot ({status}): {}",
                    self.console_tail()
                )));
            }
            if ping(&vsock).await.is_ok() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                let _ = child.start_kill();
                return Err(fail(format!(
                    "guest agent not ready within {READY_TIMEOUT:?}: {}",
                    self.console_tail()
                )));
            }
            tokio::time::sleep(READY_POLL).await;
        }
        self.stats.count_native_built();
        Ok(Vm { child, vsock })
    }

    /// Kill the previous activation's orphan VM, if one survived an abrupt
    /// harness-process death (`kill_on_drop` covers every in-process path;
    /// only a killed *process* leaks). The pid file names the candidate; the
    /// `/proc` command line naming this control directory is the pid-reuse
    /// guard — a recycled pid belongs to someone else and is left alone.
    #[cfg(target_os = "linux")]
    fn kill_stale_vm(&self) {
        let Ok(pid) = std::fs::read_to_string(self.control.join("fc.pid")) else {
            return;
        };
        let Ok(pid) = pid.trim().parse::<u32>() else {
            return;
        };
        let Ok(cmdline) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
            return;
        };
        let control = self.control.display().to_string();
        if cmdline
            .windows(control.len())
            .any(|window| window == control.as_bytes())
        {
            let _ = std::process::Command::new("kill")
                .args(["-9", &pid.to_string()])
                .status();
        }
    }

    /// Firecracker runs on Linux only; elsewhere there is no orphan to kill.
    #[cfg(not(target_os = "linux"))]
    fn kill_stale_vm(&self) {}

    /// The last bytes of the boot console, for provisioning errors.
    fn console_tail(&self) -> String {
        const TAIL: usize = 2048;
        match std::fs::read(self.control.join("console.log")) {
            Ok(bytes) if !bytes.is_empty() => {
                let start = bytes.len().saturating_sub(TAIL);
                format!("console tail: {}", String::from_utf8_lossy(&bytes[start..]))
            }
            _ => "no console output".to_string(),
        }
    }

    /// Kill and forget the VM under an already-held slot lock.
    async fn teardown(&self, slot: &mut Option<Vm>) {
        if let Some(mut vm) = slot.take() {
            let _ = vm.child.start_kill();
            let _ = vm.child.wait().await;
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
        let _ = tokio::fs::remove_dir_all(&self.control).await;
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
async fn exec_bracket(dir: &Dir, vsock: &Path, command: &str) -> Result<Value, BracketError> {
    let tar = tar_workspace(dir).map_err(|e| BracketError::Transport(format!("pack: {e}")))?;
    let mut stream = connect(vsock).await?;
    send_json(&mut stream, &json!({"op": "push"})).await?;
    send_frame(&mut stream, &tar).await?;
    expect_ok(recv_json(&mut stream).await?)?;
    send_json(&mut stream, &json!({"op": "exec", "command": command})).await?;
    let outcome = recv_json(&mut stream).await?;
    if let Some(e) = outcome.get("error").and_then(Value::as_str) {
        return Err(BracketError::Agent(e.to_string()));
    }
    send_json(&mut stream, &json!({"op": "pull"})).await?;
    expect_ok(recv_json(&mut stream).await?)?;
    let tar = recv_frame(&mut stream, MAX_TAR).await?;
    untar_workspace(dir, &tar).map_err(|e| BracketError::Transport(format!("unpack: {e}")))?;
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

/// Connect to the guest agent through Firecracker's host-side vsock socket:
/// the muxer's `CONNECT <port>` line handshake, then frames.
async fn connect(vsock: &Path) -> Result<UnixStream, std::io::Error> {
    let mut stream = UnixStream::connect(vsock).await?;
    stream
        .write_all(format!("CONNECT {VSOCK_PORT}\n").as_bytes())
        .await?;
    // The muxer answers one line, `OK <port>\n`; read to the newline and no
    // further — the bytes after it are frames.
    let mut line = Vec::with_capacity(16);
    loop {
        let byte = stream.read_u8().await?;
        if byte == b'\n' {
            break;
        }
        line.push(byte);
        if line.len() > 64 {
            return Err(std::io::Error::other("vsock handshake: oversized reply"));
        }
    }
    if !line.starts_with(b"OK ") {
        return Err(std::io::Error::other(format!(
            "vsock handshake: {}",
            String::from_utf8_lossy(&line)
        )));
    }
    Ok(stream)
}

async fn send_frame(stream: &mut UnixStream, bytes: &[u8]) -> Result<(), std::io::Error> {
    stream
        .write_all(&(bytes.len() as u32).to_le_bytes())
        .await?;
    stream.write_all(bytes).await?;
    stream.flush().await
}

async fn recv_frame(stream: &mut UnixStream, cap: usize) -> Result<Vec<u8>, std::io::Error> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await?;
    let len = u32::from_le_bytes(len) as usize;
    if len > cap {
        // Bounded before it sizes anything host-side (sandbox spec §3.2's
        // stance): never allocate what a frame header merely claims.
        return Err(std::io::Error::other(format!(
            "frame of {len} bytes exceeds the {cap}-byte cap"
        )));
    }
    let mut bytes = vec![0u8; len];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}

async fn send_json(stream: &mut UnixStream, value: &Value) -> Result<(), std::io::Error> {
    send_frame(stream, value.to_string().as_bytes()).await
}

async fn recv_json(stream: &mut UnixStream) -> Result<Value, std::io::Error> {
    let bytes = recv_frame(stream, MAX_FRAME).await?;
    serde_json::from_slice(&bytes).map_err(|e| std::io::Error::other(format!("bad frame: {e}")))
}

/// Probe the agent: connect, ping, expect `{"ok":true}`.
async fn ping(vsock: &Path) -> Result<(), std::io::Error> {
    let mut stream = connect(vsock).await?;
    send_json(&mut stream, &json!({"op": "ping"})).await?;
    let reply = recv_json(&mut stream).await?;
    if reply.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(())
    } else {
        Err(std::io::Error::other(format!("ping reply: {reply}")))
    }
}

// ---------------------------------------------------------------------------
// The workspace as a tar stream, through the capability handle (S1)
// ---------------------------------------------------------------------------

/// Per-entry budget charge beyond file contents: the 512-byte header, name
/// extensions, and padding. Without it a workspace of a million empty files
/// would pack half a gigabyte of headers against a zero-byte budget.
const TAR_ENTRY_OVERHEAD: usize = 1024;

/// Pack the workspace. Deterministic walk (sorted entries, zero mtimes);
/// budgeted against [`MAX_TAR`] *before* bytes accumulate — headers and all.
fn tar_workspace(dir: &Dir) -> Result<Vec<u8>, std::io::Error> {
    let mut builder = tar::Builder::new(Vec::new());
    let mut budget = MAX_TAR;
    append_dir(&mut builder, dir, Path::new(""), &mut budget)?;
    builder.into_inner()
}

fn charge(budget: &mut usize, cost: usize) -> Result<(), std::io::Error> {
    *budget = budget.checked_sub(cost).ok_or_else(|| {
        std::io::Error::other(format!("workspace exceeds the {MAX_TAR}-byte sync cap"))
    })?;
    Ok(())
}

fn append_dir(
    builder: &mut tar::Builder<Vec<u8>>,
    dir: &Dir,
    prefix: &Path,
    budget: &mut usize,
) -> Result<(), std::io::Error> {
    let mut names: Vec<std::ffi::OsString> = dir
        .entries()?
        .map(|entry| entry.map(|e| e.file_name()))
        .collect::<Result<_, _>>()?;
    names.sort();
    for name in names {
        let path = prefix.join(&name);
        let meta = dir.symlink_metadata(&name)?;
        let kind = meta.file_type();
        charge(budget, TAR_ENTRY_OVERHEAD)?;
        if kind.is_dir() {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Directory);
            header.set_mode(0o755);
            header.set_size(0);
            header.set_mtime(0);
            builder.append_data(&mut header, &path, std::io::empty())?;
            append_dir(builder, &dir.open_dir(&name)?, &path, budget)?;
        } else if kind.is_symlink() {
            let target = dir.read_link(&name)?;
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_mode(0o777);
            header.set_size(0);
            header.set_mtime(0);
            builder.append_link(&mut header, &path, &target)?;
        } else if kind.is_file() {
            charge(budget, meta.len() as usize)?;
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Regular);
            // rwx bits only: suid/sgid/sticky are not workspace semantics in
            // either direction.
            #[cfg(unix)]
            header.set_mode(cap_std::fs::PermissionsExt::mode(&meta.permissions()) & 0o777);
            #[cfg(not(unix))]
            header.set_mode(0o644);
            header.set_size(meta.len());
            header.set_mtime(0);
            builder.append_data(&mut header, &path, dir.open(&name)?)?;
        }
        // Anything else (sockets, fifos) is not representable in a
        // workspace; skipped, as on the pull side.
    }
    Ok(())
}

/// Replace the workspace's contents with a tar stream the guest produced.
/// Every write goes through the handle: an absolute or `..`-bearing entry
/// path is skipped here and unrepresentable below (S1, twice over).
fn untar_workspace(dir: &Dir, bytes: &[u8]) -> Result<(), std::io::Error> {
    // Clear first — the tar is already fully received and capped, so the
    // worst a failure past this point leaves is a partial workspace the
    // model can read back, never a half-merged one.
    for entry in dir.entries()? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            dir.remove_dir_all(entry.file_name())?;
        } else {
            dir.remove_file(entry.file_name())?;
        }
    }
    let mut archive = tar::Archive::new(bytes);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        if path.as_os_str().is_empty()
            || !path
                .components()
                .all(|c| matches!(c, std::path::Component::Normal(_)))
        {
            continue;
        }
        match entry.header().entry_type() {
            tar::EntryType::Directory => dir.create_dir_all(&path)?,
            tar::EntryType::Regular => {
                if let Some(parent) = path.parent()
                    && !parent.as_os_str().is_empty()
                {
                    dir.create_dir_all(parent)?;
                }
                let mut file = dir.create(&path)?;
                std::io::copy(&mut entry, &mut file)?;
                // rwx bits only — a guest must not mint a suid host file.
                #[cfg(unix)]
                if let Ok(mode) = entry.header().mode() {
                    let _ = dir.set_permissions(
                        &path,
                        <cap_std::fs::Permissions as cap_std::fs::PermissionsExt>::from_mode(
                            mode & 0o777,
                        ),
                    );
                }
            }
            tar::EntryType::Symlink => {
                if let Some(target) = entry.link_name()?
                    && !target.is_absolute()
                {
                    // An absolute target names a path outside the workspace
                    // — dropped (module docs). A relative one is created;
                    // whether it may be *followed* is decided at every open,
                    // by the handle (S1).
                    let _ = dir.symlink(&target, &path);
                }
            }
            // Hard links, devices, fifos: not representable (module docs).
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
