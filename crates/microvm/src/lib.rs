//! Shared Firecracker microVM plumbing (sandbox spec §3.5, machine spec §2.1).
//!
//! Two consumers boot Firecracker VMs and deliberately share the subtle
//! process-hygiene code between them: the agent sandbox's `Native` tier
//! (`harness-sandbox`, per-call tar sync over vsock) and the persistent
//! machine (`crates/machine`, whose drive is the disk facet's image, grain
//! §7.15). This crate owns exactly the mechanics that are identical for both
//! and nothing protocol-shaped:
//!
//! - the `--config-file` **document** ([`config_json`]): one PUT-free start,
//!   no API client on the boot path, a pure function unit-tested as such;
//! - the **process lifecycle** ([`MicroVm`]): spawn with `kill_on_drop` (a
//!   dropped handle can never leak a running VM), a pid file plus a
//!   `/proc`-cmdline guard so the *one* leak `kill_on_drop` cannot cover — a
//!   killed harness process — is swept by the next launch, the boot console
//!   captured to a file for diagnosis, and a readiness poll that fails fast
//!   with the console tail when the VMM exits during boot;
//! - the **vsock transport** ([`vsock`]): the Firecracker muxer's
//!   `CONNECT <port>\n` → `OK <port>\n` line handshake, then `u32`
//!   little-endian length-prefixed frames, every receive capped *before* it
//!   sizes anything (a frame header's claim never becomes an allocation);
//! - the **API-socket client** ([`MicroVm::pause`]/[`MicroVm::resume`]):
//!   `PATCH /vm` over the `api.sock`, the quiescent-point mechanism the
//!   machine's capture needs (machine §4) — the only post-boot use of the API
//!   socket;
//! - the **workspace tar codec** ([`ws_sync`], feature `ws`): the capped,
//!   capability-confined pack/unpack both consumers move a workspace with —
//!   the sandbox around every `Native` call, the machine at boot and at
//!   capture quiescent points. The codec is shared; each consumer keeps its
//!   own wire protocol around it.
//!
//! Firecracker runs on Linux + `/dev/kvm` only; this crate *builds*
//! everywhere, and launching where KVM is absent fails as an ordinary error.

use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::Value;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

pub mod vsock;
#[cfg(feature = "ws")]
pub mod ws_sync;

/// A VM lifecycle operation failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MicroVmError(pub String);

impl std::fmt::Display for MicroVmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "microvm: {}", self.0)
    }
}

impl std::error::Error for MicroVmError {}

/// One block device in the boot document.
#[derive(Clone, Debug)]
pub struct Drive {
    pub id: String,
    pub path_on_host: PathBuf,
    pub is_root_device: bool,
    pub is_read_only: bool,
}

/// One virtio-net device in the boot document (the machine's egress seam,
/// machine §5.2): a host tap the provider created, optionally with a fixed
/// guest MAC.
#[derive(Clone, Debug)]
pub struct NetIf {
    pub iface_id: String,
    pub host_dev_name: String,
    pub guest_mac: Option<String>,
}

/// Everything one boot needs. The consumer owns policy (which kernel, which
/// drives, which boot args — the sandbox boots its agent as init, the machine
/// boots the user rootfs's own init); this crate owns the mechanics.
#[derive(Clone, Debug)]
pub struct VmConfig {
    /// The `firecracker` executable.
    pub binary: PathBuf,
    /// An uncompressed kernel image (`vmlinux`).
    pub kernel: PathBuf,
    /// Kernel boot arguments.
    pub boot_args: String,
    pub drives: Vec<Drive>,
    pub vcpus: u32,
    pub mem_mib: u32,
    /// Enable the vsock device, its host-side unix socket at
    /// `control/v.sock` (guest CID 3, the Firecracker convention).
    pub vsock: bool,
    /// Optional network interface (none for the sandbox — its guests have no
    /// NIC by construction; the machine adds one under its egress policy).
    pub net: Option<NetIf>,
}

impl VmConfig {
    /// The shape both consumers boot: one writable root drive, vsock on, no
    /// NIC. Sizing starts minimal; the caller sets `vcpus`/`mem_mib` (and
    /// `net`) from its own policy.
    pub fn rooted(
        binary: impl Into<PathBuf>,
        kernel: impl Into<PathBuf>,
        boot_args: impl Into<String>,
        rootfs: impl Into<PathBuf>,
    ) -> VmConfig {
        VmConfig {
            binary: binary.into(),
            kernel: kernel.into(),
            boot_args: boot_args.into(),
            drives: vec![Drive {
                id: "rootfs".to_string(),
                path_on_host: rootfs.into(),
                is_root_device: true,
                is_read_only: false,
            }],
            vcpus: 1,
            mem_mib: 128,
            vsock: true,
            net: None,
        }
    }
}

/// The name of the vsock unix socket under the control directory.
const VSOCK_SOCK: &str = "v.sock";
/// The name of the Firecracker API socket under the control directory.
const API_SOCK: &str = "api.sock";

/// Where one VM's control directory lives: `<temp>/<prefix>-<key>`, under the
/// OS temp directory because unix socket paths have a ~100-byte limit. The
/// name is stable per VM (`key` is the consumer's digest of its identity) so
/// a fresh launch finds — and sweeps — the previous activation's debris, and
/// distinct across VMs so two on one node never contend.
pub fn control_path(prefix: &str, key: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{key}"))
}

/// Prepare the [`control_path`] directory for a launch: sweep the previous
/// activation's contents and (re)create it empty.
///
/// The sweep kills a previous process death's orphan VMM *before* deleting
/// the directory: the pid file inside is what identifies the orphan, so
/// deleting first would leak it.
pub async fn control_dir(prefix: &str, key: &str) -> Result<PathBuf, MicroVmError> {
    let dir = control_path(prefix, key);
    kill_stale_vm(&dir);
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| MicroVmError(format!("control dir {}: {e}", dir.display())))?;
    Ok(dir)
}

/// The `--config-file` document Firecracker boots from: one PUT-free start.
/// A pure function of its inputs, unit-tested as such. `control` locates the
/// vsock socket when [`VmConfig::vsock`] is set.
pub fn config_json(config: &VmConfig, control: &Path) -> Value {
    let mut document = json!({
        "boot-source": {
            "kernel_image_path": config.kernel.display().to_string(),
            "boot_args": config.boot_args,
        },
        "drives": config.drives.iter().map(|drive| json!({
            "drive_id": drive.id,
            "path_on_host": drive.path_on_host.display().to_string(),
            "is_root_device": drive.is_root_device,
            "is_read_only": drive.is_read_only,
        })).collect::<Vec<_>>(),
        "machine-config": {
            "vcpu_count": config.vcpus,
            "mem_size_mib": config.mem_mib,
            "smt": false,
        },
    });
    if config.vsock {
        document["vsock"] = json!({
            "guest_cid": 3,
            "uds_path": control.join(VSOCK_SOCK).display().to_string(),
        });
    }
    if let Some(net) = &config.net {
        let mut iface = json!({
            "iface_id": net.iface_id,
            "host_dev_name": net.host_dev_name,
        });
        if let Some(mac) = &net.guest_mac {
            iface["guest_mac"] = json!(mac);
        }
        document["network-interfaces"] = json!([iface]);
    }
    document
}

/// One spawned VMM process plus its control directory. The child carries
/// `kill_on_drop`, so a dropped handle can never leak a running VM.
pub struct MicroVm {
    child: tokio::process::Child,
    control: PathBuf,
}

impl MicroVm {
    /// Spawn Firecracker against `control` (an existing directory the caller
    /// prepared: drive files in place, stale contents cleared as it sees
    /// fit). Sweeps a previous *process death's* orphan VM first (the pid
    /// file + `/proc` cmdline guard), writes `config.json`, captures the boot
    /// console to `console.log`, and records the pid for the next sweep. The
    /// returned VM is booting; gate use on [`wait_ready`](MicroVm::wait_ready).
    pub async fn launch(config: &VmConfig, control: &Path) -> Result<MicroVm, MicroVmError> {
        let fail = |e: String| MicroVmError(format!("launch: {e}"));
        kill_stale_vm(control);
        let document = config_json(config, control);
        let config_path = control.join("config.json");
        tokio::fs::write(&config_path, document.to_string())
            .await
            .map_err(|e| fail(format!("config: {e}")))?;
        let console = std::fs::File::create(control.join("console.log"))
            .map_err(|e| fail(format!("console log: {e}")))?;
        let console_err = console.try_clone().map_err(|e| fail(e.to_string()))?;
        let child = tokio::process::Command::new(&config.binary)
            .arg("--api-sock")
            .arg(control.join(API_SOCK))
            .arg("--config-file")
            .arg(&config_path)
            .stdin(std::process::Stdio::null())
            .stdout(console)
            .stderr(console_err)
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| fail(format!("spawn {}: {e}", config.binary.display())))?;
        if let Some(pid) = child.id() {
            let _ = std::fs::write(control.join("fc.pid"), pid.to_string());
        }
        Ok(MicroVm {
            child,
            control: control.to_path_buf(),
        })
    }

    /// The host-side vsock unix socket ([`VmConfig::vsock`]).
    pub fn vsock_path(&self) -> PathBuf {
        self.control.join(VSOCK_SOCK)
    }

    /// Poll `probe` until it reports the guest ready, the VMM exits (failing
    /// with the console tail — a boot crash reads as itself, not a timeout),
    /// or `timeout` elapses (killing the VMM, failing with the tail).
    pub async fn wait_ready<F, Fut>(
        &mut self,
        timeout: Duration,
        poll: Duration,
        mut probe: F,
    ) -> Result<(), MicroVmError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(MicroVmError(format!(
                    "firecracker exited during boot ({status}): {}",
                    self.console_tail()
                )));
            }
            if probe().await {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                let _ = self.child.start_kill();
                return Err(MicroVmError(format!(
                    "guest not ready within {timeout:?}: {}",
                    self.console_tail()
                )));
            }
            tokio::time::sleep(poll).await;
        }
    }

    /// [`wait_ready`](MicroVm::wait_ready) with the commonest probe: the
    /// guest accepting a vsock connection on `port`. A consumer whose
    /// readiness needs a protocol exchange keeps the closure form.
    pub async fn wait_ready_vsock(
        &mut self,
        port: u32,
        timeout: Duration,
        poll: Duration,
    ) -> Result<(), MicroVmError> {
        let vsock = self.vsock_path();
        self.wait_ready(timeout, poll, || {
            let vsock = vsock.clone();
            async move { vsock::connect(&vsock, port).await.is_ok() }
        })
        .await
    }

    /// Whether the VMM exited behind the caller's back (single-tier loss is
    /// the caller's policy; this is the observation).
    pub fn try_exited(&mut self) -> Option<std::process::ExitStatus> {
        self.child.try_wait().ok().flatten()
    }

    /// Pause the guest's vCPUs (`PATCH /vm {"state":"Paused"}` over the API
    /// socket): after this resolves the guest issues no further writes to its
    /// drives — the machine capture's quiescent point (machine §4). Pause is
    /// crash-consistency, not filesystem cleanliness: the guest's page cache
    /// is not flushed (a guest-side `sync` first is the consumer's upgrade).
    pub async fn pause(&self) -> Result<(), MicroVmError> {
        self.patch_state("Paused").await
    }

    /// Resume a paused guest (`PATCH /vm {"state":"Resumed"}`).
    pub async fn resume(&self) -> Result<(), MicroVmError> {
        self.patch_state("Resumed").await
    }

    async fn patch_state(&self, state: &str) -> Result<(), MicroVmError> {
        let fail = |e: String| MicroVmError(format!("patch vm state: {e}"));
        let mut stream = UnixStream::connect(self.control.join(API_SOCK))
            .await
            .map_err(|e| fail(format!("api socket: {e}")))?;
        let body = format!("{{\"state\":\"{state}\"}}");
        let request = format!(
            "PATCH /vm HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| fail(e.to_string()))?;
        // Read the status line; the connection closes after, so no body
        // handling is needed. Expect `204 No Content`.
        let mut line = Vec::with_capacity(32);
        loop {
            let byte = stream.read_u8().await.map_err(|e| fail(e.to_string()))?;
            if byte == b'\n' {
                break;
            }
            line.push(byte);
            if line.len() > 256 {
                return Err(fail("oversized status line".into()));
            }
        }
        let status = String::from_utf8_lossy(&line);
        if status.contains("204") {
            Ok(())
        } else {
            Err(fail(format!("unexpected response: {}", status.trim())))
        }
    }

    /// Begin killing the VMM without waiting (the watchdog path).
    pub fn start_kill(&mut self) {
        let _ = self.child.start_kill();
    }

    /// Kill the VMM and reap it.
    pub async fn kill(&mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }

    /// The last bytes of the boot console, for diagnosis.
    pub fn console_tail(&self) -> String {
        console_tail(&self.control)
    }
}

/// The last bytes of `control/console.log`.
fn console_tail(control: &Path) -> String {
    const TAIL: usize = 2048;
    match std::fs::read(control.join("console.log")) {
        Ok(bytes) if !bytes.is_empty() => {
            let start = bytes.len().saturating_sub(TAIL);
            format!("console tail: {}", String::from_utf8_lossy(&bytes[start..]))
        }
        _ => "no console output".to_string(),
    }
}

/// Kill a previous *process death's* orphan VM (`kill_on_drop` covers every
/// in-process path; only a killed harness process leaks). The pid file names
/// the candidate; the `/proc` command line naming this control directory is
/// the pid-reuse guard — a recycled pid belongs to someone else and is left
/// alone.
#[cfg(target_os = "linux")]
fn kill_stale_vm(control: &Path) {
    let Ok(pid) = std::fs::read_to_string(control.join("fc.pid")) else {
        return;
    };
    let Ok(pid) = pid.trim().parse::<u32>() else {
        return;
    };
    let Ok(cmdline) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
        return;
    };
    let control = control.display().to_string();
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
fn kill_stale_vm(_control: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> VmConfig {
        VmConfig {
            binary: "/usr/bin/firecracker".into(),
            kernel: "/k/vmlinux".into(),
            boot_args: "console=ttyS0 root=/dev/vda rw".into(),
            drives: vec![Drive {
                id: "rootfs".into(),
                path_on_host: "/ctl/rootfs.ext4".into(),
                is_root_device: true,
                is_read_only: false,
            }],
            vcpus: 2,
            mem_mib: 512,
            vsock: true,
            net: None,
        }
    }

    #[test]
    fn the_config_document_pins_the_shape_firecracker_boots_from() {
        let document = config_json(&config(), Path::new("/ctl"));
        assert_eq!(document["boot-source"]["kernel_image_path"], "/k/vmlinux");
        assert_eq!(document["drives"][0]["path_on_host"], "/ctl/rootfs.ext4");
        assert_eq!(document["drives"][0]["is_root_device"], true);
        assert_eq!(document["machine-config"]["vcpu_count"], 2);
        assert_eq!(document["machine-config"]["smt"], false);
        assert_eq!(document["vsock"]["guest_cid"], 3);
        assert_eq!(document["vsock"]["uds_path"], "/ctl/v.sock");
        assert!(document.get("network-interfaces").is_none());
    }

    #[test]
    fn a_vm_without_vsock_omits_the_device() {
        let mut cfg = config();
        cfg.vsock = false;
        let document = config_json(&cfg, Path::new("/ctl"));
        assert!(document.get("vsock").is_none());
    }

    #[test]
    fn a_network_interface_appears_when_configured() {
        let mut cfg = config();
        cfg.net = Some(NetIf {
            iface_id: "eth0".into(),
            host_dev_name: "hm-1234".into(),
            guest_mac: Some("06:00:AC:10:00:02".into()),
        });
        let document = config_json(&cfg, Path::new("/ctl"));
        assert_eq!(
            document["network-interfaces"][0]["host_dev_name"],
            "hm-1234"
        );
        assert_eq!(
            document["network-interfaces"][0]["guest_mac"],
            "06:00:AC:10:00:02"
        );
    }
}
