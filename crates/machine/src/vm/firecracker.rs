//! The real VM binding (machine §2.1): Firecracker through the shared
//! [`microvm`] plumbing.
//!
//! What differs from the agent sandbox's Firecracker tier (sandbox §3.5), by
//! design (machine §2.1 "reuse, not reinvention, stated honestly"):
//!
//! - **The drive is the disk facet's materialized image** (grain §7.15),
//!   mounted in place — no per-VM rootfs copy, because the guest writing that
//!   file between captures *is* the machine's persistence model, and a
//!   non-committed outcome discards it (G20).
//! - **No `init=` override**: the guest boots the user rootfs's own init
//!   (machine §5.1); the guest agent is an ordinary service the base image
//!   ships, not pid 1.
//! - **Readiness is the guest agent accepting a vsock connection** on
//!   [`machine_proto::AGENT_PORT`], with a boot budget sized for a full
//!   distro rather than the sandbox's minimal agent-as-init image.
//! - **Pause/resume are load-bearing** (machine §4): the capture command's
//!   quiescent point is `PATCH /vm` on the API socket. Pause is
//!   crash-consistency, not filesystem cleanliness; the guest agent's `sync`
//!   op before pause is the consumer's upgrade (machine spec §2.2, M3).
//!
//! Linux + `/dev/kvm` at runtime; this module builds everywhere and boot
//! fails as an ordinary [`VmError`] where KVM is absent.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actor_core::BoxFuture;
use granary::BlobId;
use machine_proto::AGENT_PORT;
use microvm::MicroVm;

use super::MachineVm;
use super::MachineVmProvider;
use super::VmError;
use super::VmSpec;
use super::ws_proto::SyncError;

/// Deployment-level configuration for the Firecracker machine binding: the
/// node's VMM and kernel. The per-machine half — sizing, the image — arrives
/// in each [`VmSpec`] from the grain's journaled state (machine §3).
#[derive(Clone, Debug)]
pub struct FirecrackerMachineConfig {
    /// The `firecracker` executable.
    pub binary: PathBuf,
    /// An uncompressed kernel image (`vmlinux`, vsock-enabled).
    pub kernel: PathBuf,
    /// Kernel boot arguments. The default boots the rootfs's **own init** on
    /// `/dev/vda` (machine §5.1) — no `init=` override.
    pub boot_args: String,
    /// How long a boot may take before the guest agent accepts a vsock
    /// connection. A full distro boots slower than the sandbox's
    /// agent-as-init image, so the default is a minute.
    pub ready_timeout: Duration,
}

impl FirecrackerMachineConfig {
    pub fn new(binary: impl Into<PathBuf>, kernel: impl Into<PathBuf>) -> FirecrackerMachineConfig {
        FirecrackerMachineConfig {
            binary: binary.into(),
            kernel: kernel.into(),
            boot_args: "console=ttyS0 reboot=k panic=1 pci=off quiet root=/dev/vda rw".to_string(),
            ready_timeout: Duration::from_secs(60),
        }
    }
}

/// Boots machines on this node's Firecracker (machine §2.1). Injected into
/// the grain factory (`granary_named`), one per node.
pub struct FirecrackerMachineProvider {
    config: Arc<FirecrackerMachineConfig>,
}

impl FirecrackerMachineProvider {
    pub fn new(config: FirecrackerMachineConfig) -> FirecrackerMachineProvider {
        FirecrackerMachineProvider {
            config: Arc::new(config),
        }
    }
}

impl MachineVmProvider for FirecrackerMachineProvider {
    fn boot(&self, spec: VmSpec) -> BoxFuture<'static, Result<Arc<dyn MachineVm>, VmError>> {
        let config = Arc::clone(&self.config);
        Box::pin(async move {
            let fail = |e: String| VmError::Transport(format!("firecracker boot: {e}"));
            let digest = BlobId::of(spec.machine.to_string().as_bytes());
            let control =
                microvm::control_dir("harness-machine", &format!("{:.16}", digest.to_string()))
                    .await
                    .map_err(|e| fail(e.to_string()))?;
            // The disk facet's image, in place (module docs): the guest's
            // writes land in the materialization the capture command scans
            // (grain §7.15).
            let mut vm_config = microvm::VmConfig::rooted(
                &config.binary,
                &config.kernel,
                &config.boot_args,
                &spec.image,
            );
            vm_config.vcpus = spec.vcpus.max(1) as u32;
            vm_config.mem_mib = spec.mem_mib;
            let mut vm = MicroVm::launch(&vm_config, &control)
                .await
                .map_err(|e| fail(e.to_string()))?;
            vm.wait_ready_vsock(AGENT_PORT, config.ready_timeout, Duration::from_millis(100))
                .await
                .map_err(|e| fail(e.to_string()))?;
            let vsock = vm.vsock_path();
            Ok(Arc::new(FirecrackerMachineVm {
                vm: tokio::sync::Mutex::new(vm),
                vsock,
            }) as Arc<dyn MachineVm>)
        })
    }
}

/// One live machine guest (machine §1's disposable activation half).
pub struct FirecrackerMachineVm {
    vm: tokio::sync::Mutex<MicroVm>,
    vsock: PathBuf,
}

impl FirecrackerMachineVm {
    /// The host-side vsock socket, for the front door's channel bridge
    /// (machine §5.1).
    pub fn vsock_path(&self) -> &std::path::Path {
        &self.vsock
    }
}

impl MachineVm for FirecrackerMachineVm {
    fn pause(&self) -> BoxFuture<'_, Result<(), VmError>> {
        Box::pin(async move {
            let vm = self.vm.lock().await;
            vm.pause()
                .await
                .map_err(|e| VmError::Transport(e.to_string()))
        })
    }

    fn resume(&self) -> BoxFuture<'_, Result<(), VmError>> {
        Box::pin(async move {
            let vm = self.vm.lock().await;
            vm.resume()
                .await
                .map_err(|e| VmError::Transport(e.to_string()))
        })
    }

    fn kill(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let mut vm = self.vm.lock().await;
            vm.kill().await;
        })
    }

    fn push_ws(&self, ws: PathBuf) -> BoxFuture<'_, Result<(), VmError>> {
        Box::pin(async move {
            let host = |e: String| VmError::Transport(format!("ws push: {e}"));
            let dir = cap_std::fs::Dir::open_ambient_dir(&ws, cap_std::ambient_authority())
                .map_err(|e| host(format!("open {}: {e}", ws.display())))?;
            let tar = microvm::ws_sync::tar_workspace(&dir).map_err(|e| host(e.to_string()))?;
            super::ws_proto::push(&self.vsock, AGENT_PORT, &tar)
                .await
                .map_err(|e| sync_error("ws push", e))
        })
    }

    fn pull_ws(&self, ws: PathBuf) -> BoxFuture<'_, Result<(), VmError>> {
        Box::pin(async move {
            // Flush first, so the pause that follows the pull sees a
            // filesystem-clean image (module docs).
            super::ws_proto::sync(&self.vsock, AGENT_PORT)
                .await
                .map_err(|e| sync_error("ws sync", e))?;
            let tar = super::ws_proto::pull(&self.vsock, AGENT_PORT)
                .await
                .map_err(|e| sync_error("ws pull", e))?;
            // Two-phase staged restore: a corrupt guest tar leaves the host
            // workspace untouched, so nothing partial can be durably
            // captured as deletions (the staging is the codec's secret).
            microvm::ws_sync::restore_workspace(&ws, &tar)
                .map_err(|e| VmError::Transport(format!("ws pull: {e}")))
        })
    }
}

/// Map a ws-channel error onto the VM seam, preserving the guest/transport
/// policy split.
fn sync_error(op: &str, e: SyncError) -> VmError {
    match e {
        SyncError::Guest(e) => VmError::Guest(format!("{op}: {e}")),
        SyncError::Transport(e) => VmError::Transport(format!("{op}: {e}")),
    }
}
