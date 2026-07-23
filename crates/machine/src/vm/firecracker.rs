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
use granary::GrainName;
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
    /// The node's egress configuration (machine §5.2, M6). `None` — the default
    /// — boots machines with no NIC (the pre-egress posture); set it to wire
    /// the per-machine tap, node NAT, and per-machine guest addressing. Realized
    /// only behind `feature = "net"` on Linux; a node without the capability
    /// degrades to no NIC even when set (see [`FirecrackerMachineProvider::boot`]).
    pub egress: Option<crate::net::EgressConfig>,
}

impl FirecrackerMachineConfig {
    pub fn new(binary: impl Into<PathBuf>, kernel: impl Into<PathBuf>) -> FirecrackerMachineConfig {
        FirecrackerMachineConfig {
            binary: binary.into(),
            kernel: kernel.into(),
            boot_args: "console=ttyS0 reboot=k panic=1 pci=off quiet root=/dev/vda rw".to_string(),
            ready_timeout: Duration::from_secs(60),
            egress: None,
        }
    }

    /// Wire this node's egress (machine §5.2, M6): every machine this provider
    /// boots gets a policy-bound tap NAT-ed out `config.uplink`, addressed from
    /// `config.guest_pool_base`.
    pub fn with_egress(mut self, config: crate::net::EgressConfig) -> FirecrackerMachineConfig {
        self.egress = Some(config);
        self
    }
}

/// Boots machines on this node's Firecracker (machine §2.1). Injected into
/// the grain factory (`granary_named`), one per node.
pub struct FirecrackerMachineProvider {
    config: Arc<FirecrackerMachineConfig>,
    /// The node-local guest-address pool (machine §5.2), `Some` iff the config
    /// wired egress. Shared into each booted VM so a kill returns its slot.
    /// Read only by the Linux + `net` egress realization.
    #[cfg_attr(not(all(feature = "net", target_os = "linux")), allow(dead_code))]
    pool: Option<Arc<std::sync::Mutex<crate::net::GuestPool>>>,
}

impl FirecrackerMachineProvider {
    pub fn new(config: FirecrackerMachineConfig) -> FirecrackerMachineProvider {
        let pool = config.egress.as_ref().map(|egress| {
            Arc::new(std::sync::Mutex::new(crate::net::GuestPool::new(
                egress.guest_pool_base,
                egress.guest_pool_slots,
            )))
        });
        FirecrackerMachineProvider {
            config: Arc::new(config),
            pool,
        }
    }

    /// Realize a machine's egress before launch (machine §5.2, M6): allocate a
    /// guest /30, install the tap and policy ruleset, and point `vm_config` at
    /// the NIC. Returns the teardown handle to hand the VM, or `None` when
    /// egress is unconfigured, the pool is full, or the plumbing could not be
    /// installed — in which case the machine boots with no NIC (net.rs's
    /// documented graceful degrade), never failing the boot over egress.
    #[cfg(all(feature = "net", target_os = "linux"))]
    fn wire_egress(&self, spec: &VmSpec, vm_config: &mut microvm::VmConfig) -> Option<EgressHandle> {
        let pool = self.pool.as_ref()?;
        let egress = self.config.egress.as_ref()?;
        let index = match pool.lock().expect("guest pool").allocate() {
            Some(index) => index,
            None => {
                eprintln!(
                    "machine egress: guest pool exhausted, booting {} without a NIC",
                    spec.machine
                );
                return None;
            }
        };
        let net = crate::net::guest_net(&spec.machine, egress.guest_pool_base, index);
        let cidrs: Vec<&str> = egress.cluster_cidrs.iter().map(String::as_str).collect();
        let ruleset = crate::net::nft_ruleset(&spec.machine, &spec.egress, &cidrs, &egress.uplink);
        if let Err(e) = crate::net::apply::install(&spec.machine, &ruleset, &net.host_cidr) {
            eprintln!(
                "machine egress: install failed for {} ({e}), booting without a NIC",
                spec.machine
            );
            pool.lock().expect("guest pool").free(index);
            return None;
        }
        vm_config.net = Some(microvm::NetIf {
            iface_id: "eth0".to_string(),
            host_dev_name: net.tap.clone(),
            guest_mac: Some(net.guest_mac.clone()),
        });
        // Configure the guest's eth0 from the kernel command line (net.rs), so
        // the base image needs no DHCP client.
        vm_config.boot_args.push(' ');
        vm_config
            .boot_args
            .push_str(&crate::net::guest_ip_boot_arg(&net));
        Some(EgressHandle::new(spec.machine.clone(), Arc::clone(pool), index))
    }

    /// Egress is a Linux + `CAP_NET_ADMIN` realization behind `feature = "net"`
    /// (net.rs); without it a machine boots with no NIC and no handle.
    #[cfg(not(all(feature = "net", target_os = "linux")))]
    fn wire_egress(&self, _spec: &VmSpec, _vm_config: &mut microvm::VmConfig) -> Option<EgressHandle> {
        None
    }
}

/// A booted machine's egress teardown (machine §5.2): the tap and ruleset to
/// remove and the pool slot to return when the VM is killed. Held only for
/// machines that booted with a NIC.
struct EgressHandle {
    /// Read only by `apply::remove` in the Linux + `net` realization.
    #[cfg_attr(not(all(feature = "net", target_os = "linux")), allow(dead_code))]
    machine: GrainName,
    pool: Arc<std::sync::Mutex<crate::net::GuestPool>>,
    index: u32,
    /// Set the first time [`teardown`](EgressHandle::teardown) runs, so
    /// kill-then-drop tears down exactly once — a second `free` could otherwise
    /// return a slot already reallocated to another machine.
    done: std::sync::atomic::AtomicBool,
}

impl EgressHandle {
    #[cfg_attr(not(all(feature = "net", target_os = "linux")), allow(dead_code))]
    fn new(
        machine: GrainName,
        pool: Arc<std::sync::Mutex<crate::net::GuestPool>>,
        index: u32,
    ) -> EgressHandle {
        EgressHandle {
            machine,
            pool,
            index,
            done: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Remove the tap and ruleset and return the slot. Runs its effects once
    /// (the `done` latch), so a kill followed by a drop tears down cleanly.
    fn teardown(&self) {
        use std::sync::atomic::Ordering;
        if self.done.swap(true, Ordering::SeqCst) {
            return;
        }
        #[cfg(all(feature = "net", target_os = "linux"))]
        crate::net::apply::remove(&self.machine);
        self.pool.lock().expect("guest pool").free(self.index);
    }
}

impl MachineVmProvider for FirecrackerMachineProvider {
    fn boot(&self, spec: VmSpec) -> BoxFuture<'static, Result<Arc<dyn MachineVm>, VmError>> {
        let config = Arc::clone(&self.config);
        // The disk facet's image, in place (module docs): the guest's writes
        // land in the materialization the capture command scans (grain §7.15).
        let mut vm_config =
            microvm::VmConfig::rooted(&config.binary, &config.kernel, &config.boot_args, &spec.image);
        vm_config.vcpus = spec.vcpus.max(1) as u32;
        vm_config.mem_mib = spec.mem_mib;
        // Realize egress (M6) before launch: firecracker opens the tap by name,
        // so it must exist first. A None handle means this machine has no NIC.
        let egress = self.wire_egress(&spec, &mut vm_config);
        Box::pin(async move {
            let fail = |e: String| VmError::Transport(format!("firecracker boot: {e}"));
            let digest = BlobId::of(spec.machine.to_string().as_bytes());
            let control =
                microvm::control_dir("harness-machine", &format!("{:.16}", digest.to_string()))
                    .await
                    .map_err(|e| fail(e.to_string()));
            let boot = async {
                let control = control?;
                let mut vm = MicroVm::launch(&vm_config, &control)
                    .await
                    .map_err(|e| fail(e.to_string()))?;
                vm.wait_ready_vsock(AGENT_PORT, config.ready_timeout, Duration::from_millis(100))
                    .await
                    .map_err(|e| fail(e.to_string()))?;
                Ok::<_, VmError>(vm)
            }
            .await;
            let vm = match boot {
                Ok(vm) => vm,
                Err(e) => {
                    // A boot that fails after the tap is installed must not leak
                    // it or its pool slot.
                    if let Some(egress) = &egress {
                        egress.teardown();
                    }
                    return Err(e);
                }
            };
            let vsock = vm.vsock_path();
            Ok(Arc::new(FirecrackerMachineVm {
                vm: tokio::sync::Mutex::new(vm),
                vsock,
                egress,
            }) as Arc<dyn MachineVm>)
        })
    }
}

/// One live machine guest (machine §1's disposable activation half).
pub struct FirecrackerMachineVm {
    vm: tokio::sync::Mutex<MicroVm>,
    vsock: PathBuf,
    /// The egress teardown (machine §5.2), `Some` iff this machine booted with a
    /// NIC. Torn down on kill and again on drop (idempotent), so a dropped
    /// activation can never leak a tap or its pool slot.
    egress: Option<EgressHandle>,
}

impl FirecrackerMachineVm {
    /// The host-side vsock socket, for the front door's channel bridge
    /// (machine §5.1).
    pub fn vsock_path(&self) -> &std::path::Path {
        &self.vsock
    }
}

impl Drop for FirecrackerMachineVm {
    /// A dropped activation must leak neither a running guest (MicroVm kills
    /// itself on drop) nor its egress plumbing (machine §5.2). The teardown is
    /// a no-op if kill already ran it.
    fn drop(&mut self) {
        if let Some(egress) = &self.egress {
            egress.teardown();
        }
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
            // Remove the egress plumbing with the guest (machine §5.2): the tap
            // and ruleset outlive the VM process otherwise.
            if let Some(egress) = &self.egress {
                egress.teardown();
            }
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
