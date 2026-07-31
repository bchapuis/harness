//! The machine's production binding (machine §2.1): one implementation over
//! [`machine_host::MachineHost`], whichever mechanism a node was configured with.
//!
//! A machine's durability does not depend on how its guest is held. The disk
//! facet's image is the durable thing (grain §7.15), the capture command's
//! quiescent point is a pause, and `/workspace` travels as a tar over a channel
//! to the guest agent — and all three read the same whether the guest is a
//! Firecracker microVM or a container holding the same rootfs. So this file
//! states them once, and `machine-host` states the two mechanisms:
//!
//! - **The drive is the disk facet's materialized image** (grain §7.15), used in
//!   place — no per-guest copy, because the guest writing that file between
//!   captures *is* the machine's persistence model, and a non-committed outcome
//!   discards it (G20).
//! - **The guest boots its own init** (machine §5.1): the agent is an ordinary
//!   service the rootfs ships, not pid 1. Under the container mechanism there is
//!   no init at all and the agent is the container's only process — that grade's
//!   one visible departure.
//! - **Readiness is the guest agent answering a channel** on
//!   [`machine_proto::AGENT_PORT`], with a boot budget sized for a full distro.
//! - **Pause/resume are load-bearing** (machine §4): the capture command's
//!   quiescent point. How much consistency a pause gives is the mechanism's to
//!   promise (`machine_host::MachineGuest::pause`); the agent's `sync` before the pull is
//!   this layer's contribution either way (machine §2.2, M3).
//! - **Egress is the microVM grade's alone** (machine §5.2, M6): the tap and
//!   ruleset are realized here, from journaled policy, and skipped where the
//!   mechanism cannot take a NIC — the same degradation as a node without
//!   `CAP_NET_ADMIN`, never a boot failed over egress.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actor_core::BoxFuture;
use granary::BlobId;
use granary::GrainName;
use machine_host::Duplex;
use machine_host::GuestError;
use machine_host::GuestKey;
use machine_host::GuestSpec;
use machine_host::MachineGuest;
use machine_host::MachineHost;
use machine_host::Network;
use machine_proto::AGENT_PORT;
use machine_proto::ChannelKind;

use super::BootSpec;
use super::MachineRuntime;
use super::MachineRuntimeProvider;
use super::RuntimeError;
use super::ws_proto;
use super::ws_proto::SyncError;

/// The kernel command line a machine's guest boots with under the microVM
/// mechanism: the rootfs's **own init** on `/dev/vda` (machine §5.1) — no
/// `init=` override, unlike the agent sandbox's agent-as-init image.
const BOOT_ARGS: &str = "console=ttyS0 reboot=k panic=1 pci=off quiet root=/dev/vda rw";

/// Names machine guests apart from any other consumer's on one host: the
/// microVM mechanism's control directories and the container mechanism's
/// container names both derive from it.
const GUEST_PREFIX: &str = "harness-machine";

/// This node's microVM mechanism, carrying the machine's own boot policy
/// (machine §5.1's own-init boot, and a name prefix no other consumer uses).
///
/// A constructor rather than exported constants: policy that must travel
/// *together* should not be two values a deployment is trusted to pair.
pub fn microvm_host(
    binary: impl Into<std::path::PathBuf>,
    kernel: impl Into<std::path::PathBuf>,
) -> Arc<dyn MachineHost> {
    Arc::new(machine_host::microvm::MicroVmHost::new(
        binary,
        kernel,
        BOOT_ARGS,
        GUEST_PREFIX,
    ))
}

/// This node's container mechanism: `runner_image` is what loop-mounts a
/// machine's disk and chroots into it (`guest/machine-docker/build.sh`).
pub fn container_host(
    cli: impl Into<String>,
    runner_image: impl Into<String>,
) -> Arc<dyn MachineHost> {
    Arc::new(
        machine_host::container::ContainerHost::new(cli, GUEST_PREFIX)
            .with_runner_image(runner_image),
    )
}

/// A machine's guest identity on this node: a digest of the name, because a
/// `GrainName` may hold characters a container name or a socket path cannot, and
/// node-scoped because several nodes host machines on one host in the standalone
/// deployments (`machine_host::GuestKey`).
pub fn guest_key(node: &str, machine: &GrainName) -> GuestKey {
    GuestKey::new(
        node,
        format!("{:.16}", BlobId::of(machine.to_string().as_bytes())),
    )
}

/// Open one channel to `machine`'s guest agent on this node, from the machine's
/// name alone: what a front door needs, holding no guest object (machine §5.1).
///
/// Node-local by construction — [`MachineHost::connect_by_key`] answers
/// [`GuestError::Gone`] for a machine whose activation is on another node, which
/// is the ordinary case a door must report rather than treat as a fault.
pub async fn open_channel(
    host: &dyn MachineHost,
    node: &str,
    machine: &GrainName,
    kind: &ChannelKind,
) -> Result<Box<dyn Duplex>, GuestError> {
    let key = guest_key(node, machine);
    let mut channel = host.connect_by_key(&key, AGENT_PORT).await?;
    send_header(&mut channel, kind)
        .await
        .map_err(|e| GuestError::Host(format!("{machine}: channel header: {e}")))?;
    Ok(channel)
}

/// Tell the agent what this channel is for. The first frame of every channel
/// (machine §5.1), sent by whoever opened it.
async fn send_header(channel: &mut Box<dyn Duplex>, kind: &ChannelKind) -> std::io::Result<()> {
    machine_host::vsock::send_frame(channel, &kind.header()).await
}

/// Deployment-level configuration for the machine binding: how long a boot may
/// take, and this node's egress. The mechanism itself (VMM assets, or container
/// CLI and runner image) is configured on the [`MachineHost`]; the per-machine
/// half — sizing, the image — arrives in each [`BootSpec`] from journaled state
/// (machine §3).
#[derive(Clone, Debug)]
pub struct HostedRuntimeConfig {
    /// This node's id, scoping its guests' names ([`guest_key`]).
    pub node: String,
    /// How long a boot may take before the guest agent answers a channel. A full
    /// distro boots slower than an agent-as-init image, so the default is a
    /// minute; a container needs a fraction of it.
    pub ready_timeout: Duration,
    /// The node's egress configuration (machine §5.2, M6). `None` — the default
    /// — boots machines with no NIC; set it to wire the per-machine tap, node
    /// NAT, and guest addressing. Realized only behind `feature = "net"` on
    /// Linux and only at [`MachineIsolation::MicroVm`]; anything else degrades to no NIC.
    pub egress: Option<crate::net::EgressConfig>,
}

impl HostedRuntimeConfig {
    pub fn new(node: impl Into<String>) -> HostedRuntimeConfig {
        HostedRuntimeConfig {
            node: node.into(),
            ready_timeout: Duration::from_secs(60),
            egress: None,
        }
    }
}

/// Boots machines on this node's mechanism (machine §2.1). One per node,
/// injected into the grain factory (`granary_named`).
pub struct HostedRuntimeProvider {
    host: Arc<dyn MachineHost>,
    config: Arc<HostedRuntimeConfig>,
    /// The node-local guest-address pool (machine §5.2), `Some` iff the config
    /// wired egress *and* the mechanism can realize it. Shared into each booted
    /// guest so a kill returns its slot.
    #[cfg_attr(not(all(feature = "net", target_os = "linux")), allow(dead_code))]
    pool: Option<Arc<std::sync::Mutex<crate::net::GuestPool>>>,
}

impl HostedRuntimeProvider {
    pub fn new(host: Arc<dyn MachineHost>, config: HostedRuntimeConfig) -> HostedRuntimeProvider {
        // A pool only where a tap could actually be installed: at the container
        // grade the addresses would be allocated and never used.
        let pool = config
            .egress
            .as_ref()
            .filter(|_| host.accepts_tap())
            .map(|egress| {
                Arc::new(std::sync::Mutex::new(crate::net::GuestPool::new(
                    egress.guest_pool_base,
                    egress.guest_pool_slots,
                )))
            });
        HostedRuntimeProvider {
            host,
            config: Arc::new(config),
            pool,
        }
    }

    /// Realize a machine's egress before the guest starts (machine §5.2, M6):
    /// allocate a guest /30, install the tap and policy ruleset, and return the
    /// NIC to boot with plus its teardown. `None` when egress is unconfigured,
    /// the mechanism cannot take a NIC, the pool is full, or the plumbing could
    /// not be installed — in which case the machine boots with no NIC rather
    /// than failing the boot over egress.
    #[cfg(all(feature = "net", target_os = "linux"))]
    fn wire_egress(&self, spec: &BootSpec) -> (Network, Option<EgressHandle>) {
        let none = (Network::None, None);
        let (Some(pool), Some(egress)) = (self.pool.as_ref(), self.config.egress.as_ref()) else {
            return none;
        };
        let index = match pool.lock().expect("guest pool").allocate() {
            Some(index) => index,
            None => {
                eprintln!(
                    "machine egress: guest pool exhausted, booting {} without a NIC",
                    spec.machine
                );
                return none;
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
            return none;
        }
        let network = Network::Tap {
            interface: machine_host::microvm::NetIf {
                iface_id: "eth0".to_string(),
                host_dev_name: net.tap.clone(),
                guest_mac: Some(net.guest_mac.clone()),
            },
            boot_arg: crate::net::guest_ip_boot_arg(&net),
        };
        (
            network,
            Some(EgressHandle::new(
                spec.machine.clone(),
                Arc::clone(pool),
                index,
            )),
        )
    }

    /// Egress is a Linux + `CAP_NET_ADMIN` realization behind `feature = "net"`
    /// (net.rs); without it a machine boots with no NIC and no handle.
    #[cfg(not(all(feature = "net", target_os = "linux")))]
    fn wire_egress(&self, _spec: &BootSpec) -> (Network, Option<EgressHandle>) {
        (Network::None, None)
    }
}

impl MachineRuntimeProvider for HostedRuntimeProvider {
    fn boot(
        &self,
        spec: BootSpec,
    ) -> BoxFuture<'static, Result<Arc<dyn MachineRuntime>, RuntimeError>> {
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let (network, egress) = self.wire_egress(&spec);
        Box::pin(async move {
            let guest_spec = GuestSpec {
                key: guest_key(&config.node, &spec.machine),
                disk: spec.image.clone(),
                vcpus: spec.vcpus,
                mem_mib: spec.mem_mib,
                network,
            };
            let started = host.start(guest_spec).await;
            let guest = match started {
                Ok(guest) => guest,
                Err(e) => {
                    // A boot that fails after the tap is installed must leak
                    // neither it nor its pool slot.
                    if let Some(egress) = &egress {
                        egress.teardown();
                    }
                    return Err(boot_error(&spec.machine, e));
                }
            };
            if let Err(e) =
                machine_host::wait_ready(guest.as_ref(), AGENT_PORT, config.ready_timeout).await
            {
                // The guest's own account of why, which nothing else in this
                // stack sees: a kernel that panicked, a rootfs that would not
                // mount, an agent the image does not ship.
                let words = guest.console_tail().await;
                guest.stop().await;
                if let Some(egress) = &egress {
                    egress.teardown();
                }
                return Err(RuntimeError::Transport(if words.is_empty() {
                    format!("{}: {e}", spec.machine)
                } else {
                    format!("{}: {e}; guest said: {words}", spec.machine)
                }));
            }
            Ok(Arc::new(HostedRuntime { guest, egress }) as Arc<dyn MachineRuntime>)
        })
    }
}

/// A booted machine's egress teardown (machine §5.2): the tap and ruleset to
/// remove and the pool slot to return when the guest is killed. Held only for
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

/// One live machine guest (machine §1's disposable activation half).
pub struct HostedRuntime {
    guest: Arc<dyn MachineGuest>,
    /// The egress teardown (machine §5.2), `Some` iff this machine booted with a
    /// NIC. Torn down on kill and again on drop (idempotent), so a dropped
    /// activation can never leak a tap or its pool slot.
    egress: Option<EgressHandle>,
}

impl HostedRuntime {
    /// Open one channel to this guest's agent, header sent.
    async fn channel(&self, kind: &ChannelKind) -> Result<Box<dyn Duplex>, RuntimeError> {
        let mut channel = self
            .guest
            .connect(AGENT_PORT)
            .await
            .map_err(|e| RuntimeError::Transport(format!("open {kind:?} channel: {e}")))?;
        send_header(&mut channel, kind)
            .await
            .map_err(|e| RuntimeError::Transport(format!("open {kind:?} channel: {e}")))?;
        Ok(channel)
    }
}

impl Drop for HostedRuntime {
    /// A dropped activation must leak no egress plumbing (machine §5.2); the
    /// guest itself is the mechanism's to reap on drop (`machine_host::MachineGuest`).
    fn drop(&mut self) {
        if let Some(egress) = &self.egress {
            egress.teardown();
        }
    }
}

impl MachineRuntime for HostedRuntime {
    fn pause(&self) -> BoxFuture<'_, Result<(), RuntimeError>> {
        Box::pin(async move { self.guest.pause().await.map_err(runtime_error) })
    }

    fn resume(&self) -> BoxFuture<'_, Result<(), RuntimeError>> {
        Box::pin(async move { self.guest.resume().await.map_err(runtime_error) })
    }

    fn kill(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.guest.stop().await;
            // The tap and ruleset outlive the guest otherwise (machine §5.2).
            if let Some(egress) = &self.egress {
                egress.teardown();
            }
        })
    }

    fn push_ws(&self, ws: PathBuf) -> BoxFuture<'_, Result<(), RuntimeError>> {
        Box::pin(async move {
            let transport = |e: String| RuntimeError::Transport(format!("ws push: {e}"));
            let dir = cap_std::fs::Dir::open_ambient_dir(&ws, cap_std::ambient_authority())
                .map_err(|e| transport(format!("open {}: {e}", ws.display())))?;
            let tar =
                machine_host::ws_sync::tar_workspace(&dir).map_err(|e| transport(e.to_string()))?;
            let channel = self.channel(&ChannelKind::WsPush).await?;
            ws_proto::push(channel, &tar)
                .await
                .map_err(|e| sync_error("ws push", e))
        })
    }

    fn pull_ws(&self, ws: PathBuf) -> BoxFuture<'_, Result<(), RuntimeError>> {
        Box::pin(async move {
            // Flush first, so the pause that follows the pull sees a
            // filesystem-clean image whichever mechanism holds the guest.
            let channel = self.channel(&ChannelKind::Sync).await?;
            ws_proto::sync(channel)
                .await
                .map_err(|e| sync_error("ws sync", e))?;
            let channel = self.channel(&ChannelKind::WsPull).await?;
            let tar = ws_proto::pull(channel)
                .await
                .map_err(|e| sync_error("ws pull", e))?;
            // Two-phase staged restore: a corrupt guest tar leaves the host
            // workspace untouched, so nothing partial can be durably captured
            // as deletions.
            machine_host::ws_sync::restore_workspace(&ws, &tar)
                .map_err(|e| RuntimeError::Transport(format!("ws pull: {e}")))
        })
    }
}

/// Map a mechanism failure onto the runtime seam. Both arms land on
/// [`RuntimeError::Transport`] deliberately: the machine's policy for a guest that is
/// gone and for a mechanism that failed is the same one — end the activation and
/// let the next boot rehydrate (machine §4) — so the distinction stays in the
/// message rather than becoming a second code path.
fn runtime_error(e: GuestError) -> RuntimeError {
    RuntimeError::Transport(e.to_string())
}

fn boot_error(machine: &GrainName, e: GuestError) -> RuntimeError {
    RuntimeError::Transport(format!("{machine}: boot: {e}"))
}

/// Map a ws-channel error onto the runtime seam, preserving the refusal/transport
/// policy split: a guest that answered and refused leaves a live guest the grain
/// can keep serving.
fn sync_error(op: &str, e: SyncError) -> RuntimeError {
    match e {
        SyncError::Refused(e) => RuntimeError::Refused(format!("{op}: {e}")),
        SyncError::Transport(e) => RuntimeError::Transport(format!("{op}: {e}")),
    }
}
