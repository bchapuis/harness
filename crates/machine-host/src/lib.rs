//! The host side of a machine (sandbox spec §3.4, §3.5; machine spec §2.1).
//!
//! Three words, one job each, across this crate and its consumers:
//!
//! - **host** — the node side: what boots and holds the box. Never the box.
//! - **guest** — the box itself, a microVM or a container, and everything on
//!   its side of the boundary. Never the code inside it.
//! - **agent** — the process inside the guest that answers the protocol. Never
//!   the box.
//!
//! Two consumers run untrusted code inside a guest: the agent sandbox's
//! `Native` tier (`harness-sandbox`) and the persistent machine
//! (`crates/machine-grain`). Two mechanisms can hold one: a Firecracker microVM
//! ([`microvm`]) or an OCI container driven through the `docker` CLI
//! ([`container`]). This crate owns both mechanisms and the seam between them,
//! so a consumer states *what* it wants held and never *how*:
//!
//! - [`MachineHost`] — the node's mechanism, one per node, chosen by an
//!   operator's flag. Never probed: which mechanism a node runs is deployment
//!   configuration, because the two [grades](MachineIsolation) of confinement
//!   are not interchangeable (§3.4) and a host must not silently pick the
//!   weaker one.
//! - [`MachineGuest`] — one live guest: a byte stream to an agent inside it, the
//!   quiescent point a capture needs (machine §4), and a stop that cannot leak.
//! - [`GuestError`] — a mechanism failure, split by what the caller must do
//!   next: re-provision ([`Gone`](GuestError::Gone)), or keep the guest and
//!   report ([`Host`](GuestError::Host)).
//!
//! Only the two seam traits carry the `Machine` prefix, because only they name
//! the abstraction. The data they carry ([`GuestSpec`], [`GuestKey`],
//! [`GuestError`]) is unambiguous inside a crate called `machine-host`, and a
//! type belonging to *one* mechanism keeps its own name — [`microvm::MicroVm`],
//! [`container::ContainerGuest`] — because the contrast is the point:
//! `Machine…` is the abstraction, `MicroVm…`/`Container…` is one realization.
//!
//! What is *not* here is anything protocol-shaped. Each consumer ships its own
//! guest agent and speaks its own framed protocol over [`MachineGuest::connect`]:
//! `guest/fc-agent` for the sandbox, `guest/machine-agent` for machines. The
//! shared halves of that plumbing are the muxer handshake and frame codec
//! ([`vsock`]) and the workspace tar codec ([`ws_sync`], feature `ws`).
//!
//! Running a command is likewise absent from [`MachineGuest`]: a microVM has no
//! host-side exec — it is reachable only through its agent — so
//! [`container::ContainerGuest::exec`] lives on the mechanism that can honor
//! it, and the one consumer that needs it holds that type directly. A trait
//! whose implementations must fake half their methods hides nothing.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;

pub mod container;
pub mod microvm;
pub mod vsock;
#[cfg(feature = "ws")]
pub mod ws_sync;

/// A boxed future: what every method of the two seam traits returns, so
/// `dyn MachineHost` and `dyn MachineGuest` stay object-safe. Interchangeable with
/// `actor_core::BoxFuture`, which is the same alias.
pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Which isolation a mechanism gives (sandbox §3.4's two grades of
/// confinement). Reported so
/// a node can log what it runs, and so a consumer can skip a capability the
/// weaker grade cannot realize (the machine's egress tap, machine §5.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MachineIsolation {
    /// Hardware virtualization: the guest speaks to a VMM, not the host kernel.
    MicroVm,
    /// Shared kernel: the guest is a container. §3.4's SHOULD grade — a
    /// development mechanism, priced by kernel privilege-escalation bugs.
    Container,
}

impl std::fmt::Display for MachineIsolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MachineIsolation::MicroVm => write!(f, "microvm"),
            MachineIsolation::Container => write!(f, "container"),
        }
    }
}

/// A mechanism operation failed. Split by the caller's next move, not by the
/// mechanism's internals: the same two arms serve a tier that must decide
/// whether to re-provision (sandbox §4) and a grain that must decide whether
/// its guest is gone (machine §4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GuestError {
    /// The guest is gone, or was never started here. Retrying in place cannot
    /// work; the caller re-provisions or reports the absence.
    Gone(String),
    /// The mechanism itself failed while the guest may well still be live (a
    /// CLI that would not spawn, an API socket that refused, a short read).
    Host(String),
}

impl std::fmt::Display for GuestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GuestError::Gone(e) => write!(f, "guest gone: {e}"),
            GuestError::Host(e) => write!(f, "host: {e}"),
        }
    }
}

impl std::error::Error for GuestError {}

/// One guest's stable identity, and the whole of what a mechanism needs to name
/// it: a control directory under [`microvm`], a container name under
/// [`container`].
///
/// Scoped by node as well as by guest, because a container namespace is
/// per-*host* while a guest belongs to the node running it: two nodes on one
/// host — what the standalone deployments do — would otherwise derive one name,
/// and one node's sweep would tear down the other's live guest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GuestKey {
    node: String,
    guest: String,
}

impl GuestKey {
    /// `guest` should already be a digest or another docker-legal, filesystem-legal
    /// fragment: mechanisms use it verbatim in paths and container names.
    pub fn new(node: impl Into<String>, guest: impl Into<String>) -> GuestKey {
        GuestKey {
            node: node.into(),
            guest: guest.into(),
        }
    }

    /// The flat form both mechanisms name resources with.
    pub fn slug(&self) -> String {
        if self.node.is_empty() {
            self.guest.clone()
        } else {
            format!("{}-{}", self.node, self.guest)
        }
    }
}

/// What a guest gets for a network. `None` is every sandbox guest (§1.1: no NIC
/// by construction) and every containerized guest; a tap is the machine's
/// egress seam (machine §5.2), realized by the consumer's policy and honored
/// only by [`MachineIsolation::MicroVm`].
#[derive(Clone, Debug, Default)]
pub enum Network {
    #[default]
    None,
    /// A host tap the consumer created, plus the guest addressing to boot with.
    Tap {
        interface: microvm::NetIf,
        /// Kernel `ip=` argument configuring the guest's side, so the guest
        /// image needs no DHCP client.
        boot_arg: String,
    },
}

/// What one guest is made of. Only what varies per guest: the mechanism's own
/// assets (kernel, VMM binary, runner image, container CLI) are configured once
/// per node on the [`MachineHost`].
#[derive(Clone, Debug)]
pub struct GuestSpec {
    pub key: GuestKey,
    /// The guest's root block image, used **in place**: both mechanisms read
    /// and write this very file, which is what makes it the durable thing a
    /// capture scans (grain §7.15). A consumer wanting a disposable root
    /// copies it first and names the copy here.
    pub disk: PathBuf,
    pub vcpus: u8,
    pub mem_mib: u32,
    pub network: Network,
}

/// A node's confinement mechanism: what it uses to hold guests.
///
/// One per node, built from the operator's flag. Implementations:
/// [`microvm::MicroVmHost`] and [`container::ContainerHost`].
pub trait MachineHost: Send + Sync + 'static {
    fn isolation(&self) -> MachineIsolation;

    /// Whether this mechanism can give a guest the host tap of
    /// [`Network::Tap`]. Asked rather than inferred from
    /// [`isolation`](MachineHost::isolation), which reports confinement
    /// strength and is not a capability list: a
    /// consumer that wants a NIC must not have to know which mechanisms happen
    /// to have one today.
    fn accepts_tap(&self) -> bool {
        false
    }

    /// Start a guest against `spec.disk`. Returns once the mechanism reports the
    /// guest up; whether the *agent* inside it is serving is the consumer's
    /// probe, over [`MachineGuest::connect`].
    fn start(
        &self,
        spec: GuestSpec,
    ) -> BoxFuture<'static, Result<Arc<dyn MachineGuest>, GuestError>>;

    /// Open a stream to a guest this node may be running, from its key alone —
    /// what a front door needs when it does not hold the guest object (machine
    /// §5.1). [`GuestError::Gone`] when no such guest is here, which is the
    /// ordinary case for a machine whose activation is on another node.
    fn connect_by_key<'a>(
        &'a self,
        key: &'a GuestKey,
        port: u32,
    ) -> BoxFuture<'a, Result<Box<dyn Duplex>, GuestError>>;
}

/// One live guest. Held by its consumer's activation and by nothing durable;
/// dropped or stopped with it, and never leaking either way.
pub trait MachineGuest: Send + Sync + 'static {
    /// Whether the guest is still there at all: a VMM that has not exited, a
    /// container still running. Cheap, and never a substitute for
    /// [`connect`](MachineGuest::connect) — a guest can be alive with its agent not yet
    /// serving, which is exactly the state [`wait_ready`] waits out.
    fn alive(&self) -> BoxFuture<'_, bool>;

    /// A byte stream to the listener numbered `port` inside the guest: a vsock
    /// port under [`microvm`], the unix socket at `/run/guest/<port>.sock` under
    /// [`container`]. Both ends perform the Firecracker muxer's
    /// `CONNECT <port>` handshake, so one guest agent serves either mechanism
    /// unchanged and the caller speaks only its own framed protocol
    /// ([`vsock::send_frame`]).
    fn connect(&self, port: u32) -> BoxFuture<'_, Result<Box<dyn Duplex>, GuestError>>;

    /// Reach a quiescent point: once this resolves, the guest issues no further
    /// writes to its disk until [`resume`](MachineGuest::resume), so a capture scans a
    /// stable image (machine §4, grain §7.15).
    ///
    /// Each mechanism promises as much consistency as it can: freezing a
    /// container leaves its writes in the host kernel's cache, so that
    /// implementation flushes first and the capture reads a filesystem-clean
    /// image; a paused microVM is crash-consistent only, and the consumer's
    /// agent-level `sync` is what upgrades it (machine §2.2).
    fn pause(&self) -> BoxFuture<'_, Result<(), GuestError>>;

    fn resume(&self) -> BoxFuture<'_, Result<(), GuestError>>;

    /// Stop the guest. Idempotent — a forced step-down and an ordinary
    /// passivation both call it, possibly for a guest already gone — and an
    /// implementation whose handle outlives the call must also stop on drop, so
    /// a dropped activation can never leak a running guest.
    fn stop(&self) -> BoxFuture<'_, ()>;

    /// The guest's own last words, for a diagnostic nothing else in the stack
    /// sees: the VMM's serial console under [`microvm`], the container's log
    /// tail under [`container`]. Where a boot fails inside the guest — an init
    /// that panicked, a rootfs that would not mount — this is the only place it
    /// said so. Empty when the mechanism has nothing to report.
    fn console_tail(&self) -> BoxFuture<'_, String>;
}

/// Wait until `guest`'s agent answers on `port`, or give up.
///
/// One implementation for both mechanisms, because "the guest is up but its
/// agent is not serving yet" is the same state under either. A successful open
/// is a real exchange — both mechanisms perform the muxer handshake, which only
/// a serving agent answers — so this needs no protocol of its own.
///
/// Two things keep a doomed boot from costing the whole budget: a guest that has
/// died is reported as [`GuestError::Gone`] at once ([`MachineGuest::alive`]), and the
/// poll backs off, so a mechanism whose probe is itself expensive (a container's
/// probe spawns a process) is asked less and less often.
pub async fn wait_ready(
    guest: &dyn MachineGuest,
    port: u32,
    timeout: std::time::Duration,
) -> Result<(), GuestError> {
    /// First gap between probes, for a guest that may serve almost at once.
    const POLL_MIN: std::time::Duration = std::time::Duration::from_millis(50);
    /// Ceiling on the gap: past this, waiting longer buys nothing a boot budget
    /// does not already bound.
    const POLL_MAX: std::time::Duration = std::time::Duration::from_secs(1);

    let deadline = tokio::time::Instant::now() + timeout;
    let mut poll = POLL_MIN;
    loop {
        let last = match guest.connect(port).await {
            Ok(_) => return Ok(()),
            Err(e) => e,
        };
        if !guest.alive().await {
            return Err(GuestError::Gone(format!(
                "guest exited before its agent answered on port {port}: {last}"
            )));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(GuestError::Host(format!(
                "agent not serving on port {port} within {timeout:?}: {last}"
            )));
        }
        tokio::time::sleep(poll).await;
        poll = (poll * 2).min(POLL_MAX);
    }
}

/// One bidirectional byte stream: what [`MachineGuest::connect`] hands back, whatever
/// carries it (a vsock socket, a relayed pipe). Blanket-implemented, so no
/// mechanism has to name a wrapper type.
pub trait Duplex: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Duplex for T {}
