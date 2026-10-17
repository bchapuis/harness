//! The machine's runtime binding seam (machine §1, §2.1): how the entity
//! reaches its live guest.
//!
//! The activation *is* a running guest (machine §1), but that guest is one of
//! the machine's two disposable things: stopping it loses no committed disk
//! block. The entity drives it through this seam: boot against the rehydrated
//! disk-facet image, pause for a capture's quiescent point (machine §4),
//! resume, kill, and never owns the mechanism's internals. The deterministic
//! simulation binds [`fake::FakeRuntimeProvider`]; production binds
//! [`hosted::HostedRuntimeProvider`], which holds its guest through
//! `machine_host::MachineHost`: a Firecracker microVM, or a container holding
//! the same rootfs on a host with no KVM (machine §2.1). Which mechanism is the
//! node's configuration, not this seam's concern.
//!
//! Named `runtime`, not `vm`: half the implementations are containers, and a
//! seam that calls a container a VM misleads at every call site. The crate's
//! vocabulary is `machine-host`'s (host / guest / agent).

use std::path::PathBuf;
use std::sync::Arc;

use actor_core::BoxFuture;
use entity::EntityName;

use crate::entity::EgressPolicy;

#[cfg(feature = "host")]
pub mod hosted;
#[cfg(feature = "host")]
pub mod ws_proto;

/// A runtime operation failed. An application-level outcome (the entity maps it
/// into replies or retries), never a durability failure. One message, not a
/// refused-vs-transport split: the entity's policy for every failure here is
/// the same; end the activation and let the next boot rehydrate (machine §4),
/// so the distinction lives in the message, never as a second code path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeError(pub String);

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for RuntimeError {}

/// An IO failure maps mechanically; a site with more to say formats its own.
impl From<std::io::Error> for RuntimeError {
    fn from(e: std::io::Error) -> RuntimeError {
        RuntimeError(e.to_string())
    }
}

/// What a boot needs (machine §3): the disk-facet image to mount as the
/// rootfs, the journaled sizing, and the machine's name for attribution.
#[derive(Clone, Debug)]
pub struct BootSpec {
    /// The disk facet's materialized image (`ctx.disk().path()`), the guest's
    /// backing drive. The guest writes it in place between captures (entity
    /// §7.15's one departure).
    pub image: PathBuf,
    pub vcpus: u8,
    pub mem_mib: u32,
    /// The machine this guest belongs to: the attribution key (machine §5.2).
    pub machine: EntityName,
    /// The machine's journaled egress policy (machine §5.2, M6): what the guest
    /// may reach out to. The provider realizes exactly what it grants; the fake
    /// provider ignores it.
    pub egress: EgressPolicy,
}

/// One live guest. Held by the activation and by nothing durable; dropped or
/// killed with the activation (machine §1).
pub trait MachineRuntime: Send + Sync + 'static {
    /// Pause the guest at a quiescent point (machine §4): once resolved, the
    /// guest issues no further writes to the image until
    /// [`resume`](MachineRuntime::resume), so a capture's scan sees a stable
    /// image (entity §7.15's capture seam).
    fn pause(&self) -> BoxFuture<'_, Result<(), RuntimeError>>;

    /// Resume a paused guest.
    fn resume(&self) -> BoxFuture<'_, Result<(), RuntimeError>>;

    /// Stop the guest. Idempotent: the forced step-down path (machine §4, M5)
    /// and `on_deactivate` both call it, possibly for a guest already gone; an
    /// implementation whose process handle outlives the call must also kill
    /// on drop, so a dropped activation can never leak a running guest.
    fn kill(&self) -> BoxFuture<'_, ()>;

    /// Replace the guest's `/workspace` (a tmpfs, machine §3) with the host
    /// workspace directory's contents. Called once per boot, before the first
    /// attach is answered; a failure means the guest must not serve (the
    /// entity kills the guest and fails the command, machine §4).
    fn push_ws(&self, ws: PathBuf) -> BoxFuture<'_, Result<(), RuntimeError>>;

    /// Flush the guest and replace the host workspace directory's contents
    /// with the guest's `/workspace`. Must be called while the guest is
    /// *running* (a paused guest cannot answer), so the capture sequence is
    /// pull → pause → capture → resume (machine §4). On failure the host
    /// directory is left untouched, so nothing partial can be durably
    /// captured.
    fn pull_ws(&self, ws: PathBuf) -> BoxFuture<'_, Result<(), RuntimeError>>;
}

/// Boots a machine's guest. One per node, injected into the entity factory
/// (`entities_named`), so each activation binds its node's mechanism.
pub trait MachineRuntimeProvider: Send + Sync + 'static {
    /// Boot a guest against `spec.image`. The image was rehydrated by the disk
    /// facet before the first command (entity §7.15), so the boot reads the
    /// committed rootfs.
    fn boot(
        &self,
        spec: BootSpec,
    ) -> BoxFuture<'static, Result<Arc<dyn MachineRuntime>, RuntimeError>>;
}

/// The simulation's runtime (machine §7): a "guest" whose activity is a
/// deterministic, seed-stable stream of block writes into the image file, so
/// captures have real dirty blocks and one seed reproduces a whole
/// attach–crash–failover–reconnect narrative byte-identically.
pub mod fake;
