//! The machine's runtime binding seam (machine §1, §2.1): how the grain
//! reaches its live microVM.
//!
//! The activation *is* a running microVM (machine §1), but the VM is one of
//! the machine's two disposable things: stopping it loses no committed disk
//! block. The grain therefore drives the VM through this narrow seam — boot
//! against the rehydrated disk-facet image, pause for a capture's quiescent
//! point (machine §4), resume, kill — and never owns VMM mechanics. The
//! deterministic simulation binds [`fake::FakeVmProvider`]; production binds
//! the Firecracker `Native` mechanics the sandbox proved (sandbox §3.5).

use std::path::PathBuf;
use std::sync::Arc;

use actor_core::BoxFuture;
use granary::GrainName;

use crate::grain::EgressPolicy;

#[cfg(feature = "firecracker")]
pub mod firecracker;
#[cfg(feature = "firecracker")]
pub mod ws_proto;

/// A VM operation failed. An application-level outcome (the grain maps it
/// into replies or retries), never a durability failure. The two variants
/// carry the caller's policy split, mirroring the sandbox tier's
/// `BracketError::{Agent, Transport}`: a guest refusal leaves a live VM the
/// grain can keep serving; a transport failure means the guest may be gone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VmError {
    /// The guest agent answered and refused (e.g. a non-zero status with its
    /// stderr): the VM and its transport still work.
    Guest(String),
    /// The transport, VMM, or host-side plumbing failed: the guest may be
    /// wedged or gone.
    Transport(String),
}

impl std::fmt::Display for VmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VmError::Guest(e) => write!(f, "vm guest error: {e}"),
            VmError::Transport(e) => write!(f, "vm transport error: {e}"),
        }
    }
}

impl std::error::Error for VmError {}

/// What a boot needs (machine §3): the disk-facet image to mount as the
/// rootfs, the journaled sizing, and the machine's name for attribution.
#[derive(Clone, Debug)]
pub struct VmSpec {
    /// The disk facet's materialized image (`ctx.disk().path()`), the VM's
    /// backing drive. The guest writes it in place between captures (grain
    /// §7.15's one departure).
    pub image: PathBuf,
    pub vcpus: u8,
    pub mem_mib: u32,
    /// The machine this VM belongs to — the attribution key (machine §5.2).
    pub machine: GrainName,
    /// The machine's journaled egress policy (machine §5.2, M6): what the guest
    /// may reach out to. The provider realizes exactly what it grants; the fake
    /// provider ignores it (M6 is verified against the pure rule generator).
    pub egress: EgressPolicy,
}

/// One live guest. Held by the activation and by nothing durable; dropped or
/// killed with the activation (machine §1).
pub trait MachineVm: Send + Sync + 'static {
    /// Pause the guest at a quiescent point (machine §4): once resolved, the
    /// guest issues no further writes to the image until [`resume`]
    /// (MachineVm::resume), so a capture's scan sees a stable image (grain
    /// §7.15's capture seam).
    fn pause(&self) -> BoxFuture<'_, Result<(), VmError>>;

    /// Resume a paused guest.
    fn resume(&self) -> BoxFuture<'_, Result<(), VmError>>;

    /// Stop the guest. Idempotent: the forced step-down path (machine §4, M5)
    /// and `on_passivate` both call it, possibly for a VM already gone; an
    /// implementation whose process handle outlives the call must also kill
    /// on drop, so a dropped activation can never leak a running guest.
    fn kill(&self) -> BoxFuture<'_, ()>;

    /// Replace the guest's `/workspace` (a tmpfs, machine §3) with the host
    /// workspace directory's contents. Called once per boot, before the first
    /// attach is answered; a failure means the guest must not serve (the
    /// grain kills the VM and fails the command, machine §4).
    fn push_ws(&self, ws: PathBuf) -> BoxFuture<'_, Result<(), VmError>>;

    /// Flush the guest and replace the host workspace directory's contents
    /// with the guest's `/workspace`. Must be called while the guest is
    /// *running* (a paused guest cannot answer), so the capture sequence is
    /// pull → pause → capture → resume (machine §4). On failure the host
    /// directory is left untouched, so nothing partial can be durably
    /// captured.
    fn pull_ws(&self, ws: PathBuf) -> BoxFuture<'_, Result<(), VmError>>;
}

/// Boots machines. One per node, injected into the grain factory
/// (`granary_named`), so each activation binds its node's VMM.
pub trait MachineVmProvider: Send + Sync + 'static {
    /// Boot a guest against `spec.image`. The image was rehydrated by the disk
    /// facet before the first command (grain §7.15), so the boot reads the
    /// committed rootfs.
    fn boot(&self, spec: VmSpec) -> BoxFuture<'static, Result<Arc<dyn MachineVm>, VmError>>;
}

/// The simulation's VM (machine §7): a "guest" whose activity is a
/// deterministic, seed-stable stream of block writes into the image file, so
/// captures have real dirty blocks and one seed reproduces a whole
/// attach–crash–failover–reconnect narrative byte-identically.
pub mod fake;
