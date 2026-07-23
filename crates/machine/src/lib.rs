//! Persistent machines (machine spec): a durable lightweight VM as a grain.
//!
//! A **machine** is a grain plus two things: durable storage (the rootfs as a
//! disk facet, grain §7.15, and `/workspace` as a workspace facet, grain §7.11)
//! and a network seam (machine §5). Everything else — identity, the journal,
//! the single-writer fence, placement, virtual activation, idle hibernation,
//! lossless failover — is inherited from the grain unchanged (machine §1). This
//! crate supplies the grain half:
//!
//! - [`Machine`], the grain: `Facets = (Disk, Alarm, Ws)`, facet-0 state holding
//!   only metadata (keys, host key, egress policy, sizing, intervals — machine
//!   §3), and the session-grained durability discipline of machine §4: capture
//!   at quiescent points driven by the checkpoint alarm, `can_passivate`
//!   refusing eviction while the image holds uncaptured writes, and the
//!   deposed side self-fencing (M5) because every alarm fire *is* a fenced
//!   append — a non-committed outcome forces the step-down whose
//!   `on_passivate` kills the microVM.
//! - [`MachineVmProvider`]/[`MachineVm`], the runtime binding seam: the grain
//!   drives boot, pause-for-capture, resume, and kill through it, so the
//!   deterministic simulation runs a [`FakeVmProvider`](vm::fake::FakeVmProvider)
//!   while production binds Firecracker (machine §2.1, sandbox §3.5).
//!
//! The network seam — the SSH front door (machine §5.1, M4) and policy-bound
//! egress (machine §5.2, M6) — lives in its own crates; this one defines the
//! journaled state both read (authorized keys, host key, egress policy,
//! attachments).

mod grain;
mod net;
mod vm;

pub use grain::AddKey;
pub use grain::Attach;
pub use grain::AttachReply;
pub use grain::Attachment;
pub use grain::Detach;
pub use grain::DetachReason;
pub use grain::EgressPolicy;
pub use grain::MACHINE_TYPE;
pub use grain::MAX_WS_FILE;
pub use grain::Machine;
pub use grain::MachineError;
pub use grain::MachineEvent;
pub use grain::MachineState;
pub use grain::Provision;
pub use grain::RevokeKey;
pub use grain::SetEgress;
pub use grain::Status;
pub use grain::StatusReply;
pub use grain::WsFileInfo;
pub use grain::WsList;
pub use grain::WsRead;
pub use grain::WsRemove;
pub use grain::WsWrite;
pub use net::EgressConfig;
pub use net::GuestNet;
pub use net::GuestPool;
pub use net::guest_ip_boot_arg;
pub use net::guest_mac;
pub use net::guest_net;
pub use net::nft_ruleset;
pub use net::tap_name;
pub use vm::MachineVm;
pub use vm::MachineVmProvider;
pub use vm::VmError;
pub use vm::VmSpec;
pub use vm::fake;
#[cfg(feature = "firecracker")]
pub use vm::firecracker;
