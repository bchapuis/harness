//! Persistent machines (machine spec): a durable lightweight VM as an entity.
//!
//! A **machine** is an entity plus two things: durable storage (the rootfs as a
//! disk facet, entity §7.15, and `/workspace` as a workspace facet, entity §7.11)
//! and a network seam (machine §5). Everything else (identity, the journal,
//! the single-writer fence, placement, virtual activation, idle hibernation,
//! lossless failover) is inherited from the entity unchanged (machine §1). This
//! crate supplies the entity half:
//!
//! - [`Machine`], the entity: `Facets = (Disk, Alarm, Ws)`, facet-0 state holding
//!   only metadata (keys, host key, egress policy, sizing, intervals; machine
//!   §3), and the session-grained durability discipline of machine §4.
//! - [`MachineRuntimeProvider`]/[`MachineRuntime`], the runtime binding seam:
//!   the entity drives boot, pause-for-capture, resume, and kill through it, so
//!   the deterministic simulation runs a
//!   [`FakeRuntimeProvider`](runtime::fake::FakeRuntimeProvider) while
//!   production runs a [`HostedRuntimeProvider`](runtime::hosted::HostedRuntimeProvider)
//!   over whichever mechanism the node's `machine_host::MachineHost` is; a
//!   microVM or a container (machine §2.1, sandbox §3.5).
//!
//! The network seam has two halves, and both read the journaled state this
//! crate defines (authorized keys, host key, egress policy, attachments). The
//! SSH front door (machine §5.1, M4) lives in `machine-frontdoor`. Policy-bound
//! egress (machine §5.2, M6) lives here: the hosted binding realizes a
//! machine's journaled policy at boot as a per-machine tap and ruleset, on a
//! node configured with an [`EgressConfig`].

mod entity;
// Only the hosted binding applies egress. Without it the module still builds,
// so the M6 ruleset tests run in every configuration.
#[cfg_attr(not(feature = "host"), allow(dead_code))]
mod net;
mod runtime;

pub use entity::AddKey;
pub use entity::Attach;
pub use entity::AttachReply;
pub use entity::Attachment;
pub use entity::Detach;
pub use entity::DetachReason;
pub use entity::DoorPolicy;
pub use entity::DoorPolicyReply;
pub use entity::EgressPolicy;
pub use entity::MACHINE_TYPE;
pub use entity::MAX_WS_FILE;
pub use entity::Machine;
pub use entity::MachineError;
pub use entity::MachineEvent;
pub use entity::MachineState;
pub use entity::Provision;
pub use entity::RevokeKey;
pub use entity::SetEgress;
pub use entity::Status;
pub use entity::StatusReply;
pub use entity::WsFileInfo;
pub use entity::WsList;
pub use entity::WsRead;
pub use entity::WsRemove;
pub use entity::WsWrite;
pub use net::EgressConfig;
pub use runtime::BootSpec;
pub use runtime::MachineRuntime;
pub use runtime::MachineRuntimeProvider;
pub use runtime::RuntimeError;
pub use runtime::fake;
#[cfg(feature = "host")]
pub use runtime::hosted;
