//! Persistent machines: a durable, name-addressed lightweight VM as an entity.
//!
//! A **machine** is an entity whose durable state is a raw disk (the rootfs, a
//! disk facet, facets §4.4) and a synced `/workspace` (a workspace facet,
//! facets §4.1), reached over SSH, and whose live guest is disposable.
//! Identity, the journal, the single-writer fence, placement, activation, idle
//! hibernation, and failover are the entity's, unchanged. This crate is the
//! entity half of the machine; the SSH front door is `machine-frontdoor`, the
//! guest mechanisms are `machine-host`, and the guest-agent wire is
//! `machine-proto`.
//!
//! Seams:
//!
//! - [`Machine`] implements the entity's `Entity` with `Facets = (Disk, Alarm,
//!   Ws)`: metadata-only facet-0 state ([`MachineState`]), the journal
//!   ([`MachineEvent`]), and the command surface front doors and operators use
//!   ([`Provision`], [`Attach`], [`Detach`], [`DoorPolicy`], [`AddKey`],
//!   [`RevokeKey`], [`SetEgress`], [`Status`], and the workspace file commands).
//! - [`MachineRuntimeProvider`] and [`MachineRuntime`], the runtime binding the
//!   entity drives its guest through (boot, pause, resume, kill, workspace push
//!   and pull). A plug point: a node injects one provider into the entity
//!   factory. The simulation binds [`fake::FakeRuntimeProvider`]; production
//!   binds `hosted::HostedRuntimeProvider` (feature `host`), which holds its
//!   guest through whichever `machine_host::MachineHost` the node was
//!   configured with.
//! - [`EgressPolicy`], journaled per machine, and [`EgressConfig`], a node's
//!   egress plumbing: the egress half of the network seam (M6), realized by the
//!   hosted binding at boot.
//!
//! Mechanism. The `entity` module holds the session-grained durability
//! discipline: the checkpoint alarm is the whole capture cadence and the
//! session lease at once (every fire is a fenced append), and one capture
//! spine pulls the workspace, quiesces the guest, captures the disk in the same
//! batch, and resumes. `runtime::hosted` maps the binding onto `machine-host`
//! and `machine-proto`, bounding every exchange with the guest agent in size
//! and time. `net` generates each machine's egress ruleset as a pure function
//! and, behind feature `net` on Linux, installs it with a per-machine tap.
//!
//! Specified by `docs/machine-spec.md`.

// The docs link the hosted binding, which resolves only with `host` on. CI
// checks them with every feature, so a narrower build warns.
#![cfg_attr(not(feature = "host"), warn(rustdoc::broken_intra_doc_links))]

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
