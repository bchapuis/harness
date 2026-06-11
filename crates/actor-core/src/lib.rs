//! Core abstractions of the distributed actor framework (spec §3, §4, §14).
//!
//! This crate is plain generic code: the actor model, the runtime seam, and the
//! single-node [`LocalSystem`] live here, with no required macros (serde's
//! derives in user code are the only ones). The cluster runtime is a separate
//! crate (`actor-cluster`); the deterministic simulator is another
//! (`actor-simulation`). Both are built on the same traits defined here, which
//! is what makes simulation run the real code rather than a model of it.

pub mod actor;
pub mod context;
pub mod error;
pub mod event;
pub mod host;
pub mod id;
pub mod mailbox;
pub mod message;
pub mod rebind;
pub mod receptionist;
pub mod refs;
pub mod registry;
pub mod reply;
pub mod runtime;
pub mod supervision;
pub mod system;

pub use actor::{Actor, BoxError, Handler, StopReason, Terminated, TerminationReason};
pub use context::Ctx;
pub use error::CallError;
pub use event::{AppEvent, Event, EventSink, SupervisionDecision};
pub use id::{ActorId, NodeId, Path};
pub use mailbox::Mailbox;
pub use message::{Manifest, Message};
pub use rebind::with_decoding_system;
pub use receptionist::{Key, Listing, Receptionist};
pub use refs::ActorRef;
pub use registry::{DispatchFn, HandlerRegistry};
pub use reply::{ReplyHandle, ReplyResult};
pub use runtime::{BoxFuture, Clock, Elapsed, Entropy, Instant, Spawner};
pub use supervision::{Backoff, Fault, Supervision, SupervisionDirective};
pub use system::{ActorSystem, LocalSystem, LocalSystemBuilder};
