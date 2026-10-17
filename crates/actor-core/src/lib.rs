//! Core abstractions of the distributed actor framework (spec §3, §4, §14).
//!
//! The actor model, the runtime seam, and the single-node [`LocalSystem`] live
//! here, as plain generic code with no required macros (serde's derives in user
//! code are the only ones). The cluster runtime (`actor-cluster`) and the
//! deterministic simulator (`actor-simulation`) are separate crates built on
//! the traits defined here, so simulation runs the real code rather than a
//! model of it.

mod actor;
mod context;
mod error;
mod event;
pub mod host;
mod id;
mod mailbox;
mod message;
mod rebind;
mod receptionist;
mod refs;
mod registry;
mod reply;
mod runtime;
mod supervision;
mod system;

pub use actor::{Actor, BoxError, Handler, StopReason, Terminated, TerminationReason};
// The codec vocabulary, re-exported so a deployment choosing a wire codec
// (spec §5) needs no direct `actor-serialization` dependency.
pub use actor_serialization::{Codec, CodecError, JsonCodec, PostcardCodec};
pub use context::Ctx;
pub use error::CallError;
pub use error::join_all_results;
pub use event::{AppEvent, Event, EventSink, SupervisionDecision};
pub use id::{ActorId, NodeId, Path};
pub use mailbox::Mailbox;
pub use message::{Manifest, Message};
pub use receptionist::{Key, Listing, Receptionist};
pub use refs::ActorRef;
pub use registry::HandlerRegistry;
pub use reply::{ReplyHandle, ReplyResult};
pub use runtime::{BoxFuture, Clock, Elapsed, Entropy, Instant, Spawner};
pub use supervision::{Backoff, Fault, Supervision, SupervisionDirective};
pub use system::{ActorSystem, LocalSystem, LocalSystemBuilder};
