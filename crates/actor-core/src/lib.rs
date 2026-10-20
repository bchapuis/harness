//! Core abstractions of the distributed actor framework: the actor model, the
//! [`ActorSystem`] seam, the host-environment seam, and [`LocalSystem`], the
//! single-node system. Everything here is plain generic code with no macros of
//! its own (serde's derives in user code are the only ones). The cluster
//! runtime (`actor-cluster`) and the simulator (`actor-simulation`) build on
//! these traits, so a simulation runs the production code, not a model of it.
//!
//! # Seams
//!
//! - **Application traits**: [`Actor`], [`Message`] (with its [`Manifest`]),
//!   and [`Handler`], which applications implement; [`ActorRef`] and [`Ctx`],
//!   which they call; [`Supervision`] and [`SupervisionDirective`], which they
//!   choose; [`Terminated`] for death watch and [`Receptionist`] for discovery.
//! - **[`ActorSystem`]**: the runtime an actor runs on, with a consumer face
//!   (spawn, resolve, time, randomness, tasks, events) and an implementor face
//!   (local resolution, the codec, the byte-level `remote_ask`/`remote_tell`,
//!   watch registration, receptionist replication). [`LocalSystem`] implements
//!   it here; `actor-cluster`'s `ClusterSystem` implements it for a cluster.
//! - **Host environment**: [`Clock`], [`Entropy`], and [`Spawner`], which
//!   `actor-runtime` implements for production and `actor-simulation` for
//!   simulation. No code in the framework reads time, randomness, or a task
//!   runtime any other way.
//! - **Observability**: [`Event`] and the [`EventSink`] a deployment supplies;
//!   a layer above extends the stream through [`Event::App`] and [`AppEvent`].
//! - **Implementor toolkit**: the [`host`] module ([`host::LocalHost`] and its
//!   companions) is what an `ActorSystem` implementation is built from. It is
//!   not application API.
//! - **Codec**: [`Codec`] and its two implementations, re-exported from
//!   `actor-serialization` so a deployment choosing one needs no direct
//!   dependency.
//!
//! # Mechanism
//!
//! One [`host::LocalHost`] per node owns the actors: it assigns ids (stamped
//! with the process incarnation), runs one serial executor per actor over a
//! bounded mailbox (`mailbox`), applies supervision in that executor, keeps the
//! death-watch table, and dispatches inbound remote messages through each actor
//! type's [`HandlerRegistry`], the deserialization allowlist (`registry`). A
//! send through [`ActorRef`] classifies the target from its id on every call:
//! a local target is enqueued by value, a remote one is encoded with the system
//! codec and handed to `remote_ask`/`remote_tell`, and an `ActorRef` decoded
//! inside a message is rebound to the decoding system (`rebind`). `ask`
//! brackets every call with `AskIssued`/`AskOutcome` events (`refs`).
//!
//! Specified by `docs/distributed-actor-spec.md`.

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
// (spec §3.4) needs no direct `actor-serialization` dependency.
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
