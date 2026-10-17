//! The agent runtime (agent-spec.md): compound AI sessions run as **entities** on
//! a mutualized cluster. An agent is an entity ([`entity`]) plus a self-driving
//! loop, a model seam, and a sandbox; identity, the journal, the single-writer
//! fence, placement, activation, hibernation, and lossless failover are inherited
//! from the entity unchanged.
//!
//! The design in one sentence (§2.1): **the entity is the session; the activation
//! and the sandbox are disposable; the seams are the only world.** The loop runs
//! as the entity's activation behavior: serial, journaled, effect-free outside
//! its two seams (the [`Model`] and the [`Sandbox`]) and the entity's own journal,
//! with every tool effect landing in one isolated environment per session (§5.1).
//! Time, randomness, task spawning, transport, and the journal come from entity
//! and the core seams, so one seed reproduces an entire multi-node agentic run
//! (§12).
//!
//! ```ignore
//! let h = Harness::builder(system, &kinds).host_all(model, sandboxes).build();
//! let s = h.session("researcher", SessionId::new("report-42"));
//! let out = s.prompt(Turn::new(TurnId::new("t-1"), "Summarize the corpus.")).await;
//! ```

pub mod agent;
pub mod budget;
pub mod client;
pub mod config;
/// The desired set, as a pure function of committed state: this runtime's
/// `Workflow::pending`, and with it the `Effect` type the workflow carries from
/// `pending` to `perform`.
pub mod desired;
pub mod event;
pub mod http;
pub mod kind;
pub mod model;
pub mod sandbox;
pub mod session;
#[cfg(feature = "testing")]
pub mod testing;
pub mod tool;

pub use budget::Budget;
pub use budget::Usage;
pub use client::Followed;
pub use client::Follower;
pub use client::Harness;
pub use client::HarnessBuilder;
pub use client::HarnessSystem;
pub use client::HasRoutes;
pub use client::NoRoutes;
pub use client::SessionRef;
pub use client::TAIL_PAGE;
pub use client::UnknownKind;
pub use config::HarnessConfig;
pub use event::AgentEvent;
pub use http::HttpError;
pub use http::HttpPost;
pub use http::HttpResponse;
pub use http::Protocol;
pub use http::ProtocolModel;
pub use kind::Kind;
pub use kind::Kinds;
pub use model::Model;
pub use model::ModelError;
pub use model::ModelParams;
pub use model::ModelRequest;
pub use model::ModelResponse;
pub use model::ToolCall;
pub use model::ToolSpec;
pub use sandbox::ComputeLimits;
pub use sandbox::Sandbox;
pub use sandbox::SandboxError;
pub use sandbox::SandboxProfile;
pub use sandbox::SandboxProvider;
pub use sandbox::Tier;
pub use session::CallId;
pub use session::Completion;
pub use session::Entry;
pub use session::WORKSPACE_RESET_NOTICE;
pub use session::KindId;
pub use session::Lineage;
pub use session::Record;
pub use session::RecordBody;
pub use session::RunError;
pub use session::RunOutcome;
pub use session::SessionId;
pub use session::Turn;
pub use session::TurnId;
pub use tool::DELEGATE;
pub use tool::DelegateInput;
pub use tool::OnOrphan;
pub use tool::ToolDecl;
pub use tool::ToolError;
pub use tool::ToolRegistry;

// The future every seam method returns (`Model`, `HttpPost`, `Sandbox`,
// `SandboxProvider`), so an adapter implementing a seam needs no direct
// actor-core dependency.
pub use actor_core::BoxFuture;

// Re-exported entity types that appear in the agent runtime's public surface, so a
// consumer needs no direct entity dependency for ordinary use.
pub use entity::EntityConfig;
pub use entity::EntityError;
pub use entity::EventHistory;
pub use entity::Seq;
pub use entity::store::EntityStoreFactory;
pub use entity::store::FileEntityStore;
