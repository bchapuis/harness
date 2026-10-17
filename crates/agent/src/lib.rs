//! The agent runtime: compound AI sessions (loops of model calls, tool calls,
//! and delegation) run as entities on a shared cluster. A session is an
//! [`entity`] whose activation drives itself as a [`workflow`]: identity, the
//! journal, the single-writer fence, placement, activation, hibernation, and
//! failover are the entity's, and the derive-launch-settle cycle is the
//! workflow's. This crate adds the transcript and its fold, two seams, kinds,
//! budgets, delegation, and cancellation. Specified by `docs/agent-spec.md`.
//!
//! # Seams
//!
//! - [`Model`]: inference, one request and one response. Production adapters
//!   build on the [`http`] kit: a deployment supplies [`http::HttpPost`] (the
//!   socket), an adapter crate a [`http::Protocol`] (the wire format), and
//!   [`http::ProtocolModel`] joins them under a bounded, seeded retry.
//! - [`SandboxProvider`] and [`Sandbox`]: the isolated environment every
//!   declared tool except `delegate` runs in, one per activation, opened
//!   lazily and called at the [`Tier`] each tool declares.
//! - [`Kind`], [`Kinds`], [`ToolDecl`], [`ToolRegistry`]: a deployment's agent
//!   types; the registry is the allowlist a call dispatches against.
//! - [`Harness`] and [`SessionRef`]: the client view (prompt, cancel, tail,
//!   follow) for applications, the HTTP edge, and delegating parents.
//! - [`Record`] and [`AgentEvent`]: the journal schema every reader depends
//!   on, and the checker-facing event vocabulary.
//!
//! The session type implements [`entity::Entity`] and [`workflow::Workflow`].
//!
//! # Mechanism
//!
//! The session entity (module `agent`) folds [`Record`]s into its state
//! (module `session`) and implements the workflow: `desired` computes the
//! effects committed state calls for (the step's model call, its tool calls
//! and delegations, owed cancels), `perform` runs one effect on the seams, and
//! `settle` folds its outcome into records. Ahead of every tick an
//! environment pre-pass (`Agent::reconcile`) settles what no fold can express:
//! opening the sandbox, journaling a tier acquisition, reporting a lost
//! workspace. After each sandboxed call the workspace facet is captured, so
//! the call's file delta commits with its outcome.
//!
//! The client view (module `client`) hosts one entity type per kind through
//! the entity's `entities_named` factory, injecting the node's seams, and
//! implements the attach protocol `prompt` and a delegation's child wait
//! share: an ephemeral reply mailbox per submit attempt, re-submitted under
//! the same `TurnId`. Every operation is launched through the system's
//! `Spawner` and timed by its `Clock`, so one seed reproduces a multi-node run
//! under simulation.
//!
//! ```ignore
//! // A kind, identical on every node; the registry is the allowlist.
//! let researcher = Kind::new("You are a research agent.")
//!     .tool("read_file", "Read a workspace file", &READ_SCHEMA, Tier::Workspace)
//!     .delegates_to(&["researcher"])
//!     .sandbox(SandboxProfile::image("workspace:base"))
//!     .entity(EntityConfig { shards: 16, ..EntityConfig::default() })
//!     .budget(Budget::new(200_000, 50));
//! let kinds = Kinds::new().register("researcher", researcher);
//!
//! // Per node: one harness, with the model and sandbox seams injected.
//! let h = Harness::cluster(system, &kinds, model, sandboxes);
//!
//! // Any node: address the session by name; its shard leader hosts it.
//! let s = h.session("researcher", SessionId::new("report-42"))?;
//! match s.prompt(Turn::new(TurnId::new("t-1"), "Summarize the corpus.")).await {
//!     Ok(Ok(completion)) => println!("{}", completion.text()),
//!     Ok(Err(run_error)) => eprintln!("run ended: {run_error}"),
//!     // A caller bug (the TurnId reused for other content): fix, never retry.
//!     Err(PromptError::Rejected(reject)) => eprintln!("{reject}"),
//!     // Ambiguous or transient: re-prompting the same TurnId is safe.
//!     Err(PromptError::Entity(e)) => eprintln!("retry t-1: {e}"),
//! }
//! ```

mod agent;
mod budget;
mod client;
mod config;
/// The desired set, as a pure function of committed state: this runtime's
/// `Workflow::pending`, and with it the `Effect` type the workflow carries from
/// `pending` to `perform`.
mod desired;
mod event;
pub mod http;
mod kind;
mod model;
mod sandbox;
mod session;
#[cfg(feature = "testing")]
pub mod testing;
mod tool;

pub use agent::SubmitReject;
pub use budget::Budget;
pub use budget::Usage;
pub use client::Followed;
pub use client::Follower;
pub use client::Harness;
pub use client::HarnessBuilder;
pub use client::HarnessSystem;
pub use client::HasRoutes;
pub use client::NoRoutes;
pub use client::PromptError;
pub use client::SessionRef;
pub use client::TAIL_PAGE;
pub use client::UnknownKind;
pub use config::HarnessConfig;
pub use event::AgentEvent;
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
