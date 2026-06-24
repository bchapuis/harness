//! The agentic harness (agentic-harness-spec.md): compound AI sessions run as
//! **grains** on a mutualized cluster. An agent is a grain ([`granary`]) plus
//! three things — a self-driving loop, a model seam, and a sandbox — and nothing
//! else: identity, the journal, the single-writer fence, placement, activation,
//! hibernation, and lossless failover are all inherited from the grain unchanged.
//!
//! One sentence carries the design (§2.1): **the grain is the session; the
//! activation and the sandbox are disposable; the seams are the only world.** The
//! loop runs as the grain's activation behavior — serial, journaled, effect-free
//! outside its two seams (the [`Model`] and the [`Sandbox`]) and the grain's own
//! journal — and every tool effect lands in one isolated environment per session
//! (§5.1). Time, randomness, task spawning, transport, and the journal come from
//! granary and the core seams, so deterministic simulation extends to the harness
//! unchanged: one seed reproduces an entire multi-node agentic run (§12).
//!
//! ```ignore
//! let h = Harness::new(system, kinds, model, sandboxes);
//! let s = h.session("researcher", SessionId::new("report-42"));
//! let out = s.prompt(Turn::new(TurnId::new("t-1"), "Summarize the corpus.")).await;
//! ```

pub mod agent;
pub mod budget;
pub mod client;
pub mod event;
pub mod kind;
pub mod model;
pub mod sandbox;
pub mod session;
pub mod tool;

pub use agent::Accepted;
pub use agent::Agent;
pub use agent::Cancel;
pub use agent::RunCompleted;
pub use agent::Submit;
pub use agent::SubmitReject;
pub use agent::SubmitStatus;
pub use agent::Tail;
pub use budget::Budget;
pub use budget::Spend;
pub use budget::Usage;
pub use client::Follower;
pub use client::Harness;
pub use client::HarnessConfig;
pub use client::HarnessSystem;
pub use client::SessionRef;
pub use event::HarnessEvent;
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
pub use session::KindId;
pub use session::Lineage;
pub use session::Record;
pub use session::RecordBody;
pub use session::RunError;
pub use session::RunOutcome;
pub use session::SessionId;
pub use session::SessionState;
pub use session::Turn;
pub use session::TurnId;
pub use tool::DELEGATE;
pub use tool::DelegateInput;
pub use tool::OnDangling;
pub use tool::ToolDecl;
pub use tool::ToolError;
pub use tool::ToolRegistry;

// Re-exported granary types that appear in the harness's public surface, so a
// consumer needs no direct granary dependency for ordinary use.
pub use granary::FileGrainStore;
pub use granary::GrainError;
pub use granary::GrainStoreFactory;
pub use granary::GranaryConfig;
pub use granary::Seq;
