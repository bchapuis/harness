//! The agentic harness (agentic-harness-spec.md): compound AI sessions run
//! as actors on a mutualized cluster, built on the distributed actor
//! framework and the cluster utilities and modifying neither.
//!
//! One sentence carries the design (§2.1): **the journal is the session; the
//! actor and the sandbox are disposable; the seams are the only world.** The
//! loop runs in the agent actor — serial, journaled, effect-free outside its
//! three seams (the [`Model`], the [`Journal`], the [`Sandbox`]) — and every
//! tool effect lands in one isolated environment per session (§5.1). Time,
//! randomness, task spawning, and transport come from the core seams, so
//! deterministic simulation extends to the harness unchanged: one seed
//! reproduces an entire multi-node agentic run (§12).
//!
//! ```ignore
//! let h = Harness::new(system, kinds, journal, model, sandboxes);
//! let s = h.session("researcher", SessionId::new("report-42"));
//! let out = s.prompt(Turn::new(TurnId::new("t-1"), "Summarize the corpus.")).await;
//! ```

pub mod agent;
pub mod budget;
pub mod client;
pub mod event;
pub mod host;
pub mod journal;
pub mod model;
pub mod sandbox;
pub mod session;
pub mod tool;

pub use budget::Budget;
pub use budget::Spend;
pub use budget::Usage;
pub use client::Harness;
pub use client::HarnessConfig;
pub use client::HarnessSystem;
pub use client::SessionRef;
pub use event::HarnessEvent;
pub use host::Awaited;
pub use host::Host;
pub use host::HostReject;
pub use host::Kind;
pub use host::Kinds;
pub use host::host_key;
pub use journal::AppendError;
pub use journal::InMemoryJournal;
pub use journal::Journal;
pub use journal::JournalError;
pub use journal::SeqNo;
pub use model::Model;
pub use model::ModelError;
pub use model::ModelParams;
pub use model::ModelRequest;
pub use model::ModelResponse;
pub use model::ToolCall;
pub use model::ToolSpec;
pub use sandbox::Sandbox;
pub use sandbox::SandboxError;
pub use sandbox::SandboxProfile;
pub use sandbox::SandboxProvider;
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
