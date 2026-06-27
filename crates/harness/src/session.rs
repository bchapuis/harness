//! Session identity, records, and the fold (harness spec §2, §6.3).
//!
//! One sentence carries the design: **the grain is the session; the activation
//! and the sandbox are disposable; the seams are the only world** (§2.1). A
//! session *is* a grain: its [`SessionId`] is the key half of a `GrainName`
//! (`(KindId, SessionId)`, §2.2), each [`Record`] is the grain's `Event`, and
//! [`SessionState`] is the grain's `State` — the pure, deterministic fold of the
//! journal that granary rehydrates on activation (invariant H1). Anything not
//! journaled is, by definition, lost on deactivation: a design constraint, not
//! an accident to discover.
//!
//! Identity is layered deliberately (§2.2), and the layering is now the grain's:
//! `SessionId` (= the `GrainName` key: durable, application-chosen) → `ActorId`
//! (one activation, system-assigned) → `TurnId` (one run). The `KindId` is the
//! session's grain *type* (granary hosts one `Agent` grain under each kind's name
//! via `granary_named`); granary owns name→shard→leader resolution.

use std::collections::BTreeMap;
use std::sync::Arc;

use granary::Seq;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::budget::Budget;
use crate::budget::Spend;
use crate::budget::Usage;
use crate::model::ModelError;
use crate::model::ToolCall;
use crate::sandbox::Tier;
use crate::tool::DELEGATE;
use crate::tool::ToolError;

macro_rules! id_string {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(
            Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        pub struct $name(String);

        impl $name {
            pub fn new(id: impl Into<String>) -> $name {
                $name(id.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

id_string! {
    /// A session's durable identity (harness spec §2.2): the key half of the
    /// grain's `GrainName`, application-chosen, surviving activation restarts and
    /// shard-leadership moves; an `ActorId` does not.
    SessionId
}
id_string! {
    /// One submitted turn — and the run it triggers (harness spec §2.2). The
    /// client-chosen idempotency key (§7.4): re-submitting it never starts a
    /// second run (invariant H7).
    TurnId
}
id_string! {
    /// One requested tool call, unique within its run (harness spec §5.2):
    /// the model API's tool-use id, or one the harness assigned on receipt.
    CallId
}
id_string! {
    /// A named agent definition (harness spec §2.2), registered identically
    /// on every node (§7.1).
    KindId
}

/// One submitted input (harness spec §2.2): a user prompt, or a parent
/// agent's delegation (§8). The `id` is the idempotency key (§7.4); `budget`
/// overrides the kind's default for the run it triggers (§9.1).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Turn {
    pub id: TurnId,
    pub content: String,
    #[serde(default)]
    pub budget: Option<Budget>,
}

impl Turn {
    pub fn new(id: TurnId, content: impl Into<String>) -> Turn {
        Turn {
            id,
            content: content.into(),
            budget: None,
        }
    }

    /// Set an explicit budget for the run this turn triggers (§9.1).
    pub fn with_budget(mut self, budget: Budget) -> Turn {
        self.budget = Some(budget);
        self
    }
}

/// The lineage of a delegated session (harness spec §8.1, §10.3): the
/// delegating session and turn, and the tree's `root` — the transitive
/// closure of the parent links, denormalized so any record can name its
/// logical request in O(1). Correlation metadata only; nothing routes or
/// folds on it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lineage {
    pub session: SessionId,
    pub turn: TurnId,
    pub root: SessionId,
}

/// A run's successful terminal outcome (harness spec §3.1): the final
/// assistant message, with the run's journaled token spend — own usage plus
/// carve-outs (§9.1) — for the caller's accounting.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Completion {
    content: String,
    pub tokens: u64,
}

impl Completion {
    pub fn new(content: impl Into<String>, tokens: u64) -> Completion {
        Completion {
            content: content.into(),
            tokens,
        }
    }

    pub fn text(&self) -> &str {
        &self.content
    }
}

/// A run's abnormal terminal outcome (harness spec §3.1) — an application
/// error living **inside the reply**, distinct from transport/durability
/// `GrainError`. Exactly these three: "a tool misbehaved" is not a run failure
/// (§5.4), and durability failure is not a run outcome at all — it is the
/// grain's outer `GrainError::Unavailable`, which *pauses* the run rather than
/// ending it (§3.1, §6.4), since a shard that cannot commit cannot record "I
/// could not record".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunError {
    /// The budget ran out (§9.1); recoverable by a new turn with a new budget.
    BudgetExhausted,
    /// The run was cancelled (§9.2).
    Cancelled,
    /// A model failure no retry policy absorbed (§4.3).
    Model(ModelError),
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::BudgetExhausted => f.write_str("budget exhausted"),
            RunError::Cancelled => f.write_str("cancelled"),
            RunError::Model(e) => write!(f, "model failure: {e}"),
        }
    }
}

/// A run's terminal outcome, exactly one per run (invariant H3).
pub type RunOutcome = Result<Completion, RunError>;

/// The label a terminal outcome carries on the event stream (§10.4).
pub fn outcome_label(outcome: &RunOutcome) -> &'static str {
    match outcome {
        Ok(_) => "ok",
        Err(RunError::BudgetExhausted) => "budget",
        Err(RunError::Cancelled) => "cancelled",
        Err(RunError::Model(_)) => "model",
    }
}

/// One journal entry (harness spec §6). The `at_nanos` timestamp is the
/// writing node's `Clock` reading: observational metadata the fold MUST NOT
/// let influence behavior (§10.1) — under simulation it is virtual, so
/// timestamped journals still reproduce byte-identically.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub at_nanos: u64,
    pub body: RecordBody,
}

/// What a record says (harness spec §6.4, §10.1): records are durable and
/// user-facing — the transcript, the calls and outcomes, the costs, the tree
/// links. If an observer needs it, it is a record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RecordBody {
    /// The session exists: its kind, a digest of the kind's definition
    /// (§7.1, §10.5), and its lineage (§10.3). Always the first record.
    SessionCreated {
        kind: KindId,
        digest: u64,
        parent: Option<Lineage>,
        root: SessionId,
    },
    /// A turn was accepted and its run began (§3.1). The committing append is
    /// what makes `RunStarted` fire exactly once per turn (§10.4).
    TurnSubmitted {
        turn: TurnId,
        content: String,
        budget: Budget,
    },
    /// One model response — journaled before any of its tool calls execute
    /// (intent before effect, §6.4), each requested call identified by its
    /// `CallId`.
    ModelResponse {
        turn: TurnId,
        content: String,
        calls: Vec<ToolCall>,
        usage: Usage,
    },
    /// One tool or delegation outcome, journaled before the next step shows
    /// it to the model (§6.4).
    ToolOutcome {
        turn: TurnId,
        call: CallId,
        outcome: Result<Value, ToolError>,
    },
    /// A delegation's intent (§8.1): the child kind (its grain type, needed to
    /// address the child for cancel propagation, §9.2), the child session and
    /// turn — both derived deterministically from this session, this turn, and
    /// the call, so a re-executed delegation re-derives the same pair — plus the
    /// carved budget (§9.1). Cancel propagation reads children from here (§9.2).
    ChildRun {
        turn: TurnId,
        call: CallId,
        child_kind: KindId,
        child_session: SessionId,
        child_turn: TurnId,
        budget: Budget,
    },
    /// The workspace the transcript asserts is gone (§5.5): journaled before
    /// the next model call of an activation that will open a fresh sandbox
    /// for a session whose journal records sandboxed activity, and surfaced
    /// to the model with that request.
    WorkspaceReset,
    /// The activation's first call at `tier` is about to execute (§5.6):
    /// the write-ahead discipline (§6.4) applied to capability acquisition,
    /// intent journaled before effect. The audit trail — when did this
    /// session first run guest code, first touch the network? — and the
    /// future policy hook (§13). A record, not a §10.4 event: verified by
    /// journal audit (sandbox spec S4), the way H2's quiescence audit works.
    TierAcquired { turn: TurnId, tier: Tier },
    /// The run's exactly-one terminal outcome (§3.1, invariant H3).
    RunEnded { turn: TurnId, outcome: RunOutcome },
}

/// One transcript item, as the model request carries it (harness spec §4.1).
/// A projection of the records: the fold appends here as turns, responses,
/// and outcomes commit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Entry {
    /// A submitted turn's content.
    User(String),
    /// A model response: assistant content plus requested calls.
    Assistant {
        content: String,
        calls: Vec<ToolCall>,
    },
    /// A tool call's outcome, fed back as the tool result (§5.4).
    ToolResult {
        call: CallId,
        outcome: Result<Value, ToolError>,
    },
    /// The workspace-loss notice (§5.5): input content the harness authors,
    /// the encoding's analogue of a user message — it answers no `CallId`,
    /// so it cannot ride a tool result.
    WorkspaceReset,
}

/// Serde adapter for an `Arc<Vec<Entry>>`: encode the inner slice, rebuild a
/// fresh `Arc` on decode. Lets the transcript be `Arc`-shared — so a model
/// request clones a pointer, not the whole conversation, each step (§4.1) —
/// without enabling serde's workspace-wide `rc` feature. A `SessionState` holds
/// no second reference to its transcript `Arc`, so there is no cross-`Arc`
/// sharing for a snapshot round-trip to lose: a fresh `Arc` per decode is exact.
pub(crate) mod arc_transcript {
    use std::sync::Arc;

    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;

    use super::Entry;

    pub(crate) fn serialize<S: Serializer>(v: &Arc<Vec<Entry>>, s: S) -> Result<S::Ok, S::Error> {
        (**v).serialize(s)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Arc<Vec<Entry>>, D::Error> {
        Vec::<Entry>::deserialize(d).map(Arc::new)
    }
}

/// FNV-1a 64 over a turn's content: the digest dedup compares re-submissions
/// against (§7.4) without holding a second copy of the content. Fold-local —
/// rebuilt on every replay and never journaled — so its exact value need not
/// stay stable across versions, unlike [`Kind::digest`](crate::Kind::digest).
pub fn content_digest(content: &str) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in content.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// What the fold knows about a turn (harness spec §7.4): enough to dedup a
/// re-submission and return the recorded outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TurnFacts {
    pub content_digest: u64,
    /// `None` while the run is unfinished.
    pub outcome: Option<RunOutcome>,
}

/// A journaled call intent whose outcome is not yet journaled. While the run
/// is live these are the in-flight calls of the current step; at activation
/// they are the **dangling calls** resume must resolve (§5.5).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingCall {
    pub name: String,
    pub input: Value,
    /// Set once the delegation's `ChildRun` intent committed (§8.1).
    pub child: Option<ChildRef>,
}

/// A recorded delegation target (§8.1, §9.2): the child's kind (grain type),
/// session, turn, and carved budget — enough to address it and cancel it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChildRef {
    pub kind: KindId,
    pub session: SessionId,
    pub turn: TurnId,
    pub budget: Budget,
}

/// The unfinished run, as the fold sees it (harness spec §3.1): the step is a
/// state the fold tracks, not a stack frame the executor holds (§3.2).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LiveRun {
    pub turn: TurnId,
    pub budget: Budget,
    pub spend: Spend,
    /// Journaled intents lacking outcomes, by call id. Empty ⇒ the next
    /// action is a model call.
    pub pending: BTreeMap<CallId, PendingCall>,
}

/// The session's creation facts (§7.1, §10.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Created {
    pub kind: KindId,
    pub digest: u64,
    pub parent: Option<Lineage>,
    pub root: SessionId,
}

/// The fold of a journal prefix (harness spec §6.3): a pure, deterministic
/// function of the records, with no information outside it influencing
/// behavior except new inputs arriving as messages. Replay is therefore
/// resume (invariant H1).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionState {
    pub created: Option<Created>,
    /// The model-facing conversation projection (§4.1), `Arc`-shared so each
    /// model request takes a pointer to it rather than a deep copy. The fold
    /// appends with copy-on-write ([`Arc::make_mut`]); since an in-flight request
    /// has dropped its clone by the time its response folds, the append is in
    /// place. Serialized as its inner slice (see [`arc_transcript`]).
    #[serde(with = "arc_transcript")]
    pub transcript: Arc<Vec<Entry>>,
    pub turns: BTreeMap<TurnId, TurnFacts>,
    /// At most one unfinished run; the journal's total order serializes runs.
    pub live: Option<LiveRun>,
    /// Whether the journal records sandboxed activity since the last
    /// `WorkspaceReset` — the §5.5 trigger for journaling the next one.
    pub sandbox_activity: bool,
    /// Every committed record, in `Seq` order: the grain owns the journal, so
    /// the activation mirrors it here to serve `Tail` (§10.2) — the one read
    /// that needs the raw records, not the [`transcript`](Self::transcript)
    /// projection. The grain commits one event per `Seq`, contiguously, so the
    /// record at index `i` carries `Seq(i + 1)`.
    pub records: Vec<Record>,
}

impl SessionState {
    /// The `Seq` of the record at `index` in [`records`](Self::records). The
    /// grain commits one event per `Seq`, contiguously from `Seq(1)`, so it is
    /// `index + 1`. The one place this index→`Seq` rule lives; `Tail` reads it.
    pub fn seq_of(index: usize) -> Seq {
        Seq::new(index as u64 + 1)
    }

    /// Fold one committed record — the grain's `apply` (granary §4.1), pure and
    /// deterministic, run on the live commit path and on every replay. Total: a
    /// record that fits no transition (a malformed journal) is ignored rather
    /// than panicking — the journal is the authority, and the fold's job is to
    /// read it, not police it.
    pub fn apply(&mut self, record: &Record) {
        self.records.push(record.clone());
        match &record.body {
            RecordBody::SessionCreated {
                kind,
                digest,
                parent,
                root,
            } => {
                self.created = Some(Created {
                    kind: kind.clone(),
                    digest: *digest,
                    parent: parent.clone(),
                    root: root.clone(),
                });
            }
            RecordBody::TurnSubmitted {
                turn,
                content,
                budget,
            } => {
                self.turns.insert(
                    turn.clone(),
                    TurnFacts {
                        content_digest: content_digest(content),
                        outcome: None,
                    },
                );
                self.live = Some(LiveRun {
                    turn: turn.clone(),
                    budget: *budget,
                    spend: Spend::default(),
                    pending: BTreeMap::new(),
                });
                Arc::make_mut(&mut self.transcript).push(Entry::User(content.clone()));
            }
            RecordBody::ModelResponse {
                turn,
                content,
                calls,
                usage,
            } => {
                if let Some(live) = self.live.as_mut().filter(|l| &l.turn == turn) {
                    live.spend.own_tokens += usage.total();
                    live.spend.own_steps += 1;
                    for call in calls {
                        live.pending.insert(
                            call.id.clone(),
                            PendingCall {
                                name: call.name.clone(),
                                input: call.input.clone(),
                                child: None,
                            },
                        );
                    }
                    if calls.iter().any(|c| c.name != DELEGATE) {
                        // Sandboxed intent: effects may land from here on
                        // (§5.5 — intent precedes effect).
                        self.sandbox_activity = true;
                    }
                    Arc::make_mut(&mut self.transcript).push(Entry::Assistant {
                        content: content.clone(),
                        calls: calls.clone(),
                    });
                }
            }
            RecordBody::ToolOutcome {
                turn,
                call,
                outcome,
            } => {
                if let Some(live) = self.live.as_mut().filter(|l| &l.turn == turn) {
                    live.pending.remove(call);
                    Arc::make_mut(&mut self.transcript).push(Entry::ToolResult {
                        call: call.clone(),
                        outcome: outcome.clone(),
                    });
                }
            }
            RecordBody::ChildRun {
                turn,
                call,
                child_kind,
                child_session,
                child_turn,
                budget,
            } => {
                if let Some(live) = self.live.as_mut().filter(|l| &l.turn == turn) {
                    live.spend.carved_tokens += budget.tokens;
                    live.spend.carved_steps += budget.steps;
                    if let Some(pending) = live.pending.get_mut(call) {
                        pending.child = Some(ChildRef {
                            kind: child_kind.clone(),
                            session: child_session.clone(),
                            turn: child_turn.clone(),
                            budget: *budget,
                        });
                    }
                }
            }
            RecordBody::WorkspaceReset => {
                self.sandbox_activity = false;
                Arc::make_mut(&mut self.transcript).push(Entry::WorkspaceReset);
            }
            RecordBody::TierAcquired { .. } => {
                // Held tiers are working state, scoped to the activation that
                // journaled them (§5.6 item 3): the fold records nothing, so
                // replay is harmless and nothing resurrects a tier across an
                // activation boundary. The next activation restarts at
                // `Workspace` and re-acquires under new records (§5.5). No
                // transcript entry: acquisitions are audit, not something the
                // model sees.
            }
            RecordBody::RunEnded { turn, outcome } => {
                if self.live.as_ref().is_some_and(|l| &l.turn == turn) {
                    self.live = None;
                }
                if let Some(facts) = self.turns.get_mut(turn) {
                    facts.outcome = Some(outcome.clone());
                }
            }
        }
    }

    /// Fold a sequence of records — `state = fold(records)` (§6.3). The runtime
    /// folds via granary's per-event `apply` on the commit/replay path; this
    /// whole-prefix helper exists for tests and reconstruction (§10.5).
    pub fn fold(records: &[Record]) -> SessionState {
        let mut state = SessionState::default();
        for record in records {
            state.apply(record);
        }
        state
    }
}

/// Derive a delegation's child identifiers (harness spec §8.1):
/// deterministic in the parent session, the parent's turn, and the
/// delegation's `CallId` — one run may delegate many times, so the call, not
/// the turn, is the unit of derivation. A re-executed delegation re-derives
/// the same pair, which is what lets the child's journaled `TurnId` dedup the
/// re-submission into an attach (§7.4).
pub fn derive_child(parent: &SessionId, turn: &TurnId, call: &CallId) -> (SessionId, TurnId) {
    (
        SessionId::new(format!("{parent}/{turn}/{call}")),
        TurnId::new(format!("{turn}/{call}")),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(body: RecordBody) -> Record {
        Record { at_nanos: 7, body }
    }

    #[test]
    fn fold_tracks_a_run_through_its_step() {
        let mut state = SessionState::default();
        state.apply(&rec(RecordBody::TurnSubmitted {
            turn: TurnId::new("t1"),
            content: "go".into(),
            budget: Budget::new(100, 5),
        }));
        let call = CallId::new("c1");
        state.apply(&rec(RecordBody::ModelResponse {
            turn: TurnId::new("t1"),
            content: "using a tool".into(),
            calls: vec![ToolCall {
                id: call.clone(),
                name: "shell".into(),
                input: Value::Null,
            }],
            usage: Usage {
                input_tokens: 10,
                output_tokens: 5,
            },
        }));
        let live = state.live.as_ref().expect("run live");
        assert_eq!(live.spend.own_tokens, 15);
        assert_eq!(live.pending.len(), 1);
        assert!(state.sandbox_activity);

        state.apply(&rec(RecordBody::ToolOutcome {
            turn: TurnId::new("t1"),
            call,
            outcome: Ok(Value::String("done".into())),
        }));
        assert!(state.live.as_ref().expect("still live").pending.is_empty());

        state.apply(&rec(RecordBody::RunEnded {
            turn: TurnId::new("t1"),
            outcome: Ok(Completion::new("answer", 15)),
        }));
        assert!(state.live.is_none());
        assert!(state.turns[&TurnId::new("t1")].outcome.is_some());
        // Four records committed; the grain serves them by Seq for `Tail`.
        assert_eq!(state.records.len(), 4);
    }

    #[test]
    fn child_derivation_is_deterministic_per_call() {
        let a = derive_child(&SessionId::new("s"), &TurnId::new("t"), &CallId::new("c1"));
        let b = derive_child(&SessionId::new("s"), &TurnId::new("t"), &CallId::new("c1"));
        let c = derive_child(&SessionId::new("s"), &TurnId::new("t"), &CallId::new("c2"));
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
