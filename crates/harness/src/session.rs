//! Session identity, records, and the fold (harness spec §2, §6.3).
//!
//! A session *is* a grain (§2.1): its [`SessionId`] is the key half of a
//! `GrainName` (`(KindId, SessionId)`, §2.2), each [`Record`] is the grain's
//! `Event`, and [`SessionState`] is the grain's `State` — the pure,
//! deterministic fold of the journal that granary rehydrates on activation
//! (invariant H1). Anything not journaled is lost on deactivation.
//!
//! Identity is layered (§2.2): `SessionId` (= the `GrainName` key: durable,
//! application-chosen) → `ActorId` (one activation, system-assigned) → `TurnId`
//! (one run). The `KindId` is the session's grain *type* (granary hosts one
//! `Agent` grain under each kind's name via `granary_named`); granary owns
//! name→shard→leader resolution.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;

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
/// overrides the kind's default for the run it triggers (§9.1) and joins the
/// re-submission equality check as the literal option (§7.4): `None` is not
/// `Some(default)`.
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
    tokens: u64,
}

impl Completion {
    pub fn new(content: impl Into<String>, tokens: u64) -> Completion {
        Completion {
            content: content.into(),
            tokens,
        }
    }

    /// The final assistant message.
    pub fn text(&self) -> &str {
        &self.content
    }

    /// The run's journaled token spend — own usage plus carve-outs (§9.1).
    pub fn tokens(&self) -> u64 {
        self.tokens
    }
}

/// A run's abnormal terminal outcome (harness spec §3.1) — an application
/// error living **inside the reply**, distinct from transport/durability
/// `GrainError`. Exactly these three: "a tool misbehaved" is not a run failure
/// (§5.4), and durability failure is not a run outcome at all — it is the
/// grain's outer `GrainError::Unavailable`, which *pauses* the run rather than
/// ending it (§3.1, §6.4).
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
/// links.
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
    /// A turn was accepted into the session's queue (§7.3): the committing
    /// append is what releases the `Submit` ack, so an acked turn is durable
    /// before any caller learns of it. `budget` is the run's effective budget
    /// (the turn's explicit one, else the kind's default — resolved here so
    /// replay never depends on the kind's current default, H1); `explicit`
    /// records which, so the fold can rebuild the literal `Option<Budget>` the
    /// caller sent for the §7.4 equality check (`None` ≠ `Some(default)`).
    TurnSubmitted {
        turn: TurnId,
        content: String,
        budget: Budget,
        explicit: bool,
    },
    /// The queue's head turn left the queue and its run began (§3.1): the
    /// committing append is what makes `RunStarted` fire exactly once per turn
    /// (§10.4). Journaled only by the dispatcher — the one place a run starts.
    TurnStarted { turn: TurnId },
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
    /// The environment the transcript asserts is gone (§5.5): journaled before
    /// the next model call after a mid-activation `EnvironmentLost`, and
    /// surfaced to the model with that request.
    ///
    /// The workspace's durable subtree is the agent's own facet (granary §7.11)
    /// and always survives — a routine reactivation never resets. What this
    /// record narrows to is the activation's working state: regenerable excluded
    /// trees (`target/`, `node_modules/`), running processes, and held tiers.
    WorkspaceReset,
    /// The activation's first call at `tier` is about to execute (§5.6): the
    /// write-ahead discipline (§6.4) applied to capability acquisition, intent
    /// journaled before effect. A record, not a §10.4 event: it is the audit
    /// trail, verified by journal audit (sandbox spec S4).
    TierAcquired { turn: TurnId, tier: Tier },
    /// The turn's exactly-one terminal outcome (§3.1, invariant H3). For a
    /// turn cancelled while still queued (§9.2) it is the only record after
    /// acceptance: the turn ends without a run ever starting.
    RunEnded { turn: TurnId, outcome: RunOutcome },
    /// A cancelled run's propagated `Cancel` reached the child of `(turn,
    /// call)` (§9.2): journaled when the child's ack releases, clearing the
    /// owed fact the `RunEnded { Cancelled }` fold captured. Until this record
    /// commits, every activation re-owes the propagation — that is what makes
    /// H5 survive a crash between the terminal commit and the send.
    CancelDelivered { turn: TurnId, call: CallId },
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
/// fresh `Arc` on decode. Lets the transcript be `Arc`-shared without enabling
/// serde's workspace-wide `rc` feature. A `SessionState` holds no second
/// reference to its transcript `Arc`, so there is no cross-`Arc` sharing for a
/// snapshot round-trip to lose: a fresh `Arc` per decode is exact.
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

/// FNV-1a 64 over a string. Used by the turn-equality dedup (§7.4, via
/// [`turn_digest`]) and by [`Kind::digest`](crate::Kind::digest), which **is**
/// journaled and compared cluster-wide. Because of that second use the
/// algorithm must stay stable across versions — do not swap it for a different
/// hash without re-versioning kind digests. Fast, not collision-resistant:
/// both uses treat digest equality as content equality as a guard against
/// *accident* (a reused `TurnId`, a drifted kind), never as a security
/// boundary (§7.4, §10.5).
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

/// The §7.4 turn-equality digest: the content plus the **literal** budget field
/// (`None` ≠ `Some(default)`, so whether a re-submission conflicts never depends
/// on the kind's *current* default). Length-prefixed canonical form like
/// [`Kind::digest`](crate::Kind::digest), so no two distinct turns collide by
/// juxtaposition. Fold-local: recomputed from the journaled `TurnSubmitted` on
/// every replay, never journaled itself.
pub fn turn_digest(content: &str, budget: Option<Budget>) -> u64 {
    let mut canon = String::new();
    let mut frame = |field: &str| {
        canon.push_str(&field.len().to_string());
        canon.push(':');
        canon.push_str(field);
    };
    frame(content);
    match budget {
        None => frame("none"),
        Some(budget) => {
            frame("some");
            frame(&budget.tokens.to_string());
            frame(&budget.steps.to_string());
        }
    }
    content_digest(&canon)
}

/// What the fold knows about a turn (harness spec §7.4): enough to dedup a
/// re-submission and return the recorded outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TurnFacts {
    /// [`turn_digest`] of the submitted content and literal budget.
    pub content_digest: u64,
    /// `None` while the turn is queued or its run is unfinished.
    pub outcome: Option<RunOutcome>,
}

/// One accepted turn awaiting its run (harness spec §7.3): journal-derived
/// (`TurnSubmitted` enqueues, `TurnStarted` dequeues), so an acked turn
/// survives crash and migration and the next activation's dispatcher starts it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueuedTurn {
    pub turn: TurnId,
    pub content: String,
    /// The run's effective budget, resolved at acceptance.
    pub budget: Budget,
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
    /// Accepted turns not yet started (§7.3), in acceptance order: the fold's
    /// projection of `TurnSubmitted` minus `TurnStarted` (and minus a queued
    /// turn cancelled before starting, §9.2). Fold state, so an acked turn is
    /// as durable as its record.
    pub queue: VecDeque<QueuedTurn>,
    /// At most one unfinished run; the journal's total order serializes runs.
    pub live: Option<LiveRun>,
    /// Children still owed a propagated `Cancel` (§9.2), by the delegating
    /// `(turn, call)`: captured by the fold when a `RunEnded { Cancelled }`
    /// clears a run whose `ChildRun` intents lack outcomes, cleared per child
    /// by `CancelDelivered`. Journal-derived, so a crash between the terminal
    /// commit and the propagating send loses nothing — the next activation's
    /// `advance` finds the debt here and re-sends (H5).
    pub cancels_owed: BTreeMap<TurnId, BTreeMap<CallId, ChildRef>>,
    /// Whether the journal records sandboxed activity since the last
    /// `WorkspaceReset` — the §5.5 trigger for journaling the next one.
    pub sandbox_activity: bool,
}

impl SessionState {
    /// Fold one committed record — the grain's `apply` (granary §4.1), pure and
    /// deterministic, run on the live commit path and on every replay. Total: a
    /// record that fits no transition (a malformed journal) is ignored rather
    /// than panicking. The fold keeps projections only — the raw records stay
    /// in the grain's journal, which serves `tail` directly (§10.2).
    pub fn apply(&mut self, record: &Record) {
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
                explicit,
            } => {
                self.turns.insert(
                    turn.clone(),
                    TurnFacts {
                        content_digest: turn_digest(content, explicit.then_some(*budget)),
                        outcome: None,
                    },
                );
                self.queue.push_back(QueuedTurn {
                    turn: turn.clone(),
                    content: content.clone(),
                    budget: *budget,
                });
            }
            RecordBody::TurnStarted { turn } => {
                // Start the named queued turn — the dispatcher only ever starts
                // the head while no run is live, so anything else is a
                // malformed journal, ignored (the fold is total).
                if self.live.is_none()
                    && let Some(i) = self.queue.iter().position(|q| &q.turn == turn)
                {
                    let queued = self.queue.remove(i).expect("position found");
                    self.live = Some(LiveRun {
                        turn: queued.turn,
                        budget: queued.budget,
                        spend: Spend::default(),
                        pending: BTreeMap::new(),
                    });
                    Arc::make_mut(&mut self.transcript).push(Entry::User(queued.content));
                }
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
                // nothing resurrects a tier across an activation boundary. The
                // next activation restarts at `Workspace` and re-acquires under
                // new records (§5.5). No transcript entry: acquisitions are
                // audit, not something the model sees.
            }
            RecordBody::RunEnded { turn, outcome } => {
                if self.live.as_ref().is_some_and(|l| &l.turn == turn) {
                    let live = self.live.take().expect("checked live");
                    // A cancel is the one outcome that can end a run over
                    // unresolved calls (every other terminal commits at a step
                    // boundary, §3.1), so its recorded children become owed
                    // propagations (§9.2): the durable debt `advance` drains.
                    if outcome == &Err(RunError::Cancelled) {
                        let owed: BTreeMap<CallId, ChildRef> = live
                            .pending
                            .into_iter()
                            .filter_map(|(call, p)| Some((call, p.child?)))
                            .collect();
                        if !owed.is_empty() {
                            self.cancels_owed.insert(turn.clone(), owed);
                        }
                    }
                }
                // A turn cancelled while still queued (§9.2) ends without ever
                // starting: it leaves the queue with no run, no transcript
                // entry, and no event pair — only the recorded outcome below.
                self.queue.retain(|q| &q.turn != turn);
                if let Some(facts) = self.turns.get_mut(turn) {
                    facts.outcome = Some(outcome.clone());
                }
            }
            RecordBody::CancelDelivered { turn, call } => {
                if let Some(owed) = self.cancels_owed.get_mut(turn) {
                    owed.remove(call);
                    if owed.is_empty() {
                        self.cancels_owed.remove(turn);
                    }
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
///
/// Each component is length-prefixed (like [`Kind::digest`](crate::Kind::digest)'s
/// canonical form), so distinct triples always derive distinct ids: a bare
/// `/`-join would read `("s", "t/c", "d")` and `("s/t", "c", "d")` as the same
/// child session, and ids are application-chosen, so the collision is
/// constructible.
pub fn derive_child(parent: &SessionId, turn: &TurnId, call: &CallId) -> (SessionId, TurnId) {
    fn join(parts: &[&str]) -> String {
        let mut out = String::new();
        for (i, part) in parts.iter().enumerate() {
            if i > 0 {
                out.push('/');
            }
            out.push_str(&part.len().to_string());
            out.push(':');
            out.push_str(part);
        }
        out
    }
    (
        SessionId::new(join(&[parent.as_str(), turn.as_str(), call.as_str()])),
        TurnId::new(join(&[turn.as_str(), call.as_str()])),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(body: RecordBody) -> Record {
        Record { at_nanos: 7, body }
    }

    /// Accept and start a turn: the two-record shape every run begins with
    /// (§7.3) — `TurnSubmitted` enqueues, `TurnStarted` dequeues to live.
    fn start_turn(state: &mut SessionState, turn: &str, content: &str, budget: Budget) {
        state.apply(&rec(RecordBody::TurnSubmitted {
            turn: TurnId::new(turn),
            content: content.into(),
            budget,
            explicit: false,
        }));
        state.apply(&rec(RecordBody::TurnStarted {
            turn: TurnId::new(turn),
        }));
    }

    #[test]
    fn fold_tracks_a_run_through_its_step() {
        let mut state = SessionState::default();
        start_turn(&mut state, "t1", "go", Budget::new(100, 5));
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
    }

    #[test]
    fn a_cancelled_run_folds_its_unresolved_children_into_owed_cancels() {
        // The crash-window prefix (§9.2): a delegation intent committed, then
        // `RunEnded { Cancelled }` — and nothing after. The fold alone must
        // carry the propagation debt (H5).
        let turn = TurnId::new("t1");
        let call = CallId::new("d1");
        let mut state = SessionState::default();
        start_turn(&mut state, "t1", "go", Budget::new(1_000, 5));
        state.apply(&rec(RecordBody::ModelResponse {
            turn: turn.clone(),
            content: "delegating".into(),
            calls: vec![ToolCall {
                id: call.clone(),
                name: DELEGATE.into(),
                input: Value::Null,
            }],
            usage: Usage {
                input_tokens: 10,
                output_tokens: 5,
            },
        }));
        state.apply(&rec(RecordBody::ChildRun {
            turn: turn.clone(),
            call: call.clone(),
            child_kind: KindId::new("child"),
            child_session: SessionId::new("s/t1/d1"),
            child_turn: TurnId::new("t1/d1"),
            budget: Budget::new(100, 2),
        }));
        state.apply(&rec(RecordBody::RunEnded {
            turn: turn.clone(),
            outcome: Err(RunError::Cancelled),
        }));
        assert!(state.live.is_none());
        let child = &state.cancels_owed[&turn][&call];
        assert_eq!(child.session, SessionId::new("s/t1/d1"));
        assert_eq!(child.turn, TurnId::new("t1/d1"));

        // `CancelDelivered` clears the debt, and only then.
        state.apply(&rec(RecordBody::CancelDelivered {
            turn: turn.clone(),
            call,
        }));
        assert!(state.cancels_owed.is_empty());
    }

    #[test]
    fn only_a_cancelled_outcome_owes_propagation() {
        // A resolved delegation (its `ToolOutcome` journaled) owes nothing,
        // and a non-cancelled terminal never captures.
        let turn = TurnId::new("t1");
        let call = CallId::new("d1");
        let mut base = SessionState::default();
        start_turn(&mut base, "t1", "go", Budget::new(1_000, 5));
        base.apply(&rec(RecordBody::ModelResponse {
            turn: turn.clone(),
            content: "delegating".into(),
            calls: vec![ToolCall {
                id: call.clone(),
                name: DELEGATE.into(),
                input: Value::Null,
            }],
            usage: Usage {
                input_tokens: 10,
                output_tokens: 5,
            },
        }));
        base.apply(&rec(RecordBody::ChildRun {
            turn: turn.clone(),
            call: call.clone(),
            child_kind: KindId::new("child"),
            child_session: SessionId::new("s/t1/d1"),
            child_turn: TurnId::new("t1/d1"),
            budget: Budget::new(100, 2),
        }));

        let mut resolved = base.clone();
        resolved.apply(&rec(RecordBody::ToolOutcome {
            turn: turn.clone(),
            call: call.clone(),
            outcome: Ok(Value::String("child-answer".into())),
        }));
        resolved.apply(&rec(RecordBody::RunEnded {
            turn: turn.clone(),
            outcome: Err(RunError::Cancelled),
        }));
        assert!(resolved.cancels_owed.is_empty(), "resolved child not owed");

        let mut exhausted = base;
        exhausted.apply(&rec(RecordBody::RunEnded {
            turn,
            outcome: Err(RunError::BudgetExhausted),
        }));
        assert!(
            exhausted.cancels_owed.is_empty(),
            "only a cancel owes propagation (§9.2)"
        );
    }

    #[test]
    fn a_turn_submitted_behind_a_live_run_queues_until_started() {
        let mut state = SessionState::default();
        start_turn(&mut state, "t1", "first", Budget::new(100, 5));
        state.apply(&rec(RecordBody::TurnSubmitted {
            turn: TurnId::new("t2"),
            content: "second".into(),
            budget: Budget::new(50, 2),
            explicit: true,
        }));
        // Accepted: dedup facts exist, but no run, no transcript entry.
        assert!(state.turns.contains_key(&TurnId::new("t2")));
        assert_eq!(state.queue.len(), 1);
        assert_eq!(
            state.live.as_ref().expect("t1 live").turn,
            TurnId::new("t1")
        );
        assert_eq!(state.transcript.len(), 1, "queued content is not visible");

        state.apply(&rec(RecordBody::RunEnded {
            turn: TurnId::new("t1"),
            outcome: Ok(Completion::new("done", 10)),
        }));
        assert!(
            state.live.is_none(),
            "the fold does not auto-start the head"
        );
        state.apply(&rec(RecordBody::TurnStarted {
            turn: TurnId::new("t2"),
        }));
        assert!(state.queue.is_empty());
        let live = state.live.as_ref().expect("t2 live");
        assert_eq!(live.turn, TurnId::new("t2"));
        assert_eq!(live.budget, Budget::new(50, 2));
        assert_eq!(state.transcript.last(), Some(&Entry::User("second".into())));
    }

    #[test]
    fn a_run_ended_on_a_queued_turn_removes_it_without_starting() {
        // Cancel-of-queued (§9.2): the terminal record alone clears the queue
        // entry and records the outcome — no live run, no transcript entry.
        let mut state = SessionState::default();
        start_turn(&mut state, "t1", "first", Budget::new(100, 5));
        state.apply(&rec(RecordBody::TurnSubmitted {
            turn: TurnId::new("t2"),
            content: "second".into(),
            budget: Budget::new(50, 2),
            explicit: false,
        }));
        state.apply(&rec(RecordBody::RunEnded {
            turn: TurnId::new("t2"),
            outcome: Err(RunError::Cancelled),
        }));
        assert!(state.queue.is_empty());
        assert_eq!(
            state.live.as_ref().expect("t1 unaffected").turn,
            TurnId::new("t1")
        );
        assert_eq!(
            state.turns[&TurnId::new("t2")].outcome,
            Some(Err(RunError::Cancelled))
        );
        assert_eq!(state.transcript.len(), 1);
    }

    #[test]
    fn the_turn_digest_covers_the_literal_budget() {
        // §7.4: the budget joins the equality check as the literal option —
        // `None` is not `Some(default)`, and framing prevents content/budget
        // juxtaposition collisions.
        let d = |content: &str, budget: Option<Budget>| turn_digest(content, budget);
        assert_eq!(d("go", None), d("go", None));
        assert_ne!(d("go", None), d("go", Some(Budget::new(100_000, 25))));
        assert_ne!(
            d("go", Some(Budget::new(1, 2))),
            d("go", Some(Budget::new(2, 1)))
        );
        assert_ne!(d("gonone", None), d("go", None), "framed, not concatenated");
    }

    #[test]
    fn child_derivation_is_deterministic_per_call() {
        let a = derive_child(&SessionId::new("s"), &TurnId::new("t"), &CallId::new("c1"));
        let b = derive_child(&SessionId::new("s"), &TurnId::new("t"), &CallId::new("c1"));
        let c = derive_child(&SessionId::new("s"), &TurnId::new("t"), &CallId::new("c2"));
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn child_derivation_is_unambiguous_across_separator_collisions() {
        // A bare '/'-join would derive the same child session for both triples;
        // the length prefixes keep every component boundary explicit.
        let a = derive_child(&SessionId::new("s"), &TurnId::new("t/c"), &CallId::new("d"));
        let b = derive_child(&SessionId::new("s/t"), &TurnId::new("c"), &CallId::new("d"));
        assert_ne!(a.0, b.0);
        assert_ne!(a.1, b.1);
    }
}
