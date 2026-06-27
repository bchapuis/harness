//! The agent: an autonomous grain (harness spec §3).
//!
//! A session *is* a grain ([`granary`]). What the harness adds is **autonomy**:
//! a plain grain waits for the next command, whereas an agent's activation drives
//! itself forward — model→tools→model — until a run reaches a terminal outcome.
//! The grain model is decide/apply: [`Grain::apply`] folds records into
//! [`SessionState`], and each [`GrainHandler`] is a *pure decision* returning the
//! records to journal plus a reply (granary §4.2). The loop rides three granary
//! seams the spec leans on, none of which the harness re-implements:
//!
//! - **The output gate** (granary §6): a handler's records commit before its
//!   reply releases, so "intent before effect" (§6.4) is free — a handler that
//!   journals an intent never launches the effect itself.
//! - **Self-messaging** ([`GrainCtx::this`]) + **`ctx.system().launch`**: the
//!   loop launches each external operation (a model call, a sandboxed tool, a
//!   child submit) as a background task that delivers its outcome back as an
//!   ordinary command ([`ModelDone`]/[`ToolDone`]), and nudges itself forward
//!   with [`Advance`] once an intent has committed. The activation therefore
//!   never awaits I/O inside a handler (§3.2).
//! - **`on_activate`/`on_passivate`/`can_passivate`** (granary §10): resume
//!   set-up, sandbox release on every deactivation (H8), and the veto that keeps
//!   a live run from hibernating (§7.2).
//!
//! Everything else — the journal, the single-writer fence, placement,
//! activation, hibernation, lossless failover — is the grain's, consumed
//! unchanged (§2.1). Ephemeral activation state (the sandbox handle, held tiers,
//! the subscriber set, in-flight flags) lives behind a [`Mutex`] on the grain
//! behavior, rebuilt per activation and never journaled (§5.5, §5.6, §7.4).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::ActorRef;
use actor_core::Backoff;
use actor_core::BoxError;
use actor_core::Manifest;
use actor_core::Message;
use futures::channel::oneshot;
use futures::future::Either;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainError;
use granary::GrainHandler;
use granary::GrainRegistry;
use granary::Seq;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::client::HarnessSystem;
use crate::client::ReplyMailbox;
use crate::client::Seams;
use crate::client::Shared;
use crate::event::HarnessEvent;
use crate::kind::Kind;
use crate::model::ModelError;
use crate::model::ModelRequest;
use crate::model::ModelResponse;
use crate::model::ToolSpec;
use crate::sandbox::Sandbox;
use crate::sandbox::Tier;
use crate::session::CallId;
use crate::session::ChildRef;
use crate::session::Completion;
use crate::session::KindId;
use crate::session::Lineage;
use crate::session::LiveRun;
use crate::session::PendingCall;
use crate::session::Record;
use crate::session::RecordBody;
use crate::session::RunError;
use crate::session::RunOutcome;
use crate::session::SessionId;
use crate::session::SessionState;
use crate::session::Turn;
use crate::session::TurnId;
use crate::session::content_digest;
use crate::session::derive_child;
use crate::session::outcome_label;
use crate::tool::DELEGATE;
use crate::tool::DelegateInput;
use crate::tool::OnDangling;
use crate::tool::ToolError;

/// How many times an unreachable peer is retried before a delegation/cancel
/// gives up (§8.1, §9.2): past it the child surfaces as a tool failure (§5.4)
/// rather than looping forever — the child's budget is the ultimate backstop
/// (§9.2 item 3). A transport bound, backed off between tries.
const TRANSPORT_RETRIES: u32 = 32;
/// How many times a delegating parent re-attaches to a reachable-but-slow child
/// before giving up (§8.1). Distinct from [`TRANSPORT_RETRIES`]: this bounds the
/// *wait* on a child that accepts but has not finished, so it does not back off.
const CHILD_WAIT_ATTEMPTS: u32 = 32;
/// Backoff cap for the transport-retry loop.
const PROPAGATION_BACKOFF_CAP: Duration = Duration::from_secs(2);

// ===========================================================================
// Wire contract (§7.3): commands addressed to the grain, and the outbound
// `RunCompleted` notification. A session is addressed as a grain — its `kind`
// is the grain type and its `SessionId` the key — so the `GrainName` carries
// both and the commands omit them (granary §4.3).
// ===========================================================================

/// Submit a turn (§7.3). `kind` rides every call though it binds only on the
/// first (creation is implicit in the first turn); after that it is a checked
/// redundancy against the grain's own type, rejected on mismatch (§7.4).
/// `reply_to` is **transient routing, not part of the turn**: it is not
/// journaled and is excluded from the content-equality check (§7.4).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Submit<S: HarnessSystem> {
    pub kind: KindId,
    pub turn: Turn,
    pub parent: Option<Lineage>,
    pub reply_to: Option<ActorRef<ReplyMailbox<S>>>,
}

impl<S: HarnessSystem> Message for Submit<S> {
    type Reply = Result<Accepted, SubmitReject>;
    const MANIFEST: Manifest = Manifest::new("harness.Submit");
}

/// The `Submit` ack, released by the output gate the moment the `TurnSubmitted`
/// record commits (§7.3): an ack, not the run's outcome — the outcome travels
/// later as a [`RunCompleted`] notification to the `reply_to`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Accepted {
    pub turn: TurnId,
    pub status: SubmitStatus,
}

/// What a `Submit` did (§7.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmitStatus {
    /// A fresh run was started (its turn newly journaled or queued).
    Started,
    /// The `TurnId` named a live run; the `reply_to` was registered on it (H7).
    Attached,
    /// The `TurnId` named an ended run; its recorded outcome is delivered to the
    /// `reply_to` immediately (H7).
    Ended,
}

/// Why a `Submit` was rejected (§7.4): a deterministic caller-contract
/// violation, kept distinct from a transport `GrainError`. Both cases are
/// **permanent** — the same submit always rejects the same way — so a caller
/// must never retry one (unlike a transport failure, which a peer move could
/// clear). Naming them lets the resume/delegation loops tell the two apart.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmitReject {
    /// The submitted `kind` is not this grain's type: the address and the
    /// payload disagree (§7.4).
    KindMismatch {
        addressed: KindId,
        submitted: KindId,
    },
    /// The `TurnId` was already used for different content (§7.4): turn ids are
    /// the dedup key, so reusing one for new content is a bug, never a retry.
    ContentConflict { turn: TurnId },
}

impl std::fmt::Display for SubmitReject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubmitReject::KindMismatch {
                addressed,
                submitted,
            } => write!(
                f,
                "kind mismatch: addressed grain type '{addressed}', submitted as '{submitted}'"
            ),
            SubmitReject::ContentConflict { turn } => {
                write!(f, "turn {turn} re-submitted with different content")
            }
        }
    }
}

/// Cancel the run `turn` names (§7.3, §9.2). Idempotent.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cancel {
    pub turn: TurnId,
}

impl Message for Cancel {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("harness.Cancel");
}

/// Read committed records (§7.3, §10.2): at most `limit` records after `from`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tail {
    pub from: Seq,
    pub limit: u32,
}

impl Message for Tail {
    type Reply = Vec<(Seq, Record)>;
    const MANIFEST: Manifest = Manifest::new("harness.Tail");
}

/// The outbound notification carrying a run's outcome to a registered `reply_to`
/// (§7.3): a `tell` delivered when `RunEnded` commits. Delivery, not the source
/// of truth (the `RunEnded` record is); a lost one is recovered by re-contact.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunCompleted {
    pub session: SessionId,
    pub turn: TurnId,
    pub outcome: RunOutcome,
}

impl Message for RunCompleted {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("harness.RunCompleted");
}

// ===========================================================================
// Internal self-driving commands (§3.2): local-only self-tells, never in the
// network allowlist (`register`), so no peer can inject them. They carry the
// outcomes of launched work back onto the grain's serial command path.
// ===========================================================================

/// "What does the fold call for next?" — the post-commit continuation and
/// effect launcher. Idempotent: every path is guarded by a fold fact or an
/// in-flight flag, so a resume is the same code path as ordinary progress (H1).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Advance;

impl Message for Advance {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("harness.agent.Advance");
}

/// One model call finished (§3.1 step 2): its outcome, back on the loop.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelDone {
    pub turn: TurnId,
    pub result: Result<ModelResponse, ModelError>,
}

impl Message for ModelDone {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("harness.agent.ModelDone");
}

/// One tool call or delegation finished (§5.4, §8.1): its outcome, back on the
/// loop, to be journaled and shown to the model.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolDone {
    pub turn: TurnId,
    pub call: CallId,
    pub outcome: Result<Value, ToolError>,
}

impl Message for ToolDone {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("harness.agent.ToolDone");
}

// ===========================================================================
// The grain
// ===========================================================================

/// The activation's sandbox binding (§5.3): at most one live sandbox per
/// activation (invariant H8). The opened handle is not serializable, so it is
/// filled by the open task directly under the [`Mutex`] (the one place a task
/// touches activation state off the command path) rather than carried in a
/// message; the task then nudges [`Advance`].
#[derive(Default)]
enum SandboxSlot {
    #[default]
    Closed,
    Opening,
    Open(Arc<dyn Sandbox>),
    /// The last open failed (§5.4): the next `Advance` fails the calls that
    /// needed it and resets to `Closed`, so a later call retries the open.
    Failed(String),
}

/// The sandbox binding and tier grants this activation holds (§5.4, §5.6):
/// fold-external working state, rebuilt per activation and released on
/// deactivation (H8).
#[derive(Default)]
struct SandboxState {
    /// The sandbox handle's lifecycle slot.
    slot: SandboxSlot,
    /// `SandboxBound` was emitted and not yet paired (H8).
    bound: bool,
    /// Tiers this activation holds (§5.6): working state, never fold state.
    /// Opening grants `Workspace` and nothing else (§5.6 item 1).
    tiers_held: BTreeSet<Tier>,
    /// Whether this activation has reconciled with the journal's sandbox
    /// activity (§5.5): false until a `WorkspaceReset` is journaled (or none is
    /// needed); flipped false again on environment loss.
    reconciled: bool,
}

/// The progress flags of the one live run (§3.1, §5.5): cleared between runs by
/// [`reset`](RunScope::reset), since runs are serialized one at a time.
#[derive(Default)]
struct RunScope {
    /// Calls this activation has launched: a fold-external flag, so `Advance`
    /// does not relaunch a call whose outcome has not yet committed.
    launched: BTreeSet<CallId>,
    /// Calls whose outcome is journaled or synthesized: dedups a straggler
    /// (a timeout racing the real outcome, §9.2 item 4).
    resolved: BTreeSet<CallId>,
    model_inflight: bool,
    /// Whether dangling calls have been resolved for the resumed run (§5.5).
    dangling_resolved: bool,
}

impl RunScope {
    /// Clear every flag for the next run (§3.1): a new turn starts blank.
    fn reset(&mut self) {
        self.launched.clear();
        self.resolved.clear();
        self.model_inflight = false;
        self.dangling_resolved = false;
    }
}

/// Run-boundary events this activation owes once their records commit (§10.4):
/// each list is drained in `emit_run_events`. Filled only on the committing
/// activation, so a resume re-emits none of them.
#[derive(Default)]
struct EventOutbox {
    /// Turns whose `TurnSubmitted` just committed, awaiting `RunStarted`.
    started: Vec<TurnId>,
    /// Turns whose `RunEnded` just committed, awaiting `RunEnded` (§10.4, H3).
    ended: Vec<TurnId>,
    /// Model calls whose `ModelResponse` just committed, awaiting `ModelCompleted`
    /// — scoped to journaled spend, so an uncommitted response emits nothing and
    /// a resume re-counts nothing (§10.4, §9.1.4): `(turn, tokens)`.
    model: Vec<(TurnId, u64)>,
    /// Tool calls whose `ToolOutcome` just committed, awaiting `ToolCompleted`
    /// (§10.4). Like `model`, populated only where a record is produced — a
    /// discarded straggler emits nothing.
    tools: Vec<TurnId>,
}

/// Ephemeral, per-activation working state (§5.5, §5.6, §7.4): never journaled,
/// rebuilt on every activation, lost on deactivation. Held behind a [`Mutex`] so
/// the `&self` handlers (granary's decide functions) can mutate it.
struct Activation<S: HarnessSystem> {
    /// A self-reference for the launched tasks to deliver outcomes back (§3.2).
    this: Option<granary::GrainRef<Agent<S>>>,
    /// The sandbox binding and held tiers (§5.4, §5.6).
    env: SandboxState,
    /// The live run's progress flags (§3.1, §5.5).
    run: RunScope,
    /// Run-boundary events awaiting their records' commit (§10.4).
    events: EventOutbox,
    /// Turns accepted but not yet journaled — they start when the journal shows
    /// no live run (§3.1): the runs are serialized, one at a time.
    queue: VecDeque<(Turn, Option<Lineage>)>,
    /// Callers registered for a run's outcome (§7.4): notified on `RunEnded`,
    /// rebuilt by callers re-contacting after a resume.
    subscribers: BTreeMap<TurnId, Vec<ActorRef<ReplyMailbox<S>>>>,
    /// Children to cancel once a `RunEnded { Cancelled }` commits (§9.2):
    /// captured before the fold clears the live run.
    cancel_children: Vec<ChildRef>,
}

// Hand-written rather than `#[derive(Default)]`: deriving would impose a
// spurious `S: Default` bound on the type parameter, which never needs it.
impl<S: HarnessSystem> Default for Activation<S> {
    fn default() -> Self {
        Activation {
            this: None,
            env: SandboxState::default(),
            run: RunScope::default(),
            events: EventOutbox::default(),
            queue: VecDeque::new(),
            subscribers: BTreeMap::new(),
            cancel_children: Vec::new(),
        }
    }
}

/// The agent grain (harness spec §3): one `Agent` Rust type hosted under each
/// kind's name (`granary_named`, §2.2), so a `KindId` is a grain type. The
/// behavior is built fresh per activation by the harness's factory, which
/// injects the node's seams via [`Shared`]; the folded [`SessionState`] is the
/// grain's `State`, rebuilt from the journal by granary.
pub struct Agent<S: HarnessSystem> {
    shared: Arc<Shared<S>>,
    /// This grain's kind definition (§7.1). Captured by the factory, which hosts
    /// one grain type per kind (§2.2), so an activation of grain-type `K` always
    /// has `K`'s definition — kind resolution is infallible, no lookup needed.
    kind: Arc<Kind>,
    /// The model and sandbox seams for this hosted kind (§4, §5.3), captured by
    /// the factory. An `Agent` only ever activates on a host, so the seams are
    /// always present — no `Option`, no host-only runtime check.
    seams: Seams,
    act: Arc<Mutex<Activation<S>>>,
    _marker: PhantomData<S>,
}

impl<S: HarnessSystem> Agent<S> {
    /// Build a fresh activation's behavior, injecting this node's seams and this
    /// grain type's kind (§7.4). Called by the harness factory passed to
    /// `granary_named`.
    pub(crate) fn new(shared: Arc<Shared<S>>, kind: Arc<Kind>, seams: Seams) -> Agent<S> {
        Agent {
            shared,
            kind,
            seams,
            act: Arc::new(Mutex::new(Activation::default())),
            _marker: PhantomData,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Activation<S>> {
        self.act.lock().expect("agent activation mutex poisoned")
    }

    /// This grain's kind definition (§7.1): infallible by construction.
    fn kind(&self) -> &Kind {
        &self.kind
    }

    fn session(ctx: &GrainCtx<Agent<S>>) -> SessionId {
        SessionId::new(ctx.name().key())
    }

    /// Stamp a record body with this node's clock reading (§10.1).
    fn rec(&self, ctx: &GrainCtx<Agent<S>>, body: RecordBody) -> Record {
        Record {
            at_nanos: ctx.system().now().as_nanos(),
            body,
        }
    }

    /// Resolve `call` with a failure outcome as one step: mark it resolved so it
    /// never relaunches, and return the `ToolOutcome` record to journal. The two
    /// must always happen together — a resolved call with no journaled outcome
    /// would dangle forever; a journaled failure not marked resolved would
    /// re-execute. Callers push the returned record onto the round's batch.
    fn fail_call(
        &self,
        act: &mut Activation<S>,
        ctx: &GrainCtx<Agent<S>>,
        turn: &TurnId,
        call: &CallId,
        error: ToolError,
    ) -> Record {
        act.run.resolved.insert(call.clone());
        self.rec(
            ctx,
            RecordBody::ToolOutcome {
                turn: turn.clone(),
                call: call.clone(),
                outcome: Err(error),
            },
        )
    }

    /// Launch a self-`tell` of [`Advance`] (§3.2): runs after the current
    /// handler's records commit, so launching effects from it honors the
    /// write-ahead discipline (§6.4).
    fn schedule_advance(&self, ctx: &GrainCtx<Agent<S>>) {
        let this = ctx.this();
        ctx.system().launch(Box::pin(async move {
            let _ = this.tell(Advance).await;
        }));
    }

    // -- handler bodies ------------------------------------------------------

    fn on_submit(
        &self,
        state: &SessionState,
        msg: Submit<S>,
        ctx: &GrainCtx<Agent<S>>,
    ) -> (Vec<Record>, Result<Accepted, SubmitReject>) {
        let my_kind = KindId::new(ctx.name().grain_type());
        if msg.kind != my_kind {
            return (
                Vec::new(),
                Err(SubmitReject::KindMismatch {
                    addressed: my_kind,
                    submitted: msg.kind,
                }),
            );
        }
        let turn_id = msg.turn.id.clone();
        let digest = content_digest(&msg.turn.content);
        let mut act = self.lock();

        // Dedup against the journal (§7.4): the recorded outcome, or an attach to
        // the live run — never a second run (H7).
        if let Some(facts) = state.turns.get(&turn_id) {
            if facts.content_digest != digest {
                return (
                    Vec::new(),
                    Err(SubmitReject::ContentConflict { turn: turn_id }),
                );
            }
            return match &facts.outcome {
                Some(outcome) => {
                    // Ended: deliver the recorded outcome to the fresh reply-to.
                    if let Some(reply_to) = msg.reply_to {
                        self.notify_one(ctx, reply_to, &turn_id, outcome.clone());
                    }
                    (
                        Vec::new(),
                        Ok(Accepted {
                            turn: turn_id,
                            status: SubmitStatus::Ended,
                        }),
                    )
                }
                None => {
                    if let Some(reply_to) = msg.reply_to {
                        act.subscribers
                            .entry(turn_id.clone())
                            .or_default()
                            .push(reply_to);
                    }
                    // A live run with a fresh attachment may be resuming on this
                    // activation: nudge it forward.
                    drop(act);
                    self.schedule_advance(ctx);
                    (
                        Vec::new(),
                        Ok(Accepted {
                            turn: turn_id,
                            status: SubmitStatus::Attached,
                        }),
                    )
                }
            };
        }

        // Dedup against a queued-but-unstarted turn (ephemeral, §7.4).
        if act.queue.iter().any(|(t, _)| t.id == turn_id) {
            if let Some(reply_to) = msg.reply_to {
                act.subscribers
                    .entry(turn_id.clone())
                    .or_default()
                    .push(reply_to);
            }
            return (
                Vec::new(),
                Ok(Accepted {
                    turn: turn_id,
                    status: SubmitStatus::Attached,
                }),
            );
        }

        // A new turn.
        if let Some(reply_to) = msg.reply_to {
            act.subscribers
                .entry(turn_id.clone())
                .or_default()
                .push(reply_to);
        }
        if state.live.is_none() && act.queue.is_empty() {
            // Start now: journal SessionCreated (if first) + TurnSubmitted.
            act.events.started.push(turn_id.clone());
            drop(act);
            let batch = self.start_records(state, ctx, self.kind(), msg.turn, msg.parent);
            self.schedule_advance(ctx);
            (
                batch,
                Ok(Accepted {
                    turn: turn_id,
                    status: SubmitStatus::Started,
                }),
            )
        } else {
            // Serialize behind the live/queued run (§3.1).
            act.queue.push_back((msg.turn, msg.parent));
            (
                Vec::new(),
                Ok(Accepted {
                    turn: turn_id,
                    status: SubmitStatus::Started,
                }),
            )
        }
    }

    /// The records that start a turn: `SessionCreated` on the very first turn,
    /// then `TurnSubmitted` (§3.1, §7.1).
    fn start_records(
        &self,
        state: &SessionState,
        ctx: &GrainCtx<Agent<S>>,
        kind: &Kind,
        turn: Turn,
        parent: Option<Lineage>,
    ) -> Vec<Record> {
        let mut batch = Vec::new();
        if state.created.is_none() {
            let session = Self::session(ctx);
            let root = parent.as_ref().map(|p| p.root.clone()).unwrap_or(session);
            batch.push(self.rec(
                ctx,
                RecordBody::SessionCreated {
                    kind: KindId::new(ctx.name().grain_type()),
                    digest: kind.digest(),
                    parent,
                    root,
                },
            ));
        }
        let budget = turn.budget.unwrap_or(kind.default_budget);
        batch.push(self.rec(
            ctx,
            RecordBody::TurnSubmitted {
                turn: turn.id,
                content: turn.content,
                budget,
            },
        ));
        batch
    }

    fn on_cancel(
        &self,
        state: &SessionState,
        turn: TurnId,
        ctx: &GrainCtx<Agent<S>>,
    ) -> Vec<Record> {
        let mut act = self.lock();
        // A cancel naming the live run ends it; one naming an ended, queued, or
        // unknown run is an idempotent no-op (§9.2 item 1).
        if state.live.as_ref().is_some_and(|l| l.turn == turn) {
            // Capture the live children before the fold clears the run, so the
            // committed cancel propagates to them (§9.2 item 2).
            if let Some(live) = &state.live {
                act.cancel_children = live
                    .pending
                    .values()
                    .filter_map(|p| p.child.clone())
                    .collect();
            }
            act.events.ended.push(turn.clone());
            drop(act);
            self.schedule_advance(ctx);
            vec![self.rec(
                ctx,
                RecordBody::RunEnded {
                    turn,
                    outcome: Err(RunError::Cancelled),
                },
            )]
        } else if let Some(i) = act.queue.iter().position(|(t, _)| t.id == turn) {
            // Not yet journaled: drop it and notify any attached caller.
            act.queue.remove(i);
            let subs = act.subscribers.remove(&turn).unwrap_or_default();
            drop(act);
            for sub in subs {
                self.notify_one(ctx, sub, &turn, Err(RunError::Cancelled));
            }
            Vec::new()
        } else {
            Vec::new()
        }
    }

    fn on_tail(&self, state: &SessionState, msg: Tail) -> Vec<(Seq, Record)> {
        // The grain owns the journal, so the activation mirrors committed records
        // (§10.2). A read served from the activation, read-your-leader, never a
        // write (§7.3). Record `i` carries `Seq(i + 1)`, so every record with
        // `Seq <= from` sits at index `< from` — skip that prefix instead of
        // scanning it.
        let from = msg.from.value() as usize;
        state
            .records
            .iter()
            .enumerate()
            .skip(from)
            .map(|(i, r)| (SessionState::seq_of(i), r.clone()))
            .take(msg.limit as usize)
            .collect()
    }

    fn on_model_done(
        &self,
        state: &SessionState,
        turn: TurnId,
        result: Result<ModelResponse, ModelError>,
        ctx: &GrainCtx<Agent<S>>,
    ) -> Vec<Record> {
        let mut act = self.lock();
        act.run.model_inflight = false;
        let Some(live) = state.live.as_ref().filter(|l| l.turn == turn) else {
            // The run ended while the call was in flight (a cancel, §9.2):
            // discard, do not journal, emit nothing (§3.2, §10.4).
            return Vec::new();
        };
        match result {
            Ok(mut response) => {
                // Assign ids the provider omitted (§5.2): deterministic in the
                // step and position.
                let step = live.spend.own_steps + 1;
                for (i, call) in response.calls.iter_mut().enumerate() {
                    if call.id.as_str().is_empty() {
                        call.id = CallId::new(format!("call-{step}-{i}"));
                    }
                }
                // `ModelCompleted` is emitted after the response commits (§10.4),
                // so it counts only journaled spend (§9.1.4).
                act.events
                    .model
                    .push((turn.clone(), response.usage.total()));
                // No requested tool calls ⇒ this assistant message ends the run
                // (§3.1 step 3). The termination rule is the loop's, not a
                // property of the model's reply.
                let is_final = response.calls.is_empty();
                if is_final {
                    act.events.ended.push(turn.clone());
                }
                drop(act);
                self.schedule_advance(ctx);
                if is_final {
                    // Final message and terminal outcome in one atomic batch
                    // (§6.4): no journal prefix ends between them.
                    let tokens = live.spend.tokens() + response.usage.total();
                    let completion = Completion::new(response.content.clone(), tokens);
                    vec![
                        self.rec(
                            ctx,
                            RecordBody::ModelResponse {
                                turn: turn.clone(),
                                content: response.content,
                                calls: Vec::new(),
                                usage: response.usage,
                            },
                        ),
                        self.rec(
                            ctx,
                            RecordBody::RunEnded {
                                turn,
                                outcome: Ok(completion),
                            },
                        ),
                    ]
                } else {
                    vec![self.rec(
                        ctx,
                        RecordBody::ModelResponse {
                            turn,
                            content: response.content,
                            calls: response.calls,
                            usage: response.usage,
                        },
                    )]
                }
            }
            Err(error) => {
                // A failure no retry policy absorbed ends the run (§4.3).
                act.events.ended.push(turn.clone());
                drop(act);
                self.schedule_advance(ctx);
                vec![self.rec(
                    ctx,
                    RecordBody::RunEnded {
                        turn,
                        outcome: Err(RunError::Model(error)),
                    },
                )]
            }
        }
    }

    fn on_tool_done(
        &self,
        state: &SessionState,
        turn: TurnId,
        call: CallId,
        outcome: Result<Value, ToolError>,
        ctx: &GrainCtx<Agent<S>>,
    ) -> Vec<Record> {
        let mut act = self.lock();
        let live_matches = state
            .live
            .as_ref()
            .is_some_and(|l| l.turn == turn && l.pending.contains_key(&call));
        if !live_matches || act.run.resolved.contains(&call) {
            // A straggler of an ended or cancelled run, or a duplicate
            // (a timeout racing the real outcome): discard (§3.2, §9.2 item 4).
            return Vec::new();
        }
        act.run.resolved.insert(call.clone());
        if let Err(ToolError::EnvironmentLost(_)) = &outcome {
            // The provider lost the workspace (§5.5): drop the binding, release
            // it, and require a `WorkspaceReset` before the next model call.
            self.release_sandbox(&mut act, ctx);
            act.env.reconciled = false;
        }
        // The outcome record is produced below, so its `ToolCompleted` is owed
        // once it commits (drained by `emit_run_events`, §10.4).
        act.events.tools.push(turn.clone());
        drop(act);
        self.schedule_advance(ctx);
        vec![self.rec(
            ctx,
            RecordBody::ToolOutcome {
                turn,
                call,
                outcome,
            },
        )]
    }

    // -- the dispatcher (§3.1) ----------------------------------------------

    /// What the fold calls for next, run after every committed batch. Reads the
    /// folded state and launches the next effect, journaling intents first
    /// (§6.4). Returns the records to journal this round (empty when it only
    /// launches effects); the handler schedules a re-advance iff it returns records.
    fn advance(&self, state: &SessionState, ctx: &GrainCtx<Agent<S>>) -> Vec<Record> {
        let kind = self.kind();
        let session = Self::session(ctx);
        let mut act = self.lock();

        // (a) Emit the run-boundary events this activation owes (§10.4), notify
        //     callers of any finished run, and propagate a committed cancel to
        //     the captured children (§9.2).
        self.emit_run_events(&mut act, state, ctx, &session);
        self.notify_finished(&mut act, state, ctx, &session);
        self.propagate_cancels(&mut act, ctx, kind);

        // (b) No live run: reset per-run flags and start the next queued turn.
        let Some(live) = state.live.as_ref() else {
            return self.start_next_turn(state, ctx, kind, &mut act);
        };
        let turn = live.turn.clone();

        // (c) Resume: resolve dangling calls once (§5.5).
        if let Some(events) = self.resolve_dangling(ctx, kind, &mut act, live, &turn) {
            return events;
        }

        // (d) Step boundary: no pending calls ⇒ a model call (§3.1 step 2).
        if live.pending.is_empty() {
            return self.step_model(state, ctx, kind, &mut act, live, turn);
        }

        // (e) Execute every journaled intent not yet launched or resolved
        //     (§3.1 step 4; on resume this is dangling re-execution, §5.5).
        self.dispatch_pending(state, ctx, kind, &mut act, live, &turn)
    }

    /// (b) No live run: clear the per-run flags, then start the next queued turn
    /// if one is waiting (§3.1). Returns the records to journal — empty when the
    /// queue is empty and the grain goes idle.
    fn start_next_turn(
        &self,
        state: &SessionState,
        ctx: &GrainCtx<Agent<S>>,
        kind: &Kind,
        act: &mut Activation<S>,
    ) -> Vec<Record> {
        act.run.reset();
        if let Some((turn, parent)) = act.queue.pop_front() {
            act.events.started.push(turn.id.clone());
            return self.start_records(state, ctx, kind, turn, parent);
        }
        Vec::new()
    }

    /// (c) Resume: on the first advance of a live run the pending set is exactly
    /// the dangling calls; resolve each whose policy is `Interrupt` (§5.5).
    /// `Some(events)` ⇒ commit them and stop; `None` ⇒ fall through.
    fn resolve_dangling(
        &self,
        ctx: &GrainCtx<Agent<S>>,
        kind: &Kind,
        act: &mut Activation<S>,
        live: &LiveRun,
        turn: &TurnId,
    ) -> Option<Vec<Record>> {
        if act.run.dangling_resolved {
            return None;
        }
        act.run.dangling_resolved = true;
        let mut events = Vec::new();
        for (call, pending) in &live.pending {
            if self.dangling_policy(pending, kind) == OnDangling::Interrupt {
                events.push(self.fail_call(act, ctx, turn, call, ToolError::Interrupted));
            }
        }
        (!events.is_empty()).then_some(events)
    }

    /// (d) Step boundary: no calls pending, so issue the next model call (§3.1
    /// step 2) — unless the budget is exhausted (a terminal `RunEnded`) or a
    /// lost workspace must be surfaced first (§5.5). Returns the records to
    /// journal; launches the call and returns empty when it proceeds.
    fn step_model(
        &self,
        state: &SessionState,
        ctx: &GrainCtx<Agent<S>>,
        kind: &Kind,
        act: &mut Activation<S>,
        live: &LiveRun,
        turn: TurnId,
    ) -> Vec<Record> {
        if act.run.model_inflight {
            return Vec::new();
        }
        let floor = self.shared.config.budget_floor;
        if !live.spend.allows_call(&live.budget, floor) {
            act.events.ended.push(turn.clone());
            return vec![self.rec(
                ctx,
                RecordBody::RunEnded {
                    turn,
                    outcome: Err(RunError::BudgetExhausted),
                },
            )];
        }
        // Surface a lost workspace before the model acts on state that is gone
        // (§5.5). A fresh environment resets every held tier to `Workspace`;
        // later acquisitions re-journal, never silently inherited.
        if !act.env.reconciled && state.sandbox_activity {
            act.env.reconciled = true;
            act.env.tiers_held = BTreeSet::from([Tier::Workspace]);
            return vec![self.rec(ctx, RecordBody::WorkspaceReset)];
        }
        act.env.reconciled = true;
        act.run.model_inflight = true;
        let request = self.build_request(state, kind, live);
        let this = act.this.clone().expect("self-ref set in on_activate");
        let model = self.seams.model.clone();
        ctx.system().launch(Box::pin(async move {
            let result = model.complete(request).await;
            let _ = this.tell(ModelDone { turn, result }).await;
        }));
        Vec::new()
    }

    /// (e) Execute every journaled intent not yet launched or resolved (§3.1
    /// step 4; on resume, dangling re-execution, §5.5). Synthesized failures and
    /// acquisition/delegation intents are journaled first and re-advanced; once
    /// none remain, every ready call launches. Returns the records to journal.
    fn dispatch_pending(
        &self,
        state: &SessionState,
        ctx: &GrainCtx<Agent<S>>,
        kind: &Kind,
        act: &mut Activation<S>,
        live: &LiveRun,
        turn: &TurnId,
    ) -> Vec<Record> {
        // A failed sandbox open fails the calls that needed it (§5.4), then
        // resets to `Closed` so a later call retries the open.
        if matches!(act.env.slot, SandboxSlot::Failed(_)) {
            let SandboxSlot::Failed(error) =
                std::mem::replace(&mut act.env.slot, SandboxSlot::Closed)
            else {
                unreachable!("guarded by matches! above")
            };
            let mut events = Vec::new();
            for (call, pending) in &live.pending {
                if act.run.launched.contains(call)
                    || act.run.resolved.contains(call)
                    || pending.name == DELEGATE
                {
                    continue;
                }
                events.push(self.fail_call(
                    act,
                    ctx,
                    turn,
                    call,
                    ToolError::Sandbox(error.clone()),
                ));
            }
            if !events.is_empty() {
                return events;
            }
        }

        // Collect intents (TierAcquired/ChildRun) and synthesized failures to
        // journal this round; if any, commit them first and re-advance. Else
        // launch every ready call.
        let mut events = Vec::new();
        let mut ready: Vec<(CallId, PendingCall)> = Vec::new();
        for (call, pending) in &live.pending {
            if act.run.launched.contains(call) || act.run.resolved.contains(call) {
                continue;
            }
            if pending.name == DELEGATE {
                if pending.child.is_some() {
                    ready.push((call.clone(), pending.clone()));
                } else {
                    match self.plan_delegation(state, ctx, kind, turn, call, pending, live) {
                        Ok(child_run) => events.push(child_run),
                        Err(tool_err) => {
                            events.push(self.fail_call(act, ctx, turn, call, tool_err));
                        }
                    }
                }
            } else {
                match kind.tools.get(&pending.name) {
                    None => {
                        events.push(self.fail_call(
                            act,
                            ctx,
                            turn,
                            call,
                            ToolError::UnknownTool {
                                name: pending.name.clone(),
                            },
                        ));
                    }
                    Some(_) if !pending.input.is_object() => {
                        events.push(self.fail_call(
                            act,
                            ctx,
                            turn,
                            call,
                            ToolError::InvalidArguments("tool input must be a JSON object".into()),
                        ));
                    }
                    Some(decl) if act.env.tiers_held.contains(&decl.tier) => {
                        ready.push((call.clone(), pending.clone()));
                    }
                    Some(decl) => {
                        // The acquisition gate (§5.6): journal `TierAcquired` before
                        // the first call at a tier not yet held, and mark it held
                        // optimistically so neither a same-batch sibling nor the
                        // post-commit re-advance re-journals it. A failed commit
                        // forces step-down, which resets held tiers to `Workspace`
                        // on the next activation (§5.5), so the optimism is safe.
                        if act.env.tiers_held.insert(decl.tier) {
                            events.push(self.rec(
                                ctx,
                                RecordBody::TierAcquired {
                                    turn: turn.clone(),
                                    tier: decl.tier,
                                },
                            ));
                        }
                    }
                }
            }
        }
        if !events.is_empty() {
            return events;
        }
        let root = state
            .created
            .as_ref()
            .map(|c| c.root.clone())
            .unwrap_or_else(|| Self::session(ctx));
        for (call, pending) in ready {
            self.launch_call(act, ctx, kind, turn, &root, call, pending);
        }
        Vec::new()
    }

    /// The recovery policy for a dangling call (§5.5): `delegate` re-executes by
    /// construction (its re-submission dedups, §8.1); a sandboxed tool follows
    /// its declaration, defaulting to `Interrupt` if the declaration vanished.
    fn dangling_policy(&self, pending: &PendingCall, kind: &Kind) -> OnDangling {
        if pending.name == DELEGATE {
            OnDangling::Reexecute
        } else {
            kind.tools
                .get(&pending.name)
                .map(|d| d.on_dangling)
                .unwrap_or(OnDangling::Interrupt)
        }
    }

    /// Plan a fresh delegation (§8.1 step 1): validate the child kind against the
    /// allowlist and the registry, then build the `ChildRun` intent.
    #[allow(clippy::too_many_arguments)]
    fn plan_delegation(
        &self,
        _state: &SessionState,
        ctx: &GrainCtx<Agent<S>>,
        kind: &Kind,
        turn: &TurnId,
        call: &CallId,
        pending: &PendingCall,
        live: &crate::session::LiveRun,
    ) -> Result<Record, ToolError> {
        let request: DelegateInput = serde_json::from_value(pending.input.clone())
            .map_err(|e| ToolError::InvalidArguments(e.to_string()))?;
        let child_kind = KindId::new(request.kind.clone());
        if !kind.delegates.contains(&child_kind) {
            return Err(ToolError::InvalidArguments(format!(
                "kind '{child_kind}' is not in this kind's delegation allowlist"
            )));
        }
        let Some(child_def) = self.shared.kinds.get(&child_kind) else {
            return Err(ToolError::InvalidArguments(format!(
                "delegated kind '{child_kind}' is not registered"
            )));
        };
        let session = Self::session(ctx);
        let (child_session, child_turn) = derive_child(&session, turn, call);
        let requested = request.budget.unwrap_or(child_def.default_budget);
        let budget = live.spend.carve(&live.budget, requested);
        Ok(self.rec(
            ctx,
            RecordBody::ChildRun {
                turn: turn.clone(),
                call: call.clone(),
                child_kind,
                child_session,
                child_turn,
                budget,
            },
        ))
    }

    /// Launch one ready call (§3.1): a sandboxed tool, or a delegation. The
    /// `delegate` exception executes in the loop (§5.2, §8).
    #[allow(clippy::too_many_arguments)]
    fn launch_call(
        &self,
        act: &mut Activation<S>,
        ctx: &GrainCtx<Agent<S>>,
        kind: &Kind,
        turn: &TurnId,
        root: &SessionId,
        call: CallId,
        pending: PendingCall,
    ) {
        if pending.name == DELEGATE {
            act.run.launched.insert(call.clone());
            let Some(child) = pending.child.clone() else {
                return;
            };
            let Ok(request) = serde_json::from_value::<DelegateInput>(pending.input.clone()) else {
                return;
            };
            let lineage = Lineage {
                session: Self::session(ctx),
                turn: turn.clone(),
                root: root.clone(),
            };
            self.launch_delegation(act, ctx, turn.clone(), call, request, child, lineage);
            return;
        }

        let Some(decl) = kind.tools.get(&pending.name) else {
            return;
        };
        let tier = decl.tier;
        let timeout = decl.timeout.unwrap_or(self.shared.config.tool_timeout);
        match &act.env.slot {
            SandboxSlot::Open(sandbox) => {
                let sandbox = Arc::clone(sandbox);
                act.run.launched.insert(call.clone());
                let this = act.this.clone().expect("self-ref set in on_activate");
                let system = ctx.system().clone();
                let name = pending.name.clone();
                let input = pending.input.clone();
                let turn = turn.clone();
                system.clone().launch(Box::pin(async move {
                    let call_fut = Box::pin(sandbox.call(tier, &name, input));
                    let sleep = system.sleep(timeout);
                    let outcome = match futures::future::select(call_fut, sleep).await {
                        Either::Left((outcome, _)) => outcome,
                        Either::Right(((), _)) => Err(ToolError::Timeout),
                    };
                    let _ = this
                        .tell(ToolDone {
                            turn,
                            call,
                            outcome,
                        })
                        .await;
                }));
            }
            // `Opening` waits for the open task to re-advance; `Failed` is resolved
            // by `advance` before any call reaches here, so it is never seen.
            SandboxSlot::Opening | SandboxSlot::Failed(_) => {}
            SandboxSlot::Closed => {
                // Lazy open on the first sandboxed call (§5.3 item 1); the call
                // stays pending and is re-driven when the environment is ready.
                act.env.slot = SandboxSlot::Opening;
                self.launch_open(ctx, kind.profile.clone());
            }
        }
    }

    /// Open the sandbox off the command path (§5.3). The opened handle is not
    /// serializable, so the task fills the slot under the mutex and nudges
    /// [`Advance`]; success emits `SandboxBound` (H8).
    fn launch_open(&self, ctx: &GrainCtx<Agent<S>>, profile: crate::sandbox::SandboxProfile) {
        let this = ctx.this();
        let system = ctx.system().clone();
        let provider = self.seams.sandbox.clone();
        let act = Arc::clone(&self.act);
        let session = Self::session(ctx);
        let node = ctx.system().node();
        system.clone().launch(Box::pin(async move {
            let result = provider.open(&session, &profile).await;
            let bound = {
                let mut a = act.lock().expect("agent activation mutex poisoned");
                match result {
                    Ok(sandbox) => {
                        a.env.slot = SandboxSlot::Open(sandbox);
                        a.env.bound = true;
                        true
                    }
                    Err(error) => {
                        a.env.slot = SandboxSlot::Failed(error.to_string());
                        false
                    }
                }
            };
            if bound {
                system.emit_app(HarnessEvent::SandboxBound { session, node }.into());
            }
            let _ = this.tell(Advance).await;
        }));
    }

    /// Launch the child submission of a journaled delegation (§8.1 steps 2-3): an
    /// ordinary grain submit with a reply-to mailbox, awaiting the child's
    /// `RunCompleted`, retried on transport failure (safe — the derived `TurnId`
    /// dedups, H7). The outcome becomes this call's tool outcome (§5.4).
    #[allow(clippy::too_many_arguments)]
    fn launch_delegation(
        &self,
        act: &mut Activation<S>,
        ctx: &GrainCtx<Agent<S>>,
        turn: TurnId,
        call: CallId,
        request: DelegateInput,
        child: ChildRef,
        lineage: Lineage,
    ) {
        let this = act.this.clone().expect("self-ref set in on_activate");
        let system = ctx.system().clone();
        let within = self.shared.config.submit_deadline;
        let granary = self.shared.granaries().get(&child.kind).cloned();
        system.clone().launch(Box::pin(async move {
            let outcome = match granary {
                None => Err(ToolError::Delegation(format!(
                    "child kind '{}' is not hosted on this node",
                    child.kind
                ))),
                Some(granary) => {
                    let child_ref = granary.grain(child.session.as_str());
                    run_child(system, child_ref, &child, request.prompt, lineage, within).await
                }
            };
            let _ = this
                .tell(ToolDone {
                    turn,
                    call,
                    outcome,
                })
                .await;
        }));
    }

    // -- notifications & cancellation ----------------------------------------

    /// Emit the run-boundary events this activation owes, after their records
    /// committed (§10.4): `RunStarted` once per turn this activation started (a
    /// resume emits none — it attaches, never journals a `TurnSubmitted`), and
    /// `RunEnded` once per turn this activation ended (H3).
    fn emit_run_events(
        &self,
        act: &mut Activation<S>,
        state: &SessionState,
        ctx: &GrainCtx<Agent<S>>,
        session: &SessionId,
    ) {
        let node = ctx.system().node();
        for turn in std::mem::take(&mut act.events.started) {
            let parent = state
                .created
                .as_ref()
                .and_then(|c| c.parent.as_ref())
                .map(|l| l.session.clone());
            ctx.system().emit_app(
                HarnessEvent::RunStarted {
                    session: session.clone(),
                    turn,
                    parent,
                }
                .into(),
            );
        }
        for (turn, usage) in std::mem::take(&mut act.events.model) {
            ctx.system().emit_app(
                HarnessEvent::ModelCompleted {
                    session: session.clone(),
                    turn,
                    node,
                    usage,
                }
                .into(),
            );
        }
        for turn in std::mem::take(&mut act.events.tools) {
            ctx.system().emit_app(
                HarnessEvent::ToolCompleted {
                    session: session.clone(),
                    turn,
                    node,
                }
                .into(),
            );
        }
        for turn in std::mem::take(&mut act.events.ended) {
            if let Some(outcome) = state.turns.get(&turn).and_then(|f| f.outcome.as_ref()) {
                ctx.system().emit_app(
                    HarnessEvent::RunEnded {
                        session: session.clone(),
                        turn,
                        outcome: outcome_label(outcome),
                    }
                    .into(),
                );
            }
        }
    }

    /// Notify every caller registered for a run that has finished (§7.3), then
    /// drop their registrations.
    fn notify_finished(
        &self,
        act: &mut Activation<S>,
        state: &SessionState,
        ctx: &GrainCtx<Agent<S>>,
        session: &SessionId,
    ) {
        let mut finished = Vec::new();
        act.subscribers
            .retain(|turn, subs| match state.turns.get(turn) {
                Some(facts) if facts.outcome.is_some() => {
                    let outcome = facts.outcome.clone().expect("checked some");
                    finished.push((turn.clone(), std::mem::take(subs), outcome));
                    false
                }
                _ => true,
            });
        for (turn, subs, outcome) in finished {
            for sub in subs {
                self.deliver(
                    ctx,
                    sub,
                    RunCompleted {
                        session: session.clone(),
                        turn: turn.clone(),
                        outcome: outcome.clone(),
                    },
                );
            }
        }
    }

    /// Deliver one `RunCompleted` to one reply-to (§7.3): a fire-and-forget
    /// `tell`, launched off the command path.
    fn deliver(
        &self,
        ctx: &GrainCtx<Agent<S>>,
        reply_to: ActorRef<ReplyMailbox<S>>,
        msg: RunCompleted,
    ) {
        ctx.system().launch(Box::pin(async move {
            let _ = reply_to.tell(msg).await;
        }));
    }

    fn notify_one(
        &self,
        ctx: &GrainCtx<Agent<S>>,
        reply_to: ActorRef<ReplyMailbox<S>>,
        turn: &TurnId,
        outcome: RunOutcome,
    ) {
        self.deliver(
            ctx,
            reply_to,
            RunCompleted {
                session: Self::session(ctx),
                turn: turn.clone(),
                outcome,
            },
        );
    }

    /// Propagate a committed cancel to every recorded child (§9.2 item 2).
    fn propagate_cancels(&self, act: &mut Activation<S>, ctx: &GrainCtx<Agent<S>>, _kind: &Kind) {
        let children = std::mem::take(&mut act.cancel_children);
        for child in children {
            let Some(granary) = self.shared.granaries().get(&child.kind).cloned() else {
                continue;
            };
            let system = ctx.system().clone();
            let child_turn = child.turn.clone();
            let session = child.session.clone();
            system.clone().launch(Box::pin(async move {
                let child_ref = granary.grain(session.as_str());
                let mut attempt = 0;
                loop {
                    match child_ref
                        .ask(Cancel {
                            turn: child_turn.clone(),
                        })
                        .await
                    {
                        Ok(()) => return,
                        Err(_) if attempt < TRANSPORT_RETRIES => {
                            attempt += 1;
                            system.sleep(propagation_backoff(attempt)).await;
                        }
                        Err(_) => return,
                    }
                }
            }));
        }
    }

    // -- sandbox lifecycle ---------------------------------------------------

    /// Drop and release the bound sandbox, emitting `SandboxReleased` (H8).
    /// Synchronous teardown is launched off the command path.
    fn release_sandbox(&self, act: &mut Activation<S>, ctx: &GrainCtx<Agent<S>>) {
        if let SandboxSlot::Open(sandbox) =
            std::mem::replace(&mut act.env.slot, SandboxSlot::Closed)
        {
            let sandbox = sandbox.clone();
            ctx.system()
                .launch(Box::pin(async move { sandbox.release().await }));
        }
        if act.env.bound {
            act.env.bound = false;
            ctx.system().emit_app(
                HarnessEvent::SandboxReleased {
                    session: Self::session(ctx),
                    node: ctx.system().node(),
                }
                .into(),
            );
        }
    }

    /// Build the model request from the folded transcript (§4.1), clamping
    /// `max_tokens` to the remaining budget (§9.1 item 2).
    fn build_request(
        &self,
        state: &SessionState,
        kind: &Kind,
        live: &crate::session::LiveRun,
    ) -> ModelRequest {
        let max_tokens = kind
            .params
            .max_tokens
            .min(live.spend.remaining_tokens(&live.budget));
        let mut tools: Vec<ToolSpec> = kind.tools.specs();
        if !kind.delegates.is_empty() {
            tools.push(delegate_spec(kind));
        }
        ModelRequest {
            system_prompt: kind.system_prompt.clone(),
            params: kind.params.clone(),
            tools,
            transcript: Arc::clone(&state.transcript),
            max_tokens,
        }
    }
}

impl<S: HarnessSystem> Grain for Agent<S> {
    type System = S;
    type State = SessionState;
    type Event = Record;
    // The fallback type name; the harness hosts each kind under its own name via
    // `granary_named`, so a session's grain type is its `KindId` (§2.2).
    const GRAIN_TYPE: &'static str = "harness.Agent";

    fn apply(state: &mut SessionState, event: &Record) {
        state.apply(event);
    }

    fn register(registry: &mut GrainRegistry<Self>) {
        // The network allowlist: only the wire commands (§7.3). The internal
        // self-tells (Advance/ModelDone/ToolDone) are local-only, so no peer can
        // inject them.
        registry.accept::<Submit<S>>();
        registry.accept::<Cancel>();
        registry.accept::<Tail>();
    }

    async fn on_activate(&mut self, ctx: &GrainCtx<Self>) -> Result<(), BoxError> {
        let mut act = self.lock();
        act.this = Some(ctx.this());
        // Opening grants `Workspace` and nothing else (§5.6 item 1); every other
        // tier is re-acquired under a journaled record this activation.
        act.env.tiers_held = BTreeSet::from([Tier::Workspace]);
        act.env.reconciled = false;
        act.run.dangling_resolved = false;
        Ok(())
    }

    async fn on_passivate(&mut self, ctx: &GrainCtx<Self>) {
        // Release the sandbox on every deactivation — idle hibernation, migration,
        // or forced step-down (H8). The workspace goes with it (§5.5). Scope the
        // lock so the guard is dropped before the async release.
        let to_release = {
            let mut act = self.lock();
            match std::mem::replace(&mut act.env.slot, SandboxSlot::Closed) {
                SandboxSlot::Open(sandbox) => {
                    let bound = act.env.bound;
                    act.env.bound = false;
                    Some((sandbox, bound))
                }
                _ => None,
            }
        };
        if let Some((sandbox, bound)) = to_release {
            sandbox.release().await;
            if bound {
                ctx.system().emit_app(
                    HarnessEvent::SandboxReleased {
                        session: Self::session(ctx),
                        node: ctx.system().node(),
                    }
                    .into(),
                );
            }
        }
    }

    fn can_passivate(&self, state: &SessionState) -> bool {
        // Never hibernate a session whose run is live (§7.2): no live run, no
        // queued turn, and nothing in flight.
        if state.live.is_some() {
            return false;
        }
        let act = self.lock();
        act.queue.is_empty()
            && !act.run.model_inflight
            && act.run.launched.is_empty()
            && !matches!(act.env.slot, SandboxSlot::Opening)
    }
}

impl<S: HarnessSystem> GrainHandler<Submit<S>> for Agent<S> {
    async fn handle(
        &self,
        state: &SessionState,
        msg: Submit<S>,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Record>, Result<Accepted, SubmitReject>) {
        self.on_submit(state, msg, ctx)
    }
}

impl<S: HarnessSystem> GrainHandler<Cancel> for Agent<S> {
    async fn handle(
        &self,
        state: &SessionState,
        msg: Cancel,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Record>, ()) {
        (self.on_cancel(state, msg.turn, ctx), ())
    }
}

impl<S: HarnessSystem> GrainHandler<Tail> for Agent<S> {
    async fn handle(
        &self,
        state: &SessionState,
        msg: Tail,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<Record>, Vec<(Seq, Record)>) {
        // A read: no records, commits nothing (granary §7.5).
        (Vec::new(), self.on_tail(state, msg))
    }
}

impl<S: HarnessSystem> GrainHandler<Advance> for Agent<S> {
    async fn handle(
        &self,
        state: &SessionState,
        _msg: Advance,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Record>, ()) {
        let events = self.advance(state, ctx);
        if !events.is_empty() {
            self.schedule_advance(ctx);
        }
        (events, ())
    }
}

impl<S: HarnessSystem> GrainHandler<ModelDone> for Agent<S> {
    async fn handle(
        &self,
        state: &SessionState,
        msg: ModelDone,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Record>, ()) {
        (self.on_model_done(state, msg.turn, msg.result, ctx), ())
    }
}

impl<S: HarnessSystem> GrainHandler<ToolDone> for Agent<S> {
    async fn handle(
        &self,
        state: &SessionState,
        msg: ToolDone,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Record>, ()) {
        (
            self.on_tool_done(state, msg.turn, msg.call, msg.outcome, ctx),
            (),
        )
    }
}

// ===========================================================================
// Free helpers
// ===========================================================================

/// The outcome of one [`submit_and_attach`] attempt — the re-attach protocol's
/// single mechanism, leaving the *policy* (when to give up, when to retry) to
/// the caller's loop, since the two callers bound it differently (§7.4, §8.1).
pub(crate) enum AttachOutcome {
    /// The run finished and delivered its outcome.
    Completed(RunOutcome),
    /// The submit was accepted but the wait lapsed (or the mailbox dropped):
    /// re-submitting the same `TurnId` re-attaches (H7).
    Lapsed,
    /// The submit was permanently rejected (§7.4): never worth retrying.
    Rejected(SubmitReject),
    /// The grain was unreachable: a transport failure a retry might clear.
    Unreachable(GrainError),
}

/// One submit-and-await-outcome attempt against a grain (§7.4): spawn an
/// ephemeral reply mailbox, `Submit` the turn carrying it, and await the run's
/// `RunCompleted` bounded by `within`. This is the whole re-attach mechanism;
/// `prompt` and delegation each wrap it in their own retry policy (deadline- or
/// attempt-bounded), safe because re-submitting the same `TurnId` is idempotent.
pub(crate) async fn submit_and_attach<S: HarnessSystem>(
    system: &S,
    grain: &granary::GrainRef<Agent<S>>,
    kind: &KindId,
    turn: &Turn,
    parent: Option<&Lineage>,
    within: Duration,
) -> AttachOutcome {
    let (tx, rx) = oneshot::channel::<RunOutcome>();
    let mailbox = system.spawn(ReplyMailbox::new(tx));
    let submit = Submit {
        kind: kind.clone(),
        turn: turn.clone(),
        parent: parent.cloned(),
        reply_to: Some(mailbox),
    };
    match grain.ask_timeout(submit, within).await {
        Ok(Ok(_accepted)) => {
            let sleep = system.sleep(within);
            match futures::future::select(rx, sleep).await {
                Either::Left((Ok(outcome), _)) => AttachOutcome::Completed(outcome),
                Either::Left((Err(_), _)) | Either::Right(((), _)) => AttachOutcome::Lapsed,
            }
        }
        Ok(Err(reject)) => AttachOutcome::Rejected(reject),
        Err(call_error) => AttachOutcome::Unreachable(call_error),
    }
}

/// Submit to a child and await its outcome (§8.1 step 2), with the derived
/// `TurnId` keeping every re-attempt safe (H7). Bounds the wait on a slow child
/// by [`CHILD_WAIT_ATTEMPTS`] and retries an unreachable one up to
/// [`TRANSPORT_RETRIES`] with backoff. Maps the child's `RunOutcome` onto this
/// delegation's tool outcome (§5.4).
async fn run_child<S: HarnessSystem>(
    system: S,
    child_ref: granary::GrainRef<Agent<S>>,
    child: &ChildRef,
    prompt: String,
    parent: Lineage,
    within: Duration,
) -> Result<Value, ToolError> {
    let turn = Turn {
        id: child.turn.clone(),
        content: prompt,
        budget: Some(child.budget),
    };
    let mut waits = 0;
    let mut retries = 0;
    loop {
        match submit_and_attach(
            &system,
            &child_ref,
            &child.kind,
            &turn,
            Some(&parent),
            within,
        )
        .await
        {
            AttachOutcome::Completed(outcome) => {
                return match outcome {
                    Ok(completion) => Ok(Value::String(completion.text().to_string())),
                    Err(run_error) => Err(ToolError::Delegation(run_error.to_string())),
                };
            }
            AttachOutcome::Lapsed => {
                waits += 1;
                if waits >= CHILD_WAIT_ATTEMPTS {
                    return Err(ToolError::Delegation(
                        "child run did not complete in time".into(),
                    ));
                }
            }
            AttachOutcome::Rejected(reject) => {
                return Err(ToolError::Delegation(format!(
                    "child rejected the submit: {reject}"
                )));
            }
            AttachOutcome::Unreachable(call_error) => {
                retries += 1;
                if retries >= TRANSPORT_RETRIES {
                    return Err(ToolError::Delegation(format!(
                        "child unreachable: {call_error:?}"
                    )));
                }
                system.sleep(propagation_backoff(retries)).await;
            }
        }
    }
}

/// Exponential backoff for the delegation/cancel retry loops, capped — the
/// framework's own backoff (core §11.2), reused rather than re-derived.
fn propagation_backoff(attempt: u32) -> Duration {
    Backoff::Exponential {
        base: Duration::from_millis(200),
        max: PROPAGATION_BACKOFF_CAP,
    }
    .delay(attempt)
}

/// The interface of the built-in `delegate` tool (§8.1), synthesized so the
/// model can call it like any declared tool.
fn delegate_spec(kind: &Kind) -> ToolSpec {
    let kinds: Vec<&str> = kind.delegates.iter().map(|k| k.as_str()).collect();
    ToolSpec {
        name: DELEGATE.to_string(),
        description: format!(
            "Delegate a task to a sub-agent. Allowed kinds: {}.",
            kinds.join(", ")
        ),
        input_schema: DelegateInput::input_schema(&kinds),
    }
}
