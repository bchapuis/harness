//! The agent: an autonomous entity (agent spec §3).
//!
//! A session *is* an entity ([`entity`]). What the agent runtime adds is **autonomy**:
//! an agent's activation drives itself forward — model→tools→model — until a run
//! reaches a terminal outcome. The entity model is decide/apply: [`Entity::apply`]
//! folds records into [`SessionState`], and each [`EntityHandler`] is a *pure
//! decision* returning the records to journal plus a reply (entity §4.2). The
//! loop rides three entity seams, none of which the agent runtime re-implements:
//!
//! - **The output gate** (entity §6): a handler's records commit before its
//!   reply releases, so "intent before effect" (§6.4) is free — a handler that
//!   journals an intent never launches the effect itself.
//! - **Self-messaging** ([`EntityCtx::this`]) + **`ctx.system().launch`**: the
//!   loop launches each external operation (a model call, a sandboxed tool, a
//!   child submit) as a background task that delivers its outcome back as an
//!   ordinary command ([`ModelDone`]/[`ToolDone`]), and nudges itself forward
//!   with [`Advance`] once an intent has committed. The activation therefore
//!   never awaits I/O inside a handler (§3.2).
//! - **`on_activate`/`on_deactivate`/`can_hibernate`** (entity §10): resume
//!   set-up, sandbox release on every deactivation (A8), and the veto that keeps
//!   a live run from hibernating (§7.2).
//!
//! Everything else — the journal, the single-writer fence, placement,
//! activation, hibernation, lossless failover — is the entity's (§2.1). Ephemeral
//! activation state (the sandbox handle, held tiers, the subscriber set, the
//! launch-once guard — entity's steps facet's [`LaunchGuard`], entity §7.17) lives
//! behind a [`Mutex`] on the entity behavior, rebuilt per activation and never
//! journaled (§5.5, §5.6, §7.4).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
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
use futures::future::select;
use entity::Entity;
use entity::EntityCtx;
use entity::EntityError;
use entity::EntityHandler;
use entity::EntityRegistry;
use entity::LaunchGuard;
use entity::Ws;
use entity::WsError;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::client::HarnessSystem;
use crate::client::MailboxGuard;
use crate::client::ReplyMailbox;
use crate::client::Seams;
use crate::client::Shared;
use crate::event::AgentEvent;
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
use crate::session::derive_child;
use crate::session::outcome_label;
use crate::session::turn_digest;
use crate::tool::DELEGATE;
use crate::tool::DelegateInput;
use crate::tool::OnOrphan;
use crate::tool::ToolError;

/// How many *consecutive* times an unreachable peer is retried before a
/// delegation/cancel gives up (§8.1, §9.2): past it the child surfaces as a
/// tool failure (§5.4) rather than looping forever — the child's budget is the
/// ultimate backstop (§9.2 item 3). A transport bound, backed off between
/// tries; any successful contact resets it. Distinct from the wait on a
/// reachable-but-slow child, which is budget-scaled config
/// ([`HarnessConfig::child_wait`](crate::HarnessConfig::child_wait), §8.1).
const TRANSPORT_RETRIES: u32 = 32;
/// Backoff cap for the transport-retry loop.
const PROPAGATION_BACKOFF_CAP: Duration = Duration::from_secs(2);

// ===========================================================================
// Wire contract (§7.3): commands addressed to the entity, and the outbound
// `RunCompleted` notification. A session is addressed as an entity — its `kind`
// is the entity type and its `SessionId` the key — so the `EntityName` carries
// both and the commands omit them (entity §4.3).
// ===========================================================================

/// Submit a turn (§7.3). `kind` rides every call though it binds only on the
/// first (creation is implicit in the first turn); after that it is a checked
/// redundancy against the entity's own type, rejected on mismatch (§7.4).
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
    const MANIFEST: Manifest = Manifest::new("agent.Submit");
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
    /// A fresh turn was accepted and journaled; its run starts when it
    /// reaches the head of the session's queue (§7.3).
    Started,
    /// The `TurnId` named a queued or live turn; the `reply_to` was registered
    /// on it (A7).
    Attached,
    /// The `TurnId` named an ended run; its recorded outcome is delivered to the
    /// `reply_to` immediately (A7).
    Ended,
}

/// Why a `Submit` was rejected (§7.4): a deterministic caller-contract
/// violation, kept distinct from a transport `EntityError`. Both cases are
/// **permanent** — the same submit always rejects the same way — so a caller
/// must never retry one, unlike a transport failure, which a peer move could
/// clear.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmitReject {
    /// The submitted `kind` is not this entity's type: the address and the
    /// payload disagree (§7.4).
    KindMismatch {
        addressed: KindId,
        submitted: KindId,
    },
    /// The `TurnId` was already used for a different turn (§7.4): the equality
    /// check covers the content and the literal budget field (`None` ≠
    /// `Some(default)`), and turn ids are the dedup key, so reusing one for a
    /// changed turn is a bug, never a retry.
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
                "kind mismatch: addressed entity type '{addressed}', submitted as '{submitted}'"
            ),
            SubmitReject::ContentConflict { turn } => {
                write!(
                    f,
                    "turn {turn} re-submitted with different content or budget"
                )
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
    const MANIFEST: Manifest = Manifest::new("agent.Cancel");
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
    const MANIFEST: Manifest = Manifest::new("agent.RunCompleted");
}

// ===========================================================================
// Internal self-driving commands (§3.2): local-only self-tells, never in the
// network allowlist (`register`), so no peer can inject them. They carry the
// outcomes of launched work back onto the entity's serial command path.
// ===========================================================================

/// "What does the fold call for next?" — the post-commit continuation and
/// effect launcher. Idempotent: every path is guarded by a fold fact or an
/// in-flight flag, so a resume is the same code path as ordinary progress (A1).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Advance;

impl Message for Advance {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("agent.Advance");
}

/// One model call finished (§3.1 step 2): its outcome, back on the loop.
/// `step` is the run's `own_steps` when the call was issued — the step this
/// response answers. Receipt matches it against the live run's current
/// `own_steps`, so a superseded call (an old activation's straggler racing the
/// reissue after a same-node resume) can never journal a second response for a
/// step the journal has already answered (§3.1 step 2, §9.1 item 4).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelDone {
    pub turn: TurnId,
    pub step: u32,
    pub result: Result<ModelResponse, ModelError>,
}

impl Message for ModelDone {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("agent.ModelDone");
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
    const MANIFEST: Manifest = Manifest::new("agent.ToolDone");
}

/// One propagated `Cancel` was acked by the child of `(turn, call)` (§9.2):
/// back on the loop to journal the `CancelDelivered` record that clears the
/// owed fact — the ack itself is not durable, only the record is.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CancelDone {
    pub turn: TurnId,
    pub call: CallId,
}

impl Message for CancelDone {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("agent.CancelDone");
}

// ===========================================================================
// The entity
// ===========================================================================

/// The activation's sandbox binding (§5.3): at most one live sandbox per
/// activation (invariant A8). The opened handle is not serializable, so it is
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
/// deactivation (A8).
#[derive(Default)]
struct SandboxState {
    /// The sandbox handle's lifecycle slot.
    slot: SandboxSlot,
    /// `SandboxBound` was emitted and not yet paired (A8).
    bound: bool,
    /// Tiers this activation holds (§5.6): working state, never fold state.
    /// Opening grants `Workspace` and nothing else (§5.6 item 1).
    tiers_held: BTreeSet<Tier>,
    /// Whether this activation has reconciled with the journal's sandbox
    /// activity (§5.5): false until a `WorkspaceReset` is journaled (or none is
    /// needed); flipped false again on environment loss.
    reconciled: bool,
    /// Whether the workspace was lost *during* this activation — the provider
    /// returned `EnvironmentLost` (§5.4). The durable workspace survives
    /// reactivation, so only a loss observed here resets. Set on
    /// `EnvironmentLost`, cleared per activation in `on_activate`.
    lost_this_activation: bool,
}

/// One launched effect of the live run (§3.1): the claim key of the run's
/// [`LaunchGuard`]. A model call is claimed under the step it answers
/// (`own_steps` at issue, §3.1 step 2); a tool call or delegation under its
/// `CallId` (§3.1 step 4). Claims are never released: a committed response
/// advances `own_steps`, so the next model claim is a fresh key, and a committed
/// `ToolOutcome` removes the call from the pending set the dispatcher scans. A
/// straggler therefore has nothing to "unlock" into a duplicate launch (§9.1 item 4,
/// §9.2 item 4).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum StepKey {
    /// The model call answering step `own_steps` (§3.1 step 2).
    Model(u32),
    /// The tool call (or delegation) `CallId` names (§3.1 step 4).
    Call(CallId),
}

/// The progress state of the one live run (§3.1, §5.5): cleared between runs by
/// [`reset`](RunScope::reset), since runs are serialized one at a time.
#[derive(Default)]
struct RunScope {
    /// Effects this activation has launched: entity's steps-facet launch-once
    /// guard (entity §7.17), fold-external, so `Advance` does not relaunch an
    /// effect whose outcome has not yet committed. A resume starts a fresh guard
    /// and re-launches whatever the journal still shows unresolved (§5.5): the
    /// *outcome record*, not the launch, is the durable fact.
    launched: LaunchGuard<StepKey>,
    /// Calls whose outcome is journaled or synthesized: dedups a straggler
    /// (a timeout racing the real outcome, §9.2 item 4).
    resolved: BTreeSet<CallId>,
    /// Whether orphaned calls have been resolved for the resumed run (§5.5).
    orphans_resolved: bool,
}

impl RunScope {
    /// Clear every flag for the next run (§3.1): a new turn starts blank.
    fn reset(&mut self) {
        self.launched.reset();
        self.resolved.clear();
        self.orphans_resolved = false;
    }
}

/// Run-boundary events this activation owes once their records commit (§10.4):
/// each list is drained in `emit_run_events`. Filled only on the committing
/// activation, so a resume re-emits none of them.
#[derive(Default)]
struct EventOutbox {
    /// Turns whose `TurnSubmitted` just committed, awaiting `RunStarted`.
    started: Vec<TurnId>,
    /// Turns whose `RunEnded` just committed, awaiting `RunEnded` (§10.4, A3).
    ended: Vec<TurnId>,
    /// Model calls whose `ModelResponse` just committed, awaiting
    /// `ModelCompleted` (§10.4, §9.1 item 4): `(turn, tokens)`.
    model: Vec<(TurnId, u64)>,
    /// Tool calls whose `ToolOutcome` just committed, awaiting `ToolCompleted`
    /// (§10.4).
    tools: Vec<TurnId>,
}

/// Ephemeral, per-activation working state (§5.5, §5.6, §7.4): never journaled,
/// rebuilt on every activation, lost on deactivation. Held behind a [`Mutex`] so
/// the `&self` handlers (entity's decide functions) can mutate it.
struct Activation<S: HarnessSystem> {
    /// The sandbox binding and held tiers (§5.4, §5.6).
    env: SandboxState,
    /// The live run's progress flags (§3.1, §5.5).
    run: RunScope,
    /// Run-boundary events awaiting their records' commit (§10.4).
    events: EventOutbox,
    /// Callers registered for a run's outcome (§7.4): notified on `RunEnded`,
    /// rebuilt by callers re-contacting after a resume.
    subscribers: BTreeMap<TurnId, Vec<ActorRef<ReplyMailbox<S>>>>,
    /// Owed cancels whose propagation this activation already launched (§9.2):
    /// a launch-once guard over the fold's `cancels_owed`, so repeated
    /// `Advance`s do not stack duplicate sends. Per-activation, like every
    /// launch guard: a resume re-launches whatever is still owed. A send that
    /// exhausts its transport retries removes its own entry, so the next
    /// message that drives the session re-launches it without waiting for a
    /// fresh activation.
    cancels_launched: BTreeSet<(TurnId, CallId)>,
}

impl<S: HarnessSystem> Activation<S> {
    /// Register `reply_to` (if any) for `turn`'s outcome (§7.4). A no-op when the
    /// submit carried no mailbox — the one place a subscriber is added to the list.
    fn subscribe(&mut self, turn: &TurnId, reply_to: Option<ActorRef<ReplyMailbox<S>>>) {
        if let Some(reply_to) = reply_to {
            self.subscribers
                .entry(turn.clone())
                .or_default()
                .push(reply_to);
        }
    }
}

// Hand-written rather than `#[derive(Default)]`: deriving would impose a
// spurious `S: Default` bound on the type parameter, which never needs it.
impl<S: HarnessSystem> Default for Activation<S> {
    fn default() -> Self {
        Activation {
            env: SandboxState::default(),
            run: RunScope::default(),
            events: EventOutbox::default(),
            subscribers: BTreeMap::new(),
            cancels_launched: BTreeSet::new(),
        }
    }
}

/// The agent entity (agent spec §3): one `Agent` Rust type hosted under each
/// kind's name (`entities_named`, §2.2). The behavior is built fresh per
/// activation by the agent runtime's factory, which injects the node's seams via
/// [`Shared`]; the folded [`SessionState`] is the entity's `State`, rebuilt from
/// the journal by entity.
pub struct Agent<S: HarnessSystem> {
    shared: Arc<Shared<S>>,
    /// This entity's kind definition (§7.1). Captured by the factory, which hosts
    /// one entity type per kind (§2.2), so an activation of entity-type `K` always
    /// has `K`'s definition — kind resolution is infallible, no lookup needed.
    kind: Arc<Kind>,
    /// The model and sandbox seams for this hosted kind (§4, §5.3), captured by
    /// the factory.
    seams: Seams,
    act: Arc<Mutex<Activation<S>>>,
}

impl<S: HarnessSystem> Agent<S> {
    /// Build a fresh activation's behavior, injecting this node's seams and this
    /// entity type's kind (§7.4). Called by the agent runtime factory passed to
    /// `entities_named`.
    pub(crate) fn new(shared: Arc<Shared<S>>, kind: Arc<Kind>, seams: Seams) -> Agent<S> {
        Agent {
            shared,
            kind,
            seams,
            act: Arc::new(Mutex::new(Activation::default())),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Activation<S>> {
        self.act.lock().expect("agent activation mutex poisoned")
    }

    fn session(ctx: &EntityCtx<Agent<S>>) -> SessionId {
        SessionId::new(ctx.name().key())
    }

    /// Stamp a record body with this node's clock reading (§10.1).
    fn rec(&self, ctx: &EntityCtx<Agent<S>>, body: RecordBody) -> Record {
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
        ctx: &EntityCtx<Agent<S>>,
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

    /// End the live run as one step: owe the §10.4 `RunEnded` event and return
    /// the terminal record to journal. Paired like `fail_call` — the event
    /// without the record would announce a run the journal never ended; the
    /// record without the event breaks A3's start/end pairing. A turn cancelled
    /// while still queued does not come through here: no run started, so no
    /// event is owed.
    fn end_run(
        &self,
        act: &mut Activation<S>,
        ctx: &EntityCtx<Agent<S>>,
        turn: TurnId,
        outcome: RunOutcome,
    ) -> Record {
        act.events.ended.push(turn.clone());
        self.rec(ctx, RecordBody::RunEnded { turn, outcome })
    }

    /// Launch a self-`tell` of [`Advance`] (§3.2): runs after the current
    /// handler's records commit, so launching effects from it honors the
    /// write-ahead discipline (§6.4).
    fn schedule_advance(&self, ctx: &EntityCtx<Agent<S>>) {
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
        ctx: &EntityCtx<Agent<S>>,
    ) -> (Vec<Record>, Result<Accepted, SubmitReject>) {
        let my_kind = KindId::new(ctx.name().entity_type());
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
        let digest = turn_digest(&msg.turn.content, msg.turn.budget);
        let mut act = self.lock();

        // Dedup against the journal (§7.4): every accepted turn — queued, live,
        // or ended — has folded facts, so one check covers all three (A7). The
        // digest spans content and the literal budget, so a re-submission that
        // changes either is a conflict, never a silent attach.
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
                        self.notify(ctx, reply_to, &turn_id, outcome.clone());
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
                    act.subscribe(&turn_id, msg.reply_to);
                    // A queued or live turn re-contacted after a resume may be
                    // waiting on this very nudge (§7.5): drive the dispatcher.
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

        // A new turn: journal its acceptance — the committing append releases
        // the ack, so an acked turn is durable (§7.3). The run itself starts
        // in the dispatcher when the turn reaches the queue's head with no run
        // live (`start_next_turn`, the one run-start site); the post-commit
        // `Advance` gets it there without a second caller contact.
        act.subscribe(&turn_id, msg.reply_to);
        drop(act);
        self.schedule_advance(ctx);
        (
            self.accept_records(state, ctx, &self.kind, msg.turn, msg.parent),
            Ok(Accepted {
                turn: turn_id,
                status: SubmitStatus::Started,
            }),
        )
    }

    /// The records that accept a turn: `SessionCreated` on the very first turn,
    /// then `TurnSubmitted` (§7.1, §7.3) — the enqueue, not the run start.
    fn accept_records(
        &self,
        state: &SessionState,
        ctx: &EntityCtx<Agent<S>>,
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
                    kind: KindId::new(ctx.name().entity_type()),
                    digest: kind.digest(),
                    parent,
                    root,
                },
            ));
        }
        let explicit = turn.budget.is_some();
        let budget = turn.budget.unwrap_or(kind.default_budget);
        batch.push(self.rec(
            ctx,
            RecordBody::TurnSubmitted {
                turn: turn.id,
                content: turn.content,
                budget,
                explicit,
            },
        ));
        batch
    }

    fn on_cancel(
        &self,
        state: &SessionState,
        turn: TurnId,
        ctx: &EntityCtx<Agent<S>>,
    ) -> Vec<Record> {
        // A cancel naming the live run ends it; one naming a queued turn ends
        // that turn before it ever starts; one naming an ended or unknown run
        // is an idempotent no-op (§9.2 item 1). The live run's recorded
        // children are not captured here: the fold moves them into
        // `cancels_owed` when the `RunEnded { Cancelled }` commits, and the
        // post-commit `Advance` propagates from that (§9.2 item 2) — so the
        // debt survives a crash in between.
        if state.live.as_ref().is_some_and(|l| l.turn == turn) {
            let mut act = self.lock();
            let ended = self.end_run(&mut act, ctx, turn, Err(RunError::Cancelled));
            drop(act);
            self.schedule_advance(ctx);
            vec![ended]
        } else if state.queue.iter().any(|q| q.turn == turn) {
            // Queued, never started (§9.2): the terminal record alone moves
            // the fold — the queue entry drops, and the post-commit `Advance`
            // notifies subscribers off the recorded outcome. No run began, so
            // no `RunStarted`/`RunEnded` event pair is owed (§10.4).
            self.schedule_advance(ctx);
            vec![self.rec(
                ctx,
                RecordBody::RunEnded {
                    turn,
                    outcome: Err(RunError::Cancelled),
                },
            )]
        } else {
            // A re-sent cancel is a documented resume trigger (§7.5): if this
            // session still owes propagations (a crash lost the earlier send),
            // drive them now.
            if !state.cancels_owed.is_empty() {
                self.schedule_advance(ctx);
            }
            Vec::new()
        }
    }

    fn on_model_done(
        &self,
        state: &SessionState,
        turn: TurnId,
        step: u32,
        result: Result<ModelResponse, ModelError>,
        ctx: &EntityCtx<Agent<S>>,
    ) -> Vec<Record> {
        let mut act = self.lock();
        let Some(live) = state.live.as_ref().filter(|l| l.turn == turn) else {
            // The run ended while the call was in flight (a cancel, §9.2):
            // discard, do not journal, emit nothing (§3.2, §10.4). The launch
            // guard needs no touch-up: claims are never released (`StepKey`).
            return Vec::new();
        };
        if live.spend.own_steps != step {
            // A response for a step the journal has already answered: an old
            // activation's straggler racing the reissued call after a
            // same-node resume. Exactly one response per step commits (§3.1
            // step 2); this one lost, and the claim on the current step still
            // belongs to the call answering it.
            return Vec::new();
        }
        match result {
            Ok(mut response) => {
                // Assign ids the provider omitted (§5.2): deterministic in the
                // step and position.
                for (i, call) in response.calls.iter_mut().enumerate() {
                    if call.id.as_str().is_empty() {
                        call.id = CallId::new(format!("call-{}-{i}", step + 1));
                    }
                }
                // `ModelCompleted` is emitted after the response commits (§10.4),
                // so it counts only journaled spend (§9.1 item 4).
                act.events
                    .model
                    .push((turn.clone(), response.usage.total()));
                // No requested tool calls ⇒ this assistant message ends the run
                // (§3.1 step 3).
                if response.calls.is_empty() {
                    // Final message and terminal outcome in one atomic batch
                    // (§6.4): no journal prefix ends between them.
                    let tokens = live.spend.tokens() + response.usage.total();
                    let completion = Completion::new(response.content.clone(), tokens);
                    let message = self.rec(
                        ctx,
                        RecordBody::ModelResponse {
                            turn: turn.clone(),
                            content: response.content,
                            calls: Vec::new(),
                            usage: response.usage,
                        },
                    );
                    let ended = self.end_run(&mut act, ctx, turn, Ok(completion));
                    drop(act);
                    self.schedule_advance(ctx);
                    vec![message, ended]
                } else {
                    drop(act);
                    self.schedule_advance(ctx);
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
                let ended = self.end_run(&mut act, ctx, turn, Err(RunError::Model(error)));
                drop(act);
                self.schedule_advance(ctx);
                vec![ended]
            }
        }
    }

    fn on_tool_done(
        &self,
        state: &SessionState,
        turn: TurnId,
        call: CallId,
        outcome: Result<Value, ToolError>,
        ctx: &EntityCtx<Agent<S>>,
    ) -> Vec<Record> {
        let mut act = self.lock();
        let pending = state
            .live
            .as_ref()
            .filter(|l| l.turn == turn)
            .and_then(|l| l.pending.get(&call));
        let Some(pending) = pending.filter(|_| !act.run.resolved.contains(&call)) else {
            // A straggler of an ended or cancelled run, or a duplicate
            // (a timeout racing the real outcome): discard (§3.2, §9.2 item 4).
            return Vec::new();
        };
        act.run.resolved.insert(call.clone());
        // Capture the workspace delta this tool left on disk (entity §7.11):
        // the staged records join this command's batch, so the tool outcome
        // and the files it wrote commit atomically (E19). Every sandboxed call
        // captures, success or failure (a failed shell can still have
        // written; a timed-out one may still be writing, so its capture can be
        // torn — the next capture diffs against the last committed index and
        // folds in what the straggler finished, §5.3 item 3); a delegation
        // never touched the directory, and a reported environment loss must
        // not capture — the root may be gone, and a vanished root is never a
        // mass deletion.
        let sandboxed = pending.name != DELEGATE;
        let lost_already = matches!(&outcome, Err(ToolError::EnvironmentLost(_)));
        let outcome = if sandboxed && !lost_already {
            match ctx.ws().capture() {
                Ok(_) => outcome,
                // Over the durable cap: an application outcome the model fixes
                // (nothing was staged; the next successful capture diffs
                // against the last committed index, so nothing is skipped).
                Err(WsError::TooLarge { bytes, cap }) => Err(ToolError::Sandbox(format!(
                    "durable workspace is {bytes} bytes, over the {cap}-byte cap; \
                     remove or exclude large files (this call's changes are not yet persisted)"
                ))),
                // The materialization root vanished: the environment is gone
                // (§5.5); the durable content is safe in the journal and the
                // next activation rematerializes it.
                Err(WsError::RootLost) => {
                    Err(ToolError::EnvironmentLost("workspace root lost".into()))
                }
                Err(e) => Err(ToolError::Sandbox(format!("workspace capture: {e}"))),
            }
        } else {
            outcome
        };
        if let Err(ToolError::EnvironmentLost(_)) = &outcome {
            // The environment is lost (§5.5): drop the binding, release it, and
            // require a `WorkspaceReset` before the next model call.
            self.release_sandbox(&mut act, ctx);
            act.env.reconciled = false;
            act.env.lost_this_activation = true;
        }
        // `ToolCompleted` is owed once the outcome record below commits (§10.4).
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

    /// Hold the dispatcher against the pure desired set (workflow §3.1, W1):
    /// every effect this activation launches MUST be a member of
    /// [`desired`](crate::desired::desired) over the same committed state.
    ///
    /// The check that the agent runtime can sit on the workflow's mechanism,
    /// run by every suite in the crate, including the seeded swarm and cluster
    /// runs, where a resumed activation recomputes the set from a journal it did
    /// not write. A failure means the launch decision depends on something
    /// outside the fold, which is the one thing the inversion cannot carry.
    ///
    /// Only soundness is asserted, never the converse: a member may be blocked
    /// this round by a claim or by the environment reconciliation the dispatcher
    /// still performs inline (§5.4, §5.5, §5.6), so the set is a superset of
    /// what launches until that gating is hoisted out.
    ///
    /// Debug-only: it re-derives the set per launch, which is the fold's cost
    /// again, and it proves a property of the code rather than guarding the
    /// production path.
    #[cfg(debug_assertions)]
    fn debug_assert_desired(&self, state: &SessionState, key: &StepKey) {
        let set = crate::desired::desired(&self.kind, state, self.shared.config.budget_floor);
        assert!(
            set.iter().any(|p| &p.key == key),
            "launched {key:?} outside the desired set {:?}; the launch decision \
             reads state outside the fold (workflow W1)",
            set.iter().map(|p| &p.key).collect::<Vec<_>>()
        );
    }

    #[cfg(not(debug_assertions))]
    fn debug_assert_desired(&self, _state: &SessionState, _key: &StepKey) {}

    // -- the dispatcher (§3.1) ----------------------------------------------

    /// What the fold calls for next, run after every committed batch. Reads the
    /// folded state and launches the next effect, journaling intents first
    /// (§6.4). Returns the records to journal this round (empty when it only
    /// launches effects); the handler schedules a re-advance iff it returns records.
    fn advance(&self, state: &SessionState, ctx: &EntityCtx<Agent<S>>) -> Vec<Record> {
        let kind = &self.kind;
        let session = Self::session(ctx);
        let mut act = self.lock();

        // (a) Emit the run-boundary events this activation owes (§10.4), notify
        //     callers of any finished run, and propagate the cancels the fold
        //     still owes (§9.2).
        self.emit_run_events(&mut act, state, ctx, &session);
        self.notify_finished(&mut act, state, ctx);
        self.propagate_cancels(&mut act, state, ctx);

        // (b) No live run: reset per-run flags and start the next queued turn.
        let Some(live) = state.live.as_ref() else {
            return self.start_next_turn(state, ctx, &mut act);
        };
        let turn = live.turn.clone();

        // (c) Resume: resolve orphaned calls once (§5.5).
        if let Some(events) = self.resolve_orphans(ctx, kind, &mut act, live, &turn) {
            return events;
        }

        // (d) Step boundary: no pending calls ⇒ a model call (§3.1 step 2).
        if live.pending.is_empty() {
            return self.step_model(state, ctx, kind, &mut act, live, turn);
        }

        // (e) Execute every journaled intent not yet launched or resolved
        //     (§3.1 step 4; on resume this is orphaned re-execution, §5.5).
        self.dispatch_pending(state, ctx, kind, &mut act, live, &turn)
    }

    /// (b) No live run: sweep the per-run scope, then start the fold queue's
    /// head turn if one waits (§3.1) — the **one** place a run starts.
    /// Committing the returned `TurnStarted` folds the head into `live` and is
    /// what makes `RunStarted` fire exactly once per turn (§10.4); the
    /// post-commit re-advance dispatches the new run's first model call. The
    /// sweep precedes the record: stale `launched`/`resolved` claims from the
    /// ended run must not suppress the new run's calls — synthesized
    /// `call-{step}-{i}` ids repeat across runs, and so do the step-keyed
    /// model claims.
    fn start_next_turn(
        &self,
        state: &SessionState,
        ctx: &EntityCtx<Agent<S>>,
        act: &mut Activation<S>,
    ) -> Vec<Record> {
        act.run.reset();
        if let Some(next) = state.queue.front() {
            act.events.started.push(next.turn.clone());
            return vec![self.rec(
                ctx,
                RecordBody::TurnStarted {
                    turn: next.turn.clone(),
                },
            )];
        }
        Vec::new()
    }

    /// (c) Resume: on the first advance of a live run the pending set is exactly
    /// the orphaned calls; resolve each whose policy is `Interrupt` (§5.5).
    /// `Some(events)` ⇒ commit them and stop; `None` ⇒ fall through.
    fn resolve_orphans(
        &self,
        ctx: &EntityCtx<Agent<S>>,
        kind: &Kind,
        act: &mut Activation<S>,
        live: &LiveRun,
        turn: &TurnId,
    ) -> Option<Vec<Record>> {
        if act.run.orphans_resolved {
            return None;
        }
        act.run.orphans_resolved = true;
        let mut events = Vec::new();
        for (call, pending) in &live.pending {
            if crate::desired::orphan_policy(kind, &pending.name) == OnOrphan::Interrupt {
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
        ctx: &EntityCtx<Agent<S>>,
        kind: &Kind,
        act: &mut Activation<S>,
        live: &LiveRun,
        turn: TurnId,
    ) -> Vec<Record> {
        // The step this call answers, tagged on the call and matched again on
        // receipt (§3.1 step 2) — and the launch-once claim key: an in-flight
        // step is claimed, and only its committed response advances
        // `own_steps` to a fresh key.
        let step = live.spend.own_steps;
        if act.run.launched.is_claimed(&StepKey::Model(step)) {
            return Vec::new();
        }
        let floor = self.shared.config.budget_floor;
        if !live.spend.allows_call(&live.budget, floor) {
            return vec![self.end_run(act, ctx, turn, Err(RunError::BudgetExhausted))];
        }
        // Surface a lost workspace before the model acts on state that is gone
        // (§5.5). The workspace itself is durable — the agent's facet
        // rematerializes it on every activation (entity §7.11) — so only a loss
        // observed *this* activation (the provider returned `EnvironmentLost`)
        // resets. A fresh environment resets every held tier to `Workspace`;
        // later acquisitions re-journal, never silently inherited.
        if !act.env.reconciled && act.env.lost_this_activation {
            act.env.reconciled = true;
            act.env.tiers_held = BTreeSet::from([Tier::Workspace]);
            return vec![self.rec(ctx, RecordBody::WorkspaceReset)];
        }
        act.env.reconciled = true;
        self.debug_assert_desired(state, &StepKey::Model(step));
        act.run.launched.claim(StepKey::Model(step));
        let request = self.build_request(state, kind, live);
        let this = ctx.this();
        let model = self.seams.model.clone();
        ctx.system().launch(Box::pin(async move {
            let result = model.complete(request).await;
            let _ = this.tell(ModelDone { turn, step, result }).await;
        }));
        Vec::new()
    }

    /// (e) Execute every journaled intent not yet launched or resolved (§3.1
    /// step 4; on resume, orphaned re-execution, §5.5). Synthesized failures and
    /// acquisition/delegation intents are journaled first and re-advanced; once
    /// none remain, every ready call launches. Returns the records to journal.
    fn dispatch_pending(
        &self,
        state: &SessionState,
        ctx: &EntityCtx<Agent<S>>,
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
                if act.run.launched.is_claimed(&StepKey::Call(call.clone()))
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
            // A non-claiming read: calls skipped here may still be listed as
            // `ready` intents this round, and an intent batch returns without
            // launching them (§6.4) — only `launch_call` claims.
            if act.run.launched.is_claimed(&StepKey::Call(call.clone()))
                || act.run.resolved.contains(call)
            {
                continue;
            }
            if pending.name == DELEGATE {
                if pending.child.is_some() {
                    ready.push((call.clone(), pending.clone()));
                } else {
                    match self.plan_delegation(ctx, kind, turn, call, pending, live) {
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
        // A ready call that cannot launch after claiming resolves as a failure
        // instead of orphaned (see `launch_call`); those records commit like
        // any synthesized outcome.
        let mut failed = Vec::new();
        for (call, pending) in ready {
            self.debug_assert_desired(state, &StepKey::Call(call.clone()));
            failed.extend(self.launch_call(act, ctx, kind, turn, &root, call, pending));
        }
        failed
    }

    /// Plan a fresh delegation (§8.1 step 1): validate the child kind against the
    /// allowlist and the registry, then build the `ChildRun` intent.
    fn plan_delegation(
        &self,
        ctx: &EntityCtx<Agent<S>>,
        kind: &Kind,
        turn: &TurnId,
        call: &CallId,
        pending: &PendingCall,
        live: &LiveRun,
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
    /// `delegate` exception executes in the loop (§5.2, §8). Returns the
    /// records to journal — normally none, but a call that cannot launch
    /// *after* claiming resolves as a failure here: a claimed call that bailed
    /// silently would suppress every relaunch and stall the run forever.
    #[allow(clippy::too_many_arguments)]
    fn launch_call(
        &self,
        act: &mut Activation<S>,
        ctx: &EntityCtx<Agent<S>>,
        kind: &Kind,
        turn: &TurnId,
        root: &SessionId,
        call: CallId,
        pending: PendingCall,
    ) -> Vec<Record> {
        if pending.name == DELEGATE {
            if !act.run.launched.claim(StepKey::Call(call.clone())) {
                return Vec::new(); // already in flight this activation
            }
            let Some(child) = pending.child.clone() else {
                // Unreachable by construction — `dispatch_pending` journals the
                // `ChildRun` intent before a delegation is ready — but claimed,
                // so it must resolve rather than dangle.
                return vec![self.fail_call(
                    act,
                    ctx,
                    turn,
                    &call,
                    ToolError::Delegation("delegation intent lost its ChildRun record".into()),
                )];
            };
            let request = match serde_json::from_value::<DelegateInput>(pending.input.clone()) {
                Ok(request) => request,
                Err(e) => {
                    // Same shape: `plan_delegation` validated this input when it
                    // journaled the intent, so a failure here is a drifted
                    // schema — resolve the claimed call, never strand it.
                    return vec![self.fail_call(
                        act,
                        ctx,
                        turn,
                        &call,
                        ToolError::InvalidArguments(e.to_string()),
                    )];
                }
            };
            let lineage = Lineage {
                session: Self::session(ctx),
                turn: turn.clone(),
                root: root.clone(),
            };
            self.launch_delegation(ctx, turn.clone(), call, request, child, lineage);
            return Vec::new();
        }

        let Some(decl) = kind.tools.get(&pending.name) else {
            return Vec::new();
        };
        let tier = decl.tier;
        let timeout = decl.timeout.unwrap_or(self.shared.config.tool_timeout);
        match &act.env.slot {
            SandboxSlot::Open(sandbox) => {
                if !act.run.launched.claim(StepKey::Call(call.clone())) {
                    return Vec::new(); // already in flight this activation
                }
                let sandbox = Arc::clone(sandbox);
                let this = ctx.this();
                let system = ctx.system().clone();
                let name = pending.name.clone();
                let input = pending.input.clone();
                let turn = turn.clone();
                system.clone().launch(Box::pin(async move {
                    let call_fut = Box::pin(sandbox.call(tier, &name, input));
                    let sleep = system.sleep(timeout);
                    let outcome = match select(call_fut, sleep).await {
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
                // The provider runs over the facet's materialized workspace
                // directory (entity §7.11), which rehydration built before any
                // command could reach here.
                match ctx.ws().dir_path() {
                    Ok(workspace) => {
                        act.env.slot = SandboxSlot::Opening;
                        self.launch_open(ctx, kind.profile.clone(), workspace);
                    }
                    Err(e) => {
                        act.env.slot = SandboxSlot::Failed(format!("workspace: {e}"));
                    }
                }
            }
        }
        Vec::new()
    }

    /// Open the sandbox off the command path (§5.3): the task fills the
    /// [`SandboxSlot`] under the mutex and nudges [`Advance`]; success emits
    /// `SandboxBound` (A8).
    fn launch_open(
        &self,
        ctx: &EntityCtx<Agent<S>>,
        profile: crate::sandbox::SandboxProfile,
        workspace: std::path::PathBuf,
    ) {
        let this = ctx.this();
        let system = ctx.system().clone();
        let provider = self.seams.sandbox.clone();
        let act = Arc::clone(&self.act);
        let session = Self::session(ctx);
        let node = ctx.system().node();
        system.clone().launch(Box::pin(async move {
            let result = provider.open(&session, &profile, &workspace).await;
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
                system.emit(AgentEvent::SandboxBound { session, node }.into());
            }
            let _ = this.tell(Advance).await;
        }));
    }

    /// Launch the child submission of a journaled delegation (§8.1 steps 2-3): an
    /// ordinary entity submit with a reply-to mailbox, awaiting the child's
    /// `RunCompleted`, retried on transport failure (safe — the derived `TurnId`
    /// dedups, A7). The outcome becomes this call's tool outcome (§5.4).
    fn launch_delegation(
        &self,
        ctx: &EntityCtx<Agent<S>>,
        turn: TurnId,
        call: CallId,
        request: DelegateInput,
        child: ChildRef,
        lineage: Lineage,
    ) {
        let this = ctx.this();
        let system = ctx.system().clone();
        let cadence = self.shared.config.submit_deadline;
        // Scaled to the carve (§8.1): the child's steps bound its whole
        // subtree's model calls (§9.1 item 3), so they are the right scale for
        // how long the parent waits before surfacing a tool failure.
        let wait = self.shared.config.child_wait(&child.budget);
        let entities = self.shared.hosts().get(&child.kind).cloned();
        system.clone().launch(Box::pin(async move {
            let outcome = match entities {
                // Unreachable on a node built through `HarnessBuilder`, which
                // requires every hosted kind's delegates in the directory;
                // kept as defense so a missing route degrades to a tool
                // failure, never a panic in a launched task.
                None => Err(ToolError::Delegation(format!(
                    "child kind '{}' is not hosted on this node",
                    child.kind
                ))),
                Some(entities) => {
                    let child_ref = entities.entity(child.session.as_str());
                    run_child(
                        system,
                        child_ref,
                        &child,
                        request.prompt,
                        lineage,
                        cadence,
                        wait,
                    )
                    .await
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
    /// `RunEnded` once per turn this activation ended (A3).
    fn emit_run_events(
        &self,
        act: &mut Activation<S>,
        state: &SessionState,
        ctx: &EntityCtx<Agent<S>>,
        session: &SessionId,
    ) {
        let node = ctx.system().node();
        for turn in std::mem::take(&mut act.events.started) {
            let parent = state
                .created
                .as_ref()
                .and_then(|c| c.parent.as_ref())
                .map(|l| l.session.clone());
            ctx.system().emit(
                AgentEvent::RunStarted {
                    session: session.clone(),
                    turn,
                    parent,
                }
                .into(),
            );
        }
        for (turn, usage) in std::mem::take(&mut act.events.model) {
            ctx.system().emit(
                AgentEvent::ModelCompleted {
                    session: session.clone(),
                    turn,
                    node,
                    usage,
                }
                .into(),
            );
        }
        for turn in std::mem::take(&mut act.events.tools) {
            ctx.system().emit(
                AgentEvent::ToolCompleted {
                    session: session.clone(),
                    turn,
                    node,
                }
                .into(),
            );
        }
        for turn in std::mem::take(&mut act.events.ended) {
            if let Some(outcome) = state.turns.get(&turn).and_then(|f| f.outcome.as_ref()) {
                ctx.system().emit(
                    AgentEvent::RunEnded {
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
    fn notify_finished(&self, act: &mut Activation<S>, state: &SessionState, ctx: &EntityCtx<Agent<S>>) {
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
                self.notify(ctx, sub, &turn, outcome.clone());
            }
        }
    }

    /// Deliver a run's outcome to one registered reply-to (§7.3): a
    /// fire-and-forget `tell` of the `RunCompleted`, launched off the command
    /// path.
    fn notify(
        &self,
        ctx: &EntityCtx<Agent<S>>,
        reply_to: ActorRef<ReplyMailbox<S>>,
        turn: &TurnId,
        outcome: RunOutcome,
    ) {
        let msg = RunCompleted {
            session: Self::session(ctx),
            turn: turn.clone(),
            outcome,
        };
        ctx.system().launch(Box::pin(async move {
            let _ = reply_to.tell(msg).await;
        }));
    }

    /// Propagate every cancel the fold still owes (§9.2 item 2): the owed set
    /// is journal-derived (`cancels_owed`), so a crash between the
    /// `RunEnded { Cancelled }` commit and the send loses nothing — the next
    /// activation's first `Advance` lands here and re-sends the undelivered
    /// remainder (A5). Each ack comes back as [`CancelDone`] and journals the
    /// `CancelDelivered` that clears the debt; until then a re-send is safe (a
    /// cancel of an ended or unknown run is an idempotent no-op, §9.2 item 1).
    fn propagate_cancels(
        &self,
        act: &mut Activation<S>,
        state: &SessionState,
        ctx: &EntityCtx<Agent<S>>,
    ) {
        for (turn, owed) in &state.cancels_owed {
            for (call, child) in owed {
                if !act.cancels_launched.insert((turn.clone(), call.clone())) {
                    continue; // launched this activation already
                }
                let Some(entities) = self.shared.hosts().get(&child.kind).cloned() else {
                    // No route to the child's kind — unreachable on a node
                    // built through `HarnessBuilder`, which requires every
                    // hosted kind's delegates in the directory. Kept as
                    // defense: the debt stays in the fold for a
                    // better-configured activation; the child's budget is the
                    // backstop meanwhile (§9.2 item 3).
                    continue;
                };
                let this = ctx.this();
                let system = ctx.system().clone();
                let turn = turn.clone();
                let call = call.clone();
                let child_turn = child.turn.clone();
                let session = child.session.clone();
                let launched = Arc::clone(&self.act);
                system.clone().launch(Box::pin(async move {
                    let child_ref = entities.entity(session.as_str());
                    let mut attempt = 0;
                    loop {
                        match child_ref
                            .ask(Cancel {
                                turn: child_turn.clone(),
                            })
                            .await
                        {
                            Ok(()) => break,
                            Err(_) if attempt < TRANSPORT_RETRIES => {
                                attempt += 1;
                                system.sleep(propagation_backoff(attempt)).await;
                            }
                            // Give up for this attempt only, and re-arm the
                            // launch-once guard: the fold keeps owing, so the
                            // next message that drives the session — the §7.5
                            // resume nudge, a re-sent `Cancel` — re-launches
                            // the send even on this same activation (A5); the
                            // child's budget bounds the meantime (§9.2 item 3).
                            Err(_) => {
                                launched
                                    .lock()
                                    .expect("agent activation mutex poisoned")
                                    .cancels_launched
                                    .remove(&(turn.clone(), call.clone()));
                                return;
                            }
                        }
                    }
                    let _ = this.tell(CancelDone { turn, call }).await;
                }));
            }
        }
    }

    /// A propagated cancel was acked (§9.2): journal the `CancelDelivered`
    /// that clears the owed fact — unless a concurrent delivery (an old
    /// activation's straggler) already cleared it, in which case commit
    /// nothing.
    fn on_cancel_done(
        &self,
        state: &SessionState,
        turn: TurnId,
        call: CallId,
        ctx: &EntityCtx<Agent<S>>,
    ) -> Vec<Record> {
        if !state
            .cancels_owed
            .get(&turn)
            .is_some_and(|owed| owed.contains_key(&call))
        {
            return Vec::new();
        }
        vec![self.rec(ctx, RecordBody::CancelDelivered { turn, call })]
    }

    // -- sandbox lifecycle ---------------------------------------------------

    /// Emit `SandboxReleased` for this session (A8): the one place the event is
    /// built, shared by [`release_sandbox`](Self::release_sandbox) and the
    /// deactivation teardown.
    fn emit_sandbox_released(&self, ctx: &EntityCtx<Agent<S>>) {
        ctx.system().emit(
            AgentEvent::SandboxReleased {
                session: Self::session(ctx),
                node: ctx.system().node(),
            }
            .into(),
        );
    }

    /// Take the bound sandbox out of `act` (A8): swap the slot to `Closed`, clear
    /// the `bound` flag, and report the sandbox to release (if the slot was open)
    /// plus whether a `SandboxReleased` is owed. The caller drives the release
    /// future — deferred off the command path in
    /// [`release_sandbox`](Self::release_sandbox), awaited inline on deactivation.
    /// The announce is paired with `bound` here, independent of the slot: an
    /// unpaired bind is always announced (A8), so a `SandboxReleased` matches
    /// every `SandboxBound` exactly once.
    fn take_sandbox(act: &mut Activation<S>) -> (Option<Arc<dyn Sandbox>>, bool) {
        let release = match std::mem::replace(&mut act.env.slot, SandboxSlot::Closed) {
            SandboxSlot::Open(sandbox) => Some(sandbox),
            _ => None,
        };
        let announce = act.env.bound;
        act.env.bound = false;
        (release, announce)
    }

    /// Drop and release the bound sandbox, emitting `SandboxReleased` (A8).
    /// Synchronous teardown is launched off the command path.
    fn release_sandbox(&self, act: &mut Activation<S>, ctx: &EntityCtx<Agent<S>>) {
        let (release, announce) = Self::take_sandbox(act);
        if let Some(sandbox) = release {
            ctx.system()
                .launch(Box::pin(async move { sandbox.release().await }));
        }
        if announce {
            self.emit_sandbox_released(ctx);
        }
    }

    /// Build the model request from the folded transcript (§4.1), clamping
    /// `max_tokens` to the remaining budget (§9.1 item 2).
    fn build_request(&self, state: &SessionState, kind: &Kind, live: &LiveRun) -> ModelRequest {
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

impl<S: HarnessSystem> Entity for Agent<S> {
    type System = S;
    type State = SessionState;
    type Event = Record;
    // The session's durable workspace (entity §7.11): a real directory the
    // sandbox tiers run over, captured after each tool call so the delta
    // commits atomically with the `ToolOutcome` record (E19).
    type Facets = (Ws,);
    // The fallback type name; the agent runtime hosts each kind under its own name via
    // `entities_named`, so a session's entity type is its `KindId` (§2.2).
    const ENTITY_TYPE: &'static str = "agent.Agent";

    fn apply(state: &mut SessionState, event: &Record) {
        state.apply(event);
    }

    fn register(registry: &mut EntityRegistry<Self>) {
        // The network allowlist: only the wire commands (§7.3). The internal
        // self-tells (Advance/ModelDone/ToolDone/CancelDone) are local-only, so
        // no peer can inject them. Reading a run is not here: `tail` rides entity's
        // gateway-level event read (entity §7.5), a framework built-in that
        // never touches the activation (§10.2).
        registry.accept::<Submit<S>>();
        registry.accept::<Cancel>();
    }

    async fn on_activate(&mut self, ctx: &EntityCtx<Self>) -> Result<(), BoxError> {
        let mut act = self.lock();
        // Opening grants `Workspace` and nothing else (§5.6 item 1); every other
        // tier is re-acquired under a journaled record this activation.
        act.env.tiers_held = BTreeSet::from([Tier::Workspace]);
        act.env.reconciled = false;
        act.env.lost_this_activation = false;
        act.run.orphans_resolved = false;
        drop(act);
        // Off the activation latency path: sweep checkpoint chunks a prior
        // activation orphaned (the blob handle unions the facet roots, so the
        // restored manifest's chunks always survive, entity §7.12/F3) and
        // re-fan the live ones to the current replicas (entity §7.10).
        let blobs = ctx.blobs();
        ctx.system().launch(Box::pin(async move {
            let _ = blobs.gc(&BTreeSet::new()).await;
            blobs.repair_facets().await;
        }));
        Ok(())
    }

    async fn on_deactivate(&mut self, ctx: &EntityCtx<Self>) {
        // Release the sandbox on every deactivation — idle hibernation, migration,
        // or forced step-down (A8). The workspace goes with it (§5.5). Scope the
        // lock so the guard is dropped before the async release.
        let (release, announce) = {
            let mut act = self.lock();
            Self::take_sandbox(&mut act)
        };
        if let Some(sandbox) = release {
            sandbox.release().await;
        }
        if announce {
            self.emit_sandbox_released(ctx);
        }
    }

    fn can_hibernate(&self, state: &SessionState) -> bool {
        // Never hibernate a session whose run is live or whose fold queue
        // holds an accepted turn (§7.2) — both mean work is owed now — nor one
        // with a launched effect whose outcome could still arrive (the guard's
        // claims are swept by the post-end `Advance`).
        if state.live.is_some() || !state.queue.is_empty() {
            return false;
        }
        let act = self.lock();
        act.run.launched.is_idle() && !matches!(act.env.slot, SandboxSlot::Opening)
    }
}

impl<S: HarnessSystem> EntityHandler<Submit<S>> for Agent<S> {
    async fn handle(
        &self,
        state: &SessionState,
        msg: Submit<S>,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<Record>, Result<Accepted, SubmitReject>) {
        self.on_submit(state, msg, ctx)
    }
}

impl<S: HarnessSystem> EntityHandler<Cancel> for Agent<S> {
    async fn handle(
        &self,
        state: &SessionState,
        msg: Cancel,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<Record>, ()) {
        (self.on_cancel(state, msg.turn, ctx), ())
    }
}

impl<S: HarnessSystem> EntityHandler<Advance> for Agent<S> {
    async fn handle(
        &self,
        state: &SessionState,
        _msg: Advance,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<Record>, ()) {
        let events = self.advance(state, ctx);
        if !events.is_empty() {
            self.schedule_advance(ctx);
        }
        (events, ())
    }
}

impl<S: HarnessSystem> EntityHandler<ModelDone> for Agent<S> {
    async fn handle(
        &self,
        state: &SessionState,
        msg: ModelDone,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<Record>, ()) {
        (
            self.on_model_done(state, msg.turn, msg.step, msg.result, ctx),
            (),
        )
    }
}

impl<S: HarnessSystem> EntityHandler<ToolDone> for Agent<S> {
    async fn handle(
        &self,
        state: &SessionState,
        msg: ToolDone,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<Record>, ()) {
        (
            self.on_tool_done(state, msg.turn, msg.call, msg.outcome, ctx),
            (),
        )
    }
}

impl<S: HarnessSystem> EntityHandler<CancelDone> for Agent<S> {
    async fn handle(
        &self,
        state: &SessionState,
        msg: CancelDone,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<Record>, ()) {
        (self.on_cancel_done(state, msg.turn, msg.call, ctx), ())
    }
}

// ===========================================================================
// Free helpers
// ===========================================================================

/// The outcome of one [`submit_and_attach`] attempt. The *policy* (when to give
/// up, when to retry) is the caller's loop: the two callers bound it differently
/// (§7.4, §8.1).
pub(crate) enum AttachOutcome {
    /// The run finished and delivered its outcome.
    Completed(RunOutcome),
    /// The submit was accepted but the wait lapsed (or the mailbox dropped):
    /// re-submitting the same `TurnId` re-attaches (A7).
    Lapsed,
    /// The submit was permanently rejected (§7.4): never worth retrying.
    Rejected(SubmitReject),
    /// The entity was unreachable: a transport failure a retry might clear.
    Unreachable(EntityError),
}

/// One submit-and-await-outcome attempt against an entity (§7.4): spawn an
/// ephemeral reply mailbox, `Submit` the turn carrying it, and await the run's
/// `RunCompleted` bounded by `within`. Wrapping it in a retry loop is safe
/// because re-submitting the same `TurnId` is idempotent (A7). The mailbox
/// lives exactly as long as the attempt: a [`MailboxGuard`] stops it on every
/// path that ends without an outcome — including this future being dropped —
/// so a re-attach replaces the mailbox rather than accumulating one per
/// attempt (§7.4).
pub(crate) async fn submit_and_attach<S: HarnessSystem>(
    system: &S,
    entity: &entity::EntityRef<Agent<S>>,
    kind: &KindId,
    turn: &Turn,
    parent: Option<&Lineage>,
    within: Duration,
) -> AttachOutcome {
    let (tx, rx) = oneshot::channel::<RunOutcome>();
    let mailbox = system.spawn(ReplyMailbox::new(tx));
    let guard = MailboxGuard::new(system.clone(), mailbox.clone());
    let submit = Submit {
        kind: kind.clone(),
        turn: turn.clone(),
        parent: parent.cloned(),
        reply_to: Some(mailbox),
    };
    match entity.ask_timeout(submit, within).await {
        Ok(Ok(_accepted)) => {
            let sleep = system.sleep(within);
            match select(rx, sleep).await {
                Either::Left((Ok(outcome), _)) => {
                    // Delivered: the mailbox already stopped itself.
                    guard.disarm();
                    AttachOutcome::Completed(outcome)
                }
                Either::Left((Err(_), _)) | Either::Right(((), _)) => AttachOutcome::Lapsed,
            }
        }
        Ok(Err(reject)) => AttachOutcome::Rejected(reject),
        Err(call_error) => AttachOutcome::Unreachable(call_error),
    }
}

/// Submit to a child and await its outcome (§8.1 step 2), with the derived
/// `TurnId` keeping every re-attempt safe (A7). Re-attaches on the `cadence`
/// (each lapse re-registers on whatever activation now hosts the child, §7.5)
/// until the budget-scaled `wait` lapses; retries a consecutively unreachable
/// child up to [`TRANSPORT_RETRIES`] with backoff. Maps the child's
/// `RunOutcome` onto this delegation's tool outcome (§5.4). A give-up resolves
/// the call only for the parent: the child run continues, its budget the
/// backstop (§9.2 item 3).
async fn run_child<S: HarnessSystem>(
    system: S,
    child_ref: entity::EntityRef<Agent<S>>,
    child: &ChildRef,
    prompt: String,
    parent: Lineage,
    cadence: Duration,
    wait: Duration,
) -> Result<Value, ToolError> {
    let turn = Turn {
        id: child.turn.clone(),
        content: prompt,
        budget: Some(child.budget),
    };
    let started = system.now();
    let mut unreachable = 0;
    loop {
        let elapsed = system.now().duration_since(started);
        let Some(remaining) = wait.checked_sub(elapsed).filter(|d| !d.is_zero()) else {
            return Err(ToolError::Delegation(format!(
                "child run did not complete within its {}s budget-scaled wait",
                wait.as_secs()
            )));
        };
        match submit_and_attach(
            &system,
            &child_ref,
            &child.kind,
            &turn,
            Some(&parent),
            cadence.min(remaining),
        )
        .await
        {
            AttachOutcome::Completed(outcome) => {
                return match outcome {
                    Ok(completion) => Ok(Value::String(completion.text().to_string())),
                    Err(run_error) => Err(ToolError::Delegation(run_error.to_string())),
                };
            }
            // The child accepted: it is reachable, whatever came before.
            AttachOutcome::Lapsed => unreachable = 0,
            AttachOutcome::Rejected(reject) => {
                return Err(ToolError::Delegation(format!(
                    "child rejected the submit: {reject}"
                )));
            }
            AttachOutcome::Unreachable(call_error) => {
                unreachable += 1;
                if unreachable >= TRANSPORT_RETRIES {
                    return Err(ToolError::Delegation(format!(
                        "child unreachable: {call_error:?}"
                    )));
                }
                system.sleep(propagation_backoff(unreachable)).await;
            }
        }
    }
}

/// Exponential backoff for the delegation/cancel retry loops, capped (core
/// §11.2).
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
