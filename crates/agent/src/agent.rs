//! The agent: an autonomous entity (agent spec §3).
//!
//! A session *is* an entity ([`entity`]). What the agent runtime adds is **autonomy**:
//! an agent's activation drives itself forward (model→tools→model) until a run
//! reaches a terminal outcome. The entity model is decide/apply: [`Entity::apply`]
//! folds records into [`SessionState`], and each [`EntityHandler`] is a *pure
//! decision* returning the records to journal plus a reply (entity §4.2). The
//! loop rides three entity seams:
//!
//! - **The output gate** (entity §6): a handler's records commit before its
//!   reply releases, so "intent before effect" (§6.4) is free; a handler that
//!   journals an intent never launches the effect itself.
//! - **Self-messaging** ([`EntityCtx::this`]) + **`ctx.system().launch`**: each
//!   external operation (a model call, a sandboxed tool, a child submit) is a
//!   background task that delivers its outcome back as an ordinary command, and
//!   every commit nudges the loop forward. The activation therefore never awaits
//!   I/O inside a handler (§3.2). Both messages are the workflow's now, [`Tick`]
//!   and [`Settled`], and so is the cycle between them.
//! - **`on_activate`/`on_deactivate`/`can_hibernate`** (entity §10): resume
//!   set-up, sandbox release on every deactivation (A8), and the veto that keeps
//!   a live run from hibernating (§7.2).
//!
//! Everything else (the journal, the single-writer fence, placement,
//! activation, hibernation, lossless failover) is the entity's (§2.1), and the
//! **autonomy itself is the workflow's** ([`Workflow`], workflow spec): this
//! module supplies [`pending`](Workflow::pending), [`perform`](Workflow::perform)
//! and [`settle`](Workflow::settle), and the [`Engine`] runs the cycle over
//! them, holding the launch claims, classifying the orphan set on resume, and
//! filtering the stragglers. What is left here is the agentic policy: the
//! transcript and its fold, the two seams, kinds and budgets, delegation, and
//! the environment pre-pass no fold can express.
//!
//! Ephemeral activation state (the sandbox handle, held tiers, the subscriber
//! set) lives behind a [`Mutex`] on the entity behavior, rebuilt per activation
//! and never journaled (§5.5, §5.6, §7.4); the engine keeps its own beside it.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use actor_core::ActorRef;
use actor_core::BoxError;
use actor_core::Manifest;
use actor_core::Message;
use entity::Entity;
use entity::EntityCtx;
use entity::EntityHandler;
use entity::EntityRegistry;
use entity::facet::Ws;
use entity::facet::WsError;
use futures::future::Either;
use futures::future::select;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use workflow::Binding;
use workflow::EffectError;
use workflow::Engine;
use workflow::Pending;
use workflow::Perform;
use workflow::Settled;
use workflow::Subscribers;
use workflow::Tick;
use workflow::Workflow;

use crate::client::HarnessSystem;
use crate::client::ReplyMailbox;
use crate::client::Seams;
use crate::client::Shared;
use crate::client::run_child;
use crate::desired::Effect;
use crate::desired::StepKey;
use crate::event::AgentEvent;
use crate::kind::Kind;
use crate::model::ModelError;
use crate::model::ModelResponse;
use crate::sandbox::Sandbox;
use crate::sandbox::Tier;
use crate::session::CallId;
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
use crate::session::TurnId;
use crate::session::derive_child;
use crate::session::outcome_label;
use crate::session::Turn;
use crate::session::turn_digest;
use crate::tool::DELEGATE;
use crate::tool::DelegateInput;
use crate::tool::ToolError;

// ===========================================================================
// Wire contract (§7.3): commands addressed to the entity, and the outbound
// `RunCompleted` notification. A session is addressed as an entity (its `kind`
// is the entity type and its `SessionId` the key), so the `EntityName` carries
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
/// record commits (§7.3): an ack, not the run's outcome; the outcome travels
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
/// **permanent** (the same submit always rejects the same way), so a caller
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

/// What one launched effect returned (§3.1 steps 2 and 4): the agent runtime's
/// [`Workflow::Outcome`], carried back onto the command path by the workflow's
/// [`Settled`] (workflow §4.2).
///
/// The turn, the step, and the `CallId` are not here: they are the [`StepKey`]
/// the same `Settled` carries, which is what makes a superseded response
/// (an old activation's straggler racing the reissue after a same-node resume)
/// a key that has left the desired set rather than a field to compare (workflow
/// §4.5, §9.1 item 4).
///
/// A tool failure is an ordinary value in the `Ok` arm, because a failing tool
/// never fails the run (§5.4). The workflow's own [`EffectError`] reaches this
/// entity as `Interrupted`, for an orphaned call the fold resolves without
/// launching (§5.5), and as `Failed`, for a `perform` that panicked (workflow
/// §4.3) or a cancel this entity's own `perform` could not send (§9.2).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StepOutcome {
    /// A model call's result (§3.1 step 2).
    Model(Result<ModelResponse, ModelError>),
    /// A tool call's or delegation's result (§5.4, §8.1).
    Tool(Result<Value, ToolError>),
    /// A propagated `Cancel` was acked by the child (§9.2): the ack itself is
    /// not durable, only the `CancelDelivered` record the settle journals is.
    Cancelled,
}

// ===========================================================================
// The entity
// ===========================================================================

/// What the environment pre-pass concluded about this round (§5.4, §5.5, §5.6).
enum Reconciled {
    /// Records to commit before anything launches; the tick their commit brings
    /// re-enters the cycle (§6.4).
    Records(Vec<Record>),
    /// The environment is not ready and an open is in flight. Nothing launches
    /// this round; the open's own tick re-enters.
    Waiting,
    /// Every desired effect is launchable as it stands.
    Ready,
}

/// The sandbox binding and tier grants this activation holds (§5.4, §5.6):
/// fold-external working state, rebuilt per activation and released on
/// deactivation (A8).
#[derive(Default)]
struct SandboxState {
    /// The sandbox handle's lifecycle slot: at most one live sandbox per
    /// activation (invariant A8). This is the workflow's activation-scoped
    /// binding (workflow §6): opened once on demand, vetoing hibernation while
    /// the open is in flight, released on every deactivation, and never
    /// journaled. The opened handle is not serializable, so the open task fills
    /// the binding directly under the [`Mutex`] (the one place a task touches
    /// activation state off the command path) rather than carrying it in a
    /// message; the task then re-enters the cycle with a [`Tick`].
    slot: Binding<Arc<dyn Sandbox>>,
    /// Tiers this activation holds (§5.6): working state, never fold state.
    /// Opening grants `Workspace` and nothing else (§5.6 item 1).
    tiers_held: BTreeSet<Tier>,
    /// Whether this activation has reconciled with the journal's sandbox
    /// activity (§5.5): false until a `WorkspaceReset` is journaled (or none is
    /// needed); flipped false again on environment loss.
    reconciled: bool,
    /// Whether the workspace was lost *during* this activation: the provider
    /// returned `EnvironmentLost` (§5.4). The durable workspace survives
    /// reactivation, so only a loss observed here resets. Set on
    /// `EnvironmentLost`, cleared per activation in `on_activate`.
    lost_this_activation: bool,
}

/// Ephemeral, per-activation working state (§5.5, §5.6, §7.4): never journaled,
/// rebuilt on every activation, lost on deactivation. Held behind a [`Mutex`] so
/// the `&self` handlers (entity's decide functions) can mutate it.
struct Activation<S: HarnessSystem> {
    /// The sandbox binding and held tiers (§5.4, §5.6).
    env: SandboxState,
    /// Events owed once the records they announce commit (§10.4): built by the
    /// handler that decides the record, emitted by the tick that follows the
    /// commit, in push order. Filled only on the committing activation, so a
    /// resume re-emits none of them.
    events: Vec<AgentEvent>,
    /// Callers registered for a run's outcome (§7.4): notified on `RunEnded`,
    /// rebuilt by callers re-contacting after a resume. The workflow's
    /// component (workflow §7.1), which is where the discipline this follows
    /// is written down.
    subscribers: Subscribers<TurnId, ActorRef<ReplyMailbox<S>>>,
}

impl<S: HarnessSystem> Activation<S> {
    /// Register `reply_to` (if any) for `turn`'s outcome (§7.4). A no-op when the
    /// submit carried no mailbox: the one place a subscriber is added to the list.
    fn subscribe(&mut self, turn: &TurnId, reply_to: Option<ActorRef<ReplyMailbox<S>>>) {
        if let Some(reply_to) = reply_to {
            self.subscribers.register(turn.clone(), reply_to);
        }
    }
}

// Hand-written rather than `#[derive(Default)]`: deriving would impose a
// spurious `S: Default` bound.
impl<S: HarnessSystem> Default for Activation<S> {
    fn default() -> Self {
        Activation {
            env: SandboxState::default(),
            events: Vec::new(),
            subscribers: Subscribers::default(),
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
    /// has `K`'s definition: kind resolution is infallible, no lookup needed.
    kind: Arc<Kind>,
    /// The model and sandbox seams for this hosted kind (§4, §5.3), captured by
    /// the factory.
    seams: Seams,
    /// The workflow's reconciliation engine (workflow §2.1): the claims, the
    /// orphan classification, and the launch cycle. Internally synchronized, so
    /// it sits beside the activation mutex rather than inside it.
    engine: Engine<Agent<S>>,
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
            engine: Engine::default(),
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

    /// Resolve `call` with a failure outcome the fold decided rather than an
    /// effect produced (§5.4): the `ToolOutcome` record to journal.
    ///
    /// Committing it takes the call out of the desired set, which is what stops
    /// the relaunch; there is no second place to mark, and so no way for the two
    /// to disagree.
    fn fail_call(
        &self,
        ctx: &EntityCtx<Agent<S>>,
        turn: &TurnId,
        call: &CallId,
        error: ToolError,
    ) -> Record {
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
    /// the terminal record to journal. Paired like `fail_call`: the event
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
        act.events.push(AgentEvent::RunEnded {
            session: Self::session(ctx),
            turn: turn.clone(),
            outcome: outcome_label(&outcome),
        });
        self.rec(ctx, RecordBody::RunEnded { turn, outcome })
    }

    /// Fail every desired sandboxed call with one reason (§5.4): what a sandbox
    /// the activation could not open owes the calls that needed it. One batch,
    /// so no commit lands between two of them and starts an open the rest
    /// would then wait on.
    fn fail_sandboxed(
        &self,
        ctx: &EntityCtx<Agent<S>>,
        turn: &TurnId,
        desired: &[Pending<StepKey, Effect>],
        error: &str,
    ) -> Vec<Record> {
        desired
            .iter()
            .filter_map(|p| match &p.effect {
                Effect::Tool { call, .. } => Some(call),
                _ => None,
            })
            .map(|call| self.fail_call(ctx, turn, call, ToolError::Sandbox(error.to_string())))
            .collect()
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

        // Dedup against the journal (§7.4): every accepted turn (queued, live,
        // or ended) has folded facts, so one check covers all three (A7). The
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
                    // waiting on this very nudge (§7.5): drive the cycle, and
                    // wake any key the engine had backed off to rest.
                    drop(act);
                    self.engine.tick(self, state, ctx);
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

        // A new turn: journal its acceptance; the committing append releases
        // the ack, so an acked turn is durable (§7.3). The run itself starts
        // in the dispatcher when the turn reaches the queue's head with no run
        // live (`start_next_turn`, the one run-start site); the post-commit
        // tick gets it there without a second caller contact.
        act.subscribe(&turn_id, msg.reply_to);
        drop(act);
        self.engine.tick(self, state, ctx);
        (
            self.accept_records(state, ctx, &self.kind, msg.turn, msg.parent),
            Ok(Accepted {
                turn: turn_id,
                status: SubmitStatus::Started,
            }),
        )
    }

    /// The records that accept a turn: `SessionCreated` on the very first turn,
    /// then `TurnSubmitted` (§7.1, §7.3); the enqueue, not the run start.
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
        // post-commit tick propagates from that (§9.2 item 2), so the
        // debt survives a crash in between.
        if state.live.as_ref().is_some_and(|l| l.turn == turn) {
            let mut act = self.lock();
            let ended = self.end_run(&mut act, ctx, turn, Err(RunError::Cancelled));
            drop(act);
            self.engine.tick(self, state, ctx);
            vec![ended]
        } else if state.queue.iter().any(|q| q.turn == turn) {
            // Queued, never started (§9.2): the terminal record alone moves
            // the fold; the queue entry drops, and the post-commit tick
            // notifies subscribers off the recorded outcome. No run began, so
            // no `RunStarted`/`RunEnded` event pair is owed (§10.4).
            self.engine.tick(self, state, ctx);
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
                self.engine.tick(self, state, ctx);
            }
            Vec::new()
        }
    }

    /// Fold a model call's outcome (§3.1 steps 2 and 3): the model half of
    /// [`Workflow::settle`].
    ///
    /// No straggler test of its own. A response for a step the journal has
    /// already answered, or for a run that ended while the call was in flight,
    /// arrives under a [`StepKey`] that has left the desired set, and the engine
    /// discards it before this is reached (workflow §4.5).
    fn settle_model(
        &self,
        state: &SessionState,
        turn: &TurnId,
        step: u32,
        outcome: Result<StepOutcome, EffectError>,
        ctx: &EntityCtx<Agent<S>>,
    ) -> Vec<Record> {
        let result = match outcome {
            Ok(StepOutcome::Model(result)) => result,
            // The workflow mints an `EffectError` for an orphaned effect it
            // resolved without launching, and for a `perform` that panicked
            // (workflow §4.3, §4.4). Either way no response exists, and a model
            // failure no retry policy absorbed ends the run (§4.3).
            Err(why) => Err(ModelError::Api(why.to_string())),
            // Unreachable: the effect under a `Model` key is a model call.
            Ok(StepOutcome::Tool(_) | StepOutcome::Cancelled) => Err(ModelError::Api(
                "a tool outcome was settled under a model call's key".into(),
            )),
        };
        let turn = turn.clone();
        let mut act = self.lock();
        let Some(live) = state.live.as_ref().filter(|l| l.turn == turn) else {
            return Vec::new();
        };
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
                act.events.push(AgentEvent::ModelCompleted {
                    session: Self::session(ctx),
                    turn: turn.clone(),
                    node: ctx.system().node(),
                });
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
                    vec![message, ended]
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
                vec![self.end_run(&mut act, ctx, turn, Err(RunError::Model(error)))]
            }
        }
    }

    /// Fold a tool call's or delegation's outcome (§5.4, §8.1): the call half of
    /// [`Workflow::settle`].
    ///
    /// Like the model half it needs no straggler test: a duplicate (a timeout
    /// racing the real outcome, §9.2 item 4) and a late outcome of an ended run
    /// both arrive under a key the desired set no longer holds.
    fn settle_tool(
        &self,
        state: &SessionState,
        turn: &TurnId,
        call: &CallId,
        outcome: Result<StepOutcome, EffectError>,
        ctx: &EntityCtx<Agent<S>>,
    ) -> Vec<Record> {
        let outcome = match outcome {
            Ok(StepOutcome::Tool(outcome)) => outcome,
            // An orphaned call whose declaration said `Interrupt`, resolved by
            // the workflow without a launch (§5.5, workflow §4.4). The model
            // decides whether to retry the side effect.
            Err(EffectError::Interrupted) => Err(ToolError::Interrupted),
            // A `perform` that panicked, caught by the workflow (workflow §4.3).
            Err(EffectError::Failed(why)) => Err(ToolError::Sandbox(why)),
            // Never produced under a call key: a tool's own bound and a lost
            // environment come back as the `ToolError` the call returns. Mapped
            // rather than folded into the arm above so the type stays total.
            Err(EffectError::Timeout) => Err(ToolError::Timeout),
            Err(EffectError::Lost(why)) => Err(ToolError::EnvironmentLost(why)),
            // Unreachable: the effect under a `Call` key is a tool call.
            Ok(StepOutcome::Model(_) | StepOutcome::Cancelled) => Err(ToolError::Sandbox(
                "a model response was settled under a tool call's key".into(),
            )),
        };
        let mut act = self.lock();
        let pending = state
            .live
            .as_ref()
            .filter(|l| &l.turn == turn)
            .and_then(|l| l.pending.get(call));
        let Some(pending) = pending else {
            return Vec::new();
        };
        // Capture the workspace delta this tool left on disk (entity §7.11):
        // the staged records join this command's batch, so the tool outcome
        // and the files it wrote commit atomically (E19). Every sandboxed call
        // captures, success or failure (a failed shell can still have
        // written; a timed-out one may still be writing, so its capture can be
        // torn: the next capture diffs against the last committed index and
        // folds in what the straggler finished, §5.3 item 3); a delegation
        // never touched the directory, and a reported environment loss must
        // not capture; the root may be gone, and a vanished root is never a
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
        act.events.push(AgentEvent::ToolCompleted {
            session: Self::session(ctx),
            turn: turn.clone(),
            node: ctx.system().node(),
        });
        vec![self.rec(
            ctx,
            RecordBody::ToolOutcome {
                turn: turn.clone(),
                call: call.clone(),
                outcome,
            },
        )]
    }

    /// Hold the pre-pass to what the workflow's mechanism requires (workflow
    /// §3.1, W1): once [`reconcile`](Self::reconcile) reports ready, **every**
    /// member of the desired set is launchable as it stands, with no gate left
    /// between it and its launch.
    ///
    /// The engine cannot check this direction itself: the environment a gate
    /// reads is the consumer's and invisible from below. Debug-only, since it
    /// re-walks the set per round to prove a property of the code rather than
    /// guard the production path.
    #[cfg(debug_assertions)]
    fn debug_assert_launchable(&self, act: &Activation<S>, desired: &[Pending<StepKey, Effect>]) {
        for p in desired {
            match &p.effect {
                Effect::Model { .. } => assert!(
                    act.env.reconciled,
                    "a model call is desired with the environment unreconciled; \
                     the launch decision still reads outside the fold (workflow W1)"
                ),
                Effect::Tool { call, tier, .. } => {
                    assert!(
                        act.env.tiers_held.contains(tier),
                        "tool call {call} is desired at a tier this activation does not hold; \
                         the acquisition gate is still inside the launch (§5.6)"
                    );
                    assert!(
                        act.env.slot.get().is_some(),
                        "tool call {call} is desired with no sandbox bound ({:?}); \
                         the environment gate is still inside the launch (§5.3, §5.4)",
                        act.env.slot.state()
                    );
                }
                // A delegation and a cancel run in the loop and a synthesized
                // failure runs nowhere: none needs an environment (§5.2, §5.4,
                // §8, §9.2).
                Effect::Delegate { .. } | Effect::Cancel { .. } | Effect::Fail { .. } => {}
            }
        }
    }

    #[cfg(not(debug_assertions))]
    fn debug_assert_launchable(&self, _act: &Activation<S>, _desired: &[Pending<StepKey, Effect>]) {
    }

    // -- the cycle (§3.1, workflow §3.2) -------------------------------------

    /// The workflow cycle's entry point on this entity: the body of its
    /// [`EntityHandler<Tick>`], run after every committed batch.
    ///
    /// Two things happen before the engine sees the state, neither of them an
    /// effect and both the agent runtime's own: the events this activation owes
    /// (§10.4) and the callers waiting on a run that has finished (§7.4). Then
    /// the turn queue, then the environment pre-pass, and only then the engine,
    /// which launches what [`desired`](crate::desired::desired) holds and
    /// nothing else; an owed cancel is a member of that set (§9.2).
    ///
    /// Returns the records to journal this round, which are the agent runtime's
    /// alone: the engine decides none, every outcome reaching the journal as a
    /// [`Settled`] instead (workflow §3.2).
    fn tick(&self, state: &SessionState, ctx: &EntityCtx<Agent<S>>) -> Vec<Record> {
        let kind = &self.kind;
        let mut act = self.lock();

        // The events whose records the last commit carried (§10.4).
        for event in std::mem::take(&mut act.events) {
            ctx.system().emit(event.into());
        }
        self.notify_finished(&mut act, state, ctx);

        // No live run: start the fold queue's head turn if one waits.
        let Some(live) = state.live.as_ref() else {
            let records = self.start_next_turn(state, ctx, &mut act);
            drop(act);
            if !records.is_empty() {
                self.engine.tick(self, state, ctx);
                return records;
            }
            // Nothing to start, so the desired set is empty and this is the
            // round that says so: the engine releases the last claims and
            // concludes quiescence here (workflow §3.2 step 4, §6). Returning
            // ahead of it would leave `can_hibernate` answering from a cycle
            // that ran while work was still outstanding, and the session would
            // never come to rest.
            return self.engine.advance(self, state, ctx);
        };
        let turn = live.turn.clone();

        // Reconcile the activation's environment against what the fold calls
        // for (§5.4, §5.5, §5.6), before anything launches.
        let desired = crate::desired::desired(kind, &self.shared.config, state);
        match self.reconcile(ctx, kind, &mut act, live, &turn, &desired) {
            // Records first: the commit brings the next tick, and nothing
            // launches against a fold that is about to change (§6.4).
            Reconciled::Records(records) => {
                drop(act);
                self.engine.tick(self, state, ctx);
                return records;
            }
            // An open is in flight; the open task's own tick re-enters (§5.3
            // item 1).
            Reconciled::Waiting => return Vec::new(),
            Reconciled::Ready => {}
        }
        self.debug_assert_launchable(&act, &desired);
        drop(act);
        self.engine.advance(self, state, ctx)
    }

    /// No live run: start the fold queue's head turn if one waits (§3.1); the
    /// **one** place a run starts. Committing the returned `TurnStarted` folds
    /// the head into `live` and is what makes `RunStarted` fire exactly once per
    /// turn (§10.4); the tick that commit brings dispatches the new run's first
    /// model call.
    ///
    /// No sweep is needed first: the claims are the engine's, released as their
    /// keys leave the desired set (workflow §3.2), and a key names its turn
    /// ([`StepKey`]), so no leftover of the ended run can suppress a call of the
    /// next one.
    fn start_next_turn(
        &self,
        state: &SessionState,
        ctx: &EntityCtx<Agent<S>>,
        act: &mut Activation<S>,
    ) -> Vec<Record> {
        if let Some(next) = state.queue.front() {
            // `SessionCreated` commits with the first `TurnSubmitted`, so the
            // lineage is folded before any turn can reach the queue's head.
            act.events.push(AgentEvent::RunStarted {
                session: Self::session(ctx),
                turn: next.turn.clone(),
                parent: state
                    .created
                    .as_ref()
                    .and_then(|c| c.parent.as_ref())
                    .map(|l| l.session.clone()),
            });
            return vec![self.rec(
                ctx,
                RecordBody::TurnStarted {
                    turn: next.turn.clone(),
                },
            )];
        }
        Vec::new()
    }

    /// Reconcile the activation's environment against the desired set (§5.4,
    /// §5.5, §5.6), ahead of every launch.
    ///
    /// The gates that cannot be folded live here: a workspace lost during this
    /// activation, a failed sandbox open, and a tier not yet held. Each is
    /// settled by a record or by an open in flight, and only once none of them
    /// applies does the round reach the engine. That ordering is what lets
    /// [`Workflow::pending`] be a function of the journal alone (W1): this reads
    /// the activation freely, and the engine that follows it reads none of it.
    ///
    /// Two fold-level decisions ride along because they too are records the
    /// launch must not race: the run a budget has ended (§3.1 step 2) and a
    /// delegation whose `ChildRun` is not yet journaled (§8.1 step 1), which is
    /// why the desired set omits such a call until it is.
    fn reconcile(
        &self,
        ctx: &EntityCtx<Agent<S>>,
        kind: &Kind,
        act: &mut Activation<S>,
        live: &LiveRun,
        turn: &TurnId,
        desired: &[Pending<StepKey, Effect>],
    ) -> Reconciled {
        // The budget gate (§3.1 step 2). `desired` already folds it: a live run
        // with nothing pending whose state calls for no model call is a run
        // that cannot afford one, so the missing call *is* the exhaustion. An
        // owed cancel of an earlier turn may share the set (§9.2) and says
        // nothing about this run's budget.
        let model_wanted = desired
            .iter()
            .any(|p| matches!(p.effect, Effect::Model { .. }));
        if !model_wanted && live.pending.is_empty() {
            return Reconciled::Records(vec![self.end_run(
                act,
                ctx,
                turn.clone(),
                Err(RunError::BudgetExhausted),
            )]);
        }

        // Surface a lost workspace before the model acts on state that is gone
        // (§5.5). The workspace itself is durable: the agent's facet
        // rematerializes it on every activation (entity §7.11), so only a loss
        // observed *this* activation (the provider returned `EnvironmentLost`)
        // resets. A fresh environment resets every held tier to `Workspace`;
        // later acquisitions re-journal, never silently inherited.
        if model_wanted && !act.env.reconciled {
            act.env.reconciled = true;
            if act.env.lost_this_activation {
                act.env.tiers_held = BTreeSet::from([Tier::Workspace]);
                return Reconciled::Records(vec![self.rec(ctx, RecordBody::WorkspaceReset)]);
            }
        }

        // A failed sandbox open fails the calls that needed it (§5.4); taking
        // the failure resets the slot to `Closed`, so a later call retries the
        // open.
        if let Some(error) = act.env.slot.take_failure() {
            let events = self.fail_sandboxed(ctx, turn, desired, &error);
            if !events.is_empty() {
                return Reconciled::Records(events);
            }
        }

        // A delegation's `ChildRun` intent (§8.1 step 1), journaled before the
        // child submit and before the call joins the desired set at all.
        let mut events = Vec::new();
        for (call, pending) in &live.pending {
            if pending.name != DELEGATE || pending.child.is_some() {
                continue;
            }
            match self.plan_delegation(ctx, kind, turn, call, pending, live) {
                Ok(child_run) => events.push(child_run),
                Err(tool_err) => events.push(self.fail_call(ctx, turn, call, tool_err)),
            }
        }
        // The acquisition gate (§5.6): journal `TierAcquired` before the first
        // call at a tier not yet held, and mark it held optimistically so
        // neither a same-batch sibling nor the next tick re-journals it. A
        // failed commit forces step-down, which resets held tiers to
        // `Workspace` on the next activation (§5.5), so the optimism is safe.
        for tier in desired
            .iter()
            .filter_map(|p| crate::desired::tier_of(&p.effect))
        {
            if act.env.tiers_held.insert(tier) {
                events.push(self.rec(
                    ctx,
                    RecordBody::TierAcquired {
                        turn: turn.clone(),
                        tier,
                    },
                ));
            }
        }
        if !events.is_empty() {
            return Reconciled::Records(events);
        }

        // The lazy open (§5.3 item 1): the first sandboxed call opens the
        // environment, and the round waits for it rather than launching against
        // a half-ready activation. The provider runs over the facet's
        // materialized workspace directory (entity §7.11), which rehydration
        // built before any command could reach here. A delegation waits with
        // it, which costs one open and buys a launch path with no environment
        // in it.
        let sandboxed = desired
            .iter()
            .any(|p| matches!(p.effect, Effect::Tool { .. }));
        if sandboxed && act.env.slot.get().is_none() {
            // `begin_open` is the claim: true only from `Closed`, so exactly one
            // open is ever in flight and a round that finds one already going
            // just waits for it.
            if act.env.slot.begin_open() {
                match ctx.ws().dir_path() {
                    Ok(workspace) => self.launch_open(ctx, kind.profile.clone(), workspace),
                    // No workspace to run over, so nothing to open: give the
                    // claim back and fail the calls that needed it, the same
                    // batch a failed open produces. The set holds at least one
                    // sandboxed call, so the batch is never empty.
                    Err(e) => {
                        act.env.slot.take();
                        return Reconciled::Records(self.fail_sandboxed(
                            ctx,
                            turn,
                            desired,
                            &format!("workspace: {e}"),
                        ));
                    }
                }
            }
            return Reconciled::Waiting;
        }

        Reconciled::Ready
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

    /// Open the sandbox off the command path (§5.3): the task fills the
    /// [`Binding`] under the mutex and re-enters the cycle with a [`Tick`];
    /// success emits `SandboxBound` (A8).
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
                        a.env.slot.opened(sandbox);
                        true
                    }
                    Err(error) => {
                        a.env.slot.failed(error.to_string());
                        false
                    }
                }
            };
            if bound {
                system.emit(AgentEvent::SandboxBound { session, node }.into());
            }
            let _ = this.tell(Tick).await;
        }));
    }

    // -- notifications --------------------------------------------------------

    /// Notify every caller registered for a run that has finished (§7.3), then
    /// drop their registrations.
    fn notify_finished(
        &self,
        act: &mut Activation<S>,
        state: &SessionState,
        ctx: &EntityCtx<Agent<S>>,
    ) {
        // Which registered turns finished is a question only the fold answers
        // (workflow §7.1 item 4), so the candidates come out of the component
        // and the outcomes come out of `state`. Collected first, because `take`
        // needs a mutable borrow and reading the keys holds a shared one.
        let finished: Vec<(TurnId, RunOutcome)> = act
            .subscribers
            .keys()
            .filter_map(|turn| Some((turn.clone(), state.turns.get(turn)?.outcome.clone()?)))
            .collect();
        for (turn, outcome) in finished {
            for sub in act.subscribers.take(&turn) {
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

    /// Drop and release the bound sandbox, emitting `SandboxReleased` (A8).
    /// Synchronous teardown is launched off the command path.
    fn release_sandbox(&self, act: &mut Activation<S>, ctx: &EntityCtx<Agent<S>>) {
        if let Some(sandbox) = act.env.slot.take() {
            ctx.system()
                .launch(Box::pin(async move { sandbox.release().await }));
            self.emit_sandbox_released(ctx);
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
        // self-tells (the workflow's `Tick` and `Settled`) are local-only, so
        // no peer can inject them. Reading a run is not here:
        // `tail` rides entity's gateway-level event read (entity §7.5), a
        // framework built-in that never touches the activation (§10.2).
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
        drop(act);
        // Forget every claim and count, and arm the first tick: a resumed run
        // with work still outstanding is driven by that tick alone, with no
        // caller present (workflow §3.3, §4.4).
        self.engine.on_activate(ctx);
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
        // Release the sandbox on every deactivation: idle hibernation, migration,
        // or forced step-down (A8). The workspace goes with it (§5.5). Scope the
        // lock so the guard is dropped before the async release.
        let release = {
            let mut act = self.lock();
            act.env.slot.take()
        };
        if let Some(sandbox) = release {
            sandbox.release().await;
            self.emit_sandbox_released(ctx);
        }
    }

    fn can_hibernate(&self, state: &SessionState) -> bool {
        // The workflow answers for anything an effect represents: nothing in
        // flight and every member of the desired set at rest (workflow §6, W7).
        // This runtime narrows it exactly twice, for the two things the desired
        // set cannot see. A turn accepted into the queue but not yet started is
        // work owed *now* that no effect represents, and hibernating on it would
        // strand the turn until the next contact (§7.2). A sandbox mid-open is
        // an outcome the activation owes itself (§5.3, workflow §6).
        if !state.queue.is_empty() || !self.engine.quiescent() {
            return false;
        }
        !self.lock().env.slot.is_opening()
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

// The two the workflow requires, made a compile-time obligation by `Workflow`'s
// supertraits rather than a documented one (workflow §3).

impl<S: HarnessSystem> EntityHandler<Tick> for Agent<S> {
    async fn handle(
        &self,
        state: &SessionState,
        _msg: Tick,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<Record>, ()) {
        (self.tick(state, ctx), ())
    }
}

impl<S: HarnessSystem> EntityHandler<Settled<Self>> for Agent<S> {
    async fn handle(
        &self,
        state: &SessionState,
        msg: Settled<Self>,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<Record>, ()) {
        (self.engine.settled(self, state, msg, ctx).await, ())
    }
}

impl<S: HarnessSystem> Workflow for Agent<S> {
    type Key = StepKey;
    type Effect = Effect;
    type Outcome = StepOutcome;

    fn pending(&self, state: &SessionState) -> Vec<Pending<StepKey, Effect>> {
        crate::desired::desired(&self.kind, &self.shared.config, state)
    }

    /// Perform one effect (§3.1 steps 2 and 4): the runtime's two seams and the
    /// one operation that runs in the loop.
    ///
    /// Everything the launch needs is cloned out of `&self` here, before the
    /// future exists, because the future runs outside the activation and holds
    /// no borrow of it. The sandbox handle is read the same way: the pre-pass
    /// has already established that a desired tool call has one
    /// (`debug_assert_launchable`), so this only reads it.
    fn perform(
        &self,
        effect: Effect,
        cx: Perform<S>,
    ) -> impl Future<Output = Result<StepOutcome, EffectError>> + Send + 'static {
        let model = self.seams.model.clone();
        let sandbox = self.lock().env.slot.get().cloned();
        let shared = Arc::clone(&self.shared);
        let session = SessionId::new(cx.name().key());
        let system = cx.system().clone();
        async move {
            Ok(match effect {
                Effect::Model { request, .. } => StepOutcome::Model(model.complete(request).await),
                // Decided by the fold, performed by saying so (§5.4): an
                // unknown tool and a malformed input execute nothing.
                Effect::Fail { error, .. } => StepOutcome::Tool(Err(error)),
                Effect::Tool {
                    tier,
                    name,
                    input,
                    timeout,
                    ..
                } => {
                    let Some(sandbox) = sandbox else {
                        // Unreachable: the pre-pass binds the environment before
                        // a tool call is dispatched (§5.3 item 1). Resolved as a
                        // tool failure rather than left unsettled, because an
                        // effect that returns nothing strands its key.
                        return Ok(StepOutcome::Tool(Err(ToolError::Sandbox(format!(
                            "no sandbox bound when '{name}' was dispatched"
                        )))));
                    };
                    let call_fut = Box::pin(sandbox.call(tier, &name, input));
                    let sleep = system.sleep(timeout);
                    StepOutcome::Tool(match select(call_fut, sleep).await {
                        Either::Left((outcome, _)) => outcome,
                        Either::Right(((), _)) => Err(ToolError::Timeout),
                    })
                }
                // A delegation is control flow, not an effect on the world
                // (§5.2, §8): an ordinary entity submit with a reply-to mailbox,
                // retried on transport failure (safe; the derived `TurnId`
                // dedups, A7). The child's outcome becomes this call's tool
                // outcome (§5.4).
                Effect::Delegate {
                    turn,
                    prompt,
                    child,
                    root,
                    ..
                } => {
                    let lineage = Lineage {
                        session,
                        turn,
                        root,
                    };
                    let wait = shared.config.child_wait(&child.budget);
                    let cadence = shared.config.submit_deadline;
                    StepOutcome::Tool(match shared.hosts().get(&child.kind).cloned() {
                        // Unreachable on a node built through `HarnessBuilder`,
                        // which requires every hosted kind's delegates in the
                        // directory; kept as defense so a missing route degrades
                        // to a tool failure, never a panic in a launched task.
                        None => Err(ToolError::Delegation(format!(
                            "child kind '{}' is not hosted on this node",
                            child.kind
                        ))),
                        Some(entities) => {
                            let child_ref = entities.entity(child.session.as_str());
                            run_child(system, child_ref, &child, prompt, lineage, cadence, wait)
                                .await
                        }
                    })
                }
                // An owed cancel (§9.2 item 2): one `Cancel` to the child, which
                // is idempotent on its side (§9.2 item 1), so a repeat after a
                // lost ack is safe. A transport failure is the effect failing,
                // and the engine's backoff paces the retry (workflow §3.4);
                // the child's budget bounds the meantime (§9.2 item 3).
                Effect::Cancel { child, .. } => match shared.hosts().get(&child.kind).cloned() {
                    // Unreachable on a node built through `HarnessBuilder`, as
                    // for a delegation; the debt stays in the fold for a
                    // better-configured activation.
                    None => {
                        return Err(EffectError::Failed(format!(
                            "child kind '{}' is not hosted on this node",
                            child.kind
                        )));
                    }
                    Some(entities) => {
                        let child_ref = entities.entity(child.session.as_str());
                        child_ref
                            .ask(Cancel { turn: child.turn })
                            .await
                            .map_err(|e| EffectError::Failed(e.to_string()))?;
                        StepOutcome::Cancelled
                    }
                },
            })
        }
    }

    async fn settle(
        &self,
        state: &SessionState,
        key: &StepKey,
        outcome: Result<StepOutcome, EffectError>,
        ctx: &EntityCtx<Self>,
    ) -> Vec<Record> {
        match key {
            StepKey::Model(turn, step) => self.settle_model(state, turn, *step, outcome, ctx),
            StepKey::Call(turn, call) => self.settle_tool(state, turn, call, outcome, ctx),
            // The child acked: journal the `CancelDelivered` that clears the
            // owed fact (§9.2). Nothing else commits here, and nothing else is
            // needed: a failed send commits nothing, which is what tells the
            // engine to back the key off and try again (workflow §3.2 item 3).
            StepKey::Cancel(turn, call) => match outcome {
                Ok(StepOutcome::Cancelled) => vec![self.rec(
                    ctx,
                    RecordBody::CancelDelivered {
                        turn: turn.clone(),
                        call: call.clone(),
                    },
                )],
                _ => Vec::new(),
            },
        }
    }
}
