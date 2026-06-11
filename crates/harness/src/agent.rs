//! The session actor: the message-driven run loop (harness spec §3).
//!
//! A session, while active, is one ordinary actor whose private state is the
//! folded transcript plus the in-flight run. Its handlers **never block on
//! I/O** (§3.2): every external operation — the model call, a sandboxed tool,
//! a journal append, the load itself — is launched through the system spawner
//! and returns as a `when_local` closure on the actor's own executor, so the
//! mailbox stays live during a thirty-second model call and a `Cancel` takes
//! effect at message granularity. The step is a state the fold tracks
//! (§6.3), not a stack frame the executor holds: after every committed batch
//! the actor re-reads its fold and asks one question — *what does this state
//! call for next?* ([`advance`](AgentActor::advance)) — which is exactly why
//! a resumed activation continues identically to one that never stopped
//! (invariant H1).
//!
//! Appends are serialized through a one-in-flight queue carrying the fenced
//! `after` (§6.2); a `Stale` rejection deactivates with nothing further
//! journaled or issued (invariant H2). The write-ahead discipline (§6.4) is
//! structural: a model response is journaled before its calls launch (call
//! launching *is* the post-commit hook of the response batch), an outcome
//! before the next step's model call, and a final message with its `RunEnded`
//! in one atomic batch.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::BoxError;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::Instant;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::StopReason;
use actor_core::Supervision;
use futures::channel::oneshot;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::client::Harness;
use crate::client::HarnessSystem;
use crate::event::HarnessEvent;
use crate::host::Awaited;
use crate::host::Kind;
use crate::journal::AppendError;
use crate::journal::SeqNo;
use crate::model::ModelError;
use crate::model::ModelRequest;
use crate::model::ModelResponse;
use crate::model::ToolSpec;
use crate::sandbox::Sandbox;
use crate::session::CallId;
use crate::session::ChildRef;
use crate::session::Completion;
use crate::session::KindId;
use crate::session::Lineage;
use crate::session::Record;
use crate::session::RecordBody;
use crate::session::RunError;
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

/// Page size for the activation load.
const LOAD_PAGE: usize = 256;
/// Backoff cap for the delegation and cancel-propagation retry loops.
const PROPAGATION_BACKOFF_CAP: Duration = Duration::from_secs(2);
/// Attempt bound for the delegation retry loop (§8.1): past it, the
/// unreachable child surfaces to the parent's model as a tool failure (§5.4)
/// rather than looping forever.
const DELEGATION_ATTEMPTS: u32 = 32;

/// A submission as the host hands it over (§7.2): the resolved kind, the
/// turn, the lineage when delegated, and the caller's reply sender — which is
/// why this rides a `when_local` closure and never a wire message.
pub(crate) struct SubmitOp {
    pub kind: KindId,
    pub kind_def: Arc<Kind>,
    pub turn: Turn,
    pub parent: Option<Lineage>,
    pub tx: oneshot::Sender<Awaited>,
}

/// The self-nudge that lets a sync method stop the actor: `Ctx::stop` exists
/// only inside handlers, so a method that decides to stop sets a flag and
/// pokes its own mailbox.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Poke {}

impl Message for Poke {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("harness.agent.Poke");
}

/// An operation that arrived before the journal finished loading.
enum QueuedOp {
    Submit(SubmitOp),
    Cancel(TurnId),
}

/// A turn accepted but not yet journaled: it starts when the journal says the
/// previous run ended (§3.1 — the journal's total order serializes runs).
struct QueuedTurn {
    kind: KindId,
    kind_def: Arc<Kind>,
    turn: Turn,
    parent: Option<Lineage>,
}

/// A sandboxed execution waiting for the environment to open (§5.3 item 1).
struct PendingExec {
    turn: TurnId,
    call: CallId,
    name: String,
    input: Value,
    timeout: Duration,
}

/// The activation's sandbox binding (§5.3): at most one live sandbox per
/// activation (invariant H8).
enum SandboxSlot {
    Closed,
    Opening(Vec<PendingExec>),
    Open(Arc<dyn Sandbox>),
}

/// The working state of a live activation.
struct Ready {
    state: SessionState,
    /// Resolved from the journaled `SessionCreated`, or pinned by the first
    /// submission (§7.1).
    kind: Option<Arc<Kind>>,
    /// The journaled kind is not registered on this node — a deployment
    /// error (§7.1): every triggering call fails, nothing is journaled.
    kind_missing: bool,
    /// The fenced append pipeline: one batch in flight, the rest queued. The
    /// in-flight records are kept to fold on commit.
    inflight: Option<Vec<Record>>,
    backlog: VecDeque<Vec<Record>>,
    /// Callers attached per turn (§7.4): resolved when the turn's outcome is
    /// known, `Lost` on deactivation.
    waiters: BTreeMap<TurnId, Vec<oneshot::Sender<Awaited>>>,
    queue: VecDeque<QueuedTurn>,
    /// A `TurnSubmitted` is in the append pipeline; don't start another.
    starting: Option<TurnId>,
    /// A `RunEnded` is in the append pipeline; the run is over for every
    /// decision this activation makes (outcomes arriving now are stragglers,
    /// §3.2).
    ending: Option<TurnId>,
    /// The §5.5 workspace-loss protocol: a fresh sandbox will not match what
    /// the transcript asserts, so a `WorkspaceReset` must be journaled before
    /// the next model call.
    reset_needed: bool,
    reset_enqueued: bool,
    sandbox: SandboxSlot,
    /// `SandboxBound` was emitted and not yet paired (H8).
    sandbox_bound: bool,
    /// Calls launched by this activation (a fold-external flag: relaunching
    /// is a *resume* decision, §5.5, not a fold one).
    launched: BTreeSet<CallId>,
    /// Calls whose outcome is enqueued or committed: dedups a straggler
    /// outcome racing its own timeout.
    resolved: BTreeSet<CallId>,
    model_inflight: bool,
    last_activity: Instant,
    /// Deactivate once the in-flight append commits (§7.2).
    draining: bool,
}

/// The activation's lifecycle.
enum Phase {
    /// `started` launched the journal load; operations queue until it lands.
    Boot {
        queued: Vec<QueuedOp>,
    },
    Ready(Box<Ready>),
    /// Fenced off or deactivated: every further input is discarded (H2). The
    /// sandbox, if any, is carried so `stopped` can release it.
    Gone {
        sandbox: Option<Arc<dyn Sandbox>>,
    },
}

/// A session's hosting actor (harness spec §2.1, §3): the disposable fold of
/// the journal plus the in-flight run. Stopping it loses nothing.
pub struct AgentActor<S: HarnessSystem> {
    harness: Harness<S>,
    session: SessionId,
    phase: Phase,
    me: Option<ActorRef<AgentActor<S>>>,
    /// `SessionActivated` was emitted; gates the pairing `SessionDeactivated`.
    activated: bool,
    sandbox_was_bound_on_stop: bool,
    stop_requested: bool,
}

impl<S: HarnessSystem> AgentActor<S> {
    pub(crate) fn new(harness: Harness<S>, session: SessionId) -> AgentActor<S> {
        AgentActor {
            harness,
            session,
            phase: Phase::Boot { queued: Vec::new() },
            me: None,
            activated: false,
            sandbox_was_bound_on_stop: false,
            stop_requested: false,
        }
    }

    fn me(&self) -> ActorRef<AgentActor<S>> {
        self.me.clone().expect("self reference set in started")
    }

    fn now(&self) -> Instant {
        self.harness.clock().now()
    }

    /// Set the stop flag and nudge the mailbox so a handler runs and applies
    /// it.
    fn request_stop(&mut self) {
        if self.stop_requested {
            return;
        }
        self.stop_requested = true;
        let me = self.me();
        self.harness.system().launch(Box::pin(async move {
            let _ = me.tell(Poke {}).await;
        }));
    }

    // -- operations handed over by the host (§7.2) --------------------------

    /// Accept, attach, or dedup a submission (§7.4).
    pub(crate) fn submit(&mut self, op: SubmitOp) {
        match &mut self.phase {
            Phase::Boot { queued } => queued.push(QueuedOp::Submit(op)),
            Phase::Gone { .. } => {
                let _ = op.tx.send(Awaited::Lost);
            }
            Phase::Ready(_) => {
                self.accept_submit(op);
                self.advance();
            }
        }
    }

    /// Cancel the run `turn` names (§9.2): idempotent — ended or unknown is a
    /// no-op.
    pub(crate) fn cancel(&mut self, turn: TurnId) {
        match &mut self.phase {
            Phase::Boot { queued } => queued.push(QueuedOp::Cancel(turn)),
            Phase::Gone { .. } => {}
            Phase::Ready(r) => {
                r.last_activity = self.harness.clock().now();
                let live_is_target = r.state.live.as_ref().is_some_and(|live| live.turn == turn);
                if live_is_target && r.ending.is_none() {
                    r.ending = Some(turn.clone());
                    self.enqueue(vec![RecordBody::RunEnded {
                        turn,
                        outcome: Err(RunError::Cancelled),
                    }]);
                } else if let Some(i) = r.queue.iter().position(|q| q.turn.id == turn) {
                    // Not yet journaled: nothing started, so nothing to
                    // record — the queued turn simply never runs.
                    r.queue.remove(i);
                    notify(r, &turn, || Awaited::Outcome(Err(RunError::Cancelled)));
                }
            }
        }
    }

    /// The host's deactivation sweep (§7.2): wind down when the view no
    /// longer names this node owner, or on idleness — never while a run is
    /// live.
    pub(crate) fn sweep(&mut self, owned: bool, idle_timeout: Duration, now: Instant) {
        let Phase::Ready(r) = &mut self.phase else {
            return;
        };
        if r.draining {
            return;
        }
        let idle = r.state.live.is_none()
            && r.queue.is_empty()
            && r.starting.is_none()
            && r.inflight.is_none()
            && r.backlog.is_empty()
            && now.duration_since(r.last_activity) >= idle_timeout;
        if !owned || idle {
            r.draining = true;
            if r.inflight.is_none() {
                self.go_gone();
            }
        }
    }

    // -- submission details --------------------------------------------------

    fn accept_submit(&mut self, op: SubmitOp) {
        let now = self.now();
        let Phase::Ready(r) = &mut self.phase else {
            return;
        };
        r.last_activity = now;
        if r.kind_missing {
            let _ = op.tx.send(Awaited::Rejected(format!(
                "kind of session {} is not registered on this node (deployment error, §7.1)",
                op.turn.id
            )));
            return;
        }
        if let Some(created) = &r.state.created
            && created.kind != op.kind
        {
            // A checked redundancy, rejected on mismatch rather than ignored
            // (§7.3): journaling nothing.
            let _ = op.tx.send(Awaited::Rejected(format!(
                "kind mismatch: session created as '{}', submitted as '{}'",
                created.kind, op.kind
            )));
            return;
        }
        let digest = content_digest(&op.turn.content);
        // Dedup against the journal (§7.4): the recorded outcome, or an
        // attach to the live or queued run — never a second run (H7).
        if let Some(facts) = r.state.turns.get(&op.turn.id) {
            if facts.content_digest != digest {
                let _ = op.tx.send(Awaited::Rejected(format!(
                    "turn {} re-submitted with different content",
                    op.turn.id
                )));
                return;
            }
            match &facts.outcome {
                Some(outcome) => {
                    let _ = op.tx.send(Awaited::Outcome(outcome.clone()));
                }
                None => attach(r, op.turn.id, op.tx),
            }
            return;
        }
        if let Some(queued) = r.queue.iter().find(|q| q.turn.id == op.turn.id) {
            if content_digest(&queued.turn.content) != digest {
                let _ = op.tx.send(Awaited::Rejected(format!(
                    "turn {} re-submitted with different content",
                    op.turn.id
                )));
            } else {
                attach(r, op.turn.id, op.tx);
            }
            return;
        }
        attach(r, op.turn.id.clone(), op.tx);
        r.queue.push_back(QueuedTurn {
            kind: op.kind,
            kind_def: op.kind_def,
            turn: op.turn,
            parent: op.parent,
        });
    }

    // -- activation (§7.5) ---------------------------------------------------

    /// The journal landed: fold, resolve the kind, resolve dangling calls,
    /// surface workspace loss, and continue (§6.3, §5.5).
    fn loaded(&mut self, result: Result<Vec<(SeqNo, Record)>, String>) {
        let Phase::Boot { queued } = &mut self.phase else {
            return;
        };
        let queued = std::mem::take(queued);
        let records = match result {
            Ok(records) => records,
            Err(_unavailable) => {
                // Cannot fold, so cannot serve: the contact that triggered
                // this activation retries against a fresh one (§6.5).
                for op in queued {
                    if let QueuedOp::Submit(op) = op {
                        let _ = op.tx.send(Awaited::Lost);
                    }
                }
                self.phase = Phase::Gone { sandbox: None };
                self.request_stop();
                return;
            }
        };
        let state = SessionState::fold(&records);
        let (kind, kind_missing) = match &state.created {
            Some(created) => match self.harness.kinds().get(&created.kind) {
                Some(kind) => (Some(kind), false),
                None => (None, true),
            },
            None => (None, false),
        };
        let resumed = state.live.clone();
        let reset_needed = state.sandbox_activity;
        self.phase = Phase::Ready(Box::new(Ready {
            state,
            kind,
            kind_missing,
            inflight: None,
            backlog: VecDeque::new(),
            waiters: BTreeMap::new(),
            queue: VecDeque::new(),
            starting: None,
            ending: None,
            reset_needed,
            reset_enqueued: false,
            sandbox: SandboxSlot::Closed,
            sandbox_bound: false,
            launched: BTreeSet::new(),
            resolved: BTreeSet::new(),
            model_inflight: false,
            last_activity: self.harness.clock().now(),
            draining: false,
        }));
        self.activated = true;
        let node = self.harness.system().node();
        self.harness.system().emit_event(
            HarnessEvent::SessionActivated {
                session: self.session.clone(),
                node,
            }
            .into(),
        );
        if let Some(live) = &resumed {
            self.harness.system().emit_event(
                HarnessEvent::RunResumed {
                    session: self.session.clone(),
                    turn: live.turn.clone(),
                    node,
                }
                .into(),
            );
            self.resolve_dangling();
        }
        // The deactivation sweep (§7.2), scoped to this activation: each tick
        // re-checks ownership against the local view and idleness, and the
        // agent winds itself down when either says to. The loop ends with the
        // activation — a node with no live session schedules nothing, so a
        // quiescence-driven simulation can quiesce.
        {
            let harness = self.harness.clone();
            let session = self.session.clone();
            let me = self.me();
            let clock = harness.clock();
            let tick = harness.config().tick_interval;
            let idle = harness.config().idle_timeout;
            let launcher = harness.clone();
            launcher.system().launch(Box::pin(async move {
                loop {
                    clock.sleep(tick).await;
                    let owned = harness.system().owner_of(session.as_str().as_bytes())
                        == Some(harness.system().node());
                    let now = clock.now();
                    if me
                        .when_local(move |agent: &mut AgentActor<S>| agent.sweep(owned, idle, now))
                        .await
                        .is_none()
                    {
                        // The activation stopped; the sweep dies with it.
                        return;
                    }
                }
            }));
        }
        for op in queued {
            match op {
                QueuedOp::Submit(op) => self.submit(op),
                QueuedOp::Cancel(turn) => self.cancel(turn),
            }
        }
        self.advance();
    }

    /// Resolve calls whose intent is journaled but whose outcome is not
    /// (§5.5): re-execute or interrupt, per declaration. `delegate`
    /// re-executes by construction — its re-submission dedups into an attach
    /// (§8.1).
    fn resolve_dangling(&mut self) {
        let Phase::Ready(r) = &mut self.phase else {
            return;
        };
        let Some(live) = &r.state.live else {
            return;
        };
        let turn = live.turn.clone();
        let mut interrupts = Vec::new();
        for (call, pending) in &live.pending {
            let policy = if pending.name == DELEGATE {
                OnDangling::Reexecute
            } else {
                r.kind
                    .as_ref()
                    .and_then(|k| k.tools.get(&pending.name))
                    .map(|decl| decl.on_dangling)
                    // The declaration vanished (the kind changed mid-session):
                    // the safe policy.
                    .unwrap_or(OnDangling::Interrupt)
            };
            // Re-executions need no marking: `advance` launches any pending
            // call not yet launched by this activation, which on resume is
            // exactly blind re-execution (§5.5).
            if policy == OnDangling::Interrupt {
                r.resolved.insert(call.clone());
                interrupts.push(call.clone());
            }
        }
        for call in interrupts {
            self.enqueue(vec![RecordBody::ToolOutcome {
                turn: turn.clone(),
                call,
                outcome: Err(ToolError::Interrupted),
            }]);
        }
    }

    // -- the dispatcher (§3.1) ----------------------------------------------

    /// What does the fold call for next? Idempotent: every path is guarded by
    /// a fold fact or an in-flight flag, so calling it after any transition
    /// is safe — which is what makes resume the same code path as progress.
    fn advance(&mut self) {
        self.pump();
        let harness = self.harness.clone();
        let session = self.session.clone();
        let me = self.me();
        let Phase::Ready(r) = &mut self.phase else {
            return;
        };
        if r.draining || r.kind_missing {
            return;
        }

        // Start the next queued turn once the journal shows no live run.
        if r.state.live.is_none() && r.starting.is_none() && r.ending.is_none() {
            if let Some(next) = r.queue.front() {
                let mut batch = Vec::new();
                if r.state.created.is_none() {
                    let root = next
                        .parent
                        .as_ref()
                        .map(|p| p.root.clone())
                        .unwrap_or_else(|| session.clone());
                    batch.push(RecordBody::SessionCreated {
                        kind: next.kind.clone(),
                        digest: next.kind_def.digest(),
                        parent: next.parent.clone(),
                        root,
                    });
                }
                batch.push(RecordBody::TurnSubmitted {
                    turn: next.turn.id.clone(),
                    content: next.turn.content.clone(),
                    budget: next.turn.budget.unwrap_or(next.kind_def.default_budget),
                });
                r.starting = Some(next.turn.id.clone());
                if r.kind.is_none() {
                    r.kind = Some(Arc::clone(&next.kind_def));
                }
                let _ = r; // end the borrow before enqueue
                self.enqueue(batch);
                return self.advance_rest(harness, session, me);
            }
        }
        self.advance_rest(harness, session, me);
    }

    /// The live-run half of [`advance`]; split only to satisfy the borrow
    /// checker across the enqueue above.
    fn advance_rest(&mut self, harness: Harness<S>, session: SessionId, me: ActorRef<Self>) {
        let Phase::Ready(r) = &mut self.phase else {
            return;
        };
        if r.draining || r.kind_missing || r.ending.is_some() {
            return;
        }
        let Some(live) = &r.state.live else {
            return;
        };
        let turn = live.turn.clone();
        let Some(kind) = r.kind.clone() else {
            return;
        };

        if live.pending.is_empty() {
            if r.model_inflight || r.reset_enqueued {
                return;
            }
            // Pre-call budget enforcement (§9.1 item 2).
            let floor = harness.config().budget_floor;
            if !live.spend.allows_call(&live.budget, floor) {
                r.ending = Some(turn.clone());
                self.enqueue(vec![RecordBody::RunEnded {
                    turn,
                    outcome: Err(RunError::BudgetExhausted),
                }]);
                return;
            }
            // Surface a lost workspace before the model acts on state that is
            // gone (§5.5).
            if r.reset_needed {
                r.reset_enqueued = true;
                self.enqueue(vec![RecordBody::WorkspaceReset]);
                return;
            }
            // One model call per step (§3.1), launched, never awaited inline
            // (§3.2).
            r.model_inflight = true;
            let max_tokens = kind
                .params
                .max_tokens
                .min(live.spend.remaining_tokens(&live.budget));
            let mut tools: Vec<ToolSpec> = kind.tools.specs();
            if !kind.delegates.is_empty() {
                tools.push(delegate_spec(&kind));
            }
            let request = ModelRequest {
                system_prompt: kind.system_prompt.clone(),
                params: kind.params.clone(),
                tools,
                transcript: r.state.transcript.clone(),
                max_tokens,
            };
            let model = Arc::clone(harness.model());
            harness.system().launch(Box::pin(async move {
                let result = model.complete(request).await;
                let _ = me
                    .when_local(move |agent: &mut AgentActor<S>| agent.model_done(turn, result))
                    .await;
            }));
            return;
        }

        // Execute every journaled intent that is not yet launched or resolved
        // (§3.1 step 4; on resume this is dangling-call re-execution, §5.5).
        let to_launch: Vec<(CallId, String, Value, Option<ChildRef>)> = live
            .pending
            .iter()
            .filter(|(call, _)| !r.launched.contains(*call) && !r.resolved.contains(*call))
            .map(|(call, p)| {
                (
                    call.clone(),
                    p.name.clone(),
                    p.input.clone(),
                    p.child.clone(),
                )
            })
            .collect();
        for (call, name, input, child) in to_launch {
            self.launch_call(&kind, turn.clone(), call, name, input, child);
            let Phase::Ready(_) = &self.phase else {
                return;
            };
        }
        let _ = (harness, session);
    }

    fn launch_call(
        &mut self,
        kind: &Arc<Kind>,
        turn: TurnId,
        call: CallId,
        name: String,
        input: Value,
        child: Option<ChildRef>,
    ) {
        let harness = self.harness.clone();
        let session = self.session.clone();
        let me = self.me();
        let Phase::Ready(r) = &mut self.phase else {
            return;
        };
        r.launched.insert(call.clone());

        if name == DELEGATE {
            // Delegation is control flow, executed in the loop (§5.2, §8).
            let parsed: Result<DelegateInput, _> = serde_json::from_value(input.clone());
            let request = match parsed {
                Ok(request) => request,
                Err(e) => {
                    return self.synthesize_outcome(
                        turn,
                        call,
                        Err(ToolError::InvalidArguments(e.to_string())),
                    );
                }
            };
            let child_kind = KindId::new(request.kind.clone());
            if !kind.delegates.contains(&child_kind) {
                // The allowlist (§8.1): a locked-down kind cannot escalate by
                // delegating to a permissive one.
                return self.synthesize_outcome(
                    turn,
                    call,
                    Err(ToolError::InvalidArguments(format!(
                        "kind '{child_kind}' is not in this kind's delegation allowlist"
                    ))),
                );
            }
            let Some(child_def) = harness.kinds().get(&child_kind) else {
                return self.synthesize_outcome(
                    turn,
                    call,
                    Err(ToolError::InvalidArguments(format!(
                        "delegated kind '{child_kind}' is not registered"
                    ))),
                );
            };
            match child {
                // The intent is already journaled (a dangling re-execution):
                // re-submit with the recorded identifiers and budget (§5.5).
                Some(child) => self.launch_delegation(turn, call, request, child),
                // Fresh delegation: journal the `ChildRun` intent first
                // (§8.1 step 1); the submit launches when it commits.
                None => {
                    let Phase::Ready(r) = &mut self.phase else {
                        return;
                    };
                    let Some(live) = &r.state.live else {
                        return;
                    };
                    let (child_session, child_turn) = derive_child(&session, &turn, &call);
                    let requested = request.budget.unwrap_or(child_def.default_budget);
                    let budget = live.spend.carve(&live.budget, requested);
                    self.enqueue(vec![RecordBody::ChildRun {
                        turn,
                        call,
                        child_session,
                        child_turn,
                        budget,
                    }]);
                }
            }
            return;
        }

        // Sandboxed tools: dispatch by name against the registry and nothing
        // else (§5.2).
        let Some(decl) = kind.tools.get(&name) else {
            return self.synthesize_outcome(turn, call, Err(ToolError::UnknownTool { name }));
        };
        if !input.is_object() {
            return self.synthesize_outcome(
                turn,
                call,
                Err(ToolError::InvalidArguments(
                    "tool input must be a JSON object".to_string(),
                )),
            );
        }
        let exec = PendingExec {
            turn,
            call,
            name,
            input,
            timeout: decl.timeout.unwrap_or(harness.config().tool_timeout),
        };
        let Phase::Ready(r) = &mut self.phase else {
            return;
        };
        match &mut r.sandbox {
            SandboxSlot::Open(sandbox) => {
                let sandbox = Arc::clone(sandbox);
                launch_exec(&harness, &me, sandbox, exec);
            }
            SandboxSlot::Opening(queue) => queue.push(exec),
            SandboxSlot::Closed => {
                // Lazy open on the first sandboxed call (§5.3 item 1).
                r.sandbox = SandboxSlot::Opening(vec![exec]);
                let profile = kind.profile.clone();
                let provider = Arc::clone(harness.sandboxes());
                harness.system().launch(Box::pin(async move {
                    let result = provider.open(&session, &profile).await;
                    let _ = me
                        .when_local(move |agent: &mut AgentActor<S>| agent.sandbox_opened(result))
                        .await;
                }));
            }
        }
    }

    /// A synthesized outcome for a call that executed nowhere (§5.4): unknown
    /// name, malformed arguments, failed provisioning.
    fn synthesize_outcome(
        &mut self,
        turn: TurnId,
        call: CallId,
        outcome: Result<Value, ToolError>,
    ) {
        self.tool_done(turn, call, outcome);
    }

    /// Launch the child submission of a journaled delegation (§8.1 steps
    /// 2–3): an ordinary `SessionRef` submit, retried on reported transport
    /// failure — safe because the derived `TurnId` dedups (H7).
    fn launch_delegation(
        &mut self,
        turn: TurnId,
        call: CallId,
        request: DelegateInput,
        child: ChildRef,
    ) {
        let harness = self.harness.clone();
        let me = self.me();
        let Phase::Ready(r) = &self.phase else {
            return;
        };
        let root = r
            .state
            .created
            .as_ref()
            .map(|c| c.root.clone())
            .unwrap_or_else(|| self.session.clone());
        let lineage = Lineage {
            session: self.session.clone(),
            turn: turn.clone(),
            root,
        };
        let clock = harness.clock();
        let deadline = harness.config().submit_deadline;
        let launcher = harness.clone();
        launcher.system().launch(Box::pin(async move {
            let child_ref = harness.session(&request.kind, child.session.clone());
            let child_turn = Turn {
                id: child.turn.clone(),
                content: request.prompt.clone(),
                budget: Some(child.budget),
            };
            let mut attempt: u32 = 0;
            let outcome = loop {
                match child_ref
                    .submit(child_turn.clone(), Some(lineage.clone()), deadline)
                    .await
                {
                    Ok(Ok(completion)) => break Ok(Value::String(completion.text().to_string())),
                    Ok(Err(run_error)) => break Err(ToolError::Delegation(run_error)),
                    Err(_call_error) if attempt < DELEGATION_ATTEMPTS => {
                        attempt += 1;
                        clock.sleep(propagation_backoff(attempt)).await;
                    }
                    Err(call_error) => {
                        // The child stayed unreachable past the retry bound:
                        // surface it as a tool failure for the model (§5.4);
                        // the journaled intent re-executes on a later resume.
                        break Err(ToolError::Delegation(RunError::Journal(format!(
                            "child unreachable: {call_error}"
                        ))));
                    }
                }
            };
            let _ = me
                .when_local(move |agent: &mut AgentActor<S>| agent.tool_done(turn, call, outcome))
                .await;
        }));
    }

    // -- outcomes arriving from launched tasks -------------------------------

    fn model_done(&mut self, turn: TurnId, result: Result<ModelResponse, ModelError>) {
        let node = self.harness.system().node();
        let session = self.session.clone();
        let Phase::Ready(r) = &mut self.phase else {
            return;
        };
        r.model_inflight = false;
        r.last_activity = self.harness.clock().now();
        let live = match &r.state.live {
            Some(live) if live.turn == turn && r.ending.is_none() => live,
            // The run ended while the call was in flight (a cancel, §9.2):
            // discard, do not journal (§3.2) — and emit nothing, so the event
            // stream stays scoped to journaled spend (§10.4).
            _ => return,
        };
        match result {
            Ok(mut response) => {
                // Assign call ids the provider omitted (§5.2): deterministic
                // in the step and position.
                let step = live.spend.own_steps + 1;
                for (i, call) in response.calls.iter_mut().enumerate() {
                    if call.id.as_str().is_empty() {
                        call.id = CallId::new(format!("call-{step}-{i}"));
                    }
                }
                self.harness.system().emit_event(
                    HarnessEvent::ModelCompleted {
                        session,
                        turn: turn.clone(),
                        node,
                        usage: response.usage.total(),
                    }
                    .into(),
                );
                if response.is_final() {
                    // Final message and terminal outcome in one atomic batch
                    // (§6.4): no journal prefix ends between them.
                    let tokens = live.spend.tokens() + response.usage.total();
                    let completion = Completion::new(response.content.clone(), tokens);
                    r.ending = Some(turn.clone());
                    self.enqueue(vec![
                        RecordBody::ModelResponse {
                            turn: turn.clone(),
                            content: response.content,
                            calls: Vec::new(),
                            usage: response.usage,
                        },
                        RecordBody::RunEnded {
                            turn,
                            outcome: Ok(completion),
                        },
                    ]);
                } else {
                    self.enqueue(vec![RecordBody::ModelResponse {
                        turn,
                        content: response.content,
                        calls: response.calls,
                        usage: response.usage,
                    }]);
                }
            }
            Err(error) => {
                // A failure no retry policy absorbed ends the run: journaled,
                // reported, never swallowed (§4.3).
                r.ending = Some(turn.clone());
                self.enqueue(vec![RecordBody::RunEnded {
                    turn,
                    outcome: Err(RunError::Model(error)),
                }]);
            }
        }
    }

    fn tool_done(&mut self, turn: TurnId, call: CallId, outcome: Result<Value, ToolError>) {
        let node = self.harness.system().node();
        let Phase::Ready(r) = &mut self.phase else {
            return;
        };
        r.last_activity = self.harness.clock().now();
        let live_matches = r
            .state
            .live
            .as_ref()
            .is_some_and(|live| live.turn == turn && live.pending.contains_key(&call));
        if !live_matches || r.ending.is_some() || r.resolved.contains(&call) {
            // A straggler of an ended or cancelled run, or a duplicate
            // (timeout racing the real outcome): discarded, not journaled
            // (§3.2, §9.2 item 4).
            return;
        }
        r.resolved.insert(call.clone());
        if let Err(ToolError::EnvironmentLost(_)) = &outcome {
            // The provider lost the workspace (§5.5): drop the binding and
            // arrange the reset; the next sandboxed call opens fresh.
            if let SandboxSlot::Open(sandbox) =
                std::mem::replace(&mut r.sandbox, SandboxSlot::Closed)
            {
                self.harness
                    .system()
                    .launch(Box::pin(async move { sandbox.release().await }));
            }
            if r.sandbox_bound {
                r.sandbox_bound = false;
                self.harness.system().emit_event(
                    HarnessEvent::SandboxReleased {
                        session: self.session.clone(),
                        node,
                    }
                    .into(),
                );
            }
            let Phase::Ready(r) = &mut self.phase else {
                return;
            };
            r.reset_needed = true;
        }
        self.enqueue(vec![RecordBody::ToolOutcome {
            turn,
            call,
            outcome,
        }]);
    }

    fn sandbox_opened(&mut self, result: Result<Arc<dyn Sandbox>, crate::sandbox::SandboxError>) {
        let harness = self.harness.clone();
        let me = self.me();
        let node = harness.system().node();
        let session = self.session.clone();
        match &mut self.phase {
            Phase::Ready(r) => {
                let queued = match std::mem::replace(&mut r.sandbox, SandboxSlot::Closed) {
                    SandboxSlot::Opening(queued) => queued,
                    other => {
                        r.sandbox = other;
                        return;
                    }
                };
                match result {
                    Ok(sandbox) => {
                        r.sandbox = SandboxSlot::Open(Arc::clone(&sandbox));
                        r.sandbox_bound = true;
                        harness
                            .system()
                            .emit_event(HarnessEvent::SandboxBound { session, node }.into());
                        for exec in queued {
                            launch_exec(&harness, &me, Arc::clone(&sandbox), exec);
                        }
                    }
                    Err(error) => {
                        // Provisioning failed: every queued call fails as a
                        // transcript value (§5.4); a later call retries the
                        // open.
                        for exec in queued {
                            self.synthesize_outcome(
                                exec.turn,
                                exec.call,
                                Err(ToolError::Sandbox(error.to_string())),
                            );
                            if !matches!(self.phase, Phase::Ready(_)) {
                                return;
                            }
                        }
                    }
                }
            }
            Phase::Gone { .. } | Phase::Boot { .. } => {
                // The activation went away while the environment was opening:
                // release it immediately (H8).
                if let Ok(sandbox) = result {
                    harness
                        .system()
                        .launch(Box::pin(async move { sandbox.release().await }));
                }
            }
        }
    }

    // -- the fenced append pipeline (§6.2, §6.4) ------------------------------

    /// Queue a batch for the fenced append pipeline, stamping each record
    /// with this node's clock reading (§10.1).
    fn enqueue(&mut self, bodies: Vec<RecordBody>) {
        let at_nanos = self.harness.clock().now().as_nanos();
        let Phase::Ready(r) = &mut self.phase else {
            return;
        };
        r.backlog.push_back(
            bodies
                .into_iter()
                .map(|body| Record { at_nanos, body })
                .collect(),
        );
        self.pump();
    }

    /// Send the next batch if none is in flight. `after` is the fold's head:
    /// one batch in flight at a time is what keeps the fence's condition
    /// equal to this activation's own knowledge (§6.2).
    fn pump(&mut self) {
        let harness = self.harness.clone();
        let session = self.session.clone();
        let me = self.me();
        let Phase::Ready(r) = &mut self.phase else {
            return;
        };
        if r.inflight.is_some() {
            return;
        }
        let Some(batch) = r.backlog.pop_front() else {
            return;
        };
        r.inflight = Some(batch.clone());
        let after = r.state.head;
        let journal = Arc::clone(harness.journal());
        let clock = harness.clock();
        let attempts = harness.config().journal_attempts.max(1);
        let backoff = harness.config().journal_backoff;
        harness.system().launch(Box::pin(async move {
            // Bounded retries absorb a transient outage (§6.5); `Stale` is
            // never retried — it is the fence speaking (§6.2).
            let mut attempt = 0;
            let result = loop {
                attempt += 1;
                match journal.append(&session, after, batch.clone()).await {
                    Err(AppendError::Unavailable(e)) if attempt < attempts => {
                        let factor = 2u32.saturating_pow(attempt - 1);
                        clock.sleep(backoff * factor).await;
                        let _ = e;
                    }
                    other => break other,
                }
            };
            let _ = me
                .when_local(move |agent: &mut AgentActor<S>| agent.append_done(result))
                .await;
        }));
    }

    fn append_done(&mut self, result: Result<SeqNo, AppendError>) {
        let node = self.harness.system().node();
        let session = self.session.clone();
        let Phase::Ready(r) = &mut self.phase else {
            return;
        };
        let Some(batch) = r.inflight.take() else {
            return;
        };
        match result {
            Ok(_head) => {
                // Snapshot the live children before the fold consumes the
                // batch: a committed cancellation propagates to them (§9.2).
                let cancelled_children: Vec<ChildRef> = if batch.iter().any(|rec| {
                    matches!(
                        &rec.body,
                        RecordBody::RunEnded {
                            outcome: Err(RunError::Cancelled),
                            ..
                        }
                    )
                }) {
                    r.state
                        .live
                        .as_ref()
                        .map(|live| {
                            live.pending
                                .values()
                                .filter_map(|p| p.child.clone())
                                .collect()
                        })
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };

                let base = r.state.head.0;
                for (i, record) in batch.iter().enumerate() {
                    r.state.apply(SeqNo(base + 1 + i as u64), record);
                }
                let _ = r;
                self.committed(&batch, cancelled_children);
            }
            Err(AppendError::Stale { .. }) => {
                // Another activation owns the order now: deactivate, journal
                // nothing further, issue nothing further (H2).
                self.harness
                    .system()
                    .emit_event(HarnessEvent::AppendRejected { session, node }.into());
                self.go_gone();
            }
            Err(AppendError::Unavailable(e)) => {
                // The retries did not absorb it: the session cannot record
                // (§6.5). The run pauses where the journal last saw it; the
                // terminal error reaches attached callers best-effort, and a
                // later contact resumes from the committed prefix.
                let run_turn = r
                    .state
                    .live
                    .as_ref()
                    .map(|live| live.turn.clone())
                    .or_else(|| r.starting.clone());
                if let Some(turn) = run_turn {
                    notify(r, &turn, || {
                        Awaited::Outcome(Err(RunError::Journal(e.clone())))
                    });
                }
                self.go_gone();
            }
        }
    }

    /// Post-commit hooks: the effects whose intent the batch just made
    /// durable (§6.4 — intent before effect, so launching here *is* the
    /// write-ahead discipline).
    fn committed(&mut self, batch: &[Record], cancelled_children: Vec<ChildRef>) {
        let harness = self.harness.clone();
        for record in batch {
            match &record.body {
                RecordBody::TurnSubmitted { turn, .. } => {
                    let (parent, popped) = {
                        let Phase::Ready(r) = &mut self.phase else {
                            return;
                        };
                        r.starting = None;
                        let popped = r.queue.pop_front();
                        (
                            r.state
                                .created
                                .as_ref()
                                .and_then(|c| c.parent.as_ref())
                                .map(|p| p.session.clone()),
                            popped,
                        )
                    };
                    debug_assert!(popped.is_some_and(|q| &q.turn.id == turn));
                    harness.system().emit_event(
                        HarnessEvent::RunStarted {
                            session: self.session.clone(),
                            turn: turn.clone(),
                            parent,
                        }
                        .into(),
                    );
                }
                RecordBody::ChildRun { turn, call, .. } => {
                    // The delegation's intent is durable: submit the child
                    // (§8.1 step 2).
                    let Phase::Ready(r) = &self.phase else {
                        return;
                    };
                    if r.ending.is_some() {
                        continue;
                    }
                    let Some(live) = &r.state.live else {
                        continue;
                    };
                    let Some(pending) = live.pending.get(call) else {
                        continue;
                    };
                    let Some(child) = pending.child.clone() else {
                        continue;
                    };
                    let Ok(request) =
                        serde_json::from_value::<DelegateInput>(pending.input.clone())
                    else {
                        continue;
                    };
                    self.launch_delegation(turn.clone(), call.clone(), request, child);
                }
                RecordBody::WorkspaceReset => {
                    // The loss is on the record (§5.5): the next model call
                    // may proceed, and carries the reset in its transcript.
                    let Phase::Ready(r) = &mut self.phase else {
                        return;
                    };
                    r.reset_needed = false;
                    r.reset_enqueued = false;
                }
                RecordBody::RunEnded { turn, outcome } => {
                    harness.system().emit_event(
                        HarnessEvent::RunEnded {
                            session: self.session.clone(),
                            turn: turn.clone(),
                            outcome: outcome_label(outcome),
                        }
                        .into(),
                    );
                    let Phase::Ready(r) = &mut self.phase else {
                        return;
                    };
                    r.ending = None;
                    r.model_inflight = false;
                    r.launched.clear();
                    r.resolved.clear();
                    // Release the reply only after the terminal outcome is
                    // journaled (§6.4): a caller never holds a completion the
                    // journal could lose.
                    let outcome = outcome.clone();
                    notify(r, turn, move || Awaited::Outcome(outcome.clone()));
                    // Propagate the cancel to every live child recorded in
                    // the journal (§9.2 item 2).
                    for child in &cancelled_children {
                        self.propagate_cancel(child.clone());
                        if !matches!(self.phase, Phase::Ready(_)) {
                            return;
                        }
                    }
                }
                _ => {}
            }
        }
        // The fold moved; ask it what comes next.
        if let Phase::Ready(r) = &mut self.phase
            && r.draining
            && r.inflight.is_none()
        {
            self.go_gone();
            return;
        }
        self.advance();
    }

    /// Send `Cancel` down one recorded child (§9.2): retried on *reported*
    /// failure with backoff — at-most-once per attempt, with the child's
    /// budget as the backstop under unhealed faults (§9.2 item 3).
    fn propagate_cancel(&mut self, child: ChildRef) {
        let harness = self.harness.clone();
        let clock = harness.clock();
        // The child kind is not needed for a cancel; SessionRef's kind field
        // only matters on submission. Use a placeholder.
        let launcher = harness.clone();
        launcher.system().launch(Box::pin(async move {
            let child_ref = harness.session("", child.session.clone());
            let mut attempt: u32 = 0;
            loop {
                match child_ref.cancel(&child.turn).await {
                    Ok(()) => return,
                    Err(_) if attempt < DELEGATION_ATTEMPTS => {
                        attempt += 1;
                        clock.sleep(propagation_backoff(attempt)).await;
                    }
                    Err(_) => return,
                }
            }
        }));
    }

    // -- deactivation (§7.2) -------------------------------------------------

    /// Stop serving: every attached caller learns the attachment is gone, the
    /// sandbox is carried out for release, and the actor stops. Everything
    /// worth keeping is already a record (§6.3) — deactivation needs no
    /// flush.
    fn go_gone(&mut self) {
        let phase = std::mem::replace(&mut self.phase, Phase::Gone { sandbox: None });
        let sandbox = match phase {
            Phase::Ready(mut r) => {
                for (_, senders) in std::mem::take(&mut r.waiters) {
                    for tx in senders {
                        let _ = tx.send(Awaited::Lost);
                    }
                }
                self.sandbox_was_bound_on_stop = r.sandbox_bound;
                match r.sandbox {
                    SandboxSlot::Open(sandbox) => Some(sandbox),
                    _ => None,
                }
            }
            Phase::Boot { queued } => {
                for op in queued {
                    if let QueuedOp::Submit(op) = op {
                        let _ = op.tx.send(Awaited::Lost);
                    }
                }
                None
            }
            Phase::Gone { sandbox } => sandbox,
        };
        self.phase = Phase::Gone { sandbox };
        self.request_stop();
    }
}

/// Attach a caller to a turn's eventual outcome (§7.4).
fn attach(r: &mut Ready, turn: TurnId, tx: oneshot::Sender<Awaited>) {
    r.waiters.entry(turn).or_default().push(tx);
}

/// Resolve every caller attached to `turn`.
fn notify(r: &mut Ready, turn: &TurnId, make: impl Fn() -> Awaited) {
    if let Some(senders) = r.waiters.remove(turn) {
        for tx in senders {
            let _ = tx.send(make());
        }
    }
}

/// Exponential backoff for the delegation/cancel retry loops, capped.
fn propagation_backoff(attempt: u32) -> Duration {
    let base = Duration::from_millis(200);
    (base * 2u32.saturating_pow(attempt.saturating_sub(1).min(8))).min(PROPAGATION_BACKOFF_CAP)
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
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "kind": { "type": "string", "enum": kinds },
                "prompt": { "type": "string" },
                "budget": {
                    "type": "object",
                    "properties": {
                        "tokens": { "type": "integer" },
                        "steps": { "type": "integer" }
                    }
                }
            },
            "required": ["kind", "prompt"]
        }),
    }
}

/// Run one sandboxed call to completion, bounded by its per-tool timeout
/// (§5.3 item 3), and deliver the outcome back onto the actor's executor.
fn launch_exec<S: HarnessSystem>(
    harness: &Harness<S>,
    me: &ActorRef<AgentActor<S>>,
    sandbox: Arc<dyn Sandbox>,
    exec: PendingExec,
) {
    let me = me.clone();
    let clock = harness.clock();
    harness.system().launch(Box::pin(async move {
        let outcome = match clock
            .timeout(exec.timeout, sandbox.call(&exec.name, exec.input))
            .await
        {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(ToolError::Timeout),
        };
        let _ = me
            .when_local(move |agent: &mut AgentActor<S>| {
                agent.tool_done(exec.turn, exec.call, outcome)
            })
            .await;
    }));
}

impl<S: HarnessSystem> Actor for AgentActor<S> {
    type System = S;

    async fn started(&mut self, ctx: &Ctx<Self>) -> Result<(), BoxError> {
        self.me = Some(ctx.this());
        // Activation loads the journal off the executor (§3.2): the mailbox
        // is live immediately, and operations queue in `Boot` until the fold
        // lands.
        let harness = self.harness.clone();
        let session = self.session.clone();
        let me = ctx.this();
        let journal = Arc::clone(harness.journal());
        let clock = harness.clock();
        let attempts = harness.config().journal_attempts.max(1);
        let backoff = harness.config().journal_backoff;
        harness.system().launch(Box::pin(async move {
            let mut attempt = 0;
            let result = loop {
                attempt += 1;
                match load_all(&*journal, &session).await {
                    Ok(records) => break Ok(records),
                    Err(e) if attempt < attempts => {
                        let factor = 2u32.saturating_pow(attempt - 1);
                        clock.sleep(backoff * factor).await;
                        let _ = e;
                    }
                    Err(e) => break Err(e.to_string()),
                }
            };
            let _ = me
                .when_local(move |agent: &mut AgentActor<S>| agent.loaded(result))
                .await;
        }));
        Ok(())
    }

    async fn stopped(self, _reason: StopReason) {
        // The single exit point for the pairing events (§10.4): whatever path
        // stopped this activation — drain, fence, fault — the sandbox is
        // released (H8) and the deactivation recorded.
        let node = self.harness.system().node();
        let session = self.session.clone();
        let (sandbox, bound, waiters) = match self.phase {
            Phase::Ready(mut r) => {
                let waiters: Vec<oneshot::Sender<Awaited>> = std::mem::take(&mut r.waiters)
                    .into_values()
                    .flatten()
                    .collect();
                let sandbox = match r.sandbox {
                    SandboxSlot::Open(sandbox) => Some(sandbox),
                    _ => None,
                };
                (sandbox, r.sandbox_bound, waiters)
            }
            Phase::Gone { sandbox } => (sandbox, self.sandbox_was_bound_on_stop, Vec::new()),
            Phase::Boot { queued } => {
                let waiters = queued
                    .into_iter()
                    .filter_map(|op| match op {
                        QueuedOp::Submit(op) => Some(op.tx),
                        QueuedOp::Cancel(_) => None,
                    })
                    .collect();
                (None, false, waiters)
            }
        };
        for tx in waiters {
            let _ = tx.send(Awaited::Lost);
        }
        if let Some(sandbox) = sandbox {
            sandbox.release().await;
        }
        if bound {
            self.harness.system().emit_event(
                HarnessEvent::SandboxReleased {
                    session: session.clone(),
                    node,
                }
                .into(),
            );
        }
        if self.activated {
            self.harness
                .system()
                .emit_event(HarnessEvent::SessionDeactivated { session, node }.into());
        }
    }

    /// Restart with backoff (§3.3): a restart is cheap by construction —
    /// state is a fold of the journal, so the restarted instance replays and
    /// continues, the same mechanism as a cross-node resume.
    fn supervision() -> Supervision {
        Supervision::restart(
            3,
            Duration::from_secs(60),
            actor_core::Backoff::Exponential {
                base: Duration::from_millis(100),
                max: Duration::from_secs(5),
            },
        )
    }
}

impl<S: HarnessSystem> Handler<Poke> for AgentActor<S> {
    async fn handle(&mut self, _msg: Poke, ctx: &Ctx<Self>) {
        if self.stop_requested {
            ctx.stop();
        }
    }
}

/// Load the whole journal, page by page (§6.1).
async fn load_all(
    journal: &dyn crate::journal::Journal,
    session: &SessionId,
) -> Result<Vec<(SeqNo, Record)>, crate::journal::JournalError> {
    let mut records = Vec::new();
    let mut from = SeqNo::ZERO;
    loop {
        let page = journal.load(session, from, LOAD_PAGE).await?;
        let len = page.len();
        if let Some((seq, _)) = page.last() {
            from = *seq;
        }
        records.extend(page);
        if len < LOAD_PAGE {
            return Ok(records);
        }
    }
}
